import { Logger } from '@nestjs/common';
import type Redis from 'ioredis';

export type StreamEntry = { id: string; fields: Record<string, string> };

/**
 * Redis Stream entry IDs are `<timestamp-ms>-<sequence>` — the millisecond
 * clock time the entry was XADD'd, assigned by the Redis server itself.
 * Extracting it gives an authoritative "when did this actually enter the
 * stream" timestamp for free, with no need to pass a separate clock
 * reading through the pipeline by hand.
 */
export function streamEntryTimestampMs(entryId: string): number {
  const [msPart] = entryId.split('-');
  return Number(msPart);
}

function fieldsToObject(flat: string[]): Record<string, string> {
  const obj: Record<string, string> = {};
  for (let i = 0; i < flat.length; i += 2) {
    obj[flat[i]] = flat[i + 1];
  }
  return obj;
}

/**
 * Runs a blocking XREADGROUP loop against a single stream/group, plus a
 * pending-entries sweep (once immediately at startup — crash recovery
 * shouldn't wait for the first interval tick — then periodically) to pick
 * up entries left on the PEL by a consumer that died or hung before
 * acking. Callers only implement `handle`; retry recovery is left almost
 * entirely to Redis's own pending-entries list — the one thing layered on
 * top is a delivery-count cap, past which an entry is moved to
 * `deadLetterStream` instead of being reclaimed forever.
 */
export class StreamConsumer {
  private readonly logger: Logger;
  private stopped = false;
  private loopPromise: Promise<void> | null = null;
  private claimInterval: NodeJS.Timeout | null = null;

  constructor(
    private readonly redis: Redis,
    private readonly stream: string,
    private readonly group: string,
    private readonly consumerName: string,
    private readonly handle: (entry: StreamEntry) => Promise<void>,
    private readonly options: {
      blockMs?: number;
      batchSize?: number;
      claimIntervalMs?: number;
      minIdleTimeMs?: number;
      /** Past this many delivery attempts, dead-letter instead of reclaim.
       * Omit to reclaim forever (no dead-letter). */
      maxDeliveryAttempts?: number;
      /** Stream poison entries are moved to. Required if maxDeliveryAttempts
       * is set. */
      deadLetterStream?: string;
      /** Called once per entry, with its original fields, right before it's
       * moved to deadLetterStream and acked — lets a caller record its own
       * domain-specific "this gave up permanently" outcome (e.g. marking a
       * DB row failed) without this generic consumer knowing anything about
       * that domain. Failures here are logged and swallowed; they never
       * block the dead-lettering itself. */
      onDeadLetter?: (fields: Record<string, string>) => Promise<void>;
      /** Caps how much undelivered backlog this group is handed on startup.
       * If more than this many entries piled up after this group's
       * last-delivered-id while nothing was consuming, the oldest excess is
       * treated as too stale to be worth processing: the group's cursor is
       * fast-forwarded past them (so they're never delivered) and they're
       * deleted from the stream outright, keeping only the newest N. Omit
       * to always process the full backlog, however large. */
      maxBacklogOnStartup?: number;
    } = {},
  ) {
    this.logger = new Logger(`StreamConsumer:${stream}`);
  }

  async start(): Promise<void> {
    await this.ensureGroup();

    await this.capBacklogOnStartup().catch((err: Error) =>
      this.logger.error(`Backlog cap check failed: ${err.message}`, err.stack),
    );

    // Crash recovery: anything left on the PEL from before this process
    // started (or from a previous instance that died) is picked up here,
    // before we even start reading new entries — not after the first
    // periodic tick.
    await this.sweepPending().catch((err: Error) =>
      this.logger.error(
        `Initial pending-entries sweep failed: ${err.message}`,
        err.stack,
      ),
    );

    this.loopPromise = this.readLoop();
    const claimIntervalMs = this.options.claimIntervalMs ?? 30_000;
    this.claimInterval = setInterval(() => {
      this.sweepPending().catch((err: Error) =>
        this.logger.error(
          `Pending-entries sweep failed: ${err.message}`,
          err.stack,
        ),
      );
    }, claimIntervalMs);
  }

  async stop(): Promise<void> {
    this.stopped = true;
    if (this.claimInterval) clearInterval(this.claimInterval);
    if (this.loopPromise) await this.loopPromise;
  }

  private async ensureGroup(): Promise<void> {
    try {
      // '0', not '$': a group's starting offset only matters the one time
      // it's actually created — BUSYGROUP below means every later restart
      // is a no-op that leaves an existing group's position untouched, so
      // this only fires when the group (or stream) doesn't exist yet. '$'
      // would silently skip anything already sitting in the stream at that
      // moment (which is exactly how a whole day of backlog got lost
      // before); '0' means a first-time creation always sees everything.
      await this.redis.xgroup(
        'CREATE',
        this.stream,
        this.group,
        '0',
        'MKSTREAM',
      );
    } catch (err) {
      if (!(err as Error).message.includes('BUSYGROUP')) {
        throw err;
      }
    }

    await this.warnOnForeignGroups();
  }

  /**
   * This consumer must only ever read via its own group — XREADGROUP is
   * already scoped to `this.group` and can't see another group's traffic,
   * but a stray group left on the stream (an old deploy, an ad-hoc script,
   * a shared Redis instance someone else is poking at) still gets its own
   * full copy of every entry. That's silent, unbudgeted consumption of
   * this stream, so it's surfaced loudly here rather than left to be
   * noticed later as "why is X getting these too".
   */
  private async warnOnForeignGroups(): Promise<void> {
    const groups = (await this.redis.xinfo(
      'GROUPS',
      this.stream,
    )) as string[][];

    for (const group of groups) {
      const nameIdx = group.indexOf('name');
      const name = nameIdx === -1 ? undefined : group[nameIdx + 1];
      if (name && name !== this.group) {
        this.logger.warn(
          `Foreign consumer group "${name}" also exists on stream ${this.stream} — it receives its own full copy of every entry. If nothing owns it, remove it with XGROUP DESTROY ${this.stream} ${name}.`,
        );
      }
    }
  }

  /**
   * Runs once per (re)start, before the normal read loop — never mid-run,
   * since it fast-forwards the group's own cursor and that would race
   * with entries the loop is already about to read. Only ever fires when
   * this group's backlog (everything after its last-delivered-id) exceeds
   * maxBacklogOnStartup; a healthy catch-up after an ordinary restart is
   * always well under that and this is a no-op.
   */
  private async capBacklogOnStartup(): Promise<void> {
    const cap = this.options.maxBacklogOnStartup;
    if (cap === undefined) return;

    const groups = (await this.redis.xinfo(
      'GROUPS',
      this.stream,
    )) as string[][];
    const mine = groups.find((g) => g[g.indexOf('name') + 1] === this.group);
    if (!mine) return;

    const lastDeliveredId = mine[mine.indexOf('last-delivered-id') + 1];

    const backlog = (await this.redis.xrange(
      this.stream,
      `(${lastDeliveredId}`,
      '+',
    )) as unknown as [string, string[]][];

    if (backlog.length <= cap) return;

    const skipCount = backlog.length - cap;
    const skippedIds = backlog.slice(0, skipCount).map(([id]) => id);
    const cursorId = skippedIds[skippedIds.length - 1];

    await this.redis.xgroup('SETID', this.stream, this.group, cursorId);
    await this.redis.xdel(this.stream, ...skippedIds);

    this.logger.warn(
      `${this.stream}/${this.group} backlog was ${backlog.length}, exceeding the ${cap} cap — dropped the oldest ${skipCount} entries and kept only the newest ${cap}.`,
    );
  }

  private async readLoop(): Promise<void> {
    const blockMs = this.options.blockMs ?? 5_000;
    const batchSize = this.options.batchSize ?? 10;

    while (!this.stopped) {
      let results: [string, [string, string[]][]][] | null = null;
      try {
        results = (await this.redis.xreadgroup(
          'GROUP',
          this.group,
          this.consumerName,
          'COUNT',
          batchSize,
          'BLOCK',
          blockMs,
          'STREAMS',
          this.stream,
          '>',
        )) as unknown as [string, [string, string[]][]][] | null;
      } catch (err) {
        this.logger.error(
          `XREADGROUP failed: ${(err as Error).message}`,
          (err as Error).stack,
        );
        await new Promise((r) => setTimeout(r, 1_000));
        continue;
      }

      if (!results) continue;

      for (const [, entries] of results) {
        for (const [id, flat] of entries) {
          await this.processEntry(id, flat);
        }
      }
    }
  }

  private async processEntry(id: string, flat: string[]): Promise<void> {
    try {
      await this.handle({ id, fields: fieldsToObject(flat) });
      await this.redis.xack(this.stream, this.group, id);
    } catch (err) {
      this.logger.error(
        `Handler failed for ${this.stream}#${id}, leaving unacked for retry: ${(err as Error).message}`,
        (err as Error).stack,
      );
    }
  }

  /**
   * Walks the group's PEL for entries idle at least minIdleTimeMs, using
   * XPENDING's extended form — it hands back delivery-count alongside idle
   * time in one call, which XAUTOCLAIM alone doesn't, so it decides
   * dead-letter vs reclaim before doing either.
   */
  private async sweepPending(): Promise<void> {
    const minIdleTimeMs = this.options.minIdleTimeMs ?? 60_000;
    const pageSize = 50;
    let start = '-';

    for (;;) {
      const page = (await this.redis.xpending(
        this.stream,
        this.group,
        'IDLE',
        minIdleTimeMs,
        start,
        '+',
        pageSize,
      )) as unknown as [string, string, number, number][] | null;

      if (!page || page.length === 0) break;

      const deadIds: string[] = [];
      const reclaimIds: string[] = [];
      for (const [id, , , deliveryCountRaw] of page) {
        const deliveryCount = Number(deliveryCountRaw);
        if (
          this.options.maxDeliveryAttempts !== undefined &&
          deliveryCount > this.options.maxDeliveryAttempts
        ) {
          deadIds.push(id);
        } else {
          reclaimIds.push(id);
        }
      }

      if (deadIds.length > 0) await this.deadLetter(deadIds);
      if (reclaimIds.length > 0) await this.reclaimAndProcess(reclaimIds);

      if (page.length < pageSize) break;
      start = `(${page[page.length - 1][0]}`;
    }
  }

  /** Claims entries just to read their fields, moves each to the
   * dead-letter stream with a reason, then acks the original so it stops
   * cycling through claim/fail/reclaim. */
  private async deadLetter(ids: string[]): Promise<void> {
    if (!this.options.deadLetterStream) {
      // Nowhere to put them — reclaim instead of silently dropping work.
      await this.reclaimAndProcess(ids);
      return;
    }

    const claimed = (await this.redis.xclaim(
      this.stream,
      this.group,
      this.consumerName,
      0,
      ...ids,
    )) as unknown as [string, string[]][];

    for (const [id, flat] of claimed) {
      const fields = fieldsToObject(flat);

      if (this.options.onDeadLetter) {
        await this.options.onDeadLetter(fields).catch((err: Error) => {
          this.logger.error(
            `onDeadLetter callback failed for ${this.stream}#${id}: ${err.message}`,
            err.stack,
          );
        });
      }

      await this.redis.xadd(
        this.options.deadLetterStream,
        '*',
        ...Object.entries(fields).flat(),
        '_originalId',
        id,
        '_reason',
        `exceeded ${this.options.maxDeliveryAttempts} delivery attempts`,
        '_deadLetteredAt',
        String(Date.now()),
      );
      await this.redis.xack(this.stream, this.group, id);
      this.logger.error(
        `${this.stream}#${id} exceeded max delivery attempts, moved to ${this.options.deadLetterStream}`,
      );
    }
  }

  /** Claims entries and runs each through the normal handle-then-ack path
   * — the exact same path a freshly read entry goes through. */
  private async reclaimAndProcess(ids: string[]): Promise<void> {
    const claimed = (await this.redis.xclaim(
      this.stream,
      this.group,
      this.consumerName,
      0,
      ...ids,
    )) as unknown as [string, string[]][];

    for (const [id, flat] of claimed) {
      await this.processEntry(id, flat);
    }
  }
}
