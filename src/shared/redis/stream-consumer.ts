import { Logger } from '@nestjs/common';
import type Redis from 'ioredis';
import { streamRef, trace } from '../logging/pipeline-trace';

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

/** Entries read (and deleted) per round trip when dropping startup
 * backlog — keeps memory and XDEL's argument list bounded however large
 * the backlog is. */
const BACKLOG_DROP_PAGE_SIZE = 500;

/** Upper bound for the startup summary's "still to process" count. */
const STARTUP_COUNT_LIMIT = 10_000;

/** A job id for trace lines, from either producer shape. */
function jobIdOf(fields: Record<string, string>): string {
  return fields.jobId ?? fields['Job UUID'] ?? '?';
}

/** The group's last-delivered-id, or null if the group isn't on the
 * stream. Throws (ERR no such key) if the stream itself doesn't exist. */
async function lastDeliveredIdOf(
  redis: Redis,
  stream: string,
  group: string,
): Promise<string | null> {
  const groups = (await redis.xinfo('GROUPS', stream)) as string[][];
  const mine = groups.find((g) => g[g.indexOf('name') + 1] === group);
  return mine ? mine[mine.indexOf('last-delivered-id') + 1] : null;
}

/**
 * How many entries are waiting to be delivered to `group` (after its
 * last-delivered-id), counting at most `max` — enough for a caller sizing
 * its own budget against it without reading the whole stream. With
 * `newerThanMs`, entries older than that are left out of the count (they're
 * about to be dropped by that stream's own age check anyway). A missing
 * stream or group counts as 0.
 */
export async function countUndelivered(
  redis: Redis,
  stream: string,
  group: string,
  max: number,
  newerThanMs?: number,
): Promise<number> {
  if (max <= 0) return 0;

  let lastDeliveredId: string | null;
  try {
    lastDeliveredId = await lastDeliveredIdOf(redis, stream, group);
  } catch (err) {
    if ((err as Error).message.includes('no such key')) return 0;
    throw err;
  }
  if (lastDeliveredId === null) return 0;

  const min =
    newerThanMs !== undefined &&
    streamEntryTimestampMs(lastDeliveredId) < newerThanMs
      ? `${newerThanMs}-0`
      : `(${lastDeliveredId}`;
  const newest = (await redis.xrevrange(stream, '+', min, 'COUNT', max)) as [
    string,
    string[],
  ][];
  return newest.length;
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
      /** Total deliveries an entry gets, counting the first read: once
       * XPENDING reports a delivery count of at least this many (every one
       * of them unacked), the entry is dead-lettered instead of reclaimed —
       * so 5 means the first attempt plus 4 retries, never a 6th. Omit to
       * reclaim forever (no dead-letter). */
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
      /** Also XDEL each dead-lettered entry from this stream, by its own
       * id — for a stream meant to hold only in-flight work, where a
       * permanently given-up entry shouldn't linger once it's been parked.
       * Runs strictly after the entry's XADD to deadLetterStream and its
       * XACK have both succeeded, so the entry is never deleted before a
       * copy of it is safely on the dead-letter stream; if either fails,
       * the entry is left exactly where it was (still on the PEL) for the
       * next sweep to try again. Omit to keep dead-lettered entries in the
       * stream (acked, so they're never redelivered). */
      deleteOnDeadLetter?: boolean;
      /** Caps how much undelivered backlog this group is handed on startup.
       * If more than this many entries piled up after this group's
       * last-delivered-id while nothing was consuming, the oldest excess is
       * treated as too stale to be worth processing and deleted from the
       * stream outright (so it's never delivered), keeping only the newest
       * N. A function is evaluated once, at that startup check — for a
       * caller whose budget depends on another stream's state (see
       * JobMoverService). Omit to always process the full backlog, however
       * large. Applied after maxBacklogAgeMs. */
      maxBacklogOnStartup?: number | (() => Promise<number>);
      /** On startup, deletes every undelivered entry older than this (by its
       * stream id's timestamp — when it was XADD'd), however few there
       * are: a job that sat out a long outage is stale whether 5 or 5,000
       * of them piled up. Omit to keep entries of any age. */
      maxBacklogAgeMs?: number;
      /** Called once per entry, with its fields, right before the startup
       * backlog check deletes it (by age or by count) — lets a caller close
       * out its own domain records for work that will now never happen.
       * Failures are logged and swallowed; they never block the drop. */
      onBacklogDropped?: (fields: Record<string, string>) => Promise<void>;
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
   * Runs once per (re)start, before the normal read loop. Only ever looks
   * at this group's undelivered backlog (everything after its
   * last-delivered-id) — never the PEL, whose entries were already handed
   * to a consumer and are resumed by sweepPending instead. Two passes:
   * first everything older than maxBacklogAgeMs, then, of what's left,
   * everything but the newest maxBacklogOnStartup. A healthy catch-up after
   * an ordinary restart hits neither and this is a no-op.
   *
   * Both passes walk the stream in fixed-size pages (the count pass finds
   * its boundary with one bounded XREVRANGE), so a backlog of millions of
   * entries after a very long outage is never loaded into memory or passed
   * to a single XDEL at once. Dropped entries are simply deleted: the
   * group's cursor doesn't need moving, since XREADGROUP only delivers
   * entries that still exist — and deleting page by page means a crash
   * midway leaves nothing half-done, just less left for the next start.
   */
  private async capBacklogOnStartup(): Promise<void> {
    const { maxBacklogOnStartup, maxBacklogAgeMs } = this.options;
    if (maxBacklogOnStartup === undefined && maxBacklogAgeMs === undefined) {
      return;
    }

    const lastDeliveredId = await lastDeliveredIdOf(
      this.redis,
      this.stream,
      this.group,
    );
    if (lastDeliveredId === null) return;
    const afterCursor = `(${lastDeliveredId}`;

    let expired = 0;
    if (maxBacklogAgeMs !== undefined) {
      const cutoffMs = Date.now() - maxBacklogAgeMs;
      if (cutoffMs > 0) {
        expired = await this.dropRange(afterCursor, `(${cutoffMs}-0`);
      }
    }

    let overCap = 0;
    let cap: number | undefined;
    if (maxBacklogOnStartup !== undefined) {
      const raw =
        typeof maxBacklogOnStartup === 'function'
          ? await maxBacklogOnStartup()
          : maxBacklogOnStartup;
      if (Number.isFinite(raw)) {
        cap = Math.max(0, Math.floor(raw));
        // The (cap+1)-th newest undelivered entry, if there is one, is the
        // newest one to drop — it and everything older than it go.
        const newest = (await this.redis.xrevrange(
          this.stream,
          '+',
          afterCursor,
          'COUNT',
          cap + 1,
        )) as unknown as [string, string[]][];
        if (newest.length > cap) {
          overCap = await this.dropRange(
            afterCursor,
            newest[newest.length - 1][0],
          );
        }
      }
    }

    // One summary line per stream per start, even when nothing was dropped
    // — after an outage this is the answer to "how much is about to go
    // out". The remaining count is bounded so it stays one cheap call.
    const remaining = await countUndelivered(
      this.redis,
      this.stream,
      this.group,
      STARTUP_COUNT_LIMIT,
    ).catch(() => undefined);
    trace(
      'STARTUP',
      'backlog',
      {
        stream: this.stream,
        group: this.group,
        cursor: lastDeliveredId,
        expired,
        maxAge:
          maxBacklogAgeMs === undefined
            ? 'off'
            : `${Math.round(maxBacklogAgeMs / 60_000)}min`,
        overCap,
        cap: cap ?? 'off',
        toProcess:
          remaining === undefined
            ? '?'
            : remaining >= STARTUP_COUNT_LIMIT
              ? `>=${STARTUP_COUNT_LIMIT}`
              : remaining,
      },
      expired > 0 || overCap > 0 ? 'warn' : 'log',
    );
  }

  /** Deletes every entry in [start, end] (XRANGE bounds, so `(` makes
   * either side exclusive), a page at a time, running onBacklogDropped on
   * each first. Returns how many were deleted. */
  private async dropRange(start: string, end: string): Promise<number> {
    let from = start;
    let total = 0;

    for (;;) {
      const page = (await this.redis.xrange(
        this.stream,
        from,
        end,
        'COUNT',
        BACKLOG_DROP_PAGE_SIZE,
      )) as unknown as [string, string[]][];
      if (page.length === 0) break;

      if (this.options.onBacklogDropped) {
        for (const [id, flat] of page) {
          await this.options
            .onBacklogDropped(fieldsToObject(flat))
            .catch((err: Error) =>
              this.logger.error(
                `onBacklogDropped callback failed for ${this.stream}#${id}: ${err.message}`,
                err.stack,
              ),
            );
        }
      }

      await this.redis.xdel(this.stream, ...page.map(([id]) => id));
      total += page.length;

      if (page.length < BACKLOG_DROP_PAGE_SIZE) break;
      from = `(${page[page.length - 1][0]}`;
    }

    return total;
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
          deliveryCount >= this.options.maxDeliveryAttempts
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
   * cycling through claim/fail/reclaim — and, with deleteOnDeadLetter,
   * only then deletes the original from this stream. */
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
        // Not `_originalId`: entries moved by JobMoverService already carry
        // that field (the jobs:notify id their arrival time is derived
        // from), and a second field of the same name would shadow it on
        // read-back, losing that arrival time if the entry is replayed.
        '_deadLetteredFromId',
        id,
        '_reason',
        `reached ${this.options.maxDeliveryAttempts} delivery attempts`,
        '_deadLetteredAt',
        String(Date.now()),
      );
      await this.redis.xack(this.stream, this.group, id);
      if (this.options.deleteOnDeadLetter) {
        await this.redis.xdel(this.stream, id);
      }
      trace(
        'PEL',
        'dead',
        {
          job: jobIdOf(fields),
          from: streamRef(this.stream, id),
          to: this.options.deadLetterStream,
          reason: `reached ${this.options.maxDeliveryAttempts} delivery attempts`,
        },
        'error',
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
      trace(
        'PEL',
        'reclaimed',
        {
          job: jobIdOf(fieldsToObject(flat)),
          entry: streamRef(this.stream, id),
          reason: 'left unacked by an earlier attempt, retrying',
        },
        'warn',
      );
      await this.processEntry(id, flat);
    }
  }
}
