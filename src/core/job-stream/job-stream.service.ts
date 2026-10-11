import { randomUUID } from 'crypto';
import {
  Inject,
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import type Redis from 'ioredis';
import { DataSource } from 'typeorm';
import {
  CONSUMER_GROUPS,
  DEFAULT_STARTUP_BACKLOG_CAP,
  MAX_DELIVERY_ATTEMPTS,
  REDIS_CLIENT,
  REDIS_STREAMS,
  StreamConsumer,
  StreamEntry,
  streamEntryTimestampMs,
} from '../../shared/redis';
import { colorDuration, colorModule } from '../../shared/logging/colors';
import { NotificationsService } from '../notifications/notifications.service';

/**
 * Matches Python's actual jobs:notify payload field names (see
 * docs/jobs-new-stream-contract.md). Only Title/Description/Source/Short
 * URL/Categories reach the rendered message — Job UUID, URL, Decision
 * Reason, Category ID, and Category Selection Method are present but
 * intentionally not surfaced to users.
 */
interface JobPayload {
  Title?: string;
  Description?: string;
  Source?: string;
  'Short URL'?: string;
  Categories?: string[];
  [key: string]: unknown;
}

@Injectable()
export class JobStreamService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(JobStreamService.name);
  private consumer: StreamConsumer;
  private readonly loggingEnabled: boolean;

  constructor(
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
    private readonly dataSource: DataSource,
    private readonly notifications: NotificationsService,
    private readonly config: ConfigService,
  ) {
    this.loggingEnabled = config.get<string>('NODE_ENV') !== 'test';
  }

  async onModuleInit() {
    const backlogCap = Number(
      this.config.get<string>('JOB_STREAM_BACKLOG_CAP') ??
        DEFAULT_STARTUP_BACKLOG_CAP,
    );

    this.consumer = new StreamConsumer(
      this.redis,
      REDIS_STREAMS.jobsDelivering,
      CONSUMER_GROUPS.delivering,
      `worker-${process.pid}-${randomUUID()}`,
      (entry) => this.handleJob(entry),
      {
        maxBacklogOnStartup: backlogCap,
        // Without a cap, an entry whose handling keeps throwing (Postgres
        // unreachable for it, a bug tripped by one specific payload, ...)
        // would be reclaimed and re-failed on every pending-entries sweep
        // forever. Past MAX_DELIVERY_ATTEMPTS it's parked on the
        // dead-letter stream instead, where it can be inspected and
        // replayed by hand.
        maxDeliveryAttempts: MAX_DELIVERY_ATTEMPTS,
        deadLetterStream: REDIS_STREAMS.jobsDeliveringDead,
        onDeadLetter: (fields) => this.removeDeadLetteredEntry(fields),
      },
    );
    await this.consumer.start();
  }

  async onModuleDestroy() {
    await this.consumer?.stop();
  }

  private async handleJob(entry: StreamEntry): Promise<void> {
    const start = process.hrtime.bigint();
    // The *original* jobs:notify entry's own timestamp — when this job
    // actually arrived, not when JobMoverService re-XADD'd it into
    // jobs:delivering under a new id. JobMoverService preserves the
    // original id in _originalId for exactly this; falls back to this
    // entry's own id for anything that predates that field existing.
    const enqueuedAtMs = streamEntryTimestampMs(
      entry.fields._originalId ?? entry.id,
    );

    const { jobId, category, source, payload } = this.normalizeEntry(
      entry.fields,
    );
    if (!jobId || !category || !source) {
      // Dropped like an unusable payload below: deleted from
      // jobs:delivering as well as acked, so it doesn't sit there forever.
      this.logger.warn(`Dropping malformed jobs:delivering entry ${entry.id}`);
      await this.redis.xdel(REDIS_STREAMS.jobsDelivering, entry.id);
      return;
    }

    // A payload that isn't a JSON object can never render into a
    // meaningful message, and retrying won't change that — so it's dropped
    // right here (deleted from jobs:delivering and, by returning normally,
    // acked) rather than thrown, which would leave it on the PEL to fail
    // again on every sweep until it's dead-lettered.
    const parsedPayload = this.parsePayload(payload);
    if (!parsedPayload) {
      this.logger.warn(
        `Dropping jobs:delivering entry ${entry.id} (job ${jobId}): payload is not a valid JSON object`,
      );
      await this.redis.xdel(REDIS_STREAMS.jobsDelivering, entry.id);
      return;
    }

    // Rows already recorded for this job mean an earlier attempt got at
    // least as far as insertPendingForChats — but not necessarily as far
    // as the notify:telegram XADD below (the process may have died in
    // between). So existing rows alone are no longer a reason to skip:
    //
    // - Recipients still `pending` were never confirmed handed off, so the
    //   job is re-handed off to exactly those. They're the recorded
    //   original fan-out, deliberately *not* a fresh re-match: re-matching
    //   now could pull in subscribers who joined after the job first
    //   arrived and were never part of it, which isn't a faithful retry.
    // - If no row is `pending` any more (all sent/failed/cancelled), the
    //   job was fully handed off and fully resolved — skip, exactly like
    //   the same-jobId redelivery idempotency in
    //   docs/jobs-new-stream-contract.md promises.
    //
    // Re-handing off a job whose first notify:telegram entry did in fact
    // go out (crash after the XADD but before the XDEL/ack here, or Python
    // redelivering the same jobId while the first fan-out is still being
    // sent) can't double-send either: TelegramDispatchService.deliverOne
    // atomically SADDs each chatId into notif:delivered:{jobId} *before*
    // sending and skips any chatId already in it. That holds even when
    // the two notify:telegram entries for one job are processed at the
    // same time by different senders — SADD is atomic, so for each
    // recipient exactly one of them gets the 1 and sends, and the other
    // gets 0 and skips. The only way the loser's skip could strand a
    // recipient is if the winner's send then fails transiently — but in
    // that case the winner SREMs the claim and throws, leaving *its*
    // entry unacked for XCLAIM to retry, so the recipient is still
    // covered.
    const recorded = await this.findRecordedRecipients(jobId);
    const isRetry = recorded.length > 0;
    let chatIds: string[];

    if (isRetry) {
      chatIds = recorded
        .filter((row) => row.status === 'pending')
        .map((row) => row.chat_id);
      if (chatIds.length === 0) {
        this.logger.debug(`Job ${jobId} already fully processed, skipping`);
        await this.redis.xdel(REDIS_STREAMS.jobsDelivering, entry.id);
        return;
      }
      this.logger.log(
        `Job ${jobId} was already partially processed — re-handing off to its ${chatIds.length} still-pending recipient(s)`,
      );
    } else {
      chatIds = await this.findSubscribers(category, source);
      if (chatIds.length === 0) {
        this.logger.debug(
          `Job ${jobId} (${category}/${source}) has no subscribers`,
        );
        await this.redis.xdel(REDIS_STREAMS.jobsDelivering, entry.id);
        return;
      }

      // Persist who is supposed to receive this job before handing it off
      // to the telegram-bot module, so a crash between here and the XADD
      // below never loses the record of an intended delivery — a retry
      // picks the recorded pending recipients back up (see above).
      await this.notifications.insertPendingForChats(
        jobId,
        chatIds,
        enqueuedAtMs,
      );
    }

    const categoryName = await this.getCategoryName(category);

    await this.redis.xadd(
      REDIS_STREAMS.notifyTelegram,
      '*',
      'jobId',
      jobId,
      'title',
      parsedPayload.Title ?? '',
      'link',
      parsedPayload['Short URL'] ?? '',
      'description',
      parsedPayload.Description ?? '',
      'categories',
      JSON.stringify(parsedPayload.Categories ?? []),
      'category',
      category,
      'categoryName',
      categoryName ?? category,
      'source',
      source,
      'chatIds',
      JSON.stringify(chatIds),
      'enqueuedAt',
      String(enqueuedAtMs),
    );

    // Fully handed off — jobs:delivering should only ever hold what's
    // genuinely still in flight, not a permanent history.
    await this.redis.xdel(REDIS_STREAMS.jobsDelivering, entry.id);

    if (this.loggingEnabled) {
      const processingMs = Number(process.hrtime.bigint() - start) / 1_000_000;
      console.log(
        [
          colorModule('CORE'),
          `job=${jobId}`,
          `${category}/${source}`,
          `${isRetry ? 'resumed' : 'matched'}=${chatIds.length}`,
          colorDuration(processingMs),
        ].join(' '),
      );
    }
  }

  /**
   * The Python producer is currently writing the raw job record's own
   * fields flat onto the stream entry (`Job UUID`, `Category ID`,
   * `Source`, ...) instead of the documented contract shape (top-level
   * `jobId`/`category`/`source` plus a JSON `payload` — see
   * docs/jobs-new-stream-contract.md). Rather than dropping every entry
   * as malformed, this maps the raw shape onto the contract shape this
   * service actually runs on. An entry already carrying the contract's
   * own top-level fields is passed through untouched, so this keeps
   * working if the producer is ever fixed to match the doc.
   */
  private normalizeEntry(fields: Record<string, string>): {
    jobId?: string;
    category?: string;
    source?: string;
    payload?: string;
  } {
    if (fields.jobId && fields.category && fields.source) {
      return fields;
    }

    const jobId = fields['Job UUID'];
    const category = fields['Category ID'];
    const source = fields['Source']?.toLowerCase();
    if (!jobId || !category || !source) {
      return {};
    }

    const payload = JSON.stringify({
      'Job UUID': jobId,
      Title: fields['Title'] ?? '',
      Description: fields['Description'] ?? '',
      Source: fields['Source'] ?? '',
      URL: fields['URL'] ?? '',
      'Short URL': fields['Short URL'] || fields['URL'] || '',
      'Decision Reason': fields['Decision Reason'] ?? '',
      Categories: (fields['Categories'] ?? '')
        .split(',')
        .map((c) => c.trim())
        .filter(Boolean),
      'Category ID': category,
      'Category Selection Method': fields['Category Selection Method'] ?? '',
    });

    return { jobId, category, source, payload };
  }

  /**
   * The job's `payload` field decoded into an object, `{}` if it's absent
   * or empty (the contract allows omitting it), or null if it's present
   * but unusable — not valid JSON at all, or valid JSON that isn't an
   * object (`"null"`, `"42"`, an array), any of which would otherwise
   * throw further down when its fields are read.
   */
  private parsePayload(payload: string | undefined): JobPayload | null {
    if (!payload) return {};
    let parsed: unknown;
    try {
      parsed = JSON.parse(payload);
    } catch {
      return null;
    }
    if (
      typeof parsed !== 'object' ||
      parsed === null ||
      Array.isArray(parsed)
    ) {
      return null;
    }
    return parsed as JobPayload;
  }

  /**
   * Every notifications row already recorded for this job, with its
   * status. Queried directly rather than through NotificationsService,
   * since all this needs is a read of the recipients an earlier attempt
   * recorded (see the retry handling in handleJob).
   */
  private async findRecordedRecipients(
    jobId: string,
  ): Promise<{ chat_id: string; status: string }[]> {
    return this.dataSource.query(
      `SELECT chat_id, status FROM notifications WHERE job_id = $1`,
      [jobId],
    );
  }

  /**
   * Called by StreamConsumer right before it dead-letters a jobs:delivering
   * entry that exhausted its delivery attempts. The consumer itself only
   * copies the entry to jobs:delivering:dead and acks it — it never XDELs
   * — which would leave a permanently-given-up entry sitting in
   * jobs:delivering, a stream meant to hold only what's genuinely in
   * flight. So it's deleted here.
   *
   * The callback is only handed the entry's fields, not its own id, so the
   * entry is found by its `_originalId` (the jobs:notify id JobMoverService
   * stamps on every entry it moves, unique per moved entry). That's a scan
   * of jobs:delivering, but a cheap one: the stream only ever holds
   * in-flight work, and this only runs for the rare entry that gives up
   * for good. Notification rows the entry may already have inserted are
   * deliberately left `pending`: replaying the dead-lettered entry into
   * jobs:delivering later hands them off again via the normal retry path.
   */
  private async removeDeadLetteredEntry(
    fields: Record<string, string>,
  ): Promise<void> {
    const originalId = fields._originalId;
    if (!originalId) {
      this.logger.warn(
        `Dead-lettered jobs:delivering entry has no _originalId, so it can't be located to delete it from ${REDIS_STREAMS.jobsDelivering}`,
      );
      return;
    }

    const pageSize = 100;
    let start = '-';
    for (;;) {
      const page = (await this.redis.xrange(
        REDIS_STREAMS.jobsDelivering,
        start,
        '+',
        'COUNT',
        pageSize,
      )) as [string, string[]][];

      for (const [id, flat] of page) {
        for (let i = 0; i < flat.length; i += 2) {
          if (flat[i] === '_originalId' && flat[i + 1] === originalId) {
            await this.redis.xdel(REDIS_STREAMS.jobsDelivering, id);
            return;
          }
        }
      }

      if (page.length < pageSize) return;
      start = `(${page[page.length - 1][0]}`;
    }
  }

  /**
   * The category's presentable display name (e.g. 'mobile_app' -> 'Mobile
   * App Development'), for the outbound message header — the id itself is
   * a routing slug, never meant to be shown to a subscriber. Falls back to
   * the id if the category was deleted after this job was matched against
   * it (row still exists in the seed data in practice, but matching isn't
   * contingent on the display name existing).
   */
  private async getCategoryName(categoryId: string): Promise<string | null> {
    const rows: { name: string }[] = await this.dataSource.query(
      `SELECT name FROM categories WHERE id = $1`,
      [categoryId],
    );
    return rows[0]?.name ?? null;
  }

  /**
   * Active subscribers for this exact category+source pairing, plus active
   * subscribers who picked the category but zero sources (accepts "any
   * source"). Runs directly against Postgres — see
   * IndexUserCategoriesForMatching migration for why that's indexed.
   */
  private async findSubscribers(
    categoryId: string,
    sourceId: string,
  ): Promise<string[]> {
    const rows: { chat_id: string }[] = await this.dataSource.query(
      `SELECT DISTINCT u.chat_id
       FROM users u
       JOIN user_categories uc ON uc.chat_id = u.chat_id AND uc.category_id = $1
       WHERE u.active = true
         AND (
           EXISTS (SELECT 1 FROM user_sources us WHERE us.chat_id = u.chat_id AND us.source_id = $2)
           OR NOT EXISTS (SELECT 1 FROM user_sources us WHERE us.chat_id = u.chat_id)
         )`,
      [categoryId, sourceId],
    );
    return rows.map((row) => row.chat_id);
  }
}
