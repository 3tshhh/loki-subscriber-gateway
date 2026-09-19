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
      { maxBacklogOnStartup: backlogCap },
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
      this.logger.warn(`Dropping malformed jobs:delivering entry ${entry.id}`);
      return;
    }

    if (await this.notifications.hasBeenProcessed(jobId)) {
      this.logger.debug(`Job ${jobId} already processed, skipping`);
      await this.redis.xdel(REDIS_STREAMS.jobsDelivering, entry.id);
      return;
    }

    const parsedPayload = (payload ? JSON.parse(payload) : {}) as JobPayload;

    const chatIds = await this.findSubscribers(category, source);
    if (chatIds.length === 0) {
      this.logger.debug(
        `Job ${jobId} (${category}/${source}) has no subscribers`,
      );
      await this.redis.xdel(REDIS_STREAMS.jobsDelivering, entry.id);
      return;
    }

    // Persist who is supposed to receive this job before handing it off to
    // the telegram-bot module, so a crash between here and the XADD below
    // never loses the record of an intended delivery.
    await this.notifications.insertPendingForChats(jobId, chatIds);

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
          `matched=${chatIds.length}`,
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
