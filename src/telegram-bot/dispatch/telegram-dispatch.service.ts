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
import { Bot, GrammyError, InlineKeyboard } from 'grammy';
import PQueue from 'p-queue';
import {
  CONSUMER_GROUPS,
  DELIVERED_SET_TTL_SECONDS,
  MAX_DELIVERY_ATTEMPTS,
  REDIS_CLIENT,
  REDIS_KEYS,
  REDIS_STREAMS,
  StreamConsumer,
  StreamEntry,
} from '../../shared/redis';
import { BOT } from '../bot.provider';
import { BotSubscriberService } from '../bot-subscriber.service';
import { NotificationReason } from '../../core/notifications/entities/notification.entity';
import { buildJobMessage, safeButtonUrl } from './message-builder';
import {
  colorDeliveryDuration,
  colorModule,
  colorOutcome,
} from '../../shared/logging/colors';

/** Thrown for transient failures that should leave the stream entry unacked. */
class TransientDeliveryError extends Error {}

/** Total attempts (including the first) for a single 429-rate-limited send. */
const MAX_RATE_LIMIT_ATTEMPTS = 3;

type SendMessageOptions = NonNullable<Parameters<Bot['api']['sendMessage']>[2]>;

interface NotifyTelegramFields {
  jobId: string;
  title: string;
  link: string;
  description: string;
  category: string;
  /** The category's presentable display name (e.g. 'Mobile App
   * Development'), used for the message header — 'category' above is the
   * routing slug and is never shown to a subscriber. */
  categoryName: string;
  source: string;
  categories: string[];
  /** The original jobs:notify entry's own timestamp (ms since epoch) — lets
   * dispatch compute true end-to-end latency, not just its own processing
   * time. See streamEntryTimestampMs in shared/redis/stream-consumer.ts. */
  enqueuedAtMs: number;
}

@Injectable()
export class TelegramDispatchService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(TelegramDispatchService.name);
  private consumer: StreamConsumer;
  private readonly shortUrlDomain?: string;
  private readonly loggingEnabled: boolean;
  // Telegram's global cap is ~30 msg/sec; stay comfortably under it.
  private readonly queue = new PQueue({ intervalCap: 25, interval: 1000 });

  constructor(
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
    @Inject(BOT) private readonly bot: Bot,
    private readonly botData: BotSubscriberService,
    config: ConfigService,
  ) {
    this.shortUrlDomain = config.get<string>('SHORT_URL_DOMAIN');
    this.loggingEnabled = config.get<string>('NODE_ENV') !== 'test';
  }

  async onModuleInit() {
    this.consumer = new StreamConsumer(
      this.redis,
      REDIS_STREAMS.notifyTelegram,
      CONSUMER_GROUPS.telegramSenders,
      `telegram-sender-${process.pid}-${randomUUID()}`,
      (entry) => this.handleNotification(entry),
      {
        maxDeliveryAttempts: MAX_DELIVERY_ATTEMPTS,
        deadLetterStream: REDIS_STREAMS.notifyTelegramDead,
        onDeadLetter: (fields) => this.handleDeadLetter(fields),
      },
    );
    await this.consumer.start();
  }

  async onModuleDestroy() {
    await this.consumer?.stop();
  }

  private async handleNotification(entry: StreamEntry): Promise<void> {
    const {
      jobId,
      title,
      link,
      description,
      category,
      categoryName,
      source,
      categories,
      chatIds,
      enqueuedAt,
    } = entry.fields;
    if (!jobId || !chatIds) {
      this.logger.warn(`Dropping malformed notify:telegram entry ${entry.id}`);
      return;
    }

    const recipients = JSON.parse(chatIds) as string[];
    const job: NotifyTelegramFields = {
      jobId,
      title: title ?? '',
      link: link ?? '',
      description: description ?? '',
      category: category ?? '',
      categoryName: categoryName || category || '',
      source: source ?? '',
      categories: categories ? (JSON.parse(categories) as string[]) : [],
      enqueuedAtMs: enqueuedAt ? Number(enqueuedAt) : Date.now(),
    };

    const results = await Promise.allSettled(
      recipients.map((chatId) =>
        this.queue.add(() => this.deliverOne(job, chatId)),
      ),
    );

    const transientFailure = results.some(
      (r) =>
        r.status === 'rejected' && r.reason instanceof TransientDeliveryError,
    );
    if (transientFailure) {
      throw new TransientDeliveryError(
        `Job ${jobId} had transient delivery failures, will retry via XCLAIM`,
      );
    }
  }

  private async deliverOne(
    job: NotifyTelegramFields,
    chatId: string,
  ): Promise<void> {
    const { jobId } = job;

    const stillActive = await this.redis.sismember(
      REDIS_KEYS.activeUsers,
      chatId,
    );
    if (!stillActive) {
      this.logger.debug(
        `Skipping job ${jobId} for chat ${chatId}: no longer an active subscriber`,
      );
      return;
    }

    // Atomic claim, before any side effect: SADD itself reports whether
    // chatId was newly added (1) or already a member (0), so there's no
    // separate check-then-write race between this and a concurrent retry
    // of the same entry (e.g. after XCLAIM reclaims it elsewhere). A miss
    // here (0) means this is a replay — the actual send already happened
    // or is permanently given up on, so skip straight to done.
    const claimed = await this.claimDelivery(jobId, chatId);
    if (!claimed) {
      this.logger.debug(
        `Skipping job ${jobId} for chat ${chatId}: already handled in a previous attempt`,
      );
      return;
    }

    const decoratedUrl = this.decorateLink(job.link, chatId);
    const buttonUrl = safeButtonUrl(decoratedUrl);

    const text = buildJobMessage({
      title: job.title,
      description: job.description,
      source: job.source,
      categoryName: job.categoryName,
      categories: job.categories,
      url: job.link,
      hasButton: Boolean(buttonUrl),
    });

    const options: SendMessageOptions = {
      parse_mode: 'HTML',
      ...(buttonUrl
        ? { reply_markup: new InlineKeyboard().url('🔗 View Job', buttonUrl) }
        : {}),
    };

    try {
      await this.sendWithRetry(chatId, text, options);
    } catch (err) {
      if (err instanceof GrammyError && err.error_code === 403) {
        this.logger.warn(`Chat ${chatId} blocked the bot, deactivating`);
        // No send happened, but we're never retrying this recipient either
        // — keep the claim in place so a reclaim of this entry skips them.
        await this.reportStatus(
          jobId,
          chatId,
          'failed',
          NotificationReason.BOT_BLOCKED,
        );
        await this.botData.deactivateUser(
          chatId,
          NotificationReason.BOT_BLOCKED,
        );
        this.logDelivery(job, chatId, false);
        return;
      }

      this.logger.error(
        `Transient failure sending job ${jobId} to chat ${chatId}: ${(err as Error).message}`,
      );
      // Nothing was sent — release the claim so a retry of this entry can
      // actually attempt the send again, instead of finding chatId already
      // marked delivered and skipping it forever.
      await this.redis.srem(REDIS_KEYS.deliveredForJob(jobId), chatId);
      throw new TransientDeliveryError((err as Error).message);
    }

    // The message is already sent at this point, so any failure from here
    // on must never cause this entry to be retried — that would resend a
    // message the recipient already got. The claim was already made before
    // sending, so there's nothing left to record here.
    const endToEndMs = Date.now() - job.enqueuedAtMs;
    await this.reportStatus(jobId, chatId, 'sent', null, endToEndMs);
    this.logDelivery(job, chatId, true, endToEndMs);
  }

  /**
   * End-to-end latency: from the moment the job actually entered jobs:notify
   * (the stream entry's own timestamp, not when Core happened to start
   * processing it) to the moment this recipient's send resolved. Covers
   * the whole pipeline — Core matching/persisting, the notify:telegram
   * hop, PQueue throttling, and the Telegram API call itself.
   */
  private logDelivery(
    job: NotifyTelegramFields,
    chatId: string,
    ok: boolean,
    endToEndMs: number = Date.now() - job.enqueuedAtMs,
  ): void {
    if (!this.loggingEnabled) return;
    console.log(
      [
        colorModule('TELEGRAM-BOT'),
        `job=${job.jobId}`,
        `chat=${chatId}`,
        colorOutcome(ok),
        colorDeliveryDuration(endToEndMs),
        '(end-to-end)',
      ].join(' '),
    );
  }

  /** Retries a 429 exactly as Telegram asks (its own retry_after), capped. */
  private async sendWithRetry(
    chatId: string,
    text: string,
    options: SendMessageOptions,
    attempt = 1,
  ): Promise<void> {
    try {
      await this.bot.api.sendMessage(chatId, text, options);
    } catch (err) {
      if (
        err instanceof GrammyError &&
        err.error_code === 429 &&
        attempt < MAX_RATE_LIMIT_ATTEMPTS
      ) {
        const retryAfter = err.parameters?.retry_after ?? 1;
        await new Promise((r) => setTimeout(r, retryAfter * 1000));
        return this.sendWithRetry(chatId, text, options, attempt + 1);
      }
      throw err;
    }
  }

  /**
   * Appends ?userId=<chatId> to job links that point at our own shortener
   * domain, so click-through can be attributed back to the recipient. A
   * link we don't own is never decorated — sent exactly as received.
   */
  private decorateLink(link: string, chatId: string): string {
    if (!link || !this.shortUrlDomain) return link;
    try {
      const url = new URL(link);
      if (url.hostname !== this.shortUrlDomain) return link;
      url.searchParams.set('userId', chatId);
      return url.toString();
    } catch {
      return link;
    }
  }

  /**
   * Atomic "insert if absent" against the durable per-job delivered set —
   * SADD's return value (1 = newly added, 0 = already present) is itself
   * the dedup check, so there's no separate read-then-write step for a
   * concurrent reclaim to race against.
   */
  private async claimDelivery(jobId: string, chatId: string): Promise<boolean> {
    const key = REDIS_KEYS.deliveredForJob(jobId);
    const results = await this.redis
      .pipeline()
      .sadd(key, chatId)
      .expire(key, DELIVERED_SET_TTL_SECONDS)
      .exec();
    const [saddErr, saddResult] = results?.[0] ?? [];
    if (saddErr) throw saddErr;
    return saddResult === 1;
  }

  private async reportStatus(
    jobId: string,
    chatId: string,
    status: 'sent' | 'failed',
    reason: NotificationReason | null = null,
    responseTimeMs: number | null = null,
  ): Promise<void> {
    try {
      await this.botData.setNotificationStatus(
        jobId,
        chatId,
        status,
        reason,
        responseTimeMs,
      );
    } catch (err) {
      this.logger.error(
        `Failed to report status=${status} for job ${jobId} chat ${chatId} to core: ${(err as Error).message}`,
      );
    }
  }

  /**
   * Called by StreamConsumer right before it dead-letters a notify:telegram
   * entry that exhausted its XCLAIM retries — the entry's own fields still
   * carry jobId/chatIds, so the still-`pending` recipients among them (some
   * may have already succeeded/been blocked/been cancelled) get marked
   * `failed` with a reason instead of being left `pending` forever.
   */
  private async handleDeadLetter(
    fields: Record<string, string>,
  ): Promise<void> {
    const { jobId, chatIds } = fields;
    if (!jobId || !chatIds) return;
    try {
      const recipients = JSON.parse(chatIds) as string[];
      await this.botData.markDeliveryRetriesExhausted(jobId, recipients);
    } catch (err) {
      this.logger.error(
        `Failed to mark delivery-retries-exhausted for job ${jobId}: ${(err as Error).message}`,
      );
    }
  }
}
