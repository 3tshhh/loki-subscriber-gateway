import {
  Column,
  CreateDateColumn,
  Entity,
  JoinColumn,
  ManyToOne,
  PrimaryColumn,
} from 'typeorm';
import { UserEntity } from '../../subscriber/entities/user.entity';

export type NotificationStatus = 'pending' | 'sent' | 'failed' | 'cancelled';

/**
 * Why a row is in its current status — every row carries one, whatever its
 * status, so the column is never NULL. The outcome reasons (bot_blocked,
 * user_unsubscribed, recipient_inactive, delivery_retries_exhausted) are
 * set at the exact point each `failed`/`cancelled` outcome is detected,
 * never inferred after the fact; `pending`/`sent` rows get their status's
 * own fixed reason.
 */
export enum NotificationReason {
  /** The row's `pending` reason: matched and persisted, handed off to the
   * telegram-bot module, not yet resolved either way. */
  AWAITING_DELIVERY = 'awaiting_delivery',
  /** The row's `sent` reason: Telegram accepted the message. */
  DELIVERED = 'delivered',
  /** Telegram returned 403 (recipient blocked the bot) — the delivery
   * attempt itself is marked `failed` with this reason, and it also
   * triggers the cascading `cancelled` on that chat's other pending rows. */
  BOT_BLOCKED = 'bot_blocked',
  /** The recipient ran /stop — cancels their still-pending rows. */
  USER_UNSUBSCRIBED = 'user_unsubscribed',
  /** Dispatch found the recipient missing from users:active when it came
   * to send — e.g. a row inserted from a match against a user who was
   * deactivated in between, or Postgres and users:active having drifted
   * apart (see the README's users:active section). Nothing was sent, and
   * the row is `cancelled` rather than left `pending` forever. Only ever
   * written onto a row that's still `pending`, so a more specific reason
   * already recorded (user_unsubscribed, bot_blocked) is kept. */
  RECIPIENT_INACTIVE = 'recipient_inactive',
  /** Exhausted MAX_DELIVERY_ATTEMPTS XCLAIM retries and was moved to the
   * dead-letter stream without ever successfully sending. */
  DELIVERY_RETRIES_EXHAUSTED = 'delivery_retries_exhausted',
  /** Backfill-only (FillAllNotificationColumns migration): a `failed`/
   * `cancelled` row written before the `reason` column existed, whose real
   * cause was never recorded. Used instead of guessing one; application
   * code never writes it. */
  UNKNOWN_LEGACY = 'unknown_legacy',
}

/**
 * The reasons application code may give a `failed`/`cancelled` row — the
 * per-status reasons (AWAITING_DELIVERY/DELIVERED) are derived from the
 * status itself, and UNKNOWN_LEGACY only exists for backfilled history, so
 * neither can be passed in by mistake.
 */
export type NotificationOutcomeReason =
  | NotificationReason.BOT_BLOCKED
  | NotificationReason.USER_UNSUBSCRIBED
  | NotificationReason.RECIPIENT_INACTIVE
  | NotificationReason.DELIVERY_RETRIES_EXHAUSTED;

/** Every NotificationOutcomeReason value, for runtime validation (DTOs). */
export const NOTIFICATION_OUTCOME_REASONS: readonly NotificationOutcomeReason[] =
  [
    NotificationReason.BOT_BLOCKED,
    NotificationReason.USER_UNSUBSCRIBED,
    NotificationReason.RECIPIENT_INACTIVE,
    NotificationReason.DELIVERY_RETRIES_EXHAUSTED,
  ];

/** response_time_ms is a Postgres INTEGER — anything above this (~24.8
 * days) is clamped rather than overflowing the write. */
export const MAX_RESPONSE_TIME_MS = 2147483647;

/**
 * Normalizes a computed latency for response_time_ms: rounded to a whole
 * ms, floored at 0 (clock skew between Redis's stream-id clock and this
 * process can make a fresh row look a few ms "early"), and capped at
 * MAX_RESPONSE_TIME_MS.
 */
export function toResponseTimeMs(ms: number): number {
  if (!Number.isFinite(ms)) return 0;
  return Math.min(MAX_RESPONSE_TIME_MS, Math.max(0, Math.round(ms)));
}

@Entity('notifications')
export class NotificationEntity {
  @PrimaryColumn({ name: 'job_id' })
  jobId: string;

  @PrimaryColumn({ name: 'chat_id', type: 'bigint' })
  chatId: string;

  @ManyToOne(() => UserEntity, { onDelete: 'CASCADE' })
  @JoinColumn({ name: 'chat_id' })
  user: UserEntity;

  @Column({ type: 'text', default: 'pending' })
  status: NotificationStatus;

  @CreateDateColumn({ name: 'created_at' })
  createdAt: Date;

  /**
   * When the row last entered its current status — its insert time while
   * `pending`, then the moment it became `sent`/`failed`/`cancelled`.
   * Written by the application (never a DB default) so it shares one
   * clock with response_time_ms's arithmetic; see NotificationsService.
   */
  @Column({ name: 'status_changed_at', type: 'timestamp' })
  statusChangedAt: Date;

  /** Why the row is in its current status — see NotificationReason. */
  @Column({ type: 'text' })
  reason: NotificationReason;

  /**
   * End-to-end latency in ms — from the job's own jobs:notify timestamp
   * (its arrival) to this row's latest status change: arrival→insert while
   * `pending`, arrival→send for `sent` (the same number logged to the
   * terminal, see logDelivery in telegram-dispatch.service.ts), and
   * arrival→failure/cancellation for `failed`/`cancelled`.
   */
  @Column({ name: 'response_time_ms', type: 'int' })
  responseTimeMs: number;
}
