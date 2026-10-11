import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, UpdateQueryBuilder } from 'typeorm';
import {
  MAX_RESPONSE_TIME_MS,
  NotificationEntity,
  NotificationOutcomeReason,
  NotificationReason,
  toResponseTimeMs,
} from './entities/notification.entity';

/**
 * A row's move out of `pending`. Each variant carries everything the row
 * needs so no column is ever written as NULL: `sent` gets its reason
 * (DELIVERED) derived from the status, while `failed`/`cancelled` must name
 * the outcome that caused them. `responseTimeMs` is always arrival (the
 * job's jobs:notify timestamp) → now.
 */
export type NotificationStatusChange =
  | { status: 'sent'; responseTimeMs: number }
  | {
      status: 'failed' | 'cancelled';
      reason: NotificationOutcomeReason;
      responseTimeMs: number;
    };

/** A NotificationStatusChange that closes a row without it being sent. */
export type NotificationCloseChange = Extract<
  NotificationStatusChange,
  { status: 'failed' | 'cancelled' }
>;

/** The columns every status write must set, all non-nullable. */
type StatusColumns = Pick<
  NotificationEntity,
  'status' | 'statusChangedAt' | 'reason' | 'responseTimeMs'
>;

/** The full set of status columns for one row's change, stamped now. */
function toStatusColumns(change: NotificationStatusChange): StatusColumns {
  return {
    status: change.status,
    statusChangedAt: new Date(),
    reason:
      change.status === 'sent' ? NotificationReason.DELIVERED : change.reason,
    responseTimeMs: toResponseTimeMs(change.responseTimeMs),
  };
}

/**
 * Applies the SET clause for bulk-closing still-`pending` rows as `failed`/
 * `cancelled` — shared by the dead-letter write below and
 * SubscriberService.deactivateUser, which runs inside its own transaction
 * and so passes its own manager's query builder in. Callers add their own
 * WHERE, which must keep `status = 'pending'`: the response_time_ms
 * arithmetic relies on it.
 *
 * Bulk SQL has no per-row arrival time, but a pending row doesn't need
 * one: its response_time_ms already holds arrival→insert and its
 * status_changed_at holds the insert time, so adding the time elapsed
 * since status_changed_at gives arrival→now. (Postgres evaluates every SET
 * expression against the row's old values, so status_changed_at here is
 * still the insert time even though the same statement overwrites it.)
 *
 * `now` is bound as a parameter instead of using now()/LOCALTIMESTAMP so
 * the subtraction stays on the application's clock — the same one every
 * other status_changed_at write (a JS Date into a `timestamp` column) uses
 * — even if the Node process and the database session disagree on time
 * zone. Clamped to [0, MAX_RESPONSE_TIME_MS] like toResponseTimeMs, so a
 * row left pending for weeks can't overflow the INTEGER column and abort
 * the whole statement.
 */
export function closePendingNotifications(
  qb: UpdateQueryBuilder<NotificationEntity>,
  status: 'failed' | 'cancelled',
  reason: NotificationOutcomeReason,
  now: Date = new Date(),
): UpdateQueryBuilder<NotificationEntity> {
  return qb
    .set({
      status,
      reason,
      statusChangedAt: now,
      responseTimeMs: () =>
        `LEAST(${MAX_RESPONSE_TIME_MS}, "response_time_ms"::bigint + GREATEST(0, ROUND(EXTRACT(EPOCH FROM (CAST(:closedAt AS timestamp) - "status_changed_at")) * 1000))::bigint)::int`,
    })
    .setParameter('closedAt', now);
}

@Injectable()
export class NotificationsService {
  constructor(
    @InjectRepository(NotificationEntity)
    private readonly notifications: Repository<NotificationEntity>,
  ) {}

  /**
   * Inserts one pending row per matched recipient, ignoring rows that
   * already exist so re-delivery of the same jobs:notify entry (via XCLAIM)
   * can never create duplicates.
   *
   * `enqueuedAtMs` is the job's arrival in jobs:notify (ms since epoch), so
   * each row starts out fully filled: status_changed_at = now (the insert),
   * reason = AWAITING_DELIVERY, response_time_ms = arrival→insert — the
   * baseline the later bulk failed/cancelled writes add onto (see
   * closePendingNotifications).
   */
  async insertPendingForChats(
    jobId: string,
    chatIds: string[],
    enqueuedAtMs: number,
  ): Promise<void> {
    if (chatIds.length === 0) return;

    const now = new Date();
    const pending: StatusColumns = {
      status: 'pending',
      statusChangedAt: now,
      reason: NotificationReason.AWAITING_DELIVERY,
      responseTimeMs: toResponseTimeMs(now.getTime() - enqueuedAtMs),
    };

    await this.notifications
      .createQueryBuilder()
      .insert()
      .into(NotificationEntity)
      .values(chatIds.map((chatId) => ({ jobId, chatId, ...pending })))
      .orIgnore()
      .execute();
  }

  /**
   * Records a single row's move out of `pending`, always rewriting all
   * three status columns together so none of them can go stale or NULL.
   */
  async setStatus(
    jobId: string,
    chatId: string,
    change: NotificationStatusChange,
  ): Promise<void> {
    await this.notifications.update({ jobId, chatId }, toStatusColumns(change));
  }

  /**
   * Like setStatus, but for closing a single row as `failed`/`cancelled`
   * only if it's still `pending` — the per-row counterpart of
   * closePendingNotifications, for a caller that knows the job's own
   * arrival time and so computes response_time_ms itself (arrival→now),
   * exactly as setStatus's callers do. A row that has already moved on
   * (sent by an earlier attempt, or cancelled by /stop with its own, more
   * specific reason) is left as it is. Resolves to whether a row was
   * actually closed.
   */
  async closeIfPending(
    jobId: string,
    chatId: string,
    change: NotificationCloseChange,
  ): Promise<boolean> {
    const result = await this.notifications.update(
      { jobId, chatId, status: 'pending' },
      toStatusColumns(change),
    );
    return (result.affected ?? 0) > 0;
  }

  /**
   * Bulk write for a dead-lettered notify:telegram entry (exhausted its
   * XCLAIM retries without ever sending) — only rows still `pending` are
   * touched, since some recipients in the same job may have already
   * succeeded, been blocked, or been cancelled before the entry died.
   */
  async markDeliveryRetriesExhausted(
    jobId: string,
    chatIds: string[],
  ): Promise<void> {
    if (chatIds.length === 0) return;
    await closePendingNotifications(
      this.notifications.createQueryBuilder().update(),
      'failed',
      NotificationReason.DELIVERY_RETRIES_EXHAUSTED,
    )
      .where('job_id = :jobId', { jobId })
      .andWhere('chat_id IN (:...chatIds)', { chatIds })
      .andWhere('status = :pending', { pending: 'pending' })
      .execute();
  }

  /**
   * Bulk write for job-level resume's stale rows: recipients still
   * `pending` past RESUME_WINDOW_SECONDS, which are no longer safe to
   * re-hand off (see NotificationReason.DELIVERY_STATUS_UNKNOWN). Same
   * shape as markDeliveryRetriesExhausted — only rows still `pending` are
   * touched, with response_time_ms computed in SQL as arrival→now.
   */
  async markDeliveryStatusUnknown(
    jobId: string,
    chatIds: string[],
  ): Promise<void> {
    if (chatIds.length === 0) return;
    await closePendingNotifications(
      this.notifications.createQueryBuilder().update(),
      'failed',
      NotificationReason.DELIVERY_STATUS_UNKNOWN,
    )
      .where('job_id = :jobId', { jobId })
      .andWhere('chat_id IN (:...chatIds)', { chatIds })
      .andWhere('status = :pending', { pending: 'pending' })
      .execute();
  }
}
