import { Inject, Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { DataSource, Repository } from 'typeorm';
import type Redis from 'ioredis';
import { REDIS_CLIENT, REDIS_KEYS } from '../shared/redis';
import { UserCategoryEntity } from '../core/subscriber/entities/user-category.entity';
import { UserSourceEntity } from '../core/subscriber/entities/user-source.entity';
import {
  NotificationEntity,
  NotificationReason,
} from '../core/notifications/entities/notification.entity';

export interface CategoryDto {
  id: string;
  name: string;
  enabled: boolean;
}

export interface SourceDto {
  id: string;
  name: string;
  enabled: boolean;
}

export interface BootstrapSnapshot {
  active: boolean;
  categoryIds: string[];
  sourceIds: string[];
  categories: CategoryDto[];
  sources: SourceDto[];
}

interface SnapshotRow {
  kind: 'category' | 'source';
  id: string;
  name: string;
  selected: boolean;
  active: boolean | null;
}

/**
 * The telegram-bot module's own direct Postgres access — no HTTP hop to
 * Core anymore. Core's subscriber.service.ts is untouched and still owns
 * the same tables for its own job-stream matching; this is a second,
 * independent data-access path onto the same schema, not a call into
 * Core's service instance.
 *
 * Reads and writes are deliberately split here: /start, /categories,
 * /sources are pure reads (getBootstrapSnapshot, one SELECT, never
 * touches Postgres for a write) — the row-creating/activating write for
 * /start happens afterward, in the background, so a slow or failed write
 * never delays the reply. See telegram-commands.service.ts for how that's
 * wired up.
 */
@Injectable()
export class BotSubscriberService {
  constructor(
    @InjectRepository(NotificationEntity)
    private readonly notifications: Repository<NotificationEntity>,
    private readonly dataSource: DataSource,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  /**
   * Everything /start, /categories, /sources need to render their first
   * screen, in exactly one SELECT (a CTE for this chat's active state,
   * UNION ALL'd across categories and sources, each LEFT JOINed against
   * this chat's picks). Works identically whether or not chat_id has a
   * users row yet — a brand new chat just comes back with active=false
   * and every selected flag false, no separate existence check needed,
   * and this method never writes anything.
   */
  async getBootstrapSnapshot(chatId: string): Promise<BootstrapSnapshot> {
    const rows: SnapshotRow[] = await this.dataSource.query(
      `WITH target_user AS (
         SELECT active FROM users WHERE chat_id = $1
       )
       SELECT 'category' AS kind, c.id, c.name,
              (uc.chat_id IS NOT NULL) AS selected, u.active
       FROM categories c
       LEFT JOIN user_categories uc ON uc.category_id = c.id AND uc.chat_id = $1
       LEFT JOIN target_user u ON true
       WHERE c.enabled = true

       UNION ALL

       SELECT 'source' AS kind, s.id, s.name,
              (us.chat_id IS NOT NULL) AS selected, u.active
       FROM sources s
       LEFT JOIN user_sources us ON us.source_id = s.id AND us.chat_id = $1
       LEFT JOIN target_user u ON true
       WHERE s.enabled = true

       ORDER BY kind, name`,
      [chatId],
    );

    const categories: CategoryDto[] = [];
    const sources: SourceDto[] = [];
    const categoryIds: string[] = [];
    const sourceIds: string[] = [];
    let active = false;

    for (const row of rows) {
      active = Boolean(row.active);
      if (row.kind === 'category') {
        categories.push({ id: row.id, name: row.name, enabled: true });
        if (row.selected) categoryIds.push(row.id);
      } else {
        sources.push({ id: row.id, name: row.name, enabled: true });
        if (row.selected) sourceIds.push(row.id);
      }
    }

    return { active, categoryIds, sourceIds, categories, sources };
  }

  /**
   * The one write /start makes — deliberately called *after* the reply,
   * never before. Upserts in a single statement: creates the row on a
   * genuinely first-ever /start, or reactivates an existing one — either
   * way, one round trip, no prior read needed (getBootstrapSnapshot
   * already told the caller everything it needs to render the reply).
   */
  async activateUserUpsert(
    chatId: string,
    username?: string | null,
  ): Promise<void> {
    await this.dataSource.query(
      `INSERT INTO users (chat_id, active, deactivated_at, username)
       VALUES ($1, true, NULL, $2)
       ON CONFLICT (chat_id) DO UPDATE SET active = true, deactivated_at = NULL, username = EXCLUDED.username`,
      [chatId, username ?? null],
    );
    await this.redis.sadd(REDIS_KEYS.activeUsers, chatId);
  }

  /**
   * /stop's write (reason: USER_UNSUBSCRIBED) — also the write for the
   * auto-deactivation that follows a Telegram 403 (reason: BOT_BLOCKED),
   * both called after their respective replies/logging. Tolerant of a
   * chat_id that was never actually a subscriber (no row to deactivate is
   * treated as success, not an error worth telling the user to retry).
   */
  async deactivateUser(
    chatId: string,
    reason: NotificationReason,
  ): Promise<void> {
    await this.redis.srem(REDIS_KEYS.activeUsers, chatId);

    await this.dataSource.transaction(async (manager) => {
      const queryResult: [unknown[], number] = await manager.query(
        `UPDATE users SET active = false, deactivated_at = now() WHERE chat_id = $1`,
        [chatId],
      );
      const rowCount = queryResult[1];
      if (rowCount === 0) return; // never existed — nothing to cancel either
      await manager.query(
        `UPDATE notifications SET status = 'cancelled', reason = $2 WHERE chat_id = $1 AND status = 'pending'`,
        [chatId, reason],
      );
    });
  }

  /**
   * Replaces the user's category/source selection in Postgres. Upserts the
   * user row first so this works even for a chat whose only interaction
   * so far was /categories or /sources (never /start) — those commands are
   * pure reads and never create the row themselves.
   */
  async updateSubscriptions(
    chatId: string,
    categoryIds: string[],
    sourceIds: string[],
    username?: string | null,
  ): Promise<{ categoryIds: string[]; sourceIds: string[] }> {
    await this.dataSource.transaction(async (manager) => {
      await manager.query(
        `INSERT INTO users (chat_id, active, username) VALUES ($1, true, $2)
         ON CONFLICT (chat_id) DO UPDATE SET username = EXCLUDED.username`,
        [chatId, username ?? null],
      );
      await manager.delete(UserCategoryEntity, { chatId });
      await manager.delete(UserSourceEntity, { chatId });
      if (categoryIds.length > 0) {
        await manager.insert(
          UserCategoryEntity,
          categoryIds.map((categoryId) => ({ chatId, categoryId })),
        );
      }
      if (sourceIds.length > 0) {
        await manager.insert(
          UserSourceEntity,
          sourceIds.map((sourceId) => ({ chatId, sourceId })),
        );
      }
    });

    return { categoryIds, sourceIds };
  }

  async setNotificationStatus(
    jobId: string,
    chatId: string,
    status: 'sent' | 'failed',
    reason: NotificationReason | null = null,
  ): Promise<void> {
    await this.notifications.update(
      { jobId, chatId },
      { status, sentAt: status === 'sent' ? new Date() : null, reason },
    );
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
    await this.notifications
      .createQueryBuilder()
      .update()
      .set({
        status: 'failed',
        reason: NotificationReason.DELIVERY_RETRIES_EXHAUSTED,
      })
      .where('job_id = :jobId', { jobId })
      .andWhere('chat_id IN (:...chatIds)', { chatIds })
      .andWhere('status = :pending', { pending: 'pending' })
      .execute();
  }
}
