import {
  ConflictException,
  Inject,
  Injectable,
  NotFoundException,
} from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { DataSource, LessThanOrEqual, Repository } from 'typeorm';
import type Redis from 'ioredis';
import { REDIS_CLIENT, REDIS_KEYS } from '../../shared/redis';
import { UserEntity } from './entities/user.entity';
import { CategoryEntity } from './entities/category.entity';
import { SourceEntity } from './entities/source.entity';
import { UserCategoryEntity } from './entities/user-category.entity';
import { UserSourceEntity } from './entities/user-source.entity';
import {
  NotificationEntity,
  NotificationReason,
} from '../notifications/entities/notification.entity';

/** Hard-delete deactivated users after this long, per retention policy. */
export const RETENTION_DAYS = 30;

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
 * Subscriber matching is a direct Postgres query now (see
 * job-stream.service.ts's findSubscribers) — the sub:{categoryId}:
 * {sourceId} / sub:{categoryId} Redis sets this service used to maintain
 * are gone. `users:active` is unrelated to matching (it's a delivery-time
 * liveness gate in telegram-dispatch.service.ts) and is still kept here.
 */
@Injectable()
export class SubscriberService {
  constructor(
    @InjectRepository(UserEntity)
    private readonly users: Repository<UserEntity>,
    @InjectRepository(CategoryEntity)
    private readonly categories: Repository<CategoryEntity>,
    @InjectRepository(SourceEntity)
    private readonly sources: Repository<SourceEntity>,
    @InjectRepository(UserCategoryEntity)
    private readonly userCategories: Repository<UserCategoryEntity>,
    @InjectRepository(UserSourceEntity)
    private readonly userSources: Repository<UserSourceEntity>,
    private readonly dataSource: DataSource,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  /**
   * Idempotent row creation only — never reactivates an existing user.
   * Every command handler calls this first (matching every command in the
   * pre-split bot calling ensure_user before anything else), so the row is
   * guaranteed to exist by the time /start or the "Resume" button need to
   * explicitly activate it.
   */
  async ensureUser(chatId: string): Promise<UserEntity> {
    const existing = await this.users.findOne({ where: { chatId } });
    if (existing) return existing;

    const created = await this.users.save(
      this.users.create({ chatId, active: true, deactivatedAt: null }),
    );
    await this.redis.sadd(REDIS_KEYS.activeUsers, chatId);
    return created;
  }

  /**
   * Explicit activation — used by /start (always, even if already active)
   * and by the "Resume notifications" button shown on the pickers while
   * inactive.
   */
  async activateUser(chatId: string): Promise<void> {
    const user = await this.getUserOrThrow(chatId);
    user.active = true;
    user.deactivatedAt = null;
    await this.users.save(user);
    await this.redis.sadd(REDIS_KEYS.activeUsers, chatId);
  }

  /**
   * The one write /start makes (and the "Resume" button) — an upsert in a
   * single statement, no prior read needed: creates the row on a genuinely
   * first-ever activation, or reactivates an existing one either way.
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
   * Everything /start, /categories, /sources need to render their first
   * screen, in exactly one SELECT (a CTE for this chat's active state,
   * UNION ALL'd across categories and sources, each LEFT JOINed against
   * this chat's picks). Works identically whether or not chat_id has a
   * users row yet — a brand new chat just comes back with active=false and
   * every selected flag false — and this method never writes anything.
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
   * Composes the two calls above into the one synchronous request/response
   * an HTTP caller expects: optionally activate, then return the snapshot.
   * (/start passes activate:true; /categories, /sources pass false.)
   */
  async bootstrap(
    chatId: string,
    activate: boolean,
    username?: string | null,
  ): Promise<BootstrapSnapshot> {
    if (activate) {
      await this.activateUserUpsert(chatId, username);
    }
    return this.getBootstrapSnapshot(chatId);
  }

  async getEnabledCategories(): Promise<CategoryEntity[]> {
    return this.categories.find({ where: { enabled: true } });
  }

  async getEnabledSources(): Promise<SourceEntity[]> {
    return this.sources.find({ where: { enabled: true } });
  }

  /** Admin operation — adds a new category with a caller-chosen slug id
   * (e.g. 'graphic_design'), matched directly against jobs:notify's category
   * field. */
  async createCategory(id: string, name: string): Promise<CategoryEntity> {
    const existing = await this.categories.findOne({ where: { id } });
    if (existing) {
      throw new ConflictException(`Category '${id}' already exists`);
    }
    return this.categories.save(this.categories.create({ id, name }));
  }

  /** Admin operation — adds a new source with a caller-chosen slug id
   * (e.g. 'linkedin'), matched directly against jobs:notify's source field. */
  async createSource(id: string, name: string): Promise<SourceEntity> {
    const existing = await this.sources.findOne({ where: { id } });
    if (existing) {
      throw new ConflictException(`Source '${id}' already exists`);
    }
    return this.sources.save(this.sources.create({ id, name }));
  }

  async getUserSubscriptions(
    chatId: string,
  ): Promise<{ categoryIds: string[]; sourceIds: string[] }> {
    const user = await this.getUserOrThrow(chatId);
    const [cats, srcs] = await Promise.all([
      this.userCategories.find({ where: { chatId: user.chatId } }),
      this.userSources.find({ where: { chatId: user.chatId } }),
    ]);
    return {
      categoryIds: cats.map((c) => c.categoryId),
      sourceIds: srcs.map((s) => s.sourceId),
    };
  }

  /**
   * Replaces the user's category/source selection in Postgres. Upserts the
   * user row first so this works even for a chat whose only interaction so
   * far was /categories or /sources (never /start) — those are pure reads
   * and never create the row themselves. Picking zero sources means "any
   * source" for each picked category (mirrors the pre-split bot's picker:
   * "If you select none, you'll receive jobs from all sources") —
   * job-stream.service.ts's matching query treats a user with no
   * user_sources rows exactly this way.
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

  /**
   * Soft-deactivation, shared by the explicit /stop command and the
   * telegram-bot module reporting a delivery as permanently unreachable
   * (bot blocked). Keeps category/source rows so /start or "Resume" can
   * restore them later, and — matching cancel_pending_user_notifications —
   * cancels any of their notifications still sitting pending so a later
   * resubscribe doesn't trigger a burst of stale jobs. Hard deletion only
   * happens via the retention sweep.
   *
   * Tolerant of a chat_id that was never actually a subscriber — /stop must
   * be safe to send even for a chat with no users row, so this returns
   * `false` instead of throwing; callers that need 404 semantics (the HTTP
   * surface) check the return value themselves rather than relying on an
   * exception, so a bot caller can just ignore it.
   */
  async deactivateUser(
    chatId: string,
    reason: NotificationReason = NotificationReason.USER_UNSUBSCRIBED,
  ): Promise<boolean> {
    await this.redis.srem(REDIS_KEYS.activeUsers, chatId);

    return this.dataSource.transaction(async (manager) => {
      const result = await manager.update(
        UserEntity,
        { chatId },
        { active: false, deactivatedAt: new Date() },
      );
      if (!result.affected) return false; // never existed — nothing to cancel either

      await manager.update(
        NotificationEntity,
        { chatId, status: 'pending' },
        { status: 'cancelled', reason },
      );
      return true;
    });
  }

  /**
   * Daily retention sweep: hard-deletes users who have been deactivated for
   * longer than RETENTION_DAYS. Postgres FKs (ON DELETE CASCADE) take care
   * of user_categories/user_sources/notifications.
   */
  async hardDeleteExpiredDeactivations(): Promise<number> {
    const cutoff = new Date(Date.now() - RETENTION_DAYS * 24 * 60 * 60 * 1000);
    const result = await this.users.delete({
      active: false,
      deactivatedAt: LessThanOrEqual(cutoff),
    });
    return result.affected ?? 0;
  }

  private async getUserOrThrow(chatId: string): Promise<UserEntity> {
    const user = await this.users.findOne({ where: { chatId } });
    if (!user) {
      throw new NotFoundException(`No user with chatId=${chatId}`);
    }
    return user;
  }
}
