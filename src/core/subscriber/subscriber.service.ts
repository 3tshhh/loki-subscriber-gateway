import {
  ConflictException,
  Inject,
  Injectable,
  Logger,
  NotFoundException,
  OnModuleInit,
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
  NotificationOutcomeReason,
  NotificationReason,
} from '../notifications/entities/notification.entity';
import { closePendingNotifications } from '../notifications/notifications.service';

/** Hard-delete deactivated users after this long, per retention policy. */
export const RETENTION_DAYS = 30;

/** How many chat_ids go into each SADD while rebuilding `users:active` —
 * keeps any single command (and its argument list) bounded no matter how
 * many active users there are. */
const ACTIVE_USERS_REBUILD_CHUNK = 1000;

/** Safety TTL on the temporary rebuild key, so a process that dies
 * mid-rebuild doesn't leave a stray set behind forever. Cleared (PERSIST)
 * in the same MULTI as the RENAME, so `users:active` itself never expires. */
const ACTIVE_USERS_REBUILD_TTL_SECONDS = 60 * 60;

/** How long startup waits for the rebuild before letting boot continue
 * without it. The Redis client is configured with maxRetriesPerRequest:
 * null, so with Redis down a command would otherwise wait indefinitely and
 * hold up the whole application's startup with it. */
const ACTIVE_USERS_REBUILD_STARTUP_TIMEOUT_MS = 15_000;

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
 *
 * Postgres is the source of truth for who's active; `users:active` is a
 * copy written separately from it (activate: DB then SADD; deactivate: SREM
 * then DB), so the two can drift — a crash between the two steps, a Redis
 * flush or restore, a path like updateSubscriptions that creates an active
 * row without touching the set. Since dispatch silently skips anyone not
 * in the set, drift means silently missed deliveries, so the set is rebuilt
 * from Postgres on every startup (see rebuildActiveUsersSet).
 */
@Injectable()
export class SubscriberService implements OnModuleInit {
  private readonly logger = new Logger(SubscriberService.name);

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
   * Rebuilds `users:active` before anything that reads or writes it starts.
   * Deliberately OnModuleInit (awaited), not OnApplicationBootstrap: Nest
   * runs every module's onModuleInit — dependencies first, one module at a
   * time — before any onApplicationBootstrap, and TelegramBotModule (which
   * imports SubscriberModule, so its hooks run after this one's) starts
   * long-polling and the notify:telegram consumer from its own
   * onModuleInit. An OnApplicationBootstrap hook would therefore run with
   * the bot already polling; this one finishes before the bot sees a single
   * update, before dispatch consumes anything, and before the HTTP server
   * is listening (app.listen() only happens after init).
   *
   * That closes the /start-or-/stop-during-rebuild race for this process.
   * The residual window: a write from somewhere else — e.g. a previous
   * instance still polling or serving HTTP during an overlapping deploy —
   * landing between the SELECT and the RENAME is lost (an SADD overwritten,
   * or an SREM undone if its Postgres write hadn't committed before the
   * SELECT), until the next restart rebuilds again. The same window reopens
   * if the rebuild exceeds ACTIVE_USERS_REBUILD_STARTUP_TIMEOUT_MS: boot
   * carries on and the rebuild finishes in the background, still atomic,
   * but now concurrent with live traffic.
   */
  async onModuleInit(): Promise<void> {
    let timer: NodeJS.Timeout | undefined;
    const timedOut = await Promise.race([
      this.rebuildActiveUsersSet().then(() => false),
      new Promise<boolean>((resolve) => {
        timer = setTimeout(
          () => resolve(true),
          ACTIVE_USERS_REBUILD_STARTUP_TIMEOUT_MS,
        );
      }),
    ]);
    clearTimeout(timer);
    if (timedOut) {
      this.logger.warn(
        `users:active rebuild still running after ${ACTIVE_USERS_REBUILD_STARTUP_TIMEOUT_MS}ms — continuing startup, it will finish in the background`,
      );
    }
  }

  /**
   * Replaces `users:active` with exactly the chat_ids Postgres has as
   * active, atomically from dispatch's point of view: the new set is built
   * under a temporary per-process key (SADD in chunks), then swapped in
   * with a single RENAME — or `users:active` is DELeted outright if there
   * are no active users, since RENAME can't move a key that was never
   * created. Dispatch only ever sees the old set or the complete new one,
   * never a half-built one.
   *
   * Never throws: a failure is logged and the existing set is left as it
   * was (drifted, possibly, but no worse than before), so a Redis or
   * Postgres hiccup can't take startup down with it. Returns the number of
   * active users written, or null on failure.
   */
  async rebuildActiveUsersSet(): Promise<number | null> {
    const tempKey = `${REDIS_KEYS.activeUsers}:rebuild:${process.pid}`;
    try {
      const rows: { chat_id: string }[] = await this.dataSource.query(
        `SELECT chat_id FROM users WHERE active = true`,
      );

      // Anything left over under this key (a previous process with the same
      // pid that died mid-rebuild) must not leak into the new set.
      await this.redis.del(tempKey);
      for (let i = 0; i < rows.length; i += ACTIVE_USERS_REBUILD_CHUNK) {
        const chunk = rows
          .slice(i, i + ACTIVE_USERS_REBUILD_CHUNK)
          .map((row) => String(row.chat_id));
        await this.redis.sadd(tempKey, ...chunk);
        if (i === 0) {
          await this.redis.expire(tempKey, ACTIVE_USERS_REBUILD_TTL_SECONDS);
        }
      }

      if (rows.length > 0) {
        // RENAME carries the temp key's safety TTL over with it, so PERSIST
        // in the same MULTI — otherwise users:active itself would expire.
        const results = await this.redis
          .multi()
          .rename(tempKey, REDIS_KEYS.activeUsers)
          .persist(REDIS_KEYS.activeUsers)
          .exec();
        const failed = results?.find(([err]) => err);
        if (!results || failed) {
          throw failed?.[0] ?? new Error('MULTI was aborted');
        }
      } else {
        await this.redis.del(REDIS_KEYS.activeUsers);
      }

      this.logger.log(
        `Rebuilt users:active from Postgres: ${rows.length} active user(s)`,
      );
      return rows.length;
    } catch (err) {
      const error = err instanceof Error ? err : new Error(String(err));
      this.logger.error(
        `Failed to rebuild users:active from Postgres — leaving the existing set as-is: ${error.message}`,
        error.stack,
      );
      await this.redis.del(tempKey).catch(() => undefined);
      return null;
    }
  }

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
   *
   * `username` only ever overwrites a stored one when the caller actually
   * has a value (COALESCE): ctx.from?.username is undefined for Telegram
   * users without an @handle, and HTTP callers may omit it entirely —
   * neither should wipe a handle we already know. The tradeoff is that a
   * user who genuinely removes their Telegram handle keeps the stale one
   * here until they set a new one; that's preferred over losing a known
   * handle every time a caller simply doesn't pass it.
   */
  async activateUserUpsert(
    chatId: string,
    username?: string | null,
  ): Promise<void> {
    await this.dataSource.query(
      `INSERT INTO users (chat_id, active, deactivated_at, username)
       VALUES ($1, true, NULL, $2)
       ON CONFLICT (chat_id) DO UPDATE SET active = true, deactivated_at = NULL,
         username = COALESCE(EXCLUDED.username, users.username)`,
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
   *
   * A brand-new user created by this upsert is inserted as `active = true`
   * — intended behavior: finishing the picker is as clear an opt-in as
   * /start. An existing row's `active` flag is never touched here, though,
   * so a deactivated user editing their picks stays deactivated until /start
   * or "Resume". Note this path doesn't SADD to `users:active`, so a
   * brand-new user created here is only picked up by dispatch once /start
   * or "Resume" adds them to the set, or the next startup's
   * rebuildActiveUsersSet does.
   *
   * `username` follows the same COALESCE rule as activateUserUpsert: a
   * caller with no username never wipes a stored one (at the cost of a
   * removed Telegram handle lingering until a new one is set).
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
         ON CONFLICT (chat_id) DO UPDATE
         SET username = COALESCE(EXCLUDED.username, users.username)`,
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
    reason: NotificationOutcomeReason = NotificationReason.USER_UNSUBSCRIBED,
  ): Promise<boolean> {
    await this.redis.srem(REDIS_KEYS.activeUsers, chatId);

    return this.dataSource.transaction(async (manager) => {
      const result = await manager.update(
        UserEntity,
        { chatId },
        { active: false, deactivatedAt: new Date() },
      );
      if (!result.affected) return false; // never existed — nothing to cancel either

      // Same bulk write as the dead-letter path: status_changed_at = now
      // and response_time_ms = arrival→now for every row still pending.
      await closePendingNotifications(
        manager.createQueryBuilder().update(NotificationEntity),
        'cancelled',
        reason,
      )
        .where('chat_id = :chatId', { chatId })
        .andWhere('status = :pending', { pending: 'pending' })
        .execute();
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
