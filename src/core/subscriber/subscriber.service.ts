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
import { NotificationEntity } from '../notifications/entities/notification.entity';

/** Hard-delete deactivated users after this long, per retention policy. */
export const RETENTION_DAYS = 30;

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
   * Everything a bot command needs to render its first screen, in one call:
   * ensures the row exists, optionally activates (true for /start, false
   * for /categories, /sources), and returns current state + both enabled
   * lists together.
   */
  async bootstrap(
    chatId: string,
    activate: boolean,
  ): Promise<{
    active: boolean;
    categoryIds: string[];
    sourceIds: string[];
    categories: CategoryEntity[];
    sources: SourceEntity[];
  }> {
    let user = await this.ensureUser(chatId);
    if (activate) {
      await this.activateUser(chatId);
      user = { ...user, active: true, deactivatedAt: null };
    }

    const [categoryRows, sourceRows, categories, sources] = await Promise.all([
      this.userCategories.find({ where: { chatId: user.chatId } }),
      this.userSources.find({ where: { chatId: user.chatId } }),
      this.getEnabledCategories(),
      this.getEnabledSources(),
    ]);

    return {
      active: user.active,
      categoryIds: categoryRows.map((r) => r.categoryId),
      sourceIds: sourceRows.map((r) => r.sourceId),
      categories,
      sources,
    };
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
   * Replaces the user's category/source selection in Postgres. Picking
   * zero sources means "any source" for each picked category (mirrors the
   * pre-split bot's picker: "If you select none, you'll receive jobs from
   * all sources") — job-stream.service.ts's matching query treats a user
   * with no user_sources rows exactly this way.
   */
  async updateSubscriptions(
    chatId: string,
    categoryIds: string[],
    sourceIds: string[],
  ): Promise<void> {
    const user = await this.getUserOrThrow(chatId);

    await this.dataSource.transaction(async (manager) => {
      await manager.delete(UserCategoryEntity, { chatId: user.chatId });
      await manager.delete(UserSourceEntity, { chatId: user.chatId });
      if (categoryIds.length > 0) {
        await manager.insert(
          UserCategoryEntity,
          categoryIds.map((categoryId) => ({
            chatId: user.chatId,
            categoryId,
          })),
        );
      }
      if (sourceIds.length > 0) {
        await manager.insert(
          UserSourceEntity,
          sourceIds.map((sourceId) => ({ chatId: user.chatId, sourceId })),
        );
      }
    });
  }

  /**
   * Soft-deactivation, shared by the explicit /stop command and the
   * telegram-bot module reporting a delivery as permanently unreachable
   * (bot blocked). Keeps category/source rows so /start or "Resume" can
   * restore them later, and — matching cancel_pending_user_notifications —
   * cancels any of their notifications still sitting pending so a later
   * resubscribe doesn't trigger a burst of stale jobs. Hard deletion only
   * happens via the retention sweep.
   */
  async deactivateUser(chatId: string): Promise<void> {
    const user = await this.getUserOrThrow(chatId);
    await this.redis.srem(REDIS_KEYS.activeUsers, chatId);

    await this.dataSource.transaction(async (manager) => {
      await manager.update(
        UserEntity,
        { chatId: user.chatId },
        { active: false, deactivatedAt: new Date() },
      );
      await manager.update(
        NotificationEntity,
        { chatId: user.chatId, status: 'pending' },
        { status: 'cancelled' },
      );
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
