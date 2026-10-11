import { Logger } from '@nestjs/common';
import type Redis from 'ioredis';
import type { DataSource, Repository } from 'typeorm';
import { REDIS_KEYS } from '../../shared/redis';
import {
  NotificationEntity,
  NotificationReason,
} from '../notifications/entities/notification.entity';
import { UserCategoryEntity } from './entities/user-category.entity';
import { UserSourceEntity } from './entities/user-source.entity';
import { UserEntity } from './entities/user.entity';
import { SubscriberService } from './subscriber.service';

// @nestjs/config and @nestjs/typeorm ship as ES modules, which Jest can't
// require() on Node < 24.9. The service is constructed by hand here, so
// only the names its module graph references at load time are needed.
jest.mock('@nestjs/config', () => ({
  ConfigModule: {},
  ConfigService: class ConfigService {},
}));
jest.mock('@nestjs/typeorm', () => ({
  InjectRepository: () => () => undefined,
}));

const TEMP_KEY = `${REDIS_KEYS.activeUsers}:rebuild:${process.pid}`;

/** A chainable stand-in for TypeORM's update query builder. */
function queryBuilderMock() {
  const qb: Record<string, jest.Mock> = {};
  for (const method of ['update', 'set', 'setParameter', 'where', 'andWhere']) {
    qb[method] = jest.fn(() => qb);
  }
  qb.execute = jest.fn().mockResolvedValue({ affected: 2 });
  return qb;
}

describe('SubscriberService', () => {
  let redis: {
    sadd: jest.Mock;
    srem: jest.Mock;
    del: jest.Mock;
    expire: jest.Mock;
    multi: jest.Mock;
  };
  let multi: { rename: jest.Mock; persist: jest.Mock; exec: jest.Mock };
  let qb: ReturnType<typeof queryBuilderMock>;
  let manager: {
    query: jest.Mock;
    delete: jest.Mock;
    insert: jest.Mock;
    update: jest.Mock;
    createQueryBuilder: jest.Mock;
  };
  let dataSource: { query: jest.Mock; transaction: jest.Mock };
  let service: SubscriberService;

  beforeAll(() => {
    Logger.overrideLogger(false);
  });

  beforeEach(() => {
    multi = {
      rename: jest.fn().mockReturnThis(),
      persist: jest.fn().mockReturnThis(),
      exec: jest.fn().mockResolvedValue([
        [null, 'OK'],
        [null, 1],
      ]),
    };
    redis = {
      sadd: jest.fn().mockResolvedValue(1),
      srem: jest.fn().mockResolvedValue(1),
      del: jest.fn().mockResolvedValue(1),
      expire: jest.fn().mockResolvedValue(1),
      multi: jest.fn(() => multi),
    };
    qb = queryBuilderMock();
    manager = {
      query: jest.fn().mockResolvedValue([{ active: true }]),
      delete: jest.fn().mockResolvedValue({ affected: 0 }),
      insert: jest.fn().mockResolvedValue({}),
      update: jest.fn().mockResolvedValue({ affected: 1 }),
      createQueryBuilder: jest.fn(() => qb),
    };
    dataSource = {
      query: jest.fn().mockResolvedValue([]),
      transaction: jest.fn((work: (m: typeof manager) => Promise<unknown>) =>
        work(manager),
      ),
    };
    const repo = {} as Repository<never>;

    service = new SubscriberService(
      repo,
      repo,
      repo,
      repo,
      repo,
      dataSource as unknown as DataSource,
      redis as unknown as Redis,
    );
  });

  describe('updateSubscriptions', () => {
    it('upserts the user keeping a stored username when none is given', async () => {
      await service.updateSubscriptions('111', ['web_dev'], []);

      const [sql, params] = manager.query.mock.calls[0] as [string, unknown[]];
      expect(sql).toContain('ON CONFLICT (chat_id) DO UPDATE');
      expect(sql).toContain('COALESCE(EXCLUDED.username, users.username)');
      expect(sql).toContain('RETURNING active');
      expect(params).toEqual(['111', null]);
    });

    it('passes a given username through to the upsert', async () => {
      await service.updateSubscriptions('111', [], [], 'alice');

      expect((manager.query.mock.calls as unknown[][])[0][1]).toEqual([
        '111',
        'alice',
      ]);
    });

    it('replaces the stored category and source picks', async () => {
      const result = await service.updateSubscriptions(
        '111',
        ['web_dev', 'mobile_app'],
        ['upwork'],
      );

      expect(manager.delete).toHaveBeenCalledWith(UserCategoryEntity, {
        chatId: '111',
      });
      expect(manager.delete).toHaveBeenCalledWith(UserSourceEntity, {
        chatId: '111',
      });
      expect(manager.insert).toHaveBeenCalledWith(UserCategoryEntity, [
        { chatId: '111', categoryId: 'web_dev' },
        { chatId: '111', categoryId: 'mobile_app' },
      ]);
      expect(manager.insert).toHaveBeenCalledWith(UserSourceEntity, [
        { chatId: '111', sourceId: 'upwork' },
      ]);
      expect(result).toEqual({
        categoryIds: ['web_dev', 'mobile_app'],
        sourceIds: ['upwork'],
      });
    });

    it('inserts no picks when none are chosen', async () => {
      await service.updateSubscriptions('111', [], []);

      expect(manager.insert).not.toHaveBeenCalled();
    });

    it('adds the chat to users:active when the upserted row is active', async () => {
      manager.query.mockResolvedValue([{ active: true }]);

      await service.updateSubscriptions('111', ['web_dev'], []);

      expect(redis.sadd).toHaveBeenCalledWith(REDIS_KEYS.activeUsers, '111');
      expect(redis.sadd.mock.invocationCallOrder[0]).toBeGreaterThan(
        dataSource.transaction.mock.invocationCallOrder[0],
      );
    });

    it('leaves a stopped user out of users:active', async () => {
      manager.query.mockResolvedValue([{ active: false }]);

      await service.updateSubscriptions('111', ['web_dev'], []);

      expect(redis.sadd).not.toHaveBeenCalled();
    });

    it('does not touch users:active when the transaction fails', async () => {
      manager.delete.mockRejectedValue(new Error('deadlock'));

      await expect(
        service.updateSubscriptions('111', ['web_dev'], []),
      ).rejects.toThrow('deadlock');
      expect(redis.sadd).not.toHaveBeenCalled();
    });
  });

  describe('activateUserUpsert', () => {
    it('reactivates via an upsert that keeps a stored username, then joins users:active', async () => {
      await service.activateUserUpsert('111');

      const [sql, params] = dataSource.query.mock.calls[0] as [
        string,
        unknown[],
      ];
      expect(sql).toContain('COALESCE(EXCLUDED.username, users.username)');
      expect(params).toEqual(['111', null]);
      expect(redis.sadd).toHaveBeenCalledWith(REDIS_KEYS.activeUsers, '111');
    });
  });

  describe('rebuildActiveUsersSet', () => {
    const activeRows = (count: number) =>
      Array.from({ length: count }, (_, i) => ({ chat_id: 1000 + i }));

    it('builds a temporary set in chunks and swaps it in with RENAME', async () => {
      dataSource.query.mockResolvedValue(activeRows(2500));

      await expect(service.rebuildActiveUsersSet()).resolves.toBe(2500);

      expect(dataSource.query).toHaveBeenCalledWith(
        'SELECT chat_id FROM users WHERE active = true',
      );
      expect(redis.del).toHaveBeenCalledWith(TEMP_KEY);
      expect(redis.sadd).toHaveBeenCalledTimes(3);
      expect(
        (redis.sadd.mock.calls as string[][]).map(([key, ...ids]) => [
          key,
          ids.length,
        ]),
      ).toEqual([
        [TEMP_KEY, 1000],
        [TEMP_KEY, 1000],
        [TEMP_KEY, 500],
      ]);
      expect((redis.sadd.mock.calls as unknown[][])[0][1]).toBe('1000');
      expect(redis.expire).toHaveBeenCalledTimes(1);
      expect(redis.expire).toHaveBeenCalledWith(TEMP_KEY, expect.any(Number));
      expect(multi.rename).toHaveBeenCalledWith(
        TEMP_KEY,
        REDIS_KEYS.activeUsers,
      );
      expect(multi.persist).toHaveBeenCalledWith(REDIS_KEYS.activeUsers);
      expect(redis.del).not.toHaveBeenCalledWith(REDIS_KEYS.activeUsers);
    });

    it('deletes users:active outright when there are no active users', async () => {
      dataSource.query.mockResolvedValue([]);

      await expect(service.rebuildActiveUsersSet()).resolves.toBe(0);

      expect(redis.sadd).not.toHaveBeenCalled();
      expect(redis.multi).not.toHaveBeenCalled();
      expect(redis.del).toHaveBeenCalledWith(REDIS_KEYS.activeUsers);
    });

    it('returns null without throwing when Postgres fails, leaving users:active alone', async () => {
      dataSource.query.mockRejectedValue(new Error('connection refused'));

      await expect(service.rebuildActiveUsersSet()).resolves.toBeNull();

      expect(redis.multi).not.toHaveBeenCalled();
      expect(redis.del).not.toHaveBeenCalledWith(REDIS_KEYS.activeUsers);
      expect(redis.del).toHaveBeenCalledWith(TEMP_KEY);
    });

    it('returns null without throwing when the RENAME fails, cleaning up the temp key', async () => {
      dataSource.query.mockResolvedValue(activeRows(3));
      multi.exec.mockResolvedValue([
        [new Error('ERR no such key'), null],
        [null, 0],
      ]);

      await expect(service.rebuildActiveUsersSet()).resolves.toBeNull();

      expect(redis.del).toHaveBeenLastCalledWith(TEMP_KEY);
    });

    it('returns null when the MULTI is aborted', async () => {
      dataSource.query.mockResolvedValue(activeRows(3));
      multi.exec.mockResolvedValue(null);

      await expect(service.rebuildActiveUsersSet()).resolves.toBeNull();
    });

    it('never throws even when Redis cleanup fails too', async () => {
      dataSource.query.mockRejectedValue(new Error('pg down'));
      redis.del.mockRejectedValue(new Error('redis down'));

      await expect(service.rebuildActiveUsersSet()).resolves.toBeNull();
    });
  });

  describe('deactivateUser', () => {
    it('returns false for an unknown chat without cancelling anything', async () => {
      manager.update.mockResolvedValue({ affected: 0 });

      await expect(service.deactivateUser('999')).resolves.toBe(false);

      expect(redis.srem).toHaveBeenCalledWith(REDIS_KEYS.activeUsers, '999');
      expect(manager.createQueryBuilder).not.toHaveBeenCalled();
    });

    it('deactivates the user and cancels their pending rows with every column filled', async () => {
      await expect(service.deactivateUser('111')).resolves.toBe(true);

      expect(redis.srem).toHaveBeenCalledWith(REDIS_KEYS.activeUsers, '111');
      expect(redis.srem.mock.invocationCallOrder[0]).toBeLessThan(
        manager.update.mock.invocationCallOrder[0],
      );
      expect(manager.update).toHaveBeenCalledWith(
        UserEntity,
        { chatId: '111' },
        { active: false, deactivatedAt: expect.any(Date) as unknown },
      );
      expect(qb.update).toHaveBeenCalledWith(NotificationEntity);

      const values = (qb.set.mock.calls as unknown[][])[0][0] as Record<
        string,
        unknown
      >;
      for (const column of [
        'status',
        'statusChangedAt',
        'reason',
        'responseTimeMs',
      ]) {
        expect(values[column]).not.toBeNull();
        expect(values[column]).not.toBeUndefined();
      }
      expect(values).toEqual({
        status: 'cancelled',
        reason: NotificationReason.USER_UNSUBSCRIBED,
        statusChangedAt: expect.any(Date) as unknown,
        responseTimeMs: expect.any(Function) as unknown,
      });
      expect(qb.where).toHaveBeenCalledWith('chat_id = :chatId', {
        chatId: '111',
      });
      expect(qb.andWhere).toHaveBeenCalledWith('status = :pending', {
        pending: 'pending',
      });
      expect(qb.execute).toHaveBeenCalled();
    });

    it('records the given reason on the cancelled rows', async () => {
      await service.deactivateUser('111', NotificationReason.BOT_BLOCKED);

      const values = (qb.set.mock.calls as unknown[][])[0][0] as Record<
        string,
        unknown
      >;
      expect(values.reason).toBe(NotificationReason.BOT_BLOCKED);
    });
  });
});
