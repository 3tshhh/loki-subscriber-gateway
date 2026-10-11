import { Logger } from '@nestjs/common';
import type { ConfigService } from '@nestjs/config';
import type Redis from 'ioredis';
import type { DataSource } from 'typeorm';
import {
  REDIS_STREAMS,
  RESUME_WINDOW_SECONDS,
  StreamEntry,
} from '../../shared/redis';
import { StreamConsumer } from '../../shared/redis/stream-consumer';
import type { NotificationsService } from '../notifications/notifications.service';
import { JobStreamService } from './job-stream.service';

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

type NormalizedEntry = {
  jobId?: string;
  category?: string;
  source?: string;
  payload?: string;
};

/** The private methods exercised directly. */
type JobStreamInternals = {
  normalizeEntry(fields: Record<string, string>): NormalizedEntry;
  handleJob(entry: StreamEntry): Promise<void>;
};

describe('JobStreamService', () => {
  const ORIGINAL_ID = '1700000000000-0';
  const ORIGINAL_MS = 1700000000000;

  let redis: { xadd: jest.Mock; xdel: jest.Mock };
  let dataSource: { query: jest.Mock };
  let notifications: {
    insertPendingForChats: jest.Mock;
    markDeliveryStatusUnknown: jest.Mock;
  };
  let service: JobStreamService;

  /** Rows the mocked Postgres hands back, keyed by which table is queried. */
  let recordedRows: { chat_id: string; status: string; resumable: boolean }[];
  let subscriberRows: { chat_id: string }[];
  let categoryRows: { name: string }[];

  beforeAll(() => {
    Logger.overrideLogger(false);
  });

  beforeEach(() => {
    recordedRows = [];
    subscriberRows = [];
    categoryRows = [{ name: 'Mobile App Development' }];

    redis = {
      xadd: jest.fn().mockResolvedValue('1700000000001-0'),
      xdel: jest.fn().mockResolvedValue(1),
    };
    dataSource = {
      query: jest.fn((sql: string) => {
        if (sql.includes('FROM notifications'))
          return Promise.resolve(recordedRows);
        if (sql.includes('FROM categories'))
          return Promise.resolve(categoryRows);
        if (sql.includes('FROM users')) return Promise.resolve(subscriberRows);
        return Promise.reject(new Error(`unexpected query: ${sql}`));
      }),
    };
    notifications = {
      insertPendingForChats: jest.fn().mockResolvedValue(undefined),
      markDeliveryStatusUnknown: jest.fn().mockResolvedValue(undefined),
    };
    const config = {
      get: jest.fn((key: string) => (key === 'NODE_ENV' ? 'test' : undefined)),
    };

    service = new JobStreamService(
      redis as unknown as Redis,
      dataSource as unknown as DataSource,
      notifications as unknown as NotificationsService,
      config as unknown as ConfigService,
    );
  });

  const normalize = (fields: Record<string, string>): NormalizedEntry =>
    (service as unknown as JobStreamInternals).normalizeEntry(fields);

  const handle = (entry: StreamEntry): Promise<void> =>
    (service as unknown as JobStreamInternals).handleJob(entry);

  const contractEntry = (
    overrides: Record<string, string> = {},
  ): StreamEntry => ({
    id: '1700000005000-0',
    fields: {
      jobId: 'job-1',
      category: 'mobile_app',
      source: 'upwork',
      payload: JSON.stringify({
        Title: 'Build an app',
        Description: 'Flutter work',
        'Short URL': 'https://s.example.com/abc',
        Categories: ['Mobile', 'Flutter'],
      }),
      _originalId: ORIGINAL_ID,
      ...overrides,
    },
  });

  /** The flat field/value list passed to the notify:telegram XADD. */
  const notifyFields = (): Record<string, string> => {
    const call = redis.xadd.mock.calls.find(
      ([stream]) => stream === REDIS_STREAMS.notifyTelegram,
    ) as string[] | undefined;
    if (!call) throw new Error('no notify:telegram XADD');
    const [, id, ...flat] = call;
    expect(id).toBe('*');
    const fields: Record<string, string> = {};
    for (let i = 0; i < flat.length; i += 2) fields[flat[i]] = flat[i + 1];
    return fields;
  };

  describe('onModuleInit', () => {
    it('dead-letters jobs:delivering entries and has the consumer delete them from the stream', async () => {
      const start = jest
        .spyOn(StreamConsumer.prototype, 'start')
        .mockResolvedValue(undefined);
      try {
        await service.onModuleInit();
      } finally {
        start.mockRestore();
      }

      const consumer = (
        service as unknown as {
          consumer: { options: Record<string, unknown> };
        }
      ).consumer;
      expect(consumer.options).toEqual(
        expect.objectContaining({
          deadLetterStream: REDIS_STREAMS.jobsDeliveringDead,
          deleteOnDeadLetter: true,
        }),
      );
      expect(consumer.options).not.toHaveProperty('onDeadLetter');
    });
  });

  describe('normalizeEntry', () => {
    it('passes an entry in the documented contract shape through untouched', () => {
      const fields = {
        jobId: 'job-1',
        category: 'mobile_app',
        source: 'upwork',
        payload: '{"Title":"x"}',
      };
      expect(normalize(fields)).toBe(fields);
    });

    it('maps the raw flat Python shape onto the contract shape', () => {
      const result = normalize({
        'Job UUID': 'job-2',
        'Category ID': 'web_dev',
        Source: 'UpWork',
        Title: 'Landing page',
        Description: 'Next.js site',
        URL: 'https://www.example.com/jobs/2',
        'Short URL': 'https://s.example.com/2',
        'Decision Reason': 'matched',
        Categories: ' Web , Frontend ,, React ',
        'Category Selection Method': 'ai',
      });

      expect(result.jobId).toBe('job-2');
      expect(result.category).toBe('web_dev');
      expect(result.source).toBe('upwork');
      expect(JSON.parse(result.payload!)).toEqual({
        'Job UUID': 'job-2',
        Title: 'Landing page',
        Description: 'Next.js site',
        Source: 'UpWork',
        URL: 'https://www.example.com/jobs/2',
        'Short URL': 'https://s.example.com/2',
        'Decision Reason': 'matched',
        Categories: ['Web', 'Frontend', 'React'],
        'Category ID': 'web_dev',
        'Category Selection Method': 'ai',
      });
    });

    it('falls back to URL when the raw shape has no Short URL', () => {
      const result = normalize({
        'Job UUID': 'job-3',
        'Category ID': 'web_dev',
        Source: 'Freelancer',
        URL: 'https://www.example.com/jobs/3',
        'Short URL': '',
      });

      const payload = JSON.parse(result.payload!) as Record<string, unknown>;
      expect(payload['Short URL']).toBe('https://www.example.com/jobs/3');
      expect(payload.Categories).toEqual([]);
      expect(payload.Title).toBe('');
    });

    it.each([
      ['Job UUID', { 'Category ID': 'web_dev', Source: 'upwork' }],
      ['Category ID', { 'Job UUID': 'job-4', Source: 'upwork' }],
      ['Source', { 'Job UUID': 'job-4', 'Category ID': 'web_dev' }],
    ])('drops a raw entry missing %s', (_missing, fields) => {
      expect(normalize(fields)).toEqual({});
    });

    it('does not treat a partial contract shape as complete', () => {
      expect(normalize({ jobId: 'job-5', category: 'web_dev' })).toEqual({});
    });
  });

  describe('handleJob', () => {
    it('drops an entry with missing routing fields from jobs:delivering without handing it off', async () => {
      const entry: StreamEntry = {
        id: '1700000005000-0',
        fields: { jobId: 'job-1', category: 'mobile_app' },
      };

      await expect(handle(entry)).resolves.toBeUndefined();

      expect(redis.xdel).toHaveBeenCalledWith(
        REDIS_STREAMS.jobsDelivering,
        entry.id,
      );
      expect(dataSource.query).not.toHaveBeenCalled();
      expect(notifications.insertPendingForChats).not.toHaveBeenCalled();
      expect(redis.xadd).not.toHaveBeenCalled();
    });

    it.each([
      ['malformed JSON', '{not json'],
      ['JSON null', 'null'],
      ['a JSON number', '42'],
      ['a JSON array', '["a"]'],
    ])(
      'drops (XDELs) an entry whose payload is %s instead of throwing',
      async (_label, payload) => {
        const entry = contractEntry({ payload });

        await expect(handle(entry)).resolves.toBeUndefined();

        expect(redis.xdel).toHaveBeenCalledWith(
          REDIS_STREAMS.jobsDelivering,
          entry.id,
        );
        expect(notifications.insertPendingForChats).not.toHaveBeenCalled();
        expect(redis.xadd).not.toHaveBeenCalled();
      },
    );

    it('XDELs a job with no subscribers without inserting notifications or handing off', async () => {
      const entry = contractEntry();

      await handle(entry);

      expect(redis.xdel).toHaveBeenCalledWith(
        REDIS_STREAMS.jobsDelivering,
        entry.id,
      );
      expect(notifications.insertPendingForChats).not.toHaveBeenCalled();
      expect(redis.xadd).not.toHaveBeenCalled();
    });

    it('records pending rows, hands a fresh job off to notify:telegram, then XDELs it', async () => {
      subscriberRows = [{ chat_id: '111' }, { chat_id: '222' }];
      const entry = contractEntry();

      await handle(entry);

      expect(notifications.insertPendingForChats).toHaveBeenCalledWith(
        'job-1',
        ['111', '222'],
        ORIGINAL_MS,
      );
      expect(notifyFields()).toEqual({
        jobId: 'job-1',
        title: 'Build an app',
        link: 'https://s.example.com/abc',
        description: 'Flutter work',
        categories: JSON.stringify(['Mobile', 'Flutter']),
        category: 'mobile_app',
        categoryName: 'Mobile App Development',
        source: 'upwork',
        chatIds: JSON.stringify(['111', '222']),
        enqueuedAt: String(ORIGINAL_MS),
      });
      expect(redis.xdel).toHaveBeenCalledWith(
        REDIS_STREAMS.jobsDelivering,
        entry.id,
      );

      const insertOrder =
        notifications.insertPendingForChats.mock.invocationCallOrder[0];
      const xaddOrder = redis.xadd.mock.invocationCallOrder[0];
      const xdelOrder = redis.xdel.mock.invocationCallOrder[0];
      expect(insertOrder).toBeLessThan(xaddOrder);
      expect(xaddOrder).toBeLessThan(xdelOrder);
    });

    it("uses the entry's own id as the arrival time when _originalId is absent", async () => {
      subscriberRows = [{ chat_id: '111' }];
      const entry = contractEntry();
      delete entry.fields._originalId;

      await handle(entry);

      expect(notifications.insertPendingForChats).toHaveBeenCalledWith(
        'job-1',
        ['111'],
        1700000005000,
      );
    });

    it('falls back to the category id when the category has no display name', async () => {
      subscriberRows = [{ chat_id: '111' }];
      categoryRows = [];

      await handle(contractEntry());

      expect(notifyFields().categoryName).toBe('mobile_app');
    });

    it('re-hands off a partially processed job only to its still-pending recipients, without re-matching', async () => {
      recordedRows = [
        { chat_id: '111', status: 'sent', resumable: true },
        { chat_id: '222', status: 'pending', resumable: true },
        { chat_id: '333', status: 'failed', resumable: true },
        { chat_id: '444', status: 'pending', resumable: true },
      ];
      subscriberRows = [{ chat_id: '999' }];
      const entry = contractEntry();

      await handle(entry);

      const queries = dataSource.query.mock.calls.map(([sql]) => String(sql));
      expect(queries.some((sql) => sql.includes('FROM users'))).toBe(false);
      expect(notifications.insertPendingForChats).not.toHaveBeenCalled();
      expect(notifyFields().chatIds).toBe(JSON.stringify(['222', '444']));
      expect(notifications.markDeliveryStatusUnknown).not.toHaveBeenCalled();
      expect(redis.xdel).toHaveBeenCalledWith(
        REDIS_STREAMS.jobsDelivering,
        entry.id,
      );
    });

    it('asks Postgres which rows are inside the resume window using a cutoff from the app clock', async () => {
      const NOW = 1_700_900_000_000;
      jest.useFakeTimers({ now: NOW });
      try {
        await handle(contractEntry());
      } finally {
        jest.useRealTimers();
      }

      const call = dataSource.query.mock.calls.find(([sql]) =>
        String(sql).includes('FROM notifications'),
      ) as [string, unknown[]];
      expect(call[0]).toContain('status_changed_at >= CAST($2 AS timestamp)');
      expect(call[0]).not.toMatch(/now\(\)/i);
      expect(call[1]).toEqual([
        'job-1',
        new Date(NOW - RESUME_WINDOW_SECONDS * 1000),
      ]);
    });

    it('marks pending rows older than the resume window as delivery-status-unknown and resumes only the rest', async () => {
      recordedRows = [
        { chat_id: '111', status: 'sent', resumable: false },
        { chat_id: '222', status: 'pending', resumable: false },
        { chat_id: '333', status: 'pending', resumable: true },
      ];

      await handle(contractEntry());

      expect(notifications.markDeliveryStatusUnknown).toHaveBeenCalledWith(
        'job-1',
        ['222'],
      );
      expect(notifyFields().chatIds).toBe(JSON.stringify(['333']));
    });

    it('does not hand off at all when every pending row is past the resume window', async () => {
      recordedRows = [
        { chat_id: '222', status: 'pending', resumable: false },
        { chat_id: '333', status: 'pending', resumable: false },
      ];
      const entry = contractEntry();

      await handle(entry);

      expect(notifications.markDeliveryStatusUnknown).toHaveBeenCalledWith(
        'job-1',
        ['222', '333'],
      );
      expect(redis.xadd).not.toHaveBeenCalled();
      expect(redis.xdel).toHaveBeenCalledWith(
        REDIS_STREAMS.jobsDelivering,
        entry.id,
      );
    });

    it('leaves the entry unacked when closing stale rows fails', async () => {
      recordedRows = [{ chat_id: '222', status: 'pending', resumable: false }];
      notifications.markDeliveryStatusUnknown.mockRejectedValue(
        new Error('db down'),
      );

      await expect(handle(contractEntry())).rejects.toThrow('db down');
      expect(redis.xadd).not.toHaveBeenCalled();
      expect(redis.xdel).not.toHaveBeenCalled();
    });

    it('skips (XDELs) a retried job none of whose recipients is still pending', async () => {
      recordedRows = [
        { chat_id: '111', status: 'sent', resumable: true },
        { chat_id: '222', status: 'cancelled', resumable: true },
      ];
      const entry = contractEntry();

      await handle(entry);

      expect(redis.xadd).not.toHaveBeenCalled();
      expect(notifications.insertPendingForChats).not.toHaveBeenCalled();
      expect(redis.xdel).toHaveBeenCalledWith(
        REDIS_STREAMS.jobsDelivering,
        entry.id,
      );
    });

    it('accepts an entry in the raw Python shape end to end', async () => {
      subscriberRows = [{ chat_id: '111' }];

      await handle({
        id: '1700000005000-0',
        fields: {
          'Job UUID': 'job-raw',
          'Category ID': 'mobile_app',
          Source: 'Upwork',
          Title: 'Raw title',
          URL: 'https://www.example.com/jobs/raw',
          Categories: 'Mobile, iOS',
          _originalId: ORIGINAL_ID,
        },
      });

      expect(notifications.insertPendingForChats).toHaveBeenCalledWith(
        'job-raw',
        ['111'],
        ORIGINAL_MS,
      );
      const fields = notifyFields();
      expect(fields.source).toBe('upwork');
      expect(fields.link).toBe('https://www.example.com/jobs/raw');
      expect(fields.categories).toBe(JSON.stringify(['Mobile', 'iOS']));
    });

    it('propagates a database failure so the entry stays unacked for retry', async () => {
      dataSource.query.mockRejectedValueOnce(new Error('connection refused'));

      await expect(handle(contractEntry())).rejects.toThrow(
        'connection refused',
      );
      expect(redis.xdel).not.toHaveBeenCalled();
    });
  });
});
