import type { Repository, UpdateQueryBuilder } from 'typeorm';
import {
  MAX_RESPONSE_TIME_MS,
  NotificationEntity,
  NotificationReason,
  toResponseTimeMs,
} from './entities/notification.entity';
import {
  closePendingNotifications,
  NotificationStatusChange,
  NotificationsService,
} from './notifications.service';

// @nestjs/typeorm ships as an ES module, which Jest can't require() on
// Node < 24.9; only its decorator is referenced when the service loads.
jest.mock('@nestjs/typeorm', () => ({
  InjectRepository: () => () => undefined,
}));

/** The four columns that must be filled on every write, never NULL. */
const STATUS_COLUMNS = [
  'status',
  'statusChangedAt',
  'reason',
  'responseTimeMs',
] as const;

function expectAllStatusColumnsFilled(values: Record<string, unknown>) {
  for (const column of STATUS_COLUMNS) {
    expect(values).toHaveProperty(column);
    expect(values[column]).not.toBeNull();
    expect(values[column]).not.toBeUndefined();
  }
}

/** A chainable stand-in for TypeORM's query builders. */
function queryBuilderMock() {
  const qb: Record<string, jest.Mock> = {};
  for (const method of [
    'insert',
    'into',
    'values',
    'orIgnore',
    'update',
    'set',
    'setParameter',
    'where',
    'andWhere',
  ]) {
    qb[method] = jest.fn(() => qb);
  }
  qb.execute = jest.fn().mockResolvedValue({ affected: 1 });
  return qb;
}

describe('NotificationsService', () => {
  const NOW = 1_700_000_100_000;

  let qb: ReturnType<typeof queryBuilderMock>;
  let repo: { createQueryBuilder: jest.Mock; update: jest.Mock };
  let service: NotificationsService;

  beforeEach(() => {
    jest.useFakeTimers({ now: NOW });
    qb = queryBuilderMock();
    repo = {
      createQueryBuilder: jest.fn(() => qb),
      update: jest.fn().mockResolvedValue({ affected: 1 }),
    };
    service = new NotificationsService(
      repo as unknown as Repository<NotificationEntity>,
    );
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  describe('insertPendingForChats', () => {
    it('inserts one fully filled pending row per recipient, ignoring duplicates', async () => {
      await service.insertPendingForChats('job-1', ['111', '222'], NOW - 750);

      expect(qb.into).toHaveBeenCalledWith(NotificationEntity);
      expect(qb.orIgnore).toHaveBeenCalled();
      expect(qb.execute).toHaveBeenCalled();

      const rows = (qb.values.mock.calls as unknown[][])[0][0] as Record<
        string,
        unknown
      >[];
      expect(rows).toHaveLength(2);
      for (const row of rows) expectAllStatusColumnsFilled(row);
      expect(rows).toEqual([
        {
          jobId: 'job-1',
          chatId: '111',
          status: 'pending',
          statusChangedAt: new Date(NOW),
          reason: NotificationReason.AWAITING_DELIVERY,
          responseTimeMs: 750,
        },
        expect.objectContaining({ jobId: 'job-1', chatId: '222' }),
      ]);
    });

    it('fills response_time_ms with 0 rather than NULL or a negative for a clock-skewed arrival', async () => {
      await service.insertPendingForChats('job-1', ['111'], NOW + 5_000);

      const [row] = (qb.values.mock.calls as unknown[][])[0][0] as Record<
        string,
        unknown
      >[];
      expect(row.responseTimeMs).toBe(0);
    });

    it('fills response_time_ms with 0 for a non-numeric arrival time', async () => {
      await service.insertPendingForChats('job-1', ['111'], Number.NaN);

      const [row] = (qb.values.mock.calls as unknown[][])[0][0] as Record<
        string,
        unknown
      >[];
      expect(row.responseTimeMs).toBe(0);
    });

    it('writes nothing for an empty recipient list', async () => {
      await service.insertPendingForChats('job-1', [], NOW);

      expect(repo.createQueryBuilder).not.toHaveBeenCalled();
    });
  });

  describe('setStatus', () => {
    const changes: [string, NotificationStatusChange, NotificationReason][] = [
      [
        'sent',
        { status: 'sent', responseTimeMs: 1200 },
        NotificationReason.DELIVERED,
      ],
      [
        'failed (bot blocked)',
        {
          status: 'failed',
          reason: NotificationReason.BOT_BLOCKED,
          responseTimeMs: 1200,
        },
        NotificationReason.BOT_BLOCKED,
      ],
      [
        'failed (retries exhausted)',
        {
          status: 'failed',
          reason: NotificationReason.DELIVERY_RETRIES_EXHAUSTED,
          responseTimeMs: 1200,
        },
        NotificationReason.DELIVERY_RETRIES_EXHAUSTED,
      ],
      [
        'cancelled (unsubscribed)',
        {
          status: 'cancelled',
          reason: NotificationReason.USER_UNSUBSCRIBED,
          responseTimeMs: 1200,
        },
        NotificationReason.USER_UNSUBSCRIBED,
      ],
      [
        'cancelled (bot blocked)',
        {
          status: 'cancelled',
          reason: NotificationReason.BOT_BLOCKED,
          responseTimeMs: 1200,
        },
        NotificationReason.BOT_BLOCKED,
      ],
    ];

    it.each(changes)(
      'fills every status column for %s',
      async (_label, change, expectedReason) => {
        await service.setStatus('job-1', '111', change);

        expect(repo.update).toHaveBeenCalledTimes(1);
        const [criteria, columns] = repo.update.mock.calls[0] as [
          unknown,
          Record<string, unknown>,
        ];
        expect(criteria).toEqual({ jobId: 'job-1', chatId: '111' });
        expectAllStatusColumnsFilled(columns);
        expect(columns).toEqual({
          status: change.status,
          statusChangedAt: new Date(NOW),
          reason: expectedReason,
          responseTimeMs: 1200,
        });
      },
    );

    it('clamps response_time_ms into the INTEGER column range', async () => {
      await service.setStatus('job-1', '111', {
        status: 'sent',
        responseTimeMs: Number.MAX_SAFE_INTEGER,
      });

      const [, columns] = repo.update.mock.calls[0] as [
        unknown,
        Record<string, unknown>,
      ];
      expect(columns.responseTimeMs).toBe(MAX_RESPONSE_TIME_MS);
    });
  });

  describe('markDeliveryRetriesExhausted', () => {
    it('closes only still-pending rows as failed with every status column filled', async () => {
      await service.markDeliveryRetriesExhausted('job-1', ['111', '222']);

      expect(qb.update).toHaveBeenCalled();
      const values = (qb.set.mock.calls as unknown[][])[0][0] as Record<
        string,
        unknown
      >;
      expectAllStatusColumnsFilled(values);
      expect(values).toEqual({
        status: 'failed',
        reason: NotificationReason.DELIVERY_RETRIES_EXHAUSTED,
        statusChangedAt: new Date(NOW),
        responseTimeMs: expect.any(Function) as unknown,
      });
      expect(qb.setParameter).toHaveBeenCalledWith('closedAt', new Date(NOW));
      expect(qb.where).toHaveBeenCalledWith('job_id = :jobId', {
        jobId: 'job-1',
      });
      expect(qb.andWhere).toHaveBeenCalledWith('chat_id IN (:...chatIds)', {
        chatIds: ['111', '222'],
      });
      expect(qb.andWhere).toHaveBeenCalledWith('status = :pending', {
        pending: 'pending',
      });
      expect(qb.execute).toHaveBeenCalled();
    });

    it('writes nothing for an empty recipient list', async () => {
      await service.markDeliveryRetriesExhausted('job-1', []);

      expect(repo.createQueryBuilder).not.toHaveBeenCalled();
    });
  });

  describe('closePendingNotifications', () => {
    it('computes response_time_ms in SQL as arrival-to-now, clamped to the INTEGER range', () => {
      const now = new Date(NOW);
      closePendingNotifications(
        qb as unknown as UpdateQueryBuilder<NotificationEntity>,
        'cancelled',
        NotificationReason.USER_UNSUBSCRIBED,
        now,
      );

      const values = (qb.set.mock.calls as unknown[][])[0][0] as Record<
        string,
        unknown
      >;
      expectAllStatusColumnsFilled(values);
      const sql = (values.responseTimeMs as () => string)();
      expect(sql).toContain('"response_time_ms"');
      expect(sql).toContain('"status_changed_at"');
      expect(sql).toContain(':closedAt');
      expect(sql).toContain(`LEAST(${MAX_RESPONSE_TIME_MS}`);
      expect(sql).toContain('GREATEST(0');
      expect(qb.setParameter).toHaveBeenCalledWith('closedAt', now);
    });
  });
});

describe('toResponseTimeMs', () => {
  it.each([
    [1234.4, 1234],
    [1234.5, 1235],
    [-10, 0],
    [Number.NaN, 0],
    [Number.POSITIVE_INFINITY, 0],
    [MAX_RESPONSE_TIME_MS + 1, MAX_RESPONSE_TIME_MS],
  ])('normalizes %p to %p', (input, expected) => {
    expect(toResponseTimeMs(input)).toBe(expected);
  });
});
