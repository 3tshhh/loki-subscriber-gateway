import type { ConfigService } from '@nestjs/config';
import type Redis from 'ioredis';
import { CONSUMER_GROUPS, REDIS_STREAMS } from '../../shared/redis';
import { StreamConsumer } from '../../shared/redis/stream-consumer';
import { JobMoverService } from './job-mover.service';

// @nestjs/config ships as ES modules, which Jest can't require() on
// Node < 24.9. The service is constructed by hand here.
jest.mock('@nestjs/config', () => ({
  ConfigModule: {},
  ConfigService: class ConfigService {},
}));

type MoverOptions = {
  maxBacklogAgeMs?: number;
  maxBacklogOnStartup: () => Promise<number>;
};

describe('JobMoverService', () => {
  const NOW = 10_000_000;
  let redis: { xinfo: jest.Mock; xrevrange: jest.Mock };

  const startWith = async (env: Record<string, number>) => {
    const config = { get: jest.fn((key: string) => env[key]) };
    const service = new JobMoverService(
      redis as unknown as Redis,
      config as unknown as ConfigService,
    );
    const start = jest
      .spyOn(StreamConsumer.prototype, 'start')
      .mockResolvedValue(undefined);
    try {
      await service.onModuleInit();
    } finally {
      start.mockRestore();
    }
    return (service as unknown as { consumer: { options: MoverOptions } })
      .consumer.options;
  };

  beforeEach(() => {
    jest.spyOn(Date, 'now').mockReturnValue(NOW);
    redis = {
      xinfo: jest
        .fn()
        .mockResolvedValue([
          ['name', CONSUMER_GROUPS.delivering, 'last-delivered-id', '0-0'],
        ]),
      xrevrange: jest.fn(),
    };
  });

  afterEach(() => jest.restoreAllMocks());

  it('passes the age limit through, in ms', async () => {
    const options = await startWith({ JOB_STREAM_MAX_AGE_MINUTES: 2 });
    expect(options.maxBacklogAgeMs).toBe(120_000);
  });

  it('disables the age limit when it is 0', async () => {
    const options = await startWith({ JOB_STREAM_MAX_AGE_MINUTES: 0 });
    expect(options.maxBacklogAgeMs).toBeUndefined();
  });

  it("only keeps what's left of the cap after jobs:delivering's own fresh backlog", async () => {
    redis.xrevrange.mockResolvedValue([
      ['1-0', []],
      ['2-0', []],
      ['3-0', []],
    ]);
    const options = await startWith({
      JOB_STREAM_BACKLOG_CAP: 10,
      JOB_STREAM_MAX_AGE_MINUTES: 1,
    });

    await expect(options.maxBacklogOnStartup()).resolves.toBe(7);
    // Counted on jobs:delivering, from the age cutoff, at most `cap`.
    expect(redis.xrevrange).toHaveBeenCalledWith(
      REDIS_STREAMS.jobsDelivering,
      '+',
      `${NOW - 60_000}-0`,
      'COUNT',
      10,
    );
  });

  it('never goes below 0', async () => {
    redis.xrevrange.mockResolvedValue(
      Array.from({ length: 10 }, (_, i) => [`${i}-0`, []]),
    );
    const options = await startWith({ JOB_STREAM_BACKLOG_CAP: 10 });

    await expect(options.maxBacklogOnStartup()).resolves.toBe(0);
  });

  it('falls back to the full cap if jobs:delivering cannot be counted', async () => {
    redis.xinfo.mockRejectedValue(new Error('connection lost'));
    const options = await startWith({ JOB_STREAM_BACKLOG_CAP: 10 });

    await expect(options.maxBacklogOnStartup()).resolves.toBe(10);
  });
});
