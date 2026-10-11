import { Logger } from '@nestjs/common';
import type Redis from 'ioredis';
import { StreamConsumer, StreamEntry } from './stream-consumer';

const STREAM = 'jobs:delivering';
const GROUP = 'delivering-group';
const DEAD = 'jobs:delivering:dead';

/** The private methods exercised directly. */
type StreamConsumerInternals = {
  capBacklogOnStartup(): Promise<void>;
  sweepPending(): Promise<void>;
};

describe('StreamConsumer', () => {
  let redis: {
    xinfo: jest.Mock;
    xrange: jest.Mock;
    xgroup: jest.Mock;
    xdel: jest.Mock;
    xpending: jest.Mock;
    xclaim: jest.Mock;
    xadd: jest.Mock;
    xack: jest.Mock;
  };
  let handle: jest.Mock<Promise<void>, [StreamEntry]>;
  let onDeadLetter: jest.Mock<Promise<void>, [Record<string, string>]>;

  beforeAll(() => {
    Logger.overrideLogger(false);
  });

  beforeEach(() => {
    redis = {
      xinfo: jest.fn(),
      xrange: jest.fn(),
      xgroup: jest.fn().mockResolvedValue('OK'),
      xdel: jest.fn().mockResolvedValue(1),
      xpending: jest.fn().mockResolvedValue([]),
      // Hands each claimed id back with fields derived from it.
      xclaim: jest.fn((...args: unknown[]) =>
        Promise.resolve(
          (args.slice(4) as string[]).map((id) => [id, ['jobId', `job-${id}`]]),
        ),
      ),
      xadd: jest.fn().mockResolvedValue('99-0'),
      xack: jest.fn().mockResolvedValue(1),
    };
    handle = jest
      .fn<Promise<void>, [StreamEntry]>()
      .mockResolvedValue(undefined);
    onDeadLetter = jest
      .fn<Promise<void>, [Record<string, string>]>()
      .mockResolvedValue(undefined);
  });

  const consumer = (
    options: ConstructorParameters<typeof StreamConsumer>[5] = {},
  ) =>
    new StreamConsumer(
      redis as unknown as Redis,
      STREAM,
      GROUP,
      'worker-1',
      handle,
      options,
    );

  describe('capBacklogOnStartup', () => {
    const groupInfo = (lastDeliveredId: string) => [
      ['name', 'some-other-group', 'last-delivered-id', '0-0'],
      ['name', GROUP, 'consumers', 1, 'last-delivered-id', lastDeliveredId],
    ];
    const backlog = (count: number): [string, string[]][] =>
      Array.from({ length: count }, (_, i) => [`${i + 11}-0`, ['jobId', 'x']]);

    const cap = (c: StreamConsumer): Promise<void> =>
      (c as unknown as StreamConsumerInternals).capBacklogOnStartup();

    it('does nothing when no cap is configured', async () => {
      await cap(consumer());

      expect(redis.xinfo).not.toHaveBeenCalled();
      expect(redis.xgroup).not.toHaveBeenCalled();
    });

    it('does nothing when the backlog is within the cap', async () => {
      redis.xinfo.mockResolvedValue(groupInfo('10-0'));
      redis.xrange.mockResolvedValue(backlog(3));

      await cap(consumer({ maxBacklogOnStartup: 3 }));

      expect(redis.xrange).toHaveBeenCalledWith(STREAM, '(10-0', '+');
      expect(redis.xgroup).not.toHaveBeenCalled();
      expect(redis.xdel).not.toHaveBeenCalled();
    });

    it('fast-forwards the group past the oldest excess entries and deletes them', async () => {
      redis.xinfo.mockResolvedValue(groupInfo('10-0'));
      redis.xrange.mockResolvedValue(backlog(5));

      await cap(consumer({ maxBacklogOnStartup: 2 }));

      expect(redis.xgroup).toHaveBeenCalledWith('SETID', STREAM, GROUP, '13-0');
      expect(redis.xdel).toHaveBeenCalledWith(STREAM, '11-0', '12-0', '13-0');
      expect(redis.xgroup.mock.invocationCallOrder[0]).toBeLessThan(
        redis.xdel.mock.invocationCallOrder[0],
      );
    });

    it("does nothing when this consumer's group is not on the stream", async () => {
      redis.xinfo.mockResolvedValue([
        ['name', 'some-other-group', 'last-delivered-id', '0-0'],
      ]);

      await cap(consumer({ maxBacklogOnStartup: 2 }));

      expect(redis.xrange).not.toHaveBeenCalled();
      expect(redis.xgroup).not.toHaveBeenCalled();
    });
  });

  describe('sweepPending', () => {
    const sweep = (c: StreamConsumer): Promise<void> =>
      (c as unknown as StreamConsumerInternals).sweepPending();

    /** XPENDING extended form: [id, consumer, idleMs, deliveryCount]. */
    const pending = (...entries: [string, number][]) =>
      entries.map(([id, count]) => [id, 'worker-0', 70_000, count]);

    const deadLetterOptions = () => ({
      maxDeliveryAttempts: 5,
      deadLetterStream: DEAD,
      onDeadLetter,
    });

    it('dead-letters entries over the delivery cap and reclaims the rest', async () => {
      redis.xpending.mockResolvedValueOnce(
        pending(['1-0', 6], ['2-0', 5], ['3-0', 1]),
      );

      await sweep(consumer(deadLetterOptions()));

      expect(redis.xpending).toHaveBeenCalledWith(
        STREAM,
        GROUP,
        'IDLE',
        60_000,
        '-',
        '+',
        50,
      );

      // Over the cap: callback, copy to the dead stream, ack — never handled.
      expect(onDeadLetter).toHaveBeenCalledTimes(1);
      expect(onDeadLetter).toHaveBeenCalledWith({ jobId: 'job-1-0' });
      expect(redis.xadd).toHaveBeenCalledTimes(1);
      expect(redis.xadd).toHaveBeenCalledWith(
        DEAD,
        '*',
        'jobId',
        'job-1-0',
        '_deadLetteredFromId',
        '1-0',
        '_reason',
        'exceeded 5 delivery attempts',
        '_deadLetteredAt',
        expect.any(String),
      );
      expect(redis.xack).toHaveBeenCalledWith(STREAM, GROUP, '1-0');

      // At or under the cap: claimed and run through the normal handler.
      expect(redis.xclaim).toHaveBeenCalledWith(
        STREAM,
        GROUP,
        'worker-1',
        0,
        '2-0',
        '3-0',
      );
      expect(handle.mock.calls.map(([entry]) => entry)).toEqual([
        { id: '2-0', fields: { jobId: 'job-2-0' } },
        { id: '3-0', fields: { jobId: 'job-3-0' } },
      ]);
      expect(redis.xack).toHaveBeenCalledWith(STREAM, GROUP, '2-0');
      expect(redis.xack).toHaveBeenCalledWith(STREAM, GROUP, '3-0');
      expect(handle.mock.calls.map(([entry]) => entry.id)).not.toContain('1-0');
    });

    it('still dead-letters an entry when the onDeadLetter callback fails', async () => {
      redis.xpending.mockResolvedValueOnce(pending(['1-0', 9]));
      onDeadLetter.mockRejectedValue(new Error('callback broke'));

      await sweep(consumer(deadLetterOptions()));

      expect(redis.xadd).toHaveBeenCalledWith(
        DEAD,
        '*',
        ...([
          'jobId',
          'job-1-0',
          '_deadLetteredFromId',
          '1-0',
          '_reason',
        ] as const),
        expect.any(String),
        '_deadLetteredAt',
        expect.any(String),
      );
      expect(redis.xack).toHaveBeenCalledWith(STREAM, GROUP, '1-0');
    });

    it('leaves a reclaimed entry unacked when its handler throws', async () => {
      redis.xpending.mockResolvedValueOnce(pending(['2-0', 1]));
      handle.mockRejectedValue(new Error('still failing'));

      await sweep(consumer(deadLetterOptions()));

      expect(handle).toHaveBeenCalledTimes(1);
      expect(redis.xack).not.toHaveBeenCalled();
    });

    it('reclaims over-cap entries instead of dropping them when no dead-letter stream is set', async () => {
      redis.xpending.mockResolvedValueOnce(pending(['1-0', 9]));

      await sweep(consumer({ maxDeliveryAttempts: 5 }));

      expect(redis.xadd).not.toHaveBeenCalled();
      expect(handle).toHaveBeenCalledWith({
        id: '1-0',
        fields: { jobId: 'job-1-0' },
      });
    });

    it('pages through the PEL starting after the last id of a full page', async () => {
      const fullPage = Array.from({ length: 50 }, (_, i) => [
        `${i + 1}-0`,
        'worker-0',
        70_000,
        1,
      ]);
      redis.xpending
        .mockResolvedValueOnce(fullPage)
        .mockResolvedValueOnce(pending(['51-0', 1]));

      await sweep(consumer(deadLetterOptions()));

      expect(redis.xpending).toHaveBeenCalledTimes(2);
      expect((redis.xpending.mock.calls as unknown[][])[1][4]).toBe('(50-0');
      expect(handle).toHaveBeenCalledTimes(51);
    });
  });
});
