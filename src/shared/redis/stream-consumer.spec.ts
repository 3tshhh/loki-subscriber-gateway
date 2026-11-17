import { Logger } from '@nestjs/common';
import type Redis from 'ioredis';
import {
  countUndelivered,
  StreamConsumer,
  StreamEntry,
} from './stream-consumer';

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
    xrevrange: jest.Mock;
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
      xrevrange: jest.fn(),
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

    /** Serves XRANGE/XREVRANGE from an in-memory stream, honouring
     * exclusive `(` bounds and COUNT — so paging is exercised for real. */
    const serveStream = (entries: [string, string[]][]) => {
      const cmp = (a: string, b: string) => {
        const [am, as] = a.split('-').map(Number);
        const [bm, bs] = b.split('-').map(Number);
        return am - bm || as - bs;
      };
      const within = (id: string, lo: string, hi: string) => {
        const loOk =
          lo === '-' ||
          (lo.startsWith('(') ? cmp(id, lo.slice(1)) > 0 : cmp(id, lo) >= 0);
        const hiOk =
          hi === '+' ||
          (hi.startsWith('(') ? cmp(id, hi.slice(1)) < 0 : cmp(id, hi) <= 0);
        return loOk && hiOk;
      };
      redis.xrange.mockImplementation(
        (_s: string, lo: string, hi: string, _c: string, count: number) =>
          Promise.resolve(
            entries.filter(([id]) => within(id, lo, hi)).slice(0, count),
          ),
      );
      redis.xrevrange.mockImplementation(
        (_s: string, hi: string, lo: string, _c: string, count: number) =>
          Promise.resolve(
            entries
              .filter(([id]) => within(id, lo, hi))
              .reverse()
              .slice(0, count),
          ),
      );
    };
    const deletedIds = () =>
      (redis.xdel.mock.calls as string[][]).flatMap((args) => args.slice(1));

    it('does nothing when the backlog is within the cap', async () => {
      redis.xinfo.mockResolvedValue(groupInfo('10-0'));
      serveStream(backlog(3));

      await cap(consumer({ maxBacklogOnStartup: 3 }));

      expect(redis.xrevrange).toHaveBeenCalledWith(
        STREAM,
        '+',
        '(10-0',
        'COUNT',
        4,
      );
      expect(redis.xdel).not.toHaveBeenCalled();
    });

    it('deletes the oldest excess entries, keeping only the newest N', async () => {
      redis.xinfo.mockResolvedValue(groupInfo('10-0'));
      serveStream(backlog(5));

      await cap(consumer({ maxBacklogOnStartup: 2 }));

      expect(deletedIds()).toEqual(['11-0', '12-0', '13-0']);
      // Deleting is enough — the group's cursor is never moved.
      expect(redis.xgroup).not.toHaveBeenCalled();
    });

    it('drops everything when the cap is 0', async () => {
      redis.xinfo.mockResolvedValue(groupInfo('10-0'));
      serveStream(backlog(3));

      await cap(consumer({ maxBacklogOnStartup: 0 }));

      expect(deletedIds()).toEqual(['11-0', '12-0', '13-0']);
    });

    it('evaluates a function cap once, at check time', async () => {
      redis.xinfo.mockResolvedValue(groupInfo('10-0'));
      serveStream(backlog(5));
      const budget = jest.fn().mockResolvedValue(4);

      await cap(consumer({ maxBacklogOnStartup: budget }));

      expect(budget).toHaveBeenCalledTimes(1);
      expect(deletedIds()).toEqual(['11-0']);
    });

    it('drops a huge backlog in bounded pages, never all at once', async () => {
      redis.xinfo.mockResolvedValue(groupInfo('10-0'));
      serveStream(backlog(1_203));

      await cap(consumer({ maxBacklogOnStartup: 3 }));

      expect(deletedIds()).toHaveLength(1_200);
      for (const call of redis.xdel.mock.calls as string[][]) {
        expect(call.length - 1).toBeLessThanOrEqual(500);
      }
      for (const call of redis.xrange.mock.calls as unknown[][]) {
        expect(call[3]).toBe('COUNT');
      }
    });

    it('drops undelivered entries older than maxBacklogAgeMs, however few', async () => {
      const now = 1_000_000;
      jest.spyOn(Date, 'now').mockReturnValue(now);
      try {
        redis.xinfo.mockResolvedValue(groupInfo('10-0'));
        serveStream([
          ['500-0', ['jobId', 'old-1']],
          ['939999-5', ['jobId', 'old-2']],
          ['940000-0', ['jobId', 'fresh-1']],
          ['999000-0', ['jobId', 'fresh-2']],
        ]);

        await cap(consumer({ maxBacklogAgeMs: 60_000 }));

        expect(deletedIds()).toEqual(['500-0', '939999-5']);
      } finally {
        jest.restoreAllMocks();
      }
    });

    it('applies the count cap to what the age check left', async () => {
      const now = 1_000_000;
      jest.spyOn(Date, 'now').mockReturnValue(now);
      try {
        redis.xinfo.mockResolvedValue(groupInfo('10-0'));
        const entries: [string, string[]][] = [
          ['500-0', ['jobId', 'old']],
          ['950000-0', ['jobId', 'a']],
          ['960000-0', ['jobId', 'b']],
          ['970000-0', ['jobId', 'c']],
        ];
        serveStream(entries);
        // Make deletions visible to later reads, like a real stream.
        redis.xdel.mockImplementation((_s: string, ...ids: string[]) => {
          for (const id of ids) {
            entries.splice(
              entries.findIndex(([e]) => e === id),
              1,
            );
          }
          return Promise.resolve(ids.length);
        });

        await cap(
          consumer({ maxBacklogAgeMs: 60_000, maxBacklogOnStartup: 2 }),
        );

        expect(deletedIds()).toEqual(['500-0', '950000-0']);
      } finally {
        jest.restoreAllMocks();
      }
    });

    it('never touches entries at or before the last-delivered-id (the PEL)', async () => {
      redis.xinfo.mockResolvedValue(groupInfo('12-0'));
      serveStream(backlog(5)); // 11-0 … 15-0

      await cap(consumer({ maxBacklogOnStartup: 0 }));

      expect(deletedIds()).toEqual(['13-0', '14-0', '15-0']);
    });

    it('runs onBacklogDropped on each dropped entry before deleting it, swallowing its errors', async () => {
      redis.xinfo.mockResolvedValue(groupInfo('10-0'));
      serveStream(backlog(3));
      const onBacklogDropped = jest
        .fn()
        .mockRejectedValueOnce(new Error('db down'))
        .mockResolvedValue(undefined);

      await cap(consumer({ maxBacklogOnStartup: 1, onBacklogDropped }));

      expect(onBacklogDropped).toHaveBeenCalledTimes(2);
      expect(onBacklogDropped).toHaveBeenCalledWith({ jobId: 'x' });
      expect(deletedIds()).toEqual(['11-0', '12-0']);
      expect(onBacklogDropped.mock.invocationCallOrder[1]).toBeLessThan(
        redis.xdel.mock.invocationCallOrder[0],
      );
    });

    it("does nothing when this consumer's group is not on the stream", async () => {
      redis.xinfo.mockResolvedValue([
        ['name', 'some-other-group', 'last-delivered-id', '0-0'],
      ]);

      await cap(consumer({ maxBacklogOnStartup: 2, maxBacklogAgeMs: 1 }));

      expect(redis.xrange).not.toHaveBeenCalled();
      expect(redis.xrevrange).not.toHaveBeenCalled();
      expect(redis.xdel).not.toHaveBeenCalled();
    });
  });

  describe('read connection', () => {
    it('runs the blocking XREADGROUP on its own connection, everything else on the shared one, and closes it on stop', async () => {
      let reads = 0;
      const reader = {
        // First read hands back one entry, later reads come back empty —
        // each on a macrotask, like a real BLOCK timing out.
        xreadgroup: jest.fn(
          () =>
            new Promise((resolve) =>
              setImmediate(() =>
                resolve(
                  reads++ === 0
                    ? [[STREAM, [['5-0', ['jobId', 'job-5']]]]]
                    : null,
                ),
              ),
            ),
        ),
        quit: jest.fn().mockResolvedValue('OK'),
        disconnect: jest.fn(),
      };
      const shared = {
        ...redis,
        xinfo: jest.fn().mockResolvedValue([]),
        xreadgroup: jest.fn(),
        duplicate: jest.fn(() => reader),
      };
      const c = new StreamConsumer(
        shared as unknown as Redis,
        STREAM,
        GROUP,
        'worker-1',
        handle,
        { claimIntervalMs: 60_000 },
      );

      await c.start();
      while (handle.mock.calls.length === 0) {
        await new Promise((r) => setImmediate(r));
      }
      await c.stop();

      expect(shared.duplicate).toHaveBeenCalledTimes(1);
      expect(reader.xreadgroup).toHaveBeenCalled();
      expect(shared.xreadgroup).not.toHaveBeenCalled();
      expect(handle).toHaveBeenCalledWith({
        id: '5-0',
        fields: { jobId: 'job-5' },
      });
      // The ack goes out on the shared client, not behind a BLOCK.
      expect(shared.xack).toHaveBeenCalledWith(STREAM, GROUP, '5-0');
      expect(reader.quit).toHaveBeenCalledTimes(1);
    });
  });

  describe('countUndelivered', () => {
    const groupInfo = (lastDeliveredId: string) => [
      ['name', GROUP, 'last-delivered-id', lastDeliveredId],
    ];

    it('counts entries after the last-delivered-id, up to max', async () => {
      redis.xinfo.mockResolvedValue(groupInfo('10-0'));
      redis.xrevrange.mockResolvedValue([['11-0', []]]);

      await expect(
        countUndelivered(redis as unknown as Redis, STREAM, GROUP, 7),
      ).resolves.toBe(1);
      expect(redis.xrevrange).toHaveBeenCalledWith(
        STREAM,
        '+',
        '(10-0',
        'COUNT',
        7,
      );
    });

    it('starts the count at newerThanMs when that is past the cursor', async () => {
      redis.xinfo.mockResolvedValue(groupInfo('10-0'));
      redis.xrevrange.mockResolvedValue([]);

      await countUndelivered(redis as unknown as Redis, STREAM, GROUP, 7, 500);

      expect(redis.xrevrange).toHaveBeenCalledWith(
        STREAM,
        '+',
        '500-0',
        'COUNT',
        7,
      );
    });

    it('counts 0 for a missing stream or group', async () => {
      redis.xinfo.mockRejectedValueOnce(new Error('ERR no such key'));
      await expect(
        countUndelivered(redis as unknown as Redis, STREAM, GROUP, 7),
      ).resolves.toBe(0);

      redis.xinfo.mockResolvedValueOnce([]);
      await expect(
        countUndelivered(redis as unknown as Redis, STREAM, GROUP, 7),
      ).resolves.toBe(0);
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

    it('dead-letters entries that reached the delivery cap and reclaims the rest', async () => {
      redis.xpending.mockResolvedValueOnce(
        pending(['1-0', 5], ['2-0', 4], ['3-0', 1]),
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

      // Delivered 5 times already, cap 5: callback, copy to the dead
      // stream, ack — never handed a 6th delivery.
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
        'reached 5 delivery attempts',
        '_deadLetteredAt',
        expect.any(String),
      );
      expect(redis.xack).toHaveBeenCalledWith(STREAM, GROUP, '1-0');
      // Without deleteOnDeadLetter the original stays in the stream.
      expect(redis.xdel).not.toHaveBeenCalled();

      // Under the cap: claimed and run through the normal handler.
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

    it('dead-letters an entry delivered more than the cap too', async () => {
      redis.xpending.mockResolvedValueOnce(pending(['1-0', 6]));

      await sweep(consumer(deadLetterOptions()));

      expect(redis.xadd).toHaveBeenCalledTimes(1);
      expect(handle).not.toHaveBeenCalled();
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

    describe('with deleteOnDeadLetter', () => {
      const deletingOptions = () => ({
        ...deadLetterOptions(),
        deleteOnDeadLetter: true,
      });

      it('XDELs the entry by its own id only after the dead-letter XADD and the XACK', async () => {
        redis.xpending.mockResolvedValueOnce(pending(['1-0', 5], ['3-0', 1]));

        await sweep(consumer(deletingOptions()));

        expect(redis.xdel).toHaveBeenCalledTimes(1);
        expect(redis.xdel).toHaveBeenCalledWith(STREAM, '1-0');
        const xaddOrder = redis.xadd.mock.invocationCallOrder[0];
        const xackOrder = redis.xack.mock.invocationCallOrder[0];
        const xdelOrder = redis.xdel.mock.invocationCallOrder[0];
        expect(xaddOrder).toBeLessThan(xackOrder);
        expect(xackOrder).toBeLessThan(xdelOrder);
        // Never scans the stream to find it.
        expect(redis.xrange).not.toHaveBeenCalled();
      });

      it('neither acks nor deletes the entry when copying it to the dead-letter stream fails', async () => {
        redis.xpending.mockResolvedValueOnce(pending(['1-0', 5]));
        redis.xadd.mockRejectedValue(new Error('OOM'));

        await expect(sweep(consumer(deletingOptions()))).rejects.toThrow('OOM');

        expect(redis.xack).not.toHaveBeenCalled();
        expect(redis.xdel).not.toHaveBeenCalled();
      });

      it('does not delete the entry when the XACK fails', async () => {
        redis.xpending.mockResolvedValueOnce(pending(['1-0', 5]));
        redis.xack.mockRejectedValue(new Error('connection lost'));

        await expect(sweep(consumer(deletingOptions()))).rejects.toThrow(
          'connection lost',
        );

        expect(redis.xadd).toHaveBeenCalledTimes(1);
        expect(redis.xdel).not.toHaveBeenCalled();
      });
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
