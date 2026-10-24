import { Logger } from '@nestjs/common';
import type { ConfigService } from '@nestjs/config';
import { Bot, GrammyError } from 'grammy';
import type Redis from 'ioredis';
import { NotificationReason } from '../../core/notifications/entities/notification.entity';
import type { NotificationsService } from '../../core/notifications/notifications.service';
import type { SubscriberService } from '../../core/subscriber/subscriber.service';
import { REDIS_KEYS, StreamEntry } from '../../shared/redis';
import { TelegramDispatchService } from './telegram-dispatch.service';

// @nestjs/config, @nestjs/typeorm and p-queue ship as ES modules, which
// Jest can't require() on Node < 24.9. The service is constructed by hand
// here; the queue is replaced with one that runs each task immediately.
jest.mock('@nestjs/config', () => ({
  ConfigModule: {},
  ConfigService: class ConfigService {},
}));
jest.mock('@nestjs/typeorm', () => ({
  InjectRepository: () => () => undefined,
}));
jest.mock('p-queue', () => ({
  __esModule: true,
  default: class {
    add<T>(task: () => Promise<T>): Promise<T> {
      return task();
    }
  },
}));

function telegramError(
  errorCode: number,
  description: string,
  parameters?: { retry_after?: number },
): GrammyError {
  return new GrammyError(
    "Call to 'sendMessage' failed!",
    { ok: false, error_code: errorCode, description, parameters },
    'sendMessage',
    {},
  );
}

type DeliverableJob = {
  jobId: string;
  title: string;
  link: string;
  description: string;
  category: string;
  categoryName: string;
  source: string;
  sourceName: string;
  categories: string[];
  enqueuedAtMs: number;
};

/** The private methods exercised directly. */
type DispatchInternals = {
  deliverOne(job: DeliverableJob, chatId: string): Promise<void>;
  handleNotification(entry: StreamEntry): Promise<void>;
  handleDeadLetter(fields: Record<string, string>): Promise<void>;
};

describe('TelegramDispatchService', () => {
  const NOW = 1_700_000_100_000;
  const JOB_ID = 'job-1';
  const CHAT_ID = '111';

  let redis: {
    sismember: jest.Mock;
    srem: jest.Mock;
    pipeline: jest.Mock;
  };
  let pipeline: { sadd: jest.Mock; expire: jest.Mock; exec: jest.Mock };
  let sendMessage: jest.Mock;
  let subscriber: { deactivateUser: jest.Mock };
  let notifications: {
    setStatus: jest.Mock;
    closeIfPending: jest.Mock;
    markDeliveryRetriesExhausted: jest.Mock;
  };
  let service: TelegramDispatchService;

  beforeAll(() => {
    Logger.overrideLogger(false);
  });

  beforeEach(() => {
    jest.useFakeTimers({ now: NOW });

    pipeline = {
      sadd: jest.fn().mockReturnThis(),
      expire: jest.fn().mockReturnThis(),
      // [[saddErr, saddResult], [expireErr, expireResult]]
      exec: jest.fn().mockResolvedValue([
        [null, 1],
        [null, 1],
      ]),
    };
    redis = {
      sismember: jest.fn().mockResolvedValue(1),
      srem: jest.fn().mockResolvedValue(1),
      pipeline: jest.fn(() => pipeline),
    };
    sendMessage = jest.fn().mockResolvedValue({ message_id: 1 });
    subscriber = { deactivateUser: jest.fn().mockResolvedValue(true) };
    notifications = {
      setStatus: jest.fn().mockResolvedValue(undefined),
      closeIfPending: jest.fn().mockResolvedValue(true),
      markDeliveryRetriesExhausted: jest.fn().mockResolvedValue(undefined),
    };
    const config = {
      get: jest.fn((key: string) =>
        key === 'NODE_ENV'
          ? 'test'
          : key === 'SHORT_URL_DOMAIN'
            ? 's.example.com'
            : undefined,
      ),
    };

    service = new TelegramDispatchService(
      redis as unknown as Redis,
      { api: { sendMessage } } as unknown as Bot,
      subscriber as unknown as SubscriberService,
      notifications as unknown as NotificationsService,
      config as unknown as ConfigService,
    );
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  const job = (overrides: Partial<DeliverableJob> = {}): DeliverableJob => ({
    jobId: JOB_ID,
    title: 'Build an app',
    link: 'https://s.example.com/abc',
    description: 'Flutter work',
    category: 'mobile_app',
    categoryName: 'Mobile App Development',
    source: 'upwork',
    sourceName: 'Upwork',
    categories: ['Mobile'],
    enqueuedAtMs: NOW - 1500,
    ...overrides,
  });

  const deliverOne = (
    chatId = CHAT_ID,
    overrides: Partial<DeliverableJob> = {},
  ): Promise<void> =>
    (service as unknown as DispatchInternals).deliverOne(
      job(overrides),
      chatId,
    );

  describe('deliverOne', () => {
    it('sends the message and records it as sent with its end-to-end response time', async () => {
      await deliverOne();

      expect(pipeline.sadd).toHaveBeenCalledWith(
        REDIS_KEYS.deliveredForJob(JOB_ID),
        CHAT_ID,
      );
      expect(sendMessage).toHaveBeenCalledTimes(1);
      const [chatId, text, options] = sendMessage.mock.calls[0] as [
        string,
        string,
        { parse_mode: string; reply_markup?: unknown },
      ];
      expect(chatId).toBe(CHAT_ID);
      expect(text).toContain('Build an app');
      expect(text).toContain('<b>Platform</b>\nUpwork');
      expect(options.parse_mode).toBe('HTML');
      expect(JSON.stringify(options.reply_markup)).toContain(
        'https://s.example.com/abc?userId=111',
      );
      expect(notifications.setStatus).toHaveBeenCalledWith(JOB_ID, CHAT_ID, {
        status: 'sent',
        responseTimeMs: 1500,
      });
      expect(redis.srem).not.toHaveBeenCalled();
    });

    it('does not attach a button for a link that is not a usable URL', async () => {
      await deliverOne(CHAT_ID, { link: 'https://broken-link-fragm' });

      const [, text, options] = sendMessage.mock.calls[0] as [
        string,
        string,
        { reply_markup?: unknown },
      ];
      expect(options.reply_markup).toBeUndefined();
      expect(text).toContain('https://broken-link-fragm');
    });

    it('cancels the still-pending row of a recipient no longer in users:active, claiming them so a retry skips them', async () => {
      redis.sismember.mockResolvedValue(0);

      await expect(deliverOne()).resolves.toBeUndefined();

      expect(redis.sismember).toHaveBeenCalledWith(
        REDIS_KEYS.activeUsers,
        CHAT_ID,
      );
      expect(pipeline.sadd).toHaveBeenCalledWith(
        REDIS_KEYS.deliveredForJob(JOB_ID),
        CHAT_ID,
      );
      expect(notifications.closeIfPending).toHaveBeenCalledWith(
        JOB_ID,
        CHAT_ID,
        {
          status: 'cancelled',
          reason: NotificationReason.RECIPIENT_INACTIVE,
          responseTimeMs: 1500,
        },
      );
      expect(pipeline.sadd.mock.invocationCallOrder[0]).toBeLessThan(
        notifications.closeIfPending.mock.invocationCallOrder[0],
      );
      expect(sendMessage).not.toHaveBeenCalled();
      expect(notifications.setStatus).not.toHaveBeenCalled();
      expect(redis.srem).not.toHaveBeenCalled();
    });

    it('writes nothing for an inactive recipient an earlier attempt already handled', async () => {
      redis.sismember.mockResolvedValue(0);
      pipeline.exec.mockResolvedValue([
        [null, 0],
        [null, 1],
      ]);

      await expect(deliverOne()).resolves.toBeUndefined();

      expect(notifications.closeIfPending).not.toHaveBeenCalled();
      expect(sendMessage).not.toHaveBeenCalled();
    });

    it('releases the claim and fails transiently if cancelling an inactive recipient fails', async () => {
      redis.sismember.mockResolvedValue(0);
      notifications.closeIfPending.mockRejectedValue(new Error('db down'));

      const error = (await deliverOne().catch((err: unknown) => err)) as Error;

      expect(error.constructor.name).toBe('TransientDeliveryError');
      expect(redis.srem).toHaveBeenCalledWith(
        REDIS_KEYS.deliveredForJob(JOB_ID),
        CHAT_ID,
      );
      expect(sendMessage).not.toHaveBeenCalled();
    });

    it('skips a recipient already in the delivered set for this job', async () => {
      pipeline.exec.mockResolvedValue([
        [null, 0],
        [null, 1],
      ]);

      await deliverOne();

      expect(sendMessage).not.toHaveBeenCalled();
      expect(notifications.setStatus).not.toHaveBeenCalled();
    });

    it('marks a 403 as failed BOT_BLOCKED, deactivates the chat and keeps the claim', async () => {
      sendMessage.mockRejectedValue(
        telegramError(403, 'Forbidden: bot was blocked by the user'),
      );

      await expect(deliverOne()).resolves.toBeUndefined();

      expect(notifications.setStatus).toHaveBeenCalledWith(JOB_ID, CHAT_ID, {
        status: 'failed',
        reason: NotificationReason.BOT_BLOCKED,
        responseTimeMs: 1500,
      });
      expect(subscriber.deactivateUser).toHaveBeenCalledWith(
        CHAT_ID,
        NotificationReason.BOT_BLOCKED,
      );
      expect(redis.srem).not.toHaveBeenCalled();
    });

    it("retries a 429 after Telegram's retry_after and then records the send", async () => {
      sendMessage
        .mockRejectedValueOnce(
          telegramError(429, 'Too Many Requests: retry after 2', {
            retry_after: 2,
          }),
        )
        .mockResolvedValueOnce({ message_id: 1 });

      const delivery = deliverOne();
      await jest.advanceTimersByTimeAsync(1999);
      expect(sendMessage).toHaveBeenCalledTimes(1);
      await jest.advanceTimersByTimeAsync(1);
      await delivery;

      expect(sendMessage).toHaveBeenCalledTimes(2);
      expect(notifications.setStatus).toHaveBeenCalledWith(JOB_ID, CHAT_ID, {
        status: 'sent',
        responseTimeMs: 3500,
      });
    });

    it('gives up on repeated 429s as a transient failure and releases the claim', async () => {
      sendMessage.mockRejectedValue(
        telegramError(429, 'Too Many Requests: retry after 1', {
          retry_after: 1,
        }),
      );

      const delivery = deliverOne();
      const settled = expect(delivery).rejects.toThrow('Too Many Requests');
      await jest.advanceTimersByTimeAsync(2000);
      await settled;

      expect(sendMessage).toHaveBeenCalledTimes(3);
      expect(redis.srem).toHaveBeenCalledWith(
        REDIS_KEYS.deliveredForJob(JOB_ID),
        CHAT_ID,
      );
      expect(notifications.setStatus).not.toHaveBeenCalled();
    });

    it('throws a TransientDeliveryError on any other failure and releases the claim', async () => {
      sendMessage.mockRejectedValue(new Error('socket hang up'));

      const error = (await deliverOne().catch((err: unknown) => err)) as Error;

      expect(error).toBeInstanceOf(Error);
      expect(error.constructor.name).toBe('TransientDeliveryError');
      expect(error.message).toBe('socket hang up');
      expect(redis.srem).toHaveBeenCalledWith(
        REDIS_KEYS.deliveredForJob(JOB_ID),
        CHAT_ID,
      );
      expect(notifications.setStatus).not.toHaveBeenCalled();
      expect(subscriber.deactivateUser).not.toHaveBeenCalled();
    });

    it('does not throw after a successful send even if recording the status fails', async () => {
      notifications.setStatus.mockRejectedValue(new Error('db down'));

      await expect(deliverOne()).resolves.toBeUndefined();
      expect(redis.srem).not.toHaveBeenCalled();
    });
  });

  describe('handleNotification', () => {
    const handle = (entry: StreamEntry): Promise<void> =>
      (service as unknown as DispatchInternals).handleNotification(entry);

    const entry = (chatIds: string[]): StreamEntry => ({
      id: '1700000005000-0',
      fields: {
        jobId: JOB_ID,
        title: 'Build an app',
        link: '',
        description: '',
        categories: '[]',
        category: 'mobile_app',
        categoryName: 'Mobile App Development',
        source: 'upwork',
        sourceName: 'Upwork',
        chatIds: JSON.stringify(chatIds),
        enqueuedAt: String(NOW - 1500),
      },
    });

    it('delivers to every recipient and resolves when all succeed', async () => {
      await expect(handle(entry(['111', '222']))).resolves.toBeUndefined();
      expect(sendMessage).toHaveBeenCalledTimes(2);
    });

    it('rethrows so the entry stays unacked when any recipient failed transiently', async () => {
      sendMessage
        .mockResolvedValueOnce({ message_id: 1 })
        .mockRejectedValueOnce(new Error('ETIMEDOUT'));

      await expect(handle(entry(['111', '222']))).rejects.toThrow(
        'transient delivery failures',
      );
    });

    it.each([
      ['unparseable JSON', '[111,'],
      ['a JSON object', '{"a":"111"}'],
      ['an array of numbers', '[111, 222]'],
      ['JSON null', 'null'],
    ])(
      'drops (acks) an entry whose chatIds is %s instead of throwing',
      async (_label, chatIds) => {
        await expect(
          handle({ ...entry([]), fields: { ...entry([]).fields, chatIds } }),
        ).resolves.toBeUndefined();
        expect(redis.sismember).not.toHaveBeenCalled();
        expect(sendMessage).not.toHaveBeenCalled();
      },
    );

    it.each([
      ['unparseable JSON', '["Mobile",'],
      ['a JSON string', '"Mobile"'],
      ['an array of numbers', '[1, 2]'],
    ])(
      'still delivers, with the default tag, when categories is %s',
      async (_label, categories) => {
        const base = entry(['111']);

        await expect(
          handle({ ...base, fields: { ...base.fields, categories } }),
        ).resolves.toBeUndefined();

        expect(sendMessage).toHaveBeenCalledTimes(1);
        const [, text] = sendMessage.mock.calls[0] as [string, string];
        // Same as an empty list: tagged with the category's display name.
        expect(text).toContain('#MobileAppDevelopment');
      },
    );

    it('renders well-formed categories as hashtags', async () => {
      const base = entry(['111']);

      await handle({
        ...base,
        fields: { ...base.fields, categories: '["Mobile"]' },
      });

      const [, text] = sendMessage.mock.calls[0] as [string, string];
      expect(text).toContain('#Mobile');
    });

    it('renders the source display name on the Platform line', async () => {
      await handle(entry(['111']));

      const [, text] = sendMessage.mock.calls[0] as [string, string];
      expect(text).toContain('<b>Platform</b>\nUpwork');
    });

    it('falls back to the source id for an entry queued without sourceName', async () => {
      const base = entry(['111']);
      const fields = { ...base.fields };
      delete fields.sourceName;

      await handle({ ...base, fields });

      const [, text] = sendMessage.mock.calls[0] as [string, string];
      expect(text).toContain('<b>Platform</b>\nupwork');
    });

    it('drops an entry without jobId or chatIds', async () => {
      await expect(
        handle({ id: '1-0', fields: { jobId: JOB_ID } }),
      ).resolves.toBeUndefined();
      expect(sendMessage).not.toHaveBeenCalled();
    });
  });

  describe('handleDeadLetter', () => {
    it("marks the dead-lettered entry's recipients as delivery-retries-exhausted", async () => {
      await (service as unknown as DispatchInternals).handleDeadLetter({
        jobId: JOB_ID,
        chatIds: JSON.stringify(['111', '222']),
      });

      expect(notifications.markDeliveryRetriesExhausted).toHaveBeenCalledWith(
        JOB_ID,
        ['111', '222'],
      );
    });

    it('does nothing when the chatIds field is malformed', async () => {
      await expect(
        (service as unknown as DispatchInternals).handleDeadLetter({
          jobId: JOB_ID,
          chatIds: 'not json',
        }),
      ).resolves.toBeUndefined();

      expect(notifications.markDeliveryRetriesExhausted).not.toHaveBeenCalled();
    });
  });
});
