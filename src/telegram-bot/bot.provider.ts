import { Logger, Provider } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Bot, BotError } from 'grammy';
import { createTelegramLoggerMiddleware } from '../shared/logging/telegram-logger.middleware';

export const BOT = Symbol('BOT');

export const botProvider: Provider = {
  provide: BOT,
  inject: [ConfigService],
  useFactory: (config: ConfigService) => {
    const bot = new Bot(config.get<string>('TELEGRAM_BOT_TOKEN')!);

    // Registered before TelegramCommandsService's onModuleInit() adds the
    // actual command/callback handlers (provider construction always runs
    // before onModuleInit), so this correctly times every one of them.
    const isTest = config.get<string>('NODE_ENV') === 'test';
    bot.use(createTelegramLoggerMiddleware(!isTest));

    // grammY's default error handler stops the bot entirely (and rethrows)
    // whenever a middleware/handler throws — a single transient error (a
    // dropped connection while calling Core's API, say) would otherwise
    // kill Telegram connectivity for the life of the process. Logging and
    // continuing keeps the bot serving every other update.
    const logger = new Logger('TelegramBotErrorHandler');
    bot.catch((err: BotError) => {
      const cause =
        err.error instanceof Error ? err.error : new Error(String(err.error));
      logger.error(
        `Unhandled error in bot middleware (update ${err.ctx.update.update_id}): ${cause.message}`,
        cause.stack,
      );
    });

    return bot;
  },
};
