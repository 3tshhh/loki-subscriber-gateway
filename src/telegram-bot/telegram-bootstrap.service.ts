import {
  Inject,
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { Bot } from 'grammy';
import { BOT } from './bot.provider';

/**
 * Polling only — no public endpoint, no attack surface, no TLS/ngrok
 * churn. See the polling-vs-webhook comparison in conversation history for
 * why: at this bot's scale (single instance, no plan to horizontally
 * scale), webhook mode's only real costs (a public POST endpoint with no
 * secret-token validation, ngrok/TLS operational overhead) bought nothing.
 */
@Injectable()
export class TelegramBootstrapService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(TelegramBootstrapService.name);

  constructor(@Inject(BOT) private readonly bot: Bot) {}

  onModuleInit(): void {
    this.bot
      .start()
      .catch((err: Error) =>
        this.logger.error(
          `Bot polling loop crashed: ${err.message}`,
          err.stack,
        ),
      );
    this.logger.log('Telegram bot started in long-polling mode');
  }

  /**
   * Releases the long-poll connection to Telegram on shutdown. Without
   * this, a killed process — including a dev-server hot-reload — leaves an
   * outstanding getUpdates call open, and the next process to start
   * collides with it (409: terminated by other getUpdates request).
   */
  async onModuleDestroy(): Promise<void> {
    await this.bot.stop();
  }
}
