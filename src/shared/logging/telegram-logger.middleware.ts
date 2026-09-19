import type { Context, NextFunction } from 'grammy';
import { colorDuration, colorModule, colorOutcome } from './colors';

/**
 * grammY-level equivalent of RequestLoggerMiddleware: one colorized line
 * per update, with response time. Necessary as its own thing because
 * RequestLoggerMiddleware only sees inbound HTTP requests — in long-polling
 * mode (the default here), incoming Telegram updates never go through Nest's
 * HTTP pipeline at all, so bot commands/callbacks were previously invisible
 * to any timing log.
 *
 * Registered via bot.use() in bot.provider.ts, which runs at provider
 * construction time — always before TelegramCommandsService's
 * onModuleInit() registers the actual command/callback handlers, so this
 * correctly wraps timing around all of them.
 */
export function createTelegramLoggerMiddleware(enabled: boolean) {
  return async (ctx: Context, next: NextFunction): Promise<void> => {
    if (!enabled) {
      await next();
      return;
    }

    const start = process.hrtime.bigint();
    let ok = true;
    try {
      await next();
    } catch (err) {
      ok = false;
      throw err; // still let grammy's bot.catch log the actual error detail
    } finally {
      const durationMs = Number(process.hrtime.bigint() - start) / 1_000_000;
      const chatId = ctx.chat?.id ?? ctx.from?.id ?? '-';
      console.log(
        [
          colorModule('TELEGRAM-BOT'),
          `chat=${chatId}`,
          describeUpdate(ctx),
          colorOutcome(ok),
          colorDuration(durationMs),
        ].join(' '),
      );
    }
  };
}

function describeUpdate(ctx: Context): string {
  const text = ctx.message?.text;
  if (text?.startsWith('/')) return text.split(/\s+/)[0];

  const data = ctx.callbackQuery?.data;
  if (data) return `cb:${data}`;

  return `update:${ctx.update.update_id}`;
}
