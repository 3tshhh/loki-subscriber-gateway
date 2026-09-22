import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { Bot, Context, GrammyError } from 'grammy';
import { BOT } from '../bot.provider';
import { BotSubscriberService } from '../bot-subscriber.service';
import { NotificationReason } from '../../core/notifications/entities/notification.entity';
import {
  categoryCaption,
  categoryKeyboard,
  RESUME_CATEGORIES_CALLBACK,
  RESUME_SOURCES_CALLBACK,
  reviewMessage,
  sourceCaption,
  sourceKeyboard,
} from './selector.util';
import { toggleId, WizardState, WizardStateStore } from './wizard-state.store';

/**
 * Reads and writes are deliberately split for UX, not just performance:
 * /start, /categories, /sources are pure reads — one SELECT via
 * BotSubscriberService.getBootstrapSnapshot, nothing else — so the reply
 * never waits on a write. /start's row-creating/activating write and
 * /stop's deactivation write both happen *after* the reply is already
 * sent, in the background; if either fails, a follow-up message asks the
 * user to try again rather than the original reply being delayed or
 * silently wrong. Toggling categories/sources never touches Postgres at
 * all — the enabled lists and in-progress picks both live in
 * WizardStateStore once fetched at bootstrap. Only "Done" writes anything,
 * via updateSubscriptions. See backup/*.ts.bak for earlier iterations of
 * this file.
 */
@Injectable()
export class TelegramCommandsService implements OnModuleInit {
  private readonly logger = new Logger(TelegramCommandsService.name);

  constructor(
    @Inject(BOT) private readonly bot: Bot,
    private readonly botData: BotSubscriberService,
    private readonly wizard: WizardStateStore,
  ) {}

  onModuleInit() {
    this.bot.command('start', (ctx) => this.handleStart(ctx));
    this.bot.command('stop', (ctx) => this.handleStop(ctx));
    this.bot.command('categories', (ctx) => this.handleCategoriesCommand(ctx));
    this.bot.command('sources', (ctx) => this.handleSourcesCommand(ctx));
    this.bot.on('callback_query:data', (ctx) => this.handleCallback(ctx));
  }

  private async handleStart(ctx: Context): Promise<void> {
    if (!ctx.chat) return;
    const chatId = String(ctx.chat.id);

    // Pure read — works even if this chat has never had a users row at all.
    const snapshot = await this.botData.getBootstrapSnapshot(chatId);
    this.wizard.start(chatId, {
      categoryIds: snapshot.categoryIds,
      sourceIds: snapshot.sourceIds,
      categories: snapshot.categories,
      sources: snapshot.sources,
      mode: 'wizard',
    });

    await ctx.reply(
      'Welcome to Loki Jobs 👋\n\nChoose the categories you want to receive.',
      {
        reply_markup: categoryKeyboard(
          snapshot.categories,
          snapshot.categoryIds,
          false,
        ),
      },
    );

    // The only write /start makes — deliberately after the reply above, so
    // a slow or failed write never delays what the user sees.
    this.botData
      .activateUserUpsert(chatId, ctx.from?.username)
      .catch((err: Error) => {
        this.logger.error(
          `Failed to activate chat ${chatId} after /start: ${err.message}`,
          err.stack,
        );
        ctx
          .reply(
            "Something went wrong saving your subscription — please send /start again to make sure it's saved.",
          )
          .catch(() => {});
      });
  }

  private async handleStop(ctx: Context): Promise<void> {
    if (!ctx.chat) return;
    const chatId = String(ctx.chat.id);
    this.wizard.clear(chatId);

    // No read needed at all — this reply doesn't depend on any stored
    // state. The deactivation write happens after, in the background.
    await ctx.reply(
      "You've been unsubscribed from notifications. You won't receive any more job notifications.\n\n" +
        'To start receiving jobs again, send /start.',
    );

    this.botData
      .deactivateUser(chatId, NotificationReason.USER_UNSUBSCRIBED)
      .catch((err: Error) => {
        this.logger.error(
          `Failed to deactivate chat ${chatId} after /stop: ${err.message}`,
          err.stack,
        );
        ctx
          .reply(
            'Something went wrong unsubscribing you — please send /stop again to make sure it goes through.',
          )
          .catch(() => {});
      });
  }

  private async handleCategoriesCommand(ctx: Context): Promise<void> {
    if (!ctx.chat) return;
    const chatId = String(ctx.chat.id);
    const snapshot = await this.botData.getBootstrapSnapshot(chatId);
    this.wizard.start(chatId, {
      categoryIds: snapshot.categoryIds,
      sourceIds: snapshot.sourceIds,
      categories: snapshot.categories,
      sources: snapshot.sources,
      mode: 'categories-only',
    });
    await ctx.reply(categoryCaption(!snapshot.active), {
      reply_markup: categoryKeyboard(
        snapshot.categories,
        snapshot.categoryIds,
        !snapshot.active,
      ),
    });
  }

  private async handleSourcesCommand(ctx: Context): Promise<void> {
    if (!ctx.chat) return;
    const chatId = String(ctx.chat.id);
    const snapshot = await this.botData.getBootstrapSnapshot(chatId);
    this.wizard.start(chatId, {
      categoryIds: snapshot.categoryIds,
      sourceIds: snapshot.sourceIds,
      categories: snapshot.categories,
      sources: snapshot.sources,
      mode: 'sources-only',
    });
    await ctx.reply(sourceCaption(!snapshot.active), {
      reply_markup: sourceKeyboard(
        snapshot.sources,
        snapshot.sourceIds,
        !snapshot.active,
      ),
    });
  }

  private async handleCallback(ctx: Context): Promise<void> {
    const data = ctx.callbackQuery?.data;
    if (!data || !ctx.chat) return;
    const chatId = String(ctx.chat.id);

    try {
      if (
        data === RESUME_CATEGORIES_CALLBACK ||
        data === RESUME_SOURCES_CALLBACK
      ) {
        await this.handleResume(ctx, chatId, data);
        return;
      }

      const state = this.wizard.get(chatId);
      if (!state) {
        await ctx.answerCallbackQuery({
          text: 'Session expired, please send /start again.',
          show_alert: true,
        });
        return;
      }

      if (data === 'done') {
        await this.handleCategoriesDone(ctx, chatId, state);
        return;
      }

      if (data === 'src:done') {
        await ctx.answerCallbackQuery();
        await this.finalizeWizard(ctx, chatId, state);
        return;
      }

      if (data.startsWith('src:')) {
        await this.handleSourceToggle(ctx, state, data.slice(4));
        return;
      }

      if (data.startsWith('cat:')) {
        await this.handleCategoryToggle(ctx, state, data.slice(4));
        return;
      }

      await ctx.answerCallbackQuery();
    } catch (err) {
      this.logger.error(
        `Failed to handle callback ${data}: ${(err as Error).message}`,
        (err as Error).stack,
      );
      await ctx.answerCallbackQuery({
        text: 'Something went wrong, please try again.',
      });
    }
  }

  /** Resume re-renders from the cached wizard state — no read at all. If
   * the session was lost (process restart mid-wizard), there's nothing to
   * rebuild the screen from, so just point them back at /start. */
  private async handleResume(
    ctx: Context,
    chatId: string,
    data: string,
  ): Promise<void> {
    const state = this.wizard.get(chatId);
    await this.botData.activateUserUpsert(chatId, ctx.from?.username);

    if (!state) {
      await ctx.answerCallbackQuery({
        text: 'Notifications resumed ✅ — send /start to see your picks again.',
        show_alert: true,
      });
      return;
    }

    await ctx.answerCallbackQuery({ text: 'Notifications resumed ✅' });

    if (data === RESUME_SOURCES_CALLBACK) {
      await this.editIgnoringNoChange(() =>
        ctx.editMessageText(sourceCaption(false), {
          reply_markup: sourceKeyboard(state.sources, state.sourceIds, false),
        }),
      );
    } else {
      await this.editIgnoringNoChange(() =>
        ctx.editMessageText(categoryCaption(false), {
          reply_markup: categoryKeyboard(
            state.categories,
            state.categoryIds,
            false,
          ),
        }),
      );
    }
  }

  /** Categories "Done": wizard mode chains to the sources screen (cached,
   * no read, no save yet); categories-only mode finalizes immediately. */
  private async handleCategoriesDone(
    ctx: Context,
    chatId: string,
    state: WizardState,
  ): Promise<void> {
    if (state.mode === 'wizard') {
      await ctx.answerCallbackQuery();
      await this.editIgnoringNoChange(() =>
        ctx.editMessageText(sourceCaption(false), {
          reply_markup: sourceKeyboard(state.sources, state.sourceIds, false),
        }),
      );
      return;
    }

    await ctx.answerCallbackQuery();
    await this.finalizeWizard(ctx, chatId, state);
  }

  private async handleSourceToggle(
    ctx: Context,
    state: WizardState,
    raw: string,
  ): Promise<void> {
    const sourceId = raw;
    if (!state.sources.some((s) => s.id === sourceId)) {
      await ctx.answerCallbackQuery({
        text: 'That source is no longer available.',
        show_alert: true,
      });
      return;
    }

    toggleId(state.sourceIds, sourceId);
    await ctx.answerCallbackQuery();
    await this.editIgnoringNoChange(() =>
      ctx.editMessageReplyMarkup({
        reply_markup: sourceKeyboard(state.sources, state.sourceIds, false),
      }),
    );
  }

  private async handleCategoryToggle(
    ctx: Context,
    state: WizardState,
    raw: string,
  ): Promise<void> {
    const categoryId = raw;
    if (!state.categories.some((c) => c.id === categoryId)) {
      await ctx.answerCallbackQuery({
        text: 'That category is no longer available.',
        show_alert: true,
      });
      return;
    }

    toggleId(state.categoryIds, categoryId);
    await ctx.answerCallbackQuery();
    await this.editIgnoringNoChange(() =>
      ctx.editMessageReplyMarkup({
        reply_markup: categoryKeyboard(
          state.categories,
          state.categoryIds,
          false,
        ),
      }),
    );
  }

  /** The one and only server write for the wizard itself: a single
   * combined save, then the full categories+sources review, built from
   * the already-cached lists — no read here either. */
  private async finalizeWizard(
    ctx: Context,
    chatId: string,
    state: WizardState,
  ): Promise<void> {
    const updated = await this.botData.updateSubscriptions(
      chatId,
      state.categoryIds,
      state.sourceIds,
      ctx.from?.username,
    );
    this.wizard.clear(chatId);

    await this.editIgnoringNoChange(() =>
      ctx.editMessageText(
        reviewMessage(
          state.categories,
          updated.categoryIds,
          state.sources,
          updated.sourceIds,
        ),
      ),
    );
  }

  /**
   * Telegram rejects an edit that doesn't actually change the message's
   * text/markup (400: "message is not modified") — this isn't a real
   * failure, it just means a callback was delivered twice (a double-tap,
   * or Telegram retrying) and the screen already shows the right thing.
   */
  private async editIgnoringNoChange(
    fn: () => Promise<unknown>,
  ): Promise<void> {
    try {
      await fn();
    } catch (err) {
      if (
        err instanceof GrammyError &&
        err.description.includes('message is not modified')
      ) {
        return;
      }
      throw err;
    }
  }
}
