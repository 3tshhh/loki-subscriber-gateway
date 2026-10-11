import { Module } from '@nestjs/common';
import { SubscriberModule } from '../core/subscriber/subscriber.module';
import { NotificationsModule } from '../core/notifications/notifications.module';
import { botProvider } from './bot.provider';
import { TelegramBootstrapService } from './telegram-bootstrap.service';
import { TelegramCommandsService } from './commands/telegram-commands.service';
import { WizardStateStore } from './commands/wizard-state.store';
import { TelegramDispatchService } from './dispatch/telegram-dispatch.service';

/**
 * Talks to Postgres via Core's own SubscriberService/NotificationsService
 * (imported directly, in-process — no HTTP hop), so the bot's business
 * logic and Core's HTTP-facing logic share one canonical implementation
 * per operation instead of drifting independently. The bot's old raw-SQL
 * duplicate of that logic (BotSubscriberService) has been deleted; if this
 * consolidation ever needs to be reverted, git history is the rollback
 * path.
 *
 * No TypeOrmModule.forFeature here: nothing in this module injects a
 * repository itself — notification writes go through NotificationsService
 * from the imported NotificationsModule. UserCategoryEntity/
 * UserSourceEntity are only ever referenced as plain class tokens
 * (manager.insert/delete), which works off entity metadata Core's
 * SubscriberModule already registers app-wide via autoLoadEntities.
 */
@Module({
  imports: [SubscriberModule, NotificationsModule],
  providers: [
    botProvider,
    TelegramBootstrapService,
    TelegramCommandsService,
    WizardStateStore,
    TelegramDispatchService,
  ],
})
export class TelegramBotModule {}
