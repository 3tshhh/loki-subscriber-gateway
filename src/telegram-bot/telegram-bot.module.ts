import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { NotificationEntity } from '../core/notifications/entities/notification.entity';
import { SubscriberModule } from '../core/subscriber/subscriber.module';
import { NotificationsModule } from '../core/notifications/notifications.module';
import { botProvider } from './bot.provider';
import { BotSubscriberService } from './bot-subscriber.service';
import { TelegramBootstrapService } from './telegram-bootstrap.service';
import { TelegramCommandsService } from './commands/telegram-commands.service';
import { WizardStateStore } from './commands/wizard-state.store';
import { TelegramDispatchService } from './dispatch/telegram-dispatch.service';

/**
 * Talks to Postgres via Core's own SubscriberService/NotificationsService
 * now (imported directly, in-process — no HTTP hop), so the bot's business
 * logic and Core's HTTP-facing logic share one canonical implementation
 * per operation instead of drifting independently. BotSubscriberService is
 * kept around, still fully wired below, purely as a rollback path — if
 * this consolidation needs to be reverted, swapping call sites back to it
 * is a one-line change per call site, not a rewrite.
 *
 * Only NotificationEntity needs @InjectRepository here (for
 * BotSubscriberService, still registered) — UserCategoryEntity/
 * UserSourceEntity are only ever referenced as plain class tokens
 * (manager.insert/delete), which works off entity metadata Core's
 * SubscriberModule already registers app-wide via autoLoadEntities — no
 * need to redeclare them here too.
 */
@Module({
  imports: [
    TypeOrmModule.forFeature([NotificationEntity]),
    SubscriberModule,
    NotificationsModule,
  ],
  providers: [
    botProvider,
    BotSubscriberService,
    TelegramBootstrapService,
    TelegramCommandsService,
    WizardStateStore,
    TelegramDispatchService,
  ],
})
export class TelegramBotModule {}
