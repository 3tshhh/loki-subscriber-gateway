import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { NotificationEntity } from '../core/notifications/entities/notification.entity';
import { botProvider } from './bot.provider';
import { BotSubscriberService } from './bot-subscriber.service';
import { TelegramBootstrapService } from './telegram-bootstrap.service';
import { TelegramCommandsService } from './commands/telegram-commands.service';
import { WizardStateStore } from './commands/wizard-state.store';
import { TelegramDispatchService } from './dispatch/telegram-dispatch.service';

/**
 * Talks to Postgres directly (via BotSubscriberService) instead of over
 * HTTP to Core — see bot-subscriber.service.ts for why. Core's own
 * subscriber.service.ts is untouched and keeps using the same tables for
 * its own job-stream matching; this is a second, independent data-access
 * path onto the same schema.
 *
 * Only NotificationEntity needs @InjectRepository here — everything
 * user/category/source-related goes through raw SQL now
 * (bot-subscriber.service.ts), and UserCategoryEntity/UserSourceEntity are
 * only ever referenced as plain class tokens (manager.insert/delete), which
 * works off entity metadata Core's SubscriberModule already registers
 * app-wide via autoLoadEntities — no need to redeclare them here too.
 */
@Module({
  imports: [TypeOrmModule.forFeature([NotificationEntity])],
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
