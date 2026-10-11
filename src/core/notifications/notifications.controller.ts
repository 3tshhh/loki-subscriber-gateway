import { Body, Controller, Param, Patch } from '@nestjs/common';
import {
  NotificationStatusChange,
  NotificationsService,
} from './notifications.service';
import { UpdateNotificationStatusDto } from './dto/update-notification-status.dto';
import { NotificationReason } from './entities/notification.entity';

/**
 * Called by the telegram-bot module (over HTTP, never a direct import) to
 * report the outcome of a delivery attempt.
 */
@Controller('notifications')
export class NotificationsController {
  constructor(private readonly notifications: NotificationsService) {}

  @Patch(':jobId/:chatId')
  async updateStatus(
    @Param('jobId') jobId: string,
    @Param('chatId') chatId: string,
    @Body() dto: UpdateNotificationStatusDto,
  ) {
    // The DTO's @ValidateIf already guarantees a reason for `failed`; the
    // non-null assertion just carries that guarantee into the type.
    const change: NotificationStatusChange =
      dto.status === 'sent'
        ? { status: 'sent', responseTimeMs: dto.responseTimeMs }
        : {
            status: 'failed',
            reason: dto.reason!,
            responseTimeMs: dto.responseTimeMs,
          };
    await this.notifications.setStatus(jobId, chatId, change);
    return {
      jobId,
      chatId,
      status: change.status,
      reason:
        change.status === 'sent' ? NotificationReason.DELIVERED : change.reason,
      responseTimeMs: change.responseTimeMs,
    };
  }
}
