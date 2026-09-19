import { Body, Controller, Param, Patch } from '@nestjs/common';
import { NotificationsService } from './notifications.service';
import { UpdateNotificationStatusDto } from './dto/update-notification-status.dto';

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
    await this.notifications.setStatus(jobId, chatId, dto.status);
    return { jobId, chatId, status: dto.status };
  }
}
