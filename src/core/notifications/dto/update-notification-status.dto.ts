import { IsIn } from 'class-validator';
import { NotificationStatus } from '../entities/notification.entity';

export class UpdateNotificationStatusDto {
  @IsIn(['sent', 'failed'])
  status: Extract<NotificationStatus, 'sent' | 'failed'>;
}
