import { IsEnum, IsIn, IsInt, IsOptional } from 'class-validator';
import {
  NotificationReason,
  NotificationStatus,
} from '../entities/notification.entity';

export class UpdateNotificationStatusDto {
  @IsIn(['sent', 'failed'])
  status: Extract<NotificationStatus, 'sent' | 'failed'>;

  @IsOptional()
  @IsEnum(NotificationReason)
  reason?: NotificationReason;

  @IsOptional()
  @IsInt()
  responseTimeMs?: number;
}
