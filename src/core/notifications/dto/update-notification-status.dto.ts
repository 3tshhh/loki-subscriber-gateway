import { IsIn, IsInt, Min, ValidateIf } from 'class-validator';
import {
  NOTIFICATION_OUTCOME_REASONS,
  type NotificationOutcomeReason,
  NotificationStatus,
} from '../entities/notification.entity';

export class UpdateNotificationStatusDto {
  @IsIn(['sent', 'failed'])
  status: Extract<NotificationStatus, 'sent' | 'failed'>;

  /** Required for `failed` (which outcome caused it); ignored for `sent`,
   * whose reason is always DELIVERED. */
  @ValidateIf((dto: UpdateNotificationStatusDto) => dto.status === 'failed')
  @IsIn(NOTIFICATION_OUTCOME_REASONS)
  reason?: NotificationOutcomeReason;

  /** Arrival in jobs:notify → this outcome, in ms — required for every
   * status, since response_time_ms is never NULL. */
  @IsInt()
  @Min(0)
  responseTimeMs: number;
}
