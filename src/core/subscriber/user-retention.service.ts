import { Injectable, Logger } from '@nestjs/common';
import { Cron, CronExpression } from '@nestjs/schedule';
import { SubscriberService } from './subscriber.service';

@Injectable()
export class UserRetentionService {
  private readonly logger = new Logger(UserRetentionService.name);

  constructor(private readonly subscriber: SubscriberService) {}

  @Cron(CronExpression.EVERY_DAY_AT_3AM)
  async sweep(): Promise<void> {
    const deleted = await this.subscriber.hardDeleteExpiredDeactivations();
    if (deleted > 0) {
      this.logger.log(
        `Hard-deleted ${deleted} user(s) past the retention window`,
      );
    }
  }
}
