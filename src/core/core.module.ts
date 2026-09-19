import { Module } from '@nestjs/common';
import { SubscriberModule } from './subscriber/subscriber.module';
import { JobStreamModule } from './job-stream/job-stream.module';
import { NotificationsModule } from './notifications/notifications.module';

@Module({
  imports: [SubscriberModule, JobStreamModule, NotificationsModule],
})
export class CoreModule {}
