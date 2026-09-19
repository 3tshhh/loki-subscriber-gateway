import { Module } from '@nestjs/common';
import { NotificationsModule } from '../notifications/notifications.module';
import { JobMoverService } from './job-mover.service';
import { JobStreamService } from './job-stream.service';

@Module({
  imports: [NotificationsModule],
  providers: [JobMoverService, JobStreamService],
})
export class JobStreamModule {}
