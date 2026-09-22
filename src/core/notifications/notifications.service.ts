import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import {
  NotificationEntity,
  NotificationReason,
  NotificationStatus,
} from './entities/notification.entity';

@Injectable()
export class NotificationsService {
  constructor(
    @InjectRepository(NotificationEntity)
    private readonly notifications: Repository<NotificationEntity>,
  ) {}

  /** True if this job has already been fanned out to at least one user. */
  async hasBeenProcessed(jobId: string): Promise<boolean> {
    const count = await this.notifications.count({ where: { jobId } });
    return count > 0;
  }

  /**
   * Inserts one pending row per matched recipient, ignoring rows that
   * already exist so re-delivery of the same jobs:notify entry (via XCLAIM)
   * can never create duplicates.
   */
  async insertPendingForChats(jobId: string, chatIds: string[]): Promise<void> {
    if (chatIds.length === 0) return;

    await this.notifications
      .createQueryBuilder()
      .insert()
      .into(NotificationEntity)
      .values(chatIds.map((chatId) => ({ jobId, chatId, status: 'pending' })))
      .orIgnore()
      .execute();
  }

  async setStatus(
    jobId: string,
    chatId: string,
    status: NotificationStatus,
    reason: NotificationReason | null = null,
  ): Promise<void> {
    await this.notifications.update(
      { jobId, chatId },
      { status, sentAt: status === 'sent' ? new Date() : null, reason },
    );
  }
}
