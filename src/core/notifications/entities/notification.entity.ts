import {
  Column,
  CreateDateColumn,
  Entity,
  JoinColumn,
  ManyToOne,
  PrimaryColumn,
} from 'typeorm';
import { UserEntity } from '../../subscriber/entities/user.entity';

export type NotificationStatus = 'pending' | 'sent' | 'failed' | 'cancelled';

/** Why a `failed`/`cancelled` row ended up that way — set at the exact
 * point each outcome is detected, never inferred after the fact. */
export enum NotificationReason {
  /** Telegram returned 403 (recipient blocked the bot) — the delivery
   * attempt itself is marked `failed` with this reason, and it also
   * triggers the cascading `cancelled` on that chat's other pending rows. */
  BOT_BLOCKED = 'bot_blocked',
  /** The recipient ran /stop — cancels their still-pending rows. */
  USER_UNSUBSCRIBED = 'user_unsubscribed',
  /** Exhausted MAX_DELIVERY_ATTEMPTS XCLAIM retries and was moved to the
   * dead-letter stream without ever successfully sending. */
  DELIVERY_RETRIES_EXHAUSTED = 'delivery_retries_exhausted',
}

@Entity('notifications')
export class NotificationEntity {
  @PrimaryColumn({ name: 'job_id' })
  jobId: string;

  @PrimaryColumn({ name: 'chat_id', type: 'bigint' })
  chatId: string;

  @ManyToOne(() => UserEntity, { onDelete: 'CASCADE' })
  @JoinColumn({ name: 'chat_id' })
  user: UserEntity;

  @Column({ type: 'text', default: 'pending' })
  status: NotificationStatus;

  @CreateDateColumn({ name: 'created_at' })
  createdAt: Date;

  @Column({ name: 'sent_at', type: 'timestamp', nullable: true })
  sentAt: Date | null;

  /** Null for `pending`/`sent` rows — only ever set alongside `failed`/`cancelled`. */
  @Column({ type: 'text', nullable: true })
  reason: NotificationReason | null;
}
