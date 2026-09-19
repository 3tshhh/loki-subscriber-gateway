import { Column, CreateDateColumn, Entity, PrimaryColumn } from 'typeorm';

/**
 * `chat_id` is the primary key: this bot only ever does 1:1 DMs, where
 * Telegram's chat_id and user_id are numerically identical, so there's no
 * value in carrying a separate surrogate id or a redundant telegram_user_id
 * column — chat_id already is the user's identity everywhere in this system.
 */
@Entity('users')
export class UserEntity {
  @PrimaryColumn({ name: 'chat_id', type: 'bigint' })
  chatId: string;

  @Column({ default: true })
  active: boolean;

  /** Telegram @username at the time of last /start or resume — nullable,
   * since a user can have none set, and re-saved on every activation so it
   * stays current if they change it later. */
  @Column({ type: 'text', nullable: true })
  username: string | null;

  @Column({ name: 'deactivated_at', type: 'timestamp', nullable: true })
  deactivatedAt: Date | null;

  @CreateDateColumn({ name: 'created_at' })
  createdAt: Date;
}
