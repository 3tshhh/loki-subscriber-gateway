import {
  CreateDateColumn,
  Entity,
  JoinColumn,
  ManyToOne,
  PrimaryColumn,
} from 'typeorm';
import { UserEntity } from './user.entity';
import { SourceEntity } from './source.entity';

@Entity('user_sources')
export class UserSourceEntity {
  @PrimaryColumn({ name: 'chat_id', type: 'bigint' })
  chatId: string;

  @PrimaryColumn({ name: 'source_id' })
  sourceId: string;

  @ManyToOne(() => UserEntity, { onDelete: 'CASCADE' })
  @JoinColumn({ name: 'chat_id' })
  user: UserEntity;

  @ManyToOne(() => SourceEntity, { onDelete: 'CASCADE' })
  @JoinColumn({ name: 'source_id' })
  source: SourceEntity;

  /** Nullable since rows created before this column existed have none. */
  @CreateDateColumn({ name: 'created_at', nullable: true })
  createdAt: Date | null = null;
}
