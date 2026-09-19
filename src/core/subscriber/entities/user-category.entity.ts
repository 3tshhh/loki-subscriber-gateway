import { Entity, JoinColumn, ManyToOne, PrimaryColumn } from 'typeorm';
import { UserEntity } from './user.entity';
import { CategoryEntity } from './category.entity';

@Entity('user_categories')
export class UserCategoryEntity {
  @PrimaryColumn({ name: 'chat_id', type: 'bigint' })
  chatId: string;

  @PrimaryColumn({ name: 'category_id' })
  categoryId: string;

  @ManyToOne(() => UserEntity, { onDelete: 'CASCADE' })
  @JoinColumn({ name: 'chat_id' })
  user: UserEntity;

  @ManyToOne(() => CategoryEntity, { onDelete: 'CASCADE' })
  @JoinColumn({ name: 'category_id' })
  category: CategoryEntity;
}
