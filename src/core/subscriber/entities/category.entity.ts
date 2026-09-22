import { Column, CreateDateColumn, Entity, PrimaryColumn } from 'typeorm';

/** id is a stable slug (e.g. 'backend'), not an auto-increment number —
 * matching is done on this id, not on `name`. */
@Entity('categories')
export class CategoryEntity {
  @PrimaryColumn()
  id: string;

  @Column({ unique: true })
  name: string;

  @Column({ default: true })
  enabled: boolean;

  /** Nullable since rows created before this column existed have none. */
  @CreateDateColumn({ name: 'created_at', nullable: true })
  createdAt: Date | null = null;
}
