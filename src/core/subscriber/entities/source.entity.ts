import { Column, Entity, PrimaryColumn } from 'typeorm';

/** id is a stable slug (e.g. 'linkedin'), not an auto-increment number —
 * matching is done on this id, not on `name`. */
@Entity('sources')
export class SourceEntity {
  @PrimaryColumn()
  id: string;

  @Column({ unique: true })
  name: string;

  @Column({ default: true })
  enabled: boolean;
}
