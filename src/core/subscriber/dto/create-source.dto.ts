import { Matches, MaxLength, MinLength } from 'class-validator';

/** id is a stable slug used directly in Redis subscriber-set keys — kept
 * to lowercase snake_case so it never needs normalizing for matching. */
export class CreateSourceDto {
  @Matches(/^[a-z0-9_]+$/, {
    message: 'id must be lowercase snake_case (e.g. "linkedin")',
  })
  @MaxLength(100)
  id: string;

  @MinLength(1)
  @MaxLength(200)
  name: string;
}
