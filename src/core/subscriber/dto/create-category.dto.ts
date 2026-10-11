import { Matches, MaxLength, MinLength } from 'class-validator';

/** id is a stable slug matched exactly (case-sensitive, no normalization)
 * against the `category` field of incoming jobs, so it's kept to lowercase
 * snake_case. Capped at 60 characters because it's also sent as Telegram
 * button callback data (`cat:<id>`), which Telegram limits to 64 bytes —
 * a longer id would make the picker fail to render. */
export class CreateCategoryDto {
  @Matches(/^[a-z0-9_]+$/, {
    message: 'id must be lowercase snake_case (e.g. "graphic_design")',
  })
  @MaxLength(60)
  id: string;

  @MinLength(1)
  @MaxLength(200)
  name: string;
}
