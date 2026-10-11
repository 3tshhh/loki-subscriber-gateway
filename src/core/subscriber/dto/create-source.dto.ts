import { IsNotIn, Matches, MaxLength, MinLength } from 'class-validator';
import { RESERVED_SOURCE_IDS } from './reserved-ids';

/** id is a stable slug matched exactly (case-sensitive, no normalization)
 * against the `source` field of incoming jobs, so it's kept to lowercase
 * snake_case. Capped at 60 characters because it's also sent as Telegram
 * button callback data (`src:<id>`), which Telegram limits to 64 bytes —
 * a longer id would make the picker fail to render. Ids the picker
 * already uses as its own callback data (e.g. `done`) are rejected — see
 * reserved-ids.ts. */
export class CreateSourceDto {
  @Matches(/^[a-z0-9_]+$/, {
    message: 'id must be lowercase snake_case (e.g. "linkedin")',
  })
  @MaxLength(60)
  @IsNotIn(RESERVED_SOURCE_IDS, {
    message: `id must not be one of the reserved ids (${RESERVED_SOURCE_IDS.join(', ')}) — they're already used by the bot's picker buttons`,
  })
  id: string;

  @MinLength(1)
  @MaxLength(200)
  name: string;
}
