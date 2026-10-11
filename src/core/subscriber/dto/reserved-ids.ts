/**
 * Ids a category/source may not take, because the bot's picker keyboards
 * already use them as their own callback data (see
 * telegram-bot/commands/selector.util.ts). A source's button is
 * `src:<id>` and the sources picker's Done button is `src:done`, so a
 * source with id `done` would render a button indistinguishable from Done
 * — tapping it would finish the wizard instead of toggling the source.
 *
 * The categories picker's Done button is the bare `done`, so `cat:done`
 * wouldn't actually collide today; `done` is reserved for categories too
 * so both pickers stay interchangeable — moving categories' Done to
 * `cat:done` to match sources can never silently break an existing
 * category.
 *
 * Nothing else can collide: "Resume notifications" uses the separate
 * top-level `reactivate:cat` / `reactivate:src` callbacks, and an id can't
 * contain a `:` (lowercase snake_case only), so `cat:<id>`/`src:<id>` can
 * never equal those or the bare `done`.
 */
export const PICKER_DONE_ID = 'done';

/** Every id CreateCategoryDto rejects — see above. */
export const RESERVED_CATEGORY_IDS: readonly string[] = [PICKER_DONE_ID];

/** Every id CreateSourceDto rejects — see above. */
export const RESERVED_SOURCE_IDS: readonly string[] = [PICKER_DONE_ID];
