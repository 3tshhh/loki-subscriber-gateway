import { InlineKeyboard } from 'grammy';
import { CategoryDto, SourceDto } from '../bot-subscriber.service';

/**
 * Ported from the pre-split bot's user_bot.py: a staged wizard (categories
 * first, "Done" transitions to sources, sources has its own "Done" that
 * finalizes with a plain summary) rather than one combined picker. Distinct
 * top-level "reactivate:" prefix so it can never collide with a real
 * category/source id used in the "cat:"/"src:" callback data.
 */
export const RESUME_CATEGORIES_CALLBACK = 'reactivate:cat';
export const RESUME_SOURCES_CALLBACK = 'reactivate:src';

const INACTIVE_NOTICE =
  "⏸ You're currently unsubscribed from notifications.\n" +
  "Your choices below are saved, but won't be delivered until you resume notifications.\n\n";

export function categoryKeyboard(
  categories: CategoryDto[],
  selectedCategoryIds: string[],
  inactive: boolean,
): InlineKeyboard {
  const selected = new Set(selectedCategoryIds);
  const keyboard = new InlineKeyboard();
  if (inactive) {
    keyboard.text('▶️ Resume notifications', RESUME_CATEGORIES_CALLBACK).row();
  }
  for (const category of categories) {
    const mark = selected.has(category.id) ? '✅' : '⬜';
    keyboard.text(`${mark} ${category.name}`, `cat:${category.id}`).row();
  }
  keyboard.text('Done', 'done');
  return keyboard;
}

export function categoryCaption(inactive: boolean): string {
  return (
    (inactive ? INACTIVE_NOTICE : '') +
    'Choose the job categories you want to receive.\n\n' +
    'You can select more than one.'
  );
}

export function sourceKeyboard(
  sources: SourceDto[],
  selectedSourceIds: string[],
  inactive: boolean,
): InlineKeyboard {
  const selected = new Set(selectedSourceIds);
  const keyboard = new InlineKeyboard();
  if (inactive) {
    keyboard.text('▶️ Resume notifications', RESUME_SOURCES_CALLBACK).row();
  }
  for (const source of sources) {
    const mark = selected.has(source.id) ? '✅' : '⬜';
    keyboard.text(`${mark} ${source.name}`, `src:${source.id}`).row();
  }
  keyboard.text('Done', 'src:done');
  return keyboard;
}

export function sourceCaption(inactive: boolean): string {
  return (
    (inactive ? INACTIVE_NOTICE : '') +
    'Choose the sources you want to receive.\n\n' +
    "If you select none, you'll receive jobs from all sources."
  );
}

/**
 * Plain-text summary shown once the wizard finishes — after the /start
 * wizard's sources screen, or after a standalone /categories-only or
 * /sources-only edit's Done (both dimensions are always shown, even though
 * only one changed, so the user sees their full current selection).
 */
export function reviewMessage(
  categories: CategoryDto[],
  selectedCategoryIds: string[],
  sources: SourceDto[],
  selectedSourceIds: string[],
): string {
  const categoryNames = categories
    .filter((c) => selectedCategoryIds.includes(c.id))
    .map((c) => c.name);
  const categoryLine =
    categoryNames.length > 0
      ? `📂 Categories:\n${categoryNames.map((name) => `• ${name}`).join('\n')}`
      : "📂 Categories: none selected — you won't receive any notifications until you pick at least one.";

  const sourceNames = sources
    .filter((s) => selectedSourceIds.includes(s.id))
    .map((s) => s.name);
  const sourceLine =
    sourceNames.length > 0
      ? `🔗 Sources:\n${sourceNames.map((name) => `• ${name}`).join('\n')}`
      : '🔗 Sources: all sources';

  return (
    `✅ Saved!\n\n${categoryLine}\n\n${sourceLine}\n\n` +
    "You'll receive notifications from now on based on these choices."
  );
}
