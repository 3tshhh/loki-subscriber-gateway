import { Injectable } from '@nestjs/common';
import { CategoryDto, SourceDto } from '../bot-subscriber.service';

export type WizardMode = 'wizard' | 'categories-only' | 'sources-only';

export interface WizardState {
  categoryIds: string[];
  sourceIds: string[];
  /** The enabled category/source lists themselves, captured once at
   * bootstrap — toggle/resume/finalize all reuse these instead of
   * re-querying Postgres on every tap. */
  categories: CategoryDto[];
  sources: SourceDto[];
  mode: WizardMode;
}

/**
 * In-progress category/source picks for a chat, held only while a picker is
 * open. Nothing is written to Core per toggle — only when the wizard
 * finishes does the accumulated state get sent as one combined save. An
 * in-memory Map is deliberate: this is a single-instance app, and losing an
 * in-progress (not yet saved) pick to a process restart is low-stakes —
 * worst case the user re-sends /start.
 */
@Injectable()
export class WizardStateStore {
  private readonly states = new Map<string, WizardState>();

  start(chatId: string, state: WizardState): void {
    this.states.set(chatId, state);
  }

  get(chatId: string): WizardState | undefined {
    return this.states.get(chatId);
  }

  clear(chatId: string): void {
    this.states.delete(chatId);
  }
}

/** Toggles `id` in place: removes it if present, appends it if absent. */
export function toggleId(ids: string[], id: string): void {
  const index = ids.indexOf(id);
  if (index === -1) {
    ids.push(id);
  } else {
    ids.splice(index, 1);
  }
}
