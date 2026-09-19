# Backup — pre-wizard bot logic

Snapshots taken before porting the bot's conversation flow and subscriber
matching semantics to match `user_bot.py` (staged category→source wizard,
"no sources selected = all sources", ensure_user/activate split, /stop
cancels pending notifications).

Not compiled — `.ts.bak` extension, outside `src/`, ignored by tsconfig/eslint.

- `subscriber.service.combined-picker.ts.bak` — the old cartesian-only
  matching model (every combo required an explicit category+source pick,
  no "all sources" wildcard) and the old combined findOrCreateUser
  (create-or-reactivate in one call, no ensure/activate split).
- `telegram-commands.service.combined-picker.ts.bak` — the old bot flow:
  categories and sources shown together in one message, immediate
  per-toggle save, no staged "Done" transition, no resume-from-inactive
  button.
- `selector.util.combined-picker.ts.bak` — the old single combined
  keyboard builder (mode: 'both' | 'categories' | 'sources').
- `job-stream.service.combined-picker.ts.bak` — the old job-arrival lookup
  (a single `SMEMBERS sub:{category}:{source}`, no wildcard union).
