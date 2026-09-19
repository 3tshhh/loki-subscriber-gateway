# Subscriber Gateway

Matches incoming jobs to Telegram subscribers and delivers them, kept as two
NestJS modules in one process.

- **`src/core`** — the `jobs:notify` stream consumer that matches jobs to
  subscribers via a direct Postgres query (see "Subscriber matching"
  below), plus its own `subscriber`/`notifications` services and HTTP
  endpoints (currently unused — see "Architecture notes").
- **`src/telegram-bot`** — all Telegram API interaction: inbound commands
  (`/start`, `/stop`, `/categories`, `/sources`) and the `notify:telegram`
  stream consumer that sends the actual messages. Talks to Postgres
  **directly** (`bot-subscriber.service.ts`), not through Core's HTTP API —
  see "Architecture notes" for why and what that currently costs.
- **`src/shared`** — Postgres (TypeORM) and Redis (ioredis) infrastructure
  shared by both modules.

The only interface with the upstream Python job pipeline is the `jobs:notify`
Redis Stream — see [`docs/jobs-new-stream-contract.md`](docs/jobs-new-stream-contract.md)
for the exact entry format, required fields, and idempotency rules.

> **Subscriber matching no longer uses Redis at all.** It used to read
> `sub:{category}:{source}` / `sub:{category}` Redis sets, which had to be
> kept in sync with every Postgres write from two independent services
> (Core's and the bot's) — a real source of drift risk, and the actual
> cause of a "no subscribers matched" bug when the bot's write path fell
> out of sync with Redis. Replaced with one direct Postgres query in
> `job-stream.service.ts` (see "Subscriber matching" below) — there's
> nothing left to keep in sync, because there's only one source of truth.

`chat_id` is the user's identity everywhere — this bot only ever does 1:1
DMs, where Telegram's `chat_id` and `user_id` are the same value, so there's
no separate internal id or `telegram_user_id` column to keep in sync.

## Prerequisites

- Node.js 20+
- A running PostgreSQL instance
- A running Redis instance (6.2+, for `XAUTOCLAIM`)
- A Telegram bot token from [@BotFather](https://t.me/BotFather)

This repo does not ship a docker-compose file — point it at your own
Postgres/Redis via the environment variables below.

## Setup

```bash
npm install
cp .env.example .env   # then fill in DATABASE_*, REDIS_*, TELEGRAM_BOT_TOKEN
npm run migration:run
npm run start:dev
```

### Seeding categories/sources

There's no seed endpoint yet — insert rows directly:

```sql
INSERT INTO categories (name) VALUES ('backend'), ('frontend');
INSERT INTO sources (name) VALUES ('linkedin'), ('company-site');
```

Nothing to pre-seed in Redis — matching reads `user_categories`/`user_sources`
directly (see "Subscriber matching" below).

### Telegram bot mode

Long-polling only — no public endpoint, no TLS/ngrok setup, nothing to
expose to the internet. `TelegramBootstrapService` calls `bot.start()` on
boot; that's the entire setup. Webhook mode was deliberately removed: at
this bot's scale (single instance, no plan to horizontally scale), it
bought no real latency/scaling benefit but did add a live public POST
endpoint with no secret-token validation, plus ngrok/TLS operational
overhead in dev. Revisit only if this ever needs to scale across multiple
replicas behind a real public domain.

### Link shortener attribution

Set `SHORT_URL_DOMAIN` to the hostname of your link shortener (e.g.
`short.example.com`). A job's `link` gets `?userId=<chatId>` appended only
when its hostname matches that domain; any other link is sent untouched —
we never decorate a URL we don't own.

## Migrations

Schema changes go through TypeORM migrations (`synchronize` is off):

```bash
npm run migration:generate -- src/shared/database/migrations/SomeChange
npm run migration:run
npm run migration:revert
```

## HTTP endpoints (Core module)

Most of these still exist on `SubscriberController`/`NotificationsController`
(kept as-is, not deleted) but **aren't called by anything anymore** — the
telegram-bot module used to be their only caller and now talks to Postgres
directly instead (`bot-subscriber.service.ts`). The two `POST` category/
source endpoints are the exception — they're the actual, live way to add a
new category or source (see `docs/jobs-new-stream-contract.md`).

| Method | Path                            | Purpose                                        |
| ------ | -------------------------------- | ----------------------------------------------- |
| POST   | `/users/:chatId`                 | `ensureUser` — idempotent row creation only, never reactivates. *(unused — see above)* |
| POST   | `/users/:chatId/activate`        | Explicit activation *(unused)* |
| POST   | `/users/:chatId/bootstrap`       | ensure + optional activate + current selection + enabled lists, in one call *(unused)* |
| GET    | `/categories`                    | Enabled categories *(unused)* |
| POST   | `/categories`                    | **Add a new category** — `{ id, name }`, id must be `^[a-z0-9_]+$`, 409 if it already exists |
| GET    | `/sources`                       | Enabled sources *(unused)* |
| POST   | `/sources`                       | **Add a new source** — `{ id, name }`, same id rule/409 behavior |
| GET    | `/users/:chatId/subscriptions`   | Current category/source selection *(unused)* |
| POST   | `/users/:chatId/subscriptions`   | Replace selection in Postgres *(unused)* |
| POST   | `/users/:chatId/deactivate`      | Soft-deactivate + cancel pending notifications *(unused)* |
| PATCH  | `/notifications/:jobId/:chatId`  | Delivery status callback *(unused)* |

`src/telegram-bot/bot-subscriber.service.ts` is the module's real, current
data-access path — it re-implements the same operations (`ensureUser`,
`activateUser`, `deactivateUser`, `bootstrap`, `updateSubscriptions`,
`setNotificationStatus`) as direct TypeORM repository calls against the same
tables, registered via its own `TypeOrmModule.forFeature([...])` in
`telegram-bot.module.ts`. It is a second, independent data-access path onto
the same schema — not a call into Core's `SubscriberService` instance.

## Subscriber matching

A direct Postgres query in `job-stream.service.ts`'s `findSubscribers`, run
once per incoming job — no Redis involved:

```sql
SELECT DISTINCT u.chat_id
FROM users u
JOIN user_categories uc ON uc.chat_id = u.chat_id AND uc.category_id = $1
WHERE u.active = true
  AND (
    EXISTS (SELECT 1 FROM user_sources us WHERE us.chat_id = u.chat_id AND us.source_id = $2)
    OR NOT EXISTS (SELECT 1 FROM user_sources us WHERE us.chat_id = u.chat_id)
  )
```

`$1`/`$2` are the job's `category`/`source` ids. The `EXISTS` branch catches
users who picked that exact source; `NOT EXISTS` catches users with zero
`user_sources` rows at all — the "any source" wildcard. `active = true`
excludes deactivated users. Indexed by the `IndexUserCategoriesForMatching`
migration (`user_categories.category_id` — the table's only other index is
the composite PK `(chat_id, category_id)`, which doesn't help a
category-only filter).

## Redis structures

Redis is used for stream transport and a couple of delivery-time
mechanisms — **not** for subscriber matching:

| Key                          | Purpose                                                                 |
| ----------------------------- | ------------------------------------------------------------------------ |
| `users:active`                | Set of every currently-active chat_id; dispatch checks this before sending, closing the race where a user stops right after their job was queued |
| `notif:delivered:{jobId}`    | Per-job set of chat_ids already handled (sent or permanently failed); guards a retried `notify:telegram` entry from re-sending to someone it already reached. TTL'd (7 days) |
| `jobs:notify` / `notify:telegram` | Streams — see `src/shared/redis/redis.constants.ts` for exact names and consumer groups |

## Reliability notes

- `jobs:notify` and `notify:telegram` are both consumed via `XREADGROUP` +
  `XACK`, with a periodic `XAUTOCLAIM` sweep (see
  `src/shared/redis/stream-consumer.ts`) picking up entries left on a dead
  consumer's PEL. There's no custom retry logic on top — Redis's own
  pending-entries list is the retry mechanism.
- `notifications` inserts use `INSERT ... ON CONFLICT DO NOTHING`
  (`orIgnore()`), so reprocessing a `jobs:notify` entry after an `XCLAIM` never
  creates duplicate rows at the job level.
- Per-recipient idempotency is a separate concern from job-level dedup: a
  `notify:telegram` entry can list many chat_ids, and if one recipient's
  send fails transiently the *whole entry* gets retried via XCLAIM — without
  a guard, everyone else in that batch would get the message twice.
  `TelegramDispatchService.deliverOne` checks `notif:delivered:{jobId}`
  before sending and only adds to it after a successful send or a permanent
  (403) failure — never on a transient failure, so the stream-level retry
  still works for the recipient that actually needs it.
- A 429 from Telegram carries its own `retry_after`; `sendWithRetry` waits
  exactly that long and retries the send directly (capped at 3 attempts)
  instead of failing the whole batch and waiting for the next `XAUTOCLAIM`
  sweep.
- The consumer separates "the Telegram send failed" (safe to retry) from
  "the send succeeded but reporting status back to Core failed" (must
  **not** retry, or the recipient gets the same message twice).

## Bot conversation flow

Ported from the pre-split pipeline's `user_bot.py` — a staged wizard, not a
single combined picker (see `backup/*.combined-picker.ts.bak` for that
earlier version, and `backup/*.immediate-save-wizard.ts.bak` for the next
iteration this superseded).

- `/start`, `/categories`, and `/sources` each call `bot-subscriber.service.ts`'s
  `bootstrap()` once — ensures the row exists, optionally activates
  (`/start` only), and returns current selection + both enabled lists in a
  single call.
- **Toggling a category/source never hits Postgres.** The in-progress picks
  live in `WizardStateStore` (an in-memory `Map<chatId, state>` — see
  `commands/wizard-state.store.ts`), keyed by which screen opened it
  (`wizard` for `/start`'s categories→sources chain, `categories-only` /
  `sources-only` for the standalone commands). Only pressing "Done" at the
  end of that chain sends one combined `updateSubscriptions(chatId,
  categoryIds, sourceIds)` write, followed by a combined review message
  listing both categories and sources — the same for every entry point,
  even a standalone single-dimension edit.
- `/start` always explicitly activates (even if already active), then shows
  the **category** picker (one screen). Its "Done" button transitions to
  the **source** picker (a separate screen, not shown alongside categories)
  without saving yet. The source picker's own "Done" is what actually
  finalizes (the combined write + review above).
- Picking **zero sources means "any source"**: the source screen says so
  explicitly ("If you select none, you'll receive jobs from all sources"),
  and it's a real matching-semantics difference, not just UI wording — see
  "Subscriber matching" above (the `NOT EXISTS` branch).
- While inactive, the category/source pickers show a notice plus a
  "▶️ Resume notifications" button (`reactivate:cat` / `reactivate:src`
  callback data — a distinct top-level prefix so it can never collide with
  a real category/source id) so a stopped user can resume without retyping
  `/start`.
- **`/stop`**: soft-deactivates — `active=false`, `deactivated_at=now()`,
  removed from `users:active` — but `user_categories`/`user_sources` rows
  are kept (matching queries already exclude inactive users via
  `active = true`, so there's nothing else to clean up) so `/start` or
  "Resume" can restore visible state later. In the same Postgres
  transaction, any of that user's still-`pending` `notifications` rows are
  marked `cancelled`, matching `cancel_pending_user_notifications` — so a
  later resubscribe doesn't trigger a burst of stale jobs that piled up
  while they were unsubscribed. The same deactivation path (minus the
  bot-side reply) is used when Telegram reports the bot as blocked (403).
- **Retention sweep**: a daily cron (`UserRetentionService`, 3am) hard-deletes
  anyone deactivated for more than 30 days (`RETENTION_DAYS` in
  `subscriber.service.ts`). One `DELETE FROM users`, cascading to
  `user_categories`/`user_sources`/`notifications` via `ON DELETE CASCADE`.
  Nothing is left in Redis to clean up at that point.

## Architecture notes / open questions

- **Why telegram-bot talks to Postgres directly.** It used to call Core over
  `localhost` HTTP for every operation. Traced a real `/start` press through
  the code: 4 sequential HTTP round trips (`ensureUser` → `activateUser` →
  `getCategories` + `getSubscriptions`), each through Nest's full pipeline
  (routing, `ValidationPipe`, `RequestLoggerMiddleware`, controller,
  service), and 3 separate Postgres lookups for the *same* `chatId` inside
  those calls — measurable, user-visible latency on every bot interaction.
  `bot-subscriber.service.ts` now does the same operations as direct
  repository calls in-process. Core's `subscriber.service.ts` is untouched
  and still used by `job-stream.service.ts` for matching — this is two
  independent data-access paths onto the same tables, not one service
  calling the other.
- **Why matching moved off Redis entirely.** The two independent Postgres
  write paths (`subscriber.service.ts` and `bot-subscriber.service.ts`)
  each had to reconcile the same Redis subscriber-set state on every
  change — first duplicated, then extracted into a shared helper
  (`shared/redis/subscription-targets.ts`, since removed) when the bot's
  write path was found to have silently fallen out of sync with Redis. That
  fix still left two systems that had to agree (Postgres as source of
  truth, Redis as a derived cache) with a real, demonstrated way for them
  to drift. A direct Postgres query removes the second system outright —
  slower per-lookup than an O(1) Redis `SUNION` (an indexed join instead),
  but there's now exactly one place subscription state lives, and nothing
  can fall out of sync with it by construction.
- **Single process, two modules**: `CoreModule` and `TelegramBotModule` load
  into one `AppModule`. If this ever needs independent scaling or deploys,
  splitting them into a Nest monorepo (`apps/core`, `apps/telegram-bot`)
  would need the telegram-bot module's direct-Postgres approach reconsidered
  — it only works cleanly because both modules currently share one process
  and one Postgres connection.
- **No fixed/admin broadcast destination**: this repo only does
  subscriber-based routing, per the spec's explicit non-goal. A "send every
  job to one ops channel" path would be a separate feature.
- **At-least-once delivery**: a crash between a Telegram send succeeding and
  its status callback landing can leave a `notifications` row stuck at
  `pending` even though the message went out. This is accepted rather than
  building custom transactional-outbox logic on top of the streams, per the
  "don't build custom retry logic" instruction — flag if stronger exactly-once
  guarantees are needed.
- **Job messages are HTML-formatted with an inline button** — see
  `src/telegram-bot/dispatch/message-builder.ts` (ported from the pre-split
  pipeline's `message_builder.py`, channel/subscriber-style branch only).
  Link buttons are validated with `safeButtonUrl` before attaching; an
  unusable link falls back to plain text in the message body instead of
  failing the whole send.
