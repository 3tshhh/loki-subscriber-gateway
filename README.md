# Subscriber Gateway

Matches incoming jobs to Telegram subscribers and delivers them, kept as two
NestJS modules in one process.

- **`src/core`** — the job pipeline's matching side (`JobMoverService` and
  `JobStreamService`, see "Pipeline" below), plus the `subscriber` and
  `notifications` services that own every Postgres write, and their HTTP
  endpoints (mostly unused now — see "HTTP endpoints").
- **`src/telegram-bot`** — all Telegram API interaction: inbound commands
  (`/start`, `/stop`, `/categories`, `/sources`) and the `notify:telegram`
  stream consumer that sends the actual messages. It has no data-access
  code of its own: it calls Core's `SubscriberService` and
  `NotificationsService` **in-process** (plain Nest DI, no HTTP hop) — see
  "Architecture notes" for how it got here.
- **`src/shared`** — Postgres (TypeORM) and Redis (ioredis) infrastructure,
  plus the generic `StreamConsumer` both modules build on.

The only interface with the upstream Python job pipeline is the `jobs:notify`
Redis Stream — see [`docs/jobs-new-stream-contract.md`](docs/jobs-new-stream-contract.md)
for the exact entry format, required fields, idempotency rules, and the
current mismatch between the documented shape and what the producer actually
sends.

> **Subscriber matching doesn't use Redis at all.** It used to read
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
- A running Redis instance (6.2+, for `XPENDING ... IDLE`)
- A Telegram bot token from [@BotFather](https://t.me/BotFather)

## Setup

```bash
npm install
cp .env.example .env   # then fill in DATABASE_*, REDIS_URL, TELEGRAM_BOT_TOKEN
npm run migration:run
npm run start:dev
```

Optional env: `JOB_STREAM_BACKLOG_CAP` (default 250, see "Reliability
notes"), `SHORT_URL_DOMAIN` (see below), `PORT` (default 3000), `NODE_ENV`
(`test` silences all logging).

### Docker

The repo ships a `Dockerfile`, a `docker-entrypoint.sh` and a
`docker-compose.yml`:

- **`Dockerfile`** — two-stage build on `node:20-alpine`: the build stage
  compiles TypeScript (migrations and `data-source` included); the
  production stage carries only prod dependencies, `dist/`, and
  `postgresql-client`.
- **`docker-entrypoint.sh`** — on every container start: creates the
  `DATABASE_NAME` database if it doesn't exist yet (Postgres never does that
  on its own, and TypeORM migrations only create tables inside an existing
  database), runs pending migrations against the compiled data-source (no
  ts-node needed at runtime), then `exec`s `node dist/main`. Both steps are
  idempotent, so a plain restart is a fast no-op before the app starts.
- **`docker-compose.yml`** — a single `app` service. It runs the prebuilt
  image `etshhh/subscriber-gateway:latest` (there's no `build:` section, so
  build and tag that image from the `Dockerfile` yourself if you're not
  pulling it), reads `.env`, and publishes `${PORT:-3000}`. It does **not**
  run Postgres or Redis: Postgres is expected in its own container with its
  port published to the host, so compose overrides `DATABASE_HOST` to
  `host.docker.internal` (mapped to `host-gateway` so it also works on native
  Linux Docker); Redis is reached directly via `REDIS_URL` from `.env`.

### Seeding categories/sources

The `CategorySourceSlugIds` migration seeds the current category and source
lists (slug ids like `backend`, `linkedin` — see
`docs/jobs-new-stream-contract.md` for the full list). New ones are added
through the HTTP API, no migration needed:

```
POST /categories   { "id": "devops", "name": "DevOps" }
POST /sources      { "id": "upwork", "name": "Upwork" }
```

Nothing to pre-seed in Redis — matching reads `user_categories`/`user_sources`
directly (see "Subscriber matching" below), and `users:active` is rebuilt
from Postgres on every startup.

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

## Testing

```bash
npm test
```

Runs the jest unit tests (`src/**/*.spec.ts`). Redis and Postgres are mocked,
so no running services are needed.

Line endings are LF everywhere, enforced by `.gitattributes`
(`* text=auto eol=lf`) — prettier's default `endOfLine: "lf"` and the `sh`
entrypoint inside the Docker image both expect it.

## Pipeline

```
jobs:notify ─► JobMoverService ─► jobs:delivering ─► JobStreamService ─► notify:telegram ─► TelegramDispatchService ─► Telegram
 (Python)      dispatch-group                        delivering-group                       telegram-senders
```

1. **`JobMoverService`** (`dispatch-group` on `jobs:notify`) does no business
   logic: it re-`XADD`s each entry into `jobs:delivering` (stamping the
   original entry id as `_originalId`), `XDEL`s it from `jobs:notify`, and
   acks. `jobs:notify` therefore never accumulates.
2. **`JobStreamService`** (`delivering-group` on `jobs:delivering`) does the
   real work: normalizes the entry (see the contract doc for the producer
   shape it accepts), matches subscribers in Postgres, inserts one
   `pending` `notifications` row per recipient, and hands the job off as a
   single `notify:telegram` entry carrying the recipient list. Only after
   that `XADD` succeeds is the entry `XDEL`ed from `jobs:delivering`, so
   that stream only ever holds what's genuinely in flight.
3. **`TelegramDispatchService`** (`telegram-senders` on `notify:telegram`)
   sends each recipient's message through a rate-limited queue (25 msg/s,
   under Telegram's ~30/s global cap) and records the outcome on the
   recipient's `notifications` row.

Splitting the mover from the matcher means a crash during the heavier
matching/handoff work is recovered from `jobs:delivering`'s own pending
entries list, independently of `jobs:notify`'s.

The job's arrival time — the original `jobs:notify` entry id's timestamp,
carried through as `_originalId` and then `enqueuedAt` — is the base for
every `response_time_ms` and every end-to-end latency in the logs.

## Subscriber matching

A direct Postgres query in `job-stream.service.ts`'s `findSubscribers`, run
once per new job — no Redis involved:

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
mechanisms — **not** for subscriber matching. Exact names live in
`src/shared/redis/redis.constants.ts`.

| Key                       | Purpose                                                                                                                                                           |
| ------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `jobs:notify`             | Inbound stream from the Python producer (`dispatch-group`)                                                                                                        |
| `jobs:delivering`         | Internal stream of jobs being matched/handed off (`delivering-group`)                                                                                             |
| `jobs:delivering:dead`    | Dead-letter stream for `jobs:delivering` entries that exceeded `MAX_DELIVERY_ATTEMPTS`                                                                            |
| `notify:telegram`         | Internal stream of per-job send batches (`telegram-senders`)                                                                                                      |
| `notify:telegram:dead`    | Dead-letter stream for `notify:telegram` entries that exceeded `MAX_DELIVERY_ATTEMPTS`                                                                            |
| `users:active`            | Set of every currently-active chat_id; dispatch checks it before sending, closing the race where a user stops right after their job was queued (see below)        |
| `notif:delivered:{jobId}` | Per-job set of chat_ids already claimed (sent, being sent, or permanently failed); guards retried/duplicated `notify:telegram` entries from re-sending. 7-day TTL |

### `users:active`

Postgres is the source of truth for who's active; `users:active` is a copy
written separately (activate: Postgres, then `SADD`; deactivate: `SREM`, then
Postgres). Since dispatch silently skips anyone missing from the set, drift
means silently missed deliveries, so:

- **Rebuilt from Postgres on every startup** (`SubscriberService.onModuleInit`
  → `rebuildActiveUsersSet`). The new set is built under a temporary key and
  swapped in with one `RENAME` (or `DEL` if nobody is active), so dispatch
  only ever sees the old set or the complete new one. It's deliberately an
  awaited `onModuleInit`, not `onApplicationBootstrap`: `TelegramBotModule`
  imports `SubscriberModule`, so its own `onModuleInit` — which starts bot
  polling and the `notify:telegram` consumer — runs only after the rebuild
  finishes, and the HTTP server isn't listening yet either. A rebuild failure
  is logged and leaves the existing set untouched; it never blocks startup.
- **`SADD` on every path that makes a user active**: `/start` and "Resume"
  (`activateUserUpsert`), and "Done" (`updateSubscriptions`) whenever the
  resulting row is active — which covers a brand-new chat that only ever used
  `/categories` or `/sources`.

Residual windows, accepted at this scale:

- A write from _another_ process (e.g. the previous instance still polling
  during an overlapping deploy) landing between the rebuild's `SELECT` and
  its `RENAME` is lost until the next restart.
- If the rebuild takes longer than 15 s, startup carries on and the rebuild
  finishes in the background — still atomic, but now concurrent with live
  traffic, so the same window applies.
- A crash between the Postgres write and the Redis write of an
  activate/deactivate leaves the two disagreeing until the next restart. On
  the deactivate side that means a user still active in Postgres but missing
  from the set: they keep getting matched, dispatch skips them, and their
  rows stay `pending`.

## Notifications table

One row per `(job_id, chat_id)`. Every column is always filled — the
`FillAllNotificationColumns` migration made `status_changed_at`, `reason`,
and `response_time_ms` `NOT NULL`, and the write paths are typed so a NULL
can't compile.

| `status`    | `status_changed_at`             | `reason`                                                                                                            | `response_time_ms`         |
| ----------- | ------------------------------- | ------------------------------------------------------------------------------------------------------------------- | -------------------------- |
| `pending`   | insert time                     | `awaiting_delivery`                                                                                                 | job arrival → insert       |
| `sent`      | when Telegram accepted the send | `delivered`                                                                                                         | job arrival → send         |
| `failed`    | when the failure was detected   | `bot_blocked` (Telegram 403) or `delivery_retries_exhausted` (`notify:telegram` entry dead-lettered)                | job arrival → failure      |
| `cancelled` | when the row was cancelled      | `user_unsubscribed` (`/stop`) or `bot_blocked` (the chat's other pending rows, cancelled when a 403 deactivates it) | job arrival → cancellation |

- **`sent_at` was renamed to `status_changed_at`** and now changes on every
  status transition, not only on `sent`. Anyone with external SQL, views, or
  dashboards reading `sent_at` must update them (for "when was it sent",
  use `status_changed_at WHERE status = 'sent'`).
- `failed`/`cancelled` rows written before the `reason` column existed were
  backfilled with `unknown_legacy` rather than a guessed cause; application
  code never writes it. Backfilled `response_time_ms` values are
  `status_changed_at − created_at` (the true arrival time was never stored),
  so treat legacy latencies as lower bounds.
- `response_time_ms` is a Postgres `INTEGER`, clamped to `[0, 2147483647]`.

## Reliability notes

- **Consumer groups + PEL as the retry mechanism.** Every stream is consumed
  via `XREADGROUP` + `XACK` (`src/shared/redis/stream-consumer.ts`). A
  pending-entries sweep runs once at startup and then every 30 s, reclaiming
  entries idle for 60 s+ via `XPENDING ... IDLE` + `XCLAIM` and running them
  through the same handler. A new group is created at offset `0`, never `$`,
  so a first-time creation never silently skips entries already in the
  stream. A foreign consumer group on any of these streams is logged as a
  warning — it would be receiving its own full copy of every entry.
- **Dead-lettering.** `jobs:delivering` and `notify:telegram` both cap
  retries at `MAX_DELIVERY_ATTEMPTS` (5). An entry delivered more often than
  that is copied to its dead-letter stream (`jobs:delivering:dead` /
  `notify:telegram:dead`, with `_originalId`, `_reason`, `_deadLetteredAt`)
  and acked, instead of cycling through claim/fail/reclaim forever. For
  `notify:telegram`, the entry's still-`pending` recipients are marked
  `failed` / `delivery_retries_exhausted`. For `jobs:delivering`, the entry
  is also `XDEL`ed from `jobs:delivering`, and any `pending` rows it already
  inserted are deliberately left `pending` — re-`XADD`ing the dead-lettered
  fields into `jobs:delivering` once the underlying problem is fixed resumes
  them through the normal retry path below. `jobs:notify` has no cap: its
  only work is a move, so it just retries.
- **Startup backlog cap.** If a group's undelivered backlog on
  `jobs:notify` or `jobs:delivering` exceeds `JOB_STREAM_BACKLOG_CAP`
  (default 250) at startup — e.g. after a long outage — the group's cursor is
  fast-forwarded and the oldest excess is deleted, so only the newest N are
  processed. Stale jobs flooding subscribers all at once is judged worse than
  losing them. This runs only once per start, before the read loop, and
  doesn't touch entries already on a PEL.
- **Malformed entries are dropped, not retried.** A `jobs:delivering` entry
  with no usable `jobId`/`category`/`source`, or whose `payload` isn't a JSON
  object, is logged and acked — retrying can't fix it, and throwing would
  only park it on the PEL until it's dead-lettered. Same for a
  `notify:telegram` entry without `jobId`/`chatIds`.
- **Job-level resume.** A crash can land between inserting a job's `pending`
  rows and its `notify:telegram` `XADD`. So when `JobStreamService` sees a
  job that already has rows, it doesn't simply skip it: if any rows are
  still `pending`, it re-hands off to **exactly those** recipients (the
  recorded original fan-out — deliberately not a fresh re-match, which could
  pull in subscribers who joined after the job arrived); only a job with no
  `pending` rows left is skipped as fully processed. Row inserts use
  `ON CONFLICT DO NOTHING`, so they never duplicate either.
- **Why a double `XADD` can't double-send.** Resume can hand off a job whose
  first `notify:telegram` entry did go out (crash after the `XADD` but before
  the ack, or the producer redelivering the same `jobId` while the first
  fan-out is still sending). `TelegramDispatchService.deliverOne` atomically
  `SADD`s the chat_id into `notif:delivered:{jobId}` **before** sending and
  skips it if it was already a member — `SADD`'s own return value is the
  check, so even two entries for the same job processed concurrently by
  different senders produce exactly one send per recipient. A transient send
  failure `SREM`s the claim and throws, leaving that entry unacked for
  `XCLAIM` to retry, so the recipient is never stranded. A permanent (403)
  failure keeps the claim, so a retry won't try again.
- A 429 from Telegram carries its own `retry_after`; `sendWithRetry` waits
  exactly that long and retries the send directly (3 attempts total) instead
  of failing the whole batch and waiting for the next sweep.
- Once a send has succeeded, nothing after it can cause a retry: a failure
  recording the `sent` status is logged and swallowed, because retrying
  would resend a message the recipient already got.

## HTTP endpoints (Core module)

Since the bot calls `SubscriberService`/`NotificationsService` in-process,
nothing in this repo calls these endpoints any more. They're kept as a thin
HTTP surface over the same service methods the bot uses (so they can't drift
from the bot's behavior). The two `POST` category/source endpoints are the
exception — they're the actual, live way to add a new category or source.

| Method | Path                            | Purpose                                                                                                                                                                |
| ------ | ------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| POST   | `/users/:chatId`                | `ensureUser` — creates the row (active) if missing, never reactivates an existing one _(unused)_                                                                       |
| POST   | `/users/:chatId/activate`       | Explicit activation, 404 if the user doesn't exist _(unused)_                                                                                                          |
| POST   | `/users/:chatId/bootstrap`      | `{ activate?, username? }` — optional activate-upsert, then current selection + enabled lists in one call _(unused)_                                                   |
| GET    | `/categories`                   | Enabled categories _(unused)_                                                                                                                                          |
| POST   | `/categories`                   | **Add a new category** — `{ id, name }`, id must be `^[a-z0-9_]+$`, 409 if it already exists                                                                           |
| GET    | `/sources`                      | Enabled sources _(unused)_                                                                                                                                             |
| POST   | `/sources`                      | **Add a new source** — `{ id, name }`, same id rule/409 behavior                                                                                                       |
| GET    | `/users/:chatId/subscriptions`  | Current category/source selection _(unused)_                                                                                                                           |
| POST   | `/users/:chatId/subscriptions`  | Replace selection — same `updateSubscriptions` as the bot's "Done" (creates a missing user as active) _(unused)_                                                       |
| POST   | `/users/:chatId/deactivate`     | Soft-deactivate + cancel pending notifications, 404 if the user doesn't exist _(unused)_                                                                               |
| PATCH  | `/notifications/:jobId/:chatId` | Delivery status: `{ status: 'sent' \| 'failed', reason?, responseTimeMs }` — `responseTimeMs` (int ≥ 0) is required for every status, `reason` for `failed` _(unused)_ |

## Users

- **`updateSubscriptions` creates brand-new chats as active, by design.**
  Finishing the picker ("Done") is treated as an opt-in as clear as
  `/start`, so a chat whose only interaction was `/categories` or `/sources`
  gets an active row (and joins `users:active`). An existing row's `active`
  flag is never touched by it, though: a stopped user editing their picks
  stays stopped until `/start` or "Resume".
- **`username` is COALESCEd.** `/start`, "Resume", and "Done" store the
  Telegram `@username`, but a caller with none (users without a handle, HTTP
  callers that omit it) never wipes a stored one. The tradeoff: a user who
  removes their handle keeps the stale one here until they set a new one —
  preferred over losing a known handle every time a caller doesn't pass it.

## Bot conversation flow

Ported from the pre-split pipeline's `user_bot.py` — a staged wizard, not a
single combined picker (see `backup/*.combined-picker.ts.bak` for that
earlier version, and `backup/*.immediate-save-wizard.ts.bak` for the next
iteration this superseded).

- **Reads before replies, writes after.** `/start`, `/categories`, and
  `/sources` each do exactly one read (`getBootstrapSnapshot` — one SELECT
  returning active state, current selection, and both enabled lists) and
  reply from it. `/start`'s activate-upsert and `/stop`'s deactivation run
  _after_ the reply, in the background; if either fails, a follow-up message
  asks the user to send the command again.
- **Toggling a category/source never hits Postgres.** The in-progress picks
  live in `WizardStateStore` (an in-memory `Map<chatId, state>` — see
  `commands/wizard-state.store.ts`), keyed by which screen opened it
  (`wizard` for `/start`'s categories→sources chain, `categories-only` /
  `sources-only` for the standalone commands). Only pressing "Done" at the
  end of that chain sends one combined `updateSubscriptions(chatId,
categoryIds, sourceIds, username)` write, followed by a combined review
  message listing both categories and sources — the same for every entry
  point, even a standalone single-dimension edit.
- **Wizard state is in-memory only.** A process restart while someone is
  mid-picking loses their unsaved picks; the next tap answers "Session
  expired, please send /start again". Accepted at this scale (single
  instance, nothing is lost that was ever saved) rather than persisting
  half-finished picker state.
- `/start` always activates (even if already active), then shows the
  **category** picker (one screen). Its "Done" button transitions to the
  **source** picker (a separate screen, not shown alongside categories)
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
- **`/stop`**: soft-deactivates — removed from `users:active`,
  `active=false`, `deactivated_at=now()` — but `user_categories`/
  `user_sources` rows are kept (matching already excludes inactive users) so
  `/start` or "Resume" can restore visible state later. In the same Postgres
  transaction, any of that user's still-`pending` `notifications` rows are
  marked `cancelled` / `user_unsubscribed`, matching
  `cancel_pending_user_notifications` — so a later resubscribe doesn't
  trigger a burst of stale jobs. `/stop` is safe for a chat that never had a
  users row. The same deactivation path (with reason `bot_blocked`) runs
  when Telegram reports the bot as blocked (403).
- **Retention sweep**: a daily cron (`UserRetentionService`, 3am) hard-deletes
  anyone deactivated for more than 30 days (`RETENTION_DAYS` in
  `subscriber.service.ts`). One `DELETE FROM users`, cascading to
  `user_categories`/`user_sources`/`notifications` via `ON DELETE CASCADE`.
  Nothing is left in Redis to clean up at that point.

## Architecture notes / open questions

- **How the bot reaches Postgres, and why.** Originally the bot called Core
  over `localhost` HTTP for every operation — a traced `/start` press was 4
  sequential HTTP round trips through Nest's full pipeline and 3 separate
  Postgres lookups for the same `chatId`. The first fix was a bot-side
  duplicate data-access service doing the same operations as direct
  repository calls; that removed the latency but left two independent
  implementations of every write against the same tables, which is exactly
  the kind of drift that broke Redis-based matching. The current shape keeps
  the in-process speed and drops the duplicate: `TelegramBotModule` imports
  `SubscriberModule`/`NotificationsModule` and calls Core's services
  directly, so there's one implementation per operation, shared by the bot
  and the HTTP endpoints. The bot's hot paths were also tightened along the
  way (one-statement activate upsert, one-query bootstrap snapshot).
- **Why matching moved off Redis entirely.** Postgres as source of truth plus
  Redis subscriber sets as a derived cache had a real, demonstrated way to
  drift. A direct Postgres query removes the second system outright — slower
  per-lookup than an O(1) Redis `SUNION` (an indexed join instead), but
  there's now exactly one place subscription state lives. `users:active` is
  the one remaining derived copy, which is why it's rebuilt on startup.
- **Single process, two modules**: `CoreModule` and `TelegramBotModule` load
  into one `AppModule`, and the bot depends on Core's services by direct
  import. If this ever needs independent scaling or deploys, splitting them
  into a Nest monorepo (`apps/core`, `apps/telegram-bot`) would mean turning
  that import back into a network boundary (the HTTP endpoints above are the
  starting point). The `users:active` startup rebuild and the in-memory
  wizard state also both assume a single instance.
- **No fixed/admin broadcast destination**: this repo only does
  subscriber-based routing, per the spec's explicit non-goal. A "send every
  job to one ops channel" path would be a separate feature.
- **At-least-once delivery, with gaps in the status record**: a crash
  between a Telegram send succeeding and its `sent` status write can leave a
  row `pending` even though the message went out (the delivered-set claim
  prevents a resend, so it stays `pending`). Same for a recipient skipped
  because they're missing from `users:active`. This is accepted rather than
  building transactional-outbox logic on top of the streams — flag if
  stronger guarantees are needed.
- **Job messages are HTML-formatted with an inline button** — see
  `src/telegram-bot/dispatch/message-builder.ts` (ported from the pre-split
  pipeline's `message_builder.py`, channel/subscriber-style branch only).
  Link buttons are validated with `safeButtonUrl` before attaching; an
  unusable link falls back to plain text in the message body instead of
  failing the whole send.
