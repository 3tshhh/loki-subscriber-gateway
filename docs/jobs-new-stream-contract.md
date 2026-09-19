# `jobs:notify` Redis Stream Contract

The only interface between the Python job pipeline (producer) and this
repo (consumer). Everything downstream of this stream — `notify:telegram`,
Postgres, the Telegram bot itself — is internal to this repo and is not
part of this contract.

- **Stream key**: `jobs:notify`
- **Producer**: the Python pipeline (external to this repo)
- **Consumer**: `JobMoverService` (`src/core/job-stream/job-mover.service.ts`),
  via a Redis Streams consumer group named `dispatch-group`. This stage does
  no business logic — it immediately re-`XADD`s each entry into an internal
  `jobs:delivering` stream and deletes it from `jobs:notify`, so this stream
  never accumulates. The actual subscriber-matching and Telegram handoff
  happen downstream, in `JobStreamService` reading `jobs:delivering` — both
  are entirely internal to this repo and not part of this contract.

The consumer group and the stream itself are created automatically on this
repo's side (`XGROUP CREATE jobs:notify dispatch-group 0 MKSTREAM`, idempotent
— ignores `BUSYGROUP` if it already exists). **The Python side does not need
to create the stream or the group** — a plain `XADD jobs:notify * ...` against
a stream that doesn't exist yet will create it, and this repo's consumer
will pick it up on its next poll regardless of which side got there first.

On startup, if either internal stage (`jobs:notify` or `jobs:delivering`)
finds more than `JOB_STREAM_BACKLOG_CAP` (default 250) entries undelivered to
its group — e.g. after an unusually long outage — the oldest excess is
dropped (deleted, never processed) and only the newest `JOB_STREAM_BACKLOG_CAP`
are handled, so an extreme backlog can't flood subscribers with stale
notifications all at once. Doesn't affect the Python side at all — it's
purely how this repo's own consumers treat their own backlog.

## Entry format

Each `XADD` to `jobs:notify` must carry these fields (all flat strings, as
Redis Streams require — no nested types except inside `payload`, which is a
JSON-encoded string):

| Field      | Type   | Required | Meaning |
| ---------- | ------ | -------- | ------- |
| `jobId`    | string | **yes**  | Unique identifier for this job. Must be identical across any redelivery/retry of the *same* logical job (see Idempotency below) — reusing it for a *different* job will cause that job to be silently dropped. |
| `category` | string | **yes**  | The job's category **id** (a stable slug, e.g. `backend`, `graphic_design` — see `POST /categories` below for the exact set), **not** the display name. Subscriber matching is done on this id directly. Must exactly match a `categories.id` row in Postgres (case-sensitive — ids are plain lowercase slugs, no normalization is applied). |
| `source`   | string | **yes**  | The job's source **id** (e.g. `linkedin`, `wuzzuf`), same rule as `category` — must exactly match a `sources.id` row. |
| `payload`  | string | **yes*** | A JSON-encoded object (see below). If omitted or empty, treated as `{}` — the message still sends, just with no title/link/description. |

\* `payload` isn't hard-required the way `jobId`/`category`/`source` are —
an entry missing it doesn't get dropped — but omitting it produces an
essentially empty notification, so treat it as required in practice.

### `payload` JSON shape

Matches Python's actual job record field names (Title-Case, with spaces —
not camelCase). Full record as sent by the pipeline:

```json
{
  "Job UUID": "3f2a1c...",
  "Title": "Senior Backend Engineer (Node.js/NestJS)",
  "Description": "We are looking for an experienced backend engineer...",
  "Source": "LinkedIn",
  "URL": "https://linkedin.com/jobs/view/12345",
  "Short URL": "https://short.example.com/abc123",
  "Decision Reason": "Matches backend + remote criteria",
  "Categories": ["Backend Development"],
  "Category ID": "backend",
  "Category Selection Method": "keyword-match"
}
```

Only 5 of these 10 fields reach the rendered Telegram message — the rest
are accepted (harmless to send) but intentionally not surfaced to users:

| Key                          | Rendered? | Used for |
| ----------------------------- | --------- | -------- |
| `Title`                       | **yes**   | Bold headline in the outbound message. |
| `Description`                 | **yes**   | Shown under a "📋 Description" section, truncated to ~700 characters. |
| `Source`                      | **yes** — informational only | Not actually read from the payload; the message's "🏢 Platform" line uses the top-level `source` field instead, which carries the same value. Fine to keep sending it in the payload too. |
| `Short URL`                   | **yes**   | The job URL. If its hostname matches this repo's `SHORT_URL_DOMAIN` env var, `?userId=<chatId>` is appended per recipient before sending (click attribution) — links on any other domain are sent untouched. If the (possibly decorated) URL parses as an absolute `http(s)` link with a plausible TLD, it becomes an inline "🔗 View Job" button; otherwise it's shown as plain text in the message body instead (never silently dropped, and never allowed to break the send — a malformed URL just isn't buttoned). |
| `Categories`                  | **yes**   | Array of **display names** (e.g. `["Backend Development"]`), shown as `#hashtags` under a "🏷 Tags" section, and its first entry is also what's shown in the message header ("New Backend Development Opportunity"). This is the only place a display name is used — matching itself is entirely on the top-level `category` id. Falls back to `[the top-level category id]` if empty/omitted (so the header shows a raw slug rather than nothing — always send this). |
| `Job UUID`                    | no        | Not read. The top-level `jobId` field is what this repo actually keys off of. |
| `URL`                         | no        | Not read — `Short URL` is what's used for the link/button. Include it if useful for your own records; it's simply ignored here. |
| `Decision Reason`              | no        | Not read — internal pipeline metadata, not user-facing. |
| `Category ID`                 | no        | Not read from the payload — redundant with the top-level `category` field, which is what actually drives matching. Fine to keep sending both. |
| `Category Selection Method`   | no        | Not read — internal pipeline metadata. |

There is no `budget` field in this contract — the earlier draft of this doc
had one; it's been removed since Python's actual payload doesn't send it.

Any other keys beyond these 10 are also ignored by this repo — safe to
include, but they won't reach the Telegram message.

## Dropped entries

If `jobId`, `category`, or `source` is missing, the entire entry is
logged as malformed, **acknowledged (`XACK`'d), and discarded** — it will
not be retried. Always send all three.

## Idempotency / retry semantics

- This repo dedups **at the job level**: before matching subscribers, it
  checks whether *any* row already exists in its `notifications` table for
  `jobId`. If one does, the entry is treated as already processed and
  skipped (still acked).
- This means: redelivering the exact same `jobId` (e.g. Python retries an
  `XADD` after a timeout, unsure whether the first one went through) is
  **safe** — it will not double-notify subscribers.
- It also means: **`jobId` must never be reused for a genuinely different
  job** — the second one would be silently treated as a duplicate and
  never delivered to anyone.
- This repo's own consumer-side crash recovery (`XCLAIM`/`XAUTOCLAIM` on
  its `dispatch-group` pending-entries list) is fully internal and requires
  nothing from the Python side.

## `job:added:{jobId}` — informational only, not part of this contract

The original spec notes a `job:added:{jobId}` string key (`NX`+`EX` guard)
used by the Python side for its *own* pre-`XADD` idempotency check, sharing
the same Redis instance. **This repo never reads or writes that key** — it's
listed here only so it isn't mistaken for something this repo depends on.

## No subscriber-matching details leak into this contract

Subscriber matching is a direct Postgres query internal to this repo (see
`job-stream.service.ts`'s `findSubscribers` and the main README's
"Subscriber matching" section) — it used to go through Redis subscriber
sets, but that's gone now. The Python side only ever needs to know
`category`/`source` as plain id strings; it never touches Postgres or Redis
for this at all.

## Valid category/source ids

`category`/`source` on `jobs:notify` must be an id that actually exists in
Postgres's `categories`/`sources` tables — an unrecognized id just means no
subscriber sets will ever match it (not an error, just silent no-op
delivery). Current seed list (`src/shared/database/migrations/1789538982035-CategorySourceSlugIds.ts`):

**Categories**: `ai_ml`, `backend`, `data_analysis`, `frontend`, `full_stack`,
`game_dev`, `mobile_app`, `graphic_design`

**Sources**: `kafiil`, `freelancer`, `mostaql`, `nafezly`, `linkedin`, `wuzzuf`

New ones can be added without a migration via:

```
POST /categories   { "id": "devops", "name": "DevOps" }
POST /sources       { "id": "upwork", "name": "Upwork" }
```

`id` must be lowercase snake_case (`^[a-z0-9_]+$`) since it's used directly
in Redis keys. Returns 409 if the id already exists.
