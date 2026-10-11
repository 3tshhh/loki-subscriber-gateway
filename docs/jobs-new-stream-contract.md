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
  are entirely internal to this repo and not part of this contract. (For
  operators: a `jobs:delivering` entry that keeps failing is moved to
  `jobs:delivering:dead` after `MAX_DELIVERY_ATTEMPTS` (5) attempts, where it
  can be inspected and replayed — see the main README's "Reliability notes".)

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

> **Current state:** the Python producer does **not** yet send this shape —
> it writes the raw job record's fields flat onto the entry. This repo
> accepts that too (see "Raw producer shape (currently accepted)" below),
> but the shape documented here is the contract, and the producer should
> move to it.

Each `XADD` to `jobs:notify` must carry these fields (all flat strings, as
Redis Streams require — no nested types except inside `payload`, which is a
JSON-encoded string):

| Field      | Type   | Required | Meaning                                                                                                                                                                                                                                                                                                                                      |
| ---------- | ------ | -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `jobId`    | string | **yes**  | Unique identifier for this job. Must be identical across any redelivery/retry of the _same_ logical job (see Idempotency below) — reusing it for a _different_ job will cause that job to be silently dropped.                                                                                                                               |
| `category` | string | **yes**  | The job's category **id** (a stable slug, e.g. `backend`, `graphic_design` — see `POST /categories` below for the exact set), **not** the display name. Subscriber matching is done on this id directly. Must exactly match a `categories.id` row in Postgres (case-sensitive — ids are plain lowercase slugs, no normalization is applied). |
| `source`   | string | **yes**  | The job's source **id** (e.g. `linkedin`, `wuzzuf`), same rule as `category` — must exactly match a `sources.id` row.                                                                                                                                                                                                                        |
| `payload`  | string | **yes*** | A JSON-encoded object (see below). If omitted or empty, treated as `{}` — the message still sends, just with no title/link/description.                                                                                                                                                                                                      |

\* `payload` isn't hard-required the way `jobId`/`category`/`source` are —
an entry missing it doesn't get dropped — but omitting it produces an
essentially empty notification, so treat it as required in practice.

### `payload` JSON shape

Matches Python's actual job record field names (Title-Case, with spaces —
not camelCase). The full record, JSON-encoded into `payload` (today the
pipeline sends these same fields flat instead — see "Raw producer shape"
below):

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

| Key                         | Rendered?                    | Used for                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| --------------------------- | ---------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Title`                     | **yes**                      | Bold headline in the outbound message.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `Description`               | **yes**                      | Shown under a "📋 Description" section, truncated to ~700 characters.                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `Source`                    | **yes** — informational only | Not actually read from the payload; the message's "🏢 Platform" line uses the top-level `source` field instead, which carries the same value. Fine to keep sending it in the payload too.                                                                                                                                                                                                                                                                                                                              |
| `Short URL`                 | **yes**                      | The job URL. If its hostname matches this repo's `SHORT_URL_DOMAIN` env var, `?userId=<chatId>` is appended per recipient before sending (click attribution) — links on any other domain are sent untouched. If the (possibly decorated) URL parses as an absolute `http(s)` link with a plausible TLD, it becomes an inline "🔗 View Job" button; otherwise it's shown as plain text in the message body instead (never silently dropped, and never allowed to break the send — a malformed URL just isn't buttoned). |
| `Categories`                | **yes**                      | Array of **display names** (e.g. `["Backend Development"]`), shown as `#hashtags` under a "🏷 Tags" section, and its first entry is also what's shown in the message header ("New Backend Development Opportunity"). This is the only place a display name is used — matching itself is entirely on the top-level `category` id. Falls back to `[the top-level category id]` if empty/omitted (so the header shows a raw slug rather than nothing — always send this).                                                  |
| `Job UUID`                  | no                           | Not read. The top-level `jobId` field is what this repo actually keys off of.                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `URL`                       | no                           | Not read — `Short URL` is what's used for the link/button. Include it if useful for your own records; it's simply ignored here.                                                                                                                                                                                                                                                                                                                                                                                        |
| `Decision Reason`           | no                           | Not read — internal pipeline metadata, not user-facing.                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `Category ID`               | no                           | Not read from the payload — redundant with the top-level `category` field, which is what actually drives matching. Fine to keep sending both.                                                                                                                                                                                                                                                                                                                                                                          |
| `Category Selection Method` | no                           | Not read — internal pipeline metadata.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |

There is no `budget` field in this contract — the earlier draft of this doc
had one; it's been removed since Python's actual payload doesn't send it.

Any other keys beyond these 10 are also ignored by this repo — safe to
include, but they won't reach the Telegram message.

## Raw producer shape (currently accepted)

What the Python producer actually writes today is the raw job record's
fields directly as the stream entry's fields — no top-level
`jobId`/`category`/`source`, no JSON `payload`. Rather than drop every such
entry as malformed, `normalizeEntry` in
`src/core/job-stream/job-stream.service.ts` maps it onto the contract shape:

- If the entry already carries **all three** of `jobId`, `category`, and
  `source`, it's treated as the documented shape and passed through
  untouched (so fixing the producer needs no change here). Otherwise it's
  read as the raw shape, and any `payload` field on it is ignored.
- `jobId` ← `Job UUID`; `category` ← `Category ID` (used as-is, no
  normalization); `source` ← `Source`, **lowercased** (`LinkedIn` →
  `linkedin`). If any of these three is missing or empty, the entry is
  dropped as malformed (see "Dropped entries").
- The payload is rebuilt from the flat fields `Title`, `Description`,
  `Source`, `URL`, `Short URL`, `Decision Reason`, `Category ID` and
  `Category Selection Method` (each defaulting to `""`), with two
  differences from the documented payload:
  - `Categories` is a **comma-separated string** (e.g.
    `Backend Development, APIs`), split on commas and trimmed, empty
    entries dropped.
  - `Short URL` falls back to `URL` when it's missing or empty.
- Any other flat field is ignored.

Known limits of the raw shape, and why the producer should switch:

- `source` only matches if the lowercased display name _is_ the source id.
  That holds for the current seed list, but a source whose name and id
  differ (e.g. a multi-word name) will silently match nobody.
- Detecting the shape by field presence means a half-migrated producer
  (e.g. sending `jobId` but not `category`) falls into the raw path and is
  dropped unless it also still sends the raw fields.
- Comma-splitting `Categories` breaks any display name that itself
  contains a comma.

Recommended: have the producer send top-level `jobId` / `category` (the
slug, from `Category ID`) / `source` (the slug, not the display name) plus
the full record as a JSON-encoded `payload` with `Categories` as a real
array, exactly as documented above.

## Dropped entries

These are logged, **acknowledged (`XACK`'d), and discarded** — never
retried, since retrying can't fix them:

- `jobId`, `category`, or `source` missing (after the raw-shape mapping
  above). Always send all three.
- `payload` present but not a valid JSON **object** — unparseable JSON, or
  valid JSON that isn't an object (`null`, a number, an array). An omitted
  or empty `payload` is still fine (treated as `{}`).

## Idempotency / retry semantics

- This repo dedups **at the job level**, using its `notifications` table
  (one row per `jobId` + recipient, written before the job is handed off
  for sending):
  - No rows for `jobId` yet → a new job: subscribers are matched and
    notified.
  - Rows exist and none is still `pending` (all sent/failed/cancelled) →
    the job was fully processed; the entry is skipped (still acked).
  - Rows exist and some are still `pending` → an earlier attempt was
    interrupted between recording recipients and handing them off. The job
    is re-handed off to **exactly those still-pending recipients** — the
    original fan-out, not a fresh match, so subscribers who joined later
    are not added — but only those recorded less than
    `RESUME_WINDOW_SECONDS` ago (7 days minus a 1-hour safety margin).
    Older still-pending recipients are marked `failed` /
    `delivery_status_unknown` and **not** re-sent: whether an earlier
    attempt reached them can no longer be told safely (see below).
- This means: redelivering the exact same `jobId` (e.g. Python retries an
  `XADD` after a timeout, unsure whether the first one went through) is
  **safe** — it will not double-notify subscribers, even if it lands while
  the first delivery is still in progress: each recipient is atomically
  claimed in a per-job Redis set before sending, so at most one send per
  recipient goes out **while that set exists**. The set is kept for 7 days
  after its most recent claim, and resume only re-sends to recipients
  recorded within `RESUME_WINDOW_SECONDS` of now, so the guarantee holds
  for any redelivery inside that window — provided the re-handed-off batch
  is actually sent within the 1-hour margin. A redelivery after the window
  either finds every recipient resolved (skipped) or closes the stale ones
  as `delivery_status_unknown` rather than risking a duplicate.
- It also means: **`jobId` must never be reused for a genuinely different
  job** — the second one would be silently treated as a duplicate and
  never delivered to anyone.
- This repo's own consumer-side crash recovery (`XCLAIM`/`XAUTOCLAIM` on
  its `dispatch-group` pending-entries list) is fully internal and requires
  nothing from the Python side.

## `job:added:{jobId}` — informational only, not part of this contract

The original spec notes a `job:added:{jobId}` string key (`NX`+`EX` guard)
used by the Python side for its _own_ pre-`XADD` idempotency check, sharing
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
subscriber will ever match it (not an error, just silent no-op delivery). Current seed list (`src/shared/database/migrations/1789538982035-CategorySourceSlugIds.ts`):

**Categories**: `ai_ml`, `backend`, `data_analysis`, `frontend`, `full_stack`,
`game_dev`, `mobile_app`, `graphic_design`

**Sources**: `kafiil`, `freelancer`, `mostaql`, `nafezly`, `linkedin`, `wuzzuf`

New ones can be added without a migration via:

```
POST /categories   { "id": "devops", "name": "DevOps" }
POST /sources       { "id": "upwork", "name": "Upwork" }
```

`id` must be lowercase snake_case (`^[a-z0-9_]+$`). Matching compares the
stream's `category`/`source` against these ids by exact, case-sensitive
string equality in Postgres, with no normalization on either side — keeping
ids to one plain format means there's no case or whitespace variant for the
producer to get subtly wrong. Ids are capped at 60 characters, because they
also appear verbatim in the bot's button callback data (`cat:<id>` /
`src:<id>`), which Telegram limits to 64 bytes. `done` is reserved (400):
the sources picker's Done button is `src:done`, so a source with that id
would be indistinguishable from it (reserved for categories too, to keep
the two pickers symmetric). Returns 409 if the id already exists.
