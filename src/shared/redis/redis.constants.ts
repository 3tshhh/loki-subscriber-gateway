export const REDIS_CLIENT = Symbol('REDIS_CLIENT');

export const REDIS_KEYS = {
  /** Set of every currently-active chat_id, checked by dispatch before sending. */
  activeUsers: 'users:active',
  /**
   * Per-job set of chat_ids already handled (sent, permanently failed, or
   * cancelled because they were no longer in users:active).
   * Guards against re-delivering to the same recipient when a notify:telegram
   * entry is retried after an XCLAIM. TTL'd so it doesn't accumulate forever.
   */
  deliveredForJob: (jobId: string) => `notif:delivered:${jobId}`,
};

/** How long a per-job delivered-set is kept before it's safe to expire. */
export const DELIVERED_SET_TTL_SECONDS = 7 * 24 * 60 * 60;

/**
 * How old a `pending` notifications row (its status_changed_at, i.e. its
 * insert time) may be for job-level resume to still re-hand it off.
 *
 * Re-handing off is only send-once-safe while notif:delivered:{jobId}
 * still exists: it's what makes dispatch skip a recipient an earlier
 * notify:telegram entry already sent to. Every claim (SADD) re-arms that
 * set's TTL to DELIVERED_SET_TTL_SECONDS, and every claim happens after the
 * job's rows were inserted, so a set that ever got a member can't expire
 * sooner than insert + DELIVERED_SET_TTL_SECONDS. Past that, an expired set
 * is indistinguishable from "never claimed", and resuming could resend.
 *
 * The one-hour margin under the TTL absorbs clock skew between this
 * process (which writes status_changed_at) and Redis (which runs the TTL),
 * plus the time between the resume decision and dispatch actually claiming
 * each recipient of the re-handed-off notify:telegram entry. A pending row
 * older than this is closed as `failed` / delivery_status_unknown instead
 * of being resent — see JobStreamService.handleJob.
 */
export const RESUME_WINDOW_SECONDS = DELIVERED_SET_TTL_SECONDS - 60 * 60;

export const REDIS_STREAMS = {
  jobsNew: 'jobs:notify',
  /** Holds a job's fields for exactly as long as it's actively being
   * matched against subscribers and handed off to notify:telegram — the
   * JobMoverService moves each entry here (and deletes it from jobsNew)
   * the instant it's picked up, so jobsNew never accumulates; JobStreamService
   * deletes it from here again once the handoff succeeds, so this only
   * ever holds what's genuinely in flight right now. */
  jobsDelivering: 'jobs:delivering',
  /** jobs:delivering entries that exceeded MAX_DELIVERY_ATTEMPTS (e.g. one
   * that keeps failing because Postgres is unreachable for it) land here
   * instead of being reclaimed every sweep forever. Each is also deleted
   * from jobsDelivering (StreamConsumer's deleteOnDeadLetter, only once the
   * copy here and the ack have succeeded), so jobsDelivering keeps holding only
   * genuinely in-flight work — this stream is the record of what gave up,
   * and an entry here can be replayed by re-XADDing its fields into
   * jobsDelivering once the underlying problem is fixed. */
  jobsDeliveringDead: 'jobs:delivering:dead',
  notifyTelegram: 'notify:telegram',
  /** Poison entries that exceeded MAX_DELIVERY_ATTEMPTS land here instead of
   * cycling through claim/fail/reclaim forever. */
  notifyTelegramDead: 'notify:telegram:dead',
};

export const CONSUMER_GROUPS = {
  /** JobMoverService, reading jobs:notify. */
  dispatch: 'dispatch-group',
  /** JobStreamService, reading jobs:delivering. */
  delivering: 'delivering-group',
  telegramSenders: 'telegram-senders',
};

/** An entry claimed and failed more than this many times is dead-lettered
 * instead of reclaimed again. */
export const MAX_DELIVERY_ATTEMPTS = 5;

/** Default cap on how much undelivered backlog a consumer group will
 * accept on startup before treating the excess as too stale to be worth
 * processing — see StreamConsumer's maxBacklogOnStartup option. Overridable
 * via the JOB_STREAM_BACKLOG_CAP env var. */
export const DEFAULT_STARTUP_BACKLOG_CAP = 250;
