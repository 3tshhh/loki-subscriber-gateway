export const REDIS_CLIENT = Symbol('REDIS_CLIENT');

export const REDIS_KEYS = {
  jobAdded: (jobId: string) => `job:added:${jobId}`,
  /** Set of every currently-active chat_id, checked by dispatch before sending. */
  activeUsers: 'users:active',
  /**
   * Per-job set of chat_ids already handled (sent, or permanently failed).
   * Guards against re-delivering to the same recipient when a notify:telegram
   * entry is retried after an XCLAIM. TTL'd so it doesn't accumulate forever.
   */
  deliveredForJob: (jobId: string) => `notif:delivered:${jobId}`,
};

/** How long a per-job delivered-set is kept before it's safe to expire. */
export const DELIVERED_SET_TTL_SECONDS = 7 * 24 * 60 * 60;

export const REDIS_STREAMS = {
  jobsNew: 'jobs:notify',
  /** Holds a job's fields for exactly as long as it's actively being
   * matched against subscribers and handed off to notify:telegram — the
   * JobMoverService moves each entry here (and deletes it from jobsNew)
   * the instant it's picked up, so jobsNew never accumulates; JobStreamService
   * deletes it from here again once the handoff succeeds, so this only
   * ever holds what's genuinely in flight right now. */
  jobsDelivering: 'jobs:delivering',
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
