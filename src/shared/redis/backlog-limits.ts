import type { ConfigService } from '@nestjs/config';
import {
  DEFAULT_STARTUP_BACKLOG_CAP,
  DEFAULT_STARTUP_BACKLOG_MAX_AGE_MINUTES,
} from './redis.constants';

/** The startup backlog limits every pipeline stream applies — see
 * StreamConsumer's maxBacklogOnStartup / maxBacklogAgeMs options. */
export interface BacklogLimits {
  /** Newest N undelivered entries kept per stream. */
  cap: number;
  /** Undelivered entries older than this are dropped; undefined disables
   * the age check. */
  maxAgeMs?: number;
}

export function backlogLimitsFromConfig(config: ConfigService): BacklogLimits {
  const cap = Number(
    config.get<number>('JOB_STREAM_BACKLOG_CAP') ?? DEFAULT_STARTUP_BACKLOG_CAP,
  );
  const ageMinutes = Number(
    config.get<number>('JOB_STREAM_MAX_AGE_MINUTES') ??
      DEFAULT_STARTUP_BACKLOG_MAX_AGE_MINUTES,
  );
  return {
    cap,
    maxAgeMs: ageMinutes > 0 ? ageMinutes * 60_000 : undefined,
  };
}
