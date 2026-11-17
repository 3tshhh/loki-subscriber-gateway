import { randomUUID } from 'crypto';
import {
  Inject,
  Injectable,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import type Redis from 'ioredis';
import {
  backlogLimitsFromConfig,
  CONSUMER_GROUPS,
  countUndelivered,
  REDIS_CLIENT,
  REDIS_STREAMS,
  StreamConsumer,
  StreamEntry,
  streamEntryTimestampMs,
} from '../../shared/redis';
import { streamRef, trace } from '../../shared/logging/pipeline-trace';

/**
 * Stage 1 of the jobs:notify pipeline: moves each entry into jobs:delivering
 * and deletes it from jobs:notify, then acks — so jobs:notify never
 * accumulates (entries live there only until picked up) and jobs:delivering
 * only ever holds what's genuinely in flight right now. The actual
 * business logic (subscriber matching, notify:telegram handoff) lives in
 * JobStreamService, reading jobs:delivering — kept separate so a crash
 * during that heavier work is recovered via jobs:delivering's own PEL,
 * independently of this stage's.
 */
@Injectable()
export class JobMoverService implements OnModuleInit, OnModuleDestroy {
  private consumer: StreamConsumer;

  constructor(
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
    private readonly config: ConfigService,
  ) {}

  async onModuleInit() {
    const { cap, maxAgeMs } = backlogLimitsFromConfig(this.config);

    this.consumer = new StreamConsumer(
      this.redis,
      REDIS_STREAMS.jobsNew,
      CONSUMER_GROUPS.dispatch,
      `mover-${process.pid}-${randomUUID()}`,
      (entry) => this.moveEntry(entry),
      {
        maxBacklogAgeMs: maxAgeMs,
        // One budget for both stages, not one each: whatever is already
        // waiting in jobs:delivering (from before the restart) counts
        // against the cap, and jobs:notify only keeps the remainder. Every
        // entry moved from here lands in jobs:delivering, whose own startup
        // check holds it to `cap` too — so whichever order the two checks
        // run in, at most `cap` jobs go on to be matched. Entries there
        // already past the age limit aren't counted: that stage's own age
        // check is about to drop them.
        maxBacklogOnStartup: async () => {
          const waiting = await countUndelivered(
            this.redis,
            REDIS_STREAMS.jobsDelivering,
            CONSUMER_GROUPS.delivering,
            cap,
            maxAgeMs === undefined ? undefined : Date.now() - maxAgeMs,
          ).catch(() => 0);
          return Math.max(0, cap - waiting);
        },
      },
    );
    await this.consumer.start();
  }

  async onModuleDestroy() {
    await this.consumer?.stop();
  }

  private async moveEntry(entry: StreamEntry): Promise<void> {
    const newId = await this.redis.xadd(
      REDIS_STREAMS.jobsDelivering,
      '*',
      ...Object.entries(entry.fields).flat(),
      '_originalId',
      entry.id,
    );
    await this.redis.xdel(REDIS_STREAMS.jobsNew, entry.id);

    trace('MOVER', 'moved', {
      // Raw producer shape carries 'Job UUID' instead (see
      // JobStreamService.normalizeEntry).
      job: entry.fields.jobId ?? entry.fields['Job UUID'] ?? '?',
      from: streamRef(REDIS_STREAMS.jobsNew, entry.id),
      to: streamRef(REDIS_STREAMS.jobsDelivering, String(newId)),
      age: `${Date.now() - streamEntryTimestampMs(entry.id)}ms`,
    });
  }
}
