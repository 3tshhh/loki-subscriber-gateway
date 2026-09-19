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
  CONSUMER_GROUPS,
  DEFAULT_STARTUP_BACKLOG_CAP,
  REDIS_CLIENT,
  REDIS_STREAMS,
  StreamConsumer,
  StreamEntry,
} from '../../shared/redis';

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
    const backlogCap = Number(
      this.config.get<string>('JOB_STREAM_BACKLOG_CAP') ??
        DEFAULT_STARTUP_BACKLOG_CAP,
    );

    this.consumer = new StreamConsumer(
      this.redis,
      REDIS_STREAMS.jobsNew,
      CONSUMER_GROUPS.dispatch,
      `mover-${process.pid}-${randomUUID()}`,
      (entry) => this.moveEntry(entry),
      { maxBacklogOnStartup: backlogCap },
    );
    await this.consumer.start();
  }

  async onModuleDestroy() {
    await this.consumer?.stop();
  }

  private async moveEntry(entry: StreamEntry): Promise<void> {
    await this.redis.xadd(
      REDIS_STREAMS.jobsDelivering,
      '*',
      ...Object.entries(entry.fields).flat(),
      '_originalId',
      entry.id,
    );
    await this.redis.xdel(REDIS_STREAMS.jobsNew, entry.id);
  }
}
