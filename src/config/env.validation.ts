import { plainToInstance } from 'class-transformer';
import {
  IsIn,
  IsInt,
  IsOptional,
  IsString,
  Max,
  Min,
  validateSync,
} from 'class-validator';

class EnvironmentVariables {
  /** Set to 'test' to silence all console logging (Nest's Logger and the
   * per-request logger below) — everything else (development/production)
   * logs normally. */
  @IsIn(['development', 'production', 'test'])
  @IsOptional()
  NODE_ENV: 'development' | 'production' | 'test' = 'development';

  @IsInt()
  @Min(1)
  @Max(65535)
  PORT: number = 3000;

  @IsString()
  DATABASE_HOST: string;

  @IsInt()
  DATABASE_PORT: number = 5432;

  @IsString()
  DATABASE_USER: string;

  @IsString()
  DATABASE_PASSWORD: string;

  @IsString()
  DATABASE_NAME: string;

  @IsString()
  REDIS_URL: string;

  /** Caps how much undelivered backlog jobs:notify/jobs:delivering will
   * accept on startup before treating the excess as too stale to process —
   * see StreamConsumer's maxBacklogOnStartup option. */
  @IsInt()
  @IsOptional()
  JOB_STREAM_BACKLOG_CAP: number = 250;

  @IsString()
  TELEGRAM_BOT_TOKEN: string;

  /**
   * Hostname of the link shortener whose URLs are safe to decorate with
   * ?userId=. A link on any other domain is sent untouched.
   */
  @IsString()
  @IsOptional()
  SHORT_URL_DOMAIN?: string;
}

export function validate(config: Record<string, unknown>) {
  const validatedConfig = plainToInstance(EnvironmentVariables, config, {
    enableImplicitConversion: true,
  });
  const errors = validateSync(validatedConfig, {
    skipMissingProperties: false,
  });

  if (errors.length > 0) {
    throw new Error(
      `Config validation error:\n${errors
        .map((e) => Object.values(e.constraints ?? {}).join(', '))
        .join('\n')}`,
    );
  }

  return validatedConfig;
}
