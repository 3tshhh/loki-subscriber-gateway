import { NestFactory } from '@nestjs/core';
import { ValidationPipe } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { AppModule } from './app.module';

async function bootstrap() {
  // NODE_ENV=test silences Nest's own Logger too (every this.logger.* call
  // across the app), not just the per-request logger in RequestLoggerMiddleware.
  const isTest = process.env.NODE_ENV === 'test';
  const app = await NestFactory.create(AppModule, {
    logger: isTest ? false : ['log', 'error', 'warn', 'debug', 'verbose'],
  });
  app.useGlobalPipes(new ValidationPipe({ whitelist: true, transform: true }));
  // Without this, killing the process (e.g. nest start --watch restarting on
  // a file change) never runs onModuleDestroy, so the bot's long-poll
  // connection to Telegram is never released — the next process to start
  // then collides with it (409: terminated by other getUpdates request).
  app.enableShutdownHooks();
  const config = app.get(ConfigService);
  await app.listen(config.get<number>('PORT') ?? 3000);
}
void bootstrap();
