import { Injectable, NestMiddleware } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import type { NextFunction, Request, Response } from 'express';
import { colorDuration, colorMethod, colorModule, colorStatus } from './colors';

/**
 * One colorized line per request: which module handled it, method, path,
 * status, and response time. Disabled entirely when NODE_ENV=test so test
 * runs stay quiet.
 */
@Injectable()
export class RequestLoggerMiddleware implements NestMiddleware {
  private readonly enabled: boolean;

  constructor(config: ConfigService) {
    this.enabled = config.get<string>('NODE_ENV') !== 'test';
  }

  use(req: Request, res: Response, next: NextFunction): void {
    if (!this.enabled) {
      next();
      return;
    }

    const start = process.hrtime.bigint();
    res.on('finish', () => {
      const durationMs = Number(process.hrtime.bigint() - start) / 1_000_000;
      console.log(
        [
          colorModule('CORE'),
          colorMethod(req.method),
          req.originalUrl,
          colorStatus(res.statusCode),
          colorDuration(durationMs),
        ].join(' '),
      );
    });
    next();
  }
}
