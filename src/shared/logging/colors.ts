/**
 * Minimal ANSI colorizer — no dependency needed for a handful of codes.
 */
const RESET = '\x1b[0m';
const BOLD = '\x1b[1m';
const CODE = {
  red: '\x1b[31m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  magenta: '\x1b[35m',
  cyan: '\x1b[36m',
  white: '\x1b[37m',
};

function paint(text: string, ...codes: string[]): string {
  return `${codes.join('')}${text}${RESET}`;
}

const METHOD_COLORS: Record<string, string> = {
  GET: CODE.blue,
  POST: CODE.green,
  PATCH: CODE.yellow,
  PUT: CODE.magenta,
  DELETE: CODE.red,
};

export function colorModule(moduleName: string): string {
  const color = moduleName === 'TELEGRAM-BOT' ? CODE.magenta : CODE.cyan;
  return paint(`[${moduleName}]`, BOLD, color);
}

export function colorMethod(method: string): string {
  return paint(method.padEnd(6), METHOD_COLORS[method] ?? CODE.white);
}

export function colorStatus(status: number): string {
  const color =
    status >= 500
      ? CODE.red
      : status >= 400
        ? CODE.yellow
        : status >= 300
          ? CODE.cyan
          : CODE.green;
  return paint(String(status), BOLD, color);
}

export function colorDuration(ms: number): string {
  const color = ms >= 500 ? CODE.red : ms >= 150 ? CODE.yellow : CODE.green;
  return paint(`${ms.toFixed(1)}ms`, color);
}

export function colorOutcome(ok: boolean): string {
  return paint(ok ? 'OK ' : 'ERR', BOLD, ok ? CODE.green : CODE.red);
}

/**
 * For end-to-end job-delivery latency specifically — deliberately more
 * lenient than colorDuration. That pipeline includes PQueue's intentional
 * ~25msg/sec throttling, so a later recipient in a large batch legitimately
 * takes several seconds without anything being wrong; colorDuration's
 * HTTP-oriented thresholds (red past 500ms) would flag that as alarming
 * when it's just the rate limiter doing its job.
 */
export function colorDeliveryDuration(ms: number): string {
  const color = ms >= 5000 ? CODE.red : ms >= 1000 ? CODE.yellow : CODE.green;
  const label =
    ms >= 1000 ? `${(ms / 1000).toFixed(2)}s` : `${ms.toFixed(0)}ms`;
  return paint(label, color);
}
