import { Logger } from '@nestjs/common';

/**
 * One greppable line per pipeline step, so a job can be followed end to end
 * and every hand-off between streams is visible:
 *
 *   [Pipeline] MOVER     moved      job=abc from=jobs:notify#1-0 to=jobs:delivering#2-0
 *   [Pipeline] MATCH     matched    job=abc category=backend source=linkedin recipients=3 chats=111,222,333
 *   [Pipeline] MATCH     handed-off job=abc from=jobs:delivering#2-0 to=notify:telegram#3-0 recipients=3
 *   [Pipeline] DISPATCH  batch      job=abc entry=notify:telegram#3-0 recipients=3
 *   [Pipeline] DISPATCH  done       job=abc sent=2 skipped=0 inactive=1 blocked=0 failed=0
 *
 * `grep "job=abc"` gives one job's whole journey; `grep "DISPATCH  done"`
 * gives how many were sent per job. Goes through Nest's Logger, so
 * NODE_ENV=test silences it like every other log line.
 */
export type PipelineStage = 'STARTUP' | 'MOVER' | 'MATCH' | 'DISPATCH' | 'PEL';

type TraceValue = string | number | boolean | undefined | null;

const logger = new Logger('Pipeline');

/** Recipient lists are logged in full up to this many ids, then truncated
 * with a count — a job matching thousands of users shouldn't print a
 * multi-kilobyte line. Each recipient's own outcome is logged by dispatch
 * anyway. */
const MAX_LOGGED_CHAT_IDS = 20;

function formatValue(value: TraceValue): string {
  const text = String(value);
  return /[\s"=]/.test(text) ? JSON.stringify(text) : text;
}

export function formatChatIds(chatIds: readonly string[]): string {
  if (chatIds.length <= MAX_LOGGED_CHAT_IDS) return chatIds.join(',');
  return `${chatIds.slice(0, MAX_LOGGED_CHAT_IDS).join(',')},…(+${chatIds.length - MAX_LOGGED_CHAT_IDS})`;
}

/** `stream#id` — how every stream position is written in the trace. */
export function streamRef(stream: string, id: string): string {
  return `${stream}#${id}`;
}

export function trace(
  stage: PipelineStage,
  event: string,
  fields: Record<string, TraceValue>,
  level: 'log' | 'warn' | 'error' | 'debug' = 'log',
): void {
  const parts = [stage.padEnd(8), event.padEnd(10)];
  for (const [key, value] of Object.entries(fields)) {
    if (value === undefined || value === null || value === '') continue;
    parts.push(`${key}=${formatValue(value)}`);
  }
  logger[level](parts.join(' '));
}
