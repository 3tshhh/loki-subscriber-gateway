import { Logger } from '@nestjs/common';
import { formatChatIds, streamRef, trace } from './pipeline-trace';

describe('pipeline-trace', () => {
  beforeAll(() => {
    Logger.overrideLogger(false);
  });

  it('writes stage, event and key=value fields, skipping empty ones', () => {
    const log = jest.spyOn(Logger.prototype, 'log');
    try {
      trace('MATCH', 'matched', {
        job: 'abc',
        recipients: 2,
        category: undefined,
        source: '',
        note: null,
      });
      expect(log).toHaveBeenCalledWith(
        'MATCH    matched    job=abc recipients=2',
      );
    } finally {
      log.mockRestore();
    }
  });

  it('quotes values containing spaces, quotes or =', () => {
    const warn = jest.spyOn(Logger.prototype, 'warn');
    try {
      trace('MATCH', 'dropped', { reason: 'bad payload' }, 'warn');
      expect(warn).toHaveBeenCalledWith(
        'MATCH    dropped    reason="bad payload"',
      );
    } finally {
      warn.mockRestore();
    }
  });

  it('truncates long recipient lists with a count', () => {
    const ids = Array.from({ length: 25 }, (_, i) => String(i));
    expect(formatChatIds(ids)).toBe(`${ids.slice(0, 20).join(',')},…(+5)`);
    expect(formatChatIds(['1', '2'])).toBe('1,2');
  });

  it('writes stream positions as stream#id', () => {
    expect(streamRef('jobs:notify', '1-0')).toBe('jobs:notify#1-0');
  });
});
