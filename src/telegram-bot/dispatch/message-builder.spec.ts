import {
  buildJobMessage,
  JobMessageParams,
  MAX_DESCRIPTION_LENGTH,
  MAX_MESSAGE_LENGTH,
  safeButtonUrl,
  TELEGRAM_MESSAGE_LIMIT,
} from './message-builder';

describe('safeButtonUrl', () => {
  it.each(['https://example.com/jobs/1', 'http://s.example.io/abc?x=1'])(
    'accepts the absolute http(s) URL %s',
    (url) => {
      expect(safeButtonUrl(url)).toBe(url);
    },
  );

  it('returns the URL trimmed of surrounding whitespace', () => {
    expect(safeButtonUrl('  https://example.com/a  ')).toBe(
      'https://example.com/a',
    );
  });

  it.each([
    ['an empty string', ''],
    ['whitespace', '   '],
    ['undefined', undefined as unknown as string],
    ['a relative path', '/jobs/1'],
    ['text that is not a URL', 'not a url'],
    ['a non-http scheme', 'ftp://example.com/file'],
    ['a javascript: URL', 'javascript:alert(1)'],
    ['a truncated hyphenated TLD', 'https://www.example.co-'],
    ['a numeric last label', 'http://192.168.0.1/admin'],
    ['a one-letter TLD', 'https://example.c/x'],
  ])('rejects %s', (_label, url) => {
    expect(safeButtonUrl(url)).toBe('');
  });
});

describe('buildJobMessage', () => {
  const params = (
    overrides: Partial<JobMessageParams> = {},
  ): JobMessageParams => ({
    title: 'Build an app',
    description: 'Flutter work',
    sourceName: 'Upwork',
    categoryName: 'Mobile App Development',
    categories: ['Mobile', 'Cross Platform'],
    url: 'https://s.example.com/abc',
    hasButton: true,
    ...overrides,
  });

  it('renders the category header, title, platform, description and tags', () => {
    const message = buildJobMessage(params());

    expect(message).toContain('<b>New Mobile App Development Opportunity</b>');
    expect(message).toContain('<b>Build an app</b>');
    expect(message).toContain('<b>Platform</b>\nUpwork');
    expect(message).toContain('<b>Description</b>\n\nFlutter work');
    expect(message).toContain('#Mobile #CrossPlatform');
  });

  it('HTML-escapes every user-supplied field', () => {
    const message = buildJobMessage(
      params({
        title: '<script>alert("x")</script>',
        description: "Tom & Jerry's <b>deal</b>",
        sourceName: 'a<b',
        categoryName: 'R&D',
        categories: ['C<>'],
      }),
    );

    expect(message).not.toContain('<script>');
    expect(message).toContain(
      '&lt;script&gt;alert(&quot;x&quot;)&lt;/script&gt;',
    );
    expect(message).toContain('Tom &amp; Jerry&#x27;s &lt;b&gt;deal&lt;/b&gt;');
    expect(message).toContain('a&lt;b');
    expect(message).toContain('New R&amp;D Opportunity');
    expect(message).toContain('#C&lt;&gt;');
  });

  it('truncates a long description with an ellipsis', () => {
    const description = 'word '.repeat(400);
    const message = buildJobMessage(params({ description }));

    const rendered = message.split('<b>Description</b>\n\n')[1].split('\n')[0];
    expect(rendered.length).toBeLessThanOrEqual(MAX_DESCRIPTION_LENGTH);
    expect(rendered.endsWith('...')).toBe(true);
  });

  it('leaves a description at the limit untouched', () => {
    const description = 'x'.repeat(MAX_DESCRIPTION_LENGTH);
    expect(buildJobMessage(params({ description }))).toContain(description);
  });

  it('omits the description section when there is no description', () => {
    expect(buildJobMessage(params({ description: '' }))).not.toContain(
      'Description',
    );
  });

  it('falls back to the category name as the tag when there are no categories', () => {
    const message = buildJobMessage(params({ categories: [] }));
    expect(message).toContain('#MobileAppDevelopment');
  });

  it("falls back to 'Freelance' for both header and tag when the category name is blank", () => {
    const message = buildJobMessage(
      params({ categoryName: '  ', categories: undefined }),
    );
    expect(message).toContain('New Freelance Opportunity');
    expect(message).toContain('#Freelance');
  });

  it('leaves the link out of the body when it is on a button', () => {
    expect(buildJobMessage(params({ hasButton: true }))).not.toContain(
      'https://s.example.com/abc',
    );
  });

  it('shows the link as text when there is no usable button', () => {
    const message = buildJobMessage(
      params({ url: ' https://s.example.com/abc ', hasButton: false }),
    );
    expect(message).toContain('<b>Link</b>\nhttps://s.example.com/abc');
  });

  it("keeps an oversized message under Telegram's limit with balanced bold tags", () => {
    const message = buildJobMessage(
      params({ title: 'T'.repeat(MAX_MESSAGE_LENGTH + 500) }),
    );

    // Truncation targets MAX_MESSAGE_LENGTH approximately (closing tags are
    // appended after the cut); the safety margin keeps it under the limit.
    expect(message.length).toBeLessThan(TELEGRAM_MESSAGE_LIMIT);
    expect(message.length).toBeLessThanOrEqual(MAX_MESSAGE_LENGTH + 10);
    expect(message.endsWith('...')).toBe(true);
    const opens = (message.match(/<b>/g) ?? []).length;
    const closes = (message.match(/<\/b>/g) ?? []).length;
    expect(opens).toBe(closes);
  });
});
