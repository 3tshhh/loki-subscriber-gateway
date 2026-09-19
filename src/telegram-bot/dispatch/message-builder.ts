/**
 * Ported from the pre-split pipeline's message_builder.py (channel_style
 * branch only — this repo does subscriber-based routing exclusively, no
 * direct-match/AI-recommendation path). Constants below are best-effort
 * defaults; the original RUNTIME-configured values weren't available to
 * port over, so adjust if these don't match what was actually deployed.
 *
 * Field set matches what Python's jobs:notify payload actually carries
 * ("Title", "Description", "Source", "Short URL", "Categories" reach the
 * user; "Job UUID", "URL", "Decision Reason", "Category ID", "Category
 * Selection Method" are present in the payload but not rendered) — see
 * docs/jobs-new-stream-contract.md. No "budget" field exists in that
 * contract, so there's no budget section here.
 */

/** Telegram's hard per-message limit, kept under with a safety margin. */
export const TELEGRAM_MESSAGE_LIMIT = 4096;
const SAFETY_MARGIN = 200;
export const MAX_MESSAGE_LENGTH = TELEGRAM_MESSAGE_LIMIT - SAFETY_MARGIN;
export const MAX_DESCRIPTION_LENGTH = 700;

// A plausible top-level domain: purely alphabetic (ASCII), 2+ chars. Real
// job-post links always end in an ordinary TLD (.com/.net/.io/...);
// truncated scraper output ends in a hyphenated word fragment, which
// Telegram's Bot API rejects as Button_url_invalid when used as a button.
const TLD_RE = /^[A-Za-z]{2,}$/;

function escapeHtml(text: string): string {
  return text
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#x27;');
}

function truncate(text: string, limit: number): string {
  if (text.length <= limit) return text;
  return text.slice(0, Math.max(limit - 3, 0)).trimEnd() + '...';
}

/**
 * Returns a URL usable as an inline button target, or "" if it should not
 * be attached to a button. Telegram rejects the whole sendMessage call if
 * a button URL doesn't parse as an absolute http(s) link, and job posts
 * sourced from scraped channels sometimes carry truncated/malformed links
 * — an unusable one must never reach the keyboard.
 */
export function safeButtonUrl(url: string): string {
  const cleaned = (url ?? '').trim();
  if (!cleaned) return '';

  let parsed: URL;
  try {
    parsed = new URL(cleaned);
  } catch {
    return '';
  }

  const host = parsed.hostname ?? '';
  const lastLabel = host.includes('.') ? host.split('.').pop()! : host;
  if (
    (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') ||
    !host ||
    !TLD_RE.test(lastLabel)
  ) {
    return '';
  }

  return cleaned;
}

/**
 * Truncates an already-built HTML message to (approximately) `limit`
 * characters without leaving malformed HTML behind. buildJobMessage only
 * ever produces simple, non-nested <b>...</b> spans, so this doesn't need
 * a real HTML parser: cut the text, drop any tag fragment left dangling
 * right at the cut point, then close any <b> span still left open.
 */
function safeHtmlTruncate(html: string, limit: number): string {
  if (html.length <= limit) return html;

  let truncated = html.slice(0, Math.max(limit - 3, 0)).trimEnd();

  const lastLt = truncated.lastIndexOf('<');
  const lastGt = truncated.lastIndexOf('>');
  if (lastLt > lastGt) {
    truncated = truncated.slice(0, lastLt).trimEnd();
  }

  const openCount = (truncated.match(/<b>/g) ?? []).length;
  const closeCount = (truncated.match(/<\/b>/g) ?? []).length;
  if (openCount > closeCount) {
    truncated += '</b>'.repeat(openCount - closeCount);
  }

  return truncated + '...';
}

export interface JobMessageParams {
  title: string;
  description?: string;
  source: string;
  /**
   * The category's presentable display name (e.g. 'Backend Development'),
   * resolved from the categories table against the routing slug id — this
   * is what drives the header. Never the raw slug itself, which reads
   * badly to a user.
   */
  categoryName: string;
  /** The payload's "Categories" field — hashtag section only; no longer
   * drives the header, since it can carry an inconsistent/internal
   * taxonomy rather than the actual routing category. */
  categories?: string[];
  url?: string;
  /** Whether `url` (once decorated/validated) is safe to use as a button — if
   * so, the link is left off the body since it's on the button instead. */
  hasButton: boolean;
}

/** HTML-formatted job notification for a category/source subscriber. */
export function buildJobMessage(params: JobMessageParams): string {
  const description = params.description
    ? truncate(params.description, MAX_DESCRIPTION_LENGTH)
    : '';
  const tagCategories =
    params.categories && params.categories.length > 0
      ? params.categories
      : [params.categoryName.trim() || 'Freelance'];
  const displayCategory = params.categoryName.trim() || 'Freelance';

  let header = `🚀 <b>New ${escapeHtml(displayCategory)} Opportunity</b>\n\n`;
  header += `📄 <b>${escapeHtml(params.title)}</b>\n`;
  header += `🏢 <b>Platform</b>\n${escapeHtml(params.source)}`;

  const sections: string[] = [];

  if (description) {
    sections.push(
      `────────────────────────\n\n📋 <b>Description</b>\n\n${escapeHtml(description)}`,
    );
  }

  const hashtags = tagCategories
    .map((name) => `#${name.replace(/ /g, '')}`)
    .join(' ');
  sections.push(
    `────────────────────────\n🏷 <b>Tags</b>\n${escapeHtml(hashtags)}`,
  );

  if (params.url && !params.hasButton) {
    // No usable button for this link — keep it visible as text instead.
    sections.push(`🔗 <b>Link</b>\n${escapeHtml(params.url.trim())}`);
  }

  let message = header;
  for (const section of sections) {
    message += '\n\n' + section;
  }

  if (message.length > MAX_MESSAGE_LENGTH) {
    message = safeHtmlTruncate(message, MAX_MESSAGE_LENGTH);
  }

  return message;
}
