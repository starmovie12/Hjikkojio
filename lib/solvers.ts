import axios from 'axios';
import * as cheerio from 'cheerio';
import {
  AXIOS_TIMEOUT_MS,
  HUBCLOUD_API,
  TIMER_API,
  HUBCLOUD_TLDS,
  HUBDRIVE_TLDS,
  CDN_DOMAINS,
  VALID_LANGUAGES,
  FORMAT_PRIORITY,
  isJunkLinkText,
  isJunkDomain,
  HTTP_522_MAX_RETRIES,
} from './config';
import type {
  ExtractMovieLinksResult,
  HubCloudNativeResult,
  HBLinksResult,
  HubCDNResult,
  HubDriveResult,
  GadgetsWebResult,
  MovieMetadata,
  MoviePreview,
} from './types';

// =============================================================================
// PHASE 4: HTTP 522 RETRY WRAPPER
// Cloudflare 522 = origin server not responding. Auto-retry with backoff.
// =============================================================================

async function axiosWithRetry(
  url: string,
  options: { headers?: Record<string, string>; timeout?: number; responseType?: string } = {},
  maxRetries = HTTP_522_MAX_RETRIES,
): Promise<any> {
  let lastError: any;
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      const res = await axios.get(url, {
        headers: options.headers || BROWSER_HEADERS,
        timeout: options.timeout || AXIOS_TIMEOUT_MS,
        responseType: (options.responseType as any) || 'text',
      });
      return res;
    } catch (err: any) {
      lastError = err;
      const status = err?.response?.status;
      // Only retry on 522 (Cloudflare), 502, 503, 504 (server errors)
      if ([522, 502, 503, 504].includes(status) && attempt < maxRetries) {
        const wait = 2000 * (attempt + 1); // 2s, 4s backoff
        await new Promise(r => setTimeout(r, wait));
        continue;
      }
      throw err;
    }
  }
  throw lastError;
}

// =============================================================================
// HEADERS
// =============================================================================

const BROWSER_HEADERS: Record<string, string> = {
  'User-Agent':      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
  'Accept':          'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
  'Accept-Language': 'en-US,en;q=0.9,hi;q=0.8',
  'Accept-Encoding': 'gzip, deflate, br',
  'Sec-Fetch-Dest':  'document',
  'Sec-Fetch-Mode':  'navigate',
  'Sec-Fetch-Site':  'none',
  'Sec-Fetch-User':  '?1',
  'Upgrade-Insecure-Requests': '1',
  'Cache-Control':   'max-age=0',
  'Sec-Ch-Ua':          '"Chromium";v="125", "Google Chrome";v="125", "Not.A/Brand";v="24"',
  'Sec-Ch-Ua-Mobile':   '?0',
  'Sec-Ch-Ua-Platform': '"Windows"',
};

const MOBILE_HEADERS: Record<string, string> = {
  'User-Agent':      'Mozilla/5.0 (Linux; Android 14; SM-S928B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Mobile Safari/537.36',
  'Accept':          'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
  'Accept-Language': 'en-US,en;q=0.9',
  'Accept-Encoding': 'gzip, deflate, br',
  'Sec-Fetch-Dest':  'document',
  'Sec-Fetch-Mode':  'navigate',
  'Sec-Fetch-Site':  'none',
  'Sec-Fetch-User':  '?1',
  'Upgrade-Insecure-Requests': '1',
  'Sec-Ch-Ua':          '"Chromium";v="125", "Google Chrome";v="125", "Not.A/Brand";v="24"',
  'Sec-Ch-Ua-Mobile':   '?1',
  'Sec-Ch-Ua-Platform': '"Android"',
};

const EXTRACT_MOBILE_HEADERS: Record<string, string> = {
  'User-Agent':      'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36',
  'Referer':         'https://hdhub4u.fo/',
  'Sec-Ch-Ua':          '"Chromium";v="124", "Google Chrome";v="124", "Not.A/Brand";v="24"',
  'Sec-Ch-Ua-Mobile':   '?1',
  'Sec-Ch-Ua-Platform': '"Android"',
};


// =============================================================================
// FUNCTION 1: extractMovieLinks
// ✅ FIX 1 APPLIED — Absolute URL resolution via new URL(rawLink, pageUrl).href
//
// ROOT CAUSE OF THE BUG:
// The previous code took rawLink directly from href/data-href and added it to
// foundLinks without ever checking if it was absolute. A relative URL like
// "/go/hubcloud?id=abc123" contains the string "hubcloud", so it passed the
// TARGET_DOMAINS_LIST check and was stored in the database. Phase 2 then called
// axios.get("/go/hubcloud?id=abc123") which threw ERR_INVALID_URL and crashed.
//
// THE FIX:
// Every raw link string is immediately resolved through new URL(rawLink, url).
// This API handles all cases in one call:
//   "https://hubcloud.foo/abc"   → unchanged (already absolute)
//   "//hubcloud.foo/abc"         → adds https: (protocol-relative)
//   "/go/hubcloud?id=xyz"        → adds page origin (root-relative)
//   "../dl/hubcloud/xyz"         → resolves against page path (relative)
//   "not a url %%"               → throws → we skip with continue
// After this block, resolvedLink is always a valid fully-qualified absolute URL.
// =============================================================================

export async function extractMovieLinks(url: string): Promise<ExtractMovieLinksResult> {
  try {
    // Phase 4 FIX: Use axiosWithRetry for 522 Cloudflare error auto-retry
    const response = await axiosWithRetry(url, {
      headers: EXTRACT_MOBILE_HEADERS as any,
      timeout: AXIOS_TIMEOUT_MS,
      responseType: 'text',
    });

    const html = response.data;
    const $    = cheerio.load(html);

    const preview  = extractMoviePreview(html);
    const metadata = extractMovieMetadata(html);

    const foundLinks: Array<{ name: string; link: string }> = [];
    const seenUrls = new Set<string>();

    const DOWNLOAD_KEYWORDS   = ['DOWNLOAD', '720P', '480P', '1080P', '4K', 'DIRECT', 'GDRIVE'];
    const TARGET_DOMAINS_LIST = ['hblinks', 'hubdrive', 'hubcdn', 'hubcloud', 'gdflix', 'drivehub'];

    // -------------------------------------------------------------------------
    // STEP 1: Collect all candidate elements.
    // Scan (a) standard anchor tags inside content areas, AND
    //      (b) elements with .btn or .button classes (styled download buttons).
    // -------------------------------------------------------------------------
    const candidateElements: ReturnType<typeof $>[] = [];

    // Standard anchors inside main content areas
    $('.entry-content a, main a, .post-content a').each((_i, el) => {
      candidateElements.push($(el));
    });

    // Button class scan — catches styled download buttons (.btn, .button).
    // Only processes non-anchor elements to avoid double-counting plain <a> tags.
    $('.entry-content .btn, .entry-content .button, main .btn, main .button, .post-content .btn, .post-content .button').each((_i, el) => {
      const tagName = (el as { tagName?: string }).tagName?.toLowerCase();
      if (tagName !== 'a') {
        // For non-anchor button wrappers, prefer any nested anchor inside them
        const $innerA = $(el).find('a').first();
        if ($innerA.length > 0) {
          candidateElements.push($innerA);
        } else {
          // No nested anchor — use the wrapper element itself (may carry data-href)
          candidateElements.push($(el));
        }
      }
    });

    // -------------------------------------------------------------------------
    // STEP 2: Process every collected candidate element
    // -------------------------------------------------------------------------
    for (const $el of candidateElements) {

      // STEP 2a: Extract raw link string from href, then fall back to data-href.
      // Many movie sites encode the real URL in data-href to slow down scrapers.
      const hrefRaw     = ($el.attr('href')      || '').trim();
      const dataHrefRaw = ($el.attr('data-href') || '').trim();
      const rawLink     = hrefRaw || dataHrefRaw;

      // Skip immediately if no link string exists at all
      if (!rawLink)                          continue;
      // Skip in-page anchor links (e.g. "#comments")
      if (rawLink.startsWith('#'))           continue;
      // Skip javascript: pseudo-links (e.g. "javascript:void(0)")
      if (rawLink.startsWith('javascript:')) continue;

      // -----------------------------------------------------------------------
      // STEP 2b: *** FIX 1 — MANDATORY ABSOLUTE URL RESOLUTION ***
      // Resolve rawLink against the original page URL so it becomes absolute.
      // If resolution throws (truly malformed string), skip this element.
      // -----------------------------------------------------------------------
      let resolvedLink: string;
      try {
        resolvedLink = new URL(rawLink, url).href;
      } catch {
        // rawLink cannot be resolved to any valid URL (mailto:, tel:, garbage).
        // Skip silently — this is expected noise on every real page.
        continue;
      }

      // STEP 2c: Junk domain check on the RESOLVED (absolute) URL.
      // Always check the absolute URL, not rawLink, to avoid false positives.
      if (isJunkDomain(resolvedLink)) continue;

      // STEP 2d: Get visible text content of the element
      const text = $el.text().trim();

      // STEP 2e: Junk text check on raw anchor text
      if (isJunkLinkText(text)) continue;

      // STEP 2f: Parent container junk text check.
      // If the wrapping container is labeled as junk (e.g. "How to Download"),
      // skip all buttons inside it.
      const $parentContainer = $el.closest('p, div, h3, h4, li');
      const parentText       = $parentContainer.text().trim();
      if (isJunkLinkText(parentText)) continue;

      // STEP 2g: Domain + keyword relevance gate.
      // A link must EITHER point to a known download CDN domain
      // OR the visible button text must contain a known download keyword.
      const isTargetDomainLink = TARGET_DOMAINS_LIST.some((domain) =>
        resolvedLink.toLowerCase().includes(domain)
      );
      const isDownloadKeywordText = DOWNLOAD_KEYWORDS.some((keyword) =>
        text.toUpperCase().includes(keyword)
      );

      if (!isTargetDomainLink && !isDownloadKeywordText) continue;

      // STEP 2h: Duplicate check on the RESOLVED URL.
      // Using the resolved URL as the dedup key ensures the same destination
      // reached via different relative/absolute forms is correctly deduplicated.
      if (seenUrls.has(resolvedLink)) continue;
      seenUrls.add(resolvedLink);

      // -----------------------------------------------------------------------
      // STEP 3: Build a clean human-readable display name for this link
      // -----------------------------------------------------------------------

      // Start with the raw visible text of the anchor
      let cleanName = text;

      // STEP 3a: Full Unicode emoji removal — strip ALL emoji, not just a few.
      cleanName = cleanName
        .replace(/[\p{Emoji_Presentation}\p{Extended_Pictographic}]/gu, '')
        .trim();

      // STEP 3b: If name is still too short (<2 chars) after emoji removal,
      // climb the DOM to find a descriptive heading label for this button group.
      if (!cleanName || cleanName.length < 2) {
        const $container   = $el.closest('p, div, h3, h4, li');
        const $prevHeading = $container.prev('h3, h4, h5, strong');

        if ($prevHeading.length > 0) {
          // Sibling heading directly above describes this link group.
          // Strip emojis from heading text too.
          cleanName = $prevHeading.text()
            .replace(/[\p{Emoji_Presentation}\p{Extended_Pictographic}]/gu, '')
            .trim();
        } else {
          // No sibling heading — use the container's own text (excluding children)
          // to avoid pulling in all nested button texts.
          const containerOwnText = $container
            .clone()
            .children()
            .remove()
            .end()
            .text()
            .trim();
          cleanName = containerOwnText || 'Download Link';
        }
      }

      // STEP 3c: Truncate to maximum 50 characters
      cleanName = cleanName.substring(0, 50).trim();

      // STEP 3d: FINAL junk check on the fully CLEANED name.
      // Run again because emoji stripping and truncation may have exposed
      // a bare junk word like "download" or "4k".
      if (isJunkLinkText(cleanName)) continue;

      // Final fallback for empty or too-short name after all processing
      if (!cleanName || cleanName.length < 2) {
        cleanName = 'Download Link';
      }

      // -----------------------------------------------------------------------
      // STEP 4: All checks passed — push the RESOLVED absolute URL to results
      // -----------------------------------------------------------------------
      foundLinks.push({ name: cleanName, link: resolvedLink });
    }

    if (foundLinks.length === 0) {
      return {
        status:  'error',
        message: 'No download links found. Page structure may have changed.',
      };
    }

    return {
      status:   'success',
      total:    foundLinks.length,
      links:    foundLinks,
      metadata,
      preview,
    };

  } catch (e: unknown) {
    return {
      status:  'error',
      message: e instanceof Error ? e.message : String(e),
    };
  }
}

// =============================================================================
// FUNCTION 2: extractMoviePreview
// (No changes — logic is correct as-is)
// =============================================================================

export function extractMoviePreview(html: string): MoviePreview {
  const $ = cheerio.load(html);
  let title = '';

  const h1Text = $('h1.entry-title, h1.post-title, h1').first().text().trim();
  if (h1Text) {
    title = h1Text;
  } else {
    const ogTitle = $('meta[property="og:title"]').attr('content') || '';
    title = ogTitle || $('title').text().trim() || 'Unknown Movie';
  }

  title = title
    .replace(/\s*[-\u2013|].*?(HDHub|HdHub|hdhub|Download|Free|Watch|Online).*$/i, '')
    .trim();
  if (!title) title = 'Unknown Movie';

  let posterUrl: string | null = null;
  const ogImage = $('meta[property="og:image"]').attr('content') || '';
  if (ogImage && !ogImage.toLowerCase().includes('logo') && !ogImage.toLowerCase().includes('favicon')) {
    posterUrl = ogImage;
  } else {
    const contentImg = $('.entry-content img, .post-content img, main img').first().attr('src');
    if (contentImg && !contentImg.toLowerCase().includes('logo') && !contentImg.toLowerCase().includes('icon')) {
      posterUrl = contentImg;
    }
  }

  return { title, posterUrl };
}

// =============================================================================
// FUNCTION 3: extractMovieMetadata
// ✅ FIX 2 APPLIED — Wide sibling context label (replaces narrow closest() only)
// ✅ FIX 3 APPLIED — Linked quality+format pair (atomic candidate, never mixed)
//
// FIX 2 ROOT CAUSE:
// $(el).closest('h3,h4,p') only traverses UP through ancestors. The most common
// real-world HTML pattern on HdHub4u/HdHub4u3 is:
//   <h3>720p WEB-DL | Hindi + English</h3>  ← SIBLING — closest() MISSES THIS
//   <p><a href="hubcloud.foo/abc">⚡ Download</a></p>
// The <h3> is a sibling of <p>, not an ancestor. closest() returns the <p>
// whose text is "⚡ Download" — zero metadata extracted from PASS 1.
//
// FIX 3 ROOT CAUSE:
// resolution and format were updated as two INDEPENDENT variables. On a page
// with "720p WEB-DL" + "1080p HDTC", resolution upgraded to 1080p (from HDTC
// button) while format stayed WEB-DL (higher FORMAT_PRIORITY from 720p button).
// Result: phantom "1080p WEB-DL" — a quality pair that never existed on the page.
// =============================================================================

export function extractMovieMetadata(html: string): MovieMetadata {
  const $ = cheerio.load(html);

  const foundLanguages = new Set<string>();

  // ---------------------------------------------------------------------------
  // FIX 3 — QUALITY CANDIDATE: Linked resolution + format as an atomic PAIR.
  //
  // We maintain a single bestCandidate object. Both resolution and format are
  // ALWAYS from the same button label — never mixed from different buttons.
  //
  // Replacement rules (only one of these two conditions triggers a replace):
  //   Condition A: new button resolution is strictly HIGHER than current best.
  //   Condition B: new button resolution is EQUAL to current best AND
  //                new button format has strictly HIGHER FORMAT_PRIORITY.
  //
  // Any other combination (lower res, or equal res + equal/lower format) → keep current.
  // ---------------------------------------------------------------------------
  interface QualityCandidate {
    resolution:      string; // e.g. "1080P", "720P", "4K", ""
    resolutionScore: number; // numeric: 4K=2160, 1080p=1080, 720p=720, 0=none
    format:          string; // e.g. "WEB-DL", "BluRay", ""
    formatScore:     number; // FORMAT_PRIORITY value, -1 = no format on this button
  }

  // Empty zero-score starting point — any real button value will beat this.
  let bestCandidate: QualityCandidate = {
    resolution:      '',
    resolutionScore: 0,
    format:          '',
    formatScore:     -1,
  };

  // ---------------------------------------------------------------------------
  // Content scope: main.page-body → div.entry-content → document root
  // ---------------------------------------------------------------------------
  let $mainContent = $('main.page-body');
  if ($mainContent.length === 0) $mainContent = $('div.entry-content');
  if ($mainContent.length === 0) $mainContent = $.root() as ReturnType<typeof $>;

  // Narrow scope to the DOWNLOAD LINKS section to avoid stray metadata from
  // comments, sidebars, or unrelated parts of the page.
  let $downloadSection: ReturnType<typeof $> = $mainContent;
  $mainContent.find('h2, h3, h4').each((_i, heading) => {
    if ($(heading).text().toUpperCase().includes('DOWNLOAD LINKS')) {
      $downloadSection = $(heading).parent() as ReturnType<typeof $>;
      return false;
    }
  });

  // ---------------------------------------------------------------------------
  // FORMAT PATTERN TABLE — used in PASS 1 and PASS 4 fallback.
  // The loop that uses this table has NO break statement — every pattern is
  // evaluated so the highest-priority format token on a single label wins.
  // ---------------------------------------------------------------------------
  const FORMAT_PATTERNS: Array<[RegExp, string]> = [
    [/WEB-DL/i,           'WEB-DL'],
    [/BLURAY|BLU-RAY/i,   'BluRay'],
    [/WEBRIP|WEB-RIP/i,   'WEBRip'],
    [/HDTC|HD-TC/i,       'HDTC'],
    [/HEVC|H\.265|x265/i, 'HEVC'],
    [/x264|H\.264/i,      'x264'],
    [/10[- ]?Bit/i,       '10Bit'],
  ];

  // ---------------------------------------------------------------------------
  // PASS 1 — Extract from CDN download link button labels
  //
  // FIX 2 — WIDE SIBLING CONTEXT LABEL (4 text sources combined)
  //
  // Instead of closest('h3,h4,p') which only looks UP at ancestors,
  // we build a contextLabel from four sources:
  //
  //   SOURCE 1: The anchor element's own visible text (e.g. "⚡ Download")
  //   SOURCE 2: The anchor's direct parent element text (e.g. the <p> text)
  //   SOURCE 3: Nearest preceding SIBLING heading of the direct parent
  //             — this is the <h3>720p WEB-DL | Hindi</h3> that closest() missed
  //   SOURCE 4: Nearest preceding SIBLING heading of the grandparent
  //             — one more level of nesting depth for safety
  //
  // All four are joined into one contextLabel string. Regex word-boundary
  // matching correctly finds tokens across join boundaries.
  // ---------------------------------------------------------------------------
  $downloadSection.find('a[href]').each((_i, el) => {
    const href = ($(el).attr('href') || '').toLowerCase();

    // Only process links pointing to known CDN download domains
    if (!CDN_DOMAINS.some((domain) => href.includes(domain))) return;

    // SOURCE 1: Anchor's own visible text
    const anchorOwnText = $(el).text().trim();

    // SOURCE 2: Direct parent element's full text
    const $directParent    = $(el).parent();
    const directParentText = $directParent.text().trim();

    // SOURCE 3: Nearest preceding sibling heading of the direct parent.
    // prevAll() returns all preceding siblings; .first() gives the closest one.
    const $siblingOfParent     = $directParent.prevAll('h2, h3, h4, strong').first();
    const siblingOfParentText  = $siblingOfParent.text().trim();

    // SOURCE 4: Nearest preceding sibling heading of the grandparent.
    // Handles one more level of nesting (e.g. <p> inside <div class="dl-block">).
    const $grandParent             = $directParent.parent();
    const $siblingOfGrandParent    = $grandParent.prevAll('h2, h3, h4, strong').first();
    const siblingOfGrandParentText = $siblingOfGrandParent.text().trim();

    // Combine all four sources into one wide, searchable context string.
    const contextLabel = [
      anchorOwnText,
      directParentText,
      siblingOfParentText,
      siblingOfGrandParentText,
    ]
      .filter(Boolean)
      .join(' ')
      .replace(/\s+/g, ' ')
      .trim();

    if (!contextLabel) return;

    // ── Language extraction — search contextLabel (not just direct parent) ───
    for (const lang of VALID_LANGUAGES) {
      if (new RegExp(`\\b${lang}\\b`, 'i').test(contextLabel)) {
        foundLanguages.add(lang);
      }
    }

    // ── Quality candidate extraction — FIX 3 atomic pair logic ──────────────

    // Compute resolution score for this button's contextLabel
    const resolutionMatch   = contextLabel.match(/(480p|720p|1080p|2160p|4K)/i);
    let thisResolution      = '';
    let thisResolutionScore = 0;
    if (resolutionMatch) {
      thisResolution      = resolutionMatch[1].toUpperCase();
      thisResolutionScore = thisResolution === '4K'
        ? 2160
        : parseInt(thisResolution.replace(/\D/g, '') || '0', 10);
    }

    // Scan ALL format patterns in contextLabel — NO break.
    // Keep the highest-priority format found within this single label.
    let thisFormat      = '';
    let thisFormatScore = -1;
    for (const [pattern, formatName] of FORMAT_PATTERNS) {
      if (pattern.test(contextLabel)) {
        const score = FORMAT_PRIORITY[formatName] ?? -1;
        if (score > thisFormatScore) {
          thisFormatScore = score;
          thisFormat      = formatName;
        }
        // NO break — continue evaluating all remaining format patterns
      }
    }

    // Apply atomic replacement logic — only update bestCandidate if this
    // button is genuinely better, and ALWAYS update both fields together.
    if (thisResolutionScore > 0) {
      const shouldReplace =
        thisResolutionScore > bestCandidate.resolutionScore ||
        (
          thisResolutionScore === bestCandidate.resolutionScore &&
          thisFormatScore     >  bestCandidate.formatScore
        );

      if (shouldReplace) {
        bestCandidate = {
          resolution:      thisResolution,
          resolutionScore: thisResolutionScore,
          format:          thisFormat,
          formatScore:     thisFormatScore,
        };
      }
    }
  });

  // ---------------------------------------------------------------------------
  // PASS 2 — MULTi tag pattern scan
  // Handles: "MULTi [HINDI + ENGLISH + TAMIL]" multi-language release labels.
  // ---------------------------------------------------------------------------
  const pageText   = $downloadSection.text();
  const multiMatch = pageText.match(/MULTi[\s\S]*?\[([\s\S]*?HINDI[\s\S]*?)\]/i);
  if (multiMatch && multiMatch[1]) {
    for (const lang of VALID_LANGUAGES) {
      if (new RegExp(`\\b${lang}\\b`, 'i').test(multiMatch[1])) {
        foundLanguages.add(lang);
      }
    }
  }

  // ---------------------------------------------------------------------------
  // PASS 3 — Language field fallback
  // Runs only if PASS 1 + PASS 2 found zero languages.
  // Scans for an explicit "Language : Hindi, English" info field.
  // ---------------------------------------------------------------------------
  if (foundLanguages.size === 0) {
    $mainContent.find('div, span, p').each((_i, elem) => {
      const elemText       = $(elem).text();
      const langFieldMatch = elemText.match(/Language\s*:(.+?)(?:\n|\/|$)/i);

      if (langFieldMatch && langFieldMatch[1]) {
        for (const lang of VALID_LANGUAGES) {
          if (new RegExp(`\\b${lang}\\b`, 'i').test(langFieldMatch[1])) {
            foundLanguages.add(lang);
          }
        }
        return false; // Stop after the first "Language :" field
      }
    });
  }

  // ---------------------------------------------------------------------------
  // PASS 4 — Quality field fallback
  // Runs only if PASS 1 found no resolution at all (bestCandidate.resolution is empty).
  // Scans for an explicit "Quality : 1080p WEB-DL" info field.
  // Uses the same atomic-pair logic as PASS 1 — both fields updated together.
  // ---------------------------------------------------------------------------
  if (!bestCandidate.resolution) {
    $mainContent.find('div, span, p').each((_i, elem) => {
      const elemText = $(elem).text();

      if (!/Quality\s*:/i.test(elemText)) return;

      const qualityFieldMatch = elemText.match(/Quality\s*:(.+?)(?:\n|$)/i);
      if (!qualityFieldMatch || !qualityFieldMatch[1]) return;

      const fieldContent = qualityFieldMatch[1];

      // Extract resolution from the Quality field
      const resInField        = fieldContent.match(/(480p|720p|1080p|2160p|4K)/i);
      let fallbackResolution      = '';
      let fallbackResolutionScore = 0;
      if (resInField) {
        fallbackResolution      = resInField[1].toUpperCase();
        fallbackResolutionScore = fallbackResolution === '4K'
          ? 2160
          : parseInt(fallbackResolution.replace(/\D/g, '') || '0', 10);
      }

      // Scan ALL format patterns from the Quality field — NO break.
      let fallbackFormat      = '';
      let fallbackFormatScore = -1;
      for (const [pattern, formatName] of FORMAT_PATTERNS) {
        if (pattern.test(fieldContent)) {
          const score = FORMAT_PRIORITY[formatName] ?? -1;
          if (score > fallbackFormatScore) {
            fallbackFormatScore = score;
            fallbackFormat      = formatName;
          }
          // NO break — check all patterns for highest priority
        }
      }

      // Apply as atomic pair — both resolution and format from this one field
      if (fallbackResolutionScore > 0) {
        bestCandidate = {
          resolution:      fallbackResolution,
          resolutionScore: fallbackResolutionScore,
          format:          fallbackFormat,
          formatScore:     fallbackFormatScore,
        };
      }

      return false; // Stop after the first "Quality :" field
    });
  }

  // ---------------------------------------------------------------------------
  // Build final output values
  // ---------------------------------------------------------------------------
  const langArray = Array.from(foundLanguages).sort();
  const langCount = langArray.length;

  const audioLabel =
    langCount === 0 ? 'Not Found'  :
    langCount === 1 ? langArray[0] :
    langCount === 2 ? 'Dual Audio' :
                      'Multi Audio';

  // Both fields of qualityString always came from the same source (FIX 3 guarantee)
  const qualityString = bestCandidate.resolution
    ? `${bestCandidate.resolution}${bestCandidate.format ? ' ' + bestCandidate.format : ''}`.trim()
    : 'Unknown Quality';

  return {
    quality:    qualityString,
    languages:  langArray.length > 0 ? langArray.join(', ') : 'Not Specified',
    audioLabel,
  };
}

// =============================================================================
// FUNCTION 4: solveHBLinks
// (No changes — logic is correct as-is)
// =============================================================================

export async function solveHBLinks(url: string): Promise<HBLinksResult> {
  try {
    const response = await axios.get<string>(url, {
      headers: BROWSER_HEADERS, timeout: AXIOS_TIMEOUT_MS, responseType: 'text',
    });

    if (response.status !== 200) {
      return { status: 'fail', message: `HTTP ${response.status}` };
    }

    const $ = cheerio.load(response.data);

    for (const tld of HUBCLOUD_TLDS) {
      const found = $(`a[href*="hubcloud${tld}"]`).attr('href');
      if (found) return { status: 'success', link: found, source: `HubCloud${tld} (P1)` };
    }

    for (const tld of HUBDRIVE_TLDS) {
      const found = $(`a[href*="hubdrive${tld}"]`).attr('href');
      if (found) return { status: 'success', link: found, source: `HubDrive${tld} (P2)` };
    }

    const generic = $('a[href*="hubcloud"], a[href*="hubdrive"]').first().attr('href');
    if (generic) return { status: 'success', link: generic, source: 'Generic (P3)' };

    return { status: 'fail', message: 'No HubCloud or HubDrive link found' };
  } catch (e: unknown) {
    return { status: 'error', message: e instanceof Error ? e.message : String(e) };
  }
}

// =============================================================================
// FUNCTION 5: solveHubCDN
// ✅ FIX 4 APPLIED — Empty catch {} replaced with console.warn + full diagnostics
//
// ROOT CAUSE OF THE BUG:
// The inner try/catch block around the Base64 decode logic had an empty catch {}.
// When new URL(cleanUrl) threw (e.g. HubCDN changed their JS variable format),
// the error was silently swallowed. targetUrl stayed === the original url, so
// the second axios.get fetched the SAME original page again instead of the
// decoded redirect target. The function returned { status: 'failed' } with no
// indication the decode step had failed — making production debugging impossible.
//
// THE FIX:
// Replace catch {} with catch (decodeError) that logs a structured console.warn
// including the original URL, the raw matched string from the page, and the
// error message. targetUrl still falls back to original (same runtime behavior)
// but the failure is now fully visible in Vercel function logs.
// =============================================================================

export async function solveHubCDN(url: string): Promise<HubCDNResult> {
  try {
    let targetUrl = url;

    if (!url.includes('/dl/')) {
      const resp = await axios.get<string>(url, {
        headers: MOBILE_HEADERS, timeout: AXIOS_TIMEOUT_MS, responseType: 'text',
      });

      const reurlMatch = (resp.data as string).match(/var reurl\s*=\s*"(.*?)"/);
      if (reurlMatch && reurlMatch[1]) {
        try {
          const cleanUrl = reurlMatch[1].replace(/&amp;/g, '&');
          const rParam   = new URL(cleanUrl).searchParams.get('r');
          if (rParam) {
            const padding = (4 - (rParam.length % 4)) % 4;
            targetUrl = Buffer.from(rParam + '='.repeat(padding), 'base64').toString('utf-8');
          }
        } catch (decodeError: unknown) {
          // Log structured warning — engineers can see exactly what the site
          // returned and update parsing logic without needing to reproduce.
          console.warn(
            '[solveHubCDN] WARNING: Failed to decode reurl param — falling back to original URL.',
            {
              originalUrl:    url,
              matchedRawStr:  reurlMatch[1],
              decodeErrorMsg: decodeError instanceof Error ? decodeError.message : String(decodeError),
            }
          );
          // targetUrl remains === url. The second axios.get below will still
          // attempt a fetch; it may or may not find a#vd on the original page.
        }
      }
    }

    const finalResp = await axios.get<string>(targetUrl, {
      headers: MOBILE_HEADERS, timeout: AXIOS_TIMEOUT_MS, responseType: 'text',
    });

    const $         = cheerio.load(finalResp.data as string);
    const finalLink = $('a#vd').attr('href');
    if (finalLink) return { status: 'success', final_link: finalLink };

    const scriptMatch = (finalResp.data as string).match(/window\.location\.href\s*=\s*["'](.*?)['"]/);
    if (scriptMatch && scriptMatch[1]) return { status: 'success', final_link: scriptMatch[1] };

    return { status: 'failed', message: 'a#vd not found in HubCDN page' };
  } catch (e: unknown) {
    return { status: 'error', message: e instanceof Error ? e.message : String(e) };
  }
}

// =============================================================================
// FUNCTION 6: solveHubDrive
// (No changes — logic is correct as-is)
// =============================================================================

export async function solveHubDrive(url: string): Promise<HubDriveResult> {
  try {
    const response = await axios.get<string>(url, {
      headers: BROWSER_HEADERS, timeout: AXIOS_TIMEOUT_MS, responseType: 'text',
    });

    const $ = cheerio.load(response.data);
    let finalLink = '';

    const btnSuccess = $('a.btn-success[href*="hubcloud"]');
    if (btnSuccess.length > 0) finalLink = btnSuccess.attr('href') || '';

    if (!finalLink) {
      const dlBtn = $('a#dl');
      if (dlBtn.length > 0) finalLink = dlBtn.attr('href') || '';
    }

    if (!finalLink) {
      $('a[href]').each((_i, el) => {
        const href = $(el).attr('href') || '';
        if (href.includes('hubcloud') || href.includes('hubcdn')) { finalLink = href; return false; }
      });
    }

    if (finalLink) return { status: 'success', link: finalLink };
    return { status: 'fail', message: 'No HubCloud/HubCDN link found on HubDrive page' };
  } catch (e: unknown) {
    return { status: 'error', message: e instanceof Error ? e.message : String(e) };
  }
}

// =============================================================================
// FUNCTION 7: solveHubCloudNative
// (No changes — logic is correct as-is)
// =============================================================================

export async function solveHubCloudNative(url: string): Promise<HubCloudNativeResult> {
  console.log(`[HubCloud] Starting VPS solver: ${url}`);
  try {
    const resp = await axios.get(`${HUBCLOUD_API}/solve?url=${encodeURIComponent(url)}`, {
      timeout: AXIOS_TIMEOUT_MS,
      headers: { 'User-Agent': 'MflixPro/1.0' },
    });

    const data = resp.data;
    if (data.status === 'success' && data.best_download_link) {
      return {
        status:                'success',
        best_button_name:      data.best_button_name      || undefined,
        best_download_link:    data.best_download_link,
        all_available_buttons: data.all_available_buttons || [],
      };
    }

    return { status: 'error', message: data.message || 'VPS API returned no download link' };
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : String(e);
    return { status: 'error', message: `VPS API error: ${msg}` };
  }
}

// =============================================================================
// FUNCTION 8: solveGadgetsWebNative
// (No changes — logic is correct as-is)
// =============================================================================

export async function solveGadgetsWebNative(url: string): Promise<GadgetsWebResult> {
  console.log(`[GadgetsWeb] Starting VPS Timer solver: ${url}`);
  try {
    const resp = await axios.get(`${TIMER_API}/solve?url=${encodeURIComponent(url)}`, {
      timeout: AXIOS_TIMEOUT_MS,
      headers: { 'User-Agent': 'MflixPro/1.0' },
    });

    const data = resp.data;
    if (data.status === 'success' && data.extracted_link) {
      return { status: 'success', link: data.extracted_link };
    }

    return { status: 'error', message: data.message || 'Timer bypass failed' };
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : String(e);
    return { status: 'error', message: `VPS Port 10000 error: ${msg}` };
  }
}
