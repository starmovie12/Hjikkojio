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
// PHASE 4: HTTP 522 RETRY WRAPPER (RELAY RACE READY)
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
// =============================================================================

export async function extractMovieLinks(url: string): Promise<ExtractMovieLinksResult> {
  try {
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

    const candidateElements: ReturnType<typeof $>[] = [];

    $('.entry-content a, main a, .post-content a').each((_i, el) => {
      candidateElements.push($(el));
    });

    $('.entry-content .btn, .entry-content .button, main .btn, main .button, .post-content .btn, .post-content .button').each((_i, el) => {
      const tagName = (el as { tagName?: string }).tagName?.toLowerCase();
      if (tagName !== 'a') {
        const $innerA = $(el).find('a').first();
        if ($innerA.length > 0) {
          candidateElements.push($innerA);
        } else {
          candidateElements.push($(el));
        }
      }
    });

    for (const $el of candidateElements) {

      const hrefRaw     = ($el.attr('href')      || '').trim();
      const dataHrefRaw = ($el.attr('data-href') || '').trim();
      const rawLink     = hrefRaw || dataHrefRaw;

      if (!rawLink)                          continue;
      if (rawLink.startsWith('#'))           continue;
      if (rawLink.startsWith('javascript:')) continue;

      let resolvedLink: string;
      try {
        resolvedLink = new URL(rawLink, url).href;
      } catch {
        continue;
      }

      if (isJunkDomain(resolvedLink)) continue;

      const text = $el.text().trim();

      if (isJunkLinkText(text)) continue;

      const $parentContainer = $el.closest('p, div, h3, h4, li');
      const parentText       = $parentContainer.text().trim();
      if (isJunkLinkText(parentText)) continue;

      const isTargetDomainLink = TARGET_DOMAINS_LIST.some((domain) =>
        resolvedLink.toLowerCase().includes(domain)
      );
      const isDownloadKeywordText = DOWNLOAD_KEYWORDS.some((keyword) =>
        text.toUpperCase().includes(keyword)
      );

      if (!isTargetDomainLink && !isDownloadKeywordText) continue;

      if (seenUrls.has(resolvedLink)) continue;
      seenUrls.add(resolvedLink);

      let cleanName = text;

      cleanName = cleanName
        .replace(/[\p{Emoji_Presentation}\p{Extended_Pictographic}]/gu, '')
        .trim();

      if (!cleanName || cleanName.length < 2) {
        const $container   = $el.closest('p, div, h3, h4, li');
        const $prevHeading = $container.prev('h3, h4, h5, strong');

        if ($prevHeading.length > 0) {
          cleanName = $prevHeading.text()
            .replace(/[\p{Emoji_Presentation}\p{Extended_Pictographic}]/gu, '')
            .trim();
        } else {
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

      cleanName = cleanName.substring(0, 50).trim();

      if (isJunkLinkText(cleanName)) continue;

      if (!cleanName || cleanName.length < 2) {
        cleanName = 'Download Link';
      }

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
// =============================================================================

export function extractMovieMetadata(html: string): MovieMetadata {
  const $ = cheerio.load(html);

  const foundLanguages = new Set<string>();

  interface QualityCandidate {
    resolution:      string; 
    resolutionScore: number; 
    format:          string; 
    formatScore:     number; 
  }

  let bestCandidate: QualityCandidate = {
    resolution:      '',
    resolutionScore: 0,
    format:          '',
    formatScore:     -1,
  };

  let $mainContent = $('main.page-body');
  if ($mainContent.length === 0) $mainContent = $('div.entry-content');
  if ($mainContent.length === 0) $mainContent = $.root() as ReturnType<typeof $>;

  let $downloadSection: ReturnType<typeof $> = $mainContent;
  $mainContent.find('h2, h3, h4').each((_i, heading) => {
    if ($(heading).text().toUpperCase().includes('DOWNLOAD LINKS')) {
      $downloadSection = $(heading).parent() as ReturnType<typeof $>;
      return false;
    }
  });

  const FORMAT_PATTERNS: Array<[RegExp, string]> = [
    [/WEB-DL/i,           'WEB-DL'],
    [/BLURAY|BLU-RAY/i,   'BluRay'],
    [/WEBRIP|WEB-RIP/i,   'WEBRip'],
    [/HDTC|HD-TC/i,       'HDTC'],
    [/HEVC|H\.265|x265/i, 'HEVC'],
    [/x264|H\.264/i,      'x264'],
    [/10[- ]?Bit/i,       '10Bit'],
  ];

  $downloadSection.find('a[href]').each((_i, el) => {
    const href = ($(el).attr('href') || '').toLowerCase();

    if (!CDN_DOMAINS.some((domain) => href.includes(domain))) return;

    const anchorOwnText = $(el).text().trim();
    const $directParent    = $(el).parent();
    const directParentText = $directParent.text().trim();

    const $siblingOfParent     = $directParent.prevAll('h2, h3, h4, strong').first();
    const siblingOfParentText  = $siblingOfParent.text().trim();

    const $grandParent             = $directParent.parent();
    const $siblingOfGrandParent    = $grandParent.prevAll('h2, h3, h4, strong').first();
    const siblingOfGrandParentText = $siblingOfGrandParent.text().trim();

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

    for (const lang of VALID_LANGUAGES) {
      if (new RegExp(`\\b${lang}\\b`, 'i').test(contextLabel)) {
        foundLanguages.add(lang);
      }
    }

    const resolutionMatch   = contextLabel.match(/(480p|720p|1080p|2160p|4K)/i);
    let thisResolution      = '';
    let thisResolutionScore = 0;
    if (resolutionMatch) {
      thisResolution      = resolutionMatch[1].toUpperCase();
      thisResolutionScore = thisResolution === '4K'
        ? 2160
        : parseInt(thisResolution.replace(/\D/g, '') || '0', 10);
    }

    let thisFormat      = '';
    let thisFormatScore = -1;
    for (const [pattern, formatName] of FORMAT_PATTERNS) {
      if (pattern.test(contextLabel)) {
        const score = FORMAT_PRIORITY[formatName] ?? -1;
        if (score > thisFormatScore) {
          thisFormatScore = score;
          thisFormat      = formatName;
        }
      }
    }

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

  const pageText   = $downloadSection.text();
  const multiMatch = pageText.match(/MULTi[\s\S]*?\[([\s\S]*?HINDI[\s\S]*?)\]/i);
  if (multiMatch && multiMatch[1]) {
    for (const lang of VALID_LANGUAGES) {
      if (new RegExp(`\\b${lang}\\b`, 'i').test(multiMatch[1])) {
        foundLanguages.add(lang);
      }
    }
  }

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
        return false; 
      }
    });
  }

  if (!bestCandidate.resolution) {
    $mainContent.find('div, span, p').each((_i, elem) => {
      const elemText = $(elem).text();

      if (!/Quality\s*:/i.test(elemText)) return;

      const qualityFieldMatch = elemText.match(/Quality\s*:(.+?)(?:\n|$)/i);
      if (!qualityFieldMatch || !qualityFieldMatch[1]) return;

      const fieldContent = qualityFieldMatch[1];

      const resInField        = fieldContent.match(/(480p|720p|1080p|2160p|4K)/i);
      let fallbackResolution      = '';
      let fallbackResolutionScore = 0;
      if (resInField) {
        fallbackResolution      = resInField[1].toUpperCase();
        fallbackResolutionScore = fallbackResolution === '4K'
          ? 2160
          : parseInt(fallbackResolution.replace(/\D/g, '') || '0', 10);
      }

      let fallbackFormat      = '';
      let fallbackFormatScore = -1;
      for (const [pattern, formatName] of FORMAT_PATTERNS) {
        if (pattern.test(fieldContent)) {
          const score = FORMAT_PRIORITY[formatName] ?? -1;
          if (score > fallbackFormatScore) {
            fallbackFormatScore = score;
            fallbackFormat      = formatName;
          }
        }
      }

      if (fallbackResolutionScore > 0) {
        bestCandidate = {
          resolution:      fallbackResolution,
          resolutionScore: fallbackResolutionScore,
          format:          fallbackFormat,
          formatScore:     fallbackFormatScore,
        };
      }

      return false; 
    });
  }

  const langArray = Array.from(foundLanguages).sort();
  const langCount = langArray.length;

  const audioLabel =
    langCount === 0 ? 'Not Found'  :
    langCount === 1 ? langArray[0] :
    langCount === 2 ? 'Dual Audio' :
                      'Multi Audio';

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
          console.warn(
            '[solveHubCDN] WARNING: Failed to decode reurl param — falling back to original URL.',
            {
              originalUrl:    url,
              matchedRawStr:  reurlMatch[1],
              decodeErrorMsg: decodeError instanceof Error ? decodeError.message : String(decodeError),
            }
          );
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
// FUNCTION 7: solveHubCloudNative (Ready for parallel execution)
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
// FUNCTION 8: solveGadgetsWebNative (Ready for Sequential Timer Queue)
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
