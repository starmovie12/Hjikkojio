import { db } from '@/lib/firebaseAdmin';
import {
  solveHBLinks,
  solveHubCDN,
  solveHubDrive,
  solveHubCloudNative,
  solveGadgetsWebNative,
} from '@/lib/solvers';
import {
  TIMER_API,
  TIMER_DOMAINS,
  TARGET_DOMAINS,
  LINK_TIMEOUT_MS,
  OVERALL_TIMEOUT_MS,
} from '@/lib/config';
import { getCachedLink, setCachedLink } from '@/lib/cache';

export const maxDuration = 60;

// ═══════════════════════════════════════════════════════════════════════════════
// PHASE 6: STREAM SOLVE — Live NDJSON Streaming with Sequential Timer Links
// ═══════════════════════════════════════════════════════════════════════════════
//
// This endpoint streams real-time logs to the browser via NDJSON.
// TIMER links: STRICTLY ONE BY ONE (sequential) — VPS can only handle 1 at a time.
// DIRECT links: CONCURRENT via Promise.allSettled (they're fast).
//
// Supports:
//   • Full task processing (all pending links)
//   • Retry failed only (retryFailedOnly: true)
//   • Single link retry (singleLinkId provided)
//
// NDJSON format:
//   { "id": 0, "msg": "⏱ Timer solving...", "type": "info" }    ← log message
//   { "id": 0, "status": "done", "final": "https://...", ... }  ← result
//   { "id": 0, "status": "finished" }                           ← link complete
// ═══════════════════════════════════════════════════════════════════════════════

// ─── HELPER: fetchJSON ────────────────────────────────────────────────────────
// VPS API call with timeout from config (50s), NOT hardcoded 20s
async function fetchJSON(url: string, timeoutMs = LINK_TIMEOUT_MS): Promise<any> {
  const ctrl  = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      signal:  ctrl.signal,
      headers: { 'User-Agent': 'MflixPro/3.0' },
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return await res.json();
  } catch (err: any) {
    if (err.name === 'AbortError') throw new Error(`Timed out after ${timeoutMs / 1000}s`);
    throw err;
  } finally {
    clearTimeout(timer);
  }
}

// ─── HELPER: saveToFirestore ──────────────────────────────────────────────────
// Atomic transaction — saves ONLY final direct download link
async function saveToFirestore(
  taskId: string | undefined,
  lid: number | string,
  linkData: any,
  result: {
    status?: string;
    finalLink?: string | null;
    error?: string | null;
    logs?: any[];
    best_button_name?: string | null;
    all_available_buttons?: any[];
  },
  extractedBy: string,
): Promise<void> {
  if (!taskId) return;

  try {
    const taskRef = db.collection('scraping_tasks').doc(taskId);
    await db.runTransaction(async (tx) => {
      const doc = await tx.get(taskRef);
      if (!doc.exists) return;

      const existing = doc.data()?.links || [];
      const updated = existing.map((l: any) => {
        if (l.id === lid || l.link === linkData.link) {
          return {
            ...l,
            finalLink:             result.finalLink            ?? l.finalLink ?? null,
            status:                result.status               ?? 'error',
            error:                 result.error                ?? null,
            logs:                  result.logs                 ?? [],
            best_button_name:      result.best_button_name     ?? null,
            all_available_buttons: result.all_available_buttons ?? [],
            solvedAt: new Date().toISOString(),
          };
        }
        return l;
      });

      const allDone = updated.every((l: any) =>
        ['done', 'success', 'error', 'failed'].includes((l.status || '').toLowerCase())
      );
      const anySuccess = updated.some((l: any) =>
        ['done', 'success'].includes((l.status || '').toLowerCase())
      );

      tx.update(taskRef, {
        links: updated,
        status: allDone ? (anySuccess ? 'completed' : 'failed') : 'processing',
        extractedBy,
        ...(allDone ? { completedAt: new Date().toISOString() } : {}),
      });
    });
  } catch (e: any) {
    console.error('[Stream] DB save error:', e.message);
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// POST /api/stream_solve — MAIN HANDLER
// ═══════════════════════════════════════════════════════════════════════════════
export async function POST(req: Request) {
  let body: any;
  try { body = await req.json(); } catch {
    return new Response('Invalid JSON', { status: 400 });
  }

  const allLinks: any[]   = body?.links || [];
  const taskId: string     = body?.taskId;
  const extractedBy        = body?.extractedBy || 'Browser/Live';
  const retryFailedOnly    = body?.retryFailedOnly === true;  // Problem 4: Retry Failed Only
  const singleLinkId       = body?.singleLinkId;               // Problem 5: Individual Repeat

  if (!allLinks.length) {
    return new Response(JSON.stringify({ error: 'No links provided' }), { status: 400 });
  }

  // ─── Filter links based on mode ───────────────────────────────────────
  let linksToProcess: any[];

  if (singleLinkId !== undefined && singleLinkId !== null) {
    // PROBLEM 5: Single link retry — only process this specific link
    linksToProcess = allLinks.filter((l: any) => l.id === singleLinkId);
    if (linksToProcess.length === 0) {
      return new Response(JSON.stringify({ error: 'Link not found' }), { status: 404 });
    }
  } else if (retryFailedOnly) {
    // PROBLEM 4: Retry Failed Only — skip already-done links
    linksToProcess = allLinks.filter((l: any) => {
      const s = (l.status || '').toLowerCase();
      return s !== 'done' && s !== 'success';
    });
    if (linksToProcess.length === 0) {
      return new Response(JSON.stringify({ error: 'No failed/pending links to retry' }), { status: 200 });
    }
  } else {
    // Normal: process all provided links
    linksToProcess = allLinks;
  }

  const stream = new ReadableStream({
    async start(controller) {
      const encoder      = new TextEncoder();
      const overallStart = Date.now();

      const send = (data: any) => {
        try {
          controller.enqueue(encoder.encode(JSON.stringify(data) + '\n'));
        } catch { /* stream closed */ }
      };

      // ─── processLink (stream version) ─────────────────────────────────
      const processLink = async (linkData: any, lid: number | string): Promise<void> => {
        const originalUrl = linkData.link;
        let   currentLink = originalUrl;
        const logs: { msg: string; type: string }[] = [];

        // Real-time log sender
        const log = (msg: string, type = 'info') => {
          logs.push({ msg, type });
          send({ id: lid, msg, type });
        };

        // ─── CACHE CHECK ─────────────────────────────────────────────────
        try {
          const cached = await getCachedLink(originalUrl);
          if (cached && cached.finalLink) {
            log('⚡ CACHE HIT — resolved in 0ms', 'success');
            // Save to Firestore
            const taskRef = db.collection('scraping_tasks').doc(taskId);
            await db.runTransaction(async (tx: any) => {
              const snap = await tx.get(taskRef);
              if (!snap.exists) return;
              const dbLinks = snap.data()!.links || [];
              const idx = dbLinks.findIndex((l: any) => String(l.id) === String(lid));
              if (idx === -1) return;
              dbLinks[idx] = {
                ...dbLinks[idx],
                status: 'done',
                finalLink: cached.finalLink,
                best_button_name: cached.best_button_name ?? null,
                all_available_buttons: cached.all_available_buttons ?? [],
                logs: [{ msg: '⚡ CACHE HIT', type: 'success' }],
                solvedAt: new Date().toISOString(),
              };
              tx.update(taskRef, { links: dbLinks });
            });
            send({ id: lid, status: 'done', final: cached.finalLink, best_button_name: cached.best_button_name });
            send({ id: lid, status: 'finished' });
            return;
          }
        } catch { /* cache miss */ }

        let resultPayload: any;

        try {
          const solving = async () => {
            // HubCDN.fans shortcut
            if (currentLink.includes('hubcdn.fans')) {
              log('⚡ HubCDN.fans detected — direct solve');
              const r = await solveHubCDN(currentLink);
              if (r.status === 'success') return { finalLink: r.final_link, status: 'done', logs };
              return { status: 'error', error: r.message, logs };
            }

            // Timer bypass loop
            let loopCount = 0;
            while (loopCount < 3 && !(TARGET_DOMAINS.some(d => currentLink.includes(d)))) {
              if (!TIMER_DOMAINS.some(d => currentLink.includes(d)) && loopCount === 0) break;

              if (currentLink.includes('gadgetsweb')) {
                log(`🔁 GadgetsWeb native solve (loop ${loopCount + 1})`);
                const r = await solveGadgetsWebNative(currentLink);
                if (r.status === 'success' && r.link) { currentLink = r.link; loopCount++; continue; }
                log(`❌ GadgetsWeb failed: ${r.message}`, 'error');
                break;
              } else {
                // VPS Timer bypass — timeout from config (50s)
                log(`⏱ Timer bypass via VPS (loop ${loopCount + 1})`);
                const r = await fetchJSON(
                  `${TIMER_API}/solve?url=${encodeURIComponent(currentLink)}`,
                  LINK_TIMEOUT_MS,
                );
                if (r.status === 'success' && r.extracted_link) { currentLink = r.extracted_link; loopCount++; continue; }
                log('❌ Timer bypass failed', 'error');
                break;
              }
            }

            // HBLinks
            if (currentLink.includes('hblinks')) {
              log('🔗 HBLinks solving...');
              const r = await solveHBLinks(currentLink);
              if (r.status === 'success' && r.link) {
                currentLink = r.link;
                log(`✅ HBLinks → ${r.source || 'resolved'}`);
              }
              else return { status: 'error', error: r.message, logs };
            }

            // HubDrive
            if (currentLink.includes('hubdrive')) {
              log('💾 HubDrive solving...');
              const r = await solveHubDrive(currentLink);
              if (r.status === 'success' && r.link) {
                currentLink = r.link;
                log('✅ HubDrive → resolved');
              }
              else return { status: 'error', error: r.message, logs };
            }

            // HubCloud / HubCDN
            if (currentLink.includes('hubcloud') || currentLink.includes('hubcdn')) {
              log('☁️ HubCloud solving via VPS...');
              const r = await solveHubCloudNative(currentLink);
              if (r.status === 'success' && r.best_download_link) {
                log(`✅ Done: ${r.best_download_link}`, 'success');
                return {
                  finalLink:             r.best_download_link,
                  status:                'done',
                  best_button_name:      r.best_button_name      ?? null,
                  all_available_buttons: r.all_available_buttons ?? [],
                  logs,
                };
              }
              return { status: 'error', error: r.message, logs };
            }

            // GDflix / DriveHub
            if (currentLink.includes('gdflix') || currentLink.includes('drivehub')) {
              log(`✅ Resolved: ${currentLink}`, 'success');
              return { finalLink: currentLink, status: 'done', logs };
            }

            // Fallback — if current link changed from original, it might be resolved
            if (currentLink !== originalUrl) {
              log(`✅ Resolved: ${currentLink}`, 'success');
              return { finalLink: currentLink, status: 'done', logs };
            }

            return { status: 'error', error: 'No solver matched', logs };
          };

          // Per-link timeout race (50s from config)
          resultPayload = await Promise.race([
            solving(),
            new Promise<any>((_, rej) =>
              setTimeout(() => rej(new Error(`Timeout ${LINK_TIMEOUT_MS / 1000}s`)), LINK_TIMEOUT_MS),
            ),
          ]);
        } catch (err: any) {
          resultPayload = { status: 'error', error: err.message, logs };
        }

        // Stream result to browser
        send({
          id:               lid,
          status:           resultPayload.status,
          final:            resultPayload.finalLink,
          best_button_name: resultPayload.best_button_name,
        });

        // Save to Firestore
        try {
          await saveToFirestore(taskId, lid, linkData, resultPayload, extractedBy);
        } catch { /* non-fatal */ }

        // Cache successful result
        if (resultPayload.status === 'done' && resultPayload.finalLink) {
          try {
            await setCachedLink(originalUrl, resultPayload.finalLink, 'stream_solve', {
              best_button_name: resultPayload.best_button_name,
              all_available_buttons: resultPayload.all_available_buttons,
            });
          } catch { /* non-critical */ }
        }

        // Finished marker for this link
        send({ id: lid, status: 'finished' });
      };

      // ─── Smart routing: Timer vs Direct ─────────────────────────────────
      const timerLinks  = linksToProcess.filter((l: any) => TIMER_DOMAINS.some(d => (l.link || '').includes(d)));
      const directLinks = linksToProcess.filter((l: any) => !TIMER_DOMAINS.some(d => (l.link || '').includes(d)));

      // PROBLEM 1 FIX: Direct links → CONCURRENT (parallel, fast)
      const directPromises = directLinks.map((l: any) => processLink(l, l.id));

      // PROBLEM 1 FIX: Timer links → STRICTLY SEQUENTIAL (one by one)
      // VPS Timer API can only handle 1 request at a time.
      // If we send 2+ simultaneously, one will succeed and others crash.
      const timerPromise = (async () => {
        for (let i = 0; i < timerLinks.length; i++) {
          const l = timerLinks[i];

          // Time budget check — don't start a new timer link if we might not finish
          if (Date.now() - overallStart > OVERALL_TIMEOUT_MS) {
            send({ id: l.id, msg: '⏳ Time budget exceeded — will retry in background', type: 'warn' });
            send({ id: l.id, status: 'finished' });
            continue;
          }

          // Process this ONE timer link — VPS gets full attention
          send({ id: l.id, msg: `⏱ Timer link ${i + 1}/${timerLinks.length} — starting...`, type: 'info' });
          await processLink(l, l.id);
        }
      })();

      // Run direct (parallel) + timer (sequential) simultaneously
      // Timer won't interfere with direct because they use different VPS ports
      await Promise.allSettled([...directPromises, timerPromise]);

      controller.close();
    },
  });

  return new Response(stream, {
    headers: {
      'Content-Type':  'application/x-ndjson',
      'Cache-Control': 'no-cache',
      'Connection':    'keep-alive',
    },
  });
}
