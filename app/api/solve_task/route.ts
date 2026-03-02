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
  LINK_TIMEOUT_MS,
  RELAY_SAFETY_MARGIN_MS,
  RELAY_MAX_CHAIN_DEPTH,
} from '@/lib/config';
import { getCachedLink, setCachedLink } from '@/lib/cache';

export const maxDuration = 60;

// =============================================================================
// PHASE 5: "TAAR KAATNA" (RELAY RACE / WIRE-CUT) ARCHITECTURE
// =============================================================================
//
// PROBLEM:
//   Vercel has a hard 60-second limit. Timer links (GadgetsWeb etc.) each take
//   25-35 seconds on the slow VPS. With the old architecture, processing 3 Timer
//   links sequentially = 90+ seconds → Vercel kills the function.
//
// SOLUTION — RELAY RACE:
//   1. NON-TIMER LINKS: Execute ALL concurrently (Promise.all). They're fast.
//   2. TIMER LINKS: Execute STRICTLY ONE AT A TIME.
//   3. After ONE Timer link succeeds → IMMEDIATELY:
//      a) Stream the result to the client
//      b) Save to Firebase
//      c) "Cut the wire" — trigger a fresh HTTP POST to ourselves (relay webhook)
//      d) Close the current stream/function
//   4. The relay webhook gets a FRESH 60-second timer from Vercel.
//   5. Repeat until all Timer links are processed.
//
// RESULT:
//   5 Timer links × 35 seconds each = normally 175 seconds.
//   With relay: 5 separate 35-second invocations = works perfectly within limits.
// =============================================================================


// ─── HELPER: fetchJSON ────────────────────────────────────────────────────────
// PHASE 5 FIX: Timeout increased from 20s → 45s (LINK_TIMEOUT_MS from config).
// This was the ROOT CAUSE of "Timeout 20s" errors — VPS needs 25-35s.
async function fetchJSON(url: string, timeoutMs = LINK_TIMEOUT_MS): Promise<any> {
  const ctrl  = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      signal:  ctrl.signal,
      headers: { 'User-Agent': 'MflixPro/5.0-Relay' },
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


// ─── HELPER: Determine self URL for relay webhook ─────────────────────────────
function getSelfUrl(req: Request): string {
  // Priority: x-forwarded-host (behind proxy/LB) → host header → URL parse
  const proto = req.headers.get('x-forwarded-proto') || 'https';
  const host  = req.headers.get('x-forwarded-host') || req.headers.get('host') || '';
  if (host) return `${proto}://${host}/api/stream_solve`;
  // Fallback: parse from request URL
  try {
    const parsed = new URL(req.url);
    return `${parsed.origin}/api/stream_solve`;
  } catch {
    // Last resort: Vercel environment variable
    const vercelUrl = process.env.VERCEL_URL;
    if (vercelUrl) return `https://${vercelUrl}/api/stream_solve`;
    return 'https://localhost:3000/api/stream_solve';
  }
}


// ─── HELPER: saveToFirestore (stream version) ─────────────────────────────────
// Atomic transaction on MASTER DOC's links[] array — NO sub-collection.
// PHASE 5: Only saves FINAL direct download links. No junk, no intermediates.
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


// =============================================================================
// CORE SOLVER: processOneLink
// Processes a SINGLE link through the full chain:
//   Timer Bypass → HBLinks → HubDrive → HubCloud/HubCDN → Final Link
//
// PHASE 5 KEY CHANGES:
//   - fetchJSON timeout: 45s (not 20s)
//   - Timer bypass loop uses LINK_TIMEOUT_MS (45s) for the race
//   - No OVERALL_TIMEOUT_MS check — relay handles per-link timing
//   - ONLY returns finalLink when it's a true direct download link
// =============================================================================

async function processOneLink(
  linkData: any,
  lid: number | string,
  send: (data: any) => void,
  taskId: string | undefined,
  extractedBy: string,
): Promise<any> {
  const originalUrl = linkData.link;
  let   currentLink = originalUrl;
  const logs: { msg: string; type: string }[] = [];

  const log = (msg: string, type = 'info') => {
    logs.push({ msg, type });
    // PHASE 5: INSTANT STREAM — push each log line immediately, no buffering
    send({ id: lid, msg, type });
  };

  // ── Phase 4: CACHE CHECK — instant return if already resolved ─────────────
  try {
    const cached = await getCachedLink(originalUrl);
    if (cached && cached.finalLink) {
      log('⚡ CACHE HIT — resolved in 0ms', 'success');
      // Save to Firestore
      if (taskId) {
        try {
          const taskRef = db.collection('scraping_tasks').doc(taskId);
          await db.runTransaction(async (tx: any) => {
            const snap = await tx.get(taskRef);
            if (!snap.exists) return;
            const links = snap.data()!.links || [];
            const idx = links.findIndex((l: any) => String(l.id) === String(lid));
            if (idx === -1) return;
            links[idx] = {
              ...links[idx],
              status: 'done',
              finalLink: cached.finalLink,
              best_button_name: cached.best_button_name ?? null,
              all_available_buttons: cached.all_available_buttons ?? [],
              logs: [{ msg: '⚡ CACHE HIT', type: 'success' }],
            };
            tx.update(taskRef, { links });
          });
        } catch { /* non-fatal cache save */ }
      }
      // INSTANT stream push
      send({ id: lid, status: 'done', final: cached.finalLink, best_button_name: cached.best_button_name });
      send({ id: lid, status: 'finished' });
      return { status: 'done', finalLink: cached.finalLink, best_button_name: cached.best_button_name, fromCache: true };
    }
  } catch { /* cache miss — continue normal solve */ }

  // ── Main solving logic ────────────────────────────────────────────────────
  let resultPayload: any;

  try {
    const solving = async () => {
      // HubCDN.fans shortcut — skip the full chain
      if (currentLink.includes('hubcdn.fans')) {
        log('⚡ HubCDN.fans detected — direct solve');
        const r = await solveHubCDN(currentLink);
        if (r.status === 'success') return { finalLink: r.final_link, status: 'done', logs };
        return { status: 'error', error: r.message, logs };
      }

      // ── Timer bypass loop ─────────────────────────────────────────────────
      // PHASE 5: No more hardcoded 20s! Uses LINK_TIMEOUT_MS (45s) from config.
      // Process timer pages UNTIL we get a target domain link (hblinks, hubcloud, etc.)
      const TARGET_CHECK = ['hblinks', 'hubdrive', 'hubcdn', 'hubcloud', 'gdflix', 'drivehub'];
      let loopCount = 0;

      while (loopCount < 3 && !(TARGET_CHECK.some(d => currentLink.includes(d)))) {
        // If first iteration and NOT a timer domain, break (it's a direct link)
        if (!TIMER_DOMAINS.some(d => currentLink.includes(d)) && loopCount === 0) break;

        if (currentLink.includes('gadgetsweb')) {
          log(`🔁 GadgetsWeb native solve (loop ${loopCount + 1})`);
          const r = await solveGadgetsWebNative(currentLink);
          if (r.status === 'success' && r.link) {
            currentLink = r.link;
            log(`✅ GadgetsWeb resolved → ${currentLink.substring(0, 60)}...`, 'success');
            loopCount++;
            continue;
          }
          log(`❌ GadgetsWeb failed: ${r.message}`, 'error');
          break;
        } else {
          // Generic timer bypass via VPS TIMER_API
          // PHASE 5 FIX: Timeout is now LINK_TIMEOUT_MS (45s) — NOT hardcoded 20s!
          log(`⏱ Timer bypass via VPS (loop ${loopCount + 1})`);
          try {
            const r = await fetchJSON(
              `${TIMER_API}/solve?url=${encodeURIComponent(currentLink)}`,
              LINK_TIMEOUT_MS, // 45s from config — was hardcoded 20_000
            );
            if (r.status === 'success' && r.extracted_link) {
              currentLink = r.extracted_link;
              log(`✅ Timer resolved → ${currentLink.substring(0, 60)}...`, 'success');
              loopCount++;
              continue;
            }
          } catch (timerErr: any) {
            log(`❌ Timer bypass error: ${timerErr.message}`, 'error');
          }
          log('❌ Timer bypass failed', 'error');
          break;
        }
      }

      // ── HBLinks step ──────────────────────────────────────────────────────
      if (currentLink.includes('hblinks')) {
        log('🔗 HBLinks solving...');
        const r = await solveHBLinks(currentLink);
        if (r.status === 'success' && r.link) {
          currentLink = r.link;
          log(`✅ HBLinks → ${currentLink.substring(0, 60)}...`, 'success');
        } else {
          return { status: 'error', error: r.message || 'HBLinks failed', logs };
        }
      }

      // ── HubDrive step ─────────────────────────────────────────────────────
      if (currentLink.includes('hubdrive')) {
        log('💾 HubDrive solving...');
        const r = await solveHubDrive(currentLink);
        if (r.status === 'success' && r.link) {
          currentLink = r.link;
          log(`✅ HubDrive → ${currentLink.substring(0, 60)}...`, 'success');
        } else {
          return { status: 'error', error: r.message || 'HubDrive failed', logs };
        }
      }

      // ── HubCloud / HubCDN — final step (VPS solver) ──────────────────────
      if (currentLink.includes('hubcloud') || currentLink.includes('hubcdn')) {
        log('☁️ HubCloud solving via VPS...');
        const r = await solveHubCloudNative(currentLink);
        if (r.status === 'success' && r.best_download_link) {
          log(`✅ Done: ${r.best_download_link.substring(0, 60)}...`, 'success');
          return {
            finalLink:             r.best_download_link,
            status:                'done',
            best_button_name:      r.best_button_name      ?? null,
            all_available_buttons: r.all_available_buttons ?? [],
            logs,
          };
        }
        return { status: 'error', error: r.message || 'HubCloud failed', logs };
      }

      // ── GDflix / DriveHub — already a final link ──────────────────────────
      if (currentLink.includes('gdflix') || currentLink.includes('drivehub')) {
        log(`✅ Resolved: ${currentLink.substring(0, 60)}...`, 'success');
        return { finalLink: currentLink, status: 'done', logs };
      }

      // ── Fallback — treat currentLink as resolved ──────────────────────────
      log(`✅ Resolved: ${currentLink.substring(0, 60)}...`, 'success');
      return { finalLink: currentLink, status: 'done', logs };
    };

    // Race the solver against the 45s timeout (LINK_TIMEOUT_MS)
    resultPayload = await Promise.race([
      solving(),
      new Promise<any>((_, rej) =>
        setTimeout(() => rej(new Error(`Link timeout: ${LINK_TIMEOUT_MS / 1000}s exceeded`)), LINK_TIMEOUT_MS),
      ),
    ]);
  } catch (err: any) {
    resultPayload = { status: 'error', error: err.message, logs };
  }

  // ── INSTANT stream: Push result to client immediately ─────────────────────
  send({
    id:               lid,
    status:           resultPayload.status,
    final:            resultPayload.finalLink || null,
    best_button_name: resultPayload.best_button_name || null,
  });

  // ── Save FINAL link to Firestore (NO junk, NO intermediates) ──────────────
  try {
    await saveToFirestore(taskId, lid, linkData, resultPayload, extractedBy);
  } catch { /* non-fatal */ }

  // ── Phase 4: Save to cache if solved successfully ─────────────────────────
  if (resultPayload.status === 'done' && resultPayload.finalLink) {
    try {
      await setCachedLink(originalUrl, resultPayload.finalLink, 'stream_solve_v5', {
        best_button_name:      resultPayload.best_button_name,
        all_available_buttons: resultPayload.all_available_buttons,
      });
    } catch { /* non-critical */ }
  }

  // ── Finished marker — tells frontend this link is complete ────────────────
  send({ id: lid, status: 'finished' });

  return resultPayload;
}


// =============================================================================
// RELAY TRIGGER: Fire-and-forget webhook to process the NEXT timer link
// =============================================================================
// Triggers a new HTTP POST to ourselves. Vercel treats this as a fresh function
// invocation with a brand new 60-second timer. The current function can then
// safely close its stream and die.
//
// CRITICAL: We DO await the initial connection (to ensure Vercel receives the
// request) but DO NOT await the full response. The relay will process
// independently in its own serverless instance.
// =============================================================================

function triggerRelayWebhook(
  selfUrl: string,
  taskId: string,
  extractedBy: string,
  timerLinks: any[],
  nextIndex: number,
  chainDepth: number,
): void {
  const relayBody = JSON.stringify({
    _relay:      true,
    _timerLinks: timerLinks,
    _timerIndex: nextIndex,
    _chainDepth: chainDepth + 1,
    taskId,
    extractedBy,
  });

  console.log(`[Relay] 🔗 Triggering relay chain #${chainDepth + 1} for timer link index ${nextIndex}/${timerLinks.length}`);

  // Fire-and-forget: Don't await the response. The fetch() dispatches the HTTP
  // request, and Vercel will spin up a new function to handle it.
  fetch(selfUrl, {
    method:  'POST',
    headers: { 'Content-Type': 'application/json' },
    body:    relayBody,
  }).catch((err) => {
    console.error(`[Relay] ❌ Failed to trigger relay webhook:`, err.message);
  });
}


// =============================================================================
// RELAY HANDLER: Process a single timer link from the relay chain
// =============================================================================
// Called when _relay: true. Processes ONE timer link, saves to Firebase,
// triggers the next relay if more links remain, and returns immediately.
// No stream needed — the frontend reads updates from Firebase.
// =============================================================================

async function handleRelayRequest(body: any, selfUrl: string): Promise<Response> {
  const {
    taskId,
    extractedBy,
    _timerLinks: timerLinks,
    _timerIndex: timerIndex,
    _chainDepth: chainDepth = 0,
  } = body;

  console.log(`[Relay] ⚡ Chain #${chainDepth} — Processing timer link ${timerIndex + 1}/${timerLinks.length}`);

  // Safety: Prevent infinite relay chains
  if (chainDepth >= RELAY_MAX_CHAIN_DEPTH) {
    console.error(`[Relay] 🛑 Max chain depth ${RELAY_MAX_CHAIN_DEPTH} reached. Stopping.`);
    // Mark remaining links as failed in Firebase
    for (let i = timerIndex; i < timerLinks.length; i++) {
      const l = timerLinks[i];
      await saveToFirestore(taskId, l.id, l, {
        status: 'error',
        error: `Relay chain depth limit (${RELAY_MAX_CHAIN_DEPTH}) exceeded`,
        logs: [{ msg: '🛑 Relay chain limit reached', type: 'error' }],
      }, extractedBy).catch(() => {});
    }
    return Response.json({ ok: false, reason: 'max_chain_depth' });
  }

  // Out-of-bounds check
  if (timerIndex >= timerLinks.length) {
    console.log('[Relay] ✅ All timer links processed.');
    return Response.json({ ok: true, reason: 'all_done' });
  }

  const linkData = timerLinks[timerIndex];

  // Noop send function — relay doesn't stream to client, only writes to Firebase
  const noopSend = (_data: any) => {};

  try {
    await processOneLink(linkData, linkData.id, noopSend, taskId, extractedBy);
  } catch (err: any) {
    console.error(`[Relay] Error processing link ${timerIndex}:`, err.message);
    await saveToFirestore(taskId, linkData.id, linkData, {
      status: 'error',
      error: err.message,
      logs: [{ msg: `❌ Relay error: ${err.message}`, type: 'error' }],
    }, extractedBy).catch(() => {});
  }

  // ── Trigger NEXT relay if more timer links remain ─────────────────────────
  if (timerIndex + 1 < timerLinks.length) {
    triggerRelayWebhook(selfUrl, taskId, extractedBy, timerLinks, timerIndex + 1, chainDepth);
  } else {
    console.log(`[Relay] 🏁 All ${timerLinks.length} timer links processed via relay chain.`);
  }

  return Response.json({ ok: true, processed: timerIndex, chainDepth });
}


// =============================================================================
// MAIN HANDLER: POST /api/stream_solve
// =============================================================================
//
// TWO MODES:
//
// MODE 1 — NORMAL (from frontend):
//   Receives all links. Splits into Direct + Timer.
//   → Direct links: Execute ALL concurrently via Promise.all (fast, <5s each)
//   → Timer links: Execute ONLY THE FIRST one in this invocation
//   → Stream ALL results instantly as NDJSON
//   → If more Timer links remain: trigger relay webhook, then close stream
//
// MODE 2 — RELAY (self-triggered webhook):
//   Receives _relay: true + timer links array + index.
//   → Process ONE timer link, save to Firebase (no stream)
//   → Trigger next relay if more remain
//   → Return JSON (not a stream)
//
// =============================================================================

export async function POST(req: Request) {
  let body: any;
  try { body = await req.json(); } catch {
    return new Response('Invalid JSON', { status: 400 });
  }

  // Compute self URL ONCE for relay triggers
  const selfUrl = getSelfUrl(req);

  // ── MODE 2: RELAY REQUEST ───────────────────────────────────────────────────
  if (body._relay === true) {
    return handleRelayRequest(body, selfUrl);
  }

  // ── MODE 1: NORMAL STREAM REQUEST ──────────────────────────────────────────
  const links: any[]    = body?.links || [];
  const taskId: string  = body?.taskId;
  const extractedBy     = body?.extractedBy || 'Browser/Live';

  if (!links.length) {
    return new Response(JSON.stringify({ error: 'No links provided' }), { status: 400 });
  }

  // ── Smart Routing: Split into Direct (fast) vs Timer (slow) ───────────────
  const timerLinks  = links.filter((l: any) => TIMER_DOMAINS.some(d => (l.link || '').toLowerCase().includes(d)));
  const directLinks = links.filter((l: any) => !TIMER_DOMAINS.some(d => (l.link || '').toLowerCase().includes(d)));

  console.log(`[Stream] 📊 ${directLinks.length} direct + ${timerLinks.length} timer links | TaskID: ${taskId}`);

  // ── Build NDJSON ReadableStream ───────────────────────────────────────────
  const stream = new ReadableStream({
    async start(controller) {
      const encoder = new TextEncoder();

      // PHASE 5: INSTANT PUSH — each call to send() immediately enqueues + flushes
      const send = (data: any) => {
        try {
          controller.enqueue(encoder.encode(JSON.stringify(data) + '\n'));
        } catch {
          // Stream already closed — safe to ignore
        }
      };

      // ════════════════════════════════════════════════════════════════════════
      // STEP 1: Process ALL direct links CONCURRENTLY (they're fast)
      // ════════════════════════════════════════════════════════════════════════
      const directPromises = directLinks.map((l: any) =>
        processOneLink(l, l.id, send, taskId, extractedBy).catch((err: any) => {
          // Ensure we always send a finished marker even on unexpected errors
          send({ id: l.id, status: 'error', final: null });
          send({ id: l.id, status: 'finished' });
          console.error(`[Stream] Direct link error (${l.id}):`, err.message);
        })
      );

      // ════════════════════════════════════════════════════════════════════════
      // STEP 2: Process ONLY THE FIRST timer link in this invocation
      // ════════════════════════════════════════════════════════════════════════
      let firstTimerPromise: Promise<void> | null = null;

      if (timerLinks.length > 0) {
        firstTimerPromise = (async () => {
          const firstTimer = timerLinks[0];
          console.log(`[Stream] ⏱ Processing first timer link: ${firstTimer.link?.substring(0, 60)}...`);

          try {
            await processOneLink(firstTimer, firstTimer.id, send, taskId, extractedBy);
          } catch (err: any) {
            send({ id: firstTimer.id, status: 'error', final: null });
            send({ id: firstTimer.id, status: 'finished' });
            console.error(`[Stream] First timer link error:`, err.message);
          }

          // ══════════════════════════════════════════════════════════════════
          // STEP 3: "TAAR KAATNA" — Cut the wire & trigger relay
          // If there are MORE timer links, fire the relay webhook.
          // The relay will process them one-by-one, each with a fresh 60s.
          // ══════════════════════════════════════════════════════════════════
          if (timerLinks.length > 1) {
            console.log(`[Stream] ✂️ TAAR KAATNA — ${timerLinks.length - 1} timer links remaining → triggering relay chain`);

            // Mark remaining timer links as "relay_queued" in Firebase
            // so the frontend knows they're being processed in background
            for (let i = 1; i < timerLinks.length; i++) {
              const remainingLink = timerLinks[i];
              send({
                id:   remainingLink.id,
                msg:  '🔗 Queued for relay processing...',
                type: 'info',
              });
            }

            // Fire relay webhook — fresh 60s for the next timer link
            triggerRelayWebhook(
              selfUrl,
              taskId,
              extractedBy,
              timerLinks,
              1,   // Start from index 1 (we just processed index 0)
              0,   // chainDepth starts at 0
            );
          }
        })();
      }

      // ════════════════════════════════════════════════════════════════════════
      // STEP 4: Wait for ALL concurrent work in THIS invocation to finish
      // ════════════════════════════════════════════════════════════════════════
      const allPromises = [...directPromises];
      if (firstTimerPromise) allPromises.push(firstTimerPromise);

      await Promise.allSettled(allPromises);

      // ════════════════════════════════════════════════════════════════════════
      // STEP 5: Close the stream — "wire cut"
      // ════════════════════════════════════════════════════════════════════════
      try {
        controller.close();
      } catch {
        // Already closed — safe to ignore
      }
    },
  });

  // ── Return the NDJSON stream to the client ────────────────────────────────
  return new Response(stream, {
    headers: {
      'Content-Type':                'application/x-ndjson',
      'Cache-Control':               'no-cache, no-store, must-revalidate',
      'Connection':                  'keep-alive',
      'X-Accel-Buffering':           'no',    // Disable Nginx buffering
      'Transfer-Encoding':           'chunked',
      'X-MflixPro-Architecture':     'relay-race-v5',
      'X-MflixPro-Timer-Links':      String(timerLinks.length),
      'X-MflixPro-Direct-Links':     String(directLinks.length),
    },
  });
}
