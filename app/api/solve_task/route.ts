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
  RELAY_MAX_CHAIN_DEPTH,
} from '@/lib/config';
import { getCachedLink, setCachedLink } from '@/lib/cache';

export const maxDuration = 60;

// =============================================================================
// PHASE 5.2: "TAAR KAATNA" (RELAY RACE) — CLEAN ARCHITECTURE
// =============================================================================
//
// RULES:
//   1. Direct links (hblinks, hubdrive, hubcloud, gdflix) → ALL concurrent
//   2. Timer links (gadgetsweb, review-tech, etc.) → ONE per invocation
//   3. After first timer link resolves to FINAL direct download link → save → relay
//   4. NEVER save intermediate URLs. If chain times out at 45s → status = 'error'
//   5. Each relay invocation gets a fresh 60s Vercel timer
//   6. Flush/stream results instantly via NDJSON
//
// EXECUTION MODEL:
//   Invocation 1 (stream to frontend):
//     → Process ALL direct links concurrently
//     → Process FIRST timer link through FULL chain to FINAL link
//     → Save FINAL link to Firebase → Trigger relay for remaining timers
//     → Close stream
//
//   Invocation 2+ (relay, no stream):
//     → Process ONE timer link through FULL chain to FINAL link
//     → Save FINAL link to Firebase → Trigger relay for next timer
//     → Return JSON
// =============================================================================

// ─── HELPER: fetchJSON ────────────────────────────────────────────────────────
async function fetchJSON(url: string, timeoutMs = LINK_TIMEOUT_MS): Promise<any> {
  const ctrl  = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      signal:  ctrl.signal,
      headers: { 'User-Agent': 'MflixPro/5.2-Relay' },
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

// ─── HELPER: getSelfUrl ──────────────────────────────────────────────────────
function getSelfUrl(req: Request): string {
  const proto = req.headers.get('x-forwarded-proto') || 'https';
  const host  = req.headers.get('x-forwarded-host') || req.headers.get('host') || '';
  if (host) return `${proto}://${host}/api/stream_solve`;
  try {
    const parsed = new URL(req.url);
    return `${parsed.origin}/api/stream_solve`;
  } catch {
    const vercelUrl = process.env.VERCEL_URL;
    if (vercelUrl) return `https://${vercelUrl}/api/stream_solve`;
    return 'https://localhost:3000/api/stream_solve';
  }
}

// ─── HELPER: saveToFirestore ─────────────────────────────────────────────────
// RULE: ONLY saves FINAL direct download links (status='done') or errors (status='error').
// NEVER saves intermediate URLs (hblinks, hubdrive, etc.)
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
// =============================================================================
// Resolves ONE link through the FULL chain: Timer → HBLinks → HubDrive → HubCloud
// Returns ONLY when chain reaches FINAL direct download link or errors.
// NEVER returns intermediate URLs. If chain times out at 45s → status = 'error'.
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
    send({ id: lid, msg, type });
  };

  // ── Phase 4: CACHE CHECK ──────────────────────────────────────────────────
  try {
    const cached = await getCachedLink(originalUrl);
    if (cached && cached.finalLink) {
      log('⚡ CACHE HIT — resolved in 0ms', 'success');
      // Save cache hit to Firestore
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
        } catch { /* non-fatal */ }
      }
      send({ id: lid, status: 'done', final: cached.finalLink, best_button_name: cached.best_button_name });
      send({ id: lid, status: 'finished' });
      return { status: 'done', finalLink: cached.finalLink, best_button_name: cached.best_button_name, fromCache: true };
    }
  } catch { /* cache miss */ }

  // ── Main solving logic — runs full chain, returns FINAL link or error ──────
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

      // ── Timer bypass loop (max 3 iterations) ─────────────────────────────
      const TARGET_CHECK = ['hblinks', 'hubdrive', 'hubcdn', 'hubcloud', 'gdflix', 'drivehub'];
      let loopCount = 0;

      while (loopCount < 3 && !(TARGET_CHECK.some(d => currentLink.includes(d)))) {
        if (!TIMER_DOMAINS.some(d => currentLink.includes(d)) && loopCount === 0) break;

        if (currentLink.includes('gadgetsweb')) {
          log(`🔁 GadgetsWeb native solve (loop ${loopCount + 1})`);
          const r = await solveGadgetsWebNative(currentLink);
          if (r.status === 'success' && r.link) {
            currentLink = r.link;
            loopCount++;
            continue;
          }
          log(`❌ GadgetsWeb failed: ${r.message}`, 'error');
          break;
        } else {
          log(`⏱ Timer bypass via VPS (loop ${loopCount + 1})`);
          try {
            const r = await fetchJSON(
              `${TIMER_API}/solve?url=${encodeURIComponent(currentLink)}`,
              LINK_TIMEOUT_MS,
            );
            if (r.status === 'success' && r.extracted_link) {
              currentLink = r.extracted_link;
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
        } else {
          return { status: 'error', error: r.message || 'HubDrive failed', logs };
        }
      }

      // ── HubCloud / HubCDN final resolver ──────────────────────────────────
      if (currentLink.includes('hubcloud') || currentLink.includes('hubcdn')) {
        log('☁️ HubCloud solving...');
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

      // ── GDflix / DriveHub ──────────────────────────────────────────────────
      if (currentLink.includes('gdflix') || currentLink.includes('drivehub')) {
        log(`✅ Resolved: ${currentLink.substring(0, 60)}...`, 'success');
        return { finalLink: currentLink, status: 'done', logs };
      }

      // ── Fallback — no solver matched ──────────────────────────────────────
      return { status: 'error', error: 'No solver matched for this URL', logs };
    };

    // Race solver against LINK_TIMEOUT_MS (45s)
    resultPayload = await Promise.race([
      solving(),
      new Promise<any>((_, rej) =>
        setTimeout(() => rej(new Error(`Timed out after ${LINK_TIMEOUT_MS / 1000}s`)), LINK_TIMEOUT_MS),
      ),
    ]);
  } catch (err: any) {
    resultPayload = { status: 'error', error: err.message, logs };
  }

  // ── Stream push result ────────────────────────────────────────────────────
  send({
    id:               lid,
    status:           resultPayload.status,
    final:            resultPayload.finalLink || null,
    best_button_name: resultPayload.best_button_name || null,
  });

  // ── Save FINAL link to Firestore (ONLY done or error — NEVER intermediate) ─
  try {
    await saveToFirestore(taskId, lid, linkData, resultPayload, extractedBy);
  } catch { /* non-fatal */ }

  // ── Cache ─────────────────────────────────────────────────────────────────
  if (resultPayload.status === 'done' && resultPayload.finalLink) {
    try {
      await setCachedLink(originalUrl, resultPayload.finalLink, 'stream_solve', {
        best_button_name:      resultPayload.best_button_name,
        all_available_buttons: resultPayload.all_available_buttons,
      });
    } catch { /* non-critical */ }
  }

  // ── Finished marker ───────────────────────────────────────────────────────
  send({ id: lid, status: 'finished' });

  return resultPayload;
}


// =============================================================================
// RELAY TRIGGER: Fire-and-forget POST to self
// =============================================================================
// Triggers a fresh invocation → fresh 60s Vercel timer.
// Does NOT await response — fire and forget.

function triggerRelayWebhook(
  selfUrl: string,
  taskId: string,
  extractedBy: string,
  timerLinks: any[],
  timerIndex: number,
  chainDepth: number,
): void {
  const relayBody = JSON.stringify({
    _relay:      true,
    _timerLinks: timerLinks,
    _timerIndex: timerIndex,
    _chainDepth: chainDepth,
    taskId,
    extractedBy,
  });

  console.log(`[Relay] ✂️ TAAR KAATNA — timer link ${timerIndex + 1}/${timerLinks.length} → fresh 60s`);

  // Fire-and-forget — do NOT await
  fetch(selfUrl, {
    method:  'POST',
    headers: { 'Content-Type': 'application/json' },
    body:    relayBody,
  }).catch((err) => {
    console.error(`[Relay] ❌ Trigger failed:`, err.message);
  });
}


// =============================================================================
// RELAY HANDLER: Processes ONE timer link per invocation (fresh 60s)
// =============================================================================

async function handleRelayRequest(body: any, selfUrl: string): Promise<Response> {
  const {
    _timerLinks: timerLinks,
    _timerIndex: timerIndex = 0,
    _chainDepth: chainDepth = 0,
    taskId,
    extractedBy,
  } = body;

  // Safety: Prevent infinite chains
  if (chainDepth >= RELAY_MAX_CHAIN_DEPTH) {
    console.error(`[Relay] 🛑 Max chain depth ${RELAY_MAX_CHAIN_DEPTH} reached. Stopping.`);
    return Response.json({ ok: false, reason: 'max_chain_depth' });
  }

  // Bounds check
  if (!timerLinks || timerIndex >= timerLinks.length) {
    return Response.json({ ok: true, reason: 'all_timer_links_done' });
  }

  const linkData = timerLinks[timerIndex];
  console.log(`[Relay] ⚡ Processing timer ${timerIndex + 1}/${timerLinks.length}: ${linkData.link?.substring(0, 60)}`);

  // No-op send — relay has no stream to push to
  const noopSend = (_data: any) => {};

  try {
    // Process this ONE timer link through the FULL chain to FINAL link or error.
    // processOneLink handles: Timer bypass → HBLinks → HubDrive → HubCloud → save to Firebase
    await processOneLink(linkData, linkData.id, noopSend, taskId, extractedBy);
  } catch (err: any) {
    console.error(`[Relay] ❌ Timer ${timerIndex + 1} error:`, err.message);
    // Save error to Firebase — NO intermediate link
    await saveToFirestore(taskId, linkData.id, linkData, {
      status: 'error',
      error:  err.message,
      logs:   [{ msg: `❌ Relay error: ${err.message}`, type: 'error' }],
    }, extractedBy).catch(() => {});
  }

  // ── TAAR KAATNA: If more timer links remain, trigger NEXT relay ─────────
  // Each relay gets a FRESH 60s Vercel timer.
  if (timerIndex + 1 < timerLinks.length) {
    triggerRelayWebhook(
      selfUrl,
      taskId,
      extractedBy,
      timerLinks,
      timerIndex + 1,
      chainDepth + 1,
    );
  }

  return Response.json({
    ok:        true,
    processed: timerIndex + 1,
    total:     timerLinks.length,
    remaining: timerLinks.length - (timerIndex + 1),
  });
}


// =============================================================================
// MAIN HANDLER: POST /api/stream_solve
// =============================================================================

export async function POST(req: Request) {
  let body: any;
  try { body = await req.json(); } catch {
    return new Response('Invalid JSON', { status: 400 });
  }

  const selfUrl = getSelfUrl(req);

  // ── MODE 2: RELAY REQUEST (triggered by self — fresh 60s) ─────────────────
  if (body._relay === true) {
    return handleRelayRequest(body, selfUrl);
  }

  // ── MODE 1: NORMAL STREAM REQUEST (from frontend) ─────────────────────────
  const links: any[]    = body?.links || [];
  const taskId: string  = body?.taskId;
  const extractedBy     = body?.extractedBy || 'Browser/Live';

  if (!links.length) {
    return new Response(JSON.stringify({ error: 'No links provided' }), { status: 400 });
  }

  // Split into timer and direct links
  const timerLinks  = links.filter((l: any) => TIMER_DOMAINS.some(d => (l.link || '').toLowerCase().includes(d)));
  const directLinks = links.filter((l: any) => !TIMER_DOMAINS.some(d => (l.link || '').toLowerCase().includes(d)));

  console.log(`[Stream] 📊 ${directLinks.length} direct + ${timerLinks.length} timer links | TaskID: ${taskId}`);

  const stream = new ReadableStream({
    async start(controller) {
      const encoder = new TextEncoder();

      // Instant NDJSON push — zero buffering
      const send = (data: any) => {
        try {
          controller.enqueue(encoder.encode(JSON.stringify(data) + '\n'));
        } catch { /* stream closed */ }
      };

      // ═══════════════════════════════════════════════════════════════════════
      // STEP 1: Process ALL direct links CONCURRENTLY
      // ═══════════════════════════════════════════════════════════════════════
      const directPromises = directLinks.map((l: any) =>
        processOneLink(l, l.id, send, taskId, extractedBy).catch((err: any) => {
          send({ id: l.id, status: 'error', final: null });
          send({ id: l.id, status: 'finished' });
          console.error(`[Stream] Direct link error (${l.id}):`, err.message);
        })
      );

      // ═══════════════════════════════════════════════════════════════════════
      // STEP 2: Process FIRST timer link in THIS invocation (full chain)
      // ═══════════════════════════════════════════════════════════════════════
      let firstTimerPromise: Promise<void> | null = null;

      if (timerLinks.length > 0) {
        firstTimerPromise = (async () => {
          const firstTimer = timerLinks[0];
          console.log(`[Stream] ⏱ Processing first timer: ${firstTimer.link?.substring(0, 60)}...`);

          try {
            // Process through FULL chain — Timer → HBLinks → HubDrive → HubCloud
            // Returns ONLY with FINAL direct download link or error.
            await processOneLink(firstTimer, firstTimer.id, send, taskId, extractedBy);
          } catch (err: any) {
            send({ id: firstTimer.id, status: 'error', final: null });
            send({ id: firstTimer.id, status: 'finished' });
            console.error(`[Stream] First timer error:`, err.message);
          }

          // ═════════════════════════════════════════════════════════════════
          // STEP 3: TAAR KAATNA — Relay remaining timer links
          // ═════════════════════════════════════════════════════════════════
          // First timer link is DONE (saved to Firebase with FINAL link).
          // Now CUT THE WIRE — trigger relay for remaining timer links.
          // Each relay gets a FRESH 60s Vercel timer.

          if (timerLinks.length > 1) {
            console.log(`[Stream] ✂️ TAAR KAATNA — ${timerLinks.length - 1} remaining timer links → relay chain`);

            // Notify frontend that remaining timer links are queued for relay
            for (let i = 1; i < timerLinks.length; i++) {
              send({
                id:   timerLinks[i].id,
                msg:  '🔗 Queued for relay processing...',
                type: 'info',
              });
            }

            // Fire-and-forget relay trigger — fresh 60s for link #2
            triggerRelayWebhook(
              selfUrl,
              taskId,
              extractedBy,
              timerLinks,
              1,       // Start from index 1 (first timer already done)
              0,       // Chain depth starts at 0
            );
          }
        })();
      }

      // ═══════════════════════════════════════════════════════════════════════
      // STEP 4: Wait for ALL work in THIS invocation to complete
      // ═══════════════════════════════════════════════════════════════════════
      const allPromises = [...directPromises];
      if (firstTimerPromise) allPromises.push(firstTimerPromise);

      await Promise.allSettled(allPromises);

      // ═══════════════════════════════════════════════════════════════════════
      // STEP 5: Close stream
      // ═══════════════════════════════════════════════════════════════════════
      try { controller.close(); } catch { /* already closed */ }
    },
  });

  return new Response(stream, {
    headers: {
      'Content-Type':            'application/x-ndjson',
      'Cache-Control':           'no-cache, no-store, must-revalidate',
      'Connection':              'keep-alive',
      'X-Accel-Buffering':       'no',
      'Transfer-Encoding':       'chunked',
      'X-MflixPro-Architecture': 'relay-race-v5.2',
      'X-MflixPro-Timer-Links':  String(timerLinks.length),
      'X-MflixPro-Direct-Links': String(directLinks.length),
    },
  });
}
