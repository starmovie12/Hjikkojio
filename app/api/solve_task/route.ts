import { NextRequest, NextResponse } from 'next/server';
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
  RELAY_MAX_CHAIN_DEPTH,
} from '@/lib/config';
import { getCachedLink, setCachedLink } from '@/lib/cache';

export const dynamic = 'force-dynamic';
export const maxDuration = 60;

// ═══════════════════════════════════════════════════════════════════════════════
// PHASE 6: "RELAY RACE" (Taar Kaatna) ARCHITECTURE
// ═══════════════════════════════════════════════════════════════════════════════
//
// PROBLEM:  Vercel kills serverless functions at 60s. Timer links take 20-35s
//           each. 2+ timer links = VPS crash + Vercel timeout.
//
// SOLUTION: Process ONE timer link per API call. After each link:
//           1. Save result to Firebase
//           2. "Cut the wire" — respond to current request
//           3. Fire a NEW HTTP request (relay) for the next timer link
//           Each relay gets a fresh 60-second Vercel budget.
//
// RULES:
//   • Timer links:  STRICTLY SEQUENTIAL — one at a time, never parallel
//   • Direct links: CONCURRENT via Promise.allSettled (they're fast)
//   • Database:     ONLY save final direct download link — no intermediate URLs
//
// FLOW:
//   Browser/Cron → POST /api/solve_task { taskId, links }
//     ├─ Direct links → Promise.allSettled (parallel, fast)
//     ├─ Timer link #0 → processLink() → save → RELAY to timer #1
//     │   └─ Timer link #1 → processLink() → save → RELAY to timer #2
//     │       └─ Timer link #2 → processLink() → save → DONE
//     └─ Return summary
// ═══════════════════════════════════════════════════════════════════════════════

// ─── HELPER: fetchWithTimeout ───────────────────────────────────────────────
// VPS API call with AbortController. Timeout from config (50s), NOT hardcoded.
async function fetchWithTimeout(url: string, timeoutMs = LINK_TIMEOUT_MS): Promise<any> {
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

// ─── HELPER: saveResultToFirestore ─────────────────────────────────────────
// Atomic transaction on MASTER DOC's links[] array.
// ONLY saves FINAL direct download link — no intermediate URLs.
export async function saveResultToFirestore(
  taskId: string,
  lid: number | string,
  linkUrl: string,
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
  const taskRef = db.collection('scraping_tasks').doc(taskId);

  await db.runTransaction(async (tx) => {
    const snap = await tx.get(taskRef);
    if (!snap.exists) return;

    const existing = snap.data()?.links || [];
    const updated = existing.map((l: any) => {
      if (l.id === lid || l.link === linkUrl) {
        return {
          ...l,
          // ONLY save finalLink if it's a real direct download link
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

    // Check if ALL links are now in terminal state
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
}

// ─── HELPER: processLink ────────────────────────────────────────────────────
// Solves a SINGLE link through the full chain:
//   Timer bypass → HBLinks → HubDrive → HubCloud → Final Download Link
// Auto-retry on first failure (attempt 2/2).
export async function processLink(
  linkData: any,
  lid: number | string,
  taskId: string,
  extractedBy: string,
  attempt = 1,
): Promise<{ lid: number | string; status: string; finalLink?: string }> {
  const originalUrl = linkData.link;
  let   currentLink = originalUrl;
  const logs: { msg: string; type: string }[] = [];

  // ─── CACHE CHECK — instant return if already resolved ─────────────────
  try {
    const cached = await getCachedLink(originalUrl);
    if (cached && cached.finalLink) {
      logs.push({ msg: `⚡ CACHE HIT — resolved in 0ms`, type: 'success' });
      await saveResultToFirestore(taskId, lid, originalUrl, {
        status: 'done',
        finalLink: cached.finalLink,
        best_button_name: cached.best_button_name,
        all_available_buttons: cached.all_available_buttons,
        logs,
      }, extractedBy);
      return { lid, status: 'done', finalLink: cached.finalLink };
    }
  } catch { /* cache miss — proceed */ }

  // ─── Inner solving chain ──────────────────────────────────────────────
  const solveWork = async () => {
    // Step A — HubCDN.fans shortcut
    if (currentLink.includes('hubcdn.fans')) {
      logs.push({ msg: '⚡ HubCDN.fans detected — direct solve', type: 'info' });
      const r = await solveHubCDN(currentLink);
      if (r.status === 'success') {
        return { finalLink: r.final_link, status: 'done', logs };
      }
      return { status: 'error', error: r.message, logs };
    }

    // Step B — Timer Bypass Loop (max 3 iterations)
    // gadgetsweb → native solve | others → VPS port 10000
    let loopCount = 0;
    while (loopCount < 3 && !TARGET_DOMAINS.some(d => currentLink.includes(d))) {
      if (!TIMER_DOMAINS.some(d => currentLink.includes(d)) && loopCount === 0) break;

      if (currentLink.includes('gadgetsweb')) {
        logs.push({ msg: `🔁 GadgetsWeb native solve (loop ${loopCount + 1})`, type: 'info' });
        const r = await solveGadgetsWebNative(currentLink);
        if (r.status === 'success' && r.link) {
          currentLink = r.link;
          loopCount++;
          continue;
        }
        logs.push({ msg: `❌ GadgetsWeb failed: ${r.message}`, type: 'error' });
        break;
      } else {
        // VPS Timer bypass — timeout from config (50s), NOT hardcoded 20s
        logs.push({ msg: `⏱ Timer bypass via VPS (loop ${loopCount + 1})`, type: 'info' });
        const r = await fetchWithTimeout(
          `${TIMER_API}/solve?url=${encodeURIComponent(currentLink)}`,
          LINK_TIMEOUT_MS,
        );
        if (r.status === 'success' && r.extracted_link) {
          currentLink = r.extracted_link;
          loopCount++;
          continue;
        }
        logs.push({ msg: `❌ Timer bypass failed`, type: 'error' });
        break;
      }
    }

    // Step C — HBLinks resolver
    if (currentLink.includes('hblinks')) {
      logs.push({ msg: '🔗 HBLinks solving...', type: 'info' });
      const r = await solveHBLinks(currentLink);
      if (r.status === 'success' && r.link) {
        currentLink = r.link;
        logs.push({ msg: `✅ HBLinks → ${r.source || 'resolved'}`, type: 'info' });
      } else {
        return { status: 'error', error: r.message || 'HBLinks failed', logs };
      }
    }

    // Step D — HubDrive resolver
    if (currentLink.includes('hubdrive')) {
      logs.push({ msg: '💾 HubDrive solving...', type: 'info' });
      const r = await solveHubDrive(currentLink);
      if (r.status === 'success' && r.link) {
        currentLink = r.link;
        logs.push({ msg: '✅ HubDrive → resolved', type: 'info' });
      } else {
        return { status: 'error', error: r.message || 'HubDrive failed', logs };
      }
    }

    // Step E — HubCloud / HubCDN final resolver (VPS port 5001)
    if (currentLink.includes('hubcloud') || currentLink.includes('hubcdn')) {
      logs.push({ msg: '☁️ HubCloud solving via VPS...', type: 'info' });
      const r = await solveHubCloudNative(currentLink);
      if (r.status === 'success' && r.best_download_link) {
        logs.push({ msg: `✅ Done: ${r.best_download_link}`, type: 'success' });
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

    // Step F — GDflix / DriveHub (already final)
    if (currentLink.includes('gdflix') || currentLink.includes('drivehub')) {
      logs.push({ msg: `✅ GDflix/DriveHub resolved: ${currentLink}`, type: 'success' });
      return { finalLink: currentLink, status: 'done', logs };
    }

    // Step G — No solver matched
    return { status: 'error', error: 'No solver matched for this URL', logs };
  };

  // ─── Per-link timeout race (50s from config) ─────────────────────────
  let result: any;
  try {
    result = await Promise.race([
      solveWork(),
      new Promise<never>((_, reject) =>
        setTimeout(
          () => reject(new Error(`Timed out after ${LINK_TIMEOUT_MS / 1000}s`)),
          LINK_TIMEOUT_MS,
        ),
      ),
    ]);
  } catch (err: any) {
    result = { status: 'error', error: err.message, logs };
  }

  // ─── Auto-Retry: attempt 1 fail → attempt 2 ─────────────────────────
  if (result.status === 'error' && attempt === 1) {
    logs.push({ msg: '🔄 Auto-retrying (attempt 2/2)...', type: 'warn' });
    return processLink(linkData, lid, taskId, extractedBy, 2);
  }

  // ─── Save FINAL result to Firestore ──────────────────────────────────
  await saveResultToFirestore(taskId, lid, originalUrl, { ...result, logs }, extractedBy);

  // ─── Cache successful result ─────────────────────────────────────────
  if (result.status === 'done' && result.finalLink) {
    try {
      await setCachedLink(originalUrl, result.finalLink, 'solve_task', {
        best_button_name: result.best_button_name,
        all_available_buttons: result.all_available_buttons,
      });
    } catch { /* cache write non-critical */ }
  }

  return { lid, status: result.status, finalLink: result.finalLink };
}

// ─── HELPER: triggerRelay ───────────────────────────────────────────────────
// "Taar Kaatna" — Fire a fresh HTTP request for the next timer link.
// This gives the next link a fresh 60s Vercel budget.
async function triggerRelay(
  taskId: string,
  timerLinks: any[],
  nextTimerIndex: number,
  extractedBy: string,
  relayDepth: number,
): Promise<void> {
  // Safety: Max relay depth to prevent infinite loops
  if (nextTimerIndex >= timerLinks.length) return;
  if (relayDepth >= RELAY_MAX_CHAIN_DEPTH) {
    console.warn(`[Relay] Max depth ${RELAY_MAX_CHAIN_DEPTH} reached — stopping chain`);
    return;
  }

  const baseUrl = process.env.VERCEL_URL
    ? `https://${process.env.VERCEL_URL}`
    : process.env.NEXT_PUBLIC_BASE_URL || 'http://localhost:3000';

  try {
    // Fire-and-forget — we don't wait for the relay response
    // The relay will process the next timer link in its own 60s window
    fetch(`${baseUrl}/api/solve_task`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-mflix-internal': 'true',
      },
      body: JSON.stringify({
        taskId,
        timerLinks,
        timerIndex:  nextTimerIndex,
        extractedBy,
        relayDepth:  relayDepth + 1,
        mode:        'relay',
      }),
    }).catch(err => {
      console.error(`[Relay] Failed to trigger relay #${nextTimerIndex}:`, err.message);
    });
  } catch (err: any) {
    console.error(`[Relay] triggerRelay error:`, err.message);
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// POST /api/solve_task — MAIN HANDLER
// ═══════════════════════════════════════════════════════════════════════════════
//
// MODE 1: Initial call (mode !== 'relay')
//   Body: { taskId, links, extractedBy }
//   Action: Process ALL direct links concurrently + FIRST timer link
//           Then relay to next timer link
//
// MODE 2: Relay call (mode === 'relay')
//   Body: { taskId, timerLinks, timerIndex, extractedBy, relayDepth }
//   Action: Process ONE timer link at timerIndex
//           Then relay to next timer link
// ═══════════════════════════════════════════════════════════════════════════════
export async function POST(req: NextRequest) {
  // ─── Auth check ───────────────────────────────────────────────────────
  const cronSecret = process.env.CRON_SECRET;
  if (cronSecret) {
    const authHeader     = req.headers.get('Authorization') || '';
    const internalHeader = req.headers.get('x-mflix-internal') || '';
    const isBearer   = authHeader === `Bearer ${cronSecret}`;
    const isInternal = internalHeader === 'true';
    if (!isBearer && !isInternal) {
      return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
    }
  }

  let body: any;
  try {
    body = await req.json();
  } catch {
    return NextResponse.json({ error: 'Invalid JSON in request body' }, { status: 400 });
  }

  const taskId      = body?.taskId      as string;
  const extractedBy = (body?.extractedBy as string) || 'Browser/Live';
  const mode        = (body?.mode        as string) || 'initial';

  if (!taskId) {
    return NextResponse.json({ error: 'taskId is required' }, { status: 400 });
  }

  try {
    // ═════════════════════════════════════════════════════════════════════
    // MODE 2: RELAY — Process exactly ONE timer link
    // ═════════════════════════════════════════════════════════════════════
    if (mode === 'relay') {
      const timerLinks  = body?.timerLinks  as any[];
      const timerIndex  = body?.timerIndex  as number;
      const relayDepth  = (body?.relayDepth as number) || 0;

      if (!timerLinks || timerIndex === undefined || timerIndex >= timerLinks.length) {
        return NextResponse.json({ ok: true, message: 'No more timer links to process' });
      }

      const link = timerLinks[timerIndex];
      console.log(`[Relay #${relayDepth}] Processing timer link ${timerIndex + 1}/${timerLinks.length}: ${link.link}`);

      // Process this ONE timer link (gets full 50s timeout)
      const result = await processLink(link, link.id, taskId, extractedBy);

      console.log(`[Relay #${relayDepth}] Timer link ${timerIndex + 1} → ${result.status}`);

      // Trigger relay for NEXT timer link (fresh 60s)
      if (timerIndex + 1 < timerLinks.length) {
        await triggerRelay(taskId, timerLinks, timerIndex + 1, extractedBy, relayDepth);
      }

      return NextResponse.json({
        ok:          true,
        mode:        'relay',
        taskId,
        timerIndex,
        status:      result.status,
        relayDepth,
        remaining:   timerLinks.length - timerIndex - 1,
      });
    }

    // ═════════════════════════════════════════════════════════════════════
    // MODE 1: INITIAL — Process direct links + first timer link
    // ═════════════════════════════════════════════════════════════════════
    const taskSnap = await db.collection('scraping_tasks').doc(taskId).get();
    if (!taskSnap.exists) {
      return NextResponse.json({ error: 'Task not found' }, { status: 404 });
    }

    const data = taskSnap.data()!;

    // Get links from body or Firestore
    const bodyLinks = body?.links as any[] | undefined;
    const allLinks: any[] = (bodyLinks && bodyLinks.length > 0)
      ? bodyLinks
      : (data.links || []);

    // Filter: Only pending/processing links (skip already done/success)
    const pendingLinks = allLinks.filter(
      (l: any) => !l.status || l.status === 'pending' || l.status === 'processing',
    );

    if (!pendingLinks.length) {
      return NextResponse.json({ ok: true, taskId, processed: 0, done: 0, errors: 0 });
    }

    // Mark task as 'processing'
    await db.collection('scraping_tasks').doc(taskId).update({
      status:              'processing',
      extractedBy:         extractedBy,
      processingStartedAt: new Date().toISOString(),
    });

    // ─── Smart Routing: Separate timer vs direct ────────────────────────
    const timerLinks  = pendingLinks.filter((l: any) => TIMER_DOMAINS.some(d => l.link?.includes(d)));
    const directLinks = pendingLinks.filter((l: any) => !TIMER_DOMAINS.some(d => l.link?.includes(d)));

    console.log(`[solve_task] Task ${taskId}: ${directLinks.length} direct + ${timerLinks.length} timer links`);

    // ─── Direct links → CONCURRENT (Promise.allSettled) ─────────────────
    // These are fast (HBLinks, HubDrive, HubCloud) — safe to run in parallel
    const directPromises = directLinks.map((l: any) =>
      processLink(l, l.id, taskId, extractedBy),
    );

    // ─── Timer link #0 → SEQUENTIAL (first one only) ───────────────────
    // Process ONLY the first timer link in this invocation
    let firstTimerResult: any = null;
    if (timerLinks.length > 0) {
      console.log(`[solve_task] Processing first timer link: ${timerLinks[0].link}`);
      firstTimerResult = await processLink(timerLinks[0], timerLinks[0].id, taskId, extractedBy);
      console.log(`[solve_task] First timer link → ${firstTimerResult.status}`);
    }

    // Wait for all direct links to finish
    const directSettled = await Promise.allSettled(directPromises);

    // ─── Trigger relay for remaining timer links ────────────────────────
    // Each gets a fresh 60s window — VPS will never get overloaded
    if (timerLinks.length > 1) {
      console.log(`[solve_task] Triggering relay for ${timerLinks.length - 1} remaining timer links`);
      await triggerRelay(taskId, timerLinks, 1, extractedBy, 0);
    }

    // ─── Count results ──────────────────────────────────────────────────
    const directDone = directSettled.filter(
      r => r.status === 'fulfilled' && (r.value as any)?.status === 'done',
    ).length;
    const timerDone = firstTimerResult?.status === 'done' ? 1 : 0;
    const doneCount = directDone + timerDone;

    // Note: remaining timer links will be processed by relay chain
    // Their results will appear in Firestore as each relay completes

    return NextResponse.json({
      ok:           true,
      taskId,
      processed:    directLinks.length + (timerLinks.length > 0 ? 1 : 0),
      done:         doneCount,
      errors:       (directLinks.length + (timerLinks.length > 0 ? 1 : 0)) - doneCount,
      directCount:  directLinks.length,
      timerCount:   timerLinks.length,
      relayPending: Math.max(0, timerLinks.length - 1),
    });

  } catch (err: any) {
    console.error('[solve_task] Error:', err.message);
    return NextResponse.json({ error: err.message }, { status: 500 });
  }
}
