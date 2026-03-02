import { NextRequest, NextResponse } from 'next/server';
import { db } from '@/lib/firebaseAdmin';
import {
  solveHBLinks,
  solveHubCDN,
  solveHubDrive,
  solveHubCloudNative,
  solveGadgetsWebNative,
} from '@/lib/solvers';
// ALL constants from config — ZERO hardcoded values anywhere
import {
  TIMER_API,
  TIMER_DOMAINS,
  TARGET_DOMAINS,
  LINK_TIMEOUT_MS,
  OVERALL_TIMEOUT_MS,
} from '@/lib/config';
// Phase 4: Link Cache
import { getCachedLink, setCachedLink } from '@/lib/cache';

export const dynamic = 'force-dynamic';
export const maxDuration = 60;

// ─── HELPER 1: fetchWithTimeout ───────────────────────────────────────────────
// FIX: Default timeout is LINK_TIMEOUT_MS (45s) from config.
// Previously hardcoded as 20_000 which killed VPS Timer calls (25-35s).
async function fetchWithTimeout(url: string, timeoutMs = LINK_TIMEOUT_MS): Promise<any> {
  const ctrl  = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      signal:  ctrl.signal,
      headers: { 'User-Agent': 'MflixPro/5.2' },
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

// ─── HELPER 2: saveResultToFirestore ─────────────────────────────────────────
// Atomic transaction on MASTER DOC's links[] array — NO sub-collection.
// RULE: ONLY saves FINAL direct download links or errors. NEVER intermediate URLs.
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
}

// ─── HELPER 3: processLink ────────────────────────────────────────────────────
// Resolves ONE link through the full chain: Timer → HBLinks → HubDrive → HubCloud
// Returns ONLY when chain reaches FINAL direct download link or errors.
// NEVER saves intermediate URLs. If chain times out at 45s, status = 'error'.
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

  // ─── Phase 4: CACHE CHECK — instant return if already resolved ───────────
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
  } catch { /* cache miss — proceed normally */ }

  // ─── Inner solving chain ─────────────────────────────────────────────────────
  const solveWork = async () => {
    // 4a — HubCDN.fans shortcut
    if (currentLink.includes('hubcdn.fans')) {
      logs.push({ msg: '⚡ HubCDN.fans detected — direct solve', type: 'info' });
      const r = await solveHubCDN(currentLink);
      if (r.status === 'success') {
        return { finalLink: r.final_link, status: 'done', logs };
      }
      return { status: 'error', error: r.message, logs };
    }

    // 4b — Timer Bypass Loop (max 3 iterations)
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
        // Generic timer bypass via VPS — USES LINK_TIMEOUT_MS (45s) from config
        logs.push({ msg: `⏱ Timer bypass via VPS (loop ${loopCount + 1})`, type: 'info' });
        try {
          const r = await fetchWithTimeout(
            `${TIMER_API}/solve?url=${encodeURIComponent(currentLink)}`,
            LINK_TIMEOUT_MS,
          );
          if (r.status === 'success' && r.extracted_link) {
            currentLink = r.extracted_link;
            loopCount++;
            continue;
          }
        } catch (timerErr: any) {
          logs.push({ msg: `❌ Timer error: ${timerErr.message}`, type: 'error' });
        }
        logs.push({ msg: `❌ Timer bypass failed`, type: 'error' });
        break;
      }
    }

    // 4c — HBLinks resolver
    if (currentLink.includes('hblinks')) {
      logs.push({ msg: '🔗 HBLinks solving...', type: 'info' });
      const r = await solveHBLinks(currentLink);
      if (r.status === 'success' && r.link) {
        currentLink = r.link;
      } else {
        return { status: 'error', error: r.message || 'HBLinks failed', logs };
      }
    }

    // 4d — HubDrive resolver
    if (currentLink.includes('hubdrive')) {
      logs.push({ msg: '💾 HubDrive solving...', type: 'info' });
      const r = await solveHubDrive(currentLink);
      if (r.status === 'success' && r.link) {
        currentLink = r.link;
      } else {
        return { status: 'error', error: r.message || 'HubDrive failed', logs };
      }
    }

    // 4e — HubCloud / HubCDN final resolver
    if (currentLink.includes('hubcloud') || currentLink.includes('hubcdn')) {
      logs.push({ msg: '☁️ HubCloud solving...', type: 'info' });
      const r = await solveHubCloudNative(currentLink);
      if (r.status === 'success' && r.best_download_link) {
        logs.push({ msg: `✅ Done: ${r.best_download_link.substring(0, 60)}...`, type: 'success' });
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

    // 4f — GDflix / DriveHub
    if (currentLink.includes('gdflix') || currentLink.includes('drivehub')) {
      logs.push({ msg: `✅ Resolved: ${currentLink.substring(0, 60)}...`, type: 'success' });
      return { finalLink: currentLink, status: 'done', logs };
    }

    // 4g — No solver matched
    return { status: 'error', error: 'No solver matched for this URL', logs };
  };

  // ─── Per-link timeout race — LINK_TIMEOUT_MS (45s) ─────────────────────────
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

  // ─── Auto-Retry: attempt 1 fail → attempt 2 automatically ────────────────────
  if (result.status === 'error' && attempt === 1) {
    logs.push({ msg: '🔄 Auto-retrying (attempt 2/2)...', type: 'warn' });
    return processLink(linkData, lid, taskId, extractedBy, 2);
  }

  // ─── Save FINAL result to Firestore (ONLY done or error — NEVER intermediate) ──
  await saveResultToFirestore(taskId, lid, originalUrl, { ...result, logs }, extractedBy);

  // ─── Phase 4: Save to Cache if solved successfully ─────────────────────────
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

// ─── POST /api/solve_task ─────────────────────────────────────────────────────
export async function POST(req: NextRequest) {
  // ─── Auth ─────────────────────────────────────────────────────────────────────
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
  const bodyLinks   = body?.links       as any[] | undefined;
  const extractedBy = (body?.extractedBy as string) || 'Browser/Live';

  if (!taskId) {
    return NextResponse.json({ error: 'taskId is required' }, { status: 400 });
  }

  try {
    const taskSnap = await db.collection('scraping_tasks').doc(taskId).get();
    if (!taskSnap.exists) {
      return NextResponse.json({ error: 'Task not found' }, { status: 404 });
    }

    const data = taskSnap.data()!;

    const allLinks: any[] = (bodyLinks && bodyLinks.length > 0)
      ? bodyLinks
      : (data.links || []);

    const pendingLinks = allLinks.filter(
      (l: any) => !l.status || l.status === 'pending' || l.status === 'processing',
    );

    if (!pendingLinks.length) {
      return NextResponse.json({ ok: true, taskId, processed: 0, done: 0, errors: 0 });
    }

    // ─── Step 1: Mark task as 'processing' ───────────────────────────────────
    await db.collection('scraping_tasks').doc(taskId).update({
      status:              'processing',
      extractedBy:         extractedBy || 'Unknown',
      processingStartedAt: new Date().toISOString(),
    });

    // ─── Step 2: Start overall timer ─────────────────────────────────────────
    const overallStart = Date.now();

    // ─── Step 3: Smart Routing ───────────────────────────────────────────────
    const timerLinks  = pendingLinks.filter((l: any) => TIMER_DOMAINS.some(d => l.link?.includes(d)));
    const directLinks = pendingLinks.filter((l: any) => !TIMER_DOMAINS.some(d => l.link?.includes(d)));

    // 45s hard cap for timer links — 15s buffer before Vercel 60s kill
    const TIME_BUDGET_MS = 45_000;

    // Direct links — ALL PARALLEL (Promise.allSettled)
    const directPromises = directLinks.map((l: any) =>
      processLink(l, l.id, taskId, extractedBy),
    );

    // Timer links — STRICTLY SEQUENTIAL with TIME BUDGET
    const timerPromise = (async () => {
      const timerResults: any[] = [];

      for (let i = 0; i < timerLinks.length; i++) {
        const l = timerLinks[i];

        // Budget check BEFORE processing — if time exceeded, mark remaining as pending
        // (cron will pick them up next run). NO intermediate saves.
        if (Date.now() - overallStart > TIME_BUDGET_MS) {
          await Promise.all(
            timerLinks.slice(i).map((deferred: any) =>
              saveResultToFirestore(taskId, deferred.id, deferred.link, {
                status:    'pending',
                error:     null,
                finalLink: null,
                logs: [{
                  msg: `⏳ Time budget exceeded (${TIME_BUDGET_MS / 1000}s) — deferred to next cron run`,
                  type: 'warn',
                }],
              }, extractedBy),
            ),
          );
          break;
        }

        const r = await processLink(l, l.id, taskId, extractedBy);
        timerResults.push(r);
      }

      return timerResults;
    })();

    // Run both groups concurrently
    const [directSettled, timerResults] = await Promise.all([
      Promise.allSettled(directPromises),
      timerPromise,
    ]);

    // ─── Step 4: Count results ────────────────────────────────────────────────
    const directDone = directSettled.filter(
      r => r.status === 'fulfilled' && (r.value as any)?.status === 'done',
    ).length;
    const timerDone = (timerResults as any[]).filter(
      r => r?.status === 'done' || r?.status === 'success',
    ).length;
    const doneCount  = directDone + timerDone;
    const errorCount = pendingLinks.length - doneCount;

    return NextResponse.json({
      ok:          true,
      taskId,
      processed:   pendingLinks.length,
      done:        doneCount,
      errors:      errorCount,
      directCount: directLinks.length,
      timerCount:  timerLinks.length,
    });
  } catch (err: any) {
    return NextResponse.json({ error: err.message }, { status: 500 });
  }
}
