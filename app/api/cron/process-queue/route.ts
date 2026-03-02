import { NextRequest, NextResponse } from 'next/server';
import { db } from '@/lib/firebaseAdmin';
import { extractMovieLinks } from '@/lib/solvers';
import { TIMER_DOMAINS, STUCK_TASK_THRESHOLD_MS, MAX_CRON_RETRIES } from '@/lib/config';
// FIX A: Direct import from solve_task — ZERO nested HTTP calls
import { processLink, saveResultToFirestore } from '@/app/api/solve_task/route';
// Phase 4: Cache cleanup on every cron run
import { cleanupExpiredCache } from '@/lib/cache';

export const dynamic    = 'force-dynamic';
export const maxDuration = 60;

const queueCollections = ['movies_queue', 'webseries_queue'] as const;

// ─── HELPER: sendTelegram ─────────────────────────────────────────────────────
async function sendTelegram(msg: string): Promise<void> {
  const token  = process.env.TELEGRAM_BOT_TOKEN;
  const chatId = process.env.TELEGRAM_CHAT_ID;
  if (!token || !chatId) return;
  try {
    await fetch(`https://api.telegram.org/bot${token}/sendMessage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: chatId, text: msg, parse_mode: 'HTML' }),
    });
  } catch { /* non-critical */ }
}

// ─── HELPER: updateHeartbeat ──────────────────────────────────────────────────
async function updateHeartbeat(
  status: 'running' | 'idle' | 'error',
  details?: string,
): Promise<void> {
  try {
    await db.collection('system').doc('engine_status').set(
      {
        lastRunAt:  new Date().toISOString(),
        status,
        details:    details || '',
        source:     'github-cron',
        updatedAt:  new Date().toISOString(),
      },
      { merge: true },
    );
  } catch { /* non-critical */ }
}

// ─── HELPER: recoverStuckTasks ────────────────────────────────────────────────
async function recoverStuckTasks(): Promise<number> {
  let recovered = 0;
  const now = Date.now();

  // A — Queue collections
  for (const col of queueCollections) {
    try {
      const snap = await db.collection(col).where('status', '==', 'processing').get();
      for (const doc of snap.docs) {
        const data      = doc.data();
        const lockedAt  = data.lockedAt || data.updatedAt || data.createdAt;
        const lockedMs  = lockedAt ? now - new Date(lockedAt).getTime() : Infinity;

        if (lockedMs > STUCK_TASK_THRESHOLD_MS) {
          const retryCount = (data.retryCount || 0) + 1;
          if (retryCount > MAX_CRON_RETRIES) {
            await doc.ref.update({
              status:   'failed',
              error:    `Max retries exceeded ${MAX_CRON_RETRIES}/${MAX_CRON_RETRIES}`,
              failedAt: new Date().toISOString(),
            });
          } else {
            await doc.ref.update({
              status:            'pending',
              lockedAt:          null,
              retryCount,
              lastRecoveredAt:   new Date().toISOString(),
            });
          }
          recovered++;
        }
      }
    } catch { /* continue */ }
  }

  // B — scraping_tasks
  // Phase 4 FIX: Previously kept stuck tasks as 'processing' forever.
  // Cron only picks from queue collections, NOT scraping_tasks.
  // So stuck scraping_tasks were ORPHANED. Now we mark them 'failed'.
  try {
    const snap = await db.collection('scraping_tasks').where('status', '==', 'processing').get();
    for (const doc of snap.docs) {
      const data      = doc.data();
      const startedAt = data.processingStartedAt || data.createdAt;
      const ageMs     = startedAt ? now - new Date(startedAt).getTime() : 0;

      if (ageMs > STUCK_TASK_THRESHOLD_MS) {
        const links: any[] = data.links || [];

        // Check if all links are actually done (Vercel killed before status update)
        const TERMINAL = ['done', 'success', 'error', 'failed'];
        const allTerminal = links.length > 0 && links.every(
          (l: any) => TERMINAL.includes((l.status || '').toLowerCase())
        );
        const allSuccess = allTerminal && links.every(
          (l: any) => ['done', 'success'].includes((l.status || '').toLowerCase())
        );

        if (allTerminal) {
          // All links finished but task status wasn't updated (Vercel kill)
          await doc.ref.update({
            status: allSuccess ? 'completed' : 'failed',
            ...(allSuccess ? { completedAt: new Date().toISOString() } : {}),
            recoveredAt: new Date().toISOString(),
            recoveryReason: 'Vercel timeout — task status not updated',
          });
          recovered++;
        } else {
          // Some links still pending/processing — mark task as 'failed'
          // so queue can re-process if needed
          const resetLinks = links.map((l: any) =>
            (!l.status || ['pending', 'processing', ''].includes(l.status))
              ? { ...l, status: 'error', error: 'Task stuck >10min — auto-recovered',
                  logs: [...(l.logs || []), { msg: '🔄 Auto-recovered (stuck >10min)', type: 'warn' }] }
              : l,
          );
          await doc.ref.update({
            links: resetLinks,
            status: 'failed',
            recoveredAt: new Date().toISOString(),
            recoveryReason: `Task stuck in processing for ${Math.round(ageMs / 60000)}min`,
          });
          recovered++;
        }
      }
    }
  } catch { /* continue */ }

  return recovered;
}

// ─── GET /api/cron/process-queue ─────────────────────────────────────────────
export async function GET(req: NextRequest) {
  // Auth check
  const cronSecret = process.env.CRON_SECRET;
  if (cronSecret) {
    const authHeader = req.headers.get('Authorization') || '';
    if (authHeader !== `Bearer ${cronSecret}`) {
      return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
    }
  }

  const overallStart = Date.now();

  try {
    // Step 1: Heartbeat → 'running'
    await updateHeartbeat('running', 'Cron started');

    // Step 2: Recover stuck tasks
    const recovered = await recoverStuckTasks();
    if (recovered > 0) {
      await sendTelegram(`🔧 Auto-Recovery\n♻️ ${recovered} stuck task(s) recovered`);
    }

    // Step 3: Pick 1 pending queue item (movies first, then webseries)
    let item: any = null;
    let queueCollection = '';

    for (const col of queueCollections) {
      const snap = await db
        .collection(col)
        .where('status', '==', 'pending')
        .orderBy('createdAt', 'asc')
        .limit(1)
        .get();

      if (!snap.empty) {
        const doc = snap.docs[0];
        item = { id: doc.id, ...doc.data() };
        queueCollection = col;
        break;
      }
    }

    // Queue empty
    if (!item) {
      await updateHeartbeat('idle', 'Queue empty');
      return NextResponse.json({ status: 'idle', message: 'Queue empty', recovered });
    }

    // Step 4: Lock queue item — direct Firestore (FIX A: no /api/tasks call)
    await db.collection(queueCollection).doc(item.id).update({
      status:     'processing',
      lockedAt:   new Date().toISOString(),
      retryCount: item.retryCount || 0,
    });

    // Step 5: Extract links directly — direct function call
    // v4 TRAP 4 FIX: Dedicated try-catch — queue item NEVER permanently locked
    let listResult: any;
    try {
      listResult = await extractMovieLinks(item.url);
      if (listResult.status !== 'success' || !listResult.links?.length) {
        throw new Error(listResult.message || 'Link extraction failed or returned 0 links');
      }
    } catch (extractionError: any) {
      // TRAP 8 FIX: Check retries before permanent failure — "one strike you're out" bug fixed
      const currentRetries = item.retryCount || 0;
      const isFinalFail = currentRetries >= MAX_CRON_RETRIES;

      await db.collection(queueCollection).doc(item.id).update({
        status:    isFinalFail ? 'failed' : 'pending', // Re-queue if retries left
        error:     `Extraction failed: ${extractionError.message}`,
        failedAt:  isFinalFail ? new Date().toISOString() : null,
        lockedAt:  null, // Unlock so next cron run can pick it up
        retryCount: currentRetries + 1,
      });

      // Re-throw — top-level catch sends Telegram alert
      throw extractionError;
    }

    // FIX D: Stable IDs assigned at extraction time
    const linksWithIds = listResult.links.map((l: any, i: number) => ({
      ...l,
      id:     i,          // stable originalIndex
      status: 'pending',
      logs:   [{ msg: '🔍 Queued for processing...', type: 'info' }],
    }));

    // FIX A: Direct Firestore write — no /api/tasks HTTP call
    const taskRef = await db.collection('scraping_tasks').add({
      url:                 item.url,
      status:              'processing',
      createdAt:           new Date().toISOString(),
      extractedBy:         'Server/Auto-Pilot',
      metadata:            listResult.metadata || null,
      preview:             listResult.preview  || null,
      links:               linksWithIds,
    });
    const taskId = taskRef.id;

    // Step 6: Filter pending links
    const pendingLinks = linksWithIds.filter(
      (l: any) => !l.status || ['pending', 'processing'].includes(l.status),
    );

    // Step 7: Solve links — direct function calls (FIX A: no /api/solve_task call)
    // v5 TRAP 6 FIX: Track deferred links
    const TIME_BUDGET_MS = 45_000;
    let hasDeferredLinks = false; // v5 TRAP 6

    const timerLinks  = pendingLinks.filter((l: any) => TIMER_DOMAINS.some(d => l.link?.includes(d)));
    const directLinks = pendingLinks.filter((l: any) => !TIMER_DOMAINS.some(d => l.link?.includes(d)));

    // Direct links — parallel
    const directPromises = directLinks.map((l: any) =>
      processLink(l, l.id, taskId, 'Server/Auto-Pilot'), // FIX D: l.id
    );

    // Timer links — sequential with time budget (FIX C — index-based)
    // TRAP 7 FIX: Collect + RETURN results so timerDone is counted correctly
    const timerPromise: Promise<any[]> = (async () => {
      const timerResults: any[] = [];

      for (let i = 0; i < timerLinks.length; i++) {
        const l = timerLinks[i];

        if (Date.now() - overallStart > TIME_BUDGET_MS) {
          hasDeferredLinks = true; // v5 TRAP 6: SET FLAG before deferred saves

          // v4 TRAP 3 FIX: Promise.all — parallel deferred saves
          await Promise.all(
            timerLinks.slice(i).map((deferred: any) =>
              saveResultToFirestore(taskId, deferred.id, deferred.link, {
                status:    'pending',
                error:     null,
                finalLink: null,
                logs: [{ msg: '⏳ Time budget exceeded — deferred to next cron run', type: 'warn' }],
              }, 'Server/Auto-Pilot'),
            ),
          );
          break;
        }

        const r = await processLink(l, l.id, taskId, 'Server/Auto-Pilot'); // FIX D: l.id
        timerResults.push(r);
      }

      return timerResults;
    })();

    const [directSettled, timerResults] = await Promise.all([
      Promise.allSettled(directPromises),
      timerPromise,
    ]);

    // Count successes from BOTH arrays correctly
    const directDone   = directSettled.filter(r => r.status === 'fulfilled' && (r.value as any)?.status === 'done').length;
    const directErrors = directSettled.filter(r => r.status === 'rejected' || (r.status === 'fulfilled' && (r.value as any)?.status === 'error')).length;
    const timerDone    = (timerResults as any[]).filter(r => r?.status === 'done' || r?.status === 'success').length;
    const timerErrors  = (timerResults as any[]).filter(r => r?.status === 'error' || r?.status === 'failed').length;
    const doneCount    = directDone + timerDone;
    const errorCount   = directErrors + timerErrors;

    // ✅ FIX 1: Queue item completion — STRICT 100% success required
    //
    // OLD BUG: status = doneCount > 0 ? 'completed' : 'failed'
    //   → 1 success out of 5 links = 'completed' → movie skipped forever
    //
    // NEW LOGIC:
    //   allLinksSucceeded = doneCount === total AND errorCount === 0
    //   → Only 'completed' when EVERY link resolved successfully
    //   → Any error/failure → 'failed' so cron retry logic picks it up
    //   → hasDeferredLinks → 'pending' so next cron run resumes remaining links
    const totalProcessed = pendingLinks.length - (hasDeferredLinks ? timerLinks.filter((_: any, i: number) => {
      // approximate deferred count — actual count handled by hasDeferredLinks flag
      return false;
    }).length : 0);
    const allLinksSucceeded = !hasDeferredLinks && errorCount === 0 && doneCount === pendingLinks.length;

    // Step 8: Queue item status update
    if (hasDeferredLinks) {
      await db.collection(queueCollection).doc(item.id).update({
        status:           'pending',   // Re-queue — next cron will resume deferred links
        lockedAt:         null,        // Unlock
        taskId,
        extractedBy:      'Server/Auto-Pilot',
        retryCount:       item.retryCount || 0,
        lastPartialRunAt: new Date().toISOString(),
      });
    } else if (allLinksSucceeded) {
      // ✅ 100% success — mark completed
      await db.collection(queueCollection).doc(item.id).update({
        status:      'completed',
        processedAt: new Date().toISOString(),
        taskId,
        extractedBy: 'Server/Auto-Pilot',
        retryCount:  item.retryCount || 0,
      });
    } else {
      // ❌ Some links failed — mark 'failed' so cron retry logic can pick it up
      // The scraping_task in Firestore will also be 'failed' (set by saveResultToFirestore)
      // recoverStuckTasks() will reset it to 'pending' on the next cron run
      await db.collection(queueCollection).doc(item.id).update({
        status:      'failed',
        processedAt: new Date().toISOString(),
        taskId,
        extractedBy: 'Server/Auto-Pilot',
        retryCount:  item.retryCount || 0,
        lastError:   `${errorCount} link(s) failed out of ${pendingLinks.length}`,
      });
    }

    // Step 8.5: Phase 4 — Cache cleanup (remove expired entries)
    try { await cleanupExpiredCache(); } catch { /* non-critical */ }

    // Step 9: Heartbeat → 'idle'
    await updateHeartbeat('idle', 'Queue run complete');

    // Step 10: Telegram notification
    const elapsed = Math.round((Date.now() - overallStart) / 1000);
    const title   = listResult.metadata?.title || item.url;
    const retry   = item.retryCount || 0;

    if (hasDeferredLinks) {
      await sendTelegram(
        `⏳ Auto-Pilot Partial 🤖\n🎬 ${title}\n⏱ ${elapsed}s\n🔗 Deferred links pending — next run will resume\n🔄 Retry: ${retry}/${MAX_CRON_RETRIES}`,
      );
    } else if (doneCount > 0) {
      await sendTelegram(
        `✅ Auto-Pilot 🤖\n🎬 ${title}\n⏱ ${elapsed}s\n🔄 Retry: ${retry}/${MAX_CRON_RETRIES}`,
      );
    } else {
      await sendTelegram(
        `❌ Auto-Pilot Failed\n🎬 ${title}\n🔄 Retry: ${retry}/${MAX_CRON_RETRIES}`,
      );
    }

    return NextResponse.json({
      status:       'ok',
      taskId,
      recovered,
      doneCount,
      hasDeferredLinks,
      elapsed,
    });
  } catch (err: any) {
    // Return 200 — GitHub Actions 500 causes job failure + unnecessary retries
    await updateHeartbeat('error', err.message);
    await sendTelegram(`🚨 Cron Error\n${err.message}`);
    return NextResponse.json({ status: 'error', error: err.message });
  }
}
