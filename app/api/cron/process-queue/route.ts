import { NextRequest, NextResponse } from 'next/server';
import { db } from '@/lib/firebaseAdmin';
import { extractMovieLinks } from '@/lib/solvers';
import { TIMER_DOMAINS, STUCK_TASK_THRESHOLD_MS, MAX_CRON_RETRIES } from '@/lib/config';
import { processLink } from '@/app/api/solve_task/route';
import { cleanupExpiredCache } from '@/lib/cache';

export const dynamic    = 'force-dynamic';
export const maxDuration = 60;

const queueCollections = ['movies_queue', 'webseries_queue'] as const;

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

async function recoverStuckTasks(): Promise<number> {
  let recovered = 0;
  const now = Date.now();

  for (const col of queueCollections) {
    try {
      const snap = await db.collection(col).where('status', '==', 'processing').get();
      for (const doc of snap.docs) {
        const data     = doc.data();
        const lockedAt = data.lockedAt || data.updatedAt || data.createdAt;
        const lockedMs = lockedAt ? now - new Date(lockedAt).getTime() : Infinity;

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
              status:          'pending',
              lockedAt:        null,
              retryCount,
              lastRecoveredAt: new Date().toISOString(),
            });
          }
          recovered++;
        }
      }
    } catch { /* continue */ }
  }

  try {
    const snap = await db.collection('scraping_tasks').where('status', '==', 'processing').get();
    for (const doc of snap.docs) {
      const data      = doc.data();
      const startedAt = data.processingStartedAt || data.createdAt;
      const ageMs     = startedAt ? now - new Date(startedAt).getTime() : 0;

      if (ageMs > STUCK_TASK_THRESHOLD_MS) {
        const links: any[] = data.links || [];
        const TERMINAL = ['done', 'success', 'error', 'failed'];
        const allTerminal = links.length > 0 && links.every(
          (l: any) => TERMINAL.includes((l.status || '').toLowerCase())
        );
        const allSuccess = allTerminal && links.every(
          (l: any) => ['done', 'success'].includes((l.status || '').toLowerCase())
        );

        if (allTerminal) {
          await doc.ref.update({
            status: allSuccess ? 'completed' : 'failed',
            ...(allSuccess ? { completedAt: new Date().toISOString() } : {}),
            recoveredAt: new Date().toISOString(),
          });
          recovered++;
        } else {
          const resetLinks = links.map((l: any) =>
            (!l.status || ['pending', 'processing', ''].includes(l.status))
              ? { ...l, status: 'error', error: 'Task stuck >10min',
                  logs: [...(l.logs || []), { msg: '🔄 Auto-recovered (stuck >10min)', type: 'warn' }] }
              : l,
          );
          await doc.ref.update({
            links: resetLinks,
            status: 'failed',
            recoveredAt: new Date().toISOString(),
            recoveryReason: `Stuck ${Math.round(ageMs / 60000)}min`,
          });
          recovered++;
        }
      }
    }
  } catch { /* continue */ }

  return recovered;
}

// ═══════════════════════════════════════════════════════════════════════════════
// PHASE 7: STRICT SEQUENTIAL — Finish ALL links before starting next movie
// ═══════════════════════════════════════════════════════════════════════════════
//
// RULE: Jab tak ek movie ki SAARI links complete na ho,
//       dusri movie START NAHI HOGI.
//
// FLOW:
//   Cron run → getActiveTask() check karo:
//     Active task hai? → Uske pending links process karo (continue)
//     Nahi hai?        → Naya queue item pick karo (new movie start)
//
//   Queue item "completed" TAB mark hoga jab scraping_task ke
//   SAARE links done/error ho jayein — NOT before.
// ═══════════════════════════════════════════════════════════════════════════════

const TERMINAL = ['done', 'success', 'error', 'failed'];

// Check if any scraping_task is still processing with pending links
async function getActiveTask(): Promise<{ taskId: string; links: any[] } | null> {
  try {
    const snap = await db.collection('scraping_tasks')
      .where('status', '==', 'processing')
      .orderBy('createdAt', 'asc')
      .limit(1)
      .get();

    if (snap.empty) return null;

    const doc  = snap.docs[0];
    const data = doc.data();
    const links: any[] = data.links || [];

    const pendingLinks = links.filter(
      (l: any) => !TERMINAL.includes((l.status || '').toLowerCase())
    );

    if (pendingLinks.length === 0) {
      // All links terminal — finalize task
      const allSuccess = links.every(
        (l: any) => ['done', 'success'].includes((l.status || '').toLowerCase())
      );
      await doc.ref.update({
        status: allSuccess ? 'completed' : 'failed',
        completedAt: new Date().toISOString(),
      });

      // Also mark queue item as completed
      for (const col of queueCollections) {
        const qSnap = await db.collection(col)
          .where('taskId', '==', doc.id)
          .where('status', '==', 'processing')
          .limit(1)
          .get();
        if (!qSnap.empty) {
          await qSnap.docs[0].ref.update({
            status: 'completed',
            processedAt: new Date().toISOString(),
          });
        }
      }

      return null; // No active task — can pick new movie
    }

    // Active task with pending links found
    return { taskId: doc.id, links };
  } catch (e: any) {
    console.error('[Cron] getActiveTask error:', e.message);
    return null;
  }
}

// Process pending links of a task (shared logic for both continue & new)
async function processPendingLinks(
  taskId: string,
  links: any[],
  req: NextRequest,
): Promise<{ triggeredRelay: boolean; stillPending: number; doneLinks: number; totalLinks: number }> {
  const pendingLinks = links.filter(
    (l: any) => !TERMINAL.includes((l.status || '').toLowerCase())
  );

  const timerLinks  = pendingLinks.filter((l: any) => TIMER_DOMAINS.some(d => l.link?.includes(d)));
  const directLinks = pendingLinks.filter((l: any) => !TIMER_DOMAINS.some(d => l.link?.includes(d)));

  console.log(`[Cron] Task ${taskId}: ${directLinks.length} direct + ${timerLinks.length} timer pending`);

  // Direct links → CONCURRENT
  const directPromises = directLinks.map((l: any) =>
    processLink(l, l.id, taskId, 'Server/Auto-Pilot')
  );

  // Timer links → STRICTLY SEQUENTIAL (one per execution, relay rest)
  let triggeredRelay = false;
  const timerPromise = (async () => {
    if (timerLinks.length > 0) {
      await processLink(timerLinks[0], timerLinks[0].id, taskId, 'Server/Auto-Pilot');

      if (timerLinks.length > 1) {
        triggeredRelay = true;
        const targetUrl = process.env.VERCEL_URL
          ? `https://${process.env.VERCEL_URL}/api/solve_task`
          : `${req.nextUrl.origin}/api/solve_task`;

        try {
          fetch(targetUrl, {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              'x-mflix-internal': 'true',
            },
            body: JSON.stringify({
              taskId,
              timerLinks,
              timerIndex:  1,
              extractedBy: 'Server/Auto-Pilot',
              relayDepth:  1,
              mode:        'relay',
            }),
            keepalive: true,
          }).catch(e => console.error('[Relay] error:', e));
        } catch { /* non-critical */ }
      }
    }
  })();

  await Promise.allSettled([...directPromises, timerPromise]);

  // Re-read fresh data from DB
  const freshSnap  = await db.collection('scraping_tasks').doc(taskId).get();
  const freshLinks: any[] = freshSnap.data()?.links || [];
  const stillPending = freshLinks.filter(
    (l: any) => !TERMINAL.includes((l.status || '').toLowerCase())
  ).length;
  const doneLinks = freshLinks.filter(
    (l: any) => ['done', 'success'].includes((l.status || '').toLowerCase())
  ).length;

  // If ALL done and no relay pending, finalize everything
  if (stillPending === 0 && !triggeredRelay) {
    const allSuccess = freshLinks.every(
      (l: any) => ['done', 'success'].includes((l.status || '').toLowerCase())
    );
    await db.collection('scraping_tasks').doc(taskId).update({
      status: allSuccess ? 'completed' : 'failed',
      completedAt: new Date().toISOString(),
    });

    // Mark queue item as completed
    for (const col of queueCollections) {
      const qSnap = await db.collection(col)
        .where('taskId', '==', taskId)
        .where('status', '==', 'processing')
        .limit(1)
        .get();
      if (!qSnap.empty) {
        await qSnap.docs[0].ref.update({
          status: 'completed',
          processedAt: new Date().toISOString(),
        });
      }
    }
  }
  // If still pending: task stays "processing" — next cron continues it

  return { triggeredRelay, stillPending, doneLinks, totalLinks: freshLinks.length };
}

// ─── GET /api/cron/process-queue ─────────────────────────────────────────────
export async function GET(req: NextRequest) {
  const cronSecret = process.env.CRON_SECRET;
  if (cronSecret) {
    const authHeader = req.headers.get('Authorization') || '';
    if (authHeader !== `Bearer ${cronSecret}`) {
      return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
    }
  }

  const overallStart = Date.now();

  try {
    await updateHeartbeat('running', 'Cron started');

    // Step 1: Recover stuck tasks
    const recovered = await recoverStuckTasks();
    if (recovered > 0) {
      await sendTelegram(`🔧 Auto-Recovery\n♻️ ${recovered} stuck task(s) recovered`);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Step 2: CHECK ACTIVE TASK — Continue existing movie if links pending
    // ═══════════════════════════════════════════════════════════════════════
    const activeTask = await getActiveTask();

    if (activeTask) {
      console.log(`[Cron] Active task ${activeTask.taskId} — continuing (not starting new movie)`);

      const result = await processPendingLinks(activeTask.taskId, activeTask.links, req);

      try { await cleanupExpiredCache(); } catch { /* */ }

      const elapsed = Math.round((Date.now() - overallStart) / 1000);

      if (result.stillPending === 0 && !result.triggeredRelay) {
        await sendTelegram(
          `✅ Movie Complete 🎬\n🔗 ${result.doneLinks}/${result.totalLinks} done\n⏱ ${elapsed}s`
        );
        await updateHeartbeat('idle', 'Movie complete — ready for next');
      } else {
        await updateHeartbeat('idle', `Continuing — ${result.stillPending}/${result.totalLinks} pending`);
      }

      return NextResponse.json({
        status:        'continued',
        taskId:        activeTask.taskId,
        stillPending:  result.stillPending,
        doneLinks:     result.doneLinks,
        totalLinks:    result.totalLinks,
        triggeredRelay: result.triggeredRelay,
        recovered,
        elapsed,
      });
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Step 3: No active task — Pick NEW queue item (start new movie)
    // ═══════════════════════════════════════════════════════════════════════
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

    if (!item) {
      await updateHeartbeat('idle', 'Queue empty');
      return NextResponse.json({ status: 'idle', message: 'Queue empty', recovered });
    }

    // Lock queue item
    await db.collection(queueCollection).doc(item.id).update({
      status:     'processing',
      lockedAt:   new Date().toISOString(),
      retryCount: item.retryCount || 0,
    });

    // Extract links
    let listResult: any;
    try {
      listResult = await extractMovieLinks(item.url);
      if (listResult.status !== 'success' || !listResult.links?.length) {
        throw new Error(listResult.message || 'Link extraction failed');
      }
    } catch (extractionError: any) {
      const currentRetries = item.retryCount || 0;
      const isFinalFail = currentRetries >= MAX_CRON_RETRIES;

      await db.collection(queueCollection).doc(item.id).update({
        status:     isFinalFail ? 'failed' : 'pending',
        error:      `Extraction failed: ${extractionError.message}`,
        failedAt:   isFinalFail ? new Date().toISOString() : null,
        lockedAt:   null,
        retryCount: currentRetries + 1,
      });
      throw extractionError;
    }

    const linksWithIds = listResult.links.map((l: any, i: number) => ({
      ...l,
      id:     i,
      status: 'pending',
      logs:   [],
    }));

    // Create scraping task
    const taskRef = await db.collection('scraping_tasks').add({
      url:         item.url,
      status:      'processing',
      createdAt:   new Date().toISOString(),
      extractedBy: 'Server/Auto-Pilot',
      metadata:    listResult.metadata || null,
      preview:     listResult.preview  || null,
      links:       linksWithIds,
    });
    const taskId = taskRef.id;

    // Link taskId to queue item
    await db.collection(queueCollection).doc(item.id).update({ taskId });

    // Process links
    const result = await processPendingLinks(taskId, linksWithIds, req);

    try { await cleanupExpiredCache(); } catch { /* */ }

    const elapsed = Math.round((Date.now() - overallStart) / 1000);
    const title   = listResult.metadata?.title || item.url;
    const retry   = item.retryCount || 0;

    if (result.stillPending === 0 && !result.triggeredRelay) {
      await updateHeartbeat('idle', 'Complete');
      await sendTelegram(
        `✅ Auto-Pilot Complete 🤖\n🎬 ${title}\n⏱ ${elapsed}s\n🔗 ${result.doneLinks}/${result.totalLinks} done\n🔄 Retry: ${retry}/${MAX_CRON_RETRIES}`
      );
    } else {
      await updateHeartbeat('idle', `Processing ${title} — ${result.stillPending} pending`);
      await sendTelegram(
        `⏳ Auto-Pilot Processing 🤖\n🎬 ${title}\n⏱ ${elapsed}s\n🔗 ${result.doneLinks}/${result.totalLinks} done — ${result.stillPending} pending\n${result.triggeredRelay ? '🔄 Relay active' : '⏳ Next cron continues'}`
      );
    }

    return NextResponse.json({
      status:        result.stillPending > 0 ? 'in-progress' : 'ok',
      taskId,
      recovered,
      triggeredRelay: result.triggeredRelay,
      stillPending:  result.stillPending,
      totalLinks:    result.totalLinks,
      doneLinks:     result.doneLinks,
      elapsed,
    });
  } catch (err: any) {
    await updateHeartbeat('error', err.message);
    await sendTelegram(`🚨 Cron Error\n${err.message}`);
    return NextResponse.json({ status: 'error', error: err.message });
  }
}
