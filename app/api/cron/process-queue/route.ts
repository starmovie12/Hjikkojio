import { NextRequest, NextResponse } from 'next/server';
import { db } from '@/lib/firebaseAdmin';
import { extractMovieLinks } from '@/lib/solvers';
import { TIMER_DOMAINS, STUCK_TASK_THRESHOLD_MS, MAX_CRON_RETRIES } from '@/lib/config';
// FIX A: Direct import from solve_task — ZERO nested HTTP calls
import { processLink } from '@/app/api/solve_task/route';
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
            recoveryReason: 'Vercel timeout — task status not updated',
          });
          recovered++;
        } else {
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

    // Step 3: Pick 1 pending queue item
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

    // Step 4: Lock queue item
    await db.collection(queueCollection).doc(item.id).update({
      status:     'processing',
      lockedAt:   new Date().toISOString(),
      retryCount: item.retryCount || 0,
    });

    // Step 5: Extract links directly
    let listResult: any;
    try {
      listResult = await extractMovieLinks(item.url);
      if (listResult.status !== 'success' || !listResult.links?.length) {
        throw new Error(listResult.message || 'Link extraction failed or returned 0 links');
      }
    } catch (extractionError: any) {
      const currentRetries = item.retryCount || 0;
      const isFinalFail = currentRetries >= MAX_CRON_RETRIES;

      await db.collection(queueCollection).doc(item.id).update({
        status:    isFinalFail ? 'failed' : 'pending',
        error:     `Extraction failed: ${extractionError.message}`,
        failedAt:  isFinalFail ? new Date().toISOString() : null,
        lockedAt:  null,
        retryCount: currentRetries + 1,
      });

      throw extractionError;
    }

    const linksWithIds = listResult.links.map((l: any, i: number) => ({
      ...l,
      id:     i,
      status: 'pending',
      logs:   [{ msg: '🔍 Queued for processing...', type: 'info' }],
    }));

    // Save scraping task to DB
    const taskRef = await db.collection('scraping_tasks').add({
      url:                 item.url,
      status:              'processing',
      createdAt:           new Date().toISOString(),
      extractedBy:         'Server/Auto-Pilot',
      metadata:            listResult.metadata || null,
      preview:             listResult.preview  || null,
      links:               linksWithIds,
      completedLinksCount: 0,
    });
    const taskId = taskRef.id;

    // Step 6: Filter pending links
    const pendingLinks = linksWithIds.filter(
      (l: any) => !l.status || ['pending', 'processing'].includes(l.status),
    );

    // ─── STEP 7: RELAY RACE / TAAR KAATNA LOGIC ────────────────────────────────

    const timerLinks  = pendingLinks.filter((l: any) => TIMER_DOMAINS.some(d => l.link?.includes(d)));
    const directLinks = pendingLinks.filter((l: any) => !TIMER_DOMAINS.some(d => l.link?.includes(d)));

    // RULE 1: Non-Timer APIs -> Execute Concurrently (Parallel)
    const directPromises = directLinks.map((l: any) =>
      processLink(l, l.id, taskId, 'Server/Auto-Pilot')
    );

    // RULE 2: Timer Page API -> Execute STRICTLY SEQUENTIALLY using Relay Race
    let triggeredRelay = false;
    const timerPromise = (async () => {
      if (timerLinks.length > 0) {
        const currentTimerLink = timerLinks[0];
        
        // Process EXACTLY ONE timer link per Vercel execution
        await processLink(currentTimerLink, currentTimerLink.id, taskId, 'Server/Auto-Pilot');

        // Check if there are more timer links left in the queue -> FIRE RELAY RACE
        if (timerLinks.length > 1) {
          triggeredRelay = true;
          // Determine origin safely for Vercel
          const targetUrl = process.env.VERCEL_URL 
            ? `https://${process.env.VERCEL_URL}/api/stream_solve` 
            : `${req.nextUrl.origin}/api/stream_solve`;

          try {
            fetch(targetUrl, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({
                links: timerLinks,
                taskId,
                extractedBy: 'Server/Auto-Pilot',
                isRelay: true, 
                currentTimerIndex: 1 
              }),
              keepalive: true // Don't await this, let it run in background to "cut the wire"
            }).catch(e => console.error('[Relay Race] Cron fetch error:', e));
          } catch (err) {
            console.error('[Relay Race] Cron fetch trigger failed:', err);
          }
        }
      }
    })();

    // Wait for parallel non-timer links and the SINGLE timer link to finish
    await Promise.allSettled([...directPromises, timerPromise]);

    // ─── STEP 8: QUEUE ITEM STATUS UPDATE ────────────────────────────────────
    // Since Relay Race handles the individual scraping task completion, 
    // the cron queue item is marked "completed" instantly! Wire cut!
    await db.collection(queueCollection).doc(item.id).update({
      status:      'completed',
      processedAt: new Date().toISOString(),
      taskId,
      extractedBy: 'Server/Auto-Pilot',
      retryCount:  item.retryCount || 0,
    });

    // Step 8.5: Cache cleanup
    try { await cleanupExpiredCache(); } catch { /* non-critical */ }

    // Step 9: Heartbeat → 'idle'
    await updateHeartbeat('idle', 'Queue run complete');

    // Step 10: Telegram notification
    const elapsed = Math.round((Date.now() - overallStart) / 1000);
    const title   = listResult.metadata?.title || item.url;
    const retry   = item.retryCount || 0;

    if (triggeredRelay) {
      await sendTelegram(
        `⏳ Auto-Pilot Relay Race Started 🤖\n🎬 ${title}\n⏱ ${elapsed}s\n🔗 1 Timer link processed, handed off ${timerLinks.length - 1} links to Relay.\n🔄 Retry: ${retry}/${MAX_CRON_RETRIES}`,
      );
    } else {
      await sendTelegram(
        `✅ Auto-Pilot Complete 🤖\n🎬 ${title}\n⏱ ${elapsed}s\n🔄 Retry: ${retry}/${MAX_CRON_RETRIES}`,
      );
    }

    return NextResponse.json({
      status:       'ok',
      taskId,
      recovered,
      triggeredRelay,
      elapsed,
    });
  } catch (err: any) {
    await updateHeartbeat('error', err.message);
    await sendTelegram(`🚨 Cron Error\n${err.message}`);
    return NextResponse.json({ status: 'error', error: err.message });
  }
}
