/**
 * POST /api/admin/reset-stuck
 *
 * Phase 4: Force reset ALL stuck tasks
 * - Finds scraping_tasks stuck in 'processing' > 10 min
 * - Marks them as 'failed'
 * - Resets queue items stuck in 'processing'
 * - Returns count of recovered items
 *
 * Called from: MflixApp.tsx "Force Reset" button
 */

import { NextRequest, NextResponse } from 'next/server';
import { db } from '@/lib/firebaseAdmin';
import { STUCK_TASK_THRESHOLD_MS, MAX_CRON_RETRIES } from '@/lib/config';
import { cleanupExpiredCache } from '@/lib/cache';

export const dynamic = 'force-dynamic';
export const maxDuration = 30;

export async function POST(req: NextRequest) {
  const now = Date.now();
  let recoveredTasks = 0;
  let recoveredQueue = 0;
  let cacheCleared = 0;

  try {
    // Parse optional body for threshold override
    let thresholdMs = STUCK_TASK_THRESHOLD_MS;
    try {
      const body = await req.json();
      if (body?.forceAll) thresholdMs = 0; // Reset ALL processing tasks
      if (body?.thresholdMinutes) thresholdMs = body.thresholdMinutes * 60 * 1000;
    } catch { /* no body = use default */ }

    // ─── 1. Reset stuck scraping_tasks ─────────────────────────────────────
    const tasksSnap = await db.collection('scraping_tasks')
      .where('status', '==', 'processing')
      .get();

    for (const doc of tasksSnap.docs) {
      const data = doc.data();
      const startedAt = data.processingStartedAt || data.createdAt;
      const ageMs = startedAt ? now - new Date(startedAt).getTime() : Infinity;

      if (ageMs > thresholdMs) {
        const links: any[] = data.links || [];

        // Check if all links are actually done
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
            recoveryReason: 'Manual force reset — task was completed but status not updated',
          });
        } else {
          // Mark unfinished links as error
          const resetLinks = links.map((l: any) =>
            (!l.status || ['pending', 'processing', ''].includes(l.status))
              ? { ...l, status: 'error', error: 'Force reset by admin' }
              : l,
          );
          await doc.ref.update({
            links: resetLinks,
            status: 'failed',
            recoveredAt: new Date().toISOString(),
            recoveryReason: 'Manual force reset by admin',
          });
        }
        recoveredTasks++;
      }
    }

    // ─── 2. Reset stuck queue items ────────────────────────────────────────
    for (const col of ['movies_queue', 'webseries_queue']) {
      const qSnap = await db.collection(col)
        .where('status', '==', 'processing')
        .get();

      for (const doc of qSnap.docs) {
        const data = doc.data();
        const lockedAt = data.lockedAt || data.updatedAt || data.createdAt;
        const ageMs = lockedAt ? now - new Date(lockedAt).getTime() : Infinity;

        if (ageMs > thresholdMs) {
          const retryCount = (data.retryCount || 0) + 1;
          await doc.ref.update({
            status: retryCount > MAX_CRON_RETRIES ? 'failed' : 'pending',
            lockedAt: null,
            retryCount,
            lastRecoveredAt: new Date().toISOString(),
          });
          recoveredQueue++;
        }
      }
    }

    // ─── 3. Cleanup expired cache (bonus) ──────────────────────────────────
    cacheCleared = await cleanupExpiredCache();

    // ─── 4. Update engine status ───────────────────────────────────────────
    await db.collection('system').doc('engine_status').set({
      status: 'idle',
      lastRunAt: new Date().toISOString(),
      details: `Force reset: ${recoveredTasks} tasks, ${recoveredQueue} queue items recovered`,
      source: 'admin-force-reset',
      updatedAt: new Date().toISOString(),
    }, { merge: true });

    return NextResponse.json({
      ok: true,
      recoveredTasks,
      recoveredQueue,
      cacheCleared,
      message: `Reset ${recoveredTasks} stuck task(s) and ${recoveredQueue} queue item(s)`,
    });
  } catch (err: any) {
    return NextResponse.json({ error: err.message }, { status: 500 });
  }
}
