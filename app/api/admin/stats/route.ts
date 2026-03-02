/**
 * GET /api/admin/stats
 *
 * Phase 4-5: System-wide statistics for dashboard
 * Returns: task counts, queue health, engine status, recent activity, cache stats
 */

import { NextResponse } from 'next/server';
import { db } from '@/lib/firebaseAdmin';
import { TIMER_API, HUBCLOUD_API, STUCK_TASK_THRESHOLD_MS } from '@/lib/config';
import { getCacheStats } from '@/lib/cache';

export const dynamic = 'force-dynamic';
export const maxDuration = 30;

export async function GET() {
  const start = Date.now();

  try {
    // ─── 1. Task Counts from scraping_tasks ──────────────────────────────
    const tasksSnap = await db.collection('scraping_tasks').get();
    let totalTasks = 0, completedTasks = 0, failedTasks = 0, processingTasks = 0;
    let todayProcessed = 0, todaySuccess = 0, todayFailed = 0;
    let totalLinks = 0, doneLinks = 0, errorLinks = 0;
    let stuckTasks = 0;
    const recentActivity: any[] = [];
    const now = Date.now();
    const todayStart = new Date();
    todayStart.setHours(0, 0, 0, 0);

    for (const doc of tasksSnap.docs) {
      const d = doc.data();
      totalTasks++;

      const status = (d.status || '').toLowerCase();
      if (status === 'completed' || status === 'done') completedTasks++;
      else if (status === 'failed' || status === 'error') failedTasks++;
      else if (status === 'processing') {
        processingTasks++;
        // Check if stuck
        const startedAt = d.processingStartedAt || d.createdAt;
        if (startedAt && now - new Date(startedAt).getTime() > STUCK_TASK_THRESHOLD_MS) {
          stuckTasks++;
        }
      }

      // Count links
      const links: any[] = d.links || [];
      totalLinks += links.length;
      links.forEach((l: any) => {
        const ls = (l.status || '').toLowerCase();
        if (ls === 'done' || ls === 'success') doneLinks++;
        else if (ls === 'error' || ls === 'failed') errorLinks++;
      });

      // Today's stats
      const createdAt = d.createdAt ? new Date(d.createdAt) : null;
      if (createdAt && createdAt >= todayStart) {
        todayProcessed++;
        if (status === 'completed' || status === 'done') todaySuccess++;
        else if (status === 'failed' || status === 'error') todayFailed++;
      }

      // Recent activity (last 10 updated tasks)
      if (d.updatedAt || d.completedAt || d.createdAt) {
        recentActivity.push({
          id: doc.id,
          title: d.movieTitle || d.title || d.url?.split('/').pop() || doc.id,
          status: d.status,
          linksCount: links.length,
          doneLinks: links.filter((l: any) => ['done', 'success'].includes((l.status || '').toLowerCase())).length,
          timestamp: d.completedAt || d.updatedAt || d.createdAt,
          url: d.url,
        });
      }
    }

    // Sort recent by time descending, take 10
    recentActivity.sort((a, b) => new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime());
    const recent10 = recentActivity.slice(0, 10);

    // ─── 2. Queue Counts ─────────────────────────────────────────────────
    let pendingQueue = 0, processingQueue = 0, failedQueue = 0, completedQueue = 0;
    for (const col of ['movies_queue', 'webseries_queue']) {
      try {
        const qSnap = await db.collection(col).get();
        for (const doc of qSnap.docs) {
          const s = (doc.data().status || '').toLowerCase();
          if (s === 'pending') pendingQueue++;
          else if (s === 'processing') processingQueue++;
          else if (s === 'failed') failedQueue++;
          else if (s === 'completed' || s === 'done') completedQueue++;
        }
      } catch { /* skip */ }
    }

    // ─── 3. Engine Status ────────────────────────────────────────────────
    let engineStatus: any = { status: 'unknown', lastRunAt: null };
    try {
      const eDoc = await db.collection('system').doc('engine_status').get();
      if (eDoc.exists) engineStatus = eDoc.data();
    } catch { /* skip */ }

    // ─── 4. VPS Health (quick ping) ──────────────────────────────────────
    let vpsTimer = { status: 'unknown', latencyMs: 0 };
    let vpsHubcloud = { status: 'unknown', latencyMs: 0 };
    try {
      const t1 = Date.now();
      const r = await fetch(`${TIMER_API}/health`, { signal: AbortSignal.timeout(5000) });
      vpsTimer = { status: r.ok ? 'online' : 'error', latencyMs: Date.now() - t1 };
    } catch { vpsTimer = { status: 'offline', latencyMs: 0 }; }

    try {
      const t1 = Date.now();
      const r = await fetch(`${HUBCLOUD_API}/health`, { signal: AbortSignal.timeout(5000) });
      vpsHubcloud = { status: r.ok ? 'online' : 'error', latencyMs: Date.now() - t1 };
    } catch { vpsHubcloud = { status: 'offline', latencyMs: 0 }; }

    // ─── 5. Cache Stats ──────────────────────────────────────────────────
    const cacheStats = await getCacheStats();

    // ─── 6. Success Rate ─────────────────────────────────────────────────
    const successRate = totalTasks > 0
      ? Math.round((completedTasks / totalTasks) * 100)
      : 0;

    return NextResponse.json({
      totalTasks,
      completedTasks,
      failedTasks,
      processingTasks,
      stuckTasks,
      pendingQueue,
      processingQueue,
      failedQueue,
      completedQueue,
      todayProcessed,
      todaySuccess,
      todayFailed,
      successRate,
      totalLinks,
      doneLinks,
      errorLinks,
      engineStatus,
      vpsTimer,
      vpsHubcloud,
      cacheStats,
      recentActivity: recent10,
      fetchTimeMs: Date.now() - start,
    });
  } catch (err: any) {
    return NextResponse.json({ error: err.message }, { status: 500 });
  }
}
