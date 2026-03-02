/**
 * /api/ai/diagnose — MFLIX PRO System Diagnostics
 *
 * Ye endpoint puri website ka deep health-check karta hai:
 * 1. Firebase connectivity
 * 2. Engine/Cron heartbeat
 * 3. Queue health (stuck, pending, failed items)
 * 4. Task health (stuck tasks, error patterns, link failure rates)
 * 5. VPS connectivity (timer + hubcloud APIs)
 * 6. Recent error analysis
 *
 * Returns a structured diagnostics object for AI analysis.
 */

import { NextResponse } from 'next/server';
import { db } from '@/lib/firebaseAdmin';
import { TIMER_API, HUBCLOUD_API, STUCK_TASK_THRESHOLD_MS, MAX_CRON_RETRIES } from '@/lib/config';
import { getCacheStats } from '@/lib/cache';

export const dynamic = 'force-dynamic';
export const maxDuration = 30;

interface DiagnosticResult {
  timestamp: string;
  overall: 'healthy' | 'warning' | 'critical';
  checks: {
    firebase: CheckResult;
    engine: CheckResult;
    vpsTimer: CheckResult;
    vpsHubcloud: CheckResult;
    queueHealth: CheckResult;
    taskHealth: CheckResult;
    recentErrors: CheckResult;
  };
  summary: string[];
  rawData: {
    engineStatus: any;
    queueStats: any;
    taskStats: any;
    recentFailedTasks: any[];
    stuckItems: any[];
    errorPatterns: Record<string, number>;
  };
}

interface CheckResult {
  status: 'ok' | 'warning' | 'critical' | 'unknown';
  message: string;
  details?: any;
}

// ─── Helper: Check with timeout ──────────────────────────────────────────────
async function checkWithTimeout<T>(
  fn: () => Promise<T>,
  timeoutMs: number,
  label: string
): Promise<{ ok: boolean; data?: T; error?: string; latencyMs: number }> {
  const start = Date.now();
  try {
    const result = await Promise.race([
      fn(),
      new Promise<never>((_, reject) =>
        setTimeout(() => reject(new Error(`${label} timed out after ${timeoutMs}ms`)), timeoutMs)
      ),
    ]);
    return { ok: true, data: result, latencyMs: Date.now() - start };
  } catch (e: any) {
    return { ok: false, error: e.message, latencyMs: Date.now() - start };
  }
}

export async function GET() {
  const timestamp = new Date().toISOString();
  const summaryIssues: string[] = [];
  let overallStatus: 'healthy' | 'warning' | 'critical' = 'healthy';

  const setWarning = (msg: string) => {
    summaryIssues.push(`⚠️ ${msg}`);
    if (overallStatus === 'healthy') overallStatus = 'warning';
  };
  const setCritical = (msg: string) => {
    summaryIssues.push(`🚨 ${msg}`);
    overallStatus = 'critical';
  };

  // ═══════════════════════════════════════════════════════════════════════════
  // CHECK 1: Firebase Connectivity
  // ═══════════════════════════════════════════════════════════════════════════
  let firebaseCheck: CheckResult;
  const fbResult = await checkWithTimeout(
    async () => {
      const snap = await db.collection('system').doc('engine_status').get();
      return snap.exists ? snap.data() : null;
    },
    5000,
    'Firebase'
  );

  if (fbResult.ok) {
    firebaseCheck = {
      status: fbResult.latencyMs > 3000 ? 'warning' : 'ok',
      message: `Connected (${fbResult.latencyMs}ms)`,
      details: { latencyMs: fbResult.latencyMs },
    };
    if (fbResult.latencyMs > 3000) setWarning(`Firebase slow: ${fbResult.latencyMs}ms`);
  } else {
    firebaseCheck = { status: 'critical', message: `Firebase connection failed: ${fbResult.error}` };
    setCritical('Firebase is DOWN — entire system non-functional');
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // CHECK 2: Engine/Cron Heartbeat
  // ═══════════════════════════════════════════════════════════════════════════
  let engineCheck: CheckResult;
  let engineData: any = null;

  if (fbResult.ok) {
    try {
      const snap = await db.collection('system').doc('engine_status').get();
      engineData = snap.exists ? snap.data() : null;

      if (!engineData || !engineData.lastRunAt) {
        engineCheck = { status: 'critical', message: 'No heartbeat data found — cron never ran' };
        setCritical('GitHub Cron job has NEVER run — check GitHub Actions workflow');
      } else {
        const lastRun = new Date(engineData.lastRunAt).getTime();
        const diffMs = Date.now() - lastRun;
        const diffMin = Math.floor(diffMs / 60000);

        if (diffMin < 10) {
          engineCheck = {
            status: 'ok',
            message: `ONLINE — last heartbeat ${diffMin}m ago`,
            details: engineData,
          };
        } else if (diffMin < 30) {
          engineCheck = {
            status: 'warning',
            message: `Stale heartbeat — ${diffMin}m since last run`,
            details: engineData,
          };
          setWarning(`Engine heartbeat stale: ${diffMin} minutes ago`);
        } else {
          engineCheck = {
            status: 'critical',
            message: `OFFLINE — ${diffMin}m since last heartbeat`,
            details: engineData,
          };
          setCritical(`Engine OFFLINE for ${diffMin} minutes — GitHub Actions may be disabled`);
        }
      }
    } catch (e: any) {
      engineCheck = { status: 'unknown', message: `Failed to check: ${e.message}` };
    }
  } else {
    engineCheck = { status: 'unknown', message: 'Skipped — Firebase is down' };
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // CHECK 3: VPS Timer API
  // ═══════════════════════════════════════════════════════════════════════════
  let vpsTimerCheck: CheckResult;
  const timerResult = await checkWithTimeout(
    async () => {
      const res = await fetch(`${TIMER_API}/health`, {
        signal: AbortSignal.timeout(8000),
        headers: { 'User-Agent': 'MflixPro/Diag' },
      });
      return { status: res.status, ok: res.ok };
    },
    10000,
    'VPS Timer'
  );

  if (timerResult.ok) {
    vpsTimerCheck = {
      status: 'ok',
      message: `Reachable (${timerResult.latencyMs}ms)`,
      details: { latencyMs: timerResult.latencyMs, url: TIMER_API },
    };
  } else {
    // Timer VPS down is not always critical — might just be /health not existing
    // Try the actual solve endpoint with a test
    vpsTimerCheck = {
      status: 'warning',
      message: `Health check failed: ${timerResult.error} (${timerResult.latencyMs}ms)`,
      details: { url: TIMER_API, error: timerResult.error },
    };
    setWarning(`VPS Timer API health check failed — timer bypass links may not work`);
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // CHECK 4: VPS HubCloud API
  // ═══════════════════════════════════════════════════════════════════════════
  let vpsHubcloudCheck: CheckResult;
  const hubResult = await checkWithTimeout(
    async () => {
      const res = await fetch(`${HUBCLOUD_API}/health`, {
        signal: AbortSignal.timeout(8000),
        headers: { 'User-Agent': 'MflixPro/Diag' },
      });
      return { status: res.status, ok: res.ok };
    },
    10000,
    'VPS HubCloud'
  );

  if (hubResult.ok) {
    vpsHubcloudCheck = {
      status: 'ok',
      message: `Reachable (${hubResult.latencyMs}ms)`,
      details: { latencyMs: hubResult.latencyMs, url: HUBCLOUD_API },
    };
  } else {
    vpsHubcloudCheck = {
      status: 'warning',
      message: `Health check failed: ${hubResult.error} (${hubResult.latencyMs}ms)`,
      details: { url: HUBCLOUD_API, error: hubResult.error },
    };
    setWarning(`VPS HubCloud API health check failed — HubCloud bypass may not work`);
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // CHECK 5: Queue Health
  // ═══════════════════════════════════════════════════════════════════════════
  let queueCheck: CheckResult;
  let queueStats: any = { moviesPending: 0, webseriesPending: 0, stuck: 0, failed: 0 };
  const stuckItems: any[] = [];

  if (fbResult.ok) {
    try {
      const now = Date.now();
      const collections = ['movies_queue', 'webseries_queue'] as const;
      let totalPending = 0;
      let totalFailed = 0;
      let totalStuck = 0;

      for (const col of collections) {
        // Pending count
        const pendingSnap = await db.collection(col).where('status', '==', 'pending').get();
        totalPending += pendingSnap.size;
        if (col === 'movies_queue') queueStats.moviesPending = pendingSnap.size;
        else queueStats.webseriesPending = pendingSnap.size;

        // Failed count
        const failedSnap = await db.collection(col).where('status', '==', 'failed').get();
        totalFailed += failedSnap.size;

        // Stuck processing items
        const processingSnap = await db.collection(col).where('status', '==', 'processing').get();
        for (const doc of processingSnap.docs) {
          const data = doc.data();
          const lockedAt = data.lockedAt || data.updatedAt || data.createdAt;
          const ageMs = lockedAt ? now - new Date(lockedAt).getTime() : Infinity;
          if (ageMs > STUCK_TASK_THRESHOLD_MS) {
            totalStuck++;
            stuckItems.push({
              id: doc.id,
              collection: col,
              url: data.url,
              title: data.title,
              lockedAt,
              ageMinutes: Math.round(ageMs / 60000),
              retryCount: data.retryCount || 0,
            });
          }
        }
      }

      queueStats = { ...queueStats, totalPending, totalFailed, totalStuck };

      if (totalStuck > 0) {
        queueCheck = {
          status: 'warning',
          message: `${totalStuck} stuck item(s) in queue (processing > 10min)`,
          details: { ...queueStats, stuckItems },
        };
        setWarning(`${totalStuck} queue items stuck in 'processing' state`);
      } else if (totalFailed > 3) {
        queueCheck = {
          status: 'warning',
          message: `${totalFailed} failed items in queue`,
          details: queueStats,
        };
        setWarning(`${totalFailed} failed items in queue — check extraction errors`);
      } else {
        queueCheck = {
          status: 'ok',
          message: `Healthy — ${totalPending} pending, ${totalFailed} failed`,
          details: queueStats,
        };
      }
    } catch (e: any) {
      queueCheck = { status: 'unknown', message: `Failed to check queue: ${e.message}` };
    }
  } else {
    queueCheck = { status: 'unknown', message: 'Skipped — Firebase is down' };
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // CHECK 6: Task Health (scraping_tasks)
  // ═══════════════════════════════════════════════════════════════════════════
  let taskCheck: CheckResult;
  let taskStats: any = {};
  const recentFailedTasks: any[] = [];
  const errorPatterns: Record<string, number> = {};

  if (fbResult.ok) {
    try {
      const now = Date.now();

      // Recent tasks (last 20)
      const recentSnap = await db.collection('scraping_tasks')
        .orderBy('createdAt', 'desc')
        .limit(20)
        .get();

      let totalTasks = recentSnap.size;
      let completedTasks = 0;
      let failedTasks = 0;
      let processingTasks = 0;
      let totalLinks = 0;
      let doneLinks = 0;
      let errorLinks = 0;
      let pendingLinks = 0;
      let stuckTasks = 0;

      for (const doc of recentSnap.docs) {
        const data = doc.data();
        const status = (data.status || '').toLowerCase();

        if (status === 'completed') completedTasks++;
        else if (status === 'failed') failedTasks++;
        else if (status === 'processing') {
          processingTasks++;
          // Check if stuck
          const createdAt = data.createdAt;
          const ageMs = createdAt ? now - new Date(createdAt).getTime() : 0;
          if (ageMs > STUCK_TASK_THRESHOLD_MS) stuckTasks++;
        }

        // Link-level analysis
        const links: any[] = data.links || [];
        totalLinks += links.length;

        for (const link of links) {
          const ls = (link.status || '').toLowerCase();
          if (ls === 'done' || ls === 'success') doneLinks++;
          else if (ls === 'error' || ls === 'failed') {
            errorLinks++;
            // Error pattern tracking
            const errMsg = link.error || 'Unknown error';
            const pattern = errMsg.length > 60 ? errMsg.substring(0, 60) : errMsg;
            errorPatterns[pattern] = (errorPatterns[pattern] || 0) + 1;
          } else {
            pendingLinks++;
          }
        }

        // Collect recent failed tasks for analysis
        if (status === 'failed') {
          recentFailedTasks.push({
            id: doc.id,
            url: data.url,
            error: data.error,
            createdAt: data.createdAt,
            linkCount: links.length,
            errorLinks: links.filter((l: any) => ['error', 'failed'].includes((l.status || '').toLowerCase())).length,
          });
        }
      }

      const linkSuccessRate = totalLinks > 0
        ? Math.round((doneLinks / totalLinks) * 100)
        : 0;

      taskStats = {
        totalTasks,
        completedTasks,
        failedTasks,
        processingTasks,
        stuckTasks,
        totalLinks,
        doneLinks,
        errorLinks,
        pendingLinks,
        linkSuccessRate,
      };

      if (stuckTasks > 0) {
        taskCheck = {
          status: 'warning',
          message: `${stuckTasks} task(s) stuck in processing`,
          details: taskStats,
        };
        setWarning(`${stuckTasks} scraping tasks stuck in 'processing' > 10min`);
      } else if (linkSuccessRate < 50 && totalLinks > 5) {
        taskCheck = {
          status: 'critical',
          message: `Link success rate critically low: ${linkSuccessRate}%`,
          details: taskStats,
        };
        setCritical(`Link extraction success rate only ${linkSuccessRate}% — solvers may be broken`);
      } else if (linkSuccessRate < 75 && totalLinks > 5) {
        taskCheck = {
          status: 'warning',
          message: `Link success rate below 75%: ${linkSuccessRate}%`,
          details: taskStats,
        };
        setWarning(`Link success rate ${linkSuccessRate}% — some solvers may need attention`);
      } else {
        taskCheck = {
          status: 'ok',
          message: `Healthy — ${linkSuccessRate}% success rate (${doneLinks}/${totalLinks} links)`,
          details: taskStats,
        };
      }
    } catch (e: any) {
      taskCheck = { status: 'unknown', message: `Failed to check tasks: ${e.message}` };
    }
  } else {
    taskCheck = { status: 'unknown', message: 'Skipped — Firebase is down' };
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // CHECK 7: Recent Errors Analysis
  // ═══════════════════════════════════════════════════════════════════════════
  let errorsCheck: CheckResult;
  const topErrors = Object.entries(errorPatterns)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 5);

  if (topErrors.length === 0) {
    errorsCheck = { status: 'ok', message: 'No recent errors found' };
  } else if (topErrors[0][1] > 5) {
    errorsCheck = {
      status: 'warning',
      message: `Repeated error pattern: "${topErrors[0][0]}" (${topErrors[0][1]}x)`,
      details: { topErrors },
    };
    setWarning(`Repeated error: "${topErrors[0][0]}" appeared ${topErrors[0][1]} times`);
  } else {
    errorsCheck = {
      status: 'ok',
      message: `${topErrors.length} unique error types (low frequency)`,
      details: { topErrors },
    };
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // BUILD FINAL RESPONSE
  // ═══════════════════════════════════════════════════════════════════════════
  if (summaryIssues.length === 0) {
    summaryIssues.push('✅ All systems operational — no issues detected');
  }

  const result: DiagnosticResult = {
    timestamp,
    overall: overallStatus,
    checks: {
      firebase: firebaseCheck,
      engine: engineCheck,
      vpsTimer: vpsTimerCheck,
      vpsHubcloud: vpsHubcloudCheck,
      queueHealth: queueCheck,
      taskHealth: taskCheck,
      recentErrors: errorsCheck,
    },
    summary: summaryIssues,
    rawData: {
      engineStatus: engineData,
      queueStats,
      taskStats,
      recentFailedTasks,
      stuckItems,
      errorPatterns,
      cacheStats: await getCacheStats(),
    },
  };

  return NextResponse.json(result);
}
