'use client';

import { useState, useEffect, useRef, useCallback, useMemo } from 'react';
import {
  Bolt,
  Link as LinkIcon,
  Rocket,
  Loader2,
  RotateCcw,
  AlertTriangle,
  CircleCheck,
  History,
  ChevronRight,
  ChevronDown,
  ChevronUp,
  Film,
  Globe,
  Volume2,
  Sparkles,
  Home,
  Clock,
  CheckCircle2,
  XCircle,
  Trash2,
  RefreshCw,
  Bot,
  ListVideo,
  Zap,
  Cpu,
  Radio,
  TrendingUp,
  Tv2,
  Play,
  Pause,
  RefreshCcw,
  AlertCircle,
  Eye,
  Cloud,
  Smartphone,
  Wifi,
  WifiOff,
  Shield,
  Activity,
  Brain,
} from 'lucide-react';
import { motion, AnimatePresence } from 'motion/react';
import LinkCard from '@/components/LinkCard';
import AiControlCenter from '@/components/AiControlCenter';

// ─────────────────────────────────────────────────────────────────────────────
// INTERFACES — Inline defined here. lib/types.ts se import NAHI karna.
// ─────────────────────────────────────────────────────────────────────────────

interface LogEntry {
  msg: string;
  type: 'info' | 'success' | 'error' | 'warn';
}

interface Task {
  id: string;
  url: string;
  status: 'processing' | 'completed' | 'failed';
  createdAt: string;
  links: any[];
  error?: string;
  extractedBy?: 'Browser/Live' | 'Server/Auto-Pilot';
  preview?: {
    title: string;
    posterUrl: string | null;
  };
  metadata?: {
    quality: string;
    languages: string;
    audioLabel: string;
  };
}

interface QueueItem {
  id: string;
  collection: string; // 'movies_queue' | 'webseries_queue'
  type: 'movie' | 'webseries';
  url: string;
  title?: string;
  status: string;
  createdAt?: string;
}

interface QueueStats {
  totalPending: number;
  totalCompleted: number;
  totalFailed: number;
  totalAll: number;
  pendingItems: QueueItem[];
}

type TabType = 'home' | 'processing' | 'completed' | 'failed' | 'history';
type AutoPilotPhase = 'idle' | 'fetching' | 'running' | 'done' | 'stopped';

interface EngineStatus {
  signal: 'ONLINE' | 'OFFLINE';
  status: string;
  lastRunAt: string | null;
  timeSinceLastRun: string;
  details: string;
  backgroundActive: boolean;
  pendingCount: number;
  processingCount: number;
}

// ─────────────────────────────────────────────────────────────────────────────
// HELPER FUNCTIONS — Component ke bahar define hain
// ─────────────────────────────────────────────────────────────────────────────

/**
 * ISO date string ko 12-hour format mein convert karta hai
 * e.g. "Jan 5, 3:45 PM"
 */
function formatTime12h(isoString: string): string {
  try {
    const date = new Date(isoString);
    if (isNaN(date.getTime())) return isoString;
    return date.toLocaleString('en-US', {
      month: 'short',
      day: 'numeric',
      hour: 'numeric',
      minute: '2-digit',
      hour12: true,
    });
  } catch {
    return isoString;
  }
}

/**
 * Links array se done/failed/pending count nikalta hai
 * NOTE: task.totalLinks field exist nahi karta — task.links?.length use karo
 */
function getLinkStats(links: any[]): {
  total: number;
  done: number;
  failed: number;
  pending: number;
} {
  if (!links || links.length === 0) {
    return { total: 0, done: 0, failed: 0, pending: 0 };
  }
  let done = 0;
  let failed = 0;
  let pending = 0;
  for (const link of links) {
    const s = (link.status || '').toLowerCase();
    if (s === 'done' || s === 'success') {
      done++;
    } else if (s === 'error' || s === 'failed') {
      failed++;
    } else {
      pending++;
    }
  }
  return { total: links.length, done, failed, pending };
}

/**
 * Animated pulse dot — engine online/offline indicator ke liye
 */
const PulseDot = ({ color = 'bg-emerald-400' }: { color?: string }) => (
  <span className="relative flex h-2 w-2">
    <span
      className={`animate-ping absolute inline-flex h-full w-full rounded-full ${color} opacity-75`}
    />
    <span className={`relative inline-flex rounded-full h-2 w-2 ${color}`} />
  </span>
);

/**
 * Sleep utility — Auto-Pilot cooldown ke liye
 */
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

// ─────────────────────────────────────────────────────────────────────────────
// MAIN COMPONENT
// ─────────────────────────────────────────────────────────────────────────────

export default function MflixApp() {
  // ─── Manual Process States ──────────────────────────────────────────────
  const [url, setUrl] = useState<string>('');
  const [isConnecting, setIsConnecting] = useState<boolean>(false);
  const [isProcessing, setIsProcessing] = useState<boolean>(false);
  const [isDone, setIsDone] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);

  // ─── Tasks + UI States ──────────────────────────────────────────────────
  const [tasks, setTasks] = useState<Task[]>([]);
  const [expandedTask, setExpandedTask] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<TabType>('home');

  // ─── Live Stream States ─────────────────────────────────────────────────
  // Yeh states NDJSON stream se real-time update hoti hain
  const [activeTaskId, setActiveTaskId] = useState<string | null>(null);
  const [liveLogs, setLiveLogs] = useState<Record<number, LogEntry[]>>({});
  const [liveLinks, setLiveLinks] = useState<Record<number, string | null>>({});
  const [liveStatuses, setLiveStatuses] = useState<Record<number, string>>({});

  // ─── Task Action Loading States ─────────────────────────────────────────
  const [deletingTaskId, setDeletingTaskId] = useState<string | null>(null);
  const [retryingTaskId, setRetryingTaskId] = useState<string | null>(null);

  // ─── Auto-Pilot States ──────────────────────────────────────────────────

  // isAutoPilotEnabled: Persisted ON/OFF toggle — survives page refresh.
  const [isAutoPilotEnabled, setIsAutoPilotEnabled] = useState<boolean>(() => {
    if (typeof window === 'undefined') return false;
    try {
      return localStorage.getItem('mflix_autopilot_enabled') === 'true';
    } catch {
      return false;
    }
  });

  const [autoPilotPhase, setAutoPilotPhase] = useState<AutoPilotPhase>('idle');
  const [queueStats, setQueueStats] = useState<QueueStats | null>(null);
  const [currentQueueItem, setCurrentQueueItem] = useState<QueueItem | null>(null);
  const [autoPilotProcessed, setAutoPilotProcessed] = useState<number>(0);
  const [autoPilotStatusMsg, setAutoPilotStatusMsg] = useState<string>('');
  const [autoPilotLog, setAutoPilotLog] = useState<
    { msg: string; type: 'info' | 'success' | 'error' | 'warn' }[]
  >([]);
  const [showQueuePreview, setShowQueuePreview] = useState<boolean>(false);
  const [showAutoPilotLog, setShowAutoPilotLog] = useState<boolean>(false);

  // ─── Engine Status State ─────────────────────────────────────────────────
  const [engineStatus, setEngineStatus] = useState<EngineStatus | null>(null);

  // ─── AI Control Center State ────────────────────────────────────────────
  const [showAiPanel, setShowAiPanel] = useState(false);

  // ─── Phase 4: Stuck Task Detection + Force Reset ───────────────────────
  const [stuckCount, setStuckCount] = useState<number>(0);
  const [isResettingStuck, setIsResettingStuck] = useState(false);
  const [resetResult, setResetResult] = useState<string | null>(null);

  // ─────────────────────────────────────────────────────────────────────────
  // REFS
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * streamStartedRef: Duplicate stream start se bachne ke liye.
   * Ek task ke liye sirf ek stream at a time.
   */
  const streamStartedRef = useRef<Set<string>>(new Set());

  /**
   * pollRef: 5-second polling interval reference — cleanup ke liye.
   */
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  /**
   * completedLinksRef: ⭐ SHIELD PATTERN ⭐
   * Stream se solve hue links ko yahan cache karo.
   * Polling se aaya stale "pending" data in solved links ko overwrite na kare.
   * Structure: { taskId: { linkIdx: { status, finalLink, logs, best_button_name } } }
   */
  const completedLinksRef = useRef<Record<string, Record<number, any>>>({});

  /**
   * streamEndedAtRef: Stream khatam hone ka timestamp.
   * 15 second grace period mein shield active rehta hai.
   */
  const streamEndedAtRef = useRef<Record<string, number>>({});

  /**
   * autoPilotActiveRef: Auto-Pilot run/stop flag.
   * useRef isliye kyunki setInterval/async loop mein latest value chahiye.
   */
  const autoPilotActiveRef = useRef<boolean>(false);

  /**
   * autoPilotLogRef: Log entries ref — async closure mein latest log access ke liye.
   */
  const autoPilotLogRef = useRef<
    { msg: string; type: 'info' | 'success' | 'error' | 'warn' }[]
  >([]);

  /**
   * enginePollRef: 20-second engine status polling interval reference.
   */
  const enginePollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  /**
   * tasksRef: Latest tasks array for use in async closures.
   * Used by fetchEngineStatus to avoid duplicate /api/tasks call.
   */
  const tasksRef = useRef<Task[]>([]);

  /**
   * repeatingLinkId: Currently repeating individual link ID.
   * Problem 5: Track which link is being individually repeated.
   */
  const [repeatingLinkId, setRepeatingLinkId] = useState<string | number | null>(null);

  // Sync isAutoPilotEnabled → localStorage
  useEffect(() => {
    try { localStorage.setItem('mflix_autopilot_enabled', String(isAutoPilotEnabled)); } catch {}
  }, [isAutoPilotEnabled]);

  // Sync tasksRef with latest tasks state (for async closures)
  useEffect(() => {
    tasksRef.current = tasks;
  }, [tasks]);

  // On mount: if autopilot was ON, auto-restart
  useEffect(() => {
    if (isAutoPilotEnabled && autoPilotPhase === 'idle') {
      const timer = setTimeout(() => {
        autoPilotActiveRef.current = true;
        runAutoPilot();
      }, 1500);
      return () => clearTimeout(timer);
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // ─────────────────────────────────────────────────────────────────────────
  // COMPUTED VALUES
  // ─────────────────────────────────────────────────────────────────────────

  const isAutoPilotRunning =
    autoPilotPhase === 'running' || autoPilotPhase === 'fetching';

  const totalInQueue = queueStats ? queueStats.totalAll : 0;

  const autoPilotProgress =
    totalInQueue > 0
      ? Math.min(100, Math.round((autoPilotProcessed / totalInQueue) * 100))
      : 0;

  // ─────────────────────────────────────────────────────────────────────────
  // CORE DATA FUNCTIONS
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * rehydrateFromFirebase()
   * Page load pe /api/tasks se saare tasks fetch karo.
   * Agar koi task processing mein hai → auto-expand + Processing tab.
   */
  const rehydrateFromFirebase = useCallback(async () => {
    try {
      const res = await fetch('/api/tasks');
      if (!res.ok) return;
      const data = await res.json();
      if (!Array.isArray(data)) return;
      setTasks(data);

      // Agar koi processing task hai → usse expand karo
      const processingTask = data.find((t: Task) => t.status === 'processing');
      if (processingTask) {
        setExpandedTask(processingTask.id);
        setActiveTab('processing');
      }
    } catch (e) {
      console.error('Rehydration failed:', e);
    }
  }, []);

  /**
   * fetchTasks() — ⭐ SHIELD PATTERN ke saath polling ⭐
   *
   * Har 5 second /api/tasks call karta hai.
   * LEKIN stream se already solved links ko Firebase polling data overwrite na kare.
   *
   * 3 Layers:
   * Layer 1 (HIGHEST): completedLinksRef — in-memory solved results
   * Layer 2 (MEDIUM):  liveStatuses     — active stream data
   * Layer 3 (LOWEST):  Firebase data    — /api/tasks se aaya data
   */
  const fetchTasks = useCallback(async () => {
    try {
      const res = await fetch('/api/tasks');
      if (!res.ok) return;

      const contentType = res.headers.get('content-type');
      if (!contentType || !contentType.includes('application/json')) return;

      const data = await res.json();

      if (Array.isArray(data)) {
        setTasks((prevTasks) => {
          const currentlyStreamingIds = streamStartedRef.current;
          const now = Date.now();

          return data.map((serverTask: Task) => {
            // SHIELD CHECK 1: Agar task ka stream abhi active hai
            // → local (streaming) data rakhna, polling overwrite nahi
            if (currentlyStreamingIds.has(serverTask.id)) {
              const localTask = prevTasks.find((t) => t.id === serverTask.id);
              if (localTask) return localTask;
            }

            // Grace period check: stream end ke 15 sec tak shield active
            const endedAt = streamEndedAtRef.current[serverTask.id];
            const isRecentlyEnded =
              endedAt && now - endedAt < 15000;

            // Shield data for this task
            const shieldData =
              completedLinksRef.current[serverTask.id] || {};

            // SHIELD CHECK 2: Har link ke liye shield data check karo
            const mergedLinks = (serverTask.links || []).map(
              (fbLink: any, idx: number) => {
                const protectedLink = shieldData[idx];

                if (protectedLink) {
                  const fbStatus = (fbLink.status || '').toLowerCase();
                  const isFbPending =
                    fbStatus === 'pending' ||
                    fbStatus === 'processing' ||
                    fbStatus === '';

                  // Agar Firebase mein abhi bhi pending hai aur shield mein solved hai
                  // → Shield data use karo (done/error ko pending se bachao)
                  if (isFbPending || isRecentlyEnded) {
                    return {
                      ...fbLink,
                      status: protectedLink.status,
                      finalLink:
                        protectedLink.finalLink || fbLink.finalLink,
                      best_button_name:
                        protectedLink.best_button_name ||
                        fbLink.best_button_name,
                      logs: protectedLink.logs || fbLink.logs,
                    };
                  }
                }

                // No shield → Firebase data as-is
                return fbLink;
              }
            );

            // Task status recalculate based on merged links
            const allDone =
              mergedLinks.length > 0 &&
              mergedLinks.every((l: any) => {
                const s = (l.status || '').toLowerCase();
                return (
                  s === 'done' ||
                  s === 'success' ||
                  s === 'error' ||
                  s === 'failed'
                );
              });

            const anySuccess = mergedLinks.some((l: any) => {
              const s = (l.status || '').toLowerCase();
              return s === 'done' || s === 'success';
            });

            let newTaskStatus = serverTask.status;
            if (allDone) {
              newTaskStatus = anySuccess ? 'completed' : 'failed';
            } else if (isRecentlyEnded) {
              // Grace period mein → processing rakhna
              newTaskStatus = 'processing';
            }

            return {
              ...serverTask,
              status: newTaskStatus,
              links: mergedLinks,
            };
          });
        });
      }
    } catch (e) {
      console.error('Failed to fetch tasks:', e);
    }
  }, []);

  /**
   * fetchEngineStatus()
   * /api/engine-status se GitHub Actions cron job ka status fetch karta hai.
   * ⚠️ engineStatus.signal === 'ONLINE' check karo — data.online nahi.
   */
  const fetchEngineStatus = useCallback(async () => {
    try {
      const res = await fetch('/api/engine-status');
      if (!res.ok) return;
      const data = await res.json();
      setEngineStatus(data);

      // Phase 4: Detect stuck tasks — use tasksRef (NO duplicate /api/tasks call)
      const stuckN = (data.processingCount || 0);
      if (stuckN > 0) {
        const now = Date.now();
        const stuck = tasksRef.current.filter((t: any) => {
          if (t.status !== 'processing') return false;
          const started = t.processingStartedAt || t.createdAt;
          return started && (now - new Date(started).getTime() > 10 * 60 * 1000);
        });
        setStuckCount(stuck.length);
      } else {
        setStuckCount(0);
      }
    } catch {
      // Silent fail — engine status non-critical
    }
  }, []);

  // ─── Phase 4: Force Reset All Stuck Tasks ──────────────────────────────
  const handleForceReset = useCallback(async (forceAll = false) => {
    setIsResettingStuck(true);
    setResetResult(null);
    try {
      const res = await fetch('/api/admin/reset-stuck', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ forceAll }),
      });
      const data = await res.json();
      if (data.ok) {
        setResetResult(`✅ Reset: ${data.recoveredTasks} task(s) + ${data.recoveredQueue} queue item(s)`);
        setStuckCount(0);
        // Refresh tasks after reset
        setTimeout(() => {
          fetchTasks();
          fetchEngineStatus();
          setResetResult(null);
        }, 2000);
      } else {
        setResetResult(`❌ Error: ${data.error || 'Unknown'}`);
      }
    } catch (err: any) {
      setResetResult(`❌ Network error: ${err.message}`);
    } finally {
      setIsResettingStuck(false);
    }
  }, [fetchTasks, fetchEngineStatus]);

  // ─────────────────────────────────────────────────────────────────────────
  // COMPUTED HELPERS — 3-Layer Data Resolution
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * getEffectiveStats()
   * Ek task ke links ka actual status count — 3 layers combine karke.
   * Live stream > Shield > Firebase polling
   */
  const getEffectiveStats = useCallback(
    (task: Task) => {
      const isLive = activeTaskId === task.id;
      const shieldData = completedLinksRef.current[task.id] || {};
      const total = task.links?.length || 0;

      let done = 0;
      let failed = 0;
      let pending = 0;

      for (let i = 0; i < total; i++) {
        let status = '';

        if (isLive && liveStatuses[i]) {
          // Layer 2: Stream active hai → stream status use karo
          status = liveStatuses[i];
        } else if (shieldData[i]) {
          // Layer 1: Shield data available → shield status use karo
          status = shieldData[i].status;
        } else {
          // Layer 3: Firebase data fallback
          status = task.links[i]?.status || '';
        }

        status = status.toLowerCase();

        if (status === 'done' || status === 'success') {
          done++;
        } else if (status === 'error' || status === 'failed') {
          failed++;
        } else {
          pending++;
        }
      }

      return { total, done, failed, pending };
    },
    [activeTaskId, liveStatuses]
  );

  /**
   * getTrueTaskStatus()
   * Task ka effective status return karta hai.
   * Active stream wala task hamesha 'processing' hai.
   */
  const getTrueTaskStatus = (
    task: Task,
    stats: ReturnType<typeof getEffectiveStats>
  ): 'processing' | 'completed' | 'failed' => {
    // Stream active hai toh always processing
    if (activeTaskId === task.id) return 'processing';

    // Sab links complete hain (done ya error)
    if (stats.total > 0 && stats.pending === 0) {
      return stats.done > 0 ? 'completed' : 'failed';
    }

    // Firebase se aaya status
    return task.status;
  };

  /**
   * getEffectiveLinkData()
   * Ek link ke liye 3-layer data resolution:
   * Layer 2 (Live stream) > Layer 1 (Shield) > Layer 3 (Firebase)
   */
  const getEffectiveLinkData = (
    task: Task,
    linkIdx: number,
    link: any
  ): { logs: LogEntry[]; finalLink: string | null; status: string } => {
    const isLive = activeTaskId === task.id;
    const shield = completedLinksRef.current[task.id]?.[linkIdx];

    // Layer 2: Live stream active hai aur is link ka status available hai
    if (isLive && liveStatuses[linkIdx]) {
      return {
        logs: liveLogs[linkIdx] || [],
        finalLink: liveLinks[linkIdx] || null,
        status: liveStatuses[linkIdx],
      };
    }

    // Layer 1: Shield data available hai (stream completed, protected from polling)
    if (shield) {
      return {
        logs: shield.logs || [],
        finalLink: shield.finalLink || link.finalLink || null,
        status: shield.status,
      };
    }

    // Layer 3: Firebase polling data (fallback)
    return {
      logs: link.logs || [],
      finalLink: link.finalLink || null,
      status: link.status || 'processing',
    };
  };

  // ─────────────────────────────────────────────────────────────────────────
  // LIVE STREAM — startLiveStream()
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * startLiveStream()
   * Manual "START ENGINE" button ke liye NDJSON stream.
   * POST /api/stream_solve → ReadableStream reader → real-time logs per link.
   *
   * ⭐ NDJSON format (stream_solve se aata hai):
   * { "id": 0, "msg": "⚡ HubCloud processing...", "type": "info" }
   * { "id": 0, "status": "done", "final": "https://...", "best_button_name": "Server 1" }
   * { "id": 0, "status": "finished" }
   */
  const startLiveStream = useCallback(
    async (taskId: string, links: any[]) => {
      // Duplicate stream check — ek task ke liye ek stream only
      if (streamStartedRef.current.has(taskId)) {
        console.log('Stream already started for task:', taskId);
        return;
      }

      // Shield se already solved links filter out karo
      // Unhe dobara send nahi karna stream_solve ko
      const shieldData = completedLinksRef.current[taskId] || {};
      const linksToSend = links.filter((_: any, idx: number) => {
        const s = shieldData[idx]?.status;
        return s !== 'done' && s !== 'success';
      });

      // Stream register karo — polling se protect hoga
      streamStartedRef.current.add(taskId);
      setActiveTaskId(taskId);

      // ── Initialize per-link states ──────────────────────────────────────
      const initLogs: Record<number, LogEntry[]> = {};
      const initLinks: Record<number, string | null> = {};
      const initStatuses: Record<number, string> = {};

      links.forEach((link: any, idx: number) => {
        const s = (link.status || '').toLowerCase();

        if (s === 'done' || s === 'success') {
          // Already solved — preserve as done
          initStatuses[idx] = 'done';
          initLinks[idx] = link.finalLink || null;
          initLogs[idx] = link.logs || [];
        } else if (s === 'error' || s === 'failed') {
          // Previously failed — retry karo, processing state se start
          initStatuses[idx] = 'processing';
          initLogs[idx] = [{ msg: 'Retrying...', type: 'warn' as const }];
          initLinks[idx] = null;
        } else {
          // Fresh pending link
          initStatuses[idx] = 'processing';
          initLogs[idx] = [];
          initLinks[idx] = null;
        }
      });

      setLiveLogs(initLogs);
      setLiveLinks(initLinks);
      setLiveStatuses(initStatuses);

      // ── Local refs for closure access ──────────────────────────────────
      // State se closure mein stale value milti hai,
      // isliye local mutable objects rakhte hain
      const currentLogs: Record<number, LogEntry[]> = { ...initLogs };
      const currentLinks: Record<number, string | null> = { ...initLinks };
      const currentStatus: Record<number, string> = { ...initStatuses };

      // ── Fetch NDJSON Stream ─────────────────────────────────────────────
      try {
        const response = await fetch('/api/stream_solve', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            links: linksToSend,
            taskId,
            extractedBy: 'Browser/Live',
          }),
        });

        if (!response.ok || !response.body) {
          throw new Error(`Stream request failed: ${response.status}`);
        }

        // ReadableStream reader — NDJSON line-by-line parse karo
        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = '';

        while (true) {
          const { done, value } = await reader.read();
          if (done) break;

          // Buffer mein append karo aur newlines pe split karo
          buffer += decoder.decode(value, { stream: true });
          const lines = buffer.split('\n');
          buffer = lines.pop() || ''; // Incomplete line ko buffer mein rakhna

          for (const line of lines) {
            if (!line.trim()) continue;

            try {
              const data = JSON.parse(line);
              const lid: number = data.id; // Link index

              // ── Log message ──────────────────────────────────────────
              if (data.msg && data.type) {
                const newLog: LogEntry = {
                  msg: data.msg,
                  type: data.type as LogEntry['type'],
                };
                currentLogs[lid] = [...(currentLogs[lid] || []), newLog];
                setLiveLogs((prev) => ({
                  ...prev,
                  [lid]: currentLogs[lid],
                }));
              }

              // ── Final link resolved ──────────────────────────────────
              if (data.final) {
                currentLinks[lid] = data.final;
                setLiveLinks((prev) => ({
                  ...prev,
                  [lid]: data.final,
                }));
              }

              // ── Status: done / error ─────────────────────────────────
              // Shield mein save karo — polling se protect ho jaye
              if (data.status === 'done' || data.status === 'error') {
                currentStatus[lid] = data.status;
                setLiveStatuses((prev) => ({
                  ...prev,
                  [lid]: data.status,
                }));

                // ⭐ Shield mein save karo
                completedLinksRef.current[taskId][lid] = {
                  status: data.status,
                  finalLink: data.final || currentLinks[lid],
                  best_button_name: data.best_button_name,
                  logs: [...(currentLogs[lid] || [])],
                };
              }

              // ── Status: finished ─────────────────────────────────────
              // Stream ka final marker — agar abhi bhi done/error nahi toh error mark karo
              if (data.status === 'finished') {
                if (
                  currentStatus[lid] !== 'done' &&
                  currentStatus[lid] !== 'error'
                ) {
                  completedLinksRef.current[taskId][lid] = {
                    status: 'error',
                    logs: [...(currentLogs[lid] || [])],
                  };
                  setLiveStatuses((prev) => ({
                    ...prev,
                    [lid]: 'error',
                  }));
                }
              }
            } catch (parseErr) {
              // Invalid JSON line — skip karo
              console.warn('Failed to parse NDJSON line:', line, parseErr);
            }
          }
        }
      } catch (streamErr) {
        console.error('Stream error:', streamErr);
      } finally {
        // ── Cleanup ───────────────────────────────────────────────────────
        streamStartedRef.current.delete(taskId);
        streamEndedAtRef.current[taskId] = Date.now();

        // Multiple fetches to catch relay results
        fetchTasks();                     // Immediate
        setTimeout(fetchTasks, 3000);     // Catch first relay
        setTimeout(fetchTasks, 8000);     // Catch slower relays
        setTimeout(fetchTasks, 15000);    // Final catch

        setActiveTaskId(null);
      }
    },
    [fetchTasks]
  );

  // ─────────────────────────────────────────────────────────────────────────
  // AUTO-PILOT SYSTEM — Full Feature (~200 lines)
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * addAutoPilotLog()
   * Log entry add karo (newest first, max 50 entries).
   * Ref + state dono update karo.
   */
  const addAutoPilotLog = useCallback(
    (msg: string, type: 'info' | 'success' | 'error' | 'warn' = 'info') => {
      const entry = { msg, type };
      const newLog = [entry, ...autoPilotLogRef.current].slice(0, 50);
      autoPilotLogRef.current = newLog;
      setAutoPilotLog(newLog);
    },
    []
  );

  /**
   * fetchFirebaseQueue()
   * GET /api/auto-process/queue?type=all
   * Queue stats aur pending items return karta hai.
   */
  const fetchFirebaseQueue =
    useCallback(async (): Promise<QueueStats | null> => {
      try {
        const res = await fetch('/api/auto-process/queue?type=all');
        if (!res.ok) return null;

        const data = await res.json();
        if (!data || !Array.isArray(data.items)) return null;

        const items: QueueItem[] = data.items;
        const pendingItems = items.filter((i) => i.status === 'pending');
        const completedItems = items.filter(
          (i) => i.status === 'completed'
        );
        const failedItems = items.filter(
          (i) => i.status === 'failed' || i.status === 'error'
        );

        return {
          totalPending: pendingItems.length,
          totalCompleted: completedItems.length,
          totalFailed: failedItems.length,
          totalAll: items.length,
          pendingItems,
        };
      } catch {
        return null;
      }
    }, []);

  /**
   * patchQueueStatus()
   * PATCH /api/auto-process/queue — Queue item ka status update karo.
   */
  const patchQueueStatus = useCallback(
    async (item: QueueItem, status: string, errorMsg?: string) => {
      try {
        await fetch('/api/auto-process/queue', {
          method: 'PATCH',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            id: item.id,
            collection: item.collection,
            status,
            error: errorMsg,
          }),
        });
      } catch (e) {
        console.error('Patch queue status failed:', e);
      }
    },
    []
  );

  /**
   * processQueueItemAutoPilot()
   * Ek queue item ko 3 steps mein process karta hai:
   * 1. POST /api/tasks → taskId milta hai
   * 2. GET /api/tasks → pending links filter karo
   * 3. POST /api/stream_solve → NDJSON stream (LIVE logs + output)
   *    Stream tab tak open rehta hai jab tak SAARE links done nahi ho jaate.
   *    Isliye auto-pilot NEXT movie TABHI start karega jab current movie
   *    ka stream close ho jayega → 1 MOVIE AT A TIME guaranteed.
   */
  const processQueueItemAutoPilot = useCallback(
    async (item: QueueItem): Promise<boolean> => {
      try {
        addAutoPilotLog(`📋 Processing: ${item.title || item.url}`, 'info');

        // ── Step 1: Task create karo ──────────────────────────────────────
        const taskRes = await fetch('/api/tasks', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ url: item.url }),
        });

        if (!taskRes.ok) {
          throw new Error(`Failed to create task: ${taskRes.status}`);
        }

        const taskData = await taskRes.json();
        const taskId: string = taskData.taskId;

        addAutoPilotLog(`✅ Task created: ${taskId}`, 'success');

        // ── Step 2: Links fetch karo ──────────────────────────────────────
        // Firebase ko task write karne ka waqt dena — 2s wait
        await sleep(2000);

        const tasksRes = await fetch('/api/tasks');
        if (!tasksRes.ok) {
          throw new Error('Failed to fetch tasks after creation');
        }

        const tasksData = await tasksRes.json();
        const task = tasksData.find((t: Task) => t.id === taskId);

        if (!task) {
          throw new Error(`Task ${taskId} not found after creation`);
        }

        // Only pending/unprocessed links bhejo
        const pendingLinks = (task.links || []).filter((l: any) => {
          const s = (l.status || '').toLowerCase();
          return s !== 'done' && s !== 'success';
        });

        addAutoPilotLog(
          `🔗 Found ${pendingLinks.length} links to solve (${(task.links || []).length} total)`,
          'info'
        );

        // ── Step 3: LIVE NDJSON Stream Solve ──────────────────────────────
        // stream_solve use karo — NDJSON stream return karta hai
        // Jab tak stream open hai, sab links process ho rahe hain
        // Stream close = sab links done = auto-pilot WAITS = 1 movie at a time
        //
        // PROBLEM 1 FIX: Ye stream tab tak close nahi hota jab tak SAARE links
        // process nahi ho jaate. Isliye auto-pilot NEXT movie tab tak start nahi
        // karega jab tak current movie complete nahi ho jaati.
        //
        // PROBLEM 2 FIX: NDJSON stream se har link ka live log, status, aur
        // final output real-time mein UI pe dikhta hai.

        // Set active task for UI — logs real-time dikhenge
        setActiveTaskId(taskId);
        setExpandedTask(taskId);

        // Initialize per-link live states
        const initLogs: Record<number, LogEntry[]> = {};
        const initLinks: Record<number, string | null> = {};
        const initStatuses: Record<number, string> = {};
        (task.links || []).forEach((link: any, idx: number) => {
          initStatuses[idx] = 'processing';
          initLogs[idx] = link.logs || [];
          initLinks[idx] = link.finalLink || null;
        });
        setLiveLogs(initLogs);
        setLiveLinks(initLinks);
        setLiveStatuses(initStatuses);

        // Shield initialize karo
        if (!completedLinksRef.current[taskId]) {
          completedLinksRef.current[taskId] = {};
        }

        // Local mutable objects for closure access (stale closure se bachne ke liye)
        const currentLogs: Record<number, LogEntry[]> = { ...initLogs };
        const currentLinks: Record<number, string | null> = { ...initLinks };
        const currentStatus: Record<number, string> = { ...initStatuses };

        addAutoPilotLog(`🚀 Starting LIVE stream for ${pendingLinks.length} links...`, 'info');

        const streamRes = await fetch('/api/stream_solve', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            links: pendingLinks,
            taskId,
            extractedBy: 'Server/Auto-Pilot',
          }),
        });

        if (!streamRes.ok || !streamRes.body) {
          throw new Error(`stream_solve failed: ${streamRes.status}`);
        }

        // ── Read NDJSON stream — BLOCKS until ALL links are done ──────────
        const reader = streamRes.body.getReader();
        const decoder = new TextDecoder();
        let buffer = '';

        while (true) {
          const { done, value } = await reader.read();
          if (done) break;

          buffer += decoder.decode(value, { stream: true });
          const streamLines = buffer.split('\n');
          buffer = streamLines.pop() || '';

          for (const line of streamLines) {
            if (!line.trim()) continue;
            try {
              const data = JSON.parse(line);
              const lid: number = data.id;

              // ── Live log message → UI update ────────────────────────
              if (data.msg && data.type) {
                const newLog: LogEntry = { msg: data.msg, type: data.type as LogEntry['type'] };
                currentLogs[lid] = [...(currentLogs[lid] || []), newLog];
                setLiveLogs(prev => ({ ...prev, [lid]: currentLogs[lid] }));
              }

              // ── Final link resolved ─────────────────────────────────
              if (data.final) {
                currentLinks[lid] = data.final;
                setLiveLinks(prev => ({ ...prev, [lid]: data.final }));
              }

              // ── Status: done / error → Shield save ──────────────────
              if (data.status === 'done' || data.status === 'error') {
                currentStatus[lid] = data.status;
                setLiveStatuses(prev => ({ ...prev, [lid]: data.status }));
                completedLinksRef.current[taskId][lid] = {
                  status: data.status,
                  finalLink: data.final || currentLinks[lid],
                  best_button_name: data.best_button_name,
                  logs: [...(currentLogs[lid] || [])],
                };
              }

              // ── Finished marker ─────────────────────────────────────
              if (data.status === 'finished') {
                if (currentStatus[lid] !== 'done' && currentStatus[lid] !== 'error') {
                  completedLinksRef.current[taskId][lid] = {
                    status: 'error',
                    logs: [...(currentLogs[lid] || [])],
                  };
                  setLiveStatuses(prev => ({ ...prev, [lid]: 'error' }));
                }
              }
            } catch { /* skip bad JSON line */ }
          }
        }

        // ── Stream ended — all links processed within this request ────────
        setActiveTaskId(null);
        fetchTasks();
        await sleep(2000);
        fetchTasks();

        addAutoPilotLog(`🎉 Done: ${item.title || item.url}`, 'success');
        await patchQueueStatus(item, 'completed');

        return true;
      } catch (e: any) {
        const errorMsg = e?.message || 'Unknown error';
        addAutoPilotLog(`❌ Failed: ${errorMsg}`, 'error');
        await patchQueueStatus(item, 'failed', errorMsg);
        setActiveTaskId(null);
        return false;
      }
    },
    [addAutoPilotLog, patchQueueStatus, fetchTasks]
  );

  /**
   * runAutoPilot()
   * Main Auto-Pilot loop:
   * idle → fetching → running → done/stopped
   *
   * Har item ke baad 3s cooldown.
   * autoPilotActiveRef.current false hone pe stop.
   */
  const runAutoPilot = useCallback(async () => {
    // ── Reset all state ────────────────────────────────────────────────
    autoPilotLogRef.current = [];
    setAutoPilotLog([]);
    setAutoPilotProcessed(0);
    setCurrentQueueItem(null);
    setAutoPilotPhase('fetching');
    setAutoPilotStatusMsg('Fetching queue from Firebase...');
    addAutoPilotLog('🚀 Auto-Pilot started', 'info');

    // ── Fetch queue ────────────────────────────────────────────────────
    const stats = await fetchFirebaseQueue();

    if (!stats) {
      addAutoPilotLog('❌ Failed to fetch queue from Firebase', 'error');
      setAutoPilotPhase('stopped');
      setAutoPilotStatusMsg('Failed to fetch queue. Check Firebase connection.');
      autoPilotActiveRef.current = false;
      return;
    }

    setQueueStats(stats);

    // ── Empty queue check ──────────────────────────────────────────────
    if (stats.pendingItems.length === 0) {
      addAutoPilotLog('✅ Queue is empty — no pending items', 'success');
      setAutoPilotPhase('done');
      setAutoPilotStatusMsg('Queue is empty. All done!');
      autoPilotActiveRef.current = false;
      return;
    }

    // ── Start processing loop ──────────────────────────────────────────
    setAutoPilotPhase('running');
    addAutoPilotLog(
      `📋 ${stats.pendingItems.length} pending items found. Starting...`,
      'info'
    );

    let processed = 0;

    for (const item of stats.pendingItems) {
      // Stop button check — har iteration mein check karo
      if (!autoPilotActiveRef.current) {
        setAutoPilotStatusMsg('Stopped by user. Current item was completed.');
        setAutoPilotPhase('stopped');
        addAutoPilotLog('⛔ Stopped by user', 'warn');
        setCurrentQueueItem(null);
        return;
      }

      // Current item display update
      setCurrentQueueItem(item);
      setAutoPilotStatusMsg(
        `Processing ${processed + 1}/${stats.pendingItems.length}: ${item.title || item.url}`
      );

      // Item process karo
      const success = await processQueueItemAutoPilot(item);
      processed++;
      setAutoPilotProcessed(processed);

      // Live stats update karo
      setQueueStats((prev) => {
        if (!prev) return prev;
        return {
          ...prev,
          totalPending: Math.max(0, prev.totalPending - 1),
          totalCompleted: success
            ? prev.totalCompleted + 1
            : prev.totalCompleted,
          totalFailed: !success ? prev.totalFailed + 1 : prev.totalFailed,
        };
      });

      // Last item ke baad cooldown nahi chahiye
      if (processed < stats.pendingItems.length) {
        addAutoPilotLog('⏳ Cooling down 3 seconds...', 'info');
        await sleep(3000);
      }
    }

    // ── All items done ─────────────────────────────────────────────────
    setCurrentQueueItem(null);
    setAutoPilotPhase('done');
    setAutoPilotStatusMsg(`✅ All done! ${processed} items processed.`);
    addAutoPilotLog(
      `🎉 Auto-Pilot complete. ${processed} items processed.`,
      'success'
    );
    autoPilotActiveRef.current = false;
  }, [addAutoPilotLog, fetchFirebaseQueue, processQueueItemAutoPilot]);

  /**
   * handleStartAutoPilot()
   * autoPilotActiveRef set karo aur runAutoPilot() call karo.
   */
  const handleStartAutoPilot = () => {
    setIsAutoPilotEnabled(true);
    autoPilotActiveRef.current = true;
    runAutoPilot();
  };

  /**
   * handleStopAutoPilot()
   * autoPilotActiveRef false karo — current item complete hone ke baad stop.
   */
  const handleStopAutoPilot = () => {
    setIsAutoPilotEnabled(false);
    autoPilotActiveRef.current = false;
    addAutoPilotLog('⛔ Stop requested — finishing current item...', 'warn');
    setAutoPilotStatusMsg('Stopping after current item completes...');
  };

  /**
   * handleResetAutoPilot()
   * Saari Auto-Pilot state reset karo — fresh start ke liye.
   */
  const handleResetAutoPilot = () => {
    setIsAutoPilotEnabled(false);
    autoPilotActiveRef.current = false;
    setAutoPilotPhase('idle');
    setQueueStats(null);
    setCurrentQueueItem(null);
    setAutoPilotProcessed(0);
    setAutoPilotStatusMsg('');
    setAutoPilotLog([]);
    autoPilotLogRef.current = [];
  };

  /**
   * handleRefreshQueue()
   * Queue stats manually refresh karo.
   */
  const handleRefreshQueue = async () => {
    const stats = await fetchFirebaseQueue();
    if (stats) {
      setQueueStats(stats);
      addAutoPilotLog(`🔄 Queue refreshed: ${stats.totalPending} pending`, 'info');
    }
  };

  // ─────────────────────────────────────────────────────────────────────────
  // PROBLEM 4 FIX: GLOBAL "RETRY FAILED" — Only retry failed/pending links
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * handleRetryFailed()
   * SIRF failed/pending links ko retry karta hai.
   * Successfully solved links (done/success) ko KABHI touch nahi karta.
   */
  const handleRetryFailed = useCallback(async (task: Task) => {
    if (!task.links || task.links.length === 0) return;

    // Filter: Only failed/pending/error links
    const failedLinks = task.links.filter((l: any) => {
      const s = (l.status || '').toLowerCase();
      return s !== 'done' && s !== 'success';
    });

    if (failedLinks.length === 0) return;

    // Shield initialize
    completedLinksRef.current[task.id] = completedLinksRef.current[task.id] || {};

    // Preserve already-done links in shield
    task.links.forEach((l: any, idx: number) => {
      const s = (l.status || '').toLowerCase();
      if (s === 'done' || s === 'success') {
        completedLinksRef.current[task.id][idx] = {
          status: 'done',
          finalLink: l.finalLink,
          best_button_name: l.best_button_name,
          logs: l.logs || [{ msg: '⚡ Previously solved', type: 'success' }],
        };
      }
    });

    setExpandedTask(task.id);
    setActiveTab('processing');

    // Send ONLY failed links to stream_solve with retryFailedOnly flag
    await startLiveStream(task.id, task.links);
  }, [startLiveStream]);

  // ─────────────────────────────────────────────────────────────────────────
  // PROBLEM 5 FIX: INDIVIDUAL LINK REPEAT
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * handleRepeatLink()
   * Ek specific link ko individually repeat karta hai.
   * Baaqi saare links untouched rehte hain.
   */
  const handleRepeatLink = useCallback(async (taskId: string, linkId: number | string, allLinks: any[]) => {
    setRepeatingLinkId(linkId);

    try {
      // Reset this link's status in Firebase first
      const taskRef = `/api/tasks`;
      // We don't need to update Firebase directly — stream_solve will handle it

      // Stream solve with singleLinkId parameter
      const response = await fetch('/api/stream_solve', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          links: allLinks,
          taskId,
          extractedBy: 'Browser/Live',
          singleLinkId: linkId,
        }),
      });

      if (!response.ok || !response.body) {
        console.error('Single link retry failed:', response.status);
        return;
      }

      // Parse NDJSON response
      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop() || '';

        for (const line of lines) {
          if (!line.trim()) continue;
          try {
            const data = JSON.parse(line);
            const lid = data.id;

            // Update live states for this link
            if (data.msg && data.type) {
              setLiveLogs(prev => ({
                ...prev,
                [lid]: [...(prev[lid] || []), { msg: data.msg, type: data.type }],
              }));
            }

            if (data.final) {
              setLiveLinks(prev => ({ ...prev, [lid]: data.final }));
            }

            if (data.status === 'done' || data.status === 'error') {
              setLiveStatuses(prev => ({ ...prev, [lid]: data.status }));

              // Update shield
              if (!completedLinksRef.current[taskId]) completedLinksRef.current[taskId] = {};
              completedLinksRef.current[taskId][lid] = {
                status: data.status,
                finalLink: data.final,
                best_button_name: data.best_button_name,
                logs: [],
              };
            }
          } catch { /* skip bad JSON */ }
        }
      }

      // Refresh tasks to get updated data
      fetchTasks();
      setTimeout(fetchTasks, 3000);
    } catch (err: any) {
      console.error('handleRepeatLink error:', err);
    } finally {
      setRepeatingLinkId(null);
    }
  }, [fetchTasks]);

  // ─────────────────────────────────────────────────────────────────────────
  // MANUAL PROCESS — startProcess()
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * startProcess()
   * "START ENGINE" button handler.
   *
   * Steps:
   * 1. Duplicate URL check
   * 2. POST /api/tasks → taskId
   * 3. fetchTasks → expand new task → switch to Processing tab
   * 4. startLiveStream(taskId, links)
   * 5. Cleanup states
   *
   * ⚠️ URL input Enter key se bhi submit hota hai — <form> nahi hai isliye no reload.
   */
  const startProcess = async () => {
    if (!url.trim()) return;
    setError(null);

    // ── Duplicate URL Check ────────────────────────────────────────────
    // Normalize: lowercase, remove protocol, remove trailing slash
    const normalize = (u: string) =>
      u
        .toLowerCase()
        .replace(/^https?:\/\//, '')
        .replace(/\/$/, '');

    const normalizedInput = normalize(url.trim());
    const duplicate = tasks.find(
      (t) => normalize(t.url) === normalizedInput
    );

    if (duplicate) {
      const dupStats = getEffectiveStats(duplicate);
      const dupStatus = getTrueTaskStatus(duplicate, dupStats);

      if (dupStatus === 'completed') {
        setError('Pehle se nikal chuki hai ✅ — Completed tab mein dekho');
        setExpandedTask(duplicate.id);
        setActiveTab('completed');
        return;
      }

      if (dupStatus === 'processing') {
        setError('Already process ho rahi hai ⏳ — Processing tab mein dekho');
        setExpandedTask(duplicate.id);
        setActiveTab('processing');
        return;
      }
    }

    // ── Step 2: POST /api/tasks ────────────────────────────────────────
    try {
      setIsConnecting(true);

      const res = await fetch('/api/tasks', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ url: url.trim() }),
      });

      if (!res.ok) {
        const d = await res.json().catch(() => ({}));
        throw new Error(d.error || `Request failed: ${res.status}`);
      }

      const { taskId } = await res.json();

      // ── Step 3: Expand task + switch tab ──────────────────────────────
      setIsConnecting(false);
      setIsProcessing(true);

      await fetchTasks();
      setExpandedTask(taskId);
      setActiveTab('processing');

      // Shield initialize karo is task ke liye
      completedLinksRef.current[taskId] = {};

      // ── Step 4: Start Live Stream ──────────────────────────────────────
      // Fresh fetch karo — task mein links honge ab
      const tasksRes = await fetch('/api/tasks');
      if (tasksRes.ok) {
        const tasksData = await tasksRes.json();
        const newTask = tasksData.find((t: Task) => t.id === taskId);
        if (newTask) {
          setTasks(tasksData);
          await startLiveStream(taskId, newTask.links || []);
        }
      }

      // ── Step 5: Cleanup ────────────────────────────────────────────────
      setUrl('');
      setIsProcessing(false);
      setIsDone(true);

      // "ALL DONE" badge 3 second ke baad hide ho jaata hai
      setTimeout(() => {
        setIsDone(false);
      }, 3000);
    } catch (e: any) {
      setIsConnecting(false);
      setIsProcessing(false);
      setError(e?.message || 'Something went wrong. Try again.');
    }
  };

  // ─────────────────────────────────────────────────────────────────────────
  // TASK ACTIONS
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * handleDeleteTask()
   * Task delete karo + local state se remove karo + shield cleanup.
   * e.stopPropagation() — task expand toggle prevent karo.
   */
  const handleDeleteTask = async (
    taskId: string,
    e: React.MouseEvent
  ) => {
    e.stopPropagation();
    setDeletingTaskId(taskId);

    try {
      await fetch('/api/tasks', {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ taskId }),
      });

      // Local state se hatao
      setTasks((prev) => prev.filter((t) => t.id !== taskId));

      // Shield cleanup
      delete completedLinksRef.current[taskId];
      delete streamEndedAtRef.current[taskId];

      // Expanded state cleanup
      if (expandedTask === taskId) {
        setExpandedTask(null);
      }
    } catch (e) {
      console.error('Delete failed:', e);
    } finally {
      setDeletingTaskId(null);
    }
  };

  /**
   * handleRetryTask()
   * Old task delete karo + naya task create karo same URL se + live stream start karo.
   */
  const handleRetryTask = async (task: Task, e: React.MouseEvent) => {
    e.stopPropagation();
    setRetryingTaskId(task.id);

    try {
      // ── Old task cleanup ───────────────────────────────────────────────
      await fetch('/api/tasks', {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ taskId: task.id }),
      });

      setTasks((prev) => prev.filter((t) => t.id !== task.id));
      delete completedLinksRef.current[task.id];
      delete streamEndedAtRef.current[task.id];

      if (expandedTask === task.id) {
        setExpandedTask(null);
      }

      // ── New task create karo ───────────────────────────────────────────
      const res = await fetch('/api/tasks', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ url: task.url }),
      });

      if (!res.ok) {
        throw new Error(`Retry task creation failed: ${res.status}`);
      }

      const { taskId: newTaskId } = await res.json();

      // Shield initialize karo
      completedLinksRef.current[newTaskId] = {};
      setExpandedTask(newTaskId);
      setActiveTab('processing');

      await fetchTasks();

      // Links fetch karo aur stream start karo
      const tasksRes = await fetch('/api/tasks');
      if (tasksRes.ok) {
        const tasksData = await tasksRes.json();
        const newTask = tasksData.find((t: Task) => t.id === newTaskId);
        if (newTask) {
          setTasks(tasksData);
          await startLiveStream(newTaskId, newTask.links || []);
        }
      }
    } catch (e) {
      console.error('Retry failed:', e);
    } finally {
      setRetryingTaskId(null);
    }
  };

  // ─────────────────────────────────────────────────────────────────────────
  // TAB FILTERING — getFilteredTasks()
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * getFilteredTasks()
   * Active tab ke according tasks filter karo.
   * History tab mein newest first sort hota hai.
   */
  const getFilteredTasks = (): Task[] => {
    switch (activeTab) {
      case 'processing':
        return tasks.filter(
          (t) => getTrueTaskStatus(t, getEffectiveStats(t)) === 'processing'
        );
      case 'completed':
        return tasks.filter(
          (t) => getTrueTaskStatus(t, getEffectiveStats(t)) === 'completed'
        );
      case 'failed':
        return tasks.filter(
          (t) => getTrueTaskStatus(t, getEffectiveStats(t)) === 'failed'
        );
      case 'history':
        return [...tasks].sort(
          (a, b) =>
            new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime()
        );
      case 'home':
      default:
        return tasks;
    }
  };

  // ─────────────────────────────────────────────────────────────────────────
  // TAB BADGE COUNTS
  // ─────────────────────────────────────────────────────────────────────────

  // ─── TAB BADGE COUNTS (memoized — no re-calc every render) ──────────────
  const processingCount = useMemo(() =>
    tasks.filter(t => getTrueTaskStatus(t, getEffectiveStats(t)) === 'processing').length,
    [tasks, activeTaskId, liveStatuses]
  );

  const completedCount = useMemo(() =>
    tasks.filter(t => getTrueTaskStatus(t, getEffectiveStats(t)) === 'completed').length,
    [tasks, activeTaskId, liveStatuses]
  );

  const failedCount = useMemo(() =>
    tasks.filter(t => getTrueTaskStatus(t, getEffectiveStats(t)) === 'failed').length,
    [tasks, activeTaskId, liveStatuses]
  );

  const historyCount = tasks.length;

  // ─────────────────────────────────────────────────────────────────────────
  // USE EFFECTS
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * Mount effect:
   * 1. Page load pe tasks rehydrate karo
   * 2. Engine status fetch karo
   * 3. 5-second tasks polling start karo  ← setInterval (NOT onSnapshot)
   * 4. 20-second engine polling start karo
   * 5. Cleanup on unmount
   */
  useEffect(() => {
    // Initial load
    rehydrateFromFirebase();
    fetchEngineStatus();

    // ⭐ 10-second polling — reduced from 5s to cut API calls 50%
    // Skip polling entirely when stream is active (stream gives real-time data)
    const pollInterval = setInterval(() => {
      if (streamStartedRef.current.size > 0) return; // Skip during stream
      fetchTasks();
    }, 10_000);

    // 60-second engine status polling (was 20s — 67% reduction)
    enginePollRef.current = setInterval(fetchEngineStatus, 60_000);

    return () => {
      clearInterval(pollInterval);
      if (enginePollRef.current) {
        clearInterval(enginePollRef.current);
      }
    };
  }, [rehydrateFromFirebase, fetchTasks, fetchEngineStatus]);

  // ─────────────────────────────────────────────────────────────────────────
  // ENGINE STATUS HELPERS
  // ─────────────────────────────────────────────────────────────────────────

  const isEngineOnline = engineStatus?.signal === 'ONLINE';

  /**
   * Engine status bar ka background gradient
   * - ONLINE + backgroundActive: emerald glow (active processing)
   * - ONLINE + idle: subtle emerald
   * - OFFLINE: rose tint
   */
  const getEngineBarStyles = (): string => {
    if (!engineStatus) return 'bg-slate-900/50 border border-white/5';
    if (!isEngineOnline) {
      return 'bg-rose-950/50 border border-rose-500/20';
    }
    if (engineStatus.backgroundActive) {
      return 'bg-gradient-to-r from-emerald-950/80 to-emerald-900/40 border border-emerald-500/30 shadow-lg shadow-emerald-500/10';
    }
    return 'bg-emerald-950/30 border border-emerald-500/20';
  };

  // ─────────────────────────────────────────────────────────────────────────
  // RENDER
  // ─────────────────────────────────────────────────────────────────────────

  const filteredTasks = getFilteredTasks();

  // ─── AI Control Center fullscreen mode ──────────────────────────────────
  if (showAiPanel) {
    return <AiControlCenter onBack={() => setShowAiPanel(false)} />;
  }

  return (
    <div className="min-h-screen min-h-dvh bg-black text-white overflow-x-hidden">

      {/* ── BACKGROUND GRADIENT ──────────────────────────────────────────── */}
      <div className="fixed inset-0 bg-[radial-gradient(ellipse_at_top,_var(--tw-gradient-stops))] from-indigo-950/20 via-black to-black pointer-events-none z-0" />

      {/* ── MAIN CONTENT AREA ────────────────────────────────────────────── */}
      <div className="relative z-10 max-w-2xl mx-auto px-4 pt-5 pb-28">

        {/* ════════════════════════════════════════════════════════════════ */}
        {/* HEADER                                                          */}
        {/* ════════════════════════════════════════════════════════════════ */}
        <div className="flex items-center justify-between mb-4">

          {/* Left: Logo + Title */}
          <div className="flex items-center gap-2.5">
            <div className="w-9 h-9 rounded-xl bg-indigo-600 flex items-center justify-center shadow-lg shadow-indigo-600/30">
              <Bolt className="w-5 h-5 text-white fill-white" />
            </div>
            <div>
              <span className="text-xl font-black tracking-tight bg-gradient-to-r from-white to-slate-400 bg-clip-text text-transparent">
                MFLIX PRO
              </span>
            </div>
          </div>

          {/* Right: AI Button + Engine status indicator */}
          <div className="flex items-center gap-3">
            {/* AI Control Center Button */}
            <button
              onClick={() => setShowAiPanel(true)}
              className="relative w-8 h-8 rounded-lg bg-violet-600/20 border border-violet-500/30 flex items-center justify-center hover:bg-violet-600/40 active:scale-95 transition-all group"
            >
              <Brain className="w-4 h-4 text-violet-400 group-hover:text-violet-300" />
              <span className="absolute -top-0.5 -right-0.5 w-2 h-2 rounded-full bg-violet-500 animate-pulse" />
            </button>

            <div className="flex items-center gap-2">
              <PulseDot
              color={isEngineOnline ? 'bg-emerald-400' : 'bg-rose-400'}
            />
            <span className="text-[10px] font-mono text-slate-400 uppercase tracking-widest">
              {isEngineOnline ? 'LIVE ENGINE' : 'ENGINE OFFLINE'}
            </span>
            </div>
          </div>
        </div>

        {/* ════════════════════════════════════════════════════════════════ */}
        {/* ENGINE STATUS BAR                                               */}
        {/* Full-width card — gradient based on engine state               */}
        {/* ════════════════════════════════════════════════════════════════ */}
        <AnimatePresence>
          {engineStatus && (
            <motion.div
              key="engine-bar"
              initial={{ opacity: 0, y: -8 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -8 }}
              transition={{ duration: 0.3 }}
              className={`rounded-2xl p-4 mb-4 transition-all ${getEngineBarStyles()}`}
            >
              <div className="flex items-center justify-between">

                {/* Left: Icon + Status Info */}
                <div className="flex items-center gap-3">
                  {isEngineOnline ? (
                    <Wifi className="w-4 h-4 text-emerald-400 flex-shrink-0" />
                  ) : (
                    <WifiOff className="w-4 h-4 text-rose-400 flex-shrink-0" />
                  )}

                  <div>
                    {/* Status line */}
                    <div className="flex items-center gap-2 flex-wrap">
                      <span
                        className={`text-xs font-bold ${
                          isEngineOnline
                            ? 'text-emerald-400'
                            : 'text-rose-400'
                        }`}
                      >
                        GitHub Engine:{' '}
                        {isEngineOnline ? 'ONLINE' : 'OFFLINE'}
                      </span>

                      {/* BACKGROUND ACTIVE badge */}
                      {engineStatus.backgroundActive && (
                        <motion.span
                          initial={{ opacity: 0, scale: 0.9 }}
                          animate={{ opacity: 1, scale: 1 }}
                          className="px-2 py-0.5 text-[10px] font-mono font-bold rounded-full bg-emerald-500/20 border border-emerald-500/30 text-emerald-400 uppercase"
                        >
                          BACKGROUND ACTIVE
                        </motion.span>
                      )}
                    </div>

                    {/* Sub info line */}
                    <div className="text-[11px] text-slate-500 mt-0.5 font-mono">
                      Last ping:{' '}
                      {engineStatus.timeSinceLastRun || 'N/A'}
                      {engineStatus.pendingCount > 0 &&
                        ` · ${engineStatus.pendingCount} pending`}
                      {engineStatus.processingCount > 0 &&
                        ` · ${engineStatus.processingCount} processing`}
                    </div>
                  </div>
                </div>

                {/* Right: Safe to close browser (agar background active) */}
                {engineStatus.backgroundActive && !stuckCount && (
                  <div className="flex items-center gap-1.5 text-[11px] text-emerald-400 flex-shrink-0">
                    <Shield className="w-3.5 h-3.5" />
                    <span className="hidden sm:inline font-mono">
                      Safe to close browser
                    </span>
                  </div>
                )}

                {/* Phase 4: STUCK TASK WARNING + FORCE RESET */}
                {stuckCount > 0 && (
                  <div className="flex items-center gap-2 flex-shrink-0">
                    <span className="text-[10px] font-mono text-amber-400">
                      ⚠️ {stuckCount} stuck
                    </span>
                    <button
                      onClick={() => handleForceReset(false)}
                      disabled={isResettingStuck}
                      className="px-2.5 py-1 text-[10px] font-mono font-bold rounded-lg bg-rose-500/20 border border-rose-500/30 text-rose-400 hover:bg-rose-500/30 transition-all disabled:opacity-50"
                    >
                      {isResettingStuck ? '⏳ Resetting...' : '🔄 Force Reset'}
                    </button>
                  </div>
                )}
              </div>

              {/* Phase 4: Reset Result Toast */}
              {resetResult && (
                <div className={`mt-2 px-3 py-1.5 rounded-lg text-[11px] font-mono ${
                  resetResult.startsWith('✅') ? 'bg-emerald-500/10 text-emerald-400' : 'bg-rose-500/10 text-rose-400'
                }`}>
                  {resetResult}
                </div>
              )}
            </motion.div>
          )}
        </AnimatePresence>

        {/* ════════════════════════════════════════════════════════════════ */}
        {/* HOME TAB — Input Section + Auto-Pilot Dashboard                */}
        {/* ════════════════════════════════════════════════════════════════ */}
        {activeTab === 'home' && (
          <>
            {/* ── URL INPUT + START ENGINE BUTTON ────────────────────── */}
            <div className="bg-white/5 backdrop-blur-xl border border-white/10 rounded-[2rem] p-6 mb-4">

              {/* URL Input Field */}
              <div className="relative mb-4">
                <LinkIcon className="absolute left-4 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-500 pointer-events-none" />
                <input
                  type="url"
                  value={url}
                  onChange={(e) => {
                    setUrl(e.target.value);
                    setError(null);
                  }}
                  onKeyDown={(e) => {
                    // Enter key se start — <form> nahi hai isliye no page reload
                    if (e.key === 'Enter') startProcess();
                  }}
                  placeholder="🔗 Paste Movie URL here..."
                  className="w-full bg-black/40 border border-white/10 pl-12 pr-4 py-4 rounded-2xl text-white placeholder-slate-600 text-sm focus:outline-none focus:border-indigo-500/50 transition-colors"
                />
              </div>

              {/* Error Message */}
              <AnimatePresence>
                {error && (
                  <motion.div
                    key="error-msg"
                    initial={{ opacity: 0, height: 0, marginBottom: 0 }}
                    animate={{ opacity: 1, height: 'auto', marginBottom: 12 }}
                    exit={{ opacity: 0, height: 0, marginBottom: 0 }}
                    className="overflow-hidden"
                  >
                    <div className="text-rose-400 text-xs font-mono bg-rose-500/10 border border-rose-500/20 rounded-xl px-3 py-2.5 flex items-center gap-2">
                      <AlertCircle className="w-3.5 h-3.5 flex-shrink-0" />
                      {error}
                    </div>
                  </motion.div>
                )}
              </AnimatePresence>

              {/* START ENGINE Button — 4 visual states */}
              <button
                onClick={startProcess}
                disabled={
                  isConnecting ||
                  isProcessing ||
                  isDone ||
                  isAutoPilotRunning
                }
                className={`w-full py-4 rounded-2xl font-bold text-sm flex items-center justify-center gap-2.5 transition-all duration-200 ${
                  isDone
                    ? 'bg-emerald-600 text-white shadow-lg shadow-emerald-600/30'
                    : error && !isConnecting && !isProcessing && !isDone
                    ? 'bg-rose-600 hover:bg-rose-500 text-white'
                    : isConnecting || isProcessing
                    ? 'bg-indigo-700 text-white cursor-not-allowed opacity-80'
                    : 'bg-indigo-600 hover:bg-indigo-500 text-white shadow-lg shadow-indigo-600/20 disabled:opacity-50 disabled:cursor-not-allowed'
                }`}
              >
                {/* State: Connecting */}
                {isConnecting && (
                  <>
                    <Loader2 className="w-4 h-4 animate-spin" />
                    CONNECTING...
                  </>
                )}

                {/* State: Processing Live */}
                {isProcessing && !isConnecting && (
                  <>
                    <RotateCcw className="w-4 h-4 animate-spin" />
                    PROCESSING LIVE...
                  </>
                )}

                {/* State: All Done */}
                {isDone && !isProcessing && !isConnecting && (
                  <>
                    <CircleCheck className="w-4 h-4" />
                    ALL DONE ✅
                  </>
                )}

                {/* State: Error */}
                {error &&
                  !isConnecting &&
                  !isProcessing &&
                  !isDone && (
                    <>
                      <AlertTriangle className="w-4 h-4" />
                      ERROR — Try Again
                    </>
                  )}

                {/* State: Default */}
                {!isConnecting &&
                  !isProcessing &&
                  !isDone &&
                  !error && (
                    <>
                      <Rocket className="w-4 h-4" />
                      START ENGINE
                    </>
                  )}
              </button>

              {/* Auto-Pilot running warning */}
              {isAutoPilotRunning && (
                <p className="text-[11px] text-slate-600 text-center mt-2 font-mono">
                  Auto-Pilot chal raha hai — manual processing disabled
                </p>
              )}
            </div>

            {/* ── AUTO-PILOT DASHBOARD ─────────────────────────────────── */}
            <motion.div
              key="autopilot-dashboard"
              initial={{ opacity: 0, y: 10 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.1 }}
              className={`rounded-[2rem] p-5 mb-4 transition-all relative overflow-hidden ${
                autoPilotPhase === 'running' || autoPilotPhase === 'fetching'
                  ? 'border border-violet-500/40 bg-violet-950/20 shadow-lg shadow-violet-500/10'
                  : autoPilotPhase === 'done'
                  ? 'border border-emerald-500/30 bg-emerald-950/10'
                  : autoPilotPhase === 'stopped'
                  ? 'border border-rose-500/30 bg-rose-950/10'
                  : 'border border-white/10 bg-white/[0.02]'
              }`}
            >
              {/* Scanning line animation — running phase mein */}
              {isAutoPilotRunning && (
                <div className="absolute inset-0 overflow-hidden pointer-events-none rounded-[2rem]">
                  <motion.div
                    animate={{ y: ['-100%', '200%'] }}
                    transition={{
                      duration: 2.5,
                      repeat: Infinity,
                      ease: 'linear',
                    }}
                    className="absolute inset-x-0 h-px bg-gradient-to-r from-transparent via-violet-400/50 to-transparent"
                  />
                </div>
              )}

              {/* ─ Dashboard Header Row ─────────────────────────────────── */}
              <div className="flex items-center justify-between mb-4 relative z-10">

                {/* Left: Icon + Title + Phase Badge */}
                <div className="flex items-center gap-2.5">
                  {isAutoPilotRunning ? (
                    <Cpu className="w-5 h-5 text-violet-400 animate-pulse" />
                  ) : (
                    <Bot className="w-5 h-5 text-violet-400" />
                  )}

                  <span className="font-bold text-white text-sm tracking-wider">
                    AUTO-PILOT
                  </span>

                  {/* Phase Badge */}
                  <AnimatePresence mode="wait">
                    {autoPilotPhase !== 'idle' && (
                      <motion.span
                        key={autoPilotPhase}
                        initial={{ opacity: 0, scale: 0.8 }}
                        animate={{ opacity: 1, scale: 1 }}
                        exit={{ opacity: 0, scale: 0.8 }}
                        className={`px-2 py-0.5 text-[10px] font-mono font-bold rounded-full uppercase ${
                          isAutoPilotRunning
                            ? 'bg-violet-500/20 border border-violet-500/30 text-violet-400'
                            : autoPilotPhase === 'done'
                            ? 'bg-emerald-500/20 border border-emerald-500/30 text-emerald-400'
                            : 'bg-rose-500/20 border border-rose-500/30 text-rose-400'
                        }`}
                      >
                        {autoPilotPhase === 'fetching'
                          ? 'FETCHING'
                          : autoPilotPhase === 'running'
                          ? 'LIVE'
                          : autoPilotPhase === 'done'
                          ? 'DONE'
                          : 'STOPPED'}
                      </motion.span>
                    )}
                  </AnimatePresence>
                </div>

                {/* Right: Action Buttons */}
                <div className="flex items-center gap-2">

                  {/* Refresh button — idle ya done ya stopped pe */}
                  {autoPilotPhase !== 'idle' && (
                    <button
                      onClick={handleRefreshQueue}
                      className="p-1.5 rounded-lg text-slate-500 hover:text-slate-300 transition-colors"
                      title="Refresh queue"
                    >
                      <RefreshCcw className="w-3.5 h-3.5" />
                    </button>
                  )}

                  {/* START button — idle/done/stopped pe */}
                  {(autoPilotPhase === 'idle' ||
                    autoPilotPhase === 'done' ||
                    autoPilotPhase === 'stopped') && (
                    <button
                      onClick={handleStartAutoPilot}
                      className="flex items-center gap-1.5 px-3 py-1.5 rounded-xl bg-violet-600 hover:bg-violet-500 text-white text-xs font-bold transition-all active:scale-95"
                    >
                      <Play className="w-3 h-3 fill-white" />
                      START
                    </button>
                  )}

                  {/* STOP button — fetching/running pe */}
                  {(autoPilotPhase === 'fetching' ||
                    autoPilotPhase === 'running') && (
                    <button
                      onClick={handleStopAutoPilot}
                      className="flex items-center gap-1.5 px-3 py-1.5 rounded-xl bg-rose-600 hover:bg-rose-500 text-white text-xs font-bold transition-all active:scale-95"
                    >
                      <Pause className="w-3 h-3 fill-white" />
                      STOP
                    </button>
                  )}

                  {/* RESET button — done/stopped pe */}
                  {(autoPilotPhase === 'done' ||
                    autoPilotPhase === 'stopped') && (
                    <button
                      onClick={handleResetAutoPilot}
                      className="p-1.5 rounded-xl bg-white/5 hover:bg-white/10 text-slate-400 hover:text-white transition-all"
                      title="Reset Auto-Pilot"
                    >
                      <RefreshCw className="w-3.5 h-3.5" />
                    </button>
                  )}
                </div>
              </div>

              {/* ─ Status Message (AnimatePresence swap) ──────────────── */}
              <AnimatePresence mode="wait">
                {autoPilotStatusMsg && (
                  <motion.div
                    key={autoPilotStatusMsg}
                    initial={{ opacity: 0, y: 4 }}
                    animate={{ opacity: 1, y: 0 }}
                    exit={{ opacity: 0, y: -4 }}
                    transition={{ duration: 0.2 }}
                    className="text-xs text-slate-400 font-mono mb-4 relative z-10"
                  >
                    {autoPilotStatusMsg}
                  </motion.div>
                )}
              </AnimatePresence>

              {/* ─ Current Item Card ────────────────────────────────────── */}
              <AnimatePresence>
                {currentQueueItem && (
                  <motion.div
                    key="current-item"
                    initial={{ opacity: 0, scale: 0.98 }}
                    animate={{ opacity: 1, scale: 1 }}
                    exit={{ opacity: 0, scale: 0.98 }}
                    className="bg-black/40 rounded-2xl p-3.5 mb-4 border border-violet-500/20 relative z-10"
                  >
                    <div className="flex items-start gap-3">
                      {/* Animated indicator dot */}
                      <div className="w-1.5 h-1.5 rounded-full bg-violet-400 mt-1.5 animate-pulse flex-shrink-0" />

                      <div className="min-w-0 flex-1">
                        {/* Title */}
                        <div className="text-xs font-semibold text-white truncate">
                          {currentQueueItem.title || 'Untitled'}
                        </div>

                        {/* URL */}
                        <div className="text-[10px] text-slate-500 font-mono truncate mt-0.5">
                          {currentQueueItem.url}
                        </div>

                        {/* Type badge */}
                        <span
                          className={`inline-block mt-1.5 px-2 py-0.5 rounded-full text-[10px] font-bold uppercase ${
                            currentQueueItem.type === 'movie'
                              ? 'bg-indigo-500/20 text-indigo-400 border border-indigo-500/30'
                              : 'bg-violet-500/20 text-violet-400 border border-violet-500/30'
                          }`}
                        >
                          {currentQueueItem.type === 'movie'
                            ? '🎬 Movie'
                            : '📺 Series'}
                        </span>
                      </div>
                    </div>
                  </motion.div>
                )}
              </AnimatePresence>

              {/* ─ Stats Grid (4 columns) ───────────────────────────────── */}
              <AnimatePresence>
                {queueStats && (
                  <motion.div
                    key="stats-grid"
                    initial={{ opacity: 0 }}
                    animate={{ opacity: 1 }}
                    className="grid grid-cols-4 gap-2 mb-4 relative z-10"
                  >
                    {[
                      {
                        label: 'Total',
                        val: queueStats.totalAll,
                        color: 'text-slate-300',
                        bg: 'bg-black/30',
                      },
                      {
                        label: 'Pending',
                        val: queueStats.totalPending,
                        color: 'text-amber-400',
                        bg: 'bg-amber-950/20',
                      },
                      {
                        label: 'Done',
                        val: queueStats.totalCompleted,
                        color: 'text-emerald-400',
                        bg: 'bg-emerald-950/20',
                      },
                      {
                        label: 'Failed',
                        val: queueStats.totalFailed,
                        color: 'text-rose-400',
                        bg: 'bg-rose-950/20',
                      },
                    ].map(({ label, val, color, bg }) => (
                      <div
                        key={label}
                        className={`${bg} rounded-xl p-2.5 text-center`}
                      >
                        <motion.div
                          key={val}
                          initial={{ scale: 1.2 }}
                          animate={{ scale: 1 }}
                          className={`text-xl font-black tabular-nums ${color}`}
                        >
                          {val}
                        </motion.div>
                        <div className="text-[9px] text-slate-600 uppercase tracking-wider mt-0.5">
                          {label}
                        </div>
                      </div>
                    ))}
                  </motion.div>
                )}
              </AnimatePresence>

              {/* ─ Progress Bar ─────────────────────────────────────────── */}
              {queueStats && autoPilotPhase !== 'idle' && (
                <div className="mb-4 relative z-10">
                  <div className="flex justify-between text-[10px] text-slate-600 mb-1.5 font-mono">
                    <span>{autoPilotProcessed} processed</span>
                    <span>{autoPilotProgress}%</span>
                  </div>
                  <div className="h-2 bg-black/50 rounded-full overflow-hidden">
                    <motion.div
                      className={`h-full rounded-full ${
                        autoPilotPhase === 'done'
                          ? 'bg-emerald-500'
                          : 'bg-gradient-to-r from-violet-500 via-indigo-500 to-violet-400'
                      }`}
                      initial={{ width: '0%' }}
                      animate={{ width: `${autoPilotProgress}%` }}
                      transition={{ duration: 0.5, ease: 'easeOut' }}
                    />
                  </div>
                </div>
              )}

              {/* ─ Queue Preview (Collapsible) ──────────────────────────── */}
              {queueStats && queueStats.pendingItems.length > 0 && (
                <div className="mb-3 relative z-10">
                  <button
                    onClick={() => setShowQueuePreview((v) => !v)}
                    className="flex items-center gap-1.5 text-xs text-slate-400 hover:text-slate-200 transition-colors w-full text-left"
                  >
                    {showQueuePreview ? (
                      <ChevronUp className="w-3.5 h-3.5" />
                    ) : (
                      <ChevronDown className="w-3.5 h-3.5" />
                    )}
                    <span>
                      Queue Preview ({queueStats.totalPending} pending)
                    </span>
                  </button>

                  <AnimatePresence>
                    {showQueuePreview && (
                      <motion.div
                        key="queue-preview"
                        initial={{ opacity: 0, height: 0 }}
                        animate={{ opacity: 1, height: 'auto' }}
                        exit={{ opacity: 0, height: 0 }}
                        className="overflow-hidden"
                      >
                        <div className="mt-2 space-y-1 max-h-[200px] overflow-y-auto pr-1">
                          {queueStats.pendingItems.map((item, idx) => (
                            <div
                              key={item.id}
                              className="flex items-center gap-2 text-[11px] font-mono text-slate-500 bg-black/30 rounded-lg px-2.5 py-1.5"
                            >
                              <span className="text-slate-700 w-4 flex-shrink-0">
                                {idx + 1}.
                              </span>
                              <span className="truncate flex-1 text-slate-400">
                                {item.title || item.url}
                              </span>
                              <span
                                className={`flex-shrink-0 px-1.5 py-0.5 rounded-full text-[9px] font-bold uppercase ${
                                  item.type === 'movie'
                                    ? 'bg-indigo-500/20 text-indigo-400'
                                    : 'bg-violet-500/20 text-violet-400'
                                }`}
                              >
                                {item.type === 'movie' ? 'M' : 'S'}
                              </span>
                            </div>
                          ))}
                        </div>
                      </motion.div>
                    )}
                  </AnimatePresence>
                </div>
              )}

              {/* ─ Activity Log (Collapsible) ───────────────────────────── */}
              {autoPilotLog.length > 0 && (
                <div className="relative z-10">
                  <button
                    onClick={() => setShowAutoPilotLog((v) => !v)}
                    className="flex items-center gap-1.5 text-xs text-slate-400 hover:text-slate-200 transition-colors w-full text-left"
                  >
                    {showAutoPilotLog ? (
                      <ChevronUp className="w-3.5 h-3.5" />
                    ) : (
                      <ChevronDown className="w-3.5 h-3.5" />
                    )}
                    <span>Activity Log ({autoPilotLog.length})</span>
                  </button>

                  <AnimatePresence>
                    {showAutoPilotLog && (
                      <motion.div
                        key="activity-log"
                        initial={{ opacity: 0, height: 0 }}
                        animate={{ opacity: 1, height: 'auto' }}
                        exit={{ opacity: 0, height: 0 }}
                        className="overflow-hidden"
                      >
                        <div className="mt-2 bg-black/60 rounded-xl p-3 font-mono text-[11px] max-h-[200px] overflow-y-auto space-y-0.5">
                          {autoPilotLog.map((entry, i) => (
                            <div
                              key={i}
                              className={
                                entry.type === 'success'
                                  ? 'text-emerald-400'
                                  : entry.type === 'error'
                                  ? 'text-rose-400'
                                  : entry.type === 'warn'
                                  ? 'text-amber-400'
                                  : 'text-slate-400'
                              }
                            >
                              &gt; {entry.msg}
                            </div>
                          ))}
                        </div>
                      </motion.div>
                    )}
                  </AnimatePresence>
                </div>
              )}

              {/* ─ Idle Placeholder ─────────────────────────────────────── */}
              {autoPilotPhase === 'idle' && !queueStats && (
                <div className="text-center py-5 relative z-10">
                  <Bot className="w-9 h-9 text-slate-700 mx-auto mb-2" />
                  <p className="text-xs text-slate-600">
                    Firebase queue se pending items process karo
                  </p>
                  <p className="text-[10px] text-slate-700 mt-1">
                    Browser safe to close during Auto-Pilot
                  </p>
                </div>
              )}
            </motion.div>
          </>
        )}

        {/* ════════════════════════════════════════════════════════════════ */}
        {/* TAB HEADER (non-home tabs)                                      */}
        {/* ════════════════════════════════════════════════════════════════ */}
        {activeTab !== 'home' && (
          <div className="flex items-center gap-2.5 mb-4">
            <div
              className={`w-2 h-2 rounded-full flex-shrink-0 ${
                activeTab === 'processing'
                  ? 'bg-indigo-400 animate-pulse'
                  : activeTab === 'completed'
                  ? 'bg-emerald-400'
                  : activeTab === 'failed'
                  ? 'bg-rose-400'
                  : 'bg-slate-500'
              }`}
            />
            <h2 className="text-sm font-bold text-slate-300 uppercase tracking-widest">
              {activeTab === 'processing' && '⏳ Processing Tasks'}
              {activeTab === 'completed' && '✅ Completed Tasks'}
              {activeTab === 'failed' && '❌ Failed Tasks'}
              {activeTab === 'history' && '📋 History'}
              <span className="ml-2 text-slate-600 font-mono normal-case font-normal">
                ({filteredTasks.length})
              </span>
            </h2>
          </div>
        )}

        {/* ════════════════════════════════════════════════════════════════ */}
        {/* TASK LIST                                                        */}
        {/* Shown in all tabs (home tab shows tasks below auto-pilot)       */}
        {/* ════════════════════════════════════════════════════════════════ */}
        {(activeTab !== 'home' || tasks.length > 0) && (
          <div className="space-y-3">
            <AnimatePresence initial={false}>
              {filteredTasks.map((task) => {
                const stats = getEffectiveStats(task);
                const trueStatus = getTrueTaskStatus(task, stats);
                const isExpanded = expandedTask === task.id;

                return (
                  <motion.div
                    key={task.id}
                    layout
                    initial={{ opacity: 0, y: 12 }}
                    animate={{ opacity: 1, y: 0 }}
                    exit={{ opacity: 0, scale: 0.97 }}
                    transition={{ duration: 0.2 }}
                    className={`rounded-2xl border overflow-hidden transition-colors ${
                      trueStatus === 'processing'
                        ? 'border-indigo-500/30 bg-indigo-950/20'
                        : trueStatus === 'completed'
                        ? 'border-emerald-500/20 bg-emerald-950/10'
                        : 'border-rose-500/20 bg-rose-950/10'
                    }`}
                  >
                    {/* ── Collapsed Card Header ──────────────────────── */}
                    <div
                      className="flex items-start gap-3 p-4 cursor-pointer hover:bg-white/[0.03] transition-colors select-none"
                      onClick={() =>
                        setExpandedTask(isExpanded ? null : task.id)
                      }
                    >
                      {/* Poster Thumbnail */}
                      <div className="w-11 h-11 rounded-xl overflow-hidden bg-slate-800 flex items-center justify-center flex-shrink-0">
                        {task.preview?.posterUrl ? (
                          <>
                            <img
                              src={task.preview.posterUrl}
                              alt="Movie"
                              className="w-full h-full object-cover"
                              onError={(e) => {
                                // poster load fail → hide image, show Film icon
                                (e.target as HTMLImageElement).style.display =
                                  'none';
                                (
                                  e.target as HTMLImageElement
                                ).nextElementSibling?.classList.remove(
                                  'hidden'
                                );
                              }}
                            />
                            <Film
                              className={`w-5 h-5 text-indigo-400 ${
                                task.preview?.posterUrl ? 'hidden' : ''
                              }`}
                            />
                          </>
                        ) : (
                          <Film className="w-5 h-5 text-indigo-400" />
                        )}
                      </div>

                      {/* Task Info */}
                      <div className="flex-1 min-w-0">

                        {/* Title */}
                        <div className="text-sm font-bold text-white truncate">
                          {task.preview?.title || 'Processing...'}
                        </div>

                        {/* URL (mono, truncated) */}
                        <div className="text-[10px] text-slate-600 font-mono truncate mt-0.5">
                          {task.url}
                        </div>

                        {/* Badges Row */}
                        <div className="flex flex-wrap items-center gap-1.5 mt-1.5">

                          {/* Status Badge */}
                          <span
                            className={`px-2 py-0.5 rounded-full text-[10px] font-bold uppercase ${
                              trueStatus === 'processing'
                                ? 'bg-indigo-500/20 text-indigo-400 border border-indigo-500/30'
                                : trueStatus === 'completed'
                                ? 'bg-emerald-500/20 text-emerald-400 border border-emerald-500/30'
                                : 'bg-rose-500/20 text-rose-400 border border-rose-500/30'
                            }`}
                          >
                            {trueStatus === 'processing'
                              ? '⚡ LIVE'
                              : trueStatus === 'completed'
                              ? '✅ Done'
                              : '❌ Failed'}
                          </span>

                          {/* Extracted By badge */}
                          {task.extractedBy && (
                            <span className="px-2 py-0.5 rounded-full text-[10px] bg-slate-800/80 text-slate-500 border border-slate-700/50">
                              {task.extractedBy === 'Browser/Live'
                                ? '🌐 Live'
                                : '🤖 Auto-Pilot'}
                            </span>
                          )}

                          {/* Link stats badges */}
                          {stats.total > 0 && (
                            <span className="text-[10px] text-slate-600 font-mono">
                              {stats.total} links
                              {stats.done > 0 && (
                                <span className="text-emerald-600/80">
                                  {' '}
                                  ✓{stats.done}
                                </span>
                              )}
                              {stats.failed > 0 && (
                                <span className="text-rose-600/80">
                                  {' '}
                                  ✗{stats.failed}
                                </span>
                              )}
                              {stats.pending > 0 && (
                                <span className="text-slate-600">
                                  {' '}
                                  ⏳{stats.pending}
                                </span>
                              )}
                            </span>
                          )}

                          {/* Created at */}
                          <span className="text-[10px] text-slate-700 font-mono hidden sm:inline">
                            {formatTime12h(task.createdAt)}
                          </span>
                        </div>
                      </div>

                      {/* Action Buttons + Chevron */}
                      <div
                        className="flex items-center gap-1 flex-shrink-0"
                        onClick={(e) => e.stopPropagation()}
                      >
                        {/* Retry Button */}
                        <button
                          onClick={(e) => handleRetryTask(task, e)}
                          disabled={retryingTaskId === task.id}
                          className="p-1.5 rounded-lg text-slate-600 hover:text-indigo-400 hover:bg-indigo-500/10 transition-all"
                          title="Retry task"
                        >
                          {retryingTaskId === task.id ? (
                            <Loader2 className="w-3.5 h-3.5 animate-spin" />
                          ) : (
                            <RefreshCw className="w-3.5 h-3.5" />
                          )}
                        </button>

                        {/* Delete Button */}
                        <button
                          onClick={(e) => handleDeleteTask(task.id, e)}
                          disabled={deletingTaskId === task.id}
                          className="p-1.5 rounded-lg text-slate-600 hover:text-rose-400 hover:bg-rose-500/10 transition-all"
                          title="Delete task"
                        >
                          {deletingTaskId === task.id ? (
                            <Loader2 className="w-3.5 h-3.5 animate-spin" />
                          ) : (
                            <Trash2 className="w-3.5 h-3.5" />
                          )}
                        </button>

                        {/* Expand chevron */}
                        <div
                          className="p-1.5 cursor-pointer"
                          onClick={() =>
                            setExpandedTask(isExpanded ? null : task.id)
                          }
                        >
                          {isExpanded ? (
                            <ChevronUp className="w-4 h-4 text-slate-600" />
                          ) : (
                            <ChevronDown className="w-4 h-4 text-slate-600" />
                          )}
                        </div>
                      </div>
                    </div>

                    {/* ── Expanded Content ────────────────────────────── */}
                    <AnimatePresence>
                      {isExpanded && (
                        <motion.div
                          key={`expanded-${task.id}`}
                          initial={{ opacity: 0, height: 0 }}
                          animate={{ opacity: 1, height: 'auto' }}
                          exit={{ opacity: 0, height: 0 }}
                          transition={{ duration: 0.25 }}
                          className="overflow-hidden"
                        >
                          <div className="px-4 pb-5 pt-1 relative">

                            {/* Poster blur background */}
                            {task.preview?.posterUrl && (
                              <div className="absolute inset-0 overflow-hidden pointer-events-none">
                                <img
                                  src={task.preview.posterUrl}
                                  alt=""
                                  className="w-full h-full object-cover blur-3xl opacity-[0.07] scale-110"
                                />
                              </div>
                            )}

                            <div className="relative z-10">

                              {/* Divider */}
                              <div className="h-px bg-white/5 mb-4" />

                              {/* ── Stats Grid (4 columns) ─────────────── */}
                              <div className="grid grid-cols-4 gap-2 mb-4">
                                {[
                                  {
                                    label: 'Total',
                                    val: stats.total,
                                    color: 'text-slate-300',
                                  },
                                  {
                                    label: 'Done',
                                    val: stats.done,
                                    color: 'text-emerald-400',
                                  },
                                  {
                                    label: 'Failed',
                                    val: stats.failed,
                                    color: 'text-rose-400',
                                  },
                                  {
                                    label: 'Pending',
                                    val: stats.pending,
                                    color: 'text-amber-400',
                                  },
                                ].map(({ label, val, color }) => (
                                  <div
                                    key={label}
                                    className="bg-black/40 rounded-xl p-2.5 text-center"
                                  >
                                    <div
                                      className={`text-xl font-black tabular-nums ${color}`}
                                    >
                                      {val}
                                    </div>
                                    <div className="text-[9px] text-slate-600 uppercase tracking-wider mt-0.5">
                                      {label}
                                    </div>
                                  </div>
                                ))}
                              </div>

                              {/* ── Metadata Grid (3 columns) ──────────── */}
                              {task.metadata &&
                                (task.metadata.quality ||
                                  task.metadata.languages ||
                                  task.metadata.audioLabel) && (
                                  <div className="grid grid-cols-3 gap-2 mb-4">
                                    {/* Quality */}
                                    {task.metadata.quality && (
                                      <div className="bg-black/30 rounded-xl p-2.5 flex items-center gap-2">
                                        <Sparkles className="w-3.5 h-3.5 text-indigo-400 flex-shrink-0" />
                                        <div className="min-w-0">
                                          <div className="text-[9px] text-slate-600 uppercase tracking-wider">
                                            Quality
                                          </div>
                                          <div className="text-[11px] text-white font-semibold truncate">
                                            {task.metadata.quality}
                                          </div>
                                        </div>
                                      </div>
                                    )}

                                    {/* Languages */}
                                    {task.metadata.languages && (
                                      <div className="bg-black/30 rounded-xl p-2.5 flex items-center gap-2">
                                        <Globe className="w-3.5 h-3.5 text-indigo-400 flex-shrink-0" />
                                        <div className="min-w-0">
                                          <div className="text-[9px] text-slate-600 uppercase tracking-wider">
                                            Languages
                                          </div>
                                          <div className="text-[11px] text-white font-semibold truncate">
                                            {task.metadata.languages}
                                          </div>
                                        </div>
                                      </div>
                                    )}

                                    {/* Audio */}
                                    {task.metadata.audioLabel && (
                                      <div className="bg-black/30 rounded-xl p-2.5 flex items-center gap-2">
                                        <Volume2 className="w-3.5 h-3.5 text-indigo-400 flex-shrink-0" />
                                        <div className="min-w-0">
                                          <div className="text-[9px] text-slate-600 uppercase tracking-wider">
                                            Audio
                                          </div>
                                          <div className="text-[11px] text-white font-semibold truncate">
                                            {task.metadata.audioLabel}
                                          </div>
                                        </div>
                                      </div>
                                    )}
                                  </div>
                                )}

                              {/* ── Error message (agar failed) ────────── */}
                              {task.error && trueStatus === 'failed' && (
                                <div className="mb-4 bg-rose-950/30 border border-rose-500/20 rounded-xl px-3 py-2.5 flex items-start gap-2">
                                  <AlertCircle className="w-3.5 h-3.5 text-rose-400 flex-shrink-0 mt-0.5" />
                                  <p className="text-[11px] text-rose-400 font-mono">
                                    {task.error}
                                  </p>
                                </div>
                              )}

                              {/* ── Links (LinkCards) ──────────────────── */}
                              {task.links && task.links.length > 0 ? (
                                <>
                                  {/* PROBLEM 4: Global "Retry Failed" button */}
                                  {(() => {
                                    const failedLinks = task.links.filter((l: any) => {
                                      const s = (l.status || '').toLowerCase();
                                      return s === 'error' || s === 'failed';
                                    });
                                    const hasFailedLinks = failedLinks.length > 0;

                                    return hasFailedLinks && trueStatus !== 'processing' ? (
                                      <button
                                        onClick={() => handleRetryFailed(task)}
                                        className="w-full mb-4 py-2.5 px-4 rounded-xl bg-rose-500/10 border border-rose-500/30 text-rose-400 text-xs font-bold flex items-center justify-center gap-2 hover:bg-rose-500/20 transition-all active:scale-[0.98]"
                                      >
                                        <RotateCcw className="w-3.5 h-3.5" />
                                        RETRY {failedLinks.length} FAILED LINK{failedLinks.length > 1 ? 'S' : ''} ONLY
                                      </button>
                                    ) : null;
                                  })()}

                                  {task.links.map(
                                    (link: any, idx: number) => {
                                      const effective =
                                        getEffectiveLinkData(
                                          task,
                                          idx,
                                          link
                                        );
                                      return (
                                        <LinkCard
                                          key={idx}
                                          id={idx}
                                          linkId={link.id ?? idx}
                                          name={
                                            link.name ||
                                            `Link ${idx + 1}`
                                          }
                                          logs={effective.logs}
                                          finalLink={effective.finalLink}
                                          status={
                                            effective.status as
                                              | 'processing'
                                              | 'done'
                                              | 'error'
                                          }
                                          onRepeat={(linkId) =>
                                            handleRepeatLink(task.id, linkId, task.links)
                                          }
                                          isRepeating={repeatingLinkId === (link.id ?? idx)}
                                        />
                                      );
                                    }
                                  )}
                                </>
                              ) : (
                                /* Empty links — scraping in progress */
                                <div className="text-center py-8">
                                  <Loader2 className="w-6 h-6 text-slate-600 animate-spin mx-auto mb-2.5" />
                                  <p className="text-xs text-slate-600 font-medium">
                                    Scraping in progress...
                                  </p>
                                  <p className="text-[10px] text-slate-700 mt-1">
                                    Server is processing — safe to close
                                    browser.
                                  </p>
                                </div>
                              )}
                            </div>
                          </div>
                        </motion.div>
                      )}
                    </AnimatePresence>
                  </motion.div>
                );
              })}
            </AnimatePresence>

            {/* Empty state for non-home tabs */}
            {filteredTasks.length === 0 && activeTab !== 'home' && (
              <motion.div
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                className="text-center py-20"
              >
                <div className="text-5xl mb-4">
                  {activeTab === 'processing'
                    ? '⏳'
                    : activeTab === 'completed'
                    ? '✅'
                    : activeTab === 'failed'
                    ? '❌'
                    : '📋'}
                </div>
                <p className="text-slate-600 text-sm">
                  No {activeTab} tasks yet
                </p>
              </motion.div>
            )}
          </div>
        )}

        {/* Home tab empty state */}
        {activeTab === 'home' && tasks.length === 0 && (
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1, transition: { delay: 0.3 } }}
            className="text-center py-10"
          >
            <Zap className="w-8 h-8 text-slate-700 mx-auto mb-3" />
            <p className="text-slate-700 text-sm">
              Paste a movie URL above and hit START ENGINE
            </p>
          </motion.div>
        )}
      </div>

      {/* ════════════════════════════════════════════════════════════════════ */}
      {/* BOTTOM NAVIGATION BAR (Fixed)                                       */}
      {/* fixed bottom-0 · bg-black/80 backdrop-blur · safe-area-inset       */}
      {/* ════════════════════════════════════════════════════════════════════ */}
      <nav className="fixed bottom-0 left-0 right-0 z-50 bg-black/80 backdrop-blur-xl border-t border-white/10 safe-area-inset-bottom">
        <div className="max-w-2xl mx-auto flex">
          {(
            [
              {
                tab: 'home' as TabType,
                icon: Home,
                label: 'Home',
                count: 0,
              },
              {
                tab: 'processing' as TabType,
                icon: Clock,
                label: 'Processing',
                count: processingCount,
              },
              {
                tab: 'completed' as TabType,
                icon: CheckCircle2,
                label: 'Completed',
                count: completedCount,
              },
              {
                tab: 'failed' as TabType,
                icon: XCircle,
                label: 'Failed',
                count: failedCount,
              },
              {
                tab: 'history' as TabType,
                icon: History,
                label: 'History',
                count: historyCount,
              },
            ] as const
          ).map(({ tab, icon: Icon, label, count }) => {
            const isActive = activeTab === tab;

            return (
              <button
                key={tab}
                onClick={() => setActiveTab(tab)}
                className={`flex-1 flex flex-col items-center justify-center py-3 gap-1 relative transition-colors ${
                  isActive
                    ? 'text-indigo-400'
                    : 'text-slate-600 hover:text-slate-400 active:text-slate-300'
                }`}
              >
                {/* Top active indicator line */}
                {isActive && (
                  <motion.div
                    layoutId="nav-active-indicator"
                    className="absolute top-0 left-1/2 -translate-x-1/2 w-8 h-0.5 bg-indigo-400 rounded-full"
                    transition={{ type: 'spring', stiffness: 380, damping: 30 }}
                  />
                )}

                {/* Icon + Count Badge */}
                <div className="relative">
                  <Icon className="w-5 h-5" />

                  {/* Count badge */}
                  {count > 0 && (
                    <motion.span
                      key={count}
                      initial={{ scale: 1.3 }}
                      animate={{ scale: 1 }}
                      className={`absolute -top-1.5 -right-2.5 text-[9px] font-bold px-1 rounded-full min-w-[16px] h-4 flex items-center justify-center ${
                        tab === 'processing'
                          ? 'bg-indigo-600 text-white'
                          : tab === 'completed'
                          ? 'bg-emerald-600 text-white'
                          : tab === 'failed'
                          ? 'bg-rose-600 text-white'
                          : 'bg-slate-600 text-white'
                      }`}
                    >
                      {count > 99 ? '99+' : count}
                    </motion.span>
                  )}
                </div>

                {/* Label */}
                <span className="text-[10px] uppercase tracking-wide">
                  {label}
                </span>
              </button>
            );
          })}
        </div>
      </nav>
    </div>
  );
}
