'use client';

import { useState, useEffect, useRef, useCallback } from 'react';
import {
  Brain, ArrowLeft, Activity, Shield, Wifi, WifiOff, Server, Database,
  Clock, AlertTriangle, CheckCircle2, XCircle, RefreshCw, Send, Copy,
  ChevronDown, ChevronUp, Loader2, Zap, Radio, HardDrive, Bot, Sparkles,
  BarChart3, Heart, Cpu, Settings, Key, Save, Eye, EyeOff, Trash2,
  MessageSquare, ChevronRight, Info, Gauge, PenLine,
} from 'lucide-react';
import { motion, AnimatePresence } from 'motion/react';

// ─── Types ───────────────────────────────────────────────────────────────────

interface DiagData {
  timestamp: string;
  overall: 'healthy' | 'warning' | 'critical';
  checks: Record<string, { status: string; message: string; details?: any }>;
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

interface ChatMsg {
  role: 'user' | 'assistant';
  content: string;
  ts: string;
  model?: string;
  tokens?: number;
}

interface GeminiModel {
  id: string;
  name: string;
  description: string;
  tier: string;
  contextWindow: string;
}

interface AiSettings {
  apiKey?: string;
  hasApiKey?: boolean;
  model: string;
  customInstructions: string;
}

type ViewMode = 'dashboard' | 'chat' | 'settings';

// ─── Helpers ─────────────────────────────────────────────────────────────────

const sColor = (s: string) =>
  s === 'ok' ? 'text-emerald-400' : s === 'warning' ? 'text-amber-400' : s === 'critical' ? 'text-rose-400' : 'text-slate-500';

const sBg = (s: string) =>
  s === 'ok' ? 'bg-emerald-500/10 border-emerald-500/20' : s === 'warning' ? 'bg-amber-500/10 border-amber-500/20' : s === 'critical' ? 'bg-rose-500/10 border-rose-500/20' : 'bg-slate-800/50 border-slate-700/30';

const sIcon = (s: string) =>
  s === 'ok' ? <CheckCircle2 className="w-3.5 h-3.5 text-emerald-400" /> : s === 'warning' ? <AlertTriangle className="w-3.5 h-3.5 text-amber-400" /> : s === 'critical' ? <XCircle className="w-3.5 h-3.5 text-rose-400" /> : <Clock className="w-3.5 h-3.5 text-slate-500" />;

const cIcon: Record<string, any> = {
  firebase: <Database className="w-3.5 h-3.5" />,
  engine: <Cpu className="w-3.5 h-3.5" />,
  vpsTimer: <Server className="w-3.5 h-3.5" />,
  vpsHubcloud: <HardDrive className="w-3.5 h-3.5" />,
  queueHealth: <Radio className="w-3.5 h-3.5" />,
  taskHealth: <BarChart3 className="w-3.5 h-3.5" />,
  recentErrors: <AlertTriangle className="w-3.5 h-3.5" />,
};

const cLabel: Record<string, string> = {
  firebase: 'Firebase', engine: 'Cron Engine', vpsTimer: 'VPS Timer',
  vpsHubcloud: 'VPS HubCloud', queueHealth: 'Queue', taskHealth: 'Tasks', recentErrors: 'Errors',
};

const tierBadge: Record<string, string> = {
  recommended: 'bg-emerald-500/20 text-emerald-400 border-emerald-500/30',
  fast: 'bg-sky-500/20 text-sky-400 border-sky-500/30',
  powerful: 'bg-violet-500/20 text-violet-400 border-violet-500/30',
  economy: 'bg-slate-500/20 text-slate-400 border-slate-500/30',
};

const QUICK_ACTIONS = [
  { label: '🔍 System Status', prompt: 'System ka overall health kya hai? Koi problem hai kya?' },
  { label: '🔗 Links Fail', prompt: 'Links kyun fail ho rahe hain? Error patterns analyze karo.' },
  { label: '⚙️ Engine Band', prompt: 'Engine OFFLINE kyun hai? Fix steps batao.' },
  { label: '📋 Queue Stuck', prompt: 'Queue items stuck hain processing mein. Root cause kya hai?' },
  { label: '🚀 Performance', prompt: 'System ki performance aur speed kaise improve karein?' },
  { label: '🛠️ VPS Check', prompt: 'VPS Timer aur HubCloud API ka status check karo.' },
  { label: '📊 Success Rate', prompt: 'Link success rate low kyun hai? Kaun sa solver fail ho raha hai?' },
  { label: '🔄 Full Audit', prompt: 'Puri website ka complete audit karo — chhoti se chhoti problem bhi batao.' },
];

// ═══════════════════════════════════════════════════════════════════════════════
// MAIN COMPONENT
// ═══════════════════════════════════════════════════════════════════════════════

export default function AiControlCenter({ onBack }: { onBack: () => void }) {
  // ─── State ──────────────────────────────────────────────────────────────
  const [view, setView] = useState<ViewMode>('dashboard');
  const [diag, setDiag] = useState<DiagData | null>(null);
  const [loadingDiag, setLoadingDiag] = useState(true);

  const [msgs, setMsgs] = useState<ChatMsg[]>([]);
  const [input, setInput] = useState('');
  const [sending, setSending] = useState(false);
  const [chatErr, setChatErr] = useState<string | null>(null);
  const [totalTokens, setTotalTokens] = useState(0);

  const [settings, setSettings] = useState<AiSettings>({ model: 'gemini-2.5-flash', customInstructions: '' });
  const [models, setModels] = useState<GeminiModel[]>([]);
  const [isConfigured, setIsConfigured] = useState(false);
  const [newApiKey, setNewApiKey] = useState('');
  const [showKey, setShowKey] = useState(false);
  const [savingSettings, setSavingSettings] = useState(false);
  const [settingsMsg, setSettingsMsg] = useState<string | null>(null);
  const [customInst, setCustomInst] = useState('');

  const [expandedCheck, setExpandedCheck] = useState<string | null>(null);
  const [copied, setCopied] = useState(false);

  const chatEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLTextAreaElement>(null);

  // ─── Auto-scroll ───────────────────────────────────────────────────────
  useEffect(() => { chatEndRef.current?.scrollIntoView({ behavior: 'smooth' }); }, [msgs]);

  // ─── Fetch diagnostics ─────────────────────────────────────────────────
  const fetchDiag = useCallback(async () => {
    setLoadingDiag(true);
    try {
      const r = await fetch('/api/ai/diagnose');
      if (r.ok) setDiag(await r.json());
    } catch {}
    setLoadingDiag(false);
  }, []);

  // ─── Fetch settings ────────────────────────────────────────────────────
  const fetchSettings = useCallback(async () => {
    try {
      const r = await fetch('/api/ai/settings');
      if (!r.ok) return;
      const data = await r.json();
      setSettings(data.settings || { model: 'gemini-2.5-flash', customInstructions: '' });
      setModels(data.models || []);
      setIsConfigured(data.isConfigured || false);
      setCustomInst(data.settings?.customInstructions || '');
    } catch {}
  }, []);

  // ─── Init ──────────────────────────────────────────────────────────────
  useEffect(() => {
    fetchDiag();
    fetchSettings();
    const interval = setInterval(fetchDiag, 60000);
    return () => clearInterval(interval);
  }, [fetchDiag, fetchSettings]);

  // ─── Save settings ─────────────────────────────────────────────────────
  const saveSettings = async () => {
    setSavingSettings(true);
    setSettingsMsg(null);
    try {
      const payload: any = { model: settings.model, customInstructions: customInst };
      if (newApiKey && !newApiKey.startsWith('••••')) {
        payload.apiKey = newApiKey;
      }
      const r = await fetch('/api/ai/settings', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      if (r.ok) {
        setSettingsMsg('✅ Settings saved!');
        setNewApiKey('');
        await fetchSettings();
      } else {
        const d = await r.json().catch(() => ({}));
        setSettingsMsg(`❌ ${d.error || 'Save failed'}`);
      }
    } catch (e: any) {
      setSettingsMsg(`❌ ${e.message}`);
    }
    setSavingSettings(false);
  };

  // ─── Send message ──────────────────────────────────────────────────────
  const sendMsg = async (msg?: string) => {
    const text = (msg || input).trim();
    if (!text || sending) return;
    setInput('');
    setChatErr(null);

    if (!isConfigured) {
      setChatErr('Pehle Settings mein API key add karo!');
      return;
    }

    const userMsg: ChatMsg = { role: 'user', content: text, ts: new Date().toISOString() };
    setMsgs(prev => [...prev, userMsg]);
    setSending(true);

    // Switch to chat view if not already
    if (view !== 'chat') setView('chat');

    try {
      const history = msgs.slice(-10).map(m => ({ role: m.role, content: m.content }));
      const r = await fetch('/api/ai/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ message: text, history, diagnostics: diag }),
      });

      const data = await r.json();

      if (!r.ok) {
        if (data.needsSetup) {
          setChatErr(data.error + ' → Settings ⚙️ mein jaao');
        } else {
          setChatErr(data.error);
        }
        setMsgs(prev => prev.slice(0, -1));
        return;
      }

      const aiMsg: ChatMsg = {
        role: 'assistant',
        content: data.response,
        ts: new Date().toISOString(),
        model: data.model,
        tokens: data.usage?.totalTokens || 0,
      };
      setMsgs(prev => [...prev, aiMsg]);
      setTotalTokens(prev => prev + (data.usage?.totalTokens || 0));
    } catch (e: any) {
      setChatErr(e.message);
      setMsgs(prev => prev.slice(0, -1));
    } finally {
      setSending(false);
      inputRef.current?.focus();
    }
  };

  // ─── Copy ──────────────────────────────────────────────────────────────
  const copyText = (t: string) => {
    navigator.clipboard.writeText(t);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  // ─── Overall status ────────────────────────────────────────────────────
  const ovr = diag?.overall || 'unknown';
  const ovrColor = ovr === 'healthy' ? 'text-emerald-400' : ovr === 'warning' ? 'text-amber-400' : ovr === 'critical' ? 'text-rose-400' : 'text-slate-400';
  const ovrBgGrad = ovr === 'healthy' ? 'from-emerald-600/15' : ovr === 'warning' ? 'from-amber-600/15' : ovr === 'critical' ? 'from-rose-600/15' : 'from-slate-600/15';

  const currentModel = models.find(m => m.id === settings.model);

  // ═══════════════════════════════════════════════════════════════════════════
  // RENDER
  // ═══════════════════════════════════════════════════════════════════════════

  return (
    <div className="min-h-screen min-h-dvh bg-black text-white">
      <div className={`fixed inset-0 bg-gradient-to-b ${ovrBgGrad} via-transparent to-transparent pointer-events-none z-0`} />
      <div className="fixed inset-0 bg-[radial-gradient(ellipse_at_top_right,_var(--tw-gradient-stops))] from-violet-950/10 via-transparent to-transparent pointer-events-none z-0" />

      <div className="relative z-10 max-w-2xl mx-auto px-4 pt-4 pb-8">

        {/* ═══ HEADER ═══════════════════════════════════════════════════════ */}
        <div className="flex items-center gap-2.5 mb-4">
          <button onClick={onBack} className="w-8 h-8 rounded-lg bg-white/5 border border-white/10 flex items-center justify-center hover:bg-white/10 active:scale-95 transition-all">
            <ArrowLeft className="w-4 h-4 text-slate-300" />
          </button>

          <div className="w-8 h-8 rounded-lg bg-gradient-to-br from-violet-600 to-indigo-600 flex items-center justify-center shadow-lg shadow-violet-600/25">
            <Brain className="w-4 h-4 text-white" />
          </div>
          <div className="flex-1">
            <h1 className="text-base font-bold tracking-tight">AI Control Center</h1>
            <p className="text-[9px] text-slate-600 font-mono tracking-wider">
              {currentModel ? currentModel.name : 'Not configured'} • {isConfigured ? '🟢 Active' : '🔴 Setup needed'}
            </p>
          </div>

          <button onClick={fetchDiag} disabled={loadingDiag} className="w-8 h-8 rounded-lg bg-white/5 border border-white/10 flex items-center justify-center hover:bg-white/10 active:scale-95 transition-all">
            <RefreshCw className={`w-3.5 h-3.5 text-slate-400 ${loadingDiag ? 'animate-spin' : ''}`} />
          </button>
        </div>

        {/* ═══ TAB SWITCHER ═══════════════════════════════════════════════ */}
        <div className="flex gap-1 mb-4 bg-white/[0.03] rounded-xl p-1 border border-white/5">
          {([
            { key: 'dashboard', icon: Activity, label: 'Dashboard' },
            { key: 'chat', icon: MessageSquare, label: 'AI Chat' },
            { key: 'settings', icon: Settings, label: 'Settings' },
          ] as const).map(({ key, icon: Icon, label }) => (
            <button
              key={key}
              onClick={() => setView(key)}
              className={`flex-1 flex items-center justify-center gap-1.5 py-2 rounded-lg text-xs font-medium transition-all ${
                view === key
                  ? 'bg-violet-600/20 text-violet-300 border border-violet-500/30'
                  : 'text-slate-500 hover:text-slate-300'
              }`}
            >
              <Icon className="w-3.5 h-3.5" />
              {label}
            </button>
          ))}
        </div>

        {/* ═══════════════════════════════════════════════════════════════════ */}
        {/* DASHBOARD VIEW                                                     */}
        {/* ═══════════════════════════════════════════════════════════════════ */}
        {view === 'dashboard' && (
          <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} className="space-y-4">

            {/* Overall Status */}
            <div className={`rounded-2xl border p-4 ${
              ovr === 'healthy' ? 'bg-emerald-950/30 border-emerald-500/20' :
              ovr === 'warning' ? 'bg-amber-950/30 border-amber-500/20' :
              ovr === 'critical' ? 'bg-rose-950/30 border-rose-500/20' : 'bg-slate-900/50 border-slate-700/30'
            }`}>
              <div className="flex items-center gap-3">
                {loadingDiag ? <Loader2 className="w-5 h-5 text-slate-400 animate-spin" /> :
                  ovr === 'healthy' ? <Heart className="w-5 h-5 text-emerald-400" /> :
                  ovr === 'warning' ? <AlertTriangle className="w-5 h-5 text-amber-400" /> :
                  <XCircle className="w-5 h-5 text-rose-400" />}
                <div className="flex-1">
                  <p className={`text-sm font-bold ${ovrColor}`}>
                    {loadingDiag ? 'Scanning...' : ovr === 'healthy' ? 'All Systems OK' : ovr === 'warning' ? 'Issues Found' : 'Critical Problems'}
                  </p>
                  {diag && <p className="text-[9px] text-slate-600 font-mono">{new Date(diag.timestamp).toLocaleTimeString()}</p>}
                </div>
                {diag && <div className="flex gap-1">
                  {Object.values(diag.checks).map((c, i) => (
                    <div key={i} className={`w-1.5 h-1.5 rounded-full ${c.status === 'ok' ? 'bg-emerald-400' : c.status === 'warning' ? 'bg-amber-400' : c.status === 'critical' ? 'bg-rose-400' : 'bg-slate-600'}`} />
                  ))}
                </div>}
              </div>
              {diag && diag.overall !== 'healthy' && diag.summary.length > 0 && (
                <div className="mt-3 pt-3 border-t border-white/5 space-y-1">
                  {diag.summary.map((s, i) => <p key={i} className="text-[11px] text-slate-300">{s}</p>)}
                </div>
              )}
            </div>

            {/* Health Checks Grid */}
            {diag && (
              <div className="grid grid-cols-2 gap-1.5">
                {Object.entries(diag.checks).map(([key, check]) => (
                  <button key={key} onClick={() => setExpandedCheck(expandedCheck === key ? null : key)}
                    className={`text-left rounded-xl border p-2.5 transition-all ${sBg(check.status)} ${expandedCheck === key ? 'col-span-2' : ''}`}>
                    <div className="flex items-center gap-1.5">
                      <span className={sColor(check.status)}>{cIcon[key] || <Activity className="w-3.5 h-3.5" />}</span>
                      <span className="text-[11px] font-semibold text-white flex-1">{cLabel[key] || key}</span>
                      {sIcon(check.status)}
                    </div>
                    <p className={`text-[9px] mt-1 ${sColor(check.status)} opacity-75 line-clamp-2`}>{check.message}</p>
                    <AnimatePresence>
                      {expandedCheck === key && check.details && (
                        <motion.div initial={{ height: 0, opacity: 0 }} animate={{ height: 'auto', opacity: 1 }} exit={{ height: 0, opacity: 0 }} className="overflow-hidden">
                          <pre className="mt-2 pt-2 border-t border-white/5 text-[8px] text-slate-500 font-mono whitespace-pre-wrap break-all max-h-32 overflow-y-auto">
                            {JSON.stringify(check.details, null, 2)}
                          </pre>
                        </motion.div>
                      )}
                    </AnimatePresence>
                  </button>
                ))}
              </div>
            )}

            {/* Link Success Rate */}
            {diag?.rawData?.taskStats?.totalLinks > 0 && (() => {
              const ts = diag.rawData.taskStats;
              const rate = ts.linkSuccessRate;
              return (
                <div className="rounded-xl bg-white/[0.03] border border-white/5 p-3">
                  <div className="flex items-center gap-2 mb-2">
                    <Gauge className="w-3.5 h-3.5 text-violet-400" />
                    <span className="text-[11px] font-semibold">Link Success Rate</span>
                    <span className={`ml-auto text-sm font-bold ${rate >= 75 ? 'text-emerald-400' : rate >= 50 ? 'text-amber-400' : 'text-rose-400'}`}>{rate}%</span>
                  </div>
                  <div className="w-full h-1.5 bg-slate-800 rounded-full overflow-hidden mb-2">
                    <motion.div initial={{ width: 0 }} animate={{ width: `${rate}%` }} transition={{ duration: 0.8 }}
                      className={`h-full rounded-full ${rate >= 75 ? 'bg-emerald-500' : rate >= 50 ? 'bg-amber-500' : 'bg-rose-500'}`} />
                  </div>
                  <div className="grid grid-cols-4 gap-1 text-center">
                    {[
                      { l: 'Done', v: ts.doneLinks, c: 'text-emerald-400' },
                      { l: 'Error', v: ts.errorLinks, c: 'text-rose-400' },
                      { l: 'Pending', v: ts.pendingLinks, c: 'text-amber-400' },
                      { l: 'Total', v: ts.totalLinks, c: 'text-slate-300' },
                    ].map(i => (
                      <div key={i.l}>
                        <p className={`text-base font-bold ${i.c}`}>{i.v}</p>
                        <p className="text-[8px] text-slate-600 uppercase">{i.l}</p>
                      </div>
                    ))}
                  </div>
                </div>
              );
            })()}

            {/* Error Patterns */}
            {diag?.rawData?.errorPatterns && Object.keys(diag.rawData.errorPatterns).length > 0 && (
              <div className="rounded-xl bg-rose-950/20 border border-rose-500/10 p-3">
                <div className="flex items-center gap-2 mb-2">
                  <AlertTriangle className="w-3.5 h-3.5 text-rose-400" />
                  <span className="text-[11px] font-semibold text-rose-300">Error Patterns</span>
                </div>
                <div className="space-y-1.5">
                  {Object.entries(diag.rawData.errorPatterns).sort((a, b) => b[1] - a[1]).slice(0, 5).map(([p, c], i) => (
                    <div key={i} className="flex items-start gap-2">
                      <span className="text-[9px] font-mono text-rose-400 font-bold min-w-[20px]">{c}x</span>
                      <p className="text-[9px] text-slate-400 font-mono break-all">{p}</p>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* Quick Actions → Chat */}
            <div>
              <p className="text-[9px] text-slate-600 font-mono uppercase tracking-widest mb-2">⚡ Quick Ask — AI se pucho</p>
              <div className="flex flex-wrap gap-1.5">
                {QUICK_ACTIONS.map((a, i) => (
                  <button key={i} onClick={() => sendMsg(a.prompt)} disabled={sending || !isConfigured}
                    className="text-[10px] px-2.5 py-1.5 rounded-full bg-white/5 border border-white/10 text-slate-400 hover:bg-violet-600/20 hover:text-violet-300 hover:border-violet-500/30 active:scale-95 transition-all disabled:opacity-30">
                    {a.label}
                  </button>
                ))}
              </div>
              {!isConfigured && (
                <p className="text-[10px] text-amber-400/80 mt-2">
                  ⚠️ Pehle <button onClick={() => setView('settings')} className="underline font-medium">Settings</button> mein API key add karo
                </p>
              )}
            </div>
          </motion.div>
        )}

        {/* ═══════════════════════════════════════════════════════════════════ */}
        {/* CHAT VIEW                                                          */}
        {/* ═══════════════════════════════════════════════════════════════════ */}
        {view === 'chat' && (
          <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} className="flex flex-col">

            {/* Active Model Badge */}
            <div className="flex items-center gap-2 mb-3">
              <Bot className="w-3.5 h-3.5 text-violet-400" />
              <span className="text-[10px] text-slate-400">{currentModel?.name || settings.model}</span>
              {totalTokens > 0 && (
                <span className="text-[9px] text-slate-600 ml-auto font-mono">{totalTokens.toLocaleString()} tokens used</span>
              )}
            </div>

            {/* Messages */}
            <div className="rounded-2xl bg-white/[0.02] border border-white/5 overflow-hidden">
              <div className="max-h-[55vh] overflow-y-auto p-3 space-y-3">
                {msgs.length === 0 && !sending && (
                  <div className="text-center py-10">
                    <Brain className="w-10 h-10 text-violet-500/20 mx-auto mb-3" />
                    <p className="text-xs text-slate-500">Apni website ki koi bhi problem pucho</p>
                    <p className="text-[9px] text-slate-600 mt-1">Diagnostics auto-attach honge</p>
                  </div>
                )}

                {msgs.map((m, i) => (
                  <motion.div key={i} initial={{ opacity: 0, y: 6 }} animate={{ opacity: 1, y: 0 }}
                    className={`flex ${m.role === 'user' ? 'justify-end' : 'justify-start'}`}>
                    <div className={`max-w-[88%] rounded-2xl px-3.5 py-2.5 ${
                      m.role === 'user'
                        ? 'bg-indigo-600/25 border border-indigo-500/20'
                        : 'bg-white/[0.03] border border-white/5'
                    }`}>
                      {m.role === 'assistant' && (
                        <div className="flex items-center gap-1.5 mb-1.5">
                          <Bot className="w-3 h-3 text-violet-400" />
                          <span className="text-[8px] text-violet-400/70 font-mono">{m.model || 'AI'}</span>
                          {m.tokens ? <span className="text-[8px] text-slate-600 font-mono">{m.tokens}t</span> : null}
                          <button onClick={() => copyText(m.content)} className="ml-auto p-0.5 rounded hover:bg-white/10 transition-colors">
                            <Copy className="w-3 h-3 text-slate-600 hover:text-slate-400" />
                          </button>
                        </div>
                      )}
                      <div className="text-[12px] leading-relaxed whitespace-pre-wrap break-words text-slate-200">
                        {m.content.split('\n').map((line, li) => {
                          const parts = line.split(/(\*\*.*?\*\*|`[^`]+`)/g);
                          return (
                            <p key={li} className={li > 0 ? 'mt-1' : ''}>
                              {parts.map((p, pi) => {
                                if (p.startsWith('**') && p.endsWith('**')) {
                                  return <strong key={pi} className="text-white font-semibold">{p.slice(2, -2)}</strong>;
                                }
                                if (p.startsWith('`') && p.endsWith('`')) {
                                  return <code key={pi} className="text-[10px] px-1 py-0.5 rounded bg-black/40 text-violet-300 font-mono">{p.slice(1, -1)}</code>;
                                }
                                return <span key={pi}>{p}</span>;
                              })}
                            </p>
                          );
                        })}
                      </div>
                    </div>
                  </motion.div>
                ))}

                {sending && (
                  <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} className="flex justify-start">
                    <div className="bg-white/[0.03] border border-white/5 rounded-2xl px-3.5 py-2.5 flex items-center gap-2">
                      <Loader2 className="w-3.5 h-3.5 text-violet-400 animate-spin" />
                      <span className="text-[11px] text-slate-400">Analyzing...</span>
                    </div>
                  </motion.div>
                )}

                {chatErr && (
                  <div className="bg-rose-950/30 border border-rose-500/20 rounded-lg px-3 py-2 text-[11px] text-rose-300">❌ {chatErr}</div>
                )}

                <div ref={chatEndRef} />
              </div>

              {/* Input */}
              <div className="border-t border-white/5 p-2.5 flex gap-2 items-end">
                <textarea ref={inputRef} value={input} onChange={e => setInput(e.target.value)}
                  onKeyDown={e => { if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); sendMsg(); } }}
                  placeholder={isConfigured ? 'Problem batao...' : 'Pehle Settings mein API key add karo...'}
                  disabled={!isConfigured}
                  rows={1}
                  className="flex-1 bg-white/5 border border-white/10 rounded-xl px-3 py-2 text-sm text-white placeholder:text-slate-600 resize-none focus:outline-none focus:ring-1 focus:ring-violet-500/40 disabled:opacity-40"
                  style={{ minHeight: '36px', maxHeight: '100px' }} />
                <button onClick={() => sendMsg()} disabled={!input.trim() || sending || !isConfigured}
                  className="w-9 h-9 rounded-xl bg-violet-600 flex items-center justify-center hover:bg-violet-500 active:scale-95 transition-all disabled:opacity-25 flex-shrink-0">
                  {sending ? <Loader2 className="w-3.5 h-3.5 text-white animate-spin" /> : <Send className="w-3.5 h-3.5 text-white" />}
                </button>
              </div>
            </div>

            {/* Quick Actions below chat */}
            <div className="flex flex-wrap gap-1 mt-3">
              {QUICK_ACTIONS.slice(0, 4).map((a, i) => (
                <button key={i} onClick={() => sendMsg(a.prompt)} disabled={sending || !isConfigured}
                  className="text-[9px] px-2 py-1 rounded-full bg-white/5 text-slate-500 hover:text-violet-300 transition-all disabled:opacity-30">
                  {a.label}
                </button>
              ))}
            </div>

            {/* Clear chat */}
            {msgs.length > 0 && (
              <button onClick={() => { setMsgs([]); setTotalTokens(0); }}
                className="flex items-center gap-1 mt-3 text-[9px] text-slate-600 hover:text-rose-400 transition-colors mx-auto">
                <Trash2 className="w-3 h-3" /> Clear chat
              </button>
            )}
          </motion.div>
        )}

        {/* ═══════════════════════════════════════════════════════════════════ */}
        {/* SETTINGS VIEW                                                      */}
        {/* ═══════════════════════════════════════════════════════════════════ */}
        {view === 'settings' && (
          <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} className="space-y-4">

            {/* API Key */}
            <div className="rounded-2xl bg-white/[0.03] border border-white/5 p-4">
              <div className="flex items-center gap-2 mb-3">
                <Key className="w-4 h-4 text-amber-400" />
                <span className="text-sm font-semibold">Google Gemini API Key</span>
                {isConfigured && <span className="ml-auto text-[9px] bg-emerald-500/20 text-emerald-400 px-2 py-0.5 rounded-full border border-emerald-500/30">Active</span>}
              </div>

              {/* Current key status */}
              {settings.hasApiKey && (
                <div className="mb-3 flex items-center gap-2 text-[11px] text-slate-400 bg-black/30 rounded-lg px-3 py-2">
                  <Shield className="w-3.5 h-3.5 text-emerald-400" />
                  <span>Current: {settings.apiKey}</span>
                </div>
              )}

              <div className="relative">
                <input
                  type={showKey ? 'text' : 'password'}
                  value={newApiKey}
                  onChange={e => setNewApiKey(e.target.value)}
                  placeholder="AIzaSy... (new key paste karo)"
                  className="w-full bg-black/40 border border-white/10 rounded-xl px-3 py-2.5 text-sm text-white placeholder:text-slate-600 focus:outline-none focus:ring-1 focus:ring-violet-500/40 pr-10 font-mono"
                />
                <button onClick={() => setShowKey(!showKey)} className="absolute right-2 top-1/2 -translate-y-1/2 p-1 rounded hover:bg-white/10 transition-colors">
                  {showKey ? <EyeOff className="w-4 h-4 text-slate-500" /> : <Eye className="w-4 h-4 text-slate-500" />}
                </button>
              </div>

              <p className="text-[9px] text-slate-600 mt-2">
                🔗 Key yahan se lo:{' '}
                <a href="https://aistudio.google.com/apikey" target="_blank" rel="noopener noreferrer" className="text-violet-400 underline">
                  aistudio.google.com/apikey
                </a>
                {' '}— Free hai
              </p>
            </div>

            {/* Model Selector */}
            <div className="rounded-2xl bg-white/[0.03] border border-white/5 p-4">
              <div className="flex items-center gap-2 mb-3">
                <Cpu className="w-4 h-4 text-violet-400" />
                <span className="text-sm font-semibold">AI Model</span>
              </div>

              <div className="space-y-2">
                {models.map(m => (
                  <button key={m.id} onClick={() => setSettings(s => ({ ...s, model: m.id }))}
                    className={`w-full text-left rounded-xl border p-3 transition-all ${
                      settings.model === m.id
                        ? 'bg-violet-600/15 border-violet-500/30 ring-1 ring-violet-500/20'
                        : 'bg-black/20 border-white/5 hover:bg-white/5'
                    }`}>
                    <div className="flex items-center gap-2">
                      <div className={`w-2 h-2 rounded-full ${settings.model === m.id ? 'bg-violet-400' : 'bg-slate-700'}`} />
                      <span className="text-[12px] font-semibold text-white flex-1">{m.name}</span>
                      <span className={`text-[8px] px-1.5 py-0.5 rounded-full border font-medium ${tierBadge[m.tier] || tierBadge.economy}`}>
                        {m.tier}
                      </span>
                    </div>
                    <p className="text-[10px] text-slate-500 mt-1 ml-4">{m.description}</p>
                    <p className="text-[8px] text-slate-600 mt-0.5 ml-4 font-mono">Context: {m.contextWindow}</p>
                  </button>
                ))}
              </div>
            </div>

            {/* Custom Instructions */}
            <div className="rounded-2xl bg-white/[0.03] border border-white/5 p-4">
              <div className="flex items-center gap-2 mb-3">
                <PenLine className="w-4 h-4 text-sky-400" />
                <span className="text-sm font-semibold">Custom Instructions</span>
                <span className="text-[8px] text-slate-600 ml-auto">{customInst.length}/2000</span>
              </div>
              <textarea
                value={customInst}
                onChange={e => setCustomInst(e.target.value.slice(0, 2000))}
                placeholder="AI ko extra instructions do... (optional)&#10;e.g. 'Hamesha code examples de', 'Response chhota rakh'"
                rows={3}
                className="w-full bg-black/40 border border-white/10 rounded-xl px-3 py-2.5 text-[12px] text-white placeholder:text-slate-600 resize-none focus:outline-none focus:ring-1 focus:ring-violet-500/40"
              />
            </div>

            {/* Save Button */}
            <button onClick={saveSettings} disabled={savingSettings}
              className="w-full flex items-center justify-center gap-2 py-3 rounded-xl bg-violet-600 hover:bg-violet-500 active:scale-[0.98] transition-all font-semibold text-sm disabled:opacity-50">
              {savingSettings ? <Loader2 className="w-4 h-4 animate-spin" /> : <Save className="w-4 h-4" />}
              {savingSettings ? 'Saving...' : 'Save Settings'}
            </button>

            {settingsMsg && (
              <motion.p initial={{ opacity: 0 }} animate={{ opacity: 1 }}
                className={`text-center text-xs ${settingsMsg.startsWith('✅') ? 'text-emerald-400' : 'text-rose-400'}`}>
                {settingsMsg}
              </motion.p>
            )}

            {/* Info */}
            <div className="rounded-xl bg-slate-900/50 border border-white/5 p-3">
              <div className="flex items-start gap-2">
                <Info className="w-3.5 h-3.5 text-slate-500 mt-0.5 flex-shrink-0" />
                <div className="text-[10px] text-slate-500 space-y-1">
                  <p>• API key Firebase mein encrypted save hoti hai (system/ai_settings)</p>
                  <p>• Settings kabhi bhi change kar sakte ho — model bhi, key bhi</p>
                  <p>• Gemini API FREE hai — Google AI Studio se key lo</p>
                  <p>• 2.0 Flash recommended hai — fast + smart balance</p>
                </div>
              </div>
            </div>
          </motion.div>
        )}

        <div className="h-8" />
      </div>
    </div>
  );
}
