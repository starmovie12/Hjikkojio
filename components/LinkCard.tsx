'use client';
import { Video, CircleCheck, CircleDashed, Copy, Check, AlertCircle, RotateCcw, Loader2 } from 'lucide-react';
import { useState, useEffect, useRef } from 'react';
import { motion, AnimatePresence } from 'motion/react';

interface LogEntry {
  msg: string;
  type: 'info' | 'success' | 'error' | 'warn';
}

interface LinkCardProps {
  id: number;
  linkId: number | string;
  name: string;
  logs: LogEntry[];
  finalLink: string | null;
  status: 'processing' | 'done' | 'error';
  onRepeat?: (linkId: number | string) => void;
  isRepeating?: boolean;
}

export default function LinkCard({
  id,
  linkId,
  name,
  logs,
  finalLink,
  status,
  onRepeat,
  isRepeating = false,
}: LinkCardProps) {
  const [copied, setCopied] = useState(false);
  const [showLogs, setShowLogs] = useState(true);
  const logEndRef = useRef<HTMLDivElement>(null);

  const handleCopy = async () => {
    if (!finalLink) return;
    try {
      await navigator.clipboard.writeText(finalLink);
      setCopied(true);
      if (navigator.vibrate) navigator.vibrate(50);
      setTimeout(() => setCopied(false), 2000);
    } catch (err) {
      console.error('Failed to copy:', err);
    }
  };

  useEffect(() => {
    if (logEndRef.current) {
      logEndRef.current.scrollTop = logEndRef.current.scrollHeight;
    }
  }, [logs]);

  const getStatusColor = () => {
    switch (status) {
      case 'done':  return 'border-emerald-500 bg-emerald-500/5';
      case 'error': return 'border-rose-500 bg-rose-500/5';
      default:      return 'border-indigo-500 bg-white/5';
    }
  };

  const getLogColor = (type: string) => {
    switch (type) {
      case 'success': return 'text-emerald-400 font-bold';
      case 'error':   return 'text-rose-400';
      case 'warn':    return 'text-amber-400';
      case 'info':    return 'text-blue-400';
      default:        return 'text-slate-400';
    }
  };

  // Filter out "Queued for processing..." — only show real logs
  const realLogs = logs.filter(
    (log) => !log.msg.includes('Queued for processing')
  );

  return (
    <motion.div
      initial={{ opacity: 0, y: 10 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ delay: id * 0.05 }}
      className={`p-4 mb-3 rounded-2xl border-l-4 backdrop-blur-md border transition-all ${getStatusColor()}`}
    >
      {/* Header Row */}
      <div className="flex items-center justify-between mb-2">
        <div className="flex items-center gap-2 min-w-0 flex-1">
          <Video className="w-4 h-4 text-indigo-400 flex-shrink-0" />
          <span className="text-white text-sm font-semibold truncate">{name}</span>
        </div>

        <div className="flex items-center gap-1.5 flex-shrink-0">
          {(status === 'error' || status === 'done') && onRepeat && (
            <button
              onClick={(e) => {
                e.stopPropagation();
                onRepeat(linkId);
              }}
              disabled={isRepeating}
              className={`p-1.5 rounded-lg transition-all ${
                status === 'error'
                  ? 'text-rose-400 hover:bg-rose-500/10 hover:text-rose-300'
                  : 'text-slate-500 hover:bg-slate-500/10 hover:text-slate-300'
              }`}
              title={status === 'error' ? 'Retry this link' : 'Re-extract this link'}
            >
              {isRepeating ? (
                <Loader2 className="w-3.5 h-3.5 animate-spin" />
              ) : (
                <RotateCcw className="w-3.5 h-3.5" />
              )}
            </button>
          )}

          {status === 'processing' && <CircleDashed className="w-4 h-4 text-indigo-400 animate-spin" />}
          {status === 'done' && <CircleCheck className="w-4 h-4 text-emerald-400" />}
          {status === 'error' && <AlertCircle className="w-4 h-4 text-rose-400" />}
        </div>
      </div>

      {/* Logs — ONLY when real logs exist (no "Queued" or "Processing" filler) */}
      {realLogs.length > 0 && (
        <>
          <button
            onClick={() => setShowLogs(!showLogs)}
            className="text-[10px] text-slate-600 hover:text-slate-400 mb-1.5 flex items-center gap-1 transition-colors"
          >
            {showLogs ? '\u25BC' : '\u25B6'} Logs ({realLogs.length})
          </button>

          {showLogs && (
            <div
              ref={logEndRef}
              className="bg-black/80 p-3 rounded-lg font-mono text-[11px] max-h-[150px] overflow-y-auto mb-3 space-y-0.5"
            >
              {realLogs.map((log, i) => (
                <div key={i} className={`leading-relaxed ${getLogColor(log.type)}`}>
                  &gt; {log.msg}
                </div>
              ))}
            </div>
          )}
        </>
      )}

      {/* Final Link — OUTPUT */}
      <AnimatePresence>
        {finalLink && (
          <motion.div
            initial={{ opacity: 0, height: 0 }}
            animate={{ opacity: 1, height: 'auto' }}
            exit={{ opacity: 0, height: 0 }}
            transition={{ duration: 0.2 }}
          >
            <div
              onClick={handleCopy}
              className="cursor-pointer bg-emerald-500/10 border border-emerald-500/50 text-emerald-400 rounded-xl font-mono text-xs p-3 flex items-center justify-between gap-2 hover:bg-emerald-500/20 transition-colors"
            >
              <span className="truncate">
                {copied ? 'COPIED TO CLIPBOARD! \u2705' : finalLink}
              </span>
              {copied ? (
                <Check className="w-3.5 h-3.5 flex-shrink-0" />
              ) : (
                <Copy className="w-3.5 h-3.5 flex-shrink-0" />
              )}
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </motion.div>
  );
}
