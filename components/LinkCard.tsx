'use client';
import { Video, CircleCheck, CircleDashed, Copy, Check, AlertCircle } from 'lucide-react';
import { useState, useEffect, useRef } from 'react';
import { motion, AnimatePresence } from 'motion/react';

interface LogEntry {
  msg: string;
  type: 'info' | 'success' | 'error' | 'warn';
}

interface LinkCardProps {
  id: number;
  name: string;
  logs: LogEntry[];
  finalLink: string | null;
  status: 'processing' | 'done' | 'error';
}

export default function LinkCard({ id, name, logs, finalLink, status }: LinkCardProps) {
  const [copied, setCopied] = useState(false);
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

  return (
    <motion.div
      initial={{ opacity: 0, y: 10 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ delay: id * 0.05 }}
      className={`p-4 mb-4 rounded-2xl border-l-4 backdrop-blur-md border transition-all ${getStatusColor()}`}
    >
      {/* Header */}
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-2">
          <Video className="w-4 h-4 text-indigo-400 flex-shrink-0" />
          <span className="text-white text-sm font-semibold truncate max-w-[200px]">{name}</span>
        </div>
        <div className="flex-shrink-0">
          {status === 'processing' && <CircleDashed className="w-4 h-4 text-indigo-400 animate-spin" />}
          {status === 'done' && <CircleCheck className="w-4 h-4 text-emerald-400" />}
          {status === 'error' && <AlertCircle className="w-4 h-4 text-rose-400" />}
        </div>
      </div>

      {/* Live Logs Terminal */}
      {logs.length > 0 ? (
        <div
          ref={logEndRef}
          className="bg-black/80 p-3 rounded-lg font-mono text-[11px] max-h-[150px] overflow-y-auto mb-3 space-y-0.5"
        >
          {logs.map((log, i) => (
            <div key={i} className={`leading-relaxed ${getLogColor(log.type)}`}>
              &gt; {log.msg}
            </div>
          ))}
          {status === 'processing' && (
            <div className="text-slate-500 animate-pulse">&gt; Processing...</div>
          )}
        </div>
      ) : status === 'processing' ? (
        <div className="bg-black/80 p-3 rounded-lg font-mono text-[11px] mb-3">
          <div className="text-slate-500 animate-pulse">&gt; Queued for processing...</div>
        </div>
      ) : null}

      {/* Final Link */}
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
                {copied ? 'COPIED TO CLIPBOARD! ✅' : finalLink}
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
