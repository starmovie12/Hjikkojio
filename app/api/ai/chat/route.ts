/**
 * /api/ai/chat — MFLIX PRO AI Assistant (Google Gemini)
 *
 * Reads API key + model from Firebase (system/ai_settings).
 * Sends diagnostics + conversation history to Gemini.
 * Returns AI response with full MFLIX system knowledge.
 */

import { NextRequest, NextResponse } from 'next/server';
import { db } from '@/lib/firebaseAdmin';

export const dynamic = 'force-dynamic';
export const maxDuration = 60;

// ─── MFLIX PRO System Prompt ─────────────────────────────────────────────────
const SYSTEM_PROMPT = `Tu MFLIX PRO ka AI System Administrator hai. Tu is website ka har ek part jaanta hai — har API route, har function, har Firebase collection, har solver, har config value. Tu Hindi + English mix mein baat karta hai (Hinglish). Tera jawab CHHOTA aur TO-THE-POINT hona chahiye. Faltu lectures mat de. Direct problem → cause → fix batao.

## MFLIX PRO Architecture:
- **Stack**: Next.js 15 (App Router) + Firebase Firestore + Vercel Hosting
- **VPS Server**: Python APIs at port 10000 (Timer bypass) and port 5001 (HubCloud bypass)
- **Cron**: GitHub Actions har ~5 minute mein /api/cron/process-queue ko call karta hai
- **Vercel Limit**: Har API route max 60 seconds run kar sakta hai (maxDuration: 60)

## Firebase Collections:
1. **movies_queue** & **webseries_queue**: URLs jo process hone baki hain (status: pending/processing/completed/failed)
2. **scraping_tasks**: Active tasks ki full detail — preview, links array (har link ka status: pending/processing/done/error), metadata
3. **system/engine_status**: Cron heartbeat — lastRunAt, status, details
4. **system/ai_settings**: AI model config (API key, model name, custom instructions)
5. **link_cache**: [Phase 4] Resolved link cache — 24h expiry, MD5 hash keys

## API Routes:
- **POST /api/tasks**: Movie URL se links extract karta hai, Firebase mein task create karta hai.
- **POST /api/solve_task**: Core solver — taskId + links, timer sequential + direct parallel. 45s time budget.
- **POST /api/stream_solve**: NDJSON streaming solver — manual START ENGINE button.
- **GET /api/cron/process-queue**: GitHub Cron — heartbeat, stuck recovery (10min → FAILED), pick 1 item, extract + solve.
- **GET /api/engine-status**: Heartbeat check + queue counts.
- **GET/PATCH /api/auto-process/queue**: Queue items list + status updates.
- **POST /api/admin/reset-stuck**: [Phase 4] Force reset ALL stuck tasks — admin button.
- **GET /api/admin/stats**: [Phase 4] Dashboard stats — totals, queue health, VPS status, cache stats.
- **POST /api/tasks/bulk**: [Phase 4] Bulk URL import — up to 100 URLs at once with deduplication.

## Solvers (lib/solvers.ts):
- extractMovieLinks(url): Movie page scrape → download links (Phase 4: 522 auto-retry added)
- solveHubCloudNative(url): HubCloud bypass → best_download_link
- solveHubDrive(url), solveHBLinks(url), solveHubCDN(url), solveGadgetsWebNative(url)
- axiosWithRetry(url): [Phase 4] Auto-retry on 522/502/503/504 errors (max 2 retries, 2s backoff)

## Config (lib/config.ts):
- TIMER_DOMAINS: ['gadgetsweb', 'review-tech', 'ngwin', 'cryptoinsights', 'techbigs', 'apkdone', 'linkvertise', ...] — SEQUENTIAL
- TARGET_DOMAINS: ['hblinks', 'hubdrive', 'hubcdn', 'hubcloud', 'gdflix', 'drivehub', 'filepress', 'hubstream', ...] — PARALLEL
- LINK_TIMEOUT_MS: 20000 (reduced from 25s) | OVERALL_TIMEOUT_MS: 50000 | STUCK_TASK_THRESHOLD_MS: 10min | MAX_CRON_RETRIES: 3
- [Phase 4] HTTP_522_MAX_RETRIES: 2 | SMART_RETRY_MAX: 3 | CACHE_EXPIRY_MS: 24h

## Link Cache (lib/cache.ts) [Phase 4]:
- getCachedLink(url): Check if link was already resolved (0ms vs 15-30s)
- setCachedLink(url, finalLink, solver): Store resolved link with 24h expiry
- cleanupExpiredCache(): Remove expired entries
- getCacheStats(): Total entries, valid, expired, total hits

## 4 Rules:
1. VPS Protection: Timer SEQUENTIAL, Direct PARALLEL
2. Zero-Drop Decoupling: Browser close = no effect on backend
3. State Hydration: Refresh pe Firebase se restore
4. Complete Extraction: Sab links done/error hone tak incomplete

## Phase 4 Features:
- **Stuck Task Recovery**: Cron automatically marks tasks stuck >10min as 'failed' (not kept as 'processing')
- **Force Reset Button**: Admin can force-reset all stuck tasks with one click
- **522 Auto-Retry**: Cloudflare 522/502/503/504 errors get 2 automatic retries with backoff
- **Link Cache**: Duplicate links resolved in 0ms from cache
- **Missing Domains**: Added 8+ new timer/target domains found in production logs
- **Reduced Timeout**: LINK_TIMEOUT 25s→20s to prevent Vercel 60s budget overflow
- **Bulk Import**: Add up to 100 URLs at once with automatic deduplication

## Frontend (MflixApp.tsx):
- Shield Pattern for polling protection
- 3-Layer: Stream > Shield > Firebase
- 5s task polling, 20s engine polling
- Auto-Pilot: Queue items via solve_task (localStorage persistent)
- [Phase 4] Force Reset button when stuck tasks detected
- [Phase 4] Stuck task auto-detection in engine status bar

## Common Problems:
- "Links fail" → VPS down / source website changed / env wrong / missing domain in config
- "Engine OFFLINE" → GitHub Actions disabled / CRON_SECRET mismatch
- "Task stuck" → Vercel 60s kill / cron auto-recovery will mark as failed
- "Queue pending" → Engine offline / stuck item blocking / use Force Reset
- "Low success rate" → Solver broken / VPS overloaded / check cache hit rate
- "No solver matched" → Domain missing from TIMER_DOMAINS or TARGET_DOMAINS in config.ts
- "522 errors" → VPS overloaded / Cloudflare timeout / auto-retry handles most cases

RESPONSE FORMAT: Problem → Root Cause → Step-by-step Fix. Chhoti problem bhi batao — chhoti se badi kharabi hoti hai.`;

// ─── Load settings from Firebase ─────────────────────────────────────────────
async function loadSettings(): Promise<{ apiKey: string; model: string; customInstructions: string }> {
  try {
    const snap = await db.doc('system/ai_settings').get();
    if (!snap.exists) return { apiKey: '', model: 'gemini-2.5-flash', customInstructions: '' };
    const data = snap.data()!;
    return {
      apiKey: data.apiKey || '',
      model: data.model || 'gemini-2.5-flash',
      customInstructions: data.customInstructions || '',
    };
  } catch {
    return { apiKey: '', model: 'gemini-2.5-flash', customInstructions: '' };
  }
}

// ─── POST /api/ai/chat ──────────────────────────────────────────────────────
export async function POST(req: NextRequest) {
  let body: any;
  try { body = await req.json(); } catch {
    return NextResponse.json({ error: 'Invalid JSON' }, { status: 400 });
  }

  const userMessage = body?.message;
  const conversationHistory: any[] = body?.history || [];
  const diagnostics = body?.diagnostics || null;
  const overrideApiKey = body?.apiKey;
  const overrideModel = body?.model;

  if (!userMessage) {
    return NextResponse.json({ error: 'message is required' }, { status: 400 });
  }

  const settings = await loadSettings();
  const apiKey = overrideApiKey || settings.apiKey;
  let model = overrideModel || settings.model;

  // Auto-fallback retired models
  const VALID_MODELS = ['gemini-3.1-pro-preview','gemini-3-flash-preview','gemini-2.5-flash','gemini-2.5-flash-lite','gemini-2.5-pro','gemini-2.0-flash'];
  if (!VALID_MODELS.includes(model)) {
    model = 'gemini-2.5-flash';
  }

  if (!apiKey) {
    return NextResponse.json(
      { error: 'API key set nahi hai. Settings (⚙️) mein jaake Gemini API key add karo.', needsSetup: true },
      { status: 400 }
    );
  }

  // ─── Build Gemini contents ─────────────────────────────────────────────
  const contents: any[] = [];

  for (const msg of conversationHistory) {
    contents.push({
      role: msg.role === 'assistant' ? 'model' : 'user',
      parts: [{ text: msg.content }],
    });
  }

  let fullMessage = userMessage;
  if (diagnostics) {
    fullMessage = `[SYSTEM DIAGNOSTICS — abhi ka data]\n\`\`\`json\n${JSON.stringify(diagnostics, null, 2)}\n\`\`\`\n\n[USER KA SAWAAL]\n${userMessage}`;
  }

  const systemInstruction = settings.customInstructions
    ? `${SYSTEM_PROMPT}\n\n## Custom Instructions:\n${settings.customInstructions}`
    : SYSTEM_PROMPT;

  contents.push({ role: 'user', parts: [{ text: fullMessage }] });

  // ─── Call Gemini API ───────────────────────────────────────────────────
  const geminiUrl = `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${apiKey}`;

  try {
    const response = await fetch(geminiUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        contents,
        systemInstruction: { parts: [{ text: systemInstruction }] },
        generationConfig: {
          temperature: 0.7,
          topP: 0.95,
          topK: 40,
          maxOutputTokens: 4096,
        },
        safetySettings: [
          { category: 'HARM_CATEGORY_HARASSMENT', threshold: 'BLOCK_NONE' },
          { category: 'HARM_CATEGORY_HATE_SPEECH', threshold: 'BLOCK_NONE' },
          { category: 'HARM_CATEGORY_SEXUALLY_EXPLICIT', threshold: 'BLOCK_NONE' },
          { category: 'HARM_CATEGORY_DANGEROUS_CONTENT', threshold: 'BLOCK_NONE' },
        ],
      }),
    });

    if (!response.ok) {
      const errData = await response.json().catch(() => ({}));
      const errMsg = errData?.error?.message || `HTTP ${response.status}`;

      if (response.status === 400 && errMsg.toLowerCase().includes('api key')) {
        return NextResponse.json({ error: 'API key galat ya expired hai. Settings check karo.', needsSetup: true }, { status: 400 });
      }
      if (response.status === 404) {
        return NextResponse.json({ error: `Model "${model}" available nahi. Settings mein dusra model chuno.`, needsSetup: true }, { status: 400 });
      }
      if (response.status === 429) {
        return NextResponse.json({ error: 'Rate limit — 1-2 min wait karo, phir try karo.' }, { status: 429 });
      }
      return NextResponse.json({ error: `Gemini error: ${errMsg}` }, { status: 502 });
    }

    const data = await response.json();
    const candidate = data.candidates?.[0];

    if (!candidate) {
      return NextResponse.json({ error: 'Gemini ne response nahi diya. Retry karo.' }, { status: 502 });
    }
    if (candidate.finishReason === 'SAFETY') {
      return NextResponse.json({ error: 'Safety block — rephrase karke try karo.' }, { status: 400 });
    }

    const text = candidate.content?.parts?.map((p: any) => p.text)?.join('\n') || 'No response.';

    return NextResponse.json({
      response: text,
      model,
      usage: {
        promptTokens: data.usageMetadata?.promptTokenCount || 0,
        completionTokens: data.usageMetadata?.candidatesTokenCount || 0,
        totalTokens: data.usageMetadata?.totalTokenCount || 0,
      },
    });
  } catch (e: any) {
    return NextResponse.json({ error: `Gemini connect fail: ${e.message}` }, { status: 502 });
  }
}
