/**
 * /api/ai/settings — AI Configuration Manager
 *
 * GET  → Load saved AI settings from Firebase
 * POST → Save AI settings (API key, model, custom instructions)
 *
 * Firebase doc: system/ai_settings
 */

import { NextRequest, NextResponse } from 'next/server';
import { db } from '@/lib/firebaseAdmin';

export const dynamic = 'force-dynamic';

const SETTINGS_DOC = 'system/ai_settings';

// ─── Available Gemini Models ─────────────────────────────────────────────────
// Ye list frontend ko bhi jaayegi for model selector
export const AVAILABLE_MODELS = [
  {
    id: 'gemini-3.1-pro-preview',
    name: 'Gemini 3.1 Pro (Preview)',
    description: 'Newest & most powerful — advanced reasoning & coding (Feb 2026)',
    tier: 'powerful',
    contextWindow: '1M tokens',
  },
  {
    id: 'gemini-3-flash-preview',
    name: 'Gemini 3 Flash (Preview)',
    description: 'Frontier-class performance — fast & smart',
    tier: 'recommended',
    contextWindow: '1M tokens',
  },
  {
    id: 'gemini-2.5-flash',
    name: 'Gemini 2.5 Flash',
    description: 'Stable & reliable — best balance of speed & quality',
    tier: 'recommended',
    contextWindow: '1M tokens',
  },
  {
    id: 'gemini-2.5-flash-lite',
    name: 'Gemini 2.5 Flash Lite',
    description: 'Ultra-fast, lightweight — for quick questions',
    tier: 'fast',
    contextWindow: '1M tokens',
  },
  {
    id: 'gemini-2.5-pro',
    name: 'Gemini 2.5 Pro',
    description: 'Deep reasoning — complex analysis & coding tasks',
    tier: 'powerful',
    contextWindow: '1M tokens',
  },
  {
    id: 'gemini-2.0-flash',
    name: 'Gemini 2.0 Flash',
    description: 'Legacy stable — still works (retiring June 2026)',
    tier: 'economy',
    contextWindow: '1M tokens',
  },
];

const DEFAULT_SETTINGS = {
  apiKey: '',
  model: 'gemini-2.5-flash',
  customInstructions: '',
  createdAt: new Date().toISOString(),
  updatedAt: new Date().toISOString(),
};

// ─── GET /api/ai/settings ────────────────────────────────────────────────────
export async function GET() {
  try {
    const snap = await db.doc(SETTINGS_DOC).get();

    if (!snap.exists) {
      return NextResponse.json({
        settings: DEFAULT_SETTINGS,
        models: AVAILABLE_MODELS,
        isConfigured: false,
      });
    }

    const data = snap.data()!;

    // Auto-migrate retired models to default
    const validModelIds = AVAILABLE_MODELS.map(m => m.id);
    let currentModel = data.model || 'gemini-2.5-flash';
    let needsMigration = false;
    if (!validModelIds.includes(currentModel)) {
      currentModel = 'gemini-2.5-flash'; // Migrate to stable default
      needsMigration = true;
      // Save migration
      try { await db.doc(SETTINGS_DOC).update({ model: currentModel, updatedAt: new Date().toISOString() }); } catch {}
    }

    // Mask API key for security — show only last 8 chars
    const maskedKey = data.apiKey
      ? '••••••••' + data.apiKey.slice(-8)
      : '';

    return NextResponse.json({
      settings: {
        ...data,
        model: currentModel,
        apiKey: maskedKey,
        hasApiKey: !!data.apiKey,
      },
      models: AVAILABLE_MODELS,
      isConfigured: !!data.apiKey,
    });
  } catch (e: any) {
    return NextResponse.json({ error: e.message }, { status: 500 });
  }
}

// ─── POST /api/ai/settings ───────────────────────────────────────────────────
export async function POST(req: NextRequest) {
  try {
    const body = await req.json();
    const { apiKey, model, customInstructions } = body;

    // Build update object — only include fields that were sent
    const update: any = {
      updatedAt: new Date().toISOString(),
    };

    if (apiKey !== undefined && apiKey !== null) {
      // If apiKey is empty string, clear it
      // If it starts with '••••', don't update (it's the masked version)
      if (apiKey === '') {
        update.apiKey = '';
      } else if (!apiKey.startsWith('••••')) {
        update.apiKey = apiKey.trim();
      }
    }

    if (model) {
      // Validate model exists in our list
      const validModel = AVAILABLE_MODELS.find(m => m.id === model);
      if (!validModel) {
        return NextResponse.json({ error: `Invalid model: ${model}` }, { status: 400 });
      }
      update.model = model;
    }

    if (customInstructions !== undefined) {
      update.customInstructions = (customInstructions || '').slice(0, 2000); // Max 2000 chars
    }

    // Create or update
    const snap = await db.doc(SETTINGS_DOC).get();
    if (!snap.exists) {
      await db.doc(SETTINGS_DOC).set({
        ...DEFAULT_SETTINGS,
        ...update,
        createdAt: new Date().toISOString(),
      });
    } else {
      await db.doc(SETTINGS_DOC).update(update);
    }

    return NextResponse.json({ success: true, message: 'Settings saved' });
  } catch (e: any) {
    return NextResponse.json({ error: e.message }, { status: 500 });
  }
}
