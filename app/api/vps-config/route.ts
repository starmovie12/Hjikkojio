/**
 * GET  /api/admin/vps-config  — Read current VPS config from Firebase
 * POST /api/admin/vps-config  — Save new VPS config to Firebase
 *
 * Firebase path: system/vps_config
 * Fields: vpsBaseUrl, hubcloudPort, timerPort, updatedAt
 */

import { NextResponse }              from 'next/server';
import { db }                        from '@/lib/firebaseAdmin';
import { VPS_DEFAULTS, invalidateVpsConfigCache } from '@/lib/vpsConfig';

export const dynamic     = 'force-dynamic';
export const maxDuration = 15;

const DOC_REF = () => db.collection('system').doc('vps_config');

// ─────────────────────────────────────────────────────────────────────────────
// GET — return current saved config (or defaults if not yet configured)
// ─────────────────────────────────────────────────────────────────────────────
export async function GET() {
  try {
    const doc  = await DOC_REF().get();
    const data = doc.exists ? doc.data() : null;

    return NextResponse.json({
      vpsBaseUrl:   data?.vpsBaseUrl   || VPS_DEFAULTS.vpsBaseUrl,
      hubcloudPort: data?.hubcloudPort || VPS_DEFAULTS.hubcloudPort,
      timerPort:    data?.timerPort    || VPS_DEFAULTS.timerPort,
      updatedAt:    data?.updatedAt    || null,
      isDefault:    !doc.exists,
    });
  } catch (err: any) {
    return NextResponse.json({ error: err.message }, { status: 500 });
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// POST — validate + save new VPS config to Firebase
// Body: { vpsBaseUrl: string, hubcloudPort: string, timerPort: string }
// ─────────────────────────────────────────────────────────────────────────────
export async function POST(req: Request) {
  try {
    const body = await req.json();
    const { vpsBaseUrl, hubcloudPort, timerPort } = body;

    // ── Validation ──────────────────────────────────────────────────────────
    if (!vpsBaseUrl || typeof vpsBaseUrl !== 'string') {
      return NextResponse.json({ error: 'vpsBaseUrl is required' }, { status: 400 });
    }
    if (!hubcloudPort || typeof hubcloudPort !== 'string') {
      return NextResponse.json({ error: 'hubcloudPort is required' }, { status: 400 });
    }
    if (!timerPort || typeof timerPort !== 'string') {
      return NextResponse.json({ error: 'timerPort is required' }, { status: 400 });
    }

    // Basic URL sanity check (must start with http:// or https://)
    const cleanBase = vpsBaseUrl.trim().replace(/\/$/, ''); // remove trailing slash
    if (!cleanBase.startsWith('http://') && !cleanBase.startsWith('https://')) {
      return NextResponse.json(
        { error: 'vpsBaseUrl must start with http:// or https://' },
        { status: 400 }
      );
    }

    // Port must be numeric
    const hPort = hubcloudPort.trim();
    const tPort = timerPort.trim();
    if (!/^\d+$/.test(hPort) || !/^\d+$/.test(tPort)) {
      return NextResponse.json({ error: 'Ports must be numeric' }, { status: 400 });
    }

    // ── Save to Firebase ─────────────────────────────────────────────────────
    const payload = {
      vpsBaseUrl:   cleanBase,
      hubcloudPort: hPort,
      timerPort:    tPort,
      updatedAt:    new Date().toISOString(),
    };

    await DOC_REF().set(payload, { merge: true });

    // Bust the in-memory cache in this serverless instance
    invalidateVpsConfigCache();

    return NextResponse.json({
      success: true,
      saved:   payload,
      hubcloudApi: `${cleanBase}:${hPort}`,
      timerApi:    `${cleanBase}:${tPort}`,
    });

  } catch (err: any) {
    return NextResponse.json({ error: err.message }, { status: 500 });
  }
}
