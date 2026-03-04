/**
 * lib/vpsConfig.ts
 *
 * Dynamic VPS configuration loader.
 * Reads from Firebase Firestore (system/vps_config) with a 60-second in-memory cache.
 * Falls back to env vars / hardcoded defaults if Firebase read fails.
 *
 * Usage in solvers:
 *   const { hubcloudApi, timerApi } = await getVpsConfig();
 */

import { db } from './firebaseAdmin';

// ─── Shape of the config document ────────────────────────────────────────────
export interface VpsConfig {
  vpsBaseUrl:    string;   // e.g. "http://85.121.5.246"
  hubcloudPort:  string;   // e.g. "5001"
  timerPort:     string;   // e.g. "10000"
}

// ─── Defaults (env vars → hardcoded fallback) ─────────────────────────────────
const DEFAULTS: VpsConfig = {
  vpsBaseUrl:   process.env.VPS_BASE_URL   || 'http://85.121.5.246',
  hubcloudPort: process.env.HUBCLOUD_PORT  || '5001',
  timerPort:    process.env.TIMER_PORT     || '10000',
};

// ─── 60-second in-memory cache ───────────────────────────────────────────────
let _cache: { config: VpsConfig; at: number } | null = null;
const CACHE_TTL_MS = 60_000; // 1 minute

export interface ResolvedVpsConfig {
  config:       VpsConfig;
  hubcloudApi:  string;   // full URL, e.g. "http://85.121.5.246:5001"
  timerApi:     string;   // full URL, e.g. "http://85.121.5.246:10000"
}

function resolve(cfg: VpsConfig): ResolvedVpsConfig {
  return {
    config:      cfg,
    hubcloudApi: `${cfg.vpsBaseUrl}:${cfg.hubcloudPort}`,
    timerApi:    `${cfg.vpsBaseUrl}:${cfg.timerPort}`,
  };
}

/**
 * Returns VPS API URLs from Firebase (cached for 60s).
 * Never throws — always returns a usable config.
 */
export async function getVpsConfig(): Promise<ResolvedVpsConfig> {
  const now = Date.now();

  // Return cache if still fresh
  if (_cache && now - _cache.at < CACHE_TTL_MS) {
    return resolve(_cache.config);
  }

  try {
    const doc  = await db.collection('system').doc('vps_config').get();
    const data = doc.exists ? (doc.data() as Partial<VpsConfig>) : {};

    const config: VpsConfig = {
      vpsBaseUrl:   (data.vpsBaseUrl   || DEFAULTS.vpsBaseUrl).trim(),
      hubcloudPort: (data.hubcloudPort || DEFAULTS.hubcloudPort).trim(),
      timerPort:    (data.timerPort    || DEFAULTS.timerPort).trim(),
    };

    _cache = { config, at: now };
    return resolve(config);
  } catch (err) {
    console.warn('[vpsConfig] Firebase read failed, using defaults:', err);
    return resolve(DEFAULTS);
  }
}

/**
 * Call this after saving new config to Firebase so next call re-fetches.
 */
export function invalidateVpsConfigCache(): void {
  _cache = null;
}

/** Expose defaults for the settings UI initial state */
export { DEFAULTS as VPS_DEFAULTS };
