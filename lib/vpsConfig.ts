/**
 * lib/vpsConfig.ts
 *
 * Dynamic VPS configuration loader.
 * Reads directly from Firebase Firestore (system/vps_config).
 * Cache removed to ensure INSTANT updates across all serverless functions.
 * Falls back to env vars / hardcoded defaults if Firebase read fails.
 *
 * Usage in solvers:
 * const { hubcloudApi, timerApi } = await getVpsConfig();
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

export interface ResolvedVpsConfig {
  config:       VpsConfig;
  hubcloudApi:  string;   // full URL, e.g. "http://85.121.5.246:5001"
  timerApi:     string;   // full URL, e.g. "http://85.121.5.246:10000"
}

function resolve(cfg: VpsConfig): ResolvedVpsConfig {
  // Sabse zaroori fix: URL ke aakhiri ka '/' hatao taaki http://.../:5001 na bane
  const cleanBaseUrl = cfg.vpsBaseUrl.replace(/\/+$/, '');
  
  return {
    config:      cfg,
    hubcloudApi: `${cleanBaseUrl}:${cfg.hubcloudPort}`,
    timerApi:    `${cleanBaseUrl}:${cfg.timerPort}`,
  };
}

/**
 * Returns VPS API URLs from Firebase (ALWAYS FRESH).
 * Never throws — always returns a usable config.
 */
export async function getVpsConfig(): Promise<ResolvedVpsConfig> {
  try {
    const doc  = await db.collection('system').doc('vps_config').get();
    const data = doc.exists ? (doc.data() as Partial<VpsConfig>) : {};

    const config: VpsConfig = {
      vpsBaseUrl:   (data.vpsBaseUrl   || DEFAULTS.vpsBaseUrl).trim(),
      hubcloudPort: (data.hubcloudPort || DEFAULTS.hubcloudPort).trim(),
      timerPort:    (data.timerPort    || DEFAULTS.timerPort).trim(),
    };

    console.log('[VPS Config] Loaded FRESH URL from Firebase:', config.vpsBaseUrl);
    return resolve(config);
  } catch (err) {
    console.warn('[vpsConfig] Firebase read failed, using defaults:', err);
    return resolve(DEFAULTS);
  }
}

/**
 * Empty function kept so old API route doesn't crash.
 * Cache is no longer used, so nothing needs to be invalidated.
 */
export function invalidateVpsConfigCache(): void {
  // Intentionally empty
}

/** Expose defaults for the settings UI initial state */
export { DEFAULTS as VPS_DEFAULTS };
