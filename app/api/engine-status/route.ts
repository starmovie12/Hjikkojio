import { NextResponse } from 'next/server';
import { db } from '@/lib/firebaseAdmin';

export const dynamic = 'force-dynamic';

export async function GET() {
  try {
    const snap = await db.collection('system').doc('engine_status').get();

    if (!snap.exists) {
      return NextResponse.json({
        status:           'unknown',
        signal:           'OFFLINE',
        lastRunAt:        null,
        details:          'No heartbeat data found',
        backgroundActive: false,
      });
    }

    const data    = snap.data()!;
    const lastRunAt: string | null = data.lastRunAt || null;

    const now        = Date.now();
    const lastRun    = lastRunAt ? new Date(lastRunAt).getTime() : null;
    const TEN_MINUTES = 10 * 60 * 1000;
    const isOnline   = lastRun ? (now - lastRun < TEN_MINUTES) : false;

    // Human-readable time since last run
    let timeSinceLastRun = 'Unknown';
    if (lastRun) {
      const diffMs  = now - lastRun;
      const diffSec = Math.floor(diffMs / 1000);
      const diffMin = Math.floor(diffSec / 60);
      const diffHr  = Math.floor(diffMin / 60);

      if (diffSec < 60) {
        timeSinceLastRun = `${diffSec}s ago`;
      } else if (diffMin < 60) {
        timeSinceLastRun = `${diffMin}m ${diffSec % 60}s ago`;
      } else {
        timeSinceLastRun = `${diffHr}h ${diffMin % 60}m ago`;
      }
    }

    // Queue counts
    const [moviesPending, webseriesPending, moviesProcessing, webseriesProcessing] =
      await Promise.all([
        db.collection('movies_queue').where('status', '==', 'pending').get(),
        db.collection('webseries_queue').where('status', '==', 'pending').get(),
        db.collection('movies_queue').where('status', '==', 'processing').get(),
        db.collection('webseries_queue').where('status', '==', 'processing').get(),
      ]);

    const pendingCount    = moviesPending.size    + webseriesPending.size;
    const processingCount = moviesProcessing.size + webseriesProcessing.size;

    const backgroundActive = isOnline && (pendingCount > 0 || processingCount > 0);

    return NextResponse.json({
      status:           data.status || 'unknown',
      signal:           isOnline ? 'ONLINE' : 'OFFLINE',
      lastRunAt,
      timeSinceLastRun,
      details:          data.details || '',
      source:           data.source  || 'unknown',
      backgroundActive,
      pendingCount,
      processingCount,
    });
  } catch (err: any) {
    return NextResponse.json({ error: err.message }, { status: 500 });
  }
}
