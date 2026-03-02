/**
 * POST /api/tasks/bulk
 *
 * Phase 4-5: Bulk URL Import
 * - Accepts array of URLs
 * - Deduplicates against existing queue & tasks
 * - Adds unique URLs to movies_queue with status 'pending'
 */

import { NextRequest, NextResponse } from 'next/server';
import { db } from '@/lib/firebaseAdmin';

export const dynamic = 'force-dynamic';
export const maxDuration = 30;

export async function POST(req: NextRequest) {
  try {
    const body = await req.json();
    const urls: string[] = body.urls || [];
    const priority: number = body.priority || 3; // 1=urgent, 3=normal, 5=low

    if (!urls.length) {
      return NextResponse.json({ error: 'No URLs provided' }, { status: 400 });
    }

    if (urls.length > 100) {
      return NextResponse.json({ error: 'Max 100 URLs per batch' }, { status: 400 });
    }

    // ─── 1. Normalize URLs ───────────────────────────────────────────────
    const normalized = urls
      .map(u => u.trim())
      .filter(u => u.startsWith('http'));

    // ─── 2. Get existing URLs to deduplicate ─────────────────────────────
    const existingUrls = new Set<string>();

    // Check scraping_tasks
    const tasksSnap = await db.collection('scraping_tasks').get();
    tasksSnap.docs.forEach(doc => {
      const url = doc.data().url;
      if (url) existingUrls.add(url.toLowerCase().trim());
    });

    // Check queue collections
    for (const col of ['movies_queue', 'webseries_queue']) {
      const qSnap = await db.collection(col).get();
      qSnap.docs.forEach(doc => {
        const url = doc.data().url;
        if (url) existingUrls.add(url.toLowerCase().trim());
      });
    }

    // ─── 3. Filter unique URLs ───────────────────────────────────────────
    const unique: string[] = [];
    const duplicates: string[] = [];

    for (const url of normalized) {
      if (existingUrls.has(url.toLowerCase().trim())) {
        duplicates.push(url);
      } else {
        unique.push(url);
        existingUrls.add(url.toLowerCase().trim()); // prevent self-duplicates
      }
    }

    // ─── 4. Add to movies_queue ──────────────────────────────────────────
    const batch = db.batch();
    const now = new Date().toISOString();

    for (const url of unique) {
      const docRef = db.collection('movies_queue').doc();
      batch.set(docRef, {
        url,
        status: 'pending',
        priority,
        retryCount: 0,
        createdAt: now,
        updatedAt: now,
        source: 'bulk-import',
      });
    }

    if (unique.length > 0) {
      await batch.commit();
    }

    return NextResponse.json({
      ok: true,
      added: unique.length,
      duplicates: duplicates.length,
      skipped: urls.length - normalized.length,
      total: urls.length,
      message: `Added ${unique.length} URLs to queue. ${duplicates.length} duplicates skipped.`,
    });
  } catch (err: any) {
    return NextResponse.json({ error: err.message }, { status: 500 });
  }
}
