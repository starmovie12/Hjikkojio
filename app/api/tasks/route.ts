import { NextRequest, NextResponse } from 'next/server';
import { db } from '@/lib/firebaseAdmin';
import { extractMovieLinks } from '@/lib/solvers';

export const dynamic = 'force-dynamic';
export const maxDuration = 60;

// ─── Telegram Alert ──────────────────────────────────────────────────────────
async function sendTelegramAlert(failedUrl: string, errorMessage: string) {
  const token  = process.env.TELEGRAM_BOT_TOKEN;
  const chatId = process.env.TELEGRAM_CHAT_ID;
  if (!token || !chatId) return;

  const text = `🚨 MFLIX ERROR 🚨\nURL: ${failedUrl}\nError: ${errorMessage}`;
  try {
    await fetch(`https://api.telegram.org/bot${token}/sendMessage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: chatId, text, parse_mode: 'HTML' }),
    });
  } catch { /* non-critical */ }
}

// ─── GET /api/tasks ───────────────────────────────────────────────────────────
export async function GET() {
  try {
    const snap = await db
      .collection('scraping_tasks')
      .orderBy('createdAt', 'desc')
      .limit(20)
      .get();

    const tasks = snap.docs.map(d => ({ id: d.id, ...d.data() }));
    return NextResponse.json(tasks);
  } catch (err: any) {
    return NextResponse.json({ error: err.message }, { status: 500 });
  }
}

// ─── POST /api/tasks ──────────────────────────────────────────────────────────
export async function POST(req: NextRequest) {
  let body: any;
  try {
    body = await req.json();
  } catch {
    return NextResponse.json({ error: 'Invalid JSON in request body' }, { status: 400 });
  }

  const url = body?.url;
  if (!url) return NextResponse.json({ error: 'URL is required' }, { status: 400 });
  const trimmedUrl = url.trim();

  try {

  // Step 2 — Duplicate check (non-fatal)
  let existingTaskId: string | null   = null;
  let existingTaskData: any           = null;
  try {
    const dupSnap = await db
      .collection('scraping_tasks')
      .where('url', '==', trimmedUrl)
      .limit(5)
      .get();

    if (!dupSnap.empty) {
      const sorted = dupSnap.docs.sort((a, b) =>
        (b.data().createdAt || '').localeCompare(a.data().createdAt || ''),
      );
      existingTaskId   = sorted[0].id;
      existingTaskData = sorted[0].data();
    }
  } catch { /* continue */ }

  // Step 3 — Extract links
  let listResult: Awaited<ReturnType<typeof extractMovieLinks>>;
  try {
    listResult = await extractMovieLinks(trimmedUrl);
  } catch (err: any) {
    listResult = { status: 'error', message: err.message, links: [], metadata: null, preview: null } as any;
  }

  // Step 4 — Merge / Retry if duplicate exists
  if (existingTaskId && existingTaskData) {
    if (listResult.status === 'success' && listResult.links?.length) {
      const existingLinks: any[] = existingTaskData.links || [];

      // Build existing URL set
      const existingUrls = new Set(existingLinks.map((l: any) => l.link));

      // Retry errored/failed links
      const updatedExisting = existingLinks.map((l: any) =>
        ['error', 'failed'].includes(l.status)
          ? { ...l, status: 'pending', logs: [{ msg: '🔄 Retrying...', type: 'info' }] }
          : l,
      );

      // Truly new links
      const trulyNewLinks = listResult.links.filter((l: any) => !existingUrls.has(l.link));

      // v4 TRAP 2 FIX: Safe unique IDs — use max existing ID, not length
      const existingIds = existingLinks.map((l: any) => l.id ?? 0);
      const maxExistingId = existingIds.length > 0 ? Math.max(...existingIds) : -1;

      const newLinksWithIds = trulyNewLinks.map((l: any, i: number) => ({
        ...l,
        id: maxExistingId + 1 + i,
        status: 'pending',
        logs: [],
      }));

      const mergedLinks = [...updatedExisting, ...newLinksWithIds];

      // v3 FIX E: NO fire-and-forget — cron will pick it up
      await db.collection('scraping_tasks').doc(existingTaskId).update({
        status: 'pending',
        error: null,
        links: mergedLinks,
        metadata: listResult.metadata || existingTaskData.metadata,
        preview:  listResult.preview  || existingTaskData.preview,
        updatedAt: new Date().toISOString(),
      });

      return NextResponse.json({
        taskId:       existingTaskId,
        metadata:     listResult.metadata || existingTaskData.metadata,
        preview:      listResult.preview  || existingTaskData.preview,
        merged:       true,
        newLinksAdded: trulyNewLinks.length,
        note: 'Task reset for retry successfully',
      });
    }

    // 4b — Extraction failed but duplicate exists
    return NextResponse.json({
      taskId:       existingTaskId,
      metadata:     existingTaskData.metadata,
      preview:      existingTaskData.preview,
      merged:       true,
      newLinksAdded: 0,
    });
  }

  // Step 5 — Create new task
  const hasLinks = listResult.status === 'success' && listResult.links?.length;

  const taskData: any = {
    url: trimmedUrl,
    status: 'pending',  // v3 FIX: cron will pick up — NOT 'processing'
    createdAt: new Date().toISOString(),
    extractedBy: 'Browser/Live',
    metadata: listResult.metadata || null,
    preview:  listResult.preview  || null,
    links: hasLinks
      ? listResult.links.map((l: any, i: number) => ({
          ...l,
          id: i,   // FIX D: stable originalIndex, assigned ONCE
          status: 'pending',
          logs: [{ msg: '🔍 Queued for processing...', type: 'info' }],
        }))
      : [],
  };

  const taskRef = await db.collection('scraping_tasks').add(taskData);
  const taskId  = taskRef.id;

  if (!hasLinks) {
    const errMsg = listResult.message || 'Extraction failed — 0 links found';
    await taskRef.update({ status: 'failed', error: errMsg });
    await sendTelegramAlert(trimmedUrl, errMsg);
  }

  // v3 FIX E: NO fire-and-forget. GitHub Cron (~1 min) will pick up.

  return NextResponse.json({
    taskId,
    metadata: listResult.metadata || null,
    preview:  listResult.preview  || null,
  });

  } catch (err: any) {
    // Step 6 — Unhandled error → 500
    return NextResponse.json({ error: err.message }, { status: 500 });
  }
}

// ─── DELETE /api/tasks ────────────────────────────────────────────────────────
export async function DELETE(req: NextRequest) {
  try {
    const { searchParams } = new URL(req.url);
    let taskId = searchParams.get('taskId');

    if (!taskId) {
      try {
        const body = await req.json();
        taskId = body?.taskId;
      } catch { /* ignore */ }
    }

    if (!taskId) return NextResponse.json({ error: 'taskId is required' }, { status: 400 });

    const doc = await db.collection('scraping_tasks').doc(taskId).get();
    if (!doc.exists) {
      return NextResponse.json({ success: true, note: 'already deleted' });
    }

    await db.collection('scraping_tasks').doc(taskId).delete();
    return NextResponse.json({ success: true, deletedId: taskId });
  } catch (err: any) {
    return NextResponse.json({ success: false, error: err.message });
  }
}
