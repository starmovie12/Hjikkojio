import { NextRequest, NextResponse } from 'next/server';
import { db } from '@/lib/firebaseAdmin';

export const dynamic = 'force-dynamic';

// ─── GET /api/auto-process/queue ──────────────────────────────────────────────
export async function GET(req: NextRequest) {
  try {
    const { searchParams } = new URL(req.url);
    const type           = searchParams.get('type') || 'all';           // 'movies' | 'webseries' | 'all'
    const includeActive  = searchParams.get('include_active') === 'true'; // default false

    const fetchQueue = async (col: string, label: 'movie' | 'webseries') => {
      let query: FirebaseFirestore.Query;
      if (includeActive) {
        query = db.collection(col).orderBy('createdAt', 'desc').limit(100);
      } else {
        query = db.collection(col).where('status', '==', 'pending');
      }
      const snap = await query.get();
      return snap.docs.map(doc => ({
        id:         doc.id,
        collection: col,
        type:       label,
        ...doc.data(),
      }));
    };

    let items: any[] = [];

    if (type === 'movies') {
      items = await fetchQueue('movies_queue', 'movie');
    } else if (type === 'webseries') {
      items = await fetchQueue('webseries_queue', 'webseries');
    } else {
      const [movies, webseries] = await Promise.all([
        fetchQueue('movies_queue', 'movie'),
        fetchQueue('webseries_queue', 'webseries'),
      ]);
      items = [...movies, ...webseries];
    }

    return NextResponse.json({ status: 'success', total: items.length, items });
  } catch (err: any) {
    return NextResponse.json({ error: err.message }, { status: 500 });
  }
}

// ─── PATCH /api/auto-process/queue ────────────────────────────────────────────
export async function PATCH(req: NextRequest) {
  try {
    const body = await req.json();
    const { id, collection: col, status, error } = body;

    if (!id || !col || !status) {
      return NextResponse.json(
        { error: 'id, collection, and status are required' },
        { status: 400 },
      );
    }

    const updateData: any = { status, updatedAt: new Date().toISOString() };
    if (error !== undefined) updateData.error = error;

    await db.collection(col).doc(id).update(updateData);

    return NextResponse.json({ status: 'success', id, newStatus: status });
  } catch (err: any) {
    return NextResponse.json({ error: err.message }, { status: 500 });
  }
}
