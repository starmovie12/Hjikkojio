import { NextRequest, NextResponse } from 'next/server';
import { extractMovieLinks } from '@/lib/solvers';

export const dynamic = 'force-dynamic';

export async function POST(req: NextRequest) {
  const body = await req.json().catch(() => ({}));
  const url  = body?.url;
  if (!url) return NextResponse.json({ error: 'URL is required' }, { status: 400 });
  const result = await extractMovieLinks(url);
  return NextResponse.json(result);
}
