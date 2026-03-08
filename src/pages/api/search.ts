import type { APIRoute } from 'astro';
import { search } from '../../lib/db';

export const prerender = false;

export const GET: APIRoute = async ({ url, locals }) => {
  const query = url.searchParams.get('q')?.trim();
  if (!query || query.length < 2) {
    return new Response(JSON.stringify({ error: 'Query must be at least 2 characters' }), {
      status: 400,
      headers: { 'Content-Type': 'application/json' },
    });
  }

  const db = (locals as any).runtime?.env?.DB;
  if (!db) {
    return new Response(JSON.stringify({ error: 'Database unavailable' }), {
      status: 503,
      headers: { 'Content-Type': 'application/json' },
    });
  }

  const results = await search(db, query, 10);

  return new Response(JSON.stringify(results), {
    headers: {
      'Content-Type': 'application/json',
      'Cache-Control': 'public, max-age=300',
    },
  });
};
