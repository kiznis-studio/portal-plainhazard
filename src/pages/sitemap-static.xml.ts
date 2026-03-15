import type { APIRoute } from 'astro';

export const GET: APIRoute = async () => {
  const pages = [
    '',
    '/states',
    '/counties',
    '/hazards',
    '/disaster-types',
    '/rankings/most-disasters',
    '/rankings/most-damage',
    '/rankings/deadliest',
    '/privacy',
    '/about',
    '/guides',
    '/guides/understanding-disaster-data',
    '/guides/natural-hazard-risk-by-region',
    '/guides/preparing-for-natural-disasters',
    '/guides/most-disaster-prone-counties',
    '/guides/climate-change-and-increasing-disasters',
  ];
  const xml = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${pages.map(p => `  <url><loc>https://plainhazard.com${p}</loc><changefreq>weekly</changefreq><priority>${p === '' ? '1.0' : '0.8'}</priority></url>`).join('\n')}
</urlset>`;
  return new Response(xml, { headers: { 'Content-Type': 'application/xml', 'Cache-Control': 'public, max-age=86400' } });
};
