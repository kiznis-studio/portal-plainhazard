import type { APIRoute } from 'astro';

export const GET: APIRoute = async () => {
  const sitemaps = [
    'sitemap-static.xml',
    'sitemap-states.xml',
    'sitemap-counties.xml',
    'sitemap-hazards.xml',
    'sitemap-disaster-types.xml',
  ];

  const xml = `<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${sitemaps.map(s => `  <sitemap><loc>https://plainhazard.com/${s}</loc></sitemap>`).join('\n')}
</sitemapindex>`;

  return new Response(xml, {
    headers: { 'Content-Type': 'application/xml', 'Cache-Control': 'public, max-age=86400' },
  });
};
