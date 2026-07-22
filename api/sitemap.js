// api/sitemap.js — Sitemap dinâmico XML
export default async function handler(req, res) {
  // 🔐 TF-AUTH (Fase 1): chave obrigatória em toda chamada
  const _tfk = (req.query && req.query.k) || req.headers['x-tf-key'] || '';
  if (_tfk !== ((process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim())) {
    return res.status(401).json({ ok: false, error: 'não autorizado' });
  }

  const U=(process.env.UPSTASH_URL||'').replace(/['"]/g,'').trim();
  const T=(process.env.UPSTASH_TOKEN||'').replace(/['"]/g,'').trim();
  async function dbGet(k){try{const r=await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});const j=await r.json();let v=j[0]?.result;if(!v)return null;try{let x=JSON.parse(v);if(typeof x==='string')x=JSON.parse(x);return x;}catch(e){return null;}}catch(e){return null;}}

  const BASE = 'https://reparoeletroadm.com';
  const db = (await dbGet('blog_posts')) || { posts:[] };
  const posts = (db.posts||[]).filter(p=>p.status==='publicado');
  const now = new Date().toISOString().split('T')[0];

  const urls = [
    `<url><loc>${BASE}/</loc><changefreq>weekly</changefreq><priority>0.8</priority></url>`,
    `<url><loc>${BASE}/blog</loc><changefreq>daily</changefreq><priority>1.0</priority></url>`,
    `<url><loc>${BASE}/geladeira</loc><changefreq>monthly</changefreq><priority>0.9</priority></url>`,
    `<url><loc>${BASE}/lava-e-seca</loc><changefreq>monthly</changefreq><priority>0.9</priority></url>`,
    ...posts.map(p => `<url>
  <loc>${BASE}/blog/${p.slug}</loc>
  <lastmod>${(p.publicadoEm||now).split('T')[0]}</lastmod>
  <changefreq>monthly</changefreq>
  <priority>0.8</priority>
</url>`)
  ];

  const xml = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"
        xmlns:news="http://www.google.com/schemas/sitemap-news/0.9">
${urls.join('\n')}
</urlset>`;

  res.setHeader('Content-Type','application/xml');
  res.setHeader('Cache-Control','public, max-age=3600, s-maxage=3600');
  return res.status(200).send(xml);
}
