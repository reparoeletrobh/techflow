'use strict';
// GROWTH GAMIFICADO — API v2 | acoes + registros diários

const GROWTH_KEY = 'reparoeletro_growth_v2';
const U=(process.env.UPSTASH_URL||'').replace(/['"]/g,'').trim();
const T=(process.env.UPSTASH_TOKEN||'').replace(/['"]/g,'').trim();

async function dbGet(k){
  try{const r=await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});const j=await r.json();const v=j[0]?.result;if(!v)return null;let x=JSON.parse(v);if(typeof x==='string')x=JSON.parse(x);return x;}catch(e){return null;}
}
async function dbSet(k,v){
  try{await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});}catch(e){console.error('[growth]',e.message);}
}

module.exports = async function(req, res) {
  res.setHeader('Access-Control-Allow-Origin','*');
  const action = req.query.action || '';

  if (action === 'carregar' || action === 'carregar-kanban') {
    const data = await dbGet(GROWTH_KEY);
    const d2 = data || {};
    return res.status(200).json({ ok:true, data: d2, acoes:d2.acoes||[], registros:d2.registros||{}, cols:d2.cols||[], vals:d2.vals||{}, influencers:d2.influencers||[] });
  }

  if ((action === 'salvar' || action === 'salvar-kanban') && req.method === 'POST') {
    const { acoes, registros, cols, vals, influencers } = req.body || {};
    await dbSet(GROWTH_KEY, { acoes: acoes||[], registros: registros||{}, cols: cols||[], vals: vals||{}, influencers: influencers||[], savedAt: new Date().toISOString() });
    return res.status(200).json({ ok: true });
  }

  if (action === 'carregar-influencers') {
    const db = await dbGet('growth_influencers') || { influencers: [] };
    return res.status(200).json({ ok: true, influencers: db.influencers || [] });
  }
  if (action === 'salvar-influencers' && req.method === 'POST') {
    const { influencers } = req.body || {};
    if (!Array.isArray(influencers)) return res.status(400).json({ ok: false, error: 'array obrigatorio' });
    await dbSet('growth_influencers', { influencers });
    return res.status(200).json({ ok: true, total: influencers.length });
  }
  return res.status(400).json({ ok: false, error: 'acao nao encontrada: '+action });
};
