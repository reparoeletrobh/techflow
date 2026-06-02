'use strict';
// GROWTH — API | Canais de aquisição + leads + métricas

const GROWTH_KEY = 'reparoeletro_growth';
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

  if (action === 'carregar') {
    const data = await dbGet(GROWTH_KEY);
    return res.status(200).json({ ok: true, data: data || { canais:[], leads:[] } });
  }

  if (action === 'salvar' && req.method === 'POST') {
    const { canais, leads } = req.body || {};
    const now = new Date().toISOString();
    await dbSet(GROWTH_KEY, { canais: canais||[], leads: leads||[], updatedAt: now });
    return res.status(200).json({ ok: true });
  }

  return res.status(400).json({ ok: false, error: 'acao nao encontrada: '+action });
};
