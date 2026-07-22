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
  // 🔐 TF-AUTH (Fase 1): chave obrigatória em toda chamada
  const _tfk = (req.query && req.query.k) || req.headers['x-tf-key'] || '';
  if (_tfk !== ((process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim())) {
    return res.status(401).json({ ok: false, error: 'não autorizado' });
  }

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
  // ── GET historico — buscar semanas anteriores ──────────────────────────────
  if (action === 'historico') {
    const hist = await dbGet('reparoeletro_growth_hist') || { semanas: [] };
    return res.status(200).json({ ok: true, semanas: hist.semanas || [] });
  }

  // ── POST salvar-snapshot — salvar snapshot da semana atual ──────────────────
  if (action === 'salvar-snapshot' && req.method === 'POST') {
    const { vals, cols, semanaInicio, semanaFim, label } = req.body || {};
    const hist = await dbGet('reparoeletro_growth_hist') || { semanas: [] };
    // Não duplicar a mesma semana
    const exists = hist.semanas.find(s => s.semanaInicio === semanaInicio);
    if (!exists) {
      hist.semanas.unshift({
        semanaInicio, semanaFim,
        label: label || semanaInicio,
        vals: vals || {}, cols: cols || [],
        savedAt: new Date().toISOString()
      });
      hist.semanas = hist.semanas.slice(0, 52); // guardar até 52 semanas
      await dbSet('reparoeletro_growth_hist', hist);
    }
    return res.status(200).json({ ok: true, total: hist.semanas.length });
  }

  // ── POST auto-reset — verificar e executar resets automáticos ──────────────
  if (action === 'auto-reset' && req.method === 'POST') {
    const { hoje, semanaAtual, vals, cols, lastDailyReset, lastWeeklyReset } = req.body || {};
    const result = { resetDiario: false, resetSemanal: false };

    // Reset diário
    if (lastDailyReset !== hoje) result.resetDiario = true;

    // Reset semanal — salva snapshot se semana mudou
    if (lastWeeklyReset !== semanaAtual) {
      result.resetSemanal = true;
      if (lastWeeklyReset && vals && cols) {
        const hist = await dbGet('reparoeletro_growth_hist') || { semanas: [] };
        const exists = hist.semanas.find(s => s.semanaInicio === lastWeeklyReset);
        if (!exists) {
          hist.semanas.unshift({
            semanaInicio: lastWeeklyReset,
            label: 'Semana de ' + lastWeeklyReset,
            vals, cols, savedAt: new Date().toISOString()
          });
          hist.semanas = hist.semanas.slice(0, 52);
          await dbSet('reparoeletro_growth_hist', hist);
        }
      }
    }
    return res.status(200).json({ ok: true, ...result });
  }

    return res.status(400).json({ ok: false, error: 'acao nao encontrada: '+action });
};
