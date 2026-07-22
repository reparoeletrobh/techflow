// api/cupons.js — Sistema de Cupons | Reparo Eletro BH
'use strict';
const U = (process.env.UPSTASH_URL   || '').replace(/['"]/g,'').trim();
const T = (process.env.UPSTASH_TOKEN || '').replace(/['"]/g,'').trim();
const CUPONS_KEY = 'reparoeletro_cupons';
const USOS_KEY   = 'reparoeletro_cupons_usos';

async function dbGet(k) {
  try {
    const r = await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});
    const j = await r.json(); const v = j[0]?.result;
    if (!v) return null;
    let x = JSON.parse(v); if (typeof x==='string') x=JSON.parse(x); return x;
  } catch(e) { return null; }
}
async function dbSet(k,v) {
  await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});
}

function gerarId() { return 'CUP-'+Date.now().toString(36).toUpperCase(); }
function gerarUsoId() { return 'USO-'+Date.now().toString(36).toUpperCase()+'-'+Math.random().toString(36).slice(2,5).toUpperCase(); }

export default async function handler(req, res) {
  // 🔐 TF-AUTH (Fase 1): chave obrigatória em toda chamada
  const _tfk = (req.query && req.query.k) || req.headers['x-tf-key'] || '';
  if (_tfk !== ((process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim())) {
    return res.status(401).json({ ok: false, error: 'não autorizado' });
  }

  res.setHeader('Access-Control-Allow-Origin','*');
  const action = req.query.action || '';
  const now = new Date().toISOString();

  // ── GET listar ──────────────────────────────────────────────────────────────
  if (action === 'listar') {
    const db = (await dbGet(CUPONS_KEY)) || { cupons: [] };
    const dbU = (await dbGet(USOS_KEY))  || { usos: [] };
    const usos = dbU.usos || [];
    const cupons = (db.cupons || []).map(function(c) {
      const meuUsos = usos.filter(function(u){ return u.cupomId === c.id; });
      return Object.assign({}, c, {
        usosRealizados: meuUsos.length,
        descontoTotal: meuUsos.reduce(function(s,u){ return s+(u.desconto||0); }, 0),
        valorMovimentado: meuUsos.reduce(function(s,u){ return s+(u.valorFinal||0); }, 0),
      });
    });
    return res.status(200).json({ ok:true, cupons });
  }

  // ── GET validar?codigo=XXX&valor=300 ────────────────────────────────────────
  if (action === 'validar') {
    const codigo = (req.query.codigo || '').toUpperCase().trim();
    const valorOriginal = parseFloat(req.query.valor||'0') || 0;
    if (!codigo) return res.status(400).json({ ok:false, error:'código obrigatório' });
    const db = (await dbGet(CUPONS_KEY)) || { cupons: [] };
    const dbU = (await dbGet(USOS_KEY))  || { usos: [] };
    const cupom = (db.cupons||[]).find(function(c){ return c.codigo === codigo; });
    if (!cupom) return res.status(200).json({ ok:false, valido:false, error:'Cupom não encontrado' });
    if (!cupom.ativo) return res.status(200).json({ ok:false, valido:false, error:'Cupom inativo' });
    const hoje = new Date().toISOString().slice(0,10);
    if (cupom.dataFim && hoje > cupom.dataFim) return res.status(200).json({ ok:false, valido:false, error:'Cupom expirado em '+cupom.dataFim });
    const usosCupom = (dbU.usos||[]).filter(function(u){ return u.cupomId === cupom.id; });
    if (cupom.usosMaximos && usosCupom.length >= cupom.usosMaximos)
      return res.status(200).json({ ok:false, valido:false, error:'Limite de usos atingido ('+cupom.usosMaximos+')' });
    const desconto = cupom.tipo === 'percentual'
      ? Math.round(valorOriginal * cupom.valor / 100 * 100) / 100
      : Math.min(cupom.valor, valorOriginal);
    const valorFinal = Math.max(0, valorOriginal - desconto);
    return res.status(200).json({
      ok:true, valido:true, cupom: {
        id: cupom.id, codigo: cupom.codigo,
        influencerNome: cupom.influencerNome,
        tipo: cupom.tipo, valor: cupom.valor,
      },
      desconto, valorFinal,
      msg: cupom.tipo==='percentual' ? cupom.valor+'% de desconto' : 'R$'+cupom.valor.toFixed(2)+' de desconto',
    });
  }

  // ── POST criar ──────────────────────────────────────────────────────────────
  if (req.method === 'POST' && action === 'criar') {
    const b = req.body || {};
    const codigo = (b.codigo || '').toUpperCase().replace(/\s/g,'');
    if (!codigo) return res.status(400).json({ ok:false, error:'código obrigatório' });
    if (!b.tipo || !b.valor) return res.status(400).json({ ok:false, error:'tipo e valor obrigatórios' });
    const db = (await dbGet(CUPONS_KEY)) || { cupons: [] };
    if ((db.cupons||[]).find(function(c){ return c.codigo === codigo; }))
      return res.status(400).json({ ok:false, error:'Código já existe: '+codigo });
    const novo = {
      id:              gerarId(),
      codigo,
      influencerId:    b.influencerId   || null,
      influencerNome:  b.influencerNome || '',
      tipo:            b.tipo,             // 'percentual' | 'fixo'
      valor:           parseFloat(b.valor) || 0,
      ativo:           true,
      dataInicio:      b.dataInicio || now.slice(0,10),
      dataFim:         b.dataFim    || null,
      usosMaximos:     b.usosMaximos ? parseInt(b.usosMaximos) : null,
      obs:             b.obs || '',
      criadoEm:        now,
    };
    db.cupons = [novo].concat(db.cupons || []);
    await dbSet(CUPONS_KEY, db);
    return res.status(200).json({ ok:true, cupom: novo });
  }

  // ── POST usar ───────────────────────────────────────────────────────────────
  if (req.method === 'POST' && action === 'usar') {
    const b = req.body || {};
    const codigo = (b.codigo || '').toUpperCase().trim();
    if (!codigo) return res.status(400).json({ ok:false, error:'código obrigatório' });
    const db   = (await dbGet(CUPONS_KEY)) || { cupons: [] };
    const dbU  = (await dbGet(USOS_KEY))   || { usos: [] };
    const cupom = (db.cupons||[]).find(function(c){ return c.codigo === codigo; });
    if (!cupom || !cupom.ativo) return res.status(400).json({ ok:false, error:'Cupom inválido ou inativo' });
    const valorOriginal = parseFloat(b.valorOriginal || 0) || 0;
    const desconto = cupom.tipo === 'percentual'
      ? Math.round(valorOriginal * cupom.valor / 100 * 100) / 100
      : Math.min(cupom.valor, valorOriginal);
    const valorFinal = Math.max(0, valorOriginal - desconto);
    const uso = {
      id:             gerarUsoId(),
      cupomId:        cupom.id,
      cupomCodigo:    cupom.codigo,
      influencerId:   cupom.influencerId,
      influencerNome: cupom.influencerNome,
      origem:         b.origem   || 'manual',
      fichaId:        b.fichaId  || '',
      fichaCliente:   b.cliente  || '',
      valorOriginal,
      desconto,
      valorFinal,
      usadoEm:        now,
    };
    dbU.usos = [uso].concat(dbU.usos || []);
    if (dbU.usos.length > 5000) dbU.usos = dbU.usos.slice(0, 5000);
    await dbSet(USOS_KEY, dbU);
    return res.status(200).json({ ok:true, uso, desconto, valorFinal });
  }

  // ── POST editar ─────────────────────────────────────────────────────────────
  if (req.method === 'POST' && action === 'editar') {
    const b = req.body || {};
    const db = (await dbGet(CUPONS_KEY)) || { cupons: [] };
    const idx = (db.cupons||[]).findIndex(function(c){ return c.id === b.id; });
    if (idx < 0) return res.status(404).json({ ok:false, error:'Cupom não encontrado' });
    const c = db.cupons[idx];
    if (b.influencerId !== undefined) c.influencerId = b.influencerId;
    if (b.influencerNome !== undefined) c.influencerNome = b.influencerNome;
    if (b.tipo !== undefined) c.tipo = b.tipo;
    if (b.valor !== undefined) c.valor = parseFloat(b.valor) || 0;
    if (b.dataFim !== undefined) c.dataFim = b.dataFim || null;
    if (b.usosMaximos !== undefined) c.usosMaximos = b.usosMaximos ? parseInt(b.usosMaximos) : null;
    if (b.ativo !== undefined) c.ativo = !!b.ativo;
    if (b.obs !== undefined) c.obs = b.obs;
    c.editadoEm = now;
    await dbSet(CUPONS_KEY, db);
    return res.status(200).json({ ok:true, cupom: c });
  }

  // ── GET relatorio ───────────────────────────────────────────────────────────
  if (action === 'relatorio') {
    const dbU = (await dbGet(USOS_KEY)) || { usos: [] };
    const usos = dbU.usos || [];
    const de  = req.query.de  || new Date(Date.now()-30*864e5).toISOString().slice(0,10);
    const ate = req.query.ate || new Date().toISOString().slice(0,10);
    const filtInfluencer = req.query.influencer || '';

    const filtrados = usos.filter(function(u) {
      const dt = (u.usadoEm||'').slice(0,10);
      if (dt < de || dt > ate) return false;
      if (filtInfluencer && u.influencerNome !== filtInfluencer) return false;
      return true;
    });

    // Agrupar por influenciadora
    const porInfluencer = {};
    filtrados.forEach(function(u) {
      const k = u.influencerNome || 'Sem influenciadora';
      if (!porInfluencer[k]) porInfluencer[k] = { nome:k, usos:0, descontoTotal:0, valorMovimentado:0, cupons:{} };
      porInfluencer[k].usos++;
      porInfluencer[k].descontoTotal   += u.desconto    || 0;
      porInfluencer[k].valorMovimentado += u.valorFinal  || 0;
      porInfluencer[k].cupons[u.cupomCodigo] = (porInfluencer[k].cupons[u.cupomCodigo]||0)+1;
    });

    // Agrupar por dia
    const porDia = {};
    filtrados.forEach(function(u) {
      const d = (u.usadoEm||'').slice(0,10);
      if (!porDia[d]) porDia[d] = { data:d, usos:0, descontoTotal:0, valorMovimentado:0 };
      porDia[d].usos++;
      porDia[d].descontoTotal   += u.desconto   || 0;
      porDia[d].valorMovimentado += u.valorFinal || 0;
    });

    const totais = {
      usos:             filtrados.length,
      descontoTotal:    filtrados.reduce(function(s,u){ return s+(u.desconto||0); }, 0),
      valorMovimentado: filtrados.reduce(function(s,u){ return s+(u.valorFinal||0); }, 0),
    };

    return res.status(200).json({
      ok: true, de, ate,
      totais,
      porInfluencer: Object.values(porInfluencer).sort(function(a,b){ return b.usos-a.usos; }),
      porDia: Object.values(porDia).sort(function(a,b){ return a.data.localeCompare(b.data); }),
      usos: filtrados.slice(0, 200),
    });
  }

  // ── GET listar-usos ─────────────────────────────────────────────────────────
  if (action === 'listar-usos') {
    const dbU = (await dbGet(USOS_KEY)) || { usos: [] };
    return res.status(200).json({ ok:true, usos: (dbU.usos||[]).slice(0,200) });
  }

  return res.status(404).json({ ok:false, error:'ação não encontrada' });
}
