// api/tv-pecas.js — Catálogo de Peças TV | Reparo Eletro BH
'use strict';
const U = (process.env.UPSTASH_URL   || '').replace(/['"]/g,'').trim();
const T = (process.env.UPSTASH_TOKEN || '').replace(/['"]/g,'').trim();
const DB_KEY = 'tv_pecas';

async function dbGet(k) {
  try {
    const r = await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});
    const j = await r.json(); let v = j[0]?.result;
    if (!v) return null;
    if (typeof v==='string') v=JSON.parse(v);
    if (typeof v==='string') v=JSON.parse(v);
    return v;
  } catch(e){ return null; }
}
async function dbSet(k,v) {
  await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});
}
function gid(prefix){ return (prefix||'PCA')+'-'+Date.now().toString(36).toUpperCase()+'-'+Math.random().toString(36).slice(2,5).toUpperCase(); }

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin','*');
  const action = req.query.action || '';
  const now    = new Date().toISOString();

  // ── GET listar ──────────────────────────────────────────────────────────────
  if (action === 'listar') {
    const db = (await dbGet(DB_KEY)) || { pecas:[], usos:[] };
    const q  = (req.query.q||'').toLowerCase().trim();
    let pecas = db.pecas || [];
    if (q) {
      pecas = pecas.filter(function(p){
        return (p.nomePeca||'').toLowerCase().includes(q) ||
               (p.modelo||'').toLowerCase().includes(q) ||
               (p.codigo||'').toLowerCase().includes(q) ||
               (p.tipo||'').toLowerCase().includes(q) ||
               (p.marca||'').toLowerCase().includes(q);
      });
    }
    // Calcular qty usada por peça
    const usos = db.usos || [];
    pecas = pecas.map(function(p){
      const usosP = usos.filter(function(u){ return u.pecaId===p.id; });
      const qtdUsada = usosP.reduce(function(s,u){ return s+(u.qtdUsada||1); }, 0);
      return Object.assign({}, p, { qtdUsada, usoCount: usosP.length });
    });
    return res.status(200).json({ ok:true, pecas, total:pecas.length });
  }

  // ── POST cadastrar ──────────────────────────────────────────────────────────
  if (req.method==='POST' && action==='cadastrar') {
    const b = req.body || {};
    if (!b.nomePeca) return res.status(400).json({ok:false,error:'nomePeca obrigatório'});
    const db = (await dbGet(DB_KEY)) || { pecas:[], usos:[] };
    const nova = {
      id:        gid('PCA'),
      nomePeca:  b.nomePeca.trim(),
      modelo:    b.modelo   || '',
      marca:     b.marca    || '',
      codigo:    b.codigo   || '',
      tipo:      b.tipo     || '',
      qtd:       parseInt(b.qtd)||1,
      qtdMin:    parseInt(b.qtdMin)||1,
      obs:       b.obs      || '',
      status:    'disponivel',
      criadoEm:  now,
    };
    db.pecas.unshift(nova);
    await dbSet(DB_KEY, db);
    return res.status(200).json({ ok:true, peca:nova });
  }

  // ── POST editar ─────────────────────────────────────────────────────────────
  if (req.method==='POST' && action==='editar') {
    const b = req.body || {};
    const db = (await dbGet(DB_KEY)) || { pecas:[], usos:[] };
    const idx = (db.pecas||[]).findIndex(function(p){ return p.id===b.id; });
    if (idx<0) return res.status(404).json({ok:false,error:'Peça não encontrada'});
    const p = db.pecas[idx];
    ['nomePeca','modelo','marca','codigo','tipo','obs'].forEach(function(f){ if(b[f]!==undefined) p[f]=b[f]; });
    if (b.qtd!==undefined)    p.qtd    = parseInt(b.qtd)||0;
    if (b.qtdMin!==undefined) p.qtdMin = parseInt(b.qtdMin)||1;
    if (b.status!==undefined) p.status = b.status;
    p.editadoEm = now;
    await dbSet(DB_KEY, db);
    return res.status(200).json({ ok:true, peca:p });
  }

  // ── POST dar-baixa ──────────────────────────────────────────────────────────
  if (req.method==='POST' && action==='dar-baixa') {
    const b = req.body || {};
    if (!b.pecaId) return res.status(400).json({ok:false,error:'pecaId obrigatório'});
    const db = (await dbGet(DB_KEY)) || { pecas:[], usos:[] };
    const peca = (db.pecas||[]).find(function(p){ return p.id===b.pecaId; });
    if (!peca) return res.status(404).json({ok:false,error:'Peça não encontrada'});
    const qtdUsada = parseInt(b.qtdUsada)||1;
    const qtdDisp = (peca.qtd||0) - (peca.qtdUsada||0);
    if (qtdUsada > qtdDisp) return res.status(400).json({ok:false,error:'Quantidade insuficiente em estoque (disponível: '+qtdDisp+')'});
    const uso = {
      id:         gid('USO'),
      pecaId:     peca.id,
      nomePeca:   peca.nomePeca,
      modelo:     peca.modelo,
      codigo:     peca.codigo,
      osId:       b.osId      || '',
      osCliente:  b.osCliente || '',
      qtdUsada,
      tecnico:    b.tecnico   || '',
      obs:        b.obs       || '',
      usadoEm:    now,
    };
    if (!db.usos) db.usos = [];
    db.usos.unshift(uso);
    // Descontar do estoque
    peca.qtd = Math.max(0, (peca.qtd||0) - qtdUsada);
    if (peca.qtd === 0) peca.status = 'esgotado';
    await dbSet(DB_KEY, db);
    return res.status(200).json({ ok:true, uso, qtdRestante: peca.qtd });
  }

  // ── POST repor ──────────────────────────────────────────────────────────────
  if (req.method==='POST' && action==='repor') {
    const b = req.body || {};
    const db = (await dbGet(DB_KEY)) || { pecas:[], usos:[] };
    const peca = (db.pecas||[]).find(function(p){ return p.id===b.pecaId; });
    if (!peca) return res.status(404).json({ok:false,error:'Peça não encontrada'});
    peca.qtd = (peca.qtd||0) + (parseInt(b.qtd)||1);
    peca.status = 'disponivel';
    peca.editadoEm = now;
    await dbSet(DB_KEY, db);
    return res.status(200).json({ ok:true, peca });
  }

  // ── GET historico ───────────────────────────────────────────────────────────
  if (action === 'historico') {
    const db = (await dbGet(DB_KEY)) || { pecas:[], usos:[] };
    const q  = (req.query.q||'').toLowerCase().trim();
    let usos = db.usos || [];
    if (q) {
      usos = usos.filter(function(u){
        return (u.nomePeca||'').toLowerCase().includes(q) ||
               (u.osId||'').toLowerCase().includes(q) ||
               (u.osCliente||'').toLowerCase().includes(q) ||
               (u.tecnico||'').toLowerCase().includes(q);
      });
    }
    return res.status(200).json({ ok:true, usos: usos.slice(0,200) });
  }

  // ── GET alertas — peças abaixo do mínimo ────────────────────────────────────
  if (action === 'alertas') {
    const db = (await dbGet(DB_KEY)) || { pecas:[], usos:[] };
    const alertas = (db.pecas||[]).filter(function(p){ return (p.qtd||0) <= (p.qtdMin||1); });
    return res.status(200).json({ ok:true, alertas, total:alertas.length });
  }

  return res.status(404).json({ok:false,error:'ação não encontrada'});
}
