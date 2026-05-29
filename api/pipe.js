// api/pipe.js — Pipeline ADM
const UPSTASH_URL   = (process.env.UPSTASH_URL   || '').replace(/['"]/g,'').trim();
const UPSTASH_TOKEN = (process.env.UPSTASH_TOKEN  || '').replace(/['"]/g,'').trim();
const PIPEFY_TOKEN  = (process.env.PIPEFY_TOKEN   || '').replace(/['"]/g,'').trim();
const PIPE_KEY      = 'reparoeletro_pipe';

const PHASES = [
  { id:'aguardando_aprovacao', name:'Aguardando Aprovação', cor:'#f5c800' },
  { id:'aprovados',            name:'Aprovados',            cor:'#22c55e' },
  { id:'video_enviado',        name:'Vídeo Enviado',        cor:'#a855f7' },
  { id:'analise_compra',       name:'Análise de Compra',    cor:'#3b9eff' },
  { id:'equipamento_comprado', name:'Equipamento Comprado', cor:'#3b9eff' },
  { id:'programar_entrega',    name:'Programar Entrega',    cor:'#f5c800' },
  { id:'solicitar_entrega',    name:'Solicitar Entrega',    cor:'#f97316' },
  { id:'entrega_solicitada',   name:'Entrega Solicitada',   cor:'#f97316' },
  { id:'receber',              name:'Receber',              cor:'#22c55e' },
  { id:'erp',                  name:'ERP',                  cor:'#22c55e' },
];

async function dbGet(k) {
  try {
    const r = await fetch(UPSTASH_URL + '/pipeline', {
      method: 'POST',
      headers: { Authorization: 'Bearer ' + UPSTASH_TOKEN, 'Content-Type': 'application/json' },
      body: JSON.stringify([['GET', k]])
    });
    const j = await r.json();
    const result = j[0]?.result;
    if (!result) return null;
    let val = JSON.parse(result);
    // Tolerância a dupla codificação de versões anteriores
    if (typeof val === 'string') { try { val = JSON.parse(val); } catch(e2) {} }
    return (val && typeof val === 'object') ? val : null;
  } catch(e) { return null; }
}

async function dbSet(k, v) {
  try {
    await fetch(UPSTASH_URL + '/pipeline', {
      method: 'POST',
      headers: { Authorization: 'Bearer ' + UPSTASH_TOKEN, 'Content-Type': 'application/json' },
      body: JSON.stringify([['SET', k, JSON.stringify(v)]])
    });
  } catch(e) { console.error('dbSet error:', e.message); }
}

async function pipefyReq(query) {
  const ctrl = new AbortController();
  const tid  = setTimeout(function() { ctrl.abort(); }, 15000);
  try {
    const r = await fetch('https://api.pipefy.com/graphql', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + PIPEFY_TOKEN },
      body:    JSON.stringify({ query: query }),
      signal:  ctrl.signal
    });
    const j = await r.json();
    clearTimeout(tid);
    if (j.errors && j.errors.length) throw new Error(j.errors[0].message);
    return j.data;
  } catch(e) {
    clearTimeout(tid);
    throw e;
  }
}

function defaultDB() {
  return { cards: [], syncedPipefyIds: [], lastSync: null };
}


// Função standalone para sync de uma fase — fora do handler para evitar conflito de escopo
async function syncFase(pipefyPhaseId, phaseLocal, PIPE_KEY, dbGetFn, dbSetFn, pipefyReqFn) {
  var db = await dbGetFn(PIPE_KEY);
  if (!db) db = { cards: [], syncedPipefyIds: [], lastSync: null };
  if (!Array.isArray(db.cards)) db.cards = [];
  if (!Array.isArray(db.syncedPipefyIds)) db.syncedPipefyIds = [];

  var existIds = {};
  for (var ci = 0; ci < db.cards.length; ci++) {
    if (db.cards[ci].pipefyId) existIds[db.cards[ci].pipefyId] = true;
  }

  var added = 0, skipped = 0, cursor = null, hasMore = true, paginas = 0, erros = [];

  while (hasMore && paginas < 20) {
    paginas++;
    var ca = cursor ? (', after: "' + cursor + '"') : '';
    var q  = 'query { phase(id: "' + pipefyPhaseId + '") { cards(first: 50' + ca + ') { pageInfo { hasNextPage endCursor } edges { node { id title fields { name value } } } } } }';

    var data = null;
    try { data = await pipefyReqFn(q); }
    catch(e2) { erros.push(String(e2.message)); hasMore = false; break; }

    var ph      = data && data.phase ? data.phase : null;
    var phCards = ph && ph.cards ? ph.cards : null;
    var edges   = phCards && phCards.edges ? phCards.edges : [];
    var pgInfo  = phCards && phCards.pageInfo ? phCards.pageInfo : {};

    for (var ei = 0; ei < edges.length; ei++) {
      var nd  = edges[ei].node;
      var pid = String(nd.id);
      if (existIds[pid]) { skipped++; continue; }

      var flds = nd.fields || [];
      var nome = '', tel = '', equip = '';
      for (var fi = 0; fi < flds.length; fi++) {
        var fn = (flds[fi].name || '').toLowerCase();
        var fv = flds[fi].value || '';
        if (!nome  && fn.indexOf('nome')     !== -1) nome  = fv;
        if (!tel   && fn.indexOf('telefone') !== -1) tel   = fv;
        if (!tel   && fn.indexOf('fone')     !== -1) tel   = fv;
        if (!equip && fn.indexOf('descri')   !== -1) equip = fv;
        if (!equip && fn.indexOf('equip')    !== -1) equip = fv;
      }

      var ts = new Date().toISOString();
      db.cards.push({
        id:              'PIPE-' + String(db.cards.length + 1).padStart(4, '0'),
        pipefyId:        pid,
        phase:           phaseLocal,
        nomeContato:     nome || nd.title || '',
        telefone:        tel,
        equipamento:     equip,
        descricao:       nd.title || '',
        valor:           0,
        origem:          'pipefy',
        criadoEm:        ts,
        movedAt:         ts,
        aguardandoDesde: phaseLocal === 'aguardando_aprovacao' ? ts : null,
        history:         [],
        analiseCompra:   false
      });
      db.syncedPipefyIds.push(pid);
      existIds[pid] = true;
      added++;
    }

    hasMore = pgInfo.hasNextPage ? true : false;
    cursor  = pgInfo.endCursor  || null;
  }

  db.lastSync = new Date().toISOString();
  await dbSetFn(PIPE_KEY, db);
  return { added: added, skipped: skipped, total: db.cards.length, paginas: paginas, erros: erros };
}


export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const action = req.query.action || '';


  // ── POST reset-pipe: limpa todos os dados do pipe (dados corrompidos) ──────
  if (action === 'reset-pipe') {
    const fresh = { cards: [], syncedPipefyIds: [], lastSync: null };
    await dbSet(PIPE_KEY, fresh);
    return res.status(200).json({ ok: true, info: 'pipe resetado — pronto para nova sincronização' });
  }

  // ── status ────────────────────────────────────────────────────────────────
  if (action === 'status') {
    var db = (await dbGet(PIPE_KEY)) || defaultDB();
    var cards = db.cards || [];
    var porFase = {};
    PHASES.forEach(function(ph) { porFase[ph.name] = 0; });
    cards.forEach(function(c) {
      var ph = PHASES.find(function(p) { return p.id === c.phase; });
      if (ph) porFase[ph.name] = (porFase[ph.name] || 0) + 1;
    });
    return res.status(200).json({
      ok: true, total: cards.length, lastSync: db.lastSync,
      porFase: porFase,
      amostra: cards.slice(0,3).map(function(c) {
        return { id: c.id, nome: c.nomeContato, fase: c.phase, pipefyId: c.pipefyId };
      })
    });
  }

  // ── load ──────────────────────────────────────────────────────────────────
  if (action === 'load') {
    var db = (await dbGet(PIPE_KEY)) || defaultDB();
    return res.status(200).json({ ok: true, cards: db.cards || [], phases: PHASES, lastSync: db.lastSync });
  }

  // ── sync-fase: sincroniza UMA fase por vez ─────────────────────────────────
  if (action === 'sync-fase') {
    const sfPhaseId = req.query.phaseId;
    const sfLocal   = req.query.phaseLocal;
    if (!sfPhaseId || !sfLocal) {
      return res.status(400).json({ ok: false, error: 'phaseId e phaseLocal obrigatorios' });
    }
    try {
      const sfResult = await syncFase(sfPhaseId, sfLocal, PIPE_KEY, dbGet, dbSet, pipefyReq);
      return res.status(200).json({ ok: true, fase: sfLocal, ...sfResult });
    } catch(sfErr) {
      return res.status(500).json({ ok: false, error: String(sfErr.message), stack: String(sfErr.stack).substring(0,300) });
    }
  }
  // ── mover ─────────────────────────────────────────────────────────────────
  if (req.method === 'POST' && action === 'mover') {
    var body  = req.body || {};
    var id    = body.id;
    var phase = body.phase;
    if (!id || !phase) return res.status(400).json({ ok: false, error: 'id e phase obrigatorios' });
    var phOk = PHASES.find(function(p) { return p.id === phase; });
    if (!phOk) return res.status(400).json({ ok: false, error: 'fase invalida' });
    var db   = (await dbGet(PIPE_KEY)) || defaultDB();
    var card = (db.cards || []).find(function(c) { return c.id === id; });
    if (!card) return res.status(404).json({ ok: false, error: 'nao encontrado' });
    var now = new Date().toISOString();
    card.history = (card.history || []).concat([{ phase: card.phase, ts: now }]);
    card.phase   = phase;
    card.movedAt = now;
    await dbSet(PIPE_KEY, db);
    return res.status(200).json({ ok: true, card: card });
  }

  // ── add-card ──────────────────────────────────────────────────────────────
  if (req.method === 'POST' && action === 'add-card') {
    var body = req.body || {};
    if (!body.nomeContato) return res.status(400).json({ ok: false, error: 'nomeContato obrigatorio' });
    var db = (await dbGet(PIPE_KEY)) || defaultDB();
    if (!Array.isArray(db.cards)) db.cards = [];
    if (body.pipefyId && db.cards.find(function(c) { return c.pipefyId === String(body.pipefyId); }))
      return res.status(200).json({ ok: true, info: 'ja existe' });
    var now  = new Date().toISOString();
    var ph   = body.phase || 'aguardando_aprovacao';
    var card = {
      id: 'PIPE-' + String(db.cards.length + 1).padStart(4, '0'),
      pipefyId:        body.pipefyId ? String(body.pipefyId) : null,
      phase:           ph,
      nomeContato:     body.nomeContato || '',
      telefone:        body.telefone    || '',
      equipamento:     body.equipamento || '',
      descricao:       body.descricao   || '',
      valor:           parseFloat(body.valor) || 0,
      origem:          body.origem || 'manual',
      criadoEm:        now, movedAt: now,
      aguardandoDesde: ph === 'aguardando_aprovacao' ? now : null,
      history: [], analiseCompra: false
    };
    db.cards.unshift(card);
    await dbSet(PIPE_KEY, db);
    return res.status(200).json({ ok: true, card: card });
  }

  // ── toggle-analise-compra ─────────────────────────────────────────────────
  if (req.method === 'POST' && action === 'toggle-analise-compra') {
    var body = req.body || {};
    var db   = (await dbGet(PIPE_KEY)) || defaultDB();
    var card = (db.cards || []).find(function(c) { return c.id === body.id; });
    if (!card) return res.status(404).json({ ok: false, error: 'nao encontrado' });
    card.analiseCompra = !card.analiseCompra;
    await dbSet(PIPE_KEY, db);
    return res.status(200).json({ ok: true, analiseCompra: card.analiseCompra });
  }

  // ── excluir ───────────────────────────────────────────────────────────────
  if (req.method === 'POST' && action === 'excluir') {
    var body = req.body || {};
    var db   = (await dbGet(PIPE_KEY)) || defaultDB();
    db.cards = (db.cards || []).filter(function(c) { return c.id !== body.id; });
    await dbSet(PIPE_KEY, db);
    return res.status(200).json({ ok: true });
  }

  return res.status(404).json({ ok: false, error: 'acao nao encontrada: ' + action });
}
