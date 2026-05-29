// api/pipe.js — Pipeline ADM
const UPSTASH_URL   = (process.env.UPSTASH_URL   || '').replace(/['"]/g,'').trim();
const UPSTASH_TOKEN = (process.env.UPSTASH_TOKEN  || '').replace(/['"]/g,'').trim();
const PIPEFY_TOKEN  = (process.env.PIPEFY_TOKEN   || '').replace(/['"]/g,'').trim();
const PIPE_KEY      = 'reparoeletro_pipe';

const PHASES = [
  { id:'aguardando_aprovacao', name:'Aguardando Aprovação', cor:'#f5c800' },
  { id:'ultima_chamada',       name:'Última Chamada',       cor:'#ef4444' },
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
  const tid  = setTimeout(function() { ctrl.abort(); }, 25000);
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

  while (hasMore && paginas < 30) {
    paginas++;
    var ca = cursor ? (', after: "' + cursor + '"') : '';
    var q  = 'query { phase(id: "' + pipefyPhaseId + '") { cards(first: 50' + ca + ') { pageInfo { hasNextPage endCursor } edges { node { id title fields { name value } } } } } }';

    var data = null;
    try { data = await pipefyReqFn(q); }
    catch(e2) {
      erros.push('Pag ' + paginas + ': ' + String(e2.message));
      // Tentar uma vez mais antes de desistir
      await new Promise(function(r){ setTimeout(r, 2000); });
      try { data = await pipefyReqFn(q); }
      catch(e3) { erros.push('Retry falhou: ' + String(e3.message)); hasMore = false; break; }
    }

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


  // ── GET comparar-pipefy: compara Redis vs Pipefy e gera log de divergências ──
  if (action === 'comparar-pipefy') {
    if (!PIPEFY_TOKEN) return res.status(200).json({ ok:false, error:'sem PIPEFY_TOKEN' });
    const db       = (await dbGet(PIPE_KEY)) || defaultDB();
    const nosCards = db.cards || [];

    // Fases a comparar (mesmas do sync)
    const FASES = [
      { phId:'334875152', local:'aguardando_aprovacao', nome:'Aguardando Aprovação' },
      { phId:'338413470', local:'ultima_chamada',       nome:'Última Chamada' },
      { phId:'334879132', local:'aprovados',            nome:'Aprovado' },
      { phId:'342533760', local:'video_enviado',        nome:'Vídeo Enviado' },
      { phId:'342584529', local:'analise_compra',       nome:'Análise de Compra' },
      { phId:'338439265', local:'programar_entrega',    nome:'Programar Entrega' },
      { phId:'334875186', local:'solicitar_entrega',    nome:'Solicitar Entrega' },
      { phId:'335066834', local:'entrega_solicitada',   nome:'Entrega Solicitada' },
      { phId:'334875204', local:'receber',              nome:'Receber' },
      { phId:'339008925', local:'erp',                  nome:'ERP' },
    ];

    const log = [];
    let totalPipefy = 0, totalNosso = 0, sincronizados = 0, faltando = 0, extra = 0, divergencia = 0;

    // Mapa dos nossos cards por pipefyId
    const nosMap = {};
    for (const c of nosCards) { if (c.pipefyId) nosMap[c.pipefyId] = c; }
    const nosIdsVistos = new Set();

    // Buscar cada fase no Pipefy
    for (const fase of FASES) {
      var pipefyCards = [];
      var cursor = null;
      var hasMore = true;
      var tentativa = 0;

      while (hasMore && tentativa < 10) {
        tentativa++;
        var ca = cursor ? ', after: "' + cursor + '"' : '';
        var data = await pipefyReq(
          'query { phase(id: "' + fase.phId + '") { cards(first: 50' + ca + ') { pageInfo { hasNextPage endCursor } edges { node { id title } } } } }'
        ).catch(() => null);

        var edges    = (data && data.phase && data.phase.cards && data.phase.cards.edges) ? data.phase.cards.edges : [];
        var pageInfo = (data && data.phase && data.phase.cards && data.phase.cards.pageInfo) ? data.phase.cards.pageInfo : {};

        for (var e of edges) {
          pipefyCards.push({ id: String(e.node.id), title: e.node.title });
        }
        hasMore = pageInfo.hasNextPage || false;
        cursor  = pageInfo.endCursor  || null;
      }

      totalPipefy += pipefyCards.length;

      for (var pCard of pipefyCards) {
        nosIdsVistos.add(pCard.id);
        var nosCard = nosMap[pCard.id];

        if (!nosCard) {
          faltando++;
          log.push({
            tipo: 'FALTANDO',
            pipefyId: pCard.id,
            fase: fase.nome,
            titulo: pCard.title,
            msg: 'Card existe no Pipefy mas NÃO está no nosso sistema'
          });
        } else {
          // Verificar divergência de fase
          if (nosCard.phase !== fase.local) {
            divergencia++;
            log.push({
              tipo: 'FASE_DIVERGENTE',
              pipefyId: pCard.id,
              nossoId: nosCard.id,
              nomeContato: nosCard.nomeContato,
              faseNosso: nosCard.phase,
              fasePipefy: fase.nome,
              msg: 'Fase no Pipefy: "' + fase.nome + '" | Fase no nosso sistema: "' + nosCard.phase + '"'
            });
          } else {
            sincronizados++;
          }
        }
      }
    }

    // Cards no nosso sistema (de origem pipefy) que não foram vistos no Pipefy
    for (var nc of nosCards) {
      if (nc.pipefyId && nc.origem === 'pipefy' && !nosIdsVistos.has(nc.pipefyId)) {
        extra++;
        log.push({
          tipo: 'EXTRA_NOSSO',
          nossoId: nc.id,
          pipefyId: nc.pipefyId,
          nomeContato: nc.nomeContato,
          fase: nc.phase,
          msg: 'Card no nosso sistema mas NÃO encontrado nas fases monitoradas do Pipefy (pode ter avançado de fase)'
        });
      }
    }

    totalNosso = nosCards.filter(c => c.pipefyId).length;

    // Salvar log no Redis para histórico
    var logEntry = {
      ts:           new Date().toISOString(),
      totalPipefy,
      totalNosso,
      sincronizados, faltando, extra, divergencia,
      log
    };
    var logKey = 'reparoeletro_pipe_log';
    var logDb  = (await dbGet(logKey)) || { entradas: [] };
    logDb.entradas = [logEntry, ...(logDb.entradas || [])].slice(0, 20); // manter 20 últimas
    await dbSet(logKey, logDb);

    return res.status(200).json({ ok:true, ...logEntry });
  }

  // ── GET historico-log: retorna histórico de comparações ───────────────────
  if (action === 'historico-log') {
    var logKey = 'reparoeletro_pipe_log';
    var logDb  = (await dbGet(logKey)) || { entradas: [] };
    return res.status(200).json({ ok:true, entradas: logDb.entradas || [] });
  }


  // ── GET pipe-sem-resposta: move cards 48h+ em aguardando_aprovacao → ultima_chamada ──
  if (action === 'pipe-sem-resposta') {
    const db       = (await dbGet(PIPE_KEY)) || defaultDB();
    const agora    = Date.now();
    const MS_48H   = 48 * 60 * 60 * 1000;
    let movidos = 0;
    for (const card of (db.cards || [])) {
      if (card.phase !== 'aguardando_aprovacao') continue;
      const desde = card.aguardandoDesde ? new Date(card.aguardandoDesde).getTime() : 0;
      if (!desde || (agora - desde) < MS_48H) continue;
      const now = new Date().toISOString();
      card.history = (card.history || []).concat([{ phase: 'aguardando_aprovacao', ts: now }]);
      card.phase   = 'ultima_chamada';
      card.movedAt = now;
      movidos++;
    }
    if (movidos > 0) await dbSet(PIPE_KEY, db);
    return res.status(200).json({ ok: true, movidos });
  }


  // ── GET debug-board: mostra cards do board que batem com um nome ──────────
  if (action === 'debug-board') {
    const busca = (req.query.q || '').toLowerCase();
    const pipeDb  = (await dbGet(PIPE_KEY)) || defaultDB();
    const boardDb = await dbGet('reparoeletro_board');
    const pipeMatch  = (pipeDb.cards  || []).filter(c => (c.nomeContato||'').toLowerCase().includes(busca) || (c.id||'').toLowerCase().includes(busca));
    const boardMatch = boardDb ? (boardDb.cards || []).filter(c => (c.nomeContato||c.title||'').toLowerCase().includes(busca) || (c.osCode||'').toLowerCase().includes(busca)) : [];
    return res.status(200).json({
      ok: true, busca,
      pipe:  pipeMatch.map(c => ({ id:c.id, pipefyId:c.pipefyId, nome:c.nomeContato, fase:c.phase })),
      board: boardMatch.map(c => ({ pipefyId:c.pipefyId, phaseId:c.phaseId, nome:c.nomeContato||c.title, osCode:c.osCode, localOnly:c.localOnly })),
      boardTotal: boardDb ? (boardDb.cards||[]).length : 0
    });
  }

  // ── POST force-board: força criação de card no board pelo id do Pipe ───────
  if (req.method === 'POST' && action === 'force-board') {
    const { pipeId } = req.body || {};
    if (!pipeId) return res.status(400).json({ ok:false, error:'pipeId obrigatorio' });
    const pipeDb  = (await dbGet(PIPE_KEY)) || defaultDB();
    const card    = (pipeDb.cards || []).find(c => c.id === pipeId);
    if (!card) return res.status(404).json({ ok:false, error:'Card nao encontrado no Pipe: '+pipeId });
    const boardDb = (await dbGet('reparoeletro_board')) || { cards:[], syncedIds:[], movesLog:[], metaLog:[], phases:[], rsPhases:[], rsRuaPhases:[], rsCards:[], rsRuaCards:[] };
    if (!Array.isArray(boardDb.cards)) boardDb.cards = [];
    const boardPid = card.pipefyId ? String(card.pipefyId) : ('LOCAL-'+card.id);
    // Remover entrada antiga se existir
    boardDb.cards = boardDb.cards.filter(c => c.pipefyId !== boardPid && c.osCode !== card.id);
    const now = new Date().toISOString();
    const novoCard = {
      pipefyId:    boardPid,
      phaseId:     'producao',
      nomeContato: card.nomeContato || '',
      title:       card.descricao || card.nomeContato || '',
      telefone:    card.telefone || '',
      descricao:   card.equipamento || card.descricao || '',
      osCode:      card.id,
      valor:       card.valor || 0,
      movedBy:     'Pipe ADM',
      flFichaId:   null,
      localOnly:   !card.pipefyId,
      syncedAt:    now,
      movedAt:     now
    };
    boardDb.cards.unshift(novoCard);
    if (!boardDb.syncedIds) boardDb.syncedIds = [];
    if (!boardDb.syncedIds.includes(boardPid)) boardDb.syncedIds.push(boardPid);
    if (!boardDb.movesLog) boardDb.movesLog = [];
    boardDb.movesLog.push({ phaseId:'aprovado_entrada', pipefyId:boardPid, timestamp:now });
    await dbSet('reparoeletro_board', boardDb);
    return res.status(200).json({ ok:true, card:novoCard, boardTotal:boardDb.cards.length });
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

  // ── POST editar-valor ─────────────────────────────────────────────────────
  if (req.method === 'POST' && action === 'editar-valor') {
    var body  = req.body || {};
    var id    = body.id;
    var valor = parseFloat(body.valor) || 0;
    if (!id) return res.status(400).json({ ok: false, error: 'id obrigatorio' });
    var db   = (await dbGet(PIPE_KEY)) || defaultDB();
    var card = (db.cards || []).find(function(c) { return c.id === id; });
    if (!card) return res.status(404).json({ ok: false, error: 'nao encontrado' });
    card.valor = valor;
    await dbSet(PIPE_KEY, db);
    return res.status(200).json({ ok: true, valor: valor });
  }


  // ── GET force-valores: atualiza valores do Pipe via Logística/FL/Redis ──────
  if (action === 'force-valores') {
    const db  = (await dbGet(PIPE_KEY)) || defaultDB();
    const sem = (db.cards || []).filter(function(c){ return !c.valor || c.valor === 0; });
    let atualizados = 0;

    // Carregar fontes Redis
    const logDb = await dbGet('reparoeletro_logistica').catch(() => null);
    const flDb  = await dbGet('reparoeletro_frenteloja').catch(() => null);
    const logFichas = (logDb && logDb.fichas) ? logDb.fichas : [];
    const flFichas  = (flDb  && flDb.fichas)  ? flDb.fichas  : [];

    for (var ci = 0; ci < sem.length; ci++) {
      var card = sem[ci];
      var pid  = card.pipefyId || null;
      var novoValor = 0;

      // 1. Tentar logística (diagnostico.preco)
      var logFicha = logFichas.find(function(f){ return pid && f.pipefyCardId === String(pid); });
      if (logFicha && logFicha.diagnostico && logFicha.diagnostico.preco) {
        novoValor = parseFloat(logFicha.diagnostico.preco) || 0;
      }

      // 2. Tentar frente de loja (orcamento.valor)
      if (!novoValor) {
        var flFicha = flFichas.find(function(f){ return pid && f.pipefyCardId === String(pid); });
        if (flFicha && flFicha.orcamento && flFicha.orcamento.valor) {
          novoValor = parseFloat(flFicha.orcamento.valor) || 0;
        }
      }

      // 3. Tentar Pipefy (valor_de_contrato) se ainda não encontrou
      if (!novoValor && pid && PIPEFY_TOKEN) {
        try {
          var pfData = await pipefyReq(
            'query { card(id: "' + pid + '") { fields { name value } } }'
          ).catch(() => null);
          if (pfData && pfData.card && pfData.card.fields) {
            var valField = pfData.card.fields.find(function(f){
              return f.name && (f.name.toLowerCase().includes('valor') || f.name.toLowerCase().includes('contrato'));
            });
            if (valField && valField.value) novoValor = parseFloat(valField.value) || 0;
          }
        } catch(ep) { /* ignora */ }
      }

      if (novoValor > 0) {
        card.valor = novoValor;
        atualizados++;
      }
    }

    if (atualizados > 0) await dbSet(PIPE_KEY, db);
    return res.status(200).json({
      ok: true,
      semValor: sem.length,
      atualizados: atualizados,
      restante: sem.length - atualizados
    });
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
    if (phase === 'aguardando_aprovacao') card.aguardandoDesde = now;
    await dbSet(PIPE_KEY, db);

    // ── Gatilhos downstream ──────────────────────────────────────────────
    var pid = card.pipefyId;
    // Aprovados → Board Técnico (producao ou cliente_loja)
    if (phase === 'aprovados') {
      try {
        var boardPid = pid ? String(pid) : ('LOCAL-' + card.id);
        // Usar dbGet/dbSet do BOARD (mesmo formato) — leitura direta via Upstash
        var bU = (process.env.UPSTASH_URL   || '').replace(/['"]/g,'').trim();
        var bT = (process.env.UPSTASH_TOKEN || '').replace(/['"]/g,'').trim();
        async function bGet(k) {
          var r = await fetch(bU+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+bT,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});
          var j = await r.json(); var v = j[0]?.result; if(!v) return null;
          try { return JSON.parse(v); } catch(e){ return null; }
        }
        async function bSet(k,v) {
          await fetch(bU+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+bT,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});
        }
        var BOARD_KEY2 = 'reparoeletro_board';
        var boardDb2 = await bGet(BOARD_KEY2);
        if (!boardDb2 || typeof boardDb2 !== 'object') boardDb2 = { cards:[], syncedIds:[], movesLog:[], metaLog:[], phases:[], rsPhases:[], rsRuaPhases:[], rsCards:[], rsRuaCards:[] };
        if (!Array.isArray(boardDb2.cards)) boardDb2.cards = [];
        // Remover entrada antiga se existir
        boardDb2.cards = boardDb2.cards.filter(function(x){ return x.pipefyId !== boardPid && x.osCode !== card.id; });
        // Sempre inserir/recriar o card
        boardDb2.cards.unshift({
          pipefyId:    boardPid,
          phaseId:     'producao',
          nomeContato: card.nomeContato || '',
          title:       card.descricao   || card.nomeContato || '',
          telefone:    card.telefone    || '',
          descricao:   card.equipamento || card.descricao || '',
          osCode:      card.id,
          valor:       card.valor || 0,
          movedBy:     'Pipe ADM',
          flFichaId:   null,
          localOnly:   !pid,
          syncedAt:    now,
          movedAt:     now
        });
        if (!Array.isArray(boardDb2.syncedIds)) boardDb2.syncedIds = [];
        if (!boardDb2.syncedIds.includes(boardPid)) boardDb2.syncedIds.push(boardPid);
        if (!Array.isArray(boardDb2.movesLog)) boardDb2.movesLog = [];
        boardDb2.movesLog.push({ phaseId:'aprovado_entrada', pipefyId:boardPid, timestamp:now });
        if (!Array.isArray(boardDb2.metaLog)) boardDb2.metaLog = [];
        boardDb2.metaLog.push({ phaseId:'aprovado_entrada', pipefyId:boardPid, timestamp:now });
        await bSet(BOARD_KEY2, boardDb2);
      } catch(e) { console.error('[pipe→board]', e.message); }
    }
    // Video Enviado → criar ficha no Financeiro
    if (phase === 'video_enviado') {
      try {
        var finDb2 = await dbGet('reparoeletro_financeiro') || { records: [] };
        if (!Array.isArray(finDb2.records)) finDb2.records = [];
        var jaFinExiste = finDb2.records.find(function(r){ return r.pipefyId === String(pid); });
        if (!jaFinExiste) {
          finDb2.records.unshift({
            id: 'FIN-PIPE-' + String(Date.now()),
            pipefyId: String(pid),
            nomeContato: card.nomeContato || '',
            telefone: card.telefone || '',
            valor: card.valor || 0,
            phaseId: 'nf_emitida',
            criadoEm: now, movedAt: now,
            history: [{ phaseId: 'nf_emitida', ts: now }],
            origem: 'pipe_video_enviado'
          });
          await dbSet('reparoeletro_financeiro', finDb2);
        }
      } catch(e) { console.error('[pipe→financeiro]', e.message); }
    }
    // Analise de Compra → criar entrada em compra-equip
    if (phase === 'analise_compra') {
      try {
        var compraDb2 = await dbGet('reparoeletro_compra_equip') || { fichas: [] };
        if (!Array.isArray(compraDb2.fichas)) compraDb2.fichas = [];
        var jaCompraExiste = compraDb2.fichas.find(function(f){ return f.pipefyId === String(pid); });
        if (!jaCompraExiste) {
          compraDb2.fichas.unshift({
            id: String(pid), pipefyId: String(pid),
            nomeContato: card.nomeContato || '',
            descricao: card.equipamento || card.descricao || '',
            valor: card.valor || 0,
            status: 'analise', fotos: [], criadoEm: now
          });
          await dbSet('reparoeletro_compra_equip', compraDb2);
        }
      } catch(e) { console.error('[pipe→compra]', e.message); }
    }

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
