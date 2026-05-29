function fmt4dig(nome, tel) {
  if (!nome) return '';
  var n = String(nome).trim();
  if (/\s\d{4}$/.test(n)) return n;
  if (!tel) return n;
  var digits = String(tel).replace(/\D/g,'');
  var last4 = digits.slice(-4);
  if (last4.length < 4) return n;
  return n + ' ' + last4;
}


// ── Helper: gravar no log central ────────────────────────────────────────
async function logAction(entry) {
  try {
    const _U = (process.env.UPSTASH_URL   || '').replace(/['"]/g,'').trim();
    const _T = (process.env.UPSTASH_TOKEN || '').replace(/['"]/g,'').trim();
    const _K = 'reparoeletro_log';
    const _r = await fetch(_U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+_T,'Content-Type':'application/json'},body:JSON.stringify([['GET',_K]])});
    const _j = await _r.json(); const _v = _j[0]?.result;
    let _log = []; if(_v){try{_log=JSON.parse(_v);if(typeof _log==='string')_log=JSON.parse(_log);}catch(e){}} if(!Array.isArray(_log))_log=[];
    _log.unshift({ ts:new Date().toISOString(), modulo:entry.modulo||'—', fichaId:entry.fichaId||'', ficha:entry.ficha||'', acao:entry.acao||'', de:entry.de||'', para:entry.para||'', gatilho:entry.gatilho||'', status:entry.status||'ok', detalhe:entry.detalhe||'' });
    if(_log.length>500) _log.splice(500);
    await fetch(_U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+_T,'Content-Type':'application/json'},body:JSON.stringify([['SET',_K,JSON.stringify(_log)]])});
  } catch(e){ /* log silencioso */ }
}

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
        nomeContato:     fmt4dig(nome || nd.title || '', tel),
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
  if (action === 'force-board') {  // aceita GET e POST
    const { pipeId } = req.body || req.query || {};
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


  // ── GET limpar-fin-pipe: remove registros FIN-PIPE-* espúrios do financeiro ──
  if (action === 'limpar-fin-pipe') {
    try {
      const U2 = (process.env.UPSTASH_URL   || '').replace(/['"]/g,'').trim();
      const T2 = (process.env.UPSTASH_TOKEN || '').replace(/['"]/g,'').trim();
      async function _fg(k){const r=await fetch(U2+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T2,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});const j=await r.json();const v=j[0]?.result;if(!v)return null;let val=JSON.parse(v);if(typeof val==='string'){try{val=JSON.parse(val);}catch(e){}}return(val&&typeof val==='object')?val:null;}
      async function _fs(k,v){await fetch(U2+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T2,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});}
      const fin = await _fg('reparoeletro_financeiro');
      if (!fin || !Array.isArray(fin.records)) return res.status(200).json({ ok:true, removidos:0, info:'financeiro vazio ou sem records' });
      const antes   = fin.records.length;
      const espurios = fin.records.filter(r => (r.id||'').startsWith('FIN-PIPE-') || r.origem === 'pipe_video_enviado');
      fin.records    = fin.records.filter(r => !(r.id||'').startsWith('FIN-PIPE-') && r.origem !== 'pipe_video_enviado');
      const removidos = antes - fin.records.length;
      if (removidos > 0) await _fs('reparoeletro_financeiro', fin);
      return res.status(200).json({ ok:true, removidos, restantes:fin.records.length, espurios: espurios.map(r=>({id:r.id,ficha:r.nomeContato,fase:r.phaseId})) });
    } catch(e) {
      return res.status(500).json({ ok:false, error: e.message });
    }
  }


  // ── GET video-para-financeiro: cria fichas no financeiro para cards em video_enviado após 15h ──
  if (action === 'video-para-financeiro') {
    try {
      // Limite: 15h horário Brasília = 18h UTC
      var hoje = new Date();
      var limite = new Date(Date.UTC(hoje.getUTCFullYear(), hoje.getUTCMonth(), hoje.getUTCDate(), 18, 0, 0)); // 15h BRT = 18h UTC
      
      // Ler Pipe
      var pipeDb = (await dbGet(PIPE_KEY)) || defaultDB();
      var candidatos = (pipeDb.cards || []).filter(function(c) {
        if (c.phase !== 'video_enviado') return false;
        // Verificar se foi movido após as 15h hoje
        var movedAt = c.movedAt ? new Date(c.movedAt) : null;
        if (!movedAt) return false;
        return movedAt >= limite;
      });

      if (!candidatos.length) {
        return res.status(200).json({ ok: true, criados: 0, info: 'Nenhum card em video_enviado após 15h hoje', limite: limite.toISOString() });
      }

      // Ler Financeiro
      var U3 = (process.env.UPSTASH_URL   || '').replace(/['"]/g,'').trim();
      var T3 = (process.env.UPSTASH_TOKEN || '').replace(/['"]/g,'').trim();
      async function _fg(k){var r=await fetch(U3+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T3,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});var j=await r.json();var v=j[0]?.result;if(!v)return null;try{var val=JSON.parse(v);if(typeof val==='string')val=JSON.parse(val);return(val&&typeof val==='object')?val:null;}catch(e){return null;}}
      async function _fs(k,v){await fetch(U3+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T3,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});}

      var finDb = await _fg('reparoeletro_financeiro');
      if (!finDb || !Array.isArray(finDb.records)) finDb = { records: [], syncedIds: [], movesLog: [] };

      var criados = [];
      var ignorados = [];

      for (var i = 0; i < candidatos.length; i++) {
        var card = candidatos[i];
        var pid  = card.pipefyId || ('PIPE-LOCAL-' + card.id);
        // Verificar se já existe no financeiro
        var jaExiste = finDb.records.find(function(r) {
          return (card.pipefyId && r.pipefyId === String(card.pipefyId)) ||
                 (r.osCode && r.osCode === card.id);
        });
        if (jaExiste) { ignorados.push({ id: card.id, ficha: card.nomeContato, motivo: 'já existe (phase: '+jaExiste.phaseId+')' }); continue; }

        var novoRec = {
          id:          'FIN-' + card.id,
          pipefyId:    card.pipefyId ? String(card.pipefyId) : null,
          osCode:      card.id,
          nomeContato: card.nomeContato || '',
          telefone:    card.telefone    || '',
          equipamento: card.equipamento || card.descricao || '',
          valor:       card.valor || 0,
          phaseId:     'aguardando_dados',
          criadoEm:    now,
          movedAt:     now,
          history:     [{ phaseId: 'aguardando_dados', ts: now }],
          origem:      'video_enviado_manual'
        };
        finDb.records.unshift(novoRec);
        criados.push({ id: novoRec.id, ficha: novoRec.nomeContato, pipefyId: novoRec.pipefyId, valor: novoRec.valor });
      }

      if (criados.length > 0) {
        await _fs('reparoeletro_financeiro', finDb);
      }

      return res.status(200).json({
        ok: true,
        criados: criados.length,
        ignorados: ignorados.length,
        fichas: criados,
        jaTinham: ignorados,
        limite: limite.toISOString()
      });
    } catch(e) {
      return res.status(500).json({ ok: false, error: e.message });
    }
  }


  // ── GET restaurar-financeiro: restaura do backup ────────────────────────
  if (action === 'restaurar-financeiro') {
    try {
      var U4=(process.env.UPSTASH_URL||'').replace(/['"]/g,'').trim();
      var T4=(process.env.UPSTASH_TOKEN||'').replace(/['"]/g,'').trim();
      async function _r4(k){var r=await fetch(U4+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T4,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});var j=await r.json();var v=j[0]?.result;if(!v)return null;try{var val=JSON.parse(v);if(typeof val==='string')val=JSON.parse(val);return(val&&typeof val==='object')?val:null;}catch(e){return null;}}
      async function _s4(k,v){await fetch(U4+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T4,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});}

      var backup = await _r4('reparoeletro_financeiro_backup');
      if (!backup || !Array.isArray(backup.records)) {
        return res.status(404).json({ ok:false, error:'Backup não encontrado ou inválido', backup });
      }
      // Remover campo de controle do backup antes de restaurar
      delete backup.backedUpAt;
      await _s4('reparoeletro_financeiro', backup);
      return res.status(200).json({
        ok: true,
        restaurados: backup.records.length,
        backedUpAt: backup.backedUpAt || 'desconhecido',
        info: 'Financeiro restaurado do backup com sucesso'
      });
    } catch(e) {
      return res.status(500).json({ ok:false, error: e.message });
    }
  }


  // ── GET video-para-financeiro: cria fichas no financeiro para video_enviado após 15h ──
  if (action === 'video-para-financeiro') {
    try {
      var U4=(process.env.UPSTASH_URL||'').replace(/['"]/g,'').trim();
      var T4=(process.env.UPSTASH_TOKEN||'').replace(/['"]/g,'').trim();

      async function _rfin(k){
        var r=await fetch(U4+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T4,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});
        var j=await r.json();var v=j[0]?.result;
        if(!v)return null;
        try{var val=JSON.parse(v);if(typeof val==='string')val=JSON.parse(val);return(val&&typeof val==='object')?val:null;}catch(e){return null;}
      }
      async function _sfin(k,v){
        await fetch(U4+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T4,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});
      }

      // 1. LER financeiro — abortar se falhar
      var finDb = await _rfin('reparoeletro_financeiro');
      if (!finDb) return res.status(500).json({ ok:false, error:'Falha ao ler financeiro — abortando para não corromper dados' });
      if (!Array.isArray(finDb.records)) return res.status(500).json({ ok:false, error:'Financeiro sem array records — estrutura inesperada', finDb });

      // 2. LER Pipe — abortar se falhar
      var pipeDb = await _rfin(PIPE_KEY);
      if (!pipeDb || !Array.isArray(pipeDb.cards)) return res.status(500).json({ ok:false, error:'Falha ao ler Pipe — abortando' });

      // 3. Filtrar cards video_enviado após 15h BRT (18h UTC) de hoje
      var hoje = new Date();
      var limite = new Date(Date.UTC(hoje.getUTCFullYear(), hoje.getUTCMonth(), hoje.getUTCDate(), 18, 0, 0));
      var candidatos = pipeDb.cards.filter(function(card){
        if (card.phase !== 'video_enviado') return false;
        var movedAt = card.movedAt ? new Date(card.movedAt) : null;
        return movedAt && movedAt >= limite;
      });

      if (!candidatos.length) return res.status(200).json({ ok:true, criados:0, info:'Nenhum card em video_enviado após 15h', limite:limite.toISOString() });

      // 4. Inserir apenas os que não existem
      var criados=[], ignorados=[];
      for (var i=0;i<candidatos.length;i++){
        var card=candidatos[i];
        var jaExiste=finDb.records.find(function(r){
          return (card.pipefyId && r.pipefyId===String(card.pipefyId)) || (r.osCode && r.osCode===card.id);
        });
        if(jaExiste){ignorados.push({id:card.id,ficha:card.nomeContato,fase:jaExiste.phaseId});continue;}
        var rec={
          id:'FIN-'+card.id, pipefyId:card.pipefyId?String(card.pipefyId):null,
          osCode:card.id, nomeContato:card.nomeContato||'', telefone:card.telefone||'',
          equipamento:card.equipamento||card.descricao||'', valor:card.valor||0,
          phaseId:'aguardando_dados', criadoEm:now, movedAt:now,
          history:[{phaseId:'aguardando_dados',ts:now}], origem:'video_enviado_manual'
        };
        finDb.records.unshift(rec);
        criados.push({id:rec.id,ficha:rec.nomeContato,pipefyId:rec.pipefyId,valor:rec.valor});
      }

      // 5. Salvar SOMENTE se algo foi criado
      if(criados.length>0){
        // Backup antes de salvar
        try{await _sfin('reparoeletro_financeiro_backup',{...finDb,backedUpAt:now});}catch(e){}
        await _sfin('reparoeletro_financeiro', finDb);
      }

      return res.status(200).json({ok:true,criados:criados.length,ignorados:ignorados.length,fichas:criados,jaTinham:ignorados,limite:limite.toISOString()});
    } catch(e) {
      return res.status(500).json({ok:false,error:e.message});
    }
  }


  // ══ FINANCEIRO via pipe.js (bypass de financeiro.js quebrado) ══════════════

  if (action === 'fin-load') {
    try {
      var fin = await dbGet('reparoeletro_financeiro');
      if (!fin || !Array.isArray(fin.records)) fin = { records:[] };
      var FP = [{id:'aguardando_dados',name:'Aguardando Dados'},{id:'nf_emitida',name:'NF Emitida'},{id:'faturamento',name:'Faturamento'},{id:'entrega_liberada',name:'Entrega Liberada'},{id:'solicitar_entrega',name:'Solicitar Entrega'},{id:'rota_criada',name:'Rota Criada'},{id:'pagamento_confirmado',name:'Pagamento Confirmado'}];
      var pc={}; FP.forEach(function(p){pc[p.id]=0;});
      fin.records.forEach(function(r){if(pc[r.phaseId]!==undefined)pc[r.phaseId]++;});
      return res.status(200).json({ok:true,records:fin.records,phases:FP,phaseCounts:pc,goals:{today:{faturamento:{count:0,goal:20},rota:{count:0,goal:20}},week:{faturamento:{count:0,goal:120},rota:{count:0,goal:120}}},todayLabel:'',weekLabel:''});
    } catch(e){return res.status(500).json({ok:false,error:e.message});}
  }

  if (action === 'fin-mover' && req.method === 'POST') {
    try {
      var body = req.body || {};
      var fin = await dbGet('reparoeletro_financeiro');
      if (!fin || !Array.isArray(fin.records)) return res.status(404).json({ok:false,error:'financeiro vazio'});
      var rec = fin.records.find(function(r){return r.id===body.id;});
      if (!rec) return res.status(404).json({ok:false,error:'ficha nao encontrada'});
      rec.history=(rec.history||[]).concat([{phaseId:rec.phaseId,ts:now}]);
      rec.phaseId=body.phaseId; rec.movedAt=now;
      if(body.valor!==undefined) rec.valor=parseFloat(body.valor)||0;
      await dbSet('reparoeletro_financeiro',fin);
      return res.status(200).json({ok:true,record:rec,pipefyMoveOk:false});
    } catch(e){return res.status(500).json({ok:false,error:e.message});}
  }


  // ── GET fix-fin-records: corrige registros FIN-PIPE sem ts no history ────
  if (action === 'fix-fin-records') {
    try {
      var U6=(process.env.UPSTASH_URL||'').replace(/['"]/g,'').trim();
      var T6=(process.env.UPSTASH_TOKEN||'').replace(/['"]/g,'').trim();
      async function _g6(k){var r=await fetch(U6+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T6,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});var j=await r.json();var v=j[0]?.result;if(!v)return null;try{var x=JSON.parse(v);if(typeof x==='string')x=JSON.parse(x);return x;}catch(e){return null;}}
      async function _s6(k,v){await fetch(U6+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T6,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});}
      var fin=await _g6('reparoeletro_financeiro');
      if(!fin||!Array.isArray(fin.records))return res.status(200).json({ok:false,msg:'financeiro vazio'});
      var fixed=0;
      fin.records.forEach(function(rec){
        // 1. Garantir ts em cada history item
        if(Array.isArray(rec.history)){
          rec.history.forEach(function(h){
            if(!h.ts) h.ts = rec.criadoEm || rec.createdAt || rec.movedAt || new Date().toISOString();
          });
        }
        // 2. Garantir campos mínimos
        if(!rec.nomeContato) rec.nomeContato = rec.title || '';
        if(!rec.valor && rec.preco) rec.valor = rec.preco;
        fixed++;
      });
      await _s6('reparoeletro_financeiro',fin);
      return res.status(200).json({ok:true,processados:fixed,total:fin.records.length});
    } catch(e){return res.status(500).json({ok:false,error:e.message});}
  }


  // ── GET mover-fin: move registro do financeiro para fase correta ──────────
  if (action === 'mover-fin') {
    var id      = req.query.id      || '';
    var fase    = req.query.fase    || 'aguardando_dados';
    var busca   = req.query.busca   || '';
    try {
      var U7=(process.env.UPSTASH_URL||'').replace(/['"]/g,'').trim();
      var T7=(process.env.UPSTASH_TOKEN||'').replace(/['"]/g,'').trim();
      async function _g7(k){var r=await fetch(U7+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T7,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});var j=await r.json();var v=j[0]?.result;if(!v)return null;try{var x=JSON.parse(v);if(typeof x==='string')x=JSON.parse(x);return x;}catch(e){return null;}}
      async function _s7(k,v){await fetch(U7+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T7,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});}
      var fin=await _g7('reparoeletro_financeiro');
      if(!fin||!Array.isArray(fin.records))return res.status(404).json({ok:false,error:'financeiro vazio'});
      // Buscar por id OU por nome
      var rec=null;
      if(id) rec=fin.records.find(function(r){return r.id===id||r.pipefyId===id;});
      if(!rec&&busca) rec=fin.records.find(function(r){return (r.nomeContato||'').toLowerCase().includes(busca.toLowerCase())||(r.title||'').toLowerCase().includes(busca.toLowerCase());});
      if(!rec)return res.status(404).json({ok:false,error:'Registro não encontrado',busca:busca,id:id});
      var faseAnterior=rec.phaseId;
      rec.history=(rec.history||[]).concat([{phaseId:faseAnterior,ts:rec.movedAt||now}]);
      rec.phaseId=fase;
      rec.movedAt=now;
      // Backup antes de salvar
      try{await _s7('reparoeletro_financeiro_backup',Object.assign({},fin,{backedUpAt:now}));}catch(e){}
      await _s7('reparoeletro_financeiro',fin);
      return res.status(200).json({ok:true,id:rec.id,nome:rec.nomeContato||rec.title,de:faseAnterior,para:fase});
    } catch(e){return res.status(500).json({ok:false,error:e.message});}
  }


  // ── GET fin-buscar: lista registros recentes do financeiro ────────────────
  if (action === 'fin-buscar') {
    var q = (req.query.q||'').toLowerCase();
    try {
      var fin = await dbGet('reparoeletro_financeiro');
      if (!fin||!Array.isArray(fin.records)) return res.status(200).json({ok:true,records:[]});
      var lista = q
        ? fin.records.filter(function(r){ return JSON.stringify(r).toLowerCase().includes(q); })
        : fin.records.slice(0,20);
      return res.status(200).json({ok:true, total:lista.length,
        records:lista.map(function(r){return {id:r.id,nome:r.nomeContato||r.title,fase:r.phaseId,valor:r.valor,pipefyId:r.pipefyId};})
      });
    } catch(e){return res.status(500).json({ok:false,error:e.message});}
  }

  // ── POST editar-fin: edita campos de um registro do financeiro ─────────────
  if (req.method==='POST' && action==='editar-fin') {
    var body=req.body||{};
    var {id, nomeContato, valor, telefone, equipamento, cpfCnpj, endereco, servicos, descricao} = body;
    if (!id) return res.status(400).json({ok:false,error:'id obrigatório'});
    try {
      var fin=await dbGet('reparoeletro_financeiro');
      if(!fin||!Array.isArray(fin.records))return res.status(404).json({ok:false,error:'financeiro vazio'});
      var rec=fin.records.find(function(r){return r.id===id||r.pipefyId===id;});
      if(!rec)return res.status(404).json({ok:false,error:'Registro não encontrado: '+id});
      if(nomeContato!==undefined) rec.nomeContato=nomeContato;
      if(valor!==undefined)       rec.valor=parseFloat(String(valor).replace(',','.'))||0;
      if(telefone!==undefined)    rec.telefone=telefone;
      if(equipamento!==undefined) rec.equipamento=equipamento;
      if(descricao!==undefined)   rec.descricao=descricao;
      if(cpfCnpj!==undefined)     rec.cpfCnpj=cpfCnpj;
      if(endereco!==undefined)    rec.endereco=endereco;
      if(servicos!==undefined)    rec.servicos=servicos;
      rec.editadoEm=now;
      try{await dbSet('reparoeletro_financeiro_backup',Object.assign({},fin,{backedUpAt:now}));}catch(e){}
      await dbSet('reparoeletro_financeiro',fin);
      return res.status(200).json({ok:true,record:rec});
    } catch(e){return res.status(500).json({ok:false,error:e.message});}
  }

  // ── POST editar-pipe: edita campos de um card do Pipe ─────────────────────
  if (req.method==='POST' && action==='editar-pipe') {
    var body=req.body||{};
    var {id, nomeContato, valor, telefone, equipamento, descricao, endereco, cpfCnpj, servicos} = body;
    if (!id) return res.status(400).json({ok:false,error:'id obrigatório'});
    try {
      var db=await dbGet(PIPE_KEY)||defaultDB();
      var card=db.cards.find(function(c){return c.id===id||c.pipefyId===id;});
      if(!card)return res.status(404).json({ok:false,error:'Card não encontrado: '+id});
      if(nomeContato!==undefined) card.nomeContato=nomeContato;
      if(valor!==undefined)       card.valor=parseFloat(String(valor).replace(',','.'))||0;
      if(telefone!==undefined)    card.telefone=telefone;
      if(equipamento!==undefined) card.equipamento=equipamento;
      if(descricao!==undefined)   card.descricao=descricao;
      if(endereco!==undefined)    card.endereco=endereco;
      if(cpfCnpj!==undefined)     card.cpfCnpj=cpfCnpj;
      if(servicos!==undefined)    card.servicos=servicos;
      card.editadoEm=now;
      await dbSet(PIPE_KEY,db);
      return res.status(200).json({ok:true,card:card});
    } catch(e){return res.status(500).json({ok:false,error:e.message});}
  }


  // ── GET fin-pendentes: lista fichas com link MP mas sem pagamento confirmado ──
  if (action === 'fin-pendentes') {
    try {
      var fin = await dbGet('reparoeletro_financeiro');
      if (!fin || !Array.isArray(fin.records)) return res.status(200).json({ok:true,pendentes:[]});
      var pendentes = fin.records.filter(function(r){
        return r.mp && r.mp.preferenceId &&
               ['faturamento','pagamento_agendado','nf_emitida','analise_pagamento'].includes(r.phaseId);
      });
      return res.status(200).json({ok:true, total:pendentes.length,
        pendentes: pendentes.map(function(r){return {
          id:r.id, nome:r.nomeContato, fase:r.phaseId, valor:r.valor,
          preferenceId:r.mp.preferenceId, geradoEm:r.mp.geradoEm,
          metodo:r.mp.metodo, pipefyId:r.pipefyId, osCode:r.osCode
        };})
      });
    } catch(e){return res.status(500).json({ok:false,error:e.message});}
  }

  // ── GET fin-confirmar-manual: confirma pagamento manualmente por id da ficha ──
  if (action === 'fin-confirmar-manual') {
    var fichaId = req.query.id || '';
    var valor   = parseFloat(req.query.valor||0);
    var metodo  = req.query.metodo || 'manual';
    if (!fichaId) return res.status(400).json({ok:false,error:'id obrigatorio'});
    try {
      var U8=(process.env.UPSTASH_URL||'').replace(/['"]/g,'').trim();
      var T8=(process.env.UPSTASH_TOKEN||'').replace(/['"]/g,'').trim();
      async function _g8(k){var r=await fetch(U8+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T8,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});var j=await r.json();var v=j[0]?.result;if(!v)return null;try{var x=JSON.parse(v);if(typeof x==='string')x=JSON.parse(x);return x;}catch(e){return null;}}
      async function _s8(k,v){await fetch(U8+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T8,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});}
      var fin=await _g8('reparoeletro_financeiro');
      if(!fin||!Array.isArray(fin.records))return res.status(404).json({ok:false,error:'financeiro vazio'});
      var rec=fin.records.find(function(r){return r.id===fichaId||r.pipefyId===fichaId;});
      if(!rec)return res.status(404).json({ok:false,error:'ficha nao encontrada: '+fichaId});
      if(rec.phaseId==='entrega_liberada')return res.status(200).json({ok:true,info:'ja em entrega_liberada',id:rec.id});
      var ts=now;
      rec.paidAt=ts; rec.movedAt=ts;
      rec.mp=Object.assign(rec.mp||{},{status:'pago',pagoEm:ts,metodo:metodo,valor:valor||rec.valor});
      rec.history=(rec.history||[]).concat([{phaseId:'pagamento_confirmado',ts:ts,via:'manual'},{phaseId:'entrega_liberada',ts:ts,via:'manual'}]);
      rec.phaseId='entrega_liberada';
      // Backup
      try{await _s8('reparoeletro_financeiro_backup',Object.assign({},fin,{backedUpAt:ts}));}catch(e){}
      await _s8('reparoeletro_financeiro',fin);
      // Mover Pipe ADM
      var pipeDb=await _g8('reparoeletro_pipe');
      var pipeOk=false;
      if(pipeDb&&Array.isArray(pipeDb.cards)){
        var pcard=pipeDb.cards.find(function(c){return (rec.pipefyId&&c.pipefyId===String(rec.pipefyId))||(rec.osCode&&c.id===String(rec.osCode));});
        if(pcard){pcard.history=(pcard.history||[]).concat([{phase:pcard.phase,ts:ts}]);pcard.phase='solicitar_entrega';pcard.movedAt=ts;await _s8('reparoeletro_pipe',pipeDb);pipeOk=true;}
      }
      return res.status(200).json({ok:true,id:rec.id,nome:rec.nomeContato,de:rec.phaseId,para:'entrega_liberada',pipeOk:pipeOk});
    } catch(e){return res.status(500).json({ok:false,error:e.message});}
  }


  // ── GET force-venda-pipe: força última(s) venda(s) para Receber no Pipe ──
  if (action === 'force-venda-pipe') {
    var limite = parseInt(req.query.n || '5');
    try {
      var UV3=(process.env.UPSTASH_URL||'').replace(/['"]/g,'').trim();
      var TV3=(process.env.UPSTASH_TOKEN||'').replace(/['"]/g,'').trim();
      async function _vg3(k){var r=await fetch(UV3+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+TV3,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});var j=await r.json();var v=j[0]?.result;if(!v)return null;try{var x=JSON.parse(v);if(typeof x==='string')x=JSON.parse(x);return x;}catch(e){return null;}}
      async function _vs3(k,v){await fetch(UV3+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+TV3,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});}

      // Ler vendas
      var vendasDb=await _vg3('reparoeletro_vendas');
      var produtos=(vendasDb?.produtos||[]).filter(function(p){return p.vendido;});
      // Ordenar por data de venda (mais recente primeiro)
      produtos.sort(function(a,b){return new Date(b.soldAt||b.criadoEm||0)-new Date(a.soldAt||a.criadoEm||0);});
      var ultimas=produtos.slice(0,limite);

      // Ler Pipe
      var pipeDb2=await _vg3('reparoeletro_pipe')||{cards:[],syncedPipefyIds:[],lastSync:null};
      if(!Array.isArray(pipeDb2.cards))pipeDb2.cards=[];

      var adicionados=[], ignorados=[];
      var ts3=new Date().toISOString();

      for(var vi=0;vi<ultimas.length;vi++){
        var p3=ultimas[vi];
        // Verificar se já existe no pipe (pelo código ou descrição)
        var jaExiste=pipeDb2.cards.find(function(c){
          return c.origem==='venda'&&(
            c.descricao===('VENDA — '+(p3.codigo||''))||
            (p3.pipefyCardId&&c.pipefyId===String(p3.pipefyCardId))
          );
        });
        if(jaExiste){ignorados.push({codigo:p3.codigo,nome:p3.compradorNome,fase:jaExiste.phase});continue;}

        pipeDb2.cards.unshift({
          id:'PIPE-'+String(pipeDb2.cards.length+1).padStart(4,'0'),
          pipefyId:p3.pipefyCardId||null,
          phase:'receber',
          nomeContato:p3.compradorNome||'',
          telefone:p3.compradorTel||'',
          equipamento:p3.descricao||'',
          descricao:'VENDA — '+(p3.codigo||''),
          valor:parseFloat(p3.preco)||0,
          origem:'venda',
          criadoEm:p3.soldAt||ts3, movedAt:ts3,
          aguardandoDesde:null, history:[], analiseCompra:false
        });
        adicionados.push({codigo:p3.codigo,nome:p3.compradorNome,valor:p3.preco});
      }

      if(adicionados.length>0)await _vs3('reparoeletro_pipe',pipeDb2);
      return res.status(200).json({ok:true,adicionados:adicionados.length,ignorados:ignorados.length,fichas:adicionados,jaEstavam:ignorados});
    } catch(e){return res.status(500).json({ok:false,error:e.message});}
  }


  // ── GET backup-auto: salva snapshot timestamped de todos os dados críticos
  if (action === 'backup-auto') {
    var chaves = ['reparoeletro_pipe','reparoeletro_financeiro','reparoeletro_board',
                  'reparoeletro_logistica','reparoeletro_frenteloja'];
    var ts  = new Date().toISOString().replace(/[:.]/g,'-').slice(0,16); // 2026-05-29T14-30
    var res2 = { ts, salvos:[], erros:[] };
    for (var ki=0; ki<chaves.length; ki++) {
      var chave = chaves[ki];
      try {
        var dado = await dbGet(chave);
        if (dado) {
          var bakKey = chave + '_bak_' + ts;
          await dbSet(bakKey, dado);
          // Manter apenas últimos 48 backups por chave (48h se hourly)
          res2.salvos.push(bakKey);
        }
      } catch(e) { res2.erros.push({chave, erro:e.message}); }
    }
    // Salvar índice de backups
    try {
      var idx2 = (await dbGet('reparoeletro_backup_index')) || [];
      idx2.push({ ts, chaves: res2.salvos });
      idx2 = idx2.slice(-96); // últimos 96 backups (4 dias)
      await dbSet('reparoeletro_backup_index', idx2);
    } catch(e){}
    return res.status(200).json({ ok:true, ...res2 });
  }


  // ── GET diagnostico-erp: conta fases no Redis, detecta divergências ──────
  if (action === 'diagnostico-erp') {
    try {
      var db = await dbGet(PIPE_KEY) || {cards:[]};
      var cards2 = db.cards || [];
      // Contar por fase (incluindo variações/erros)
      var faseCount = {};
      cards2.forEach(function(c){ faseCount[c.phase||'SEM_FASE']=(faseCount[c.phase||'SEM_FASE']||0)+1; });
      // Identificar fases com ID do Pipefy ao invés do local
      var pipefyFases = cards2.filter(function(c){ return /^\d{8,}$/.test(c.phase||''); });
      // ERP específico
      var erpLocal    = cards2.filter(function(c){ return c.phase==='erp'; });
      var erpPipefyId = cards2.filter(function(c){ return c.phase==='339008925'; });
      var erpValor    = erpLocal.reduce(function(s,c){return s+(parseFloat(c.valor)||0);},0);
      return res.status(200).json({
        ok:true,
        totalCards: cards2.length,
        porFase: faseCount,
        erpLocal: erpLocal.length,
        erpComIdPipefy: erpPipefyId.length,
        erpValorTotal: erpValor,
        fasesComIdPipefy: pipefyFases.length,
        exemplos: pipefyFases.slice(0,5).map(function(c){return {id:c.id,nome:c.nomeContato,phase:c.phase};})
      });
    } catch(e){return res.status(500).json({ok:false,error:e.message});}
  }

  // ── GET fix-fases-erp: converte phase=339008925 → phase=erp no Redis ─────
  if (action === 'fix-fases-erp') {
    try {
      var db=await dbGet(PIPE_KEY)||{cards:[]};
      var FASE_MAP={
        '342533760':'aguardando_aprovacao','342533761':'aprovados','342533762':'video_enviado',
        '342533763':'analise_compra','339008925':'erp','342533764':'solicitar_entrega',
        '342533765':'entrega_solicitada','342533766':'receber','342533767':'finalizado'
      };
      var corrigidos=0;
      (db.cards||[]).forEach(function(card){
        if(FASE_MAP[card.phase]){
          card.phase=FASE_MAP[card.phase];
          corrigidos++;
        }
      });
      if(corrigidos>0) await dbSet(PIPE_KEY,db);
      var erpDepois=(db.cards||[]).filter(function(c){return c.phase==='erp';}).length;
      return res.status(200).json({ok:true,corrigidos,erpDepois,total:(db.cards||[]).length});
    } catch(e){return res.status(500).json({ok:false,error:e.message});}
  }


  // ── GET arquivar-ultima-chamada: arquiva fichas +90 dias em ultima_chamada ─
  if (action === 'arquivar-ultima-chamada') {
    var ARQUIVO_KEY  = 'reparoeletro_arquivo';
    var LIMITE_DIAS  = parseInt(req.query.dias || '90');
    var LIMITE_MS    = LIMITE_DIAS * 24 * 60 * 60 * 1000;
    var corte        = new Date(Date.now() - LIMITE_MS);
    try {
      var db  = await dbGet(PIPE_KEY) || {cards:[]};
      var arq = await dbGet(ARQUIVO_KEY) || {fichas:[], totalArquivado:0};
      if (!Array.isArray(arq.fichas)) arq.fichas = [];

      var paraArquivar = (db.cards||[]).filter(function(c){
        if (c.phase !== 'ultima_chamada') return false;
        var dt = new Date(c.movedAt || c.criadoEm || 0);
        return dt < corte;
      });

      if (!paraArquivar.length)
        return res.status(200).json({ok:true,arquivados:0,msg:'Nenhuma ficha elegível (ultima_chamada > '+LIMITE_DIAS+' dias)'});

      // IDs já arquivados (para idempotência)
      var jaArq = {};
      arq.fichas.forEach(function(f){ jaArq[f.id]=true; });

      var novos = 0;
      paraArquivar.forEach(function(card){
        if (jaArq[card.id]) return;
        arq.fichas.unshift(Object.assign({}, card, {
          arquivadoEm: now,
          motivoArquivo: 'ultima_chamada_'+LIMITE_DIAS+'d',
          phaseAntes: card.phase
        }));
        novos++;
      });

      // Remover do pipe ativo
      var idsArquivados = paraArquivar.map(function(c){return c.id;});
      db.cards = db.cards.filter(function(c){ return !idsArquivados.includes(c.id); });

      arq.totalArquivado = (arq.totalArquivado||0) + novos;
      arq.ultimoArquivo  = now;

      // Backup antes de salvar
      try{await dbSet('reparoeletro_pipe_bak_pre_arquivo',{cards:db.cards,ts:now});}catch(e){}

      await dbSet(PIPE_KEY, db);
      await dbSet(ARQUIVO_KEY, arq);

      return res.status(200).json({
        ok:true, arquivados:novos, totalNoArquivo:arq.fichas.length,
        removidosDoAtivo:idsArquivados.length,
        fichas: paraArquivar.slice(0,10).map(function(c){return {
          id:c.id, nome:c.nomeContato, movedAt:c.movedAt||c.criadoEm
        }})
      });
    } catch(e){return res.status(500).json({ok:false,error:e.message});}
  }

  // ── GET buscar-arquivo: busca fichas arquivadas ──────────────────────────
  if (action === 'buscar-arquivo') {
    var q3 = (req.query.q||'').toLowerCase();
    try {
      var arq2 = await dbGet('reparoeletro_arquivo') || {fichas:[]};
      var lista = (arq2.fichas||[]);
      if (q3) lista = lista.filter(function(f){
        return JSON.stringify(f).toLowerCase().includes(q3);
      });
      return res.status(200).json({
        ok:true, total:lista.length, totalArquivado:arq2.totalArquivado||0,
        ultimoArquivo:arq2.ultimoArquivo||null,
        fichas: lista.slice(0,50).map(function(f){return {
          id:f.id, nome:f.nomeContato, telefone:f.telefone,
          equipamento:f.equipamento, valor:f.valor,
          arquivadoEm:f.arquivadoEm, movedAt:f.movedAt,
          pipefyId:f.pipefyId
        };})
      });
    } catch(e){return res.status(500).json({ok:false,error:e.message});}
  }

  // ── GET restaurar-arquivo: devolve uma ficha arquivada para o pipe ───────
  if (action === 'restaurar-arquivo') {
    var rid = req.query.id||'';
    if (!rid) return res.status(400).json({ok:false,error:'id obrigatorio'});
    try {
      var arq3  = await dbGet('reparoeletro_arquivo') || {fichas:[]};
      var db3   = await dbGet(PIPE_KEY) || {cards:[]};
      var idx3  = (arq3.fichas||[]).findIndex(function(f){return f.id===rid;});
      if (idx3<0) return res.status(404).json({ok:false,error:'Ficha não encontrada no arquivo: '+rid});
      var ficha3 = arq3.fichas.splice(idx3,1)[0];
      ficha3.phase = 'aguardando_aprovacao';
      ficha3.restauradoEm = now;
      db3.cards.unshift(ficha3);
      await dbSet(PIPE_KEY, db3);
      await dbSet('reparoeletro_arquivo', arq3);
      return res.status(200).json({ok:true,id:ficha3.id,nome:ficha3.nomeContato,restauradoPara:'aguardando_aprovacao'});
    } catch(e){return res.status(500).json({ok:false,error:e.message});}
  }


  // ── GET erp-count: conta ERP diretamente do Redis (sem cache) ────────────
  if (action === 'erp-count') {
    res.setHeader('Cache-Control','no-store,no-cache');
    var db2 = await dbGet(PIPE_KEY);
    var tot  = (db2&&db2.cards)?db2.cards.length:0;
    var erp2 = (db2&&db2.cards)?db2.cards.filter(function(c){return c.phase==='erp';}).length:0;
    var val2 = (db2&&db2.cards)?db2.cards.filter(function(c){return c.phase==='erp';}).reduce(function(s,c){return s+(parseFloat(c.valor)||0);},0):0;
    return res.status(200).json({ok:true,erp:erp2,total:tot,valor:val2});
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
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate');
    res.setHeader('Pragma', 'no-cache');
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
    var faseAnterior = card.phase;
    card.history = (card.history || []).concat([{ phase: card.phase, ts: now }]);
    card.phase   = phase;
    card.movedAt = now;
    if (phase === 'aguardando_aprovacao') card.aguardandoDesde = now;
    await dbSet(PIPE_KEY, db);

    // ── Gatilhos downstream ──────────────────────────────────────────────
    var pid = card.pipefyId;
    // Aprovados → Board Técnico (fase: aprovado)
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
          phaseId:     'aprovado',
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
        var pipefyStr = pid ? String(pid) : null;
        var jaFinExiste = finDb2.records.find(function(r){
          return (pipefyStr && r.pipefyId === pipefyStr) || (r.osCode && r.osCode === card.id);
        });
        if (jaFinExiste) {
          // Já existe — mover para aguardando_dados
          if (jaFinExiste.phaseId !== 'aguardando_dados') {
            jaFinExiste.history = (jaFinExiste.history||[]).concat([{phaseId:jaFinExiste.phaseId,ts:now}]);
            jaFinExiste.phaseId = 'aguardando_dados';
            jaFinExiste.movedAt = now;
            await dbSet('reparoeletro_financeiro', finDb2);
          }
        } else {
          // Novo registro em aguardando_dados
          finDb2.records.unshift({
            id: 'FIN-' + (pipefyStr || card.id),
            pipefyId: pipefyStr,
            osCode: card.id,
            nomeContato: card.nomeContato || '',
            telefone: card.telefone || '',
            equipamento: card.equipamento || card.descricao || '',
            valor: card.valor || 0,
            phaseId: 'aguardando_dados',
            criadoEm: now, movedAt: now,
            history: [{ phaseId: 'aguardando_dados', ts: now }],
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

    // Log da ação
    var gatilhosLog = [];
    if (phase === 'aprovados')       gatilhosLog.push('→ Board Técnico');
    if (phase === 'analise_compra')  gatilhosLog.push('→ Compra Equip');
    if (phase === 'aguardando_aprovacao') gatilhosLog.push('Timer 48h iniciado');
    logAction({ modulo:'Pipe ADM', fichaId:card.id, ficha:card.nomeContato, acao:'Mover ficha', de:faseAnterior||'', para:phase, gatilho:gatilhosLog.join(' | '), status:'ok' }).catch(()=>{});

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
      nomeContato:     fmt4dig(body.nomeContato || '', body.telefone || ''),
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
    // Log da ação
    var gatilhosLog = [];
    if (phase === 'aprovados')       gatilhosLog.push('→ Board Técnico');
    if (phase === 'analise_compra')  gatilhosLog.push('→ Compra Equip');
    if (phase === 'aguardando_aprovacao') gatilhosLog.push('Timer 48h iniciado');
    logAction({ modulo:'Pipe ADM', fichaId:card.id, ficha:card.nomeContato, acao:'Mover ficha', de:faseAnterior||'', para:phase, gatilho:gatilhosLog.join(' | '), status:'ok' }).catch(()=>{});

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
