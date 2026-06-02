
// Helper: cria/move card no tv_pipe
async function moverNoTvPipe(phase, dados){
  try {
    const _U=(process.env.UPSTASH_URL||'').replace(/['"]/g,'').trim();
    const _T=(process.env.UPSTASH_TOKEN||'').replace(/['"]/g,'').trim();
    async function _g(k){const r=await fetch(_U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+_T,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});const j=await r.json();const v=j[0]?.result;if(!v)return null;try{let x=JSON.parse(v);if(typeof x==='string')x=JSON.parse(x);return x;}catch(e){return null;}}
    async function _s(k,v){await fetch(_U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+_T,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});}
    const pipe=(await _g('tv_pipe'))||{cards:[],lastSync:null};
    if(!Array.isArray(pipe.cards))pipe.cards=[];
    const now=new Date().toISOString();
    const jaExiste = dados.localId && pipe.cards.find(c=>c.localId===String(dados.localId)||c.id===String(dados.localId));
    if(jaExiste){ jaExiste.phase=phase; jaExiste.movedAt=now; }
    else {
      pipe.cards.unshift({
        id:'PIPE-TV-'+Date.now().toString(36).toUpperCase()+'-'+Math.random().toString(36).slice(2,5).toUpperCase(),
        localId:dados.localId||null, pipefyId:dados.pipefyId||null,
        phase, nomeContato:dados.nome||'', telefone:dados.telefone||'',
        equipamento:dados.equipamento||'', descricao:dados.descricao||'',
        endereco:dados.endereco||'', valor:parseFloat(dados.valor)||0,
        origem:dados.origem||'sistema', criadoEm:now, movedAt:now,
        aguardandoDesde:phase==='aguardando_aprovacao'?now:null,
        history:[], analiseCompra:false
      });
    }
    pipe.lastSync=now;
    await _s('tv_pipe',pipe);
  } catch(e){ console.error('[tv_pipe trigger]',e.message); }
}
'use strict';
// TV LOGISTICA — espelho do ADM | FASE 3 | 01/06/2026


// ── fmt4dig: padrão Nome 4díg do telefone ────────────────────────────────
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
    const _U=(process.env.UPSTASH_URL||'').replace(/['"]/g,'').trim();
    const _T=(process.env.UPSTASH_TOKEN||'').replace(/['"]/g,'').trim();
    const _K='tv_log';
    const _r=await fetch(_U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+_T,'Content-Type':'application/json'},body:JSON.stringify([['GET',_K]])});
    const _j=await _r.json();const _v=_j[0]?.result;
    let _log=[];if(_v){try{_log=JSON.parse(_v);if(typeof _log==='string')_log=JSON.parse(_log);}catch(e){}}if(!Array.isArray(_log))_log=[];
    _log.unshift({ts:new Date().toISOString(),modulo:entry.modulo||'—',fichaId:entry.fichaId||'',ficha:entry.ficha||'',acao:entry.acao||'',de:entry.de||'',para:entry.para||'',gatilho:entry.gatilho||'',status:entry.status||'ok',detalhe:entry.detalhe||''});
    if(_log.length>500)_log.splice(500);
    await fetch(_U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+_T,'Content-Type':'application/json'},body:JSON.stringify([['SET',_K,JSON.stringify(_log)]])});
  }catch(e){}
}


// ── Helper: mover card no Pipe ADM pelo pipefyId (sem depender do Pipefy) ──
async function moverNoPipe(pipefyId, novaFase, dados) {
  // pipefyId pode ser null quando Pipefy falhou — usar localId como fallback
  const _refId = pipefyId || (dados && dados.localId) || null;
  if (!_refId) return;
  try {
    const PIPE_KEY_H = 'tv_pipe';
    const U = (process.env.UPSTASH_URL   || '').replace(/['"]/g,'').trim();
    const T = (process.env.UPSTASH_TOKEN || '').replace(/['"]/g,'').trim();
    async function _pg(k) {
      const r = await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});
      const j = await r.json(); const v = j[0]?.result; if(!v) return null;
      let val=JSON.parse(v); if(typeof val==='string'){try{val=JSON.parse(val);}catch(e){}} return(val&&typeof val==='object')?val:null;
    }
    async function _ps(k,v){await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});}
    const db=(await _pg(PIPE_KEY_H))||{cards:[],syncedPipefyIds:[],lastSync:null};
    const refIdStr = String(_refId);
    const card=(db.cards||[]).find(c=>
      c.pipefyId===refIdStr || c.id===refIdStr ||
      (dados?.localId && (c.id===String(dados.localId) || c.pipefyId===String(dados.localId)))
    );
    const now=new Date().toISOString();
    if(!card){
      if(dados&&dados.nomeContato){
        db.cards.unshift({
          id: 'PIPE-' + Date.now().toString(36).toUpperCase() + '-' + Math.random().toString(36).slice(2,5).toUpperCase(),
          pipefyId: pipefyId ? String(pipefyId) : null,
          localId:  dados.localId ? String(dados.localId) : null,
          phase: novaFase,
          nomeContato: dados.nomeContato||'',
          telefone: dados.telefone||'',
          equipamento: dados.equipamento||'',
          descricao: dados.descricao||'',
          endereco: dados.endereco||'',
          valor: parseFloat(dados.valor||0)||0,
          origem: dados.origem||'sistema',
          criadoEm: now, movedAt: now,
          aguardandoDesde: novaFase==='aguardando_aprovacao' ? now : null,
          history:[], analiseCompra:false
        });
        await _ps(PIPE_KEY_H,db);
      }
      return;
    }
    card.history=(card.history||[]).concat([{phase:card.phase,ts:now}]);
    card.phase=novaFase; card.movedAt=now;
    if(novaFase==='aguardando_aprovacao') card.aguardandoDesde=now;
    if(dados){if(dados.valor!==undefined)card.valor=parseFloat(dados.valor)||0;if(dados.nomeContato)card.nomeContato=dados.nomeContato;}
    await _ps(PIPE_KEY_H,db);
  } catch(e){console.error('[pipe-mover]',novaFase,e.message);}
}

// api/logistica.js — Sistema de Logística de Coleta
const U = process.env.UPSTASH_URL;
const T = process.env.UPSTASH_TOKEN;
const LOG_KEY = 'tv_logistica';

async function dbGet(key) {
  try {
    const r = await fetch(`${U}/get/${key}`, { headers: { Authorization: `Bearer ${T}` } });
    const j = await r.json();
    return j.result ? JSON.parse(j.result) : null;
  } catch(e) { return null; }
}
async function dbSet(key, val) {
  try {
    await fetch(`${U}/set/${key}`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${T}`, 'Content-Type': 'application/json' },
      body: JSON.stringify(val)
    });
    return true;
  } catch(e) { return false; }
}

function defaultDB() { return { fichas: [], nextId: 1 }; }


async function registrarPassagem(phase) {
  try {
    const hoje = new Date().toLocaleDateString('pt-BR', {timeZone:'America/Sao_Paulo'}).split('/').reverse().join('-');
    const db   = (await dbGet('tv_log_metricas')) || {};
    if (!db[hoje]) db[hoje] = {};
    db[hoje][phase] = (db[hoje][phase] || 0) + 1;
    const cutoff = new Date(); cutoff.setDate(cutoff.getDate() - 30);
    Object.keys(db).forEach(d => { if (new Date(d) < cutoff) delete db[d]; });
    await dbSet('tv_log_metricas', db);
  } catch(e) { console.error('registrarPassagem:', e.message); }
}


// ── PIPEFY: criar card direto em Aguardando Aprovação ────────
// Padrão idêntico ao api/orcamento.js → createPipefyCard
const PIPE_ID             = '305832912';
const AGUARDANDO_PHASE_ID = '334875152';

async function pipefyQuery() {
  // Pipefy desconectado — TV opera 100% local (Redis)
  return null;
}

// Busca APENAS start_form_fields — mesma lógica do orcamento.js
let _pipeStructure = null;
async function fetchPipeStructure() {
  if (_pipeStructure) return _pipeStructure;
  const data = await pipefyQuery(`query {
    pipe(id: "${PIPE_ID}") {
      phases { id name }
      start_form_fields { id label type }
    }
  }`).catch(()=>{});
  _pipeStructure = {
    phases: data?.pipe?.phases || [],
    fields: data?.pipe?.start_form_fields || [],
  };
  return _pipeStructure;
}

async function criarCardPipefy() { return null; }


// ── Pipefy é ESPELHO — nunca bloqueia o fluxo local ─────────────────────
async function pipefyBestEffort(fn) {
  try { return await fn(); } catch(e) { console.warn('[Pipefy]', e.message); return null; }
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const action = req.query.action;

  // ── GET load ──────────────────────────────────────────────
  if (action === 'load') {
    const db = await dbGet(LOG_KEY) || defaultDB();
    return res.status(200).json({ ok: true, fichas: db.fichas || [] });
  }



  // ── GET buscar-ficha: encontra ficha por nome ou id ──────────
  if (action === 'buscar-ficha') {
    const q = (req.query.q || '').toLowerCase().trim();
    if (!q) return res.status(400).json({ ok: false, error: 'q obrigatorio' });
    const db = await dbGet(LOG_KEY) || defaultDB();
    const encontradas = db.fichas.filter(f =>
      (f.nome    || '').toLowerCase().includes(q) ||
      (f.id      || '').toLowerCase().includes(q) ||
      (f.telefone|| '').includes(q)
    );
    // Incluir resultado do último fix para diagnóstico
    const fixResult = await dbGet('fix_clarice_result').catch(()=>null);
    return res.status(200).json({ ok: true, total: encontradas.length, fichas: encontradas, fixResult });
  }


  // ── GET listar-sem-pipefy — fichas em orc_registrado sem card no Pipefy ──
  if (action === 'listar-sem-pipefy') {
    const db = await dbGet(LOG_KEY) || defaultDB();
    const sem = db.fichas.filter(f =>
      !f.pipefyCardId && (f.phase === 'orc_registrado' || f.pipefyErro)
    ).map(f => ({
      id:         f.id,
      nome:       f.nome,
      equipamento:f.equipamento || '',
      fase:       f.phase,
      pipefyErro: f.pipefyErro || null,
      criadoEm:   f.criadoEm || ''
    }));
    return res.status(200).json({ ok:true, total: sem.length, fichas: sem });
  }

  // ── GET forcar-pipefy-todos — cria card no Pipefy para todas fichas pendentes ──
  if (action === 'forcar-pipefy-todos') {
    const db     = await dbGet(LOG_KEY) || defaultDB();
    const pendentes = db.fichas.filter(f => !f.pipefyCardId && f.diagnostico);
    const resultado = [];
    for (const ficha of pendentes) {
      try {
        const card = await criarCardPipefy({
          nome:        ficha.nome,
          telefone:    ficha.telefone || '',
          equipamento: ficha.equipamento || '',
          defeito:     ficha.defeito || '',
          endereco:    ficha.endereco || ''
        });
        if (card?.id) {
          ficha.pipefyCardId = String(card.id);
          ficha.pipefyErro   = null;
          const precoFinal = ficha.diagnostico?.preco;
          if (precoFinal) {
            await pipefyQuery(
              `mutation { updateCardField(input: { card_id: "${card.id}", field_id: "valor_de_contrato", new_value: "${precoFinal}" }) { success } }`
            ).catch(() => {});
          }
          await pipefyQuery(
            `mutation { moveCardToPhase(input: { card_id: "${card.id}", destination_phase_id: "${AGUARDANDO_PHASE_ID}" }) { card { id } } }`
          ).catch(() => {});
          // Atualizar reparoeletro_orcamentos para evitar duplicata pelo orc-sync
          try {
            const ORC_KEY2 = 'tv_orcamentos';
            const orcDb2 = (await dbGet(ORC_KEY2)) || { fichas:[], syncedIds:[], initialized:true };
            const orcIdx = orcDb2.fichas.findIndex(f => f.id === ficha.id || f.pipefyId === ficha.id);
            if (orcIdx >= 0) {
              orcDb2.fichas[orcIdx].id       = String(card.id);
              orcDb2.fichas[orcIdx].pipefyId = String(card.id);
            } else {
              // Ainda não está em orcamentos — adicionar agora
              orcDb2.fichas.unshift({
                id: String(card.id), pipefyId: String(card.id),
                nome: ficha.nome, tel: ficha.telefone||'',
                desc: (ficha.equipamento||'') + ' — ' + (ficha.defeito||''),
                end: ficha.endereco||'', textoOrc: ficha.diagnostico?.textoOrc||'',
                precoSugerido: precoFinal||null, status:'pendente', preco:null,
                createdAt: new Date().toISOString(),
              });
            }
            if (!orcDb2.syncedIds.includes(String(card.id))) {
              orcDb2.syncedIds.push(String(card.id));
            }
            await dbSet(ORC_KEY2, orcDb2);
          } catch(oe) { console.error('[Log] forcar sync orc-key:', oe.message); }
          resultado.push({ id: ficha.id, nome: ficha.nome, pipefyCardId: card.id, ok: true });
        } else {
          resultado.push({ id: ficha.id, nome: ficha.nome, ok: false, erro: 'card sem id' });
        }
      } catch(e) {
        ficha.pipefyErro   = e.message;
        ficha.pipefyErroTs = new Date().toISOString();
        resultado.push({ id: ficha.id, nome: ficha.nome, ok: false, erro: e.message });
      }
    }
    await dbSet(LOG_KEY, db);
    const ok  = resultado.filter(r => r.ok).length;
    const err = resultado.filter(r => !r.ok).length;
    return res.status(200).json({ ok: true, total: pendentes.length, criados: ok, erros: err, resultado });
  }

  // ── GET retry-pipefy: tenta criar card no Pipefy para ficha sem pipefyCardId ──
  if (action === 'retry-pipefy') {
    const id = req.query.id;
    if (!id) return res.status(400).json({ ok: false, error: 'id obrigatorio' });
    const db = await dbGet(LOG_KEY) || defaultDB();
    const ficha = db.fichas.find(f => f.id === id);
    if (!ficha) return res.status(404).json({ ok: false, error: 'ficha nao encontrada' });
    if (ficha.pipefyCardId) {
      return res.status(200).json({ ok: true, info: 'ja tem pipefyCardId', pipefyCardId: ficha.pipefyCardId });
    }
    try {
      const card = await criarCardPipefy({
        nome:        ficha.nome,
        telefone:    ficha.telefone || '',
        equipamento: ficha.equipamento || '',
        defeito:     ficha.defeito || '',
        endereco:    ficha.endereco || ''
      });
      // Pipefy best-effort: continua mesmo sem card
      if (!card?.id) {
        console.warn('[log] Pipefy nao retornou id — salvando ficha sem pipefyCardId');
      } else {
        ficha.pipefyCardId = String(card.id);
      }
      await dbSet(LOG_KEY, db);
      // Atualizar valor se tiver diagnóstico
      const precoFinal = ficha.diagnostico?.preco;
      if (precoFinal) {
        await pipefyQuery(`mutation { updateCardField(input: { card_id: "${card.id}", field_id: "valor_de_contrato", new_value: "${precoFinal}" }) { success } }`).catch(()=>{});
      }
      // Mover para Aguardando Aprovação
      await pipefyQuery(`mutation { moveCardToPhase(input: { card_id: "${card.id}", destination_phase_id: "${AGUARDANDO_PHASE_ID}" }) { card { id } } }`).catch(()=>{});
      return res.status(200).json({ ok: true, pipefyCardId: card.id, url: card.url, nome: ficha.nome });
    } catch(e) {
      return res.status(500).json({ ok: false, error: e.message });
    }
  }

  // ── GET metricas ─────────────────────────────────────────────
  if (action === 'metricas') {
    const MET_KEY = 'tv_log_metricas';
    const met = (await dbGet(MET_KEY)) || {};
    const hoje = new Date().toLocaleDateString('pt-BR', {timeZone:'America/Sao_Paulo'}).split('/').reverse().join('-');

    // Backfill: se hoje nao tem dados, semear com fichas em cada coluna agora
    // (baseline do dia — a partir daqui cada movimentacao acumula por cima)
    if (!met[hoje] || !Object.keys(met[hoje]).length) {
      const fichasDb = await dbGet(LOG_KEY) || defaultDB();
      met[hoje] = {};
      for (const f of fichasDb.fichas || []) {
        if (f.phase) {
          met[hoje][f.phase] = (met[hoje][f.phase] || 0) + 1;
        }
      }
      await dbSet(MET_KEY, met);
    }

    return res.status(200).json({ ok: true, metricas: met });
  }

  // ── POST criar ────────────────────────────────────────────
  if (req.method === 'POST' && action === 'criar') {
    const { nome, telefone, endereco, equipamento, defeito, pipefyCardId, texto } = req.body || {};
    if (!nome) return res.status(400).json({ ok: false, error: 'nome obrigatorio' });

    const db = await dbGet(LOG_KEY) || defaultDB();
    const id = 'LOG-' + String(db.nextId || 1).padStart(4, '0');
    const ficha = {
      id, nome, telefone: telefone || '', endereco: endereco || '',
      equipamento: equipamento || '', defeito: defeito || '',
      pipefyCardId: pipefyCardId || null, texto: texto || '',
      phase: 'liberado_coleta',
      criadoEm: new Date().toISOString(),
      movedAt: new Date().toISOString(),
      diagnostico: null,
    };
    db.fichas.unshift(ficha);
    db.nextId = (db.nextId || 1) + 1;
    await dbSet(LOG_KEY, db);
    registrarPassagem('liberado_coleta').catch(() => {});
    return res.status(201).json({ ok: true, ficha });
  }

  // ── POST mover ────────────────────────────────────────────
  if (req.method === 'POST' && action === 'mover') {
    const { id, phase } = req.body || {};
    const PHASES = ['liberado_coleta','horario_marcado','liberado_para_rota','motorista_parceiro','remarcar','coleta_efetuada','orc_registrado'];
    if (!id || !PHASES.includes(phase)) return res.status(400).json({ ok: false, error: 'invalido' });

    const db = await dbGet(LOG_KEY) || defaultDB();
    const ficha = db.fichas.find(f => f.id === id);
    if (!ficha) return res.status(404).json({ ok: false, error: 'nao encontrada' });
    ficha.phase = phase;
    ficha.movedAt = new Date().toISOString();
    await dbSet(LOG_KEY, db);
    registrarPassagem(phase).catch(() => {});

    // Trigger: liberado_para_rota -> tv_board
    if (phase === 'liberado_para_rota') {
      try {
        const _bU=(process.env.UPSTASH_URL||'').replace(/['"]/g,'').trim();
        const _bT=(process.env.UPSTASH_TOKEN||'').replace(/['"]/g,'').trim();
        async function _bgL(k){const r=await fetch(_bU+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+_bT,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});const j=await r.json();const v=j[0]?.result;if(!v)return null;try{let x=JSON.parse(v);if(typeof x==='string')x=JSON.parse(x);return x;}catch(e){return null;}}
        async function _bsL(k,v){await fetch(_bU+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+_bT,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});}
        const boardDb=(await _bgL('tv_board'))||{cards:[],syncedIds:[],movesLog:[],metaLog:[]};
        if(!Array.isArray(boardDb.cards)) boardDb.cards=[];
        const boardPid=ficha.pipefyCardId?String(ficha.pipefyCardId):('LOG-'+ficha.id);
        boardDb.cards=boardDb.cards.filter(function(x){return x.pipefyId!==boardPid&&x.osCode!==ficha.id;});
        const nowB=new Date().toISOString();
        boardDb.cards.unshift({
          pipefyId:boardPid, phaseId:'liberado_rota',
          nomeContato:ficha.nome||'', title:ficha.equipamento||ficha.nome||'',
          telefone:ficha.telefone||'', descricao:ficha.equipamento||'',
          endereco:ficha.endereco||'', osCode:ficha.id,
          valor:ficha.valor||0, movedBy:'TV Logistica',
          localOnly:!ficha.pipefyCardId, syncedAt:nowB, movedAt:nowB
        });
        if(!Array.isArray(boardDb.syncedIds)) boardDb.syncedIds=[];
        if(!boardDb.syncedIds.includes(boardPid)) boardDb.syncedIds.push(boardPid);
        await _bsL('tv_board', boardDb);
        console.log('[tv_logistica->tv_board] liberado_para_rota:', boardPid);
      } catch(eLR){ console.error('[trigger lib_rota]', eLR.message); }
    }

    return res.status(200).json({ ok: true, ficha });
  }


  // ── POST mover-motorista: move para Motorista Parceiro salvando o nome ──
  if (req.method === 'POST' && action === 'mover-motorista') {
    const { id, motoristaNome } = req.body || {};
    if (!id || !motoristaNome) return res.status(400).json({ ok: false, error: 'id e motoristaNome obrigatorios' });
    const db = await dbGet(LOG_KEY) || defaultDB();
    const ficha = db.fichas.find(f => f.id === id);
    if (!ficha) return res.status(404).json({ ok: false, error: 'nao encontrada' });
    ficha.phase        = 'motorista_parceiro';
    ficha.motoristaNome = motoristaNome.trim();
    ficha.movedAt      = new Date().toISOString();
    await dbSet(LOG_KEY, db);
    registrarPassagem('motorista_parceiro').catch(() => {});
    return res.status(200).json({ ok: true, ficha });
  }


  // ── POST marcar-horario ───────────────────────────────────────
  if (req.method === 'POST' && action === 'marcar-horario') {
    const { id, horario } = req.body || {};
    if (!id || !horario) return res.status(400).json({ ok: false, error: 'id e horario obrigatorios' });
    const db = await dbGet(LOG_KEY) || defaultDB();
    const ficha = db.fichas.find(f => f.id === id);
    if (!ficha) return res.status(404).json({ ok: false, error: 'nao encontrada' });
    ficha.phase          = 'horario_marcado';
    ficha.horarioColeta  = horario; // ISO datetime string
    ficha.movedAt        = new Date().toISOString();
    await dbSet(LOG_KEY, db);
    registrarPassagem('horario_marcado').catch(() => {});
    return res.status(200).json({ ok: true, ficha });
  }

  // ── POST atualizar-dados ──────────────────────────────────
  if (req.method === 'POST' && action === 'atualizar-dados') {
    const { id, nome, telefone, endereco, equipamento, defeito } = req.body || {};
    const db = await dbGet(LOG_KEY) || defaultDB();
    const ficha = db.fichas.find(f => f.id === id);
    if (!ficha) return res.status(404).json({ ok: false, error: 'nao encontrada' });
    if (nome)       ficha.nome = nome;
    if (telefone)   ficha.telefone = telefone;
    if (endereco)   ficha.endereco = endereco;
    if (equipamento) ficha.equipamento = equipamento;
    if (defeito)    ficha.defeito = defeito;
    await dbSet(LOG_KEY, db);
    // Trigger: tv_pipe → aguardando_aprovacao
    await moverNoTvPipe('aguardando_aprovacao', {
      localId: ficha.id||null, pipefyId: ficha.pipefyCardId||null,
      nome: ficha.nome||ficha.nomeContato||'',
      telefone: ficha.telefone||'',
      equipamento: ficha.equipamento||ficha.aparelho||'',
      descricao: ficha.defeito||ficha.descricao||'',
      endereco: ficha.endereco||'', valor: ficha.valor||0,
      origem: 'tv_logistica_orcamento'
    });
    return res.status(200).json({ ok: true, ficha });
  }

  // ── POST salvar-diagnostico ───────────────────────────────
  if (req.method === 'POST' && action === 'salvar-diagnostico') {
    const { id, diagnostico } = req.body || {};
    const db = await dbGet(LOG_KEY) || defaultDB();
    const ficha = db.fichas.find(f => f.id === id);
    if (!ficha) return res.status(404).json({ ok: false, error: 'nao encontrada' });
    ficha.diagnostico = diagnostico;
    // Não mover para orc_registrado aqui — a fase muda em gerar-orcamento
    // (só quando o Pipefy for criado/movido com sucesso)
    await dbSet(LOG_KEY, db);
    return res.status(200).json({ ok: true, ficha });
  }


  // ── POST gerar-orcamento — gera texto, salva no orc e move Pipefy ──
  if (req.method === 'POST' && action === 'gerar-orcamento') {
    const { id } = req.body || {};
    const db = await dbGet(LOG_KEY) || defaultDB();
    const ficha = db.fichas.find(f => f.id === id);
    if (!ficha) return res.status(404).json({ ok:false, error:'ficha nao encontrada' });
    if (!ficha.diagnostico) return res.status(400).json({ ok:false, error:'sem diagnostico' });

    const ORC_KEY = 'tv_orcamentos';

    // Gerar textos para cada equipamento do diagnóstico
    const equips = ficha.diagnostico.equips || [ficha.diagnostico];
    const nome   = ficha.nome || '';

    function priNome(n) { return n ? n.trim().split(/\s+/)[0] : 'cliente'; }

    function gerarTexto(tipo, subtipo, servicos, precoInput, templates, modelo) {
      const pn = priNome(nome);
      const s  = servicos || [];
      const tem = (lista) => s.some(x => lista.includes(x));
      const pecas = (lista) => s.filter(x => lista.includes(x)).join(', ') || s.join(', ');
      const x2 = (v) => String(Math.round(parseFloat(v||0)*2));
      const T = templates || {};
      // Substituir placeholders num template
      function applyTpl(tpl, pecasStr, preco) {
        return tpl
          .replace(/\[NOME\]/g, pn)
          .replace(/\[peças\]/g, pecasStr || s.join(', '))
          .replace(/\[VALOR\]/g, preco || '');
      }


      if (tipo === 'tv') {
        const chips   = servicos || [];
        const pn      = priNome(nome);

        // ── Extrair polegada: modelo → equipamento → defeito da ficha ─────
        const fontesBusca = [
          modelo || '',
          ficha.equipamento || '',
          ficha.defeito || '',
        ].join(' ');

        function extrairPol(txt) {
          // "65 pol", "65"", "55 polegadas", "Samsung 55 Smart", "UN65RU7100"
          const patterns = [
            /\b([3-7]\d)\s*(?:pol(?:egadas?)?|")/i,  // 65 pol / 65"
            /[Uu][Nn]([3-7]\d)/,                       // UN55/UN65 Samsung
            /\b([3-7]\d)\b/,                           // número solto 30-79
          ];
          for (const re of patterns) {
            const m = txt.match(re);
            if (m) { const v = parseInt(m[1]); if (v>=30&&v<=79) return v; }
          }
          return null;
        }
        const pol = extrairPol(fontesBusca);

        // ── Tabela de preços por polegada ─────────────────────────────────
        const TAB_PRECO = [
          {min:30,max:39,p:'490'}, {min:40,max:49,p:'690'},
          {min:50,max:59,p:'890'}, {min:60,max:69,p:'1490'},
          {min:70,max:79,p:'1990'},
        ];
        let precoTab = null;
        if (pol) { for (const f of TAB_PRECO) { if(pol>=f.min&&pol<=f.max){precoTab=f.p;break;} } }

        // Fallback: se não achou por polegada mas há preço manual no eq.preco (fichas antigas)
        const precoManual = parseFloat(precoInput) > 0 ? String(Math.round(parseFloat(precoInput))) : null;
        const precoStr    = precoTab || precoManual || '[VALOR]';
        const acrilicoVal = parseFloat(precoInput) || 0; // para chip acrílico

        // ── CONDENADA ─────────────────────────────────────────────────────
        if (chips.includes('condenada')) {
          return {
            texto: `Olá, bom dia ${pn}, fizemos todos os testes e identificamos que infelizmente não tem conserto viável a TV. Caso queira ela de volta me fala que providencio a entrega.`,
            preco: null,
          };
        }

        // ── BARRAMENTO e/ou PLACA (± RISCO, ± ACRILICO) ──────────────────
        const temBarramento = chips.includes('barramento');
        const temPlaca      = chips.includes('placa');
        const temRisco      = chips.includes('risco');
        const temAcrilico   = chips.includes('acrilico');

        if (temBarramento || temPlaca) {
          const peca = (temBarramento && temPlaca) ? 'barramento e placa'
                     : temBarramento ? 'barramento' : 'placa';

          let texto = `Olá, ${pn}, bom dia! Sou o Alessandro da Reparo Eletro, vou te enviar agora o orçamento:\n\nForam feitos todos os testes, identificamos que será necessário fazer a troca do ${peca} da TV, será feito a reoperação elétrica também. Este conserto completo fica em ${precoStr} reais apenas. Aprovando já iniciamos o conserto.`;

          if (temRisco) {
            texto += `\n\nObs.: Devido às condições da placa do equipamento preciso comunicar o risco de ao trabalhar nela o curto progredir e infelizmente ela apagar completamente. São poucos os casos mas existe esse risco.`;
          }

          if (temAcrilico && acrilicoVal > 0) {
            texto += `\n\nDevido ao superaquecimento dos barramentos o acrílico pode ressecar e ter pequenas rachaduras, o que faz aparecer pequenas rajadas de luz quando a TV está com cores mais claras. Sem trocar o acrílico você pode considerar uma qualidade de 80 a 90%. Trocando o Acrílico fica 100% e tem um custo adicional de ${acrilicoVal} reais. Aguardo sua resposta.`;
          }

          return { texto, preco: precoTab };
        }

        // ── APENAS RISCO (sem barramento/placa) ───────────────────────────
        if (temRisco) {
          let texto = `Olá, ${pn}, bom dia! Sou o Alessandro da Reparo Eletro, vou te enviar agora o orçamento:\n\nForam feitos todos os testes, identificamos um problema no conjunto eletrônico da TV. Este conserto completo fica em ${precoStr} reais apenas.\n\nObs.: Devido às condições da placa do equipamento preciso comunicar o risco de ao trabalhar nela o curto progredir e infelizmente ela apagar completamente. São poucos os casos mas existe esse risco. Aprovando já iniciamos o conserto.`;

          if (temAcrilico && acrilicoVal > 0) {
            texto += `\n\nDevido ao superaquecimento dos barramentos o acrílico pode ressecar e ter pequenas rachaduras, o que faz aparecer pequenas rajadas de luz quando a TV está com cores mais claras. Sem trocar o acrílico você pode considerar uma qualidade de 80 a 90%. Trocando o Acrílico fica 100% e tem um custo adicional de ${acrilicoVal} reais. Aguardo sua resposta.`;
          }

          return { texto, preco: precoTab };
        }

        // ── APENAS ACRILICO ───────────────────────────────────────────────
        if (temAcrilico) {
          return {
            texto: `Olá, ${pn}, bom dia! Sou o Alessandro da Reparo Eletro.\n\nDevido ao superaquecimento dos barramentos o acrílico pode ressecar e ter pequenas rachaduras, o que faz aparecer pequenas rajadas de luz quando a TV está com cores mais claras. Sem trocar o acrílico você pode considerar uma qualidade de 80 a 90%. Trocando o Acrílico fica 100% e tem um custo adicional de ${acrilicoVal > 0 ? acrilicoVal : '[VALOR]'} reais. Aguardo sua resposta.`,
            preco: acrilicoVal ? String(acrilicoVal) : null,
          };
        }

        return { texto: null, preco: null };
      }

      if (tipo === 'microondas') {
        if (tem(['Troca de Placa','Display'])) {
          const p = pecas(['Troca de Placa','Display']);
          const tpl = T.microondas_placa?.texto || `Ola, [NOME] bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario fazer a troca do conjunto conjunto da [peças], será feito a reoperação eletrica tambem. Este conserto completo fica em [VALOR] reais apenas. Aprovando ja iniciamos o conserto.`;
          return { texto: applyTpl(tpl, p, x2(precoInput)), preco:x2(precoInput) };
        }
        if (tem(['Vidro','Porta'])) {
          const p = pecas(['Vidro','Porta']);
          const tpl = T.microondas_vidro?.texto || `Ola, [NOME] bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orçamento:\n\nPara fazer a desmontagem, instalação da [peças], montagem e regulagem consigo fazer para você por [VALOR] reais apenas. Aprovando ja iniciamos o conserto.`;
          return { texto: applyTpl(tpl, p, x2(precoInput)), preco:x2(precoInput) };
        }
        if (tem(['Haste'])) { const tpl = T.microondas_haste?.texto || `Ola, [NOME] bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orçamento:\n\nPara fazer a desmontagem, instalação da haste, montagem e regulagem consigo fazer para você por [VALOR] reais apenas. Aprovando ja iniciamos o conserto.`; return { texto: applyTpl(tpl, 'haste', '350'), preco:'350' }; }
        if (tem(['Pintura'])) { const tpl = T.microondas_pintura?.texto || `Ola, [NOME] bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orçamento:\n\nPara fazer a desmontagem, pintura, montagem, regulagem e revisão consigo fazer para você por [VALOR] reais apenas. Aprovando ja iniciamos o conserto.`; return { texto: applyTpl(tpl, 'pintura', '350'), preco:'350' }; }
        if (tem(['Magnetron'])) {
          const tpl = T.microondas_magnetron?.texto || `Ola, [NOME] bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario fazer a troca do Magnetron, peca responsavel pelo aquecimento do aparelho. Este conserto completo fica em [VALOR] reais apenas. Aprovando ja iniciamos o conserto.`;
          return { texto: applyTpl(tpl, 'Magnetron', '390'), preco:'390' };
        }
        const p = s.join(', ');
        const tpl = T.microondas_eletrico?.texto || `Ola, [NOME] bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario refazer a parte eletrica que causou danos no conjunto do [peças], as pecas serao trocadas tambem. Este conserto completo fica em [VALOR] reais apenas. Aprovando ja iniciamos o conserto.`;
        return { texto: applyTpl(tpl, p, '350'), preco:'350' };
      }
      if (tipo === 'purificador') {
        if (subtipo === 'Motor') {
          if (tem(['Gás'])) return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario fazer a troca da valvula de gas, solda e recarga de gas refrigerante. Este conserto completo fica em 490 reais apenas. Aprovando ja iniciamos o conserto.`, preco:'490' };
          const p = s.join(', ');
          return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario fazer a troca do conjunto da ${p}. Este conserto completo fica em 490 reais apenas. Aprovando ja iniciamos o conserto.`, preco:'490' };
        }
        if (subtipo === 'Eletrônico') {
          if (tem(['Kit Termo Elétrico'])) return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario refazer a parte eletrica que causou danos no conjunto do cooler, placa de resfriamento e pasta termica, as pecas serao trocadas tambem. Este conserto completo fica em 350 reais apenas. Aprovando ja iniciamos o conserto.`, preco:'350' };
          if (tem(['Recuperação de Placa'])) return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario refazer a parte eletrica que causou danos no conjunto da placa principal, será feito a reoperação da placa tambem. Este conserto completo fica em 350 reais apenas. Aprovando ja iniciamos o conserto.`, preco:'350' };
          const p = s.join(', ');
          return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario fazer a troca do conjunto da ${p}. Este conserto completo fica em 350 reais apenas. Aprovando ja iniciamos o conserto.`, preco:'350' };
        }
      }
      if (tipo === 'adega') {
        if (subtipo === 'Motor') {
          if (tem(['Gás'])) return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario fazer a troca da valvula de gas, solda e recarga de gas refrigerante. Este conserto completo fica em 490 reais apenas. Aprovando ja iniciamos o conserto.`, preco:'490' };
          if (tem(['Recuperação de Placa'])) return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario refazer a parte eletrica que causou danos no conjunto da placa principal, será feito a reoperação da placa tambem. Este conserto completo fica em 490 reais apenas. Aprovando ja iniciamos o conserto.`, preco:'490' };
          if (tem(['Troca de Placa'])) return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario fazer a troca do conjunto conjunto da Placa Principal, será feito a reoperação eletrica tambem. Este conserto completo fica em ${x2(precoInput)} reais apenas. Aprovando ja iniciamos o conserto.`, preco:x2(precoInput) };
        }
        if (subtipo === 'Eletrônico') {
          if (tem(['Kit Termo Elétrico'])) return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario refazer a parte eletrica que causou danos no conjunto do cooler, placa de resfriamento e pasta termica, as pecas serao trocadas tambem. Este conserto completo fica em 350 reais apenas. Aprovando ja iniciamos o conserto.`, preco:'350' };
          if (tem(['Recuperação de Placa'])) return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario refazer a parte eletrica que causou danos no conjunto da placa principal, será feito a reoperação da placa tambem. Este conserto completo fica em 350 reais apenas. Aprovando ja iniciamos o conserto.`, preco:'350' };
          if (tem(['Troca de Placa'])) return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario fazer a troca do conjunto conjunto da Placa Principal, será feito a reoperação eletrica tambem. Este conserto completo fica em ${x2(precoInput)} reais apenas. Aprovando ja iniciamos o conserto.`, preco:x2(precoInput) };
        }
      }
      if (tipo === 'forno') {
        const pb = subtipo === 'Grande' ? '790' : '490';
        if (tem(['Troca de Placa','Display'])) {
          const p = pecas(['Troca de Placa','Display']);
          return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario fazer a troca do conjunto conjunto do ${p}: será feito a reoperação eletrica tambem. Este conserto completo fica em ${x2(precoInput)} reais apenas. Aprovando ja iniciamos o conserto.`, preco:x2(precoInput) };
        }
        if (tem(['Porta','Vidro','Mola'])) {
          const p = pecas(['Porta','Vidro','Mola']);
          return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orçamento:\n\nPara fazer a desmontagem, instalação da ${p}, montagem e regulagem consigo fazer para você por ${x2(precoInput)} reais apenas. Aprovando ja iniciamos o conserto.`, preco:x2(precoInput) };
        }
        const p = s.join(', ');
        return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario refazer a parte eletrica que causou danos no conjunto da ${p}, será feito a reoperação da placa tambem. Este conserto completo fica em ${pb} reais apenas. Aprovando ja iniciamos o conserto.`, preco:pb };
      }
      return { texto: null, preco: null };
    }

    // Carregar templates customizados do Redis
    let customTemplates = {};
    try {
      const tplDb = await dbGet('tv_orc_templates');
      if (tplDb) customTemplates = tplDb;
    } catch(e) { console.error('[Log] templates:', e.message); }

    // Gerar texto para cada equipamento
    const resultados = equips.map(eq =>
      gerarTexto(eq.tipo, eq.subtipo, eq.servicos, eq.preco, customTemplates, eq.modelo)
    );

    // Texto final
    let textoFinal, precoFinal;
    if (resultados.length === 1) {
      textoFinal = resultados[0].texto;
      precoFinal = resultados[0].preco;
    } else {
      const qtd      = resultados.length;
      const soma     = resultados.reduce((acc,r)=>acc+parseInt(r.preco||0),0);
      const descPerc = qtd === 2 ? 0.10 : qtd === 3 ? 0.15 : 0.20; // max 20%
      const comDesc  = Math.round(soma * (1 - descPerc));
      precoFinal     = String(comDesc);

      // Remover "Aprovando ja iniciamos o conserto" de cada texto individual
      const removeAprovando = (txt) =>
        (txt||'').replace(/\s*Aprovando ja iniciamos o conserto\.?/gi, '').trimEnd();

      // Montar textos individuais sem a frase final
      const partes = resultados.map((r,i) =>
        `Equipamento ${i+1}:\n${removeAprovando(r.texto||'')}`
      ).join('\n\n');

      // Frase de desconto no final
      const fraseFinal = `Consertando os ${qtd} juntos eu consigo um desconto para voce de ${soma} para ${comDesc} reais. Aprovando ja iniciamos o conserto.`;
      textoFinal = partes + '\n\n' + fraseFinal;
    }

    // Salvar na Logística
    ficha.diagnostico.textoOrc = textoFinal;
    ficha.diagnostico.preco    = precoFinal;
    ficha.phase   = 'orc_registrado';
    ficha.movedAt = new Date().toISOString();
    await dbSet(LOG_KEY, db);

    // Salvar no Redis de orçamentos (ORC_KEY) — formato compatível com orc-sync
    try {
      const orcDb = (await dbGet(ORC_KEY)) || { fichas:[], syncedIds:[], initialized:true };
      const orcFicha = {
        id:            ficha.pipefyCardId || ficha.id,
        pipefyId:      ficha.pipefyCardId || ficha.id,
        nome:          ficha.nome,
        tel:           ficha.telefone || '',
        desc:          ficha.equipamento + ' — ' + ficha.defeito,
        end:           ficha.endereco || '',
        age:           null,
        comentarios:   [],
        textoOrc:      textoFinal,
        precoSugerido: precoFinal,
        status:        'pendente',
        preco:         null,
        createdAt:     new Date().toISOString(),
      };
      // Evitar duplicata
      if (!orcDb.fichas.find(f => f.id === orcFicha.id)) {
        orcDb.fichas.unshift(orcFicha);
        if (ficha.pipefyCardId && !orcDb.syncedIds.includes(ficha.pipefyCardId)) {
          orcDb.syncedIds.push(ficha.pipefyCardId);
        }
        await dbSet(ORC_KEY, orcDb);
      }
    } catch(e) { console.error('[Log] orc-key:', e.message); }

    // ── Pipe ADM: criar/atualizar card em aguardando_aprovacao (SEM Pipefy) ─────
    const _pipId  = ficha.pipefyCardId || null;
    const _nome   = fmt4dig(ficha.nome || ficha.nomeContato || '', ficha.telefone||'');
    const _tel    = ficha.telefone || '';
    const _equip  = ficha.equipamento || '';
    const _desc   = ficha.defeito || '';
    const _valor  = parseFloat(precoFinal) || 0;
    await moverNoPipe(_pipId, 'aguardando_aprovacao', {
      nomeContato: _nome, telefone: _tel,
      equipamento: _equip, descricao: _desc,
      valor: _valor, origem: 'logistica',
      endereco: ficha.endereco || '',
      localId: ficha.id  // fallback quando não há pipefyCardId
    }).catch(e => console.error('[Log→Pipe]', e.message));

    // Mover ou CRIAR card no Pipefy em Aguardando Aprovação
    try {
      if (ficha.pipefyCardId) {
        // Card já existe — só mover e atualizar valor
        await pipefyQuery(`mutation { moveCardToPhase(input: { card_id: "${ficha.pipefyCardId}", destination_phase_id: "${AGUARDANDO_PHASE_ID}" }) { card { id } } }`).catch(()=>{});
        if (precoFinal) {
          await pipefyQuery(`mutation { updateCardField(input: { card_id: "${ficha.pipefyCardId}", field_id: "valor_de_contrato", new_value: "${precoFinal}" }) { success } }`).catch(()=>{});
        }
        console.log('[Log] Pipefy movido para Aguardando:', ficha.pipefyCardId);
      } else {
        // Ficha manual — criar card novo direto em Aguardando Aprovação
        const card = await criarCardPipefy({
          nome:        ficha.nome,
          telefone:    ficha.telefone || '',
          equipamento: ficha.equipamento || '',
          defeito:     ficha.defeito || '',
          endereco:    ficha.endereco || ''
        });
        if (card?.id) {
          // Salvar o pipefyCardId na ficha para uso futuro
          ficha.pipefyCardId = String(card.id);
          await dbSet(LOG_KEY, db);
          // Atualizar valor de contrato
          if (precoFinal) {
            await pipefyQuery(`mutation { updateCardField(input: { card_id: "${card.id}", field_id: "valor_de_contrato", new_value: "${precoFinal}" }) { success } }`).catch(()=>{});
          }
          // Atualizar reparoeletro_orcamentos: trocar ID local pelo ID real do Pipefy
          // e adicionar ao syncedIds para orc-sync não duplicar
          try {
            const ORC_KEY2 = 'tv_orcamentos';
            const orcDb2 = (await dbGet(ORC_KEY2)) || { fichas:[], syncedIds:[], initialized:true };
            // Trocar entrada com id=ficha.id pelo id/pipefyId real
            const orcIdx = orcDb2.fichas.findIndex(f => f.id === ficha.id || f.pipefyId === ficha.id);
            if (orcIdx >= 0) {
              orcDb2.fichas[orcIdx].id       = String(card.id);
              orcDb2.fichas[orcIdx].pipefyId = String(card.id);
            }
            // Garantir que o ID real está em syncedIds
            if (!orcDb2.syncedIds.includes(String(card.id))) {
              orcDb2.syncedIds.push(String(card.id));
            }
            await dbSet(ORC_KEY2, orcDb2);
          } catch(oe) { console.error('[Log] sync orc-key:', oe.message); }
          console.log('[Log] Pipefy card CRIADO:', card.id, card.url);
        }
      }
    } catch(e) {
      console.error('[Log] Pipefy:', e.message);
      // Salvar erro para diagnóstico — não é silencioso
      ficha.pipefyErro = e.message;
      ficha.pipefyErroTs = new Date().toISOString();
      await dbSet(LOG_KEY, db);
    }

    return res.status(200).json({
      ok:true, textoFinal, precoFinal, ficha,
      pipefyOk: !!ficha.pipefyCardId,
      pipefyErro: ficha.pipefyErro || null
    });
  }


  // ── GET limpar-orc-registrado — cron noturno, limpa coluna Orçamento Registrado ──
  if (action === 'limpar-orc-registrado') {
    const db = await dbGet(LOG_KEY) || defaultDB();
    const antes = db.fichas.length;
    // Só deletar fichas em orc_registrado que já têm pipefyCardId confirmado
    // Fichas sem pipefyCardId ficam para retry (não perder dados)
    db.fichas = db.fichas.filter(f =>
      f.phase !== 'orc_registrado' || !f.pipefyCardId
    );
    const removidas = antes - db.fichas.length;
    if (removidas > 0) await dbSet(LOG_KEY, db);
    return res.status(200).json({ ok: true, removidas, restantes: db.fichas.length });
  }

  // ── POST cancelar ────────────────────────────────────────

  // ── POST finalizar-rs: finaliza ficha de garantia sem orçamento ──────────
  if (req.method === 'POST' && action === 'finalizar-rs') {
    const { id } = req.body || {};
    if (!id) return res.status(400).json({ ok: false, error: 'id obrigatorio' });
    const db = await dbGet(LOG_KEY) || defaultDB();
    const ficha = db.fichas.find(f => f.id === id);
    if (!ficha) return res.status(404).json({ ok: false, error: 'nao encontrada' });
    ficha.phase       = 'finalizado_rs';
    ficha.finalizado  = true;
    ficha.finalizadoEm = new Date().toISOString();
    ficha.movedAt     = ficha.finalizadoEm;
    await dbSet(LOG_KEY, db);
    return res.status(200).json({ ok: true, ficha });
  }

  if (req.method === 'POST' && action === 'cancelar') {
    const { id } = req.body || {};
    const db = await dbGet(LOG_KEY) || defaultDB();
    db.fichas = db.fichas.filter(f => f.id !== id);
    await dbSet(LOG_KEY, db);
    return res.status(200).json({ ok: true });
  }

  // ── GET fix-sidney — corrige orçamento do Sidney com novo padrão ─────────────
  if (req.method === 'GET' && action === 'fix-sidney') {
    const ORC_KEY_S = 'tv_orcamentos';
    const orcDb_s   = (await dbGet(ORC_KEY_S)) || { fichas:[] };
    const idx_s     = orcDb_s.fichas.findIndex(f => f.id === 'LOG-0009' || (f.nome||'').toLowerCase() === 'sidney');
    if (idx_s < 0) return res.status(404).json({ ok:false, error:'Sidney não encontrado em tv_orcamentos' });

    // Samsung 55" → R$ 890 barramento + acrílico R$ 335 (sem modelo no texto)
    const texto_s =
      `Olá, Sidney, bom dia! Sou o Alessandro da Reparo Eletro, vou te enviar agora o orçamento:

` +
      `Foram feitos todos os testes, identificamos que será necessário fazer a troca do barramento da TV, ` +
      `será feito a reoperação elétrica também. Este conserto completo fica em 890 reais apenas. Aprovando já iniciamos o conserto.

` +
      `Devido ao superaquecimento dos barramentos o acrílico pode ressecar e ter pequenas rachaduras, ` +
      `o que faz aparecer pequenas rajadas de luz quando a TV está com cores mais claras. ` +
      `Sem trocar o acrílico você pode considerar uma qualidade de 80 a 90%. ` +
      `Trocando o Acrílico fica 100% e tem um custo adicional de 335 reais. Aguardo sua resposta.`;

    orcDb_s.fichas[idx_s].textoOrc      = texto_s;
    orcDb_s.fichas[idx_s].precoSugerido = '890';
    orcDb_s.fichas[idx_s].status        = 'pendente';
    orcDb_s.fichas[idx_s].preco         = null;
    orcDb_s.fichas[idx_s].fixedAt       = new Date().toISOString();
    await dbSet(ORC_KEY_S, orcDb_s);

    return res.status(200).json({
      ok: true,
      nome: 'Sidney',
      textoGerado: texto_s,
      preco: '890',
      acrilicoExtra: '335',
      msg: '✅ Orçamento do Sidney atualizado com novo padrão (barramento 55" + acrílico)',
    });
  }

    // ── GET listar-orc — lista tv_orcamentos para diagnóstico ──────────────────
  if (req.method === 'GET' && action === 'listar-orc') {
    const orcDb_lo = (await dbGet('tv_orcamentos')) || { fichas:[] };
    const lista_lo = (orcDb_lo.fichas||[]).map(f => ({
      id:     f.id,
      nome:   f.nome,
      tel:    f.tel||'',
      status: f.status||'pendente',
      preco:  f.precoSugerido||f.preco||null,
      textoOrc: (f.textoOrc||'').substring(0,120)+'…',
    }));
    return res.status(200).json({ ok:true, total: lista_lo.length, fichas: lista_lo });
  }

  // ── POST fix-orc-direto — atualiza textoOrc de orçamento pelo nome ─────────
  if (req.method === 'POST' && action === 'fix-orc-direto') {
    const { nome: nomeQ, modelo, chips, acrilicoVal } = req.body || {};
    if (!nomeQ || !chips || !chips.length) return res.status(400).json({ ok:false, error:'nome e chips obrigatórios' });

    const orcDb_fd = (await dbGet('tv_orcamentos')) || { fichas:[] };
    const idx_fd   = orcDb_fd.fichas.findIndex(f => (f.nome||'').toLowerCase().includes(nomeQ.toLowerCase()));
    if (idx_fd < 0) return res.status(404).json({ ok:false, error:'Não encontrado em tv_orcamentos: '+nomeQ });

    const ficha_fd   = orcDb_fd.fichas[idx_fd];
    const pn_fd      = (ficha_fd.nome||'').trim().split(/\s+/)[0] || 'cliente';
    const tvModel_fd = modelo || '';
    let pol_fd = null;
    const mPol_fd = tvModel_fd.match(/(\d{2})\s*(?:pol(?:egadas?)?|")?/i);
    if (mPol_fd) { const v=parseInt(mPol_fd[1]); if(v>=30&&v<=79) pol_fd=v; }
    if (!pol_fd) { const ns=(tvModel_fd.match(/([3-7]\d)/g)||[]); for(const n of ns){const v=parseInt(n);if(v>=30&&v<=79){pol_fd=v;break;}} }

    const TAB_fd=[{min:30,max:39,p:'490'},{min:40,max:49,p:'690'},{min:50,max:59,p:'890'},{min:60,max:69,p:'1490'},{min:70,max:79,p:'1990'}];
    let precoTab_fd=null;
    if(pol_fd){for(const f of TAB_fd){if(pol_fd>=f.min&&pol_fd<=f.max){precoTab_fd=f.p;break;}}}
    const precoStr_fd  = precoTab_fd||'[VALOR]';
    const modeloStr_fd = tvModel_fd?' '+tvModel_fd:'';
    const acrVal_fd    = parseFloat(acrilicoVal)||0;
    const temB=chips.includes('barramento'), temP=chips.includes('placa');
    const temR=chips.includes('risco'),      temA=chips.includes('acrilico');

    let texto_fd=null, preco_fd=null;
    if(chips.includes('condenada')){
      texto_fd=`Olá, bom dia ${pn_fd}, fizemos todos os testes e identificamos que infelizmente não tem conserto viável a TV${modeloStr_fd}. Caso queira ela de volta me fala que providencio a entrega.`;
    } else if(temB||temP){
      const peca=(temB&&temP)?'barramento e placa':temB?'barramento':'placa';
      texto_fd=`Olá, ${pn_fd}, bom dia! Sou o Alessandro da Reparo Eletro, vou te enviar agora o orçamento:

Foram feitos todos os testes, identificamos que será necessário fazer a troca do ${peca} da TV${modeloStr_fd}, será feito a reoperação elétrica também. Este conserto completo fica em ${precoStr_fd} reais apenas. Aprovando já iniciamos o conserto.`;
      if(temR) texto_fd+=`

Obs.: Devido às condições da placa do equipamento preciso comunicar o risco de ao trabalhar nela o curto progredir e infelizmente ela apagar completamente. São poucos os casos mas existe esse risco.`;
      if(temA&&acrVal_fd>0) texto_fd+=`

Devido ao superaquecimento dos barramentos o acrílico pode ressecar e ter pequenas rachaduras, o que faz aparecer pequenas rajadas de luz quando a TV está com cores mais claras. Sem trocar o acrílico você pode considerar uma qualidade de 80 a 90%. Trocando o Acrílico fica 100% e tem um custo adicional de ${acrVal_fd} reais. Aguardo sua resposta.`;
      preco_fd=precoTab_fd;
    } else if(temR){
      texto_fd=`Olá, ${pn_fd}, bom dia! Sou o Alessandro da Reparo Eletro, vou te enviar agora o orçamento:

Foram feitos todos os testes, identificamos um problema no conjunto eletrônico da TV${modeloStr_fd}. Este conserto completo fica em ${precoStr_fd} reais apenas.

Obs.: Devido às condições da placa do equipamento preciso comunicar o risco de ao trabalhar nela o curto progredir e infelizmente ela apagar completamente. São poucos os casos mas existe esse risco. Aprovando já iniciamos o conserto.`;
      preco_fd=precoTab_fd;
    } else if(temA){
      texto_fd=`Olá, ${pn_fd}, bom dia! Sou o Alessandro da Reparo Eletro.

Devido ao superaquecimento dos barramentos o acrílico pode ressecar e ter pequenas rachaduras, o que faz aparecer pequenas rajadas de luz quando a TV está com cores mais claras. Sem trocar o acrílico você pode considerar uma qualidade de 80 a 90%. Trocando o Acrílico fica 100% e tem um custo adicional de ${acrVal_fd>0?acrVal_fd:'[VALOR]'} reais. Aguardo sua resposta.`;
      preco_fd=acrVal_fd?String(acrVal_fd):null;
    }

    if(!texto_fd) return res.status(400).json({ok:false,error:'Chips inválidos'});

    orcDb_fd.fichas[idx_fd].textoOrc      = texto_fd;
    orcDb_fd.fichas[idx_fd].precoSugerido = preco_fd;
    orcDb_fd.fichas[idx_fd].status        = 'pendente';
    orcDb_fd.fichas[idx_fd].preco         = null;
    orcDb_fd.fichas[idx_fd].fixedAt       = new Date().toISOString();
    await dbSet('tv_orcamentos', orcDb_fd);

    return res.status(200).json({
      ok: true,
      nome: ficha_fd.nome,
      textoGerado: texto_fd,
      preco: preco_fd,
      msg: '✅ Orçamento atualizado com novo padrão',
    });
  }

    // ── GET listar-com-diag — lista fichas que têm diagnóstico ────────────────
  if (req.method === 'GET' && action === 'listar-com-diag') {
    const db_ld = await dbGet('tv_logistica_log') || defaultDB();
    const lista = (db_ld.fichas||[])
      .filter(f => f.diagnostico)
      .map(f => ({
        id:   f.id,
        nome: f.nome,
        tel:  f.telefone||'',
        fase: f.phase||'',
        chips: (f.diagnostico?.equips||[f.diagnostico]).map(e=>({modelo:e.modelo,servicos:e.servicos})),
      }));
    return res.status(200).json({ ok:true, total: lista.length, fichas: lista });
  }

    // ── GET fix-orcamento — regenera texto do orçamento de um cliente ──────────
  if (req.method === 'GET' && action === 'fix-orcamento') {
    const nome_q = (req.query.nome || '').toLowerCase().trim();
    if (!nome_q) return res.status(400).json({ ok:false, error: 'Informe ?nome=xxx' });

    const LOG_KEY2 = 'tv_logistica_log';
    const ORC_KEY3 = 'tv_orcamentos';
    const db3    = await dbGet(LOG_KEY2) || defaultDB();
    const orcDb3 = (await dbGet(ORC_KEY3)) || { fichas:[], syncedIds:[], initialized:true };

    const ficha3 = db3.fichas.find(f =>
      (f.nome||'').toLowerCase().includes(nome_q) ||
      (f.telefone||'').includes(nome_q)
    );
    if (!ficha3) return res.status(404).json({ ok:false, error: 'Ficha não encontrada para: '+nome_q });
    if (!ficha3.diagnostico) return res.status(400).json({ ok:false, error: 'Ficha sem diagnóstico', ficha: ficha3.id });

    const equips = ficha3.diagnostico.equips || [ficha3.diagnostico];
    const nome3  = ficha3.nome || '';
    function priNome3(n){ return n ? n.trim().split(/\s+/)[0] : 'cliente'; }

    function gerarTextoFix(tipo, servicos, precoInput, modelo) {
      const pn = priNome3(nome3);
      if (tipo !== 'tv') return { texto: null, preco: null };
      const chips = servicos || [];
      const tvModel = modelo || '';
      let pol = null;
      const mPol = tvModel.match(/(\d{2})\s*(?:pol(?:egadas?)?|")?/i);
      if (mPol) { const v=parseInt(mPol[1]); if(v>=30&&v<=79) pol=v; }
      if (!pol) {
        const ns = tvModel.match(/\b([3-7]\d)\b/g);
        if (ns) { for (const n of ns) { const v=parseInt(n); if(v>=30&&v<=79){pol=v;break;} } }
      }
      const TAB = [{min:30,max:39,p:'490'},{min:40,max:49,p:'690'},{min:50,max:59,p:'890'},{min:60,max:69,p:'1490'},{min:70,max:79,p:'1990'}];
      let precoTab = null;
      if (pol) { for (const f of TAB) { if(pol>=f.min&&pol<=f.max){precoTab=f.p;break;} } }
      const precoStr  = precoTab || '[VALOR]';
      const modeloStr = tvModel ? ' ' + tvModel : '';
      const acrilicoVal = parseFloat(precoInput)||0;

      if (chips.includes('condenada')) {
        return { texto: `Olá, bom dia ${pn}, fizemos todos os testes e identificamos que infelizmente não tem conserto viável a TV. Caso queira ela de volta me fala que providencio a entrega.`, preco: null };
      }
      const temB = chips.includes('barramento');
      const temP = chips.includes('placa');
      const temR = chips.includes('risco');
      const temA = chips.includes('acrilico');
      if (temB || temP) {
        const peca = (temB&&temP)?'barramento e placa':temB?'barramento':'placa';
        let texto = `Olá, ${pn}, bom dia! Sou o Alessandro da Reparo Eletro, vou te enviar agora o orçamento:\n\nForam feitos todos os testes, identificamos que será necessário fazer a troca do ${peca} da TV, será feito a reoperação elétrica também. Este conserto completo fica em ${precoStr} reais apenas. Aprovando já iniciamos o conserto.`;
        if (temR) texto += `\n\nObs.: Devido às condições da placa do equipamento preciso comunicar o risco de ao trabalhar nela o curto progredir e infelizmente ela apagar completamente. São poucos os casos mas existe esse risco.`;
        if (temA && acrilicoVal>0) texto += `\n\nDevido ao superaquecimento dos barramentos o acrílico pode ressecar e ter pequenas rachaduras, o que faz aparecer pequenas rajadas de luz quando a TV está com cores mais claras. Sem trocar o acrílico você pode considerar uma qualidade de 80 a 90%. Trocando o Acrílico fica 100% e tem um custo adicional de ${acrilicoVal} reais. Aguardo sua resposta.`;
        return { texto, preco: precoTab };
      }
      if (temR) {
        let texto = `Olá, ${pn}, bom dia! Sou o Alessandro da Reparo Eletro, vou te enviar agora o orçamento:\n\nForam feitos todos os testes, identificamos um problema no conjunto eletrônico da TV. Este conserto completo fica em ${precoStr} reais apenas.\n\nObs.: Devido às condições da placa do equipamento preciso comunicar o risco de ao trabalhar nela o curto progredir e infelizmente ela apagar completamente. São poucos os casos mas existe esse risco. Aprovando já iniciamos o conserto.`;
        if (temA && acrilicoVal>0) texto += `\n\nDevido ao superaquecimento dos barramentos o acrílico pode ressecar e ter pequenas rachaduras, o que faz aparecer pequenas rajadas de luz quando a TV está com cores mais claras. Sem trocar o acrílico você pode considerar uma qualidade de 80 a 90%. Trocando o Acrílico fica 100% e tem um custo adicional de ${acrilicoVal} reais. Aguardo sua resposta.`;
        return { texto, preco: precoTab };
      }
      if (temA) {
        return { texto: `Olá, ${pn}, bom dia! Sou o Alessandro da Reparo Eletro.\n\nDevido ao superaquecimento dos barramentos o acrílico pode ressecar e ter pequenas rachaduras, o que faz aparecer pequenas rajadas de luz quando a TV está com cores mais claras. Sem trocar o acrílico você pode considerar uma qualidade de 80 a 90%. Trocando o Acrílico fica 100% e tem um custo adicional de ${acrilicoVal>0?acrilicoVal:'[VALOR]'} reais. Aguardo sua resposta.`, preco: acrilicoVal?String(acrilicoVal):null };
      }
      return { texto: null, preco: null };
    }

    const resultados3 = equips.map(eq => gerarTextoFix(eq.tipo, eq.servicos, eq.preco, eq.modelo));
    let textoFinal3, precoFinal3;
    if (resultados3.length === 1) {
      textoFinal3 = resultados3[0].texto;
      precoFinal3 = resultados3[0].preco;
    } else {
      textoFinal3 = resultados3.filter(r=>r.texto).map((r,i)=>`TV ${i+1}:\n${r.texto}`).join('\n\n---\n\n');
      const soma  = resultados3.reduce((s,r)=>s+(parseFloat(r.preco)||0),0);
      precoFinal3 = soma > 0 ? String(soma) : null;
    }

    if (!textoFinal3) return res.status(400).json({ ok:false, error: 'Não foi possível gerar texto', diagnostico: ficha3.diagnostico });

    const fichaId3 = ficha3.pipefyCardId || ficha3.id;
    const idx3 = orcDb3.fichas.findIndex(f =>
      f.id === fichaId3 || f.id === ficha3.id ||
      (f.nome||'').toLowerCase().includes(nome_q)
    );
    if (idx3 >= 0) {
      orcDb3.fichas[idx3].textoOrc      = textoFinal3;
      orcDb3.fichas[idx3].precoSugerido = precoFinal3;
      orcDb3.fichas[idx3].status        = 'pendente';
      orcDb3.fichas[idx3].preco         = null;
      orcDb3.fichas[idx3].fixedAt       = new Date().toISOString();
    } else {
      orcDb3.fichas.unshift({
        id: fichaId3, pipefyId: fichaId3,
        nome: ficha3.nome, tel: ficha3.telefone||'',
        desc: (ficha3.equipamento||'')+(ficha3.defeito?' — '+ficha3.defeito:''),
        end: ficha3.endereco||'', age: null, comentarios:[],
        textoOrc: textoFinal3, precoSugerido: precoFinal3,
        status: 'pendente', preco: null, createdAt: new Date().toISOString(),
      });
    }
    await dbSet(ORC_KEY3, orcDb3);

    return res.status(200).json({
      ok: true,
      nome: ficha3.nome,
      diagnostico: ficha3.diagnostico,
      textoGerado: textoFinal3,
      preco: precoFinal3,
      msg: idx3 >= 0 ? '✅ Orçamento atualizado com novo padrão' : '✅ Orçamento criado com novo padrão',
    });
  }

    // ── GET buscar-ficha — busca ficha por nome/tel em toda a logistica ──────────
  if (req.method === 'GET' && action === 'buscar-ficha') {
    const q = (req.query.q || '').toLowerCase().trim();
    if (!q) return res.status(400).json({ ok:false, error: 'Informe ?q=xxx' });
    const db = await dbGet('tv_logistica_log') || defaultDB();
    const found = (db.fichas||[]).filter(f =>
      (f.nome||'').toLowerCase().includes(q) ||
      (f.telefone||'').includes(q) ||
      (f.id||'').toLowerCase().includes(q)
    ).map(f => ({
      id: f.id, nome: f.nome, tel: f.telefone||'',
      phase: f.phase,
      horarioColeta: f.horarioColeta||null,
      horarioDisplay: f.horarioColeta
        ? new Date(f.horarioColeta).toLocaleString('pt-BR',{timeZone:'America/Sao_Paulo'})
        : null,
      criadoEm: f.criadoEm, movedAt: f.movedAt,
    }));
    return res.status(200).json({ ok:true, total: found.length, fichas: found });
  }

    // ── GET listar-horario — lista fichas com horario_marcado ────────────────────
  if (req.method === 'GET' && action === 'listar-horario') {
    const db = await dbGet('tv_logistica_log') || defaultDB();
    const fichas = (db.fichas||[])
      .filter(f => f.phase === 'horario_marcado' || f.horarioColeta)
      .map(f => ({
        id: f.id, nome: f.nome, tel: f.telefone||'',
        phase: f.phase,
        horarioColeta: f.horarioColeta||null,
        horarioDisplay: f.horarioColeta
          ? new Date(f.horarioColeta).toLocaleString('pt-BR',{timeZone:'America/Sao_Paulo'})
          : null,
      }));
    return res.status(200).json({ ok:true, total: fichas.length, fichas });
  }

    // ── GET fix-horario-direto — corrige horarioColeta por id da ficha ───────────
  if (req.method === 'GET' && action === 'fix-horario-direto') {
    const fid      = (req.query.id  || '').trim();
    const novaData = (req.query.data|| '').trim(); // formato: YYYY-MM-DDTHH:MM (hora local BRT)
    if (!fid || !novaData) return res.status(400).json({ ok:false, error: 'Informe ?id=LOG-XXXX&data=YYYY-MM-DDTHH:MM' });

    const db = await dbGet('tv_logistica_log') || defaultDB();
    const ficha = (db.fichas||[]).find(f => f.id === fid);
    if (!ficha) return res.status(404).json({ ok:false, error: 'Ficha não encontrada: '+fid });

    const original = ficha.horarioColeta;
    // Parse manual para garantir hora local BRT (evita ambiguidade UTC)
    const [datePart, timePart] = novaData.split('T');
    const [y, m, d]   = datePart.split('-').map(Number);
    const [hh, mm]    = timePart.split(':').map(Number);
    const corrigido   = new Date(y, m-1, d, hh, mm, 0).toISOString();

    ficha.horarioColeta = corrigido;
    ficha.movedAt = new Date().toISOString();
    await dbSet('tv_logistica_log', db);

    return res.status(200).json({
      ok: true,
      id: ficha.id, nome: ficha.nome,
      original, corrigido,
      msg: '✅ horarioColeta corrigido',
    });
  }

    // ── GET fix-horario — corrige horarioColeta de uma ficha (offset UTC→BRT) ─────
  if (req.method === 'GET' && action === 'fix-horario') {
    const nome_q = (req.query.nome || '').toLowerCase().trim();
    const add_h  = parseInt(req.query.add) || 3; // horas a adicionar (padrão +3 = BRT)
    if (!nome_q) return res.status(400).json({ ok:false, error: 'Informe ?nome=xxx' });

    const db = await dbGet('tv_logistica_log') || defaultDB();
    const ficha = db.fichas.find(f =>
      (f.nome||'').toLowerCase().includes(nome_q) ||
      (f.telefone||'').includes(nome_q)
    );
    if (!ficha) return res.status(404).json({ ok:false, error: 'Ficha não encontrada: '+nome_q });
    if (!ficha.horarioColeta) return res.status(400).json({ ok:false, error: 'Ficha sem horarioColeta', ficha: {id:ficha.id, nome:ficha.nome} });

    const original = ficha.horarioColeta;
    const corrigido = new Date(new Date(original).getTime() + add_h * 3600 * 1000).toISOString();

    ficha.horarioColeta = corrigido;
    ficha.movedAt = new Date().toISOString();
    await dbSet('tv_logistica_log', db);

    return res.status(200).json({
      ok: true,
      nome: ficha.nome,
      original,
      corrigido,
      add_horas: add_h,
      msg: `✅ horarioColeta corrigido +${add_h}h`,
    });
  }

    // ── GET fix-ronaldo — corrige horarioColeta do Ronaldo LOG-0010 ──────────────
  if (req.method === 'GET' && action === 'fix-ronaldo') {
    const db = await dbGet('tv_logistica_log') || defaultDB();
    // Busca por qualquer campo que identifique o Ronaldo
    const ficha = (db.fichas||[]).find(f =>
      (f.nome||'').toLowerCase().includes('ronaldo') ||
      (f.telefone||'').includes('1213') ||
      String(f.id||'') === 'LOG-0010'
    );
    if (!ficha) return res.status(404).json({
      ok:false, error:'Ronaldo não encontrado',
      total: (db.fichas||[]).length,
      ids: (db.fichas||[]).slice(0,10).map(f=>({id:f.id,nome:f.nome}))
    });

    const original = ficha.horarioColeta;
    // Corrigir: 2001-04-06T17:30:00.000Z → 2026-06-04T17:30:00.000Z
    // (ano errado 2001→2026, mês errado 04→06, hora correta 17:30 UTC = 14:30 BRT)
    ficha.horarioColeta = '2026-06-04T17:30:00.000Z';
    ficha.movedAt = new Date().toISOString();
    await dbSet('tv_logistica_log', db);

    return res.status(200).json({
      ok: true,
      id: ficha.id, nome: ficha.nome,
      original,
      corrigido: ficha.horarioColeta,
      display: new Date(ficha.horarioColeta).toLocaleString('pt-BR',{timeZone:'America/Sao_Paulo'}),
      msg: '✅ Horário do Ronaldo corrigido para 04/06/2026 às 14:30'
    });
  }

    // ── GET fix-ronaldo — corrige horarioColeta 2001→2026 do Ronaldo ─────────────
  if (req.method === 'GET' && action === 'fix-ronaldo') {
    const db = await dbGet(LOG_KEY) || defaultDB();
    const ficha = (db.fichas||[]).find(f =>
      (f.nome||'').toLowerCase().includes('ronaldo') ||
      (f.telefone||'').includes('1213') ||
      String(f.id||'') === 'LOG-0010'
    );
    if (!ficha) return res.status(404).json({
      ok:false, error:'Ronaldo não encontrado em '+LOG_KEY,
      total: (db.fichas||[]).length,
      ids: (db.fichas||[]).slice(0,5).map(f=>({id:f.id,nome:f.nome,phase:f.phase}))
    });

    const original = ficha.horarioColeta;
    // 2001-04-06T17:30Z → 2026-06-04T17:30Z (mesmo horário UTC = 14:30 BRT)
    ficha.horarioColeta = '2026-06-04T17:30:00.000Z';
    ficha.movedAt = new Date().toISOString();
    await dbSet(LOG_KEY, db);

    return res.status(200).json({
      ok: true, id: ficha.id, nome: ficha.nome,
      original, corrigido: ficha.horarioColeta,
      display: new Date(ficha.horarioColeta).toLocaleString('pt-BR',{timeZone:'America/Sao_Paulo'}),
      msg: '✅ Corrigido para 04/06/2026 às 14:30 BRT'
    });
  }

    // ── GET fix-ronaldo — corrige horarioColeta 2001→2026 tentando todas as chaves
  if (req.method === 'GET' && action === 'fix-ronaldo') {
    const KEYS = ['tv_logistica', 'tv_logistica_log', LOG_KEY];
    let ficha = null, usedKey = null, usedDb = null;

    for (const k of KEYS) {
      const d = await dbGet(k);
      const f = (d?.fichas||[]).find(x =>
        (x.nome||'').toLowerCase().includes('ronaldo') ||
        (x.telefone||'').includes('1213') ||
        String(x.id||'') === 'LOG-0010'
      );
      if (f) { ficha = f; usedKey = k; usedDb = d; break; }
    }

    if (!ficha) {
      const sizes = {};
      for (const k of KEYS) {
        const d = await dbGet(k);
        sizes[k] = d?.fichas?.length ?? (d === null ? 'null' : 'no fichas');
      }
      return res.status(404).json({ ok:false, error:'Não encontrado em nenhuma chave', sizes });
    }

    const original = ficha.horarioColeta;
    ficha.horarioColeta = '2026-06-04T17:30:00.000Z';
    ficha.movedAt = new Date().toISOString();
    await dbSet(usedKey, usedDb);

    return res.status(200).json({
      ok: true, key: usedKey, id: ficha.id, nome: ficha.nome,
      original, corrigido: ficha.horarioColeta,
      display: new Date(ficha.horarioColeta).toLocaleString('pt-BR',{timeZone:'America/Sao_Paulo'}),
      msg: '✅ Corrigido para 04/06/2026 às 14:30 BRT'
    });
  }

    // ── GET regenerar-pendentes — regenera todos os orçamentos pendentes ──────────
  if (req.method === 'GET' && action === 'regenerar-pendentes') {
    const ORC_KEY_B = 'tv_orcamentos';
    const db_b     = await dbGet(LOG_KEY) || defaultDB();
    const orcDb_b  = (await dbGet(ORC_KEY_B)) || { fichas: [] };

    // Filtrar pendentes
    const pendentes = (orcDb_b.fichas || []).filter(f => f.status === 'pendente');
    if (!pendentes.length) return res.status(200).json({ ok: true, msg: 'Nenhum pendente encontrado', total: 0 });

    const resultados = [];

    for (const orc of pendentes) {
      // Achar ficha na logistica pelo id
      const ficha = (db_b.fichas || []).find(f =>
        f.id === orc.id || f.pipefyCardId === orc.pipefyId ||
        (f.nome || '').toLowerCase() === (orc.nome || '').toLowerCase()
      );

      if (!ficha || !ficha.diagnostico) {
        resultados.push({ id: orc.id, nome: orc.nome, status: 'sem_diagnostico', preco: null, texto: null });
        continue;
      }

      // Reusar a mesma lógica do gerar-orcamento
      const equips = ficha.diagnostico.equips || [ficha.diagnostico];
      const nome_b = ficha.nome || '';

      function priNome_b(n) { return n ? n.trim().split(/\s+/)[0] : 'cliente'; }

      function extrairPol_b(txt) {
        const patterns = [
          /([3-7]\d)\s*(?:pol(?:egadas?)?|")/i,
          /[Uu][Nn]([3-7]\d)/,
          /([3-7]\d)/,
        ];
        for (const re of patterns) {
          const m = txt.match(re);
          if (m) { const v = parseInt(m[1]); if (v >= 30 && v <= 79) return v; }
        }
        return null;
      }

      const TAB_B = [{min:30,max:39,p:'490'},{min:40,max:49,p:'690'},{min:50,max:59,p:'890'},{min:60,max:69,p:'1490'},{min:70,max:79,p:'1990'}];

      function gerarTextoB(tipo, servicos, precoInput, modelo) {
        const pn = priNome_b(nome_b);
        if (tipo !== 'tv') return { texto: null, preco: null };
        const chips = servicos || [];

        const fontes = [modelo || '', ficha.equipamento || '', ficha.defeito || ''].join(' ');
        const pol    = extrairPol_b(fontes);
        let precoTab = null;
        if (pol) { for (const f of TAB_B) { if (pol >= f.min && pol <= f.max) { precoTab = f.p; break; } } }

        const precoManual = parseFloat(precoInput) > 0 ? String(Math.round(parseFloat(precoInput))) : null;
        const precoStr    = precoTab || precoManual || '[VALOR]';
        const acrilico    = parseFloat(precoInput) || 0;

        if (chips.includes('condenada')) {
          return { texto: `Olá, bom dia ${pn}, fizemos todos os testes e identificamos que infelizmente não tem conserto viável a TV. Caso queira ela de volta me fala que providencio a entrega.`, preco: null };
        }
        const temB = chips.includes('barramento'), temP = chips.includes('placa');
        const temR = chips.includes('risco'),       temA = chips.includes('acrilico');

        if (temB || temP) {
          const peca = (temB && temP) ? 'barramento e placa' : temB ? 'barramento' : 'placa';
          let texto = `Olá, ${pn}, bom dia! Sou o Alessandro da Reparo Eletro, vou te enviar agora o orçamento:

Foram feitos todos os testes, identificamos que será necessário fazer a troca do ${peca} da TV, será feito a reoperação elétrica também. Este conserto completo fica em ${precoStr} reais apenas. Aprovando já iniciamos o conserto.`;
          if (temR) texto += `

Obs.: Devido às condições da placa do equipamento preciso comunicar o risco de ao trabalhar nela o curto progredir e infelizmente ela apagar completamente. São poucos os casos mas existe esse risco.`;
          if (temA && acrilico > 0) texto += `

Devido ao superaquecimento dos barramentos o acrílico pode ressecar e ter pequenas rachaduras, o que faz aparecer pequenas rajadas de luz quando a TV está com cores mais claras. Sem trocar o acrílico você pode considerar uma qualidade de 80 a 90%. Trocando o Acrílico fica 100% e tem um custo adicional de ${acrilico} reais. Aguardo sua resposta.`;
          return { texto, preco: precoTab };
        }
        if (temR) {
          let texto = `Olá, ${pn}, bom dia! Sou o Alessandro da Reparo Eletro, vou te enviar agora o orçamento:

Foram feitos todos os testes, identificamos um problema no conjunto eletrônico da TV. Este conserto completo fica em ${precoStr} reais apenas.

Obs.: Devido às condições da placa do equipamento preciso comunicar o risco de ao trabalhar nela o curto progredir e infelizmente ela apagar completamente. São poucos os casos mas existe esse risco. Aprovando já iniciamos o conserto.`;
          if (temA && acrilico > 0) texto += `

Devido ao superaquecimento dos barramentos o acrílico pode ressecar e ter pequenas rachaduras, o que faz aparecer pequenas rajadas de luz quando a TV está com cores mais claras. Sem trocar o acrílico você pode considerar uma qualidade de 80 a 90%. Trocando o Acrílico fica 100% e tem um custo adicional de ${acrilico} reais. Aguardo sua resposta.`;
          return { texto, preco: precoTab };
        }
        if (temA) {
          return { texto: `Olá, ${pn}, bom dia! Sou o Alessandro da Reparo Eletro.

Devido ao superaquecimento dos barramentos o acrílico pode ressecar e ter pequenas rachaduras, o que faz aparecer pequenas rajadas de luz quando a TV está com cores mais claras. Sem trocar o acrílico você pode considerar uma qualidade de 80 a 90%. Trocando o Acrílico fica 100% e tem um custo adicional de ${acrilico > 0 ? acrilico : '[VALOR]'} reais. Aguardo sua resposta.`, preco: acrilico ? String(acrilico) : null };
        }
        return { texto: null, preco: null };
      }

      const res_equips = equips.map(eq => gerarTextoB(eq.tipo, eq.servicos, eq.preco, eq.modelo));
      let textoFinal, precoFinal;
      if (res_equips.length === 1) {
        textoFinal = res_equips[0].texto; precoFinal = res_equips[0].preco;
      } else {
        textoFinal = res_equips.filter(r => r.texto).map((r, i) => `TV ${i+1}:
${r.texto}`).join('

---

');
        precoFinal = String(res_equips.reduce((s, r) => s + (parseInt(r.preco) || 0), 0)) || null;
      }

      if (textoFinal) {
        const idx_b = orcDb_b.fichas.findIndex(f => f.id === orc.id);
        if (idx_b >= 0) {
          orcDb_b.fichas[idx_b].textoOrc      = textoFinal;
          orcDb_b.fichas[idx_b].precoSugerido = precoFinal;
          orcDb_b.fichas[idx_b].regeneradoEm  = new Date().toISOString();
        }
        resultados.push({ id: orc.id, nome: orc.nome, chips: equips.map(e => e.servicos), equipamento: ficha.equipamento, polFound: extrairPol_b([equips[0]?.modelo || '', ficha.equipamento || '', ficha.defeito || ''].join(' ')), preco: precoFinal, textoPrev: textoFinal.substring(0, 120) + '…' });
      } else {
        resultados.push({ id: orc.id, nome: orc.nome, status: 'texto_nulo', chips: equips.map(e => e.servicos) });
      }
    }

    await dbSet(ORC_KEY_B, orcDb_b);

    return res.status(200).json({
      ok: true,
      total: pendentes.length,
      regenerados: resultados.filter(r => r.preco !== undefined).length,
      resultados,
    });
  }

    return res.status(404).json({ ok: false, error: 'ação não encontrada' });
};
