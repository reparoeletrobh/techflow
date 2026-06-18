'use strict';
// TV FRENTELOJA — espelho ADM | FASE 8


// ── Pipefy é ESPELHO — nunca bloqueia o fluxo local ─────────────────────
async function pipefyBestEffort(fn) {
  try { return await fn(); }
  catch(e) { console.warn('[Pipefy best-effort]', e.message); return null; }
}


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


// ── Helper: mover card no Pipe ADM pelo pipefyId ─────────────────────────
async function moverNoPipe(pipefyId, novaFase, dados) {
  if (!pipefyId) return;
  try {
    const PIPE_KEY = 'tv_pipe';
    const U = (process.env.UPSTASH_URL   || '').replace(/['"]/g,'').trim();
    const T = (process.env.UPSTASH_TOKEN || '').replace(/['"]/g,'').trim();
    async function _pipeGet(k) {
      const r = await fetch(U + '/pipeline', { method:'POST',
        headers:{ Authorization:'Bearer '+T,'Content-Type':'application/json' },
        body: JSON.stringify([['GET', k]]) });
      const j = await r.json();
      const v = j[0]?.result; if (!v) return null;
      let val = JSON.parse(v);
      if (typeof val === 'string') { try { val = JSON.parse(val); } catch(e) {} }
      return (val && typeof val === 'object') ? val : null;
    }
    async function _pipeSet(k, v) {
      await fetch(U + '/pipeline', { method:'POST',
        headers:{ Authorization:'Bearer '+T,'Content-Type':'application/json' },
        body: JSON.stringify([['SET', k, JSON.stringify(v)]]) });
    }
    const db   = (await _pipeGet(PIPE_KEY)) || { cards:[], syncedPipefyIds:[], lastSync:null };
    const card = (db.cards || []).find(c => c.pipefyId === String(pipefyId));
    if (!card) {
      // Card não existe — criar se dados fornecidos
      if (dados && dados.nomeContato) {
        const now = new Date().toISOString();
        db.cards.unshift({
          id:           'PIPE-' + String(db.cards.length + 1).padStart(4,'0'),
          pipefyId:     String(pipefyId),
          phase:        novaFase,
          nomeContato:  dados.nomeContato || '',
          telefone:     dados.telefone    || '',
          equipamento:  dados.equipamento || '',
          descricao:    dados.descricao   || '',
          valor:        parseFloat(dados.valor || 0) || 0,
          origem:       dados.origem      || 'sistema',
          criadoEm:     now, movedAt: now,
          aguardandoDesde: novaFase === 'aguardando_aprovacao' ? now : null,
          history: [], analiseCompra: false
        });
        await _pipeSet(PIPE_KEY, db);
      }
      return;
    }
    const now = new Date().toISOString();
    card.history = (card.history || []).concat([{ phase: card.phase, ts: now }]);
    card.phase   = novaFase;
    card.movedAt = now;
    if (dados) {
      if (dados.valor !== undefined) card.valor = parseFloat(dados.valor) || 0;
      if (dados.nomeContato)         card.nomeContato = dados.nomeContato;
    }
    await _pipeSet(PIPE_KEY, db);
  } catch(e) { console.error('[pipe-mover]', novaFase, e.message); }
}


// ── Hook: salvar no Pipe ADM quando nova ficha entra no FL ──────────────────
async function registrarNoPipe(dados) {
  try {
    const PIPE_KEY = 'tv_pipe';
    const pipeDb   = await dbGet(PIPE_KEY) || { cards:[], syncedPipefyIds:[], lastSync:null };
    if (!Array.isArray(pipeDb.cards)) pipeDb.cards = [];
    const pid = dados.pipefyId ? String(dados.pipefyId) : null;
    if (pid && pipeDb.cards.find(function(c){ return c.pipefyId === pid; })) return;
    // Para fichas sem pipefyId, verificar por fichaId local
    const fid = dados.fichaId ? String(dados.fichaId) : null;
    if (fid && pipeDb.cards.find(function(c){ return c.fichaId === fid; })) return;
    const now = new Date().toISOString();
    pipeDb.cards.unshift({
      id:              'PIPE-' + String(pipeDb.cards.length + 1).padStart(4,'0'),
      pipefyId:        pid,
      fichaId:         fid,
      phase:           dados.phase || 'aprovados',
      nomeContato:     dados.nomeContato || '',
      telefone:        dados.telefone || '',
      equipamento:     dados.equipamento || '',
      descricao:       dados.descricao || '',
      valor:           parseFloat(dados.valor || 0) || 0,
      origem:          'frenteloja',
      criadoEm:        now, movedAt: now,
      aguardandoDesde: null,
      history: [], analiseCompra: false
    });
    pipeDb.lastSync = now;
    await dbSet(PIPE_KEY, pipeDb);
  } catch(e) { console.error('[pipe-hook-fl]', e.message); }
}

// api/frenteloja.js — Sistema Frente de Loja
const PIPE_ID    = '305832912';
const FL_KEY     = 'tv_frenteloja';
const BALCAO_KEY = 'tv_balcao';

const U = process.env.UPSTASH_URL;
const T = process.env.UPSTASH_TOKEN;
const PT = (process.env.PIPEFY_TOKEN||'').trim();

async function dbGet(k){
  try{const r=await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});const j=await r.json();return j[0]?.result?JSON.parse(j[0].result):null;}catch(e){return null;}
}
async function dbSet(k,v){
  try{await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});}catch(e){}
}
async function pipefyQ(q){
  const r=await fetch(PIPEFY_API,{method:'POST',headers:{Authorization:'Bearer '+PT,'Content-Type':'application/json'},body:JSON.stringify({query:q})});
  const j=await r.json();if(j.errors?.length)throw new Error(j.errors[0].message);return j.data;
}

function defaultDB(){return {fichas:[],seq:0};}
function nextId(db){db.seq=(db.seq||0)+1;return 'FL-'+String(db.seq).padStart(4,'0');}
function brtNow(){return new Date(new Date().toLocaleString('en-US',{timeZone:'America/Sao_Paulo'}));}
function brtStartOfDay(){
  const d=brtNow();d.setHours(0,0,0,0);
  return new Date(Date.UTC(d.getFullYear(),d.getMonth(),d.getDate(),3,0,0,0));
}

async function getPipefyPhaseId(keyword){
  const d=await pipefyQ('query{pipe(id:"'+PIPE_ID+'"){phases{id name}}}');
  const phases=d?.pipe?.phases||[];
  const ph=phases.find(p=>p.name.toLowerCase().includes(keyword.toLowerCase()));
  return ph?.id||null;
}

async function createPipefyCard() {
  return null; // Pipefy desconectado
}

async function movePipefyCard(cardId, phaseId){
  const q='mutation{moveCardToPhase(input:{card_id:"'+cardId+'",destination_phase_id:"'+phaseId+'"}){card{id}}}';
  await pipefyQ(q);
}

export default async function handler(req,res){
  res.setHeader('Access-Control-Allow-Origin','*');
  res.setHeader('Access-Control-Allow-Methods','GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers','Content-Type');
  if(req.method==='OPTIONS')return res.status(200).end();
  const action=req.query.action;

  if(req.method==='GET'&&action==='load'){
    const db=await dbGet(FL_KEY)||defaultDB();
    const todayStart=brtStartOfDay();
    let changed=false;
    db.fichas.forEach(f=>{
      if(f.liberadoHoje&&new Date(f.movedAt)<todayStart){
        f.liberadoHoje=false;changed=true;
      }
      if(f.phase==='reprovado'&&new Date(f.reprovadoEm||f.movedAt||0)<todayStart){
        f.phase='encerrado';changed=true;
      }
    });
    db.fichas=db.fichas.filter(f=>f.phase!=='encerrado');
    if(changed)await dbSet(FL_KEY,db);
    return res.status(200).json({ok:true,fichas:db.fichas});
  }

  if(req.method==='POST'&&action==='criar'){
    const {nomeContato,equipamento,telefone,descricao}=req.body||{};
    if(!nomeContato||!equipamento)return res.status(400).json({ok:false,error:'Nome e equipamento obrigatórios'});
    const db=await dbGet(FL_KEY)||defaultDB();
    const id=nextId(db);const now=new Date().toISOString();
    const ficha={id,nomeContato,equipamento,telefone:(telefone||'').replace(/[^0-9]/g,''),descricao:descricao||'',phase:'analise',createdAt:now,movedAt:now,history:[{phase:'analise',ts:now}]};
    db.fichas.unshift(ficha);await dbSet(FL_KEY,db);
    // Espelhar no board em analise_loja
    try{
      const _base=process.env.FL_BASE_URL||'https://reparoeletroadm.com';
      const _title=(ficha.nomeContato+' (Loja) - '+(ficha.equipamento||'')+' | '+(ficha.descricao||'')+' OS:'+ficha.id).replace(/"/g,"'").slice(0,255);
      fetch(_base+'/api/board?action=add-loja-card',{method:'POST',headers:{'Content-Type':'application/json'},
        body:JSON.stringify({flFichaId:ficha.id,pipefyId:null,title:_title,nomeContato:ficha.nomeContato||'',telefone:ficha.telefone||'',phaseId:'analise_loja'})
      }).catch(e=>console.error('[FL] criar→board:',e.message));
    }catch(e){}
    logAction({ modulo:'Frente de Loja', fichaId:ficha.id||'', ficha:ficha.nomeContato||'', acao:'Liberar equipamento', para:'receber', gatilho:'→ Pipe receber + Pipefy Receber$', status:'ok', detalhe:'Valor: R$'+(ficha.pagoValor||0)+' '+ficha.pagoPor }).catch(()=>{});
    return res.status(200).json({ok:true,ficha});
  }

  if(req.method==='POST'&&action==='analise'){
    const {id,descricaoTecnica}=req.body||{};
    if(!id||!descricaoTecnica)return res.status(400).json({ok:false,error:'id e descrição obrigatórios'});
    const db=await dbGet(FL_KEY)||defaultDB();
    const ficha=db.fichas.find(f=>f.id===id);
    if(!ficha)return res.status(404).json({ok:false,error:'Não encontrada'});
    const now=new Date().toISOString();
    ficha.descricaoTecnica=descricaoTecnica;ficha.phase='orcamento_cadastrado';ficha.movedAt=now;
    ficha.history=(ficha.history||[]).concat([{phase:'orcamento_cadastrado',ts:now}]);
    await dbSet(FL_KEY,db);
    // Pipe ADM: mover para receber
    if (ficha.pipefyCardId) {
      await moverNoPipe(ficha.pipefyCardId, 'receber', { nomeContato: ficha.nomeContato, valor: ficha.pagoValor }).catch(() => {});
    }
    logAction({ modulo:'Frente de Loja', fichaId:ficha.id||'', ficha:ficha.nomeContato||'', acao:'Liberar equipamento', para:'receber', gatilho:'→ Pipe receber + Pipefy Receber$', status:'ok', detalhe:'Valor: R$'+(ficha.pagoValor||0)+' '+ficha.pagoPor }).catch(()=>{});
    return res.status(200).json({ok:true,ficha});
  }

  if(req.method==='POST'&&action==='passar-orcamento'){
    const {id,valor,formaPagamento,decisao}=req.body||{};
    if(!id)return res.status(400).json({ok:false,error:'id obrigatório'});
    const db=await dbGet(FL_KEY)||defaultDB();
    const ficha=db.fichas.find(f=>f.id===id);
    if(!ficha)return res.status(404).json({ok:false,error:'Não encontrada'});
    const now=new Date().toISOString();
    ficha.orcamento={valor:parseFloat(valor)||0,formaPagamento:formaPagamento||'pix',status:decisao};

    // Reprovado: mantém no banco com phase='reprovado' para exibir coluna hoje
    if(decisao==='reprovado'){
      ficha.phase='reprovado';
      ficha.reprovadoEm=now;
      ficha.motivo=req.body?.motivo||'';
      ficha.movedAt=now;
      await dbSet(FL_KEY,db);
      logAction({ modulo:'Frente de Loja', fichaId:ficha.id||'', ficha:ficha.nomeContato||'', acao:'Liberar equipamento', para:'receber', gatilho:'→ Pipe receber + Pipefy Receber$', status:'ok', detalhe:'Valor: R$'+(ficha.pagoValor||0)+' '+ficha.pagoPor }).catch(()=>{});
    return res.status(200).json({ok:true,ficha});
    }

    if(decisao==='aprovado'){
      ficha.phase='producao';ficha.movedAt=now;
      ficha.history=(ficha.history||[]).concat([{phase:'producao',ts:now}]);

      const titleCompleto=(ficha.nomeContato+' (Loja) - '+ficha.equipamento+
        ' | '+(ficha.descricao||'')+' | Diag: '+(ficha.descricaoTecnica||'')+
        ' | R$'+String(parseFloat(ficha.orcamento?.valor||0).toFixed(2))+
        ' '+(ficha.orcamento?.formaPagamento||'pix')+' OS:'+ficha.id
      ).replace(/"/g,"'").slice(0,255);

      // ── PASSO 1: Criar card no Pipefy (SÍNCRONO, antes de responder) ─
      let pipefyId = null;
      try {
        const aprovadoPhaseId = await getPipefyPhaseId('aprovad');
        if (aprovadoPhaseId) {
          const nomeCard = (ficha.nomeContato+' (Loja)').replace(/"/g,"'").slice(0,255);
          const telCard  = (ficha.telefone||'').replace(/"/g,"'").slice(0,100);
          const data = await pipefyQ(
            'mutation{createCard(input:{pipe_id:"'+PIPE_ID+'" phase_id:"'+aprovadoPhaseId+'" title:"'+titleCompleto+'" fields_attributes:[{field_id:"nome_do_contato" field_value:"'+nomeCard+'"},{field_id:"telefone" field_value:"'+telCard+'"}]}){card{id}}}'
          );
          pipefyId = data?.createCard?.card?.id || null;
          if (pipefyId) {
            ficha.pipefyCardId = pipefyId;
            // Registrar no Pipe ADM
            await registrarNoPipe({ pipefyId, fichaId: ficha.id, phase: 'aprovados', nomeContato: ficha.nomeContato||'', telefone: ficha.telefone||'', equipamento: ficha.orcamento?.equipamento||'', valor: ficha.orcamento?.valor||0, origem:'frenteloja' }).catch(()=>{});
            console.log('[FL] Pipefy card criado:', pipefyId);
            // Atualizar valor no Pipefy (fire-and-forget)
            const vn = String(parseFloat(ficha.orcamento?.valor||0).toFixed(2));
            pipefyQ('mutation{updateCardField(input:{card_id:"'+pipefyId+'" field_id:"valor_de_contrato" new_value:"'+vn+'"}){success}}').catch(()=>{});
          }
        } else {
          console.error('[FL] Fase Aprovado nao encontrada no Pipefy');
          ficha.pipefyPending = true; // cron vai retentar
        }
      } catch(e) {
        console.error('[FL] Pipefy createCard:', e.message);
        ficha.pipefyPending = true; // cron vai retentar
      }

      // ── PASSO 2: Criar card no board DIRETAMENTE no Redis (SÍNCRONO) ─
      try {
        const boardDb = await dbGet('tv_board') || {};
        const boardCards = boardDb.cards || [];
        // Mover analise_loja → cliente_loja se já existe, senão criar
        const existente = boardCards.find(c => c.flFichaId === ficha.id);
        if (existente) {
          existente.phaseId = 'cliente_loja';
          existente.movedAt = now;
          existente.movedBy = 'Aprovação FL';
          if (pipefyId) { existente.pipefyId = String(pipefyId); }
        } else {
          boardCards.unshift({
            id:          ficha.id+'-loja',
            pipefyId:    pipefyId ? String(pipefyId) : ficha.id,
            flFichaId:   ficha.id,
            title:       titleCompleto,
            nomeContato: (ficha.nomeContato||'').replace(/\(Loja\)/g,'').trim(),
            telefone:    ficha.telefone||'',
            phaseId:     'cliente_loja',
            addedAt:     now,
            movedAt:     now,
            movedBy:     'Aprovação FL',
          });
        }
        boardDb.cards = boardCards;
        // Registrar aprovado_entrada no movesLog — contabiliza nas Metas do técnico
        if (!Array.isArray(boardDb.movesLog)) boardDb.movesLog = [];
        boardDb.movesLog.push({
          phaseId:   'aprovado_entrada',
          pipefyId:  pipefyId ? String(pipefyId) : null,
          timestamp: now,
          origem:    'frente_loja',
          fichaId:   ficha.id
        });
        const cutoff90fl = new Date(Date.now()-90*24*60*60*1000).toISOString();
        boardDb.movesLog = boardDb.movesLog.filter(m=>(m.timestamp||m.ts||'')>cutoff90fl);
        await dbSet('tv_board', boardDb);
      } catch(e) { console.error('[FL] board direct write:', e.message); }

      // ── PASSO 3: Registrar no Balcão (aguardando pagamento) ─────────
      try {
        const BALCAO_KEY = 'tv_balcao';
        const balcao = (await dbGet(BALCAO_KEY)) || [];
        const balcaoId = pipefyId ? String(pipefyId) : ficha.id;
        if (!balcao.find(b => b.pipefyId === balcaoId)) {
          balcao.unshift({
            pipefyId:    balcaoId,
            flFichaId:   ficha.id,
            nomeContato: ficha.nomeContato || '—',
            osCode:      ficha.id,
            descricao:   ficha.descricao || null,
            telefone:    ficha.telefone || null,
            tecnico:     null,
            entradaEm:   now,
            status:      'aguardando_pagamento',
            pagoEm:      null,
          });
          await dbSet(BALCAO_KEY, balcao);
          console.log('[FL] Balcao registrado:', balcaoId);
        }
      } catch(e) { console.error('[FL] balcao registro:', e.message); }

      // ── PASSO 4: Salvar FL e responder ───────────────────────────────
      await dbSet(FL_KEY, db);

      logAction({ modulo:'Frente de Loja', fichaId:ficha.id||'', ficha:ficha.nomeContato||'', acao:'Liberar equipamento', para:'receber', gatilho:'→ Pipe receber + Pipefy Receber$', status:'ok', detalhe:'Valor: R$'+(ficha.pagoValor||0)+' '+ficha.pagoPor }).catch(()=>{});
    return res.status(200).json({ok:true,ficha});
    }
  }


  // ── GET buscar-finalizar: localiza ficha por nome+tel e retorna ID ──────────
  if (action === 'buscar-finalizar') {
    const nome = (req.query.nome || '').toLowerCase();
    const tel4 = (req.query.tel4 || '');
    const db   = await dbGet(FL_KEY) || defaultDB();
    const ficha = db.fichas.find(f =>
      (f.nomeContato || '').toLowerCase().startsWith(nome) &&
      (f.telefone || '').replace(/\D/g,'').slice(-4) === tel4 &&
      f.phase === 'producao'
    );
    if (!ficha) return res.status(404).json({ ok:false, error:'Ficha não encontrada em produção', nome, tel4 });
    return res.status(200).json({ ok:true, id:ficha.id, nome:ficha.nomeContato, fase:ficha.phase, pipefyCardId:ficha.pipefyCardId||null });
  }


  // ── GET finalizar-por-nome: localiza por nome+tel4 e move para conserto_realizado ──
  if (action === 'finalizar-por-nome') {
    const nome = (req.query.nome || '').toLowerCase();
    const tel4 = (req.query.tel4 || '');
    const db   = await dbGet(FL_KEY) || defaultDB();
    const ficha = db.fichas.find(f =>
      (f.nomeContato || '').toLowerCase().startsWith(nome) &&
      (f.telefone || '').replace(/\D/g,'').slice(-4) === tel4 &&
      f.phase === 'producao'
    );
    if (!ficha) return res.status(404).json({ ok:false, error:'Ficha não encontrada em produção', nome, tel4 });
    const now = new Date().toISOString();
    ficha.phase        = 'conserto_realizado';
    ficha.liberadoHoje = true;
    ficha.movedAt      = now;
    ficha.history      = (ficha.history||[]).concat([{ phase:'conserto_realizado', ts:now, via:'admin_manual' }]);
    await dbSet(FL_KEY, db);
    // Mover no Pipefy se tiver card
    if (ficha.pipefyCardId) {
      try {
        const phId = await getPipefyPhaseId('conserto');
        if (phId) await movePipefyCard(ficha.pipefyCardId, phId);
      } catch(pe) { console.error('[finalizar-por-nome] Pipefy:', pe.message); }
    }
    return res.status(200).json({ ok:true, finalizado:true, id:ficha.id, nome:ficha.nomeContato, fase:'conserto_realizado' });
  }


  // ── POST marcar-orc-whatsapp: marca orçamento como enviado pelo WhatsApp ──
  if (req.method === 'POST' && action === 'marcar-orc-whatsapp') {
    const { id } = req.body || {};
    if (!id) return res.status(400).json({ ok:false, error:'id obrigatorio' });
    const db = await dbGet(FL_KEY) || defaultDB();
    const ficha = db.fichas.find(f => f.id === id);
    if (!ficha) return res.status(404).json({ ok:false, error:'ficha nao encontrada' });
    ficha.orcEnviadoWpp     = true;
    ficha.orcEnviadoWppEm   = new Date().toISOString();
    await dbSet(FL_KEY, db);
    return res.status(200).json({ ok:true, id });
  }

  // ── fix-loja-feito-por-nome — conserta ficha presa em producao por nome/tel ──
  if (action === 'fix-loja-feito-por-nome') {
    const nome = (req.query.nome || '').toLowerCase().trim();
    const tel4 = (req.query.tel4 || '').replace(/\D/g,'');
    if (!nome) return res.status(400).json({ ok:false, error:'?nome= obrigatório' });
    const db = await dbGet(FL_KEY) || defaultDB();
    const now = new Date().toISOString();
    // Buscar em producao
    const fichas = (db.fichas||[]).filter(function(f){
      const nomeMatch = (f.nomeContato||'').toLowerCase().includes(nome);
      const telMatch  = !tel4 || (f.telefone||'').replace(/\D/g,'').slice(-4) === tel4;
      return nomeMatch && telMatch && f.phase === 'producao';
    });
    if (!fichas.length) return res.status(404).json({ ok:false, error:'Ficha não encontrada em producao com esse nome/tel', nome, tel4 });
    fichas.forEach(function(f){
      f.phase      = 'conserto_realizado';
      f.consertoEm = now;
      f.updatedAt  = now;
    });
    await dbSet(FL_KEY, db);
    return res.status(200).json({ ok:true, consertadas: fichas.length, fichas: fichas.map(function(f){ return {id:f.id, nome:f.nomeContato, phase:f.phase}; }) });
  }

  if(req.method==='POST'&&action==='conserto-realizado'){
    const {pipefyCardId, fichaId}=req.body||{};
    if(!pipefyCardId && !fichaId)
      return res.status(400).json({ok:false,error:'pipefyCardId ou fichaId obrigatório'});
    const db=await dbGet(FL_KEY)||defaultDB();
    // Buscar por fichaId (preferencial, sem Pipefy) ou por pipefyCardId (legado)
    const ficha = fichaId
      ? db.fichas.find(f=>f.id===String(fichaId))
      : db.fichas.find(f=>f.pipefyCardId===String(pipefyCardId));
    if(!ficha)return res.status(404).json({ok:false,error:'Ficha não encontrada'});
    const now=new Date().toISOString();
    ficha.phase='conserto_realizado';ficha.liberadoHoje=true;ficha.movedAt=now;
    ficha.history=(ficha.history||[]).concat([{phase:'conserto_realizado',ts:now}]);
    await dbSet(FL_KEY,db);
    logAction({ modulo:'Frente de Loja', fichaId:ficha.id||'', ficha:ficha.nomeContato||'', acao:'Liberar equipamento', para:'receber', gatilho:'→ Pipe receber + Pipefy Receber$', status:'ok', detalhe:'Valor: R$'+(ficha.pagoValor||0)+' '+ficha.pagoPor }).catch(()=>{});
    return res.status(200).json({ok:true,ficha});
  }

  if(req.method==='POST'&&action==='programar-entrega'){
    const {id}=req.body||{};
    const db=await dbGet(FL_KEY)||defaultDB();
    const ficha=db.fichas.find(f=>f.id===id);
    if(!ficha)return res.status(404).json({ok:false,error:'Não encontrada'});
    try{const phId=await getPipefyPhaseId('programar entrega');if(ficha.pipefyCardId&&phId)await movePipefyCard(ficha.pipefyCardId,phId);}catch(e){}
    return res.status(200).json({ok:true});
  }

  if(req.method==='POST'&&action==='liberar'){
    const {id,valor,formaPagamento}=req.body||{};
    if(!id)return res.status(400).json({ok:false,error:'id obrigatório'});
    const db=await dbGet(FL_KEY)||defaultDB();
    const ficha=db.fichas.find(f=>f.id===id);
    if(!ficha)return res.status(404).json({ok:false,error:'Não encontrada'});
    const now=new Date().toISOString();
    ficha.phase='pago';ficha.pagoEm=now;ficha.pagoValor=parseFloat(valor)||ficha.orcamento?.valor||0;
    ficha.pagoPor=formaPagamento||ficha.orcamento?.formaPagamento||'pix';ficha.movedAt=now;
    ficha.history=(ficha.history||[]).concat([{phase:'pago',ts:now}]);
    // Mover no Pipefy para "Receber $"
    try {
      const phId = await getPipefyPhaseId('receber');
      if (phId) {
        // Tentar pelo pipefyCardId da ficha, senão buscar no board pelo flFichaId
        let cardId = ficha.pipefyCardId || null;
        if (!cardId) {
          const boardDb = await dbGet('tv_board');
          const card = (boardDb?.cards||[]).find(c => c.flFichaId === ficha.id && c.pipefyId && !String(c.pipefyId).startsWith('FL-'));
          if (card) cardId = card.pipefyId;
        }
        if (cardId) {
          await movePipefyCard(String(cardId), phId);
          console.log('[FL] liberar→Receber$:', ficha.id, cardId, phId);
        } else {
          console.warn('[FL] liberar: pipefyCardId nulo para', ficha.id);
        }
      } else {
        console.warn('[FL] liberar: fase Receber$ nao encontrada no Pipefy');
      }
    } catch(e) { console.error('[FL] liberar→Pipefy:', e.message); }
    await dbSet(FL_KEY,db);
    // Pipe ADM: mover para receber
    if (ficha.pipefyCardId) {
      await moverNoPipe(ficha.pipefyCardId, 'receber', { nomeContato: ficha.nomeContato, valor: ficha.pagoValor }).catch(() => {});
    }
    logAction({ modulo:'Frente de Loja', fichaId:ficha.id||'', ficha:ficha.nomeContato||'', acao:'Liberar equipamento', para:'receber', gatilho:'→ Pipe receber + Pipefy Receber$', status:'ok', detalhe:'Valor: R$'+(ficha.pagoValor||0)+' '+ficha.pagoPor }).catch(()=>{});
    return res.status(200).json({ok:true,ficha});
  }


  // ── POST retentar-board — registra no board fichas que ficaram sem card ──
  if (req.method === 'POST' && action === 'retentar-board') {
    const { id } = req.body || {};
    if (!id) return res.status(400).json({ ok:false, error:'id obrigatorio' });
    const db = await dbGet(FL_KEY) || defaultDB();
    const ficha = db.fichas.find(f => f.id === id);
    if (!ficha) return res.status(404).json({ ok:false, error:'Ficha nao encontrada' });
    const titulo = (ficha.nomeContato + ' (Loja) - ' + ficha.equipamento +
      ' | ' + (ficha.descricao||'') +
      ' | Diag: ' + (ficha.descricaoTecnica||'') +
      ' | R$' + String(parseFloat(ficha.orcamento?.valor||0).toFixed(2)) +
      ' ' + (ficha.orcamento?.formaPagamento||'') +
      ' OS:' + ficha.id
    ).replace(/"/g,"'").slice(0,255);
    const boardBase = process.env.FL_BASE_URL || 'https://reparoeletroadm.com';
    const r = await fetch(boardBase+'/api/board?action=add-loja-card', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({
        flFichaId:   ficha.id,
        pipefyId:    ficha.pipefyCardId || null,
        title:       titulo,
        nomeContato: ficha.nomeContato||'',
        telefone:    ficha.telefone||'',
        phaseId:     'cliente_loja',
      })
    }).then(r=>r.json()).catch(e=>({ok:false,error:e.message}));
    return res.status(200).json({ ok: r.ok, msg: r.msg || null, error: r.error || null });
  }


  // ── GET sync-fl — corrige fichas presas (loja_feito board ≠ conserto_realizado FL) ──
  if (action === 'sync-fl') {
    const BOARD_KEY2 = 'tv_board';
    const boardDb = await dbGet(BOARD_KEY2) || { cards: [] };
    const flDb = await dbGet(FL_KEY) || defaultDB();
    const lojaFeitoCards = (boardDb.cards || []).filter(c => c.phaseId === 'loja_feito');
    let corrigidos = 0;
    for (const card of lojaFeitoCards) {
      // 3 formas: flFichaId > OS: no título > pipefyId × pipefyCardId
      const osMatch = (card.title || '').match(/OS:([a-zA-Z0-9-]+)/);
      let ficha = null;
      if (card.flFichaId)
        ficha = flDb.fichas.find(f => f.id === String(card.flFichaId));
      if (!ficha && osMatch)
        ficha = flDb.fichas.find(f => f.id === String(osMatch[1]));
      if (!ficha && card.pipefyId)
        ficha = flDb.fichas.find(f => f.pipefyCardId && String(f.pipefyCardId) === String(card.pipefyId));
      if (!ficha || ficha.phase !== 'producao') continue;
      // Ficha presa: está em loja_feito no board mas producao no FL → corrigir
      const now = new Date().toISOString();
      ficha.phase = 'conserto_realizado';
      ficha.liberadoHoje = true;
      ficha.movedAt = now;
      ficha.history = (ficha.history || []).concat([{ phase: 'conserto_realizado', ts: now, origem: 'sync-fl' }]);
      card.flFichaId = ficha.id; // guardar para notificações futuras
      corrigidos++;
    }
    if (corrigidos > 0) {
      await dbSet(FL_KEY, flDb);
      await dbSet(BOARD_KEY2, boardDb);
    }
    // Corrigir cards de loja criados em producao que deveriam estar em cliente_loja
    const BOARD_KEY3 = 'tv_board';
    const boardDb2 = await dbGet(BOARD_KEY3) || { cards: [] };
    let corrigidosLoja = 0;
    for(const card of boardDb2.cards){
      if(card.phaseId === 'producao' && card.flFichaId && String(card.id).includes('-loja')){
        card.phaseId = 'cliente_loja';
        corrigidosLoja++;
      }
    }
    if(corrigidosLoja > 0) await dbSet(BOARD_KEY3, boardDb2);
    // Sync retroativo: fichas em analise sem card → analise_loja
    const boardDb4 = await dbGet(BOARD_KEY2) || { cards: [] };
    const boardFlIds = new Set(boardDb4.cards.map(c => c.flFichaId).filter(Boolean));
    const semCard = (flDb.fichas||[]).filter(f => f.phase === 'analise' && !boardFlIds.has(f.id));
    let syncAnalise = 0;
    const _base = process.env.FL_BASE_URL || 'https://reparoeletroadm.com';
    for (const f of semCard) {
      try {
        const t = (f.nomeContato+' (Loja) - '+(f.equipamento||'')+' | '+(f.descricao||'')+' OS:'+f.id).replace(/"/g,"'").slice(0,255);
        await fetch(_base+'/api/board?action=add-loja-card',{
          method:'POST', headers:{'Content-Type':'application/json'},
          body: JSON.stringify({ flFichaId:f.id, pipefyId:null, title:t, nomeContato:f.nomeContato||'', telefone:f.telefone||'', phaseId:'analise_loja' })
        });
        syncAnalise++;
      } catch(e){ console.error('[sync-fl] analise→board:', e.message); }
    }
    return res.status(200).json({ ok: true, corrigidos, corrigidosLoja, syncAnalise, total: lojaFeitoCards.length });
  }


  // ── GET limpar-erp — remove fichas FL que já foram para ERP/Finalizado no Pipefy ──
  if (action === 'limpar-erp') {
    const db = await dbGet(FL_KEY) || defaultDB();

    // 1. Buscar todas as fases do pipe com seus cards
    let erpIds = new Set();
    let fasesErp = [];
    try {
      const data = await pipefyQ(`query {
        pipe(id: "${PIPE_ID}") {
          phases {
            id name
            cards(first: 100) {
              edges { node { id } }
              pageInfo { hasNextPage endCursor }
            }
          }
        }
      }`);
      const phases = data?.pipe?.phases || [];
      for (const ph of phases) {
        const l = ph.name.toLowerCase();
        if (l.includes('erp') || l.includes('finaliz') || l.includes('conclu') ||
            l.includes('descar') || l.includes('reprov')) {
          fasesErp.push(ph.name);
          ph.cards.edges.forEach(e => erpIds.add(String(e.node.id)));
          // Paginação
          let cursor = ph.cards.pageInfo?.hasNextPage ? ph.cards.pageInfo.endCursor : null;
          while (cursor) {
            const data2 = await pipefyQ(`query {
              pipe(id: "${PIPE_ID}") {
                phases {
                  name
                  cards(first: 100, after: "${cursor}") {
                    edges { node { id } }
                    pageInfo { hasNextPage endCursor }
                  }
                }
              }
            }`);
            const ph2 = (data2?.pipe?.phases||[]).find(p=>p.name===ph.name);
            if (!ph2) break;
            ph2.cards.edges.forEach(e => erpIds.add(String(e.node.id)));
            cursor = ph2.cards.pageInfo?.hasNextPage ? ph2.cards.pageInfo.endCursor : null;
          }
        }
      }
    } catch(e) {
      // Pipefy best-effort: falha no sync não bloqueia
      console.warn('[fl] sync-fl Pipefy falhou:', e.message);
      // erpIds fica vazio — nenhuma ficha removida, sistema continua
    }

    // 2. Encontrar fichas FL cujo pipefyCardId está em ERP/Finalizado
    const antes = db.fichas.length;
    const removidas = db.fichas.filter(f => f.pipefyCardId && erpIds.has(String(f.pipefyCardId)));
    const idsRemovidos = removidas.map(f => ({ id:f.id, nome:f.nomeContato, phase:f.phase, pipefyId:f.pipefyCardId }));

    // 3. Remover do array
    db.fichas = db.fichas.filter(f => !f.pipefyCardId || !erpIds.has(String(f.pipefyCardId)));

    if (removidas.length > 0) await dbSet(FL_KEY, db);

    return res.status(200).json({
      ok: true,
      fasesErp,
      totalErpIds: erpIds.size,
      removidas: idsRemovidos,
      antes,
      depois: db.fichas.length
    });
  }


  // ── GET retry-pipefy-pending — cria cards Pipefy que falharam ──────
  if (action === 'retry-pipefy-pending') {
    const db = await dbGet(FL_KEY) || defaultDB();
    // Fichas pendentes: pipefyPending=true OU producao sem pipefyCardId há mais de 2 min
    const cutoff = new Date(Date.now() - 2 * 60 * 1000).toISOString();
    const pendentes = db.fichas.filter(f =>
      f.phase === 'producao' &&
      !f.pipefyCardId &&
      (f.pipefyPending || (f.movedAt && f.movedAt < cutoff))
    );

    if (pendentes.length === 0) return res.status(200).json({ ok:true, pendentes:0 });

    let criados = 0, erros = 0;
    const aprovadoPhaseId = await getPipefyPhaseId('aprovad').catch(()=>null);
    if (!aprovadoPhaseId) return res.status(200).json({ ok:false, error:'Fase Aprovado nao encontrada', pendentes:pendentes.length });

    for (const ficha of pendentes) {
      try {
        const titleCompleto=(ficha.nomeContato+' (Loja) - '+(ficha.equipamento||'')+
          ' | '+(ficha.descricao||'')+' | Diag: '+(ficha.descricaoTecnica||'')+
          ' | R$'+String(parseFloat(ficha.orcamento?.valor||0).toFixed(2))+
          ' '+(ficha.orcamento?.formaPagamento||'pix')+' OS:'+ficha.id
        ).replace(/"/g,"'").slice(0,255);
        const nomeCard = (ficha.nomeContato+' (Loja)').replace(/"/g,"'").slice(0,255);
        const telCard  = (ficha.telefone||'').replace(/"/g,"'").slice(0,100);
        const data = await pipefyQ(
          'mutation{createCard(input:{pipe_id:"'+PIPE_ID+'" phase_id:"'+aprovadoPhaseId+'" title:"'+titleCompleto+'" fields_attributes:[{field_id:"nome_do_contato" field_value:"'+nomeCard+'"},{field_id:"telefone" field_value:"'+telCard+'"}]}){card{id}}}'
        );
        const pipefyId = data?.createCard?.card?.id || null;
        if (pipefyId) {
          ficha.pipefyCardId = pipefyId;
          ficha.pipefyPending = false;
          // Atualizar no board
          const boardDb = await dbGet('tv_board') || {};
          const card = (boardDb.cards||[]).find(c => c.flFichaId === ficha.id);
          if (card && (!card.pipefyId || card.pipefyId === ficha.id)) {
            card.pipefyId = String(pipefyId);
            await dbSet('tv_board', boardDb);
          }
          // Atualizar valor no Pipefy
          const vn = String(parseFloat(ficha.orcamento?.valor||0).toFixed(2));
          pipefyQ('mutation{updateCardField(input:{card_id:"'+pipefyId+'" field_id:"valor_de_contrato" new_value:"'+vn+'"}){success}}').catch(()=>{});
          criados++;
          console.log('[FL] retry-pipefy: criado', pipefyId, 'para', ficha.id);
        }
      } catch(e) {
        erros++;
        console.error('[FL] retry-pipefy:', ficha.id, e.message);
      }
    }

    await dbSet(FL_KEY, db);
    return res.status(200).json({ ok:true, pendentes:pendentes.length, criados, erros });
  }

  if(action==='rastrear'){
    const q=(req.query.q||'').trim().toLowerCase();
    if(!q)return res.status(400).json({ok:false,error:'Query obrigatória'});
    const db=await dbGet(FL_KEY)||defaultDB();
    const found=db.fichas.filter(f=>(f.id+' '+f.nomeContato+' '+f.telefone+' '+f.equipamento).toLowerCase().includes(q));
    return res.status(200).json({ok:true,fichas:found});
  }

  if(req.method==='POST'&&action==='mover'){
    const {id,phase,dados}=req.body||{};
    if(!id||!phase)return res.status(400).json({ok:false,error:'id e phase obrigatórios'});
    const db=await dbGet(FL_KEY)||defaultDB();
    const ficha=db.fichas.find(f=>f.id===id);
    if(!ficha)return res.status(404).json({ok:false,error:'Ficha não encontrada'});
    const now=new Date().toISOString();
    ficha.phase=phase;ficha.movedAt=now;
    ficha.history=(ficha.history||[]).concat([{phase,ts:now}]);
    if(dados&&phase==='endereco')ficha.endereco=dados;
    if(phase==='liberado_hoje')ficha.liberadoHoje=true;
    await dbSet(FL_KEY,db);
    // Pipe ADM: mover para receber
    if (ficha.pipefyCardId) {
      await moverNoPipe(ficha.pipefyCardId, 'receber', { nomeContato: ficha.nomeContato, valor: ficha.pagoValor }).catch(() => {});
    }
    logAction({ modulo:'Frente de Loja', fichaId:ficha.id||'', ficha:ficha.nomeContato||'', acao:'Liberar equipamento', para:'receber', gatilho:'→ Pipe receber + Pipefy Receber$', status:'ok', detalhe:'Valor: R$'+(ficha.pagoValor||0)+' '+ficha.pagoPor }).catch(()=>{});
    return res.status(200).json({ok:true,ficha});
  }

  // Limpar fichas reprovadas do banco
  if(action==='limpar-reprovados'){
    const db=await dbGet(FL_KEY)||defaultDB();
    const antes=db.fichas.length;
    db.fichas=db.fichas.filter(f=>f.phase!=='reprovado');
    const removidos=antes-db.fichas.length;
    await dbSet(FL_KEY,db);
    return res.status(200).json({ok:true,removidos});
  }

  return res.status(404).json({ok:false,error:'Ação não encontrada'});
}
