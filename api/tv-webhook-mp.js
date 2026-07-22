
// ── Trigger: move/cria card no tv_pipe ──────────────────────────────────
async function moverNoTvPipe(phase, dados){
  try {
    const _U=(process.env.UPSTASH_URL||'').replace(/['"]/g,'').trim();
    const _T=(process.env.UPSTASH_TOKEN||'').replace(/['"]/g,'').trim();
    async function _g(k){const r=await fetch(_U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+_T,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});const j=await r.json();const v=j[0]?.result;if(!v)return null;try{let x=JSON.parse(v);if(typeof x==='string')x=JSON.parse(x);return x;}catch(e){return null;}}
    async function _s(k,v){await fetch(_U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+_T,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});}
    const pipe=(await _g('tv_pipe'))||{cards:[],lastSync:null};
    if(!Array.isArray(pipe.cards))pipe.cards=[];
    const now=new Date().toISOString();
    const jaExiste=dados.localId&&pipe.cards.find(c=>c.localId===String(dados.localId)||c.id===String(dados.localId));
    if(jaExiste){jaExiste.phase=phase;jaExiste.movedAt=now;}
    else{pipe.cards.unshift({id:'PIPE-TV-'+Date.now().toString(36).toUpperCase()+'-'+Math.random().toString(36).slice(2,5).toUpperCase(),localId:dados.localId||null,pipefyId:dados.pipefyId||null,phase,nomeContato:dados.nome||'',telefone:dados.telefone||'',equipamento:dados.equipamento||'',descricao:dados.descricao||'',endereco:dados.endereco||'',valor:parseFloat(dados.valor)||0,origem:dados.origem||'sistema',criadoEm:now,movedAt:now,aguardandoDesde:phase==='aguardando_aprovacao'?now:null,history:[],analiseCompra:false});}
    pipe.lastSync=now;
    await _s('tv_pipe',pipe);
  }catch(e){console.error('[tv_pipe trigger]',e.message);}
}
'use strict';
// TV WEBHOOK MP — espelho ADM | FASE 6 | 01/06/2026

// api/webhook-mp.js
const UPSTASH_URL   = (process.env.UPSTASH_URL   || '').replace(/['"]/g, '').trim();
const UPSTASH_TOKEN = (process.env.UPSTASH_TOKEN || '').replace(/['"]/g, '').trim();
const MP_TOKEN      = (process.env.MP_ACCESS_TOKEN || '').replace(/['"]/g, '').trim();
const LOG_KEY       = 'mp_webhook_log';
const PROC_KEY      = 'tv_mp_processados'; // IDs já processados (idempotência)
const FIN_KEY2      = 'tv_financeiro';
const FIN_CONCIL2   = 'fin_conciliacao';
const FIN_RETRY_KEY = 'tv_fin_webhook_retry';
const SOLICITAR_ENTREGA_FIN = '334875186';

async function dbGet(key) {
  if (!UPSTASH_URL || !UPSTASH_TOKEN) return null;
  const r = await fetch(`${UPSTASH_URL}/pipeline`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${UPSTASH_TOKEN}`, 'Content-Type': 'application/json' },
    body: JSON.stringify([['GET', key]])
  });
  const j = await r.json();
  const result = j[0]?.result;
  if (!result) return null;
  const v1 = JSON.parse(result);
  // Tratar dupla serialização de writes anteriores
  if (typeof v1 === 'string') { try { return JSON.parse(v1); } catch(e) { return v1; } }
  return v1;
}

async function dbSet(key, value) {
  if (!UPSTASH_URL || !UPSTASH_TOKEN) return;
  await fetch(`${UPSTASH_URL}/pipeline`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${UPSTASH_TOKEN}`, 'Content-Type': 'application/json' },
    body: JSON.stringify([['SET', key, JSON.stringify(value)]])
  });
}

async function jaProcessado(paymentId) {
  const lista = (await dbGet(PROC_KEY)) || [];
  return lista.includes(String(paymentId));
}

async function marcarProcessado(paymentId) {
  const lista = (await dbGet(PROC_KEY)) || [];
  lista.unshift(String(paymentId));
  await dbSet(PROC_KEY, lista.slice(0, 500)); // manter últimos 500
}

async function logEvento(evento) {
  try {
    const logs = (await dbGet(LOG_KEY)) || [];
    logs.unshift({ ...evento, ts: new Date().toISOString() });
    await dbSet(LOG_KEY, logs.slice(0, 200));
  } catch(e) { console.error('logEvento:', e.message); }
}



// ── moverNoPipe: move card no Pipe ADM (solicitar_entrega) ──────────────
async function moverCardNoPipe(pipefyId, osCode, novaFase) {
  try {
    const U=(process.env.UPSTASH_URL||'').replace(/['"]/g,'').trim();
    const T=(process.env.UPSTASH_TOKEN||'').replace(/['"]/g,'').trim();
    const PIPE_KEY='tv_pipe';
    async function _pg(k){const r=await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});const j=await r.json();const v=j[0]?.result;if(!v)return null;try{let x=JSON.parse(v);if(typeof x==='string')x=JSON.parse(x);return x;}catch(e){return null;}}
    async function _ps(k,v){await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});}
    const db=await _pg(PIPE_KEY);
    if(!db||!Array.isArray(db.cards))return false;
    const pipefyStr  = pipefyId ? String(pipefyId) : null;
    const osCodeStr  = osCode   ? String(osCode)   : null;
    const nomeStr2 = dados.nome ? String(dados.nome).toLowerCase().trim() : null;
    const card=db.cards.find(function(c){
      return (pipefyStr && (c.pipefyId===pipefyStr || c.id===pipefyStr)) ||
             (osCodeStr && (c.id===osCodeStr || c.pipefyId===osCodeStr)) ||
             (nomeStr2  && (
               (c.nomeContato||'').toLowerCase().trim()===nomeStr2 ||
               (c.nomeContato||'').toLowerCase().includes(nomeStr2) ||
               nomeStr2.includes((c.nomeContato||'').toLowerCase().trim().split(' ')[0])
             ));
    });
    // Se não achou o card, criar novo na fase solicitada (venda checkout sem card prévio)
    if(!card){
      const now2=new Date().toISOString();
      const novoCard={
        id:'TVPIPE-'+Date.now().toString(36).toUpperCase(),
        phase: novaFase,
        nomeContato: dados.nome||'',
        telefone: dados.telefone||'',
        equipamento: dados.equipamento||dados.descricao||'',
        valor: dados.valor||0,
        origem: dados.origem||'tv_checkout',
        criadoEm: now2, movedAt: now2,
        history:[{phase:novaFase,ts:now2,via:'tv_webhook_mp_auto'}]
      };
      db.cards.unshift(novoCard);
      await _ps(PIPE_KEY,db);
      return true;
    }
    const now=new Date().toISOString();
    card.history=(card.history||[]).concat([{phase:card.phase,ts:now}]);
    card.phase=novaFase;
    card.movedAt=now;
    await _ps(PIPE_KEY,db);
    return true;
  }catch(e){console.error('[pipe-move]',e.message);return false;}
}

// ── Financeiro: processar pagamento MP → entrega_liberada ────────────────────
async function processarPagamentoFinanceiro(pmt) {
  const meta     = pmt.metadata || {};
  const fichaId  = meta.fichaId || meta.ficha_id; // MP converte camelCase→snake_case
  const prefId   = pmt.collector?.id ? null : pmt.additional_info?.items?.[0]?.id; // fallback
  const valor    = pmt.transaction_amount;
  const metodo   = pmt.payment_method_id;
  const now      = new Date().toISOString();

  const fin = await dbGet(FIN_KEY2);
  if (!fin || !fin.records) {
    // Redis pode ter sido atualizado agora — enfileirar retry
    await enfileirarRetry(pmt);
    return { ok:false, info:"ficha_nao_encontrada_retry_agendado" };
  }

  // Buscar ficha por fichaId OU por preferenceId
  const prefIdMp = pmt.preference_id;
  const rec = fin.records.find(r =>
    (fichaId && r.id === fichaId) ||
    (prefIdMp && r.mp?.preferenceId === prefIdMp)
  );

  if (!rec) {
    await enfileirarRetry(pmt);
    return { ok:false, info:"ficha_nao_encontrada_retry_agendado", fichaId, prefIdMp };
  }

  // Já processado?
  if (rec.mp?.paymentId === String(pmt.id)) {
    return { ok:false, info:"ja_processado", fichaId: rec.id };
  }

  // ── VALIDAÇÃO CRÍTICA TV: só processar se pagamento APROVADO ────────────────
  const pmtStatusTV = pmt.status || '';
  if (pmtStatusTV !== 'approved') {
    console.warn('[tv-webhook-fin] Pagamento NÃO aprovado — ignorando:', {
      fichaId: rec.id, status: pmtStatusTV, detail: pmt.status_detail, paymentId: pmt.id
    });
    await salvarConciliacaoFin({ rec, pmt, metodo, valor, now,
      status: 'nao_aprovado_' + pmtStatusTV }).catch(()=>{});
    return { ok:false, info:'pagamento_nao_aprovado_tv', fichaId:rec.id,
      status:pmtStatusTV, paymentId:pmt.id };
  }

  // Só mover se ainda em faturamento ou pagamento_agendado
  const fasesAceitas = ["faturamento","pagamento_agendado","nf_emitida","pagamento_confirmado","aguardando_dados","analise_pagamento"];
  if (!fasesAceitas.includes(rec.phaseId)) {
    // Só registrar na conciliação, não mover
    await salvarConciliacaoFin({ rec, pmt, metodo, valor, now, status:"ja_em_"+rec.phaseId });
    return { ok:true, info:"ficha_ja_movida", fichaId:rec.id, fase:rec.phaseId };
  }

  // Mover DIRETAMENTE para entrega_liberada numa operação atômica
  // NÃO usa fetch interno (era frágil — falhava silenciosamente)
  rec.mp      = { ...(rec.mp||{}), paymentId:String(pmt.id), pagoEm:now, metodo, valor, status:"pago" };
  rec.paidAt  = now;
  rec.movedAt = now;
  rec.phaseId = "entrega_liberada";
  rec.history = [...(rec.history||[]),
    { phaseId:"pagamento_confirmado", ts:now, via:"webhook_mp", paymentId:String(pmt.id) },
    { phaseId:"entrega_liberada",     ts:now, via:"webhook_mp_auto" }
  ];
  await dbSet(FIN_KEY2, fin);

  // Pipe ADM: mover para solicitar_entrega
  const pipeOk = await moverCardNoPipe(rec.pipefyId, rec.osCode, 'solicitar_entrega').catch(()=>false);
  console.log('[webhook-mp] Pipe ADM move:', pipeOk);

  // Pipefy: mover para Solicitar Entrega — função própria, sem fetch interno
  // Trigger: tv_pipe → solicitar_entrega (pagamento confirmado)
  await moverNoTvPipe('solicitar_entrega', {
    localId: rec.id||null, pipefyId: rec.pipefyId||null,
    nome: rec.nome||rec.nomeContato||'', telefone: rec.telefone||'',
    equipamento: rec.equipamento||'', descricao: rec.descricao||'',
    valor: rec.valor||0, origem:'tv_webhook_mp_pago'
  });
  const pipefyOk = false; // Pipefy desconectado

  // Salvar conciliação
  await salvarConciliacaoFin({ rec, pmt, metodo, valor, now, status:"pago", pipefyOk });
  return { ok:true, fichaId:rec.id, pipefyOk, valor, metodo };
}

async function enfileirarRetry(pmt) {
  try {
    const fila = (await dbGet(FIN_RETRY_KEY)) || [];
    // Não duplicar
    if (!fila.find(e => e.id === pmt.id)) {
      fila.push({ id:pmt.id, pmt, tentativas:0, ultimaTentativa:null, criadoEm:new Date().toISOString() });
      await dbSet(FIN_RETRY_KEY, fila.slice(-50)); // max 50
    }
  } catch(e) { console.error("[FinRetry] enfileirar:", e.message); }
}

async function salvarConciliacaoFin({ rec, pmt, metodo, valor, now, status, pipefyOk }) {
  try {
    const db = (await dbGet(FIN_CONCIL2)) || { transacoes:[] };
    db.transacoes = db.transacoes || [];
    if (db.transacoes.find(t => t.paymentId === String(pmt.id))) return;
    const d = new Date(new Date(now).toLocaleString("en-US",{timeZone:"America/Sao_Paulo"}));
    const dataBRT = d.getFullYear()+"-"+String(d.getMonth()+1).padStart(2,"0")+"-"+String(d.getDate()).padStart(2,"0");
    db.transacoes.unshift({
      tipo:         "pagamento_confirmado",
      fichaId:      rec.id,
      cliente: rec.nomeContato || rec.title || "",
      cpfCnpj:      rec.cpfCnpj || "",
    telefone:     rec.telefone  || "",
      valor:        parseFloat(valor||0),
      metodo:       metodo      || pmt.payment_method_id,
      parcelas:     pmt.installments || 1,
      preferenceId: rec.mp?.preferenceId || "",
      paymentId:    String(pmt.id),
      statusMp:     pmt.status,
      pipefyOk:     !!pipefyOk,
      data:         dataBRT,
      status,
      ts:           now
    });
    const cutoff = new Date(Date.now()-90*24*60*60*1000).toISOString();
    db.transacoes = db.transacoes.filter(t=>(t.ts||"")>cutoff).slice(0,2000);
    await dbSet(FIN_CONCIL2, db);
  } catch(e) { console.error("[ConcilFin]", e.message); }
}

// ── Pipefy: criar card Receber após venda confirmada pelo MP ────────────────
function sanitizePipefy(s) {
  return String(s||'').replace(/[\\]/g,'').replace(/"/g,"'").replace(/\n/g,' ')
    .normalize('NFD').replace(/[\u0300-\u036f]/g,'').replace(/[^\x00-\x7F]/g,'');
}

async function criarCardPipefyVenda(pipeId, produto, comprador, valor, paymentId) {
    const token = (process.env.PIPEFY_TOKEN || '').trim();
  if (!token || !pipeId) return null;

  async function pipefyQ(query) {
    const controller = new AbortController();
    const tid = setTimeout(() => controller.abort(), 10000);
    try {
          } catch(e) { clearTimeout(tid); throw e; }
  }

  // Buscar estrutura do pipe — fases + campos do formulário inicial
  const data = await pipefyQ('query { pipe(id: "' + pipeId + '") { phases { id name } start_form_fields { id label } } }');
  const phases = data?.pipe?.phases || [];
  const fields = data?.pipe?.start_form_fields || [];

  const phaseReceber = phases.find(p => p.name.toLowerCase().includes('receber'));
  if (!phaseReceber) throw new Error('Fase Receber nao encontrada no pipe ' + pipeId);

  // Encontrar campos dinamicamente por label (igual ao logistica.js)
  function findField(kws) {
    return fields.find(f => kws.some(kw => (f.label||'').toLowerCase().includes(kw)));
  }
  const nomeField = findField(['nome','contato','client']);
  const telField  = findField(['telefone','fone','celular','tel']);
  const descField = findField(['descri','empresa','observa','notas']);

  const nomeSafe  = sanitizePipefy(comprador.nome);
  const telSafe   = sanitizePipefy(comprador.telefone || '');
  const precoFmt  = parseFloat(valor).toLocaleString('pt-BR',{minimumFractionDigits:2,style:'currency',currency:'BRL'});
  const descSafe  = sanitizePipefy(
    (produto.descricao||'') + ' | Valor: ' + precoFmt +
    ' | MP #' + paymentId +
    (comprador.telefone ? ' | Tel: ' + (comprador.telefone||'') : '')
  );
  const tituloSafe = sanitizePipefy(
    'VENDA MP - ' + (produto.codigo || (produto.descricao||'').substring(0,25)) + ' | ' + (comprador.nome||'')
  );

  const fieldsAttr = [];
  if (nomeField) fieldsAttr.push('{ field_id: "' + nomeField.id + '" field_value: "' + nomeSafe + '" }');
  if (telField)  fieldsAttr.push('{ field_id: "' + telField.id  + '" field_value: "' + telSafe  + '" }');
  if (descField) fieldsAttr.push('{ field_id: "' + descField.id + '" field_value: "' + descSafe + '" }');

  const mutation = 'mutation { createCard(input: { pipe_id: "' + pipeId +
    '" phase_id: "' + phaseReceber.id +
    '" title: "' + tituloSafe + '"' +
    (fieldsAttr.length ? ' fields_attributes: [' + fieldsAttr.join(' ') + ']' : '') +
    ' }) { card { id title url } } }';

  const result = await pipefyQ(mutation);
  if (!result?.createCard?.card?.id) throw new Error('Pipefy sem card id: ' + JSON.stringify(result));
  return result.createCard.card;
}


export default async function handler(req, res) {
  // 🔐 Fase 2: segredo na URL do webhook (ativar exigência com env WEBHOOK_STRICT=1)
  if (req.method === 'POST') {
    const _vt = String((req.query && req.query.vt) || '');
    const _vtOk = _vt === ((process.env.WA_WEBHOOK_SECRET || 'wh-re2026-Kp8xQm2z').trim());
    if (String(process.env.WEBHOOK_STRICT || '') === '1' && !_vtOk) {
      return res.status(401).json({ ok: false, error: 'assinatura ausente' });
    }
  }

  // ── Validação assinatura Mercado Pago (HMAC-SHA256) ──────────────────────
  res.setHeader('Access-Control-Allow-Origin', 'https://reparoeletroadm.com');
  const MP_SECRET = process.env.MP_WEBHOOK_SECRET || '';
  if (req.method === 'POST' && MP_SECRET) {
    try {
      const crypto = require('crypto');
      const xSig   = req.headers['x-signature'] || '';
      const xReqId = req.headers['x-request-id'] || '';
      if (xSig) {
        const parts = xSig.split(',');
        const ts  = (parts.find(p=>p.startsWith('ts='))||'').replace('ts=','');
        const v1  = (parts.find(p=>p.startsWith('v1='))||'').replace('v1=','');
        const dataId = req.query.id || (req.body&&req.body.data&&req.body.data.id) || '';
        const manifest = `id:${dataId};request-id:${xReqId};ts:${ts};`;
        const expected = crypto.createHmac('sha256', MP_SECRET).update(manifest).digest('hex');
        const v1buf = Buffer.from(v1.padEnd(expected.length,'0'), 'hex');
        const expbuf= Buffer.from(expected, 'hex');
        if (v1 && v1buf.length===expbuf.length && !crypto.timingSafeEqual(v1buf, expbuf)) {
          console.warn('[webhook-mp] Assinatura HMAC inválida');
          return res.status(401).json({ ok: false, error: 'Assinatura inválida' });
        }
      }
    } catch(e) { console.warn('[webhook-mp] Erro HMAC:', e.message); }
  }

  res.setHeader('Content-Type', 'application/json');

  // ── GET: diagnóstico de logs ─────────────────────────────────
  if (req.method === 'GET') {
    const action = req.query.action;
  
  // GET search-payments
  if (action === 'search-payments') {
    const begin = req.query.begin || '2026-05-23T00:00:00.000-03:00';
    const end   = req.query.end   || '2026-05-24T00:00:00.000-03:00';
    try {
      const url = `https://api.mercadopago.com/v1/payments/search?sort=date_created&criteria=desc&range=date_created&begin_date=${encodeURIComponent(begin)}&end_date=${encodeURIComponent(end)}&limit=20`;
      const data = await (await fetch(url, { headers:{ Authorization:`Bearer ${MP_TOKEN}` } })).json();
      return res.status(200).json({ ok:true, total:data.paging?.total, pagamentos:(data.results||[]).map(p=>({ id:p.id, status:p.status, valor:p.transaction_amount, metodo:p.payment_method_id, data:p.date_approved||p.date_created, comprador:p.metadata?.comprador_nome||p.payer?.first_name||'?', produto_ids:p.metadata?.produto_ids||'' })) });
    } catch(e) { return res.status(500).json({ ok:false, error:e.message }); }
  }

  // GET check-payment
  if (action === 'check-payment') {
    const payId = req.query.paymentId;
    if (!payId) return res.status(400).json({ ok:false, error:'paymentId obrigatorio' });
    try { return res.status(200).json({ ok:true, payment: await (await fetch(`https://api.mercadopago.com/v1/payments/${payId}`,{headers:{Authorization:`Bearer ${MP_TOKEN}`}})).json() }); }
    catch(e) { return res.status(500).json({ ok:false, error:e.message }); }
  }

  // GET register-manual: registra venda perdida
  if (action === 'register-manual') {
    const payId = req.query.paymentId;
    if (!payId) return res.status(400).json({ ok:false, error:'paymentId obrigatorio' });
    try {
      const payment = await (await fetch(`https://api.mercadopago.com/v1/payments/${payId}`,{headers:{Authorization:`Bearer ${MP_TOKEN}`}})).json();
      if (payment.status !== 'approved') return res.status(200).json({ ok:false, error:'nao aprovado', status:payment.status });
      const meta=payment.metadata||{}, produtoIds=(meta.produto_ids||'').split(',').filter(Boolean);
      const nomeCliente=meta.comprador_nome||payment.payer?.first_name||'Comprador Online', telefone=meta.comprador_tel||'', cpf=meta.comprador_cpf||'', cep=meta.comprador_cep||'', endereco=meta.comprador_end||'';
      const modPag=payment.payment_method_id==='pix'?'PIX':`Cartao ${payment.installments}x`;
      const ADM_KEY='tv_vendas', TV_PROD_KEY='tv_vendas';
      const ADM_CK='tv_checkout_vendas', TV_CK='tv_checkout_vendas';
      for (const produtoId of produtoIds) {
        // Detectar se é ADM ou TV
        let db, dbKey, ckKey, idx;
        const admDb = (await dbGet(ADM_KEY))||{produtos:[]};
        idx = admDb.produtos.findIndex(p=>p.id===String(produtoId));
        if (idx >= 0) { db=admDb; dbKey=ADM_KEY; ckKey=ADM_CK; }
        else {
          const tvDb2 = (await dbGet(TV_PROD_KEY))||{produtos:[]};
          idx = tvDb2.produtos.findIndex(p=>p.id===String(produtoId));
          if (idx < 0) continue;
          db=tvDb2; dbKey=TV_PROD_KEY; ckKey=TV_CK;
        }
        const p=db.produtos[idx], now=new Date().toISOString();
        db.produtos[idx]={...p,vendido:true,soldAt:p.soldAt||now,vendidoEm:p.vendidoEm||now,nomeCliente,telefone:telefone||null,cpfCnpj:cpf||null,vendedor:'Mercado Pago',modalidade:modPag,paymentId:String(payId)};
        await dbSet(dbKey,db);
        const ckDb=(await dbGet(ckKey))||{vendas:[]};
        ckDb.vendas=ckDb.vendas||[];
        if(!ckDb.vendas.find(v=>v.paymentId===String(payId))) {
          ckDb.vendas.unshift({id:Date.now().toString(36),produto:{id:p.id,codigo:p.codigo,descricao:p.descricao,tipo:p.tipo||'',capacidade:p.capacidade||''},comprador:{nome:nomeCliente,telefone,cpf,endereco,cep},valor:payment.transaction_amount,provedor:'mercado_pago',paymentId:String(payId),paymentMethod:payment.payment_method_id,installments:payment.installments,criadoEm:now});
          ckDb.vendas=ckDb.vendas.slice(0,500); await dbSet(ckKey,ckDb);
        }
        await marcarProcessado(String(payId));
        await logEvento({tipo:'register-manual',paymentId:String(payId),produtoId,nomeCliente,valor:payment.transaction_amount});
      }
      return res.status(200).json({ok:true,nomeCliente,valor:payment.transaction_amount,produtoIds,modPag});
    } catch(e) { return res.status(500).json({ok:false,error:e.message}); }
  }





  // ── GET criar-card-pipefy: cria card no Pipefy para venda já registrada ──
  if (action === 'criar-card-pipefy') {
    const pid = req.query.paymentId;
    if (!pid) return res.status(400).json({ ok:false, error:'paymentId obrigatorio' });
    try {
      // Buscar venda no checkout ADM
      const CKKEY = 'tv_checkout_vendas';
      const ck    = (await dbGet(CKKEY)) || { vendas:[] };
      const venda = (ck.vendas||[]).find(v => String(v.paymentId) === String(pid));
      if (!venda) return res.status(404).json({ ok:false, error:'venda nao encontrada no checkout' });

      // Determinar pipe: produto TV → 306904889, ADM → 305832912
      const pipeId = (venda.produto?.tipo||'').toLowerCase().includes('tv') ? '306904889' : '305832912';

      const cardId = await criarCardPipefyVenda(
        pipeId,
        venda.produto,
        venda.comprador,
        venda.valor,
        pid
      );
      // Salvar resultado no Redis para diagnóstico
      await dbSet('pipefy_card_log', {
        paymentId: pid, cardId, pipeId,
        comprador: venda.comprador?.nome,
        produto:   venda.produto?.descricao,
        valor:     venda.valor,
        ts:        new Date().toISOString()
      });
      return res.status(200).json({
        ok: true, cardId,
        pipe: pipeId,
        comprador: venda.comprador?.nome,
        produto:   venda.produto?.descricao,
        valor:     venda.valor
      });
    } catch(e) {
      return res.status(500).json({ ok:false, error: e.message });
    }
  }

  // ── GET status-checkout: retorna resumo das vendas no checkout ────
  if (action === 'status-checkout') {
    try {
      const ck = (await dbGet('tv_checkout_vendas')) || { vendas:[] };
      const vendas = (ck.vendas || []).slice(0,10);
      return res.status(200).json({
        ok: true,
        total: (ck.vendas||[]).length,
        ultimas10: vendas.map(v => ({
          paymentId:  v.paymentId,
          comprador:  v.comprador?.nome,
          produto:    v.produto?.descricao,
          valor:      v.valor,
          metodo:     v.paymentMethod,
          data:       v.criadoEm?.slice(0,16)?.replace('T',' '),
          recuperado: v.recuperado || false,
        }))
      });
    } catch(e) {
      return res.status(500).json({ ok:false, error:e.message });
    }
  }


  // ── GET sync-vendas-mp: busca pagamentos aprovados na API do MP e registra os que faltam ──
  // Chamado pelo cron a cada 10 min — garante que TODA venda aprovada entre no checkout
  if (action === 'sync-vendas-mp') {
    try {
      const agora     = new Date();
      const inicio    = new Date(agora.getTime() - 48 * 60 * 60 * 1000); // últimas 48h (garante pegar ontem)
      const isoInicio = inicio.toISOString().replace('Z', '-00:00');
      const isoFim    = agora.toISOString().replace('Z', '-00:00');

      // Buscar pagamentos aprovados recentes no MP
      const mpUrl = `https://api.mercadopago.com/v1/payments/search?status=approved` +
        `&sort=date_created&criteria=desc&range=date_created` +
        `&begin_date=${encodeURIComponent(isoInicio)}&end_date=${encodeURIComponent(isoFim)}` +
        `&limit=20`;

      const mpRes = await fetch(mpUrl, {
        headers: { Authorization: `Bearer ${MP_TOKEN}` }
      });
      const mpData = await mpRes.json();
      const pagamentos = mpData.results || [];

      if (!pagamentos.length) {
        return res.status(200).json({ ok: true, info: 'nenhum pagamento aprovado nos últimos 30min', sincronizados: 0 });
      }

      // Ler checkout atual
      const CKKEY = 'tv_checkout_vendas';
      const ck    = (await dbGet(CKKEY)) || { vendas: [] };
      ck.vendas   = ck.vendas || [];
      const jaNoCheckout = new Set(ck.vendas.map(v => String(v.paymentId)));

      // Filtrar apenas os que ainda não foram registrados
      const pendentes = pagamentos.filter(p => !jaNoCheckout.has(String(p.id)));

      if (!pendentes.length) {
        return res.status(200).json({ ok: true, info: 'todos já registrados', total: pagamentos.length, pendentes: 0 });
      }

      const resultado = [];
      const admDb  = (await dbGet('tv_vendas')) || { produtos: [] };
      let   mudouAdm = false;

      for (const pmt of pendentes) {
        // Já processado por idempotência?
        if (await jaProcessado(String(pmt.id))) continue;
        await marcarProcessado(String(pmt.id));

        const meta         = pmt.metadata || {};
        const ids          = (meta.produto_ids || '').split(',').filter(Boolean);
        let   prodDescricao = meta.produto_nome || 'Produto';
        let   marcou        = false;

        // Marcar produto como vendido
        for (const pid of ids) {
          const idx = admDb.produtos.findIndex(p => String(p.id) === pid || p.codigo === pid);
          if (idx >= 0) {
            prodDescricao = admDb.produtos[idx].descricao || admDb.produtos[idx].nome || prodDescricao;
            if (!admDb.produtos[idx].vendido) {
              admDb.produtos[idx] = {
                ...admDb.produtos[idx],
                vendido:     true,
                soldAt:      new Date().toISOString(),
                nomeCliente: meta.comprador_nome || pmt.payer?.name || '',
                paymentId:   String(pmt.id)
              };
              mudouAdm = true;
              marcou   = true;
            }
          }
        }

        ck.vendas.unshift({
          id:            Date.now().toString(36) + Math.random().toString(36).slice(2, 5),
          produto:       { id: ids[0] || '', codigo: ids[0] || '', descricao: prodDescricao, tipo: '' },
          comprador:     {
            nome:    meta.comprador_nome || pmt.payer?.name || 'Comprador',
            telefone:meta.comprador_tel  || '',
            cpf:     meta.comprador_cpf  || '',
            endereco:meta.comprador_end  || ''
          },
          valor:         pmt.transaction_amount,
          provedor:      'mercado_pago',
          paymentId:     String(pmt.id),
          paymentMethod: pmt.payment_method_id,
          installments:  pmt.installments,
          criadoEm:      pmt.date_approved || new Date().toISOString(),
          syncAuto:      true
        });

        await logEvento({
          paymentId: String(pmt.id), status: 'approved', method: pmt.payment_method_id,
          amount: pmt.transaction_amount, tipo: 'sync-auto'
        });

        resultado.push({
          paymentId: String(pmt.id), valor: pmt.transaction_amount,
          comprador: meta.comprador_nome || pmt.payer?.name, produto: prodDescricao, marcouProduto: marcou
        });
      }

      if (resultado.length > 0) {
        ck.vendas = ck.vendas.slice(0, 500);
        await dbSet(CKKEY, ck);
        if (mudouAdm) await dbSet('tv_vendas', admDb);
      }

      return res.status(200).json({
        ok: true, sincronizados: resultado.length, total: pagamentos.length, resultado
      });
    } catch(e) {
      return res.status(500).json({ ok: false, error: e.message });
    }
  }

  // ── GET auto-recuperar: encontra e registra vendas aprovadas perdidas ──
  if (action === 'auto-recuperar') {
    try {
      const LOG_KEY  = 'mp_webhook_log';
      const PROC_KEY = 'tv_mp_processados';
      const CKKEY    = 'tv_checkout_vendas';

      const logsRaw = await dbGet(LOG_KEY);
      const logs = Array.isArray(logsRaw) ? logsRaw
        : (typeof logsRaw === 'string' ? JSON.parse(logsRaw) : []);

      const ck = (await dbGet(CKKEY)) || { vendas:[] };
      ck.vendas = ck.vendas || [];
      const jaNoCheckout = new Set(ck.vendas.map(v => String(v.paymentId)));

      // Filtrar logs: aprovados que não estão no checkout
      const aprovados = logs.filter(l => l.status === 'approved' && l.paymentId && !jaNoCheckout.has(String(l.paymentId)));

      const resultado = [];
      for (const log of aprovados) {
        try {
          // Buscar detalhes completos no MP
          const mpR = await fetch(`https://api.mercadopago.com/v1/payments/${log.paymentId}`, {
            headers: { Authorization: `Bearer ${MP_TOKEN}` }
          });
          const pmt = await mpR.json();
          if (pmt.status !== 'approved') continue;

          const meta2 = pmt.metadata || {};
          const ids2  = (meta2.produto_ids||'').split(',').filter(Boolean);

          // Marcar produto como vendido
          const admDb = (await dbGet('tv_vendas')) || { produtos:[] };
          let marcou = false, prodDescricao = meta2.produto_nome || 'Produto';
          for (const pid2 of ids2) {
            const i2 = admDb.produtos.findIndex(p => String(p.id)===pid2 || p.codigo===pid2);
            if (i2 >= 0) {
              prodDescricao = admDb.produtos[i2].descricao || admDb.produtos[i2].nome || prodDescricao;
              if (!admDb.produtos[i2].vendido) {
                admDb.produtos[i2] = {...admDb.produtos[i2], vendido:true, soldAt:new Date().toISOString(),
                  nomeCliente: meta2.comprador_nome||'', paymentId: String(log.paymentId)};
                marcou = true;
              }
            }
          }
          if (marcou) await dbSet('tv_vendas', admDb);

          // Registrar no checkout
          ck.vendas.unshift({
            id:           Date.now().toString(36) + Math.random().toString(36).slice(2,5),
            produto:      { id: ids2[0]||'', codigo: ids2[0]||'', descricao: prodDescricao, tipo:'' },
            comprador:    { nome: meta2.comprador_nome||pmt.payer?.name||'Comprador', telefone: meta2.comprador_tel||'', cpf: meta2.comprador_cpf||'', endereco: meta2.comprador_end||'' },
            valor:        pmt.transaction_amount,
            provedor:     'mercado_pago',
            paymentId:    String(log.paymentId),
            paymentMethod:pmt.payment_method_id,
            installments: pmt.installments,
            criadoEm:     pmt.date_approved || new Date().toISOString(),
            recuperado:   true,
          });

          // Criar card no Pipefy Receber
          try {
            const pipeId = checkoutKey === TV_CHECKOUT_KEY ? '306904889' : '305832912';
            await criarCardPipefyVenda(pipeId,
              { codigo: ids2[0]||'', descricao: prodDescricao },
              { nome: meta2.comprador_nome||pmt.payer?.name||'', telefone: meta2.comprador_tel||'', endereco: meta2.comprador_end||'' },
              pmt.transaction_amount, String(log.paymentId)
            );
          } catch(pe) { console.error('[webhook] Pipefy venda:', pe.message); }
          resultado.push({ paymentId: String(log.paymentId), valor: pmt.transaction_amount, comprador: meta2.comprador_nome, produto: prodDescricao, marcouProduto: marcou });
        } catch(e2) {
          resultado.push({ paymentId: String(log.paymentId), erro: e2.message });
        }
      }

      if (resultado.length > 0) {
        ck.vendas = ck.vendas.slice(0,500);
        await dbSet(CKKEY, ck);
      }

      return res.status(200).json({ ok:true, encontrados: aprovados.length, recuperados: resultado.filter(r=>!r.erro).length, resultado });
    } catch(e) {
      return res.status(500).json({ ok:false, error: e.message });
    }
  }




  // ── GET forcar-pagamento-fin: força processamento de pagamento financeiro ──
  if (action === "forcar-pagamento-fin") {
    const pid = req.query.paymentId;
    if (!pid) return res.status(400).json({ ok:false, error:"paymentId obrigatorio" });
    try {
      const mpR = await fetch(`https://api.mercadopago.com/v1/payments/${pid}`, {
        headers: { Authorization: `Bearer ${MP_TOKEN}` }
      });
      const pmt = await mpR.json();
      if (!pmt.id) return res.status(404).json({ ok:false, error:"pagamento nao encontrado", raw:pmt });
      const result = await processarPagamentoFinanceiro(pmt);
      // Garantir que está marcado como processado
      await marcarProcessado(pid);
      return res.status(200).json({ ok:true, forcado:true, paymentId:pid, valor:pmt.transaction_amount, status:pmt.status, ...result });
    } catch(e) {
      return res.status(500).json({ ok:false, error:e.message });
    }
  }


  // ── GET reconciliar-mp: busca pagamentos MP dos últimos 7 dias não processados ──
  if (action === 'reconciliar-mp') {
    const resultado = { encontrados: [], jaProcessados: [], novosConfirmados: [], erros: [] };
    try {
      const agora2   = new Date();
      const inicio7d = new Date(agora2.getTime() - 7*24*60*60*1000).toISOString().replace('Z','-00:00');
      const fim7d    = agora2.toISOString().replace('Z','-00:00');
      // Buscar approved E in_process (cartões em análise passam por in_process antes de approved)
      const url7d    = `https://api.mercadopago.com/v1/payments/search?sort=date_created&criteria=desc&range=date_created&begin_date=${encodeURIComponent(inicio7d)}&end_date=${encodeURIComponent(fim7d)}&limit=100`;
      const mpR = await fetch(url7d, { headers:{ Authorization:`Bearer ${MP_TOKEN}` } });
      const mpJ = await mpR.json();
      const pagamentos = mpJ.results || [];
      resultado.encontrados = pagamentos.length;

      const finDb2 = await dbGet(FIN_KEY2);
      const fichas = finDb2?.records || [];

      for (const pmt of pagamentos) {
        const jaProc = await jaProcessado(String(pmt.id));
        if (jaProc) { resultado.jaProcessados.push(pmt.id); continue; }

        // Tentar encontrar ficha correspondente
        const extRef = pmt.external_reference ? String(pmt.external_reference) : null;
        const ficha2 = fichas.find(f =>
          (pmt._fichaHint && pmt._fichaHint === f.id) ||
          (f.mp?.preferenceId && f.mp.preferenceId === pmt.preference_id) ||
          (pmt.metadata?.ficha_id && pmt.metadata.ficha_id === f.id) ||
          (pmt.metadata?.fichaId  && pmt.metadata.fichaId  === f.id) ||
          (extRef && extRef === f.id) ||
          (extRef && f.pipefyId && extRef === String(f.pipefyId)) ||
          (extRef && f.osCode   && extRef === String(f.osCode))
        );

        if (!ficha2) {
          resultado.semCorrespondencia = resultado.semCorrespondencia || [];
          resultado.semCorrespondencia.push({
            paymentId: pmt.id,
            valor: pmt.transaction_amount,
            data: pmt.date_approved,
            preference_id: pmt.preference_id,
            external_reference: pmt.external_reference,
            metadata: pmt.metadata,
            pagador: pmt.payer?.email || pmt.payer?.identification?.number || '?'
          });
          continue;
        }

        // Processar
        const proc = await processarPagamentoFinanceiro(pmt, ficha2.id);
        if (proc.ok) {
          resultado.novosConfirmados.push({
            fichaId: ficha2.id, nome: ficha2.nomeContato,
            paymentId: pmt.id, valor: pmt.transaction_amount,
            data: pmt.date_approved
          });
        }
      }

      return res.status(200).json({ ok: true, ...resultado });
    } catch(e) { return res.status(500).json({ ok: false, error: e.message }); }
  }


  // ── GET sync-fin-pendentes: verifica pagamentos de fichas pendentes (cron 15min) ──
  if (action === 'sync-fin-pendentes') {
    try {
      const fin4 = await dbGet(FIN_KEY2);
      // Verificar todas as fases que podem ter link MP gerado
      const FASES_COM_LINK = ['faturamento','pagamento_agendado','analise_pagamento',
                              'nf_emitida','aguardando_dados','pagamento_confirmado'];
      const pendentes4 = (fin4?.records||[]).filter(r =>
        r.mp?.preferenceId &&
        r.phaseId !== 'entrega_liberada' &&
        r.phaseId !== 'rota_criada' &&
        r.phaseId !== 'item_coletado'
      );
      if (!pendentes4.length) return res.status(200).json({ ok:true, info:'nenhuma ficha pendente', verificados:0 });

      const processados4 = [];
      for (const ficha of pendentes4) {
        try {
          // Buscar diretamente pelo preference_id (mais eficiente)
          // Busca por preference_id
          const urlPref4 = `https://api.mercadopago.com/v1/payments/search?preference_id=${encodeURIComponent(ficha.mp.preferenceId)}&limit=10`;
          const rPref4   = await fetch(urlPref4, { headers:{ Authorization:`Bearer ${MP_TOKEN}` } });
          const dPref4   = await rPref4.json();
          let pgtos4 = (dPref4.results||[]).filter(p => p.status==='approved'||p.status==='in_process');

          // Busca alternativa por external_reference se não encontrou pelo preference_id
          if (!pgtos4.length && ficha.id) {
            const urlExt4 = `https://api.mercadopago.com/v1/payments/search?external_reference=${encodeURIComponent(ficha.id)}&limit=5`;
            const rExt4   = await fetch(urlExt4, { headers:{ Authorization:`Bearer ${MP_TOKEN}` } });
            const dExt4   = await rExt4.json();
            pgtos4 = (dExt4.results||[]).filter(p => p.status==='approved'||p.status==='in_process');
          }
          if (!pgtos4.length) continue;

          const pmt4 = pgtos4.sort((a,b)=>new Date(b.date_approved||0)-new Date(a.date_approved||0))[0];
          if (await jaProcessado(String(pmt4.id))) {
            // Já processado mas fase pode estar errada — corrigir
            const finFix = await dbGet(FIN_KEY2);
            const recFix = (finFix?.records||[]).find(r => r.id===ficha.id);
            if (recFix && recFix.phaseId !== 'entrega_liberada') {
              recFix.phaseId = 'entrega_liberada';
              recFix.paidAt  = recFix.paidAt || new Date().toISOString();
              await dbSet(FIN_KEY2, finFix);
              processados4.push({ fichaId:ficha.id, paymentId:pmt4.id, info:'fase_corrigida' });
            }
            continue;
          }
          const result4 = await processarPagamentoFinanceiro(pmt4, ficha.id);
          if (result4.ok) processados4.push({ fichaId:ficha.id, paymentId:pmt4.id, valor:pmt4.transaction_amount });
        } catch(e4){ console.error('[sync-fin-pendentes]', ficha.id, e4.message); }
      }

      return res.status(200).json({ ok:true, verificados:pendentes4.length, processados:processados4.length, fichas:processados4 });
    } catch(e){ return res.status(500).json({ ok:false, error:e.message }); }
  }

  // ── GET processar-pendentes-fin: busca pagamentos de TODAS fichas faturamento pendentes ──
  // Sem limite de janela de tempo — verifica cada preference_id diretamente no MP
  if (action === "processar-pendentes-fin") {
    const resultado = { verificados: 0, pagos: 0, pendentes: 0, erros: [], processados: [] };
    try {
      const finDb = await dbGet(FIN_KEY2);
      const fichasPendentes = (finDb?.records||[]).filter(r =>
        r.mp?.preferenceId &&
        ["faturamento","pagamento_agendado","pagamento_confirmado"].includes(r.phaseId)
      );
      resultado.verificados = fichasPendentes.length;

      for (const ficha of fichasPendentes) {
        try {
          // Buscar por external_reference (ficha ID) que é indexável no MP
          // Também buscar por ficha_id no metadata (MP converte camelCase→snake_case)
          const fichaIdMeta = ficha.id.replace(/['"]/g,'');
          const searchUrl = `https://api.mercadopago.com/v1/payments/search?status=approved&external_reference=${encodeURIComponent(fichaIdMeta)}&limit=10`;
          const r1 = await fetch(searchUrl, { headers:{ Authorization:`Bearer ${MP_TOKEN}` } });
          const d1 = await r1.json();

          // Busca ampla por data de criação (últimos 30 dias) + filtrar por preference_id nos resultados
          const inicio30 = new Date(Date.now()-30*24*60*60*1000).toISOString().replace('Z','-00:00');
          const fim30    = new Date().toISOString().replace('Z','-00:00');
          const searchUrl2 = `https://api.mercadopago.com/v1/payments/search?status=approved&sort=date_created&criteria=desc&range=date_created&begin_date=${encodeURIComponent(inicio30)}&end_date=${encodeURIComponent(fim30)}&limit=50`;
          const r2 = await fetch(searchUrl2, { headers:{ Authorization:`Bearer ${MP_TOKEN}` } });
          const d2 = await r2.json();

          // Filtrar resultados por preference_id OU por ficha_id no metadata
          const prefId = ficha.mp.preferenceId;
          const todos  = [...(d1.results||[]), ...(d2.results||[])];
          const uniq   = todos.filter((p,i,a) =>
            a.findIndex(x=>x.id===p.id)===i &&
            (p.preference_id === prefId ||
             p.metadata?.ficha_id  === ficha.id ||
             p.metadata?.fichaId   === ficha.id)
          );

          if (!uniq.length) { resultado.pendentes++; continue; }

          // Processar o pagamento aprovado mais recente
          const pmt = uniq.sort((a,b) => new Date(b.date_approved||0)-new Date(a.date_approved||0))[0];
          if (await jaProcessado(String(pmt.id))) {
            // Já processado — verificar se fase está correta e corrigir se necessário
            const finCheck = await dbGet(FIN_KEY2);
            const recCheck = (finCheck?.records||[]).find(r => r.id===ficha.id || r.mp?.paymentId===String(pmt.id));
            if (recCheck && recCheck.phaseId !== 'entrega_liberada') {
              // Fase errada — corrigir
              const nowFix = new Date().toISOString();
              recCheck.phaseId = 'entrega_liberada';
              recCheck.paidAt  = recCheck.paidAt || nowFix;
              recCheck.movedAt = nowFix;
              recCheck.history = [...(recCheck.history||[]), { phaseId:'entrega_liberada', ts:nowFix, via:'fix_ja_processado' }];
              await dbSet(FIN_KEY2, finCheck);
              resultado.processados.push({ fichaId:ficha.id, paymentId:pmt.id, info:'fase_corrigida', de:recCheck.phaseId, para:'entrega_liberada' });
            } else {
              resultado.processados.push({ fichaId:ficha.id, paymentId:pmt.id, info:'ja_processado' });
            }
            continue;
          }
          await marcarProcessado(String(pmt.id));
          const res = await processarPagamentoFinanceiro(pmt);
          resultado.pagos++;
          resultado.processados.push({ fichaId:ficha.id, paymentId:pmt.id, valor:pmt.transaction_amount, ...res });
        } catch(e) {
          resultado.erros.push({ fichaId:ficha.id, erro:e.message });
        }
      }
    } catch(e) { resultado.erroGeral = e.message; }
    return res.status(200).json({ ok:true, ...resultado });
  }

  // ── GET sync-fin-mp: reprocessa fila de retry + sync pagamentos financeiro ─
  if (action === "sync-fin-mp") {
    const resultado = { retry:[], sync:[] };
    try {
      // 1. Processar fila de retry
      const fila = (await dbGet(FIN_RETRY_KEY)) || [];
      const pendentes = fila.filter(e => e.tentativas < 5);
      const novaFila  = [];
      for (const entry of pendentes) {
        try {
          const mpR = await fetch(`https://api.mercadopago.com/v1/payments/${entry.id}`, {
            headers: { Authorization: `Bearer ${MP_TOKEN}` }
          });
          const pmt = await mpR.json();
          if (pmt.status === "approved" && pmt.metadata?.origem === "financeiro") {
            const r = await processarPagamentoFinanceiro(pmt);
            if (r.ok) { resultado.retry.push({ id:entry.id, ok:true }); continue; }
          }
          entry.tentativas++;
          entry.ultimaTentativa = new Date().toISOString();
          novaFila.push(entry);
          resultado.retry.push({ id:entry.id, tentativa:entry.tentativas });
        } catch(e) {
          entry.tentativas++;
          novaFila.push(entry);
          resultado.retry.push({ id:entry.id, erro:e.message });
        }
      }
      await dbSet(FIN_RETRY_KEY, novaFila);

      // 2. Sync MP: últimos 30min de pagamentos fin não processados
      const agora   = new Date();
      const inicio  = new Date(agora.getTime() - 30*60*1000);
      const mpUrl   = `https://api.mercadopago.com/v1/payments/search?status=approved&sort=date_created&criteria=desc&range=date_created&begin_date=${encodeURIComponent(inicio.toISOString())}&end_date=${encodeURIComponent(agora.toISOString())}&limit=20`;
      const mpRes   = await fetch(mpUrl, { headers:{ Authorization:`Bearer ${MP_TOKEN}` } });
      const mpData   = await mpRes.json();
      const finDbSync = await dbGet(FIN_KEY2).catch(()=>null);
      for (const pmt of (mpData.results||[])) {
        // Verificar se é pagamento financeiro por preference_id OU metadata.origem
        const isFinanceiro = pmt.metadata?.origem === "financeiro" ||
          ((finDbSync?.records||[]).some(r => r.mp?.preferenceId === pmt.preference_id));
        if (!isFinanceiro) continue;
        if (await jaProcessado(String(pmt.id))) continue;
        await marcarProcessado(String(pmt.id));
        const r = await processarPagamentoFinanceiro(pmt);
        resultado.sync.push({ id:pmt.id, ...r });
      }
    } catch(e) { resultado.erro = e.message; }
    return res.status(200).json({ ok:true, ...resultado });
  }

  // ── GET recuperar-venda: re-processa um paymentId manualmente ──────
  if (action === 'recuperar-venda') {
    const pid = req.query.paymentId || req.query.pid;
    if (!pid) return res.status(400).json({ ok:false, error:'paymentId obrigatorio' });
    try {
      const mpR = await fetch(`https://api.mercadopago.com/v1/payments/${pid}`, {
        headers: { Authorization: `Bearer ${MP_TOKEN}` }
      });
      const pmt = await mpR.json();
      if (!pmt.id) return res.status(404).json({ ok:false, error:'nao encontrado no MP', raw:pmt });

      const meta2 = pmt.metadata || {};
      const CKKEY = 'tv_checkout_vendas';
      const ck    = (await dbGet(CKKEY)) || { vendas:[] };
      ck.vendas   = ck.vendas || [];

      if (ck.vendas.find(v => v.paymentId === String(pid))) {
        return res.status(200).json({ ok:true, info:'ja registrado', paymentId:pid });
      }

      ck.vendas.unshift({
        id:           Date.now().toString(36),
        produto:      { id: meta2.produto_ids||'', codigo: meta2.produto_ids||'', descricao: meta2.produto_nome||'Produto recuperado', tipo:'' },
        comprador:    { nome: meta2.comprador_nome||pmt.payer?.name||'Comprador', telefone: meta2.comprador_tel||'', cpf: meta2.comprador_cpf||'', endereco: meta2.comprador_end||'' },
        valor:        pmt.transaction_amount,
        provedor:     'mercado_pago',
        paymentId:    String(pid),
        paymentMethod:pmt.payment_method_id,
        installments: pmt.installments,
        criadoEm:     new Date().toISOString(),
        recuperado:   true,
      });
      ck.vendas = ck.vendas.slice(0,500);
      await dbSet(CKKEY, ck);

      // Marcar produto como vendido
      const admDb = (await dbGet('tv_vendas')) || { produtos:[] };
      const ids2  = (meta2.produto_ids||'').split(',').filter(Boolean);
      let marcou  = false;
      for (const pid2 of ids2) {
        const i2 = admDb.produtos.findIndex(p => String(p.id)===pid2 || p.codigo===pid2);
        if (i2>=0 && !admDb.produtos[i2].vendido) {
          admDb.produtos[i2] = {...admDb.produtos[i2], vendido:true, soldAt:new Date().toISOString(), nomeCliente:meta2.comprador_nome||'', paymentId:String(pid)};
          marcou = true;
        }
      }
      if (marcou) await dbSet('tv_vendas', admDb);

      return res.status(200).json({ ok:true, registrado:true, paymentId:pid, valor:pmt.transaction_amount, status:pmt.status, comprador:meta2.comprador_nome, marcouProduto:marcou });
    } catch(e) {
      return res.status(500).json({ ok:false, error:e.message });
    }
  }

  if (action === 'logs') {
      try {
        const logsRaw = await dbGet(LOG_KEY);
        const procRaw = await dbGet(PROC_KEY);
        // dbGet pode retornar string ou array dependendo da serialização
        const logs = Array.isArray(logsRaw) ? logsRaw : (typeof logsRaw === 'string' ? JSON.parse(logsRaw) : []);
        const proc = Array.isArray(procRaw) ? procRaw : (typeof procRaw === 'string' ? JSON.parse(procRaw) : []);
        const out = {
          ok: true,
          totalLogs: logs.length,
          ultimosLogs: logs.slice(0, 10),
          processados: proc.slice(0, 20)
        };
        res.setHeader('Content-Type', 'application/json');
        return res.status(200).end(JSON.stringify(out));
      } catch(e) {
        return res.status(200).json({ ok: false, error: e.message });
      }
    }
    return res.status(200).json({ ok: true, info: 'webhook-mp ativo. Use ?action=logs para ver logs.' });
  }

  const body  = req.body || {};
  const tipo  = body.type || req.query.type || '';
  const payId = body.data?.id || req.query['data.id'] || '';

  if (tipo !== 'payment' || !payId) {
    return res.status(200).json({ ok: true, ignored: tipo });
  }

  if (!MP_TOKEN) {
    return res.status(200).json({ ok: true, error: 'MP_ACCESS_TOKEN ausente' });
  }

  // ── IDEMPOTÊNCIA: ignorar pagamentos já processados ──────────────
  if (await jaProcessado(payId)) {
    console.log('webhook-mp: paymentId já processado, ignorando:', payId);
    return res.status(200).json({ ok: true, duplicata: true, paymentId: payId });
  }

  try {
    // 1. Buscar detalhes do pagamento no MP
    const mpRes = await fetch(`https://api.mercadopago.com/v1/payments/${payId}`, {
      headers: { Authorization: `Bearer ${MP_TOKEN}` }
    });
    const payment = await mpRes.json();

    await logEvento({
      paymentId:  String(payId),
      status:     payment.status,
      method:     payment.payment_method_id,
      amount:     payment.transaction_amount,
      metadata:   payment.metadata
    });

    if (payment.status !== 'approved') {
      return res.status(200).json({ ok: true, status: payment.status });
    }

    // 2. Marcar como processado ANTES de executar (evita duplicata por timeout)
    await marcarProcessado(payId);

    // 3. Extrair metadados
    const meta         = payment.metadata || {};
    const produtoIds   = (meta.produto_ids || '').split(',').filter(Boolean);
    const nomeCliente  = meta.comprador_nome || payment.payer?.name || 'Comprador Online';
    const telefone     = meta.comprador_tel  || '';
    const cpf          = meta.comprador_cpf  || '';
    const endereco     = meta.comprador_end  || '';
    const cep          = meta.comprador_cep  || '';

    const modPagamento = payment.payment_method_id === 'pix'
      ? 'PIX' : `Cartao ${payment.installments}x`;

    const proto   = req.headers['x-forwarded-proto'] || 'https';
    const host    = req.headers['x-forwarded-host']  || req.headers.host || 'reparoeletroadm.com';
    const siteUrl = `${proto}://${host}`;

    // 3b. Verificar se é pagamento de OS financeiro via preference_id
    // MP NÃO copia metadata da preferência para o pagamento — checar pelo preference_id
    try {
      const prefId = payment.preference_id;
      if (prefId) {
        const finDb = await dbGet(FIN_KEY2);
        const fichaFin = (finDb?.records||[]).find(r => r.mp?.preferenceId === prefId);
        if (fichaFin) {
          console.log('[webhook] Pagamento financeiro via preference_id:', prefId);
          const finResult = await processarPagamentoFinanceiro(payment);
          await logEvento({ tipo:'fin_pago', paymentId:String(payId), fichaId:fichaFin.id, valor:payment.transaction_amount });
          return res.status(200).json({ ok:true, financeiro:true, ...finResult });
        }
      }
    } catch(finErr) { console.error('[webhook] fin-check:', finErr.message); }

    // 4. Para cada produto: marcar vendido direto no Redis
    //    Verifica reparoeletro_vendas (Micro/Bebe) OU tv_vendas (TV)
    //    e salva no relatório de checkout correto para cada tipo
    const ADM_VENDAS_KEY     = 'tv_vendas';
    const TV_VENDAS_KEY      = 'tv_vendas';
    const ADM_CHECKOUT_KEY   = 'tv_checkout_vendas';
    const TV_CHECKOUT_KEY    = 'tv_checkout_vendas';

    for (const produtoId of produtoIds) {
      let produtoInfo  = { id: produtoId, codigo: meta.produto_codigos || '' };
      let checkoutKey  = ADM_CHECKOUT_KEY; // default: ADM (Micro/Bebe)

      try {
        // Tentar reparoeletro_vendas (Microondas / Bebedouro)
        const admDb  = (await dbGet(ADM_VENDAS_KEY)) || { produtos: [] };
        const admIdx = admDb.produtos.findIndex(p => p.id === String(produtoId));

        if (admIdx >= 0) {
          // ── Produto ADM (Microondas/Bebedouro) ──
          const p = admDb.produtos[admIdx];
          if (p.vendido) { console.log('[Webhook] Já vendido (ADM):', produtoId); }
          else {
            const now = new Date().toISOString();
            admDb.produtos[admIdx] = { ...p, vendido:true, soldAt:now, nomeCliente,
              telefone:telefone||null, cpfCnpj:cpf||null, vendedor:'Mercado Pago',
              modalidade:modPagamento, paymentId:String(payId), vendidoEm:now };
            await dbSet(ADM_VENDAS_KEY, admDb);
          }
          produtoInfo = { id:produtoId, codigo:p.codigo, descricao:p.descricao,
            tipo:p.tipo||'', capacidade:p.capacidade||'' };
          checkoutKey = ADM_CHECKOUT_KEY;

        } else {
          // ── Tentar tv_vendas (Televisão) ──
          const tvVendas  = (await dbGet(TV_VENDAS_KEY)) || { produtos: [] };
          const tvProdIdx = tvVendas.produtos.findIndex(p => p.id === String(produtoId));

          if (tvProdIdx >= 0) {
            const p = tvVendas.produtos[tvProdIdx];
            if (p.vendido) { console.log('[Webhook] Já vendido (TV):', produtoId); }
            else {
              const now = new Date().toISOString();
              tvVendas.produtos[tvProdIdx] = { ...p, vendido:true, soldAt:now, nomeCliente,
                telefone:telefone||null, cpfCnpj:cpf||null, vendedor:'Mercado Pago',
                modalidade:modPagamento, paymentId:String(payId), vendidoEm:now };
              await dbSet(TV_VENDAS_KEY, tvVendas);
            }
            produtoInfo = { id:produtoId, codigo:p.codigo, descricao:p.descricao,
              tipo:p.tipo||'', capacidade:p.capacidade||'' };
            checkoutKey = TV_CHECKOUT_KEY;
          } else {
            console.error('[Webhook] Produto não encontrado em nenhum catálogo:', produtoId);
            await logEvento({ tipo:'erro', produtoId, erro:'nao_encontrado', paymentId:String(payId) });
            // Registrar mesmo assim — nenhuma venda aprovada pode ser descartada
            produtoInfo  = { id: produtoId, codigo: produtoId, descricao: meta.produto_nome || 'Produto', tipo: '' };
            checkoutKey  = ADM_CHECKOUT_KEY;
          }
        }
      } catch(e) { console.error('vender:', e.message); }

      // 4a. Se origem=financeiro → processar como pagamento de OS
      if (meta.origem === 'financeiro') {
        const finResult = await processarPagamentoFinanceiro(payment);
        console.log('[webhook] financeiro:', JSON.stringify(finResult));
        return res.status(200).json({ ok:true, financeiro:true, ...finResult });
      }

      // 4b. Criar card no Pipefy Receber (após marcar produto e antes de salvar checkout)
      try {
        const pipeIdVenda = checkoutKey === TV_CHECKOUT_KEY ? '306904889' : '305832912';
        await criarCardPipefyVenda(pipeIdVenda,
          produtoInfo,
          { nome: nomeCliente, telefone, endereco },
          payment.transaction_amount, String(payId)
        );
      } catch(pe) { console.error('[webhook] Pipefy venda tempo-real:', pe.message); }

      // 5. Registrar no relatório de checkout correto (ADM ou TV)
      try {
        const ckDb = (await dbGet(checkoutKey)) || { vendas:[] };
        ckDb.vendas = ckDb.vendas || [];
        ckDb.vendas.unshift({ id:Date.now().toString(36), produto:produtoInfo,
          comprador:{nome:nomeCliente,telefone,cpf,endereco,cep},
          valor:payment.transaction_amount, provedor:'mercado_pago',
          paymentId:String(payId), paymentMethod:payment.payment_method_id,
          installments:payment.installments, criadoEm:new Date().toISOString() });
        ckDb.vendas = ckDb.vendas.slice(0,500);
        await dbSet(checkoutKey, ckDb);
        console.log('[Webhook] Checkout registrado em', checkoutKey, '| produto', produtoId);
      } catch(e) { console.error('registrar-checkout:', e.message); }

      // ── Google Ads: conversão server-side via Measurement Protocol ────
      try {
        await fetch('https://www.googletagmanager.com/gtag/js?id=AW-11030361270', {method:'GET'}).catch(()=>{});
        // Registrar via gtag collect (dispara mesmo se cliente fechou a janela)
        await fetch('https://www.google-analytics.com/g/collect?' + new URLSearchParams({
          v:'2', tid:'AW-11030361270',
          cid: String(payId),
          en: 'conversion',
          'epn.value':  String(payment.transaction_amount),
          'ep.currency':'BRL',
          'ep.transaction_id': String(payId),
          'ep.send_to': 'AW-11030361270/saNSCK6yyrAcELbp14sp'
        }), {method:'POST'}).catch(()=>{});
      } catch(e){ console.error('[GA] server-side:', e.message); }
    }

    return res.status(200).json({ ok: true, processados: produtoIds.length });

  } catch(e) {
    console.error('webhook-mp:', e.message);
    // Não marcar como processado se deu erro — MP pode tentar novamente
    return res.status(200).json({ ok: true });
  }
}
