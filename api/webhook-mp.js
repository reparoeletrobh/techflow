// api/webhook-mp.js
const UPSTASH_URL   = (process.env.UPSTASH_URL   || '').replace(/['"]/g, '').trim();
const UPSTASH_TOKEN = (process.env.UPSTASH_TOKEN || '').replace(/['"]/g, '').trim();
const MP_TOKEN      = (process.env.MP_ACCESS_TOKEN || '').replace(/['"]/g, '').trim();
const LOG_KEY       = 'mp_webhook_log';
const PROC_KEY      = 'mp_processados'; // IDs já processados (idempotência)

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

export default async function handler(req, res) {
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
      const VENDAS_KEY='reparoeletro_vendas', TV_KEY='tv_checkout_vendas';
      for (const produtoId of produtoIds) {
        const db=(await dbGet(VENDAS_KEY))||{produtos:[]}, tvDb=(await dbGet(TV_KEY))||{vendas:[]};
        const idx=db.produtos.findIndex(p=>p.id===String(produtoId)); if(idx<0) continue;
        const p=db.produtos[idx], now=new Date().toISOString();
        db.produtos[idx]={...p,vendido:true,soldAt:p.soldAt||now,vendidoEm:p.vendidoEm||now,nomeCliente,telefone:telefone||null,cpfCnpj:cpf||null,vendedor:'Mercado Pago',modalidade:modPag,paymentId:String(payId)};
        await dbSet(VENDAS_KEY,db);
        tvDb.vendas=tvDb.vendas||[];
        if(!tvDb.vendas.find(v=>v.paymentId===String(payId))) {
          tvDb.vendas.unshift({id:Date.now().toString(36),produto:{id:p.id,codigo:p.codigo,descricao:p.descricao,tipo:p.tipo||'',capacidade:p.capacidade||''},comprador:{nome:nomeCliente,telefone,cpf,endereco,cep},valor:payment.transaction_amount,provedor:'mercado_pago',paymentId:String(payId),paymentMethod:payment.payment_method_id,installments:payment.installments,criadoEm:now});
          tvDb.vendas=tvDb.vendas.slice(0,500); await dbSet(TV_KEY,tvDb);
        }
        await marcarProcessado(String(payId));
        await logEvento({tipo:'register-manual',paymentId:String(payId),produtoId,nomeCliente,valor:payment.transaction_amount});
      }
      return res.status(200).json({ok:true,nomeCliente,valor:payment.transaction_amount,produtoIds,modPag});
    } catch(e) { return res.status(500).json({ok:false,error:e.message}); }
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

    // 4. Para cada produto: marcar vendido direto no Redis (sem self-call)
    const VENDAS_KEY = 'reparoeletro_vendas';
    for (const produtoId of produtoIds) {
      let produtoInfo = { id: produtoId, codigo: meta.produto_codigos || '' };
      try {
        const db  = (await dbGet(VENDAS_KEY)) || { produtos: [] };
        const idx = db.produtos.findIndex(p => p.id === String(produtoId));
        if (idx < 0) { console.error('[Webhook] Produto não encontrado:', produtoId); await logEvento({ tipo:'erro', produtoId, erro:'nao_encontrado', paymentId:String(payId) }); continue; }
        const p = db.produtos[idx];
        if (p.vendido) { console.log('[Webhook] Já vendido:', produtoId); continue; }
        const now = new Date().toISOString();
        db.produtos[idx] = { ...p, vendido:true, soldAt:now, nomeCliente, telefone:telefone||null, cpfCnpj:cpf||null, vendedor:'Mercado Pago', modalidade:modPagamento, paymentId:String(payId), vendidoEm:now };
        await dbSet(VENDAS_KEY, db);
        produtoInfo = { id:produtoId, codigo:p.codigo, descricao:p.descricao, tipo:p.tipo||'', capacidade:p.capacidade||'' };
      } catch(e) { console.error('vender:', e.message); }

      // 5. Espelho no relatório de checkout
      try {
        const TV_KEY = 'tv_checkout_vendas';
        const tvDb = (await dbGet(TV_KEY)) || { vendas:[] };
        tvDb.vendas = tvDb.vendas || [];
        tvDb.vendas.unshift({ id:Date.now().toString(36), produto:produtoInfo, comprador:{nome:nomeCliente,telefone,cpf,endereco,cep}, valor:payment.transaction_amount, provedor:'mercado_pago', paymentId:String(payId), paymentMethod:payment.payment_method_id, installments:payment.installments, criadoEm:new Date().toISOString() });
        tvDb.vendas = tvDb.vendas.slice(0,500);
        await dbSet(TV_KEY, tvDb);
      } catch(e) { console.error('registrar-venda:', e.message); }

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
