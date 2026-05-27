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
      const ADM_KEY='reparoeletro_vendas', TV_PROD_KEY='tv_vendas';
      const ADM_CK='reparoeletro_checkout_vendas', TV_CK='tv_checkout_vendas';
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
      const CKKEY = 'reparoeletro_checkout_vendas';
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
      const admDb = (await dbGet('reparoeletro_vendas')) || { produtos:[] };
      const ids2  = (meta2.produto_ids||'').split(',').filter(Boolean);
      let marcou  = false;
      for (const pid2 of ids2) {
        const i2 = admDb.produtos.findIndex(p => String(p.id)===pid2 || p.codigo===pid2);
        if (i2>=0 && !admDb.produtos[i2].vendido) {
          admDb.produtos[i2] = {...admDb.produtos[i2], vendido:true, soldAt:new Date().toISOString(), nomeCliente:meta2.comprador_nome||'', paymentId:String(pid)};
          marcou = true;
        }
      }
      if (marcou) await dbSet('reparoeletro_vendas', admDb);

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

    // 4. Para cada produto: marcar vendido direto no Redis
    //    Verifica reparoeletro_vendas (Micro/Bebe) OU tv_vendas (TV)
    //    e salva no relatório de checkout correto para cada tipo
    const ADM_VENDAS_KEY     = 'reparoeletro_vendas';
    const TV_VENDAS_KEY      = 'tv_vendas';
    const ADM_CHECKOUT_KEY   = 'reparoeletro_checkout_vendas';
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
