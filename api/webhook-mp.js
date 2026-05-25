// api/webhook-mp.js
const UPSTASH_URL   = (process.env.UPSTASH_URL   || '').replace(/['"]/g, '').trim();
const UPSTASH_TOKEN = (process.env.UPSTASH_TOKEN || '').replace(/['"]/g, '').trim();
const MP_TOKEN      = (process.env.MP_ACCESS_TOKEN || '').replace(/['"]/g, '').trim();
const LOG_KEY       = 'mp_webhook_log';
const PROC_KEY      = 'mp_processados'; // IDs já processados (idempotência)

async function dbGet(key) {
  if (!UPSTASH_URL || !UPSTASH_TOKEN) return null;
  const r = await fetch(`${UPSTASH_URL}/get/${encodeURIComponent(key)}`, {
    headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` }
  });
  const j = await r.json();
  return j.result ? JSON.parse(j.result) : null;
}

async function dbSet(key, value) {
  if (!UPSTASH_URL || !UPSTASH_TOKEN) return;
  await fetch(`${UPSTASH_URL}/set/${encodeURIComponent(key)}`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${UPSTASH_TOKEN}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(JSON.stringify(value))
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
    // ── GET search-payments: busca pagamentos por data ──────────
  if (action === 'search-payments') {
    const begin = req.query.begin || '2026-05-23T00:00:00.000-03:00';
    const end   = req.query.end   || '2026-05-24T00:00:00.000-03:00';
    try {
      const url = `https://api.mercadopago.com/v1/payments/search?sort=date_created&criteria=desc&range=date_created&begin_date=${encodeURIComponent(begin)}&end_date=${encodeURIComponent(end)}&limit=20`;
      const mpRes = await fetch(url, { headers: { Authorization: `Bearer ${MP_TOKEN}` } });
      const data = await mpRes.json();
      const pagamentos = (data.results || []).map(p => ({
        id:       p.id,
        status:   p.status,
        valor:    p.transaction_amount,
        metodo:   p.payment_method_id,
        data:     p.date_approved || p.date_created,
        comprador: p.metadata?.comprador_nome || p.payer?.first_name || '?',
        produto_ids: p.metadata?.produto_ids || '',
      }));
      return res.status(200).json({ ok: true, total: data.paging?.total, pagamentos });
    } catch(e) {
      return res.status(500).json({ ok: false, error: e.message });
    }
  }

  // ── GET check-payment: busca detalhes de um pagamento no MP ──
  if (action === 'check-payment') {
    const payId = req.query.paymentId;
    if (!payId) return res.status(400).json({ ok: false, error: 'paymentId obrigatorio' });
    try {
      const mpRes = await fetch(`https://api.mercadopago.com/v1/payments/${payId}`, {
        headers: { Authorization: `Bearer ${MP_TOKEN}` }
      });
      const payment = await mpRes.json();
      return res.status(200).json({ ok: true, payment });
    } catch(e) {
      return res.status(500).json({ ok: false, error: e.message });
    }
  }

  // ── GET replay: reprocessar um pagamento aprovado ─────────────
  if (action === 'replay') {
    const payId = req.query.paymentId;
    if (!payId) return res.status(400).json({ ok: false, error: 'paymentId obrigatorio' });
    // Remover da lista de processados para permitir replay
    const proc = (await dbGet(PROC_KEY)) || [];
    const novaLista = proc.filter(id => id !== String(payId));
    await dbSet(PROC_KEY, novaLista);
    // Chamar o processamento novamente como se fosse um webhook POST
    req.method = 'POST';
    req.body = { type: 'payment', data: { id: payId } };
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

    // 4. Para cada produto: registrar venda DIRETO no Redis (sem HTTP self-call)
    const VENDAS_KEY  = 'reparoeletro_vendas';
    const FIN_KEY     = 'reparoeletro_financeiro';

    for (const produtoId of produtoIds) {
      let produtoInfo = { id: produtoId, codigo: meta.produto_codigos || '' };
      try {
        const [db, fin] = await Promise.all([
          dbGet(VENDAS_KEY).then(d => d || { produtos: [], nextId: 1 }),
          dbGet(FIN_KEY).then(d => d || { fichas: [] }),
        ]);
        const idx = db.produtos.findIndex(p => p.id === String(produtoId));
        if (idx < 0) {
          console.error('[Webhook] Produto não encontrado:', produtoId);
          await logEvento({ tipo: 'erro_vender', produtoId, erro: 'produto_nao_encontrado', paymentId: String(payId) });
          continue;
        }
        const p = db.produtos[idx];
        if (p.vendido) {
          console.log('[Webhook] Produto já vendido:', produtoId);
          continue;
        }
        const now = new Date().toISOString();
        const precoFmt = parseFloat(p.preco).toLocaleString('pt-BR',{minimumFractionDigits:2,style:'currency',currency:'BRL'});
        const dataVenda = new Date().toLocaleDateString('pt-BR',{timeZone:'America/Sao_Paulo'});

        // Marcar produto como vendido
        db.produtos[idx] = { ...p, vendido: true, soldAt: now,
          nomeCliente, telefone: telefone||null, cpfCnpj: cpf||null,
          vendedor: 'Mercado Pago', modalidade: modPagamento,
          paymentId: String(payId), vendidoEm: now };
        await dbSet(VENDAS_KEY, db);

        // Registrar no financeiro
        const fichaId = `venda-${Date.now()}`;
        fin.fichas = fin.fichas || [];
        fin.fichas.unshift({
          id: fichaId, pipefyId: fichaId, osCode: p.codigo,
          nomeContato: nomeCliente, telefone: telefone||null, cpfCnpj: cpf||null,
          title: (p.tipo ? p.tipo + ' — ' : '') + p.descricao.substring(0,60),
          descricao: `${p.tipo||''} ${p.descricao}`.trim(),
          valor: parseFloat(p.preco), formaPagamento: modPagamento,
          vendedor: 'Mercado Pago', dataVenda, criadoEm: now, phase: 'emitir_nf',
        });
        await dbSet(FIN_KEY, fin);

        produtoInfo = { id: produtoId, codigo: p.codigo, descricao: p.descricao, tipo: p.tipo||'', capacidade: p.capacidade||'' };
        const vData = { ok: true, produto: p };
        if (vData.ok && vData.produto) {
          // Enriquecer com dados reais retornados pelo vender
          produtoInfo = {
            id:         produtoId,
            codigo:     vData.produto.codigo     || meta.produto_codigos || '',
            descricao:  vData.produto.descricao  || '',
            tipo:       vData.produto.tipo       || '',
            capacidade: vData.produto.capacidade || ''
          };
        } else {
          console.error('vender erro:', produtoId, vData?.error);
        }
      } catch(e) { console.error('vender:', e.message); }

      // 5. Espelho no relatório de checkout
      try {
        await fetch(`${siteUrl}/api/tv-checkout?action=registrar-venda`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            produto:       produtoInfo,
            comprador:     { nome: nomeCliente, telefone, cpf, endereco, cep },
            valor:         payment.transaction_amount,
            provedor:      'mercado_pago',
            paymentId:     String(payId),
            paymentMethod: payment.payment_method_id,
            installments:  payment.installments
          })
        });
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
