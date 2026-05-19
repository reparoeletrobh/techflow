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

  if (req.method !== 'POST') return res.status(200).json({ ok: true });

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

    // 4. Para cada produto: vender (remove catálogo + Pipefy)
    for (const produtoId of produtoIds) {
      let produtoData = { id: produtoId, codigo: meta.produto_codigos || '' };
      try {
        const vRes = await fetch(`${siteUrl}/api/vendas?action=vender`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            produtoId,
            nomeCliente,
            telefone:   telefone || null,
            cpfCnpj:    cpf      || null,
            vendedor:   'Mercado Pago',
            modalidade: modPagamento
          })
        });
        const vData = await vRes.json();
        if (vData.ok && vData.produto) {
          // Enriquecer com dados reais do produto (descricao, tipo, capacidade)
          produtoData = {
            id:        produtoId,
            codigo:    vData.produto.codigo    || meta.produto_codigos || '',
            descricao: vData.produto.descricao || '',
            nome:      vData.produto.descricao || '',
            tipo:      vData.produto.tipo      || '',
            capacidade:vData.produto.capacidade|| '',
            preco:     vData.produto.preco     || payment.transaction_amount
          };
        } else if (!vData.ok) {
          console.error('vender erro:', produtoId, vData.error);
        }
      } catch(e) { console.error('vender:', e.message); }

      // 5. Espelho no relatório de checkout com dados completos
      try {
        await fetch(`${siteUrl}/api/tv-checkout?action=registrar-venda`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            produto:       produtoData,
            comprador:     { nome: nomeCliente, telefone, cpf, endereco, cep },
            valor:         payment.transaction_amount,
            provedor:      'mercado_pago',
            paymentId:     String(payId),
            paymentMethod: payment.payment_method_id,
            installments:  payment.installments
          })
        });
      } catch(e) { console.error('registrar-venda:', e.message); }
    }

    return res.status(200).json({ ok: true, processados: produtoIds.length });

  } catch(e) {
    console.error('webhook-mp:', e.message);
    // Não marcar como processado se deu erro — MP pode tentar novamente
    return res.status(200).json({ ok: true });
  }
}
