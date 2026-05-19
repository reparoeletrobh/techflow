// api/webhook-mp.js
// Recebe notificações do Mercado Pago e processa pagamentos aprovados:
//   1. Marca produto como vendido (api/vendas?action=vender)
//   2. Registra no relatório de checkout (api/tv-checkout?action=registrar-venda)
//   3. Cria ficha no Pipefy almoxarifado (dentro do vender)
import { Redis } from '@upstash/redis';

const redis = new Redis({
  url:   (process.env.UPSTASH_URL   || '').replace(/['"]/g,'').trim(),
  token: (process.env.UPSTASH_TOKEN || '').replace(/['"]/g,'').trim()
});

const MP_TOKEN = process.env.MP_ACCESS_TOKEN || '';
const LOG_KEY  = 'mp_webhook_log';

async function logEvento(evento) {
  try {
    const logs = await redis.get(LOG_KEY) || [];
    logs.unshift({ ...evento, ts: new Date().toISOString() });
    await redis.set(LOG_KEY, logs.slice(0, 200)); // guardar últimos 200
  } catch(e) { console.error('logEvento:', e.message); }
}

export default async function handler(req, res) {
  // MP exige sempre 200 para não retentar
  res.setHeader('Content-Type', 'application/json');

  if (req.method !== 'POST') return res.status(200).json({ ok: true });

  const body   = req.body || {};
  const tipo   = body.type || req.query.type || '';
  const payId  = body.data?.id || req.query['data.id'] || '';

  // Ignorar eventos que não são pagamentos
  if (tipo !== 'payment' || !payId) {
    return res.status(200).json({ ok: true, ignored: tipo });
  }

  try {
    // ── 1. Buscar detalhes do pagamento no MP ─────────────────────────
    const mpRes = await fetch(`https://api.mercadopago.com/v1/payments/${payId}`, {
      headers: { 'Authorization': `Bearer ${MP_TOKEN}` }
    });
    const payment = await mpRes.json();

    await logEvento({
      paymentId: payId,
      status:    payment.status,
      method:    payment.payment_method_id,
      amount:    payment.transaction_amount,
      metadata:  payment.metadata
    });

    // Só processar pagamentos aprovados
    if (payment.status !== 'approved') {
      return res.status(200).json({ ok: true, status: payment.status });
    }

    // ── 2. Extrair metadados da preferência ───────────────────────────
    const meta          = payment.metadata || {};
    const produtoIds    = (meta.produto_ids || '').split(',').filter(Boolean);
    const compradorNome = meta.comprador_nome || payment.payer?.name || 'Comprador Online';
    const compradorTel  = meta.comprador_tel  || '';
    const compradorCpf  = meta.comprador_cpf  || '';
    const compradorEnd  = meta.comprador_end  || '';
    const compradorCep  = meta.comprador_cep  || '';

    const modPagamento = payment.payment_method_id === 'pix'
      ? 'PIX'
      : `Cartão ${payment.installments}x`;

    const proto   = req.headers['x-forwarded-proto'] || 'https';
    const host    = req.headers['x-forwarded-host']  || req.headers.host || 'reparoeletroadm.com';
    const siteUrl = `${proto}://${host}`;

    // ── 3. Para cada produto: vender + registrar-venda ────────────────
    for (const produtoId of produtoIds) {
      // 3a. Marcar como vendido + criar ficha financeiro + Pipefy almoxarifado
      try {
        const vR = await fetch(`${siteUrl}/api/vendas?action=vender`, {
          method:  'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            produtoId,
            nomeCliente: compradorNome,
            telefone:    compradorTel  || null,
            cpfCnpj:     compradorCpf  || null,
            vendedor:    'Mercado Pago',
            modalidade:  modPagamento
          })
        });
        const vData = await vR.json();
        if (!vData.ok) {
          console.error('vender error produto', produtoId, vData.error);
        }
      } catch(e) {
        console.error('vender fetch:', e.message);
      }

      // 3b. Registrar no relatório de checkout (seção Vendas Padrão)
      try {
        await fetch(`${siteUrl}/api/tv-checkout?action=registrar-venda`, {
          method:  'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            produto: {
              id:      produtoId,
              codigo:  meta.produto_codigos || ''
            },
            comprador: {
              nome:     compradorNome,
              telefone: compradorTel,
              cpf:      compradorCpf,
              endereco: compradorEnd,
              cep:      compradorCep
            },
            valor:        payment.transaction_amount,
            provedor:     'mercado_pago',
            paymentId,
            paymentMethod: payment.payment_method_id,
            installments:  payment.installments
          })
        });
      } catch(e) {
        console.error('registrar-venda fetch:', e.message);
      }
    }

    return res.status(200).json({ ok: true, processados: produtoIds.length });

  } catch(e) {
    console.error('webhook-mp:', e.message);
    // Sempre 200 para MP não retentar
    return res.status(200).json({ ok: true, error: e.message });
  }
}
