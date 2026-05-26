// api/checkout-mp-criar.js
// Cria uma preferência de pagamento no Mercado Pago
// Suporta: PIX + Cartão de Crédito 3x sem juros
export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST,OPTIONS');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'POST') return res.status(405).json({ ok: false, error: 'Method not allowed' });

  const MP_TOKEN = process.env.MP_ACCESS_TOKEN || '';
  if (!MP_TOKEN) return res.status(500).json({ ok: false, error: 'MP_ACCESS_TOKEN não configurado' });

  const { itens, comprador, metodoPagamento, origem } = req.body || {};
  if (!itens?.length) return res.status(400).json({ ok: false, error: 'itens obrigatório' });

  const proto = req.headers['x-forwarded-proto'] || 'https';
  const host  = req.headers['x-forwarded-host'] || req.headers.host || 'reparoeletroadm.com';
  const siteUrl = `${proto}://${host}`;

  const preference = {
    items: itens.map(i => ({
      id:          String(i.id),
      title:       i.nome?.substring(0, 256) || 'Equipamento',
      unit_price:  parseFloat(i.preco),
      quantity:    1,
      currency_id: 'BRL',
      ...(i.foto ? { picture_url: i.foto } : {})
    })),

    payer: comprador ? {
      name:  comprador.nome || '',
      email: 'comprador@reparoeletro.com',
      phone: comprador.telefone
        ? { area_code: comprador.telefone.replace(/\D/g,'').slice(0,2),
            number:    comprador.telefone.replace(/\D/g,'').slice(2) }
        : undefined,
      identification: comprador.cpf
        ? { type: 'CPF', number: comprador.cpf.replace(/\D/g,'') }
        : undefined,
      address: comprador.endereco
        ? { street_name: comprador.endereco, zip_code: (comprador.cep||'').replace(/\D/g,'') }
        : undefined
    } : {},

    payment_methods: metodoPagamento === 'pix'
      ? {
          // Apenas PIX (preço com 10% de desconto já aplicado)
          excluded_payment_types: [
            { id: 'credit_card' },
            { id: 'debit_card' },
            { id: 'ticket' },
            { id: 'atm' }
          ]
        }
      : {
          // Apenas Cartão de Crédito (preço cheio, 3x sem juros)
          excluded_payment_types: [
            { id: 'bank_transfer' }, // exclui PIX
            { id: 'ticket' },
            { id: 'atm' }
          ],
          installments:             3,
          no_interest_installments: 3
        },

    back_urls: {
      success: `${siteUrl}/${origem === 'tv' ? 'tv/equipamentos' : 'produto.html'}?mp=success`,
      failure: `${siteUrl}/${origem === 'tv' ? 'tv/equipamentos' : 'produto.html'}?mp=failure`,
      pending: `${siteUrl}/${origem === 'tv' ? 'tv/equipamentos' : 'produto.html'}?mp=pending`
    },
    auto_return: 'approved',

    notification_url: `${siteUrl}/api/webhook-mp`,

    metadata: {
      produto_ids:      itens.map(i => i.id).join(','),
      produto_codigos:  itens.map(i => i.codigo).join(','),
      comprador_nome:   comprador?.nome   || '',
      comprador_tel:    comprador?.telefone || '',
      comprador_cpf:    comprador?.cpf    || '',
      comprador_end:    comprador?.endereco || '',
      comprador_cep:    comprador?.cep      || ''
    },

    statement_descriptor: 'REPARO ELETRO BH',
    expires: false
  };

  try {
    const mpRes = await fetch('https://api.mercadopago.com/checkout/preferences', {
      method:  'POST',
      headers: {
        'Content-Type':  'application/json',
        'Authorization': `Bearer ${MP_TOKEN}`
      },
      body: JSON.stringify(preference)
    });

    const data = await mpRes.json();

    if (!mpRes.ok) {
      console.error('MP preference error:', JSON.stringify(data));
      return res.status(500).json({ ok: false, error: data?.message || 'Erro MP' });
    }

    return res.status(200).json({
      ok:          true,
      checkoutUrl: data.init_point,
      sandboxUrl:  data.sandbox_init_point,
      preferenceId: data.id
    });

  } catch (e) {
    console.error('checkout-mp-criar:', e.message);
    return res.status(500).json({ ok: false, error: e.message });
  }
}
