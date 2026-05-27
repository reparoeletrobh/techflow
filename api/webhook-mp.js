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

// ── Pipefy: criar card Receber após venda confirmada pelo MP ────────────────
function sanitizePipefy(s) {
  return String(s||'').replace(/[\\]/g,'').replace(/"/g,"'").replace(/\n/g,' ')
    .normalize('NFD').replace(/[\u0300-\u036f]/g,'').replace(/[^\x00-\x7F]/g,'');
}

async function criarCardPipefyVenda(pipeId, produto, comprador, valor, paymentId) {
  const PIPEFY_API = 'https://api.pipefy.com/graphql';
  const token = (process.env.PIPEFY_TOKEN || '').trim();
  if (!token || !pipeId) return null;

  async function pipefyQ(query) {
    const controller = new AbortController();
    const tid = setTimeout(() => controller.abort(), 10000);
    try {
      const r = await fetch(PIPEFY_API, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + token },
        body: JSON.stringify({ query }),
        signal: controller.signal
      });
      const j = await r.json();
      clearTimeout(tid);
      if (j.errors) throw new Error(j.errors[0].message);
      return j.data;
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





  // ── GET criar-card-pipefy: cria card no Pipefy para venda já registrada ──
  if (action === 'criar-card-pipefy') {
    const pid = req.query.paymentId;
    if (!pid) return res.status(400).json({ ok:false, error:'paymentId obrigatorio' });
    try {
      // Buscar venda no checkout ADM
      const CKKEY = 'reparoeletro_checkout_vendas';
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
      const ck = (await dbGet('reparoeletro_checkout_vendas')) || { vendas:[] };
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
      const inicio    = new Date(agora.getTime() - 30 * 60 * 1000); // últimos 30 min
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
      const CKKEY = 'reparoeletro_checkout_vendas';
      const ck    = (await dbGet(CKKEY)) || { vendas: [] };
      ck.vendas   = ck.vendas || [];
      const jaNoCheckout = new Set(ck.vendas.map(v => String(v.paymentId)));

      // Filtrar apenas os que ainda não foram registrados
      const pendentes = pagamentos.filter(p => !jaNoCheckout.has(String(p.id)));

      if (!pendentes.length) {
        return res.status(200).json({ ok: true, info: 'todos já registrados', total: pagamentos.length, pendentes: 0 });
      }

      const resultado = [];
      const admDb  = (await dbGet('reparoeletro_vendas')) || { produtos: [] };
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
        if (mudouAdm) await dbSet('reparoeletro_vendas', admDb);
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
      const PROC_KEY = 'mp_processados';
      const CKKEY    = 'reparoeletro_checkout_vendas';

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
          const admDb = (await dbGet('reparoeletro_vendas')) || { produtos:[] };
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
          if (marcou) await dbSet('reparoeletro_vendas', admDb);

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
