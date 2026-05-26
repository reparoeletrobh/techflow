// api/fix-clarice-0273.js — autossuficiente, sem dependências externas
module.exports = async function handler(req, res) {
  res.setHeader('Cache-Control', 'no-store');

  const U = (process.env.UPSTASH_URL   || '').replace(/['"]/g,'').trim();
  const T = (process.env.UPSTASH_TOKEN || '').replace(/['"]/g,'').trim();
  const PT = (process.env.PIPEFY_TOKEN || '').replace(/['"]/g,'').trim();
  const LOG_KEY = 'reparoeletro_logistica';
  const ORC_KEY = 'reparoeletro_orcamentos';
  const PIPE_ID = '305832912';
  const AGUARDANDO = '334875152';

  const steps = [];

  // ── Redis: leitura via REST simples ───────────────────────
  async function rGet(key) {
    const r = await fetch(`${U}/get/${encodeURIComponent(key)}`, {
      headers: { Authorization: `Bearer ${T}` }
    });
    const j = await r.json();
    if (!j.result) return null;
    try { return JSON.parse(j.result); } catch(e) { return j.result; }
  }

  async function rSet(key, val) {
    const body = JSON.stringify(val);
    await fetch(`${U}/set/${encodeURIComponent(key)}`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${T}`, 'Content-Type': 'application/json' },
      body: JSON.stringify([body])
    });
  }

  // ── Pipefy GraphQL ─────────────────────────────────────────
  async function pipefy(query) {
    const r = await fetch('https://api.pipefy.com/graphql', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${PT}` },
      body: JSON.stringify({ query })
    });
    const text = await r.text();
    let j;
    try { j = JSON.parse(text); } catch(e) { throw new Error('Pipefy resposta inválida: ' + text.slice(0,200)); }
    if (j.errors) throw new Error(j.errors.map(e => e.message).join('; '));
    return j.data;
  }

  try {
    // 1. Verificar env vars
    if (!U || !T) return res.status(500).json({ ok:false, erro:'UPSTASH não configurado', U: !!U, T: !!T });
    if (!PT)      return res.status(500).json({ ok:false, erro:'PIPEFY_TOKEN não configurado' });
    steps.push('✅ Env vars OK');

    // 2. Ler logística
    const logDb = await rGet(LOG_KEY);
    if (!logDb) return res.status(500).json({ ok:false, erro:'reparoeletro_logistica vazio', steps });
    const fichas = logDb.fichas || [];
    steps.push(`✅ Logística lida: ${fichas.length} fichas`);

    // 3. Encontrar Clarice
    const ficha = fichas.find(f =>
      (f.nome || '').toLowerCase().includes('clarice') ||
      (f.id   || '').includes('0273')
    );
    if (!ficha) {
      const nomes = fichas.slice(0,5).map(f => f.nome);
      return res.status(404).json({ ok:false, erro:'Ficha Clarice não encontrada', total: fichas.length, primeiros5: nomes, steps });
    }
    steps.push(`✅ Ficha: ${ficha.id} — ${ficha.nome} (phase: ${ficha.phase}, pipefyCardId: ${ficha.pipefyCardId || 'NULL'})`);

    // 4. Se já tem card → só mover
    if (ficha.pipefyCardId) {
      const d = await pipefy(`mutation { moveCardToPhase(input: { card_id: "${ficha.pipefyCardId}", destination_phase_id: "${AGUARDANDO}" }) { card { id } } }`);
      steps.push(`✅ Card ${ficha.pipefyCardId} movido para Aguardando Aprovação`);
      return res.status(200).json({ ok:true, steps, pipefyCardId: ficha.pipefyCardId, acao: 'movido' });
    }

    // 5. Testar conexão Pipefy
    const meData = await pipefy(`query { me { id name } }`);
    steps.push(`✅ Pipefy autenticado: ${meData?.me?.name || meData?.me?.id}`);

    // 6. Buscar campos do pipe
    const pipeData = await pipefy(`query { pipe(id: "${PIPE_ID}") { start_form_fields { id label type } } }`);
    const fields = pipeData?.pipe?.start_form_fields || [];
    steps.push(`✅ Pipe campos: ${fields.map(f => f.label).join(', ')}`);

    // 7. Mapear campos
    function findF(kws) { return fields.find(f => kws.some(k => f.label.toLowerCase().includes(k))); }
    const nomeF = findF(['nome']);
    const telF  = findF(['telefone','fone','celular']);
    const descF = findF(['descrição','descricao','empresa','descri']);
    const endF  = findF(['endereço','endereco','endere']);

    const ultimos4    = (ficha.telefone||'').replace(/\D/g,'').slice(-4);
    const nomeContato = `${ficha.nome} ${ultimos4}`.trim();
    const descricao   = [ficha.equipamento, ficha.defeito].filter(Boolean).join(' — ');

    const attrs = [];
    if (nomeF) attrs.push(`{ field_id: "${nomeF.id}", field_value: ${JSON.stringify(nomeContato)} }`);
    if (telF && ficha.telefone) attrs.push(`{ field_id: "${telF.id}", field_value: ${JSON.stringify(ficha.telefone)} }`);
    if (descF && descricao)     attrs.push(`{ field_id: "${descF.id}", field_value: ${JSON.stringify(descricao)} }`);
    if (endF && ficha.endereco) attrs.push(`{ field_id: "${endF.id}", field_value: ${JSON.stringify(ficha.endereco)} }`);
    steps.push(`✅ Attrs mapeados (${attrs.length}): nome=${!!nomeF} tel=${!!telF} desc=${!!descF} end=${!!endF}`);

    // 8. Criar card — tenta com phase_id, depois sem
    let card = null;
    let createErro = null;
    for (const comPhase of [true, false]) {
      try {
        const phaseArg = comPhase ? `\n        phase_id: "${AGUARDANDO}"` : '';
        const d = await pipefy(`mutation {
          createCard(input: {
            pipe_id: "${PIPE_ID}"${phaseArg}
            fields_attributes: [${attrs.join(', ')}]
          }) { card { id title url current_phase { name } } }
        }`);
        card = d?.createCard?.card;
        if (card?.id) { steps.push(`✅ Card criado (comPhase=${comPhase}): ${card.id} — fase: ${card.current_phase?.name}`); break; }
      } catch(e) {
        createErro = e.message;
        steps.push(`⚠️ createCard comPhase=${comPhase} falhou: ${e.message}`);
      }
    }

    if (!card?.id) return res.status(500).json({ ok:false, erro: createErro || 'card.id null', steps });

    // 9. Mover para Aguardando se necessário
    if (card.current_phase?.name && !card.current_phase.name.toLowerCase().includes('aguardando')) {
      try {
        await pipefy(`mutation { moveCardToPhase(input: { card_id: "${card.id}", destination_phase_id: "${AGUARDANDO}" }) { card { id } } }`);
        steps.push(`✅ Movido para Aguardando Aprovação`);
      } catch(e) { steps.push(`⚠️ Move falhou: ${e.message}`); }
    }

    // 10. Atualizar valor
    const preco = ficha.diagnostico?.preco;
    if (preco) {
      try {
        await pipefy(`mutation { updateCardField(input: { card_id: "${card.id}", field_id: "valor_de_contrato", new_value: "${preco}" }) { success } }`);
        steps.push(`✅ Valor R$${preco} atualizado`);
      } catch(e) { steps.push(`⚠️ Valor: ${e.message}`); }
    }

    // 11. Salvar pipefyCardId na logística
    const fi = logDb.fichas.findIndex(f => f.id === ficha.id);
    if (fi >= 0) {
      logDb.fichas[fi].pipefyCardId = String(card.id);
      delete logDb.fichas[fi].pipefyErro;
      await rSet(LOG_KEY, logDb);
      steps.push(`✅ pipefyCardId ${card.id} salvo na logística`);
    }

    // 12. Salvar no orçamento
    try {
      const orcDb = await rGet(ORC_KEY);
      if (orcDb?.fichas) {
        const oi = orcDb.fichas.findIndex(f => f.id === ficha.id || f.id === ficha.pipefyCardId);
        if (oi >= 0) { orcDb.fichas[oi].id = String(card.id); orcDb.fichas[oi].pipefyId = String(card.id); await rSet(ORC_KEY, orcDb); steps.push(`✅ Orçamento atualizado`); }
      }
    } catch(e) { steps.push(`⚠️ Orçamento: ${e.message}`); }

    return res.status(200).json({ ok:true, steps, pipefyCardId: card.id, url: card.url, nome: ficha.nome, preco });
  } catch(e) {
    steps.push(`❌ ERRO FATAL: ${e.message}`);
    return res.status(500).json({ ok:false, erro: e.message, steps });
  }
};
