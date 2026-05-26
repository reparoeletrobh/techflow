module.exports = async function handler(req, res) {
  res.setHeader('Cache-Control', 'no-store');
  const U  = (process.env.UPSTASH_URL   || '').replace(/['"]/g,'').trim();
  const T  = (process.env.UPSTASH_TOKEN || '').replace(/['"]/g,'').trim();
  const PT = (process.env.PIPEFY_TOKEN  || '').replace(/['"]/g,'').trim();
  const PIPE_ID   = '305832912';
  const AGUARDANDO = '334875152';
  const steps = [];

  async function rGet(key) {
    try {
      const r = await fetch(`${U}/pipeline`, {
        method:'POST',
        headers:{ Authorization:`Bearer ${T}`, 'Content-Type':'application/json' },
        body: JSON.stringify([['GET', key]])
      });
      const j = await r.json();
      const v = j[0]?.result;
      if (!v) return null;
      const p = JSON.parse(v);
      return typeof p === 'string' ? JSON.parse(p) : p;
    } catch(e) { return null; }
  }

  async function rSet(key, val) {
    await fetch(`${U}/pipeline`, {
      method:'POST',
      headers:{ Authorization:`Bearer ${T}`, 'Content-Type':'application/json' },
      body: JSON.stringify([['SET', key, JSON.stringify(val)]])
    });
  }

  async function pipefy(query) {
    const r = await fetch('https://api.pipefy.com/graphql', {
      method:'POST',
      headers:{ 'Content-Type':'application/json', Authorization:`Bearer ${PT}` },
      body: JSON.stringify({ query })
    });
    const text = await r.text();
    let j; try { j = JSON.parse(text); } catch(e) { throw new Error('Pipefy inválido: ' + text.slice(0,200)); }
    if (j.errors) throw new Error(j.errors.map(e=>e.message).join('; '));
    return j.data;
  }

  try {
    if (!U || !T) return res.status(500).json({ ok:false, erro:'UPSTASH ausente', steps });
    if (!PT)      return res.status(500).json({ ok:false, erro:'PIPEFY_TOKEN ausente', steps });
    steps.push('✅ Env OK');

    // Buscar na logística primeiro
    let nome, telefone, equipamento, defeito, endereco, preco;
    const logDb = await rGet('reparoeletro_logistica');
    const fichaLog = (logDb?.fichas || []).find(f =>
      (f.nome||'').toLowerCase().includes('clarice') || (f.id||'').includes('0273')
    );

    if (fichaLog) {
      steps.push(`✅ Ficha na logística: ${fichaLog.id} (phase: ${fichaLog.phase})`);
      if (fichaLog.pipefyCardId) {
        steps.push(`ℹ️ Já tem pipefyCardId: ${fichaLog.pipefyCardId}`);
        // Mover para Aguardando
        await pipefy(`mutation { moveCardToPhase(input:{ card_id:"${fichaLog.pipefyCardId}", destination_phase_id:"${AGUARDANDO}" }) { card { id } } }`);
        steps.push(`✅ Movido para Aguardando`);
        return res.status(200).json({ ok:true, steps, pipefyCardId: fichaLog.pipefyCardId, acao:'movido' });
      }
      nome = fichaLog.nome; telefone = fichaLog.telefone;
      equipamento = fichaLog.equipamento; defeito = fichaLog.defeito;
      endereco = fichaLog.endereco; preco = fichaLog.diagnostico?.preco;
    } else {
      steps.push('⚠️ Ficha não na logística — buscando no orçamento...');
      // Buscar no orçamento (ficha pode ter sido deletada pelo cron mas orçamento persiste)
      const orcDb = await rGet('reparoeletro_orcamentos');
      const fichaOrc = (orcDb?.fichas || []).find(f =>
        (f.nome||'').toLowerCase().includes('clarice') ||
        (f.id||'').includes('0273')
      );
      if (!fichaOrc) {
        // Listar últimas fichas do orçamento para debug
        const ultimas = (orcDb?.fichas || []).slice(0,5).map(f=>({id:f.id,nome:f.nome,status:f.status}));
        return res.status(404).json({ ok:false, erro:'Clarice não encontrada em nenhuma base', ultimas5Orc:ultimas, steps });
      }
      steps.push(`✅ Ficha no orçamento: ${fichaOrc.id} — ${fichaOrc.nome}`);
      nome = fichaOrc.nome; telefone = fichaOrc.tel;
      // desc formato "Microondas — Defeito"
      const descParts = (fichaOrc.desc || '').split(' — ');
      equipamento = descParts[0] || ''; defeito = descParts.slice(1).join(' — ') || '';
      endereco = fichaOrc.end; preco = fichaOrc.precoSugerido;
    }

    steps.push(`📋 Dados: nome=${nome} | tel=${telefone} | equip=${equipamento} | defeito=${defeito} | preco=${preco}`);

    // Testar Pipefy
    const me = await pipefy(`query { me { id name } }`);
    steps.push(`✅ Pipefy auth: ${me?.me?.name}`);

    // Campos do pipe
    const pd = await pipefy(`query { pipe(id:"${PIPE_ID}") { start_form_fields { id label type } } }`);
    const fields = pd?.pipe?.start_form_fields || [];
    steps.push(`✅ Campos: ${fields.map(f=>f.label).join(', ')}`);

    const ff = kws => fields.find(f => kws.some(k => f.label.toLowerCase().includes(k)));
    const nF = ff(['nome']); const tF = ff(['telefone','fone','celular']);
    const dF = ff(['descrição','descricao','empresa','descri']); const eF = ff(['endereço','endereco','endere']);

    const u4 = (telefone||'').replace(/\D/g,'').slice(-4);
    const attrs = [];
    if (nF) attrs.push(`{ field_id:"${nF.id}", field_value:${JSON.stringify(`${nome} ${u4}`.trim())} }`);
    if (tF && telefone) attrs.push(`{ field_id:"${tF.id}", field_value:${JSON.stringify(telefone)} }`);
    if (dF) attrs.push(`{ field_id:"${dF.id}", field_value:${JSON.stringify([equipamento,defeito].filter(Boolean).join(' — '))} }`);
    if (eF && endereco) attrs.push(`{ field_id:"${eF.id}", field_value:${JSON.stringify(endereco)} }`);
    steps.push(`✅ Attrs(${attrs.length}): nome=${!!nF} tel=${!!tF} desc=${!!dF} end=${!!eF}`);

    // Criar card
    let card = null; let lastErr = '';
    for (const fase of [true, false]) {
      try {
        const phaseArg = fase ? `phase_id:"${AGUARDANDO}"` : '';
        const mut = `mutation { createCard(input:{ pipe_id:"${PIPE_ID}" ${phaseArg} fields_attributes:[${attrs.join(',')}] }) { card { id title url current_phase { name } } } }`;
        const d = await pipefy(mut);
        card = d?.createCard?.card;
        if (card?.id) { steps.push(`✅ Card criado (fase=${fase}): ${card.id} — ${card.current_phase?.name}`); break; }
      } catch(e) { lastErr = e.message; steps.push(`⚠️ Tentar(fase=${fase}): ${e.message}`); }
    }
    if (!card?.id) return res.status(500).json({ ok:false, erro:lastErr||'sem id', steps });

    // Mover se necessário
    if (card.current_phase?.name && !card.current_phase.name.toLowerCase().includes('aguardando')) {
      await pipefy(`mutation { moveCardToPhase(input:{ card_id:"${card.id}", destination_phase_id:"${AGUARDANDO}" }) { card { id } } }`).catch(e=>steps.push('⚠️ move: '+e.message));
      steps.push('✅ Movido para Aguardando Aprovação');
    }

    // Atualizar valor
    if (preco) {
      await pipefy(`mutation { updateCardField(input:{ card_id:"${card.id}", field_id:"valor_de_contrato", new_value:"${preco}" }) { success } }`).catch(e=>steps.push('⚠️ valor: '+e.message));
      steps.push(`✅ Valor R$${preco}`);
    }

    // Salvar resultado
    await rSet('fix_clarice_result', { ok:true, steps, pipefyCardId:card.id, url:card.url, nome, preco, ts:new Date().toISOString() });
    return res.status(200).json({ ok:true, steps, pipefyCardId:card.id, url:card.url, nome, preco });

  } catch(e) {
    steps.push(`❌ FATAL: ${e.message}`);
    await rSet('fix_clarice_result', { ok:false, erro:e.message, steps, ts:new Date().toISOString() }).catch(()=>{});
    return res.status(500).json({ ok:false, erro:e.message, steps });
  }
};
