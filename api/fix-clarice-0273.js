// api/fix-clarice-0273.js — one-shot: força Pipefy para ficha Clarice 0273
const U = process.env.UPSTASH_URL;
const T = process.env.UPSTASH_TOKEN;
const PIPEFY_API = 'https://api.pipefy.com/graphql';
const PIPE_ID = '305832912';
const AGUARDANDO_PHASE_ID = '334875152';
const LOG_KEY = 'reparoeletro_logistica';
const ORC_KEY = 'reparoeletro_orcamentos';
const RESULT_KEY = 'fix_clarice_result';

async function dbGet(key) {
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
}

async function dbSet(key, val) {
  await fetch(`${U}/pipeline`, {
    method:'POST',
    headers:{ Authorization:`Bearer ${T}`, 'Content-Type':'application/json' },
    body: JSON.stringify([['SET', key, JSON.stringify(val)]])
  });
}

async function pipefyQ(query) {
  const token = (process.env.PIPEFY_TOKEN || '').trim();
  const r = await fetch(PIPEFY_API, {
    method:'POST',
    headers:{ 'Content-Type':'application/json', Authorization:`Bearer ${token}` },
    body: JSON.stringify({ query })
  });
  const j = await r.json();
  if (j.errors) throw new Error(j.errors[0].message);
  return j.data;
}

async function runFix() {
  const steps = [];

  // 1. Buscar ficha Clarice 0273 na logística
  const db = await dbGet(LOG_KEY);
  if (!db) return { ok:false, erro:'LOG_KEY vazio' };

  const ficha = db.fichas.find(f =>
    (f.nome || '').toLowerCase().includes('clarice') ||
    (f.id   || '').includes('0273')
  );

  if (!ficha) return { ok:false, erro:'Ficha Clarice 0273 não encontrada na logística', totalFichas: db.fichas.length };
  steps.push(`✅ Ficha encontrada: ${ficha.id} — ${ficha.nome} (phase: ${ficha.phase})`);
  steps.push(`   pipefyCardId atual: ${ficha.pipefyCardId || 'NULL'}`);
  steps.push(`   pipefyErro: ${ficha.pipefyErro || 'nenhum'}`);
  steps.push(`   precoFinal: ${ficha.diagnostico?.preco || 'N/A'}`);

  // 2. Se já tem pipefyCardId — só mover para Aguardando
  if (ficha.pipefyCardId) {
    try {
      await pipefyQ(`mutation { moveCardToPhase(input: { card_id: "${ficha.pipefyCardId}", destination_phase_id: "${AGUARDANDO_PHASE_ID}" }) { card { id } } }`);
      steps.push(`✅ Card ${ficha.pipefyCardId} movido para Aguardando Aprovação`);
      return { ok:true, steps, pipefyCardId: ficha.pipefyCardId };
    } catch(e) {
      steps.push(`❌ Erro ao mover: ${e.message}`);
      return { ok:false, steps, erro: e.message };
    }
  }

  // 3. Criar card novo no Pipefy
  // Buscar campos do pipe
  const pipeData = await pipefyQ(`query { pipe(id: "${PIPE_ID}") { start_form_fields { id label type } } }`);
  const fields = pipeData?.pipe?.start_form_fields || [];
  steps.push(`✅ Pipe estrutura: ${fields.length} campos`);

  function findField(kws) { return fields.find(f => kws.some(k => f.label.toLowerCase().includes(k))); }

  const nomeF = findField(['nome']);
  const telF  = findField(['telefone','fone','celular']);
  const descF = findField(['descrição','descricao','empresa','descri']);
  const endF  = findField(['endereço','endereco','endere']);

  const ultimos4 = (ficha.telefone || '').replace(/\D/g,'').slice(-4);
  const nomeContato = `${ficha.nome} ${ultimos4}`.trim();
  const descricao = [ficha.equipamento, ficha.defeito].filter(Boolean).join(' — ');

  const attrs = [];
  if (nomeF) attrs.push(`{ field_id: "${nomeF.id}", field_value: ${JSON.stringify(nomeContato)} }`);
  if (telF && ficha.telefone) attrs.push(`{ field_id: "${telF.id}", field_value: ${JSON.stringify(ficha.telefone)} }`);
  if (descF && descricao) attrs.push(`{ field_id: "${descF.id}", field_value: ${JSON.stringify(descricao)} }`);
  if (endF && ficha.endereco) attrs.push(`{ field_id: "${endF.id}", field_value: ${JSON.stringify(ficha.endereco)} }`);
  steps.push(`✅ Campos mapeados: ${attrs.length}`);

  // Tentar criar com phase_id (Aguardando Aprovação)
  let card = null;
  try {
    const res = await pipefyQ(`mutation {
      createCard(input: {
        pipe_id: "${PIPE_ID}"
        phase_id: "${AGUARDANDO_PHASE_ID}"
        fields_attributes: [${attrs.join(', ')}]
      }) { card { id title url } }
    }`);
    card = res?.createCard?.card;
    steps.push(`✅ Card criado com phase_id: ${card?.id}`);
  } catch(e) {
    steps.push(`⚠️ Criar com phase_id falhou (${e.message}), tentando sem phase_id...`);
    try {
      const res = await pipefyQ(`mutation {
        createCard(input: {
          pipe_id: "${PIPE_ID}"
          fields_attributes: [${attrs.join(', ')}]
        }) { card { id title url } }
      }`);
      card = res?.createCard?.card;
      steps.push(`✅ Card criado sem phase_id: ${card?.id}`);
      // Mover para Aguardando
      if (card?.id) {
        await pipefyQ(`mutation { moveCardToPhase(input: { card_id: "${card.id}", destination_phase_id: "${AGUARDANDO_PHASE_ID}" }) { card { id } } }`);
        steps.push(`✅ Card movido para Aguardando Aprovação`);
      }
    } catch(e2) {
      steps.push(`❌ Criação sem phase_id também falhou: ${e2.message}`);
      return { ok:false, steps, erro: e2.message };
    }
  }

  if (!card?.id) return { ok:false, steps, erro:'Card não retornou id' };

  // 4. Atualizar valor no Pipefy
  const preco = ficha.diagnostico?.preco;
  if (preco) {
    try {
      await pipefyQ(`mutation { updateCardField(input: { card_id: "${card.id}", field_id: "valor_de_contrato", new_value: "${preco}" }) { success } }`);
      steps.push(`✅ Valor R$${preco} atualizado no Pipefy`);
    } catch(e) { steps.push(`⚠️ Valor não atualizado: ${e.message}`); }
  }

  // 5. Salvar pipefyCardId na ficha
  const fichaIdx = db.fichas.findIndex(f => f.id === ficha.id);
  if (fichaIdx >= 0) {
    db.fichas[fichaIdx].pipefyCardId = String(card.id);
    delete db.fichas[fichaIdx].pipefyErro;
    await dbSet(LOG_KEY, db);
    steps.push(`✅ pipefyCardId ${card.id} salvo na ficha`);
  }

  // 6. Atualizar ficha no orçamento também
  const orcDb = await dbGet(ORC_KEY);
  if (orcDb) {
    const orcFicha = orcDb.fichas?.find(f => f.id === ficha.id || f.id === ficha.pipefyCardId);
    if (orcFicha) {
      orcFicha.pipefyId = String(card.id);
      orcFicha.id = String(card.id);
      await dbSet(ORC_KEY, orcDb);
      steps.push(`✅ ID atualizado no orçamento`);
    }
  }

  // 7. Salvar resultado para consulta
  const result = { ok:true, steps, ficha: ficha.id, nome: ficha.nome, pipefyCardId: card.id, url: card.url, preco, ts: new Date().toISOString() };
  await dbSet(RESULT_KEY, result);
  return result;
}

module.exports = async function handler(req, res) {
  res.setHeader('Cache-Control','no-store');
  try {
    const result = await runFix();
    return res.status(200).json(result);
  } catch(e) {
    return res.status(500).json({ ok:false, erro: e.message });
  }
};
