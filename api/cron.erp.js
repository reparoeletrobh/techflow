// Cron job: todo domingo às 23h BRT move cards de ERP para Finalizado
const PIPEFY_API = "https://api.pipefy.com/graphql";
const PIPE_ID    = "305832912";
const ERP_PHASE_ID       = "339008925";
const FINALIZADO_PHASE_ID = "334875153";

async function pipefyQuery(query) {
  const r = await fetch(PIPEFY_API, {
    method: "POST",
    headers: { "Content-Type": "application/json", Authorization: `Bearer ${(process.env.PIPEFY_TOKEN||"").trim()}` },
    body: JSON.stringify({ query }),
  });
  const j = await r.json();
  if (j.errors) throw new Error(j.errors[0].message);
  return j.data;
}

async function getErpCards() {
  const all = []; let cursor = null, hasNext = true;
  while (hasNext) {
    const after = cursor ? `, after: "${cursor}"` : "";
    const data = await pipefyQuery(`query {
      phase(id: "${ERP_PHASE_ID}") {
        cards(first: 50${after}) {
          pageInfo { hasNextPage endCursor }
          edges { node { id title } }
        }
      }
    }`);
    const phase = data?.phase;
    if (!phase) break;
    phase.cards.edges.forEach(({ node }) => all.push(node));
    hasNext = phase.cards.pageInfo?.hasNextPage ?? false;
    cursor  = phase.cards.pageInfo?.endCursor ?? null;
  }
  return all;
}

async function moveToFinalizado(cardId) {
  return await pipefyQuery(`mutation {
    moveCardToPhase(input: { card_id: "${cardId}", destination_phase_id: "${FINALIZADO_PHASE_ID}" }) {
      card { id current_phase { name } }
    }
  }`);
}

module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");

  // Verifica se é domingo 23h BRT (UTC-3)
  const now = new Date(new Date().toLocaleString("en-US", { timeZone: "America/Sao_Paulo" }));
  const isDomingo = now.getDay() === 0;
  const hora      = now.getHours();

  // Permite chamada manual via ?force=1 ou automática quando domingo 23h
  const isAuto    = isDomingo && hora === 23;
  const isForced  = req.query.force === "1";

  if (!isAuto && !isForced) {
    return res.status(200).json({ ok: true, skipped: true, msg: `Agendado para domingo 23h BRT. Hoje: ${["Dom","Seg","Ter","Qua","Qui","Sex","Sab"][now.getDay()]} ${hora}h` });
  }

  try {
    const cards = await getErpCards();
    if (!cards.length) return res.status(200).json({ ok: true, moved: 0, msg: "Nenhum card em ERP" });

    const results = [];
    for (const card of cards) {
      try {
        await moveToFinalizado(card.id);
        results.push({ id: card.id, title: card.title, ok: true });
      } catch(e) {
        results.push({ id: card.id, title: card.title, ok: false, error: e.message });
      }
    }
    const moved = results.filter(r => r.ok).length;
    return res.status(200).json({ ok: true, moved, total: cards.length, results });
  } catch(e) {
    return res.status(200).json({ ok: false, error: e.message });
  }
};
