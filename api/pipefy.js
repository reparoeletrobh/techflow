const PIPEFY_API = "https://api.pipefy.com/graphql";
const PIPE_ID = "305832912";

async function pipefy(query, variables = {}) {
  const TOKEN = process.env.PIPEFY_TOKEN;
  const res = await fetch(PIPEFY_API, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${TOKEN}`,
    },
    body: JSON.stringify({ query, variables }),
  });
  const json = await res.json();
  if (json.errors) throw new Error(json.errors[0].message);
  return json.data;
}

module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(200).end();

  try {
    const { action } = req.query;

    // GET: carrega todas as fases e cards do pipe
    if (req.method === "GET" && action === "board") {
      const data = await pipefy(`
        query {
          pipe(id: "${PIPE_ID}") {
            name
            phases {
              id
              name
              cards_count
              cards {
                edges {
                  node {
                    id
                    title
                    fields { name value }
                    current_phase { id name }
                    age
                  }
                }
              }
            }
          }
        }
      `);
      return res.status(200).json({ ok: true, data: data.pipe });
    }

    // POST: move card para outra fase
    if (req.method === "POST" && action === "move") {
      const { cardId, phaseId } = req.body;
      if (!cardId || !phaseId)
        return res.status(400).json({ ok: false, error: "cardId e phaseId são obrigatórios" });

      const data = await pipefy(
        `mutation MoveCard($cardId: ID!, $phaseId: ID!) {
          moveCardToPhase(input: { card_id: $cardId, destination_phase_id: $phaseId }) {
            card { id title current_phase { id name } }
          }
        }`,
        { cardId, phaseId }
      );
      return res.status(200).json({ ok: true, card: data.moveCardToPhase.card });
    }

    // POST: cria novo card
    if (req.method === "POST" && action === "create") {
      const { title, phaseId } = req.body;
      if (!title)
        return res.status(400).json({ ok: false, error: "title é obrigatório" });

      const data = await pipefy(`
        mutation {
          createCard(input: {
            pipe_id: "${PIPE_ID}"
            title: "${title.replace(/"/g, '\\"')}"
            ${phaseId ? `phase_id: "${phaseId}"` : ""}
          }) {
            card { id title current_phase { id name } }
          }
        }
      `);
      return res.status(200).json({ ok: true, card: data.createCard.card });
    }

    return res.status(404).json({ ok: false, error: "Ação não encontrada" });

  } catch (err) {
    console.error(err);
    return res.status(500).json({ ok: false, error: err.message });
  }
};
