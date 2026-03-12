// ── Variáveis de ambiente necessárias na Vercel:
// PIPEFY_TOKEN   → token da API do Pipefy
// UPSTASH_URL    → URL do banco Upstash (ex: https://xxx.upstash.io)
// UPSTASH_TOKEN  → token do Upstash

const PIPEFY_API  = "https://api.pipefy.com/graphql";
const PIPE_ID     = "305832912";
const BOARD_KEY   = "techflow:board";

// ── Upstash REST helpers ──────────────────────────────────────
async function dbGet(key) {
  const r = await fetch(`${process.env.UPSTASH_URL}/get/${key}`, {
    headers: { Authorization: `Bearer ${process.env.UPSTASH_TOKEN}` },
  });
  const j = await r.json();
  return j.result ? JSON.parse(j.result) : null;
}

async function dbSet(key, value) {
  await fetch(`${process.env.UPSTASH_URL}/set/${key}`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${process.env.UPSTASH_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(JSON.stringify(value)),
  });
}

// ── Pipefy: busca cards da fase "Aprovado" ────────────────────
async function fetchApprovedCards() {
  const res = await fetch(PIPEFY_API, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${process.env.PIPEFY_TOKEN}`,
    },
    body: JSON.stringify({
      query: `query {
        pipe(id: "${PIPE_ID}") {
          phases {
            name
            cards {
              edges {
                node {
                  id
                  title
                  fields { name value }
                  created_at
                  age
                }
              }
            }
          }
        }
      }`,
    }),
  });
  const json = await res.json();
  if (json.errors) throw new Error(json.errors[0].message);

  const approvedPhase = json.data.pipe.phases.find(
    (p) => p.name.toLowerCase().includes("aprovad")
  );
  if (!approvedPhase) throw new Error('Fase "Aprovado" não encontrada no Pipe.');

  return approvedPhase.cards.edges.map((e) => ({
    pipefyId: String(e.node.id),
    title:    e.node.title,
    fields:   e.node.fields || [],
    age:      e.node.age,
    addedAt:  new Date().toISOString(),
  }));
}

// ── Board padrão ──────────────────────────────────────────────
function defaultBoard() {
  return {
    phases: [
      { id: "entrada",     name: "Entrada"             },
      { id: "diagnostico", name: "Diagnóstico"          },
      { id: "aguardando",  name: "Aguard. Peças"        },
      { id: "manutencao",  name: "Manutenção"           },
      { id: "teste",       name: "Teste / QA"           },
      { id: "pronto",      name: "Pronto p/ Retirada"   },
      { id: "entregue",    name: "Entregue"             },
    ],
    cards: [],          // { pipefyId, title, fields, age, addedAt, phaseId, movedAt, movedBy }
    syncedIds: [],      // ids já importados do Pipefy
  };
}

// ── Handler principal ─────────────────────────────────────────
module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(200).end();

  try {
    const { action } = req.query;

    // ── GET /api/board?action=load ─────────────────────────────
    // Carrega board + importa novos aprovados do Pipefy
    if (req.method === "GET" && action === "load") {
      let board = await dbGet(BOARD_KEY);
      if (!board) board = defaultBoard();

      // Importa novos cards aprovados do Pipefy
      let newCount = 0;
      try {
        const approved = await fetchApprovedCards();
        for (const c of approved) {
          if (!board.syncedIds.includes(c.pipefyId)) {
            board.cards.unshift({ ...c, phaseId: "entrada", movedAt: c.addedAt, movedBy: "Pipefy" });
            board.syncedIds.push(c.pipefyId);
            newCount++;
          }
        }
        if (newCount > 0) await dbSet(BOARD_KEY, board);
      } catch (e) {
        // Pipefy offline não derruba o board
        console.error("Pipefy sync error:", e.message);
      }

      return res.status(200).json({ ok: true, board, newCount });
    }

    // ── POST /api/board?action=move ────────────────────────────
    // Move card para outra fase (só no board, não toca o Pipefy)
    if (req.method === "POST" && action === "move") {
      const { pipefyId, phaseId, movedBy } = req.body;
      if (!pipefyId || !phaseId)
        return res.status(400).json({ ok: false, error: "pipefyId e phaseId são obrigatórios" });

      const board = await dbGet(BOARD_KEY) || defaultBoard();
      const card = board.cards.find((c) => c.pipefyId === String(pipefyId));
      if (!card)
        return res.status(404).json({ ok: false, error: "OS não encontrada no dashboard" });

      const oldPhase = card.phaseId;
      card.phaseId  = phaseId;
      card.movedAt  = new Date().toISOString();
      card.movedBy  = movedBy || "—";
      await dbSet(BOARD_KEY, board);

      return res.status(200).json({ ok: true, card, oldPhase });
    }

    // ── POST /api/board?action=create ──────────────────────────
    // Cria card APENAS no dashboard (não cria no Pipefy)
    if (req.method === "POST" && action === "create") {
      const { title, phaseId, createdBy } = req.body;
      if (!title)
        return res.status(400).json({ ok: false, error: "title é obrigatório" });

      const board = await dbGet(BOARD_KEY) || defaultBoard();
      const newId = "local-" + Date.now();
      const card = {
        pipefyId:  newId,
        title,
        fields:    [],
        age:       0,
        addedAt:   new Date().toISOString(),
        phaseId:   phaseId || "entrada",
        movedAt:   new Date().toISOString(),
        movedBy:   createdBy || "—",
        localOnly: true,
      };
      board.cards.unshift(card);
      board.syncedIds.push(newId);
      await dbSet(BOARD_KEY, board);

      return res.status(200).json({ ok: true, card });
    }

    return res.status(404).json({ ok: false, error: "Ação não encontrada" });

  } catch (err) {
    console.error(err);
    return res.status(500).json({ ok: false, error: err.message });
  }
};
