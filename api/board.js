const PIPEFY_API  = "https://api.pipefy.com/graphql";
const PIPE_ID     = "305832912";
const BOARD_KEY   = "techflow:board";

const UPSTASH_URL   = (process.env.UPSTASH_URL   || "").replace(/['"]/g, "").trim();
const UPSTASH_TOKEN = (process.env.UPSTASH_TOKEN || "").replace(/['"]/g, "").trim();

// ── Upstash helpers ───────────────────────────────────────────
async function dbGet(key) {
  try {
    const r = await fetch(`${UPSTASH_URL}/get/${encodeURIComponent(key)}`, {
      headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` },
    });
    const j = await r.json();
    return j.result ? JSON.parse(j.result) : null;
  } catch(e) {
    console.error("dbGet error:", e.message);
    return null;
  }
}

async function dbSet(key, value) {
  try {
    await fetch(`${UPSTASH_URL}/set/${encodeURIComponent(key)}`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${UPSTASH_TOKEN}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(JSON.stringify(value)),
    });
  } catch(e) {
    console.error("dbSet error:", e.message);
  }
}

// ── Board padrão ──────────────────────────────────────────────
function defaultBoard() {
  return {
    phases: [
      { id: "entrada",     name: "Entrada"           },
      { id: "diagnostico", name: "Diagnóstico"        },
      { id: "aguardando",  name: "Aguard. Peças"      },
      { id: "manutencao",  name: "Manutenção"         },
      { id: "teste",       name: "Teste / QA"         },
      { id: "pronto",      name: "Pronto p/ Retirada" },
      { id: "entregue",    name: "Entregue"           },
    ],
    cards: [],
    syncedIds: [],
  };
}

function sanitizeBoard(board) {
  if (!board || typeof board !== "object") return defaultBoard();
  if (!Array.isArray(board.phases) || board.phases.length === 0) board.phases = defaultBoard().phases;
  if (!Array.isArray(board.cards))     board.cards     = [];
  if (!Array.isArray(board.syncedIds)) board.syncedIds = [];
  const validIds = board.phases.map(p => p.id);
  board.cards = board.cards.map(c => ({
    ...c,
    phaseId: validIds.includes(c.phaseId) ? c.phaseId : board.phases[0].id,
  }));
  return board;
}

// ── Pipefy: busca OS da fase Aprovado ─────────────────────────
async function fetchApprovedCards() {
  const res = await fetch(PIPEFY_API, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${(process.env.PIPEFY_TOKEN || "").trim()}`,
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

  const phases = json.data?.pipe?.phases;
  if (!Array.isArray(phases)) throw new Error("Resposta inesperada do Pipefy");

  const approvedPhase = phases.find(p => p.name.toLowerCase().includes("aprovad"));
  if (!approvedPhase) throw new Error('Fase "Aprovado" não encontrada no Pipe');

  return approvedPhase.cards.edges.map(e => ({
    pipefyId: String(e.node.id),
    title:    e.node.title || "Sem título",
    fields:   e.node.fields || [],
    age:      e.node.age ?? null,
    addedAt:  new Date().toISOString(),
  }));
}

// ── Handler ───────────────────────────────────────────────────
module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(200).end();

  try {
    const { action } = req.query;

    // ── GET load ──────────────────────────────────────────────
    if (req.method === "GET" && action === "load") {
      let board = sanitizeBoard(await dbGet(BOARD_KEY));

      let newCount = 0;
      let pipefyError = null;
      try {
        const approved = await fetchApprovedCards();
        for (const c of approved) {
          // Só importa OS que ainda não foram vistas (não estão em syncedIds)
          if (!board.syncedIds.includes(c.pipefyId)) {
            board.cards.unshift({
              ...c,
              phaseId:   board.phases[0].id,
              movedAt:   c.addedAt,
              movedBy:   "Pipefy",
              localOnly: false,
            });
            board.syncedIds.push(c.pipefyId);
            newCount++;
          }
        }
        if (newCount > 0) await dbSet(BOARD_KEY, board);
      } catch (e) {
        pipefyError = e.message;
        console.error("Pipefy sync error:", e.message);
      }

      return res.status(200).json({ ok: true, board, newCount, pipefyError });
    }

    // ── GET reset (acesse /api/board?action=reset no navegador)
    if (req.method === "GET" && action === "reset") {
      const fresh = defaultBoard();
      try {
        const approved = await fetchApprovedCards();
        fresh.syncedIds = approved.map(c => c.pipefyId);
      } catch (e) {
        console.error("Reset fetch error:", e.message);
      }
      await dbSet(BOARD_KEY, fresh);
      return res.status(200).json({ ok: true, cleared: true, markedAsSeen: fresh.syncedIds.length, message: "Board zerado com sucesso!" });
    }

    // ── POST reset ────────────────────────────────────────────
    // Limpa o board E marca todas as OS atuais do Pipefy como "já vistas"
    // Assim o board fica vazio e só OS NOVAS (aprovadas depois disso) vão aparecer
    if (req.method === "POST" && action === "reset") {
      const fresh = defaultBoard();

      try {
        const approved = await fetchApprovedCards();
        // Marca todos os IDs atuais como já vistos — mas NÃO importa para o board
        fresh.syncedIds = approved.map(c => c.pipefyId);
      } catch (e) {
        console.error("Reset fetch error:", e.message);
      }

      await dbSet(BOARD_KEY, fresh);
      return res.status(200).json({ ok: true, message: "Board resetado. Somente novas OS aprovadas aparecerão." });
    }

    // ── POST move ─────────────────────────────────────────────
    if (req.method === "POST" && action === "move") {
      const { pipefyId, phaseId, movedBy } = req.body || {};
      if (!pipefyId || !phaseId)
        return res.status(400).json({ ok: false, error: "pipefyId e phaseId são obrigatórios" });

      const board = sanitizeBoard(await dbGet(BOARD_KEY));
      const card = board.cards.find(c => c.pipefyId === String(pipefyId));
      if (!card)
        return res.status(404).json({ ok: false, error: "OS não encontrada no dashboard" });

      const oldPhase = card.phaseId;
      card.phaseId = phaseId;
      card.movedAt = new Date().toISOString();
      card.movedBy = movedBy || "—";
      await dbSet(BOARD_KEY, board);

      return res.status(200).json({ ok: true, card, oldPhase });
    }

    // ── POST create ───────────────────────────────────────────
    if (req.method === "POST" && action === "create") {
      const { title, phaseId, createdBy } = req.body || {};
      if (!title)
        return res.status(400).json({ ok: false, error: "title é obrigatório" });

      const board = sanitizeBoard(await dbGet(BOARD_KEY));
      const newId = "local-" + Date.now();
      const card = {
        pipefyId:  newId,
        title,
        fields:    [],
        age:       0,
        addedAt:   new Date().toISOString(),
        phaseId:   phaseId || board.phases[0].id,
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
    console.error("Handler error:", err);
    return res.status(500).json({ ok: false, error: err.message });
  }
};
