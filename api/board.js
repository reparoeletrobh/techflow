const PIPEFY_API = "https://api.pipefy.com/graphql";
const PIPE_ID    = "305832912";
const BOARD_KEY  = "reparoeletro:board";

const UPSTASH_URL   = (process.env.UPSTASH_URL   || "").replace(/['"]/g, "").trim();
const UPSTASH_TOKEN = (process.env.UPSTASH_TOKEN || "").replace(/['"]/g, "").trim();

async function dbGet(key) {
  try {
    const r = await fetch(`${UPSTASH_URL}/pipeline`, {
      method: "POST",
      headers: { Authorization: `Bearer ${UPSTASH_TOKEN}`, "Content-Type": "application/json" },
      body: JSON.stringify([["GET", key]]),
    });
    const j = await r.json();
    const result = j[0]?.result;
    return result ? JSON.parse(result) : null;
  } catch (e) { console.error("dbGet:", e.message); return null; }
}

async function dbSet(key, value) {
  try {
    const r = await fetch(`${UPSTASH_URL}/pipeline`, {
      method: "POST",
      headers: { Authorization: `Bearer ${UPSTASH_TOKEN}`, "Content-Type": "application/json" },
      body: JSON.stringify([["SET", key, JSON.stringify(value)]]),
    });
    const j = await r.json();
    return j[0]?.result === "OK";
  } catch (e) { console.error("dbSet:", e.message); return false; }
}

function defaultBoard() {
  return {
    phases: [
      { id: "aprovado",          name: "Aprovado"           },
      { id: "producao",          name: "Produção"           },
      { id: "cliente_loja",      name: "Cliente Loja"       },
      { id: "urgencia",          name: "Urgência"           },
      { id: "comprar_peca",      name: "Comprar Peça"       },
      { id: "aguardando_peca",   name: "Aguardando Peça"    },
      { id: "peca_disponivel",   name: "Peça Disponível"    },
      { id: "loja_feito",        name: "Loja Feito"         },
      { id: "delivery_feito",    name: "Delivery Feito"     },
      { id: "aguardando_ret",    name: "Aguardando Retirada"},
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

  return approvedPhase.cards.edges.map(e => {
    const fields = e.node.fields || [];
    const nomeField = fields.find(f =>
      f.name.toLowerCase().includes("nome") || f.name.toLowerCase().includes("contato")
    );
    const descField = fields.find(f =>
      f.name.toLowerCase().includes("descri") || f.name.toLowerCase().includes("problema") || f.name.toLowerCase().includes("servi")
    );
    return {
      pipefyId:    String(e.node.id),
      title:       e.node.title || "Sem título",
      nomeContato: nomeField?.value || null,
      descricao:   descField?.value || null,
      age:         e.node.age ?? null,
      addedAt:     new Date().toISOString(),
    };
  });
}

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
      let newCount = 0, pipefyError = null;
      try {
        const approved = await fetchApprovedCards();
        for (const c of approved) {
          if (!board.syncedIds.includes(c.pipefyId)) {
            board.cards.unshift({ ...c, phaseId: board.phases[0].id, movedBy: "Pipefy" });
            board.syncedIds.push(c.pipefyId);
            newCount++;
          }
        }
        if (newCount > 0) await dbSet(BOARD_KEY, board);
      } catch (e) { pipefyError = e.message; }
      return res.status(200).json({ ok: true, board, newCount, pipefyError });
    }

    // ── POST/GET reset ────────────────────────────────────────
    if (action === "reset") {
      const fresh = defaultBoard();
      try {
        const approved = await fetchApprovedCards();
        fresh.syncedIds = approved.map(c => c.pipefyId);
      } catch (e) { console.error("Reset fetch error:", e.message); }
      const saved = await dbSet(BOARD_KEY, fresh);
      const verify = await dbGet(BOARD_KEY);
      const verifyOk = verify && Array.isArray(verify.syncedIds) && verify.syncedIds.length === fresh.syncedIds.length;
      return res.status(200).json({ ok: saved, board: fresh, verified: verifyOk, markedAsSeen: fresh.syncedIds.length });
    }

    // ── POST move ─────────────────────────────────────────────
    if (req.method === "POST" && action === "move") {
      const { pipefyId, phaseId, movedBy } = req.body || {};
      if (!pipefyId || !phaseId)
        return res.status(400).json({ ok: false, error: "pipefyId e phaseId são obrigatórios" });
      const board = sanitizeBoard(await dbGet(BOARD_KEY));
      const card = board.cards.find(c => c.pipefyId === String(pipefyId));
      if (!card)
        return res.status(404).json({ ok: false, error: "OS não encontrada" });
      card.phaseId = phaseId;
      card.movedAt = new Date().toISOString();
      card.movedBy = movedBy || "—";
      await dbSet(BOARD_KEY, board);
      return res.status(200).json({ ok: true, card });
    }

    // ── POST move-batch (fim do dia: Loja Feito + Delivery Feito → Aguardando Retirada) ──
    if (req.method === "POST" && action === "move-batch") {
      const { movedBy } = req.body || {};
      const board = sanitizeBoard(await dbGet(BOARD_KEY));
      const FROM_PHASES = ["loja_feito", "delivery_feito"];
      const TO_PHASE    = "aguardando_ret";
      let count = 0;
      const now = new Date().toISOString();
      for (const card of board.cards) {
        if (FROM_PHASES.includes(card.phaseId)) {
          card.phaseId = TO_PHASE;
          card.movedAt = now;
          card.movedBy = movedBy || "Sistema";
          count++;
        }
      }
      await dbSet(BOARD_KEY, board);
      return res.status(200).json({ ok: true, moved: count, board });
    }

    // ── POST create ───────────────────────────────────────────
    if (req.method === "POST" && action === "create") {
      const { title, phaseId, createdBy } = req.body || {};
      if (!title)
        return res.status(400).json({ ok: false, error: "title é obrigatório" });
      const board = sanitizeBoard(await dbGet(BOARD_KEY));
      const newId = "local-" + Date.now();
      board.cards.unshift({
        pipefyId: newId, title, nomeContato: null, descricao: null,
        age: 0, addedAt: new Date().toISOString(),
        phaseId: phaseId || board.phases[0].id,
        movedAt: new Date().toISOString(), movedBy: createdBy || "—", localOnly: true,
      });
      board.syncedIds.push(newId);
      await dbSet(BOARD_KEY, board);
      return res.status(200).json({ ok: true });
    }

    return res.status(404).json({ ok: false, error: "Ação não encontrada" });
  } catch (err) {
    console.error("Handler error:", err);
    return res.status(500).json({ ok: false, error: err.message });
  }
};
