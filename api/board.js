const PIPEFY_API = "https://api.pipefy.com/graphql";
const PIPE_ID    = "305832912";
const BOARD_KEY  = "techflow:board";

const UPSTASH_URL   = (process.env.UPSTASH_URL   || "").replace(/['"]/g, "").trim();
const UPSTASH_TOKEN = (process.env.UPSTASH_TOKEN || "").replace(/['"]/g, "").trim();

// ── Upstash: usa o formato de pipeline REST (o mais confiável) ─
async function dbGet(key) {
  try {
    const r = await fetch(`${UPSTASH_URL}/pipeline`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${UPSTASH_TOKEN}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify([["GET", key]]),
    });
    const j = await r.json();
    const result = j[0]?.result;
    if (!result) return null;
    return JSON.parse(result);
  } catch (e) {
    console.error("dbGet error:", e.message);
    return null;
  }
}

async function dbSet(key, value) {
  try {
    const r = await fetch(`${UPSTASH_URL}/pipeline`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${UPSTASH_TOKEN}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify([["SET", key, JSON.stringify(value)]]),
    });
    const j = await r.json();
    const ok = j[0]?.result === "OK";
    if (!ok) console.error("dbSet: Upstash não confirmou OK", j);
    return ok;
  } catch (e) {
    console.error("dbSet error:", e.message);
    return false;
  }
}

// ── Board padrão ───────────────────────────────────────────────
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

// ── Handler ────────────────────────────────────────────────────
module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(200).end();

  try {
    const { action } = req.query;

    // ── GET load ───────────────────────────────────────────────
    if (req.method === "GET" && action === "load") {
      let board = sanitizeBoard(await dbGet(BOARD_KEY));

      let newCount = 0;
      let pipefyError = null;
      try {
        const approved = await fetchApprovedCards();
        for (const c of approved) {
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
        if (newCount > 0) {
          const saved = await dbSet(BOARD_KEY, board);
          if (!saved) console.error("load: falha ao salvar board com novos cards");
        }
      } catch (e) {
        pipefyError = e.message;
        console.error("Pipefy sync error:", e.message);
      }

      return res.status(200).json({ ok: true, board, newCount, pipefyError });
    }

    // ── POST reset ─────────────────────────────────────────────
    // Zera o board e marca todas as OS atuais do Pipefy como já vistas
    if ((req.method === "POST" || req.method === "GET") && action === "reset") {
      const fresh = defaultBoard();
      try {
        const approved = await fetchApprovedCards();
        fresh.syncedIds = approved.map(c => c.pipefyId);
      } catch (e) {
        console.error("Reset fetch error:", e.message);
      }

      const saved = await dbSet(BOARD_KEY, fresh);

      // Verifica se salvou mesmo lendo de volta
      const verify = await dbGet(BOARD_KEY);
      const verifyOk = verify && Array.isArray(verify.syncedIds) && verify.syncedIds.length === fresh.syncedIds.length;

      return res.status(200).json({
        ok: saved,
        board: fresh,
        saved,
        verified: verifyOk,
        markedAsSeen: fresh.syncedIds.length,
        message: verifyOk
          ? `Board zerado. ${fresh.syncedIds.length} OS marcadas como já vistas.`
          : "AVISO: reset pode não ter sido salvo corretamente.",
      });
    }

    // ── POST move ──────────────────────────────────────────────
    if (req.method === "POST" && action === "move") {
      const { pipefyId, phaseId, movedBy } = req.body || {};
      if (!pipefyId || !phaseId)
        return res.status(400).json({ ok: false, error: "pipefyId e phaseId são obrigatórios" });

      const board = sanitizeBoard(await dbGet(BOARD_KEY));
      const card = board.cards.find(c => c.pipefyId === String(pipefyId));
      if (!card)
        return res.status(404).json({ ok: false, error: "OS não encontrada no dashboard" });

      card.phaseId = phaseId;
      card.movedAt = new Date().toISOString();
      card.movedBy = movedBy || "—";
      await dbSet(BOARD_KEY, board);

      return res.status(200).json({ ok: true, card });
    }

    // ── POST create ────────────────────────────────────────────
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
