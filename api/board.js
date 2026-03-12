const PIPEFY_API = "https://api.pipefy.com/graphql";
const PIPE_ID    = "305832912";
const BOARD_KEY  = "reparoeletro_board";   // sem ":" para evitar conflito de encoding

const UPSTASH_URL   = (process.env.UPSTASH_URL   || "").replace(/['"]/g, "").trim();
const UPSTASH_TOKEN = (process.env.UPSTASH_TOKEN || "").replace(/['"]/g, "").trim();

// Usa /pipeline para GET e SET — chave sempre em JSON, nunca na URL
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
    const ok = j[0]?.result === "OK";
    if (!ok) console.error("dbSet NOT OK:", JSON.stringify(j));
    return ok;
  } catch (e) { console.error("dbSet:", e.message); return false; }
}

function defaultBoard() {
  return {
    phases: [
      { id: "aprovado",        name: "Aprovado"           },
      { id: "producao",        name: "Produção"           },
      { id: "cliente_loja",    name: "Cliente Loja"       },
      { id: "urgencia",        name: "Urgência"           },
      { id: "comprar_peca",    name: "Comprar Peça"       },
      { id: "aguardando_peca", name: "Aguardando Peça"    },
      { id: "peca_disponivel", name: "Peça Disponível"    },
      { id: "loja_feito",      name: "Loja Feito"         },
      { id: "delivery_feito",  name: "Delivery Feito"     },
      { id: "aguardando_ret",  name: "Aguardando Retirada"},
    ],
    cards: [],
    syncedIds: [],   // IDs que já estão no dashboard (não reimportar)
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
  const allCards = [];
  let cursor = null;
  let hasNextPage = true;

  while (hasNextPage) {
    const afterClause = cursor ? `, after: "${cursor}"` : "";
    const query = `query {
      pipe(id: "${PIPE_ID}") {
        phases {
          name
          cards(first: 50${afterClause}) {
            pageInfo { hasNextPage endCursor }
            edges {
              node {
                id title
                fields { name value }
                age
                created_at
              }
            }
          }
        }
      }
    }`;

    const res = await fetch(PIPEFY_API, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${(process.env.PIPEFY_TOKEN || "").trim()}`,
      },
      body: JSON.stringify({ query }),
    });
    const json = await res.json();
    if (json.errors) throw new Error(json.errors[0].message);

    const phases = json.data?.pipe?.phases;
    if (!Array.isArray(phases)) throw new Error("Resposta inesperada do Pipefy");
    const approvedPhase = phases.find(p => p.name.toLowerCase().includes("aprovad"));
    if (!approvedPhase) throw new Error('Fase "Aprovado" não encontrada no Pipe');

    const cards = approvedPhase.cards;
    for (const edge of cards.edges) {
      const fields = edge.node.fields || [];
      const nomeField = fields.find(f =>
        f.name.toLowerCase().includes("nome") || f.name.toLowerCase().includes("contato")
      );
      const descField = fields.find(f =>
        f.name.toLowerCase().includes("descri") || f.name.toLowerCase().includes("problema") || f.name.toLowerCase().includes("servi")
      );
      allCards.push({
        pipefyId:      String(edge.node.id),
        title:         edge.node.title || "Sem título",
        nomeContato:   nomeField?.value || null,
        descricao:     descField?.value || null,
        age:           edge.node.age ?? null,
        addedAt:       new Date().toISOString(),
        addedAtPipefy: edge.node.created_at || null,
      });
    }

    hasNextPage = cards.pageInfo?.hasNextPage ?? false;
    cursor      = cards.pageInfo?.endCursor ?? null;
  }

  return allCards;
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
          // Só importa se o ID não está em syncedIds — simples e confiável
          if (board.syncedIds.includes(c.pipefyId)) continue;
          board.cards.unshift({ ...c, phaseId: board.phases[0].id, movedBy: "Pipefy" });
          board.syncedIds.push(c.pipefyId);
          newCount++;
        }
        if (newCount > 0) await dbSet(BOARD_KEY, board);
      } catch (e) { pipefyError = e.message; }

      return res.status(200).json({ ok: true, board, newCount, pipefyError });
    }

    // ── POST reset ────────────────────────────────────────────
    if (action === "reset") {
      const fresh = defaultBoard();
      fresh.cutoffDate = new Date().toISOString(); // só OS aprovadas APÓS esta data entram
      try {
        const approved = await fetchApprovedCards();
        // Marca TODOS os IDs atuais como já vistos (sem importar nenhum)
        fresh.syncedIds = approved.map(c => c.pipefyId);
      } catch (e) { console.error("Reset Pipefy error:", e.message); }

      const saved = await dbSet(BOARD_KEY, fresh);
      const verify = await dbGet(BOARD_KEY);
      const ok = verify && Array.isArray(verify.syncedIds);

      return res.status(200).json({
        ok: saved && ok,
        board: fresh,
        markedAsSeen: fresh.syncedIds.length,
        cutoffDate: fresh.cutoffDate,
      });
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

    // ── POST move-batch (fim do dia) ──────────────────────────
    if (req.method === "POST" && action === "move-batch") {
      const board = sanitizeBoard(await dbGet(BOARD_KEY));
      const FROM  = ["loja_feito", "delivery_feito"];
      const TO    = "aguardando_ret";
      let count   = 0;
      const now   = new Date().toISOString();
      for (const card of board.cards) {
        if (FROM.includes(card.phaseId)) {
          card.phaseId = TO;
          card.movedAt = now;
          card.movedBy = "Sistema";
          count++;
        }
      }
      await dbSet(BOARD_KEY, board);
      return res.status(200).json({ ok: true, moved: count, board });
    }

    // ── POST create ───────────────────────────────────────────
    if (req.method === "POST" && action === "create") {
      const { title, phaseId, createdBy, nomeContato, descricao } = req.body || {};
      if (!title)
        return res.status(400).json({ ok: false, error: "title é obrigatório" });
      const board = sanitizeBoard(await dbGet(BOARD_KEY));
      const newId = "local-" + Date.now();
      board.cards.unshift({
        pipefyId: newId, title, nomeContato: nomeContato || null,
        descricao: descricao || null, age: 0,
        addedAt: new Date().toISOString(),
        phaseId: phaseId || board.phases[0].id,
        movedAt: new Date().toISOString(),
        movedBy: createdBy || "—", localOnly: true,
      });
      board.syncedIds.push(newId);
      await dbSet(BOARD_KEY, board);
      return res.status(200).json({ ok: true });
    }


    // ── GET debug ─────────────────────────────────────────────
    if (action === "debug") {
      const result = {};

      // 1. Testa conexão Upstash
      try {
        const r = await fetch(`${UPSTASH_URL}/pipeline`, {
          method: "POST",
          headers: { Authorization: `Bearer ${UPSTASH_TOKEN}`, "Content-Type": "application/json" },
          body: JSON.stringify([["PING"]]),
        });
        const j = await r.json();
        result.upstash_ping = j[0]?.result;
      } catch(e) { result.upstash_ping = "ERRO: " + e.message; }

      // 2. Lê o board atual do Upstash
      try {
        const board = await dbGet(BOARD_KEY);
        result.board_found = !!board;
        result.board_cards  = board?.cards?.length ?? 0;
        result.board_synced = board?.syncedIds?.length ?? 0;
        result.board_synced_ids_sample = board?.syncedIds?.slice(0, 5) ?? [];
      } catch(e) { result.board_read_error = e.message; }

      // 3. Busca OS aprovadas no Pipefy
      try {
        const approved = await fetchApprovedCards();
        result.pipefy_approved_count = approved.length;
        result.pipefy_approved_sample = approved.slice(0, 3).map(c => ({ id: c.pipefyId, title: c.title }));
      } catch(e) { result.pipefy_error = e.message; }

      // 4. Variáveis de ambiente presentes?
      result.env_pipefy_token_set  = !!(process.env.PIPEFY_TOKEN);
      result.env_upstash_url_set   = !!UPSTASH_URL;
      result.env_upstash_token_set = !!UPSTASH_TOKEN;
      result.board_key = BOARD_KEY;

      return res.status(200).json(result);
    }

    return res.status(404).json({ ok: false, error: "Ação não encontrada" });

  } catch (err) {
    console.error("Handler error:", err);
    return res.status(500).json({ ok: false, error: err.message });
  }
};
