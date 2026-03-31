// api/tv-board.js — Board do painel TV (independente do Reparo Eletro)
const PIPE_ID   = "306904889";
const BOARD_KEY = "tv_board";
const LOGS_KEY  = "tv_logs";
const UPSTASH_URL   = (process.env.UPSTASH_URL   || "").replace(/['"]/g,"").trim();
const UPSTASH_TOKEN = (process.env.UPSTASH_TOKEN || "").replace(/['"]/g,"").trim();
const PIPEFY_API    = "https://api.pipefy.com/graphql";

async function dbGet(key) {
  try {
    const r = await fetch(`${UPSTASH_URL}/pipeline`, {
      method: "POST",
      headers: { Authorization: `Bearer ${UPSTASH_TOKEN}`, "Content-Type": "application/json" },
      body: JSON.stringify([["GET", key]]),
    });
    const j = await r.json();
    return j[0]?.result ? JSON.parse(j[0].result) : null;
  } catch(e) { return null; }
}

async function dbSet(key, val) {
  try {
    await fetch(`${UPSTASH_URL}/pipeline`, {
      method: "POST",
      headers: { Authorization: `Bearer ${UPSTASH_TOKEN}`, "Content-Type": "application/json" },
      body: JSON.stringify([["SET", key, JSON.stringify(val)]]),
    });
    return true;
  } catch(e) { return false; }
}

async function pipefyQuery(query) {
  const r = await fetch(PIPEFY_API, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${(process.env.PIPEFY_TOKEN || "").trim()}`,
    },
    body: JSON.stringify({ query }),
  });
  const j = await r.json();
  if (j.errors) {
    const msg = Array.isArray(j.errors) ? j.errors.map(e => e.message).join("; ") : String(j.errors);
    throw new Error(msg);
  }
  return j.data;
}

function defaultBoard() {
  return {
    phases: [
      { id: "aprovado",        name: "Aprovado"           },
      { id: "producao",        name: "Produção"           },
      { id: "urgencia",        name: "Urgência"           },
      { id: "comprar_peca",    name: "Comprar Peça"       },
      { id: "aguardando_peca", name: "Aguardando Peça"    },
      { id: "peca_disponivel", name: "Peça Disponível"    },
      { id: "loja_feito",      name: "Loja Feito"         },
      { id: "delivery_feito",  name: "Delivery Feito"     },
      { id: "aguardando_ret",  name: "Aguardando Retirada"},
    ],
    cards:     [],
    syncedIds: [],
    movesLog:  [],
    metaLog:   [],
  };
}

function sanitizeBoard(b) {
  if (!b) return defaultBoard();
  if (!Array.isArray(b.phases))    b.phases    = defaultBoard().phases;
  if (!Array.isArray(b.cards))     b.cards     = [];
  if (!Array.isArray(b.syncedIds)) b.syncedIds = [];
  if (!Array.isArray(b.movesLog))  b.movesLog  = [];
  if (!Array.isArray(b.metaLog))   b.metaLog   = [];
  return b;
}

function trimLog(arr, max = 500) {
  return arr.length > max ? arr.slice(-max) : arr;
}

async function saveLogs(board) {
  await dbSet(LOGS_KEY, { movesLog: board.movesLog || [], metaLog: board.metaLog || [] });
}

// Busca cards aprovados do Pipefy
async function fetchApprovedCards() {
  const data = await pipefyQuery(`query {
    pipe(id: "${PIPE_ID}") {
      phases {
        name
        cards(first: 50) {
          edges {
            node {
              id title
              fields { name value }
            }
          }
        }
      }
    }
  }`);
  const phases = data?.pipe?.phases || [];
  const cards = [];
  for (const ph of phases) {
    const l = ph.name.toLowerCase();
    if (l.includes("aprovado") || l.includes("recebido") || l.includes("produção") || l.includes("producao")) {
      for (const { node } of (ph.cards?.edges || [])) {
        const fields  = node.fields || [];
        const nomeF   = fields.find(f => f.name.toLowerCase().includes("nome"));
        const telF    = fields.find(f => f.name.toLowerCase().includes("telefone") || f.name.toLowerCase().includes("fone"));
        const endF    = fields.find(f => f.name.toLowerCase().includes("endere"));
        const descF   = fields.find(f => f.name.toLowerCase().includes("descri"));
        const title   = node.title || "";
        const m       = title.match(/^(.*?)\s+(\d{3,6})$/);
        cards.push({
          pipefyId:    String(node.id),
          osCode:      m ? m[2] : null,
          nomeContato: nomeF?.value?.trim() || (m ? m[1].trim() : title),
          telefone:    telF?.value || null,
          endereco:    endF?.value || null,
          descricao:   descF?.value || null,
          title,
        });
      }
    }
  }
  return cards;
}

// Busca IDs de cards em fases de conclusão
async function fetchDoneIds() {
  try {
    const data = await pipefyQuery(`query {
      pipe(id: "${PIPE_ID}") {
        phases {
          name
          cards(first: 50) { edges { node { id } } }
        }
      }
    }`);
    const ids = [];
    for (const ph of (data?.pipe?.phases || [])) {
      const l = ph.name.toLowerCase();
      if (l.includes("erp") || l.includes("finaliz") || l.includes("conclu") ||
          l.includes("descar") || l.includes("reprov")) {
        ph.cards.edges.forEach(e => ids.push(String(e.node.id)));
      }
    }
    return ids;
  } catch(e) { return []; }
}

// Busca fase atual de um card
async function fetchCardPhase(pipefyId) {
  try {
    const data = await pipefyQuery(`query { card(id: "${pipefyId}") { current_phase { name } } }`);
    if (!data?.card) return "NOT_FOUND";
    return data.card.current_phase?.name || "NOT_FOUND";
  } catch(e) { return "NOT_FOUND"; }
}

// Move card no Pipefy
async function moveCardPipefy(cardId, phaseId) {
  const data = await pipefyQuery(`mutation {
    moveCardToPhase(input: { card_id: ${cardId}, destination_phase_id: ${phaseId} }) {
      card { id current_phase { id name } }
    }
  }`);
  return data?.moveCardToPhase?.card;
}

module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(200).end();

  const action = req.query.action || "";

  try {
    // ── GET load ──────────────────────────────────────────────
    if (req.method === "GET" && action === "load") {
      const board = sanitizeBoard(await dbGet(BOARD_KEY));
      return res.status(200).json({ ok: true, board, newCount: 0 });
    }

    // ── GET load-logs ─────────────────────────────────────────
    if (req.method === "GET" && action === "load-logs") {
      const logs = await dbGet(LOGS_KEY);
      if (logs) return res.status(200).json({ ok: true, movesLog: logs.movesLog || [], metaLog: logs.metaLog || [] });
      const board = sanitizeBoard(await dbGet(BOARD_KEY));
      return res.status(200).json({ ok: true, movesLog: board.movesLog || [], metaLog: board.metaLog || [] });
    }

    // ── GET sync — importa novos cards do Pipefy ──────────────
    if (req.method === "GET" && action === "sync") {
      let board = sanitizeBoard(await dbGet(BOARD_KEY));
      let newCount = 0, erpRemoved = 0;

      try {
        const approved  = await fetchApprovedCards();
        const activeIds = new Set(board.cards.map(c => c.pipefyId));
        const syncedSet = new Set(board.syncedIds);
        for (const c of approved) {
          if (activeIds.has(c.pipefyId) || syncedSet.has(c.pipefyId)) continue;
          board.cards.unshift({ ...c, phaseId: board.phases[0].id, movedBy: "Pipefy", movedAt: new Date().toISOString() });
          activeIds.add(c.pipefyId);
          if (!board.syncedIds.includes(c.pipefyId)) board.syncedIds.push(c.pipefyId);
          newCount++;
        }
        board.movesLog = trimLog(board.movesLog);
        if (newCount > 0) await dbSet(BOARD_KEY, board);
        await saveLogs(board);
      } catch(e) { console.error("TV sync:", e.message); }

      // Remove cards em ERP/Finalizado
      try {
        const doneIds = await fetchDoneIds();
        if (doneIds.length) {
          const before = board.cards.length;
          board.cards = board.cards.filter(c => !doneIds.includes(c.pipefyId));
          erpRemoved  = before - board.cards.length;
          if (erpRemoved > 0) { await dbSet(BOARD_KEY, board); await saveLogs(board); }
        }
      } catch(e) { console.error("TV done check:", e.message); }

      // Tracking metaLog: ERP e Aguardando Aprovação
      try {
        const data = await pipefyQuery(`query {
          pipe(id: "${PIPE_ID}") {
            phases {
              name
              cards(first: 50) { edges { node { id } } }
            }
          }
        }`);
        const phases = data?.pipe?.phases || [];
        if (!Array.isArray(board.metaLog)) board.metaLog = [];
        const seenErp = new Set(board.metaLog.filter(m => m.phaseId === "erp_entrada").map(m => m.pipefyId));
        const seenAg  = new Set(board.metaLog.filter(m => m.phaseId === "aguardando_aprovacao").map(m => m.pipefyId));
        const seenCol = new Set(board.metaLog.filter(m => m.phaseId === "coleta_solicitada").map(m => m.pipefyId));
        let metaChanged = false;
        for (const ph of phases) {
          const l = ph.name.toLowerCase();
          for (const { node } of (ph.cards?.edges || [])) {
            const id = String(node.id);
            if (l.includes("erp") && !seenErp.has(id)) {
              board.metaLog.push({ phaseId: "erp_entrada", pipefyId: id, timestamp: new Date().toISOString() });
              metaChanged = true;
            }
            if ((l.includes("aguardando") && l.includes("aprov")) && !seenAg.has(id)) {
              board.metaLog.push({ phaseId: "aguardando_aprovacao", pipefyId: id, timestamp: new Date().toISOString() });
              metaChanged = true;
            }
            if (l.includes("coleta solicitada") && !seenCol.has(id)) {
              board.metaLog.push({ phaseId: "coleta_solicitada", pipefyId: id, timestamp: new Date().toISOString() });
              metaChanged = true;
            }
          }
        }
        if (metaChanged) { await dbSet(BOARD_KEY, board); await saveLogs(board); }
      } catch(e) { console.error("TV metaLog:", e.message); }

      return res.status(200).json({ ok: true, board, newCount, erpRemoved });
    }

    // ── POST mover — move card de fase ────────────────────────
    if (req.method === "POST" && action === "mover") {
      const { pipefyId, phaseId, tecnico } = req.body || {};
      if (!pipefyId || !phaseId) return res.status(400).json({ ok: false, error: "pipefyId e phaseId obrigatórios" });
      const board = sanitizeBoard(await dbGet(BOARD_KEY));
      const idx   = board.cards.findIndex(c => c.pipefyId === pipefyId);
      if (idx < 0) return res.status(404).json({ ok: false, error: "Card não encontrado" });
      board.cards[idx].phaseId = phaseId;
      board.cards[idx].movedBy = tecnico || "sistema";
      board.cards[idx].movedAt = new Date().toISOString();
      board.movesLog.push({ phaseId, pipefyId, tecnico: tecnico || "sistema", timestamp: new Date().toISOString() });
      board.movesLog = trimLog(board.movesLog);
      await dbSet(BOARD_KEY, board);
      await saveLogs(board);
      return res.status(200).json({ ok: true, card: board.cards[idx] });
    }

    // ── POST criar — cria card local ──────────────────────────
    if (req.method === "POST" && action === "criar") {
      const { nomeContato, telefone, endereco, descricao } = req.body || {};
      if (!nomeContato) return res.status(400).json({ ok: false, error: "nomeContato obrigatório" });
      const board = sanitizeBoard(await dbGet(BOARD_KEY));
      const newCard = {
        pipefyId:    "local-" + Date.now(),
        osCode:      null,
        nomeContato, telefone: telefone || null,
        endereco:    endereco || null,
        descricao:   descricao || null,
        title:       nomeContato,
        phaseId:     board.phases[0].id,
        movedBy:     "manual",
        movedAt:     new Date().toISOString(),
        localOnly:   true,
      };
      board.cards.unshift(newCard);
      await dbSet(BOARD_KEY, board);
      return res.status(200).json({ ok: true, card: newCard });
    }

    // ── POST deletar ──────────────────────────────────────────
    if (req.method === "POST" && action === "deletar") {
      const { pipefyId } = req.body || {};
      if (!pipefyId) return res.status(400).json({ ok: false, error: "pipefyId obrigatório" });
      const board = sanitizeBoard(await dbGet(BOARD_KEY));
      board.cards = board.cards.filter(c => c.pipefyId !== pipefyId);
      await dbSet(BOARD_KEY, board);
      return res.status(200).json({ ok: true });
    }

    // ── GET metas — conta por fase para o painel de métricas ──
    if (req.method === "GET" && action === "metas") {
      const logs  = await dbGet(LOGS_KEY);
      const mLog  = logs?.metaLog || [];
      const today = new Date().toISOString().slice(0, 10);
      const week  = (() => {
        const d = new Date(); const day = d.getDay();
        d.setDate(d.getDate() - (day === 0 ? 6 : day - 1)); d.setHours(0,0,0,0);
        return d.toISOString().slice(0, 10);
      })();
      const cnt = (phase, from) => mLog.filter(m => m.phaseId === phase && m.timestamp >= from).length;
      return res.status(200).json({
        ok: true,
        hoje: {
          erp:              cnt("erp_entrada",        today),
          orcEnviado:       cnt("aguardando_aprovacao", today),
          coletaSolicitada: cnt("coleta_solicitada",  today),
        },
        semana: {
          erp:              cnt("erp_entrada",        week),
          orcEnviado:       cnt("aguardando_aprovacao", week),
          coletaSolicitada: cnt("coleta_solicitada",  week),
        },
      });
    }

    return res.status(404).json({ ok: false, error: "Ação não encontrada" });

  } catch(e) {
    console.error("tv-board handler:", e.message);
    return res.status(200).json({ ok: false, error: "Erro interno: " + e.message });
  }
};
