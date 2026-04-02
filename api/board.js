const PIPEFY_API = "https://api.pipefy.com/graphql";
const PIPE_ID    = "305832912";
const BOARD_KEY   = "reparoeletro_board";
const LOGS_KEY    = "reparoeletro_logs";
const BACKUP_KEY  = "reparoeletro_board_backup";

const UPSTASH_URL   = (process.env.UPSTASH_URL   || "").replace(/['"]/g, "").trim();
const UPSTASH_TOKEN = (process.env.UPSTASH_TOKEN || "").replace(/['"]/g, "").trim();

const TECNICOS = ["Lucas", "Diego", "Kassio", "Roberto", "Carlos"];

// ── Upstash ────────────────────────────────────────────────────
async function dbGet(key) {
  try {
    const r = await fetch(`${UPSTASH_URL}/pipeline`, {
      method: "POST",
      headers: { Authorization: `Bearer ${UPSTASH_TOKEN}`, "Content-Type": "application/json" },
      body: JSON.stringify([["GET", key]]),
    });
    const j = await r.json();
    return j[0]?.result ? JSON.parse(j[0].result) : null;
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

// ── Board padrão ───────────────────────────────────────────────
function defaultBoard() {
  return {
    // Fases OS principal
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
    // Fases RS
    rsPhases: [
      { id: "rs_loja",  name: "RS na Loja" },
      { id: "rs_feito", name: "RS Feito"   },
    ],
    // Fases RS Rua
    rsRuaPhases: [
      { id: "rs_rua",       name: "RS Rua"      },
      { id: "rs_rua_feito", name: "RS Rua Feito"},
    ],
    cards:      [],  // OS principais
    rsCards:    [],  // RS
    rsRuaCards: [],  // RS Rua
    syncedIds:  [],  // IDs Pipefy já importados
    movesLog:   [],  // { phaseId, timestamp, tecnico }
  };
}

function sanitizeBoard(b) {
  if (!b || typeof b !== "object") return defaultBoard();
  const def = defaultBoard();
  if (!Array.isArray(b.phases)      || !b.phases.length)      b.phases      = def.phases;
  if (!Array.isArray(b.rsPhases)    || !b.rsPhases.length)    b.rsPhases    = def.rsPhases;
  if (!Array.isArray(b.rsRuaPhases) || !b.rsRuaPhases.length) b.rsRuaPhases = def.rsRuaPhases;
  if (!Array.isArray(b.cards))      b.cards      = [];
  if (!Array.isArray(b.rsCards))    b.rsCards    = [];
  if (!Array.isArray(b.rsRuaCards)) b.rsRuaCards = [];
  if (!Array.isArray(b.syncedIds))  b.syncedIds  = [];
  if (!Array.isArray(b.movesLog))   b.movesLog   = [];
  // Garante phaseId válido
  const validMain   = b.phases.map(p => p.id);
  const validRs     = b.rsPhases.map(p => p.id);
  const validRsRua  = b.rsRuaPhases.map(p => p.id);
  // Só reseta phaseId se for null/undefined — mantém fases desconhecidas para não perder posição
  b.cards      = b.cards.map(c => ({ ...c, phaseId: (c.phaseId && validMain.includes(c.phaseId)) ? c.phaseId : (c.phaseId || b.phases[0].id) }));
  b.rsCards    = b.rsCards.map(c => ({ ...c, phaseId: validRs.includes(c.phaseId)   ? c.phaseId : b.rsPhases[0].id }));
  b.rsRuaCards = b.rsRuaCards.map(c => ({ ...c, phaseId: validRsRua.includes(c.phaseId) ? c.phaseId : b.rsRuaPhases[0].id }));

  // Deduplica por pipefyId — garante que não haja cards repetidos
  const seenIds = new Set();
  b.cards = b.cards.filter(c => {
    if (seenIds.has(c.pipefyId)) return false;
    seenIds.add(c.pipefyId);
    return true;
  });

  return b;
}

// ── Pipefy helpers ─────────────────────────────────────────────
async function pipefyQuery(query, attempt = 1) {
  const TIMEOUT_MS = 15000;
  const MAX_RETRIES = 3;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), TIMEOUT_MS);
  try {
    const res = await fetch(PIPEFY_API, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${(process.env.PIPEFY_TOKEN || "").trim()}`,
      },
      body: JSON.stringify({ query }),
      signal: controller.signal,
    });
    const text = await res.text();
    let json;
    try { json = JSON.parse(text); }
    catch(e) { throw new Error("INVALID_RESPONSE"); }
    if (json.errors) throw new Error(json.errors[0].message);
    return json.data;
  } catch(e) {
    if (attempt < MAX_RETRIES && (e.name === "AbortError" || e.message === "INVALID_RESPONSE")) {
      await new Promise(r => setTimeout(r, 2000 * attempt));
      return pipefyQuery(query, attempt + 1);
    }
    throw e;
  } finally {
    clearTimeout(timer);
  }
}

// Busca OS aprovadas com paginação
async function fetchApprovedCards() {
  const all = [];
  let cursor = null, hasNext = true;

  // Início do dia BRT
  const nowBRT = new Date(new Date().toLocaleString("en-US", { timeZone: "America/Sao_Paulo" }));
  nowBRT.setHours(0, 0, 0, 0);
  const todayStartUTC = new Date(nowBRT.getTime() + 3 * 60 * 60 * 1000);

  while (hasNext) {
    const after = cursor ? `, after: "${cursor}"` : "";
    const data = await pipefyQuery(`query {
      pipe(id: "${PIPE_ID}") {
        phases {
          name
          cards(first: 50${after}) {
            pageInfo { hasNextPage endCursor }
            edges {
              node {
                id title age
                updated_at
                fields { name value }
              }
            }
          }
        }
      }
    }`);

    const phases = data?.pipe?.phases;
    if (!Array.isArray(phases)) throw new Error("Resposta inesperada do Pipefy");
    const phase = phases.find(p => p.name.toLowerCase().includes("aprovad"));
    if (!phase) throw new Error('Fase "Aprovado" não encontrada');

    for (const { node } of phase.cards.edges) {
      // updated_at = quando o card foi modificado pela última vez (inclui mover de fase)
      const updatedAt = node.updated_at ? new Date(node.updated_at) : null;
      const isToday = updatedAt && updatedAt >= todayStartUTC;
      if (!isToday) continue;

      const fields = node.fields || [];
      const nomeField = fields.find(f =>
        f.name.toLowerCase().includes("nome") || f.name.toLowerCase().includes("contato")
      );
      const descField = fields.find(f =>
        f.name.toLowerCase().includes("descri") || f.name.toLowerCase().includes("problema") || f.name.toLowerCase().includes("servi")
      );
      const nomeVal = nomeField?.value || "";
      const digitsMatch = nomeVal.match(/(\d{4})\D*$/);

      all.push({
        pipefyId:    String(node.id),
        title:       node.title || "Sem título",
        nomeContato: nomeVal || null,
        osCode:      digitsMatch ? digitsMatch[1] : null,
        descricao:   descField?.value || null,
        age:         node.age ?? null,
        addedAt:     new Date().toISOString(),
        approvedAt:  updatedAt ? updatedAt.toISOString() : new Date().toISOString(),
      });
    }

    hasNext = phase.cards.pageInfo?.hasNextPage ?? false;
    cursor  = phase.cards.pageInfo?.endCursor ?? null;
  }
  return all;
}

// Busca todas as fases com seus cards em uma única query (até 50 cards por fase)
async function fetchAllPhaseCards() {
  const data = await pipefyQuery(`query {
    pipe(id: "${PIPE_ID}") {
      phases {
        name
        cards(first: 50) {
          pageInfo { hasNextPage endCursor }
          edges { node { id } }
        }
      }
    }
  }`);
  return data?.pipe?.phases || [];
}

// Busca campos (incluindo valor) de um card específico
async function fetchCardFields(pipefyId) {
  try {
    const data = await pipefyQuery(`query { card(id: "${pipefyId}") { id fields { name value } } }`);
    return data?.card?.fields || [];
  } catch(e) { return []; }
}

function extractCardValor(node) {
  const fields = node?.fields || [];
  const f = fields.find(f => f.name.toLowerCase().includes("valor"));
  if (!f?.value) return 0;
  return parseFloat(String(f.value).replace(/[^\d.,]/g,"").replace(",",".")) || 0;
}

// Busca IDs de cards que estão em ERP, Finalizado ou Reprovado no Pipefy
async function fetchErpCardIds() {
  try {
    const phases = await fetchAllPhaseCards();
    const ids = [], targetPhases = [];
    for (const ph of phases) {
      const l = ph.name.toLowerCase();
      if (l.includes("erp") || l.includes("finaliz") || l.includes("conclu") ||
          l.includes("descar") || l.includes("reprov")) {
        targetPhases.push(ph.name);
        ph.cards.edges.forEach(e => ids.push(String(e.node.id)));
        // Paginação se houver mais de 50
        if (ph.cards.pageInfo?.hasNextPage) {
          let cursor = ph.cards.pageInfo.endCursor;
          while (cursor) {
            const data2 = await pipefyQuery(`query {
              pipe(id: "${PIPE_ID}") {
                phases {
                  name
                  cards(first: 50, after: "${cursor}") {
                    pageInfo { hasNextPage endCursor }
                    edges { node { id } }
                  }
                }
              }
            }`);
            const ph2 = (data2?.pipe?.phases || []).find(p => p.name === ph.name);
            if (!ph2) break;
            ph2.cards.edges.forEach(e => ids.push(String(e.node.id)));
            cursor = ph2.cards.pageInfo?.hasNextPage ? ph2.cards.pageInfo.endCursor : null;
          }
        }
      }
    }
    return { ids, targetPhases };
  } catch (e) {
    console.error("fetchErpCardIds:", e.message);
    return { ids: [], targetPhases: [] };
  }
}

// Busca IDs de cards em "Aguardando Aprovação" e "ERP" para tracking de metas
async function fetchMetaPhaseIds() {
  try {
    const phases = await fetchAllPhaseCards();
    const aguardandoIds = [], erpCards = [];
    for (const ph of phases) {
      const l = ph.name.toLowerCase();
      if (l.includes("aguardando") && (l.includes("aprov") || l.includes("aprovação")))
        ph.cards.edges.forEach(e => aguardandoIds.push(String(e.node.id)));
      if (l.includes("erp"))
        ph.cards.edges.forEach(e => erpCards.push({ id: String(e.node.id) }));
    }
    return { aguardandoIds, erpCards, erpIds: erpCards.map(c => c.id) };
  } catch(e) { return { aguardandoIds: [], erpCards: [], erpIds: [] }; }
}

// Consulta a fase atual de um card no Pipefy diretamente
async function fetchCardPhase(pipefyId) {
  try {
    const data = await pipefyQuery(`query {
      card(id: "${pipefyId}") {
        id
        current_phase { name }
      }
    }`);
    // card: null = arquivado ou não existe mais
    if (!data?.card) return "NOT_FOUND";
    return data.card.current_phase?.name || "NOT_FOUND";
  } catch(e) {
    // Qualquer erro de acesso = tratar como finalizado
    return "NOT_FOUND";
  }
}

// Remove do aguardando_ret os cards cujo pipefyId está em fase de conclusão no Pipefy
async function cleanupAguardandoRet(board) {
  const retCards = board.cards.filter(c =>
    c.phaseId === "aguardando_ret" && !c.localOnly && !c.pipefyId.includes("-split-")
  );

  const DONE_PHASES = ["erp","finaliz","conclu","descar","reprova"];
  const isDone = (name) => {
    if (!name || name === "NOT_FOUND") return true;
    const l = name.toLowerCase();
    return DONE_PHASES.some(kw => l.includes(kw));
  };

  const removedIds = new Set();

  // Consulta em paralelo (até 5 por vez para não sobrecarregar)
  const BATCH = 5;
  for (let i = 0; i < retCards.length; i += BATCH) {
    const batch = retCards.slice(i, i + BATCH);
    const results = await Promise.all(batch.map(c => fetchCardPhase(c.pipefyId)));
    batch.forEach((card, idx) => {
      const phase = results[idx];
      if (isDone(phase)) removedIds.add(card.pipefyId);
    });
  }

  // Remove também split cards cujo pai foi removido
  const removedPipefyIds = new Set(removedIds);
  board.cards.forEach(c => {
    if (c.phaseId === "aguardando_ret" && c.splitFrom && removedPipefyIds.has(c.splitFrom)) {
      removedIds.add(c.pipefyId);
    }
  });

  const before = board.cards.length;
  const removedList = [...removedIds];
  board.cards = board.cards.filter(c => !(c.phaseId === "aguardando_ret" && removedIds.has(c.pipefyId)));

  return {
    removed: before - board.cards.length,
    ids: removedList,
    retTotal: retCards.length,
  };
}

// ── Log helpers ────────────────────────────────────────────────
function trimLog(log) {
  // Mantém 90 dias no log completo
  const cutoff = new Date(); cutoff.setDate(cutoff.getDate() - 90);
  return log.filter(m => new Date(m.timestamp) > cutoff);
}

// Salva logs em chave separada (mais leve para o /api/metas ler)
async function saveLogs(board) {
  try {
    // Trim antes de salvar — mantém só 90 dias
    const cutoff90 = new Date(); cutoff90.setDate(cutoff90.getDate() - 90);
    const logs = {
      movesLog: (board.movesLog || []).filter(m => new Date(m.timestamp) > cutoff90),
      metaLog:  (board.metaLog  || []).filter(m => new Date(m.timestamp) > cutoff90),
    };
    await dbSet(LOGS_KEY, logs);
  } catch(e) {}
}

// ── Handler ────────────────────────────────────────────────────
module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(200).end();

  try {
    const { action } = req.query;

    // ── GET load — retorna banco imediatamente, sem chamar Pipefy ──
    if (req.method === "GET" && action === "load") {
      const board = sanitizeBoard(await dbGet(BOARD_KEY));
      // Retorna apenas o essencial para renderizar — SEM logs pesados
      // Logs são buscados separadamente por /api/metas
      const boardLeve = {
        phases:      board.phases,
        rsPhases:    board.rsPhases,
        rsRuaPhases: board.rsRuaPhases,
        cards:       board.cards,
        rsCards:     board.rsCards,
        rsRuaCards:  board.rsRuaCards,
        syncedIds:   board.syncedIds,
        movesLog:    [],
        metaLog:     [],
      };
      return res.status(200).json({ ok: true, board: boardLeve, newCount: 0, pipefyError: null });
    }

    // ── GET load-logs — retorna apenas os logs (para o painel Metas)
    if (req.method === "GET" && action === "load-logs") {
      const logs = await dbGet(LOGS_KEY);
      if (logs) return res.status(200).json({ ok: true, movesLog: logs.movesLog || [], metaLog: logs.metaLog || [] });
      // Fallback: lê do board
      const board = sanitizeBoard(await dbGet(BOARD_KEY));
      await saveLogs(board); // popula LOGS_KEY para próximas chamadas
      return res.status(200).json({ ok: true, movesLog: board.movesLog || [], metaLog: board.metaLog || [] });
    }

    // ── GET sync — chama Pipefy e atualiza banco (chamada separada) ──
    if (req.method === "GET" && action === "sync") {
      let board = sanitizeBoard(await dbGet(BOARD_KEY));
      let newCount = 0, pipefyError = null, erpRemoved = 0;

      try {
        const approved = await fetchApprovedCards();
        const activeIds  = new Set(board.cards.map(c => c.pipefyId));
        const syncedSet  = new Set(board.syncedIds);
        for (const c of approved) {
          // Nunca re-importa card já conhecido — mesmo que saiu de board.cards por qualquer motivo
          if (activeIds.has(c.pipefyId) || syncedSet.has(c.pipefyId)) continue;
          board.cards.unshift({ ...c, phaseId: board.phases[0].id, movedBy: "Pipefy" });
          activeIds.add(c.pipefyId);
          if (!board.syncedIds.includes(c.pipefyId)) {
            board.syncedIds.push(c.pipefyId);
            board.movesLog.push({ phaseId: "aprovado_entrada", timestamp: new Date().toISOString() });
          }
          newCount++;
        }
        board.movesLog = trimLog(board.movesLog);
        if (newCount > 0) {
          await dbSet(BOARD_KEY, board);
          try { await dbSet(BACKUP_KEY, { ...board, backedUpAt: new Date().toISOString() }); } catch(e) {}
        }
      await saveLogs(board);
      } catch (e) { pipefyError = e.message; }

      try {
        // Remove qualquer card que está em ERP, Finalizado ou Reprovado no Pipefy
        const { ids: erpIds } = await fetchErpCardIds();
        if (erpIds.length > 0) {
          const before = board.cards.length;

          // Antes de remover: registra no metaLog cards que ainda não foram registrados
          if (!Array.isArray(board.metaLog)) board.metaLog = [];
          const seenErpSet = new Set(board.metaLog.filter(m=>m.phaseId==="erp_entrada").map(m=>m.pipefyId));
          
          // Busca phases_history e valor em lotes de 10 para eficiência
          const newErpIds = erpIds.filter(id => !seenErpSet.has(id));
          for (let i = 0; i < newErpIds.length; i += 10) {
            const lote = newErpIds.slice(i, i + 10);
            const aliases = lote.map((id, j) => `c${j}: card(id: "${id}") { id fields { name value } phases_history { phase { name } firstTimeIn } }`).join(" ");
            try {
              const loteData = await pipefyQuery(`query { ${aliases} }`);
              for (let j = 0; j < lote.length; j++) {
                const id = lote[j];
                const card = loteData[`c${j}`];
                if (!card) continue;
                // Valor do contrato
                const fields = card.fields || [];
                const vf = fields.find(f => f.name.toLowerCase().includes("valor"));
                const valor = vf?.value ? parseFloat(String(vf.value).replace(/[^\d.,]/g,"").replace(",",".")) || 0 : 0;
                // Timestamp real de entrada em ERP via phases_history
                const hist = (card.phases_history || []).find(h =>
                  h.phase?.name?.toLowerCase().includes("erp") && h.firstTimeIn
                );
                const erpTs = hist?.firstTimeIn || new Date().toISOString();
                board.metaLog.push({ phaseId: "erp_entrada", pipefyId: id, valor, timestamp: erpTs });
                seenErpSet.add(id);
              }
            } catch(e) {
              // Fallback: log com timestamp atual
              for (const id of lote) {
                if (!seenErpSet.has(id)) {
                  board.metaLog.push({ phaseId: "erp_entrada", pipefyId: id, valor: 0, timestamp: new Date().toISOString() });
                  seenErpSet.add(id);
                }
              }
            }
          }

          board.cards       = board.cards.filter(c => !erpIds.includes(c.pipefyId));
          board.rsCards     = (board.rsCards     || []).filter(c => !erpIds.includes(c.pipefyId));
          board.rsRuaCards  = (board.rsRuaCards  || []).filter(c => !erpIds.includes(c.pipefyId));
          erpRemoved = before - board.cards.length;
          if (erpRemoved > 0) await dbSet(BOARD_KEY, board);
          await saveLogs(board);
        }
      } catch (e) { console.error("ERP/Reprovado check:", e.message); }

      // Tracking de metas: Aguardando Aprovação e ERP
      try {
        const { aguardandoIds, erpCards } = await fetchMetaPhaseIds();
        let metaChanged = false;
        if (!Array.isArray(board.metaLog)) board.metaLog = [];

        const seenAg     = new Set(board.metaLog.filter(m=>m.phaseId==="aguardando_aprovacao").map(m=>m.pipefyId));
        const seenErp    = new Set(board.metaLog.filter(m=>m.phaseId==="erp_entrada").map(m=>m.pipefyId));
        const seenColeta = new Set(board.metaLog.filter(m=>m.phaseId==="coleta_solicitada").map(m=>m.pipefyId));

        for (const id of aguardandoIds) {
          if (!seenAg.has(id)) {
            board.metaLog.push({ phaseId: "aguardando_aprovacao", pipefyId: id, timestamp: new Date().toISOString() });
            metaChanged = true;
          }
        }
        for (const { id } of erpCards) {
          if (!seenErp.has(id)) {
            // Busca valor E timestamp real de entrada em ERP via phases_history
            let valor = 0;
            let erpTimestamp = new Date().toISOString();
            try {
              const cardData = await pipefyQuery(`query {
                card(id: "${id}") {
                  fields { name value }
                  phases_history { phase { name } firstTimeIn lastTimeOut }
                }
              }`);
              // Valor do contrato
              const fields = cardData?.card?.fields || [];
              const vf = fields.find(f => f.name.toLowerCase().includes("valor"));
              if (vf?.value) valor = parseFloat(String(vf.value).replace(/[^\d.,]/g,"").replace(",",".")) || 0;
              // Timestamp real de entrada em ERP
              const hist = (cardData?.card?.phases_history || []).find(h =>
                h.phase?.name?.toLowerCase().includes("erp")
              );
              if (hist?.firstTimeIn) erpTimestamp = hist.firstTimeIn;
            } catch(e) { console.error("ERP card fetch:", e.message); }
            board.metaLog.push({ phaseId: "erp_entrada", pipefyId: id, valor, timestamp: erpTimestamp });
            metaChanged = true;
          }
        }
        // Tracking coleta_solicitada via Pipefy phases
        try {
          const allPhases = await fetchAllPhaseCards();
          const coletaIds = [];
          for (const ph of allPhases) {
            if (ph.name.toLowerCase().trim() === "coleta solicitada")
              ph.cards.edges.forEach(e => coletaIds.push(String(e.node.id)));
          }
          for (const id of coletaIds) {
            if (!seenColeta.has(id)) {
              board.metaLog.push({ phaseId: "coleta_solicitada", pipefyId: id, timestamp: new Date().toISOString() });
              metaChanged = true;
            }
          }
        } catch(e) { console.error("coleta_solicitada tracking:", e.message); }
        // Trim metaLog to 180 days
        const cutoff = new Date(); cutoff.setDate(cutoff.getDate() - 180);
        board.metaLog = board.metaLog.filter(m => new Date(m.timestamp) > cutoff);
        if (metaChanged) await dbSet(BOARD_KEY, board);
      await saveLogs(board);
      } catch(e) { console.error("meta tracking:", e.message); }

      return res.status(200).json({ ok: true, board, newCount, erpRemoved, pipefyError });
    }

    // ── POST reset ─────────────────────────────────────────────
    if (action === "reset") {
      const fresh = defaultBoard();
      try {
        const approved = await fetchApprovedCards();
        fresh.syncedIds = approved.map(c => c.pipefyId);
      } catch (e) { console.error("Reset:", e.message); }
      const saved = await dbSet(BOARD_KEY, fresh);
      // Backup automático
      try { await dbSet(BACKUP_KEY, { ...fresh, backedUpAt: new Date().toISOString() }); } catch(e) {}
      return res.status(200).json({ ok: saved, board: fresh, markedAsSeen: fresh.syncedIds.length });
    }

    // ── POST move (OS principal) ───────────────────────────────
    if (req.method === "POST" && action === "move") {
      const { pipefyId, phaseId, movedBy, tecnico, fotosCompra, descricaoCompra } = req.body || {};
      if (!pipefyId || !phaseId) return res.status(400).json({ ok: false, error: "pipefyId e phaseId são obrigatórios" });
      const board = sanitizeBoard(await dbGet(BOARD_KEY));
      const card = board.cards.find(c => c.pipefyId === String(pipefyId));
      if (!card) return res.status(404).json({ ok: false, error: "OS não encontrada" });
      card.phaseId = phaseId; card.movedAt = new Date().toISOString();
      card.movedBy = movedBy || "—"; card.tecnico = tecnico || null;
      // Salva fotos e descrição quando move para comprar_peca
      if (phaseId === "comprar_peca") {
        if (fotosCompra)    card.fotosCompra    = fotosCompra;
        if (descricaoCompra) card.descricaoCompra = descricaoCompra;
      }
      if (["loja_feito", "delivery_feito", "cliente_loja"].includes(phaseId)) {
        board.movesLog.push({ phaseId, timestamp: card.movedAt, tecnico: tecnico || null, pipefyId: String(pipefyId) });
        board.movesLog = trimLog(board.movesLog);
      }
      await dbSet(BOARD_KEY, board);
      await saveLogs(board);

      // Auto-adiciona à fila do Lalamove quando move para coleta/entrega solicitada
      if (["coleta_solicitada", "entrega_solicitada"].includes(phaseId)) {
        const LALA_KEY = "reparoeletro_lalamove";
        try {
          const lalaDb = await dbGet(LALA_KEY) || { fichas: [] };
          if (!Array.isArray(lalaDb.fichas)) lalaDb.fichas = [];
          const tipo = phaseId === "coleta_solicitada" ? "coleta" : "entrega";
          if (!Array.isArray(lalaDb.removedIds)) lalaDb.removedIds = [];
          const jaExiste = lalaDb.fichas.find(f => f.pipefyId === String(pipefyId) && f.tipo === tipo);
          const jaRemovida = lalaDb.removedIds.includes(String(pipefyId) + ":" + tipo);
          if (!jaExiste && !jaRemovida) {
            lalaDb.fichas.push({
              pipefyId:    String(pipefyId),
              tipo,
              osCode:      card.osCode      || null,
              nomeContato: card.nomeContato || card.title || null,
              descricao:   card.descricao   || null,
              endereco:    null, // será preenchido na tela do Lalamove ou buscado do Pipefy
              addedAt:     new Date().toISOString(),
              status:      "pendente",
            });
            await dbSet(LALA_KEY, lalaDb);
          }
        } catch(e) { console.error("lalamove queue:", e.message); }
      }

      return res.status(200).json({ ok: true, card });
    }

    // ── POST move-rs ───────────────────────────────────────────
    if (req.method === "POST" && action === "move-rs") {
      const { cardId, phaseId, boardType } = req.body || {};
      if (!cardId || !phaseId || !boardType) return res.status(400).json({ ok: false, error: "Campos obrigatórios" });
      const board = sanitizeBoard(await dbGet(BOARD_KEY));
      const arr = boardType === "rs" ? board.rsCards : board.rsRuaCards;
      const card = arr.find(c => c.id === cardId);
      if (!card) return res.status(404).json({ ok: false, error: "RS não encontrado" });
      const prevPhase = card.phaseId;
      card.phaseId = phaseId; card.movedAt = new Date().toISOString();
      // Log quando vai para feito
      const feitoPhase = boardType === "rs" ? "rs_feito" : "rs_rua_feito";
      if (phaseId === feitoPhase) {
        board.movesLog.push({ phaseId: boardType === "rs" ? "rs_feito" : "rs_rua_feito", timestamp: card.movedAt });
        board.movesLog = trimLog(board.movesLog);
      }
      await dbSet(BOARD_KEY, board);
      await saveLogs(board);
      return res.status(200).json({ ok: true, card, prevPhase });
    }

    // ── POST move-batch (fim do dia) ───────────────────────────
    if (req.method === "POST" && action === "move-batch") {
      const board = sanitizeBoard(await dbGet(BOARD_KEY));
      const FROM = ["loja_feito", "delivery_feito"], TO = "aguardando_ret";
      let count = 0; const now = new Date().toISOString();
      for (const card of board.cards) {
        if (FROM.includes(card.phaseId)) { card.phaseId = TO; card.movedAt = now; card.movedBy = "Sistema"; count++; }
      }
      await dbSet(BOARD_KEY, board);
      await saveLogs(board);
      return res.status(200).json({ ok: true, moved: count, board });
    }

    // ── POST create ────────────────────────────────────────────
    if (req.method === "POST" && action === "create") {
      const { codigo, nome, descricao, boardType, phaseId } = req.body || {};
      if (!nome && !codigo) return res.status(400).json({ ok: false, error: "Código ou nome obrigatório" });
      const board = sanitizeBoard(await dbGet(BOARD_KEY));
      const newId = "local-" + Date.now();
      const card = {
        id: newId, pipefyId: newId,
        osCode: codigo || null, nomeContato: nome || null,
        title: (codigo ? "#" + codigo + " " : "") + (nome || ""),
        descricao: descricao || null,
        age: 0, addedAt: new Date().toISOString(),
        movedAt: new Date().toISOString(), movedBy: "Manual", localOnly: true,
      };

      if (boardType === "rs") {
        card.phaseId = phaseId || board.rsPhases[0].id;
        board.rsCards.unshift(card);
        board.movesLog.push({ phaseId: "rs_criado", timestamp: card.addedAt });
      } else if (boardType === "rs_rua") {
        card.phaseId = phaseId || board.rsRuaPhases[0].id;
        board.rsRuaCards.unshift(card);
        board.movesLog.push({ phaseId: "rs_rua_criado", timestamp: card.addedAt });
      } else {
        card.phaseId = phaseId || board.phases[0].id;
        board.cards.unshift(card);
        board.syncedIds.push(newId);
      }
      board.movesLog = trimLog(board.movesLog);
      await dbSet(BOARD_KEY, board);
      await saveLogs(board);
      return res.status(200).json({ ok: true, card });
    }

    // ── POST delete-rs ─────────────────────────────────────────
    if (req.method === "POST" && action === "delete-rs") {
      const { cardId, boardType } = req.body || {};
      const board = sanitizeBoard(await dbGet(BOARD_KEY));
      if (boardType === "rs")     board.rsCards    = board.rsCards.filter(c => c.id !== cardId);
      if (boardType === "rs_rua") board.rsRuaCards = board.rsRuaCards.filter(c => c.id !== cardId);
      await dbSet(BOARD_KEY, board);
      await saveLogs(board);
      return res.status(200).json({ ok: true });
    }

    // ── GET goals ──────────────────────────────────────────────
    if (req.method === "GET" && action === "goals") {
      const board = sanitizeBoard(await dbGet(BOARD_KEY));
      const log = board.movesLog;

      function toBRT(d) { return new Date(new Date(d).toLocaleString("en-US", { timeZone: "America/Sao_Paulo" })); }
      function startOfDayUTC(d) { const b = toBRT(d); b.setHours(0,0,0,0); return new Date(b.getTime() + 3*60*60*1000); }
      function startOfWeekUTC(d) {
        const b = toBRT(d); const day = b.getDay();
        b.setDate(b.getDate() + (day === 0 ? -6 : 1 - day)); b.setHours(0,0,0,0);
        return new Date(b.getTime() + 3*60*60*1000);
      }
      function startOfMonthUTC(d) { const b = toBRT(d); b.setDate(1); b.setHours(0,0,0,0); return new Date(b.getTime() + 3*60*60*1000); }

      const now = new Date();
      const todayUTC    = startOfDayUTC(now);
      const weekUTC     = startOfWeekUTC(now);
      const monthUTC    = startOfMonthUTC(now);
      const prevWeekEnd = new Date(weekUTC.getTime() - 1);
      const prevWeekStart = new Date(weekUTC.getTime() - 7*24*60*60*1000);

      // Conta entradas únicas por card (pipefyId) — evita contar re-movimentações
      const cnt = (phaseId, since, until) => {
        const entries = log.filter(m =>
          m.phaseId === phaseId &&
          new Date(m.timestamp) >= since &&
          (!until || new Date(m.timestamp) <= until)
        );
        // Se tem pipefyId, deduplica — conta só a última entrada por card
        const withId    = entries.filter(m => m.pipefyId);
        const withoutId = entries.filter(m => !m.pipefyId);
        const uniqueIds = new Set(withId.map(m => m.pipefyId));
        return uniqueIds.size + withoutId.length;
      };

      const cntByTecnico = (phaseId, since) => {
        const map = {};
        TECNICOS.forEach(t => map[t] = 0);
        const entries = log.filter(m => m.phaseId === phaseId && new Date(m.timestamp) >= since && m.tecnico);
        // Deduplica por pipefyId — pega a entrada mais recente por card
        const latest = new Map();
        for (const m of entries) {
          if (m.pipefyId) {
            const prev = latest.get(m.pipefyId);
            if (!prev || new Date(m.timestamp) > new Date(prev.timestamp)) latest.set(m.pipefyId, m);
          } else {
            // sem pipefyId: conta normalmente
            const key = "noid_" + m.timestamp;
            latest.set(key, m);
          }
        }
        for (const m of latest.values()) {
          if (map[m.tecnico] !== undefined) map[m.tecnico]++;
          else map[m.tecnico] = 1;
        }
        return map;
      };

      // Histórico mensal (últimos 6 meses)
      const monthHistory = [];
      const monthNames = ["Jan","Fev","Mar","Abr","Mai","Jun","Jul","Ago","Set","Out","Nov","Dez"];
      for (let i = 5; i >= 0; i--) {
        const d = new Date(now); d.setMonth(d.getMonth() - i);
        const ms = startOfMonthUTC(d);
        const me = new Date(ms); me.setMonth(me.getMonth() + 1); me.setTime(me.getTime() - 1);
        const brt = toBRT(ms);
        monthHistory.push({
          label: monthNames[brt.getMonth()] + "/" + String(brt.getFullYear()).slice(2),
          rs:     cnt("rs_feito",     ms, me),
          rsRua:  cnt("rs_rua_feito", ms, me),
          loja:   cnt("loja_feito",   ms, me),
          delivery: cnt("delivery_feito", ms, me),
        });
      }

      const nowBRT = toBRT(now);
      const days = ["Domingo","Segunda","Terça","Quarta","Quinta","Sexta","Sábado"];
      const fmt = d => { const b = toBRT(d); return `${String(b.getDate()).padStart(2,"0")}/${String(b.getMonth()+1).padStart(2,"0")}`; };
      const weekDates = Array.from({length:6}, (_,i) => fmt(new Date(weekUTC.getTime() + i*24*60*60*1000)));
      const prevWeekDates = Array.from({length:6}, (_,i) => fmt(new Date(prevWeekStart.getTime() + i*24*60*60*1000)));

      // Busca stats de vendas
      let vendasStats = {};
      try {
        const vr = await fetch(`${UPSTASH_URL}/pipeline`, {
          method: "POST",
          headers: { Authorization: `Bearer ${UPSTASH_TOKEN}`, "Content-Type": "application/json" },
          body: JSON.stringify([["GET", "reparoeletro_vendas"]]),
        });
        const vj = await vr.json();
        const produtos = vj[0]?.result ? JSON.parse(vj[0].result).produtos || [] : [];
        function toBRTv(d) { return new Date(new Date(d).toLocaleString("en-US",{timeZone:"America/Sao_Paulo"})); }
        const nBRT = toBRTv(new Date()); nBRT.setHours(0,0,0,0);
        const tUTC = new Date(nBRT.getTime() + 3*60*60*1000);
        const wBRT = toBRTv(new Date()); const wdv = wBRT.getDay();
        wBRT.setDate(wBRT.getDate() + (wdv===0?-6:1-wdv)); wBRT.setHours(0,0,0,0);
        const wUTC = new Date(wBRT.getTime() + 3*60*60*1000);
        vendasStats = {
          cadastradosHoje:   produtos.filter(p => p.createdAt && new Date(p.createdAt) >= tUTC).length,
          cadastradosSemana: produtos.filter(p => p.createdAt && new Date(p.createdAt) >= wUTC).length,
          vendaLojaHoje:     produtos.filter(p => p.soldAt && new Date(p.soldAt) >= tUTC && p.vendedor === "Loja").length,
          vendaLojaSemana:   produtos.filter(p => p.soldAt && new Date(p.soldAt) >= wUTC && p.vendedor === "Loja").length,
          vendaOnlineHoje:   produtos.filter(p => p.soldAt && new Date(p.soldAt) >= tUTC && p.vendedor === "Online").length,
          vendaOnlineSemana: produtos.filter(p => p.soldAt && new Date(p.soldAt) >= wUTC && p.vendedor === "Online").length,
        };
      } catch(e) { console.error("vendas stats:", e.message); }

      return res.status(200).json({
        ok: true,
        todayLabel: `${days[nowBRT.getDay()]}, ${String(nowBRT.getDate()).padStart(2,"0")} ${["Jan","Fev","Mar","Abr","Mai","Jun","Jul","Ago","Set","Out","Nov","Dez"][nowBRT.getMonth()]}`,
        weekLabel: `${weekDates[0]} – ${weekDates[5]}`,
        prevWeekLabel: `${prevWeekDates[0]} – ${prevWeekDates[5]}`,
        today: {
          coletaSolicitada: { count: cnt("coleta_solicitada",    todayUTC), goal: 40 },
          orcEnviado:       { count: cnt("aguardando_aprovacao", todayUTC), goal: 50 },
          aprovadoLoja:     { count: cnt("cliente_loja",         todayUTC), goal: 15 },
          aprovadoTotal:    { count: cnt("aprovado_entrada",     todayUTC), goal: 35 },
          erp:              { count: cnt("erp_entrada",          todayUTC), goal: 35 },
          aprovado: { count: cnt("aprovado_entrada", todayUTC), goal: 35 },
          loja:     { count: cnt("loja_feito",       todayUTC), goal: 15 },
          delivery: { count: cnt("delivery_feito",   todayUTC), goal: 20 },
          rsCriado: cnt("rs_criado",    todayUTC),
          rsFeito:  cnt("rs_feito",     todayUTC),
          rsRuaCriado: cnt("rs_rua_criado",  todayUTC),
          rsRuaFeito:  cnt("rs_rua_feito",   todayUTC),
        },
        week: {
          coletaSolicitada: { count: cnt("coleta_solicitada",    weekUTC), goal: 200 },
          orcEnviado:       { count: cnt("aguardando_aprovacao", weekUTC), goal: 200 },
          aprovadoLoja:     { count: cnt("cliente_loja",         weekUTC), goal: 90  },
          aprovadoTotal:    { count: cnt("aprovado_entrada",     weekUTC), goal: 200 },
          erp:              { count: cnt("erp_entrada",          weekUTC), goal: 200 },
          aprovado: { count: cnt("aprovado_entrada", weekUTC), goal: 210 },
          loja:     { count: cnt("loja_feito",       weekUTC), goal: 90 },
          delivery: { count: cnt("delivery_feito",   weekUTC), goal: 120 },
          rsFeito:  cnt("rs_feito",    weekUTC),
          rsRuaFeito: cnt("rs_rua_feito", weekUTC),
        },
        month: {
          rsFeito:    cnt("rs_feito",    monthUTC),
          rsRuaFeito: cnt("rs_rua_feito", monthUTC),
        },
        prevWeek: {
          aprovado: { count: cnt("aprovado_entrada", prevWeekStart, prevWeekEnd), goal: 210 },
          loja:     { count: cnt("loja_feito",       prevWeekStart, prevWeekEnd), goal: 90 },
          delivery: { count: cnt("delivery_feito",   prevWeekStart, prevWeekEnd), goal: 120 },
        },
        tecnicoHoje: {
          loja:     cntByTecnico("loja_feito",     todayUTC),
          delivery: cntByTecnico("delivery_feito", todayUTC),
        },
        tecnicoSemana: {
          loja:     cntByTecnico("loja_feito",     weekUTC),
          delivery: cntByTecnico("delivery_feito", weekUTC),
        },
        monthHistory,
        vendas: vendasStats,
      });
    }

    // ── GET debug ──────────────────────────────────────────────
    // ── GET card-phases-debug — mostra phases_history de um card específico
    if (action === "card-phases-debug") {
      const { pipefyId } = req.query;
      if (!pipefyId) return res.status(400).json({ ok: false, error: "pipefyId obrigatório" });
      try {
        const data = await pipefyQuery(`query {
          card(id: "${pipefyId}") {
            id title
            current_phase { name id }
            fields { name value }
            phases_history { phase { id name } firstTimeIn lastTimeOut }
          }
        }`);
        const card = data?.card;
        return res.status(200).json({
          ok: true,
          id: card?.id,
          title: card?.title,
          currentPhase: card?.current_phase,
          valor: (card?.fields||[]).find(f=>f.name.toLowerCase().includes("valor"))?.value,
          phasesHistory: card?.phases_history || [],
        });
      } catch(e) { return res.status(200).json({ ok: false, error: e.message }); }
    }

    if (action === "debug") {
      const result = {};

      // Upstash ping
      try {
        const r = await fetch(`${UPSTASH_URL}/pipeline`, {
          method: "POST", headers: { Authorization: `Bearer ${UPSTASH_TOKEN}`, "Content-Type": "application/json" },
          body: JSON.stringify([["PING"]]),
        });
        result.upstash_ping = (await r.json())[0]?.result;
      } catch(e) { result.upstash_ping = "ERRO: " + e.message; }

      // Board state
      try {
        const board = await dbGet(BOARD_KEY);
        result.board_found = !!board;
        result.board_cards = board?.cards?.length ?? 0;
        result.board_rs = board?.rsCards?.length ?? 0;
        result.board_rs_rua = board?.rsRuaCards?.length ?? 0;
        result.board_synced = board?.syncedIds?.length ?? 0;
        result.board_log = board?.movesLog?.length ?? 0;
        result.board_synced_sample = board?.syncedIds?.slice(-5) ?? [];
      } catch(e) { result.board_error = e.message; }

      // Pipefy approved
      try {
        const approved = await fetchApprovedCards();
        result.pipefy_approved_count = approved.length;
        result.pipefy_sample = approved.slice(-3).map(c => ({ id: c.pipefyId, title: c.title }));
      } catch(e) { result.pipefy_error = e.message; }

      // Simulate load: check which cards would be NEW
      try {
        const board = await dbGet(BOARD_KEY);
        const approved = await fetchApprovedCards();
        const newOnes = approved.filter(c => !(board?.syncedIds || []).includes(c.pipefyId));
        result.would_import = newOnes.length;
        result.new_cards_sample = newOnes.slice(0, 5).map(c => ({ id: c.pipefyId, title: c.title }));
      } catch(e) { result.simulate_error = e.message; }

      // Test dbSet
      try {
        const testKey = BOARD_KEY + "_test";
        const setOk = await dbSet(testKey, { test: true, ts: Date.now() });
        const getBack = await dbGet(testKey);
        result.dbset_works = setOk && getBack?.test === true;
      } catch(e) { result.dbset_error = e.message; }

      // Lista todas as fases do Pipefy
      try {
        const data = await pipefyQuery(`query {
          pipe(id: "${PIPE_ID}") {
            phases { name cards(first: 1) { edges { node { id } } } }
          }
        }`);
        result.all_phases = (data?.pipe?.phases || []).map(p => ({
          name: p.name,
          cards: p.cards.edges.length
        }));
      } catch(e) { result.phases_error = e.message; }

      result.env_pipefy = !!process.env.PIPEFY_TOKEN;
      result.env_upstash = !!UPSTASH_URL;
      result.board_key = BOARD_KEY;

      return res.status(200).json(result);
    }

    // ── POST clean-aprovado ───────────────────────────────────
    // Remove apenas cards antigos da fase Aprovado — preserva todas as outras fases
    if (action === "clean-aprovado") {
      const board = sanitizeBoard(await dbGet(BOARD_KEY));

      // Início do dia em BRT
      const nowBRT = new Date(new Date().toLocaleString("en-US", { timeZone: "America/Sao_Paulo" }));
      nowBRT.setHours(0, 0, 0, 0);
      const todayStartUTC = new Date(nowBRT.getTime() + 3 * 60 * 60 * 1000);

      const before = board.cards.length;

      board.cards = board.cards.filter(c => {
        // Mantém cards que NÃO estão em Aprovado
        if (c.phaseId !== "aprovado") return true;
        // Em Aprovado: mantém só os de hoje
        const approvedAt = c.approvedAt ? new Date(c.approvedAt) : null;
        return approvedAt && approvedAt >= todayStartUTC;
      });

      const removed = before - board.cards.length;
      await dbSet(BOARD_KEY, board);
      await saveLogs(board);

      return res.status(200).json({ ok: true, removed, remaining: board.cards.length, board });
    }

        // ── POST fix-log ───────────────────────────────────────────
    // Remove entradas duplicadas do log de hoje (sem pipefyId) e reconstrói
    if (action === "fix-log") { // aceita GET e POST
      const board = sanitizeBoard(await dbGet(BOARD_KEY));

      const nowBRT = new Date(new Date().toLocaleString("en-US", { timeZone: "America/Sao_Paulo" }));
      nowBRT.setHours(0, 0, 0, 0);
      const todayStartUTC = new Date(nowBRT.getTime() + 3 * 60 * 60 * 1000);

      const before = board.movesLog.length;

      // Remove entradas de hoje sem pipefyId para loja_feito e delivery_feito
      board.movesLog = board.movesLog.filter(m => {
        const isToday = new Date(m.timestamp) >= todayStartUTC;
        const isTarget = ["loja_feito", "delivery_feito"].includes(m.phaseId);
        if (isToday && isTarget && !m.pipefyId) return false; // remove
        return true;
      });

      // Reconstrói entradas de hoje a partir do estado atual dos cards
      const now = new Date().toISOString();
      for (const card of board.cards) {
        if (!["loja_feito", "delivery_feito"].includes(card.phaseId)) continue;
        const movedAt = card.movedAt ? new Date(card.movedAt) : null;
        if (!movedAt || movedAt < todayStartUTC) continue;
        // Verifica se já existe entrada com pipefyId para este card hoje
        const alreadyLogged = board.movesLog.some(m =>
          m.pipefyId === card.pipefyId &&
          m.phaseId === card.phaseId &&
          new Date(m.timestamp) >= todayStartUTC
        );
        if (!alreadyLogged) {
          board.movesLog.push({
            phaseId:  card.phaseId,
            timestamp: card.movedAt,
            tecnico:  card.tecnico || null,
            pipefyId: card.pipefyId,
          });
        }
      }

      board.movesLog = trimLog(board.movesLog);
      await dbSet(BOARD_KEY, board);
      await saveLogs(board);

      const after = board.movesLog.length;
      return res.status(200).json({ ok: true, removedEntries: before - after, totalLog: after });
    }

        // ── POST split-card ────────────────────────────────────────
    // Quebra uma OS em múltiplos cards (cliente com vários equipamentos)
    if (req.method === "POST" && action === "split-card") {
      const { pipefyId, splits, tecnico } = req.body || {};
      // splits = [{ equipamento, phaseId }, ...]
      if (!pipefyId || !Array.isArray(splits) || splits.length < 2)
        return res.status(400).json({ ok: false, error: "pipefyId e ao menos 2 splits obrigatórios" });

      const board = sanitizeBoard(await dbGet(BOARD_KEY));
      const original = board.cards.find(c => c.pipefyId === String(pipefyId));
      if (!original) return res.status(404).json({ ok: false, error: "OS não encontrada" });

      const now = new Date().toISOString();
      const newCards = [];

      splits.forEach((s, i) => {
        const suffix = i === 0 ? "" : `-${i + 1}`;
        const newId = `${pipefyId}-split-${i + 1}`;
        const equipLabel = s.equipamento ? ` [${s.equipamento}]` : "";
        const card = {
          ...original,
          pipefyId:   newId,
          title:      original.title + equipLabel,
          equipamento: s.equipamento || null,
          phaseId:    s.phaseId || original.phaseId,
          movedAt:    now,
          movedBy:    tecnico || original.movedBy,
          tecnico:    tecnico || original.tecnico || null,
          splitFrom:  String(pipefyId),
          localOnly:  true,
        };
        newCards.push(card);

        // Log se foi para fase de conclusão
        if (["loja_feito", "delivery_feito"].includes(s.phaseId)) {
          board.movesLog.push({
            phaseId:   s.phaseId,
            timestamp: now,
            tecnico:   tecnico || null,
            pipefyId:  newId,
            equipamento: s.equipamento || null,
          });
        }
      });

      // Remove o card original e insere os splits no lugar
      board.cards = board.cards.filter(c => c.pipefyId !== String(pipefyId));
      // Adiciona IDs dos splits ao syncedIds
      newCards.forEach(c => { if (!board.syncedIds.includes(c.pipefyId)) board.syncedIds.push(c.pipefyId); });
      board.cards.unshift(...newCards);
      board.movesLog = trimLog(board.movesLog);
      await dbSet(BOARD_KEY, board);
      await saveLogs(board);

      return res.status(200).json({ ok: true, created: newCards.length, cards: newCards });
    }

        // ── GET check-card — diagnóstico de fase de um card específico ──
    if (req.method === "GET" && action === "check-card") {
      const { id } = req.query;
      if (!id) return res.status(400).json({ ok: false, error: "id obrigatório" });
      const phase = await fetchCardPhase(id);
      return res.status(200).json({ ok: true, id, phase });
    }

    // ── GET check-ret — mostra fase atual de todos os cards em aguardando_ret ──
    if (req.method === "GET" && action === "check-ret") {
      const board = sanitizeBoard(await dbGet(BOARD_KEY));
      const retCards = board.cards.filter(c =>
        c.phaseId === "aguardando_ret" && !c.localOnly && !c.pipefyId.includes("-split-")
      ).slice(0, 10); // primeiros 10 para diagnóstico
      const results = await Promise.all(retCards.map(async c => ({
        pipefyId: c.pipefyId,
        osCode:   c.osCode,
        phase:    await fetchCardPhase(c.pipefyId),
      })));
      return res.status(200).json({ ok: true, total: retCards.length, results });
    }

    // ── GET cleanup-ret — remove aguardando_ret que foram ERP/Finalizado ──
    if (req.method === "GET" && action === "cleanup-ret") {
      const board = sanitizeBoard(await dbGet(BOARD_KEY));
      let removed = 0, removedIds = [], pipefyError = null;
      let retTotal = 0;
      try {
        const result = await cleanupAguardandoRet(board);
        removed    = result.removed;
        removedIds = result.ids;
        retTotal   = result.retTotal || 0;
        if (removed > 0) {
          board.movesLog.push({
            phaseId:   "cleanup_ret",
            timestamp: new Date().toISOString(),
            removed,
            pipefyIds: removedIds,
          });
          board.movesLog = trimLog(board.movesLog);
          await dbSet(BOARD_KEY, board);
      await saveLogs(board);
        }
      } catch(e) { pipefyError = e.message; }
      return res.status(200).json({
        ok: true, removed, removedIds, pipefyError,
        debug: { retTotal }
      });
    }

    // ── POST clear-compra — limpa dados de compra de um card específico ──
    if (req.method === "POST" && action === "clear-compra") {
      const { pipefyId } = req.body || {};
      if (!pipefyId) return res.status(400).json({ ok: false, error: "pipefyId obrigatório" });
      const board = sanitizeBoard(await dbGet(BOARD_KEY));
      const card  = board.cards.find(c => c.pipefyId === String(pipefyId));
      if (!card) return res.status(404).json({ ok: false, error: "Card não encontrado" });
      delete card.descricaoCompra;
      delete card.fotosCompra;
      delete card.alertaCompra;
      delete card.tipoCompra;
      delete card.previsao;
      await dbSet(BOARD_KEY, board);
      await saveLogs(board);
      return res.status(200).json({ ok: true, pipefyId, msg: "Dados de compra removidos." });
    }

    // ── GET sync-lalamove ─────────────────────────────────────
    if (action === "sync-lalamove") {
      try {
        const LALA_KEY = "reparoeletro_lalamove";
        const lalaDb = (await dbGet(LALA_KEY)) || { fichas: [] };
        if (!Array.isArray(lalaDb.fichas))    lalaDb.fichas    = [];
        if (!Array.isArray(lalaDb.removedIds)) lalaDb.removedIds = [];

        // Importa tudo que não está em removedIds e não está na fila
        // O "limpar-tudo" move as fichas atuais para removedIds, então elas não voltam
        // Novas fichas movidas para a fase SEMPRE entram (sem filtro de tempo)
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
        let added = 0;

        for (const ph of phases) {
          const l    = ph.name.toLowerCase().trim();
          const tipo = l === "coleta solicitada" ? "coleta"
                     : l === "entrega solicitada" ? "entrega"
                     : null;
          if (!tipo) continue;

          for (const { node } of (ph.cards?.edges || [])) {
            const pipefyId   = String(node.id);
            const removedKey = pipefyId + ":" + tipo;

            // Regra 1: já está na fila ativa
            if (lalaDb.fichas.find(f => f.pipefyId === pipefyId && f.tipo === tipo)) continue;
            // Regra 2: foi removida (limpar-tudo ou remoção manual) → não volta
            if (lalaDb.removedIds.includes(removedKey)) continue;

            const fields   = node.fields || [];
            const endField  = fields.find(f => f.name.toLowerCase().includes("endere"));
            const telField  = fields.find(f => f.name.toLowerCase().includes("telefone") || f.name.toLowerCase().includes("fone"));
            const nomeField = fields.find(f => f.name.toLowerCase().includes("nome"));
            const title     = node.title || "";
            const m         = title.match(/^(.*?)\s+(\d{3,6})$/);
            // Nome: campo Nome do Pipefy (mais confiável)
            // Fallback: extrai do título apenas a parte antes de " — " ou " | "
            let nomeContato = nomeField?.value?.trim() || null;
            if (!nomeContato) {
              const tClean = title.split(/\s*[—–|]\s*/)[0].trim();
              const mNome  = tClean.match(/^(.*?)\s+(\d{3,6})$/);
              nomeContato  = mNome ? mNome[1].trim() : tClean;
            }

            lalaDb.fichas.push({
              pipefyId, tipo,
              osCode:      m ? m[2] : null,
              nomeContato,
              descricao:   null,
              endereco:    endField?.value || null,
              telefone:    telField?.value || null,
              lat: null, lng: null,
              addedAt: new Date().toISOString(),
              status:  "pendente",
            });
            added++;
          }
        }

        if (added > 0) await dbSet(LALA_KEY, lalaDb);

        // Registra coletas solicitadas no metaLog para metas
        if (added > 0) {
          const board = await dbGet(BOARD_KEY) || { metaLog: [] };
          if (!Array.isArray(board.metaLog)) board.metaLog = [];
          const seenColeta = new Set(board.metaLog.filter(m=>m.phaseId==="coleta_solicitada").map(m=>m.pipefyId));
          let metaChanged = false;
          lalaDb.fichas.filter(f=>f.tipo==="coleta" && f.status==="pendente").forEach(f => {
            if (!seenColeta.has(f.pipefyId)) {
              board.metaLog.push({ phaseId: "coleta_solicitada", pipefyId: f.pipefyId, timestamp: f.addedAt || new Date().toISOString() });
              metaChanged = true;
            }
          });
          if (metaChanged) await dbSet(BOARD_KEY, board);
      await saveLogs(board);
        }

        return res.status(200).json({ ok: true, added, total: lalaDb.fichas.filter(f => f.status === "pendente").length });
      } catch(e) {
        return res.status(200).json({ ok: false, error: e.message });
      }
    }

    // ── POST limpar-nao-movidas-hoje ─────────────────────────
    if (req.method === "POST" && action === "limpar-nao-movidas-hoje") {
      const board = sanitizeBoard(await dbGet(BOARD_KEY));

      // Meia-noite BRT de hoje em UTC
      function toBRT(d) { return new Date(new Date(d).toLocaleString("en-US",{timeZone:"America/Sao_Paulo"})); }
      const nowBRT = toBRT(new Date()); nowBRT.setHours(0,0,0,0);
      const todayUTC = new Date(nowBRT.getTime() + 3*60*60*1000);

      const before = board.cards.length;
      board.cards = board.cards.filter(card => {
        if (!card.movedAt) return false;           // sem data: remove
        return new Date(card.movedAt) >= todayUTC; // mantém só as movidas hoje
      });
      const removed = before - board.cards.length;

      if (removed > 0) await dbSet(BOARD_KEY, board);
      await saveLogs(board);
      return res.status(200).json({ ok: true, removed, remaining: board.cards.length, board });
    }

    // ── GET erp-to-finalizado — move todos os cards de ERP para Finalizado no Pipefy
    if (action === "erp-to-finalizado") {
      const ERP_PHASE_ID        = "339008925";
      const FINALIZADO_PHASE_ID = "334875153";
      try {
        // Busca cards em ERP
        const all = [];
        let cursor = null, hasNext = true;
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
          if (!data?.phase) break;
          data.phase.cards.edges.forEach(({ node }) => all.push(node));
          hasNext = data.phase.cards.pageInfo?.hasNextPage ?? false;
          cursor  = data.phase.cards.pageInfo?.endCursor ?? null;
        }
        if (!all.length) return res.status(200).json({ ok: true, moved: 0, msg: "Nenhum card em ERP" });

        const results = [];
        for (const card of all) {
          try {
            await pipefyQuery(`mutation {
              moveCardToPhase(input: { card_id: "${card.id}", destination_phase_id: "${FINALIZADO_PHASE_ID}" }) {
                card { id }
              }
            }`);
            results.push({ id: card.id, title: card.title, ok: true });
          } catch(e) {
            results.push({ id: card.id, title: card.title, ok: false, error: e.message });
          }
        }
        const moved = results.filter(r => r.ok).length;
        return res.status(200).json({ ok: true, moved, total: all.length, results });
      } catch(e) {
        return res.status(200).json({ ok: false, error: e.message });
      }
    }

    // ── GET/POST html-store — armazena/retorna HTML no Redis (uso temporário)
    if (action === "html-store") {
      if (req.method === "POST") {
        const { key, html } = req.body || {};
        if (!key || !html) return res.status(400).json({ ok:false, error:"key e html obrigatorios" });
        const buf = Buffer.from(html, "utf8").toString("base64");
        await dbSet("html_store_" + key, buf);
        return res.status(200).json({ ok: true, size: html.length });
      }
      if (req.method === "GET") {
        const { key } = req.query;
        if (!key) return res.status(400).json({ ok:false, error:"key obrigatoria" });
        const b64 = await dbGet("html_store_" + key);
        if (!b64) return res.status(404).json({ ok:false, error:"nao encontrado" });
        const html = Buffer.from(b64, "base64").toString("utf8");
        res.setHeader("Content-Type","text/html; charset=utf-8");
        return res.status(200).send(html);
      }
    }

    // ── GET/POST reset-lala-timestamp — zera clearTimestamp e faz sync inline (acessível via link)
    if (action === "reset-lala-timestamp") {
      const LALA_KEY = "reparoeletro_lalamove";
      const lalaDb = (await dbGet(LALA_KEY)) || { fichas: [], removedIds: [] };
      if (!Array.isArray(lalaDb.fichas))    lalaDb.fichas    = [];
      if (!Array.isArray(lalaDb.removedIds)) lalaDb.removedIds = [];
      delete lalaDb.clearTimestamp;
      // Sync inline: importa todos os cards de Coleta/Entrega Solicitada sem filtro de tempo
      let added = 0;
      try {
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
        for (const ph of (data?.pipe?.phases || [])) {
          const l = ph.name.toLowerCase().trim();
          const tipo = l === "coleta solicitada" ? "coleta" : l === "entrega solicitada" ? "entrega" : null;
          if (!tipo) continue;
          for (const { node } of (ph.cards?.edges || [])) {
            const pipefyId = String(node.id);
            if (lalaDb.fichas.find(f => f.pipefyId === pipefyId && f.tipo === tipo)) continue;
            if (lalaDb.removedIds.includes(pipefyId + ":" + tipo)) continue;
            const fields = node.fields || [];
            const endField = fields.find(f => f.name.toLowerCase().includes("endere"));
            const telField = fields.find(f => f.name.toLowerCase().includes("telefone") || f.name.toLowerCase().includes("fone"));
            const m = (node.title || "").match(/^(.*?)\s+(\d{3,6})$/);
            lalaDb.fichas.push({
              pipefyId, tipo,
              osCode:      m ? m[2] : null,
              nomeContato: m ? m[1].trim() : (node.title || ""),
              descricao: null,
              endereco:  endField?.value || null,
              telefone:  telField?.value || null,
              lat: null, lng: null,
              addedAt: new Date().toISOString(),
              status: "pendente",
            });
            added++;
          }
        }
      } catch(e) { console.error("reset-lala sync:", e.message); }
      await dbSet(LALA_KEY, lalaDb);
      return res.status(200).json({ ok: true, msg: `Reset feito! ${added} ficha(s) importada(s). Recarregue o painel Lalamove.`, added, total: lalaDb.fichas.filter(f=>f.status==="pendente").length });
    }

    // ── GET debug-lalamove — diagnóstico completo do sync ────────────────
    if (action === "debug-lalamove") {
      const LALA_KEY = "reparoeletro_lalamove";
      const lalaDb   = (await dbGet(LALA_KEY)) || { fichas: [] };
      const result   = {
        clearTimestamp:    lalaDb.clearTimestamp || null,
        clearTimestampMs:  lalaDb.clearTimestamp ? new Date(lalaDb.clearTimestamp).getTime() : 0,
        agoraMs:           Date.now(),
        fichasNoRedis:     (lalaDb.fichas || []).length,
        fichasPendentes:   (lalaDb.fichas || []).filter(f => f.status === "pendente").length,
        removedIds:        lalaDb.removedIds || [],
        pipefyFases:       [],
        cardsColeta:       [],
        bloqueados:        [],
        motivos:           [],
      };

      try {
        const data = await pipefyQuery(`query {
          pipe(id: "${PIPE_ID}") {
            phases {
              name
              cards(first: 50) {
                edges {
                  node {
                    id title updated_at
                    phases_history { phase { name } firstTimeIn }
                  }
                }
              }
            }
          }
        }`);

        const phases = data?.pipe?.phases || [];
        result.pipefyFases = phases.map(p => ({ nome: p.name, cards: p.cards?.edges?.length || 0 }));

        for (const ph of phases) {
          const l    = ph.name.toLowerCase().trim();
          const tipo = l === "coleta solicitada" ? "coleta" : l === "entrega solicitada" ? "entrega" : null;
          if (!tipo) continue;

          for (const { node } of (ph.cards?.edges || [])) {
            const pipefyId   = String(node.id);
            const removedKey = pipefyId + ":" + tipo;

            // Calcula entradaMs
            const histEntradas = (node.phases_history || []).filter(h => h.phase?.name?.toLowerCase().trim() === l);
            histEntradas.sort((a, b) => new Date(b.firstTimeIn).getTime() - new Date(a.firstTimeIn).getTime());
            const ultimaEntrada = histEntradas[0];
            let entradaMs = 0;
            if (ultimaEntrada?.firstTimeIn) entradaMs = new Date(ultimaEntrada.firstTimeIn).getTime();
            else if (node.updated_at)       entradaMs = node.updated_at * 1000;
            if (entradaMs === 0)            entradaMs = Date.now();

            // Verifica cada regra
            const jaExiste  = !!(lalaDb.fichas || []).find(f => f.pipefyId === pipefyId && f.tipo === tipo);
            const removido  = (lalaDb.removedIds || []).includes(removedKey);
            const bloqueado = result.clearTimestampMs > 0 && entradaMs <= result.clearTimestampMs;

            const card = {
              pipefyId, tipo, titulo: node.title,
              entradaMs, entradaData: entradaMs ? new Date(entradaMs).toISOString() : null,
              clearTimestamp: result.clearTimestamp,
              jaExisteNaFila: jaExiste,
              removido,
              bloqueadoPorClearTs: bloqueado,
              temHistorico: histEntradas.length > 0,
              updated_at: node.updated_at,
              resultado: jaExiste ? "JÁ NA FILA" : removido ? "REMOVIDO" : bloqueado ? "BLOQUEADO (clearTimestamp)" : "SERIA IMPORTADO",
            };
            result.cardsColeta.push(card);
            if (bloqueado || removido) result.bloqueados.push({ pipefyId, titulo: node.title, motivo: removido ? "removido manualmente" : `entrada ${new Date(entradaMs).toISOString()} <= clearTs ${result.clearTimestamp}` });
          }
        }
        result.resumo = {
          totalNaPipefy: result.cardsColeta.length,
          seriaImportado: result.cardsColeta.filter(c => c.resultado === "SERIA IMPORTADO").length,
          jaExistem: result.cardsColeta.filter(c => c.resultado === "JÁ NA FILA").length,
          bloqueados: result.cardsColeta.filter(c => c.resultado.includes("BLOQUEADO")).length,
          removidos: result.cardsColeta.filter(c => c.resultado === "REMOVIDO").length,
        };
      } catch(e) { result.erroQuery = e.message; }

      return res.status(200).json(result);
    }

    // ── GET scan-erp-day — varre TODAS as fases buscando cards que entraram em ERP num dia
    // Necessário para capturar cards que já saíram do ERP (Finalizado, Descarte, etc.)
    if (action === "scan-erp-day") {
      const targetDate = req.query.date || new Date(Date.now() - 3*60*60*1000).toISOString().slice(0,10);
      try {
        const board = sanitizeBoard(await dbGet(BOARD_KEY));
        if (!Array.isArray(board.metaLog)) board.metaLog = [];
        const seenErp = new Set(board.metaLog.filter(m=>m.phaseId==="erp_entrada").map(m=>m.pipefyId));

        // Busca todas as fases do pipe incluindo downstream (ERP, Finalizado, Descarte, RS, etc.)
        const scanPhases = ["erp","finaliz","conclu","descar","reprov","rs","fechamento","entregue"];

        const phasesData = await pipefyQuery(`query {
          pipe(id: "${PIPE_ID}") {
            phases {
              name
              cards(first: 50) {
                edges {
                  node {
                    id
                    fields { name value }
                    phases_history { phase { name } firstTimeIn }
                  }
                }
              }
            }
          }
        }`);

        const allPhases = phasesData?.pipe?.phases || [];
        let added = 0, scanned = 0;

        for (const ph of allPhases) {
          const l = ph.name.toLowerCase();
          // Só varre fases que fazem sentido (downstream do ERP)
          if (!scanPhases.some(s => l.includes(s))) continue;

          for (const { node } of (ph.cards?.edges || [])) {
            scanned++;
            const id = String(node.id);
            if (seenErp.has(id)) continue;

            // Verifica se entrou em ERP no dia alvo via phases_history
            const erpHist = (node.phases_history || []).find(h =>
              h.phase?.name?.toLowerCase().includes("erp") && h.firstTimeIn
            );
            if (!erpHist) continue;

            // Converte timestamp para BH e compara com targetDate
            const erpTs = erpHist.firstTimeIn;
            const erpDateBH = new Date(new Date(erpTs).getTime() - 3*60*60*1000).toISOString().slice(0,10);
            if (erpDateBH !== targetDate) continue;

            // Busca valor
            const fields = node.fields || [];
            const vf = fields.find(f => f.name.toLowerCase().includes("valor"));
            const valor = vf?.value ? parseFloat(String(vf.value).replace(/[^\d.,]/g,"").replace(",",".")) || 0 : 0;

            board.metaLog.push({ phaseId: "erp_entrada", pipefyId: id, valor, timestamp: erpTs });
            seenErp.add(id);
            added++;
          }
        }

        // Também varre a fase ERP atual
        const erpPhase = allPhases.find(ph => ph.name.toLowerCase().includes("erp") && !ph.name.toLowerCase().includes("finaliz"));
        if (erpPhase) {
          for (const { node } of (erpPhase.cards?.edges || [])) {
            scanned++;
            const id = String(node.id);
            if (seenErp.has(id)) continue;
            const erpHist = (node.phases_history || []).find(h =>
              h.phase?.name?.toLowerCase().includes("erp") && h.firstTimeIn
            );
            const erpTs = erpHist?.firstTimeIn || new Date().toISOString();
            const erpDateBH = new Date(new Date(erpTs).getTime() - 3*60*60*1000).toISOString().slice(0,10);
            if (erpDateBH !== targetDate) continue;
            const fields = node.fields || [];
            const vf = fields.find(f => f.name.toLowerCase().includes("valor"));
            const valor = vf?.value ? parseFloat(String(vf.value).replace(/[^\d.,]/g,"").replace(",",".")) || 0 : 0;
            board.metaLog.push({ phaseId: "erp_entrada", pipefyId: id, valor, timestamp: erpTs });
            seenErp.add(id);
            added++;
          }
        }

        if (added > 0) {
          await dbSet(BOARD_KEY, board);
          await saveLogs(board);
        }

        return res.status(200).json({ ok: true, targetDate, scanned, added, totalErpMetalog: board.metaLog.filter(m=>m.phaseId==="erp_entrada").length });
      } catch(e) {
        return res.status(200).json({ ok: false, error: "scan-erp-day: " + e.message });
      }
    }

    // ── GET reprocess-erp-timestamps — corrige timestamps do metaLog usando phases_history real
    if (action === "reprocess-erp-timestamps") {
      try {
        const board = sanitizeBoard(await dbGet(BOARD_KEY));
        if (!Array.isArray(board.metaLog)) board.metaLog = [];
        
        // Pega todos os erp_entrada únicos
        const erpEntries = board.metaLog.filter(m => m.phaseId === "erp_entrada");
        const uniqueIds = [...new Set(erpEntries.map(m => m.pipefyId))].filter(id => !id.startsWith("manual-"));
        
        let fixed = 0, errors = 0;
        // Processa em lotes de 10
        for (let i = 0; i < uniqueIds.length; i += 10) {
          const lote = uniqueIds.slice(i, i + 10);
          const aliases = lote.map((id, j) => `c${j}: card(id: "${id}") { id fields { name value } phases_history { phase { name } firstTimeIn } }`).join(" ");
          try {
            const data = await pipefyQuery(`query { ${aliases} }`);
            for (let j = 0; j < lote.length; j++) {
              const id = lote[j];
              const card = data[`c${j}`];
              if (!card) continue;
              const hist = (card.phases_history || []).find(h =>
                h.phase?.name?.toLowerCase().includes("erp") && h.firstTimeIn
              );
              if (!hist?.firstTimeIn) continue;
              const realTs = hist.firstTimeIn;
              const vf = (card.fields || []).find(f => f.name.toLowerCase().includes("valor"));
              const valor = vf?.value ? parseFloat(String(vf.value).replace(/[^\d.,]/g,"").replace(",",".")) || 0 : 0;
              // Atualiza a entrada no metaLog
              const entry = board.metaLog.find(m => m.phaseId === "erp_entrada" && m.pipefyId === id);
              if (entry) {
                entry.timestamp = realTs;
                entry.valor = valor;
                fixed++;
              }
            }
          } catch(e) { errors++; }
        }
        
        await dbSet(BOARD_KEY, board);
        await saveLogs(board);
        return res.status(200).json({ ok: true, fixed, errors, total: uniqueIds.length });
      } catch(e) {
        return res.status(200).json({ ok: false, error: e.message });
      }
    }

    // ── POST reset-erp-batch — remove entradas ERP de um intervalo e força re-sync com phases_history
    if (req.method === "POST" && action === "reset-erp-batch") {
      const { after, before } = req.body || {};
      if (!after || !before) return res.status(400).json({ ok: false, error: "after e before obrigatórios" });
      try {
        const board = sanitizeBoard(await dbGet(BOARD_KEY));
        if (!Array.isArray(board.metaLog)) board.metaLog = [];
        const afterMs  = new Date(after).getTime();
        const beforeMs = new Date(before).getTime();
        // Coleta pipefyIds das entradas ruins
        const badIds = new Set();
        board.metaLog = board.metaLog.filter(m => {
          if (m.phaseId !== "erp_entrada") return true;
          const ts = new Date(m.timestamp).getTime();
          if (ts >= afterMs && ts <= beforeMs) { badIds.add(m.pipefyId); return false; }
          return true;
        });
        // Remove esses IDs do seenErp implicitamente (já removemos do metaLog)
        // O próximo sync vai re-detectá-los e buscar phases_history
        await dbSet(BOARD_KEY, board);
        await saveLogs(board);
        return res.status(200).json({ ok: true, removed: badIds.size, ids: [...badIds].slice(0,5) });
      } catch(e) { return res.status(200).json({ ok: false, error: e.message }); }
    }

    // ── GET fix-recovered — move cards recuperados para aguardando_ret, remove ERP/Finalizado
    if (action === "fix-recovered") {
      try {
        const board = sanitizeBoard(await dbGet(BOARD_KEY));
        const recovered = board.cards.filter(c => c.recoveredAt);
        if (!recovered.length) return res.status(200).json({ ok: true, msg: "Nenhum card recuperado", moved: 0 });

        // Busca quais estão em ERP ou Finalizado no Pipefy
        const ids = recovered.map(c => c.pipefyId);
        const aliases = ids.slice(0, 50).map((id, j) => `c${j}: card(id: "${id}") { id current_phase { name } }`).join(" ");
        let erpIds = new Set();
        try {
          const data = await pipefyQuery(`query { ${aliases} }`);
          for (let j = 0; j < ids.length; j++) {
            const card = data[`c${j}`];
            const fase = card?.current_phase?.name?.toLowerCase() || "";
            if (fase.includes("erp") || fase.includes("finaliz") || fase.includes("conclu") || fase.includes("descar") || fase.includes("reprov")) {
              erpIds.add(ids[j]);
            }
          }
        } catch(e) { console.error("fix-recovered pipefy check:", e.message); }

        let moved = 0, removed = 0;
        board.cards = board.cards.filter(c => {
          if (!c.recoveredAt) return true; // não mexe em cards normais
          if (erpIds.has(c.pipefyId)) { removed++; return false; } // remove ERP/Finalizado
          c.phaseId = "aguardando_ret"; // move para aguardando retirada
          delete c.recoveredAt; // limpa flag
          moved++;
          return true;
        });

        await dbSet(BOARD_KEY, board);
        await dbSet(BACKUP_KEY, { ...board, backedUpAt: new Date().toISOString() });
        return res.status(200).json({ ok: true, moved, removed, total: board.cards.length });
      } catch(e) {
        return res.status(200).json({ ok: false, error: "fix-recovered: " + e.message });
      }
    }

    // ── GET restore-backup — restaura board do último backup
    if (action === "restore-backup") {
      try {
        const backup = await dbGet(BACKUP_KEY);
        if (!backup) return res.status(200).json({ ok: false, error: "Nenhum backup encontrado" });
        await dbSet(BOARD_KEY, backup);
        return res.status(200).json({ ok: true, backedUpAt: backup.backedUpAt, cards: backup.cards?.length });
      } catch(e) {
        return res.status(200).json({ ok: false, error: e.message });
      }
    }

    // ── GET get-pipefy-phases — mostra fases reais do Pipefy ──
    if (action === "get-pipefy-phases") {
      try {
        const data = await pipefyQuery(`query {
          pipe(id: "${PIPE_ID}") {
            phases {
              name
              cards_count
            }
          }
        }`);
        const phases = (data?.pipe?.phases || []).map(p => ({
          name: p.name,
          count: p.cards_count || 0,
          lower: p.name.toLowerCase()
        }));
        return res.status(200).json({ ok: true, phases });
      } catch(e) {
        return res.status(200).json({ ok: false, error: e.message });
      }
    }

    // ── GET recover-all — reimporta TODOS os cards de todas as fases ativas ──
    if (action === "recover-all") {
      try {
        const data = await pipefyQuery(`query {
          pipe(id: "${PIPE_ID}") {
            phases {
              name
              cards(first: 50) {
                edges {
                  node {
                    id title age
                    fields { name value }
                  }
                }
              }
            }
          }
        }`);

        const board = sanitizeBoard(await dbGet(BOARD_KEY));
        const phases = data?.pipe?.phases || [];
        const activeIds = new Set(board.cards.map(c => c.pipefyId));

        // Fases que NÃO devem ser importadas (já saíram do fluxo)
        const skipPhases = ["erp","finaliz","conclu","descar","reprov","entregue","concluíd"];

        let added = 0, skipped = 0;

        for (const ph of phases) {
          const l = ph.name.toLowerCase();
          if (skipPhases.some(s => l.includes(s))) { skipped++; continue; }

          // Mapeia nome da fase Pipefy → phaseId interno do board
          let phaseId = "aprovado";
          if (l.includes("produção") || l.includes("producao")) phaseId = "producao";
          else if (l.includes("urgência") || l.includes("urgencia")) phaseId = "urgencia";
          else if (l.includes("comprar") && l.includes("peça")) phaseId = "comprar_peca";
          else if (l.includes("aguardando") && l.includes("peça")) phaseId = "aguardando_peca";
          else if (l.includes("peça disponível") || l.includes("peca disponivel")) phaseId = "peca_disponivel";
          else if (l.includes("loja feito") || (l.includes("loja") && l.includes("feito"))) phaseId = "loja_feito";
          else if (l.includes("delivery") && l.includes("feito")) phaseId = "delivery_feito";
          else if (l.includes("aguardando") && l.includes("ret")) phaseId = "aguardando_ret";
          else if (l.includes("cliente") && l.includes("loja")) phaseId = "cliente_loja";

          for (const { node } of (ph.cards?.edges || [])) {
            const id = String(node.id);
            if (activeIds.has(id)) continue; // já está no board

            const fields = node.fields || [];
            const nomeField = fields.find(f => f.name.toLowerCase().includes("nome") || f.name.toLowerCase().includes("contato"));
            const descField = fields.find(f => f.name.toLowerCase().includes("descri") || f.name.toLowerCase().includes("problem") || f.name.toLowerCase().includes("servi"));
            const nomeVal = nomeField?.value || "";
            const digitsMatch = nomeVal.match(/(\d{4})\D*$/);

            board.cards.push({
              pipefyId:    id,
              title:       node.title || "",
              nomeContato: nomeVal || null,
              osCode:      digitsMatch ? digitsMatch[1] : null,
              descricao:   descField?.value || null,
              age:         node.age ?? null,
              phaseId,
              addedAt:     new Date().toISOString(),
              recoveredAt: new Date().toISOString(),
            });
            if (!board.syncedIds.includes(id)) board.syncedIds.push(id);
            activeIds.add(id);
            added++;
          }
        }

        await dbSet(BOARD_KEY, board);
        return res.status(200).json({ ok: true, added, skippedPhases: skipped, total: board.cards.length });
      } catch(e) {
        return res.status(200).json({ ok: false, error: "Erro recover-all: " + e.message });
      }
    }

    return res.status(404).json({ ok: false, error: "Ação não encontrada" });

  } catch (err) {
    console.error("Handler error:", err);
    return res.status(500).json({ ok: false, error: err.message });
  }
};
