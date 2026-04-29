const UPSTASH_URL   = (process.env.UPSTASH_URL   || "").replace(/['"]/g,"").trim();
const UPSTASH_TOKEN = (process.env.UPSTASH_TOKEN  || "").replace(/['"]/g,"").trim();
const PIPEFY_API    = "https://api.pipefy.com/graphql";
const TV_PIPE_ID    = "306904889";
const SOLICITAR_ENTREGA_PHASE_ID = "341638199"; // "Solicitar Entrega" no Pipefy
const COLETA_KEY    = "tv_coleta_cards";
const BOARD_KEY     = "tv_board";

const COLETA_PHASES = [
  { id: "liberado_coleta",       name: "Liberado para Coleta" },
  { id: "comunicacao_realizada", name: "Comunicacao Realizada" },
  { id: "coleta_realizada",      name: "Coleta Realizada" },
  { id: "remarcar",              name: "Remarcar" },
  { id: "orcamento_registrado",  name: "Orcamento Registrado" },
];


const ENTREGA_KEY   = "tv_entrega_cards";
const SOLICITAR_ENTREGA_BOARD_PHASE = "solicitar_entrega"; // phaseId no tv_board

const ENTREGA_PHASES = [
  { id: "liberado_entrega",    name: "Liberado para Entrega" },
  { id: "comunicacao_entrega", name: "Comunicacao Realizada" },
  { id: "entrega_realizada",   name: "Entrega Realizada" },
];

async function dbGet(key) {
  try {
    const r = await fetch(UPSTASH_URL + "/pipeline", {
      method: "POST",
      headers: { Authorization: "Bearer " + UPSTASH_TOKEN, "Content-Type": "application/json" },
      body: JSON.stringify([["GET", key]]),
    });
    const j = await r.json();
    return j[0] && j[0].result ? JSON.parse(j[0].result) : null;
  } catch(e) { return null; }
}

async function dbSet(key, val) {
  try {
    await fetch(UPSTASH_URL + "/pipeline", {
      method: "POST",
      headers: { Authorization: "Bearer " + UPSTASH_TOKEN, "Content-Type": "application/json" },
      body: JSON.stringify([["SET", key, JSON.stringify(val)]]),
    });
    return true;
  } catch(e) { return false; }
}

async function pipefyMutation(query) {
  const token = (process.env.PIPEFY_TOKEN || "").trim();
  if (!token) throw new Error("PIPEFY_TOKEN ausente");
  const r = await fetch(PIPEFY_API, {
    method: "POST",
    headers: { "Content-Type": "application/json", Authorization: "Bearer " + token },
    body: JSON.stringify({ query }),
  });
  const j = await r.json();
  if (j.errors) throw new Error(j.errors.map(function(e){ return e.message; }).join("; "));
  return j.data;
}

// Phase ID hardcoded — confirmado em api/tv-orcamento.js linha 906
const AGUARDANDO_APROVACAO_ID = "341638194";

async function getAguardandoAprovacaoId() {
  return AGUARDANDO_APROVACAO_ID;
}

async function getERPPhaseId() {
  const token = (process.env.PIPEFY_TOKEN || "").trim();
  if (!token) return null;
  try {
    const r = await fetch(PIPEFY_API, {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: "Bearer " + token },
      body: JSON.stringify({ query: 'query { pipe(id: "' + TV_PIPE_ID + '") { phases { id name } } }' }),
    });
    const j = await r.json();
    const phases = (j.data && j.data.pipe && j.data.pipe.phases) || [];
    const found = phases.find(function(p) {
      return p.name.toLowerCase().includes("erp");
    });
    return found ? found.id : null;
  } catch(e) { return null; }
}

module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(200).end();

  const action = req.query && req.query.action;

  try {

    // GET load
    if (action === "load") {
      const coletaRaw = await dbGet(COLETA_KEY);
      const boardRaw  = await dbGet(BOARD_KEY);
      const coleta = coletaRaw || { cards: [] };
      const board  = boardRaw  || { cards: [] };

      var changed = false;
      var boardCards = (board.cards || []).filter(function(c) { return c.phaseId === "liberado_rota"; });
      for (var i = 0; i < boardCards.length; i++) {
        var bc = boardCards[i];
        var id = String(bc.pipefyId);
        var exists = coleta.cards.find(function(c) { return c.pipefyId === id; });
        if (!exists) {
          coleta.cards.unshift({
            pipefyId:     id,
            nomeContato:  bc.nomeContato || bc.title || "Ã¢ÂÂ",
            osCode:       bc.osCode  || null,
            endereco:     bc.endereco || null,
            telefone:     bc.telefone || null,
            descricao:    bc.descricao || null,
            coletaPhase:  "liberado_coleta",
            entradaEm:    bc.movedAt || new Date().toISOString(),
            diagnostico:  null,
          });
          changed = true;
        }
      }
      if (changed) await dbSet(COLETA_KEY, coleta);

      var byPhase = {};
      COLETA_PHASES.forEach(function(p) { byPhase[p.id] = []; });
      coleta.cards.forEach(function(c) {
        var ph = c.coletaPhase || "liberado_coleta";
        if (byPhase[ph]) byPhase[ph].push(c);
      });

      return res.status(200).json({ ok: true, phases: COLETA_PHASES, byPhase: byPhase, total: coleta.cards.length });
    }

    // POST move
    if (req.method === "POST" && action === "move") {
      var body = req.body || {};
      var pipefyId = body.pipefyId;
      var phase    = body.phase;
      if (!pipefyId || !phase) return res.status(400).json({ ok: false, error: "pipefyId e phase obrigatorios" });
      if (!COLETA_PHASES.find(function(p) { return p.id === phase; }))
        return res.status(400).json({ ok: false, error: "Fase invalida" });

      var coleta = (await dbGet(COLETA_KEY)) || { cards: [] };
      var card = coleta.cards.find(function(c) { return String(c.pipefyId) === String(pipefyId); });
      if (!card) {
        var board = (await dbGet(BOARD_KEY)) || { cards: [] };
        var bc2 = (board.cards || []).find(function(c) { return String(c.pipefyId) === String(pipefyId); });
        card = {
          pipefyId:    String(pipefyId),
          nomeContato: (bc2 && (bc2.nomeContato || bc2.title)) || "Ã¢ÂÂ",
          osCode:      (bc2 && bc2.osCode)    || null,
          endereco:    (bc2 && bc2.endereco)  || null,
          telefone:    (bc2 && bc2.telefone)  || null,
          descricao:   (bc2 && bc2.descricao) || null,
          coletaPhase: "liberado_coleta",
          entradaEm:   (bc2 && bc2.movedAt)  || new Date().toISOString(),
          diagnostico: null,
        };
        coleta.cards.unshift(card);
      }
      card.coletaPhase = phase;
      card[phase + "Em"] = new Date().toISOString();
      await dbSet(COLETA_KEY, coleta);
      return res.status(200).json({ ok: true, card: card });
    }

    // POST diagnostico
    if (req.method === "POST" && action === "diagnostico") {
      var body = req.body || {};
      var pipefyId = body.pipefyId;
      var texto    = body.texto;
      if (!pipefyId || !(texto && texto.trim()))
        return res.status(400).json({ ok: false, error: "pipefyId e texto obrigatorios" });

      var coleta = (await dbGet(COLETA_KEY)) || { cards: [] };
      var card = coleta.cards.find(function(c) { return String(c.pipefyId) === String(pipefyId); });
      var modelo = body.modelo;
      if (card) {
        card.coletaPhase  = "orcamento_registrado";
        card.diagnostico  = texto.trim();
        card.diagnosticoEm = new Date().toISOString();
        if (modelo && modelo.trim()) card.modelo = modelo.trim();
      }
      await dbSet(COLETA_KEY, coleta);

      var pipefyComment = { ok: false };
      try {
        var t = texto.trim().replace(/"/g, '\\"').replace(/\n/g, ' ');
        var m = (modelo && modelo.trim()) ? ('Modelo: ' + modelo.trim().replace(/"/g, '') + ' | ') : '';
        await pipefyMutation('mutation { createComment(input: { card_id: "' + pipefyId + '", text: "' + m + 'Diagnostico: ' + t + '" }) { comment { id } } }');
        pipefyComment = { ok: true };
      } catch(e) { pipefyComment = { ok: false, error: e.message }; }

      var pipefyMove = { ok: false };
      try {
        // Tenta direto para Aguardando Aprovação (341638194).
        // Se recusar (fase não válida), passa por Aguardando Orçamento (341638197) primeiro.
        async function moverParaAprovacao(cid) {
          const APROVACAO = "341638194";
          const ORCAMENTO = "341638197";
          const move = function(pid, dest) {
            return pipefyMutation('mutation { moveCardToPhase(input: { card_id: "' + pid + '", destination_phase_id: "' + dest + '" }) { card { id } } }');
          };
          try {
            await move(cid, APROVACAO);
            return { ok: true, via: "direto" };
          } catch(e1) {
            if (e1.message && e1.message.includes("fase")) {
              try {
                await move(cid, ORCAMENTO);
                await move(cid, APROVACAO);
                return { ok: true, via: "intermediaria" };
              } catch(e2) { return { ok: false, error: e2.message }; }
            }
            return { ok: false, error: e1.message };
          }
        }
        pipefyMove = await moverParaAprovacao(pipefyId);
      } catch(e) { pipefyMove = { ok: false, error: e.message }; }

      // Se o move Pipefy falhou, retorna aviso mas ainda ok:true (diagnostico salvo)
      const moveWarning = (!pipefyMove.ok) ? ("Pipefy move falhou: " + (pipefyMove.error || "nao encontrado")) : null;
      return res.status(200).json({ ok: true, card: card, pipefyComment: pipefyComment, pipefyMove: pipefyMove, moveWarning: moveWarning });
    }

    // POST salvar-relatorio (motorista)
    if (req.method === "POST" && action === "salvar-relatorio") {
      var body = req.body || {};
      var pipefyId  = body.pipefyId;
      var descricao = body.descricao;
      var temFoto   = !!(body.foto);
      var temAssin  = !!(body.assinatura);
      if (!pipefyId) return res.status(400).json({ ok: false, error: "pipefyId obrigatorio" });

      var coleta = (await dbGet(COLETA_KEY)) || { cards: [] };
      var card = coleta.cards.find(function(c) { return String(c.pipefyId) === String(pipefyId); });
      if (!card) {
        card = { pipefyId: String(pipefyId), coletaPhase: "liberado_coleta", entradaEm: new Date().toISOString() };
        coleta.cards.unshift(card);
      }
      // Nao armazenar base64 no Redis (muito grande) â apenas flags
      card.relatorio = {
        descricao:      descricao || null,
        temFoto:        temFoto,
        temAssinatura:  temAssin,
        registradoEm:   new Date().toISOString(),
      };
      card.coletaPhase       = "coleta_realizada";
      card.coleta_realizadaEm = new Date().toISOString();
      await dbSet(COLETA_KEY, coleta);
      return res.status(200).json({ ok: true, card: card });
    }

    // GET fases
    if (action === "fases") {
      return res.status(200).json({ ok: true, coletaPhases: COLETA_PHASES });
    }

    // GET sync-entrega — busca cards na fase 341638199 (Solicitar Entrega) e atualiza o board
    if (action === "sync-entrega") {
      const board = (await dbGet(BOARD_KEY)) || { cards: [], syncedIds: [] };
      if (!board.syncedIds) board.syncedIds = [];
      let edges = [], pipefyErr = null;
      try {
        const token = (process.env.PIPEFY_TOKEN || "").trim();
        const q = `query { phase(id: "${SOLICITAR_ENTREGA_PHASE_ID}") { cards(first: 50) { edges { node { id title fields { name value } } } } } }`;
        const r = await fetch(PIPEFY_API, {
          method: "POST",
          headers: { "Content-Type": "application/json", Authorization: "Bearer " + token },
          body: JSON.stringify({ query: q }),
        });
        const j = await r.json();
        edges = j?.data?.phase?.cards?.edges || [];
      } catch(e) { pipefyErr = e.message; }

      let moved = 0;
      for (const edge of edges) {
        const node = edge.node;
        const id   = String(node.id);
        const fields = node.fields || [];
        const nomeF  = fields.find(function(f){ return f.name.toLowerCase().includes("nome"); });
        const telF   = fields.find(function(f){ return f.name.toLowerCase().includes("telefone") || f.name.toLowerCase().includes("fone"); });
        const endF   = fields.find(function(f){ return f.name.toLowerCase().includes("endere"); });
        const descF  = fields.find(function(f){ return f.name.toLowerCase().includes("descri"); });
        const tel    = (telF && telF.value) ? telF.value : "";
        const nome   = (nomeF && nomeF.value) ? nomeF.value : node.title;
        const existing = board.cards.find(function(c){ return c.pipefyId === id; });
        if (existing) {
          if (existing.phaseId !== "solicitar_entrega") {
            existing.phaseId = "solicitar_entrega";
            existing.movedAt = new Date().toISOString();
            moved++;
          }
        } else {
          board.cards.unshift({
            pipefyId:    id,
            title:       node.title,
            nomeContato: nome,
            telefone:    tel,
            endereco:    (endF && endF.value) ? endF.value : "",
            descricao:   (descF && descF.value) ? descF.value : "",
            phaseId:     "solicitar_entrega",
            movedAt:     new Date().toISOString(),
            addedAt:     new Date().toISOString(),
          });
          if (board.syncedIds.indexOf(id) === -1) board.syncedIds.push(id);
          moved++;
        }
      }
      // Remover fantasmas: cards marcados solicitar_entrega que nao estao mais no Pipefy
      const idsNoPipefy = new Set(edges.map(function(e){ return String(e.node.id); }));
      board.cards.forEach(function(c){
        if (c.phaseId === "solicitar_entrega" && !idsNoPipefy.has(c.pipefyId)) {
          c.phaseId = "entrega_andamento";
          c.movedAt = new Date().toISOString();
        }
      });
      if (moved > 0 || edges.length > 0) {
        try { await dbSet(BOARD_KEY, board); } catch(e) { /* ignore */ }
      }
      const filaAtual = board.cards.filter(function(c){ return c.phaseId === "solicitar_entrega"; });
      return res.status(200).json({ ok: true, found: edges.length, moved, filaCount: filaAtual.length, pipefyErr });
    }

        // GET load-entrega
    if (action === "load-entrega") {
      var entrega = (await dbGet(ENTREGA_KEY)) || { cards: [] };
      var board   = (await dbGet(BOARD_KEY))   || { cards: [] };
      var changed = false;
      var boardCards = (board.cards || []).filter(function(c) { return c.phaseId === SOLICITAR_ENTREGA_BOARD_PHASE; });
      for (var i = 0; i < boardCards.length; i++) {
        var bc = boardCards[i];
        var id = String(bc.pipefyId);
        var exists = entrega.cards.find(function(c) { return c.pipefyId === id; });
        if (!exists) {
          entrega.cards.unshift({
            pipefyId:    id,
            nomeContato: bc.nomeContato || bc.title || "â",
            osCode:      bc.osCode   || null,
            endereco:    bc.endereco || null,
            telefone:    bc.telefone || null,
            descricao:   bc.descricao || null,
            entregaPhase: "liberado_entrega",
            entradaEm:   bc.movedAt || new Date().toISOString(),
          });
          changed = true;
        }
      }
      if (changed) await dbSet(ENTREGA_KEY, entrega);
      var byPhase = {};
      ENTREGA_PHASES.forEach(function(p) { byPhase[p.id] = []; });
      (entrega.cards || []).forEach(function(c) {
        var ph = c.entregaPhase || "liberado_entrega";
        if (byPhase[ph]) byPhase[ph].push(c);
      });
      return res.status(200).json({ ok: true, phases: ENTREGA_PHASES, byPhase: byPhase, total: (entrega.cards || []).length });
    }

    // POST move-entrega
    if (req.method === "POST" && action === "move-entrega") {
      var body = req.body || {};
      var pipefyId = String(body.pipefyId || "");
      var phase    = body.phase;
      if (!pipefyId || !phase) return res.status(400).json({ ok: false, error: "pipefyId e phase obrigatorios" });
      if (!ENTREGA_PHASES.find(function(p) { return p.id === phase; }))
        return res.status(400).json({ ok: false, error: "Fase invalida para entrega" });
      var entrega = (await dbGet(ENTREGA_KEY)) || { cards: [] };
      var card = entrega.cards.find(function(c) { return c.pipefyId === pipefyId; });
      if (!card) {
        var board2 = (await dbGet(BOARD_KEY)) || { cards: [] };
        var bc2    = (board2.cards || []).find(function(c) { return String(c.pipefyId) === pipefyId; });
        card = {
          pipefyId:     pipefyId,
          nomeContato:  (bc2 && (bc2.nomeContato || bc2.title)) || "â",
          osCode:       (bc2 && bc2.osCode)    || null,
          endereco:     (bc2 && bc2.endereco)  || null,
          telefone:     (bc2 && bc2.telefone)  || null,
          descricao:    (bc2 && bc2.descricao) || null,
          entregaPhase: "liberado_entrega",
          entradaEm:    new Date().toISOString(),
        };
        entrega.cards.unshift(card);
      }
      card.entregaPhase   = phase;
      card[phase + "Em"]  = new Date().toISOString();
      await dbSet(ENTREGA_KEY, entrega);
      return res.status(200).json({ ok: true, card: card });
    }

    // POST confirmar-pagamento â move Pipefy card para ERP e remove da entrega local
    if (req.method === "POST" && action === "confirmar-pagamento") {
      var body = req.body || {};
      var pipefyId = String(body.pipefyId || "");
      if (!pipefyId) return res.status(400).json({ ok: false, error: "pipefyId obrigatorio" });
      // Remove do Redis de entrega
      var entrega = (await dbGet(ENTREGA_KEY)) || { cards: [] };
      entrega.cards = (entrega.cards || []).filter(function(c) { return c.pipefyId !== pipefyId; });
      await dbSet(ENTREGA_KEY, entrega);
      // Move card no Pipefy para ERP
      var pipefyERP = { ok: false };
      try {
        var erpPhaseId = await getERPPhaseId();
        if (erpPhaseId) {
          await pipefyMutation('mutation { moveCardToPhase(input: { card_id: "' + pipefyId + '", destination_phase_id: "' + erpPhaseId + '" }) { card { id } } }');
          pipefyERP = { ok: true, erpPhaseId: erpPhaseId };
        } else {
          pipefyERP = { ok: false, error: "Fase ERP nao encontrada no pipe TV" };
        }
      } catch(e) { pipefyERP = { ok: false, error: e.message }; }
      return res.status(200).json({ ok: true, pipefyERP: pipefyERP });
    }

    // POST excluir — remove ficha da view do motorista (coleta ou entrega)
    if (req.method === "POST" && action === "excluir") {
      var body = req.body || {};
      var pipefyId = String(body.pipefyId || "");
      var type     = body.type || "coleta"; // 'coleta' ou 'entrega'
      if (!pipefyId) return res.status(400).json({ ok: false, error: "pipefyId obrigatorio" });

      // 1. Marcar no tv_board para nao reaparecer no proximo sync
      var board = (await dbGet(BOARD_KEY)) || { cards: [] };
      var bc = (board.cards || []).find(function(c) { return String(c.pipefyId) === pipefyId; });
      if (bc) {
        bc.phaseId = "excluido_motorista";
        await dbSet(BOARD_KEY, board);
      }

      // 2. Remover de tv_coleta_cards (se for coleta)
      if (type === "coleta") {
        var coleta = (await dbGet(COLETA_KEY)) || { cards: [] };
        coleta.cards = (coleta.cards || []).filter(function(c) { return String(c.pipefyId) !== pipefyId; });
        await dbSet(COLETA_KEY, coleta);
      }

      // 3. Remover de tv_entrega_cards (se for entrega)
      if (type === "entrega") {
        var entrega = (await dbGet(ENTREGA_KEY)) || { cards: [] };
        entrega.cards = (entrega.cards || []).filter(function(c) { return String(c.pipefyId) !== pipefyId; });
        await dbSet(ENTREGA_KEY, entrega);
      }

      return res.status(200).json({ ok: true });
    }

    
  // ── GET retry-mover-aguardando ─────────────────────────────────────────────
  if (action === "retry-mover-aguardando") {
    const coleta = (await dbGet(COLETA_KEY)) || { cards: [] };
    const pendentes = (coleta.cards || []).filter(function(c) {
      return c.coletaPhase === "orcamento_registrado" && c.diagnostico && c.pipefyId;
    });
    const results = [];
    for (const card of pendentes) {
      try {
        await pipefyMutation(
          'mutation { moveCardToPhase(input: { card_id: "' + card.pipefyId +
          '", destination_phase_id: "' + AGUARDANDO_APROVACAO_ID + '" }) { card { id } } }'
        );
        results.push({ pipefyId: card.pipefyId, nome: card.nomeContato || card.osCode || card.pipefyId, ok: true });
      } catch(e) {
        results.push({ pipefyId: card.pipefyId, nome: card.nomeContato || card.osCode || card.pipefyId, ok: false, error: e.message });
      }
    }
    return res.status(200).json({ ok: true, total: pendentes.length, results: results });
  }

    // ── retry-mover-aguardando — move retroativamente fichas com diagnostico ──
  if (action === "retry-mover-aguardando") {
    const coleta = (await dbGet(COLETA_KEY)) || { cards: [] };
    const pendentes = (coleta.cards || []).filter(function(c) {
      return c.coletaPhase === "orcamento_registrado" && c.diagnostico && c.pipefyId;
    });
    const results = [];
    const mover2passos = async function(cid) {
      const move = function(pid, dest) {
        return pipefyMutation('mutation { moveCardToPhase(input: { card_id: "' + pid + '", destination_phase_id: "' + dest + '" }) { card { id } } }');
      };
      try { await move(cid, "341638194"); return { ok: true, via: "direto" }; }
      catch(e1) {
        if (e1.message && e1.message.includes("fase")) {
          try { await move(cid, "341638197"); await move(cid, "341638194"); return { ok: true, via: "intermediaria" }; }
          catch(e2) { return { ok: false, error: e2.message }; }
        }
        return { ok: false, error: e1.message };
      }
    };
    for (const card of pendentes) {
      const r = await mover2passos(card.pipefyId);
      results.push({ pipefyId: card.pipefyId, nome: card.nomeContato || card.osCode || card.pipefyId, ...r });
    }
    return res.status(200).json({ ok: true, total: pendentes.length, results: results });
  }

    // ── forcar-aguardando — força 6 fichas específicas para Aguardando Aprovação ──
  if (action === "forcar-aguardando") {
    // IDs confirmados do resultado anterior
    const targets = [
      { os: "7807", pipefyId: "1343132179", nome: "Pedro 7807"    },
      { os: "4401", pipefyId: "1343239129", nome: "Fabiano 4401"  },
      { os: "9941", pipefyId: "1342560226", nome: "Sheila 9941"   },
      { os: "7631", pipefyId: "1342463023", nome: "Edileuza 7631" },
      { os: "2230", pipefyId: "1342167738", nome: "Roseane 2230"  },
      { os: "2750", pipefyId: "1340294528", nome: "Luana 2750"    },
    ];

    // Fases candidatas — tenta em ordem até alguma aceitar
    const CANDIDATE_PHASES = [
      "341638194", // Aguardando Aprovação (Enviados)
      "341638197", // Aguardando Orçamento (novos)
      "341638196", // possível fase intermediária
      "341638195", // possível fase intermediária
      "341638198", // possível fase intermediária
    ];

    const results = [];
    for (const target of targets) {
      // Primeiro: verificar fase atual do card
      let currentPhase = null;
      try {
        const qr = await fetch(PIPEFY_API, {
          method: "POST",
          headers: { "Content-Type": "application/json", Authorization: "Bearer " + (process.env.PIPEFY_TOKEN||"").trim() },
          body: JSON.stringify({ query: 'query { card(id: "' + target.pipefyId + '") { id current_phase { id name } } }' })
        });
        const qj = await qr.json();
        currentPhase = qj.data?.card?.current_phase;
      } catch(e) {}

      // Se já está em Aguardando Aprovação, pular
      if (currentPhase && (currentPhase.id === "341638194" || currentPhase.id === "341638197")) {
        results.push({ ...target, ok: true, skipped: true, currentPhase: currentPhase.name, msg: "Já está em " + currentPhase.name });
        continue;
      }

      // Tenta mover para cada fase candidata
      let moved = false;
      let lastErr = "";
      for (const phaseId of CANDIDATE_PHASES) {
        try {
          await pipefyMutation('mutation { moveCardToPhase(input: { card_id: "' + target.pipefyId + '", destination_phase_id: "' + phaseId + '" }) { card { id current_phase { id name } } } }');
          moved = true;
          results.push({ ...target, ok: true, movedTo: phaseId, currentPhase: currentPhase?.name || "?" });
          break;
        } catch(e) {
          lastErr = e.message;
        }
      }
      if (!moved) {
        results.push({ ...target, ok: false, currentPhase: currentPhase?.name || "?", error: lastErr });
      }
    }
    return res.status(200).json({ ok: true, results });
  }

  return res.status(404).json({ ok: false, error: "Acao nao encontrada: " + action });

  } catch(e) {
    return res.status(500).json({ ok: false, error: "Erro interno: " + e.message });
  }
};
