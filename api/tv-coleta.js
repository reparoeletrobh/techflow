const UPSTASH_URL   = (process.env.UPSTASH_URL   || "").replace(/['"]/g,"").trim();
const UPSTASH_TOKEN = (process.env.UPSTASH_TOKEN  || "").replace(/['"]/g,"").trim();
const PIPEFY_API    = "https://api.pipefy.com/graphql";
const TV_PIPE_ID    = "306904889";
const COLETA_KEY    = "tv_coleta_cards";
const BOARD_KEY     = "tv_board";

const COLETA_PHASES = [
  { id: "liberado_coleta",       name: "Liberado para Coleta" },
  { id: "comunicacao_realizada", name: "Comunicacao Realizada" },
  { id: "coleta_realizada",      name: "Coleta Realizada" },
  { id: "remarcar",              name: "Remarcar" },
  { id: "orcamento_registrado",  name: "Orcamento Registrado" },
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

async function getAguardandoAprovacaoId() {
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
      return p.name.toLowerCase().includes("aguardando") && p.name.toLowerCase().includes("aprova");
    }) || phases.find(function(p) {
      return p.name.toLowerCase().includes("aprovacao") || p.name.toLowerCase().includes("aprovacao");
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
            nomeContato:  bc.nomeContato || bc.title || "â",
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
          nomeContato: (bc2 && (bc2.nomeContato || bc2.title)) || "â",
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
      if (card) {
        card.coletaPhase  = "orcamento_registrado";
        card.diagnostico  = texto.trim();
        card.diagnosticoEm = new Date().toISOString();
      }
      await dbSet(COLETA_KEY, coleta);

      var pipefyComment = { ok: false };
      try {
        var t = texto.trim().replace(/"/g, '\\"').replace(/\n/g, ' ');
        await pipefyMutation('mutation { createComment(input: { card_id: "' + pipefyId + '", text: "Diagnostico: ' + t + '" }) { comment { id } } }');
        pipefyComment = { ok: true };
      } catch(e) { pipefyComment = { ok: false, error: e.message }; }

      var pipefyMove = { ok: false };
      try {
        var phaseId = await getAguardandoAprovacaoId();
        if (phaseId) {
          await pipefyMutation('mutation { moveCardToPhase(input: { card_id: "' + pipefyId + '", destination_phase_id: "' + phaseId + '" }) { card { id } } }');
          pipefyMove = { ok: true, phaseId: phaseId };
        } else {
          pipefyMove = { ok: false, error: "Fase Aguardando Aprovacao nao encontrada" };
        }
      } catch(e) { pipefyMove = { ok: false, error: e.message }; }

      return res.status(200).json({ ok: true, card: card, pipefyComment: pipefyComment, pipefyMove: pipefyMove });
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

    return res.status(404).json({ ok: false, error: "Acao nao encontrada: " + action });

  } catch(e) {
    return res.status(500).json({ ok: false, error: "Erro interno: " + e.message });
  }
};
