const UPSTASH_URL   = (process.env.UPSTASH_URL   || "").replace(/['"]/g,"").trim();
const UPSTASH_TOKEN = (process.env.UPSTASH_TOKEN || "").replace(/['"]/g,"").trim();
const PIPEFY_API    = "https://api.pipefy.com/graphql";
const TV_PIPE_ID    = "306904889";
const COLETA_KEY    = "tv_coleta_cards";
const BOARD_KEY     = "tv_board";

const COLETA_PHASES = [
  { id: "liberado_coleta",       name: "Liberado para Coleta"  },
  { id: "comunicacao_realizada", name: "Comunicação Realizada" },
  { id: "coleta_realizada",      name: "Coleta Realizada"      },
  { id: "remarcar",              name: "Remarcar"              },
  { id: "orcamento_registrado",  name: "Orçamento Registrado"  },
];

async function dbGet(key) {
  try {
    const r = await fetch(UPSTASH_URL + "/pipeline", {
      method: "POST",
      headers: { Authorization: "Bearer " + UPSTASH_TOKEN, "Content-Type": "application/json" },
      body: JSON.stringify([["GET", key]]),
    });
    const j = await r.json();
    return j[0]?.result ? JSON.parse(j[0].result) : null;
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
  if (j.errors) throw new Error(j.errors.map(e => e.message).join("; "));
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
    const phases = j.data?.pipe?.phases || [];
    return (phases.find(p => p.name.toLowerCase().includes("aguardando") && p.name.toLowerCase().includes("aprova"))
      || phases.find(p => p.name.toLowerCase().includes("aprovacao") || p.name.toLowerCase().includes("aprovação")))?.id || null;
  } catch(e) { return null; }
}

module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(200).end();
  const { action } = req.query;
  try {

  // ── GET load ──────────────────────────────────────────────────────────────
  if (action === "load") {
    const [coletaRaw, boardRaw] = await Promise.all([dbGet(COLETA_KEY), dbGet(BOARD_KEY)]);
    const coleta = coletaRaw || { cards: [] };
    const board  = boardRaw  || { cards: [] };
    // Sync: adiciona cards liberado_rota que ainda nao estao no coleta
    let changed = false;
    for (const bc of (board.cards || []).filter(c => c.phaseId === "liberado_rota")) {
      const id = String(bc.pipefyId);
      if (!coleta.cards.find(c => c.pipefyId === id)) {
        coleta.cards.unshift({
          pipefyId: id, nomeContato: bc.nomeContato || bc.title || "—",
          osCode: bc.osCode||null, endereco: bc.endereco||null,
          telefone: bc.telefone||null, descricao: bc.descricao||null,
          coletaPhase: "liberado_coleta", entradaEm: bc.movedAt||new Date().toISOString(),
          diagnostico: null,
        });
        changed = true;
      }
    }
    if (changed) await dbSet(COLETA_KEY, coleta);
    const byPhase = {};
    COLETA_PHASES.forEach(p => { byPhase[p.id] = []; });
    coleta.cards.forEach(c => { if (byPhase[c.coletaPhase||"liberado_coleta"]) byPhase[c.coletaPhase||"liberado_coleta"].push(c); });
    return res.status(200).json({ ok: true, phases: COLETA_PHASES, byPhase, total: coleta.cards.length });
  }

  // ── POST move — muda fase do card no coleta kanban ────────────────────────
  if (req.method === "POST" && action === "move") {
    const { pipefyId, phase } = req.body || {};
    if (!pipefyId || !phase) return res.status(400).json({ ok: false, error: "pipefyId e phase obrigatorios" });
    if (!COLETA_PHASES.find(p => p.id === phase)) return res.status(400).json({ ok: false, error: "Fase invalida" });
    const coleta = (await dbGet(COLETA_KEY)) || { cards: [] };
    let card = coleta.cards.find(c => String(c.pipefyId) === String(pipefyId));
    if (!card) {
      const board = (await dbGet(BOARD_KEY)) || { cards: [] };
      const bc = (board.cards||[]).find(c => String(c.pipefyId) === String(pipefyId));
      card = { pipefyId: String(pipefyId), nomeContato: bc?.nomeContato||bc?.title||"—", osCode: bc?.osCode||null, endereco: bc?.endereco||null, telefone: bc?.telefone||null, descricao: bc?.descricao||null, coletaPhase: "liberado_coleta", entradaEm: bc?.movedAt||new Date().toISOString(), diagnostico: null };
      coleta.cards.unshift(card);
    }
    card.coletaPhase = phase;
    card[phase+"Em"] = new Date().toISOString();
    await dbSet(COLETA_KEY, coleta);
    return res.status(200).json({ ok: true, card });
  }

  // ── POST diagnostico — registra, comenta no Pipefy, move p/ aguardando ────
  if (req.method === "POST" && action === "diagnostico") {
    const { pipefyId, texto } = req.body || {};
    if (!pipefyId || !texto?.trim()) return res.status(400).json({ ok: false, error: "pipefyId e texto obrigatorios" });
    const coleta = (await dbGet(COLETA_KEY)) || { cards: [] };
    const card = coleta.cards.find(c => String(c.pipefyId) === String(pipefyId));
    if (card) { card.coletaPhase = "orcamento_registrado"; card.diagnostico = texto.trim(); card.diagnosticoEm = new Date().toISOString(); }
    await dbSet(COLETA_KEY, coleta);
    let pipefyComment = { ok: false };
    try {
      const t = texto.trim().replace(/"/g, '\"').replace(/
/g, '\n');
      await pipefyMutation('mutation { createComment(input: { card_id: "'+pipefyId+'", text: "📋 Diagnóstico: '+t+'" }) { comment { id } } }');
      pipefyComment = { ok: true };
    } catch(e) { pipefyComment = { ok: false, error: e.message }; }
    let pipefyMove = { ok: false };
    try {
      const phaseId = await getAguardandoAprovacaoId();
      if (phaseId) { await pipefyMutation('mutation { moveCardToPhase(input: { card_id: "'+pipefyId+'", destination_phase_id: "'+phaseId+'" }) { card { id } } }'); pipefyMove = { ok: true, phaseId }; }
      else { pipefyMove = { ok: false, error: "Fase Aguardando Aprovação nao encontrada no pipe" }; }
    } catch(e) { pipefyMove = { ok: false, error: e.message }; }
    return res.status(200).json({ ok: true, card, pipefyComment, pipefyMove });
  }

  // ── GET fases — lista fases do pipe TV ────────────────────────────────────
  if (action === "fases") {
    return res.status(200).json({ ok: true, aguardandoAprovacaoId: await getAguardandoAprovacaoId(), coletaPhases: COLETA_PHASES });
  }

  return res.status(404).json({ ok: false, error: "Acao nao encontrada" });
  } catch(e) { return res.status(500).json({ ok: false, error: "Erro: " + e.message }); }
};
