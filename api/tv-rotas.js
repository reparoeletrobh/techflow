const UPSTASH_URL   = (process.env.UPSTASH_URL   || "").replace(/['"]/g,"").trim();
const UPSTASH_TOKEN = (process.env.UPSTASH_TOKEN || "").replace(/['"]/g,"").trim();

const ROTAS_KEY  = "tv_rotas";
const BOARD_KEY  = "tv_board";

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

function defaultRotas() { return { rotas: [], contador: 0 }; }

module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(200).end();

  const { action } = req.query;
  try {

  // ── GET load ──────────────────────────────────────────────────
  if (action === "load") {
    const [dbRaw, boardRaw] = await Promise.all([
      dbGet(ROTAS_KEY),
      dbGet(BOARD_KEY),
    ]);
    const db    = dbRaw    || defaultRotas();
    const board = boardRaw || { cards: [] };
    const fila  = (board.cards || []).filter(function(c) { return c.phaseId === "liberado_rota"; });
    return res.status(200).json({ ok: true, rotas: db.rotas || [], fila: fila, contador: db.contador || 0 });
  }

  // ── POST criar-rota ───────────────────────────────────────────
  if (req.method === "POST" && action === "criar-rota") {
    const { cardIds } = req.body || {};
    if (!Array.isArray(cardIds) || !cardIds.length)
      return res.status(400).json({ ok: false, error: "cardIds obrigatório" });
    const [dbRaw2, boardRaw2] = await Promise.all([
      dbGet(ROTAS_KEY),
      dbGet(BOARD_KEY),
    ]);
    const db    = dbRaw2    || defaultRotas();
    const board = boardRaw2 || { cards: [] };
    db.contador = (db.contador || 0) + 1;
    const numero = db.contador;
    const cards = cardIds.map(id => {
      const c = (board.cards || []).find(x => x.pipefyId === String(id));
      return {
        pipefyId:    String(id),
        nomeContato: c?.nomeContato || c?.title || "—",
        endereco:    c?.endereco    || "",
        telefone:    c?.telefone    || "",
        descricao:   c?.descricao   || "",
        osCode:      c?.osCode      || "",
        status:      "pendente",
        relatorio:   null,
      };
    });
    // Último ponto fixo = oficina
    cards.push({
      pipefyId:    "oficina",
      nomeContato: "TV Assistência — Oficina",
      endereco:    "Rua Alcides Gonçalves 106, Camargos, Belo Horizonte",
      telefone:    "",
      status:      "destino_final",
    });
    const rota = {
      id:        "rota-" + Date.now(),
      numero,
      label:     "Rota " + numero,
      status:    "andamento",
      cards,
      criadaEm:  new Date().toISOString(),
    };
    db.rotas.unshift(rota);
    await dbSet(ROTAS_KEY, db);
    return res.status(200).json({ ok: true, rota });
  }

  // ── POST marcar-coletado ──────────────────────────────────────
  if (req.method === "POST" && action === "marcar-coletado") {
    const { rotaId, pipefyId } = req.body || {};
    const db = await dbGet(ROTAS_KEY) || defaultRotas();
    const rota = db.rotas.find(r => r.id === rotaId);
    if (!rota) return res.status(404).json({ ok: false, error: "Rota não encontrada" });
    const card = rota.cards.find(c => c.pipefyId === String(pipefyId));
    if (!card) return res.status(404).json({ ok: false, error: "Card não encontrado" });
    card.status    = "coletado";
    card.coletadoEm = new Date().toISOString();
    // Se todos os cards (exceto oficina) foram coletados → concluir rota
    const pendentes = rota.cards.filter(c => c.status === "pendente");
    if (!pendentes.length) rota.status = "concluida";
    await dbSet(ROTAS_KEY, db);
    return res.status(200).json({ ok: true, rota });
  }

  // ── POST salvar-relatorio ─────────────────────────────────────
  if (req.method === "POST" && action === "salvar-relatorio") {
    const { rotaId, pipefyId, foto, descricao, assinatura } = req.body || {};
    const db = await dbGet(ROTAS_KEY) || defaultRotas();
    const rota = db.rotas.find(r => r.id === rotaId);
    if (!rota) return res.status(404).json({ ok: false, error: "Rota não encontrada" });
    const card = rota.cards.find(c => c.pipefyId === String(pipefyId));
    if (!card) return res.status(404).json({ ok: false, error: "Card não encontrado" });
    card.relatorio = { foto: foto || null, descricao: descricao || "", assinatura: assinatura || null, savedAt: new Date().toISOString() };
    await dbSet(ROTAS_KEY, db);
    return res.status(200).json({ ok: true });
  }

  // ── POST concluir-rota ────────────────────────────────────────
  if (req.method === "POST" && action === "concluir-rota") {
    const { rotaId } = req.body || {};
    const db = await dbGet(ROTAS_KEY) || defaultRotas();
    const rota = db.rotas.find(r => r.id === rotaId);
    if (!rota) return res.status(404).json({ ok: false, error: "Rota não encontrada" });
    rota.status = "concluida";
    rota.concluidaEm = new Date().toISOString();
    await dbSet(ROTAS_KEY, db);
    return res.status(200).json({ ok: true, rota });
  }

    return res.status(404).json({ ok: false, error: "Ação não encontrada" });
  } catch(e) {
    return res.status(200).json({ ok: false, error: "Erro interno: " + e.message });
  }
};
