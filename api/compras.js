const UPSTASH_URL   = (process.env.UPSTASH_URL   || "").replace(/['"]/g, "").trim();
const UPSTASH_TOKEN = (process.env.UPSTASH_TOKEN || "").replace(/['"]/g, "").trim();
const BOARD_KEY     = "reparoeletro_board";

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

async function dbSet(key, value) {
  try {
    const r = await fetch(`${UPSTASH_URL}/pipeline`, {
      method: "POST",
      headers: { Authorization: `Bearer ${UPSTASH_TOKEN}`, "Content-Type": "application/json" },
      body: JSON.stringify([["SET", key, JSON.stringify(value)]]),
    });
    const j = await r.json();
    return j[0]?.result === "OK";
  } catch(e) { return false; }
}

module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(200).end();

  const { action } = req.query;

  // ── GET load — retorna cards em comprar_peca e aguardando_peca e peca_disponivel
  if (action === "load") {
    const board = await dbGet(BOARD_KEY);
    if (!board) return res.status(200).json({ ok: true, comprar: [], aguardando: [], disponivel: [] });
    const cards = board.cards || [];
    return res.status(200).json({
      ok: true,
      comprar:    cards.filter(c => c.phaseId === "comprar_peca"),
      aguardando: cards.filter(c => c.phaseId === "aguardando_peca"),
      disponivel: cards.filter(c => c.phaseId === "peca_disponivel"),
    });
  }

  // ── POST update-dados — atualiza fotos e descrição de um card em comprar_peca
  if (req.method === "POST" && action === "update-dados") {
    const { pipefyId, fotosCompra, descricaoCompra } = req.body || {};
    if (!pipefyId) return res.status(400).json({ ok: false, error: "pipefyId obrigatório" });

    const board = await dbGet(BOARD_KEY);
    if (!board) return res.status(404).json({ ok: false, error: "Board não encontrado" });

    const card = board.cards.find(c => c.pipefyId === String(pipefyId));
    if (!card) return res.status(404).json({ ok: false, error: "Card não encontrado" });

    // Adiciona novas fotos às existentes (sem duplicar)
    if (fotosCompra && fotosCompra.length) {
      card.fotosCompra = [...(card.fotosCompra || []), ...fotosCompra].slice(0, 12);
    }
    if (descricaoCompra !== undefined) card.descricaoCompra = descricaoCompra;

    await dbSet(BOARD_KEY, board);
    return res.status(200).json({ ok: true, card });
  }

  // ── POST atualizar — move card para aguardando_peca com tipo de compra
  if (req.method === "POST" && action === "atualizar") {
    const { pipefyId, tipoCompra, previsao } = req.body || {};
    if (!pipefyId || !tipoCompra) return res.status(400).json({ ok: false, error: "pipefyId e tipoCompra obrigatórios" });

    const board = await dbGet(BOARD_KEY);
    if (!board) return res.status(404).json({ ok: false, error: "Board não encontrado" });

    const card = board.cards.find(c => c.pipefyId === String(pipefyId));
    if (!card) return res.status(404).json({ ok: false, error: "Card não encontrado" });

    card.phaseId    = "aguardando_peca";
    card.movedAt    = new Date().toISOString();
    card.tipoCompra = tipoCompra; // "local" | "online"
    card.previsao   = previsao || null;
    card.alertaCompra = tipoCompra === "local"
      ? "🏪 Peça a caminho — compra local"
      : `📦 Compra online — previsão: ${previsao || "a definir"}`;

    await dbSet(BOARD_KEY, board);
    return res.status(200).json({ ok: true, card });
  }

  // ── POST peca-disponivel — move para peca_disponivel
  if (req.method === "POST" && action === "peca-disponivel") {
    const { pipefyId } = req.body || {};
    if (!pipefyId) return res.status(400).json({ ok: false, error: "pipefyId obrigatório" });

    const board = await dbGet(BOARD_KEY);
    if (!board) return res.status(404).json({ ok: false, error: "Board não encontrado" });

    const card = board.cards.find(c => c.pipefyId === String(pipefyId));
    if (!card) return res.status(404).json({ ok: false, error: "Card não encontrado" });

    card.phaseId  = "peca_disponivel";
    card.movedAt  = new Date().toISOString();
    card.alertaCompra = "✅ Peça disponível";

    await dbSet(BOARD_KEY, board);
    return res.status(200).json({ ok: true, card });
  }

  return res.status(404).json({ ok: false, error: "Ação não encontrada" });
};
