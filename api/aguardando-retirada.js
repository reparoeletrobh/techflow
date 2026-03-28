// api/aguardando-retirada.js — Espelho da fase Aguardando Retirada do painel Técnico
const BOARD_KEY     = "reparoeletro_board";
const UPSTASH_URL   = (process.env.UPSTASH_URL   || "").replace(/['"]/g,"").trim();
const UPSTASH_TOKEN = (process.env.UPSTASH_TOKEN || "").replace(/['"]/g,"").trim();

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

module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(200).end();

  const action = req.query.action || "";

  // ── GET load — espelha fase aguardando_ret do board técnico ─────────────
  if (action === "load") {
    const board = await dbGet(BOARD_KEY);
    if (!board) return res.status(200).json({ ok: false, error: "Board não encontrado" });

    const agora   = Date.now();
    const VINTE4H = 24 * 60 * 60 * 1000;

    const cards = (board.cards || [])
      .filter(c => c.phaseId === "aguardando_ret")
      .map(c => {
        const ts     = c.movedAt ? new Date(c.movedAt).getTime() : 0;
        const diffMs = ts ? agora - ts : 0;
        const diffH  = Math.floor(diffMs / (1000 * 60 * 60));
        const diffD  = Math.floor(diffH / 24);
        const atrasado = ts > 0 && diffMs >= VINTE4H;

        // Extrai nome e OS do título
        const titleStr = c.title || c.nomeContato || "";
        const m    = titleStr.match(/^(.*?)\s+(\d{3,6})$/);
        const nome = m ? m[1].trim() : (c.nomeContato || titleStr || "—");
        const os   = m ? m[2] : (c.osCode || null);

        return {
          pipefyId: c.pipefyId || c.id,
          osCode:   os,
          nome,
          title:    c.title || "",
          telefone: c.telefone || null,
          movedAt:  c.movedAt || null,
          diffH,
          diffD,
          atrasado,
        };
      })
      .sort((a, b) => b.diffH - a.diffH); // mais antigos primeiro

    return res.status(200).json({
      ok: true,
      cards,
      total:          cards.length,
      totalAtrasados: cards.filter(c => c.atrasado).length,
    });
  }

  return res.status(404).json({ ok: false, error: "Ação não encontrada" });
};
