// api/aguardando-retirada.js — Espelho da fase Aguardando Retirada do painel Técnico
const BOARD_KEY     = "reparoeletro_board";
const PIPE_ID       = "305832912";
const PIPEFY_API    = "https://api.pipefy.com/graphql";
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
  if (j.errors) throw new Error(j.errors.map(e => e.message).join("; "));
  return j.data;
}

// Busca telefone e nome de múltiplos cards pelo ID individual
async function fetchCardsTelefone(pipefyIds) {
  if (!pipefyIds.length) return {};
  // Busca os cards em batch usando aliases GraphQL
  const aliases = pipefyIds.map((id, i) =>
    `c${i}: card(id: "${id}") { id title fields { name value } }`
  ).join("\n");

  try {
    const data = await pipefyQuery(`query { ${aliases} }`);
    const result = {};
    pipefyIds.forEach((id, i) => {
      const card   = data[`c${i}`];
      if (!card) return;
      const fields = card.fields || [];
      const tel  = fields.find(f =>
        f.name.toLowerCase().includes("telefone") ||
        f.name.toLowerCase().includes("fone") ||
        f.name.toLowerCase().includes("celular")
      )?.value || null;
      const nome = fields.find(f => f.name.toLowerCase().includes("nome"))?.value || card.title;
      const desc = fields.find(f => f.name.toLowerCase().includes("descri"))?.value || "";
      result[String(id)] = { tel, nome, desc, title: card.title };
    });
    return result;
  } catch(e) {
    console.error("fetchCardsTelefone:", e.message);
    return {};
  }
}

module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(200).end();

  const action = req.query.action || "";

  // ── GET load — todos os cards de Aguardando Retirada ────────────────────
  if (action === "load") {
    const board = await dbGet(BOARD_KEY);
    if (!board) return res.status(200).json({ ok: false, error: "Board não encontrado" });

    const agora   = Date.now();
    const VINTE4H = 24 * 60 * 60 * 1000;

    const cards = (board.cards || [])
      .filter(c => c.phaseId === "aguardando_ret")
      .map(c => {
        const ts       = c.movedAt ? new Date(c.movedAt).getTime() : 0;
        const diffMs   = ts ? agora - ts : 0;
        const diffH    = Math.floor(diffMs / (1000 * 60 * 60));
        const diffD    = Math.floor(diffH / 24);
        const atrasado = ts > 0 && diffMs >= VINTE4H;
        const titleStr = c.title || c.nomeContato || "";
        const m        = titleStr.match(/^(.*?)\s+(\d{3,6})$/);
        const nome     = m ? m[1].trim() : (c.nomeContato || titleStr || "—");
        const os       = m ? m[2] : (c.osCode || null);
        return { pipefyId: c.pipefyId || c.id, osCode: os, nome, title: c.title || "", telefone: c.telefone || null, movedAt: c.movedAt || null, diffH, diffD, atrasado };
      })
      .sort((a, b) => b.diffH - a.diffH);

    return res.status(200).json({ ok: true, cards, total: cards.length, totalAtrasados: cards.filter(c => c.atrasado).length });
  }

  // ── GET parados — cards +24h com telefone buscado do Pipefy ─────────────
  if (action === "parados") {
    const board = await dbGet(BOARD_KEY);
    if (!board) return res.status(200).json({ ok: false, error: "Board não encontrado" });

    const agora   = Date.now();
    const VINTE4H = 24 * 60 * 60 * 1000;

    const parados = (board.cards || []).filter(c => {
      if (c.phaseId !== "aguardando_ret") return false;
      const ts = c.movedAt ? new Date(c.movedAt).getTime() : 0;
      return ts > 0 && (agora - ts) >= VINTE4H;
    });

    if (!parados.length) return res.status(200).json({ ok: true, cards: [], total: 0 });

    // IDs reais do Pipefy (ignora local-* e *-split-*)
    const pipefyIds = parados
      .map(c => c.pipefyId || c.id)
      .filter(id => id && !String(id).includes("local-") && !String(id).includes("-split-"));

    // Busca telefone/nome diretamente por card ID
    const pipefyData = await fetchCardsTelefone(pipefyIds);

    const cards = parados.map(c => {
      const ts    = new Date(c.movedAt).getTime();
      const diffH = Math.floor((agora - ts) / (1000 * 60 * 60));
      const diffD = Math.floor(diffH / 24);
      const pid   = String(c.pipefyId || c.id);
      const pdata = pipefyData[pid] || {};

      const titleStr = pdata.title || c.title || c.nomeContato || "";
      const m        = titleStr.match(/^(.*?)\s+(\d{3,6})$/);
      const nome     = m ? m[1].trim() : (pdata.nome || c.nomeContato || titleStr || "—");
      const os       = m ? m[2] : (c.osCode || null);
      const tel      = pdata.tel || c.telefone || null;

      return { pipefyId: pid, osCode: os, nome, title: c.title || "", telefone: tel, desc: pdata.desc || "", movedAt: c.movedAt, diffH, diffD };
    }).sort((a, b) => b.diffH - a.diffH);

    return res.status(200).json({ ok: true, cards, total: cards.length });
  }

  return res.status(404).json({ ok: false, error: "Ação não encontrada" });
};
