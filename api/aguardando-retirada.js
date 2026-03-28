// api/aguardando-retirada.js — Espelho da fase Aguardando Retirada do painel Técnico
// Carrega cards do board Redis, busca telefone/nome do Pipefy para os parados +24h

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

// Busca campos (telefone, nome, desc) de um conjunto de cards pelo Pipefy
async function fetchCardFields(pipefyIds) {
  if (!pipefyIds.length) return {};
  try {
    // Busca a fase Aguardando Retirada do Pipefy para pegar todos os cards com campos
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
    const idSet  = new Set(pipefyIds.map(String));
    const result = {};

    for (const ph of phases) {
      const l = ph.name.toLowerCase();
      if (!l.includes("aguardando") || !l.includes("retirada")) continue;
      for (const { node } of (ph.cards?.edges || [])) {
        if (!idSet.has(String(node.id))) continue;
        const fields = node.fields || [];
        const tel  = fields.find(f => f.name.toLowerCase().includes("telefone") || f.name.toLowerCase().includes("fone"))?.value || "";
        const nome = fields.find(f => f.name.toLowerCase().includes("nome"))?.value || node.title;
        const desc = fields.find(f => f.name.toLowerCase().includes("descri"))?.value || "";
        result[String(node.id)] = { tel, nome, desc, title: node.title };
      }
    }
    return result;
  } catch(e) {
    console.error("fetchCardFields:", e.message);
    return {};
  }
}

module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(200).end();

  const action = req.query.action || "";

  // ── GET load — todos os cards de Aguardando Retirada (painel principal) ──
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
          diffH, diffD, atrasado,
        };
      })
      .sort((a, b) => b.diffH - a.diffH);

    return res.status(200).json({
      ok: true, cards,
      total: cards.length,
      totalAtrasados: cards.filter(c => c.atrasado).length,
    });
  }

  // ── GET parados — cards +24h com telefone buscado do Pipefy ─────────────
  if (action === "parados") {
    const board = await dbGet(BOARD_KEY);
    if (!board) return res.status(200).json({ ok: false, error: "Board não encontrado" });

    const agora   = Date.now();
    const VINTE4H = 24 * 60 * 60 * 1000;

    // Filtra só os parados +24h do board
    const parados = (board.cards || [])
      .filter(c => {
        if (c.phaseId !== "aguardando_ret") return false;
        const ts = c.movedAt ? new Date(c.movedAt).getTime() : 0;
        return ts > 0 && (agora - ts) >= VINTE4H;
      });

    if (!parados.length) {
      return res.status(200).json({ ok: true, cards: [], total: 0 });
    }

    // Busca dados do Pipefy para esses cards
    const pipefyIds = parados
      .filter(c => c.pipefyId && !c.pipefyId.includes("local-") && !c.pipefyId.includes("-split-"))
      .map(c => c.pipefyId);

    const pipefyData = await fetchCardFields(pipefyIds);

    const cards = parados.map(c => {
      const ts     = new Date(c.movedAt).getTime();
      const diffMs = agora - ts;
      const diffH  = Math.floor(diffMs / (1000 * 60 * 60));
      const diffD  = Math.floor(diffH / 24);

      const pipefy  = pipefyData[String(c.pipefyId)] || {};
      const titleStr = c.title || c.nomeContato || "";
      const m    = (pipefy.title || titleStr).match(/^(.*?)\s+(\d{3,6})$/);
      const nome = m ? m[1].trim() : (pipefy.nome || c.nomeContato || titleStr || "—");
      const os   = m ? m[2] : (c.osCode || null);
      const tel  = pipefy.tel || c.telefone || null;

      return {
        pipefyId: c.pipefyId || c.id,
        osCode:   os,
        nome,
        title:    c.title || "",
        telefone: tel,
        desc:     pipefy.desc || "",
        movedAt:  c.movedAt,
        diffH, diffD,
      };
    }).sort((a, b) => b.diffH - a.diffH);

    return res.status(200).json({ ok: true, cards, total: cards.length });
  }

  return res.status(404).json({ ok: false, error: "Ação não encontrada" });
};
