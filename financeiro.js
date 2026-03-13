const PIPEFY_API    = "https://api.pipefy.com/graphql";
const PIPE_ID       = "305832912";
const BOARD_KEY     = "reparoeletro_board";
const FIN_KEY       = "reparoeletro_financeiro";

const UPSTASH_URL   = (process.env.UPSTASH_URL   || "").replace(/['"]/g, "").trim();
const UPSTASH_TOKEN = (process.env.UPSTASH_TOKEN || "").replace(/['"]/g, "").trim();

// Fases do sistema financeiro
const FIN_PHASES = [
  { id: "aguardando_dados",    name: "Aguardando Dados"    },
  { id: "emitir_nf",          name: "Emitir Nota Fiscal"  },
  { id: "faturamento",        name: "Faturamento"         },
  { id: "entrega_agendada",   name: "Entrega Agendada"    },
  { id: "entrega_liberada",   name: "Entrega Liberada"    },
  { id: "rota_criada",        name: "Rota Criada"         },
  { id: "item_coletado",      name: "Item Coletado"       },
];

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
  } catch (e) { return null; }
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
  } catch (e) { return false; }
}

// ── Pipefy ─────────────────────────────────────────────────────
async function pipefyQuery(query) {
  const res = await fetch(PIPEFY_API, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${(process.env.PIPEFY_TOKEN || "").trim()}`,
    },
    body: JSON.stringify({ query }),
  });
  const json = await res.json();
  if (json.errors) throw new Error(json.errors[0].message);
  return json.data;
}

// Busca cards na fase "Video Enviado"
async function fetchVideoEnviado() {
  const all = [];
  let cursor = null, hasNext = true;
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
                id title age updated_at
                fields { name value }
              }
            }
          }
        }
      }
    }`);
    const phases = data?.pipe?.phases || [];
    const phase  = phases.find(p => p.name.toLowerCase().replace(/\s/g,"").includes("videoenviado") ||
                                     p.name.toLowerCase().includes("video"));
    if (!phase) break;
    for (const { node } of phase.cards.edges) {
      const fields = node.fields || [];
      const nomeField = fields.find(f => f.name.toLowerCase().includes("nome") || f.name.toLowerCase().includes("contato"));
      const descField = fields.find(f => f.name.toLowerCase().includes("descri") || f.name.toLowerCase().includes("problema") || f.name.toLowerCase().includes("servi"));
      const nomeVal = nomeField?.value || "";
      const digitsMatch = nomeVal.match(/(\d{4})\D*$/);
      all.push({
        pipefyId:    String(node.id),
        title:       node.title || "Sem título",
        nomeContato: nomeVal || null,
        osCode:      digitsMatch ? digitsMatch[1] : null,
        descricao:   descField?.value || null,
        age:         node.age ?? null,
        updatedAt:   node.updated_at || null,
      });
    }
    hasNext = phase.cards.pageInfo?.hasNextPage ?? false;
    cursor  = phase.cards.pageInfo?.endCursor ?? null;
  }
  return all;
}

// Busca IDs que estão em ERP ou Finalizado no Pipefy
async function fetchFinalizadoIds() {
  try {
    const data = await pipefyQuery(`query {
      pipe(id: "${PIPE_ID}") {
        phases {
          name
          cards(first: 50) {
            edges { node { id } }
          }
        }
      }
    }`);
    const phases = data?.pipe?.phases || [];
    const ids = [];
    for (const ph of phases) {
      const n = ph.name.toLowerCase();
      if (n.includes("erp") || n.includes("finaliz") || n.includes("conclu")) {
        ph.cards.edges.forEach(e => ids.push(String(e.node.id)));
      }
    }
    return ids;
  } catch (e) {
    console.error("fetchFinalizadoIds:", e.message);
    return [];
  }
}

function defaultFin() {
  return { records: [], syncedIds: [] };
}

// ── Handler ────────────────────────────────────────────────────
module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(200).end();

  const { action } = req.query;

  // ── GET load ───────────────────────────────────────────────
  if (action === "load") {
    const fin = await dbGet(FIN_KEY) || defaultFin();
    if (!Array.isArray(fin.syncedIds)) fin.syncedIds = [];
    return res.status(200).json({ ok: true, records: fin.records, phases: FIN_PHASES });
  }

  // ── GET sync — varre Pipefy em background ──────────────────
  if (action === "sync") {
    const fin = await dbGet(FIN_KEY) || defaultFin();
    if (!Array.isArray(fin.syncedIds))  fin.syncedIds  = [];
    if (!Array.isArray(fin.records))    fin.records    = [];

    let newCount = 0, removedCount = 0, pipefyError = null;

    // 1. Importa cards de "Video Enviado"
    try {
      const videoCards = await fetchVideoEnviado();
      for (const c of videoCards) {
        if (fin.syncedIds.includes(c.pipefyId)) continue;
        fin.records.unshift({
          id:          c.pipefyId,
          pipefyId:    c.pipefyId,
          osCode:      c.osCode,
          nomeContato: c.nomeContato,
          title:       c.title,
          descricao:   c.descricao,
          age:         c.age,
          cpfCnpj:     null,
          phaseId:     "aguardando_dados",
          createdAt:   new Date().toISOString(),
          movedAt:     new Date().toISOString(),
          history:     [{ phaseId: "aguardando_dados", ts: new Date().toISOString() }],
        });
        fin.syncedIds.push(c.pipefyId);
        newCount++;
      }
    } catch (e) { pipefyError = e.message; }

    // 2. Remove fichas em "Item Coletado" cujo card foi para ERP/Finalizado
    try {
      const finIds = await fetchFinalizadoIds();
      if (finIds.length > 0) {
        const before = fin.records.length;
        fin.records = fin.records.filter(r => {
          if (r.phaseId !== "item_coletado") return true;
          return !finIds.includes(r.pipefyId);
        });
        removedCount = before - fin.records.length;
      }
    } catch (e) { console.error("finalizado check:", e.message); }

    if (newCount > 0 || removedCount > 0) await dbSet(FIN_KEY, fin);
    return res.status(200).json({ ok: true, newCount, removedCount, pipefyError });
  }

  // ── POST set-cpf ───────────────────────────────────────────
  // Cadastra CPF/CNPJ e move para Emitir NF
  if (req.method === "POST" && action === "set-cpf") {
    const { id, cpfCnpj } = req.body || {};
    if (!id || !cpfCnpj) return res.status(400).json({ ok: false, error: "id e cpfCnpj obrigatórios" });
    const fin = await dbGet(FIN_KEY) || defaultFin();
    const rec = fin.records.find(r => r.id === id);
    if (!rec) return res.status(404).json({ ok: false, error: "Ficha não encontrada" });
    rec.cpfCnpj = cpfCnpj.trim();
    rec.phaseId = "emitir_nf";
    rec.movedAt = new Date().toISOString();
    rec.history = [...(rec.history || []), { phaseId: "emitir_nf", ts: rec.movedAt }];
    await dbSet(FIN_KEY, fin);
    return res.status(200).json({ ok: true, record: rec });
  }

  // ── POST emitir-nf ─────────────────────────────────────────
  // Marca NF como emitida e move para Faturamento
  if (req.method === "POST" && action === "emitir-nf") {
    const { id } = req.body || {};
    if (!id) return res.status(400).json({ ok: false, error: "id obrigatório" });
    const fin = await dbGet(FIN_KEY) || defaultFin();
    const rec = fin.records.find(r => r.id === id);
    if (!rec) return res.status(404).json({ ok: false, error: "Ficha não encontrada" });
    if (rec.phaseId !== "emitir_nf") return res.status(400).json({ ok: false, error: "Ficha não está em Emitir NF" });
    rec.nfEmitidaAt = new Date().toISOString();
    rec.phaseId     = "faturamento";
    rec.movedAt     = rec.nfEmitidaAt;
    rec.history     = [...(rec.history || []), { phaseId: "faturamento", ts: rec.movedAt }];
    await dbSet(FIN_KEY, fin);
    return res.status(200).json({ ok: true, record: rec });
  }

  // ── POST mover ─────────────────────────────────────────────
  // Move entre fases manualmente (faturamento → entrega_agendada/liberada, etc.)
  if (req.method === "POST" && action === "mover") {
    const { id, phaseId } = req.body || {};
    if (!id || !phaseId) return res.status(400).json({ ok: false, error: "id e phaseId obrigatórios" });

    // Valida transições permitidas
    const allowed = {
      faturamento:      ["entrega_agendada", "entrega_liberada"],
      entrega_agendada: ["entrega_liberada"],
      entrega_liberada: ["rota_criada"],
      rota_criada:      ["item_coletado"],
    };

    const fin = await dbGet(FIN_KEY) || defaultFin();
    const rec = fin.records.find(r => r.id === id);
    if (!rec) return res.status(404).json({ ok: false, error: "Ficha não encontrada" });

    const ok = allowed[rec.phaseId]?.includes(phaseId);
    if (!ok) return res.status(400).json({ ok: false, error: `Transição não permitida: ${rec.phaseId} → ${phaseId}` });

    rec.phaseId = phaseId;
    rec.movedAt = new Date().toISOString();
    rec.history = [...(rec.history || []), { phaseId, ts: rec.movedAt }];
    if (phaseId === "entrega_agendada" || phaseId === "entrega_liberada") rec.paidAt = rec.movedAt;
    await dbSet(FIN_KEY, fin);
    return res.status(200).json({ ok: true, record: rec });
  }

  // ── POST excluir ───────────────────────────────────────────
  if (req.method === "POST" && action === "excluir") {
    const { id } = req.body || {};
    const fin = await dbGet(FIN_KEY) || defaultFin();
    fin.records = fin.records.filter(r => r.id !== id);
    await dbSet(FIN_KEY, fin);
    return res.status(200).json({ ok: true });
  }

  return res.status(404).json({ ok: false, error: "Ação não encontrada" });
};
