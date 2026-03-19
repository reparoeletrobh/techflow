// garantia.js — Dash de Garantia
const UPSTASH_URL   = (process.env.UPSTASH_URL   || "").replace(/['"]/g,"").trim();
const UPSTASH_TOKEN = (process.env.UPSTASH_TOKEN || "").replace(/['"]/g,"").trim();
const PIPEFY_API    = "https://api.pipefy.com/graphql";
const PIPE_ID       = "305832912";
const GARANTIA_KEY  = "reparoeletro_garantia";

const FASES = [
  { id: "garantia_acionada",  name: "Garantia Acionada" },
  { id: "em_analise",         name: "Em Análise"        },
  { id: "servico_finalizado", name: "Serviço Finalizado" },
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

async function dbSet(key, value) {
  try {
    const r = await fetch(UPSTASH_URL + "/pipeline", {
      method: "POST",
      headers: { Authorization: "Bearer " + UPSTASH_TOKEN, "Content-Type": "application/json" },
      body: JSON.stringify([["SET", key, JSON.stringify(value)]]),
    });
    const j = await r.json();
    return j[0]?.result === "OK";
  } catch(e) { return false; }
}

async function pipefyQuery(query) {
  const r = await fetch(PIPEFY_API, {
    method: "POST",
    headers: { "Content-Type": "application/json", Authorization: "Bearer " + (process.env.PIPEFY_TOKEN||"").trim() },
    body: JSON.stringify({ query }),
  });
  const j = await r.json();
  if (j.errors) throw new Error(j.errors[0].message);
  return j.data;
}

function defaultDB() { return { fichas: [], syncedIds: [] }; }

// Busca cards na fase "RS" do Pipefy
async function fetchRSCards() {
  const data = await pipefyQuery(
    "query { pipe(id: \"" + PIPE_ID + "\") { phases { name cards(first: 50) { edges { node { id title fields { name value } } } } } } }"
  );
  const phases = data?.pipe?.phases || [];
  const rsPhase = phases.find(p => p.name.toLowerCase().trim() === "rs");
  if (!rsPhase) return [];
  return (rsPhase.cards?.edges || []).map(({ node }) => {
    const fields    = node.fields || [];
    const get       = kw => fields.find(f => f.name.toLowerCase().includes(kw))?.value || "";
    const nomeVal   = get("nome");
    const m         = (node.title || "").match(/^(.*?)\s+(\d{3,6})$/);
    return {
      pipefyId:    String(node.id),
      title:       node.title || "",
      osCode:      m ? m[2] : null,
      nomeContato: nomeVal || (m ? m[1].trim() : node.title),
      telefone:    get("telefone") || get("fone"),
      descricao:   get("descri"),
    };
  });
}

// Verifica se um card foi para Finalizado/ERP no Pipefy
async function fetchFinalizadoIds(pipefyIds) {
  if (!pipefyIds.length) return new Set();
  const finalizado = new Set();
  // Busca fase atual de cada card individualmente (até 10 por vez)
  const chunks = [];
  for (let i = 0; i < pipefyIds.length; i += 5) chunks.push(pipefyIds.slice(i, i+5));
  for (const chunk of chunks) {
    try {
      const fields = chunk.map((id, i) =>
        "c" + i + ": card(id: \"" + id + "\") { id current_phase { name } }"
      ).join(" ");
      const data = await pipefyQuery("query { " + fields + " }");
      for (const key of Object.keys(data || {})) {
        const card = data[key];
        if (!card) { finalizado.add(chunk[Object.keys(data).indexOf(key)]); continue; }
        const n = (card.current_phase?.name || "").toLowerCase();
        if (n.includes("finaliz") || n.includes("erp") || n.includes("descart") || n.includes("conclu")) {
          finalizado.add(String(card.id));
        }
      }
    } catch(e) {}
  }
  return finalizado;
}

module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(200).end();
  const { action } = req.query;

  // ── GET load ──────────────────────────────────────────────
  if (action === "load") {
    const db = await dbGet(GARANTIA_KEY) || defaultDB();
    return res.status(200).json({ ok: true, fichas: db.fichas || [], fases: FASES });
  }

  // ── GET sync — busca fase RS do Pipefy + remove finalizados
  if (action === "sync") {
    const db = await dbGet(GARANTIA_KEY) || defaultDB();
    if (!Array.isArray(db.fichas))    db.fichas    = [];
    if (!Array.isArray(db.syncedIds)) db.syncedIds = [];
    let added = 0, removed = 0, pipefyError = null;

    // Se não há fichas ativas, limpa syncedIds para permitir reimportação
    if (db.fichas.length === 0) {
      db.syncedIds = [];
    } else {
      // Mantém syncedIds só dos que ainda têm ficha ativa
      const idsAtivos = new Set(db.fichas.map(f => f.pipefyId));
      db.syncedIds = db.syncedIds.filter(id => idsAtivos.has(id));
    }

    try {
      // 1. Busca novos cards em RS
      const cards = await fetchRSCards();
      const idsNaFaseRS = new Set(cards.map(c => c.pipefyId));

      // Remove fichas que saíram da fase RS (mas mantém as que foram movidas internamente)
      // Só remove pelo sync se a ficha ainda está na fase inicial (garantia_acionada)
      // e não está mais em RS no Pipefy
      const before = db.fichas.length;
      
      // 2. Adiciona fichas novas
      for (const card of cards) {
        if (db.syncedIds.includes(card.pipefyId)) continue;
        db.fichas.unshift({
          id:          card.pipefyId,
          pipefyId:    card.pipefyId,
          title:       card.title,
          osCode:      card.osCode,
          nomeContato: card.nomeContato,
          telefone:    card.telefone,
          descricao:   card.descricao,
          phaseId:     FASES[0].id, // Garantia Acionada
          createdAt:   new Date().toISOString(),
          movedAt:     null, // só recebe valor quando usuario mover entre fases
        });
        db.syncedIds.push(card.pipefyId);
        added++;
      }

      // Auto-remove fichas em Serviço Finalizado
      const beforeFin = db.fichas.length;
      db.fichas = db.fichas.filter(f => f.phaseId !== "servico_finalizado");
      const removedFin = beforeFin - db.fichas.length;
      if (removedFin > 0) removed += removedFin;

      if (added > 0 || removed > 0) await dbSet(GARANTIA_KEY, db);
    } catch(e) { pipefyError = e.message; }

    return res.status(200).json({ ok: true, added, removed, pipefyError });
  }

  // ── POST mover — move ficha entre fases internas
  if (req.method === "POST" && action === "mover") {
    const { id, phaseId } = req.body || {};
    if (!id || !phaseId) return res.status(400).json({ ok: false, error: "id e phaseId obrigatorios" });
    if (!FASES.find(f => f.id === phaseId)) return res.status(400).json({ ok: false, error: "Fase invalida" });
    const db = await dbGet(GARANTIA_KEY) || defaultDB();
    const f  = db.fichas.find(x => x.id === id);
    if (!f) return res.status(404).json({ ok: false, error: "Ficha nao encontrada" });
    f.phaseId = phaseId;
    f.movedAt = new Date().toISOString();
    await dbSet(GARANTIA_KEY, db);
    return res.status(200).json({ ok: true, ficha: f });
  }

  // ── POST excluir
  if (req.method === "POST" && action === "excluir") {
    const { id } = req.body || {};
    const db = await dbGet(GARANTIA_KEY) || defaultDB();
    db.fichas = db.fichas.filter(f => f.id !== id);
    // Mantém syncedId — impede reimportação no próximo sync
    await dbSet(GARANTIA_KEY, db);
    return res.status(200).json({ ok: true });
  }

  // ── POST limpar-finalizados — remove Serviço Finalizado
  if (req.method === "POST" && action === "limpar-finalizados") {
    const db = await dbGet(GARANTIA_KEY) || defaultDB();
    const before = db.fichas.length;
    db.fichas = db.fichas.filter(f => f.phaseId !== "servico_finalizado");
    const removed = before - db.fichas.length;
    await dbSet(GARANTIA_KEY, db);
    return res.status(200).json({ ok: true, removed });
  }

  // ── POST reset-moveat — zera movedAt de fichas nunca movidas pelo usuário
  if (req.method === "POST" && action === "reset-moveat") {
    const db = await dbGet(GARANTIA_KEY) || defaultDB();
    let count = 0;
    db.fichas.forEach(f => {
      // Se está na fase inicial, nunca foi movida pelo usuário
      if (f.phaseId === FASES[0].id) {
        f.movedAt = null;
        count++;
      }
    });
    await dbSet(GARANTIA_KEY, db);
    return res.status(200).json({ ok: true, reset: count });
  }

  // ── POST limpar-nao-movidas-hoje ─────────────────────────────
  if (req.method === "POST" && action === "limpar-nao-movidas-hoje") {
    const db = await dbGet(GARANTIA_KEY) || defaultDB();

    // Meia-noite BRT de hoje em UTC
    function toBRT(d) { return new Date(new Date(d).toLocaleString("en-US",{timeZone:"America/Sao_Paulo"})); }
    const nowBRT = toBRT(new Date()); nowBRT.setHours(0,0,0,0);
    const todayUTC = new Date(nowBRT.getTime() + 3*60*60*1000);

    const before = db.fichas.length;
    // Remove: fichas nunca movidas (movedAt null) + fichas movidas antes de hoje
    db.fichas = db.fichas.filter(f => {
      if (!f.movedAt) return false;                    // nunca movida: remove
      return new Date(f.movedAt) >= todayUTC;           // mantém só as de hoje
    });
    const removed = before - db.fichas.length;
    // Remove syncedIds das fichas apagadas — permite que voltem se ainda estiverem em RS no Pipefy
    const idsRestantes = new Set(db.fichas.map(f => f.pipefyId));
    db.syncedIds = db.syncedIds.filter(id => idsRestantes.has(id));

    if (removed > 0) await dbSet(GARANTIA_KEY, db);
    return res.status(200).json({ ok: true, removed, remaining: db.fichas.length });
  }

  // ── POST clear-synced — limpa syncedIds para forçar reimportação total ──
  if (req.method === "POST" && action === "clear-synced") {
    const db = await dbGet(GARANTIA_KEY) || defaultDB();
    const antes = db.syncedIds.length;
    // Mantém apenas IDs que ainda têm ficha ativa
    const idsAtivos = new Set(db.fichas.map(f => f.pipefyId));
    db.syncedIds = db.syncedIds.filter(id => idsAtivos.has(id));
    await dbSet(GARANTIA_KEY, db);
    return res.status(200).json({ ok: true, removidos: antes - db.syncedIds.length, restantes: db.syncedIds.length });
  }

  // ── GET sync-debug — mostra fases do Pipefy e cards em RS ──────
  if (action === "sync-debug") {
    try {
      const data = await pipefyQuery(
        "query { pipe(id: \"" + PIPE_ID + "\") { phases { name cards(first: 5) { edges { node { id title } } } } } }"
      );
      const phases = data?.pipe?.phases || [];
      return res.status(200).json({
        ok: true,
        phases: phases.map(p => ({ name: p.name, cards: p.cards?.edges?.length || 0 })),
        rsFound: phases.filter(p => p.name.toLowerCase().includes("rs")).map(p => p.name),
      });
    } catch(e) {
      return res.status(200).json({ ok: false, error: e.message });
    }
  }

  // ── GET debug — mostra syncedIds e fichas ────────────────────
  if (action === "debug") {
    const db = await dbGet(GARANTIA_KEY) || defaultDB();
    return res.status(200).json({
      ok: true,
      fichas: db.fichas.length,
      syncedIds: db.syncedIds.length,
      syncedIdsList: db.syncedIds,
    });
  }

  // ── POST forcar — remove ID do syncedIds para forçar reimportação
  if (req.method === "POST" && action === "forcar") {
    const { pipefyId } = req.body || {};
    if (!pipefyId) return res.status(400).json({ ok: false, error: "pipefyId obrigatorio" });
    const db = await dbGet(GARANTIA_KEY) || defaultDB();
    const antes = db.syncedIds.length;
    db.syncedIds = db.syncedIds.filter(id => id !== String(pipefyId));
    await dbSet(GARANTIA_KEY, db);
    return res.status(200).json({ ok: true, removed: antes - db.syncedIds.length });
  }

  return res.status(404).json({ ok: false, error: "Acao nao encontrada" });
};
