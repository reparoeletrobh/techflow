// garantia.js — Setor de Garantia
const UPSTASH_URL   = (process.env.UPSTASH_URL   || "").replace(/['"]/g,"").trim();
const UPSTASH_TOKEN = (process.env.UPSTASH_TOKEN || "").replace(/['"]/g,"").trim();
const PIPEFY_API    = "https://api.pipefy.com/graphql";
const PIPE_ID       = "306904889";
const GARANTIA_KEY  = "tv_garantia";

const FASES = [
  { id: "garantia_acionada",  name: "Garantia Acionada"  },
  { id: "em_analise",         name: "Em Análise"          },
  { id: "servico_finalizado", name: "Serviço Finalizado"  },
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

function defaultDB() { return { fichas: [], syncedIds: [] }; }

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

async function fetchRSCards() {
  const data = await pipefyQuery(
    "query { pipe(id: \"" + PIPE_ID + "\") { phases { name cards(first: 50) { edges { node { id title fields { name value } } } } } } }"
  );
  const phases = data?.pipe?.phases || [];
  const rsPhase = phases.find(p => p.name.toLowerCase().trim() === "rs");
  if (!rsPhase) return [];
  return (rsPhase.cards?.edges || []).map(({ node }) => {
    const fields = node.fields || [];
    const get = kw => fields.find(f => f.name.toLowerCase().includes(kw))?.value || "";
    const m = (node.title || "").match(/^(.*?)\s+(\d{3,6})$/);
    return {
      pipefyId:    String(node.id),
      title:       node.title || "",
      osCode:      m ? m[2] : null,
      nomeContato: get("nome") || (m ? m[1].trim() : node.title),
      telefone:    get("telefone") || get("fone"),
      descricao:   get("descri"),
    };
  });
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

  // ── GET sync ───────────────────────────────────────────────
  // REGRA: só adiciona fichas novas da fase RS
  // NUNCA remove fichas que o usuário já moveu de fase internamente
  // Fichas só saem via: mover para servico_finalizado, excluir, ou limpar
  if (action === "sync") {
    const db = await dbGet(GARANTIA_KEY) || defaultDB();
    if (!Array.isArray(db.fichas))    db.fichas    = [];
    if (!Array.isArray(db.syncedIds)) db.syncedIds = [];

    // Se não há fichas, limpa syncedIds para permitir reimportação
    if (db.fichas.length === 0) db.syncedIds = [];

    let added = 0, pipefyError = null;
    try {
      const cards = await fetchRSCards();
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
          phaseId:     FASES[0].id,
          movedAt:     null,
          createdAt:   new Date().toISOString(),
        });
        db.syncedIds.push(card.pipefyId);
        added++;
      }
      if (added > 0) await dbSet(GARANTIA_KEY, db);
    } catch(e) { pipefyError = e.message; }

    return res.status(200).json({ ok: true, added, pipefyError });
  }

  // ── POST mover ─────────────────────────────────────────────
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

  // ── POST excluir ───────────────────────────────────────────
  if (req.method === "POST" && action === "excluir") {
    const { id } = req.body || {};
    const db = await dbGet(GARANTIA_KEY) || defaultDB();
    // Mantém syncedId ao excluir — impede reimportação automática
    db.fichas = db.fichas.filter(f => f.id !== id);
    await dbSet(GARANTIA_KEY, db);
    return res.status(200).json({ ok: true });
  }

  // ── POST limpar-nao-movidas-hoje ───────────────────────────
  if (req.method === "POST" && action === "limpar-nao-movidas-hoje") {
    const db = await dbGet(GARANTIA_KEY) || defaultDB();
    function toBRT(d) { return new Date(new Date(d).toLocaleString("en-US",{timeZone:"America/Sao_Paulo"})); }
    const nowBRT = toBRT(new Date()); nowBRT.setHours(0,0,0,0);
    const todayUTC = new Date(nowBRT.getTime() + 3*60*60*1000);
    const before = db.fichas.length;
    // Remove fichas nunca movidas (movedAt null) ou movidas antes de hoje
    db.fichas = db.fichas.filter(f => f.movedAt && new Date(f.movedAt) >= todayUTC);
    const removed = before - db.fichas.length;
    // Limpa syncedIds das fichas removidas para permitir reimportação no próximo sync
    const idsRestantes = new Set(db.fichas.map(f => f.pipefyId));
    db.syncedIds = db.syncedIds.filter(id => idsRestantes.has(id));
    if (removed > 0) await dbSet(GARANTIA_KEY, db);
    return res.status(200).json({ ok: true, removed, remaining: db.fichas.length });
  }

  // ── GET debug ──────────────────────────────────────────────
  if (action === "debug") {
    const db = await dbGet(GARANTIA_KEY) || defaultDB();
    return res.status(200).json({ ok: true, fichas: db.fichas.length, syncedIds: db.syncedIds.length, syncedIdsList: db.syncedIds });
  }

  // ── GET sync-debug ─────────────────────────────────────────
  if (action === "sync-debug") {
    try {
      const data = await pipefyQuery("query { pipe(id: \"" + PIPE_ID + "\") { phases { name cards(first: 5) { edges { node { id title } } } } } }");
      const phases = data?.pipe?.phases || [];
      return res.status(200).json({ ok: true, phases: phases.map(p => ({ name: p.name, cards: p.cards?.edges?.length || 0 })), rsFound: phases.filter(p => p.name.toLowerCase().includes("rs")).map(p => p.name) });
    } catch(e) { return res.status(200).json({ ok: false, error: e.message }); }
  }

  // ── POST reset-moveat ──────────────────────────────────────
  if (req.method === "POST" && action === "reset-moveat") {
    const db = await dbGet(GARANTIA_KEY) || defaultDB();
    let count = 0;
    db.fichas.forEach(f => { if (f.phaseId === FASES[0].id) { f.movedAt = null; count++; } });
    await dbSet(GARANTIA_KEY, db);
    return res.status(200).json({ ok: true, reset: count });
  }

  // ── POST clear-synced ──────────────────────────────────────
  if (req.method === "POST" && action === "clear-synced") {
    const db = await dbGet(GARANTIA_KEY) || defaultDB();
    const idsAtivos = new Set(db.fichas.map(f => f.pipefyId));
    const antes = db.syncedIds.length;
    db.syncedIds = db.syncedIds.filter(id => idsAtivos.has(id));
    await dbSet(GARANTIA_KEY, db);
    return res.status(200).json({ ok: true, removidos: antes - db.syncedIds.length });
  }

  if (action === "tecnico-load") {
    const db = await dbGet(GARANTIA_KEY) || defaultDB();
    const all = db.fichas || [];
    const garantias = all.filter(f => f.phaseId !== "servico_finalizado");
    return res.status(200).json({ ok: true, garantias, lojaImediata: [] });
  }
  return res.status(404).json({ ok: false, error: "Acao nao encontrada" });
};
