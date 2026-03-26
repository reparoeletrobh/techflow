// compra-equip.js — Dash de Compra de Equipamentos
const https  = require("https");

const UPSTASH_URL    = (process.env.UPSTASH_URL   || "").replace(/['"]/g,"").trim();
const UPSTASH_TOKEN  = (process.env.UPSTASH_TOKEN || "").replace(/['"]/g,"").trim();
const PIPEFY_API     = "https://api.pipefy.com/graphql";
const PIPE_ID        = "305832912";
const COMPRA_KEY     = "reparoeletro_compra_equip";

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
  const token = (process.env.PIPEFY_TOKEN || "").trim();
  const r = await fetch(PIPEFY_API, {
    method: "POST",
    headers: { "Content-Type": "application/json", Authorization: "Bearer " + token },
    body: JSON.stringify({ query }),
  });
  const j = await r.json();
  if (j.errors) throw new Error(j.errors[0].message);
  return j.data;
}

// Busca cards em "Analise de Compra" no Pipefy
async function fetchAnaliseCompra() {
  const data = await pipefyQuery(
    'query { pipe(id: "' + PIPE_ID + '") { phases { name cards(first: 50) { edges { node { id title fields { name value } } } } } } }'
  );
  const phases = data?.pipe?.phases || [];
  const ph = phases.find(p => p.name.toLowerCase().trim() === "analise de compra");
  if (!ph) return [];
  return (ph.cards?.edges || []).map(({ node }) => {
    const fields = node.fields || [];
    const get = (kw) => fields.find(f => f.name.toLowerCase().includes(kw))?.value || "";
    return {
      pipefyId:    String(node.id),
      title:       node.title || "",
      nomeContato: get("nome"),
      telefone:    get("telefone") || get("fone"),
      descricao:   get("descri") || get("empresa"),
      endereco:    get("endere"),
    };
  });
}

module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(200).end();
  const { action } = req.query;

  // ── GET load ─────────────────────────────────────────────
  if (action === "load") {
    const db = await dbGet(COMPRA_KEY) || defaultDB();
    return res.status(200).json({ ok: true, fichas: db.fichas || [] });
  }

  // ── GET sync — busca fichas da fase "Analise de Compra" no Pipefy
  if (action === "sync") {
    const db = await dbGet(COMPRA_KEY) || defaultDB();
    if (!Array.isArray(db.fichas))    db.fichas    = [];
    if (!Array.isArray(db.syncedIds)) db.syncedIds = [];
    let added = 0, pipefyError = null;
    try {
      const cards = await fetchAnaliseCompra();
      // Remove syncedIds de cards que saíram da fase
      const idsNaFase = new Set(cards.map(c => c.pipefyId));
      db.syncedIds = db.syncedIds.filter(id => idsNaFase.has(id));
      for (const card of cards) {
        if (db.syncedIds.includes(card.pipefyId)) continue;
        db.fichas.unshift({
          id:          card.pipefyId,
          pipefyId:    card.pipefyId,
          title:       card.title,
          nomeContato: card.nomeContato || card.title,
          telefone:    card.telefone    || "",
          descricao:   card.descricao   || "",
          fotos:       [],
          recomendacao: null,     // "sim" | "nao" | null
          status:       "analise",// "analise" | "comprado" | "nao_comprado"
          createdAt:    new Date().toISOString(),
        });
        db.syncedIds.push(card.pipefyId);
        added++;
      }
      if (added > 0) await dbSet(COMPRA_KEY, db);
    } catch(e) { pipefyError = e.message; }
    return res.status(200).json({ ok: true, added, pipefyError });
  }

  // ── POST recomendar — registra recomendação
  if (req.method === "POST" && action === "recomendar") {
    const { id, recomendacao } = req.body || {};
    if (!id || !recomendacao) return res.status(400).json({ ok: false, error: "id e recomendacao obrigatorios" });
    const db = await dbGet(COMPRA_KEY) || defaultDB();
    const f  = db.fichas.find(x => x.id === id);
    if (!f) return res.status(404).json({ ok: false, error: "Ficha nao encontrada" });
    f.recomendacao    = recomendacao; // "sim" | "nao"
    f.recomendadoAt   = new Date().toISOString();
    await dbSet(COMPRA_KEY, db);
    return res.status(200).json({ ok: true, ficha: f });
  }

  // ── POST status — marca comprado / nao_comprado
  if (req.method === "POST" && action === "status") {
    const { id, status } = req.body || {};
    if (!id || !status) return res.status(400).json({ ok: false, error: "id e status obrigatorios" });
    const db = await dbGet(COMPRA_KEY) || defaultDB();
    const f  = db.fichas.find(x => x.id === id);
    if (!f) return res.status(404).json({ ok: false, error: "Ficha nao encontrada" });
    f.status    = status;
    f.statusAt  = new Date().toISOString();
    await dbSet(COMPRA_KEY, db);
    return res.status(200).json({ ok: true, ficha: f });
  }

  // ── POST add-foto — adiciona URL de foto à ficha
  if (req.method === "POST" && action === "add-foto") {
    const { id, fotoBase64, fotoNome } = req.body || {};
    if (!id || !fotoBase64) return res.status(400).json({ ok: false, error: "id e fotoBase64 obrigatorios" });
    const db = await dbGet(COMPRA_KEY) || defaultDB();
    const f  = db.fichas.find(x => x.id === id);
    if (!f) return res.status(404).json({ ok: false, error: "Ficha nao encontrada" });
    if (!Array.isArray(f.fotos)) f.fotos = [];
    if (f.fotos.length >= 6) return res.status(400).json({ ok: false, error: "Maximo 6 fotos por ficha" });
    f.fotos.push({ base64: fotoBase64, nome: fotoNome || "foto.jpg", addedAt: new Date().toISOString() });
    await dbSet(COMPRA_KEY, db);
    return res.status(200).json({ ok: true, fotos: f.fotos.length });
  }

  // ── POST remover-foto
  if (req.method === "POST" && action === "remover-foto") {
    const { id, fotoIdx } = req.body || {};
    const db = await dbGet(COMPRA_KEY) || defaultDB();
    const f  = db.fichas.find(x => x.id === id);
    if (!f) return res.status(404).json({ ok: false, error: "Ficha nao encontrada" });
    f.fotos = (f.fotos || []).filter((_, i) => i !== fotoIdx);
    await dbSet(COMPRA_KEY, db);
    return res.status(200).json({ ok: true });
  }

  // ── POST excluir
  if (req.method === "POST" && action === "excluir") {
    const { id } = req.body || {};
    const db = await dbGet(COMPRA_KEY) || defaultDB();
    db.fichas    = db.fichas.filter(f => f.id !== id);
    db.syncedIds = db.syncedIds.filter(s => s !== id);
    await dbSet(COMPRA_KEY, db);
    return res.status(200).json({ ok: true });
  }

  // ── POST limpar-concluidos
  if (req.method === "POST" && action === "limpar-concluidos") {
    const db = await dbGet(COMPRA_KEY) || defaultDB();
    const before = db.fichas.length;
    db.fichas    = db.fichas.filter(f => f.status === "analise");
    db.syncedIds = db.fichas.map(f => f.pipefyId);
    await dbSet(COMPRA_KEY, db);
    return res.status(200).json({ ok: true, removed: before - db.fichas.length });
  }

  if (req.method === "POST" && action === "marcar-cadastrado-vendas") {
    const { id } = req.body || {};
    if (!id) return res.status(400).json({ ok: false, error: "id obrigatorio" });
    const db = await dbGet(COMPRA_KEY) || defaultDB();
    const f  = db.fichas.find(f => f.id === id);
    if (!f) return res.status(404).json({ ok: false, error: "Ficha nao encontrada" });
    f.cadastradoVendas = true;
    await dbSet(COMPRA_KEY, db);
    return res.status(200).json({ ok: true });
  }

  return res.status(404).json({ ok: false, error: "Acao nao encontrada" });
};
