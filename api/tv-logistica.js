// api/tv-logistica.js — Controle de logística TV Assistência
// Registros de rotas: motorista, km, equipamentos, data

const UPSTASH_URL   = (process.env.UPSTASH_URL   || "").replace(/['"]/g,"").trim();
const UPSTASH_TOKEN = (process.env.UPSTASH_TOKEN || "").replace(/['"]/g,"").trim();
const LOG_KEY = "tv_logistica";

async function dbGet(key) {
  try {
    const r = await fetch(`${UPSTASH_URL}/get/${encodeURIComponent(key)}`, {
      headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` }
    });
    const j = await r.json();
    return j.result ? JSON.parse(j.result) : null;
  } catch(e) { return null; }
}

async function dbSet(key, val) {
  try {
    await fetch(`${UPSTASH_URL}/set/${encodeURIComponent(key)}`, {
      method: "POST",
      headers: { Authorization: `Bearer ${UPSTASH_TOKEN}`, "Content-Type": "application/json" },
      body: JSON.stringify(JSON.stringify(val))
    });
    return true;
  } catch(e) { return false; }
}

function uid() {
  return Date.now().toString(36) + Math.random().toString(36).slice(2, 7);
}

function defaultDB() {
  return { registros: [] };
}

module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(200).end();

  const action = req.query.action || "";

  // ── GET load ────────────────────────────────────────────────
  if (action === "load") {
    const db = await dbGet(LOG_KEY) || defaultDB();
    return res.status(200).json({ ok: true, registros: db.registros || [] });
  }

  // ── POST registrar ──────────────────────────────────────────
  if (req.method === "POST" && action === "registrar") {
    const { motorista, km, equipamentos, data, observacao } = req.body || {};
    if (!motorista || !km || !equipamentos || !data)
      return res.status(400).json({ ok: false, error: "motorista, km, equipamentos e data são obrigatórios" });

    const db = await dbGet(LOG_KEY) || defaultDB();
    const registro = {
      id:          uid(),
      motorista:   motorista.trim(),
      km:          parseFloat(km),
      equipamentos: parseInt(equipamentos),
      data,
      observacao:  (observacao || "").trim(),
      criadoEm:   new Date().toISOString()
    };
    db.registros.unshift(registro);
    await dbSet(LOG_KEY, db);
    return res.status(200).json({ ok: true, registro });
  }

  // ── POST editar ─────────────────────────────────────────────
  if (req.method === "POST" && action === "editar") {
    const { id, motorista, km, equipamentos, data, observacao } = req.body || {};
    if (!id) return res.status(400).json({ ok: false, error: "id obrigatório" });

    const db = await dbGet(LOG_KEY) || defaultDB();
    const reg = db.registros.find(r => r.id === id);
    if (!reg) return res.status(404).json({ ok: false, error: "Registro não encontrado" });

    if (motorista)    reg.motorista    = motorista.trim();
    if (km)           reg.km           = parseFloat(km);
    if (equipamentos) reg.equipamentos = parseInt(equipamentos);
    if (data)         reg.data         = data;
    if (observacao !== undefined) reg.observacao = observacao.trim();

    await dbSet(LOG_KEY, db);
    return res.status(200).json({ ok: true, registro: reg });
  }

  // ── POST excluir ─────────────────────────────────────────────
  if (req.method === "POST" && action === "excluir") {
    const { id } = req.body || {};
    if (!id) return res.status(400).json({ ok: false, error: "id obrigatório" });
    const db = await dbGet(LOG_KEY) || defaultDB();
    db.registros = db.registros.filter(r => r.id !== id);
    await dbSet(LOG_KEY, db);
    return res.status(200).json({ ok: true });
  }

  return res.status(404).json({ ok: false, error: "Ação não encontrada" });
};
