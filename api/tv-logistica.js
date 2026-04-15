// api/tv-logistica.js — Logística TV Assistência

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
    const body = JSON.stringify(val);
    await fetch(`${UPSTASH_URL}/set/${encodeURIComponent(key)}`, {
      method: "POST",
      headers: { Authorization: `Bearer ${UPSTASH_TOKEN}`, "Content-Type": "application/json" },
      body: JSON.stringify(body)
    });
    return true;
  } catch(e) { return false; }
}

function uid() {
  return Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
}

module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(200).end();

  const action = req.query.action || "";

  // ── GET load ──────────────────────────────────────────────────
  if (action === "load") {
    const db = await dbGet(LOG_KEY) || { registros: [] };
    return res.status(200).json({ ok: true, registros: db.registros || [] });
  }

  // ── POST salvar ───────────────────────────────────────────────
  if (req.method === "POST" && action === "salvar") {
    const { motorista, data, km, equipamentos, observacao } = req.body || {};

    if (!motorista || !["Wilde", "Paulo"].includes(motorista))
      return res.status(400).json({ ok: false, error: "Selecione o motorista" });
    if (!data)
      return res.status(400).json({ ok: false, error: "Data obrigatória" });
    if (!km || isNaN(parseFloat(km)) || parseFloat(km) <= 0)
      return res.status(400).json({ ok: false, error: "Informe os KMs rodados" });
    if (equipamentos === undefined || isNaN(parseInt(equipamentos)) || parseInt(equipamentos) < 0)
      return res.status(400).json({ ok: false, error: "Informe os equipamentos" });

    const db = await dbGet(LOG_KEY) || { registros: [] };
    const registro = {
      id:           uid(),
      motorista,
      data,
      km:           parseFloat(parseFloat(km).toFixed(1)),
      equipamentos: parseInt(equipamentos),
      observacao:   (observacao || "").trim(),
      criadoEm:     new Date().toISOString(),
    };
    db.registros.unshift(registro);
    await dbSet(LOG_KEY, db);
    return res.status(200).json({ ok: true, registro, registros: db.registros });
  }

  // ── POST excluir ──────────────────────────────────────────────
  if (req.method === "POST" && action === "excluir") {
    const { id } = req.body || {};
    if (!id) return res.status(400).json({ ok: false, error: "id obrigatório" });
    const db = await dbGet(LOG_KEY) || { registros: [] };
    db.registros = db.registros.filter(function(r) { return r.id !== id; });
    await dbSet(LOG_KEY, db);
    return res.status(200).json({ ok: true, registros: db.registros });
  }

  return res.status(404).json({ ok: false, error: "Ação não encontrada" });
};
