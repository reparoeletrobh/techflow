// tv-logistica v5
const UPSTASH_URL   = (process.env.UPSTASH_URL   || "").replace(/['"]/g,"").trim();
const UPSTASH_TOKEN = (process.env.UPSTASH_TOKEN || "").replace(/['"]/g,"").trim();
const LOG_KEY = "tv_logistica";

async function dbGet(key) {
  try {
    const r = await fetch(UPSTASH_URL + "/pipeline", {
      method: "POST",
      headers: { Authorization: "Bearer " + UPSTASH_TOKEN, "Content-Type": "application/json" },
      body: JSON.stringify([["GET", key]])
    });
    const j = await r.json();
    if (!j[0] || !j[0].result) return null;
    // Parse uma ou duas vezes — corrige dado corrompido com double-stringify
    var parsed = JSON.parse(j[0].result);
    if (typeof parsed === "string") parsed = JSON.parse(parsed);
    return (parsed && typeof parsed === "object") ? parsed : null;
  } catch(e) { return null; }
}

async function dbSet(key, val) {
  try {
    await fetch(UPSTASH_URL + "/pipeline", {
      method: "POST",
      headers: { Authorization: "Bearer " + UPSTASH_TOKEN, "Content-Type": "application/json" },
      body: JSON.stringify([["SET", key, JSON.stringify(val)]])
    });
    return true;
  } catch(e) { return false; }
}

function uid() {
  return Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
}

function getRegistros(db) {
  if (!db || typeof db !== "object") return [];
  return Array.isArray(db.registros) ? db.registros : [];
}

module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(200).end();

  const action = String(req.query.action || "");
  const b = req.body || {};

  try {

    if (action === "load") {
      const db = await dbGet(LOG_KEY);
      return res.status(200).json({ ok: true, registros: getRegistros(db) });
    }

    if (req.method === "POST" && (action === "registrar" || action === "salvar")) {
      const motorista = String(b.motorista || "");
      const data      = String(b.data || "");
      const km        = parseFloat(b.km) || 0;
      const equip     = parseInt(b.equipamentos) || 0;
      const obs       = String(b.observacao || "").trim();

      if (motorista !== "Wilde" && motorista !== "Paulo")
        return res.status(200).json({ ok: false, error: "Selecione o motorista" });
      if (!data)
        return res.status(200).json({ ok: false, error: "Informe a data" });
      if (km <= 0)
        return res.status(200).json({ ok: false, error: "Informe os KMs" });

      const raw = await dbGet(LOG_KEY);
      const registros = getRegistros(raw);
      const reg = {
        id: uid(),
        motorista: motorista,
        data: data,
        km: Math.round(km * 10) / 10,
        equipamentos: equip,
        observacao: obs,
        criadoEm: new Date().toISOString()
      };
      registros.unshift(reg);
      await dbSet(LOG_KEY, { registros: registros });
      return res.status(200).json({ ok: true, registro: reg, registros: registros });
    }

    if (req.method === "POST" && action === "editar") {
      const id = String(b.id || "");
      if (!id) return res.status(200).json({ ok: false, error: "id obrigatorio" });
      const raw = await dbGet(LOG_KEY);
      const registros = getRegistros(raw);
      const reg = registros.find(function(r) { return r.id === id; });
      if (!reg) return res.status(200).json({ ok: false, error: "Nao encontrado" });
      if (b.motorista !== undefined) reg.motorista    = String(b.motorista);
      if (b.data      !== undefined) reg.data         = String(b.data);
      if (b.km        !== undefined) reg.km           = Math.round(parseFloat(b.km) * 10) / 10;
      if (b.equipamentos !== undefined) reg.equipamentos = parseInt(b.equipamentos) || 0;
      if (b.observacao   !== undefined) reg.observacao   = String(b.observacao).trim();
      await dbSet(LOG_KEY, { registros: registros });
      return res.status(200).json({ ok: true, registro: reg, registros: registros });
    }

    if (req.method === "POST" && action === "excluir") {
      const id = String(b.id || "");
      if (!id) return res.status(200).json({ ok: false, error: "id obrigatorio" });
      const raw = await dbGet(LOG_KEY);
      const registros = getRegistros(raw).filter(function(r) { return r.id !== id; });
      await dbSet(LOG_KEY, { registros: registros });
      return res.status(200).json({ ok: true, registros: registros });
    }

    return res.status(200).json({ ok: false, error: "Acao nao encontrada: " + action });

  } catch(err) {
    return res.status(200).json({ ok: false, error: "Erro interno: " + String(err.message || err) });
  }
};
