const UPSTASH_URL   = (process.env.UPSTASH_URL   || "").replace(/['"]/g,"").trim();
const UPSTASH_TOKEN = (process.env.UPSTASH_TOKEN || "").replace(/['"]/g,"").trim();
const LOG_KEY = "tv_logistica";

async function dbGet(key) {
  try {
    const r = await fetch(UPSTASH_URL + "/pipeline", {
      method: "POST",
      headers: { Authorization: "Bearer " + UPSTASH_TOKEN, "Content-Type": "application/json" },
      body: JSON.stringify([["GET", key]]),
    });
    const j = await r.json();
    return j[0] && j[0].result ? JSON.parse(j[0].result) : null;
  } catch(e) { return null; }
}

async function dbSet(key, val) {
  try {
    await fetch(UPSTASH_URL + "/pipeline", {
      method: "POST",
      headers: { Authorization: "Bearer " + UPSTASH_TOKEN, "Content-Type": "application/json" },
      body: JSON.stringify([["SET", key, JSON.stringify(val)]]),
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

  var action = req.query.action || "";

  if (action === "load") {
    var db = await dbGet(LOG_KEY) || { registros: [] };
    return res.status(200).json({ ok: true, registros: db.registros || [] });
  }

  if (req.method === "POST" && (action === "registrar" || action === "salvar")) {
    var body = req.body || {};
    var motorista  = body.motorista || "";
    var data       = body.data || "";
    var km         = body.km;
    var equipamentos = body.equipamentos;
    var observacao = body.observacao || "";

    if (motorista !== "Wilde" && motorista !== "Paulo")
      return res.status(200).json({ ok: false, error: "Selecione o motorista" });
    if (!data)
      return res.status(200).json({ ok: false, error: "Data obrigatorio" });
    if (!km || isNaN(km) || Number(km) <= 0)
      return res.status(200).json({ ok: false, error: "Informe os KMs" });

    var db2 = await dbGet(LOG_KEY) || { registros: [] };
    var reg = {
      id:           uid(),
      motorista:    String(motorista),
      data:         String(data),
      km:           Math.round(Number(km) * 10) / 10,
      equipamentos: parseInt(equipamentos) || 0,
      observacao:   String(observacao).trim(),
      criadoEm:     new Date().toISOString(),
    };
    db2.registros.unshift(reg);
    await dbSet(LOG_KEY, db2);
    return res.status(200).json({ ok: true, registro: reg, registros: db2.registros });
  }

  if (req.method === "POST" && action === "editar") {
    var body3 = req.body || {};
    var id3 = body3.id || "";
    if (!id3) return res.status(200).json({ ok: false, error: "id obrigatorio" });
    var db3 = await dbGet(LOG_KEY) || { registros: [] };
    var reg3 = db3.registros.find(function(r) { return r.id === id3; });
    if (!reg3) return res.status(200).json({ ok: false, error: "Nao encontrado" });
    if (body3.motorista)    reg3.motorista    = String(body3.motorista);
    if (body3.data)         reg3.data         = String(body3.data);
    if (body3.km)           reg3.km           = Math.round(Number(body3.km) * 10) / 10;
    if (body3.equipamentos !== undefined) reg3.equipamentos = parseInt(body3.equipamentos) || 0;
    if (body3.observacao !== undefined)   reg3.observacao   = String(body3.observacao).trim();
    await dbSet(LOG_KEY, db3);
    return res.status(200).json({ ok: true, registro: reg3, registros: db3.registros });
  }

  if (req.method === "POST" && action === "excluir") {
    var body4 = req.body || {};
    var id4 = body4.id || "";
    if (!id4) return res.status(200).json({ ok: false, error: "id obrigatorio" });
    var db4 = await dbGet(LOG_KEY) || { registros: [] };
    db4.registros = db4.registros.filter(function(r) { return r.id !== id4; });
    await dbSet(LOG_KEY, db4);
    return res.status(200).json({ ok: true, registros: db4.registros });
  }

  return res.status(200).json({ ok: false, error: "Acao nao encontrada" });
};
