// tv-logistica v3 — 202604151650
const UPSTASH_URL   = (process.env.UPSTASH_URL   || "").replace(/['"]/g,"").trim();
const UPSTASH_TOKEN = (process.env.UPSTASH_TOKEN || "").replace(/['"]/g,"").trim();
const LOG_KEY = "tv_logistica";

async function dbGet(key) {
  const r = await fetch(UPSTASH_URL + "/pipeline", {
    method: "POST",
    headers: { Authorization: "Bearer " + UPSTASH_TOKEN, "Content-Type": "application/json" },
    body: JSON.stringify([["GET", key]])
  });
  const j = await r.json();
  return (j[0] && j[0].result) ? JSON.parse(j[0].result) : null;
}

async function dbSet(key, val) {
  await fetch(UPSTASH_URL + "/pipeline", {
    method: "POST",
    headers: { Authorization: "Bearer " + UPSTASH_TOKEN, "Content-Type": "application/json" },
    body: JSON.stringify([["SET", key, JSON.stringify(val)]])
  });
}

function uid() {
  return Date.now().toString(36) + Math.random().toString(36).slice(2,6);
}

module.exports = async function(req, res) {
  res.setHeader("Access-Control-Allow-Origin","*");
  res.setHeader("Access-Control-Allow-Methods","GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers","Content-Type");
  if (req.method === "OPTIONS") return res.status(200).end();

  const action = String(req.query.action || "");
  const b = req.body || {};

  try {

    if (action === "load") {
      const db = await dbGet(LOG_KEY) || { registros:[] };
      return res.json({ ok:true, registros: db.registros||[] });
    }

    if (req.method === "POST" && (action === "registrar" || action === "salvar")) {
      const motorista = String(b.motorista||"");
      const data      = String(b.data||"");
      const km        = parseFloat(b.km)||0;
      const equip     = parseInt(b.equipamentos)||0;
      const obs       = String(b.observacao||"").trim();

      if (motorista !== "Wilde" && motorista !== "Paulo")
        return res.json({ ok:false, error:"Selecione o motorista" });
      if (!data) return res.json({ ok:false, error:"Informe a data" });
      if (km <= 0) return res.json({ ok:false, error:"Informe os KMs" });

      const db = await dbGet(LOG_KEY) || { registros:[] };
      if (!Array.isArray(db.registros)) db.registros = [];
      const reg = { id:uid(), motorista, data, km:Math.round(km*10)/10, equipamentos:equip, observacao:obs, criadoEm:new Date().toISOString() };
      db.registros.unshift(reg);
      await dbSet(LOG_KEY, db);
      return res.json({ ok:true, registro:reg, registros:db.registros });
    }

    if (req.method === "POST" && action === "editar") {
      const id = String(b.id||"");
      if (!id) return res.json({ ok:false, error:"id obrigatorio" });
      const db = await dbGet(LOG_KEY) || { registros:[] };
      if (!Array.isArray(db.registros)) db.registros=[];
      const reg = db.registros.find(r => r.id===id);
      if (!reg) return res.json({ ok:false, error:"Nao encontrado" });
      if (b.motorista!==undefined) reg.motorista=String(b.motorista);
      if (b.data!==undefined) reg.data=String(b.data);
      if (b.km!==undefined) reg.km=Math.round(parseFloat(b.km)*10)/10;
      if (b.equipamentos!==undefined) reg.equipamentos=parseInt(b.equipamentos)||0;
      if (b.observacao!==undefined) reg.observacao=String(b.observacao).trim();
      await dbSet(LOG_KEY, db);
      return res.json({ ok:true, registro:reg, registros:db.registros });
    }

    if (req.method === "POST" && action === "excluir") {
      const id = String(b.id||"");
      if (!id) return res.json({ ok:false, error:"id obrigatorio" });
      const db = await dbGet(LOG_KEY) || { registros:[] };
      if (!Array.isArray(db.registros)) db.registros=[];
      db.registros = db.registros.filter(r => r.id!==id);
      await dbSet(LOG_KEY, db);
      return res.json({ ok:true, registros:db.registros });
    }

    return res.json({ ok:false, error:"Acao nao encontrada: "+action });

  } catch(err) {
    return res.json({ ok:false, error:"Erro: "+String(err.message||err) });
  }
};
