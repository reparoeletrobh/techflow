// api/garantia.js — Sistema de Garantia v2
const GARANTIA_KEY = "reparoeletro_garantia_v2";
const UPSTASH_URL   = (process.env.UPSTASH_URL   || "").replace(/['"]/g,"").trim();
const UPSTASH_TOKEN = (process.env.UPSTASH_TOKEN || "").replace(/['"]/g,"").trim();

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

function uid() { return Date.now().toString(36) + Math.random().toString(36).slice(2,7); }

// Fases por tipo
const FASES = {
  loja_imediata: [
    { id: "producao",            label: "Produção" },
    { id: "conserto_concluido",  label: "Conserto Concluído" },
    { id: "equip_retirado",      label: "Equipamento Retirado" },
  ],
  loja_acompanhamento: [
    { id: "producao",            label: "Produção" },
    { id: "conserto_concluido",  label: "Conserto Concluído" },
    { id: "teste_realizado",     label: "Teste Realizado" },
    { id: "equip_retirado",      label: "Equipamento Retirado" },
  ],
  delivery: [
    { id: "coleta_solicitada",   label: "Coleta Solicitada" },
    { id: "producao",            label: "Produção" },
    { id: "conserto_concluido",  label: "Conserto Concluído" },
    { id: "teste_realizado",     label: "Teste Realizado" },
    { id: "solicitar_entrega",   label: "Solicitar Entrega" },
    { id: "entrega_realizada",   label: "Entrega Realizada" },
  ],
  rua: [
    { id: "garantia_solicitada", label: "Garantia Solicitada" },
    { id: "equip_recolhido",     label: "Equipamento Recolhido" },
    { id: "conserto_realizado",  label: "Conserto Realizado" },
  ],
};

function primeiraFase(tipo) {
  return (FASES[tipo] || [])[0]?.id || "producao";
}

function defaultDB() { return { fichas: [] }; }

function isConcluida(ficha) {
  const ultimas = { loja_imediata: "equip_retirado", loja_acompanhamento: "equip_retirado", delivery: "entrega_realizada", rua: "conserto_realizado" };
  return ficha.faseId === ultimas[ficha.tipo];
}

module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(200).end();

  const { action } = req.query;

  try {

    // ── GET load ──────────────────────────────────────────────
    if (action === "load") {
      const db = await dbGet(GARANTIA_KEY) || defaultDB();
      return res.status(200).json({ ok: true, fichas: db.fichas || [], fases: FASES });
    }

    // ── POST cadastrar ─────────────────────────────────────────
    if (req.method === "POST" && action === "cadastrar") {
      const { nome, telefone, defeito, endereco, tipo } = req.body || {};
      if (!nome || !telefone || !defeito || !tipo)
        return res.status(400).json({ ok: false, error: "nome, telefone, defeito e tipo são obrigatórios" });
      if (!FASES[tipo])
        return res.status(400).json({ ok: false, error: "tipo inválido: " + tipo });
      const db = await dbGet(GARANTIA_KEY) || defaultDB();
      const ficha = {
        id:         uid(),
        nome:       nome.trim(),
        telefone:   telefone.trim(),
        defeito:    defeito.trim(),
        endereco:   (endereco || "").trim(),
        tipo,       // loja_imediata | loja_acompanhamento | delivery | rua
        faseId:     primeiraFase(tipo),
        criadaEm:   new Date().toISOString(),
        movidaEm:   new Date().toISOString(),
        concluida:  false,
      };
      db.fichas.unshift(ficha);
      await dbSet(GARANTIA_KEY, db);
      return res.status(200).json({ ok: true, ficha });
    }

    // ── POST mover ─────────────────────────────────────────────
    if (req.method === "POST" && action === "mover") {
      const { id, faseId } = req.body || {};
      if (!id || !faseId) return res.status(400).json({ ok: false, error: "id e faseId obrigatórios" });
      const db = await dbGet(GARANTIA_KEY) || defaultDB();
      const ficha = db.fichas.find(function(f) { return f.id === id; });
      if (!ficha) return res.status(404).json({ ok: false, error: "Ficha não encontrada" });
      const fases = FASES[ficha.tipo] || [];
      if (!fases.find(function(f) { return f.id === faseId; }))
        return res.status(400).json({ ok: false, error: "Fase inválida para este tipo" });
      ficha.faseId   = faseId;
      ficha.movidaEm = new Date().toISOString();
      ficha.concluida = isConcluida(ficha);
      await dbSet(GARANTIA_KEY, db);
      return res.status(200).json({ ok: true, ficha });
    }

    // ── POST concluir ──────────────────────────────────────────
    if (req.method === "POST" && action === "concluir") {
      const { id } = req.body || {};
      const db = await dbGet(GARANTIA_KEY) || defaultDB();
      const ficha = db.fichas.find(function(f) { return f.id === id; });
      if (!ficha) return res.status(404).json({ ok: false, error: "Ficha não encontrada" });
      const ultimas = { loja_imediata: "equip_retirado", loja_acompanhamento: "equip_retirado", delivery: "entrega_realizada", rua: "conserto_realizado" };
      ficha.faseId    = ultimas[ficha.tipo] || ficha.faseId;
      ficha.concluida = true;
      ficha.concluidaEm = new Date().toISOString();
      await dbSet(GARANTIA_KEY, db);
      return res.status(200).json({ ok: true, ficha });
    }

    // ── POST excluir ───────────────────────────────────────────
    if (req.method === "POST" && action === "excluir") {
      const { id } = req.body || {};
      const db = await dbGet(GARANTIA_KEY) || defaultDB();
      db.fichas = db.fichas.filter(function(f) { return f.id !== id; });
      await dbSet(GARANTIA_KEY, db);
      return res.status(200).json({ ok: true });
    }

    return res.status(404).json({ ok: false, error: "Ação não encontrada" });

  } catch(e) {
    return res.status(200).json({ ok: false, error: "Erro interno: " + e.message });
  }
};
