// api/tv-coleta.js — Sistema de coleta próprio da TV
const UPSTASH_URL    = (process.env.UPSTASH_URL    || "").replace(/['"]/g,"").trim();
const UPSTASH_TOKEN  = (process.env.UPSTASH_TOKEN  || "").replace(/['"]/g,"").trim();
const TV_BOARD_KEY   = "tv_board";
const COLETA_KEY     = "tv_coleta";

async function dbGet(key) {
  try {
    const r = await fetch(`${UPSTASH_URL}/pipeline`, {
      method: "POST",
      headers: { Authorization: `Bearer ${UPSTASH_TOKEN}`, "Content-Type": "application/json" },
      body: JSON.stringify([["GET", key]]),
    });
    const j = await r.json();
    return j[0]?.result ? JSON.parse(j[0].result) : null;
  } catch(e) { return null; }
}

async function dbSet(key, val) {
  try {
    await fetch(`${UPSTASH_URL}/pipeline`, {
      method: "POST",
      headers: { Authorization: `Bearer ${UPSTASH_TOKEN}`, "Content-Type": "application/json" },
      body: JSON.stringify([["SET", key, JSON.stringify(val)]]),
    });
    return true;
  } catch(e) { return false; }
}

module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(200).end();

  const action = req.query.action || "";

  try {
    // ── GET load — carrega fila de coleta ─────────────────────
    if (action === "load") {
      const db = await dbGet(COLETA_KEY) || { fichas: [], removedIds: [] };
      return res.status(200).json({
        ok: true,
        fichas:    db.fichas    || [],
        removedIds: db.removedIds || [],
        total:     (db.fichas || []).filter(f => f.status === "pendente").length,
      });
    }

    // ── GET sync — importa fichas em Coleta Solicitada do board ─
    if (action === "sync") {
      const [db, board] = await Promise.all([
        dbGet(COLETA_KEY) || { fichas: [], removedIds: [] },
        dbGet(TV_BOARD_KEY),
      ]);
      if (!Array.isArray(db.fichas))     db.fichas     = [];
      if (!Array.isArray(db.removedIds)) db.removedIds = [];

      const boardCards = (board?.cards || []).filter(c => c.phaseId === "coleta_solicitada");
      let added = 0;

      for (const c of boardCards) {
        const pipefyId   = c.pipefyId || c.id;
        const removedKey = pipefyId + ":coleta";
        if (db.fichas.find(f => f.pipefyId === pipefyId)) continue;
        if (db.removedIds.includes(removedKey)) continue;
        db.fichas.push({
          pipefyId,
          osCode:      c.osCode      || null,
          nomeContato: c.nomeContato || c.title || "—",
          telefone:    c.telefone    || null,
          endereco:    c.endereco    || null,
          descricao:   c.descricao   || null,
          lat: null, lng: null,
          status:   "pendente",
          addedAt:  new Date().toISOString(),
        });
        added++;
      }

      if (added > 0) await dbSet(COLETA_KEY, db);
      return res.status(200).json({ ok: true, added, total: db.fichas.filter(f=>f.status==="pendente").length });
    }

    // ── POST marcar-coletado — motorista marca como coletado ──
    if (req.method === "POST" && action === "marcar-coletado") {
      const { pipefyId, obs } = req.body || {};
      if (!pipefyId) return res.status(400).json({ ok: false, error: "pipefyId obrigatório" });
      const db = await dbGet(COLETA_KEY) || { fichas: [] };
      const idx = (db.fichas || []).findIndex(f => f.pipefyId === pipefyId);
      if (idx < 0) return res.status(404).json({ ok: false, error: "Ficha não encontrada" });
      db.fichas[idx].status      = "coletado";
      db.fichas[idx].coletadoEm  = new Date().toISOString();
      db.fichas[idx].obs         = obs || "";
      await dbSet(COLETA_KEY, db);
      return res.status(200).json({ ok: true, ficha: db.fichas[idx] });
    }

    // ── POST limpar — arquiva fichas coletadas em removedIds ──
    if (req.method === "POST" && action === "limpar") {
      const db = await dbGet(COLETA_KEY) || { fichas: [], removedIds: [] };
      if (!Array.isArray(db.removedIds)) db.removedIds = [];
      (db.fichas || []).forEach(f => {
        const key = f.pipefyId + ":coleta";
        if (!db.removedIds.includes(key)) db.removedIds.push(key);
      });
      db.fichas = [];
      await dbSet(COLETA_KEY, db);
      return res.status(200).json({ ok: true });
    }

    // ── POST remover — remove uma ficha da fila ───────────────
    if (req.method === "POST" && action === "remover") {
      const { pipefyId } = req.body || {};
      if (!pipefyId) return res.status(400).json({ ok: false, error: "pipefyId obrigatório" });
      const db = await dbGet(COLETA_KEY) || { fichas: [], removedIds: [] };
      if (!Array.isArray(db.removedIds)) db.removedIds = [];
      const key = pipefyId + ":coleta";
      if (!db.removedIds.includes(key)) db.removedIds.push(key);
      db.fichas = (db.fichas || []).filter(f => f.pipefyId !== pipefyId);
      await dbSet(COLETA_KEY, db);
      return res.status(200).json({ ok: true });
    }

    // ── GET motorista — dados públicos para o link do motorista ─
    if (action === "motorista") {
      const db = await dbGet(COLETA_KEY) || { fichas: [] };
      const pendentes = (db.fichas || []).filter(f => f.status === "pendente");
      // Retorna só o necessário (sem dados sensíveis extras)
      const rota = pendentes.map(f => ({
        pipefyId:    f.pipefyId,
        nomeContato: f.nomeContato,
        endereco:    f.endereco,
        telefone:    f.telefone,
        obs:         f.obs || "",
      }));
      return res.status(200).json({ ok: true, rota, total: rota.length, data: new Date().toLocaleDateString("pt-BR", {timeZone:"America/Sao_Paulo"}) });
    }

    return res.status(404).json({ ok: false, error: "Ação não encontrada" });
  } catch(e) {
    return res.status(200).json({ ok: false, error: "Erro interno: " + e.message });
  }
};
