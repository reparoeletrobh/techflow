const UPSTASH_URL   = (process.env.UPSTASH_URL   || "").replace(/['"]/g, "").trim();
const UPSTASH_TOKEN = (process.env.UPSTASH_TOKEN || "").replace(/['"]/g, "").trim();
const VENDAS_KEY    = "reparoeletro_vendas";

async function dbGet(key) {
  try {
    const r = await fetch(`${UPSTASH_URL}/pipeline`, {
      method: "POST",
      headers: { Authorization: `Bearer ${UPSTASH_TOKEN}`, "Content-Type": "application/json" },
      body: JSON.stringify([["GET", key]]),
    });
    const j = await r.json();
    return j[0]?.result ? JSON.parse(j[0].result) : null;
  } catch (e) { return null; }
}

async function dbSet(key, value) {
  try {
    const r = await fetch(`${UPSTASH_URL}/pipeline`, {
      method: "POST",
      headers: { Authorization: `Bearer ${UPSTASH_TOKEN}`, "Content-Type": "application/json" },
      body: JSON.stringify([["SET", key, JSON.stringify(value)]]),
    });
    const j = await r.json();
    return j[0]?.result === "OK";
  } catch (e) { return false; }
}

function defaultDB() {
  return { produtos: [] };
}

module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(200).end();

  const { action } = req.query;

  // ── GET load ───────────────────────────────────────────────
  if (action === "load") {
    const db = await dbGet(VENDAS_KEY) || defaultDB();
    return res.status(200).json({ ok: true, produtos: db.produtos || [] });
  }

  // ── POST salvar (criar ou editar) ──────────────────────────
  if (req.method === "POST" && action === "salvar") {
    const { id, codigo, descricao, capacidade, preco, fotos } = req.body || {};
    if (!codigo || !descricao || preco === undefined)
      return res.status(400).json({ ok: false, error: "codigo, descricao e preco são obrigatórios" });

    const db = await dbGet(VENDAS_KEY) || defaultDB();
    const fotosArr = Array.isArray(fotos) ? fotos.slice(0, 6) : [];

    if (id) {
      // Editar
      const idx = db.produtos.findIndex(p => p.id === id);
      if (idx < 0) return res.status(404).json({ ok: false, error: "Produto não encontrado" });
      db.produtos[idx] = { ...db.produtos[idx], codigo, descricao, capacidade: capacidade || null, preco, fotos: fotosArr, updatedAt: new Date().toISOString() };
      await dbSet(VENDAS_KEY, db);
      return res.status(200).json({ ok: true, produto: db.produtos[idx] });
    } else {
      // Criar
      const produto = {
        id:         Date.now().toString(),
        codigo,
        descricao,
        capacidade: capacidade || null,
        preco,
        fotos:      fotosArr,
        createdAt:  new Date().toISOString(),
        updatedAt:  new Date().toISOString(),
      };
      db.produtos.unshift(produto);
      await dbSet(VENDAS_KEY, db);
      return res.status(200).json({ ok: true, produto });
    }
  }

  // ── POST excluir ───────────────────────────────────────────
  if (req.method === "POST" && action === "excluir") {
    const { id } = req.body || {};
    if (!id) return res.status(400).json({ ok: false, error: "id obrigatório" });
    const db = await dbGet(VENDAS_KEY) || defaultDB();
    db.produtos = db.produtos.filter(p => p.id !== id);
    await dbSet(VENDAS_KEY, db);
    return res.status(200).json({ ok: true });
  }

  return res.status(404).json({ ok: false, error: "Ação não encontrada" });
};
