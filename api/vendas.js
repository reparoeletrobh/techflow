const UPSTASH_URL   = (process.env.UPSTASH_URL   || "").replace(/['"]/g, "").trim();
const UPSTASH_TOKEN = (process.env.UPSTASH_TOKEN || "").replace(/['"]/g, "").trim();
const VENDAS_KEY    = "reparoeletro_vendas";
const FIN_KEY       = "reparoeletro_financeiro";

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

function defaultDB()  { return { produtos: [] }; }
function defaultFin() { return { records: [], syncedIds: [] }; }

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

  // ── GET stats (para metas) ─────────────────────────────────
  if (action === "stats") {
    const db = await dbGet(VENDAS_KEY) || defaultDB();
    const produtos = db.produtos || [];

    function toBRT(d) { return new Date(new Date(d).toLocaleString("en-US", { timeZone: "America/Sao_Paulo" })); }
    const nowBRT = toBRT(new Date()); nowBRT.setHours(0,0,0,0);
    const todayUTC = new Date(nowBRT.getTime() + 3*60*60*1000);
    const weekBRT = toBRT(new Date()); const wd = weekBRT.getDay();
    weekBRT.setDate(weekBRT.getDate() + (wd===0?-6:1-wd)); weekBRT.setHours(0,0,0,0);
    const weekUTC = new Date(weekBRT.getTime() + 3*60*60*1000);

    const cadastradosSemana = produtos.filter(p => p.createdAt && new Date(p.createdAt) >= weekUTC).length;
    const vendidosSemana    = produtos.filter(p => p.soldAt    && new Date(p.soldAt)    >= weekUTC).length;

    return res.status(200).json({ ok: true, cadastradosSemana, vendidosSemana });
  }

  // ── POST salvar (criar ou editar) ──────────────────────────
  if (req.method === "POST" && action === "salvar") {
    const { id, codigo, tipo, descricao, capacidade, preco, fotos } = req.body || {};
    if (!codigo || !descricao || preco === undefined)
      return res.status(400).json({ ok: false, error: "codigo, descricao e preco são obrigatórios" });

    const db = await dbGet(VENDAS_KEY) || defaultDB();
    const fotosArr = Array.isArray(fotos) ? fotos.slice(0, 6) : [];

    if (id) {
      const idx = db.produtos.findIndex(p => p.id === id);
      if (idx < 0) return res.status(404).json({ ok: false, error: "Produto não encontrado" });
      db.produtos[idx] = { ...db.produtos[idx], codigo, tipo: tipo||null, descricao, capacidade: capacidade||null, preco, fotos: fotosArr, updatedAt: new Date().toISOString() };
      await dbSet(VENDAS_KEY, db);
      return res.status(200).json({ ok: true, produto: db.produtos[idx] });
    } else {
      const produto = {
        id: Date.now().toString(), codigo, tipo: tipo||null, descricao,
        capacidade: capacidade||null, preco, fotos: fotosArr,
        vendido: false, soldAt: null,
        createdAt: new Date().toISOString(), updatedAt: new Date().toISOString(),
      };
      db.produtos.unshift(produto);
      await dbSet(VENDAS_KEY, db);
      return res.status(200).json({ ok: true, produto });
    }
  }

  // ── POST vender ────────────────────────────────────────────
  // Cria ficha no financeiro em "emitir_nf" e marca produto como vendido
  if (req.method === "POST" && action === "vender") {
    const { produtoId, nomeCliente, telefone, cpfCnpj } = req.body || {};
    if (!produtoId || !nomeCliente)
      return res.status(400).json({ ok: false, error: "produtoId e nomeCliente obrigatórios" });

    const [db, fin] = await Promise.all([
      dbGet(VENDAS_KEY) || defaultDB(),
      dbGet(FIN_KEY)    || defaultFin(),
    ]);

    const idx = db.produtos.findIndex(p => p.id === produtoId);
    if (idx < 0) return res.status(404).json({ ok: false, error: "Produto não encontrado" });
    const p = db.produtos[idx];

    const now = new Date().toISOString();
    const precoFmt = parseFloat(p.preco).toLocaleString("pt-BR",{minimumFractionDigits:2,style:"currency",currency:"BRL"});

    // Cria ficha no financeiro
    const ficha = {
      id:          `venda-${Date.now()}`,
      pipefyId:    `venda-${Date.now()}`,
      osCode:      p.codigo,
      nomeContato: nomeCliente,
      telefone:    telefone || null,
      cpfCnpj:     cpfCnpj  || null,
      title:       `${p.tipo ? p.tipo + " — " : ""}${p.descricao.substring(0,60)}`,
      descricao:   `${p.descricao}${p.capacidade ? " — " + p.capacidade : ""} | Valor: ${precoFmt}`,
      equipamento: p.tipo || null,
      preco:       p.preco,
      origem:      "venda_equipamento",
      phaseId:     "emitir_nf",
      createdAt:   now, movedAt: now,
      history:     [
        { phaseId: "emitir_nf", ts: now },
      ],
    };
    if (!Array.isArray(fin.records))  fin.records  = [];
    if (!Array.isArray(fin.syncedIds)) fin.syncedIds = [];
    fin.records.unshift(ficha);

    // Marca produto como vendido
    db.produtos[idx] = { ...p, vendido: true, soldAt: now, compradorNome: nomeCliente, updatedAt: now };

    await Promise.all([dbSet(VENDAS_KEY, db), dbSet(FIN_KEY, fin)]);
    return res.status(200).json({ ok: true, ficha, produto: db.produtos[idx] });
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
