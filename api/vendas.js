const UPSTASH_URL   = (process.env.UPSTASH_URL   || "").replace(/['"]/g, "").trim();
const UPSTASH_TOKEN = (process.env.UPSTASH_TOKEN || "").replace(/['"]/g, "").trim();
const VENDAS_KEY    = "reparoeletro_vendas";
const FIN_KEY       = "reparoeletro_financeiro";
const PIPE_ID       = "305832912";
const PIPEFY_API    = "https://api.pipefy.com/graphql";

async function pipefyQuery(query) {
  const r = await fetch(PIPEFY_API, {
    method: "POST",
    headers: { "Content-Type": "application/json", Authorization: "Bearer " + (process.env.PIPEFY_TOKEN||"").trim() },
    body: JSON.stringify({ query }),
  });
  const j = await r.json();
  if (j.errors) throw new Error(j.errors[0].message);
  return j.data;
}

async function getProntoParaVendaPhaseId() {
  const data = await pipefyQuery(
    "query { pipe(id: \"" + PIPE_ID + "\") { phases { id name } } }"
  );
  const phases = data?.pipe?.phases || [];
  const phase = phases.find(p => p.name.toLowerCase().includes("pronto para venda"));
  return phase?.id || null;
}

async function criarCardPipefy(phaseId, produto, nomeCliente, telefone) {
  try {
    const titulo = (produto.tipo ? produto.tipo + " — " : "") + (produto.codigo || produto.descricao.substring(0,40));
    const descCompleta = produto.descricao + (produto.capacidade ? " — " + produto.capacidade : "");
    const precoFmt = parseFloat(produto.preco).toLocaleString("pt-BR",{minimumFractionDigits:2,style:"currency",currency:"BRL"});
    const data = await pipefyQuery(
      "mutation { createCard(input: { pipe_id: \"" + PIPE_ID + "\" phase_id: \"" + phaseId + "\" title: \"" + titulo.replace(/"/g,"'") + "\" fields_attributes: [ { field_id: \"nome_do_contato\" field_value: \"" + nomeCliente.replace(/"/g,"'") + "\" }, { field_id: \"telefone\" field_value: \"" + (telefone||"").replace(/"/g,"'") + "\" }, { field_id: \"descri_o\" field_value: \"" + descCompleta.replace(/"/g,"'") + " | Valor: " + precoFmt + "\" } ] }) { card { id title } } }"
    );
    return data?.createCard?.card?.id || null;
  } catch(e) {
    console.error("Pipefy createCard:", e.message);
    return null;
  }
}

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

    const cadastradosHoje   = produtos.filter(p => p.createdAt && new Date(p.createdAt) >= todayUTC).length;
    const cadastradosSemana = produtos.filter(p => p.createdAt && new Date(p.createdAt) >= weekUTC).length;

    const vendidosHoje      = produtos.filter(p => p.soldAt && new Date(p.soldAt) >= todayUTC).length;
    const vendidosSemana    = produtos.filter(p => p.soldAt && new Date(p.soldAt) >= weekUTC).length;

    const vendaLojaHoje     = produtos.filter(p => p.soldAt && new Date(p.soldAt) >= todayUTC   && p.vendedor === "Loja").length;
    const vendaLojaSemana   = produtos.filter(p => p.soldAt && new Date(p.soldAt) >= weekUTC    && p.vendedor === "Loja").length;
    const vendaOnlineHoje   = produtos.filter(p => p.soldAt && new Date(p.soldAt) >= todayUTC   && p.vendedor === "Online").length;
    const vendaOnlineSemana = produtos.filter(p => p.soldAt && new Date(p.soldAt) >= weekUTC    && p.vendedor === "Online").length;

    return res.status(200).json({
      ok: true,
      cadastradosHoje, cadastradosSemana,
      vendidosHoje, vendidosSemana,
      vendaLojaHoje, vendaLojaSemana,
      vendaOnlineHoje, vendaOnlineSemana,
    });
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
    const { produtoId, nomeCliente, telefone, cpfCnpj, vendedor, modalidade } = req.body || {};
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

    // Marca produto como vendido — salva vendedor (pessoa) e modalidade (canal) separados
    db.produtos[idx] = { ...p, vendido: true, soldAt: now, compradorNome: nomeCliente,
      nomeVendedor: vendedor||null, modalidade: modalidade||null,
      // retrocompatibilidade: vendedor mantém a modalidade para código legado
      vendedor: modalidade||vendedor||null,
      updatedAt: now };

    await Promise.all([dbSet(VENDAS_KEY, db), dbSet(FIN_KEY, fin)]);

    // Cria card no Pipefy na fase "Pronto para Venda" (async, não bloqueia resposta)
    let pipefyCardId = null;
    try {
      const phaseId = await getProntoParaVendaPhaseId();
      if (phaseId) {
        pipefyCardId = await criarCardPipefy(phaseId, p, nomeCliente, telefone);
        // Atualiza ficha financeiro com o pipefyId real
        if (pipefyCardId) {
          ficha.pipefyId = String(pipefyCardId);
          const fichaIdx = fin.records.findIndex(r => r.id === ficha.id);
          if (fichaIdx >= 0) fin.records[fichaIdx].pipefyId = String(pipefyCardId);
          await dbSet(FIN_KEY, fin);
        }
      }
    } catch(e) { console.error("Pipefy card creation:", e.message); }

    return res.status(200).json({ ok: true, ficha, produto: db.produtos[idx], pipefyCardId });
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
