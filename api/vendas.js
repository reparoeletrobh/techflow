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

async function getReceberPhaseId() {
  const data = await pipefyQuery(
    "query { pipe(id: \"" + PIPE_ID + "\") { phases { id name } } }"
  );
  const phases = data?.pipe?.phases || [];
  const phase = phases.find(p => p.name.toLowerCase().includes("receber"));
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
    // Para produtos vendidos, não retorna fotos (economiza payload)
    const produtos = (db.produtos || []).map(p =>
      p.vendido ? { ...p, fotos: [] } : p
    );
    return res.status(200).json({ ok: true, produtos });
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
    // Limita a 3 fotos por produto e verifica tamanho total
    const fotosRaw = Array.isArray(fotos) ? fotos.filter(Boolean).slice(0, 3) : [];
    const fotosArr = fotosRaw;

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
      // Verifica tamanho antes de salvar
      const payload = JSON.stringify(db);
      if(payload.length > 900000) {
        // Remove fotos de produtos vendidos para liberar espaço
        db.produtos = db.produtos.map(p => p.vendido ? {...p, fotos:[]} : p);
      }
      try {
        await dbSet(VENDAS_KEY, db);
      } catch(e) {
        return res.status(500).json({ ok: false, error: "Erro ao salvar: banco de dados cheio. Contate o suporte." });
      }
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
    const dataVenda = new Date().toLocaleDateString("pt-BR",{timeZone:"America/Sao_Paulo"});

    // ── Ficha no Financeiro (emitir NF) ──────────────────────────────────
    const fichaId = `venda-${Date.now()}`;
    const ficha = {
      id:          fichaId,
      pipefyId:    fichaId,
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
      history:     [{ phaseId: "emitir_nf", ts: now }],
    };
    if (!Array.isArray(fin.records))   fin.records   = [];
    if (!Array.isArray(fin.syncedIds)) fin.syncedIds = [];
    fin.records.unshift(ficha);

    // ── Marca produto como vendido ────────────────────────────────────────
    db.produtos[idx] = { ...p, vendido: true, soldAt: now, compradorNome: nomeCliente,
      nomeVendedor: vendedor||null, modalidade: modalidade||null,
      vendedor: modalidade||vendedor||null,
      updatedAt: now };

    await Promise.all([dbSet(VENDAS_KEY, db), dbSet(FIN_KEY, fin)]);

    // ── Texto WhatsApp para almoxarifado ──────────────────────────────────
    const textoAlmox = [
      "📦 *SEPARAÇÃO — ALMOXARIFADO*",
      "",
      `📅 Data: ${dataVenda}`,
      `🏷️ Código: ${p.codigo || "—"}`,
      `📦 Tipo: ${p.tipo || "—"}`,
      `📝 Descrição: ${p.descricao}`,
      `💰 Valor: ${precoFmt}`,
      "",
      `👤 Comprador: ${nomeCliente}`,
      telefone ? `📱 Telefone: ${telefone}` : null,
      "",
      `🛒 Vendedor: ${vendedor || "—"}`,
      `🔖 Modalidade: ${modalidade || "—"}`,
    ].filter(l => l !== null).join("\n");

    // ── Pipefy: card em Receber (almoxarifado) ────────────────────────────
    let pipefyReceberCardId = null;
    try {
      const phaseReceber = await getReceberPhaseId();
      if (phaseReceber) {
        const tituloReceber = `VENDA — ${p.codigo || p.tipo || "Equipamento"} | ${nomeCliente}`;
        const descReceber   = [
          p.descricao,
          `Valor: ${precoFmt}`,
          `Vendedor: ${vendedor||"—"}`,
          `Modalidade: ${modalidade||"—"}`,
          "",
          textoAlmox
        ].join("\n");
        const dataAlmox = await pipefyQuery(
          "mutation { createCard(input: { pipe_id: \"" + PIPE_ID + "\" phase_id: \"" + phaseReceber + "\" title: \"" + tituloReceber.replace(/"/g,"'") + "\" fields_attributes: [ { field_id: \"descri_o\" field_value: \"" + descReceber.replace(/"/g,"'") + "\" } ] }) { card { id } } }"
        );
        pipefyReceberCardId = dataAlmox?.createCard?.card?.id || null;
      }
    } catch(e) { console.error("Pipefy Receber card:", e.message); }

    return res.status(200).json({
      ok: true, ficha,
      produto: db.produtos[idx],
      pipefyReceberCardId,
      textoAlmox,
    });
  }

  // ── POST retentar-pipefy — recria card no Pipefy para venda já feita ──────
  if (req.method === "POST" && action === "retentar-pipefy") {
    const { produtoId } = req.body || {};
    if (!produtoId) return res.status(400).json({ ok:false, error:"produtoId obrigatório" });
    const db = await dbGet(VENDAS_KEY) || defaultDB();
    const p  = db.produtos.find(x => x.id === produtoId);
    if (!p)  return res.status(404).json({ ok:false, error:"Produto não encontrado" });
    if (!p.vendido) return res.status(400).json({ ok:false, error:"Produto não foi vendido" });

    const precoFmt = parseFloat(p.preco).toLocaleString("pt-BR",{minimumFractionDigits:2,style:"currency",currency:"BRL"});
    const dataVenda = p.soldAt ? new Date(p.soldAt).toLocaleDateString("pt-BR",{timeZone:"America/Sao_Paulo"}) : new Date().toLocaleDateString("pt-BR",{timeZone:"America/Sao_Paulo"});
    const nomeCliente = p.compradorNome || "—";
    const telefone    = p.compradorTel  || "";
    const vendedor    = p.nomeVendedor  || p.vendedor || "—";
    const modalidade  = p.modalidade    || "—";

    const textoAlmox = [
      "📦 *SEPARAÇÃO — ALMOXARIFADO*","",
      `📅 Data: ${dataVenda}`,
      `🏷️ Código: ${p.codigo || "—"}`,
      `📦 Tipo: ${p.tipo || "—"}`,
      `📝 Descrição: ${p.descricao}`,
      `💰 Valor: ${precoFmt}`,"",
      `👤 Comprador: ${nomeCliente}`,
      telefone ? `📱 Telefone: ${telefone}` : null,"",
      `🛒 Vendedor: ${vendedor}`,
      `🔖 Modalidade: ${modalidade}`,
    ].filter(l => l !== null).join("\n");

    try {
      const phaseReceber = await getReceberPhaseId();
      if (!phaseReceber) return res.status(500).json({ ok:false, error:"Fase Receber não encontrada no Pipefy" });
      // Título com todas as infos (fallback caso campos não existam)
      const tituloCompleto = `VENDA — ${p.codigo || "—"} | ${p.tipo || "—"} ${p.descricao} | ${nomeCliente} | ${precoFmt} | ${vendedor} | ${modalidade}`;
      const titulo = tituloCompleto.replace(/"/g,"'").slice(0, 255);

      // Tenta criar o card — primeiro sem campos (garante criação)
      const mutation = `mutation { createCard(input: { pipe_id: "${PIPE_ID}" phase_id: "${phaseReceber}" title: "${titulo}" }) { card { id } } }`;
      const data = await pipefyQuery(mutation);
      const cardId = data?.createCard?.card?.id;
      if (!cardId) return res.status(500).json({ ok:false, error:"Pipefy não retornou ID do card", raw: JSON.stringify(data).slice(0,300) });
      return res.status(200).json({ ok:true, pipefyCardId:cardId, textoAlmox });
    } catch(e) {
      return res.status(500).json({ ok:false, error:e.message });
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

  // ── POST cancelar-venda — devolve produto ao estoque ───────────────────
  if (req.method === "POST" && action === "cancelar-venda") {
    const { id } = req.body || {};
    if (!id) return res.status(400).json({ ok: false, error: "id obrigatório" });
    const db = await dbGet(VENDAS_KEY) || defaultDB();
    const idx = db.produtos.findIndex(p => p.id === id);
    if (idx < 0) return res.status(404).json({ ok: false, error: "Produto não encontrado" });
    // Limpa todos os dados de venda, volta ao estado de estoque
    const p = db.produtos[idx];
    db.produtos[idx] = {
      ...p,
      vendido:      false,
      soldAt:       null,
      compradorNome: null,
      nomeVendedor: null,
      modalidade:   null,
      vendedor:     null,
      updatedAt:    new Date().toISOString(),
    };
    await dbSet(VENDAS_KEY, db);
    return res.status(200).json({ ok: true, produto: db.produtos[idx] });
  }

  return res.status(404).json({ ok: false, error: "Ação não encontrada" });
};
