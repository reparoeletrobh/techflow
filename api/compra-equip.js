// compra-equip.js — Dash de Compra de Equipamentos
const https  = require("https");

const UPSTASH_URL    = (process.env.UPSTASH_URL   || "").replace(/['"]/g,"").trim();
const UPSTASH_TOKEN  = (process.env.UPSTASH_TOKEN || "").replace(/['"]/g,"").trim();
const PIPEFY_API     = "https://api.pipefy.com/graphql";
const PIPE_ID        = "305832912";
const COMPRA_KEY     = "reparoeletro_compra_equip";

async function dbGet(key) {
  try {
    const r = await fetch(UPSTASH_URL + "/pipeline", {
      method: "POST",
      headers: { Authorization: "Bearer " + UPSTASH_TOKEN, "Content-Type": "application/json" },
      body: JSON.stringify([["GET", key]]),
    });
    const j = await r.json();
    return j[0]?.result ? JSON.parse(j[0].result) : null;
  } catch(e) { return null; }
}

async function dbSet(key, value) {
  try {
    const r = await fetch(UPSTASH_URL + "/pipeline", {
      method: "POST",
      headers: { Authorization: "Bearer " + UPSTASH_TOKEN, "Content-Type": "application/json" },
      body: JSON.stringify([["SET", key, JSON.stringify(value)]]),
    });
    const j = await r.json();
    return j[0]?.result === "OK";
  } catch(e) { return false; }
}

function defaultDB() { return { fichas: [], syncedIds: [] }; }

async function pipefyQuery(query) {
  const token = (process.env.PIPEFY_TOKEN || "").trim();
  const r = await fetch(PIPEFY_API, {
    method: "POST",
    headers: { "Content-Type": "application/json", Authorization: "Bearer " + token },
    body: JSON.stringify({ query }),
  });
  const j = await r.json();
  if (j.errors) throw new Error(j.errors[0].message);
  return j.data;
}

// Busca cards em "Analise de Compra" no Pipefy (query em 2 etapas)
async function fetchAnaliseCompra() {
  // Etapa 1: buscar só IDs e nomes das fases (sem cards — baixa complexidade)
  const phasesData = await pipefyQuery(
    'query { pipe(id: "' + PIPE_ID + '") { phases { id name } } }'
  );
  const phases = phasesData?.pipe?.phases || [];
  const ph = phases.find(p => {
    const n = p.name.toLowerCase().trim().normalize('NFD').replace(/[\u0300-\u036f]/g,'');
    return n === 'analise de compra' || (n.includes('analise') && n.includes('compra'));
  });
  if (!ph) return [];

  // Etapa 2: buscar todos os cards com paginação por cursor
  const allEdges = [];
  let cursor = null;
  let pagina = 0;
  do {
    const afterClause = cursor ? ', after: "' + cursor + '"' : '';
    const cardsData = await pipefyQuery(
      'query { phase(id: "' + ph.id + '") { cards(first: 30' + afterClause + ') { pageInfo { hasNextPage endCursor } edges { node { id title fields { name value } } } } } }'
    );
    const page = cardsData?.phase?.cards;
    if (!page) break;
    allEdges.push(...(page.edges || []));
    if (!page.pageInfo?.hasNextPage) break;
    cursor = page.pageInfo.endCursor;
    pagina++;
  } while (pagina < 20); // máx 20 páginas = 600 cards

  return allEdges.map(({ node }) => {
    const fields = node.fields || [];
    const get = (kw) => fields.find(f => f.name.toLowerCase().includes(kw))?.value || "";
    return {
      pipefyId:    String(node.id),
      title:       node.title || "",
      nomeContato: get("nome"),
      telefone:    get("telefone") || get("fone"),
      descricao:   get("descri") || get("empresa"),
      endereco:    get("endere"),
    };
  });
}

module.exports = async function handler(req, res) {
  // 🔐 TF-AUTH (Fase 1): chave obrigatória em toda chamada
  const _tfk = (req.query && req.query.k) || req.headers['x-tf-key'] || '';
  if (_tfk !== ((process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim())) {
    return res.status(401).json({ ok: false, error: 'não autorizado' });
  }

  res.setHeader("Access-Control-Allow-Origin", "https://reparoeletroadm.com");
  res.setHeader("X-Content-Type-Options","nosniff");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(200).end();
  const { action } = req.query;

  // ── GET load ─────────────────────────────────────────────
  if (action === "load") {
    const db = await dbGet(COMPRA_KEY) || defaultDB();
    return res.status(200).json({ ok: true, fichas: db.fichas || [] });
  }

  // ── GET sync — busca fichas da fase "Analise de Compra" no Pipefy
  if (action === "sync") {
    const db = await dbGet(COMPRA_KEY) || defaultDB();
    if (!Array.isArray(db.fichas))    db.fichas    = [];
    if (!Array.isArray(db.syncedIds)) db.syncedIds = [];
    let added = 0, pipefyError = null;
    try {
      const cards = await fetchAnaliseCompra();
      if (cards.length > 0) {
        const idsNaFase = new Set(cards.map(c => c.pipefyId));
        db.syncedIds = db.syncedIds.filter(id => idsNaFase.has(id));
      }
      for (const card of cards) {
        const jaExiste = db.fichas.find(f => f.pipefyId === card.pipefyId);
        if (jaExiste) continue; // já existe no sistema — não re-adicionar
        if (db.syncedIds.includes(card.pipefyId)) continue;
        db.fichas.unshift({
          id:          card.pipefyId,
          pipefyId:    card.pipefyId,
          title:       card.title,
          nomeContato: card.nomeContato || card.title,
          telefone:    card.telefone    || "",
          descricao:   card.descricao   || "",
          fotos:       [],
          recomendacao: null,     // "sim" | "nao" | null
          status:       "analise",// "analise" | "comprado" | "nao_comprado"
          createdAt:    new Date().toISOString(),
        });
        db.syncedIds.push(card.pipefyId);
        added++;
      }
      if (added > 0) await dbSet(COMPRA_KEY, db);
    } catch(e) { pipefyError = e.message; }
    const _diag = await fetchAnaliseCompra().catch(()=>[]);
    return res.status(200).json({ ok:true, added, pipefyError, totalNaFase: _diag.length, titulos: _diag.map(x=>x.title||x.nomeContato).slice(0,40) });
  }

  // ── POST recomendar — registra recomendação
  if (req.method === "POST" && action === "recomendar") {
    const { id, recomendacao } = req.body || {};
    if (!id || !recomendacao) return res.status(400).json({ ok: false, error: "id e recomendacao obrigatorios" });
    const db = await dbGet(COMPRA_KEY) || defaultDB();
    const f  = db.fichas.find(x => x.id === id);
    if (!f) return res.status(404).json({ ok: false, error: "Ficha nao encontrada" });
    f.recomendacao    = recomendacao; // "sim" | "nao"
    f.recomendadoAt   = new Date().toISOString();
    await dbSet(COMPRA_KEY, db);
    return res.status(200).json({ ok: true, ficha: f });
  }

  // ── 🧹 POST limpar-analises: esvazia a coluna Em Análise ──
  // As análises acumuladas vieram da sincronização automática com o quadro e
  // não passaram pela avaliação de compra. A partir de agora a entrada é pelo
  // botão Comprar no Conflitos Bot, com preço confirmado.
  if (req.method === "POST" && action === "limpar-analises") {
    const db = (await dbGet(COMPRA_KEY)) || { fichas: [] };
    const antes = (db.fichas || []).length;
    const emAnalise = (db.fichas || []).filter(f => String(f.status || "") === "analise");
    if (String(req.query.aplicar || "") !== "1") {
      return res.status(200).json({ ok: true, modo: "prévia",
        vaoSerRemovidas: emAnalise.length, totalNaBase: antes,
        L: emAnalise.slice(0, 60).map(f => String(f.cliente || f.nome || "?").slice(0, 24) +
          " " + String(f.telefone || "").slice(-4) +
          " | " + String(f.equipamento || f.descricao || "").slice(0, 30)),
        oQueVaiAcontecer: "as fichas em análise saem; comprados e não comprados ficam",
        dica: "para limpar: &aplicar=1" });
    }
    // 🗄️ guarda cópia antes de remover: dado apagado sem backup não volta
    try {
      await dbSet("reparoeletro_compra_equip_lixeira",
        { itens: emAnalise, removidoEm: new Date().toISOString() });
    } catch (e) {}
    db.fichas = (db.fichas || []).filter(f => String(f.status || "") !== "analise");
    await dbSet(COMPRA_KEY, db);
    const conf = (await dbGet(COMPRA_KEY)) || { fichas: [] };
    const restou = (conf.fichas || []).filter(f => String(f.status || "") === "analise").length;
    if (restou) return res.status(200).json({ ok: false,
      error: "a limpeza não persistiu — ainda há " + restou + " em análise" });
    return res.status(200).json({ ok: true,
      removidas: antes - (conf.fichas || []).length,
      restaram: (conf.fichas || []).length,
      backup: "cópia em reparoeletro_compra_equip_lixeira" });
  }

  // ── 🛒 POST avaliar-compra: entra em Em Análise com preço confirmado ──
  // Chamado pelo botão Comprar do Conflitos Bot. A análise só existe quando
  // alguém decidiu avaliar e informou por quanto — sem preço não há o que
  // analisar, e era isso que enchia a coluna de fichas sem decisão.
  if (req.method === "POST" && action === "avaliar-compra") {
    const b = req.body || {};
    const preco = Number(String(b.preco || "").toString().replace(/[^\d,.-]/g, "")
      .replace(/\./g, "").replace(",", "."));
    if (!(preco > 0)) {
      return res.status(400).json({ ok: false, error: "informe o preço da avaliação" });
    }
    if (!String(b.cliente || b.nome || "").trim()) {
      return res.status(400).json({ ok: false, error: "informe o cliente" });
    }
    const db = (await dbGet(COMPRA_KEY)) || { fichas: [] };
    db.fichas = db.fichas || [];
    const d8v = t => String(t || "").replace(/\D/g, "").slice(-8);
    const tel = d8v(b.telefone);
    // não duplica avaliação aberta do mesmo cliente e equipamento
    const ja = db.fichas.find(f => String(f.status || "") === "analise" &&
      d8v(f.telefone) === tel && tel.length >= 8);
    if (ja) {
      return res.status(200).json({ ok: false,
        error: "este cliente já tem uma avaliação em análise",
        existente: { cliente: ja.cliente, preco: ja.precoAvaliado || ja.preco || 0 } });
    }
    const ficha = {
      id: "ava_" + Date.now().toString(36) + Math.random().toString(36).slice(2, 5),
      cliente: String(b.cliente || b.nome).trim().slice(0, 60),
      telefone: String(b.telefone || "").replace(/\D/g, ""),
      equipamento: String(b.equipamento || b.descricao || "").slice(0, 90),
      descricao: String(b.descricao || b.equipamento || "").slice(0, 200),
      precoAvaliado: preco,
      preco: preco,
      origem: String(b.origem || "conflitos_bot"),
      conflitoId: b.conflitoId || null,
      avaliadoPor: String(b.quem || "").slice(0, 40),
      fotos: [], recomendacao: null,
      status: "analise",
      createdAt: new Date().toISOString(),
    };
    db.fichas.unshift(ficha);
    await dbSet(COMPRA_KEY, db);
    const conf = (await dbGet(COMPRA_KEY)) || { fichas: [] };
    const ok2 = (conf.fichas || []).some(f => f.id === ficha.id);
    if (!ok2) return res.status(200).json({ ok: false, error: "não persistiu — tente de novo" });
    return res.status(200).json({ ok: true, ficha });
  }

  // ── POST status — marca comprado / nao_comprado
  if (req.method === "POST" && action === "status") {
    const { id, status } = req.body || {};
    if (!id || !status) return res.status(400).json({ ok: false, error: "id e status obrigatorios" });
    const db = await dbGet(COMPRA_KEY) || defaultDB();
    const f  = db.fichas.find(x => x.id === id);
    if (!f) return res.status(404).json({ ok: false, error: "Ficha nao encontrada" });
    const antesStatus = String(f.status || "");
    f.status    = status;
    f.statusAt  = new Date().toISOString();
    await dbSet(COMPRA_KEY, db);

    // ── 📦 equipamento comprado: avisa o almoxarifado ──
    // Sem este aviso o equipamento chega e ninguém sabe para qual setor levar,
    // e ele fica parado na recepção até alguém perguntar.
    let almox = null;
    if (status === "comprado" && antesStatus !== "comprado") {
      try {
        const KA = "reparoeletro_almoxarifado";
        const adb = (await dbGet(KA)) || { itens: [] };
        adb.itens = adb.itens || [];
        // não duplica se já houver entrada desta compra
        const jaTem = adb.itens.some(x => String(x.compraEquipId || "") === String(f.id));
        if (!jaTem) {
          const txt = String(f.equipamento || f.descricao || "").toLowerCase();
          // 🏷️ o setor decide para onde o equipamento vai fisicamente
          const setor = /\btv\b|televis|monitor|smart/.test(txt) ? "TV"
            : /micro-?ondas|forno|fog[ãa]o|cooktop/.test(txt) ? "Linha Branca — cocção"
            : /purificador|bebedouro|filtro/.test(txt) ? "Linha Branca — água"
            : /adega|climatiza|geladeira|freezer/.test(txt) ? "Linha Branca — refrigeração"
            : "A definir";
          const item = {
            id: "alm_" + Date.now().toString(36) + Math.random().toString(36).slice(2, 5),
            compraEquipId: f.id,
            origem: "compra de equipamento",
            cliente: f.cliente || f.nome || "—",
            telefone: f.telefone || "",
            equipamento: String(f.equipamento || f.descricao || "").slice(0, 90),
            valorPago: Number(f.precoAvaliado || f.preco || 0),
            setorDestino: setor,
            status: "aguardando_recebimento",
            criadoEm: new Date().toISOString(),
            observacao: "equipamento comprado do cliente — levar para " + setor,
          };
          adb.itens.unshift(item);
          await dbSet(KA, adb);
          const confA = (await dbGet(KA)) || { itens: [] };
          const okA = (confA.itens || []).some(x => x.id === item.id);
          almox = okA ? { criado: true, setor, id: item.id }
                      : { criado: false, erro: "a ficha do almoxarifado não persistiu" };
        } else { almox = { criado: false, motivo: "já existe ficha no almoxarifado" }; }
      } catch (e) { almox = { criado: false, erro: e.message }; }
    }
    return res.status(200).json({ ok: true, ficha: f, almoxarifado: almox });
  }

  // ── POST add-foto — adiciona URL de foto à ficha
  if (req.method === "POST" && action === "add-foto") {
    const { id, fotoBase64, fotoNome } = req.body || {};
    if (!id || !fotoBase64) return res.status(400).json({ ok: false, error: "id e fotoBase64 obrigatorios" });
    const db = await dbGet(COMPRA_KEY) || defaultDB();
    const f  = db.fichas.find(x => x.id === id);
    if (!f) return res.status(404).json({ ok: false, error: "Ficha nao encontrada" });
    if (!Array.isArray(f.fotos)) f.fotos = [];
    if (f.fotos.length >= 6) return res.status(400).json({ ok: false, error: "Maximo 6 fotos por ficha" });
    f.fotos.push({ base64: fotoBase64, nome: fotoNome || "foto.jpg", addedAt: new Date().toISOString() });
    await dbSet(COMPRA_KEY, db);
    return res.status(200).json({ ok: true, fotos: f.fotos.length });
  }

  // ── POST remover-foto
  if (req.method === "POST" && action === "remover-foto") {
    const { id, fotoIdx } = req.body || {};
    const db = await dbGet(COMPRA_KEY) || defaultDB();
    const f  = db.fichas.find(x => x.id === id);
    if (!f) return res.status(404).json({ ok: false, error: "Ficha nao encontrada" });
    f.fotos = (f.fotos || []).filter((_, i) => i !== fotoIdx);
    await dbSet(COMPRA_KEY, db);
    return res.status(200).json({ ok: true });
  }

  // ── POST excluir
  if (req.method === "POST" && action === "excluir") {
    const { id } = req.body || {};
    const db = await dbGet(COMPRA_KEY) || defaultDB();
    db.fichas    = db.fichas.filter(f => f.id !== id);
    db.syncedIds = db.syncedIds.filter(s => s !== id);
    await dbSet(COMPRA_KEY, db);
    return res.status(200).json({ ok: true });
  }

  // ── POST limpar-concluidos
  if (req.method === "POST" && action === "limpar-concluidos") {
    // 🚫 DESATIVADO: este botão apagou 59 equipamentos comprados em 10/08.
    // Foi removido da tela e o endpoint só responde com liberar=1, para uso técnico.
    if (String(req.query.liberar || '') !== '1') {
      return res.status(200).json({ ok: false,
        error: '🚫 função desativada — ela apagava os equipamentos já comprados',
        motivo: 'em 10/08 removeu 59 fichas sem possibilidade de desfazer',
        seRealmentePrecisar: 'acrescente &liberar=1&confirmar=1' });
    }
    const db = await dbGet(COMPRA_KEY) || defaultDB();
    const before = db.fichas.length;
    const removidas = db.fichas.filter(f => f.status !== "analise");
    // 💾 BACKUP antes de apagar — em 10/08 este botão apagou 59 comprados sem volta
    if (removidas.length) {
      try {
        await dbSet('reparoeletro_compra_equip_lixeira', {
          em: new Date().toISOString(), quantidade: removidas.length, fichas: removidas });
      } catch (e) {}
    }
    // 🚧 trava: apagar muita coisa de uma vez exige confirmação explícita
    if (removidas.length > 15 && String(req.query.confirmar || '') !== '1') {
      return res.status(200).json({ ok: false,
        error: '🚧 isto apagaria ' + removidas.length + ' fichas (comprados e não comprados)',
        detalhe: removidas.slice(0, 10).map(f => (f.nomeContato || '?') + ' | ' + f.status),
        oQueFazer: 'confirme na tela se é isso mesmo — as fichas foram copiadas para a lixeira' });
    }
    db.fichas    = db.fichas.filter(f => f.status === "analise");
    db.syncedIds = db.fichas.map(f => f.pipefyId);
    await dbSet(COMPRA_KEY, db);
    return res.status(200).json({ ok: true, removed: before - db.fichas.length,
      backup: removidas.length ? 'cópia guardada em reparoeletro_compra_equip_lixeira' : undefined });
  }

  if (req.method === "POST" && action === "desmarcar-cadastrado-vendas") {
    const { id: dId } = req.body || {};
    if (!dId) return res.status(400).json({ ok: false, error: "id obrigatorio" });
    const dDb = await dbGet(COMPRA_KEY) || defaultDB();
    const dF = dDb.fichas.find(f => f.id === dId);
    if (!dF) return res.status(404).json({ ok: false, error: "ficha nao encontrada" });
    delete dF.cadastradoVendas;
    await dbSet(COMPRA_KEY, dDb);
    return res.status(200).json({ ok: true });
  }

  if (req.method === "POST" && action === "marcar-cadastrado-vendas") {
    const { id, dadosVendas } = req.body || {};
    if (!id) return res.status(400).json({ ok: false, error: "id obrigatorio" });
    const db = await dbGet(COMPRA_KEY) || defaultDB();
    const f  = db.fichas.find(f => f.id === id);
    if (!f) return res.status(404).json({ ok: false, error: "Ficha nao encontrada" });
    f.cadastradoVendas = true;
    if (dadosVendas) f.dadosVendas = dadosVendas;
    await dbSet(COMPRA_KEY, db);
    return res.status(200).json({ ok: true });
  }

  return res.status(404).json({ ok: false, error: "Acao nao encontrada" });
};
