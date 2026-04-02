// api/metricas.js — Painel de Métricas: CAC, Ticket Médio, Custo por Ficha
const PIPEFY_API     = "https://api.pipefy.com/graphql";
const PIPE_ID        = "305832912";
const UPSTASH_URL    = (process.env.UPSTASH_URL    || "").replace(/['"]/g,"").trim();
const UPSTASH_TOKEN  = (process.env.UPSTASH_TOKEN  || "").replace(/['"]/g,"").trim();
const METRICAS_KEY   = "reparoeletro_metricas";
const BOARD_KEY      = "reparoeletro_board";
const LOGS_KEY       = "reparoeletro_logs";

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

async function pipefyQuery(query) {
  const r = await fetch(PIPEFY_API, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${(process.env.PIPEFY_TOKEN || "").trim()}`,
    },
    body: JSON.stringify({ query }),
  });
  const j = await r.json();
  if (j.errors) {
    const msg = Array.isArray(j.errors)
      ? j.errors.map(e => e.message || String(e)).join("; ")
      : String(j.errors);
    throw new Error(msg);
  }
  return j.data;
}

// Busca cards em ERP com valor + data de entrada (phases_history) — paginação completa
async function fetchErpCards() {
  try {
    const ERP_PHASE_ID = "339008925";
    const all = [];
    let cursor = null, hasNext = true;
    while (hasNext) {
      const after = cursor ? `, after: "${cursor}"` : "";
      const data = await pipefyQuery(`query {
        phase(id: "${ERP_PHASE_ID}") {
          cards(first: 50${after}) {
            pageInfo { hasNextPage endCursor }
            edges {
              node {
                id title
                fields { name value }
                phases_history { phase { id } firstTimeIn }
              }
            }
          }
        }
      }`);
      const page = data?.phase?.cards;
      hasNext = page?.pageInfo?.hasNextPage ?? false;
      cursor  = page?.pageInfo?.endCursor ?? null;
      for (const { node } of (page?.edges || [])) {
        const fields = node.fields || [];
        const valor  = fields.find(f => f.name.toLowerCase().includes("valor"))?.value || "0";
        const num    = parseFloat(String(valor).replace(/[^\d.,]/g,"").replace(",",".")) || 0;
        // Data de entrada em ERP via phases_history
        const hist = (node.phases_history || []).find(h => h.phase?.id === ERP_PHASE_ID && h.firstTimeIn);
        const entradaTs = hist?.firstTimeIn || null;
        const entradaDate = entradaTs ? new Date(new Date(entradaTs).getTime() - 3*60*60*1000).toISOString().slice(0,10) : null;
        all.push({ id: String(node.id), title: node.title, valor: num, entradaTs, entradaDate });
      }
    }
    return all;
  } catch(e) {
    console.error("fetchErpCards:", e.message);
    return [];
  }
}

// Helpers de data
function toDateStr(ts) {
  var d = new Date(ts - 3 * 60 * 60 * 1000);
  return d.toISOString().slice(0, 10);
}
function weekStart(dateStr) {
  const [y,m,d] = dateStr.split("-").map(Number);
  const dt = new Date(y, m-1, d);
  const day = dt.getDay();
  dt.setDate(dt.getDate() - (day === 0 ? 6 : day - 1));
  const yr = dt.getFullYear(), mo = String(dt.getMonth()+1).padStart(2,"0"), dy = String(dt.getDate()).padStart(2,"0");
  return yr+"-"+mo+"-"+dy;
}
function monthKey(ts) {
  var d = new Date(ts - 3 * 60 * 60 * 1000);
  return d.toISOString().slice(0, 7);
}

// Calcula métricas agregadas para um conjunto de dias
function calcMetricas(dias) {
  let fichas = 0, investimento = 0, erpCount = 0, valorErp = 0, coletasSolic = 0, orcEnviado = 0;
  for (const d of dias) {
    fichas        += d.fichas        || 0;
    investimento  += d.investimento  || 0;
    erpCount      += d.erpCount      || 0;
    valorErp      += d.valorErp      || 0;
    coletasSolic  += d.coletasSolic  || 0;
    orcEnviado    += d.orcEnviado    || 0;
  }
  return {
    fichas, investimento, erpCount, valorErp, coletasSolic, orcEnviado,
    cac:          erpCount   > 0 ? +(investimento / erpCount).toFixed(2)   : null,
    ticketMedio:  erpCount   > 0 ? +(valorErp     / erpCount).toFixed(2)   : null,
    custoPorFicha: fichas    > 0 ? +(investimento / fichas).toFixed(2)     : null,
    roi:          investimento > 0 ? +((valorErp - investimento) / investimento * 100).toFixed(1) : null,
  };
}

module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(200).end();

  const action = req.query.action || "";

  try {

  // ── GET load — carrega todos os dados de métricas ─────────────────────────
  if (action === "load") {
    const [rawDb, logsData] = await Promise.all([
      dbGet(METRICAS_KEY),
      dbGet(LOGS_KEY),
    ]);
    const db = rawDb || { dias: [] };
    if (!Array.isArray(db.dias)) db.dias = [];

    // ERP ao vivo do Pipefy — com paginação completa e agrupamento por data de entrada
    let erpCards = [];
    try { erpCards = await fetchErpCards(); } catch(e) {}

    // Agrupa ERPs por data de entrada (phases_history) — fonte primária
    const erpPorDia    = {};
    const valorErpPorDia = {};
    for (const card of erpCards) {
      if (!card.entradaDate) continue;
      erpPorDia[card.entradaDate]     = (erpPorDia[card.entradaDate]     || 0) + 1;
      valorErpPorDia[card.entradaDate] = (valorErpPorDia[card.entradaDate] || 0) + (card.valor || 0);
    }

    const erpAtual = {
      count: erpCards.length,
      valor: erpCards.reduce((s, c) => s + c.valor, 0),
      cards: erpCards,
    };

    // metaLog para Coletas e Orçamentos (esses permanecem do log local)
    const metaLog   = logsData?.metaLog || [];
    const coletaLog = metaLog.filter(m => m.phaseId === "coleta_solicitada");
    const orcLog    = metaLog.filter(m => m.phaseId === "aguardando_aprovacao");

    const coletaPorDia = {};
    const orcPorDia    = {};
    for (const e of coletaLog) {
      const d = toDateStr(new Date(e.timestamp).getTime());
      coletaPorDia[d] = (coletaPorDia[d] || 0) + 1;
    }
    for (const e of orcLog) {
      const d = toDateStr(new Date(e.timestamp).getTime());
      orcPorDia[d] = (orcPorDia[d] || 0) + 1;
    }

    // Constrói array de dias enriquecido
    // fichas/investimento/valorErp = lançamento manual (prioridade)
    // erpCount/coletasSolic/orcEnviado = SEMPRE metaLog (automático)
    // valorErp fallback: metaLog (quando board.js começar a gravar valor)
    const diasEnriq = db.dias.map(d => ({
      ...d,
      erpCount:     erpPorDia[d.data]    || 0,
      valorErp:     (d.valorErp != null && d.valorErp > 0) ? d.valorErp : (valorErpPorDia[d.data] || 0),
      coletasSolic: coletaPorDia[d.data] || 0,
      orcEnviado:   orcPorDia[d.data]    || 0,
    }));

    // Adiciona dias que aparecem no metaLog mas não têm lançamento manual
    const diasSet = new Set(diasEnriq.map(d => d.data));
    const todosDias = { ...erpPorDia };
    for (const d of Object.keys(coletaPorDia)) todosDias[d] = todosDias[d] || 0;
    for (const d of Object.keys(orcPorDia))    todosDias[d] = todosDias[d] || 0;
    for (const data of Object.keys(todosDias)) {
      if (!diasSet.has(data)) {
        diasEnriq.push({
          data,
          fichas:       0,
          investimento: 0,
          erpCount:     erpPorDia[data]    || 0,
          valorErp:     valorErpPorDia[data] || 0,
          coletasSolic: coletaPorDia[data] || 0,
          orcEnviado:   orcPorDia[data]    || 0,
        });
      }
    }
    diasEnriq.sort((a, b) => a.data.localeCompare(b.data));

    // --- RELATÓRIO DIÁRIO (últimos 30 dias) ---
    const hoje30 = Date.now() - 30*24*60*60*1000;
    const diario = diasEnriq
      .filter(d => {
        const [y,mo,dd] = (d.data||'').split('-').map(Number);
        return new Date(y, mo-1, dd).getTime() >= hoje30;
      })
      .sort((a,b) => a.data.localeCompare(b.data))
      .map(d => ({ ...d, ...calcMetricas([d]) }));

    // --- RELATÓRIO SEMANAL (últimas 12 semanas) ---
    const semanas = {};
    for (const d of diasEnriq) {
      const sk = weekStart(d.data || "");
      if (!semanas[sk]) semanas[sk] = [];
      semanas[sk].push(d);
    }
    const semanal = Object.entries(semanas)
      .sort(([a],[b]) => a.localeCompare(b))
      .slice(-12)
      .map(([semana, dias]) => ({ semana, ...calcMetricas(dias), dias: dias.length }));

    // --- RELATÓRIO MENSAL ---
    const meses = {};
    for (const d of diasEnriq) {
      const mk = d.data ? d.data.slice(0,7) : ""; // YYYY-MM
      if (!meses[mk]) meses[mk] = [];
      meses[mk].push(d);
    }
    const mensal = Object.entries(meses)
      .sort(([a],[b]) => a.localeCompare(b))
      .map(([mes, dias]) => ({ mes, ...calcMetricas(dias) }));

    // --- RESUMO GERAL ---
    const resumo = calcMetricas(diasEnriq);

    return res.status(200).json({
      ok: true,
      erpAtual,
      diario,
      semanal,
      mensal,
      resumo,
      totalDias: diasEnriq.length,
    });
  }

  // ── POST salvar-dia — salva ou atualiza dados de um dia ──────────────────
  if (req.method === "POST" && action === "salvar-dia") {
    const { data, fichas, investimento, erpCount, valorErp, coletasSolic, obs } = req.body || {};
    if (!data) return res.status(400).json({ ok: false, error: "data obrigatória (YYYY-MM-DD)" });

    const db = await dbGet(METRICAS_KEY) || { dias: [] };
    if (!Array.isArray(db.dias)) db.dias = [];

    const idx = db.dias.findIndex(d => d.data === data);
    const entry = {
      data,
      fichas:       parseInt(fichas)       || 0,
      investimento: parseFloat(investimento) || 0,
      erpCount:     parseInt(erpCount)     || 0,
      valorErp:     parseFloat(valorErp)   || 0,
      coletasSolic: parseInt(coletasSolic) || 0,
      obs:          obs || "",
      updatedAt:   new Date().toISOString(),
    };

    if (idx >= 0) db.dias[idx] = entry;
    else          db.dias.push(entry);

    await dbSet(METRICAS_KEY, db);
    return res.status(200).json({ ok: true, entry, ...calcMetricas([entry]) });
  }

  // ── GET erp-ao-vivo — busca ERP atual do Pipefy ──────────────────────────
  if (action === "erp-ao-vivo") {
    try {
      const cards = await fetchErpCards();
      const total = cards.reduce((s, c) => s + c.valor, 0);
      return res.status(200).json({ ok: true, count: cards.length, valorTotal: total, cards });
    } catch(e) {
      return res.status(200).json({ ok: false, error: e.message });
    }
  }

  // ── GET erp-por-data — ERP agrupado por data de entrada (Pipefy ao vivo)
  if (action === "erp-por-data") {
    try {
      const cards = await fetchErpCards();
      const byDate = {};
      for (const c of cards) {
        const dt = c.entradaDate || "sem-data";
        if (!byDate[dt]) byDate[dt] = { count: 0, valor: 0 };
        byDate[dt].count++;
        byDate[dt].valor += c.valor;
      }
      return res.status(200).json({ ok: true, total: cards.length, byDate });
    } catch(e) {
      return res.status(200).json({ ok: false, error: e.message });
    }
  }

  // ── DELETE deletar-dia — remove um dia ───────────────────────────────────
  if (req.method === "POST" && action === "deletar-dia") {
    const { data } = req.body || {};
    if (!data) return res.status(400).json({ ok: false, error: "data obrigatória" });
    const db = await dbGet(METRICAS_KEY) || { dias: [] };
    db.dias = (db.dias || []).filter(d => d.data !== data);
    await dbSet(METRICAS_KEY, db);
    return res.status(200).json({ ok: true });
  }

  // ── GET valor-erp-periodo — busca valor real dos ERPs do período direto no Pipefy
  if (action === "valor-erp-periodo") {
    const { de, ate } = req.query;
    if (!de || !ate) return res.status(400).json({ ok: false, error: "de e ate obrigatórios" });
    try {
      const logsData = await dbGet(LOGS_KEY);
      const metaLog  = logsData?.metaLog || [];
      // Filtra pipefyIds que entraram em ERP no período (BH timezone)
      const ids = metaLog
        .filter(m => {
          if (m.phaseId !== "erp_entrada") return false;
          const d = toDateStr(new Date(m.timestamp).getTime());
          return d >= de && d <= ate;
        })
        .map(m => m.pipefyId)
        .filter((v, i, a) => a.indexOf(v) === i); // unique

      if (!ids.length) return res.status(200).json({ ok: true, valor: 0, count: 0, ids: [] });

      // Busca valores no Pipefy em lotes de 10
      let totalValor = 0;
      for (let i = 0; i < ids.length; i += 10) {
        const lote   = ids.slice(i, i + 10);
        const aliases = lote.map((id, j) => `c${j}: card(id: "${id}") { id fields { name value } }`).join(" ");
        try {
          const data = await pipefyQuery(`query { ${aliases} }`);
          for (let j = 0; j < lote.length; j++) {
            const card   = data[`c${j}`];
            if (!card) continue;
            const valField = (card.fields || []).find(f => f.name.toLowerCase().includes("valor"));
            const num = parseFloat(String(valField?.value || "0").replace(/[^\d.,]/g,"").replace(",",".")) || 0;
            totalValor += num;
          }
        } catch(e) { console.error("valor-erp lote:", e.message); }
      }
      return res.status(200).json({ ok: true, valor: totalValor, count: ids.length, ids });
    } catch(e) {
      return res.status(200).json({ ok: false, error: e.message });
    }
  }

  // ── POST fix-metalog — corrige entradas ERP do metaLog
  if (req.method === "POST" && action === "fix-metalog") {
    const { removeAfter, removeBefore, addErps } = req.body || {};
    try {
      const logsData = await dbGet(LOGS_KEY) || {};
      let metaLog = logsData.metaLog || [];
      let removed = 0;
      if (removeAfter || removeBefore) {
        const before = removeBefore ? new Date(removeBefore).getTime() : Infinity;
        const after  = removeAfter  ? new Date(removeAfter).getTime()  : 0;
        metaLog = metaLog.filter(m => {
          if (m.phaseId !== "erp_entrada") return true;
          const ts = new Date(m.timestamp).getTime();
          if (ts >= after && ts <= before) { removed++; return false; }
          return true;
        });
      }
      let added = 0;
      if (Array.isArray(addErps)) {
        for (const entry of addErps) {
          const baseTs = new Date(entry.data + "T12:00:00-03:00").getTime();
          for (let i = 0; i < entry.count; i++) {
            metaLog.push({ phaseId: "erp_entrada", pipefyId: "manual-" + entry.data + "-" + i, valor: 0, timestamp: new Date(baseTs + i * 1000).toISOString(), manual: true });
            added++;
          }
        }
      }
      logsData.metaLog = metaLog;
      await dbSet(LOGS_KEY, logsData);
      return res.status(200).json({ ok: true, removed, added, total: metaLog.length });
    } catch(e) { return res.status(200).json({ ok: false, error: e.message }); }
  }

  return res.status(404).json({ ok: false, error: "Ação não encontrada" });
  } catch(e) {
    console.error("metricas handler error:", e.message, e.stack);
    return res.status(200).json({ ok: false, error: "Erro interno: " + e.message });
  }
};
