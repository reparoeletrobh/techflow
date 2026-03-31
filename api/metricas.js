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

// Busca cards em ERP com valor_de_contrato
async function fetchErpCards() {
  try {
    const ERP_PHASE_ID = "339008925";
    const data = await pipefyQuery(`query {
      phase(id: "${ERP_PHASE_ID}") {
        cards(first: 50) {
          edges {
            node {
              id title
              fields { name value }
            }
          }
        }
      }
    }`);
    return (data?.phase?.cards?.edges || []).map(({ node }) => {
      const fields = node.fields || [];
      const valor  = fields.find(f => f.name.toLowerCase().includes("valor"))?.value || "0";
      const num    = parseFloat(String(valor).replace(/[^\d.,]/g,"").replace(",",".")) || 0;
      return { id: String(node.id), title: node.title, valor: num };
    });
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

    // ERP ao vivo do Pipefy
    let erpCards = [];
    try { erpCards = await fetchErpCards(); } catch(e) {}

    const erpAtual = {
      count:  erpCards.length,
      valor:  erpCards.reduce((s, c) => s + c.valor, 0),
      cards:  erpCards,
    };

    // metaLog para distribuição temporal dos ERPs e Coletas
    const metaLog    = logsData?.metaLog || [];
    const erpLog     = metaLog.filter(m => m.phaseId === "erp_entrada");
    const coletaLog  = metaLog.filter(m => m.phaseId === "coleta_solicitada");
    const orcLog     = metaLog.filter(m => m.phaseId === "aguardando_aprovacao");

    // Agrupa por dia
    const erpPorDia    = {};
    const coletaPorDia = {};
    const orcPorDia    = {};
    for (const e of erpLog) {
      const d = toDateStr(new Date(e.timestamp).getTime());
      erpPorDia[d] = (erpPorDia[d] || 0) + 1;
    }
    for (const e of coletaLog) {
      const d = toDateStr(new Date(e.timestamp).getTime());
      coletaPorDia[d] = (coletaPorDia[d] || 0) + 1;
    }
    for (const e of orcLog) {
      const d = toDateStr(new Date(e.timestamp).getTime());
      orcPorDia[d] = (orcPorDia[d] || 0) + 1;
    }

    // Constrói array de dias enriquecido
    // fichas/investimento/valorErp = lançamento manual (fonte: usuário)
    // erpCount/coletasSolic/orcEnviado = SEMPRE metaLog Pipefy (fonte confiável)
    const diasEnriq = db.dias.map(d => ({
      ...d,
      erpCount:     erpPorDia[d.data]    || 0,
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
          valorErp:     0,
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

  // ── DELETE deletar-dia — remove um dia ───────────────────────────────────
  if (req.method === "POST" && action === "deletar-dia") {
    const { data } = req.body || {};
    if (!data) return res.status(400).json({ ok: false, error: "data obrigatória" });
    const db = await dbGet(METRICAS_KEY) || { dias: [] };
    db.dias = (db.dias || []).filter(d => d.data !== data);
    await dbSet(METRICAS_KEY, db);
    return res.status(200).json({ ok: true });
  }

    return res.status(404).json({ ok: false, error: "Ação não encontrada" });
  } catch(e) {
    console.error("metricas handler error:", e.message, e.stack);
    return res.status(200).json({ ok: false, error: "Erro interno: " + e.message });
  }
};
