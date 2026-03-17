const UPSTASH_URL   = (process.env.UPSTASH_URL   || "").replace(/['"]/g, "").trim();
const UPSTASH_TOKEN = (process.env.UPSTASH_TOKEN || "").replace(/['"]/g, "").trim();
const BOARD_KEY     = "reparoeletro_board";
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

module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(200).end();

  function toBRT(d) {
    return new Date(new Date(d).toLocaleString("en-US", { timeZone: "America/Sao_Paulo" }));
  }
  const nowBRT = toBRT(new Date());

  // Hoje (meia-noite BRT)
  const todayBRT = toBRT(new Date()); todayBRT.setHours(0,0,0,0);
  const todayUTC = new Date(todayBRT.getTime() + 3*60*60*1000);

  // Semana (segunda-feira BRT)
  const weekBRT = toBRT(new Date()); const wd = weekBRT.getDay();
  weekBRT.setDate(weekBRT.getDate() + (wd===0?-6:1-wd)); weekBRT.setHours(0,0,0,0);
  const weekUTC = new Date(weekBRT.getTime() + 3*60*60*1000);

  // Labels
  const days   = ["Domingo","Segunda","Terça","Quarta","Quinta","Sexta","Sábado"];
  const months = ["Jan","Fev","Mar","Abr","Mai","Jun","Jul","Ago","Set","Out","Nov","Dez"];
  const fmt    = d => { const b=toBRT(d); return `${String(b.getDate()).padStart(2,"0")}/${String(b.getMonth()+1).padStart(2,"0")}`; };
  const todayLabel = `${days[nowBRT.getDay()]}, ${String(nowBRT.getDate()).padStart(2,"0")} ${months[nowBRT.getMonth()]}`;
  const weekEnd    = new Date(weekUTC.getTime() + 5*24*60*60*1000);
  const weekLabel  = `${fmt(weekUTC)} – ${fmt(weekEnd)}`;

  // Carrega dados
  const [board, vendasDb] = await Promise.all([
    dbGet(BOARD_KEY), dbGet(VENDAS_KEY),
  ]);

  const movesLog = board?.movesLog || [];
  const metaLog  = board?.metaLog  || [];
  const produtos = vendasDb?.produtos || [];

  // Contador com deduplicação por pipefyId
  function cnt(log, phaseId, since) {
    const seen = new Set();
    return log.filter(h => {
      if (h.phaseId !== phaseId) return false;
      const ts = new Date(h.timestamp || h.ts);
      if (ts < since) return false;
      const key = h.pipefyId || (h.timestamp + h.phaseId);
      if (seen.has(key)) return false;
      seen.add(key); return true;
    }).length;
  }

  // Vendas por canal
  function cntVenda(arr, since, canal) {
    return arr.filter(p => {
      if (!p.soldAt || new Date(p.soldAt) < since) return false;
      if (canal && p.vendedor !== canal) return false;
      return true;
    }).length;
  }

  const today = {
    coletaSolicitada: { count: cnt(metaLog,  "coleta_solicitada",    todayUTC), goal: 40 },
    orcEnviado:       { count: cnt(metaLog,  "aguardando_aprovacao", todayUTC), goal: 40 },
    aprovadoLoja:     { count: cnt(movesLog, "cliente_loja",         todayUTC), goal: 15 },
    aprovadoTotal:    { count: cnt(movesLog, "aprovado_entrada",     todayUTC), goal: 35 },
    erp:              { count: cnt(metaLog,  "erp_entrada",          todayUTC), goal: 35 },
  };

  const week = {
    coletaSolicitada: { count: cnt(metaLog,  "coleta_solicitada",    weekUTC), goal: 200 },
    orcEnviado:       { count: cnt(metaLog,  "aguardando_aprovacao", weekUTC), goal: 200 },
    aprovadoLoja:     { count: cnt(movesLog, "cliente_loja",         weekUTC), goal: 90  },
    aprovadoTotal:    { count: cnt(movesLog, "aprovado_entrada",     weekUTC), goal: 200 },
    erp:              { count: cnt(metaLog,  "erp_entrada",          weekUTC), goal: 200 },
  };

  const vendas = {
    cadastradosHoje:   produtos.filter(p => p.createdAt && new Date(p.createdAt) >= todayUTC).length,
    cadastradosSemana: produtos.filter(p => p.createdAt && new Date(p.createdAt) >= weekUTC).length,
    vendaLojaHoje:     cntVenda(produtos, todayUTC, "Loja"),
    vendaLojaSemana:   cntVenda(produtos, weekUTC,  "Loja"),
    vendaOnlineHoje:   cntVenda(produtos, todayUTC, "Online"),
    vendaOnlineSemana: cntVenda(produtos, weekUTC,  "Online"),
  };

  return res.status(200).json({
    ok: true,
    todayLabel, weekLabel,
    today, week, vendas,
  });
};
