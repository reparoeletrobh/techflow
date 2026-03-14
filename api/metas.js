const UPSTASH_URL   = (process.env.UPSTASH_URL   || "").replace(/['"]/g, "").trim();
const UPSTASH_TOKEN = (process.env.UPSTASH_TOKEN || "").replace(/['"]/g, "").trim();
const BOARD_KEY     = "reparoeletro_board";
const FIN_KEY       = "reparoeletro_financeiro";
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

  // Hoje
  const todayBRT = toBRT(new Date()); todayBRT.setHours(0,0,0,0);
  const todayUTC = new Date(todayBRT.getTime() + 3*60*60*1000);

  // Semana (segunda-feira)
  const weekBRT = toBRT(new Date()); const wd = weekBRT.getDay();
  weekBRT.setDate(weekBRT.getDate() + (wd===0?-6:1-wd)); weekBRT.setHours(0,0,0,0);
  const weekUTC = new Date(weekBRT.getTime() + 3*60*60*1000);

  // Mês (dia 1)
  const monthBRT = toBRT(new Date()); monthBRT.setDate(1); monthBRT.setHours(0,0,0,0);
  const monthUTC = new Date(monthBRT.getTime() + 3*60*60*1000);

  // Labels
  const days   = ["Dom","Seg","Ter","Qua","Qui","Sex","Sáb"];
  const months = ["Jan","Fev","Mar","Abr","Mai","Jun","Jul","Ago","Set","Out","Nov","Dez"];
  const fmt    = d => { const b=toBRT(d); return `${String(b.getDate()).padStart(2,"0")}/${String(b.getMonth()+1).padStart(2,"0")}`; };
  const todayLabel = `${days[nowBRT.getDay()]}, ${String(nowBRT.getDate()).padStart(2,"0")} ${months[nowBRT.getMonth()]}`;
  const weekEnd    = new Date(weekUTC.getTime() + 5*24*60*60*1000);
  const weekLabel  = `${fmt(weekUTC)} – ${fmt(weekEnd)}`;
  const monthLabel = `${months[nowBRT.getMonth()]} ${nowBRT.getFullYear()}`;

  // Carrega tudo em paralelo
  const [board, fin, vendas] = await Promise.all([
    dbGet(BOARD_KEY), dbGet(FIN_KEY), dbGet(VENDAS_KEY),
  ]);

  const movesLog = board?.movesLog  || [];
  const metaLog  = board?.metaLog   || [];
  const finRecs  = fin?.records     || [];
  const produtos = vendas?.produtos || [];

  // Contador com deduplicação por pipefyId
  function cntLog(log, phaseId, since, until) {
    const seen = new Set();
    return log.filter(h => {
      if (h.phaseId !== phaseId) return false;
      const ts = new Date(h.timestamp || h.ts);
      if (ts < since) return false;
      if (until && ts >= until) return false;
      const key = h.pipefyId || h.timestamp || h.ts;
      if (seen.has(key)) return false;
      seen.add(key); return true;
    }).length;
  }

  function cntFinPhase(phaseId, since) {
    const seen = new Set();
    let count = 0;
    for (const r of finRecs) {
      for (const h of (r.history || [])) {
        if (h.phaseId !== phaseId) continue;
        if (new Date(h.ts) < since) continue;
        if (seen.has(r.id)) continue;
        seen.add(r.id); count++;
      }
    }
    return count;
  }

  // ── DESTAQUES DO TOPO ────────────────────────────────────────
  const top = {
    erp_hoje:    { count: cntLog(metaLog,"erp_entrada",         todayUTC), goal: 25  },
    erp_semana:  { count: cntLog(metaLog,"erp_entrada",         weekUTC),  goal: 150 },
    orc_hoje:    { count: cntLog(metaLog,"aguardando_aprovacao",todayUTC), goal: 30  },
    orc_semana:  { count: cntLog(metaLog,"aguardando_aprovacao",weekUTC),  goal: 150 },
    eq_vend_sem: { count: produtos.filter(p=>p.soldAt    && new Date(p.soldAt)   >=weekUTC).length,  goal: 25  },
    eq_vend_mes: { count: produtos.filter(p=>p.soldAt    && new Date(p.soldAt)   >=monthUTC).length, goal: 100 },
    eq_cad_sem:  { count: produtos.filter(p=>p.createdAt && new Date(p.createdAt)>=weekUTC).length,  goal: 25  },
    eq_cad_mes:  { count: produtos.filter(p=>p.createdAt && new Date(p.createdAt)>=monthUTC).length, goal: 100 },
  };

  // ── FINANCEIRO ───────────────────────────────────────────────
  const fin_hoje   = {
    faturamento: { count: cntFinPhase("faturamento", todayUTC), goal: 20 },
    rota:        { count: cntFinPhase("rota_criada", todayUTC), goal: 20 },
  };
  const fin_semana = {
    faturamento: { count: cntFinPhase("faturamento", weekUTC), goal: 120 },
    rota:        { count: cntFinPhase("rota_criada", weekUTC), goal: 120 },
  };
  const fichasByPhase = {};
  finRecs.forEach(r => { fichasByPhase[r.phaseId] = (fichasByPhase[r.phaseId]||0)+1; });

  // ── DASH PRINCIPAL ───────────────────────────────────────────
  const board_hoje   = {
    aprovados: { count: cntLog(movesLog,"aprovado_entrada",todayUTC), goal: 35 },
    loja:      { count: cntLog(movesLog,"loja_feito",      todayUTC), goal: 15 },
    delivery:  { count: cntLog(movesLog,"delivery_feito",  todayUTC), goal: 20 },
  };
  const board_semana = {
    aprovados: { count: cntLog(movesLog,"aprovado_entrada",weekUTC), goal: 210 },
    loja:      { count: cntLog(movesLog,"loja_feito",      weekUTC), goal: 90  },
    delivery:  { count: cntLog(movesLog,"delivery_feito",  weekUTC), goal: 120 },
  };
  const cardsByPhase = {};
  (board?.phases||[]).forEach(ph => { cardsByPhase[ph.id] = 0; });
  (board?.cards||[]).forEach(c  => { if (cardsByPhase[c.phaseId]!==undefined) cardsByPhase[c.phaseId]++; });

  return res.status(200).json({
    ok: true,
    todayLabel, weekLabel, monthLabel,
    top,
    fin:   { hoje: fin_hoje,   semana: fin_semana,   fichasByPhase },
    board: { hoje: board_hoje, semana: board_semana, cardsByPhase  },
    eq:    { total: produtos.length, vendidos: produtos.filter(p=>p.vendido).length },
  });
};
