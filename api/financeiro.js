const PIPEFY_API    = "https://api.pipefy.com/graphql";
const PIPE_ID       = "305832912";
const BOARD_KEY     = "reparoeletro_board";
const FIN_KEY       = "reparoeletro_financeiro";
const FIN_BACKUP_KEY = "reparoeletro_financeiro_backup";

const UPSTASH_URL   = (process.env.UPSTASH_URL   || "").replace(/['"]/g, "").trim();
const UPSTASH_TOKEN = (process.env.UPSTASH_TOKEN || "").replace(/['"]/g, "").trim();

// Fases do sistema financeiro
const FIN_PHASES = [
  { id: "aguardando_dados",    name: "Aguardando Dados"    },
  { id: "nf_emitida",          name: "NF Emitida"          },
  { id: "faturamento",        name: "Faturamento"         },
  { id: "pagamento_agendado", name: "Pagamento Agendado"  },
  { id: "entrega_agendada",   name: "Entrega Agendada"    },
  { id: "entrega_liberada",   name: "Entrega Liberada"    },
  { id: "rota_criada",        name: "Rota Criada"         },
  { id: "item_coletado",      name: "Item Coletado"       },
];

// Move card no Pipefy para uma fase
async function pipefyMoveCard(cardId, destPhaseId) {
  return await pipefyQuery(`mutation {
    moveCardToPhase(input: { card_id: "${cardId}", destination_phase_id: "${destPhaseId}" }) {
      card { id current_phase { name } }
    }
  }`);
}

// Fase "Solicitar Entrega" no Pipefy
const SOLICITAR_ENTREGA_PHASE_ID = "334875186";

// ── Upstash ────────────────────────────────────────────────────
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

// ── Pipefy ─────────────────────────────────────────────────────
async function pipefyQuery(query, attempt = 1) {
  const TIMEOUT_MS = 15000;
  const MAX_RETRIES = 3;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), TIMEOUT_MS);
  try {
    const res = await fetch(PIPEFY_API, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${(process.env.PIPEFY_TOKEN || "").trim()}`,
      },
      body: JSON.stringify({ query }),
      signal: controller.signal,
    });
    const text = await res.text();
    let json;
    try { json = JSON.parse(text); }
    catch(e) { throw new Error("INVALID_RESPONSE"); }
    if (json.errors) throw new Error(json.errors[0].message);
    return json.data;
  } catch(e) {
    if (attempt < MAX_RETRIES && (e.name === "AbortError" || e.message === "INVALID_RESPONSE")) {
      await new Promise(r => setTimeout(r, 2000 * attempt));
      return pipefyQuery(query, attempt + 1);
    }
    throw e;
  } finally {
    clearTimeout(timer);
  }
}

// Busca cards na fase "Video Enviado"
async function fetchVideoEnviado() {
  const all = [];
  let cursor = null, hasNext = true;
  while (hasNext) {
    const after = cursor ? `, after: "${cursor}"` : "";
    const data = await pipefyQuery(`query {
      pipe(id: "${PIPE_ID}") {
        phases {
          name
          cards(first: 50${after}) {
            pageInfo { hasNextPage endCursor }
            edges {
              node {
                id title age updated_at
                fields { name value }
              }
            }
          }
        }
      }
    }`);
    const phases = data?.pipe?.phases || [];
    const phase  = phases.find(p => {
      const n = p.name.toLowerCase().trim();
      return n.includes("video enviado") || n.includes("vídeo enviado") || n === "video enviado";
    });
    if (!phase) break;
    for (const { node } of phase.cards.edges) {
      const fields = node.fields || [];
      const nomeField   = fields.find(f => f.name.toLowerCase().includes("nome") || f.name.toLowerCase().includes("contato"));
      const descField   = fields.find(f => f.name.toLowerCase().includes("empresa") || (f.name.toLowerCase().includes("descri") && !f.name.toLowerCase().includes("servi")));
      const servicoField= fields.find(f => f.name.toLowerCase().includes("servi"));
      const valorField  = fields.find(f => f.name.toLowerCase().includes("valor") || f.name.toLowerCase().includes("contrato"));
      const telField    = fields.find(f => f.name.toLowerCase().includes("telefone") || f.name.toLowerCase().includes("fone"));
      const endField    = fields.find(f => f.name.toLowerCase().includes("endere"));
      const notasField  = fields.find(f => f.name.toLowerCase().includes("nota") || f.name.toLowerCase().includes("treina"));
      const nomeVal = nomeField?.value || "";
      const digitsMatch = nomeVal.match(/(\d{4})\D*$/);
      all.push({
        pipefyId:    String(node.id),
        title:       node.title || "Sem título",
        nomeContato: nomeVal || null,
        osCode:      digitsMatch ? digitsMatch[1] : null,
        descricao:   descField?.value || null,
        servicos:    servicoField?.value || notasField?.value || null,
        valor:       valorField?.value || null,
        telefone:    telField?.value || null,
        endereco:    endField?.value || null,
        age:         node.age ?? null,
        updatedAt:   node.updated_at || null,
      });
    }
    hasNext = phase.cards.pageInfo?.hasNextPage ?? false;
    cursor  = phase.cards.pageInfo?.endCursor ?? null;
  }
  return all;
}

// Busca IDs que estão em ERP ou Finalizado no Pipefy
async function fetchFinalizadoIds() {
  try {
    const data = await pipefyQuery(`query {
      pipe(id: "${PIPE_ID}") {
        phases {
          name
          cards(first: 50) {
            edges { node { id } }
          }
        }
      }
    }`);
    const phases = data?.pipe?.phases || [];
    const ids = [];
    for (const ph of phases) {
      const n = ph.name.toLowerCase();
      if (n.includes("erp") || n.includes("finaliz") || n.includes("conclu")) {
        ph.cards.edges.forEach(e => ids.push(String(e.node.id)));
      }
    }
    return ids;
  } catch (e) {
    console.error("fetchFinalizadoIds:", e.message);
    return [];
  }
}

function defaultFin() {
  return { records: [], syncedIds: [] };
}

// ── Handler ────────────────────────────────────────────────────
module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(200).end();

  const { action } = req.query;

  // ── GET load ───────────────────────────────────────────────
  if (action === "load") {
    const fin = await dbGet(FIN_KEY) || defaultFin();
    if (!Array.isArray(fin.syncedIds)) fin.syncedIds = [];
    if (!Array.isArray(fin.history))   fin.history   = [];

    const records = fin.records || [];

    // ── Calcular metas ─────────────────────────────────────
    function toBRT(d) { return new Date(new Date(d).toLocaleString("en-US", { timeZone: "America/Sao_Paulo" })); }
    const nowBRT = toBRT(new Date()); nowBRT.setHours(0,0,0,0);
    const todayUTC = new Date(nowBRT.getTime() + 3*60*60*1000);
    // Início da semana (segunda-feira)
    const weekBRT = toBRT(new Date()); const wd = weekBRT.getDay();
    weekBRT.setDate(weekBRT.getDate() + (wd===0?-6:1-wd)); weekBRT.setHours(0,0,0,0);
    const weekUTC = new Date(weekBRT.getTime() + 3*60*60*1000);

    // Log de movimentações (histórico de fases)
    const allHistory = [];
    for (const r of records) {
      for (const h of (r.history || [])) {
        allHistory.push({ ...h, pipefyId: r.id });
      }
    }

    const cntPhase = (phaseId, since) => {
      const seen = new Set();
      return allHistory.filter(h => {
        if (h.phaseId !== phaseId) return false;
        if (new Date(h.ts) < since) return false;
        if (seen.has(h.pipefyId)) return false;
        seen.add(h.pipefyId); return true;
      }).length;
    };

    const goals = {
      today: {
        faturamento: { count: cntPhase("faturamento", todayUTC), goal: 20 },
        rota:        { count: cntPhase("rota_criada",  todayUTC), goal: 20 },
      },
      week: {
        faturamento: { count: cntPhase("faturamento", weekUTC), goal: 120 },
        rota:        { count: cntPhase("rota_criada",  weekUTC), goal: 120 },
      },
    };

    // Contagem por fase atual
    const phaseCounts = {};
    FIN_PHASES.forEach(p => { phaseCounts[p.id] = 0; });
    records.forEach(r => { if (phaseCounts[r.phaseId] !== undefined) phaseCounts[r.phaseId]++; });

    const days = ["Domingo","Segunda","Terça","Quarta","Quinta","Sexta","Sábado"];
    const months = ["Jan","Fev","Mar","Abr","Mai","Jun","Jul","Ago","Set","Out","Nov","Dez"];
    const nb = toBRT(new Date());
    const todayLabel = `${days[nb.getDay()]}, ${String(nb.getDate()).padStart(2,"0")} ${months[nb.getMonth()]}`;
    const wStart = toBRT(weekUTC); const wEnd = new Date(weekUTC.getTime()+5*24*60*60*1000);
    const fmt = d => { const b = toBRT(d); return `${String(b.getDate()).padStart(2,"0")}/${String(b.getMonth()+1).padStart(2,"0")}`; };
    const weekLabel = `${fmt(weekUTC)} – ${fmt(wEnd)}`;

    return res.status(200).json({ ok: true, records, phases: FIN_PHASES, goals, phaseCounts, todayLabel, weekLabel });
  }

  // ── GET sync — varre Pipefy em background ──────────────────
  if (action === "sync") {
    const fin = await dbGet(FIN_KEY) || defaultFin();
    if (!Array.isArray(fin.syncedIds))  fin.syncedIds  = [];
    if (!Array.isArray(fin.records))    fin.records    = [];

    let newCount = 0, removedCount = 0, pipefyError = null;

    // 1. Importa cards de "Video Enviado"
    try {
      const videoCards = await fetchVideoEnviado();
      // Remove do syncedIds cards que saíram da fase (permite reimportar)
      const videoIds = new Set(videoCards.map(c => c.pipefyId));
      fin.syncedIds = fin.syncedIds.filter(id => {
        // Mantém no syncedIds se já tem ficha ativa no painel
        return fin.records.some(r => r.pipefyId === id);
      });

      for (const c of videoCards) {
        if (fin.syncedIds.includes(c.pipefyId)) continue;
        // Também pula se já tem ficha no painel
        if (fin.records.some(r => r.pipefyId === c.pipefyId)) {
          if (!fin.syncedIds.includes(c.pipefyId)) fin.syncedIds.push(c.pipefyId);
          continue;
        }
        fin.records.unshift({
          id:          c.pipefyId,
          pipefyId:    c.pipefyId,
          osCode:      c.osCode,
          nomeContato: c.nomeContato,
          title:       c.title,
          descricao:   c.descricao,
          age:         c.age,
          cpfCnpj:     null,
          valor:       c.valor || null,
          servicos:    c.servicos || null,
          telefone:    c.telefone || null,
          endereco:    c.endereco || null,
          phaseId:     "aguardando_dados",
          createdAt:   new Date().toISOString(),
          movedAt:     new Date().toISOString(),
          history:     [{ phaseId: "aguardando_dados", ts: new Date().toISOString() }],
        });
        fin.syncedIds.push(c.pipefyId);
        newCount++;
      }
    } catch (e) { pipefyError = e.message; }

    // 2. Remove fichas cujo card foi para ERP ou Finalizado no Pipefy (qualquer fase)
    try {
      const finIds = await fetchFinalizadoIds();
      if (finIds.length > 0) {
        const before = fin.records.length;
        fin.records = fin.records.filter(r => {
          if (!r.pipefyId) return true;               // sem pipefyId: mantém
          if (r.pipefyId.startsWith("venda-")) return true; // venda interna: mantém
          return !finIds.includes(r.pipefyId);         // remove se está em ERP/Finalizado
        });
        removedCount = before - fin.records.length;
      }
    } catch (e) { console.error("finalizado check:", e.message); }

    if (newCount > 0 || removedCount > 0) await dbSet(FIN_KEY, fin);
    try { await dbSet(FIN_BACKUP_KEY, { ...fin, backedUpAt: new Date().toISOString() }); } catch(e) {}
    return res.status(200).json({ ok: true, newCount, removedCount, pipefyError });
  }

  // ── POST set-cpf ───────────────────────────────────────────
  // Cadastra CPF/CNPJ e move para Emitir NF
  if (req.method === "POST" && action === "set-cpf") {
    const { id, cpfCnpj } = req.body || {};
    if (!id || !cpfCnpj) return res.status(400).json({ ok: false, error: "id e cpfCnpj obrigatórios" });
    const fin = await dbGet(FIN_KEY) || defaultFin();
    const rec = fin.records.find(r => r.id === id);
    if (!rec) return res.status(404).json({ ok: false, error: "Ficha não encontrada" });
    rec.cpfCnpj = cpfCnpj.trim();
    rec.phaseId = "nf_emitida";
    rec.movedAt = new Date().toISOString();
    rec.history = [...(rec.history || []), { phaseId: "nf_emitida", ts: rec.movedAt }];
    await dbSet(FIN_KEY, fin);
    try { await dbSet(FIN_BACKUP_KEY, { ...fin, backedUpAt: new Date().toISOString() }); } catch(e) {}
    return res.status(200).json({ ok: true, record: rec });
  }

  // ── POST set-valor ────────────────────────────────────────
  if (req.method === "POST" && action === "set-valor") {
    const { id, valor } = req.body || {};
    const fin = await dbGet(FIN_KEY) || defaultFin();
    const rec = fin.records.find(r => r.id === id);
    if (!rec) return res.status(404).json({ ok: false, error: "Ficha não encontrada" });
    rec.valor = valor;
    await dbSet(FIN_KEY, fin);
    try { await dbSet(FIN_BACKUP_KEY, { ...fin, backedUpAt: new Date().toISOString() }); } catch(e) {}
    return res.status(200).json({ ok: true });
  }

  // ── POST emitir-nf ─────────────────────────────────────────
  // Marca NF como emitida e move para Faturamento
  if (req.method === "POST" && action === "emitir-nf") {
    const { id, chaveAcesso, numeroNF } = req.body || {};
    if (!id) return res.status(400).json({ ok: false, error: "id obrigatório" });
    const fin = await dbGet(FIN_KEY) || defaultFin();
    const rec = fin.records.find(r => r.id === id);
    if (!rec) return res.status(404).json({ ok: false, error: "Ficha não encontrada" });
    rec.nfEmitidaAt  = new Date().toISOString();
    rec.chaveAcesso  = chaveAcesso || rec.chaveAcesso || "";
    rec.numeroNF     = numeroNF   || rec.numeroNF    || "";
    rec.phaseId      = "nf_emitida";
    rec.movedAt      = rec.nfEmitidaAt;
    rec.history      = [...(rec.history || []), { phaseId: "nf_emitida", ts: rec.movedAt }];
    await dbSet(FIN_KEY, fin);
    try { await dbSet(FIN_BACKUP_KEY, { ...fin, backedUpAt: new Date().toISOString() }); } catch(e) {}
    return res.status(200).json({ ok: true, record: rec });
  }

  // ── POST nf-enviada — move NF Emitida → Faturamento
  if (req.method === "POST" && action === "nf-enviada") {
    const { id } = req.body || {};
    if (!id) return res.status(400).json({ ok: false, error: "id obrigatório" });
    const fin = await dbGet(FIN_KEY) || defaultFin();
    const rec = fin.records.find(r => r.id === id);
    if (!rec) return res.status(404).json({ ok: false, error: "Ficha não encontrada" });
    rec.movedAt = new Date().toISOString();
    rec.phaseId = "faturamento";
    rec.history = [...(rec.history || []), { phaseId: "faturamento", ts: rec.movedAt }];
    await dbSet(FIN_KEY, fin);
    try { await dbSet(FIN_BACKUP_KEY, { ...fin, backedUpAt: new Date().toISOString() }); } catch(e) {}
    return res.status(200).json({ ok: true, record: rec });
  }

  // ── POST mover ─────────────────────────────────────────────
  // Move entre fases manualmente (faturamento → entrega_agendada/liberada, etc.)
  if (req.method === "POST" && action === "mover") {
    const { id, phaseId } = req.body || {};
    if (!id || !phaseId) return res.status(400).json({ ok: false, error: "id e phaseId obrigatórios" });

    // Valida transições permitidas
    const allowed = {
      nf_emitida:      ["faturamento"],
      faturamento:      ["pagamento_agendado", "analise_pagamento", "entrega_agendada", "entrega_liberada"],
      pagamento_agendado: ["analise_pagamento", "entrega_agendada", "entrega_liberada"],
      analise_pagamento:  ["pagamento_confirmado"],
      pagamento_confirmado: ["entrega_agendada", "entrega_liberada"],
      entrega_agendada: ["entrega_liberada"],
      entrega_liberada: ["rota_criada"],
      rota_criada:      ["item_coletado"],
    };

    const fin = await dbGet(FIN_KEY) || defaultFin();
    const rec = fin.records.find(r => r.id === id);
    if (!rec) return res.status(404).json({ ok: false, error: "Ficha não encontrada" });

    const ok = allowed[rec.phaseId]?.includes(phaseId);
    if (!ok) return res.status(400).json({ ok: false, error: `Transição não permitida: ${rec.phaseId} → ${phaseId}` });

    rec.phaseId = phaseId;
    rec.movedAt = new Date().toISOString();
    rec.history = [...(rec.history || []), { phaseId, ts: rec.movedAt }];
    if (phaseId === "entrega_agendada" || phaseId === "entrega_liberada") rec.paidAt = rec.movedAt;
    if ((phaseId === "entrega_agendada" || phaseId === "pagamento_agendado") && req.body.dataAgendada) {
      rec.dataAgendada        = req.body.dataAgendada;
      rec.dataAgendadaDisplay = req.body.dataAgendadaDisplay || req.body.dataAgendada;
    }
    await dbSet(FIN_KEY, fin);
    try { await dbSet(FIN_BACKUP_KEY, { ...fin, backedUpAt: new Date().toISOString() }); } catch(e) {}
    // Move no Pipefy quando vai para Entrega Liberada
    let pipefyMoveOk = null;
    if (phaseId === "entrega_liberada" && rec.pipefyId) {
      try { await pipefyMoveCard(rec.pipefyId, SOLICITAR_ENTREGA_PHASE_ID); pipefyMoveOk = true; }
      catch(e) { pipefyMoveOk = false; console.error("pipefyMove:", e.message); }
    }

    return res.status(200).json({ ok: true, record: rec, pipefyMoveOk });
  }

  // ── POST excluir ───────────────────────────────────────────
  if (req.method === "POST" && action === "excluir") {
    const { id } = req.body || {};
    const fin = await dbGet(FIN_KEY) || defaultFin();
    fin.records = fin.records.filter(r => r.id !== id);
    await dbSet(FIN_KEY, fin);
    try { await dbSet(FIN_BACKUP_KEY, { ...fin, backedUpAt: new Date().toISOString() }); } catch(e) {}
    return res.status(200).json({ ok: true });
  }

  // ── GET nf-pending — retorna NF pendente para o bookmarklet
  if (action === "nf-pending") {
    const fin = await dbGet(FIN_KEY) || defaultFin();
    const pending = (fin.nfPending || []).slice(-1)[0] || null;
    return res.status(200).json({ ok: true, nf: pending });
  }

  // ── POST nf-salvar — salva dados da NF pendente
  if (req.method === "POST" && action === "nf-salvar") {
    const nfData = req.body || {};
    const fin = await dbGet(FIN_KEY) || defaultFin();
    if (!Array.isArray(fin.nfPending)) fin.nfPending = [];
    fin.nfPending.push({ ...nfData, savedAt: new Date().toISOString() });
    // Mantém só as últimas 5
    fin.nfPending = fin.nfPending.slice(-5);
    await dbSet(FIN_KEY, fin);
    try { await dbSet(FIN_BACKUP_KEY, { ...fin, backedUpAt: new Date().toISOString() }); } catch(e) {}
    return res.status(200).json({ ok: true });
  }

  // ── POST nf-emitida — marca NF como emitida
  if (req.method === "POST" && action === "nf-emitida") {
    const { nfId } = req.body || {};
    const fin = await dbGet(FIN_KEY) || defaultFin();
    if (nfId && Array.isArray(fin.nfPending)) {
      fin.nfPending = fin.nfPending.filter(n => n.nfId !== nfId);
    }
    // Move ficha para Emitir NF se ainda estiver em aguardando_dados
    if (nfId) {
      const rec = (fin.records || []).find(r => r.id === nfId || r.pipefyId === nfId);
      if (rec && rec.phaseId === "aguardando_dados") {
        rec.phaseId = "nf_emitida";
        rec.movedAt = new Date().toISOString();
        rec.history = [...(rec.history || []), { phaseId: "nf_emitida", ts: rec.movedAt }];
      }
    }
    await dbSet(FIN_KEY, fin);
    try { await dbSet(FIN_BACKUP_KEY, { ...fin, backedUpAt: new Date().toISOString() }); } catch(e) {}
    return res.status(200).json({ ok: true });
  }

  // ── POST fin-forcar — remove pipefyId do syncedIds para reimportar
  if (req.method === "POST" && action === "fin-forcar") {
    const { pipefyId } = req.body || {};
    if (!pipefyId) return res.status(400).json({ ok: false, error: "pipefyId obrigatório" });
    const fin = await dbGet(FIN_KEY) || defaultFin();
    fin.syncedIds = (fin.syncedIds || []).filter(id => id !== String(pipefyId));
    await dbSet(FIN_KEY, fin);
    try { await dbSet(FIN_BACKUP_KEY, { ...fin, backedUpAt: new Date().toISOString() }); } catch(e) {}
    return res.status(200).json({ ok: true, msg: "Removido do syncedIds. Próximo sync importa este card." });
  }

  // ── GET debug ─────────────────────────────────────────────
  if (action === "debug") {
    const result = {};
    try {
      const data = await pipefyQuery(`query {
        pipe(id: "${PIPE_ID}") {
          phases { name cards(first: 3) { edges { node { id title } } } }
        }
      }`);
      result.phases = (data?.pipe?.phases || []).map(p => ({
        name: p.name,
        cards: p.cards.edges.length,
        sample: p.cards.edges.map(e => e.node.title).slice(0,2),
      }));
    } catch(e) { result.pipefy_error = e.message; }
    try {
      const fin = await dbGet(FIN_KEY) || defaultFin();
      result.fin_records = fin.records.length;
      result.fin_synced  = fin.syncedIds.length;
    } catch(e) { result.fin_error = e.message; }
    return res.status(200).json(result);
  }

    // ── POST mover-fase — move uma ficha para qualquer fase
  if (req.method === "POST" && action === "mover-fase") {
    const { id, phaseId } = req.body || {};
    if (!id || !phaseId) return res.status(400).json({ ok: false, error: "id e phaseId obrigatórios" });
    const fin = await dbGet(FIN_KEY) || { records: [], syncedIds: [] };
    const rec = (fin.records || []).find(r => r.id === id || r.pipefyId === id);
    if (!rec) return res.status(404).json({ ok: false, error: "Ficha não encontrada" });
    const prev = rec.phaseId;
    rec.phaseId = phaseId;
    rec.movedAt = new Date().toISOString();
    rec.history = [...(rec.history || []), { phaseId, ts: rec.movedAt }];
    await dbSet(FIN_KEY, fin);
    try { await dbSet(FIN_BACKUP_KEY, { ...fin, backedUpAt: new Date().toISOString() }); } catch(e) {}
    return res.status(200).json({ ok: true, record: rec, prev });
  }

  // ── GET restore-backup ───────────────────────────────────────
  if (action === "restore-backup") {
    try {
      const backup = await dbGet(FIN_BACKUP_KEY);
      if (!backup) return res.status(200).json({ ok: false, error: "Nenhum backup encontrado" });
      await dbSet(FIN_KEY, backup);
      return res.status(200).json({ ok: true, backedUpAt: backup.backedUpAt, records: backup.records?.length });
    } catch(e) { return res.status(200).json({ ok: false, error: e.message }); }
  }

  return res.status(404).json({ ok: false, error: "Ação não encontrada" });
};
