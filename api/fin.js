const PIPEFY_API    = "https://api.pipefy.com/graphql";
const PIPE_ID       = "305832912";
const BOARD_KEY     = "reparoeletro_board";
const FIN_KEY       = "reparoeletro_financeiro";
const FIN_BACKUP_KEY = "reparoeletro_financeiro_backup";
const FIN_CONCIL_KEY  = "fin_conciliacao"; // Banco de conciliação bancária MP
const MP_TOKEN        = (process.env.MP_ACCESS_TOKEN || "").replace(/['"]/g,"").trim();
const SOLICITAR_ENTREGA_PHASE_FIN = "334875186";

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


// ── Banco de conciliação — salvar transação ──────────────────────────────────
function dataBRT(ts) {
  const d = new Date(new Date(ts).toLocaleString("en-US",{timeZone:"America/Sao_Paulo"}));
  return d.getFullYear()+"-"+String(d.getMonth()+1).padStart(2,"0")+"-"+String(d.getDate()).padStart(2,"0");
}
async function salvarConciliacao(entry) {
  try {
    const db = (await dbGet(FIN_CONCIL_KEY)) || { transacoes: [] };
    db.transacoes = db.transacoes || [];
    // Idempotência — não duplicar mesmo paymentId
    if (entry.paymentId && db.transacoes.find(t => t.paymentId === String(entry.paymentId))) return;
    db.transacoes.unshift({ ...entry, ts: new Date().toISOString() });
    // Manter 90 dias
    const cutoff = new Date(Date.now() - 90*24*60*60*1000).toISOString();
    db.transacoes = db.transacoes.filter(t => (t.ts||"") > cutoff).slice(0, 2000);
    await dbSet(FIN_CONCIL_KEY, db);
  } catch(e) { console.error("[Concil]", e.message); }
}

// ── Criar preferência Mercado Pago ───────────────────────────────────────────
async function criarPreferenciaMp({ rec, metodo }) {
  if (!MP_TOKEN) throw new Error("MP_ACCESS_TOKEN não configurado");
  const valor = parseFloat(rec.valor || rec.total || 0);
  if (!valor || valor <= 0) throw new Error("Valor inválido: " + valor);

  const isPix = metodo === "pix";
  const body = {
    items: [{
      id:          rec.id,
      title:       "OS " + rec.id + " | " + (rec.nomeContato || rec.title || "Cliente"),
      description: "Reparo Eletro BH — Conserto de Eletrodomesticos | " +
                   (rec.equipamento || rec.descricao || "Eletrodomestico"),
      quantity:    1,
      unit_price:  valor,
      currency_id: "BRL",
      picture_url: process.env.REPARO_LOGO_URL ||
        "https://reparoeletroadm.com/logo.png" // substitua pela URL pública do logo
    }],
    payer: {
      name:  (rec.nomeContato || rec.title || "Cliente").split(" ").slice(0,2).join(" "),
      surname: (rec.nomeContato || rec.title || "").split(" ").slice(2).join(" ") || "",
      email: rec.email || "cliente@reparoeletrobh.com.br",
      phone: rec.telefone
        ? { area_code: rec.telefone.replace(/\D/g,"").slice(0,2),
            number:    rec.telefone.replace(/\D/g,"").slice(2) }
        : undefined,
      ...(rec.cpfCnpj ? { identification: {
        type:   rec.cpfCnpj.replace(/\D/g,"").length <= 11 ? "CPF" : "CNPJ",
        number: rec.cpfCnpj.replace(/\D/g,"")
      }} : {})
    },
    payment_methods: isPix
      ? {
          default_payment_method_id: "pix",
          excluded_payment_types: [{ id: "credit_card" }, { id: "debit_card" }, { id: "ticket" }],
          installments: 1
        }
      : {
          excluded_payment_types: [],
          installments:      3,
          default_installments: 1
        },
    metadata: {
      origem:    "financeiro",
      fichaId:   rec.id,
      ficha_id:  rec.id,  // snake_case para compatibilidade MP
      pipefyId:  rec.pipefyId || "",
      metodo:    metodo,
      cliente:   rec.nomeContato || rec.title || "",
      valor:     String(valor)
    },
    back_urls: {
      success: "https://reparoeletroadm.com/financeiro",
      failure: "https://reparoeletroadm.com/financeiro",
      pending: "https://reparoeletroadm.com/financeiro"
    },
    notification_url: "https://reparoeletroadm.com/api/webhook-mp",
    statement_descriptor: "REPARO ELETRO BH", // aparece na fatura do cartão do cliente
    external_reference: rec.id, // indexável no MP para busca
    // SEM expiration_date_to — link sem expiração
    binary_mode: false
  };

  const res = await fetch("https://api.mercadopago.com/checkout/preferences", {
    method:  "POST",
    headers: { "Content-Type": "application/json", Authorization: "Bearer " + MP_TOKEN },
    body:    JSON.stringify(body)
  });
  const data = await res.json();
  if (!data.id) throw new Error("MP não retornou preference id: " + JSON.stringify(data).slice(0,200));
  return data; // { id, init_point, sandbox_init_point }
}

// ── Handler ────────────────────────────────────────────────────
module.exports = async function handler(req, res) {
  // 🔐 TF-AUTH (Fase 1): chave obrigatória em toda chamada
  const _tfk = (req.query && req.query.k) || req.headers['x-tf-key'] || '';
  if (_tfk !== ((process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim())) {
    return res.status(401).json({ ok: false, error: 'não autorizado' });
  }

  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(200).end();

  const { action } = req.query;

  // ── GET ping — health check rápido ─────────────────────────
  if (action === "ping") {
    return res.status(200).json({ ok: true, ts: new Date().toISOString() });
  }

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

    // 2. Remove fichas cujo card foi para ERP ou Finalizado no Pipefy
    try {
      const finIds = await fetchFinalizadoIds();
      if (finIds.length > 0) {
        const before = fin.records.length;
        // Fases que indicam dado histórico valioso — preservar mesmo após ERP/Finalizado
        const FASES_PRESERVAR = ['pagamento_confirmado','nf_emitida','faturamento','rota_criada'];
        const cutoff90 = new Date(); cutoff90.setDate(cutoff90.getDate() - 90);
        fin.records = fin.records.filter(r => {
          if (!r.pipefyId) return true;               // sem pipefyId: mantém
          if (r.pipefyId.startsWith("venda-")) return true; // venda interna: mantém
          if (!finIds.includes(r.pipefyId)) return true; // não finalizado: mantém
          // Finalizado — só remove se NÃO passou por pagamento_confirmado ou é antigo (>90 dias)
          const passouPorPagamento = (r.history||[]).some(h => FASES_PRESERVAR.includes(h.phaseId));
          if (!passouPorPagamento) return false; // sem histórico relevante: remove
          // Com histórico: mantém por 90 dias para não perder contagens
          const movedAt = new Date(r.movedAt || r.createdAt || 0);
          return movedAt >= cutoff90;
        });
        removedCount = before - fin.records.length;
      }
    } catch (e) { console.error("finalizado check:", e.message); }

    if (newCount > 0 || removedCount > 0) await dbSet(FIN_KEY, fin);
    try { await dbSet(FIN_BACKUP_KEY, { ...fin, backedUpAt: new Date().toISOString() }); } catch(e) {}
    return res.status(200).json({ ok: true, newCount, removedCount, pipefyError });
  }



  // ── POST gerar-cobranca-mp ─────────────────────────────────────────────────
  if (req.method === "POST" && action === "gerar-cobranca-mp") {
    const { id, metodo } = req.body || {};
    if (!id || !["pix","cartao"].includes(metodo))
      return res.status(400).json({ ok:false, error: "id e metodo (pix|cartao) obrigatórios" });

    const fin = await dbGet(FIN_KEY) || defaultFin();
    const rec = fin.records.find(r => r.id === id);
    if (!rec) return res.status(404).json({ ok:false, error: "Ficha não encontrada" });
    if (rec.phaseId !== "nf_emitida")
      return res.status(400).json({ ok:false, error: "Ficha não está em NF Emitida: " + rec.phaseId });

    try {
      const pref = await criarPreferenciaMp({ rec, metodo });

      // Salvar dados MP na ficha
      rec.mp = {
        preferenceId:  pref.id,
        checkoutUrl:   pref.init_point,
        metodo,
        geradoEm:      new Date().toISOString(),
        status:        "aguardando_pagamento"
      };
      rec.phaseId  = "faturamento";
      rec.movedAt  = new Date().toISOString();
      rec.history  = [...(rec.history||[]), { phaseId:"faturamento", ts:rec.movedAt, via:"mp_cobranca" }];
      await dbSet(FIN_KEY, fin);
      try { await dbSet(FIN_BACKUP_KEY, { ...fin, backedUpAt:new Date().toISOString() }); } catch(e) {}

      // Registrar na conciliação (link gerado, aguardando pagamento)
      await salvarConciliacao({
        tipo:         "link_gerado",
        fichaId:      rec.id,
        cliente:      rec.nomeContato || rec.title || "",
        cpfCnpj:      rec.cpfCnpj || "",
        valor:        parseFloat(rec.valor||rec.total||0),
        metodo,
        preferenceId: pref.id,
        checkoutUrl:  pref.init_point,
        data:         dataBRT(new Date()),
        status:       "aguardando_pagamento"
      });

      return res.status(200).json({ ok:true, preferenceId:pref.id, checkoutUrl:pref.init_point, metodo });
    } catch(e) {
      return res.status(500).json({ ok:false, error: e.message });
    }
  }


  // ── GET listar-links-mp: todos os links MP com status atualizado ──────────
  if (action === "listar-links-mp") {
    const fin  = await dbGet(FIN_KEY)  || defaultFin();
    const concil = (await dbGet(FIN_CONCIL_KEY)) || { transacoes: [] };
    const pagosIds = new Set(
      concil.transacoes
        .filter(t => t.tipo === "pagamento_confirmado")
        .map(t => t.preferenceId)
        .filter(Boolean)
    );
    const links = (fin.records || [])
      .filter(r => r.mp && r.mp.preferenceId)
      .map(r => {
        const pago      = pagosIds.has(r.mp.preferenceId) || r.mp.status === "pago";
        const concilRec = concil.transacoes.find(t => t.preferenceId === r.mp.preferenceId && t.tipo === "pagamento_confirmado");
        return {
          fichaId:       r.id,
          cliente: r.nomeContato || r.title || "",
          cpfCnpj:       r.cpfCnpj || "",
          valor:         parseFloat(r.valor || r.total || 0),
          metodo:        r.mp.metodo,
          preferenceId:  r.mp.preferenceId,
          checkoutUrl:   r.mp.checkoutUrl,
          geradoEm:      r.mp.geradoEm,
          status:        pago ? "pago" : (["faturamento","pagamento_agendado"].includes(r.phaseId) ? "pendente" : r.phaseId),
          pagoEm:        concilRec?.ts || r.mp.pagoEm || null,
          paymentId:     concilRec?.paymentId || r.mp.paymentId || null,
          faseFicha:     r.phaseId,
          dataAgendada:  r.dataAgendadaDisplay || null
        };
      })
      .sort((a,b) => (b.geradoEm||"").localeCompare(a.geradoEm||""));
    const total    = links.length;
    const pagos    = links.filter(l => l.status === "pago").length;
    const pendentes= links.filter(l => l.status === "pendente").length;
    const valorPago = links.filter(l=>l.status==="pago").reduce((s,l)=>s+l.valor,0);
    return res.status(200).json({ ok:true, total, pagos, pendentes, valorPago:parseFloat(valorPago.toFixed(2)), links });
  }

  // ── GET relatorio-financeiro: conciliação bancária por data ────────────────
  if (action === "relatorio-financeiro") {
    const data = req.query.data || dataBRT(new Date());
    const db   = (await dbGet(FIN_CONCIL_KEY)) || { transacoes:[] };
    const dia  = (db.transacoes||[]).filter(t => t.data === data);
    const pago = dia.filter(t => t.tipo === "pagamento_confirmado");
    const totalDia = pago.reduce((s,t) => s + (t.valor||0), 0);
    return res.status(200).json({
      ok:true, data,
      totalTransacoes: dia.length,
      totalPago:       pago.length,
      valorTotal:      parseFloat(totalDia.toFixed(2)),
      transacoes:      dia
    });
  }

  // ── POST force-phase: recuperação de emergência — define fase sem validação ─
  if (req.method === "POST" && action === "force-phase") {
    const { id, phaseId, cpfCnpj, numeroNF, chaveAcesso } = req.body || {};
    if (!id || !phaseId) return res.status(400).json({ ok: false, error: "id e phaseId obrigatórios" });
    const fin = await dbGet(FIN_KEY) || defaultFin();
    const rec = fin.records.find(r => r.id === id);
    if (!rec) return res.status(404).json({ ok: false, error: "Ficha não encontrada" });
    rec.phaseId = phaseId;
    rec.movedAt = new Date().toISOString();
    rec.history = [...(rec.history || []), { phaseId, ts: rec.movedAt, origem: 'force-phase' }];
    if (cpfCnpj) rec.cpfCnpj   = cpfCnpj;
    if (numeroNF)   rec.numeroNF   = numeroNF;
    if (chaveAcesso) rec.chaveAcesso = chaveAcesso;
    await dbSet(FIN_KEY, fin);
    try { await dbSet(FIN_BACKUP_KEY, { ...fin, backedUpAt: new Date().toISOString() }); } catch(e) {}
    return res.status(200).json({ ok: true, record: rec });
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
    const { id, phaseId, anexo } = req.body || {};
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
    if (phaseId === "analise_pagamento" && anexo) {
      rec.anexo = anexo; // { data: base64, type: 'image/jpeg'|'application/pdf', name: '...' }
    }
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


  // ── GET forcar-pipefy-ficha: dispara Pipefy para ficha já em entrega_liberada ──
  if (action === "forcar-pipefy-ficha") {
    const id = req.query.id;
    if (!id) return res.status(400).json({ ok:false, error:"id obrigatorio" });
    const fin = await dbGet(FIN_KEY) || defaultFin();
    const rec = fin.records.find(r => r.id === id);
    if (!rec) return res.status(404).json({ ok:false, error:"ficha nao encontrada" });
    if (!rec.pipefyId) return res.status(400).json({ ok:false, error:"ficha sem pipefyId" });
    try {
      await pipefyMoveCard(rec.pipefyId, SOLICITAR_ENTREGA_PHASE_ID);
      return res.status(200).json({ ok:true, fichaId:id, pipefyId:rec.pipefyId, phaseId:rec.phaseId, acao:"movido_para_solicitar_entrega" });
    } catch(e) {
      return res.status(500).json({ ok:false, error:e.message, fichaId:id, pipefyId:rec.pipefyId });
    }
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
    const { id, phaseId, anexo } = req.body || {};
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
