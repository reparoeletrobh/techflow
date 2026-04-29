// api/garantia.js — Sistema de Garantia v2
const GARANTIA_KEY  = "reparoeletro_garantia_v2";
const PIPEFY_API    = "https://api.pipefy.com/graphql";
const PIPE_ID       = "305832912";
// Fases Pipefy (Reparo Eletro)
const PIPEFY_FASE_SOLICITAR_COLETA  = "334875150"; // fase inicial para delivery
const PIPEFY_FASE_SOLICITAR_ENTREGA = "334875186"; // Solicitar Entrega
const PIPEFY_FASE_FINALIZADO        = "334875153"; // Finalizado

const UPSTASH_URL   = (process.env.UPSTASH_URL   || "").replace(/['"]/g,"").trim();
const UPSTASH_TOKEN = (process.env.UPSTASH_TOKEN || "").replace(/['"]/g,"").trim();

async function dbGet(key) {
  try {
    const r = await fetch(UPSTASH_URL + "/pipeline", {
      method: "POST",
      headers: { Authorization: "Bearer " + UPSTASH_TOKEN, "Content-Type": "application/json" },
      body: JSON.stringify([["GET", key]]),
    });
    const j = await r.json();
    return j[0] && j[0].result ? JSON.parse(j[0].result) : null;
  } catch(e) { return null; }
}
async function dbSet(key, val) {
  try {
    await fetch(UPSTASH_URL + "/pipeline", {
      method: "POST",
      headers: { Authorization: "Bearer " + UPSTASH_TOKEN, "Content-Type": "application/json" },
      body: JSON.stringify([["SET", key, JSON.stringify(val)]]),
    });
    return true;
  } catch(e) { return false; }
}

function uid() { return Date.now().toString(36) + Math.random().toString(36).slice(2,7); }
function pipefyToken() { return (process.env.PIPEFY_TOKEN || "").trim(); }

// ── PIPEFY HELPERS ────────────────────────────────────────────
async function pipefyQuery(query) {
  const token = pipefyToken();
  if (!token) return { error: "PIPEFY_TOKEN ausente" };
  try {
    const r = await fetch(PIPEFY_API, {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: "Bearer " + token },
      body: JSON.stringify({ query }),
    });
    const j = await r.json();
    if (j.errors) return { error: j.errors[0].message };
    return { data: j.data };
  } catch(e) { return { error: e.message }; }
}

// Cria card no Pipefy para delivery
async function criarCardPipefy(ficha) {
  const titulo = "RS - " + ficha.defeito;
  // Monta fields_attributes com campos do pipe
  const fieldsEsc = (s) => (s || "").replace(/\\/g,"\\\\").replace(/"/g,'\\"');
  const query = `mutation {
    createCard(input: {
      pipe_id: "${PIPE_ID}"
      phase_id: "${PIPEFY_FASE_SOLICITAR_COLETA}"
      title: "${fieldsEsc(titulo)}"
      fields_attributes: [
        { field_id: "nome_do_contato", field_value: "${fieldsEsc(ficha.nome)}" }
        { field_id: "telefone", field_value: "${fieldsEsc(ficha.telefone)}" }
        { field_id: "empresa", field_value: "${fieldsEsc(ficha.defeito)}" }
        { field_id: "endere_o", field_value: "${fieldsEsc(ficha.endereco || "")}" }
      ]
    }) {
      card { id title }
    }
  }`;
  const r = await pipefyQuery(query);
  if (r.error) return { ok: false, error: r.error };
  const card = r.data?.createCard?.card;
  return { ok: true, pipefyId: card?.id, pipefyTitle: card?.title };
}

// Move card Pipefy para uma fase
async function moverCardPipefy(pipefyId, phaseId) {
  const query = `mutation { moveCardToPhase(input: { card_id: "${pipefyId}", destination_phase_id: "${phaseId}" }) { card { id current_phase { name } } } }`;
  const r = await pipefyQuery(query);
  if (r.error) return { ok: false, error: r.error };
  return { ok: true };
}

// Fases por tipo
const FASES = {
  loja_imediata: [
    { id: "producao",            label: "Produção" },
    { id: "conserto_concluido",  label: "Conserto Concluído" },
    { id: "equip_retirado",      label: "Equipamento Retirado" },
  ],
  loja_acompanhamento: [
    { id: "producao",            label: "Produção" },
    { id: "conserto_concluido",  label: "Conserto Concluído" },
    { id: "teste_realizado",     label: "Teste Realizado" },
    { id: "equip_retirado",      label: "Equipamento Retirado" },
  ],
  delivery: [
    { id: "coleta_solicitada",   label: "Coleta Solicitada" },
    { id: "producao",            label: "Produção" },
    { id: "conserto_concluido",  label: "Conserto Concluído" },
    { id: "teste_realizado",     label: "Teste Realizado" },
    { id: "solicitar_entrega",   label: "Solicitar Entrega" },
    { id: "entrega_realizada",   label: "Entrega Realizada" },
  ],
  rua: [
    { id: "garantia_solicitada", label: "Garantia Solicitada" },
    { id: "equip_recolhido",     label: "Equipamento Recolhido" },
    { id: "conserto_realizado",  label: "Conserto Realizado" },
  ],
};

function primeiraFase(tipo) { return (FASES[tipo] || [])[0]?.id || "producao"; }
function defaultDB()        { return { fichas: [] }; }
function isConcluida(ficha) {
  const ultimas = { loja_imediata: "equip_retirado", loja_acompanhamento: "equip_retirado", delivery: "entrega_realizada", rua: "conserto_realizado" };
  return ficha.faseId === ultimas[ficha.tipo];
}

module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(200).end();

  const { action } = req.query;

  try {

    // ── GET load ──────────────────────────────────────────────
    if (action === "load") {
      const db = await dbGet(GARANTIA_KEY) || defaultDB();
      return res.status(200).json({ ok: true, fichas: db.fichas || [], fases: FASES });
    }

    // ── POST cadastrar ─────────────────────────────────────────
    if (req.method === "POST" && action === "cadastrar") {
      const { nome, telefone, defeito, endereco, tipo } = req.body || {};
      if (!nome || !telefone || !defeito || !tipo)
        return res.status(400).json({ ok: false, error: "nome, telefone, defeito e tipo são obrigatórios" });
      if (!FASES[tipo])
        return res.status(400).json({ ok: false, error: "tipo inválido: " + tipo });

      const db = await dbGet(GARANTIA_KEY) || defaultDB();
            const tecnico = (req.body.tecnico || "").trim();
      ficha = {
        id:         uid(),
        nome:       nome.trim(),
        telefone:   telefone.trim(),
        defeito:    defeito.trim(),
        endereco:   (endereco || "").trim(),
        tipo,
        tecnico:    tecnico || null,
        faseId:     primeiraFase(tipo),
        criadaEm:   new Date().toISOString(),
        movidaEm:   new Date().toISOString(),
        concluida:  false,
        pipefyId:   null,
        pipefyErro: null,
      };

      // Delivery → cria card no Pipefy imediatamente
      if (tipo === "delivery") {
        const pip = await criarCardPipefy(ficha);
        if (pip.ok) {
          ficha.pipefyId    = pip.pipefyId;
          ficha.pipefyTitle = pip.pipefyTitle;
        } else {
          ficha.pipefyErro = pip.error;
        }
      }

      db.fichas.unshift(ficha);
      await dbSet(GARANTIA_KEY, db);
      return res.status(200).json({ ok: true, ficha });
    }

    // ── POST mover ─────────────────────────────────────────────
    if (req.method === "POST" && action === "mover") {
      const { id, faseId } = req.body || {};
      if (!id || !faseId) return res.status(400).json({ ok: false, error: "id e faseId obrigatórios" });

      const db = await dbGet(GARANTIA_KEY) || defaultDB();
      const ficha = db.fichas.find(function(f) { return f.id === id; });
      if (!ficha) return res.status(404).json({ ok: false, error: "Ficha não encontrada" });

      const fases = FASES[ficha.tipo] || [];
      if (!fases.find(function(f) { return f.id === faseId; }))
        return res.status(400).json({ ok: false, error: "Fase inválida para este tipo" });

      ficha.faseId   = faseId;
      ficha.movidaEm = new Date().toISOString();
      // Nunca auto-conclui ao mover — só action=concluir faz isso.
      // Assim conserto_realizado permanece visível no Kanban.
      ficha.concluida = false;

      // Delivery + solicitar_entrega → move card Pipefy para Solicitar Entrega
      let pipefyResult = null;
      if (ficha.tipo === "delivery" && faseId === "solicitar_entrega" && ficha.pipefyId) {
        pipefyResult = await moverCardPipefy(ficha.pipefyId, PIPEFY_FASE_SOLICITAR_ENTREGA);
        if (!pipefyResult.ok) ficha.pipefyErro = pipefyResult.error;
      }

      await dbSet(GARANTIA_KEY, db);
      return res.status(200).json({ ok: true, ficha, pipefy: pipefyResult });
    }

    // ── POST concluir ──────────────────────────────────────────
    if (req.method === "POST" && action === "concluir") {
      const { id } = req.body || {};
      const db = await dbGet(GARANTIA_KEY) || defaultDB();
      const ficha = db.fichas.find(function(f) { return f.id === id; });
      if (!ficha) return res.status(404).json({ ok: false, error: "Ficha não encontrada" });
      const ultimas = { loja_imediata: "equip_retirado", loja_acompanhamento: "equip_retirado", delivery: "entrega_realizada", rua: "conserto_realizado" };
      ficha.faseId      = ultimas[ficha.tipo] || ficha.faseId;
      ficha.concluida   = true;
      ficha.concluidaEm = new Date().toISOString();
      await dbSet(GARANTIA_KEY, db);
      return res.status(200).json({ ok: true, ficha });
    }

    // ── POST reabrir ───────────────────────────────────────────
    if (req.method === "POST" && action === "reabrir") {
      const { id } = req.body || {};
      const db = await dbGet(GARANTIA_KEY) || defaultDB();
      const ficha = db.fichas.find(function(f) { return f.id === id; });
      if (!ficha) return res.status(404).json({ ok: false, error: "Ficha não encontrada" });
      ficha.concluida   = false;
      ficha.concluidaEm = null;
      ficha.faseId      = primeiraFase(ficha.tipo);
      ficha.movidaEm    = new Date().toISOString();
      await dbSet(GARANTIA_KEY, db);
      return res.status(200).json({ ok: true, ficha });
    }

    // ── POST excluir ───────────────────────────────────────────
    if (req.method === "POST" && action === "excluir") {
      const { id } = req.body || {};
      const db = await dbGet(GARANTIA_KEY) || defaultDB();
      db.fichas = db.fichas.filter(function(f) { return f.id !== id; });
      await dbSet(GARANTIA_KEY, db);
      return res.status(200).json({ ok: true });
    }

    // ── GET pipefy-sync ────────────────────────────────────────
    // Verifica fichas delivery que estão no Pipefy como Finalizado
    // e move para entrega_realizada no nosso sistema
    if (action === "pipefy-sync") {
      const db = await dbGet(GARANTIA_KEY) || defaultDB();
      // Busca fichas delivery com pipefyId que ainda não foram concluídas
      const pendentes = db.fichas.filter(function(f) {
        return f.tipo === "delivery" && f.pipefyId && !f.concluida;
      });
      if (!pendentes.length) return res.status(200).json({ ok: true, sincronizados: 0 });

      // Busca o card de cada uma no Pipefy para ver a fase atual
      const ids = pendentes.map(function(f) { return f.pipefyId; });
      const cardQueries = ids.map(function(cid) {
        return '  c' + cid + ': card(id: "' + cid + '") { id current_phase { id name } }';
      }).join("\n");
      const query = "query {\n" + cardQueries + "\n}";
      const r = await pipefyQuery(query);

      let sincronizados = 0;
      if (r.data) {
        pendentes.forEach(function(ficha) {
          const cardData = r.data["c" + ficha.pipefyId];
          if (cardData && cardData.current_phase && cardData.current_phase.id === PIPEFY_FASE_FINALIZADO) {
            ficha.faseId      = "entrega_realizada";
            ficha.concluida   = true;
            ficha.concluidaEm = new Date().toISOString();
            sincronizados++;
          }
        });
        if (sincronizados > 0) await dbSet(GARANTIA_KEY, db);
      }

      return res.status(200).json({ ok: true, sincronizados, pipefyErro: r.error || null });
    }

    if (action === "tecnico-load") {
    const db = await dbGet(GARANTIA_KEY) || defaultDB();
    const all = db.fichas || [];

    // Auto-concluir fichas cujo card Pipefy está em Finalizado
    const ativas = all.filter(f => !f.concluida && f.pipefyId);
    if (ativas.length > 0) {
      try {
        const qp = ativas.map(f => 'c' + f.pipefyId + ': card(id: "' + f.pipefyId + '") { id current_phase { id name } }').join("\n");
        const pdata = await pipefyQuery("query {\n" + qp + "\n}");
        let changed = false;
        for (const ficha of ativas) {
          const card = pdata && pdata["c" + ficha.pipefyId];
          if (!card) continue;
          const pid   = (card.current_phase && card.current_phase.id)   || "";
          const pname = (card.current_phase && card.current_phase.name  || "").toLowerCase();
          if (pid === PIPEFY_FASE_FINALIZADO || pname.includes("finalizado") || pname.includes("erp") || pname.includes("concluido") || pname.includes("conclu")) {
            ficha.concluida = true;
            ficha.concluidaEm = ficha.concluidaEm || new Date().toISOString();
            ficha.concluidaMotivo = "pipefy_finalizado";
            changed = true;
          }
        }
        if (changed) await dbSet(GARANTIA_KEY, db);
      } catch(e) { /* silencioso */ }
    }

    return res.status(200).json({ ok: true,
      garantias:    all.filter(f => (f.tipo === "loja_acompanhamento" || f.tipo === "delivery") && !f.concluida),
      lojaImediata: all.filter(f => f.tipo === "loja_imediata" && !f.concluida)
    });
  }
  if (action === "relatorio-tecnico") {
    const db = await dbGet(GARANTIA_KEY) || defaultDB();
    const all = db.fichas || [];
    const agora = new Date();
    const hist = [];
    for (let m = 0; m < 6; m++) {
      const d   = new Date(agora.getFullYear(), agora.getMonth() - m, 1);
      const ym  = d.getFullYear() + "-" + String(d.getMonth()+1).padStart(2,"0");
      const lbl = d.toLocaleDateString("pt-BR", { month:"short", year:"2-digit" });
      const fichasM = all.filter(f => f.criadaEm && f.criadaEm.slice(0,7) === ym);
      const porTec  = {};
      fichasM.forEach(f => { const t = f.tecnico || "N/D"; porTec[t] = (porTec[t]||0)+1; });
      hist.push({ ym, label: lbl, total: fichasM.length, porTecnico: porTec });
    }
    return res.status(200).json({ ok: true, mesAtual: hist[0]||{label:"",total:0,porTecnico:{}}, historico: hist });
  }
    return res.status(404).json({ ok: false, error: "Ação não encontrada" });

  } catch(e) {
    return res.status(200).json({ ok: false, error: "Erro interno: " + e.message });
  }
};
