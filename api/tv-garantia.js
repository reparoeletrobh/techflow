
// Helper: cria/move card no tv_pipe
async function moverNoTvPipe(phase, dados){
  try {
    const _U=(process.env.UPSTASH_URL||'').replace(/['"]/g,'').trim();
    const _T=(process.env.UPSTASH_TOKEN||'').replace(/['"]/g,'').trim();
    async function _g(k){const r=await fetch(_U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+_T,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});const j=await r.json();const v=j[0]?.result;if(!v)return null;try{let x=JSON.parse(v);if(typeof x==='string')x=JSON.parse(x);return x;}catch(e){return null;}}
    async function _s(k,v){await fetch(_U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+_T,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});}
    const pipe=(await _g('tv_pipe'))||{cards:[],lastSync:null};
    if(!Array.isArray(pipe.cards))pipe.cards=[];
    const now=new Date().toISOString();
    const jaExiste = dados.localId && pipe.cards.find(c=>c.localId===String(dados.localId)||c.id===String(dados.localId));
    if(jaExiste){ jaExiste.phase=phase; jaExiste.movedAt=now; }
    else {
      pipe.cards.unshift({
        id:'PIPE-TV-'+Date.now().toString(36).toUpperCase()+'-'+Math.random().toString(36).slice(2,5).toUpperCase(),
        localId:dados.localId||null, pipefyId:dados.pipefyId||null,
        phase, nomeContato:dados.nome||'', telefone:dados.telefone||'',
        equipamento:dados.equipamento||'', descricao:dados.descricao||'',
        endereco:dados.endereco||'', valor:parseFloat(dados.valor)||0,
        origem:dados.origem||'sistema', criadoEm:now, movedAt:now,
        aguardandoDesde:phase==='aguardando_aprovacao'?now:null,
        history:[], analiseCompra:false
      });
    }
    pipe.lastSync=now;
    await _s('tv_pipe',pipe);
  } catch(e){ console.error('[tv_pipe trigger]',e.message); }
}
// garantia.js — Setor de Garantia
const UPSTASH_URL   = (process.env.UPSTASH_URL   || "").replace(/['"]/g,"").trim();
const UPSTASH_TOKEN = (process.env.UPSTASH_TOKEN || "").replace(/['"]/g,"").trim();
const PIPE_ID       = "306904889";
const GARANTIA_KEY  = "tv_garantia";

const FASES = [
  { id: "garantia_acionada",  name: "Garantia Acionada"  },
  { id: "em_analise",         name: "Em Análise"          },
  { id: "servico_finalizado", name: "Serviço Finalizado"  },
];

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

async function pipefyQuery() {
  // Pipefy desconectado — TV opera 100% local (Redis)
  return null;
}

async function fetchRSCards() {
  const data = await pipefyQuery(
    "query { pipe(id: \"" + PIPE_ID + "\") { phases { name cards(first: 50) { edges { node { id title fields { name value } } } } } } }"
  );
  const phases = data?.pipe?.phases || [];
  const rsPhase = phases.find(p => p.name.toLowerCase().trim() === "rs");
  if (!rsPhase) return [];
  return (rsPhase.cards?.edges || []).map(({ node }) => {
    const fields = node.fields || [];
    const get = kw => fields.find(f => f.name.toLowerCase().includes(kw))?.value || "";
    const m = (node.title || "").match(/^(.*?)\s+(\d{3,6})$/);
    return {
      pipefyId:    String(node.id),
      title:       node.title || "",
      osCode:      m ? m[2] : null,
      nomeContato: get("nome") || (m ? m[1].trim() : node.title),
      telefone:    get("telefone") || get("fone"),
      descricao:   get("descri"),
    };
  });
}

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

  // ── GET load ──────────────────────────────────────────────
  if (action === "load") {
    const db = await dbGet(GARANTIA_KEY) || defaultDB();
    return res.status(200).json({ ok: true, fichas: db.fichas || [], fases: FASES });
  }

  // ── GET sync ───────────────────────────────────────────────
  // REGRA: só adiciona fichas novas da fase RS
  // NUNCA remove fichas que o usuário já moveu de fase internamente
  // Fichas só saem via: mover para servico_finalizado, excluir, ou limpar
  if (action === "sync") {
    const db = await dbGet(GARANTIA_KEY) || defaultDB();
    if (!Array.isArray(db.fichas))    db.fichas    = [];
    if (!Array.isArray(db.syncedIds)) db.syncedIds = [];

    // Se não há fichas, limpa syncedIds para permitir reimportação
    if (db.fichas.length === 0) db.syncedIds = [];

    let added = 0, pipefyError = null;
    try {
      const cards = await fetchRSCards();
      for (const card of cards) {
        if (db.syncedIds.includes(card.pipefyId)) continue;
        db.fichas.unshift({
          id:          card.pipefyId,
          pipefyId:    card.pipefyId,
          title:       card.title,
          osCode:      card.osCode,
          nomeContato: card.nomeContato,
          telefone:    card.telefone,
          descricao:   card.descricao,
          phaseId:     FASES[0].id,
          movedAt:     null,
          createdAt:   new Date().toISOString(),
        });
        db.syncedIds.push(card.pipefyId);
        added++;
      }
      if (added > 0) await dbSet(GARANTIA_KEY, db);
    } catch(e) { pipefyError = e.message; }

    return res.status(200).json({ ok: true, added, pipefyError });
  }

  // ── POST mover ─────────────────────────────────────────────
  if (req.method === "POST" && action === "mover") {
    const { id, phaseId } = req.body || {};
    if (!id || !phaseId) return res.status(400).json({ ok: false, error: "id e phaseId obrigatorios" });
    if (!FASES.find(f => f.id === phaseId)) return res.status(400).json({ ok: false, error: "Fase invalida" });
    const db = await dbGet(GARANTIA_KEY) || defaultDB();
    const f  = db.fichas.find(x => x.id === id);
    if (!f) return res.status(404).json({ ok: false, error: "Ficha nao encontrada" });
    f.phaseId = phaseId;
    f.movedAt = new Date().toISOString();
    await dbSet(GARANTIA_KEY, db);
    return res.status(200).json({ ok: true, ficha: f });
  }

  // ── POST excluir ───────────────────────────────────────────
  if (req.method === "POST" && action === "excluir") {
    const { id } = req.body || {};
    const db = await dbGet(GARANTIA_KEY) || defaultDB();
    // Mantém syncedId ao excluir — impede reimportação automática
    db.fichas = db.fichas.filter(f => f.id !== id);
    await dbSet(GARANTIA_KEY, db);
    return res.status(200).json({ ok: true });
  }

  // ── POST limpar-nao-movidas-hoje ───────────────────────────
  if (req.method === "POST" && action === "limpar-nao-movidas-hoje") {
    const db = await dbGet(GARANTIA_KEY) || defaultDB();
    function toBRT(d) { return new Date(new Date(d).toLocaleString("en-US",{timeZone:"America/Sao_Paulo"})); }
    const nowBRT = toBRT(new Date()); nowBRT.setHours(0,0,0,0);
    const todayUTC = new Date(nowBRT.getTime() + 3*60*60*1000);
    const before = db.fichas.length;
    // Remove fichas nunca movidas (movedAt null) ou movidas antes de hoje
    db.fichas = db.fichas.filter(f => f.movedAt && new Date(f.movedAt) >= todayUTC);
    const removed = before - db.fichas.length;
    // Limpa syncedIds das fichas removidas para permitir reimportação no próximo sync
    const idsRestantes = new Set(db.fichas.map(f => f.pipefyId));
    db.syncedIds = db.syncedIds.filter(id => idsRestantes.has(id));
    if (removed > 0) await dbSet(GARANTIA_KEY, db);
    return res.status(200).json({ ok: true, removed, remaining: db.fichas.length });
  }

  // ── GET debug ──────────────────────────────────────────────
  if (action === "debug") {
    const db = await dbGet(GARANTIA_KEY) || defaultDB();
    return res.status(200).json({ ok: true, fichas: db.fichas.length, syncedIds: db.syncedIds.length, syncedIdsList: db.syncedIds });
  }

  // ── GET sync-debug ─────────────────────────────────────────
  if (action === "sync-debug") {
    try {
      const data = await pipefyQuery("query { pipe(id: \"" + PIPE_ID + "\") { phases { name cards(first: 5) { edges { node { id title } } } } } }");
      const phases = data?.pipe?.phases || [];
      return res.status(200).json({ ok: true, phases: phases.map(p => ({ name: p.name, cards: p.cards?.edges?.length || 0 })), rsFound: phases.filter(p => p.name.toLowerCase().includes("rs")).map(p => p.name) });
    } catch(e) { return res.status(200).json({ ok: false, error: e.message }); }
  }

  // ── POST reset-moveat ──────────────────────────────────────
  if (req.method === "POST" && action === "reset-moveat") {
    const db = await dbGet(GARANTIA_KEY) || defaultDB();
    let count = 0;
    db.fichas.forEach(f => { if (f.phaseId === FASES[0].id) { f.movedAt = null; count++; } });
    await dbSet(GARANTIA_KEY, db);
    return res.status(200).json({ ok: true, reset: count });
  }

  // ── POST clear-synced ──────────────────────────────────────
  if (req.method === "POST" && action === "clear-synced") {
    const db = await dbGet(GARANTIA_KEY) || defaultDB();
    const idsAtivos = new Set(db.fichas.map(f => f.pipefyId));
    const antes = db.syncedIds.length;
    db.syncedIds = db.syncedIds.filter(id => idsAtivos.has(id));
    await dbSet(GARANTIA_KEY, db);
    return res.status(200).json({ ok: true, removidos: antes - db.syncedIds.length });
  }

  if (action === "tecnico-load") {
    const db = await dbGet(GARANTIA_KEY) || defaultDB();
    const all = db.fichas || [];
    // Só mostra fichas na fase inicial (garantia_acionada)
    // Ao mover para em_analise ou servico_finalizado, sai da coluna do kanban tecnico
    const garantias = all.filter(f => f.phaseId === "garantia_acionada");
    // Trigger: tv_pipe → garantia (delivery)
    if (req.body?.tipo === 'delivery' || tipo === 'delivery') {
      await moverNoTvPipe('garantia', {
        localId: novaGarantia?.id||null, pipefyId: null,
        nome: req.body?.nome||'', telefone: req.body?.telefone||'',
        equipamento: req.body?.equipamento||'', origem:'tv_garantia_delivery'
      });
    }
    return res.status(200).json({ ok: true, garantias, lojaImediata: [] });
  }
  return res.status(404).json({ ok: false, error: "Acao nao encontrada" });
};
