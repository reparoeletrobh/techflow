// api/pipe.js — Pipeline ADM (substitui Pipefy gradualmente)
const UPSTASH_URL   = (process.env.UPSTASH_URL   || '').replace(/['"]/g,'').trim();
const UPSTASH_TOKEN = (process.env.UPSTASH_TOKEN  || '').replace(/['"]/g,'').trim();
const PIPEFY_TOKEN  = (process.env.PIPEFY_TOKEN   || '').replace(/['"]/g,'').trim();
const PIPE_KEY      = 'reparoeletro_pipe';
const PIPE_ID       = '305832912';
const PIPEFY_API    = 'https://api.pipefy.com/graphql';

// Fases que existem no pipeline
const PHASES = [
  { id:'aguardando_aprovacao', name:'Aguardando Aprovação', cor:'#f5c800' },
  { id:'aprovados',            name:'Aprovados',            cor:'#22c55e' },
  { id:'video_enviado',        name:'Vídeo Enviado',        cor:'#a855f7' },
  { id:'analise_compra',       name:'Análise de Compra',    cor:'#3b9eff' },
  { id:'equipamento_comprado', name:'Equipamento Comprado', cor:'#3b9eff' },
  { id:'programar_entrega',    name:'Programar Entrega',    cor:'#f5c800' },
  { id:'solicitar_entrega',    name:'Solicitar Entrega',    cor:'#f97316' },
  { id:'entrega_solicitada',   name:'Entrega Solicitada',   cor:'#f97316' },
  { id:'receber',              name:'Receber',              cor:'#22c55e' },
  { id:'erp',                  name:'ERP',                  cor:'#22c55e' },
];

// Fases que serão espelhadas do Pipefy
const FASES_ESPELHO = {
  aguardando_aprovacao: ['aguardando aprovação','aguardando aprovacao','aguardando aprov'],
  programar_entrega:    ['programar entrega'],
  solicitar_entrega:    ['solicitar entrega'],
  entrega_solicitada:   ['entrega solicitada'],
  receber:              ['receber'],
  erp:                  ['erp'],
};

async function dbGet(k) {
  try {
    const r = await fetch(`${UPSTASH_URL}/get/${encodeURIComponent(k)}`, {
      headers:{ Authorization:`Bearer ${UPSTASH_TOKEN}` }
    });
    const j = await r.json();
    return j.result ? JSON.parse(j.result) : null;
  } catch(e) { return null; }
}
async function dbSet(k, v) {
  try {
    await fetch(`${UPSTASH_URL}/set/${encodeURIComponent(k)}`, {
      method:'POST', headers:{ Authorization:`Bearer ${UPSTASH_TOKEN}`, 'Content-Type':'application/json' },
      body: JSON.stringify(String(JSON.stringify(v)))
    });
  } catch(e) {}
}

async function pipefyQ(query) {
  const ctrl = new AbortController();
  const tid  = setTimeout(() => ctrl.abort(), 12000);
  try {
    const r = await fetch(PIPEFY_API, {
      method:'POST',
      headers:{ 'Content-Type':'application/json', Authorization:`Bearer ${PIPEFY_TOKEN}` },
      body: JSON.stringify({ query }),
      signal: ctrl.signal
    });
    const j = await r.json();
    clearTimeout(tid);
    if (j.errors) throw new Error(j.errors[0].message);
    return j.data;
  } catch(e) { clearTimeout(tid); throw e; }
}

function defaultDB() {
  return { cards:[], syncedPipefyIds:[], lastSync:null, _v:1 };
}

function nextId(cards) {
  const nums = cards
    .map(c => c.id)
    .filter(id => id && id.startsWith('PIPE-'))
    .map(id => parseInt(id.replace('PIPE-','')) || 0);
  return 'PIPE-' + String((Math.max(0, ...nums) + 1)).padStart(4,'0');
}

function detectPhase(phaseName) {
  const l = (phaseName||'').toLowerCase();
  for (const [phId, keywords] of Object.entries(FASES_ESPELHO)) {
    if (keywords.some(k => l.includes(k))) return phId;
  }
  return null;
}

// ── Handler ──────────────────────────────────────────────────────────────────
export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin','*');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const action = req.query.action || '';

  // ── GET load ─────────────────────────────────────────────────────────────
  if (action === 'load') {
    const db = (await dbGet(PIPE_KEY)) || defaultDB();
    return res.status(200).json({ ok:true, cards:db.cards||[], phases:PHASES, lastSync:db.lastSync||null });
  }

  // ── GET phases ────────────────────────────────────────────────────────────
  if (action === 'phases') {
    return res.status(200).json({ ok:true, phases:PHASES });
  }

  // ── POST mover ────────────────────────────────────────────────────────────
  if (req.method === 'POST' && action === 'mover') {
    const { id, phase } = req.body || {};
    if (!id || !phase) return res.status(400).json({ ok:false, error:'id e phase obrigatórios' });
    if (!PHASES.find(p => p.id === phase)) return res.status(400).json({ ok:false, error:'fase inválida: '+phase });
    const db   = (await dbGet(PIPE_KEY)) || defaultDB();
    const card = db.cards.find(c => c.id === id);
    if (!card) return res.status(404).json({ ok:false, error:'card não encontrado' });
    const now  = new Date().toISOString();
    card.history = [...(card.history||[]), { phase:card.phase, ts:now }];
    card.phase   = phase;
    card.movedAt = now;
    if (phase === 'aguardando_aprovacao' && !card.aguardandoDesde) {
      card.aguardandoDesde = now;
    }
    await dbSet(PIPE_KEY, db);
    return res.status(200).json({ ok:true, card });
  }

  // ── POST add-card ─────────────────────────────────────────────────────────
  if (req.method === 'POST' && action === 'add-card') {
    const { nomeContato, telefone, equipamento, descricao, valor, phase, origem, pipefyId } = req.body || {};
    if (!nomeContato) return res.status(400).json({ ok:false, error:'nomeContato obrigatório' });
    const db  = (await dbGet(PIPE_KEY)) || defaultDB();
    if (pipefyId && db.cards.find(c => c.pipefyId === String(pipefyId))) {
      return res.status(200).json({ ok:true, info:'já existe', pipefyId });
    }
    const now  = new Date().toISOString();
    const card = {
      id:             nextId(db.cards),
      pipefyId:       pipefyId ? String(pipefyId) : null,
      phase:          phase || 'aguardando_aprovacao',
      nomeContato:    nomeContato || '',
      telefone:       telefone || '',
      equipamento:    equipamento || '',
      descricao:      descricao || '',
      valor:          parseFloat(valor)||0,
      origem:         origem || 'manual',
      criadoEm:       now,
      movedAt:        now,
      aguardandoDesde: (phase === 'aguardando_aprovacao') ? now : null,
      history:        [],
      analiseCompra:  false,
    };
    db.cards.unshift(card);
    if (pipefyId) db.syncedPipefyIds.push(String(pipefyId));
    await dbSet(PIPE_KEY, db);
    return res.status(200).json({ ok:true, card });
  }

  // ── POST toggle-analise-compra ─────────────────────────────────────────────
  if (req.method === 'POST' && action === 'toggle-analise-compra') {
    const { id } = req.body || {};
    const db   = (await dbGet(PIPE_KEY)) || defaultDB();
    const card = db.cards.find(c => c.id === id);
    if (!card) return res.status(404).json({ ok:false, error:'não encontrado' });
    card.analiseCompra = !card.analiseCompra;
    card.movedAt       = new Date().toISOString();
    await dbSet(PIPE_KEY, db);
    return res.status(200).json({ ok:true, analiseCompra:card.analiseCompra });
  }

  // timer-check removido — Sem Resposta e Última Chamada já existem em Orçamento
  if (action === 'timer-check') {
    return res.status(200).json({ ok:true, info:'gerenciado pelo modulo Orcamento', moved:0 });
  }

  // ── GET sync-pipefy: espelha fases do Pipefy ──────────────────────────────
  if (action === 'sync-pipefy') {
    if (!PIPEFY_TOKEN) return res.status(400).json({ ok:false, error:'PIPEFY_TOKEN não configurado' });
    const db       = (await dbGet(PIPE_KEY)) || defaultDB();
    const existIds = new Set(db.cards.map(c => c.pipefyId).filter(Boolean));
    let   added    = 0, skipped = 0;

    // 1. Buscar estrutura do pipe para descobrir IDs de cada fase
    const estrutura = await pipefyQ(`query { pipe(id:"${PIPE_ID}") { phases { id name } } }`).catch(()=>null);
    if (!estrutura?.pipe?.phases) {
      return res.status(500).json({ ok:false, error:'Pipefy: falha ao buscar estrutura do pipe' });
    }

    // Mapear fases por keyword
    const phaseIdMap = {};
    for (const ph of estrutura.pipe.phases) {
      const phLocal = detectPhase(ph.name);
      if (phLocal) phaseIdMap[ph.id] = phLocal;
    }

    if (Object.keys(phaseIdMap).length === 0) {
      return res.status(200).json({ ok:true, added:0, skipped:0, info:'Nenhuma fase mapeada — verifique FASES_ESPELHO' });
    }

    // 2. Buscar cards de cada fase mapeada
    for (const [pipefyPhaseId, phaseLocal] of Object.entries(phaseIdMap)) {
      let cursor = null;
      do {
        const cursorArg = cursor ? `, after:"${cursor}"` : '';
        const qCards = `query { phase(id:"${pipefyPhaseId}") { cards(first:50${cursorArg}) {
          pageInfo { hasNextPage endCursor }
          edges { node { id title fields { name value } } }
        }}}`;
        const phData = await pipefyQ(qCards).catch(()=>null);
        const pageInfo = phData?.phase?.cards?.pageInfo || {};
        const edges    = phData?.phase?.cards?.edges   || [];

        for (const edge of edges) {
          const node = edge.node;
          const pid  = String(node.id);
          skipped++;
          if (existIds.has(pid)) continue;
          skipped--;

          const getField = (kw) => {
            const f = (node.fields||[]).find(f => f.name?.toLowerCase().includes(kw));
            return f?.value || '';
          };

          const now  = new Date().toISOString();
          added++;
          const card = {
          id:             'PIPE-' + String(db.cards.length + added).padStart(4,'0'),
          pipefyId:       pid,
          phase:          phaseLocal,
          nomeContato:    getField('nome') || node.title || '',
          telefone:       getField('telefone') || getField('fone') || '',
          equipamento:    getField('descri') || getField('equip') || '',
          descricao:      node.title || '',
          valor:          parseFloat((getField('valor')||'').replace(/[^0-9.,]/g,'').replace(',','.')) || 0,
          origem:         'pipefy',
          criadoEm:       now,
          movedAt:        now,
          aguardandoDesde: phaseLocal === 'aguardando_aprovacao' ? now : null,
          history:        [],
          analiseCompra:  false,
        };
        db.cards.push(card);
        db.syncedPipefyIds.push(pid);
        existIds.add(pid);
        added++;
      }
    }

    db.lastSync = new Date().toISOString();
    // Recalcular IDs para garantir unicidade
    db.cards = db.cards.map((c, i) => ({
      ...c,
      id: c.id || ('PIPE-' + String(i+1).padStart(4,'0'))
    }));
    await dbSet(PIPE_KEY, db);
    return res.status(200).json({ ok:true, added, skipped, total:db.cards.length, lastSync:db.lastSync });
  }

  // ── POST excluir ──────────────────────────────────────────────────────────
  if (req.method === 'POST' && action === 'excluir') {
    const { id } = req.body || {};
    const db = (await dbGet(PIPE_KEY)) || defaultDB();
    db.cards = db.cards.filter(c => c.id !== id);
    await dbSet(PIPE_KEY, db);
    return res.status(200).json({ ok:true });
  }

  return res.status(404).json({ ok:false, error:'ação não encontrada' });
}
