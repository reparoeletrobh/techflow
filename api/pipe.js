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
  { id:'sem_resposta',         name:'Sem Resposta',         cor:'#f97316' },
  { id:'ultima_chamada',       name:'Última Chamada',       cor:'#ef4444' },
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

  // ── GET timer-check: move fichas com mais de 48h/72h (cron) ───────────────
  if (action === 'timer-check') {
    const db     = (await dbGet(PIPE_KEY)) || defaultDB();
    const agora  = Date.now();
    let moved    = 0;
    for (const card of db.cards) {
      if (!card.aguardandoDesde) continue;
      const desde  = new Date(card.aguardandoDesde).getTime();
      const horas  = (agora - desde) / 3600000;
      const now    = new Date().toISOString();
      if (card.phase === 'aguardando_aprovacao' && horas >= 48) {
        card.history = [...(card.history||[]), { phase:'aguardando_aprovacao', ts:now, via:'timer_48h' }];
        card.phase   = 'sem_resposta';
        card.movedAt = now;
        moved++;
      } else if (card.phase === 'sem_resposta' && horas >= 72) {
        card.history = [...(card.history||[]), { phase:'sem_resposta', ts:now, via:'timer_72h' }];
        card.phase   = 'ultima_chamada';
        card.movedAt = now;
        moved++;
      }
    }
    if (moved > 0) await dbSet(PIPE_KEY, db);
    return res.status(200).json({ ok:true, moved, total:db.cards.length });
  }

  // ── GET sync-pipefy: espelha fases do Pipefy ──────────────────────────────
  if (action === 'sync-pipefy') {
    if (!PIPEFY_TOKEN) return res.status(400).json({ ok:false, error:'PIPEFY_TOKEN não configurado' });
    const db       = (await dbGet(PIPE_KEY)) || defaultDB();
    const existIds = new Set(db.cards.map(c => c.pipefyId).filter(Boolean));
    let   added    = 0, skipped = 0;

    try {
      // Buscar estrutura do pipe com cards
      let cursor = null;
      let phases = [];
      // Buscar todas as fases e seus cards
      const data = await pipefyQ(`query {
        pipe(id: "${PIPE_ID}") {
          phases {
            id name
            cards(first: 50) {
              pageInfo { hasNextPage endCursor }
              edges { node {
                id title
                fields { name value }
                phases_history { phase { id name } firstTimeIn lastTimeIn }
              }}
            }
          }
        }
      }`);
      phases = data?.pipe?.phases || [];
    } catch(e) {
      return res.status(500).json({ ok:false, error:'Pipefy: ' + e.message });
    }

    // Reler após a declaração acima
    const data2 = await pipefyQ(`query {
      pipe(id: "${PIPE_ID}") {
        phases {
          id name
          cards(first: 50) {
            edges { node {
              id title
              fields { name value }
            }}
          }
        }
      }
    }`).catch(() => null);

    const phases2 = data2?.pipe?.phases || [];

    for (const ph of phases2) {
      const phaseLocal = detectPhase(ph.name);
      if (!phaseLocal) { skipped += (ph.cards?.edges||[]).length; continue; }

      for (const edge of (ph.cards?.edges||[])) {
        const node = edge.node;
        const pid  = String(node.id);
        if (existIds.has(pid)) { skipped++; continue; }

        const getField = (kw) => {
          const f = (node.fields||[]).find(f => f.name?.toLowerCase().includes(kw));
          return f?.value || '';
        };

        const now  = new Date().toISOString();
        const card = {
          id:             nextId([...db.cards, ...Array(added).fill({id:'PIPE-0000'})]),
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
