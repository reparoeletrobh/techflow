// api/pipe.js — Pipeline ADM
const UPSTASH_URL   = (process.env.UPSTASH_URL   || '').replace(/['"]/g,'').trim();
const UPSTASH_TOKEN = (process.env.UPSTASH_TOKEN  || '').replace(/['"]/g,'').trim();
const PIPEFY_TOKEN  = (process.env.PIPEFY_TOKEN   || '').replace(/['"]/g,'').trim();
const PIPE_KEY      = 'reparoeletro_pipe';
const PIPE_ID       = '305832912';

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

// Mapeamento: keyword do nome da fase no Pipefy → ID local
const FASES_ESPELHO = {
  aguardando_aprovacao: ['aguardando aprovação','aguardando aprovacao','aguardando aprov'],
  aprovados:            ['aprovado'],
  video_enviado:        ['video enviado','vídeo enviado'],
  analise_compra:       ['analise de compra','análise de compra'],
  programar_entrega:    ['programar entrega'],
  solicitar_entrega:    ['solicitar entrega'],
  entrega_solicitada:   ['entrega solicitada'],
  receber:              ['receber'],
  erp:                  ['erp'],
};

async function dbGet(k) {
  try {
    const r = await fetch(`${UPSTASH_URL}/get/${encodeURIComponent(k)}`,
      { headers:{ Authorization:`Bearer ${UPSTASH_TOKEN}` } });
    const j = await r.json();
    return j.result ? JSON.parse(j.result) : null;
  } catch { return null; }
}

async function dbSet(k, v) {
  try {
    await fetch(`${UPSTASH_URL}/set/${encodeURIComponent(k)}`, {
      method:'POST',
      headers:{ Authorization:`Bearer ${UPSTASH_TOKEN}`, 'Content-Type':'application/json' },
      body: JSON.stringify(String(JSON.stringify(v)))
    });
  } catch {}
}

async function pipefyQ(query) {
  const ctrl = new AbortController();
  const tid  = setTimeout(() => ctrl.abort(), 15000);
  try {
    const r = await fetch('https://api.pipefy.com/graphql', {
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

function defaultDB() { return { cards:[], syncedPipefyIds:[], lastSync:null }; }

function detectPhase(phaseName) {
  const l = (phaseName || '').toLowerCase();
  for (const [phId, kws] of Object.entries(FASES_ESPELHO)) {
    if (kws.some(k => l.includes(k))) return phId;
  }
  return null;
}

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin','*');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const action = req.query.action || '';

  // ── GET load ──────────────────────────────────────────────────────────────
  if (action === 'load') {
    const db = (await dbGet(PIPE_KEY)) || defaultDB();
    return res.status(200).json({ ok:true, cards:db.cards||[], phases:PHASES, lastSync:db.lastSync||null });
  }

  // ── POST mover ────────────────────────────────────────────────────────────
  if (req.method === 'POST' && action === 'mover') {
    const { id, phase } = req.body || {};
    if (!id || !phase) return res.status(400).json({ ok:false, error:'id e phase obrigatórios' });
    if (!PHASES.find(p => p.id === phase)) return res.status(400).json({ ok:false, error:'fase inválida' });
    const db   = (await dbGet(PIPE_KEY)) || defaultDB();
    const card = db.cards.find(c => c.id === id);
    if (!card) return res.status(404).json({ ok:false, error:'não encontrado' });
    const now = new Date().toISOString();
    card.history = [...(card.history||[]), { phase:card.phase, ts:now }];
    card.phase   = phase;
    card.movedAt = now;
    if (phase === 'aguardando_aprovacao' && !card.aguardandoDesde) card.aguardandoDesde = now;
    await dbSet(PIPE_KEY, db);
    return res.status(200).json({ ok:true, card });
  }

  // ── POST add-card ─────────────────────────────────────────────────────────
  if (req.method === 'POST' && action === 'add-card') {
    const { nomeContato, telefone, equipamento, descricao, valor, phase, origem, pipefyId } = req.body || {};
    if (!nomeContato) return res.status(400).json({ ok:false, error:'nomeContato obrigatório' });
    const db = (await dbGet(PIPE_KEY)) || defaultDB();
    if (pipefyId && db.cards.find(c => c.pipefyId === String(pipefyId)))
      return res.status(200).json({ ok:true, info:'já existe' });
    const now  = new Date().toISOString();
    const newId = 'PIPE-' + String((db.cards.length + 1)).padStart(4,'0');
    const card  = {
      id: newId, pipefyId: pipefyId ? String(pipefyId) : null,
      phase: phase || 'aguardando_aprovacao',
      nomeContato: nomeContato||'', telefone: telefone||'',
      equipamento: equipamento||'', descricao: descricao||'',
      valor: parseFloat(valor)||0, origem: origem||'manual',
      criadoEm: now, movedAt: now,
      aguardandoDesde: (phase==='aguardando_aprovacao'||!phase) ? now : null,
      history:[], analiseCompra:false,
    };
    db.cards.unshift(card);
    if (pipefyId) db.syncedPipefyIds.push(String(pipefyId));
    await dbSet(PIPE_KEY, db);
    return res.status(200).json({ ok:true, card });
  }

  // ── POST toggle-analise-compra ────────────────────────────────────────────
  if (req.method === 'POST' && action === 'toggle-analise-compra') {
    const { id } = req.body || {};
    const db   = (await dbGet(PIPE_KEY)) || defaultDB();
    const card = db.cards.find(c => c.id === id);
    if (!card) return res.status(404).json({ ok:false, error:'não encontrado' });
    card.analiseCompra = !card.analiseCompra;
    await dbSet(PIPE_KEY, db);
    return res.status(200).json({ ok:true, analiseCompra:card.analiseCompra });
  }

  // ── POST excluir ──────────────────────────────────────────────────────────
  if (req.method === 'POST' && action === 'excluir') {
    const { id } = req.body || {};
    const db = (await dbGet(PIPE_KEY)) || defaultDB();
    db.cards = db.cards.filter(c => c.id !== id);
    await dbSet(PIPE_KEY, db);
    return res.status(200).json({ ok:true });
  }


  // ── GET debug-pipefy: retorna fases reais do Pipefy ─────────────────────
  if (action === 'debug-pipefy') {
    if (!PIPEFY_TOKEN) return res.status(200).json({ ok:false, error:'sem PIPEFY_TOKEN' });
    try {
      const data = await pipefyQ(`query { pipe(id:"${PIPE_ID}") { phases { id name } } }`);
      const fases = (data?.pipe?.phases||[]).map(p => ({
        id: p.id,
        name: p.name,
        lower: p.name.toLowerCase(),
        mapeada: detectPhase(p.name) || '—'
      }));
      return res.status(200).json({ ok:true, total:fases.length, fases });
    } catch(e) {
      return res.status(500).json({ ok:false, error: e.message });
    }
  }


  // ── GET test-fase: testa buscar cards de uma fase específica ──────────────
  if (action === 'test-fase') {
    const phaseId = req.query.phaseId || '334875152'; // default: Aguardando Aprovação
    try {
      const data = await pipefyQ(
        `query { phase(id:"${phaseId}") { name cards(first:10) {
          pageInfo { hasNextPage }
          edges { node { id title fields { name value } } }
        }}}`
      );
      const cards = (data?.phase?.cards?.edges||[]).map(e => ({
        id:    e.node.id,
        title: e.node.title,
        campos: (e.node.fields||[]).filter(f=>f.value).map(f=>f.name+': '+f.value).slice(0,3)
      }));
      return res.status(200).json({
        ok:true,
        phase: data?.phase?.name,
        total: data?.phase?.cards?.pageInfo?.totalCount,
        amostra: cards,
        hasMore: data?.phase?.cards?.pageInfo?.hasNextPage
      });
    } catch(e) {
      return res.status(500).json({ ok:false, error:e.message });
    }
  }

  // ── GET sync-pipefy ───────────────────────────────────────────────────────
  if (action === 'sync-pipefy') {
    if (!PIPEFY_TOKEN) return res.status(400).json({ ok:false, error:'PIPEFY_TOKEN não configurado' });
    const db       = (await dbGet(PIPE_KEY)) || defaultDB();
    const existIds = new Set(db.cards.map(c => c.pipefyId).filter(Boolean));
    let added = 0, skipped = 0;

    // 1. Buscar estrutura do pipe
    const est = await pipefyQ(`query { pipe(id:"${PIPE_ID}") { phases { id name } } }`).catch(() => null);
    if (!est?.pipe?.phases) return res.status(500).json({ ok:false, error:'Falha ao buscar pipe' });

    // 2. Mapear fases por keyword
    const phaseIdMap = {};
    for (const ph of est.pipe.phases) {
      const local = detectPhase(ph.name);
      if (local) phaseIdMap[ph.id] = local;
    }

    if (!Object.keys(phaseIdMap).length)
      return res.status(200).json({ ok:true, added:0, skipped:0, info:'Nenhuma fase mapeada', fases: est.pipe.phases.map(p=>p.name) });

    // 3. Buscar cards de cada fase mapeada
    for (const [pipefyPhaseId, phaseLocal] of Object.entries(phaseIdMap)) {
      let cursor = null;
      let hasMore = true;
      while (hasMore) {
        const cursorArg = cursor ? `, after:"${cursor}"` : '';
        const data = await pipefyQ(
          `query { phase(id:"${pipefyPhaseId}") { cards(first:50${cursorArg}) {
            pageInfo { hasNextPage endCursor }
            edges { node { id title fields { name value } } }
          }}}`
        ).catch(() => null);

        const edges    = data?.phase?.cards?.edges    || [];
        const pageInfo = data?.phase?.cards?.pageInfo || {};

        for (const edge of edges) {
          const node = edge.node;
          const pid  = String(node.id);
          if (existIds.has(pid)) { skipped++; continue; }

          const gf = (kw) => {
            const f = (node.fields||[]).find(f => f.name?.toLowerCase().includes(kw));
            return f?.value || '';
          };

          const now = new Date().toISOString();
          const card = {
            id:              'PIPE-' + String(db.cards.length + 1).padStart(4,'0'),
            pipefyId:        pid,
            phase:           phaseLocal,
            nomeContato:     gf('nome') || node.title || '',
            telefone:        gf('telefone') || gf('fone') || '',
            equipamento:     gf('descri') || gf('equip') || '',
            descricao:       node.title || '',
            valor:           parseFloat((gf('valor')||'').replace(/[^0-9.,]/g,'').replace(',','.')) || 0,
            origem:          'pipefy',
            criadoEm:        now,
            movedAt:         now,
            aguardandoDesde: phaseLocal === 'aguardando_aprovacao' ? now : null,
            history:         [],
            analiseCompra:   false,
          };
          db.cards.push(card);
          db.syncedPipefyIds.push(pid);
          existIds.add(pid);
          added++;
        }

        hasMore = pageInfo.hasNextPage || false;
        cursor  = pageInfo.endCursor   || null;
      }
      // Salvar após cada fase — se timeout ocorrer, resultado parcial é preservado
      db.lastSync = new Date().toISOString();
      await dbSet(PIPE_KEY, db);
    }

    db.lastSync = new Date().toISOString();
    await dbSet(PIPE_KEY, db);
    return res.status(200).json({ ok:true, added, skipped, total:db.cards.length, lastSync:db.lastSync });
  }

  // ── GET timer-check (gerenciado pelo Orçamento) ───────────────────────────
  if (action === 'timer-check') {
    return res.status(200).json({ ok:true, info:'sem_resposta gerenciado pelo módulo Orçamento', moved:0 });
  }

  return res.status(404).json({ ok:false, error:'ação não encontrada' });
}
