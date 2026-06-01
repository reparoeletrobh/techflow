'use strict';
const PIPEFY_API = 'https://api.pipefy.com/graphql';
const TV_PIPE_PIPEFY_ID = '306904889';
const TV_PIPE_KEY = 'tv_pipe';

// Mapeamento nome Pipefy → id local
const PHASE_NAME_MAP = {
  'aguardando aprovação': 'aguardando_aprovacao',
  'aguardando aprovacao': 'aguardando_aprovacao',
  'aprovados': 'aprovados',
  'aprovado': 'aprovados',
  'vídeo enviado': 'video_enviado',
  'video enviado': 'video_enviado',
  'análise de compra': 'analise_compra',
  'analise de compra': 'analise_compra',
  'equipamento comprado': 'equipamento_comprado',
  'programar entrega': 'programar_entrega',
  'solicitar entrega': 'solicitar_entrega',
  'entrega solicitada': 'entrega_solicitada',
  'receber': 'receber',
  'erp': 'erp',
  'garantia': 'garantia',
  'última chamada': 'ultima_chamada',
  'ultima chamada': 'ultima_chamada',
  'finalizado': 'finalizado',
  'concluído': 'finalizado',
  'concluido': 'finalizado',
};

function norm(s){ return (s||'').toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g,'').trim(); }

function mapPhase(pipefyPhaseName){
  const n = norm(pipefyPhaseName);
  return PHASE_NAME_MAP[n] || n.replace(/\s+/g,'_');
}

async function pipefyQuery(token, query){
  try {
    const r = await fetch(PIPEFY_API, {
      method:'POST',
      headers:{ Authorization:'Bearer '+token, 'Content-Type':'application/json' },
      body: JSON.stringify({query})
    });
    const j = await r.json();
    if (j.errors) { console.error('[tv-pipefy]', JSON.stringify(j.errors)); return null; }
    return j.data;
  } catch(e){ console.error('[tv-pipefy]', e.message); return null; }
}

async function dbGet(url, token, key){
  const r = await fetch(url+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+token,'Content-Type':'application/json'},body:JSON.stringify([['GET',key]])});
  const j = await r.json();
  const v = j[0]?.result;
  if (!v) return null;
  try { let x=JSON.parse(v); if(typeof x==='string') x=JSON.parse(x); return x; } catch(e){ return null; }
}

async function dbSet(url, token, key, val){
  await fetch(url+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+token,'Content-Type':'application/json'},body:JSON.stringify([['SET',key,JSON.stringify(val)]])});
}

module.exports = async function handler(req, res){
  res.setHeader('Cache-Control','no-store');
  const action = req.query.action || '';
  const UP = (process.env.UPSTASH_URL||'').replace(/['"]/g,'').trim();
  const UT = (process.env.UPSTASH_TOKEN||'').replace(/['"]/g,'').trim();
  const PT = (process.env.PIPEFY_TOKEN||'').replace(/['"]/g,'').trim();
  const now = new Date().toISOString();

  // ── FASE 0: sync-from-pipefy-tv ─────────────────────────────────────────
  if (action === 'sync-from-pipefy-tv') {
    if (!PT) return res.status(503).json({ok:false,error:'PIPEFY_TOKEN não configurado'});
    try {
      const resultado = { fases:{}, totalImportados:0, jaExistiam:0, novos:0, erros:[] };

      // 1. Buscar todas as fases do Pipe TV
      const pipeData = await pipefyQuery(PT, `query { pipe(id:"${TV_PIPE_PIPEFY_ID}") { phases { id name cards_count } } }`);
      if (!pipeData?.pipe?.phases) return res.status(500).json({ok:false,error:'Não conseguiu acessar o Pipefy TV'});

      const phases = pipeData.pipe.phases;
      phases.forEach(p => { resultado.fases[mapPhase(p.name)] = { pipefyName:p.name, pipefyId:p.id, total:p.cards_count||0, importados:0 }; });

      // 2. Buscar todos os cards com paginação por fase
      const pipeDb = (await dbGet(UP,UT,TV_PIPE_KEY)) || {cards:[], lastSync:null};
      if (!Array.isArray(pipeDb.cards)) pipeDb.cards = [];

      // Índice de pipefyIds já existentes
      const existentes = new Set(pipeDb.cards.map(c=>String(c.pipefyId||'')).filter(Boolean));

      for (const phase of phases) {
        const localPhase = mapPhase(phase.name);
        let cursor = null, hasNext = true;
        while (hasNext) {
          const after = cursor ? `, after:"${cursor}"` : '';
          const qData = await pipefyQuery(PT, `query {
            phase(id:"${phase.id}") {
              cards(first:50${after}) {
                pageInfo { hasNextPage endCursor }
                edges { node {
                  id title
                  fields { name value }
                  phases_history { phase { id name } firstTimeIn }
                } }
              }
            }
          }`);
          const page = qData?.phase?.cards;
          hasNext = page?.pageInfo?.hasNextPage ?? false;
          cursor  = page?.pageInfo?.endCursor ?? null;

          for (const {node} of (page?.edges||[])) {
            resultado.totalImportados++;
            if (existentes.has(String(node.id))) { resultado.jaExistiam++; continue; }
            const fields = node.fields||[];
            const fNome  = fields.find(f=>/nome|cliente|name/i.test(f.name))?.value || node.title || '';
            const fTel   = fields.find(f=>/telefone|fone|phone|cel/i.test(f.name))?.value || '';
            const fEquip = fields.find(f=>/equip|aparelho|produto/i.test(f.name))?.value || '';
            const fDesc  = fields.find(f=>/defei|descri|probl/i.test(f.name))?.value || '';
            const fVal   = fields.find(f=>/valor|preço|preco|price/i.test(f.name))?.value || '';
            const fEnd   = fields.find(f=>/endere|address/i.test(f.name))?.value || '';
            const hist   = (node.phases_history||[]).find(h=>mapPhase(h.phase?.name||'')==='aguardando_aprovacao');

            pipeDb.cards.push({
              id: 'PIPE-'+Date.now().toString(36).toUpperCase()+'-'+Math.random().toString(36).slice(2,5).toUpperCase(),
              pipefyId: String(node.id),
              phase: localPhase,
              nomeContato: fNome,
              telefone: fTel,
              equipamento: fEquip,
              descricao: fDesc,
              endereco: fEnd,
              valor: parseFloat(fVal)||0,
              origem: 'sync_pipefy_tv',
              criadoEm: now, movedAt: now,
              aguardandoDesde: localPhase==='aguardando_aprovacao' ? (hist?.firstTimeIn||now) : null,
              history:[], analiseCompra:false
            });
            resultado.novos++;
            if (resultado.fases[localPhase]) resultado.fases[localPhase].importados++;
          }
        }
      }

      pipeDb.lastSync = now;
      await dbSet(UP, UT, TV_PIPE_KEY, pipeDb);

      return res.status(200).json({ok:true, ...resultado});
    } catch(e){ return res.status(500).json({ok:false,error:e.message}); }
  }

  // ── status ───────────────────────────────────────────────────────────────
  if (action === 'status') {
    try {
      const db = await dbGet(UP,UT,TV_PIPE_KEY)||{cards:[]};
      const por_fase = {};
      (db.cards||[]).forEach(c=>{ por_fase[c.phase]=(por_fase[c.phase]||0)+1; });
      return res.status(200).json({ok:true, total:(db.cards||[]).length, por_fase, lastSync:db.lastSync});
    } catch(e){ return res.status(500).json({ok:false,error:e.message}); }
  }

  return res.status(400).json({ok:false,error:'action desconhecida: '+action});
};
