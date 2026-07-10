function fmt4dig(nome, tel) {
  if (!nome) return '';
  var n = String(nome).trim();
  if (/\s\d{4}$/.test(n)) return n;
  if (!tel) return n;
  var digits = String(tel).replace(/\D/g,'');
  var last4 = digits.slice(-4);
  if (last4.length < 4) return n;
  return n + ' ' + last4;
}


// ── Helper: gravar no log central ────────────────────────────────────────
async function logAction(entry) {
  try {
    const _U = (process.env.UPSTASH_URL   || '').replace(/['"]/g,'').trim();
    const _T = (process.env.UPSTASH_TOKEN || '').replace(/['"]/g,'').trim();
    const _K = 'reparoeletro_log';
    const _r = await fetch(_U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+_T,'Content-Type':'application/json'},body:JSON.stringify([['GET',_K]])});
    const _j = await _r.json(); const _v = _j[0]?.result;
    let _log = []; if(_v){try{_log=JSON.parse(_v);if(typeof _log==='string')_log=JSON.parse(_log);}catch(e){}} if(!Array.isArray(_log))_log=[];
    _log.unshift({ ts:new Date().toISOString(), modulo:entry.modulo||'—', fichaId:entry.fichaId||'', ficha:entry.ficha||'', acao:entry.acao||'', de:entry.de||'', para:entry.para||'', gatilho:entry.gatilho||'', status:entry.status||'ok', detalhe:entry.detalhe||'' });
    if(_log.length>500) _log.splice(500);
    await fetch(_U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+_T,'Content-Type':'application/json'},body:JSON.stringify([['SET',_K,JSON.stringify(_log)]])});
  } catch(e){ /* log silencioso */ }
}

// api/pipe.js — Pipeline ADM
const UPSTASH_URL   = (process.env.UPSTASH_URL   || '').replace(/['"]/g,'').trim();
const UPSTASH_TOKEN = (process.env.UPSTASH_TOKEN  || '').replace(/['"]/g,'').trim();
const PIPEFY_TOKEN  = (process.env.PIPEFY_TOKEN   || '').replace(/['"]/g,'').trim();
const PIPE_KEY      = 'reparoeletro_pipe';

const PHASES = [
  { id:'aguardando_aprovacao', name:'Aguardando Aprovação', cor:'#f5c800' },
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
  { id:'garantia',             name:'Garantia',             cor:'#06b6d4' },
  { id:'finalizado',            name:'Finalizado',            cor:'#334155' },
  { id:'descarte',              name:'Descarte',              cor:'#7f1d1d' },
];

async function dbFetch(cmd, ...args) {
  try {
    const _u=(process.env.UPSTASH_URL||'').replace(/['"]/g,'').trim();
    const _t=(process.env.UPSTASH_TOKEN||'').replace(/['"]/g,'').trim();
    const r=await fetch(_u+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+_t,'Content-Type':'application/json'},body:JSON.stringify([[cmd,...args]])});
    const j=await r.json();
    return j[0]?.result;
  } catch(e){ console.error('[dbFetch]',e.message); return null; }
}

async function dbGet(k) {
  try {
    const r = await fetch(UPSTASH_URL + '/pipeline', {
      method: 'POST',
      headers: { Authorization: 'Bearer ' + UPSTASH_TOKEN, 'Content-Type': 'application/json' },
      body: JSON.stringify([['GET', k]])
    });
    const j = await r.json();
    const result = j[0]?.result;
    if (!result) return null;
    let val = JSON.parse(result);
    // Tolerância a dupla codificação de versões anteriores
    if (typeof val === 'string') { try { val = JSON.parse(val); } catch(e2) {} }
    return (val && typeof val === 'object') ? val : null;
  } catch(e) { return null; }
}

async function dbSet(k, v) {
  try {
    await fetch(UPSTASH_URL + '/pipeline', {
      method: 'POST',
      headers: { Authorization: 'Bearer ' + UPSTASH_TOKEN, 'Content-Type': 'application/json' },
      body: JSON.stringify([['SET', k, JSON.stringify(v)]])
    });
  } catch(e) { console.error('dbSet error:', e.message); }
}


function defaultDB() {
  return { cards: [], syncedPipefyIds: [], lastSync: null };
}


// Função standalone para sync de uma fase — fora do handler para evitar conflito de escopo



// ── safeWritePipe: mescla antes de salvar — nunca perde cards ───────────────
async function safeWritePipe(newDb) {
  try {
    const current = await dbGet(PIPE_KEY);
    if (current && Array.isArray(current.cards) && Array.isArray(newDb.cards)) {
      const newIds = new Set(newDb.cards.map(c => c.id));
      const perdidos = current.cards.filter(c => !newIds.has(c.id));
      if (perdidos.length > 0) {
        console.warn('[safeWritePipe] ' + perdidos.length + ' cards preservados:', perdidos.map(c=>c.id).join(','));
        try {
          const arch = await dbGet('reparoeletro_pipe_archive') || { cards: [] };
          perdidos.forEach(c => {
            if (!arch.cards.find(a => a.id === c.id)) {
              arch.cards.unshift({ ...c, arquivadoEm: new Date().toISOString(), motivoArchive: 'merge_protection' });
            }
          });
          arch.cards = arch.cards.slice(0, 5000);
          await dbSet('reparoeletro_pipe_archive', arch);
        } catch(e) {}
        newDb.cards = newDb.cards.concat(perdidos);
      }
    }
    await dbSet(PIPE_KEY, newDb);
    return true;
  } catch(e) {
    console.error('[safeWritePipe]', e.message);
    try { await dbSet(PIPE_KEY, newDb); } catch(e2) {}
    return false;
  }
}

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const action = req.query.action || '';


  // ── POST reset-pipe: limpa todos os dados do pipe (dados corrompidos) ──────
  if (action === 'reset-pipe') {
    const fresh = { cards: [], syncedPipefyIds: [], lastSync: null };
    await safeWritePipe( fresh);
    return res.status(200).json({ ok: true, info: 'pipe resetado — pronto para nova sincronização' });
  }



  // ── GET historico-log: retorna histórico de comparações ───────────────────
  if (action === 'historico-log') {
    var logKey = 'reparoeletro_pipe_log';
    var logDb  = (await dbGet(logKey)) || { entradas: [] };
    return res.status(200).json({ ok:true, entradas: logDb.entradas || [] });
  }


  // ── GET pipe-sem-resposta: move cards 48h+ em aguardando_aprovacao → ultima_chamada ──
  if (action === 'pipe-sem-resposta') {
    const db       = (await dbGet(PIPE_KEY)) || defaultDB();
    const agora    = Date.now();
    const MS_48H   = 48 * 60 * 60 * 1000;
    let movidos = 0;
    for (const card of (db.cards || [])) {
      if (card.phase !== 'aguardando_aprovacao') continue;
      const desde = card.aguardandoDesde ? new Date(card.aguardandoDesde).getTime() : 0;
      if (!desde || (agora - desde) < MS_48H) continue;
      const now = new Date().toISOString();
      card.history = (card.history || []).concat([{ phase: 'aguardando_aprovacao', ts: now }]);
      card.phase   = 'ultima_chamada';
      card.movedAt = now;
      movidos++;
    }
    if (movidos > 0) await safeWritePipe( db);
    return res.status(200).json({ ok: true, movidos });
  }


  // ── GET debug-board: mostra cards do board que batem com um nome ──────────
  if (action === 'debug-board') {
    const busca = (req.query.q || '').toLowerCase();
    const pipeDb  = (await dbGet(PIPE_KEY)) || defaultDB();
    const boardDb = await dbGet('reparoeletro_board');
    const pipeMatch  = (pipeDb.cards  || []).filter(c => (c.nomeContato||'').toLowerCase().includes(busca) || (c.id||'').toLowerCase().includes(busca));
    const boardMatch = boardDb ? (boardDb.cards || []).filter(c => (c.nomeContato||c.title||'').toLowerCase().includes(busca) || (c.osCode||'').toLowerCase().includes(busca)) : [];
    return res.status(200).json({
      ok: true, busca,
      pipe:  pipeMatch.map(c => ({ id:c.id, pipefyId:c.pipefyId, nome:c.nomeContato, fase:c.phase })),
      board: boardMatch.map(c => ({ pipefyId:c.pipefyId, phaseId:c.phaseId, nome:c.nomeContato||c.title, osCode:c.osCode, localOnly:c.localOnly })),
      boardTotal: boardDb ? (boardDb.cards||[]).length : 0
    });
  }

  // ── POST force-board: força criação de card no board pelo id do Pipe ───────
  if (action === 'force-board') {  // aceita GET e POST
    const { pipeId } = req.body || req.query || {};
    if (!pipeId) return res.status(400).json({ ok:false, error:'pipeId obrigatorio' });
    const pipeDb  = (await dbGet(PIPE_KEY)) || defaultDB();
    const card    = (pipeDb.cards || []).find(c => c.id === pipeId);
    if (!card) return res.status(404).json({ ok:false, error:'Card nao encontrado no Pipe: '+pipeId });
    const boardDb = (await dbGet('reparoeletro_board')) || { cards:[], syncedIds:[], movesLog:[], metaLog:[], phases:[], rsPhases:[], rsRuaPhases:[], rsCards:[], rsRuaCards:[] };
    if (!Array.isArray(boardDb.cards)) boardDb.cards = [];
    const boardPid = card.pipefyId ? String(card.pipefyId) : ('LOCAL-'+card.id);
    // Remover entrada antiga se existir
    boardDb.cards = boardDb.cards.filter(c => c.pipefyId !== boardPid && c.osCode !== card.id);
    const now = new Date().toISOString();
    const novoCard = {
      pipefyId:    boardPid,
      phaseId:     'producao',
      nomeContato: card.nomeContato || '',
      title:       card.descricao || card.nomeContato || '',
      telefone:    card.telefone || '',
      descricao:   card.equipamento || card.descricao || '',
      osCode:      card.id,
      valor:       card.valor || 0,
      movedBy:     'Pipe ADM',
      flFichaId:   null,
      localOnly:   !card.pipefyId,
      syncedAt:    now,
      movedAt:     now
    };
    boardDb.cards.unshift(novoCard);
    if (!boardDb.syncedIds) boardDb.syncedIds = [];
    if (!boardDb.syncedIds.includes(boardPid)) boardDb.syncedIds.push(boardPid);
    if (!boardDb.movesLog) boardDb.movesLog = [];
    boardDb.movesLog.push({ phaseId:'aprovado_entrada', pipefyId:boardPid, timestamp:now });
    await dbSet('reparoeletro_board', boardDb);
    return res.status(200).json({ ok:true, card:novoCard, boardTotal:boardDb.cards.length });
  }


  // ── GET limpar-fin-pipe: remove registros FIN-PIPE-* espúrios do financeiro ──
  if (action === 'limpar-fin-pipe') {
    try {
      const U2 = (process.env.UPSTASH_URL   || '').replace(/['"]/g,'').trim();
      const T2 = (process.env.UPSTASH_TOKEN || '').replace(/['"]/g,'').trim();
      async function _fg(k){const r=await fetch(U2+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T2,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});const j=await r.json();const v=j[0]?.result;if(!v)return null;let val=JSON.parse(v);if(typeof val==='string'){try{val=JSON.parse(val);}catch(e){}}return(val&&typeof val==='object')?val:null;}
      async function _fs(k,v){await fetch(U2+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T2,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});}
      const fin = await _fg('reparoeletro_financeiro');
      if (!fin || !Array.isArray(fin.records)) return res.status(200).json({ ok:true, removidos:0, info:'financeiro vazio ou sem records' });
      const antes   = fin.records.length;
      const espurios = fin.records.filter(r => (r.id||'').startsWith('FIN-PIPE-') || r.origem === 'pipe_video_enviado');
      fin.records    = fin.records.filter(r => !(r.id||'').startsWith('FIN-PIPE-') && r.origem !== 'pipe_video_enviado');
      const removidos = antes - fin.records.length;
      if (removidos > 0) await _fs('reparoeletro_financeiro', fin);
      return res.status(200).json({ ok:true, removidos, restantes:fin.records.length, espurios: espurios.map(r=>({id:r.id,ficha:r.nomeContato,fase:r.phaseId})) });
    } catch(e) {
      return res.status(500).json({ ok:false, error: e.message });
    }
  }


  // ── GET video-para-financeiro: cria fichas no financeiro para cards em video_enviado após 15h ──
  if (action === 'video-para-financeiro') {
    try {
      // Limite: 15h horário Brasília = 18h UTC
      var hoje = new Date();
      var limite = new Date(Date.UTC(hoje.getUTCFullYear(), hoje.getUTCMonth(), hoje.getUTCDate(), 18, 0, 0)); // 15h BRT = 18h UTC
      
      // Ler Pipe
      var pipeDb = (await dbGet(PIPE_KEY)) || defaultDB();
      var candidatos = (pipeDb.cards || []).filter(function(c) {
        if (c.phase !== 'video_enviado') return false;
        // Verificar se foi movido após as 15h hoje
        var movedAt = c.movedAt ? new Date(c.movedAt) : null;
        if (!movedAt) return false;
        return movedAt >= limite;
      });

      if (!candidatos.length) {
        return res.status(200).json({ ok: true, criados: 0, info: 'Nenhum card em video_enviado após 15h hoje', limite: limite.toISOString() });
      }

      // Ler Financeiro
      var U3 = (process.env.UPSTASH_URL   || '').replace(/['"]/g,'').trim();
      var T3 = (process.env.UPSTASH_TOKEN || '').replace(/['"]/g,'').trim();
      async function _fg(k){var r=await fetch(U3+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T3,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});var j=await r.json();var v=j[0]?.result;if(!v)return null;try{var val=JSON.parse(v);if(typeof val==='string')val=JSON.parse(val);return(val&&typeof val==='object')?val:null;}catch(e){return null;}}
      async function _fs(k,v){await fetch(U3+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T3,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});}

      var finDb = await _fg('reparoeletro_financeiro');
      if (!finDb || !Array.isArray(finDb.records)) finDb = { records: [], syncedIds: [], movesLog: [] };

      var criados = [];
      var ignorados = [];

      for (var i = 0; i < candidatos.length; i++) {
        var card = candidatos[i];
        var pid  = card.pipefyId || ('PIPE-LOCAL-' + card.id);
        // Verificar se já existe no financeiro
        var jaExiste = finDb.records.find(function(r) {
          return (card.pipefyId && r.pipefyId === String(card.pipefyId)) ||
                 (r.osCode && r.osCode === card.id);
        });
        if (jaExiste) { ignorados.push({ id: card.id, ficha: card.nomeContato, motivo: 'já existe (phase: '+jaExiste.phaseId+')' }); continue; }

        var novoRec = {
          id:          'FIN-' + card.id,
          pipefyId:    card.pipefyId ? String(card.pipefyId) : null,
          osCode:      card.id,
          nomeContato: card.nomeContato || '',
          telefone:    card.telefone    || '',
          equipamento: card.equipamento || card.descricao || '',
          valor:       card.valor || 0,
          phaseId:     'aguardando_dados',
          criadoEm:    now,
          movedAt:     now,
          history:     [{ phaseId: 'aguardando_dados', ts: now }],
          origem:      'video_enviado_manual'
        };
        finDb.records.unshift(novoRec);
        criados.push({ id: novoRec.id, ficha: novoRec.nomeContato, pipefyId: novoRec.pipefyId, valor: novoRec.valor });
      }

      if (criados.length > 0) {
        await _fs('reparoeletro_financeiro', finDb);
      }

      return res.status(200).json({
        ok: true,
        criados: criados.length,
        ignorados: ignorados.length,
        fichas: criados,
        jaTinham: ignorados,
        limite: limite.toISOString()
      });
    } catch(e) {
      return res.status(500).json({ ok: false, error: e.message });
    }
  }


  // ── GET restaurar-financeiro: restaura do backup ────────────────────────
  if (action === 'restaurar-financeiro') {
    try {
      var U4=(process.env.UPSTASH_URL||'').replace(/['"]/g,'').trim();
      var T4=(process.env.UPSTASH_TOKEN||'').replace(/['"]/g,'').trim();
      async function _r4(k){var r=await fetch(U4+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T4,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});var j=await r.json();var v=j[0]?.result;if(!v)return null;try{var val=JSON.parse(v);if(typeof val==='string')val=JSON.parse(val);return(val&&typeof val==='object')?val:null;}catch(e){return null;}}
      async function _s4(k,v){await fetch(U4+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T4,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});}

      var backup = await _r4('reparoeletro_financeiro_backup');
      if (!backup || !Array.isArray(backup.records)) {
      
  // ── GET historico-erp — mostra quem saiu do ERP recentemente ──────────────
  if (action === 'historico-erp') {
    const db = await dbGet(PIPE_KEY) || { cards: [] };
    const agora = Date.now();
    const limite48h = new Date(agora - 48*60*60*1000).toISOString();

    // Cards ATUALMENTE em ERP
    const emErp = (db.cards||[]).filter(c => c.phase === 'erp').map(c => ({
      id: c.id, pipefyId: c.pipefyId, nome: c.nomeContato||c.title||'',
      movedAt: c.movedAt, equipamento: c.equipamento||''
    }));

    // Cards que JÁ FORAM para ERP (têm 'erp' no history) mas saíram
    const passaramPeloErp = (db.cards||[]).filter(c => {
      if (c.phase === 'erp') return false;
      return (c.history||[]).some(h => h.phase === 'erp');
    }).map(c => {
      const erpEntry = (c.history||[]).find(h => h.phase === 'erp');
      const saidaEntry = (c.history||[]).slice().reverse().find(h => h.ts > (erpEntry?.ts||''));
      return {
        id: c.id, pipefyId: c.pipefyId, nome: c.nomeContato||c.title||'',
        phaseAtual: c.phase, movedAt: c.movedAt,
        entradaErpEm: erpEntry?.ts, saidaErpEm: saidaEntry?.ts||c.movedAt,
        equipamento: c.equipamento||''
      };
    }).filter(c => c.saidaErpEm > limite48h);

    // Cards movidos recentemente (últimas 48h) de qualquer fase
    const movimentosRecentes = (db.cards||[])
      .filter(c => c.movedAt > limite48h && c.phase !== 'erp')
      .map(c => ({ id: c.id, pipefyId: c.pipefyId, nome: c.nomeContato||'',
                   phase: c.phase, movedAt: c.movedAt }))
      .sort((a,b) => (b.movedAt||'') > (a.movedAt||'') ? 1 : -1)
      .slice(0, 20);

    return res.status(200).json({
      ok: true,
      totalErpAgora: emErp.length,
      emErp,
      sairamDoErpUlt48h: passaramPeloErp,
      movimentosRecentes
    });
  }

  return res.status(404).json({ ok:false, error:'Backup não encontrado ou inválido', backup });
      }
      // Remover campo de controle do backup antes de restaurar
      delete backup.backedUpAt;
      await _s4('reparoeletro_financeiro', backup);
      return res.status(200).json({
        ok: true,
        restaurados: backup.records.length,
        backedUpAt: backup.backedUpAt || 'desconhecido',
        info: 'Financeiro restaurado do backup com sucesso'
      });
    } catch(e) {
      return res.status(500).json({ ok:false, error: e.message });
    }
  }


  // ── GET video-para-financeiro: cria fichas no financeiro para video_enviado após 15h ──
  if (action === 'video-para-financeiro') {
    try {
      var U4=(process.env.UPSTASH_URL||'').replace(/['"]/g,'').trim();
      var T4=(process.env.UPSTASH_TOKEN||'').replace(/['"]/g,'').trim();

      async function _rfin(k){
        var r=await fetch(U4+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T4,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});
        var j=await r.json();var v=j[0]?.result;
        if(!v)return null;
        try{var val=JSON.parse(v);if(typeof val==='string')val=JSON.parse(val);return(val&&typeof val==='object')?val:null;}catch(e){return null;}
      }
      async function _sfin(k,v){
        await fetch(U4+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T4,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});
      }

      // 1. LER financeiro — abortar se falhar
      var finDb = await _rfin('reparoeletro_financeiro');
      if (!finDb) return res.status(500).json({ ok:false, error:'Falha ao ler financeiro — abortando para não corromper dados' });
      if (!Array.isArray(finDb.records)) return res.status(500).json({ ok:false, error:'Financeiro sem array records — estrutura inesperada', finDb });

      // 2. LER Pipe — abortar se falhar
      var pipeDb = await _rfin(PIPE_KEY);
      if (!pipeDb || !Array.isArray(pipeDb.cards)) return res.status(500).json({ ok:false, error:'Falha ao ler Pipe — abortando' });

      // 3. Filtrar cards video_enviado após 15h BRT (18h UTC) de hoje
      var hoje = new Date();
      var limite = new Date(Date.UTC(hoje.getUTCFullYear(), hoje.getUTCMonth(), hoje.getUTCDate(), 18, 0, 0));
      var candidatos = pipeDb.cards.filter(function(card){
        if (card.phase !== 'video_enviado') return false;
        var movedAt = card.movedAt ? new Date(card.movedAt) : null;
        return movedAt && movedAt >= limite;
      });

      if (!candidatos.length) return res.status(200).json({ ok:true, criados:0, info:'Nenhum card em video_enviado após 15h', limite:limite.toISOString() });

      // 4. Inserir apenas os que não existem
      var criados=[], ignorados=[];
      for (var i=0;i<candidatos.length;i++){
        var card=candidatos[i];
        var jaExiste=finDb.records.find(function(r){
          return (card.pipefyId && r.pipefyId===String(card.pipefyId)) || (r.osCode && r.osCode===card.id);
        });
        if(jaExiste){ignorados.push({id:card.id,ficha:card.nomeContato,fase:jaExiste.phaseId});continue;}
        var rec={
          id:'FIN-'+card.id, pipefyId:card.pipefyId?String(card.pipefyId):null,
          osCode:card.id, nomeContato:card.nomeContato||'', telefone:card.telefone||'',
          equipamento:card.equipamento||card.descricao||'', valor:card.valor||0,
          phaseId:'aguardando_dados', criadoEm:now, movedAt:now,
          history:[{phaseId:'aguardando_dados',ts:now}], origem:'video_enviado_manual'
        };
        finDb.records.unshift(rec);
        criados.push({id:rec.id,ficha:rec.nomeContato,pipefyId:rec.pipefyId,valor:rec.valor});
      }

      // 5. Salvar SOMENTE se algo foi criado
      if(criados.length>0){
        // Backup antes de salvar
        try{await _sfin('reparoeletro_financeiro_backup',{...finDb,backedUpAt:now});}catch(e){}
        await _sfin('reparoeletro_financeiro', finDb);
      }

      return res.status(200).json({ok:true,criados:criados.length,ignorados:ignorados.length,fichas:criados,jaTinham:ignorados,limite:limite.toISOString()});
    } catch(e) {
      return res.status(500).json({ok:false,error:e.message});
    }
  }


  // ══ FINANCEIRO via pipe.js (bypass de financeiro.js quebrado) ══════════════

  if (action === 'fin-load') {
    try {
      var fin = await dbGet('reparoeletro_financeiro');
      if (!fin || !Array.isArray(fin.records)) fin = { records:[] };
      var FP = [{id:'aguardando_dados',name:'Aguardando Dados'},{id:'nf_emitida',name:'NF Emitida'},{id:'faturamento',name:'Faturamento'},{id:'entrega_liberada',name:'Entrega Liberada'},{id:'solicitar_entrega',name:'Solicitar Entrega'},{id:'rota_criada',name:'Rota Criada'},{id:'pagamento_confirmado',name:'Pagamento Confirmado'}];
      var pc={}; FP.forEach(function(p){pc[p.id]=0;});
      fin.records.forEach(function(r){if(pc[r.phaseId]!==undefined)pc[r.phaseId]++;});
      return res.status(200).json({ok:true,records:fin.records,phases:FP,phaseCounts:pc,goals:{today:{faturamento:{count:0,goal:20},rota:{count:0,goal:20}},week:{faturamento:{count:0,goal:120},rota:{count:0,goal:120}}},todayLabel:'',weekLabel:''});
    } catch(e){return res.status(500).json({ok:false,error:e.message});}
  }

  if (action === 'fin-mover' && req.method === 'POST') {
    try {
      var body = req.body || {};
      var fin = await dbGet('reparoeletro_financeiro');
      if (!fin || !Array.isArray(fin.records)) return res.status(404).json({ok:false,error:'financeiro vazio'});
      var rec = fin.records.find(function(r){return r.id===body.id;});
      if (!rec) return res.status(404).json({ok:false,error:'ficha nao encontrada'});
      rec.history=(rec.history||[]).concat([{phaseId:rec.phaseId,ts:now}]);
      rec.phaseId=body.phaseId; rec.movedAt=now;
      if(body.valor!==undefined) rec.valor=parseFloat(body.valor)||0;
      await dbSet('reparoeletro_financeiro',fin);
      return res.status(200).json({ok:true,record:rec,pipefyMoveOk:false});
    } catch(e){return res.status(500).json({ok:false,error:e.message});}
  }


  // ── GET fix-fin-records: corrige registros FIN-PIPE sem ts no history ────
  if (action === 'fix-fin-records') {
    try {
      var U6=(process.env.UPSTASH_URL||'').replace(/['"]/g,'').trim();
      var T6=(process.env.UPSTASH_TOKEN||'').replace(/['"]/g,'').trim();
      async function _g6(k){var r=await fetch(U6+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T6,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});var j=await r.json();var v=j[0]?.result;if(!v)return null;try{var x=JSON.parse(v);if(typeof x==='string')x=JSON.parse(x);return x;}catch(e){return null;}}
      async function _s6(k,v){await fetch(U6+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T6,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});}
      var fin=await _g6('reparoeletro_financeiro');
      if(!fin||!Array.isArray(fin.records))return res.status(200).json({ok:false,msg:'financeiro vazio'});
      var fixed=0;
      fin.records.forEach(function(rec){
        // 1. Garantir ts em cada history item
        if(Array.isArray(rec.history)){
          rec.history.forEach(function(h){
            if(!h.ts) h.ts = rec.criadoEm || rec.createdAt || rec.movedAt || new Date().toISOString();
          });
        }
        // 2. Garantir campos mínimos
        if(!rec.nomeContato) rec.nomeContato = rec.title || '';
        if(!rec.valor && rec.preco) rec.valor = rec.preco;
        fixed++;
      });
      await _s6('reparoeletro_financeiro',fin);
      return res.status(200).json({ok:true,processados:fixed,total:fin.records.length});
    } catch(e){return res.status(500).json({ok:false,error:e.message});}
  }


  // ── GET mover-fin: move registro do financeiro para fase correta ──────────
  if (action === 'mover-fin') {
    var id      = req.query.id      || '';
    var fase    = req.query.fase    || 'aguardando_dados';
    var busca   = req.query.busca   || '';
    try {
      var U7=(process.env.UPSTASH_URL||'').replace(/['"]/g,'').trim();
      var T7=(process.env.UPSTASH_TOKEN||'').replace(/['"]/g,'').trim();
      async function _g7(k){var r=await fetch(U7+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T7,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});var j=await r.json();var v=j[0]?.result;if(!v)return null;try{var x=JSON.parse(v);if(typeof x==='string')x=JSON.parse(x);return x;}catch(e){return null;}}
      async function _s7(k,v){await fetch(U7+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T7,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});}
      var fin=await _g7('reparoeletro_financeiro');
      if(!fin||!Array.isArray(fin.records))return res.status(404).json({ok:false,error:'financeiro vazio'});
      // Buscar por id OU por nome
      var rec=null;
      if(id) rec=fin.records.find(function(r){return r.id===id||r.pipefyId===id;});
      if(!rec&&busca) rec=fin.records.find(function(r){return (r.nomeContato||'').toLowerCase().includes(busca.toLowerCase())||(r.title||'').toLowerCase().includes(busca.toLowerCase());});
      if(!rec)return res.status(404).json({ok:false,error:'Registro não encontrado',busca:busca,id:id});
      var faseAnterior=rec.phaseId;
      rec.history=(rec.history||[]).concat([{phaseId:faseAnterior,ts:rec.movedAt||now}]);
      rec.phaseId=fase;
      rec.movedAt=now;
      // Backup antes de salvar
      try{await _s7('reparoeletro_financeiro_backup',Object.assign({},fin,{backedUpAt:now}));}catch(e){}
      await _s7('reparoeletro_financeiro',fin);
      return res.status(200).json({ok:true,id:rec.id,nome:rec.nomeContato||rec.title,de:faseAnterior,para:fase});
    } catch(e){return res.status(500).json({ok:false,error:e.message});}
  }


  // ── GET fin-buscar: lista registros recentes do financeiro ────────────────
  if (action === 'fin-buscar') {
    var q = (req.query.q||'').toLowerCase();
    try {
      var fin = await dbGet('reparoeletro_financeiro');
      if (!fin||!Array.isArray(fin.records)) return res.status(200).json({ok:true,records:[]});
      var lista = q
        ? fin.records.filter(function(r){ return JSON.stringify(r).toLowerCase().includes(q); })
        : fin.records.slice(0,20);
      return res.status(200).json({ok:true, total:lista.length,
        records:lista.map(function(r){return {id:r.id,nome:r.nomeContato||r.title,fase:r.phaseId,valor:r.valor,pipefyId:r.pipefyId};})
      });
    } catch(e){return res.status(500).json({ok:false,error:e.message});}
  }

  // ── POST editar-fin: edita campos de um registro do financeiro ─────────────
  if (req.method==='POST' && action==='editar-fin') {
    var body=req.body||{};
    var {id, nomeContato, valor, telefone, equipamento, cpfCnpj, endereco, servicos, descricao} = body;
    if (!id) return res.status(400).json({ok:false,error:'id obrigatório'});
    try {
      var fin=await dbGet('reparoeletro_financeiro');
      if(!fin||!Array.isArray(fin.records))return res.status(404).json({ok:false,error:'financeiro vazio'});
      var rec=fin.records.find(function(r){return r.id===id||r.pipefyId===id;});
      if(!rec)return res.status(404).json({ok:false,error:'Registro não encontrado: '+id});
      if(nomeContato!==undefined) rec.nomeContato=nomeContato;
      if(valor!==undefined)       rec.valor=parseFloat(String(valor).replace(',','.'))||0;
      if(telefone!==undefined)    rec.telefone=telefone;
      if(equipamento!==undefined) rec.equipamento=equipamento;
      if(descricao!==undefined)   rec.descricao=descricao;
      if(cpfCnpj!==undefined)     rec.cpfCnpj=cpfCnpj;
      if(endereco!==undefined)    rec.endereco=endereco;
      if(servicos!==undefined)    rec.servicos=servicos;
      rec.editadoEm=now;
      try{await dbSet('reparoeletro_financeiro_backup',Object.assign({},fin,{backedUpAt:now}));}catch(e){}
      await dbSet('reparoeletro_financeiro',fin);
      return res.status(200).json({ok:true,record:rec});
    } catch(e){return res.status(500).json({ok:false,error:e.message});}
  }

  // ── POST editar-pipe: edita campos de um card do Pipe ─────────────────────
  if (req.method==='POST' && action==='editar-pipe') {
    var body=req.body||{};
    var {id, nomeContato, valor, telefone, equipamento, descricao, endereco, cpfCnpj, servicos, modelo} = body;
    if (!id) return res.status(400).json({ok:false,error:'id obrigatório'});
    try {
      var db=await dbGet(PIPE_KEY)||defaultDB();
      var card=db.cards.find(function(c){return c.id===id||c.pipefyId===id;});
      if(!card)return res.status(404).json({ok:false,error:'Card não encontrado: '+id});
      if(nomeContato!==undefined) card.nomeContato=nomeContato;
      if(body.vencimento!==undefined) card.vencimento=body.vencimento||null;
      if(valor!==undefined)       card.valor=parseFloat(String(valor).replace(',','.'))||0;
      if(telefone!==undefined)    card.telefone=telefone;
      if(equipamento!==undefined) card.equipamento=equipamento;
      if(descricao!==undefined)   card.descricao=descricao;
      if(endereco!==undefined)    card.endereco=endereco;
      if(cpfCnpj!==undefined)     card.cpfCnpj=cpfCnpj;
      if(servicos!==undefined)    card.servicos=servicos;
      if(modelo!==undefined)      card.modelo=modelo;
      card.editadoEm=now;
      await safeWritePipe(db);
      return res.status(200).json({ok:true,card:card});
    } catch(e){return res.status(500).json({ok:false,error:e.message});}
  }


  // ── GET fin-pendentes: lista fichas com link MP mas sem pagamento confirmado ──
  if (action === 'fin-pendentes') {
    try {
      var fin = await dbGet('reparoeletro_financeiro');
      if (!fin || !Array.isArray(fin.records)) return res.status(200).json({ok:true,pendentes:[]});
      var pendentes = fin.records.filter(function(r){
        return r.mp && r.mp.preferenceId &&
               ['faturamento','pagamento_agendado','nf_emitida','analise_pagamento'].includes(r.phaseId);
      });
      return res.status(200).json({ok:true, total:pendentes.length,
        pendentes: pendentes.map(function(r){return {
          id:r.id, nome:r.nomeContato, fase:r.phaseId, valor:r.valor,
          preferenceId:r.mp.preferenceId, geradoEm:r.mp.geradoEm,
          metodo:r.mp.metodo, pipefyId:r.pipefyId, osCode:r.osCode
        };})
      });
    } catch(e){return res.status(500).json({ok:false,error:e.message});}
  }

  // ── GET fin-confirmar-manual: confirma pagamento manualmente por id da ficha ──
  if (action === 'fin-confirmar-manual') {
    var fichaId = req.query.id || '';
    var valor   = parseFloat(req.query.valor||0);
    var metodo  = req.query.metodo || 'manual';
    if (!fichaId) return res.status(400).json({ok:false,error:'id obrigatorio'});
    try {
      var U8=(process.env.UPSTASH_URL||'').replace(/['"]/g,'').trim();
      var T8=(process.env.UPSTASH_TOKEN||'').replace(/['"]/g,'').trim();
      async function _g8(k){var r=await fetch(U8+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T8,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});var j=await r.json();var v=j[0]?.result;if(!v)return null;try{var x=JSON.parse(v);if(typeof x==='string')x=JSON.parse(x);return x;}catch(e){return null;}}
      async function _s8(k,v){await fetch(U8+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T8,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});}
      var fin=await _g8('reparoeletro_financeiro');
      if(!fin||!Array.isArray(fin.records))return res.status(404).json({ok:false,error:'financeiro vazio'});
      var rec=fin.records.find(function(r){return r.id===fichaId||r.pipefyId===fichaId;});
      if(!rec)return res.status(404).json({ok:false,error:'ficha nao encontrada: '+fichaId});
      if(rec.phaseId==='entrega_liberada')return res.status(200).json({ok:true,info:'ja em entrega_liberada',id:rec.id});
      var ts=now;
      rec.paidAt=ts; rec.movedAt=ts;
      rec.mp=Object.assign(rec.mp||{},{status:'pago',pagoEm:ts,metodo:metodo,valor:valor||rec.valor});
      rec.history=(rec.history||[]).concat([{phaseId:'pagamento_confirmado',ts:ts,via:'manual'},{phaseId:'entrega_liberada',ts:ts,via:'manual'}]);
      rec.phaseId='entrega_liberada';
      // Backup
      try{await _s8('reparoeletro_financeiro_backup',Object.assign({},fin,{backedUpAt:ts}));}catch(e){}
      await _s8('reparoeletro_financeiro',fin);
      // Mover Pipe ADM
      var pipeDb=await _g8('reparoeletro_pipe');
      var pipeOk=false;
      if(pipeDb&&Array.isArray(pipeDb.cards)){
        var pcard=pipeDb.cards.find(function(c){return (rec.pipefyId&&c.pipefyId===String(rec.pipefyId))||(rec.osCode&&c.id===String(rec.osCode));});
        if(pcard){pcard.history=(pcard.history||[]).concat([{phase:pcard.phase,ts:ts}]);pcard.phase='solicitar_entrega';pcard.movedAt=ts;await _s8('reparoeletro_pipe',pipeDb);pipeOk=true;}
      }
      return res.status(200).json({ok:true,id:rec.id,nome:rec.nomeContato,de:rec.phaseId,para:'entrega_liberada',pipeOk:pipeOk});
    } catch(e){return res.status(500).json({ok:false,error:e.message});}
  }


  // ── GET force-venda-pipe: força última(s) venda(s) para Receber no Pipe ──
  if (action === 'force-venda-pipe') {
    var limite = parseInt(req.query.n || '5');
    try {
      var UV3=(process.env.UPSTASH_URL||'').replace(/['"]/g,'').trim();
      var TV3=(process.env.UPSTASH_TOKEN||'').replace(/['"]/g,'').trim();
      async function _vg3(k){var r=await fetch(UV3+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+TV3,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});var j=await r.json();var v=j[0]?.result;if(!v)return null;try{var x=JSON.parse(v);if(typeof x==='string')x=JSON.parse(x);return x;}catch(e){return null;}}
      async function _vs3(k,v){await fetch(UV3+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+TV3,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});}

      // Ler vendas
      var vendasDb=await _vg3('reparoeletro_vendas');
      var produtos=(vendasDb?.produtos||[]).filter(function(p){return p.vendido;});
      // Ordenar por data de venda (mais recente primeiro)
      produtos.sort(function(a,b){return new Date(b.soldAt||b.criadoEm||0)-new Date(a.soldAt||a.criadoEm||0);});
      var ultimas=produtos.slice(0,limite);

      // Ler Pipe
      var pipeDb2=await _vg3('reparoeletro_pipe')||{cards:[],syncedPipefyIds:[],lastSync:null};
      if(!Array.isArray(pipeDb2.cards))pipeDb2.cards=[];

      var adicionados=[], ignorados=[];
      var ts3=new Date().toISOString();

      for(var vi=0;vi<ultimas.length;vi++){
        var p3=ultimas[vi];
        // Verificar se já existe no pipe (pelo código ou descrição)
        var jaExiste=pipeDb2.cards.find(function(c){
          return c.origem==='venda'&&(
            c.descricao===('VENDA — '+(p3.codigo||''))||
            (p3.pipefyCardId&&c.pipefyId===String(p3.pipefyCardId))
          );
        });
        if(jaExiste){ignorados.push({codigo:p3.codigo,nome:p3.compradorNome,fase:jaExiste.phase});continue;}

        pipeDb2.cards.unshift({
          id:'PIPE-'+Date.now().toString(36).toUpperCase() + '-' + Math.random().toString(36).slice(2,5).toUpperCase(),
          pipefyId:p3.pipefyCardId||null,
          phase:'receber',
          nomeContato:p3.compradorNome||'',
          telefone:p3.compradorTel||'',
          equipamento:p3.descricao||'',
          descricao:'VENDA — '+(p3.codigo||''),
          valor:parseFloat(p3.preco)||0,
          origem:'venda',
          criadoEm:p3.soldAt||ts3, movedAt:ts3,
          aguardandoDesde:null, history:[], analiseCompra:false
        });
        adicionados.push({codigo:p3.codigo,nome:p3.compradorNome,valor:p3.preco});
      }

      if(adicionados.length>0)await _vs3('reparoeletro_pipe',pipeDb2);
      return res.status(200).json({ok:true,adicionados:adicionados.length,ignorados:ignorados.length,fichas:adicionados,jaEstavam:ignorados});
    } catch(e){return res.status(500).json({ok:false,error:e.message});}
  }


  // ── GET backup-auto: salva snapshot timestamped de todos os dados críticos
  if (action === 'backup-auto') {
    var chaves = ['reparoeletro_pipe','reparoeletro_financeiro','reparoeletro_board',
                  'reparoeletro_logistica','reparoeletro_frenteloja',
                  'reparoeletro_conflitos','pj_fornecedores'];
    var ts  = new Date().toISOString().replace(/[:.]/g,'-').slice(0,16); // 2026-05-29T14-30
    var res2 = { ts, salvos:[], erros:[] };
    for (var ki=0; ki<chaves.length; ki++) {
      var chave = chaves[ki];
      try {
        var dado = await dbGet(chave);
        if (dado) {
          var bakKey = chave + '_bak_' + ts;
          await dbSet(bakKey, dado);
          // TTL de 4 dias (345600 segundos) — evita acúmulo de storage
          await dbFetch('EXPIRE', bakKey, 345600);
          res2.salvos.push(bakKey);
        }
      } catch(e) { res2.erros.push({chave, erro:e.message}); }
    }
    // Salvar índice de backups
    try {
      var idx2 = (await dbGet('reparoeletro_backup_index')) || [];
      idx2.push({ ts, chaves: res2.salvos });
      idx2 = idx2.slice(-96); // últimos 96 backups (4 dias)
      await dbSet('reparoeletro_backup_index', idx2);
    } catch(e){}
    return res.status(200).json({ ok:true, ...res2 });
  }


  // ── GET diagnostico-erp: conta fases no Redis, detecta divergências ──────
  if (action === 'diagnostico-erp') {
    try {
      var db = await dbGet(PIPE_KEY) || {cards:[]};
      var cards2 = db.cards || [];
      // Contar por fase (incluindo variações/erros)
      var faseCount = {};
      cards2.forEach(function(c){ faseCount[c.phase||'SEM_FASE']=(faseCount[c.phase||'SEM_FASE']||0)+1; });
      // Identificar fases com ID do Pipefy ao invés do local
      var pipefyFases = cards2.filter(function(c){ return /^\d{8,}$/.test(c.phase||''); });
      // ERP específico
      var erpLocal    = cards2.filter(function(c){ return c.phase==='erp'; });
      var erpPipefyId = cards2.filter(function(c){ return c.phase==='339008925'; });
      var erpValor    = erpLocal.reduce(function(s,c){return s+(parseFloat(c.valor)||0);},0);
      var resposta = {
        ok:true,
        totalCards: cards2.length,
        porFase: faseCount,
        erpLocal: erpLocal.length,
        erpComIdPipefy: erpPipefyId.length,
        erpValorTotal: erpValor,
        fasesComIdPipefy: pipefyFases.length,
        exemplos: pipefyFases.slice(0,5).map(function(c){return {id:c.id,nome:c.nomeContato,phase:c.phase};})
      };
      // Opcional: incluir dados dos cards ERP quando ?cards=1
      if (req.query.cards === '1') {
        resposta.erpCards = erpLocal.map(function(c){
          return {id:c.id,nomeContato:c.nomeContato||'',valor:c.valor||0,origem:c.origem||'',descricao:c.descricao||c.equipamento||''};
        });
      }
      return res.status(200).json(resposta);
    } catch(e){return res.status(500).json({ok:false,error:e.message});}
  }

  // ── GET fix-fases-erp: converte phase=339008925 → phase=erp no Redis ─────
  if (action === 'fix-fases-erp') {
    try {
      var db=await dbGet(PIPE_KEY)||{cards:[]};
    
      var corrigidos=0;
      (db.cards||[]).forEach(function(card){
        if(FASE_MAP[card.phase]){
          card.phase=FASE_MAP[card.phase];
          corrigidos++;
        }
      });
      if(corrigidos>0) await safeWritePipe(db);
      var erpDepois=(db.cards||[]).filter(function(c){return c.phase==='erp';}).length;
      return res.status(200).json({ok:true,corrigidos,erpDepois,total:(db.cards||[]).length});
    } catch(e){return res.status(500).json({ok:false,error:e.message});}
  }


  // ── GET arquivar-ultima-chamada: arquiva fichas +90 dias em ultima_chamada ─
  if (action === 'arquivar-ultima-chamada') {
    var ARQUIVO_KEY  = 'reparoeletro_arquivo';
    var LIMITE_DIAS  = parseInt(req.query.dias || '90');
    var LIMITE_MS    = LIMITE_DIAS * 24 * 60 * 60 * 1000;
    var corte        = new Date(Date.now() - LIMITE_MS);
    try {
      var db  = await dbGet(PIPE_KEY) || {cards:[]};
      var arq = await dbGet(ARQUIVO_KEY) || {fichas:[], totalArquivado:0};
      if (!Array.isArray(arq.fichas)) arq.fichas = [];

      // Regras por fase: ultima_chamada 90d | finalizado 7d | descarte 7d
      var REGRAS_ARQ = { ultima_chamada: LIMITE_DIAS, finalizado: 7, descarte: 7 };
      var paraArquivar = (db.cards||[]).filter(function(c){
        var diasFase = REGRAS_ARQ[c.phase];
        if (diasFase == null) return false;
        var dt = new Date(c.movedAt || c.criadoEm || 0);
        return dt < new Date(Date.now() - diasFase*24*60*60*1000);
      });

      if (!paraArquivar.length)
        return res.status(200).json({ok:true,arquivados:0,msg:'Nenhuma ficha elegível (ultima_chamada>'+LIMITE_DIAS+'d, finalizado>7d, descarte>7d)'});

      // IDs já arquivados (para idempotência)
      var jaArq = {};
      arq.fichas.forEach(function(f){ jaArq[f.id]=true; });

      var novos = 0;
      paraArquivar.forEach(function(card){
        if (jaArq[card.id]) return;
        arq.fichas.unshift(Object.assign({}, card, {
          arquivadoEm: now,
          motivoArquivo: card.phase+'_'+(REGRAS_ARQ[card.phase]||LIMITE_DIAS)+'d',
          phaseAntes: card.phase
        }));
        novos++;
      });

      // Remover do pipe ativo
      var idsArquivados = paraArquivar.map(function(c){return c.id;});
      db.cards = db.cards.filter(function(c){ return !idsArquivados.includes(c.id); });

      arq.totalArquivado = (arq.totalArquivado||0) + novos;
      arq.ultimoArquivo  = now;

      // Backup antes de salvar
      try{await dbSet('reparoeletro_pipe_bak_pre_arquivo',{cards:db.cards,ts:now});}catch(e){}

      await safeWritePipe( db);
      await dbSet(ARQUIVO_KEY, arq);

      return res.status(200).json({
        ok:true, arquivados:novos, totalNoArquivo:arq.fichas.length,
        removidosDoAtivo:idsArquivados.length,
        fichas: paraArquivar.slice(0,10).map(function(c){return {
          id:c.id, nome:c.nomeContato, movedAt:c.movedAt||c.criadoEm
        }})
      });
    } catch(e){return res.status(500).json({ok:false,error:e.message});}
  }

  // ── GET buscar-arquivo: busca fichas arquivadas ──────────────────────────
  if (action === 'buscar-arquivo') {
    var q3 = (req.query.q||'').toLowerCase();
    try {
      var arq2 = await dbGet('reparoeletro_arquivo') || {fichas:[]};
      var lista = (arq2.fichas||[]);
      if (q3) lista = lista.filter(function(f){
        return JSON.stringify(f).toLowerCase().includes(q3);
      });
      return res.status(200).json({
        ok:true, total:lista.length, totalArquivado:arq2.totalArquivado||0,
        ultimoArquivo:arq2.ultimoArquivo||null,
        fichas: lista.slice(0,50).map(function(f){return {
          id:f.id, nome:f.nomeContato, telefone:f.telefone,
          equipamento:f.equipamento, valor:f.valor,
          arquivadoEm:f.arquivadoEm, movedAt:f.movedAt,
          motivoArquivo:f.motivoArquivo||null,
          pipefyId:f.pipefyId
        };})
      });
    } catch(e){return res.status(500).json({ok:false,error:e.message});}
  }

  // ── GET restaurar-arquivo: devolve uma ficha arquivada para o pipe ───────
  if (action === 'restaurar-arquivo') {
    var rid = req.query.id||'';
    if (!rid) return res.status(400).json({ok:false,error:'id obrigatorio'});
    try {
      var arq3  = await dbGet('reparoeletro_arquivo') || {fichas:[]};
      var db3   = await dbGet(PIPE_KEY) || {cards:[]};
      var idx3  = (arq3.fichas||[]).findIndex(function(f){return f.id===rid;});
      if (idx3<0) return res.status(404).json({ok:false,error:'Ficha não encontrada no arquivo: '+rid});
      var ficha3 = arq3.fichas.splice(idx3,1)[0];
      ficha3.phase = 'aguardando_aprovacao';
      ficha3.restauradoEm = now;
      db3.cards.unshift(ficha3);
      await safeWritePipe( db3);
      await dbSet('reparoeletro_arquivo', arq3);
      return res.status(200).json({ok:true,id:ficha3.id,nome:ficha3.nomeContato,restauradoPara:'aguardando_aprovacao'});
    } catch(e){return res.status(500).json({ok:false,error:e.message});}
  }



  // ── GET erp-cards: retorna todos os cards em fase ERP ────────────────────
  if (action === 'erp-cards') {
    res.setHeader('Cache-Control','no-store,no-cache');
    var db3 = await dbGet(PIPE_KEY);
    var erpCards = (db3&&db3.cards)
      ? db3.cards.filter(function(c){return c.phase==='erp';}).map(function(c){
          return { id:c.id, nomeContato:c.nomeContato||c.title||'', valor:c.valor||0,
                   origem:c.origem||'', descricao:c.descricao||c.equipamento||'',
                   movedAt:c.movedAt||c.criadoEm||'', pipefyId:c.pipefyId||null };
        })
      : [];
    return res.status(200).json({ok:true, count:erpCards.length, cards:erpCards});
  }

  // ── GET erp-count: conta ERP diretamente do Redis (sem cache) ────────────
  if (action === 'erp-count') {
    res.setHeader('Cache-Control','no-store,no-cache');
    var db2 = await dbGet(PIPE_KEY);
    var tot  = (db2&&db2.cards)?db2.cards.length:0;
    var erp2 = (db2&&db2.cards)?db2.cards.filter(function(c){return c.phase==='erp';}).length:0;
    var val2 = (db2&&db2.cards)?db2.cards.filter(function(c){return c.phase==='erp';}).reduce(function(s,c){return s+(parseFloat(c.valor)||0);},0):0;
    return res.status(200).json({ok:true,erp:erp2,total:tot,valor:val2});
  }


  // ── GET relatorio-fichas: relatório completo de fichas por busca ────────────
  if (action === 'relatorio-fichas') {
    var termos = (req.query.q || '').split(',').map(function(t){return t.trim().toLowerCase();}).filter(Boolean);
    if (!termos.length) return res.status(400).json({ok:false,error:'Informe ?q=termo1,termo2'});
    try {
      var fin3 = await dbGet('reparoeletro_financeiro') || {records:[]};
      var pip3 = await dbGet(PIPE_KEY) || {cards:[]};
      var relFichas = [];

      termos.forEach(function(termo){
        // Buscar no financeiro
        var finMatch = (fin3.records||[]).filter(function(r){
          return JSON.stringify(r).toLowerCase().includes(termo);
        });
        // Buscar no pipe
        var pipMatch = (pip3.cards||[]).filter(function(c){
          return JSON.stringify(c).toLowerCase().includes(termo);
        });

        relFichas.push({
          busca: termo,
          financeiro: finMatch.map(function(r){return {
            id: r.id, nome: r.nomeContato, fase: r.phaseId,
            valor: r.valor, pipefyId: r.pipefyId, osCode: r.osCode,
            mpPreferenceId: r.mp && r.mp.preferenceId,
            mpStatus: r.mp && r.mp.status,
            mpPaymentId: r.mp && r.mp.paymentId,
            pagoEm: r.mp && r.mp.pagoEm,
            criadoEm: r.criadoEm
          };}),
          pipe: pipMatch.map(function(c){return {
            id: c.id, nome: c.nomeContato, fase: c.phase,
            valor: c.valor, pipefyId: c.pipefyId
          };}),
          resumo: {
            finEncontradas: finMatch.length,
            pipEncontradas: pipMatch.length,
            faseFin: finMatch.map(function(r){return r.phaseId;}).join(', '),
            fasePipe: pipMatch.map(function(c){return c.phase;}).join(', ')
          }
        });
      });

      return res.status(200).json({ok:true, termos:termos, fichas:relFichas});
    } catch(e){return res.status(500).json({ok:false,error:e.message});}
  }

  // ── GET forcar-solicitar-entrega: força card do pipe para solicitar_entrega ──
  if (action === 'forcar-solicitar-entrega') {
    var fseId = req.query.id || '';
    var fseNome = (req.query.busca||'').toLowerCase();
    if (!fseId && !fseNome) return res.status(400).json({ok:false,error:'id ou busca obrigatorio'});
    try {
      var pip4 = await dbGet(PIPE_KEY) || {cards:[]};
      var card4 = fseId
        ? (pip4.cards||[]).find(function(c){return c.id===fseId||c.pipefyId===fseId;})
        : (pip4.cards||[]).find(function(c){return JSON.stringify(c).toLowerCase().includes(fseNome);});
      if (!card4) return res.status(404).json({ok:false,error:'Card não encontrado',id:fseId,busca:fseNome});
      var faseAntes = card4.phase;
      card4.history = (card4.history||[]).concat([{phase:faseAntes,ts:now}]);
      card4.phase   = 'solicitar_entrega';
      card4.movedAt = now;
      await safeWritePipe( pip4);
      return res.status(200).json({ok:true,id:card4.id,nome:card4.nomeContato,de:faseAntes,para:'solicitar_entrega'});
    } catch(e){return res.status(500).json({ok:false,error:e.message});}
  }


  // ── GET fin-confirmar-pagamento: confirma ficha com paymentId específico do MP ──
  if (action === 'fin-confirmar-pagamento') {
    var fichaId9 = req.query.id      || '';
    var payId9   = req.query.payId   || '';
    var valor9   = parseFloat(req.query.valor||0);
    var metodo9  = req.query.metodo  || 'mp';
    if (!fichaId9) return res.status(400).json({ok:false,error:'id obrigatorio'});
    try {
      var U9=(process.env.UPSTASH_URL||'').replace(/['"]/g,'').trim();
      var T9=(process.env.UPSTASH_TOKEN||'').replace(/['"]/g,'').trim();
      async function _g9(k){var r=await fetch(U9+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T9,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});var j=await r.json();var v=j[0]?.result;if(!v)return null;try{var x=JSON.parse(v);if(typeof x==='string')x=JSON.parse(x);return x;}catch(e){return null;}}
      async function _s9(k,v){await fetch(U9+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T9,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});}
      var fin9=await _g9('reparoeletro_financeiro');
      if(!fin9||!Array.isArray(fin9.records))return res.status(404).json({ok:false,error:'financeiro vazio'});
      var rec9=fin9.records.find(function(r){return r.id===fichaId9||r.pipefyId===fichaId9||(r.osCode&&r.osCode===fichaId9);});
      if(!rec9)return res.status(404).json({ok:false,error:'ficha nao encontrada: '+fichaId9});
      if(rec9.phaseId==='entrega_liberada')return res.status(200).json({ok:true,info:'ja em entrega_liberada',id:rec9.id});
      var ts9=now;
      rec9.phaseId='entrega_liberada'; rec9.paidAt=ts9; rec9.movedAt=ts9;
      rec9.mp=Object.assign(rec9.mp||{},{status:'pago',pagoEm:ts9,metodo:metodo9,valor:valor9||rec9.valor,paymentId:payId9||null});
      rec9.history=(rec9.history||[]).concat([{phaseId:'pagamento_confirmado',ts:ts9},{phaseId:'entrega_liberada',ts:ts9}]);
      try{await _s9('reparoeletro_financeiro_backup',Object.assign({},fin9,{backedUpAt:ts9}));}catch(e){}
      await _s9('reparoeletro_financeiro',fin9);
      // Mover Pipe ADM — busca por pipefyId, osCode ou id
      var pipeDb9=await _g9('reparoeletro_pipe'); var pipeOk9=false;
      if(pipeDb9&&Array.isArray(pipeDb9.cards)){
        var pc9=pipeDb9.cards.find(function(c){
          return (rec9.pipefyId&&(c.pipefyId===String(rec9.pipefyId)||c.id===String(rec9.pipefyId)))||
                 (rec9.osCode&&(c.id===String(rec9.osCode)||c.pipefyId===String(rec9.osCode)))||
                 c.id===fichaId9;
        });
        if(pc9){pc9.history=(pc9.history||[]).concat([{phase:pc9.phase,ts:ts9}]);pc9.phase='solicitar_entrega';pc9.movedAt=ts9;await _s9('reparoeletro_pipe',pipeDb9);pipeOk9=true;}
      }
      return res.status(200).json({ok:true,id:rec9.id,nome:rec9.nomeContato,para:'entrega_liberada',pipeOk:pipeOk9,paymentId:payId9});
    } catch(e){return res.status(500).json({ok:false,error:e.message});}
  }


  // ── GET forcar-aguardando: força ficha da logistica para aguardando_aprovacao no Pipe ──
  if (action === 'forcar-aguardando') {
    var buscaA = (req.query.busca||'').toLowerCase();
    var idA    = req.query.id || '';
    if (!buscaA && !idA) return res.status(400).json({ok:false,error:'busca ou id obrigatorio'});
    try {
      // Buscar na logistica
      var logDb = await dbGet('reparoeletro_logistica') || {fichas:[]};
      var fichaL = null;
      // Normalizar busca: remover acentos e caracteres especiais
      function norm(s){ return (s||'').normalize('NFD').replace(/[\u0300-\u036f]/g,'').toLowerCase(); }
      var buscaN = norm(buscaA);

      if (idA) fichaL = (logDb.fichas||[]).find(function(f){ return f.id===idA||f.pipefyCardId===idA; });
      if (!fichaL && buscaN) fichaL = (logDb.fichas||[]).find(function(f){ return norm(JSON.stringify(f)).includes(buscaN); });

      // Buscar no orçamento se não achou na logística
      var fichaO = null;
      if (!fichaL) {
        var orcDb2 = await dbGet('reparoeletro_orcamentos') || {fichas:[]};
        if (!orcDb2.fichas) orcDb2 = await dbGet('reparoeletro_orc') || {fichas:[]};
        if (!orcDb2.fichas) orcDb2 = await dbGet('reparoeletro_logistica') || {fichas:[]};
        if (buscaN) fichaO = (orcDb2.fichas||[]).find(function(f){ return norm(JSON.stringify(f)).includes(buscaN); });
      }

      var origem = fichaL || fichaO;
      if (!origem) return res.status(404).json({ok:false,error:'Ficha não encontrada na logística/orçamento',busca:buscaA});

      // Criar ou mover no Pipe
      var pip5 = await dbGet(PIPE_KEY) || {cards:[],syncedPipefyIds:[],lastSync:null};
      if (!Array.isArray(pip5.cards)) pip5.cards=[];
      var ts5 = now;

      // Verificar se já existe
      var jaExiste5 = pip5.cards.find(function(c){
        return (origem.pipefyCardId && (c.pipefyId===String(origem.pipefyCardId)||c.id===String(origem.pipefyCardId))) ||
               (origem.id && (c.localId===String(origem.id)||c.id===String(origem.id)));
      });

      if (jaExiste5) {
        // Atualizar fase
        jaExiste5.history=(jaExiste5.history||[]).concat([{phase:jaExiste5.phase,ts:ts5}]);
        jaExiste5.phase='aguardando_aprovacao';
        jaExiste5.movedAt=ts5;
        jaExiste5.aguardandoDesde=ts5;
      } else {
        pip5.cards.unshift({
          id: 'PIPE-'+Date.now().toString(36).toUpperCase()+'-'+Math.random().toString(36).slice(2,5).toUpperCase(),
          pipefyId: origem.pipefyCardId ? String(origem.pipefyCardId) : null,
          localId:  origem.id ? String(origem.id) : null,
          phase: 'aguardando_aprovacao',
          nomeContato: origem.nome||origem.nomeContato||'',
          telefone: origem.telefone||'',
          equipamento: origem.equipamento||'',
          descricao: origem.defeito||origem.descricao||'',
          valor: parseFloat(origem.valorOrcamento||origem.valor||0)||0,
          origem: 'logistica_manual',
          criadoEm: ts5, movedAt: ts5, aguardandoDesde: ts5,
          history:[], analiseCompra:false
        });
      }
      pip5.lastSync=ts5;
      await safeWritePipe( pip5);

      return res.status(200).json({
        ok:true, acao: jaExiste5 ? 'atualizado' : 'criado',
        nome: origem.nome||origem.nomeContato,
        fase: 'aguardando_aprovacao',
        fonte: fichaL ? 'logistica' : 'orcamento'
      });
    } catch(e){ return res.status(500).json({ok:false,error:e.message}); }
  }


  // ── GET erp-report: página HTML renderizada server-side ──────────────────
  if (action === 'erp-report') {
    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    res.setHeader('Cache-Control', 'no-store');
    try {
      var db = await dbGet(PIPE_KEY) || {cards:[]};
      var erp = (db.cards||[]).filter(function(c){return c.phase==='erp';});
      erp.sort(function(a,b){return (parseFloat(a.valor)||0)-(parseFloat(b.valor)||0);});
      var total  = erp.length;
      var comVal = erp.filter(function(c){return (parseFloat(c.valor)||0)>0;});
      var soma   = comVal.reduce(function(s,c){return s+(parseFloat(c.valor)||0);},0);
      var ticket = comVal.length ? soma/comVal.length : 0;
      var semVal = total - comVal.length;
      var maxVal = Math.max.apply(null, erp.map(function(c){return parseFloat(c.valor)||0;}).concat([1]));
      function BRL(v){return 'R$ '+(+v||0).toLocaleString('pt-BR',{minimumFractionDigits:2,maximumFractionDigits:2});}
      function cor(v){return !v?'#444':v<200?'#ef4444':v<500?'#f5c800':'#22c55e';}
      function esc(s){return (s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}
      function orig(c){
        var o=(c.origem||'').toLowerCase();
        if(o.includes('venda'))return'Venda';
        if(o.includes('garantia'))return'Garantia';
        if(o.includes('loja'))return'Loja';
        return'Reparo';
      }
      var rows = erp.map(function(c,i){
        var v=parseFloat(c.valor)||0;
        var pct=Math.round(v/maxVal*100);
        var tp=orig(c);
        return '<tr><td style="color:#444">'+(i+1)+'</td>'+
          '<td>'+esc(c.nomeContato||'—')+'</td>'+
          '<td><span style="font-size:9px;padding:2px 8px;border-radius:3px;background:rgba(245,200,0,.1);color:#f5c800">'+tp+'</span></td>'+
          '<td style="font-weight:700;color:'+cor(v)+'">'+( v>0 ? BRL(v) : '<span style="color:#333">—</span>' )+'</td>'+
          '<td style="min-width:100px"><div style="display:flex;align-items:center;gap:6px">'+
            '<div style="flex:1;height:3px;background:#111;border-radius:2px;overflow:hidden">'+
              '<div style="width:'+pct+'%;height:3px;background:'+cor(v)+';border-radius:2px"></div>'+
            '</div><span style="font-size:9px;color:#444">'+pct+'%</span></div></td></tr>';
      }).join('');
      var ticketStr = BRL(ticket);
      var somaStr   = 'R$ '+(soma||0).toLocaleString('pt-BR',{minimumFractionDigits:0});
      var ticketCor = ticket<300?'#ef4444':ticket<450?'#f5c800':'#22c55e';
      var html = '<!DOCTYPE html><html lang="pt-BR"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">'+
        '<title>Análise ERP</title>'+
        '<style>*{margin:0;padding:0;box-sizing:border-box}body{background:#080808;color:#e8e8e8;font-family:system-ui,sans-serif}'+
        '.hd{background:#0f0f0f;border-bottom:1px solid #1e1e1e;padding:14px 20px;display:flex;justify-content:space-between;align-items:center}'+
        '.hd h1{font-size:16px;font-weight:700}.hd h1 span{color:#f5c800}'+
        '.hd a{font-size:11px;padding:5px 12px;border:1px solid #333;border-radius:6px;color:#888;text-decoration:none}'+
        '.hd a:hover{border-color:#f5c800;color:#f5c800}'+
        '.w{max-width:1100px;margin:0 auto;padding:20px}'+
        '.kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:10px;margin-bottom:16px}'+
        '.k{background:#0f0f0f;border:1px solid #1e1e1e;border-radius:8px;padding:14px}'+
        '.k .l{font-size:9px;color:#555;text-transform:uppercase;letter-spacing:.08em;margin-bottom:6px}'+
        '.k .v{font-size:24px;font-weight:700}.k .s{font-size:10px;color:#555;margin-top:3px}'+
        '.box{background:#0f0f0f;border:1px solid #1e1e1e;border-radius:8px;padding:18px;margin-bottom:14px}'+
        '.box h2{font-size:13px;font-weight:700;margin-bottom:12px}'+
        '.tw{max-height:500px;overflow-y:auto}.tw::-webkit-scrollbar{width:3px}.tw::-webkit-scrollbar-thumb{background:#2a2a2a}'+
        'table{width:100%;border-collapse:collapse;font-size:11px}'+
        'th{font-size:9px;text-transform:uppercase;color:#444;padding:7px 10px;border-bottom:1px solid #1a1a1a;text-align:left;background:#0f0f0f;position:sticky;top:0}'+
        'td{padding:8px 10px;border-bottom:1px solid #0d0d0d}</style></head>'+
        '<body><div class="hd"><h1>📊 Análise <span>ERP</span> — Reparo Eletro BH</h1>'+
        '<a href="/api/pipe?action=erp-report">↺ Atualizar</a></div>'+
        '<div class="w">'+
        '<div class="kpis">'+
          '<div class="k"><div class="l">Total ERP</div><div class="v" style="color:#f5c800">'+total+'</div></div>'+
          '<div class="k"><div class="l">Ticket Médio</div><div class="v" style="color:'+ticketCor+'">'+ticketStr+'</div><div class="s">'+comVal.length+' fichas c/ valor</div></div>'+
          '<div class="k"><div class="l">Volume Total</div><div class="v" style="color:#22c55e">'+somaStr+'</div></div>'+
          '<div class="k"><div class="l">Sem Valor</div><div class="v" style="color:'+(semVal>5?'#ef4444':'#666')+'">'+semVal+'</div><div class="s">'+(total?Math.round(semVal/total*100):0)+'%</div></div>'+
          '<div class="k"><div class="l">Total Cards</div><div class="v" style="color:#555">'+(db.cards||[]).length+'</div><div class="s">no pipe</div></div>'+
        '</div>'+
        '<div class="box"><h2>Fichas ERP — Menor ao Maior Valor</h2>'+
        '<div class="tw"><table><thead><tr><th>#</th><th>Nome</th><th>Tipo</th><th>Valor</th><th>Barra</th></tr></thead><tbody>'+
        rows+'</tbody></table></div></div></div></body></html>';
      return res.end(html);
    } catch(e){
      return res.end('<html><body style="background:#080808;color:#ef4444;font-family:monospace;padding:40px">Erro: '+e.message+'</body></html>');
    }
  }


  // ── GET limpar-backups: remove chaves de backup antigas do Redis ──────────
  if (action === 'limpar-backups') {
    try {
      const idx3 = (await dbGet('reparoeletro_backup_index')) || [];
      // Manter apenas os últimos 48 backups no índice
      const recentes = idx3.slice(-48);
      const chavesRecentes = new Set(recentes.flatMap(function(b){ return b.chaves||[]; }));
      // Deletar chaves de backup antigas (não estão nos últimos 48)
      const antigas = idx3.slice(0,-48);
      var deletados = 0;
      for (var bi=0; bi<antigas.length; bi++) {
        var bck = antigas[bi];
        for (var bki=0; bki<(bck.chaves||[]).length; bki++) {
          var bk = bck.chaves[bki];
          if (!chavesRecentes.has(bk)) {
            await dbFetch('DEL', bk);
            deletados++;
          }
        }
      }
      await dbSet('reparoeletro_backup_index', recentes);
      return res.status(200).json({ ok:true, deletados, mantidos:chavesRecentes.size });
    } catch(e){ return res.status(500).json({ok:false,error:e.message}); }
  }


  // ── GET limpar-backups-scan: varre Redis e apaga TODAS as chaves _bak_ antigas ──
  if (action === 'limpar-backups-scan') {
    try {
      const _lu=(process.env.UPSTASH_URL||'').replace(/['"]/g,'').trim();
      const _lt=(process.env.UPSTASH_TOKEN||'').replace(/['"]/g,'').trim();
      // Usar SCAN para encontrar todas as chaves _bak_
      // Manter apenas os 2 backups mais recentes por chave base
      const ultimasHoras = 24; // manter backups das últimas 24h
      const corte = new Date(Date.now() - ultimasHoras*60*60*1000)
                        .toISOString().replace(/[:.]/g,'-').slice(0,16);

      async function scanKeys(pattern) {
        var allKeys = [];
        var cursor = '0';
        do {
          const r = await fetch(_lu+'/pipeline', {
            method:'POST',
            headers:{ Authorization:'Bearer '+_lt, 'Content-Type':'application/json' },
            body: JSON.stringify([['SCAN', cursor, 'MATCH', pattern, 'COUNT', '100']])
          });
          const j = await r.json();
          const result = j[0]?.result || ['0',[]];
          cursor  = String(result[0]);
          const keys = result[1] || [];
          allKeys = allKeys.concat(keys);
        } while (cursor !== '0');
        return allKeys;
      }

      const bakKeys = await scanKeys('*_bak_*');
      var deletados2 = 0, mantidos2 = 0;

      for (var ki=0; ki<bakKeys.length; ki++) {
        var bk2 = bakKeys[ki];
        // Extrair timestamp da chave: ex reparoeletro_pipe_bak_2026-05-28T10-00
        var tsMatch = bk2.match(/_bak_(\d{4}-\d{2}-\d{2}T\d{2}-\d{2})$/);
        if (tsMatch && tsMatch[1] < corte) {
          await fetch(_lu+'/pipeline', {
            method:'POST',
            headers:{ Authorization:'Bearer '+_lt, 'Content-Type':'application/json' },
            body: JSON.stringify([['DEL', bk2]])
          });
          deletados2++;
        } else {
          mantidos2++;
        }
      }

      // Limpar índice de backup
      const idx4 = (await dbGet('reparoeletro_backup_index')) || [];
      const idxRecente = idx4.filter(function(b){ return !b.ts || b.ts >= corte.replace(/-/g,'').slice(0,13); });
      await dbSet('reparoeletro_backup_index', idxRecente.slice(-48));

      return res.status(200).json({ ok:true, totalEncontradas:bakKeys.length, deletados:deletados2, mantidos:mantidos2, corte });
    } catch(e){ return res.status(500).json({ok:false,error:e.message}); }
  }


  // ── GET forcar-ultima-chamada: força cards específicos para ultima_chamada ──
  if (action === 'forcar-ultima-chamada') {
    var idsParam = (req.query.ids || '').split(',').map(function(x){return x.trim();}).filter(Boolean);
    var buscaParam = (req.query.busca||'').toLowerCase();
    if (!idsParam.length && !buscaParam) return res.status(400).json({ok:false,error:'ids ou busca obrigatorio'});
    try {
      var pip6 = await dbGet(PIPE_KEY) || {cards:[]};
      var ts6  = now;
      var movidos6 = [];
      (pip6.cards||[]).forEach(function(card){
        var matchId    = idsParam.length && (idsParam.includes(card.id) || idsParam.includes(card.pipefyId));
        var matchBusca = buscaParam && JSON.stringify(card).toLowerCase().includes(buscaParam);
        if (!matchId && !matchBusca) return;
        if (card.phase === 'ultima_chamada') { movidos6.push({id:card.id,nome:card.nomeContato,info:'ja em ultima_chamada'}); return; }
        card.history = (card.history||[]).concat([{phase:card.phase,ts:ts6}]);
        card.phase   = 'ultima_chamada';
        card.movedAt = ts6;
        movidos6.push({id:card.id,nome:card.nomeContato,de:card.history.slice(-1)[0]?.phase,para:'ultima_chamada'});
      });
      if (movidos6.some(function(m){return m.para==='ultima_chamada';})) await safeWritePipe( pip6);
      return res.status(200).json({ok:true, movidos:movidos6.length, fichas:movidos6});
    } catch(e){ return res.status(500).json({ok:false,error:e.message}); }
  }


  // ── GET restaurar-fase: restaura card por nome exato para fase anterior ──
  if (action === 'restaurar-fase') {
    var rNome  = (req.query.nome||'').toLowerCase();
    var rFase  = req.query.fase || 'aprovados';
    if (!rNome) return res.status(400).json({ok:false,error:'nome obrigatorio'});
    try {
      var pip7 = await dbGet(PIPE_KEY)||{cards:[]};
      var ts7  = now;
      var card7 = (pip7.cards||[]).find(function(c){
        return (c.nomeContato||'').toLowerCase().includes(rNome) && c.phase === 'ultima_chamada';
      });
      if (!card7) return res.status(404).json({ok:false,error:'Card nao encontrado em ultima_chamada',nome:rNome});
      card7.history=(card7.history||[]).concat([{phase:'ultima_chamada',ts:ts7}]);
      card7.phase  = rFase;
      card7.movedAt= ts7;
      await safeWritePipe( pip7);
      return res.status(200).json({ok:true,id:card7.id,nome:card7.nomeContato,restauradoPara:rFase});
    } catch(e){ return res.status(500).json({ok:false,error:e.message}); }
  }


  // ── GET analisar-nomes: lista cards que não seguem padrão Nome XXXX ──────
  if (action === 'analisar-nomes') {
    try {
      var db8 = await dbGet(PIPE_KEY) || {cards:[]};
      var ignorar = ['ultima_chamada','finalizado'];
      var PADRAO = /^.+ \d{4}(?: \d{4})?$/; // "Nome 1234" ou "Nome 1234 1234"
      var semPadrao = [];
      (db8.cards||[]).forEach(function(card){
        if (ignorar.includes(card.phase)) return;
        if (!PADRAO.test((card.nomeContato||'').trim())) {
          semPadrao.push({
            id: card.id,
            nome: card.nomeContato||'',
            fase: card.phase,
            telefone: card.telefone||'',
            pipefyId: card.pipefyId||null
          });
        }
      });
      // Ordenar por fase
      const ordemFases = ['aguardando_aprovacao','aprovados','video_enviado','analise_compra','programar_entrega','solicitar_entrega','erp','garantia','receber'];
      semPadrao.sort(function(a,b){ return (ordemFases.indexOf(a.fase)||99)-(ordemFases.indexOf(b.fase)||99); });
      return res.status(200).json({ ok:true, total:semPadrao.length, cards:semPadrao });
    } catch(e){ return res.status(500).json({ok:false,error:e.message}); }
  }


  // ── POST corrigir-nomes: atualiza nomes no padrão fmt4dig ──────────────
  if (req.method === 'POST' && action === 'corrigir-nomes') {
    // body: { corrections: [{id, novoNome}] }
    var corrections = (req.body && req.body.corrections) || [];
    if (!corrections.length) return res.status(400).json({ok:false,error:'corrections obrigatorio'});
    try {
      var pip9 = await dbGet(PIPE_KEY) || {cards:[]};
      var ts9  = now;
      var corrigidos = [];
      corrections.forEach(function(fix){
        var card9 = (pip9.cards||[]).find(function(c){ return c.id===fix.id; });
        if (!card9) return;
        var nomeAntes = card9.nomeContato;
        card9.nomeContato = fix.novoNome;
        corrigidos.push({id:fix.id, de:nomeAntes, para:fix.novoNome});
      });
      if (corrigidos.length) await safeWritePipe( pip9);
      return res.status(200).json({ok:true, corrigidos});
    } catch(e){ return res.status(500).json({ok:false,error:e.message}); }
  }


  // ── GET fix-nomes-padrao: corrige as 12 fichas fora do padrão ───────────
  if (action === 'fix-nomes-padrao') {
    try {
      var pip10 = await dbGet(PIPE_KEY) || {cards:[]};
      var FIXES = [
        {id:'PIPE-0949', novo:'Weslei 3329'},
        {id:'PIPE-0942', novo:'Dalila 0402'},
        {id:'PIPE-0924', novo:'Priscila 8668'},
        {id:'PIPE-0921', novo:'Adriana 4369'},
        {id:'PIPE-0111', novo:'Tarcis 3877'},
        {id:'PIPE-0114', novo:'Edimilson 6386'},
        {id:'PIPE-0127', novo:'Cida 2077'},
        {id:'PIPE-0948', novo:'Marcelo 9545'},
        {id:'PIPE-0946', novo:'Paola 4476'},
        {id:'PIPE-0945', novo:'Leonardo 5857'},
        {id:'PIPE-0944', novo:'Simone 7660'},
        {id:'PIPE-0943', novo:'Silvana 7847'},
      ];
      var resultado = [];
      FIXES.forEach(function(fix){
        var card10 = (pip10.cards||[]).find(function(c){ return c.id===fix.id; });
        if (!card10) { resultado.push({id:fix.id, status:'nao encontrado'}); return; }
        var antes = card10.nomeContato;
        card10.nomeContato = fix.novo;
        resultado.push({id:fix.id, de:antes, para:fix.novo, status:'ok'});
      });
      var salvos = resultado.filter(function(r){return r.status==='ok';}).length;
      if (salvos > 0) await safeWritePipe( pip10);
      return res.status(200).json({ok:true, corrigidos:salvos, detalhes:resultado});
    } catch(e){ return res.status(500).json({ok:false,error:e.message}); }
  }


  // ── GET fix-nomes-padrao-v2: corrige Patrícia 8490 e Priscila ────────────
  if (action === 'fix-nomes-padrao-v2') {
    try {
      var pip11 = await dbGet(PIPE_KEY) || {cards:[]};
      var corrigidos11 = [];

      (pip11.cards||[]).forEach(function(card){
        // 1. Restaurar Patrícia 8490 (foi renomeada por engano para Priscila 8668)
        if (card.id==='PIPE-0924' && card.nomeContato==='Priscila 8668' && card.phase==='ultima_chamada') {
          card.nomeContato = 'Patrícia 8490';
          corrigidos11.push({id:card.id, de:'Priscila 8668', para:'Patrícia 8490', obs:'restaurado'});
        }
        // 2. Renomear a Priscila correta (aprovados, pipefyId 1358488260)
        if (card.id==='PIPE-0924' && card.nomeContato==='Priscila' && card.pipefyId==='1358488260') {
          card.nomeContato = 'Priscila 8668';
          corrigidos11.push({id:card.id, de:'Priscila', para:'Priscila 8668', obs:'corrigido'});
        }
        // 3. Adriana 4369 ja está correta — nada a fazer
      });

      if (corrigidos11.length) await safeWritePipe( pip11);
      return res.status(200).json({ok:true, ajustes:corrigidos11.length, detalhes:corrigidos11});
    } catch(e){ return res.status(500).json({ok:false,error:e.message}); }
  }


  // ── GET buscar-pipefy: leitura sob demanda do Pipefy (único ponto de contato restante) ──
  if (action === 'buscar-pipefy') {
    var bpCardId = req.query.cardId || '';
    var bpPhaseId = req.query.phaseId || '';
    if (!bpCardId && !bpPhaseId) return res.status(400).json({ok:false,error:'cardId ou phaseId obrigatorio'});
    try {
      const _pt = (process.env.PIPEFY_TOKEN||'').replace(/['"]/g,'').trim();
      if (!_pt) return res.status(503).json({ok:false,error:'PIPEFY_TOKEN nao configurado'});
      const query = bpCardId
        ? `query { card(id:"${bpCardId}") { id title current_phase { id name } fields { name value } } }`
        : `query { phase(id:"${bpPhaseId}") { id name cards(first:50) { edges { node { id title fields { name value } } } } } }`;
      const r = await fetch('https://api.pipefy.com/graphql', {
        method:'POST',
        headers:{ Authorization:'Bearer '+_pt, 'Content-Type':'application/json' },
        body: JSON.stringify({query})
      });
      const d = await r.json();
      return res.status(200).json({ok:true, data: d?.data || null, errors: d?.errors || null });
    } catch(e){ return res.status(500).json({ok:false,error:e.message}); }
  }

  // ── status ────────────────────────────────────────────────────────────────
  if (action === 'status') {
    var db = (await dbGet(PIPE_KEY)) || defaultDB();
    var cards = db.cards || [];
    var porFase = {};
    PHASES.forEach(function(ph) { porFase[ph.name] = 0; });
    cards.forEach(function(c) {
      var ph = PHASES.find(function(p) { return p.id === c.phase; });
      if (ph) porFase[ph.name] = (porFase[ph.name] || 0) + 1;
    });
    return res.status(200).json({
      ok: true, total: cards.length, lastSync: db.lastSync,
      porFase: porFase,
      amostra: cards.slice(0,3).map(function(c) {
        return { id: c.id, nome: c.nomeContato, fase: c.phase, pipefyId: c.pipefyId };
      })
    });
  }

  // ── load ──────────────────────────────────────────────────────────────────
  if (action === 'load') {
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate');
    res.setHeader('Pragma', 'no-cache');
    var db = (await dbGet(PIPE_KEY)) || defaultDB();
    return res.status(200).json({ ok: true, cards: db.cards || [], phases: PHASES, lastSync: db.lastSync });
  }

  // ── POST editar-valor ─────────────────────────────────────────────────────
  if (req.method === 'POST' && action === 'editar-valor') {
    var body  = req.body || {};
    var id    = body.id;
    var valor = parseFloat(body.valor) || 0;
    if (!id) return res.status(400).json({ ok: false, error: 'id obrigatorio' });
    var db   = (await dbGet(PIPE_KEY)) || defaultDB();
    var card = (db.cards || []).find(function(c) { return c.id === id; });
    if (!card) return res.status(404).json({ ok: false, error: 'nao encontrado' });
    card.valor = valor;
    await safeWritePipe( db);
    return res.status(200).json({ ok: true, valor: valor });
  }


  // ── GET force-valores: atualiza valores do Pipe via Logística/FL/Redis ──────
  if (action === 'force-valores') {
    const db  = (await dbGet(PIPE_KEY)) || defaultDB();
    const sem = (db.cards || []).filter(function(c){ return !c.valor || c.valor === 0; });
    let atualizados = 0;

    // Carregar fontes Redis
    const logDb = await dbGet('reparoeletro_logistica').catch(() => null);
    const flDb  = await dbGet('reparoeletro_frenteloja').catch(() => null);
    const logFichas = (logDb && logDb.fichas) ? logDb.fichas : [];
    const flFichas  = (flDb  && flDb.fichas)  ? flDb.fichas  : [];

    for (var ci = 0; ci < sem.length; ci++) {
      var card = sem[ci];
      var pid  = card.pipefyId || null;
      var novoValor = 0;

      // 1. Tentar logística (diagnostico.preco)
      var logFicha = logFichas.find(function(f){ return pid && f.pipefyCardId === String(pid); });
      if (logFicha && logFicha.diagnostico && logFicha.diagnostico.preco) {
        novoValor = parseFloat(logFicha.diagnostico.preco) || 0;
      }

      // 2. Tentar frente de loja (orcamento.valor)
      if (!novoValor) {
        var flFicha = flFichas.find(function(f){ return pid && f.pipefyCardId === String(pid); });
        if (flFicha && flFicha.orcamento && flFicha.orcamento.valor) {
          novoValor = parseFloat(flFicha.orcamento.valor) || 0;
        }
      }

      // 3. Tentar Pipefy (valor_de_contrato) se ainda não encontrou
      if (!novoValor && pid && PIPEFY_TOKEN) {
        try {
          
          if (pfData && pfData.card && pfData.card.fields) {
            var valField = pfData.card.fields.find(function(f){
              return f.name && (f.name.toLowerCase().includes('valor') || f.name.toLowerCase().includes('contrato'));
            });
            if (valField && valField.value) novoValor = parseFloat(valField.value) || 0;
          }
        } catch(ep) { /* ignora */ }
      }

      if (novoValor > 0) {
        card.valor = novoValor;
        atualizados++;
      }
    }

    if (atualizados > 0) await safeWritePipe( db);
    return res.status(200).json({
      ok: true,
      semValor: sem.length,
      atualizados: atualizados,
      restante: sem.length - atualizados
    });
  }

  // ── mover ─────────────────────────────────────────────────────────────────
  // ── SAIDAS-ERP-HOJE: fichas que saíram da coluna ERP hoje (history + backup) ──
  if (action === 'saidas-erp-hoje') {
    var hojeBRT = new Date(Date.now() - 3*3600000).toISOString().slice(0,10);
    var iniHoje = new Date(hojeBRT + 'T03:00:00.000Z').toISOString(); // 00:00 BRT
    var dbS = (await dbGet(PIPE_KEY)) || defaultDB();
    var porIdS = {};
    (dbS.cards||[]).forEach(function(c){ porIdS[String(c.id)] = c; });

    var saidas = {};

    // A) History: cards fora de erp cujo último registro de saída foi de erp hoje
    (dbS.cards||[]).forEach(function(c){
      if (c.phase === 'erp') return;
      var hst = c.history || [];
      if (!hst.length) return;
      var ult = hst[hst.length-1];
      if (ult.phase === 'erp' && ult.ts >= iniHoje) {
        saidas[String(c.id)] = {
          nome: c.nomeContato || c.title || '—',
          telefone: c.telefone || '',
          equipamento: c.equipamento || '',
          destino: c.phase,
          horaBRT: new Date(new Date(ult.ts).getTime()-3*3600000).toISOString().slice(11,16),
          fonte: 'history',
        };
      }
    });

    // B) Diff com o backup 03:30 de hoje (pega excluídos/arquivados também)
    var dowB = new Date(Date.now()-3*3600000).getUTCDay();
    var rawB = await dbGet('bk_' + (dowB%2) + '_reparoeletro_pipe');
    var snapEm = null;
    if (rawB) {
      var vB = rawB.v ?? rawB; snapEm = rawB.em || null;
      if (typeof vB === 'string') { try { vB = JSON.parse(vB); } catch(e) { vB = null; } }
      if (typeof vB === 'string') { try { vB = JSON.parse(vB); } catch(e) { vB = null; } }
      ((vB && vB.cards) || []).forEach(function(sc){
        if (sc.phase !== 'erp') return;
        var agora = porIdS[String(sc.id)];
        if (agora && agora.phase === 'erp') return; // ainda lá
        if (saidas[String(sc.id)]) return; // já pego pelo history
        saidas[String(sc.id)] = {
          nome: sc.nomeContato || sc.title || '—',
          telefone: sc.telefone || '',
          equipamento: sc.equipamento || '',
          destino: agora ? agora.phase : '(removido do pipe)',
          horaBRT: agora && agora.movedAt ? new Date(new Date(agora.movedAt).getTime()-3*3600000).toISOString().slice(11,16) : null,
          fonte: 'diff-backup',
        };
      });
    }

    var lista = Object.keys(saidas).map(function(k){ return Object.assign({id:k}, saidas[k]); });
    lista.sort(function(a,b){ return String(b.horaBRT||'').localeCompare(String(a.horaBRT||'')); });
    var porDestino = {};
    lista.forEach(function(s){ porDestino[s.destino] = (porDestino[s.destino]||0)+1; });

    return res.status(200).json({ ok:true, data:hojeBRT, snapshotBackup:snapEm,
      totalSaidasErp: lista.length, porDestino: porDestino, fichas: lista });
  }

  // ── LISTAR-ERP-VINDAS-DE: cards em ERP cuja fase anterior foi X (diagnóstico) ──
  if (action === 'listar-erp-vindas-de') {
    var faseOrigem = req.query.fase || 'solicitar_entrega';
    var db0 = (await dbGet(PIPE_KEY)) || defaultDB();
    var achados = (db0.cards || [])
      .filter(function(c){
        if (c.phase !== 'erp') return false;
        var h = c.history || [];
        if (!h.length) return false;
        return h[h.length-1].phase === faseOrigem;
      })
      .map(function(c){
        var h = c.history || [];
        return {
          id: c.id, pipefyId: c.pipefyId || null,
          nome: c.nomeContato || c.title || '—',
          telefone: c.telefone || '',
          equipamento: c.equipamento || '',
          movidoEm: h.length ? h[h.length-1].ts : c.movedAt,
        };
      })
      .sort(function(a,b){ return String(b.movidoEm).localeCompare(String(a.movidoEm)); });
    return res.status(200).json({ ok:true, faseOrigem: faseOrigem, total: achados.length, fichas: achados });
  }

  // ── REVERTER-FASE: devolve cards para a fase informada (correção de engano) ──
  if (action === 'reverter-fase') {
    var rvBody = req.body || {};
    var rvIds = rvBody.ids || (req.query.ids ? String(req.query.ids).split(',').map(function(s){return s.trim();}).filter(Boolean) : []);
    var rvFase = rvBody.phase || req.query.phase;
    if (!rvIds.length || !rvFase) return res.status(400).json({ ok:false, error:'ids[] e phase obrigatorios' });
    var rvOk = PHASES.find(function(p){ return p.id === rvFase; });
    if (!rvOk) return res.status(400).json({ ok:false, error:'fase invalida' });
    var rvDb = (await dbGet(PIPE_KEY)) || defaultDB();
    var rvNow = new Date().toISOString();
    var revertidos = [];
    rvIds.forEach(function(id){
      var c = (rvDb.cards || []).find(function(x){ return x.id === id; });
      if (!c) return;
      c.history = (c.history || []).concat([{ phase: c.phase, ts: rvNow }]);
      c.phase = rvFase;
      c.movedAt = rvNow;
      revertidos.push(c.nomeContato || c.title || c.id);
    });
    if (revertidos.length) await safeWritePipe(rvDb);
    return res.status(200).json({ ok:true, revertidos: revertidos.length, nomes: revertidos });
  }

  if (req.method === 'POST' && action === 'mover') {
    var body  = req.body || {};
    var id    = body.id;
    var phase = body.phase;
    if (!id || !phase) return res.status(400).json({ ok: false, error: 'id e phase obrigatorios' });
    var phOk = PHASES.find(function(p) { return p.id === phase; });
    if (!phOk) return res.status(400).json({ ok: false, error: 'fase invalida' });
    var db   = (await dbGet(PIPE_KEY)) || defaultDB();
    var card = (db.cards || []).find(function(c) { return c.id === id; });
    if (!card) return res.status(404).json({ ok: false, error: 'nao encontrado' });
    var now = new Date().toISOString();
    var faseAnterior = card.phase;
    card.history = (card.history || []).concat([{ phase: card.phase, ts: now }]);
    card.phase   = phase;
    card.movedAt = now;
    if (phase === 'aguardando_aprovacao') card.aguardandoDesde = now;
    await safeWritePipe( db);

    // ── Gatilhos downstream ──────────────────────────────────────────────
    var pid = card.pipefyId;
    // Aprovados → Board Técnico (fase: aprovado)
    if (phase === 'aprovados') {
      try {
        var boardPid = pid ? String(pid) : ('LOCAL-' + card.id);
        // Usar dbGet/dbSet do BOARD (mesmo formato) — leitura direta via Upstash
        var bU = (process.env.UPSTASH_URL   || '').replace(/['"]/g,'').trim();
        var bT = (process.env.UPSTASH_TOKEN || '').replace(/['"]/g,'').trim();
        async function bGet(k) {
          var r = await fetch(bU+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+bT,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});
          var j = await r.json(); var v = j[0]?.result; if(!v) return null;
          try { return JSON.parse(v); } catch(e){ return null; }
        }
        async function bSet(k,v) {
          await fetch(bU+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+bT,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});
        }
        var BOARD_KEY2 = 'reparoeletro_board';
        var boardDb2 = await bGet(BOARD_KEY2);
        if (!boardDb2 || typeof boardDb2 !== 'object') boardDb2 = { cards:[], syncedIds:[], movesLog:[], metaLog:[], phases:[], rsPhases:[], rsRuaPhases:[], rsCards:[], rsRuaCards:[] };
        if (!Array.isArray(boardDb2.cards)) boardDb2.cards = [];
        // Remover entrada antiga se existir
        boardDb2.cards = boardDb2.cards.filter(function(x){ return x.pipefyId !== boardPid && x.osCode !== card.id; });
        // Sempre inserir/recriar o card
        boardDb2.cards.unshift({
          pipefyId:    boardPid,
          phaseId:     'aprovado',
          nomeContato: card.nomeContato || '',
          title:       card.descricao   || card.nomeContato || '',
          telefone:    card.telefone    || '',
          descricao:   card.equipamento || card.descricao || '',
          osCode:      card.id,
          valor:       card.valor || 0,
          movedBy:     'Pipe ADM',
          flFichaId:   null,
          localOnly:   !pid,
          syncedAt:    now,
          movedAt:     now
        });
        if (!Array.isArray(boardDb2.syncedIds)) boardDb2.syncedIds = [];
        if (!boardDb2.syncedIds.includes(boardPid)) boardDb2.syncedIds.push(boardPid);
        if (!Array.isArray(boardDb2.movesLog)) boardDb2.movesLog = [];
        boardDb2.movesLog.push({ phaseId:'aprovado_entrada', pipefyId:boardPid, timestamp:now });
        if (!Array.isArray(boardDb2.metaLog)) boardDb2.metaLog = [];
        boardDb2.metaLog.push({ phaseId:'aprovado_entrada', pipefyId:boardPid, timestamp:now });
        await bSet(BOARD_KEY2, boardDb2);
      } catch(e) { console.error('[pipe→board]', e.message); }
    }
    // Video Enviado → criar ficha no Financeiro
    if (phase === 'video_enviado') {
      try {
        var finDb2 = await dbGet('reparoeletro_financeiro') || { records: [] };
        if (!Array.isArray(finDb2.records)) finDb2.records = [];
        var pipefyStr = pid ? String(pid) : null;
        var jaFinExiste = finDb2.records.find(function(r){
          return (pipefyStr && r.pipefyId === pipefyStr) || (r.osCode && r.osCode === card.id);
        });
        if (jaFinExiste) {
          // Já existe — mover para aguardando_dados
          if (jaFinExiste.phaseId !== 'aguardando_dados') {
            jaFinExiste.history = (jaFinExiste.history||[]).concat([{phaseId:jaFinExiste.phaseId,ts:now}]);
            jaFinExiste.phaseId = 'aguardando_dados';
            jaFinExiste.movedAt = now;
            await dbSet('reparoeletro_financeiro', finDb2);
          }
        } else {
          // Novo registro em aguardando_dados
          finDb2.records.unshift({
            id: 'FIN-' + (pipefyStr || card.id),
            pipefyId: pipefyStr,
            osCode: card.id,
            nomeContato: card.nomeContato || '',
            telefone: card.telefone || '',
            equipamento: card.equipamento || card.descricao || '',
            valor: card.valor || 0,
            phaseId: 'aguardando_dados',
            criadoEm: now, movedAt: now,
            history: [{ phaseId: 'aguardando_dados', ts: now }],
            origem: 'pipe_video_enviado'
          });
          await dbSet('reparoeletro_financeiro', finDb2);
        }
      } catch(e) { console.error('[pipe→financeiro]', e.message); }
    }
    // Analise de Compra → criar entrada em compra-equip
    if (phase === 'analise_compra') {
      try {
        var compraDb2 = await dbGet('reparoeletro_compra_equip') || { fichas: [] };
        if (!Array.isArray(compraDb2.fichas)) compraDb2.fichas = [];
        // Usar card.id como chave primária (pipefyId pode ser null em fichas locais)
        var compraId = (pid && String(pid) !== 'null' && String(pid) !== 'undefined')
          ? String(pid) : (card.id || card.osCode || String(Date.now()));
        var jaCompraExiste = compraDb2.fichas.find(function(f){
          return f.id === compraId || f.pipefyId === compraId ||
                 (card.nomeContato && (f.nomeContato||'').toLowerCase() === (card.nomeContato||'').toLowerCase());
        });
        if (!jaCompraExiste) {
          compraDb2.fichas.unshift({
            id: compraId, pipefyId: compraId,
            nomeContato: card.nomeContato || '',
            telefone: card.telefone || '',
            descricao: card.equipamento || card.descricao || '',
            valor: card.valor || 0,
            osCode: card.osCode || card.id || '',
            status: 'analise', fotos: [], criadoEm: now
          });
          await dbSet('reparoeletro_compra_equip', compraDb2);
        }
      } catch(e) { console.error('[pipe→compra]', e.message); }
    }

    // Log da ação
    var gatilhosLog = [];
    if (phase === 'aprovados')       gatilhosLog.push('→ Board Técnico');
    if (phase === 'analise_compra')  gatilhosLog.push('→ Compra Equip');
    if (phase === 'aguardando_aprovacao') gatilhosLog.push('Timer 48h iniciado');
    logAction({ modulo:'Pipe ADM', fichaId:card.id, ficha:card.nomeContato, acao:'Mover ficha', de:faseAnterior||'', para:phase, gatilho:gatilhosLog.join(' | '), status:'ok' }).catch(()=>{});

    return res.status(200).json({ ok: true, card: card });
  }

  // ── add-card ──────────────────────────────────────────────────────────────
  if (req.method === 'POST' && action === 'add-card') {
    var body = req.body || {};
    if (!body.nomeContato) return res.status(400).json({ ok: false, error: 'nomeContato obrigatorio' });
    var db = (await dbGet(PIPE_KEY)) || defaultDB();
    if (!Array.isArray(db.cards)) db.cards = [];
    // Checar duplicata por pipefyId
    if (body.pipefyId && db.cards.find(function(c) { return c.pipefyId === String(body.pipefyId); }))
      return res.status(200).json({ ok: true, info: 'ja existe (pipefyId)' });
    // Checar duplicata por nome+telefone nos últimos 60 segundos
    var JANELA_MS = 60 * 1000;
    var agora60 = Date.now();
    var nomeNorm = (body.nomeContato||'').trim().toLowerCase();
    var telNorm  = (body.telefone||'').replace(/\D/g,'');
    var dupRecente = db.cards.find(function(c) {
      var msDiff = agora60 - new Date(c.criadoEm||0).getTime();
      if (msDiff > JANELA_MS) return false;
      var cNome = (c.nomeContato||'').trim().toLowerCase();
      var cTel  = (c.telefone||'').replace(/\D/g,'');
      return cNome === nomeNorm && (telNorm === '' || cTel === telNorm);
    });
    if (dupRecente) return res.status(200).json({ ok: true, info: 'ja existe (duplicata recente)', card: dupRecente });
    var now  = new Date().toISOString();
    var ph   = body.phase || 'aguardando_aprovacao';
    var card = {
      id: 'PIPE-' + Date.now().toString(36).toUpperCase() + '-' + Math.random().toString(36).slice(2,5).toUpperCase(),
      pipefyId:        body.pipefyId ? String(body.pipefyId) : null,
      phase:           ph,
      nomeContato:     fmt4dig(body.nomeContato || '', body.telefone || ''),
      telefone:        body.telefone    || '',
      equipamento:     body.equipamento || '',
      descricao:       body.descricao   || '',
      valor:           parseFloat(body.valor) || 0,
      endereco:        body.endereco || '',
      origem:          body.origem || 'manual',
      criadoEm:        now, movedAt: now,
      aguardandoDesde: ph === 'aguardando_aprovacao' ? now : null,
      history: [], analiseCompra: false
    };
    db.cards.unshift(card);
    await safeWritePipe( db);
    // Log da ação
    var gatilhosLog = [];
    if (phase === 'aprovados')       gatilhosLog.push('→ Board Técnico');
    if (phase === 'analise_compra')  gatilhosLog.push('→ Compra Equip');
    if (phase === 'aguardando_aprovacao') gatilhosLog.push('Timer 48h iniciado');
    logAction({ modulo:'Pipe ADM', fichaId:card.id, ficha:card.nomeContato, acao:'Mover ficha', de:faseAnterior||'', para:phase, gatilho:gatilhosLog.join(' | '), status:'ok' }).catch(()=>{});

    return res.status(200).json({ ok: true, card: card });
  }

  // ── toggle-analise-compra ─────────────────────────────────────────────────
  if (req.method === 'POST' && action === 'toggle-analise-compra') {
    var body = req.body || {};
    var db   = (await dbGet(PIPE_KEY)) || defaultDB();
    var card = (db.cards || []).find(function(c) { return c.id === body.id; });
    if (!card) return res.status(404).json({ ok: false, error: 'nao encontrado' });
    card.analiseCompra = !card.analiseCompra;
    await safeWritePipe( db);
    return res.status(200).json({ ok: true, analiseCompra: card.analiseCompra });
  }

  // ── GET fix-compra — força criação de ficha em compra_equip ─────────────────
  if (action === 'fix-compra') {
    var q = (req.query.q || '').toLowerCase().trim();
    if (!q) return res.status(400).json({ ok:false, error:'Informe ?q=nome_ou_id' });
    var db = (await dbGet(PIPE_KEY)) || defaultDB();
    var card = (db.cards || []).find(function(c) {
      return (c.nomeContato||'').toLowerCase().includes(q) ||
             (c.id||'').toLowerCase().includes(q) ||
             (c.osCode||'').toLowerCase().includes(q);
    });
    if (!card) return res.status(404).json({ ok:false, error:'Card não encontrado: '+q,
      amostra:(db.cards||[]).slice(0,5).map(function(c){return {id:c.id,nome:c.nomeContato,phase:c.phase};}) });
    var compraDb = await dbGet('reparoeletro_compra_equip') || { fichas: [] };
    if (!Array.isArray(compraDb.fichas)) compraDb.fichas = [];
    var compraId = card.pipefyId && String(card.pipefyId) !== 'null' ? String(card.pipefyId) : (card.id || String(Date.now()));
    var jaExiste = compraDb.fichas.find(function(f){
      return f.id===compraId || (card.nomeContato && (f.nomeContato||'').toLowerCase()===(card.nomeContato||'').toLowerCase());
    });
    if (jaExiste) {
      // Se já existe mas sem telefone, atualizar
      var atualizou = false;
      if (!jaExiste.telefone && card.telefone) {
        jaExiste.telefone = card.telefone;
        atualizou = true;
      }
      if (!jaExiste.nomeContato && card.nomeContato) {
        jaExiste.nomeContato = card.nomeContato;
        atualizou = true;
      }
      if (atualizou) await dbSet('reparoeletro_compra_equip', compraDb);
      return res.status(200).json({ ok:true, msg: atualizou ? '✅ Telefone atualizado' : 'Já existe em compra_equip', ficha:jaExiste });
    }
    var now = new Date().toISOString();
    var nova = { id:compraId, pipefyId:compraId, nomeContato:card.nomeContato||'', osCode:card.osCode||card.id||'',
      telefone:card.telefone||'',
      descricao:card.equipamento||card.descricao||'', valor:card.valor||0, status:'analise', fotos:[], criadoEm:now };
    compraDb.fichas.unshift(nova);
    await dbSet('reparoeletro_compra_equip', compraDb);
    return res.status(200).json({ ok:true, msg:'✅ Ficha criada em Compra de Equipamentos', ficha:nova });
  }

    // ── GET erp-hoje — fichas que entraram em ERP hoje ─────────────────────────
  if (action === 'erp-hoje') {
    const db = (await dbGet(PIPE_KEY)) || defaultDB();
    const agora = new Date();
    // Data de hoje em horário de Brasília (UTC-3)
    const hoje = new Date(agora.getTime() - 3*60*60*1000).toISOString().slice(0,10);

    const cards = (db.cards || []).filter(function(c) {
      if (c.phase !== 'erp') return false;
      // Verificar movedAt ou último history
      const ts = c.movedAt || c.updatedAt || c.criadoEm || '';
      if (!ts) return false;
      const dia = new Date(new Date(ts).getTime() - 3*60*60*1000).toISOString().slice(0,10);
      return dia === hoje;
    });

    // Ordenar por horário de entrada
    cards.sort(function(a,b){
      return new Date(b.movedAt||b.updatedAt||0) - new Date(a.movedAt||a.updatedAt||0);
    });

    const tabela = cards.map(function(c) {
      const ts = c.movedAt || c.updatedAt || c.criadoEm || '';
      const horario = ts ? new Date(new Date(ts).getTime()-3*60*60*1000)
        .toLocaleString('pt-BR',{timeZone:'America/Sao_Paulo',
          day:'2-digit',month:'2-digit',year:'numeric',
          hour:'2-digit',minute:'2-digit'}) : '—';
      return {
        id:       c.id,
        nome:     c.nomeContato || c.title || '—',
        telefone: c.telefone || c.tel || '—',
        equipamento: (c.equipamento || c.descricao || '').slice(0,40),
        valor:    c.valor ? 'R$ '+parseFloat(c.valor).toLocaleString('pt-BR') : '—',
        horario,
      };
    });

    return res.status(200).json({
      ok: true,
      data: hoje,
      total: tabela.length,
      cards: tabela,
    });
  }

    // ── GET erp-fin-hoje — ERP e Finalizado enviados hoje ──────────────────────
  if (action === 'erp-fin-hoje') {
    const db  = (await dbGet(PIPE_KEY)) || defaultDB();
    const agora = new Date();
    const hoje = new Date(agora.getTime()-3*60*60*1000).toISOString().slice(0,10);

    function mapCard(c) {
      const ts = c.movedAt || c.updatedAt || c.criadoEm || '';
      const diaCard = ts ? new Date(new Date(ts).getTime()-3*60*60*1000).toISOString().slice(0,10) : '';
      const horario = ts ? new Date(new Date(ts).getTime()-3*60*60*1000)
        .toLocaleString('pt-BR',{timeZone:'America/Sao_Paulo',
          day:'2-digit',month:'2-digit',hour:'2-digit',minute:'2-digit'}) : '—';
      return { diaCard, horario,
        id: c.id, nome: c.nomeContato||c.title||'—',
        telefone: c.telefone||'—',
        equipamento: (c.equipamento||c.descricao||'').slice(0,50),
        valor: c.valor ? 'R$ '+parseFloat(c.valor).toLocaleString('pt-BR',{minimumFractionDigits:2}) : '—',
        endereco: c.endereco||'—',
        phase: c.phase,
      };
    }

    const erp = (db.cards||[]).filter(function(c){
      if(c.phase!=='erp') return false;
      const ts=c.movedAt||c.updatedAt||c.criadoEm||'';
      return ts && new Date(new Date(ts).getTime()-3*60*60*1000).toISOString().slice(0,10)===hoje;
    }).map(mapCard).sort(function(a,b){ return b.horario.localeCompare(a.horario); });

    const fin = (db.cards||[]).filter(function(c){
      if(c.phase!=='finalizado') return false;
      const ts=c.movedAt||c.updatedAt||c.criadoEm||'';
      return ts && new Date(new Date(ts).getTime()-3*60*60*1000).toISOString().slice(0,10)===hoje;
    }).map(mapCard).sort(function(a,b){ return b.horario.localeCompare(a.horario); });

    return res.status(200).json({ ok:true, data:hoje, totalErp:erp.length, totalFin:fin.length, erp, finalizado:fin });
  }

    // ── GET debug-ficha — busca ficha por nome/tel em pipe + board + frenteloja ─────
  if (action === 'debug-ficha') {
    const q = (req.query.q||'').toLowerCase().trim();
    if (!q) return res.status(400).json({ ok:false, error:'?q= obrigatório' });

    const BOARD_KEY = 'reparoeletro_board';
    const FL_KEY    = 'reparoeletro_frenteloja';

    const [pipeDb, boardDb, flDb, tvPipeDb, tvBoardDb] = await Promise.all([
      dbGet(PIPE_KEY),
      dbGet(BOARD_KEY),
      dbGet(FL_KEY),
      dbGet('tv_pipe'),
      dbGet('tv_board'),
    ]);

    function matchQ(c) {
      return (c.nomeContato||c.nome||c.title||'').toLowerCase().includes(q) ||
             (c.telefone||c.tel||'').replace(/\D/g,'').includes(q.replace(/\D/g,'')) ||
             (c.id||'').toLowerCase().includes(q);
    }

    const pipeCards = (pipeDb?.cards||[]).filter(matchQ).map(function(c){
      return { origem:'pipe', id:c.id, nome:c.nomeContato||c.title, tel:c.telefone,
               phase:c.phase, valor:c.valor, equipamento:c.equipamento||c.descricao,
               diagnosticoResumo:c.diagnosticoResumo||'', modeloTv:c.modeloTv||'',
               movedAt:c.movedAt, criadoEm:c.criadoEm, history:(c.history||[]).slice(-5) };
    });

    const boardCards = (boardDb?.cards||[]).filter(matchQ).map(function(c){
      return { origem:'board', id:c.pipefyId||c.id, nome:c.nomeContato||c.title, tel:c.telefone,
               phaseId:c.phaseId, movedAt:c.movedAt, tecnico:c.tecnico,
               localOnly:c.localOnly, age:c.age };
    });

    const flCards = ((flDb?.fichas||flDb?.cards||[]).filter(matchQ)).map(function(c){
      return { origem:'frenteloja', id:c.id, nome:c.nome||c.nomeContato, tel:c.telefone,
               phase:c.phase||c.status, equipamento:c.equipamento||c.aparelho,
               criadoEm:c.criadoEm, obs:c.obs };
    });

    // Cards TV pipe
    const tvPipeCards = ((tvPipeDb?.cards||[]).filter(matchQ)).map(function(c){
      return { origem:'tv_pipe', id:c.id, nome:c.nomeContato||c.title, tel:c.telefone,
               phase:c.phase, valor:c.valor, equipamento:c.equipamento||c.descricao,
               diagnosticoResumo:c.diagnosticoResumo||'', modeloTv:c.modeloTv||'',
               movedAt:c.movedAt, criadoEm:c.criadoEm, history:(c.history||[]).slice(-5) };
    });
    // Cards TV board
    const tvBoardCards = ((tvBoardDb?.cards||[]).filter(matchQ)).map(function(c){
      return { origem:'tv_board', id:c.pipefyId||c.id, nome:c.nomeContato||c.title, tel:c.telefone,
               phaseId:c.phaseId, movedAt:c.movedAt, tecnico:c.tecnico,
               diagnosticoResumo:c.diagnosticoResumo||'', modeloTv:c.modeloTv||'' };
    });

    return res.status(200).json({
      ok: true, busca: q,
      total: pipeCards.length + boardCards.length + flCards.length + tvPipeCards.length + tvBoardCards.length,
      pipe:       pipeCards,
      board:      boardCards,
      frenteloja: flCards,
      tv_pipe:    tvPipeCards,
      tv_board:   tvBoardCards,
    });
  }

    // ── GET buscar-amplo — busca em pipe+board+frenteloja ADM+TV por nome ────────
  if (action === 'buscar-amplo') {
    const q = (req.query.q||'').toLowerCase().trim();
    if (!q) return res.status(400).json({ ok:false, error:'?q= obrigatório' });

    const BOARD_KEY   = 'reparoeletro_board';
    const FL_ADM_KEY  = 'reparoeletro_frenteloja';
    const FL_TV_KEY   = 'tv_frenteloja';
    const TV_BOARD    = 'tv_board';

    const [pipeDb, boardDb, flAdm, flTv, tvBoard] = await Promise.all([
      dbGet(PIPE_KEY).catch(()=>null),
      dbGet(BOARD_KEY).catch(()=>null),
      dbGet(FL_ADM_KEY).catch(()=>null),
      dbGet(FL_TV_KEY).catch(()=>null),
      dbGet(TV_BOARD).catch(()=>null),
    ]);

    function match(c){ return (c.nomeContato||c.nome||c.title||'').toLowerCase().includes(q) || (c.telefone||c.tel||'').replace(/[^0-9]/g,'').includes(q.replace(/[^0-9]/g,'')); }
    function fmtDt(ts){ try{ return ts ? new Date(ts).toLocaleString('pt-BR',{timeZone:'America/Sao_Paulo',day:'2-digit',month:'2-digit',hour:'2-digit',minute:'2-digit'}) : '—'; }catch(e){return ts||'—';} }

    const resultado = {
      pipe:        (pipeDb?.cards||[]).filter(match).map(c=>({ id:c.id, nome:c.nomeContato||c.title, tel:c.telefone, phase:c.phase, equipamento:(c.equipamento||c.descricao||'').slice(0,50), valor:c.valor, movedAt:fmtDt(c.movedAt), criadoEm:fmtDt(c.criadoEm) })),
      board_adm:   (boardDb?.cards||[]).filter(match).map(c=>({ id:c.pipefyId||c.id, nome:c.nomeContato||c.title, tel:c.telefone, phaseId:c.phaseId, movedAt:fmtDt(c.movedAt), tecnico:c.tecnico })),
      frenteloja_adm: (flAdm?.fichas||[]).filter(match).map(f=>({ id:f.id, nome:f.nomeContato||f.nome, tel:f.telefone, phase:f.phase, equipamento:f.equipamento||f.aparelho, criadoEm:fmtDt(f.criadoEm), updatedAt:fmtDt(f.updatedAt) })),
      frenteloja_tv:  (flTv?.fichas||[]).filter(match).map(f=>({ id:f.id, nome:f.nomeContato||f.nome, tel:f.telefone, phase:f.phase, equipamento:f.equipamento, criadoEm:fmtDt(f.criadoEm), updatedAt:fmtDt(f.updatedAt) })),
      board_tv:    (tvBoard?.cards||[]).filter(match).map(c=>({ id:c.pipefyId||c.id, nome:c.nomeContato||c.title, tel:c.telefone, phaseId:c.phaseId, movedAt:fmtDt(c.movedAt), tecnico:c.tecnico })),
    };

    const total = Object.values(resultado).reduce(function(s,a){return s+a.length;},0);
    return res.status(200).json({ ok:true, busca:q, total, ...resultado });
  }

    // ── GET fix-gerusa — força Gerusa 8874 para conserto_realizado ─────────────
  if (action === 'fix-gerusa') {
    const now = new Date().toISOString();
    const resultados = [];
    for (const FL_KEY of ['reparoeletro_frenteloja','tv_frenteloja']) {
      const db = await dbGet(FL_KEY);
      if (!db || !Array.isArray(db.fichas)) { resultados.push({sistema:FL_KEY, erro:'vazio'}); continue; }
      const found = db.fichas.filter(function(f){
        const n=(f.nomeContato||f.nome||'').toLowerCase();
        const t=(f.telefone||'').replace(/[^0-9]/g,'');
        return (n.includes('gerusa')||n.includes('jerusa')) && t.slice(-4)==='8874';
      });
      if (!found.length) { resultados.push({sistema:FL_KEY, encontrado:false}); continue; }
      found.forEach(function(f){
        const antes=f.phase;
        f.phase='conserto_realizado';
        f.consertoEm=now;
        f.updatedAt=now;
        resultados.push({sistema:FL_KEY,id:f.id,nome:f.nomeContato||f.nome,antes,depois:f.phase,equipamento:f.equipamento||''});
      });
      await dbSet(FL_KEY,db);
    }
    return res.status(200).json({ok:true,resultados});
  }

    // ── GET fix-compra-lote — atualiza telefone em todas fichas sem telefone ────
  if (action === 'fix-compra-lote') {
    var pipeDb2 = (await dbGet(PIPE_KEY)) || defaultDB();
    var compraDb3 = await dbGet('reparoeletro_compra_equip') || { fichas: [] };
    var atualizados = 0, semMatch = 0;
    (compraDb3.fichas || []).forEach(function(ficha) {
      if (ficha.telefone) return; // já tem telefone
      // Buscar card no pipe por nome
      var nomeFicha = (ficha.nomeContato||'').toLowerCase().trim();
      var pipeCard = (pipeDb2.cards||[]).find(function(c) {
        return (c.nomeContato||'').toLowerCase().trim() === nomeFicha ||
               c.id === ficha.id || c.id === ficha.pipefyId ||
               c.osCode === ficha.osCode;
      });
      if (pipeCard && pipeCard.telefone) {
        ficha.telefone = pipeCard.telefone;
        atualizados++;
      } else { semMatch++; }
    });
    if (atualizados > 0) await dbSet('reparoeletro_compra_equip', compraDb3);
    return res.status(200).json({
      ok: true,
      msg: '✅ '+atualizados+' ficha(s) atualizada(s) com telefone',
      atualizados, semMatch,
      total: (compraDb3.fichas||[]).length
    });
  }

    // ── excluir ───────────────────────────────────────────────────────────────
  if (req.method === 'POST' && action === 'excluir') {
    var body = req.body || {};
    var db   = (await dbGet(PIPE_KEY)) || defaultDB();
    db.cards = (db.cards || []).filter(function(c) { return c.id !== body.id; });
    await safeWritePipe( db);
    return res.status(200).json({ ok: true });
  }


  // ── GET historico-erp ──────────────────────────────────────────────────────
  if (action === 'historico-erp') {
    const db = await dbGet(PIPE_KEY) || { cards: [] };
    const limite = new Date(Date.now() - 72*60*60*1000).toISOString();

    // Cards ATUALMENTE em ERP
    const emErp = (db.cards||[])
      .filter(c => c.phase === 'erp')
      .map(c => ({ id:c.id, pipefyId:c.pipefyId, nome:c.nomeContato||c.title||'—', movedAt:c.movedAt, equip:c.equipamento||'' }));

    // Cards que passaram pelo ERP mas já saíram (têm 'erp' no history)
    const sairamErp = (db.cards||[])
      .filter(c => c.phase !== 'erp' && (c.history||[]).some(h => h.phase === 'erp'))
      .map(c => {
        const erpH = (c.history||[]).find(h => h.phase === 'erp');
        return { id:c.id, pipefyId:c.pipefyId, nome:c.nomeContato||c.title||'—',
                 phaseAtual:c.phase, movedAt:c.movedAt, entradaErp:erpH?.ts||'—', equip:c.equipamento||'' };
      })
      .filter(c => c.movedAt > limite)
      .sort((a,b) => (b.movedAt||'') > (a.movedAt||'') ? 1 : -1);

    return res.status(200).json({ ok:true, totalErpAgora:emErp.length, emErp, sairamDoErpUlt72h:sairamErp });
  }


  // ── POST restaurar-card — insere ou restaura card no pipe ─────────────────
  if (req.method === 'POST' && action === 'restaurar-card') {
    const { id, nome, telefone, equipamento, descricao, phase, pipefyId, origem } = req.body || {};
    if (!nome || !phase) return res.status(400).json({ ok:false, error:'nome e phase obrigatórios' });
    const db = await dbGet(PIPE_KEY) || defaultDB();
    if (!Array.isArray(db.cards)) db.cards = [];
    const now = new Date().toISOString();
    // Verificar se já existe
    const existe = db.cards.find(c =>
      (id && (c.id===id||c.localId===id||c.flFichaId===id)) ||
      (pipefyId && c.pipefyId===String(pipefyId))
    );
    if (existe) {
      const oldPhase = existe.phase;
      existe.phase = phase;
      existe.movedAt = now;
      existe.history = (existe.history||[]).concat([{phase:oldPhase,ts:now}]);
      await safeWritePipe( db);
      return res.status(200).json({ ok:true, acao:'atualizado', card:existe });
    }
    const novoCard = {
      id: id || ('PIPE-REST-'+Date.now().toString(36).toUpperCase()),
      pipefyId: pipefyId || null,
      localId: id || null, flFichaId: id || null,
      phase, nomeContato: nome, telefone: telefone||'',
      equipamento: equipamento||'', descricao: descricao||'',
      origem: origem||'restaurado_manual',
      criadoEm: now, movedAt: now,
      history: [{ phase, ts: now, obs: 'restaurado_manual' }],
      aguardandoDesde: null, analiseCompra: false
    };
    db.cards.unshift(novoCard);
    await safeWritePipe( db);
    return res.status(200).json({ ok:true, acao:'inserido', card:novoCard });
  }

  // ── GET buscar-card — busca card no pipe por nome/id ──────────────────────
  if (action === 'buscar-card') {
    const q = (req.query.q||'').toLowerCase().trim();
    if (!q) return res.status(400).json({ ok:false, error:'?q= obrigatório' });
    const db = await dbGet(PIPE_KEY) || defaultDB();
    const found = (db.cards||[]).filter(c =>
      (c.nomeContato||'').toLowerCase().includes(q) ||
      (c.title||'').toLowerCase().includes(q) ||
      (c.id||'').toLowerCase().includes(q) ||
      (c.flFichaId||'').toLowerCase().includes(q)
    ).map(c => ({ id:c.id, pipefyId:c.pipefyId, nome:c.nomeContato||c.title, phase:c.phase, movedAt:c.movedAt }));
    return res.status(200).json({ ok:true, total:found.length, cards:found });
  }



  // ── GET moves-log — histórico completo de movimentos do pipe ─────────────
  if (action === 'moves-log') {
    const log = await dbGet('reparoeletro_pipe_moveslog') || { moves:[] };
    const limit = parseInt(req.query.limit)||100;
    return res.status(200).json({ ok:true, total:(log.moves||[]).length, moves:(log.moves||[]).slice(0,limit) });
  }

  // ── GET restaurar-rene — restaura René 2201 (FL-0239) para ERP ──────────────
  if (action === 'restaurar-rene') {
    const db = await dbGet(PIPE_KEY) || defaultDB();
    if (!Array.isArray(db.cards)) db.cards = [];
    const now = new Date().toISOString();
    // Verificar se já existe de alguma forma
    const existe = db.cards.find(c =>
      c.id === 'FL-0239' || c.localId === 'FL-0239' || c.flFichaId === 'FL-0239' ||
      (c.nomeContato||'').toLowerCase().includes('ren') && (c.nomeContato||'').includes('2201')
    );
    if (existe) {
      const old = existe.phase;
      existe.phase = 'erp'; existe.movedAt = now;
      existe.history = (existe.history||[]).concat([{phase:old,ts:now,obs:'restaurado'}]);
      await safeWritePipe( db);
      return res.status(200).json({ ok:true, acao:'fase_atualizada', de:old, para:'erp', card:existe });
    }
    const card = {
      id: 'PIPE-REST-RENE',
      pipefyId: null, localId: 'FL-0239', flFichaId: 'FL-0239',
      phase: 'erp',
      nomeContato: 'Renê 2201', telefone: '31997132694',
      equipamento: '', descricao: '',
      origem: 'restaurado_suporte',
      criadoEm: now, movedAt: now,
      history: [{ phase:'erp', ts:now, obs:'restaurado_suporte' }],
      aguardandoDesde: null, analiseCompra: false
    };
    db.cards.unshift(card);
    await safeWritePipe( db);
    return res.status(200).json({ ok:true, acao:'inserido_em_erp', card });
  }


  // ── POST recuperar-arquivado — restaura card do arquivo ao pipe ────────────
  if (req.method === 'POST' && action === 'recuperar-arquivado') {
    const { id, phase } = req.body || {};
    if (!id) return res.status(400).json({ ok:false, error:'id obrigatório' });
    const arch3 = await dbGet('reparoeletro_pipe_archive') || { cards:[] };
    const card3 = arch3.cards.find(c => c.id === id || c.localId === id || c.flFichaId === id);
    if (!card3) return res.status(404).json({ ok:false, error:'Card não encontrado no arquivo' });
    const db3 = await dbGet(PIPE_KEY) || defaultDB();
    const existe3 = (db3.cards||[]).find(c => c.id === card3.id);
    if (!existe3) {
      const now3 = new Date().toISOString();
      db3.cards.unshift({ ...card3, phase: phase||card3.phase, movedAt: now3,
        history: (card3.history||[]).concat([{phase:phase||card3.phase, ts:now3, obs:'recuperado_arquivo'}]),
        arquivadoEm: undefined, motivoArchive: undefined });
      await safeWritePipe(db3);
    }
    return res.status(200).json({ ok:true, card: card3, jaExistia: !!existe3 });
  }

  // ── GET listar-arquivo — lista cards no arquivo ─────────────────────────────
  if (action === 'listar-arquivo') {
    const arch4 = await dbGet('reparoeletro_pipe_archive') || { cards:[] };
    const q4 = (req.query.q||'').toLowerCase();
    const lista = q4
      ? arch4.cards.filter(c => (c.nomeContato||c.title||'').toLowerCase().includes(q4))
      : arch4.cards.slice(0,50);
    return res.status(200).json({ ok:true, total:arch4.cards.length, cards:lista });
  }


  // ── GET restaurar-graciela — insere venda Graciela Queiroz em receber ───────
  if (action === 'restaurar-graciela') {
    const db = await dbGet(PIPE_KEY) || defaultDB();
    if (!Array.isArray(db.cards)) db.cards = [];
    const existe = db.cards.find(c =>
      (c.nomeContato||'').toLowerCase().includes('graciela')
    );
    if (existe) {
      return res.status(200).json({ ok:true, acao:'ja_existe', phase:existe.phase, card:existe });
    }
    const now = new Date().toISOString();
    const card = {
      id: 'PIPE-VENDA-GRACIELA',
      pipefyId: null,
      phase: 'receber',
      nomeContato: 'Graciela Queiroz',
      telefone: '',
      equipamento: 'Consul ME18S',
      descricao: 'VENDA — MI-1938 | Consul ME18S | Pago via Pix MP',
      title: 'VENDA — MI-1938 | Graciela Queiroz',
      codEquip: 'MI-1938',
      valor: 0,
      origem: 'restaurado_venda_checkout',
      criadoEm: now, movedAt: now,
      history: [{ phase:'receber', ts:now, obs:'venda_checkout_mp_restaurado' }],
      aguardandoDesde: null, analiseCompra: false
    };
    db.cards.unshift(card);
    await safeWritePipe(db);
    return res.status(200).json({ ok:true, acao:'inserido_em_receber', card });
  }

  // ── GET dedup-pipe — remove cards duplicados ──────────────────────────────
  if (action === 'dedup-pipe') {
    const db2 = await dbGet(PIPE_KEY) || defaultDB();
    const cards = db2.cards || [];
    const vistos = new Map();
    const remover = new Set();
    const ordenados = [...cards].sort((a,b)=>new Date(a.criadoEm||0)-new Date(b.criadoEm||0));
    ordenados.forEach(function(card) {
      const nome = (card.nomeContato||'').trim().toLowerCase();
      const tel  = (card.telefone||'').replace(/\D/g,'');
      const key  = nome + '|' + tel;
      if (vistos.has(key)) {
        const primeiro = vistos.get(key);
        if (primeiro.phase === card.phase) remover.add(card.id);
      } else {
        vistos.set(key, card);
      }
    });
    if (remover.size > 0) {
      db2.cards = cards.filter(function(c){ return !remover.has(c.id); });
      await safeWritePipe(db2);
    }
    const exemplos = [...remover].slice(0,10).map(function(id){
      const c3 = cards.find(function(x){return x.id===id;});
      return c3 ? c3.nomeContato+' ('+c3.phase+')' : id;
    });
    return res.status(200).json({ ok:true, removidos: remover.size, totalAntes: cards.length, totalDepois: (db2.cards||[]).length, exemplos });
  }

  // ── GET mover-tv-log — move ficha TV logística entre fases ─────────────────
  if (action === 'mover-tv-log') {
    const tel  = (req.query.tel  || '').replace(/\D/g,'');
    const para = req.query.para  || 'liberado_para_coleta';
    const de   = req.query.de   || 'coleta_efetuada';
    if (!tel) return res.status(400).json({ ok:false, error:'tel obrigatório' });
    try {
      const TV_KEY = 'tv_logistica';
      const tvDb = (await dbGet(TV_KEY)) || { fichas:[] };
      const fichas = tvDb.fichas || [];
      const now = new Date().toISOString();
      const movidas = [];
      fichas.forEach(function(f) {
        if (f.phase !== de) return;
        const fTel = (f.telefone||'').replace(/\D/g,'');
        const match = fTel.includes(tel) || tel.includes(fTel.slice(-4)) || fTel.slice(-4)===tel;
        if (!match) return;
        f.phase   = para;
        f.movedAt = now;
        f.history = (f.history||[]).concat([{ phase:para, ts:now, via:'correcao_manual' }]);
        movidas.push({ id:f.id, nome:f.nome||f.nomeContato, tel:f.telefone });
      });
      if (movidas.length) {
        tvDb.fichas = fichas;
        await dbSet(TV_KEY, tvDb);
      }
      return res.status(200).json({ ok:true, movidas, total:movidas.length });
    } catch(e) { return res.status(200).json({ ok:false, error:e.message }); }
  }

  return res.status(404).json({ ok: false, error: 'acao nao encontrada: ' + action });
}
