// api/auditoria.js — Auditoria completa + log rastreável por OS
const UPSTASH_URL   = (process.env.UPSTASH_URL   || '').replace(/['"]/g,'').trim();
const UPSTASH_TOKEN = (process.env.UPSTASH_TOKEN || '').replace(/['"]/g,'').trim();

async function dbGet(key) {
  try {
    const r = await fetch(UPSTASH_URL+'/pipeline',{method:'POST',
      headers:{Authorization:'Bearer '+UPSTASH_TOKEN,'Content-Type':'application/json'},
      body:JSON.stringify([['GET',key]])});
    const j = await r.json();
    const v = j[0]?.result;
    if (!v) return null;
    let x = JSON.parse(v);
    if (typeof x==='string') x=JSON.parse(x);
    return x;
  } catch(e) { return null; }
}

// ── Verificação de integridade de um array de fichas/records ─────────────────
function verificarIntegridade(items, nomeChave) {
  if (!Array.isArray(items)) return { ok:false, error:'não é array', count:0 };
  const ids = items.map(i => i.id||i.pipefyId||i.osCode||'?');
  const dupes = ids.filter((id,i) => ids.indexOf(id) !== i && id !== '?');
  const semId  = items.filter(i => !i.id && !i.pipefyId && !i.osCode).length;
  const semFase = items.filter(i => !i.phase && !i.phaseId && !i.status).length;
  return {
    ok: dupes.length===0 && semId===0,
    count: items.length,
    duplicatas: [...new Set(dupes)].slice(0,5),
    semId,
    semFase,
    ultimo: items[0]?.criadoEm || items[0]?.ts || items[0]?.createdAt || null,
  };
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin','*');
  if (req.method==='OPTIONS') return res.status(200).end();
  const action = req.query.action||'';

  // ── GET health-check — varredura completa do banco ───────────────────────────
  if (action==='health-check') {
    const CHAVES = [
      // ADM
      { key:'reparoeletro_board',      campo:'cards',   label:'ADM Board' },
      { key:'reparoeletro_pipe',        campo:'cards',   label:'ADM Pipe' },
      { key:'reparoeletro_financeiro',  campo:'records', label:'ADM Financeiro' },
      { key:'reparoeletro_frenteloja',  campo:'fichas',  label:'ADM Frente de Loja' },
      { key:'reparoeletro_logistica',   campo:'fichas',  label:'ADM Logística' },
      { key:'reparoeletro_orc',         campo:'fichas',  label:'ADM Orçamentos' },
      { key:'reparoeletro_balcao',      campo:'fichas',  label:'ADM Balcão' },
      { key:'reparoeletro_vendas',      campo:'vendas',  label:'ADM Vendas' },
      // TV
      { key:'tv_logistica',             campo:'fichas',  label:'TV Logística' },
      { key:'tv_orcamentos',            campo:'fichas',  label:'TV Orçamentos' },
      { key:'tv_financeiro',            campo:'records', label:'TV Financeiro' },
      { key:'tv_frenteloja',            campo:'fichas',  label:'TV Frente de Loja' },
      { key:'tv_pipe',                  campo:'cards',   label:'TV Pipe' },
      { key:'tv_balcao',                campo:'fichas',  label:'TV Balcão' },
    ];
    const LOG_CHAVES = [
      { key:'reparoeletro_log',           label:'Log ADM (unificado)' },
      { key:'tv_log',                     label:'Log TV (unificado)' },
      { key:'reparoeletro_pipe_moveslog', label:'Moves Log ADM Pipe' },
      { key:'mp_webhook_log',             label:'Webhook MP (pagamentos)' },
      { key:'tv_pipe_moveslog',           label:'Moves Log TV Pipe' },
    ];

    const resultados = [];
    let totalOk=0, totalErro=0, totalRegistros=0;

    for (const c of CHAVES) {
      try {
        const db = await dbGet(c.key);
        if (!db) { resultados.push({chave:c.key,label:c.label,status:'VAZIO',count:0,detalhes:{}}); continue; }
        const items = db[c.campo] || db.fichas || db.cards || db.records || (Array.isArray(db)?db:[]);
        const v = verificarIntegridade(items, c.key);
        const status = !v.ok ? 'ALERTA' : 'OK';
        if (status==='OK') totalOk++; else totalErro++;
        totalRegistros += v.count;
        resultados.push({ chave:c.key, label:c.label, status, ...v });
      } catch(e) {
        totalErro++;
        resultados.push({ chave:c.key, label:c.label, status:'ERRO', error:e.message, count:0 });
      }
    }

    const logs = [];
    for (const l of LOG_CHAVES) {
      try {
        const raw = await dbGet(l.key);
        const entries = Array.isArray(raw) ? raw : (raw?.moves||[]);
        logs.push({ chave:l.key, label:l.label, entradas:entries.length, ultimo:entries[0]?.ts||null });
      } catch(e) {
        logs.push({ chave:l.key, label:l.label, entradas:0, erro:e.message });
      }
    }

    // Verificar backups
    const backupIdx = await dbGet('reparoeletro_backup_index');
    const tvBackupIdx = await dbGet('tv_backup_index');

    return res.status(200).json({
      ok: true,
      auditadoEm: new Date().toISOString(),
      resumo: { chaveVerificadas:CHAVES.length, ok:totalOk, alertas:totalErro, totalRegistros },
      chaves: resultados,
      logs,
      backups: {
        adm: backupIdx ? { total:backupIdx.total||'?', ultimo:backupIdx.ultimo||null } : null,
        tv:  tvBackupIdx ? { total:tvBackupIdx.total||'?', ultimo:tvBackupIdx.ultimo||null } : null,
      },
      coberturaLog: {
        comLog: ['board','financeiro','logistica','frenteloja','pipe','tv-logistica','tv-frenteloja','tv-pipe'],
        semLog: ['tv-orcamento','tv-financeiro','tv-webhook-mp','webhook-mp','compras','vendas','garantia'],
        nota: 'Módulos sem log não registram movimentações de OS',
      },
    });
  }

  // ── GET buscar-os — rastreia tudo que aconteceu com uma OS ──────────────────
  if (action==='buscar-os') {
    const q = (req.query.q||'').toLowerCase().trim();
    if (!q) return res.status(400).json({ ok:false, error:'Informe ?q=nome_ou_id_ou_tel' });

    const FONTES = [
      { key:'reparoeletro_log',         tipo:'log',    campo:null },
      { key:'tv_log',                    tipo:'log',    campo:null },
      { key:'mp_webhook_log',            tipo:'wh_log', campo:null },
      { key:'reparoeletro_pipe_moveslog',tipo:'moves',  campo:'moves' },
      { key:'reparoeletro_board',        campo:'cards',  tipo:'db' },
      { key:'reparoeletro_financeiro',   campo:'records',tipo:'db' },
      { key:'tv_logistica',              campo:'fichas', tipo:'db' },
      { key:'tv_orcamentos',             campo:'fichas', tipo:'db' },
      { key:'tv_financeiro',             campo:'records',tipo:'db' },
    ];

    const timeline = [];

    for (const f of FONTES) {
      try {
        const db = await dbGet(f.key);
        if (!db) continue;

        if (f.tipo==='wh_log') {
          const entries = Array.isArray(db) ? db : [];
          entries.forEach(e => {
            const match = JSON.stringify(e).toLowerCase();
            if (match.includes(q)) {
              timeline.push({ ts:e.ts, fonte:f.key, tipo:'pagamento', acao:e.action||e.type||e.evento||'mp_event', fichaId:e.fichaId||e.external_reference||e.osCode||'', ficha:e.nome||e.clienteNome||'', para:e.status||e.para||'', detalhe:e.paymentId||e.payment_id||JSON.stringify(e).slice(0,100) });
            }
          });
        } else if (f.tipo==='log') {
          const entries = Array.isArray(db) ? db : [];
          entries.forEach(e => {
            const match = [e.fichaId,e.ficha,e.detalhe].join(' ').toLowerCase();
            if (match.includes(q)) {
              timeline.push({ ts:e.ts, fonte:f.key, tipo:'log', modulo:e.modulo, acao:e.acao, fichaId:e.fichaId, ficha:e.ficha, de:e.de, para:e.para, gatilho:e.gatilho, status:e.status, detalhe:e.detalhe });
            }
          });
                } else if (f.tipo==='moves') {
          const moves = (db.moves||db||[]);
          moves.forEach(m => {
            const match = [m.fichaId||'',m.nome||'',m.pipefyId||'',m.id||''].join(' ').toLowerCase();
            if (match.includes(q)) {
              timeline.push({ ts:m.ts||m.timestamp, fonte:f.key, tipo:'move', de:m.de, para:m.para||m.phaseId, fichaId:m.fichaId||m.pipefyId, ficha:m.nome||m.ficha, detalhe:m.detalhe||'' });
            }
          });
        } else {
          const items = db[f.campo]||[];
          items.forEach(item => {
            const match = JSON.stringify(item).toLowerCase();
            if (match.includes(q)) {
              const hist = item.history||item.movesLog||[];
              hist.forEach(h => {
                timeline.push({ ts:h.ts||h.movedAt, fonte:f.key, tipo:'historico', fichaId:item.id||item.pipefyId, ficha:item.nome||item.nomeContato||item.clienteNome, de:h.phase||h.de, para:h.para||'', detalhe:h.gatilho||'' });
              });
              // Registro atual
              timeline.push({ ts:item.movedAt||item.criadoEm||item.updatedAt, fonte:f.key, tipo:'estado_atual', fichaId:item.id||item.pipefyId, ficha:item.nome||item.nomeContato||item.clienteNome, para:item.phase||item.phaseId||item.status, detalhe:'Estado atual' });
            }
          });
        }
      } catch(e) { /* continua */ }
    }

    // Ordenar por timestamp
    timeline.sort((a,b) => (new Date(b.ts||0))-(new Date(a.ts||0)));

    return res.status(200).json({ ok:true, query:q, total:timeline.length, timeline: timeline.slice(0,200) });
  }

  // ── GET log — lista log com filtros (busca por OS, módulo, status) ──────────
  if (action==='log') {
    const modulo  = req.query.modulo||'';
    const status  = req.query.status||'';
    const fichaId = (req.query.fichaId||req.query.os||'').toLowerCase();
    const limit   = Math.min(parseInt(req.query.limit||'300'),500);
    const sistema = req.query.sistema||'todos'; // 'adm','tv','todos'

    let entries = [];
    if (sistema==='adm'||sistema==='todos') {
      const a = await dbGet('reparoeletro_log')||[];
      if (Array.isArray(a)) entries.push(...a.map(e=>({...e,_sistema:'ADM'})));
    }
    if (sistema==='tv'||sistema==='todos') {
      const t = await dbGet('tv_log')||[];
      if (Array.isArray(t)) entries.push(...t.map(e=>({...e,_sistema:'TV'})));
    }

    // Filtros
    if (modulo)  entries = entries.filter(e=>(e.modulo||'').toLowerCase().includes(modulo.toLowerCase()));
    if (status)  entries = entries.filter(e=>e.status===status);
    if (fichaId) entries = entries.filter(e=>(e.fichaId||'').toLowerCase().includes(fichaId)||(e.ficha||'').toLowerCase().includes(fichaId));

    // Ordenar por ts desc
    entries.sort((a,b)=>new Date(b.ts||0)-new Date(a.ts||0));

    return res.status(200).json({ ok:true, total:entries.length, entries:entries.slice(0,limit) });
  }

  return res.status(404).json({ ok:false, error:'action não encontrada — use: health-check | buscar-os | log' });
};
