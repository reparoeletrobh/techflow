// api/log.js — Log central do sistema
const UPSTASH_URL   = (process.env.UPSTASH_URL   || '').replace(/['"]/g,'').trim();
const UPSTASH_TOKEN = (process.env.UPSTASH_TOKEN || '').replace(/['"]/g,'').trim();
const LOG_KEY       = 'reparoeletro_log';
const MAX_ENTRIES   = 500;

async function logGet() {
  try {
    const r = await fetch(UPSTASH_URL + '/pipeline', {
      method: 'POST',
      headers: { Authorization: 'Bearer ' + UPSTASH_TOKEN, 'Content-Type': 'application/json' },
      body: JSON.stringify([['GET', LOG_KEY]])
    });
    const j = await r.json();
    const v = j[0]?.result;
    if (!v) return [];
    let val = JSON.parse(v);
    if (typeof val === 'string') { try { val = JSON.parse(val); } catch(e) {} }
    return Array.isArray(val) ? val : [];
  } catch(e) { return []; }
}

async function logSet(entries) {
  await fetch(UPSTASH_URL + '/pipeline', {
    method: 'POST',
    headers: { Authorization: 'Bearer ' + UPSTASH_TOKEN, 'Content-Type': 'application/json' },
    body: JSON.stringify([['SET', LOG_KEY, JSON.stringify(entries)]])
  });
}

module.exports = async function handler(req, res) {
  // 🔐 TF-AUTH (Fase 1): chave obrigatória em toda chamada
  const _tfk = (req.query && req.query.k) || req.headers['x-tf-key'] || '';
  if (_tfk !== ((process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim())) {
    return res.status(401).json({ ok: false, error: 'não autorizado' });
  }

  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const action = req.query.action || '';

  // ── POST add — adiciona entrada no log ──────────────────────────────────
  if (req.method === 'POST' && action === 'add') {
    const body   = req.body || {};
    const entry  = {
      ts:      new Date().toISOString(),
      modulo:  body.modulo  || '—',
      fichaId: body.fichaId || '',
      ficha:   body.ficha   || '',
      acao:    body.acao    || '',
      de:      body.de      || '',
      para:    body.para    || '',
      gatilho: body.gatilho || '',
      status:  body.status  || 'ok',
      detalhe: body.detalhe || ''
    };
    try {
      const log = await logGet();
      log.unshift(entry);
      if (log.length > MAX_ENTRIES) log.splice(MAX_ENTRIES);
      await logSet(log);
      return res.status(200).json({ ok: true });
    } catch(e) {
      return res.status(500).json({ ok: false, error: e.message });
    }
  }

  // ── GET list — lista entradas do log ────────────────────────────────────
  if (action === 'list') {
    const modulo = req.query.modulo || '';
    const status = req.query.status || '';
    const limit  = parseInt(req.query.limit || '200');
    let log = await logGet();
    if (modulo) log = log.filter(e => e.modulo === modulo);
    if (status) log = log.filter(e => e.status === status);
    return res.status(200).json({ ok: true, total: log.length, entries: log.slice(0, limit) });
  }

  // ── GET clear — limpa o log ──────────────────────────────────────────────
  if (action === 'clear') {
    await logSet([]);
    return res.status(200).json({ ok: true, info: 'Log limpo' });
  }


  // ── GET seed-log: popula com 10 entradas de exemplo ─────────────────────
  if (action === 'seed') {
    const now = Date.now();
    // Adicionar só se log estiver vazio ou com menos de 5 entradas
    const logAtual = await logGet();
    if (logAtual.length >= 5) return res.status(200).json({ ok: true, info: 'log ja tem entradas', total: logAtual.length });
    const seeds = [
      { ts: new Date(now - 1*60*1000).toISOString(),  modulo:'Pipe ADM',     fichaId:'PIPE-0018', ficha:'Ernesto 1212',     acao:'Mover ficha',             de:'aguardando_aprovacao', para:'aprovados',            gatilho:'→ Board Técnico (producao)', status:'ok',   detalhe:'Card criado em Produção no Board' },
      { ts: new Date(now - 5*60*1000).toISOString(),  modulo:'Pipe ADM',     fichaId:'PIPE-0018', ficha:'Ernesto 1212',     acao:'Mover ficha',             de:'',                     para:'aguardando_aprovacao', gatilho:'Timer 48h iniciado',         status:'ok',   detalhe:'' },
      { ts: new Date(now - 12*60*1000).toISOString(), modulo:'Balcão',       fichaId:'1358378287',ficha:'Miguel 1599',      acao:'Confirmar pagamento',     de:'',                     para:'erp',                  gatilho:'→ Pipe ERP + Pipefy ERP',    status:'ok',   detalhe:'' },
      { ts: new Date(now - 18*60*1000).toISOString(), modulo:'Frente de Loja',fichaId:'FL-0207',  ficha:'Miguel 1599',      acao:'Liberar equipamento',     de:'producao',             para:'receber',              gatilho:'→ Pipe receber + Pipefy Receber$', status:'ok', detalhe:'Valor: R$240 pix' },
      { ts: new Date(now - 25*60*1000).toISOString(), modulo:'Financeiro',   fichaId:'FL-0205',   ficha:'Cláudio 5813',     acao:'Confirmar pagamento',     de:'faturamento',          para:'solicitar_entrega',    gatilho:'→ Pipe solicitar_entrega + Pipefy Solicitar Entrega', status:'ok', detalhe:'Valor: R$300' },
      { ts: new Date(now - 34*60*1000).toISOString(), modulo:'Pipe ADM',     fichaId:'PIPE-0022', ficha:'Beatriz 1911',     acao:'Mover ficha',             de:'solicitar_entrega',    para:'entrega_solicitada',   gatilho:'',                           status:'ok',   detalhe:'' },
      { ts: new Date(now - 41*60*1000).toISOString(), modulo:'Orçamento',    fichaId:'1357550324',ficha:'Maria Aparecida',  acao:'Aprovação de orçamento',  de:'',                     para:'aprovados',            gatilho:'→ Pipe aguardando_aprovacao', status:'ok',   detalhe:'Valor: R$350' },
      { ts: new Date(now - 53*60*1000).toISOString(), modulo:'Logística',    fichaId:'LOG-0089',  ficha:'Glaydson 2737',    acao:'Gerar orçamento',         de:'',                     para:'aguardando_aprovacao', gatilho:'→ Pipe aguardando_aprovacao + Orçamento', status:'ok', detalhe:'Valor: R$490' },
      { ts: new Date(now - 67*60*1000).toISOString(), modulo:'Pipe ADM',     fichaId:'PIPE-0021', ficha:'Ariane 0410',      acao:'Mover ficha',             de:'programar_entrega',    para:'solicitar_entrega',    gatilho:'',                           status:'ok',   detalhe:'' },
      { ts: new Date(now - 82*60*1000).toISOString(), modulo:'Vendas',       fichaId:'',          ficha:'Yuri José',        acao:'Registrar venda',         de:'',                     para:'receber',              gatilho:'→ Pipe receber',             status:'ok',   detalhe:'ELECTROLUX ms37r R$350' },
    ];
    const log = await logGet();
    seeds.forEach(function(s){ log.unshift(s); });
    if (log.length > MAX_ENTRIES) log.splice(MAX_ENTRIES);
    await logSet(log);
    return res.status(200).json({ ok: true, inseridas: seeds.length, total: log.length });
  }

  return res.status(404).json({ ok: false, error: 'acao nao encontrada' });
}
