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

export default async function handler(req, res) {
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

  return res.status(404).json({ ok: false, error: 'acao nao encontrada' });
}
