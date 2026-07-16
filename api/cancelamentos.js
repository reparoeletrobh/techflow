// api/cancelamentos.js — Reembolsos de corridas canceladas (Lalamove)
const U = (process.env.UPSTASH_URL || '').replace(/['"]/g, '').trim();
const T = (process.env.UPSTASH_TOKEN || '').replace(/[\n\r'"]/g, '').trim();
const KEY = 'cancelamentos_lalamove';

async function dbGet(key) {
  try {
    const r = await fetch(`${U}/get/${key}`, { headers: { Authorization: `Bearer ${T}` } });
    const j = await r.json();
    let v = j.result;
    if (typeof v === 'string') v = JSON.parse(v);
    if (typeof v === 'string') v = JSON.parse(v);
    return v;
  } catch { return null; }
}
async function dbSet(key, val) {
  await fetch(`${U}/set/${key}`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${T}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(val),
  });
}

export default async function handler(req, res) {
  res.setHeader('Cache-Control', 'no-cache');
  const action = req.query.action || '';

  if (action === 'load') {
    const db = (await dbGet(KEY)) || { itens: [] };
    const itens = db.itens || [];
    // KPIs
    const agoraBRT = new Date(Date.now() - 3 * 3600000);
    const iniMes = new Date(Date.UTC(agoraBRT.getUTCFullYear(), agoraBRT.getUTCMonth(), 1) + 3 * 3600000).toISOString();
    const pend = itens.filter(i => i.status === 'pendente');
    const conf = itens.filter(i => i.status === 'confirmado');
    const kpi = {
      pendentes: pend.length,
      pendentesValor: +pend.reduce((s, i) => s + (parseFloat(i.valor) || 0), 0).toFixed(2),
      recebidos: conf.length,
      recebidosValor: +conf.reduce((s, i) => s + (parseFloat(i.valor) || 0), 0).toFixed(2),
      recebidosMes: conf.filter(i => (i.confirmadoEm || '') >= iniMes).length,
      recebidosMesValor: +conf.filter(i => (i.confirmadoEm || '') >= iniMes)
        .reduce((s, i) => s + (parseFloat(i.valor) || 0), 0).toFixed(2),
    };
    return res.status(200).json({ ok: true, kpi, itens: itens.slice(0, 300) });
  }

  if (req.method === 'POST' && action === 'criar') {
    const { codigo, motorista, oss, valor, textoOriginal } = req.body || {};
    if (!codigo) return res.status(400).json({ ok: false, error: 'Código da corrida obrigatório' });
    const db = (await dbGet(KEY)) || { itens: [] };
    if (!Array.isArray(db.itens)) db.itens = [];
    // Idempotência por código da corrida
    const ja = db.itens.find(i => String(i.codigo).trim() === String(codigo).trim() && i.status === 'pendente');
    if (ja) return res.status(200).json({ ok: true, item: ja, duplicataEvitada: true });
    const item = {
      id: 'canc_' + Date.now().toString(36),
      codigo: String(codigo).trim().slice(0, 60),
      motorista: String(motorista || '').trim().slice(0, 60),
      oss: String(oss || '').trim().slice(0, 200),
      valor: parseFloat(String(valor || '0').replace(/\./g, '').replace(',', '.')) || parseFloat(valor) || 0,
      textoOriginal: String(textoOriginal || '').slice(0, 1500),
      status: 'pendente',
      criadoEm: new Date().toISOString(),
      confirmadoEm: null,
    };
    db.itens.unshift(item);
    if (db.itens.length > 1000) db.itens = db.itens.slice(0, 1000);
    await dbSet(KEY, db);
    return res.status(200).json({ ok: true, item });
  }

  if (req.method === 'POST' && action === 'confirmar') {
    const { id } = req.body || {};
    const db = (await dbGet(KEY)) || { itens: [] };
    const item = (db.itens || []).find(i => i.id === id);
    if (!item) return res.status(404).json({ ok: false, error: 'Não encontrado' });
    item.status = 'confirmado';
    item.confirmadoEm = new Date().toISOString();
    await dbSet(KEY, db);
    return res.status(200).json({ ok: true, item });
  }

  if (req.method === 'POST' && action === 'excluir') {
    const { id } = req.body || {};
    const db = (await dbGet(KEY)) || { itens: [] };
    db.itens = (db.itens || []).filter(i => i.id !== id);
    await dbSet(KEY, db);
    return res.status(200).json({ ok: true });
  }

  return res.status(404).json({ ok: false, error: 'Ação não encontrada' });
}
