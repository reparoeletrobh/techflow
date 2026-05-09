// api/tv-checkout.js — Gestão de Checkout VSL
const UPSTASH_URL   = process.env.UPSTASH_URL;
const UPSTASH_TOKEN = process.env.UPSTASH_TOKEN;
const CFG_KEY       = 'tv_checkout_config';
const VENDAS_KEY    = 'tv_checkout_vendas';

async function dbGet(key) {
  const r = await fetch(`${UPSTASH_URL}/pipeline`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` },
    body: JSON.stringify([['GET', key]])
  });
  const j = await r.json();
  return j[0]?.result ? JSON.parse(j[0].result) : null;
}
async function dbSet(key, val) {
  await fetch(`${UPSTASH_URL}/pipeline`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` },
    body: JSON.stringify([['SET', key, JSON.stringify(val)]])
  });
}

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const action = req.query.action || '';

  // ── GET load-config ────────────────────────────────────────────
  if (action === 'load-config') {
    const cfg = (await dbGet(CFG_KEY)) || {};
    return res.status(200).json({ ok: true, config: cfg });
  }

  // ── POST save-config ───────────────────────────────────────────
  if (req.method === 'POST' && action === 'save-config') {
    const body = req.body || {};
    const cfg  = (await dbGet(CFG_KEY)) || {};
    if (body.videoHtml   !== undefined) cfg.videoHtml   = body.videoHtml;
    if (body.pagamento   !== undefined) cfg.pagamento   = body.pagamento;
    if (body.destaques   !== undefined) cfg.destaques   = body.destaques;
    cfg.updatedAt = new Date().toISOString();
    await dbSet(CFG_KEY, cfg);
    return res.status(200).json({ ok: true });
  }

  // ── POST set-destaque ──────────────────────────────────────────
  if (req.method === 'POST' && action === 'set-destaque') {
    const { id, desconto, badge, prioridade, ativo } = req.body || {};
    if (!id) return res.status(400).json({ ok: false, error: 'id obrigatorio' });
    const cfg = (await dbGet(CFG_KEY)) || {};
    if (!cfg.destaques) cfg.destaques = {};
    if (ativo === false) {
      delete cfg.destaques[id];
    } else {
      cfg.destaques[id] = {
        desconto:   parseFloat(desconto) || 0,
        badge:      badge || '',
        prioridade: parseInt(prioridade) || 0,
        ativo:      true
      };
    }
    cfg.updatedAt = new Date().toISOString();
    await dbSet(CFG_KEY, cfg);
    return res.status(200).json({ ok: true });
  }

  // ── GET load-equipamentos ──────────────────────────────────────
  if (action === 'load-equipamentos') {
    const proto  = req.headers['x-forwarded-proto'] || 'https';
    const host   = req.headers.host;
    const d      = await fetch(`${proto}://${host}/api/vendas?action=load`).then(r => r.json());
    const prods  = (d.produtos || []).filter(p => !p.vendido);
    const cfg    = (await dbGet(CFG_KEY)) || {};
    return res.status(200).json({
      ok: true,
      produtos: prods.map(p => ({ ...p, _destaque: cfg.destaques?.[p.id] || null }))
    });
  }

  // ── POST registrar-venda ───────────────────────────────────────
  if (req.method === 'POST' && action === 'registrar-venda') {
    const { produto, comprador, valor, provedor } = req.body || {};
    if (!produto?.id) return res.status(400).json({ ok: false, error: 'produto obrigatorio' });
    const db = (await dbGet(VENDAS_KEY)) || { vendas: [] };
    db.vendas.unshift({
      id:        Date.now().toString(36),
      produto, comprador, valor,
      provedor:  provedor || 'whatsapp',
      criadoEm:  new Date().toISOString()
    });
    await dbSet(VENDAS_KEY, db);
    return res.status(200).json({ ok: true });
  }

  // ── GET load-vendas ────────────────────────────────────────────
  if (action === 'load-vendas') {
    const db    = (await dbGet(VENDAS_KEY)) || { vendas: [] };
    const vendas = db.vendas || [];
    const total  = vendas.reduce((s, v) => s + (parseFloat(v.valor) || 0), 0);
    return res.status(200).json({ ok: true, vendas, total, count: vendas.length });
  }

  return res.status(404).json({ ok: false, error: 'Ação não encontrada' });
}
