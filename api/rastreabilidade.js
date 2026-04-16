// api/rastreabilidade.js — Rastreabilidade de OS, Peças e Compras
const UPSTASH_URL = (process.env.UPSTASH_URL || '').replace(/['"]/g, '').trim();
const UPSTASH_TOKEN = (process.env.UPSTASH_TOKEN || '').replace(/['"]/g, '').trim();
const KEY = 'reparoeletro_rastreabilidade';

async function dbGet(key) {
  try {
    const r = await fetch(`${UPSTASH_URL}/pipeline`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${UPSTASH_TOKEN}`, 'Content-Type': 'application/json' },
      body: JSON.stringify([['GET', key]])
    });
    const j = await r.json();
    if (!j[0] || !j[0].result) return null;
    var parsed = JSON.parse(j[0].result);
    if (typeof parsed === 'string') parsed = JSON.parse(parsed);
    return (parsed && typeof parsed === 'object') ? parsed : null;
  } catch (e) { return null; }
}

async function dbSet(key, val) {
  await fetch(`${UPSTASH_URL}/pipeline`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${UPSTASH_TOKEN}`, 'Content-Type': 'application/json' },
    body: JSON.stringify([['SET', key, JSON.stringify(val)]])
  });
}

function uid() { return Date.now().toString(36) + Math.random().toString(36).slice(2, 6); }

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const action = req.query.action || '';

  try {
    let db = await dbGet(KEY) || { itens: [] };
    if (!Array.isArray(db.itens)) db.itens = [];

    // GET load — retorna todos os registros ordenados do mais recente
    if (action === 'load') {
      const sorted = db.itens.slice().sort((a, b) => new Date(b.criadoEm) - new Date(a.criadoEm));
      return res.status(200).json({ ok: true, itens: sorted });
    }

    // POST add — adiciona novo registro
    if (req.method === 'POST' && action === 'add') {
      const { ref, tipo, nota } = req.body || {};
      if (!nota) return res.status(200).json({ ok: false, error: 'nota obrigatória' });
      const item = {
        id: uid(),
        ref: ref || '',
        tipo: tipo || 'geral', // os | peca | compra | geral
        nota,
        criadoEm: new Date().toISOString()
      };
      db.itens.unshift(item);
      // Mantém no máximo 500 registros
      if (db.itens.length > 500) db.itens = db.itens.slice(0, 500);
      await dbSet(KEY, db);
      return res.status(200).json({ ok: true, item });
    }

    // POST del — remove registro pelo id
    if (req.method === 'POST' && action === 'del') {
      const { id } = req.body || {};
      db.itens = db.itens.filter(i => i.id !== id);
      await dbSet(KEY, db);
      return res.status(200).json({ ok: true });
    }

    return res.status(200).json({ ok: false, error: 'Ação não encontrada' });

  } catch (e) {
    return res.status(200).json({ ok: false, error: e.message });
  }
};
