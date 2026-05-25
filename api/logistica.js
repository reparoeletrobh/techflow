// api/logistica.js — Sistema de Logística de Coleta
const U = process.env.UPSTASH_URL;
const T = process.env.UPSTASH_TOKEN;
const LOG_KEY = 'reparoeletro_logistica';

async function dbGet(key) {
  try {
    const r = await fetch(`${U}/get/${key}`, { headers: { Authorization: `Bearer ${T}` } });
    const j = await r.json();
    return j.result ? JSON.parse(j.result) : null;
  } catch(e) { return null; }
}
async function dbSet(key, val) {
  try {
    await fetch(`${U}/set/${key}`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${T}`, 'Content-Type': 'application/json' },
      body: JSON.stringify(val)
    });
    return true;
  } catch(e) { return false; }
}

function defaultDB() { return { fichas: [], nextId: 1 }; }

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const action = req.query.action;

  // ── GET load ──────────────────────────────────────────────
  if (action === 'load') {
    const db = await dbGet(LOG_KEY) || defaultDB();
    return res.status(200).json({ ok: true, fichas: db.fichas || [] });
  }

  // ── POST criar ────────────────────────────────────────────
  if (req.method === 'POST' && action === 'criar') {
    const { nome, telefone, endereco, equipamento, defeito, pipefyCardId, texto } = req.body || {};
    if (!nome) return res.status(400).json({ ok: false, error: 'nome obrigatorio' });

    const db = await dbGet(LOG_KEY) || defaultDB();
    const id = 'LOG-' + String(db.nextId || 1).padStart(4, '0');
    const ficha = {
      id, nome, telefone: telefone || '', endereco: endereco || '',
      equipamento: equipamento || '', defeito: defeito || '',
      pipefyCardId: pipefyCardId || null, texto: texto || '',
      phase: 'liberado_coleta',
      criadoEm: new Date().toISOString(),
      movedAt: new Date().toISOString(),
      diagnostico: null,
    };
    db.fichas.unshift(ficha);
    db.nextId = (db.nextId || 1) + 1;
    await dbSet(LOG_KEY, db);
    return res.status(201).json({ ok: true, ficha });
  }

  // ── POST mover ────────────────────────────────────────────
  if (req.method === 'POST' && action === 'mover') {
    const { id, phase } = req.body || {};
    const PHASES = ['liberado_coleta','em_rota','remarcar','coleta_efetuada','orc_registrado'];
    if (!id || !PHASES.includes(phase)) return res.status(400).json({ ok: false, error: 'invalido' });

    const db = await dbGet(LOG_KEY) || defaultDB();
    const ficha = db.fichas.find(f => f.id === id);
    if (!ficha) return res.status(404).json({ ok: false, error: 'nao encontrada' });
    ficha.phase = phase;
    ficha.movedAt = new Date().toISOString();
    await dbSet(LOG_KEY, db);
    return res.status(200).json({ ok: true, ficha });
  }

  // ── POST atualizar-dados ──────────────────────────────────
  if (req.method === 'POST' && action === 'atualizar-dados') {
    const { id, nome, telefone, endereco, equipamento, defeito } = req.body || {};
    const db = await dbGet(LOG_KEY) || defaultDB();
    const ficha = db.fichas.find(f => f.id === id);
    if (!ficha) return res.status(404).json({ ok: false, error: 'nao encontrada' });
    if (nome)       ficha.nome = nome;
    if (telefone)   ficha.telefone = telefone;
    if (endereco)   ficha.endereco = endereco;
    if (equipamento) ficha.equipamento = equipamento;
    if (defeito)    ficha.defeito = defeito;
    await dbSet(LOG_KEY, db);
    return res.status(200).json({ ok: true, ficha });
  }

  // ── POST salvar-diagnostico ───────────────────────────────
  if (req.method === 'POST' && action === 'salvar-diagnostico') {
    const { id, diagnostico } = req.body || {};
    const db = await dbGet(LOG_KEY) || defaultDB();
    const ficha = db.fichas.find(f => f.id === id);
    if (!ficha) return res.status(404).json({ ok: false, error: 'nao encontrada' });
    ficha.diagnostico = diagnostico;
    ficha.phase = 'orc_registrado';
    ficha.movedAt = new Date().toISOString();
    await dbSet(LOG_KEY, db);
    return res.status(200).json({ ok: true, ficha });
  }

  // ── POST cancelar ────────────────────────────────────────
  if (req.method === 'POST' && action === 'cancelar') {
    const { id } = req.body || {};
    const db = await dbGet(LOG_KEY) || defaultDB();
    db.fichas = db.fichas.filter(f => f.id !== id);
    await dbSet(LOG_KEY, db);
    return res.status(200).json({ ok: true });
  }

  return res.status(404).json({ ok: false, error: 'ação não encontrada' });
};
