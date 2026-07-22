// ═══ CONTROLE DE QUALIDADE — API (beta) ═══
// Fila de inspeção + checklists + aprovação/reprovação + técnicos + certificado

const U = (process.env.UPSTASH_URL || '').replace(/['"]/g, '').trim();
const T = (process.env.UPSTASH_TOKEN || '').replace(/[\n\r'"]/g, '').trim();
const KEY = 'reparoeletro_qualidade';

async function dbGet(k) {
  try {
    const r = await fetch(`${U}/get/${k}`, { headers: { Authorization: `Bearer ${T}` } });
    const j = await r.json();
    return j.result ? JSON.parse(j.result) : null;
  } catch (e) { return null; }
}
async function dbSet(k, v) {
  const r = await fetch(`${U}/set/${k}`, {
    method: 'POST', headers: { Authorization: `Bearer ${T}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(v),
  });
  return (await r.json()).result === 'OK';
}

function defaultDB() {
  return {
    inspecoes: [],
    config: { tecnicos: [], proximoNum: 1 },
  };
}

export default async function handler(req, res) {
  // 🔐 TF-AUTH (Fase 1): chave obrigatória em toda chamada
  const _tfk = (req.query && req.query.k) || req.headers['x-tf-key'] || '';
  if (_tfk !== ((process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim())) {
    return res.status(401).json({ ok: false, error: 'não autorizado' });
  }
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type,x-tf-key');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const action = req.query.action || '';
  let db = (await dbGet(KEY)) || defaultDB();
  if (!Array.isArray(db.inspecoes)) db.inspecoes = [];
  if (!db.config) db.config = { tecnicos: [], proximoNum: 1 };

  // ── LOAD ──
  if (action === 'load') {
    return res.status(200).json({ ok: true, inspecoes: db.inspecoes, config: db.config });
  }

  // ── CRIAR inspeção (entrada manual na beta; depois virá do técnico) ──
  if (req.method === 'POST' && action === 'criar') {
    const { cliente, tel, os, equipamento, equipDesc, tecnico } = req.body || {};
    if (!cliente || !equipamento) return res.status(400).json({ ok: false, error: 'cliente e equipamento obrigatórios' });
    const num = db.config.proximoNum || 1;
    const insp = {
      id: 'QC-' + String(num).padStart(4, '0'),
      criadoEm: new Date().toISOString(),
      cliente: String(cliente).trim(),
      tel: String(tel || '').trim(),
      os: String(os || '').trim(),
      equipamento: String(equipamento).trim(),
      equipDesc: String(equipDesc || '').trim(),
      tecnico: String(tecnico || '').trim(),
      inspetor: '',
      status: 'aguardando',
      checklist: {},
      reprovacoes: [],
      aprovadoEm: null,
    };
    db.config.proximoNum = num + 1;
    db.inspecoes.unshift(insp);
    await dbSet(KEY, db);
    return res.status(200).json({ ok: true, inspecao: insp });
  }

  // ── SALVAR checklist parcial (auto-save durante a inspeção) ──
  if (req.method === 'POST' && action === 'salvar-checklist') {
    const { id, checklist, inspetor } = req.body || {};
    const insp = db.inspecoes.find(i => i.id === id);
    if (!insp) return res.status(404).json({ ok: false, error: 'inspeção não encontrada' });
    if (checklist) insp.checklist = checklist;
    if (inspetor !== undefined) insp.inspetor = String(inspetor);
    await dbSet(KEY, db);
    return res.status(200).json({ ok: true });
  }

  // ── APROVAR ──
  if (req.method === 'POST' && action === 'aprovar') {
    const { id, checklist, inspetor } = req.body || {};
    const insp = db.inspecoes.find(i => i.id === id);
    if (!insp) return res.status(404).json({ ok: false, error: 'inspeção não encontrada' });
    if (checklist) insp.checklist = checklist;
    if (inspetor) insp.inspetor = String(inspetor);
    insp.status = 'aprovado';
    insp.aprovadoEm = new Date().toISOString();
    await dbSet(KEY, db);
    return res.status(200).json({ ok: true, inspecao: insp });
  }

  // ── REPROVAR (registra retrabalho) ──
  if (req.method === 'POST' && action === 'reprovar') {
    const { id, checklist, inspetor, itensFalhos } = req.body || {};
    const insp = db.inspecoes.find(i => i.id === id);
    if (!insp) return res.status(404).json({ ok: false, error: 'inspeção não encontrada' });
    if (checklist) insp.checklist = checklist;
    if (inspetor) insp.inspetor = String(inspetor);
    insp.status = 'reprovado';
    insp.reprovacoes = insp.reprovacoes || [];
    insp.reprovacoes.push({
      em: new Date().toISOString(),
      inspetor: String(inspetor || ''),
      itensFalhos: Array.isArray(itensFalhos) ? itensFalhos : [],
    });
    await dbSet(KEY, db);
    return res.status(200).json({ ok: true, inspecao: insp });
  }

  // ── REINSPECIONAR (voltou do retrabalho → nova rodada) ──
  if (req.method === 'POST' && action === 'reinspecionar') {
    const { id } = req.body || {};
    const insp = db.inspecoes.find(i => i.id === id);
    if (!insp) return res.status(404).json({ ok: false, error: 'inspeção não encontrada' });
    insp.status = 'aguardando';
    insp.checklist = {};
    await dbSet(KEY, db);
    return res.status(200).json({ ok: true });
  }

  // ── EXCLUIR ──
  if (req.method === 'POST' && action === 'excluir') {
    const { id } = req.body || {};
    const antes = db.inspecoes.length;
    db.inspecoes = db.inspecoes.filter(i => i.id !== id);
    if (db.inspecoes.length === antes) return res.status(404).json({ ok: false, error: 'não encontrada' });
    await dbSet(KEY, db);
    return res.status(200).json({ ok: true });
  }

  // ── TÉCNICOS (config) ──
  if (req.method === 'POST' && action === 'config-tecnicos') {
    const { tecnicos } = req.body || {};
    if (!Array.isArray(tecnicos)) return res.status(400).json({ ok: false, error: 'tecnicos deve ser lista' });
    db.config.tecnicos = tecnicos.map(t => String(t).trim()).filter(Boolean);
    await dbSet(KEY, db);
    return res.status(200).json({ ok: true, tecnicos: db.config.tecnicos });
  }

  return res.status(400).json({ ok: false, error: 'action inválida' });
}
