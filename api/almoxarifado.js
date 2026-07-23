// ═══ ALMOXARIFADO — API (beta) ═══
// Motor de tarefas físicas: varre pipe + logística, detecta movimentos e gera tarefas
// F1: recebimento com foto obrigatória + movimentações + inventário vivo (sem tocar nos outros módulos)

const U = (process.env.UPSTASH_URL || '').replace(/['"]/g, '').trim();
const T = (process.env.UPSTASH_TOKEN || '').replace(/[\n\r'"]/g, '').trim();
const KEY = 'reparoeletro_almoxarifado';

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

const FASE_LBL = {
  coleta_efetuada: '📥 Chegou na loja (coleta efetuada)',
  aguardando_aprovacao: 'Aguardando Aprovação',
  ultima_chamada: 'Última Chamada',
  aprovados: 'Produção',
  descarte: 'Descarte',
  garantia: 'Garantia',
};

function defaultDB() {
  return { tarefas: [], inventario: {}, snapshot: { pipe: {}, logColeta: [] }, config: { proximoNum: 1 } };
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
  if (!Array.isArray(db.tarefas)) db.tarefas = [];
  if (!db.inventario) db.inventario = {};
  if (!db.snapshot) db.snapshot = { pipe: {}, logColeta: [] };
  if (!db.config) db.config = { proximoNum: 1 };

  function novaTarefa(t) {
    const num = db.config.proximoNum || 1;
    db.config.proximoNum = num + 1;
    return Object.assign({
      id: 'ALM-' + String(num).padStart(4, '0'),
      criadoEm: new Date().toISOString(),
      status: 'pendente', feitoPor: '', feitoEm: null, motivoFalha: '',
    }, t);
  }

  // ── SYNC: varre pipe + logística e gera tarefas dos movimentos novos ──
  if (action === 'sync' || action === 'load') {
    try {
      const [pipe, log] = await Promise.all([
        dbGet('reparoeletro_pipe'), dbGet('reparoeletro_logistica'),
      ]);
      const snapPipe = db.snapshot.pipe || {};
      const snapCol = new Set(db.snapshot.logColeta || []);
      const novoSnapPipe = {};
      const novoSnapCol = [];
      const GATILHOS = { ultima_chamada: 'aguardando_aprovacao', aprovados: 'aguardando_aprovacao', descarte: 'aguardando_aprovacao', garantia: null };
      const jaTem = (cardId, destino) => db.tarefas.some(t => t.cardId === cardId && t.destino === destino && t.status === 'pendente');

      // Pipe: entradas novas nas fases-gatilho
      for (const c of ((pipe && pipe.cards) || [])) {
        novoSnapPipe[c.id] = c.phase;
        const antes = snapPipe[c.id];
        if (antes === undefined && Object.keys(snapPipe).length === 0) continue; // primeira sync: só fotografa
        if (c.phase !== antes && GATILHOS.hasOwnProperty(c.phase)) {
          if (!jaTem(c.id, c.phase)) {
            db.tarefas.unshift(novaTarefa({
              tipo: 'mover', cardId: c.id,
              cliente: c.nomeContato || '—', tel: c.telefone || '', equipamento: c.equipamento || '',
              origem: (db.inventario[c.id] && db.inventario[c.id].local) || GATILHOS[c.phase] || '—',
              destino: c.phase,
            }));
          }
        }
      }

      // Logística: chegadas novas em coleta_efetuada → recebimento com foto
      for (const c of ((log && log.cards) || [])) {
        if (c.phase !== 'coleta_efetuada') continue;
        novoSnapCol.push(c.id);
        if (snapCol.size === 0 && (db.snapshot.logColeta || []).length === 0 && Object.keys(snapPipe).length === 0) continue; // primeira sync
        if (!snapCol.has(c.id)) {
          const existe = db.tarefas.some(t => t.cardId === c.id && t.tipo === 'receber');
          if (!existe) {
            db.tarefas.unshift(novaTarefa({
              tipo: 'receber', cardId: c.id,
              cliente: c.nomeContato || c.nome || '—', tel: c.telefone || '', equipamento: c.equipamento || '',
              origem: 'coleta_efetuada', destino: 'aguardando_aprovacao',
              modelo: '', temFoto: false,
            }));
          }
        }
      }

      db.snapshot = { pipe: novoSnapPipe, logColeta: novoSnapCol };
      await dbSet(KEY, db);
    } catch (e) {}
    return res.status(200).json({ ok: true, tarefas: db.tarefas.slice(0, 300), inventario: db.inventario, faseLbl: FASE_LBL });
  }

  // ── CONCLUIR tarefa (feito) ──
  if (req.method === 'POST' && action === 'concluir') {
    const { id, feitoPor, modelo } = req.body || {};
    const t = db.tarefas.find(x => x.id === id);
    if (!t) return res.status(404).json({ ok: false, error: 'tarefa não encontrada' });
    if (t.tipo === 'receber') {
      if (!t.temFoto) return res.status(400).json({ ok: false, error: 'foto obrigatória no recebimento' });
      if (modelo !== undefined) t.modelo = String(modelo || '').trim();
      if (!t.modelo) return res.status(400).json({ ok: false, error: 'informe o modelo' });
    }
    t.status = 'feito';
    t.feitoPor = String(feitoPor || '').trim();
    t.feitoEm = new Date().toISOString();
    db.inventario[t.cardId] = {
      cliente: t.cliente, equipamento: t.equipamento, modelo: t.modelo || (db.inventario[t.cardId] || {}).modelo || '',
      local: t.destino, atualizadoEm: t.feitoEm, por: t.feitoPor,
    };
    await dbSet(KEY, db);
    return res.status(200).json({ ok: true });
  }

  // ── FALHA (não consegui) ──
  if (req.method === 'POST' && action === 'falha') {
    const { id, feitoPor, motivo } = req.body || {};
    const t = db.tarefas.find(x => x.id === id);
    if (!t) return res.status(404).json({ ok: false, error: 'tarefa não encontrada' });
    if (!motivo) return res.status(400).json({ ok: false, error: 'motivo obrigatório' });
    t.status = 'falha';
    t.feitoPor = String(feitoPor || '').trim();
    t.feitoEm = new Date().toISOString();
    t.motivoFalha = String(motivo).trim();
    await dbSet(KEY, db);
    return res.status(200).json({ ok: true });
  }

  // ── REABRIR (falha resolvida → pendente de novo) ──
  if (req.method === 'POST' && action === 'reabrir') {
    const { id } = req.body || {};
    const t = db.tarefas.find(x => x.id === id);
    if (!t) return res.status(404).json({ ok: false, error: 'não encontrada' });
    t.status = 'pendente'; t.motivoFalha = '';
    await dbSet(KEY, db);
    return res.status(200).json({ ok: true });
  }

  // ── FOTO do recebimento (base64 comprimido no front) ──
  if (req.method === 'POST' && action === 'foto') {
    const { id, dataUrl } = req.body || {};
    const t = db.tarefas.find(x => x.id === id);
    if (!t) return res.status(404).json({ ok: false, error: 'tarefa não encontrada' });
    if (!dataUrl || String(dataUrl).length > 250000) {
      return res.status(400).json({ ok: false, error: 'foto ausente ou grande demais' });
    }
    await dbSet('alm_foto_' + t.cardId, { em: new Date().toISOString(), img: dataUrl });
    t.temFoto = true;
    await dbSet(KEY, db);
    return res.status(200).json({ ok: true });
  }

  // ── VER-FOTO ──
  if (action === 'ver-foto') {
    const f = await dbGet('alm_foto_' + String(req.query.cardId || ''));
    return res.status(200).json({ ok: !!f, foto: f || null });
  }

  // ── SEED-TESTE (fichas simuladas para o beta) ──
  if (action === 'seed-teste') {
    const seeds = [
      { tipo: 'receber', cliente: 'Pedro Recebimento', equipamento: 'Micro-ondas Electrolux 30L', origem: 'coleta_efetuada', destino: 'aguardando_aprovacao', modelo: '', temFoto: false },
      { tipo: 'mover', cliente: 'Pedro Última', equipamento: 'Adega 12 garrafas', origem: 'aguardando_aprovacao', destino: 'ultima_chamada' },
      { tipo: 'mover', cliente: 'Pedro Aprovado', equipamento: 'Purificador Soft', origem: 'aguardando_aprovacao', destino: 'aprovados' },
      { tipo: 'mover', cliente: 'Pedro Descarte', equipamento: 'Forno elétrico pequeno', origem: 'ultima_chamada', destino: 'descarte' },
      { tipo: 'mover', cliente: 'Pedro Garantia', equipamento: 'TV 43"', origem: 'entrada', destino: 'garantia' },
    ];
    const criadas = [];
    for (const s of seeds) {
      if (db.tarefas.some(t => t.cliente === s.cliente && t.status === 'pendente')) continue;
      db.tarefas.unshift(novaTarefa(Object.assign({ cardId: 'TESTE-' + s.cliente.replace(/\s/g, ''), tel: '5531997856023', teste: true }, s)));
      criadas.push(s.cliente);
    }
    await dbSet(KEY, db);
    return res.status(200).json({ ok: true, criadas });
  }

  // ── LIMPAR-TESTES ──
  if (action === 'limpar-testes') {
    const antes = db.tarefas.length;
    db.tarefas = db.tarefas.filter(t => !t.teste);
    for (const k of Object.keys(db.inventario)) if (k.startsWith('TESTE-')) delete db.inventario[k];
    await dbSet(KEY, db);
    return res.status(200).json({ ok: true, removidas: antes - db.tarefas.length });
  }

  return res.status(400).json({ ok: false, error: 'action inválida' });
}
