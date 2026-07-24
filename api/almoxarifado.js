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
        // coleta efetuada SEMPRE gera tarefa de recebimento (equipamento físico na loja agora)
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

      // ══ F2: FL reprovado + Vendas + Checkout + Compra Equip (só leitura nos módulos) ══
      try {
        const [fl, vnd, ckv, ceq] = await Promise.all([
          dbGet('reparoeletro_frenteloja'), dbGet('reparoeletro_vendas'),
          dbGet('reparoeletro_checkout_vendas'), dbGet('reparoeletro_compra_equip'),
        ]);
        const s = db.snapshot;
        const primeira = !s.f2; // primeira sync F2: só fotografa
        if (!s.f2) s.f2 = { fl: {}, vendas: [], ck: [], ceAna: [], ceComp: [] };
        const jaTarefa = (cid, tp) => db.tarefas.some(t => t.cardId === cid && t.tipo === tp);

        // FL → reprovado: levar p/ Aguardando Retirada
        const novoFl = {};
        for (const f of ((fl && fl.fichas) || [])) {
          novoFl[f.id] = f.phase;
          if (primeira) continue;
          if (f.phase === 'reprovado' && s.f2.fl[f.id] !== 'reprovado' && !jaTarefa(f.id, 'loja-reprovado')) {
            db.tarefas.unshift(novaTarefa({ tipo: 'loja-reprovado', cardId: f.id,
              cliente: f.nomeContato || '—', tel: f.telefone || '', equipamento: f.equipamento || '',
              origem: 'Frente de Loja', destino: 'aguardando_retirada' }));
          }
        }
        s.f2.fl = novoFl;

        // Vendas + Checkout → tarefa venda (2 checks)
        const vendaTarefa = (v, orig) => {
          const cid = orig + '-' + (v.id || v.vendaId || v.createdAt || Math.random());
          if (!jaTarefa(cid, 'venda')) db.tarefas.unshift(novaTarefa({ tipo: 'venda', cardId: cid,
            cliente: v.nomeCliente || v.cliente || v.nome || '—', tel: v.telefone || v.tel || '',
            equipamento: v.equipamento || v.descricao || v.titulo || '—',
            origem: orig, destino: 'entrega', videoGravado: false, separado: false }));
        };
        const novoV = ((vnd && vnd.vendas) || []).map(v => String(v.id || v.createdAt));
        if (!primeira) ((vnd && vnd.vendas) || []).forEach(v => { if (!s.f2.vendas.includes(String(v.id || v.createdAt))) vendaTarefa(v, 'Vendas'); });
        s.f2.vendas = novoV;
        const novoCk = ((ckv && ckv.vendas) || []).map(v => String(v.id || v.createdAt));
        if (!primeira) ((ckv && ckv.vendas) || []).forEach(v => { if (!s.f2.ck.includes(String(v.id || v.createdAt))) vendaTarefa(v, 'Checkout'); });
        s.f2.ck = novoCk;

        // Compra Equip: nova ficha em análise → avaliar; status comprado → levar p/ área
        const novoAna = [], novoComp = [];
        for (const f of ((ceq && ceq.fichas) || [])) {
          if (f.status === 'analise') {
            novoAna.push(f.id);
            if (!primeira && !s.f2.ceAna.includes(f.id) && !jaTarefa(f.id, 'avaliar-compra')) {
              db.tarefas.unshift(novaTarefa({ tipo: 'avaliar-compra', cardId: f.id,
                cliente: f.nomeContato || f.cliente || '—', tel: f.telefone || '', equipamento: f.equipamento || f.descricao || '—',
                origem: 'Compra Equip', destino: 'analise' }));
            }
          }
          if (f.status === 'comprado') {
            novoComp.push(f.id);
            if (!primeira && !s.f2.ceComp.includes(f.id) && !jaTarefa(f.id, 'levar-area')) {
              db.tarefas.unshift(novaTarefa({ tipo: 'levar-area', cardId: f.id,
                cliente: f.nomeContato || f.cliente || '—', tel: f.telefone || '', equipamento: f.equipamento || f.descricao || '—',
                origem: 'Equipamento Comprado', destino: 'area_correta' }));
            }
          }
        }
        s.f2.ceAna = novoAna; s.f2.ceComp = novoComp;
      } catch (e) {}

      db.snapshot = { ...db.snapshot, pipe: novoSnapPipe, logColeta: novoSnapCol };
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
    // ══ F2 efeitos ══
    // Aprovado → Produção: confirmar aqui MOVE o card no sistema técnico
    if (t.tipo === 'mover' && t.destino === 'aprovados') {
      try {
        const bdb = await dbGet('reparoeletro_board');
        if (bdb && Array.isArray(bdb.cards)) {
          const bc = bdb.cards.find(x => x.osCode === t.cardId || x.pipefyId === t.cardId);
          if (bc && bc.phaseId === 'aprovado') {
            bc.phaseId = 'producao'; bc.movedAt = new Date().toISOString(); bc.movedBy = 'Almoxarifado';
            if (!Array.isArray(bdb.movesLog)) bdb.movesLog = [];
            bdb.movesLog.push({ phaseId: 'producao', pipefyId: bc.pipefyId, timestamp: bc.movedAt });
            await dbSet('reparoeletro_board', bdb);
          }
        }
      } catch (e) {}
    }
    // Avaliar compra: grava parecer + preço na ficha do Compra Equip (verde/vermelho lá)
    if (t.tipo === 'avaliar-compra') {
      const parecer = (req.body || {}).parecer;
      const preco = (req.body || {}).preco;
      if (parecer !== 'sim' && parecer !== 'nao') return res.status(400).json({ ok: false, error: 'parecer sim/nao obrigatório' });
      try {
        const cdb = await dbGet('reparoeletro_compra_equip');
        const cf = cdb && (cdb.fichas || []).find(x => x.id === t.cardId);
        if (cf) {
          cf.recomendacao = parecer; cf.recomendadoAt = new Date().toISOString();
          if (parecer === 'sim' && preco) cf.precoSugerido = String(preco).trim();
          cf.recomendadoPor = 'Almoxarifado' + (feitoPor ? ' - ' + feitoPor : '');
          await dbSet('reparoeletro_compra_equip', cdb);
        }
      } catch (e) {}
      t.parecer = parecer; if (preco) t.precoSugerido = String(preco).trim();
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

  // ── F2: VENDA-CHECK — marca Vídeo Gravado / Separado p/ Entrega (conclui com os 2) ──
  if (req.method === 'POST' && action === 'venda-check') {
    const { id, qual, feitoPor } = req.body || {};
    const t = db.tarefas.find(x => x.id === id);
    if (!t || t.tipo !== 'venda') return res.status(404).json({ ok: false, error: 'tarefa não encontrada' });
    if (qual === 'video') t.videoGravado = true;
    else if (qual === 'separado') t.separado = true;
    else return res.status(400).json({ ok: false, error: 'qual: video|separado' });
    if (t.videoGravado && t.separado) {
      t.status = 'feito'; t.feitoPor = String(feitoPor || '').trim(); t.feitoEm = new Date().toISOString();
    }
    await dbSet(KEY, db);
    return res.status(200).json({ ok: true, videoGravado: !!t.videoGravado, separado: !!t.separado, feito: t.status === 'feito' });
  }

  // ══ F2 ROTAS: listar (ativas + últimas finalizadas) ══
  if (action === 'rota-list') {
    const rotas = Array.isArray(db.rotas) ? db.rotas : [];
    return res.status(200).json({ ok: true, rotas: rotas.slice(0, 30) });
  }

  // ══ F2 ROTAS: marcar item separado (incrementa por unidade até a qtd) ══
  if (req.method === 'POST' && action === 'rota-separar') {
    const { rotaId, cardId, feitoPor } = req.body || {};
    const rt = (db.rotas || []).find(r => r.id === rotaId);
    const item = rt && rt.itens.find(i => i.cardId === cardId);
    if (!item) return res.status(404).json({ ok: false, error: 'item não encontrado' });
    item.separado = Math.min(item.qtd, (item.separado || 0) + 1);
    if (item.separado >= item.qtd) item.status = 'separado';
    item.por = String(feitoPor || '').trim();
    await dbSet(KEY, db);
    return res.status(200).json({ ok: true, separado: item.separado, qtd: item.qtd, status: item.status });
  }

  // ══ F2 ROTAS: negar item (não pode ser separado) — front devolve a ficha no pipe ══
  if (req.method === 'POST' && action === 'rota-negar') {
    const { rotaId, cardId, motivo, feitoPor } = req.body || {};
    if (!motivo) return res.status(400).json({ ok: false, error: 'motivo obrigatório' });
    const rt2 = (db.rotas || []).find(r => r.id === rotaId);
    const item2 = rt2 && rt2.itens.find(i => i.cardId === cardId);
    if (!item2) return res.status(404).json({ ok: false, error: 'item não encontrado' });
    item2.status = 'negado'; item2.motivo = String(motivo).trim(); item2.por = String(feitoPor || '').trim();
    await dbSet(KEY, db);
    return res.status(200).json({ ok: true });
  }

  // ══ F2 ROTAS: confirmar saída (completa/parcial + foto do motorista) ══
  if (req.method === 'POST' && action === 'rota-saida') {
    const { rotaId, motorista, fotoB64, feitoPor } = req.body || {};
    const rt3 = (db.rotas || []).find(r => r.id === rotaId);
    if (!rt3) return res.status(404).json({ ok: false, error: 'rota não encontrada' });
    if (!motorista) return res.status(400).json({ ok: false, error: 'informe o motorista' });
    if (!fotoB64) return res.status(400).json({ ok: false, error: 'foto do motorista obrigatória' });
    const sairam = rt3.itens.filter(i => i.status === 'separado');
    if (!sairam.length) return res.status(400).json({ ok: false, error: 'nenhum item separado para sair' });
    const naoSairam = rt3.itens.filter(i => i.status !== 'separado');
    rt3.status = 'finalizada';
    rt3.tipoSaida = naoSairam.length ? 'parcial' : 'completa';
    rt3.motorista = String(motorista).trim();
    rt3.saidaEm = new Date().toISOString();
    rt3.saidaPor = String(feitoPor || '').trim();
    naoSairam.forEach(i => { if (i.status === 'pendente') { i.status = 'nao_saiu'; if (!i.motivo) i.motivo = String((req.body || {}).motivoPendentes || 'não saiu na rota').trim(); } });
    // foto separada do payload principal (Redis lean: 1 chave por rota, sobrescrevível)
    await dbSet(KEY + '_rotafoto_' + rt3.id, { b64: fotoB64, em: rt3.saidaEm });
    await dbSet(KEY, db);
    return res.status(200).json({ ok: true, sairam: sairam.map(i => i.cardId), naoSairam: naoSairam.map(i => ({ cardId: i.cardId, motivo: i.motivo })), tipo: rt3.tipoSaida });
  }

  // ══ F2 ROTAS: ver foto do motorista ══
  if (action === 'rota-foto') {
    const f = await dbGet(KEY + '_rotafoto_' + (req.query.rota || ''));
    if (!f) return res.status(404).json({ ok: false });
    return res.status(200).json({ ok: true, b64: f.b64, em: f.em });
  }

  // ── F2: RESET — zera o almoxarifado p/ começar limpo (tarefas/inventário/snapshot) ──
  if (action === 'reset-f2') {
    await dbSet(KEY, defaultDB());
    return res.status(200).json({ ok: true, msg: 'Almoxarifado zerado — próxima sync só fotografa, tarefas nascem dos eventos novos' });
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

  // ── ML-PENDENTES: peças compradas aguardando chegada (ADM + TV) ──
  if (action === 'ml-pendentes') {
    const [adm, tv] = await Promise.all([
      dbGet('reparoeletro_compras_pecas').then(v => v || { pecas: [] }),
      dbGet('tv_compras_pecas').then(v => v || { pecas: [] }),
    ]);
    const mapear = (arr, sis) => (arr.pecas || [])
      .filter(p => p.status === 'pago' || p.status === 'a_caminho')
      .map(p => ({ id: p.id, sistema: sis, descricao: p.descricao, os: p.os || '', qtd: p.quantidade || 1,
        status: p.status, previsao: p.previsaoChegada || null, urgente: !!p.urgente, compradoEm: p.compradoEm || p.createdAt }));
    const chegadas = (db.mlEntregas || []).slice(0, 40);
    return res.status(200).json({ ok: true, pendentes: [...mapear(adm, 'adm'), ...mapear(tv, 'tv')], chegadas });
  }

  // ── ML-CHEGOU (POST {id, sistema, tecnico, feitoPor}): marca recebida + registra o técnico destino ──
  if (req.method === 'POST' && action === 'ml-chegou') {
    const { id, sistema, tecnico, feitoPor } = req.body || {};
    if (!id || !tecnico) return res.status(400).json({ ok: false, error: 'id e técnico obrigatórios' });
    const KEYP = sistema === 'tv' ? 'tv_compras_pecas' : 'reparoeletro_compras_pecas';
    const cdb = (await dbGet(KEYP)) || { pecas: [] };
    const p = (cdb.pecas || []).find(x => x.id === id);
    if (!p) return res.status(404).json({ ok: false, error: 'peça não encontrada' });
    p.status = 'recebido';
    p.recebidoEm = new Date().toISOString();
    p.tecnicoDestino = String(tecnico).trim();
    await dbSet(KEYP, cdb);
    try {
      const pd = (await dbGet('reparoeletro_pecas_disponiveis')) || { itens: [] };
      pd.itens.unshift({ os: p.os || '', descricao: p.descricao, tecnico: String(tecnico).trim(), sistema: sistema || 'adm', em: p.recebidoEm });
      pd.itens = pd.itens.slice(0, 150);
      await dbSet('reparoeletro_pecas_disponiveis', pd);
    } catch (e) {}
    if (!Array.isArray(db.mlEntregas)) db.mlEntregas = [];
    db.mlEntregas.unshift({ id: p.id, descricao: p.descricao, os: p.os || '', sistema: sistema || 'adm',
      tecnico: String(tecnico).trim(), feitoPor: String(feitoPor || '').trim(), em: p.recebidoEm });
    db.mlEntregas = db.mlEntregas.slice(0, 200);
    await dbSet(KEY, db);
    return res.status(200).json({ ok: true });
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
