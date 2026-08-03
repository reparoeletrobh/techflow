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
// Aplica UMA mudança relendo o banco na hora (o sync roda em paralelo e regravava o banco
// inteiro por cima das conclusões — tarefa concluída "voltava" para a lista)
async function aplicarNaTarefa(KEYX, id, mudanca) {
  const atual = (await dbGet(KEYX)) || null;
  if (!atual || !Array.isArray(atual.tarefas)) return null;
  const alvo = atual.tarefas.find(x => x.id === id);
  if (!alvo) return null;
  mudanca(alvo);
  await dbSet(KEYX, atual);
  return alvo;
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
    // sufixo aleatório: duas criações simultâneas liam o mesmo número e geravam IDs IGUAIS,
    // fazendo foto e parecer irem para a tarefa errada (Lusiane x Joana com ALM-1116)
    const suf = Math.random().toString(36).slice(2, 5).toUpperCase();
    return Object.assign({
      id: 'ALM-' + String(num).padStart(4, '0') + '-' + suf,
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
      // garantia REMOVIDA dos gatilhos: abertura de garantia acontece com o equipamento na casa do cliente;
      // só vira assunto físico quando chegar em coleta_efetuada e o operador marcar RS
      const GATILHOS = { ultima_chamada: 'aguardando_aprovacao', aprovados: 'aguardando_aprovacao', descarte: 'aguardando_aprovacao' };
      const jaTem = (cardId, destino) => db.tarefas.some(t => t.cardId === cardId && t.destino === destino && t.status === 'pendente');

      // Pipe: diff por snapshot (estável). Primeira sync: só movimentos das últimas 12h,
      // com FUSÍVEL: se houver mais de 15 candidatos (movimentação em massa de cron), não cria nada — só fotografa.
      const snapVazio = Object.keys(snapPipe).length === 0;
      let candidatos = [];
      for (const c of ((pipe && pipe.cards) || [])) {
        novoSnapPipe[c.id] = c.phase;
        if (!GATILHOS.hasOwnProperty(c.phase)) continue;
        const antes = snapPipe[c.id];
        if (snapVazio) {
          const mvPipe = new Date(c.movedAt || 0).getTime();
          if (mvPipe && Date.now() - mvPipe < 12 * 3600 * 1000) candidatos.push(c);
          continue;
        }
        if (c.phase !== antes) candidatos.push(c);
      }
      if (snapVazio && candidatos.length > 15) candidatos = []; // fusível anti-enxurrada
      for (const c of candidatos) {
        {
          if (!jaTem(c.id, c.phase)) {
            // A situação ATUAL do card substitui pendências antigas de mover do mesmo card:
            // ex. estava pendente "mover p/ última chamada" e o cliente aprovou antes de executarem →
            // exclui a de última chamada e fica só a de aprovado
            db.tarefas = db.tarefas.filter(t =>
              !(t.cardId === c.id && t.tipo === 'mover' && t.status === 'pendente' && t.destino !== c.phase));
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
      for (const c of ((log && (log.fichas || log.cards)) || [])) {
        if (c.phase !== 'coleta_efetuada') continue;
        novoSnapCol.push(c.id);
        // coleta efetuada gera tarefa de recebimento (só movimentos das últimas 48h — não retro-popula paradas antigas)
        const mvTs = new Date(c.movedAt || c.registradoEm || c.criadoEm || 0).getTime();
        if (Date.now() - mvTs > 48 * 3600 * 1000) continue;
        if (!snapCol.has(c.id)) {
          const existe = db.tarefas.some(t => t.cardId === c.id && t.tipo === 'receber');
          if (!existe) {
            db.tarefas.unshift(novaTarefa({
              tipo: 'receber', cardId: c.id,
              cliente: c.nomeContato || c.nome || '—', tel: c.telefone || '', equipamento: c.equipamento || '',
              defeito: c.defeito || '', obs: c.texto || '',
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

      // fichas que SAÍRAM de coleta_efetuada (diagnóstico registrado / RS): fecha a tarefa sozinha
      try {
        const aindaColeta = new Set(novoSnapCol);
        db.tarefas.forEach(t => {
          if (t.tipo === 'receber' && t.status === 'pendente' && !aindaColeta.has(t.cardId)) {
            t.status = 'feito'; t.feitoPor = 'Sistema'; t.feitoEm = new Date().toISOString();
            t.autoConcluida = 'saiu de Coleta Efetuada (diagnóstico/RS registrado)';
          }
        });
      } catch (e) {}
      db.snapshot = { ...db.snapshot, pipe: novoSnapPipe, logColeta: novoSnapCol };
      // ══ MESCLA ANTI-CORRIDA: ações do usuário feitas durante o sync NUNCA são sobrescritas ══
      try {
        const fresco = (await dbGet(KEY)) || db;
        const mapaFresco = {};
        (fresco.tarefas || []).forEach(t => { mapaFresco[t.id] = t; });
        const idsProcessados = new Set((db.tarefas || []).map(t => t.id));
        db.tarefas = (db.tarefas || []).map(t => {
          const f = mapaFresco[t.id];
          if (!f) return t;
          // regra: o status mais AVANÇADO vence.
          // - sync avançou (auto-conclusão): t não-pendente e f pendente → fica t
          // - usuário avançou durante o sync (concluiu/falhou): f não-pendente → fica f
          if (t.status !== 'pendente' && f.status === 'pendente') return t;
          if (f.status !== 'pendente') return f;
          return t;
        });
        // tarefas criadas por outras ações durante o sync (reconciliar etc.) entram também
        (fresco.tarefas || []).forEach(t => { if (!idsProcessados.has(t.id)) db.tarefas.unshift(t); });
        db.inventario = Object.assign({}, db.inventario, fresco.inventario || {});
        if (fresco.config && fresco.config.proximoNum > (db.config.proximoNum || 0)) db.config.proximoNum = fresco.config.proximoNum;
      } catch (e) {}
      await dbSet(KEY, db);
    } catch (e) {}
    const tarefasOrd = [...db.tarefas].sort((a, b) => {
      const uc = t => (t.status === 'pendente' && t.tipo === 'mover' && t.destino === 'ultima_chamada') ? 1 : 0;
      return uc(a) - uc(b); // última chamada afunda; ordem original preservada no resto (sort estável)
    });
    return res.status(200).json({ ok: true, tarefas: tarefasOrd.slice(0, 300), inventario: db.inventario, faseLbl: FASE_LBL });
  }

  // ── CONCLUIR tarefa (feito) ──
  // ── 🔵 CRIAR-ANALISE-COMPRA: duplicata do caso de compra para a equipe dar o parecer ──
  // ── 🔧 REPARAR-IDS: corrige tarefas que ficaram com o mesmo identificador ──
  if (action === 'reparar-ids') {
    const dbR = (await dbGet(KEY)) || { tarefas: [] };
    const vistos = new Set();
    const trocadas = [];
    for (const t of (dbR.tarefas || [])) {
      if (!vistos.has(t.id)) { vistos.add(t.id); continue; }
      const antigo = t.id;
      const novo = t.id + '-' + Math.random().toString(36).slice(2, 5).toUpperCase();
      t.id = novo;
      trocadas.push({ cliente: t.cliente, equipamento: t.equipamento, de: antigo, para: novo });
      vistos.add(novo);
    }
    if (String(req.query.aplicar || '') === '1' && trocadas.length) {
      await dbSet(KEY, dbR);
      return res.status(200).json({ ok: true, corrigidas: trocadas.length, trocadas });
    }
    return res.status(200).json({ ok: true, duplicadas: trocadas.length, trocadas,
      dica: trocadas.length ? 'para corrigir: &aplicar=1' : 'nenhum identificador duplicado' });
  }

  // ── 🩺 DIAGNOSTICO-COMPRA: a cadeia da análise de compra, ponta a ponta ──
  if (action === 'diagnostico-compra') {
    const [dbC, pros] = await Promise.all([dbGet(KEY), dbGet('prospeccao_adm')]);
    const tarefas = (((dbC || {}).tarefas) || []).filter(t => t.tipo === 'avaliar-compra');
    const conflitos = (((pros || {}).fichas) || []).filter(f => f.status === 'conflitos_bot' && f.analiseCompra);
    const detalhe = [];
    for (const t of tarefas) {
      const foto = t.cardId ? await dbGet('alm_foto_' + t.cardId) : null;
      detalhe.push({
        tarefa: t.id, cliente: t.cliente, equipamento: t.equipamento,
        status: t.status, temFotoNaTarefa: !!t.temFoto,
        cardId: t.cardId || '(vazio)',
        fotoGravadaNoBanco: !!(foto && foto.img),
        tamanhoFoto: foto && foto.img ? Math.round(foto.img.length / 1024) + 'KB' : '—',
        ligadaAoConflito: t.origemConflito || '(sem vínculo)',
        parecer: t.parecer || '(pendente)',
      });
    }
    return res.status(200).json({ ok: true,
      tarefasAvaliarCompra: tarefas.length,
      conflitosAnaliseCompra: conflitos.length,
      pendentes: tarefas.filter(t => t.status === 'pendente').length,
      cadaTarefa: detalhe,
      conflitosNaProspeccao: conflitos.map(c => c.nome + ' | ' + (c.equipamento || '') + ' | foto:' + (c.temFoto ? 'sim' : 'não') +
        ' | parecer:' + (c.recomendacaoCompra ? c.recomendacaoCompra.parecer : 'pendente')),
      leitura: 'se tarefasAvaliarCompra=0 o problema é na CRIAÇÃO; se cardId estiver vazio a FOTO não tem onde ser gravada' });
  }

  // ── 🧹 LIMPAR-TAREFAS-TV: remove do almoxarifado ADM tarefas de cards de TV ──
  if (action === 'limpar-tarefas-tv') {
    const [db2, ppTv2] = await Promise.all([dbGet(KEY), dbGet('tv_pipe')]);
    const idsTv = new Set((((ppTv2 || {}).cards) || []).map(c => c.id));
    const alvo = (((db2 || {}).tarefas) || []).filter(t => idsTv.has(t.cardId) && t.status === 'pendente');
    if (String(req.query.aplicar || '') === '1' && alvo.length) {
      const ids = new Set(alvo.map(t => t.id));
      db2.tarefas = db2.tarefas.filter(t => !ids.has(t.id));
      await dbSet(KEY, db2);
      return res.status(200).json({ ok: true, removidas: alvo.length,
        lista: alvo.map(t => t.cliente + ' | ' + (t.equipamento || '')) });
    }
    return res.status(200).json({ ok: true, encontradas: alvo.length,
      lista: alvo.map(t => t.cliente + ' | ' + (t.equipamento || '') + ' | ' + t.destino),
      dica: 'para remover: &aplicar=1' });
  }

  // ── 🔁 CRIAR-MOVER: recria a tarefa de movimentação que o gatilho perdeu ──
  if (req.method === 'POST' && action === 'criar-mover') {
    const b = req.body || {};
    if (!b.cardId || !b.destino) return res.status(400).json({ ok: false, error: 'cardId e destino obrigatórios' });
    // 🚫 TV não entra no almoxarifado do ADM — tem separação própria
    try {
      const ppTv = (await dbGet('tv_pipe')) || { cards: [] };
      if (((ppTv.cards) || []).some(c => c.id === b.cardId)) {
        return res.status(200).json({ ok: true, ignorado: true, motivo: 'card de TV não entra no almoxarifado ADM' });
      }
    } catch (e) {}
    const dbM = (await dbGet(KEY)) || defaultDB();
    if (!Array.isArray(dbM.tarefas)) dbM.tarefas = [];
    if (!dbM.config) dbM.config = { proximoNum: 1 };
    const ja = dbM.tarefas.some(t => t.cardId === b.cardId && t.destino === b.destino && t.status === 'pendente');
    if (ja) return res.status(200).json({ ok: true, dedupe: true, msg: 'tarefa já existe pendente' });
    dbM.tarefas.unshift(novaTarefa({
      tipo: 'mover', cardId: b.cardId,
      cliente: String(b.cliente || 'Cliente').slice(0, 60), tel: String(b.tel || ''),
      equipamento: String(b.equipamento || '').slice(0, 60),
      origem: b.origem || 'aguardando_aprovacao', destino: b.destino,
      forcada: true, forcadaEm: new Date().toISOString(),
    }));
    await dbSet(KEY, dbM);
    return res.status(200).json({ ok: true, criada: true });
  }

  if (req.method === 'POST' && action === 'criar-analise-compra') {
    const b = req.body || {};
    if (!b.conflitoId) return res.status(400).json({ ok: false, error: 'conflitoId obrigatório' });
    const db = (await dbGet(KEY)) || defaultDB();
    if (!Array.isArray(db.tarefas)) db.tarefas = [];
    if (!db.config) db.config = { proximoNum: 1 };
    const jaTem = db.tarefas.some(t => t.origemConflito === b.conflitoId && t.status === 'pendente');
    if (jaTem) return res.status(200).json({ ok: true, dedupe: true });
    db.tarefas.unshift(novaTarefa({
      tipo: 'avaliar-compra', cardId: b.cardId || b.conflitoId, origemConflito: b.conflitoId,
      cliente: String(b.cliente || 'Cliente').slice(0, 60), tel: String(b.tel || ''),
      equipamento: String(b.equipamento || '').slice(0, 60),
      obs: String(b.obs || '').slice(0, 200), temFoto: !!b.temFoto,
      origem: 'bot — cliente quer vender o equipamento',
    }));
    await dbSet(KEY, db);
    return res.status(200).json({ ok: true });
  }

  if (req.method === 'POST' && action === 'concluir') {
    const { id, feitoPor, modelo } = req.body || {};
    try {
      const iguais = (db.tarefas || []).filter(x => x.id === id);
      if (iguais.length > 1) return res.status(409).json({ ok: false,
        error: 'identificador duplicado no sistema — rode reparar-ids antes de continuar' });
    } catch (e) {}
    const t = db.tarefas.find(x => x.id === id);
    if (!t) return res.status(404).json({ ok: false, error: 'tarefa não encontrada' });
    if (t.tipo === 'receber') {
      if ((req.body || {}).rs) {
        t.rs = true; t.destino = 'garantia';
        // Entra na fila do novo sistema de Garantia (badge azul)
        try {
          const KG = (process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim();
          await fetch(`https://reparoeletroadm.com/api/garantia?action=fila-criar&k=${KG}`, {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ nome: t.cliente || 'Cliente', telefone: t.tel || '', equipamento: t.equipamento || '',
              relato: 'RS aberto no almoxarifado' + (t.obs ? ' — ' + String(t.obs).slice(0, 200) : ''), origem: 'almoxarifado-rs' }),
          });
        } catch (e) {}
      }
      else {
        if (!t.temFoto) return res.status(400).json({ ok: false, error: 'foto obrigatória no recebimento' });
        if (modelo !== undefined) t.modelo = String(modelo || '').trim();
        if (!t.modelo) return res.status(400).json({ ok: false, error: 'informe o modelo' });
      }
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
      // Parecer volta para o card de Análise de Compra no Conflitos Bot
      if (t.origemConflito) {
        try {
          const KAC = (process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim();
          await fetch(`https://reparoeletroadm.com/api/prospeccao?action=marcar-recomendacao&k=${KAC}`, {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ id: t.origemConflito, parecer, preco: preco || null, por: feitoPor || 'Almoxarifado', temFoto: !!t.temFoto }),
          });
        } catch (e) {}
      }
    }
    t.status = 'feito';
    t.feitoPor = String(feitoPor || '').trim();
    t.feitoEm = new Date().toISOString();
    // Equipamento fisicamente movido para a GARANTIA dentro da loja → entra na fila do novo sistema
    if (t.tipo === 'mover' && t.destino === 'garantia') {
      try {
        const KG3 = (process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim();
        await fetch(`https://reparoeletroadm.com/api/garantia?action=fila-criar&k=${KG3}`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ nome: t.cliente || 'Cliente', telefone: t.tel || '',
            equipamento: t.equipamento || '', relato: 'Equipamento movido para a garantia no almoxarifado',
            origem: 'almoxarifado-garantia' }),
        });
      } catch (e) {}
    }
    // SAVE ATÔMICO: relê o banco agora e aplica só esta tarefa (o sync roda em paralelo)
    const dbF = (await dbGet(KEY)) || db;
    if (!Array.isArray(dbF.tarefas)) dbF.tarefas = db.tarefas;
    if (!dbF.inventario) dbF.inventario = {};
    const tF = dbF.tarefas.find(x => x.id === id);
    if (tF) {
      tF.status = 'feito'; tF.feitoPor = t.feitoPor; tF.feitoEm = t.feitoEm;
      if (t.modelo) tF.modelo = t.modelo;
      if (t.rs) { tF.rs = true; tF.destino = t.destino; }
      if (t.parecer) { tF.parecer = t.parecer; if (t.precoSugerido) tF.precoSugerido = t.precoSugerido; }
    }
    dbF.inventario[t.cardId] = {
      cliente: t.cliente, equipamento: t.equipamento, modelo: t.modelo || (dbF.inventario[t.cardId] || {}).modelo || '',
      local: t.destino, atualizadoEm: t.feitoEm, por: t.feitoPor,
    };
    await dbSet(KEY, dbF);
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
    if (!['video', 'separado'].includes(qual)) return res.status(400).json({ ok: false, error: 'qual: video|separado' });
    const tf = await aplicarNaTarefa(KEY, id, (alvo) => {
      if (qual === 'video') alvo.videoGravado = true;
      else alvo.separado = true;
      if (alvo.videoGravado && alvo.separado) {
        alvo.status = 'feito';
        alvo.feitoPor = String(feitoPor || '').trim();
        alvo.feitoEm = new Date().toISOString();
      }
    });
    if (!tf) return res.status(404).json({ ok: false, error: 'tarefa não encontrada' });
    return res.status(200).json({ ok: true, videoGravado: !!tf.videoGravado, separado: !!tf.separado, feito: tf.status === 'feito' });
  }

  // ══ F2 ROTAS: listar (ativas + últimas finalizadas) ══
  if (action === 'rota-list') {
    let rdb = (await dbGet('reparoeletro_almox_rotas')) || null;
    if (!rdb) {
      rdb = { rotas: Array.isArray(db.rotas) ? db.rotas : [] }; // migra as existentes uma única vez
      await dbSet('reparoeletro_almox_rotas', rdb);
    }
    return res.status(200).json({ ok: true, rotas: (rdb.rotas || []).slice(0, 30) });
  }

  // ══ F2 ROTAS: marcar item separado (incrementa por unidade até a qtd) ══
  if (req.method === 'POST' && action === 'rota-separar') {
    const { rotaId, cardId, feitoPor } = req.body || {};
    const rdb1 = (await dbGet('reparoeletro_almox_rotas')) || { rotas: [] };
    const rt = (rdb1.rotas || []).find(r => r.id === rotaId);
    const item = rt && rt.itens.find(i => i.cardId === cardId);
    if (!item) return res.status(404).json({ ok: false, error: 'item não encontrado' });
    item.separado = Math.min(item.qtd, (item.separado || 0) + 1);
    if (item.separado >= item.qtd) item.status = 'separado';
    item.por = String(feitoPor || '').trim();
    await dbSet('reparoeletro_almox_rotas', rdb1);
    return res.status(200).json({ ok: true, separado: item.separado, qtd: item.qtd, status: item.status });
  }

  // ══ F2 ROTAS: negar item (não pode ser separado) — front devolve a ficha no pipe ══
  if (req.method === 'POST' && action === 'rota-negar') {
    const { rotaId, cardId, motivo, feitoPor } = req.body || {};
    if (!motivo) return res.status(400).json({ ok: false, error: 'motivo obrigatório' });
    const rdb2 = (await dbGet('reparoeletro_almox_rotas')) || { rotas: [] };
    const rt2 = (rdb2.rotas || []).find(r => r.id === rotaId);
    const item2 = rt2 && rt2.itens.find(i => i.cardId === cardId);
    if (!item2) return res.status(404).json({ ok: false, error: 'item não encontrado' });
    item2.status = 'negado'; item2.motivo = String(motivo).trim(); item2.por = String(feitoPor || '').trim();
    await dbSet('reparoeletro_almox_rotas', rdb2);
    return res.status(200).json({ ok: true });
  }

  // ══ F2 ROTAS: confirmar saída (completa/parcial + foto do motorista) ══
  if (req.method === 'POST' && action === 'rota-saida') {
    const { rotaId, motorista, fotoB64, feitoPor } = req.body || {};
    const rdb3 = (await dbGet('reparoeletro_almox_rotas')) || { rotas: [] };
    const rt3 = (rdb3.rotas || []).find(r => r.id === rotaId);
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
    await dbSet('reparoeletro_almox_rotas', rdb3);
    return res.status(200).json({ ok: true, sairam: sairam.map(i => i.cardId), naoSairam: naoSairam.map(i => ({ cardId: i.cardId, motivo: i.motivo })), tipo: rt3.tipoSaida });
  }

  // ══ F2 ROTAS: ver foto do motorista ══
  if (action === 'rota-foto') {
    const f = await dbGet(KEY + '_rotafoto_' + (req.query.rota || ''));
    if (!f) return res.status(404).json({ ok: false });
    return res.status(200).json({ ok: true, b64: f.b64, em: f.em });
  }

  // ── ARQUIVO: busca em tudo que já passou pelo almoxarifado ──
  if (action === 'arquivo') {
    const q = String(req.query.q || '').toLowerCase().trim();
    const casa = txt => String(txt || '').toLowerCase().includes(q);
    const tarefas = (db.tarefas || []).filter(t => !q ||
      casa(t.cliente) || casa(t.tel) || casa(t.equipamento) || casa(t.modelo) || casa(t.feitoPor) || casa(t.cardId));
    const rdbA = (await dbGet('reparoeletro_almox_rotas')) || { rotas: db.rotas || [] };
    const rotas = (rdbA.rotas || []).filter(r => !q ||
      casa(r.motorista) || casa(r.id) || (r.itens || []).some(i => casa(i.cliente) || casa(i.tel) || casa(i.equipamento)));
    const ml = (db.mlEntregas || []).filter(m => !q || casa(m.descricao) || casa(m.os) || casa(m.tecnico));
    return res.status(200).json({ ok: true,
      tarefas: tarefas.slice(0, 80), rotas: rotas.slice(0, 20), ml: ml.slice(0, 40) });
  }

  // ── 🔎 BUSCAR-TUDO: procura em TODA a operação, inclusive rotas finalizadas e arquivo ──
  if (action === 'buscar-tudo') {
    const q = String(req.query.q || '').toLowerCase().trim();
    if (q.length < 3) return res.status(200).json({ ok: false, error: 'informe ao menos 3 caracteres (nome, telefone ou OS)' });
    const casa = t => String(t || '').toLowerCase().includes(q);
    const [rdb, arqA, arqT, ppA, ppT, lgA, lgT, movs] = await Promise.all([
      dbGet('reparoeletro_almox_rotas'), dbGet('reparoeletro_arquivo'), dbGet('tv_arquivo'),
      dbGet('reparoeletro_pipe'), dbGet('tv_pipe'),
      dbGet('reparoeletro_logistica'), dbGet('tv_logistica'),
      dbGet('reparoeletro_movimentacoes'),
    ]);
    const achados = { rotas: [], tarefas: [], cards: [], fichas: [], movimentos: [] };

    // ROTAS — inclusive as finalizadas
    for (const r of (((rdb || {}).rotas) || (db.rotas || []))) {
      const itens = (r.itens || []).filter(i => casa(i.cliente) || casa(i.tel) || casa(i.equipamento) || casa(i.os) || casa(i.cardId));
      const casouARota = casa(r.motorista) || casa(r.id);
      if (itens.length || casouARota) {
        // buscou pela rota ou pelo motorista → mostra TODOS os itens dela
        const mostrar = casouARota ? (r.itens || []) : itens;
        achados.rotas.push({ rota: r.id, motorista: r.motorista, status: r.status,
          criadaEm: r.criadaEm || r.em, finalizadaEm: r.finalizadaEm || null,
          veiculo: r.veiculo || r.placa || null, telefoneMotorista: r.telMotorista || null,
          totalItens: (r.itens || []).length,
          itens: mostrar.map(i => ({ cliente: i.cliente, equipamento: i.equipamento,
            tel: i.tel, endereco: i.endereco || null,
            situacao: i.entregue ? 'entregue' : (i.status || 'pendente'),
            obs: i.obs || i.motivo || null })) });
      }
    }
    // TAREFAS — todos os estados
    for (const t of (db.tarefas || [])) {
      if (casa(t.cliente) || casa(t.tel) || casa(t.equipamento) || casa(t.cardId) || casa(t.modelo)) {
        achados.tarefas.push({ id: t.id, tipo: t.tipo, cliente: t.cliente, equipamento: t.equipamento,
          status: t.status, destino: t.destino, feitoPor: t.feitoPor, feitoEm: t.feitoEm });
      }
    }
    // CARDS vivos e arquivados
    for (const [b, onde] of [[ppA, 'pipe ADM'], [ppT, 'pipe TV'], [arqA, 'arquivo ADM'], [arqT, 'arquivo TV']]) {
      for (const c of (((b || {}).cards) || [])) {
        if (casa(c.nomeContato) || casa(c.telefone) || casa(c.equipamento) || casa(c.id)) {
          achados.cards.push({ onde, id: c.id, nome: c.nomeContato, equipamento: c.equipamento,
            fase: c.phaseId || c.phase, valor: c.valor, movidoEm: c.movedAt });
        }
      }
    }
    // FICHAS da logística
    for (const [b, onde] of [[lgA, 'logística ADM'], [lgT, 'logística TV']]) {
      for (const f of (((b || {}).fichas) || [])) {
        if (casa(f.nome) || casa(f.telefone) || casa(f.equipamento)) {
          achados.fichas.push({ onde, id: f.id, nome: f.nome, equipamento: f.equipamento,
            fase: f.phase, motorista: f.coletadoPor || f.motoristaNome || null, movidoEm: f.movedAt });
        }
      }
    }
    // MOVIMENTOS registrados
    for (const m of (((movs || {}).movs) || []).slice(0, 3000)) {
      if (casa(m.ficha) || casa(m.cliente) || casa(m.fichaId) || casa(m.por)) {
        achados.movimentos.push({ quando: m.ts || m.em, modulo: m.modulo, acao: m.acao,
          de: m.de, para: m.para, por: m.por || m.gatilho });
      }
    }
    const total = Object.values(achados).reduce((s, a) => s + a.length, 0);
    return res.status(200).json({ ok: true, busca: q, total,
      resumo: Object.keys(achados).map(k => achados[k].length + ' ' + k).join(' · '),
      rotas: achados.rotas.slice(0, 10),
      tarefas: achados.tarefas.slice(0, 15),
      cards: achados.cards.slice(0, 10),
      fichas: achados.fichas.slice(0, 10),
      movimentos: achados.movimentos.slice(0, 20) });
  }

  // ── BADGE leve p/ o hub: pendentes (tarefas + rotas em separação) ──
  if (action === 'badge') {
    const pend = (db.tarefas || []).filter(t => t.status === 'pendente' || t.status === 'falha').length;
    const rdbB = (await dbGet('reparoeletro_almox_rotas')) || { rotas: db.rotas || [] };
    const rotasSep = (rdbB.rotas || []).filter(r => r.status !== 'finalizada').length;
    return res.status(200).json({ ok: true, pendentes: pend + rotasSep });
  }

  // ── LIMPAR ENXURRADA: remove tarefas 'mover' pendentes criadas nas últimas N horas (padrão 3) ──
  if (action === 'limpar-enxurrada') {
    const antes = db.tarefas.length;
    // remove TODAS as tarefas de movimentação pendentes (a enxurrada inteira, sem depender de data)
    db.tarefas = db.tarefas.filter(t => !(t.tipo === 'mover' && t.status === 'pendente'));
    const removidas = antes - db.tarefas.length;
    // recriar tarefa legítima específica (?recriar=CARD_ID)
    let recriada = null;
    const rid = req.query.recriar;
    if (rid) {
      try {
        const pdb = await dbGet('reparoeletro_pipe');
        const c = pdb && (pdb.cards || []).find(x => x.id === rid);
        if (c) {
          db.tarefas.unshift(novaTarefa({ tipo: 'mover', cardId: c.id,
            cliente: c.nomeContato || '—', tel: c.telefone || '', equipamento: c.equipamento || '',
            origem: 'aguardando_aprovacao', destino: c.fase || c.phase }));
          recriada = c.id + ' (' + (c.nomeContato || '') + ' → ' + c.phase + ')';
        }
      } catch (e) {}
    }
    await dbSet(KEY, db);
    return res.status(200).json({ ok: true, removidas, recriada });
  }

  // ── RECONCILIAR-DIA: confere movimentos reais do pipe (movedAt) x tarefas existentes ──
  // Sem &aplicar=1: só lista (dry-run). Com &aplicar=1: recria as que faltam.
  if (action === 'reconciliar-dia') {
    const horas = parseFloat(req.query.h || '18');
    const corte = Date.now() - horas * 3600 * 1000;
    const FASES_FISICAS = ['aprovados', 'ultima_chamada', 'descarte'];
    // ⚠️ os cards guardam a fase em phaseId (e alguns em phase) — ler só c.phase fazia a
    // reconciliação NUNCA encontrar nada, que é por que o caso Adriano passou batido.
    const [pdb, pdbTv] = await Promise.all([dbGet('reparoeletro_pipe'), dbGet('tv_pipe')]);
    const faltam = [];
    for (const [banco, sis] of [[pdb, 'adm'], [pdbTv, 'tv']]) {
      for (const c of ((banco && banco.cards) || [])) {
        const fase = c.phaseId || c.phase;
        if (!FASES_FISICAS.includes(fase)) continue;
        const mv = new Date(c.movedAt || 0).getTime();
        if (!mv || mv < corte) continue;
        const tem = db.tarefas.some(t => t.cardId === c.id && t.destino === fase);
        if (!tem) faltam.push({ id: c.id, cliente: c.nomeContato || '—', tel: c.telefone || '',
          equipamento: c.equipamento || '', sistema: sis, fase, movidoEm: c.movedAt });
      }
    }
    if (req.query.aplicar === '1') {
      for (const f of faltam) {
        // usa os dados já resolvidos na varredura — cobre ADM e TV, e a fase vem correta
        db.tarefas.unshift(novaTarefa({ tipo: 'mover', cardId: f.id,
          cliente: f.cliente || '—', tel: f.tel || '', equipamento: f.equipamento || '',
          origem: 'aguardando_aprovacao', destino: f.fase, reconciliada: true }));
      }
      await dbSet(KEY, db);
      return res.status(200).json({ ok: true, recriadas: faltam.length, lista: faltam });
    }
    return res.status(200).json({ ok: true, modo: 'dry-run (nada foi criado)', faltam: faltam.length, lista: faltam });
  }

  // ── FAXINA: identifica e resolve tarefas revertidas pela corrida / duplicadas / órfãs ──
  // Sem &aplicar=1: só lista. Com &aplicar=1: executa.
  if (action === 'faxina') {
    const aplicar = req.query.aplicar === '1';
    const relatorio = { orfas_coleta: [], orfas_pipe: [], duplicadas: [] };
    try {
      const [pdb, ldb] = await Promise.all([dbGet('reparoeletro_pipe'), dbGet('reparoeletro_logistica')]);
      const emColeta = new Set(((ldb && ldb.fichas) || []).filter(f => f.phase === 'coleta_efetuada').map(f => f.id));
      const fasePipe = {};
      ((pdb && pdb.cards) || []).forEach(c => { fasePipe[c.id] = c.phase; });
      const vistos = new Set();
      const manter = [];
      for (const t of db.tarefas) {
        // duplicada pendente (mesmo card+tipo+destino): mantém a primeira (mais recente, lista é unshift)
        const chave = t.cardId + '|' + t.tipo + '|' + (t.destino || '');
        if (t.status === 'pendente' && vistos.has(chave)) {
          relatorio.duplicadas.push({ id: t.id, cliente: t.cliente, tipo: t.tipo });
          if (!aplicar) manter.push(t);
          continue; // aplicar: exclui
        }
        vistos.add(chave);
        // receber órfã: ficha não está mais em coleta_efetuada → conclui como Sistema
        if (t.tipo === 'receber' && t.status === 'pendente' && !emColeta.has(t.cardId)) {
          relatorio.orfas_coleta.push({ id: t.id, cliente: t.cliente });
          if (aplicar) { t.status = 'feito'; t.feitoPor = 'Sistema (faxina)'; t.feitoEm = new Date().toISOString(); t.autoConcluida = 'ficha já saiu de Coleta Efetuada'; }
        }
        // mover órfã: card não está mais na fase destino → conclui como Sistema
        if (t.tipo === 'mover' && t.status === 'pendente' && fasePipe[t.cardId] && fasePipe[t.cardId] !== t.destino) {
          relatorio.orfas_pipe.push({ id: t.id, cliente: t.cliente, faseAtual: fasePipe[t.cardId] });
          if (aplicar) { t.status = 'feito'; t.feitoPor = 'Sistema (faxina)'; t.feitoEm = new Date().toISOString(); t.autoConcluida = 'card já saiu da fase ' + t.destino; }
        }
        manter.push(t);
      }
      if (aplicar) { db.tarefas = manter; await dbSet(KEY, db); }
    } catch (e) { return res.status(200).json({ ok: false, error: String(e).slice(0, 120) }); }
    return res.status(200).json({ ok: true, aplicado: aplicar,
      resumo: { orfas_coleta: relatorio.orfas_coleta.length, orfas_pipe: relatorio.orfas_pipe.length, duplicadas_excluidas: relatorio.duplicadas.length },
      detalhes: relatorio });
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
    // segurança: se o id estiver duplicado, não adivinha — avisa
    try {
      const iguais = (db.tarefas || []).filter(x => x.id === (req.body || {}).id);
      if (iguais.length > 1) return res.status(409).json({ ok: false,
        error: 'identificador duplicado no sistema — rode reparar-ids antes de continuar' });
    } catch (e) {}
    try { const mm = (req.body || {}).modelo; if (mm) { const tf = db.tarefas.find(x => x.id === (req.body || {}).id); if (tf) { tf.modelo = String(mm).trim(); } } } catch (e) {}
    const { id, dataUrl } = req.body || {};
    const t = db.tarefas.find(x => x.id === id);
    if (!t) return res.status(404).json({ ok: false, error: 'tarefa não encontrada' });
    if (!dataUrl || String(dataUrl).length > 250000) {
      return res.status(400).json({ ok: false, error: 'foto ausente ou grande demais' });
    }
    // se a tarefa não tiver cardId (acontece em análise de compra sem card no pipe),
    // grava pelo id da própria tarefa — antes a chave virava 'alm_foto_undefined'
    const chaveFoto = 'alm_foto_' + (t.cardId || t.id);
    if (!t.cardId) t.cardId = t.id;
    await dbSet(chaveFoto, { em: new Date().toISOString(), img: dataUrl });
    // SAVE ATÔMICO: relê e marca só esta tarefa (o sync roda em paralelo)
    const dbF = (await dbGet(KEY)) || db;
    const tF = (dbF.tarefas || []).find(x => x.id === id);
    if (tF) { tF.temFoto = true; if (!tF.cardId) tF.cardId = tF.id;
      if ((req.body || {}).modelo) tF.modelo = String((req.body || {}).modelo).trim(); }
    await dbSet(KEY, dbF);
    // Se for análise de compra, avisa o card do Conflitos Bot que a foto chegou
    if (t.tipo === 'avaliar-compra' && t.origemConflito) {
      try {
        const KFC = (process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim();
        await fetch(`https://reparoeletroadm.com/api/prospeccao?action=marcar-foto&k=${KFC}`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ id: t.origemConflito, cardId: t.cardId }),
        });
      } catch (e) {}
    }
    return res.status(200).json({ ok: true, temFoto: true });
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
