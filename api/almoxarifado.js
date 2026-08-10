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

  // 📺 é SÓ TV? (uma ou várias, sem nenhum equipamento de ADM junto)
  function soTv(txt) {
    const s = String(txt || '').toLowerCase();
    if (!/\btvs?\b|televis|\bled\b|polegada|barramento/.test(s)) return false;
    return !/micro-?\s?ondas|microondas|purificador|bebedouro|filtro|adega|cervejeir|forno|forninho|frigobar|b\.?\s?blend/.test(s);
  }
  function pushTarefa(lista, t) { if (t && Array.isArray(lista)) lista.unshift(t); return t; }
  function novaTarefa(t) {
    // 🚫 BARREIRA ÚNICA — todos os pontos que criam tarefa passam por aqui.
    // Proteger só o sync não bastava: havia 8 caminhos, incluindo 'receber' da coleta efetuada.
    if (soTv((t || {}).equipamento) || soTv((t || {}).descricao)) return null;
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
      // 📺 TV NÃO ENTRA NO ALMOXARIFADO DO ADM — TV tem separação própria.
      // Alguns cards de TV acabam no pipe ADM e geravam tarefa aqui.
      const idsTvSync = new Set();
      try {
        const ppTvS = (await dbGet('tv_pipe')) || { cards: [] };
        for (const c of (ppTvS.cards || [])) idsTvSync.add(String(c.id));
        const lgTvS = (await dbGet('tv_logistica')) || { fichas: [] };
        for (const f of (lgTvS.fichas || [])) idsTvSync.add(String(f.id));
      } catch (e) {}
      // 📺 só barra quando for SÓ TV. Atendimento misto (TV + equipamento ADM) entra
      // normalmente, porque a parte de ADM precisa de separação aqui.
      const ehTvPeloNome = (c) => {
        const txt = String((c.equipamento || '') + ' ' + (c.descricao || '')).toLowerCase();
        const temTv = /\btvs?\b|televis|\bled\b|polegada|barramento/.test(txt);
        if (!temTv) return false;
        const temAdm = /micro-?\s?ondas|microondas|purificador|bebedouro|filtro|adega|cervejeir|forno|forninho|frigobar|b\.?\s?blend/.test(txt);
        return !temAdm;                       // só barra se NÃO houver equipamento de ADM junto
      };
      let candidatos = [];
      for (const c of ((pipe && pipe.cards) || [])) {
        novoSnapPipe[c.id] = c.phase;
        if (!GATILHOS.hasOwnProperty(c.phase)) continue;
        if (idsTvSync.has(String(c.id)) || ehTvPeloNome(c)) continue;   // 📺 TV fora
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
            pushTarefa(db.tarefas, novaTarefa({
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
            pushTarefa(db.tarefas, novaTarefa({
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
            pushTarefa(db.tarefas, novaTarefa({ tipo: 'loja-reprovado', cardId: f.id,
              cliente: f.nomeContato || '—', tel: f.telefone || '', equipamento: f.equipamento || '',
              origem: 'Frente de Loja', destino: 'aguardando_retirada' }));
          }
        }
        s.f2.fl = novoFl;

        // Vendas + Checkout → tarefa venda (2 checks)
        const vendaTarefa = (v, orig) => {
          // 🚫 venda sem identificador gerava Math.random() a cada varredura, criando uma
          // tarefa NOVA toda vez — era a venda fantasma que aparecia diariamente sem dados.
          const idReal = v.id || v.vendaId || v.createdAt;
          if (!idReal) return;
          const cliente = String(v.nomeCliente || v.cliente || v.nome || '').trim();
          const equip = String(v.equipamento || v.descricao || v.titulo || '').trim();
          // sem cliente E sem equipamento não há o que separar — provável registro incompleto
          if (!cliente && !equip) return;
          const cid = orig + '-' + idReal;
          if (!jaTarefa(cid, 'venda')) pushTarefa(db.tarefas, novaTarefa({ tipo: 'venda', cardId: cid,
            cliente: cliente || '—', tel: v.telefone || v.tel || '',
            equipamento: equip || '—',
            valor: parseFloat(v.valor || v.total || 0) || null,
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
              pushTarefa(db.tarefas, novaTarefa({ tipo: 'avaliar-compra', cardId: f.id,
                cliente: f.nomeContato || f.cliente || '—', tel: f.telefone || '', equipamento: f.equipamento || f.descricao || '—',
                origem: 'Compra Equip', destino: 'analise' }));
            }
          }
          if (f.status === 'comprado') {
            novoComp.push(f.id);
            if (!primeira && !s.f2.ceComp.includes(f.id) && !jaTarefa(f.id, 'levar-area')) {
              pushTarefa(db.tarefas, novaTarefa({ tipo: 'levar-area', cardId: f.id,
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

  // ── 🧹 LIMPAR-VENDAS-FANTASMA: tarefas de venda sem cliente e sem equipamento ──
  if (action === 'limpar-vendas-fantasma') {
    const dbV = (await dbGet(KEY)) || { tarefas: [] };
    const vazia = t => t.tipo === 'venda' && t.status === 'pendente' &&
      (!t.cliente || t.cliente === '—') && (!t.equipamento || t.equipamento === '—');
    const alvo = (dbV.tarefas || []).filter(vazia);
    if (String(req.query.aplicar || '') === '1' && alvo.length) {
      const ids = new Set(alvo.map(t => t.id));
      dbV.tarefas = dbV.tarefas.filter(t => !ids.has(t.id));
      await dbSet(KEY, dbV);
      return res.status(200).json({ ok: true, removidas: alvo.length,
        lista: alvo.map(t => t.id + ' | ' + (t.origem || '?') + ' | ' + String(t.criadoEm || '').slice(0, 16)) });
    }
    return res.status(200).json({ ok: true, encontradas: alvo.length,
      lista: alvo.map(t => t.id + ' | ' + (t.origem || '?') + ' | ' + String(t.criadoEm || '').slice(0, 16)),
      dica: 'para remover: &aplicar=1' });
  }

  // ── 🔎 COLETADOS-SEM-TAREFA: fichas coletadas que nunca chegaram ao almoxarifado ──
  if (action === 'coletados-sem-tarefa') {
    const d8 = t => String(t || '').replace(/\D/g, '').slice(-8);
    const [db, logA] = await Promise.all([dbGet(KEY), dbGet('reparoeletro_logistica')]);
    const tarefas = ((db || {}).tarefas || []);
    const comTarefa = new Set(tarefas.map(t => d8(t.telefone)).filter(x => x.length >= 8));
    const alvo = (((logA || {}).fichas) || []).filter(f =>
      ['coleta_efetuada', 'orc_registrado'].includes(String(f.phase || '')));
    const faltando = alvo.filter(f => !comTarefa.has(d8(f.telefone)));
    if (String(req.query.aplicar || '') === '1') {
      const KTF = (process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim();
      const feitos = [], erros = [];
      for (const f of faltando) {
        const r = await fetch('https://reparoeletroadm.com/api/almoxarifado?action=criar-mover&k=' + KTF, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ id: f.id, cliente: f.nome, telefone: f.telefone,
            equipamento: f.equipamento, origem: 'recuperação — coletada sem tarefa' }),
        }).then(x => x.json()).catch(e => ({ error: e.message }));
        if (r && r.ok) feitos.push(f.nome); else erros.push(f.nome + ': ' + ((r && r.error) || '?'));
        await new Promise(s => setTimeout(s, 200));
      }
      return res.status(200).json({ ok: erros.length === 0, criadas: feitos.length, feitos, erros });
    }
    return res.status(200).json({ ok: faltando.length === 0,
      coletadasNaLogistica: alvo.length,
      semTarefaNoAlmoxarifado: faltando.length,
      LISTA: faltando.map(f => String(f.nome || '?').slice(0, 24) + ' | ' +
        String(f.equipamento || '').slice(0, 22) + ' | ' + f.phase +
        ' | desde ' + String(f.movedAt || '').slice(5, 10)),
      dica: faltando.length ? 'para criar todas: &aplicar=1' : undefined });
  }

  // ── 🧯 CORRIGIR-TAREFA: troca tarefa do tipo errado pelo card de diagnóstico ──
  if (action === 'corrigir-tarefa') {
    const so = String(req.query.so || '').split(',').map(x => x.trim().replace(/\D/g, '')).filter(Boolean);
    if (!so.length) return res.status(400).json({ ok: false, error: 'informe ?so=4dig,4dig' });
    const db = (await dbGet(KEY)) || { tarefas: [] };
    const log = (await dbGet('reparoeletro_logistica')) || { fichas: [] };
    const d8 = t => String(t || '').replace(/\D/g, '');
    const alvos = (log.fichas || []).filter(f =>
      so.some(s => d8(f.telefone).endsWith(s)));
    // o que existe hoje para esses telefones
    const erradas = (db.tarefas || []).filter(t =>
      so.some(s => d8(t.tel).endsWith(s)) && t.tipo !== 'receber');
    if (String(req.query.aplicar || '') !== '1') {
      return res.status(200).json({ ok: true, modo: 'prévia',
        fichas: alvos.length,
        tarefasDoTipoErrado: erradas.length,
        L: erradas.map(t => (t.cliente || '?') + ' | tipo atual: ' + t.tipo + ' → vai virar: receber'),
        criara: alvos.filter(f => !(db.tarefas || []).some(t => t.cardId === f.id && t.tipo === 'receber'))
          .map(f => String(f.nome || '?').slice(0, 18) + ' | ' + String(f.equipamento || '').slice(0, 20)),
        dica: 'para aplicar: &aplicar=1' });
    }
    // remove as erradas
    const antes = (db.tarefas || []).length;
    db.tarefas = (db.tarefas || []).filter(t =>
      !(so.some(s => d8(t.tel).endsWith(s)) && t.tipo !== 'receber'));
    const removidas = antes - db.tarefas.length;
    // cria o card de diagnóstico, no mesmo molde do fluxo normal
    const criadas = [];
    for (const f of alvos) {
      if (db.tarefas.some(t => t.cardId === f.id && t.tipo === 'receber')) continue;
      pushTarefa(db.tarefas, novaTarefa({
        tipo: 'receber', cardId: f.id,
        cliente: f.nome || '—', tel: f.telefone || '', equipamento: f.equipamento || '',
        defeito: f.defeito || '', obs: f.obs || '',
        origem: 'coleta_efetuada', destino: 'aguardando_aprovacao',
        modelo: '', temFoto: false,
      }));
      criadas.push(f.nome);
    }
    await dbSet(KEY, db);
    return res.status(200).json({ ok: true, removidas, criadas: criadas.length, lista: criadas,
      observacao: 'card de diagnóstico criado — com foto, modelo, RS, remarcar e não chegou' });
  }

  // ── 💾 PROCURAR-BACKUP: há cópia do banco com os comprados? ──
  if (action === 'procurar-backup') {
    const CANDIDATAS = [
      'reparoeletro_compra_equip_backup', 'reparoeletro_compra_equip_bak',
      'reparoeletro_compra_equip_old', 'reparoeletro_compra_equip_snapshot',
      'compra_equip_backup', 'almox_snapshot', 'reparoeletro_almoxarifado_snapshot',
      'reparoeletro_almoxarifado',
    ];
    const achados = [];
    for (const k of CANDIDATAS) {
      try {
        const d = await dbGet(k);
        if (!d) { achados.push(k + ' → não existe'); continue; }
        let info = k + ' → existe';
        for (const L of ['fichas', 'cards', 'tarefas', 'itens']) {
          if (Array.isArray(d[L])) {
            const comp = d[L].filter(f => String(f.status || '') === 'comprado').length;
            info += ' | ' + L + ': ' + d[L].length + (comp ? ' (COMPRADOS: ' + comp + ')' : '');
          }
        }
        if (d.f2) info += ' | tem snapshot f2';
        achados.push(info);
      } catch (e) { achados.push(k + ' → erro'); }
    }
    // o snapshot do almoxarifado guarda os ids que já foram vistos como comprados
    const almox = await dbGet('reparoeletro_almoxarifado');
    const snap = almox && almox.snapshot && almox.snapshot.f2;
    return res.status(200).json({ ok: true, BANCOS: achados,
      SNAPSHOT_ALMOXARIFADO: snap
        ? { idsEmAnalise: (snap.ceAna || []).length, idsComprados: (snap.ceComp || []).length,
            amostraComprados: (snap.ceComp || []).slice(0, 10) }
        : 'sem snapshot',
      observacao: 'ceComp guarda os ids que o almoxarifado viu como COMPRADOS — pode servir para restaurar o status' });
  }

  // ── 🕐 QUANDO-MUDOU: quando cada ficha da Compra Equip foi criada/alterada ──
  if (action === 'quando-mudou') {
    const ceq = (await dbGet('reparoeletro_compra_equip')) || { fichas: [] };
    const hh = d => d ? new Date(new Date(d).getTime() - 3 * 3600000).toISOString().slice(5, 16).replace('T', ' ') : '(sem data)';
    const fichas = ceq.fichas || [];
    const porCriacao = {};
    for (const f of fichas) {
      const k = String(f.criadoEm || f.createdAt || '').slice(0, 10) || '(sem data)';
      porCriacao[k] = (porCriacao[k] || 0) + 1;
    }
    // rajada por minuto
    const porMin = {};
    for (const f of fichas) { const m = hh(f.criadoEm || f.createdAt); porMin[m] = (porMin[m] || 0) + 1; }
    const rajadas = Object.entries(porMin).filter(([, n]) => n >= 5).sort((a, b) => b[1] - a[1]);
    // campos que indicam alteração de status
    const comHistorico = fichas.filter(f => f.compradoEm || f.statusEm || f.atualizadoEm).length;
    return res.status(200).json({ ok: true,
      total: fichas.length,
      POR_DIA_DE_CRIACAO: Object.fromEntries(Object.entries(porCriacao).sort()),
      RAJADAS_POR_MINUTO: rajadas.length ? rajadas.map(([m, n]) => m + ' → ' + n) : 'nenhuma',
      comCarimboDeStatus: comHistorico,
      camposDeUmaFicha: fichas[0] ? Object.keys(fichas[0]) : [],
      amostra: fichas.slice(0, 8).map(f => hh(f.criadoEm || f.createdAt) + ' | ' +
        String(f.nomeContato || '?').slice(0, 18) + ' | ' + f.status) });
  }

  // ── ⏪ QUEM-VOLTOU: cards que saíram de equipamento_comprado para trás ──
  if (action === 'quem-voltou') {
    const pp = (await dbGet('reparoeletro_pipe')) || { cards: [] };
    const hh = d => d ? new Date(new Date(d).getTime() - 3 * 3600000).toISOString().slice(5, 16).replace('T', ' ') : '?';
    const voltaram = [];
    for (const c of (pp.cards || [])) {
      const atual = String(c.phaseId || c.phase || '');
      if (atual === 'equipamento_comprado') continue;
      const hist = (c.history || []).map(x => ({
        f: String(x.phase || x.phaseId || ''), t: new Date(x.ts || x.timestamp || 0).getTime(),
      })).filter(x => x.t).sort((a, b) => a.t - b.t);
      const passou = hist.some(x => x.f === 'equipamento_comprado');
      if (!passou) continue;
      voltaram.push({ id: c.id, nome: c.nomeContato,
        tel: String(c.telefone || '').slice(-4),
        equipamento: String(c.equipamento || c.descricao || '').slice(0, 20),
        faseAgora: atual, movidoEm: c.movedAt,
        ultimaEtapa: hist.slice(-1)[0] ? hist.slice(-1)[0].f : '?' });
    }
    voltaram.sort((a, b) => String(b.movidoEm).localeCompare(String(a.movidoEm)));
    // agrupa por minuto para achar movimentação em lote
    const porMin = voltaram.reduce((o, v) => { const m = hh(v.movidoEm); o[m] = (o[m] || 0) + 1; return o; }, {});
    const rajadas = Object.entries(porMin).filter(([, n]) => n >= 5).sort((a, b) => b[1] - a[1]);
    return res.status(200).json({ ok: true,
      aindaEmComprado: (pp.cards || []).filter(c => String(c.phaseId || c.phase) === 'equipamento_comprado').length,
      voltaramParaTras: voltaram.length,
      porFaseAtual: voltaram.reduce((o, v) => { o[v.faseAgora] = (o[v.faseAgora] || 0) + 1; return o; }, {}),
      RAJADAS: rajadas.length ? rajadas.map(([m, n]) => m + ' → ' + n + ' card(s)') : 'nenhuma',
      L: voltaram.slice(0, 60).map(v => hh(v.movidoEm) + ' | ' + String(v.nome || '?').slice(0, 18) +
        ' ' + v.tel + ' | → ' + v.faseAgora) });
  }

  // ── 🔬 AUDITORIA-COMPRA: procura os equipamentos comprados em TODOS os bancos ──
  if (action === 'auditoria-compra') {
    const CHAVES = ['reparoeletro_compra_equip', 'compra_equipamentos', 'reparoeletro_equipamentos',
      'equipamentos_comprados', 'reparoeletro_compra_equip_bkp', 'reparoeletro_compra_equip_archive',
      'compra_equip', 'reparoeletro_arquivo', 'reparoeletro_pipe'];
    const achados = [];
    for (const k of CHAVES) {
      try {
        const d = await dbGet(k);
        if (!d) { achados.push({ chave: k, existe: false }); continue; }
        const listas = ['fichas', 'cards', 'itens', 'equipamentos'];
        for (const L of listas) {
          if (!Array.isArray(d[L])) continue;
          const porStatus = d[L].reduce((o, f) => {
            const s = String(f.status || f.phase || f.phaseId || '(sem)'); o[s] = (o[s] || 0) + 1; return o; }, {});
          achados.push({ chave: k, lista: L, total: d[L].length,
            porStatus: Object.fromEntries(Object.entries(porStatus).sort((a, b) => b[1] - a[1]).slice(0, 8)),
            campos: Object.keys(d) });
        }
      } catch (e) { achados.push({ chave: k, erro: e.message }); }
    }
    // procura nomes específicos da lista de comprados que o Pedro enviou
    const PROCURAR = ['9469', '9422', '8144', '6556', '6108', '1115', '3881', '3457', '0217', '7004'];
    const ondeEstao = {};
    for (const k of CHAVES) {
      try {
        const d = await dbGet(k);
        if (!d) continue;
        for (const L of ['fichas', 'cards']) {
          if (!Array.isArray(d[L])) continue;
          for (const f of d[L]) {
            const tel = String(f.telefone || '').replace(/\D/g, '');
            for (const p of PROCURAR) {
              if (tel.endsWith(p)) {
                ondeEstao[p] = ondeEstao[p] || [];
                ondeEstao[p].push(k + '.' + L + ' | ' + String(f.status || f.phase || f.phaseId || '?'));
              }
            }
          }
        }
      } catch (e) {}
    }
    return res.status(200).json({ ok: true,
      BANCOS: achados.filter(a => a.total !== undefined || a.existe === false)
        .map(a => a.existe === false ? a.chave + ' → NÃO EXISTE'
          : a.chave + '.' + a.lista + ' → ' + a.total + ' | ' + JSON.stringify(a.porStatus)),
      AMOSTRA_DA_SUA_LISTA: Object.entries(ondeEstao).map(([p, v]) => p + ': ' + v.join(' · ')),
      naoEncontrados: PROCURAR.filter(p => !ondeEstao[p]) });
  }

  // ── 🔍 ESTADO-COMPRA-EQUIP: fotografia do banco, sem alterar nada ──
  if (action === 'estado-compra-equip') {
    const ceq = (await dbGet('reparoeletro_compra_equip')) || null;
    if (!ceq) return res.status(200).json({ ok: false,
      error: '🚨 a chave reparoeletro_compra_equip NÃO EXISTE no banco' });
    const fichas = ceq.fichas || [];
    const porStatus = fichas.reduce((o, f) => {
      const s = String(f.status || '(sem status)'); o[s] = (o[s] || 0) + 1; return o; }, {});
    const maisNova = fichas.map(f => f.criadoEm || f.em).filter(Boolean).sort().slice(-1)[0];
    const maisAntiga = fichas.map(f => f.criadoEm || f.em).filter(Boolean).sort()[0];
    return res.status(200).json({ ok: true,
      totalFichas: fichas.length,
      POR_STATUS: porStatus,
      camposDoBanco: Object.keys(ceq),
      maisAntiga, maisNova,
      amostraComprados: fichas.filter(f => String(f.status) === 'comprado')
        .slice(0, 15).map(f => String(f.nomeContato || f.cliente || '?').slice(0, 20) +
          ' | ' + String(f.equipamento || '').slice(0, 20)),
      amostraOutros: fichas.filter(f => String(f.status) !== 'comprado')
        .slice(0, 5).map(f => String(f.nomeContato || f.cliente || '?').slice(0, 18) + ' | ' + f.status) });
  }

  // ── 🧹 LIMPAR-COMPRA-EQUIP: remove tarefas que duplicam a seção Compra de Equipamentos ──
  if (action === 'limpar-compra-equip') {
    const db = (await dbGet(KEY)) || { tarefas: [] };
    const todas = db.tarefas || [];
    // as que vieram da sincronização do Compra Equip
    const alvo = todas.filter(t =>
      String(t.origem || '') === 'Compra Equip' &&
      ['avaliar-compra', 'levar-area'].includes(String(t.tipo || '')));
    const ficam = todas.filter(t => !alvo.includes(t));
    const porTipo = alvo.reduce((o, t) => { o[t.tipo] = (o[t.tipo] || 0) + 1; return o; }, {});
    const porStatus = alvo.reduce((o, t) => { o[t.status || '?'] = (o[t.status || '?'] || 0) + 1; return o; }, {});
    // 🔍 CRUZAMENTO: em que status cada uma está na seção Compra de Equipamentos
    const ceq = (await dbGet('reparoeletro_compra_equip')) || { fichas: [] };
    const porId = {};
    for (const f of (ceq.fichas || [])) porId[String(f.id)] = String(f.status || '?');
    const cruzado = alvo.reduce((o, t) => {
      const st = porId[String(t.cardId)] || '(não encontrada na Compra Equip)';
      o[st] = (o[st] || 0) + 1; return o;
    }, {});
    if (String(req.query.aplicar || '') !== '1') {
      return res.status(200).json({ ok: true, modo: 'PRÉVIA — nada removido',
        totalNoAlmoxarifado: todas.length,
        VAI_EXCLUIR: alvo.length, porTipo, porStatus,
        STATUS_NA_COMPRA_EQUIP: cruzado,
        ATENCAO: cruzado['analise']
          ? '⚠️ ' + cruzado['analise'] + ' estão EM ANÁLISE na Compra Equip, não em Comprados'
          : undefined,
        VAI_MANTER: ficam.length,
        mantidosPorOrigem: ficam.reduce((o, t) => { const k = String(t.origem || '?'); o[k] = (o[k] || 0) + 1; return o; }, {}),
        LISTA_EXCLUIR: alvo.slice(0, 60).map(t => String(t.cliente || '?').slice(0, 20) + ' | ' +
          (porId[String(t.cardId)] || 'sem ficha') + ' | ' + String(t.equipamento || '').slice(0, 16)),
        dica: 'para excluir: &aplicar=1' });
    }
    db.tarefas = ficam;
    await dbSet(KEY, db);
    return res.status(200).json({ ok: true,
      excluidas: alvo.length, restaram: ficam.length,
      observacao: 'as fichas continuam na seção Compra de Equipamentos — só as tarefas duplicadas saíram do almoxarifado' });
  }

  // ── 🔁 RECRIAR-FALTANTES: cria tarefa para quem está em coleta efetuada sem ela ──
  if (action === 'recriar-faltantes') {
    const db = (await dbGet(KEY)) || { tarefas: [] };
    const log = (await dbGet('reparoeletro_logistica')) || { fichas: [] };
    const d8 = t => String(t || '').replace(/\D/g, '').slice(-8);
    const temTarefa = new Set((db.tarefas || []).map(t => String(t.fichaId || t.id || '')));
    const temTel = new Set((db.tarefas || []).map(t => d8(t.telefone)).filter(x => x.length >= 8));
    // ?so=4dig,4dig — restringe a fichas específicas, para não recriar as demais
    const so = String(req.query.so || '').split(',').map(x => x.trim().replace(/\D/g, '')).filter(Boolean);
    const semTarefa = (log.fichas || []).filter(f =>
      String(f.phase || '') === 'coleta_efetuada' &&
      !temTarefa.has(String(f.id)) && !temTel.has(d8(f.telefone)) &&
      (!so.length || so.some(s => String(f.telefone || '').replace(/\D/g, '').endsWith(s))));
    if (String(req.query.aplicar || '') !== '1') {
      return res.status(200).json({ ok: true, modo: 'prévia',
        emColetaEfetuada: (log.fichas || []).filter(f => String(f.phase || '') === 'coleta_efetuada').length,
        semTarefa: semTarefa.length,
        L: semTarefa.slice(0, 30).map(f => String(f.nome || '?').slice(0, 18) + ' ' +
          String(f.telefone || '').slice(-4) + ' | ' + String(f.equipamento || '').slice(0, 18) +
          ' | ' + (f.almoxarifadoTarefa || 'gatilho nunca rodou')),
        dica: 'para criar: &aplicar=1' });
    }
    const KTF = (process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim();
    const feitos = [], erros = [];
    for (const f of semTarefa) {
      const r = await fetch('https://reparoeletroadm.com/api/almoxarifado?action=criar-mover&k=' + KTF, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ cardId: f.id, destino: 'aguardando_aprovacao',
          tipo: 'receber', cliente: f.nome, tel: f.telefone,
          equipamento: f.equipamento, defeito: f.defeito || '',
          origem: 'coleta_efetuada' }),
      }).then(x => x.json()).catch(e => ({ error: e.message }));
      if (r && r.ok) feitos.push(f.nome); else erros.push(f.nome + ': ' + ((r && r.error) || '?'));
      await new Promise(s => setTimeout(s, 150));
    }
    return res.status(200).json({ ok: erros.length === 0, criadas: feitos.length, feitos, erros });
  }

  // ── 🩺 POR-QUE-NAO-ENTROU: por que uma ficha coletada não chegou ao almoxarifado ──
  if (action === 'por-que-nao-entrou') {
    const q = String(req.query.q || '').replace(/\D/g, '');
    if (!q) return res.status(400).json({ ok: false, error: 'informe ?q=4dígitos' });
    const d8 = t => String(t || '').replace(/\D/g, '');
    const [db, logA, logT, pipeA, pipeT] = await Promise.all([
      dbGet(KEY), dbGet('reparoeletro_logistica'), dbGet('tv_logistica'),
      dbGet('reparoeletro_pipe'), dbGet('tv_pipe'),
    ]);
    const acha = (lista, onde) => (lista || [])
      .filter(f => d8(f.telefone).endsWith(q) || String(f.nome || f.nomeContato || '').toLowerCase().includes(String(req.query.q || '').toLowerCase()))
      .map(f => ({ onde, id: f.id, nome: f.nome || f.nomeContato,
        telefone: f.telefone, fase: f.phase || f.phaseId || f.status,
        equipamento: f.equipamento || f.descricao,
        almoxTarefa: f.almoxarifadoTarefa || null,
        coletadoEm: f.coletadoEm, movedAt: f.movedAt, sistema: f._sis }));
    const achados = [
      ...acha(((logA || {}).fichas), 'Logística ADM'),
      ...acha(((logT || {}).fichas), 'Logística TV'),
      ...acha(((pipeA || {}).cards), 'Pipe ADM'),
      ...acha(((pipeT || {}).cards), 'Pipe TV'),
    ];
    const tarefas = ((db || {}).tarefas || []).filter(t =>
      d8(t.telefone).endsWith(q) ||
      String(t.cliente || t.nome || '').toLowerCase().includes(String(req.query.q || '').toLowerCase()));
    const FASES_QUE_GERAM = ['coleta_efetuada', 'recebido', 'na_oficina'];
    const analise = achados.map(a => {
      const bloqueios = [];
      if (tarefas.length) bloqueios.push('✅ JÁ EXISTE tarefa no almoxarifado (' + tarefas.length + ')');
      else {
        if (!FASES_QUE_GERAM.includes(String(a.fase)))
          bloqueios.push('a fase "' + a.fase + '" não é uma das que geram tarefa (' + FASES_QUE_GERAM.join(', ') + ')');
        if (a.onde === 'Logística TV')
          bloqueios.push('ficha de TV — o almoxarifado ADM não recebe tarefas de TV');
        if (!a.coletadoEm)
          bloqueios.push('sem carimbo de coleta — pode ter sido movida pelo quadro, sem passar pela rota do motorista');
      }
      return { ...a, situacao: bloqueios.length ? bloqueios : ['nenhum bloqueio óbvio — o gatilho pode ter falhado'] };
    });
    // resposta enxuta, para caber no chat
    return res.status(200).json({ ok: true, q,
      temTarefa: tarefas.length,
      L: analise.map(a => a.onde + ' | ' + String(a.nome || '?').slice(0, 16) +
        ' | fase: ' + a.fase +
        ' | gatilho: ' + (a.almoxTarefa || 'NUNCA RODOU') +
        ' | ' + a.situacao[0].slice(0, 40)) });
  }

  // ── 🕵️ ORIGEM-TAREFAS: de onde vieram as tarefas do almoxarifado ──
  if (action === 'origem-tarefas') {
    const horas = Math.min(720, Math.max(1, parseInt(req.query.horas || '48', 10)));
    const corte = Date.now() - horas * 3600000;
    const hh = d => d ? new Date(new Date(d).getTime() - 3 * 3600000).toISOString().slice(5, 16).replace('T', ' ') : '?';
    const db = (await dbGet(KEY)) || { tarefas: [] };
    const todas = db.tarefas || [];
    const recentes = todas.filter(t => {
      const q = new Date(t.criadaEm || t.criadoEm || t.em || 0).getTime();
      return q && q >= corte;
    });
    // agrupa por origem, tipo e minuto de criação (rajada = criação automática)
    const porOrigem = {}, porTipo = {}, porMinuto = {};
    for (const t of recentes) {
      const o = String(t.origem || t.via || '(não informada)');
      porOrigem[o] = (porOrigem[o] || 0) + 1;
      const tp = String(t.tipo || t.acao || '?');
      porTipo[tp] = (porTipo[tp] || 0) + 1;
      const min = hh(t.criadaEm || t.criadoEm || t.em);
      porMinuto[min] = (porMinuto[min] || 0) + 1;
    }
    // rajadas: minutos com 5+ tarefas indicam criação em lote
    const rajadas = Object.entries(porMinuto).filter(([, n]) => n >= 5)
      .sort((a, b) => b[1] - a[1]);
    return res.status(200).json({ ok: true, periodoHoras: horas,
      totalNoAlmoxarifado: todas.length,
      criadasNoPeriodo: recentes.length,
      POR_ORIGEM: porOrigem,
      POR_TIPO: porTipo,
      RAJADAS: rajadas.length
        ? rajadas.map(([m, n]) => m + ' → ' + n + ' tarefa(s) no mesmo minuto')
        : 'nenhuma — criadas de forma espaçada',
      DIAGNOSTICO: rajadas.length
        ? '⚠️ há criação em LOTE — ' + rajadas[0][1] + ' tarefas num único minuto (' + rajadas[0][0] + '), típico de rotina automática ou importação'
        : '✅ criação espaçada, compatível com uso manual',
      AMOSTRA: recentes.slice(0, 40).map(t => hh(t.criadaEm || t.criadoEm || t.em) + ' | ' +
        String(t.tipo || t.acao || '?').padEnd(18) + ' | ' + String(t.cliente || t.nome || '?').slice(0, 20) +
        ' | ' + String(t.equipamento || '').slice(0, 20) +
        ' | origem: ' + String(t.origem || t.via || '(não informada)')) });
  }

  // ── 🧹 LIMPAR-TAREFAS-TV: remove do almoxarifado ADM tarefas de cards de TV ──
  if (action === 'limpar-tarefas-tv') {
    // varre TODAS as origens de TV e TODOS os estados — a versão anterior só olhava
    // o tv_pipe e só removia pendentes, então concluída de TV ficava com "reabrir tarefa"
    const [db2, ppTv2, lgTv2, arqTv2] = await Promise.all([
      dbGet(KEY), dbGet('tv_pipe'), dbGet('tv_logistica'), dbGet('tv_arquivo'),
    ]);
    const idsTv = new Set();
    for (const c of (((ppTv2 || {}).cards) || [])) idsTv.add(String(c.id));
    for (const f of (((lgTv2 || {}).fichas) || [])) idsTv.add(String(f.id));
    for (const c of (((arqTv2 || {}).cards) || [])) idsTv.add(String(c.id));
    const alvo = (((db2 || {}).tarefas) || []).filter(t => {
      if (idsTv.has(String(t.cardId))) return true;
      return soTv(t.equipamento) || soTv(t.descricao);   // e pelo texto do equipamento
    });
    if (String(req.query.aplicar || '') === '1' && alvo.length) {
      const ids = new Set(alvo.map(t => t.id));
      db2.tarefas = db2.tarefas.filter(t => !ids.has(t.id));
      await dbSet(KEY, db2);
      return res.status(200).json({ ok: true, removidas: alvo.length,
        lista: alvo.map(t => t.cliente + ' | ' + (t.equipamento || '') + ' | ' + (t.status || '?')) });
    }
    const porStatus = alvo.reduce((o, t) => { const s = t.status || '?'; o[s] = (o[s] || 0) + 1; return o; }, {});
    return res.status(200).json({ ok: true, encontradas: alvo.length,
      porStatus,
      lista: alvo.map(t => t.cliente + ' | ' + (t.equipamento || '') + ' | ' + (t.status || '?') + ' | ' + (t.destino || '')),
      dica: 'para remover: &aplicar=1' });
  }

  // ── 🔁 CRIAR-MOVER: recria a tarefa de movimentação que o gatilho perdeu ──
  if (req.method === 'POST' && action === 'criar-mover') {
    const b = req.body || {};
    if (!b.cardId || !b.destino) return res.status(400).json({ ok: false, error: 'cardId e destino obrigatórios' });
    // 🚫 TV não entra no almoxarifado do ADM — tem separação própria.
    // Barreira final: vale para qualquer origem que tente criar a tarefa.
    try {
      const [ppTv, lgTv] = await Promise.all([dbGet('tv_pipe'), dbGet('tv_logistica')]);
      const noTv = ((ppTv || {}).cards || []).some(c => c.id === b.cardId)
        || ((lgTv || {}).fichas || []).some(f => f.id === b.cardId);
      const txt = String(b.equipamento || '').toLowerCase();
      const temTv = /\btvs?\b|televis|\bled\b|polegada|barramento/.test(txt);
      const temAdm = /micro-?\s?ondas|microondas|purificador|bebedouro|filtro|adega|cervejeir|forno|forninho|frigobar|b\.?\s?blend/.test(txt);
      if (noTv || (temTv && !temAdm)) {
        return res.status(200).json({ ok: true, ignorado: true, motivo: 'TV não entra no almoxarifado ADM' });
      }
    } catch (e) {}
    const dbM = (await dbGet(KEY)) || defaultDB();
    if (!Array.isArray(dbM.tarefas)) dbM.tarefas = [];
    if (!dbM.config) dbM.config = { proximoNum: 1 };
    const ja = dbM.tarefas.some(t => t.cardId === b.cardId && t.destino === b.destino && t.status === 'pendente');
    if (ja) return res.status(200).json({ ok: true, dedupe: true, msg: 'tarefa já existe pendente' });
    pushTarefa(dbM.tarefas, novaTarefa({
      tipo: b.tipo === 'receber' ? 'receber' : 'mover', cardId: b.cardId,
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
    pushTarefa(db.tarefas, novaTarefa({
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
    const { rotaId, motorista, placa, telMotorista, fotoB64, feitoPor } = req.body || {};
    const rdb3 = (await dbGet('reparoeletro_almox_rotas')) || { rotas: [] };
    const rt3 = (rdb3.rotas || []).find(r => r.id === rotaId);
    if (!rt3) return res.status(404).json({ ok: false, error: 'rota não encontrada' });
    // 🚛 os três dados do motorista são obrigatórios — sem eles a rota não fecha
    if (!motorista || String(motorista).trim().length < 3) {
      return res.status(400).json({ ok: false, error: 'informe o NOME do motorista' });
    }
    const placaLimpa = String(placa || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
    if (placaLimpa.length < 7) {
      return res.status(400).json({ ok: false, error: 'informe a PLACA do veículo (7 caracteres, ex: ABC1D23)' });
    }
    const telLimpo = String(telMotorista || '').replace(/\D/g, '');
    if (telLimpo.length < 10) {
      return res.status(400).json({ ok: false, error: 'informe o TELEFONE do motorista com DDD' });
    }
    if (!fotoB64) return res.status(400).json({ ok: false, error: 'foto do motorista obrigatória' });
    const sairam = rt3.itens.filter(i => i.status === 'separado');
    if (!sairam.length) return res.status(400).json({ ok: false, error: 'nenhum item separado para sair' });
    const naoSairam = rt3.itens.filter(i => i.status !== 'separado');
    rt3.status = 'finalizada';
    rt3.tipoSaida = naoSairam.length ? 'parcial' : 'completa';
    rt3.motorista = String(motorista).trim();
    rt3.placa = placaLimpa;
    rt3.telMotorista = telLimpo;
    rt3.saidaEm = new Date().toISOString();
    rt3.finalizadaEm = rt3.saidaEm;        // antes ficava vazio mesmo com a rota finalizada
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

  // ── 📸 ROTA-FOTO: a foto tirada na saída da rota (a tela já chamava, mas não existia) ──
  if (action === 'rota-foto') {
    const rota = String(req.query.rota || '').trim();
    if (!rota) return res.status(400).json({ ok: false, error: 'informe ?rota=ID' });
    const f = await dbGet(KEY + '_rotafoto_' + rota);
    if (!f || !f.b64) {
      // procura variações do identificador antes de desistir
      const rdbF = (await dbGet('reparoeletro_almox_rotas')) || { rotas: [] };
      const alvo = (rdbF.rotas || []).find(r => String(r.id).toLowerCase() === rota.toLowerCase());
      if (alvo) {
        const f2 = await dbGet(KEY + '_rotafoto_' + alvo.id);
        if (f2 && f2.b64) return res.status(200).json({ ok: true, b64: f2.b64, em: f2.em, rota: alvo.id });
      }
      return res.status(404).json({ ok: false, error: 'foto não encontrada para esta rota' });
    }
    // ?img=1 → devolve a IMAGEM de verdade (abre no navegador em vez de mostrar o código)
    if (String(req.query.img || '') === '1') {
      const m = String(f.b64).match(/^data:(image\/\w+);base64,(.+)$/);
      if (m) {
        const buf = Buffer.from(m[2], 'base64');
        res.setHeader('Content-Type', m[1]);
        res.setHeader('Cache-Control', 'private, max-age=3600');
        return res.status(200).send(buf);
      }
    }
    return res.status(200).json({ ok: true, b64: f.b64, em: f.em, rota,
      tamanhoKB: Math.round(String(f.b64).length / 1024),
      verNoNavegador: 'acrescente &img=1 ao link para abrir a imagem' });
  }

  // ── 📸 ROTAS-COM-FOTO: quais rotas têm foto guardada ──
  if (action === 'rotas-com-foto') {
    const rdbF = (await dbGet('reparoeletro_almox_rotas')) || { rotas: [] };
    const dias = Math.min(60, Math.max(1, parseInt(req.query.dias || '15', 10)));
    const corte = Date.now() - dias * 86400000;
    const lista = [];
    for (const r of (rdbF.rotas || [])) {
      const q = new Date(r.saidaEm || r.criadaEm || 0).getTime();
      if (!q || q < corte) continue;
      const f = await dbGet(KEY + '_rotafoto_' + r.id);
      lista.push({ rota: r.id, motorista: r.motorista, quando: r.saidaEm || r.criadaEm,
        temFoto: !!(f && f.b64), tamanhoKB: f && f.b64 ? Math.round(String(f.b64).length / 1024) : 0 });
    }
    lista.sort((a, b) => String(b.quando).localeCompare(String(a.quando)));
    return res.status(200).json({ ok: true, periodoDias: dias,
      total: lista.length, comFoto: lista.filter(x => x.temFoto).length,
      lista: lista.map(x => x.rota + ' | ' + String(x.motorista || '?').slice(0, 26) +
        ' | ' + (x.temFoto ? '📸 ' + x.tamanhoKB + 'KB' : 'sem foto') +
        ' | ' + String(x.quando).slice(0, 10)) });
  }

  // ── 🚛 MOTORISTAS: lista quem já fez rota, com o que foi registrado de cada um ──
  if (action === 'motoristas') {
    const [rdbM, lgT] = await Promise.all([dbGet('reparoeletro_almox_rotas'), dbGet('tv_logistica')]);
    const busca = String(req.query.q || '').toLowerCase().trim();
    const pessoas = {};
    const guarda = (nomeBruto, extra) => {
      const nome = String(nomeBruto || '').trim();
      if (!nome) return;
      // o campo era livre: nome, placa e telefone podiam vir grudados
      const tels = (nome.match(/\d{10,13}/g) || []).map(t => t.replace(/\D/g, ''));
      const placas = (nome.match(/\b[A-Z]{3}\d[A-Z0-9]\d{2}\b/gi) || []);
      const limpo = nome.split(/[\r\n]/)[0].replace(/\d{10,13}/g, '').trim();
      const chave = limpo.toLowerCase().slice(0, 30);
      if (!pessoas[chave]) pessoas[chave] = { nome: limpo, telefones: new Set(), placas: new Set(), rotas: 0, ultima: null, origens: new Set() };
      const p = pessoas[chave];
      p.rotas++;
      for (const t of tels) p.telefones.add(t);
      for (const pl of placas) p.placas.add(String(pl).toUpperCase());
      if (extra) {
        if (extra.tel) p.telefones.add(String(extra.tel).replace(/\D/g, ''));
        if (extra.placa) p.placas.add(String(extra.placa).toUpperCase());
        if (extra.quando && (!p.ultima || extra.quando > p.ultima)) p.ultima = extra.quando;
        if (extra.origem) p.origens.add(extra.origem);
      }
    };
    for (const r of (((rdbM || {}).rotas) || [])) {
      guarda(r.motorista, { tel: r.telMotorista, placa: r.placa, quando: r.saidaEm || r.criadaEm, origem: 'rota almoxarifado' });
    }
    for (const f of (((lgT || {}).fichas) || [])) {
      if (f.motoristaNome || f.coletadoPor) guarda(f.coletadoPor || f.motoristaNome, { quando: f.movedAt, origem: 'coleta TV' });
    }
    let lista = Object.values(pessoas).map(p => ({
      nome: p.nome,
      telefones: [...p.telefones].filter(t => t.length >= 10),
      placas: [...p.placas],
      rotas: p.rotas,
      ultimaAtividade: p.ultima,
      onde: [...p.origens],
    })).sort((a, b) => b.rotas - a.rotas);
    if (busca) lista = lista.filter(p => p.nome.toLowerCase().includes(busca));
    return res.status(200).json({ ok: true,
      total: lista.length,
      comTelefone: lista.filter(p => p.telefones.length).length,
      semTelefone: lista.filter(p => !p.telefones.length).length,
      motoristas: lista.map(p => p.nome + ' | ' + p.rotas + ' rota(s) | tel: ' +
        (p.telefones.join(', ') || 'NÃO REGISTRADO') + ' | placa: ' + (p.placas.join(', ') || '—')),
      detalhe: lista });
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
          pushTarefa(db.tarefas, novaTarefa({ tipo: 'mover', cardId: c.id,
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
    // 📺 TV tem separação própria — a reconciliação varre os dois pipes para DIAGNÓSTICO,
    // mas só recria tarefa para o ADM. Era por aqui que a TV voltava ao almoxarifado.
    const soTvTxt = (txt) => {
      const t = String(txt || '').toLowerCase();
      const temTv = /\btvs?\b|televis|\bled\b|polegada|barramento/.test(t);
      if (!temTv) return false;
      return !/micro-?\s?ondas|microondas|purificador|bebedouro|filtro|adega|cervejeir|forno|forninho|frigobar|b\.?\s?blend/.test(t);
    };
    const paraCriar = faltam.filter(f => f.sistema !== 'tv' && !soTvTxt(f.equipamento));
    const ignoradosTv = faltam.length - paraCriar.length;
    if (req.query.aplicar === '1') {
      for (const f of paraCriar) {
        pushTarefa(db.tarefas, novaTarefa({ tipo: 'mover', cardId: f.id,
          cliente: f.cliente || '—', tel: f.tel || '', equipamento: f.equipamento || '',
          origem: 'aguardando_aprovacao', destino: f.fase, reconciliada: true }));
      }
      await dbSet(KEY, db);
      return res.status(200).json({ ok: true, recriadas: paraCriar.length,
        ignoradasPorSerTv: ignoradosTv, lista: paraCriar });
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
      pushTarefa(db.tarefas, novaTarefa(Object.assign({ cardId: 'TESTE-' + s.cliente.replace(/\s/g, ''), tel: '5531997856023', teste: true }, s)));
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
