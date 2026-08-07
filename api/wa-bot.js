// api/wa-bot.js — Cérebro do bot (FASE 1: COPILOTO — sugere, humano aprova)
// actions: conversas | historico&tel= | sugerir&tel= | enviar (POST) | config
const U = (process.env.UPSTASH_URL || '').replace(/['"]/g, '').trim();
const T = (process.env.UPSTASH_TOKEN || '').replace(/[\n\r'"]/g, '').trim();
let WA_TOKEN = (process.env.WA_TOKEN || '').trim();
let WA_PHONE_ID = (process.env.WA_PHONE_ID || '').trim();
async function credenciais() {
  // 🔀 O Redis tem PRIORIDADE quando há troca ativa — permite mudar de número em
  // segundos, sem redeploy. Sem troca ativa, valem as variáveis da Vercel.
  const c = await dbGet('wa_credenciais');
  if (c && c.ativo && c.token && c.phoneId) return { token: c.token, phoneId: c.phoneId };
  if (WA_TOKEN && WA_PHONE_ID) return { token: WA_TOKEN, phoneId: WA_PHONE_ID };
  return { token: (c && c.token) || WA_TOKEN, phoneId: (c && c.phoneId) || WA_PHONE_ID };
}
const ANTHROPIC_KEY = (process.env.ANTHROPIC_API_KEY || '').trim();

const EVT_LIST = 'wa_evt_list';

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
async function bumpStat(campo) {
  try {
    const dia = new Date(Date.now() - 3 * 3600 * 1000).toISOString().slice(0, 10);
    await fetch(`${U}/hincrby/wa_stats_${dia}/${campo}/1`, { headers: { Authorization: `Bearer ${T}` } });
  } catch (e) {}
}
async function lerEvts() {
  try {
    const r = await fetch(`${U}/lrange/${EVT_LIST}/-5000/-1`, { headers: { Authorization: `Bearer ${T}` } });
    const j = await r.json();
    const out = [];
    for (const s of (j.result || [])) { try { out.push(JSON.parse(s)); } catch (_) {} }
    return out;
  } catch { return []; }
}
// 📒 índice leve msgId → template/origem, lido pelo webhook quando a entrega falha
async function indexarEnvio(msgId, template, via, texto, telefone) {
  if (!msgId) return;
  try {
    const idx = (await dbGet('wa_envio_idx')) || {};
    idx[msgId] = { template: template || null, via: via || null,
      texto: String(texto || '').slice(0, 120), telefone: String(telefone || ''),
      em: new Date().toISOString() };
    const chaves = Object.keys(idx);
    if (chaves.length > 800) {
      chaves.sort((a, b) => String(idx[a].em).localeCompare(String(idx[b].em)));
      for (const k of chaves.slice(0, chaves.length - 800)) delete idx[k];
    }
    await dbSet('wa_envio_idx', idx);
  } catch (e) {}
}

async function rpushEvt(evt) {
  try {
    await fetch(`${U}/rpush/${EVT_LIST}/${encodeURIComponent(JSON.stringify(evt))}`,
      { headers: { Authorization: `Bearer ${T}` } });
  } catch (_) {}
}

// Contexto do cliente nos sistemas (por últimos 8 dígitos do telefone)
const FASE_TECNICO_LBL = {
  producao: 'em bancada (produção)', aguardando_peca: 'aguardando chegada de peça',
  conserto_concluido: 'conserto concluído', teste_realizado: 'testado e aprovado no teste de qualidade',
  aguardando_ret: 'pronto — aguardando retirada na loja', solicitar_entrega: 'pronto — entrega sendo agendada',
  entrega_realizada: 'entregue', coleta_solicitada: 'coleta a caminho', erp: 'finalizado (registro)',
};

// ═══ FONTE ÚNICA DA VERDADE: orçamento que NÓS enviamos e que não aprovou nem virou Conflitos Bot.
// O painel de Orçamentos e o motor de reativação usam exatamente esta lista.
async function orcamentosEmAberto() {
  const [logA, tvA, envA, pipeA, pipeT, pros, flDb] = await Promise.all([
    dbGet('reparoeletro_logistica'), dbGet('tv_logistica'),
    dbGet('wa_orc_enviados').then(v => v || { ids: {} }),
    dbGet('reparoeletro_pipe'), dbGet('tv_pipe'), dbGet('prospeccao_adm'),
    dbGet('reparoeletro_frenteloja'),
  ]);
  const d8 = t => String(t || '').replace(/\D/g, '').slice(-8);
  // registro de orçamento enviado: antigo era texto com a data, novo é objeto com origem
  const dataEnvio = v => (v && typeof v === 'object') ? v.em : v;
  const origemEnvio = v => (v && typeof v === 'object') ? v.origem : null;
  // SAÍDAS: aprovou (avançou no pipe) ou virou Conflitos Bot
  const APROVOU = ['aprovados', 'video_enviado', 'analise_compra', 'equipamento_comprado',
    'programar_entrega', 'solicitar_entrega', 'entrega_solicitada', 'rota_em_andamento',
    'receber', 'erp', 'finalizado', 'descarte', 'garantia'];
  const saiu = new Set();
  for (const c of [...(((pipeA || {}).cards) || []), ...(((pipeT || {}).cards) || [])]) {
    if (APROVOU.includes(c.phaseId || c.phase)) { const d = d8(c.telefone); if (d.length >= 8) saiu.add(d); }
  }
  for (const f of (((pros || {}).fichas) || [])) {
    if (f.status === 'conflitos_bot') { const d = d8(f.telefone); if (d.length >= 8) saiu.add(d); }
  }
  const abertos = []; const vistos = new Set();
  const juntar = (f, sis, quando) => {
    const d = d8(f.telefone);
    if (d.length < 8 || saiu.has(d) || vistos.has(d)) return;
    vistos.add(d);
    const ts = quando ? new Date(quando).getTime() : 0;
    abertos.push({ fichaId: f.id, sis, tel: String(f.telefone || '').replace(/\D/g, ''), d8: d,
      nome: f.nome || 'Cliente', equipamento: f.equipamento || '',
      enviadoEm: quando || null,
      horasParado: ts ? Number(((Date.now() - ts) / 3600000).toFixed(1)) : null });
  };
  for (const f of (((logA || {}).fichas) || [])) {
    if (!['orc_registrado', 'orc_enviado'].includes(f.phase)) continue;
    if (!envA.ids[f.id]) continue;                       // só o que O BOT enviou
    juntar(f, 'adm', dataEnvio(envA.ids[f.id]));
  }
  for (const f of (((tvA || {}).fichas) || [])) {
    if (!['orc_registrado', 'orc_enviado'].includes(f.phase)) continue;
    const q = envA.ids['tv:' + f.id] || envA.ids[f.id];
    if (!q) continue;
    juntar(f, 'tv', dataEnvio(q));
  }
  // Frente de Loja: orçamento ENVIADO e ainda sem decisão entra na régua de reativação.
  // (isto não faz o bot enviar nada — o envio de loja é sempre manual pelo botão.)
  for (const f of (((flDb || {}).fichas) || [])) {
    if (f.phase !== 'orcamento_cadastrado' || !f.orcEnviadoWpp) continue;
    juntar({ id: f.id, telefone: f.telefone, nome: f.nomeContato, equipamento: f.equipamento },
      'loja', f.orcEnviadoWppEm);
  }
  abertos.sort((a, b) => (b.horasParado || 0) - (a.horasParado || 0));
  return abertos;
}

// ═══ GARANTIA DE APROVAÇÃO: move, CONFERE e força os gatilhos que faltarem ═══
// Antes, mover/board/almoxarifado eram três mecanismos soltos: se um falhasse, ninguém sabia.
// ⚠️ Abre conflito quando o bot prometeu algo ao cliente e a ação NÃO pôde ser concluída.
// Existe para que nenhuma promessa morra em silêncio — um humano confere e resolve.
async function promessaSemLastro(tel, d8, textoPrometido, motivo, causa) {
  const _K = (process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim();
  const bate = t => String(t || '').replace(/\D/g, '').slice(-8) === String(d8);
  let nome = '', equipamento = '';
  try {
    const [fA, fT, lA, lT] = await Promise.all([
      dbGet('fichas_adm'), dbGet('fichas_tv'), dbGet('reparoeletro_logistica'), dbGet('tv_logistica'),
    ]);
    const acha = b => (((b || {}).fichas) || []).find(f => bate(f.telefone));
    const c = [acha(fA), acha(fT), acha(lA), acha(lT)].filter(Boolean);
    const f = c.find(x => String(x.nome || '').trim()) || c[0] || null;
    if (f) { nome = f.nome || ''; equipamento = f.equipamento || ''; }
  } catch (e) {}
  try {
    await rpushEvt({ ts: new Date().toISOString(), tel, dir: 'acao', tipo: 'falha',
      texto: '⚠️ PROMESSA SEM LASTRO — ' + causa });
  } catch (e) {}
  try {
    await fetch(`https://reparoeletroadm.com/api/prospeccao?action=criar-conflito&k=${_K}`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        nome: nome || 'Cliente WhatsApp', telefone: String(tel).replace(/\D/g, ''),
        equipamento: equipamento || '',
        motivo: '⚠️ PROMESSA NÃO CUMPRIDA — VERIFICAR COM O CLIENTE. ' + causa +
          '. Prometido ao cliente: "' + String(textoPrometido || '').slice(0, 120) + '"' +
          (motivo ? ' (motivo do bot: ' + String(motivo).slice(0, 80) + ')' : ''),
      }),
    }).then(x => x.json()).catch(() => null);
    await bumpStat('conflitos');
  } catch (e) {}
}

async function garantirAprovacao(d8) {
  const K = (process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim();
  const post = (api, action, body) => fetch(`https://reparoeletroadm.com/api/${api}?action=${action}&k=${K}`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body),
  }).then(x => x.json()).catch(e => ({ error: e.message }));
  // aceita 4 ou 8 digitos finais: comparar por igualdade quebrava quando vinham 4
  const bate = t => String(t || '').replace(/\D/g, '').endsWith(String(d8));
  const faseDe = c => c.phaseId || c.phase || '';
  const NEGOC = ['aguardando_aprovacao', 'ultima_chamada'];
  const passos = [];

  // 1) MOVER — acha o card no sistema certo
  let [ppA, ppT, tvLog, envG] = await Promise.all([
    dbGet('reparoeletro_pipe'), dbGet('tv_pipe'), dbGet('tv_logistica'),
    dbGet('wa_orc_enviados').then(v => v || { ids: {} }),
  ]);

  // ═══ 1º: ROTEAR PELA ORIGEM DO ORÇAMENTO QUE O CLIENTE RECEBEU ═══
  // Sem isso, a busca por telefone pode casar com uma ficha ANTIGA de outro sistema
  // (cliente recorrente) e deixar o orçamento atual parado no limbo.
  let origemRegistrada = null;
  try {
    const cands = [];
    for (const k of Object.keys(envG.ids || {})) {
      const v = envG.ids[k];
      if (!v || typeof v !== 'object') continue;          // registros antigos não têm origem
      if (!v.telefone || !String(v.telefone).endsWith(String(d8))) continue;
      cands.push({ chave: k, ...v });
    }
    cands.sort((a, b) => String(b.em).localeCompare(String(a.em)));
    origemRegistrada = cands[0] || null;                   // o mais recente é o que ele respondeu
  } catch (e) {}
  if (origemRegistrada) passos.push('origem do orçamento: ' + origemRegistrada.origem);

  // ═══ TRAVA ANTI-LIMBO: sem origem registrada e com ficha aberta em mais de um sistema ═══
  if (!origemRegistrada) {
    const abertosTv = (((tvLog || {}).fichas) || []).some(f => bate(f.telefone) && ['orc_enviado', 'orc_registrado'].includes(f.phase));
    const abertosAdm = (((ppA || {}).cards) || []).some(c => bate(c.telefone) && NEGOC.includes(faseDe(c)));
    const abertosTvPipe = (((ppT || {}).cards) || []).some(c => bate(c.telefone) && NEGOC.includes(faseDe(c)));
    const quantos = [abertosTv || abertosTvPipe, abertosAdm].filter(Boolean).length;
    if (quantos > 1) {
      passos.push('❌ AMBÍGUO: cliente tem orçamento aberto em TV e em ADM — não vou adivinhar');
      return { ok: false, ambiguo: true, passos };
    }
  }

  // se a origem diz que é da logística TV, nem procura no ADM
  const soTv = origemRegistrada && origemRegistrada.origem === 'logistica-tv';
  const soAdm = origemRegistrada && origemRegistrada.origem === 'logistica-adm';
  const fTv = soAdm ? null : (((tvLog || {}).fichas) || []).find(f =>
    (origemRegistrada && origemRegistrada.fichaId ? f.id === origemRegistrada.fichaId : bate(f.telefone))
    && ['orc_enviado', 'orc_registrado'].includes(f.phase));
  let card = null, api = 'pipe', sis = 'adm';
  if (fTv) {
    const r = await post('tv-logistica', 'aprovar-orcamento', { id: fTv.id });
    passos.push('TV logística: ' + (r && !r.error ? 'aprovado' : 'FALHOU ' + (r.error || '')));
    sis = 'tv';
  }
  card = soTv ? null : (((ppA || {}).cards) || []).find(c => bate(c.telefone) && NEGOC.includes(faseDe(c)));
  if (!card) {
    const cT = (((ppT || {}).cards) || []).find(c => bate(c.telefone) && NEGOC.includes(faseDe(c)));
    if (cT) { card = cT; api = 'tv-pipe'; sis = 'tv'; }
  }
  if (card) {
    const r = await post(api, 'mover', { id: card.id, phase: 'aprovados', por: 'bot' });
    passos.push(api + ' mover: ' + (r && !r.error ? 'ok' : 'FALHOU ' + (r.error || '')));
  }

  // 2) CONFERE se o card está mesmo em aprovados
  await new Promise(r => setTimeout(r, 400));
  const [ppA2, ppT2] = await Promise.all([dbGet('reparoeletro_pipe'), dbGet('tv_pipe')]);
  const todos = [...(((ppA2 || {}).cards) || []), ...(((ppT2 || {}).cards) || [])];
  const aprovado = todos.find(c => bate(c.telefone) && faseDe(c) === 'aprovados');
  passos.push(aprovado ? 'card em aprovados: ✅' : 'card em aprovados: ❌ NÃO CONFIRMADO');
  if (!aprovado) return { ok: false, passos };

  // 3) BOARD TÉCNICO — cada sistema tem o seu: ADM em reparoeletro_board, TV em tv_board
  const ehTv = sis === 'tv';
  const chaveBoard = ehTv ? 'tv_board' : 'reparoeletro_board';
  let board = (await dbGet(chaveBoard)) || { cards: [] };
  const jaNoBoard = ((board.cards) || []).some(c =>
    c.osCode === aprovado.id || String(c.telefone || '').replace(/\D/g, '').endsWith(String(d8)));
  if (!jaNoBoard) {
    if (!ehTv) {
      const r = await post('pipe', 'reprocessar-aprovado&tel=' + d8 + '&aplicar=1', {});
      passos.push('board técnico ADM: forçado ' + (r && r.ok ? '✅' : '⚠️ ' + (r.error || '')));
    } else {
      // insere direto no board de TV, no mesmo formato que o tv-pipe usa
      try {
        if (!Array.isArray(board.cards)) board.cards = [];
        const agoraB = new Date().toISOString();
        const pidB = aprovado.pipefyId ? String(aprovado.pipefyId) : ('LOCAL-' + aprovado.id);
        board.cards.unshift({ pipefyId: pidB, phaseId: 'aprovado',
          nomeContato: aprovado.nomeContato || '', title: aprovado.descricao || aprovado.nomeContato || '',
          telefone: aprovado.telefone || '', descricao: aprovado.equipamento || aprovado.descricao || '',
          osCode: aprovado.id, valor: aprovado.valor || 0, movedBy: 'Bot Vendas (garantia)',
          localOnly: !aprovado.pipefyId, syncedAt: agoraB, movedAt: agoraB });
        if (!Array.isArray(board.syncedIds)) board.syncedIds = [];
        if (!board.syncedIds.includes(pidB)) board.syncedIds.push(pidB);
        if (!Array.isArray(board.movesLog)) board.movesLog = [];
        board.movesLog.push({ phaseId: 'aprovado_entrada', pipefyId: pidB, timestamp: agoraB });
        await dbSet(chaveBoard, board);
        passos.push('board técnico TV: inserido ✅');
      } catch (e) { passos.push('board técnico TV: ⚠️ ' + e.message); }
    }
  } else passos.push('board técnico: ✅');

  // 4) ALMOXARIFADO — SOMENTE ADM. TV tem separação própria e não entra aqui.
  if (ehTv) {
    passos.push('almoxarifado: não se aplica (TV tem fluxo próprio)');
  } else {
    const alm = (await dbGet('reparoeletro_almoxarifado')) || { tarefas: [] };
    // só tarefa PENDENTE conta — uma antiga já concluída não pode bloquear a nova
    const temTarefa = ((alm.tarefas) || []).some(t => t.cardId === aprovado.id &&
      t.destino === 'aprovados' && t.status === 'pendente');
    if (!temTarefa) {
      const r = await post('almoxarifado', 'criar-mover', { cardId: aprovado.id, destino: 'aprovados',
        cliente: aprovado.nomeContato || '', tel: aprovado.telefone || '', equipamento: aprovado.equipamento || '' });
      passos.push('almoxarifado: criado ' + (r && r.ok && !r.dedupe ? '✅' : (r && r.dedupe ? 'já existia' : '⚠️ ' + (r.error || ''))));
    } else passos.push('almoxarifado: já pendente ✅');
  }

  return { ok: true, sistema: sis, cardId: aprovado.id, passos };
}

async function contextoCliente(tel) {
  const d8 = String(tel).replace(/\D/g, '').slice(-8);
  const ctx = { fichas: [], logistica: [], pipe: [], tecnico: [], pecas: [] };
  try {
    const [fa, lg, pp, bd, pcs, ftv, orcA, orcT, tvlg] = await Promise.all([
      dbGet('fichas_adm'), dbGet('reparoeletro_logistica'), dbGet('reparoeletro_pipe'),
      dbGet('reparoeletro_board'), dbGet('reparoeletro_compras_pecas'), dbGet('fichas_tv'),
      dbGet('reparoeletro_orcamentos'), dbGet('tv_orcamentos'), dbGet('tv_logistica'),
    ]);
    const bate = (t) => String(t || '').replace(/\D/g, '').endsWith(d8);
    // 💰 ORÇAMENTO REGISTRADO no sistema (texto oficial + preço) — o bot ENVIA este texto, nunca inventa
    try {
      const oA = (((orcA || {}).fichas) || []).find(x => bate(x.tel) && x.textoOrc);
      const oT = (((orcT || {}).fichas) || []).find(x => bate(x.tel) && x.textoOrc);
      const fTvDiag = (((tvlg || {}).fichas) || []).find(x => bate(x.telefone) && x.diagnostico && x.diagnostico.textoOrc);
      const escolhido = oT || oA || (fTvDiag ? { textoOrc: fTvDiag.diagnostico.textoOrc, precoSugerido: fTvDiag.diagnostico.precoFinal, status: fTvDiag.phase } : null);
      if (escolhido) {
        ctx.orcamentoRegistrado = {
          texto: String(escolhido.textoOrc || '').slice(0, 900),
          preco: escolhido.precoSugerido != null ? escolhido.precoSugerido : (escolhido.preco || null),
          status: escolhido.status || '',
          sistema: oT || fTvDiag ? 'tv' : 'adm',
        };
      }
    } catch (e) {}
    let faU = fa, ftvU = ftv;
    const achou = () => (((faU && faU.fichas) || []).some(f => bate(f.telefone))) || ((((ftvU || {}).fichas) || []).some(f => bate(f.telefone)));
    if (!achou()) {
      // cliente sem ficha no banco: pode estar só na planilha — importa na hora e reconsulta
      try {
        await fetch(`https://reparoeletroadm.com/api/fichas?action=sync&k=${(process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim()}`);
        const [fa2, ftv2] = await Promise.all([dbGet('fichas_adm'), dbGet('fichas_tv')]);
        faU = fa2 || faU; ftvU = ftv2 || ftvU;
      } catch (e) {}
    }
    for (const f of ((faU && faU.fichas) || [])) if (bate(f.telefone)) {
      ctx.fichas.push({ id: f.id, nome: f.nome, status: f.status, equipamento: f.equipamento, defeito: f.defeito });
    }
    for (const f of (((ftvU || {}).fichas) || [])) if (bate(f.telefone)) {
      ctx.fichas.push({ id: f.id, nome: f.nome, status: f.status, equipamento: f.equipamento || 'TV', defeito: f.defeito, sistemaTV: true });
      ctx.clienteTV = true;
    }
    for (const f of ((lg && lg.fichas) || [])) if (bate(f.telefone)) {
      ctx.logistica.push({ id: f.id, nome: f.nome, fase: f.phase, equipamento: f.equipamento,
        orcamento: (f.diagnostico && f.diagnostico.preco) || f.orcamentoValor || null,
        textoOrcamento: (f.diagnostico && f.diagnostico.textoOrc) || null });
    }
    for (const c of ((pp && pp.cards) || [])) if (bate(c.telefone)) {
      ctx.pipe.push({ id: c.id, nome: c.nomeContato, fase: c.phase, equipamento: c.equipamento, valor: c.valor || null });
    }
    // Board do técnico: estágio REAL da OS (produção/peça/teste/entrega)
    const nomesCliente = [];
    for (const c of ((bd && bd.cards) || [])) if (bate(c.telefone || c.tel)) {
      ctx.tecnico.push({ os: c.os || c.numero || c.id, equipamento: c.equipamento || c.title || '',
        estagio: FASE_TECNICO_LBL[c.phase] || c.phase, fluxo: c.fluxo || c.tipo || '' });
      if (c.nomeContato || c.nome) nomesCliente.push(String(c.nomeContato || c.nome).toLowerCase().split(' ')[0]);
    }
    // Peças ligadas às OSs do cliente (previsão de chegada)
    const ossCliente = new Set(ctx.tecnico.map(t => String(t.os)));
    for (const p of ((pcs && pcs.pecas) || [])) {
      const pos = String(p.os || p.osNum || '');
      const pnome = String(p.cliente || p.nome || '').toLowerCase();
      if ((pos && ossCliente.has(pos)) || (pnome && nomesCliente.some(n => n && pnome.includes(n)))) {
        ctx.pecas.push({ os: pos, peca: p.peca || p.descricao || '', status: p.status || '',
          previsao: p.previsao || p.prazo || p.chegadaPrevista || null });
      }
    }
  } catch (_) {}
  return ctx;
}

const CONFIG_DEFAULT = {
  descontoPix: 10,          // % à vista no Pix
  descontoBalcao: 5,        // % balcão — usado APENAS na F3 da negociação (cascata sobre o Pix), nunca na abertura
  politicaTroca: 'Aceitamos seu equipamento na troca por um seminovo revisado com garantia — o valor dele vira desconto.',
  politicaCompra: 'Também compramos seu equipamento usado, mesmo com defeito.',
  argumentoNovo: 'Equipamentos novos de preço parecido geralmente são de linha inferior (menor potência, menos capacidade e vida útil menor). O conserto devolve a vida útil do SEU equipamento, que é superior.',
};

export default async function handler(req, res) {
  // 🔐 TF-AUTH (Fase 1): chave obrigatória em toda chamada
  const _tfk = (req.query && req.query.k) || req.headers['x-tf-key'] || '';
  if (_tfk !== ((process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim())) {
    return res.status(401).json({ ok: false, error: 'não autorizado' });
  }

  res.setHeader('Cache-Control', 'no-cache');
  const action = req.query.action || '';

  // 🚦 CONTA BLOQUEADA: enquanto a Meta recusar por pagamento, não adianta insistir —
  // cada tentativa falha, engrossa a fila e arrisca a qualidade do número quando liberar.
  async function contaBloqueada() {
    try {
      const c = (await dbGet('wa_bot_config')) || {};
      if (!c.bloqueioPagamentoEm) return false;
      const h = (Date.now() - new Date(c.bloqueioPagamentoEm).getTime()) / 3600000;
      return h < 2;                       // revalida a cada 2h
    } catch (e) { return false; }
  }

  // ── ABORDAGEM-FICHAS (cron 5min): ficha criada há 5-60min sem conversa iniciada → template cadastro_recebido ──
  // Interruptor: wa_bot_config.abordagemAtiva (false por padrão — ligar quando o número real estiver ativo)
  // ── 🔍 APROVADOS-DUPLICADOS: aprovado com espelho em aguardando aprovação (GET varre; POST ?limpar=1 arquiva os espelhos) ──
  if (action === 'aprovados-duplicados') {
    const pp = (await dbGet('reparoeletro_pipe')) || { cards: [] };
    const porTel = {};
    for (const c of (pp.cards || [])) {
      const d = String(c.telefone || '').replace(/\D/g, '').slice(-8);
      if (d.length < 8) continue;
      (porTel[d] = porTel[d] || []).push(c);
    }
    const APROVADAS = ['aprovados', 'aprovado', 'producao', 'em_producao', 'conserto_concluido'];
    const ESPERA = ['aguardando_aprovacao', 'ultima_chamada'];
    const pares = [];
    for (const d of Object.keys(porTel)) {
      const cards = porTel[d];
      const aprov = cards.find(c => APROVADAS.includes(c.phaseId || c.phase));
      const espelhos = cards.filter(c => ESPERA.includes(c.phaseId || c.phase));
      if (aprov && espelhos.length) {
        for (const e of espelhos) pares.push({ tel: d, nome: e.nomeContato || aprov.nomeContato || '?',
          aprovadoId: aprov.id, aprovadoFase: aprov.phaseId || aprov.phase,
          espelhoId: e.id, espelhoFase: e.phaseId || e.phase });
      }
    }
    if (String(req.query.limpar || '') === '1' && pares.length) {
      const arqL = (await dbGet('pipe_ids_arquivados')) || { ids: [] };
      const idsRemover = pares.map(p => p.espelhoId);
      for (const idr of idsRemover) if (!arqL.ids.includes(idr)) arqL.ids.push(idr);
      await dbSet('pipe_ids_arquivados', arqL);
      pp.cards = pp.cards.filter(c => !idsRemover.includes(c.id));
      await dbSet('reparoeletro_pipe', pp);
      return res.status(200).json({ ok: true, limpos: pares.length, pares });
    }
    return res.status(200).json({ ok: true, duplicados: pares.length, pares,
      dica: pares.length ? 'para arquivar os espelhos: mesmo link com &limpar=1' : 'nenhum espelho encontrado' });
  }

  // ── 💰 ORCAMENTOS-ABERTOS: conversas com orçamento enviado e ainda sem aprovação ──
  // ── 💰 ORÇAMENTOS-ABERTOS: o limbo da negociação (mesma lista que a reativação usa) ──
  if (action === 'orcamentos-abertos') {
    const abertos = await orcamentosEmAberto();
    return res.status(200).json({ ok: true, total: abertos.length,
      porSistema: abertos.reduce((o, a) => { o[a.sis] = (o[a.sis] || 0) + 1; return o; }, {}),
      abertos });
  }

  // ── 📊 BOT-STATS: contadores por dia (?dias=1..90) ──
  if (action === 'bot-stats') {
    const dias = Math.min(90, Math.max(1, parseInt(req.query.dias || '30', 10)));
    const out = [];
    for (let i = 0; i < dias; i++) {
      const d = new Date(Date.now() - 3 * 3600 * 1000 - i * 86400000).toISOString().slice(0, 10);
      try {
        const r = await fetch(`${U}/hgetall/wa_stats_${d}`, { headers: { Authorization: `Bearer ${T}` } });
        const j = await r.json();
        const arr = j.result || [];
        const obj = { dia: d };
        for (let k = 0; k < arr.length; k += 2) obj[arr[k]] = parseInt(arr[k + 1], 10) || 0;
        out.push(obj);
      } catch (e) { out.push({ dia: d }); }
    }
    const tot = {};
    for (const o of out) for (const k of Object.keys(o)) if (k !== 'dia') tot[k] = (tot[k] || 0) + o[k];
    return res.status(200).json({ ok: true, dias: out, totais: tot });
  }

  // ── 🔓 ATIVAR-GERAL: liga o bot para TODOS os clientes (abordagem + respostas + ações + orçamentos novos) ──
  if (action === 'ativar-geral') {
    const cfgAt = (await dbGet('wa_bot_config')) || {};
    cfgAt.modoAberto = true;
    cfgAt.abordagemAtiva = true;
    cfgAt.orcMarcoTs = cfgAt.orcMarcoTs || new Date().toISOString(); // orçamentos: só os criados a partir de agora
    cfgAt.ativadoEm = new Date().toISOString();
    await dbSet('wa_bot_config', cfgAt);
    return res.status(200).json({ ok: true, msg: '🤖 BOT ATIVADO PARA TODOS OS CLIENTES', abordagem: true, modoAberto: true, orcamentosAPartirDe: cfgAt.orcMarcoTs });
  }
  // ── 🚨 DESLIGAR-GERAL: botão de emergência — para tudo na hora ──
  if (action === 'desligar-geral') {
    const cfgDs = (await dbGet('wa_bot_config')) || {};
    cfgDs.modoAberto = false;
    cfgDs.abordagemAtiva = false;
    await dbSet('wa_bot_config', cfgDs);
    return res.status(200).json({ ok: true, msg: '🚨 BOT DESLIGADO (abordagem e modo aberto off; conversas de teste continuam pela trava execTels)' });
  }

  // Horário comercial Reparo Eletro (Brasília UTC-3): seg-sex 8h-15h, sáb 8h-10h
  function dentroHorarioComercial() {
    const bras = new Date(Date.now() - 3 * 3600 * 1000);
    const dia = bras.getUTCDay(), hora = bras.getUTCHours() + bras.getUTCMinutes() / 60;
    if (dia >= 1 && dia <= 5) return hora >= 8 && hora < 15;
    if (dia === 6) return hora >= 8 && hora < 10;
    return false;
  }

  // ── 🔁 REATIVAR-CONVERSAS: nenhum orçamento morre no vácuo (espaço da loja é limitado) ──
  // Escada: 6h → 24h → 48h → 72h; sem resposta no fim = Conflitos Bot (ligação + entrega)
  if (action === 'reativar-conversas') {
    const cfgR = (await dbGet('wa_bot_config')) || {};
    // OPT-IN: só dispara se ligado explicitamente (era opt-out e disparou sozinho para 99 clientes)
    if (cfgR.reativacaoAtiva !== true) {
      return res.status(200).json({ ok: true, msg: 'reativação DESLIGADA — precisa ser ligada explicitamente em ?action=reativacao-ligar' });
    }
    if (!dentroHorarioComercial()) return res.status(200).json({ ok: true, msg: 'fora do horário comercial — reativações em standby' });
    const { token, phoneId } = await credenciais();
    if (!token || !phoneId) return res.status(200).json({ ok: false, error: 'credenciais ausentes' });

    const [evtsR, reatR] = await Promise.all([
      lerEvts(), dbGet('wa_reativacao').then(v => v || { alvos: {} }),
    ]);
    // Última mensagem do CLIENTE e último toque nosso, por telefone
    const ultimaIn = {}, ultimaOut = {};
    for (const e of evtsR) {
      const d8e = String(e.tel || '').replace(/\D/g, '').slice(-8);
      if (!d8e) continue;
      const t = new Date(e.ts || 0).getTime();
      if (e.dir === 'in' && (!ultimaIn[d8e] || t > ultimaIn[d8e])) ultimaIn[d8e] = t;
      if (e.dir === 'out' && (!ultimaOut[d8e] || t > ultimaOut[d8e])) ultimaOut[d8e] = t;
    }
    // ALVOS = exatamente o filtro de Orçamentos: recebeu o nosso orçamento, não aprovou, não virou conflito
    const abertosR = await orcamentosEmAberto();
    const alvos = abertosR.map(a => ({ f: { id: a.fichaId, telefone: a.tel, nome: a.nome, equipamento: a.equipamento, orcEnviadoEm: a.enviadoEm }, sis: a.sis }));
    const ESCADA = [
      { h: 6,  txt: (n, eq) => `Oi ${n}! Conseguiu dar uma olhada no orçamento do seu ${eq}? Qualquer dúvida sobre o serviço eu te explico, é só me chamar 😊` },
      { h: 24, txt: (n) => `${n}, uma condição que costuma ajudar: pagando no Pix a gente consegue um valor melhor pra você. Quer que eu veja isso?` },
      { h: 48, txt: (n, eq) => `${n}, seu ${eq} já está aqui com a gente e fico no aguardo da sua aprovação para prosseguir com o conserto. Com a sua confirmação, acredito que entre hoje e amanhã mesmo a gente já consegue te entregar 😊` },
      { h: 72, txt: (n, eq) => `${n}, só passando para me colocar à disposição sobre o seu ${eq}. Se quiser seguir com o conserto, é só me avisar que já encaminho para a bancada. E se preferir deixar para outro momento, também tudo bem — me avisa que a equipe organiza a devolução com você.` },
    ];
    const agoraR = Date.now();
    const feitos = [];
    const TETO_CICLO = Math.min(10, Math.max(1, parseInt(cfgR.reativacaoTeto || 5, 10)));
    // TRAVA: só reativa quem O BOT atendeu (tem conversa) E para quem O BOT enviou o orçamento.
    // Sem isso o motor escrevia para clientes que nunca falaram com o bot.
    const pulados = [];
    for (const { f, sis } of alvos) {
      const d8r = String(f.telefone || '').replace(/\D/g, '').slice(-8);
      if (d8r.length < 8) continue;
      const chave = (sis === 'tv' ? 'tv:' : '') + f.id;
      const st = reatR.alvos[chave] || { toques: 0, ultimo: 0 };
      // Cliente respondeu DEPOIS do nosso último toque? negociação viva — reseta o relógio, não incomoda
      if (ultimaIn[d8r] && ultimaIn[d8r] > (st.ultimo || 0) && agoraR - ultimaIn[d8r] < 6 * 3600000) continue;
      const base = Math.max(st.ultimo || 0, ultimaIn[d8r] || 0, ultimaOut[d8r] || 0,
        new Date(f.orcEnviadoEm || f.movedAt || f.criadoEm || 0).getTime());
      const horas = (agoraR - base) / 3600000;
      const degrau = ESCADA[st.toques];
      // Esgotou a escada → CONFLITOS BOT (ligação humana + definir entrega)
      if (!degrau) {
        if (horas < 24) continue;
        try {
          const KRC = (process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim();
          await fetch(`https://reparoeletroadm.com/api/prospeccao?action=criar-conflito&k=${KRC}`, {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ nome: f.nome || 'Cliente', telefone: String(f.telefone || '').replace(/\D/g, ''),
              equipamento: f.equipamento || '',
              motivo: 'ciclo comercial esgotado — cliente não respondeu aos 4 toques do orçamento; ligar para aprovar ou definir a devolução' }),
          }).then(x => x.json()).catch(() => null);
          await bumpStat('conflitos');
          reatR.alvos[chave] = { toques: st.toques, ultimo: agoraR, encerrado: true };
          feitos.push({ nome: f.nome, acao: 'enviado para Conflitos Bot' });
        } catch (e) {}
        continue;
      }
      if (st.encerrado) continue;
      if (horas < degrau.h) continue;
      const toR = String(f.telefone || '').replace(/\D/g, '');
      const to55 = toR.startsWith('55') ? toR : '55' + toR;
      const nomeR = (f.nome || 'tudo bem').split(' ')[0];
      const equipR = f.equipamento || 'equipamento';
      const janelaAberta = ultimaIn[d8r] && (agoraR - ultimaIn[d8r]) < 24 * 3600000;
      let enviado = false;
      if (janelaAberta) {
        const rr = await fetch(`https://graph.facebook.com/v20.0/${phoneId}/messages`, {
          method: 'POST', headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
          body: JSON.stringify({ messaging_product: 'whatsapp', to: to55, type: 'text', text: { body: degrau.txt(nomeR, equipR) } }),
        }).then(x => x.json()).catch(() => null);
        enviado = !!(rr && rr.messages && rr.messages[0]);
        if (enviado) await rpushEvt({ ts: new Date().toISOString(), tel: to55, dir: 'out', texto: degrau.txt(nomeR, equipR), tipo: 'reativacao' });
      } else {
        const rt = await fetch(`https://graph.facebook.com/v20.0/${phoneId}/messages`, {
          method: 'POST', headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
          body: JSON.stringify({ messaging_product: 'whatsapp', to: to55, type: 'template',
            template: { name: 'orcamento_pronto', language: { code: 'pt_BR' },
              components: [{ type: 'body', parameters: [{ type: 'text', text: nomeR }, { type: 'text', text: equipR }] }] } }),
        }).then(x => x.json()).catch(() => null);
        enviado = !!(rt && rt.messages && rt.messages[0]);
        if (enviado) await rpushEvt({ ts: new Date().toISOString(), tel: to55, dir: 'out', texto: `📨 [reativação ${st.toques + 1}/4 — janela fechada] ${f.nome || ''}`, tipo: 'template' });
      }
      if (enviado) {
        reatR.alvos[chave] = { toques: st.toques + 1, ultimo: agoraR };
        feitos.push({ nome: f.nome, sistema: sis, toque: st.toques + 1, via: janelaAberta ? 'mensagem' : 'template' });
        if (feitos.length >= TETO_CICLO) break;   // teto por ciclo — nada de rajada
      }
    }
    for (const k of Object.keys(reatR.alvos)) {
      if (agoraR - (reatR.alvos[k].ultimo || 0) > 20 * 86400000) delete reatR.alvos[k];
    }
    await dbSet('wa_reativacao', reatR);
    return res.status(200).json({ ok: true, alvosAtivos: alvos.length, acoes: feitos.length, feitos,
      pulados: pulados.length, motivosPulados: pulados.slice(0, 20) });
  }

  // ── 🗄 ARQUIVAR-INATIVAS: 30 dias sem o cliente responder → arquiva (nunca com orçamento aberto) ──
  if (action === 'arquivar-inativas') {
    const dias = Math.min(180, Math.max(7, parseInt(req.query.dias || '30', 10)));
    const corteA = Date.now() - dias * 86400000;
    const [evtsAI, arqAI] = await Promise.all([lerEvts(), dbGet('wa_arquivadas').then(v => v || { tels: {} })]);
    const abertosAI = await orcamentosEmAberto();
    const comOrcamento = new Set(abertosAI.map(a => a.d8));
    // última mensagem DO CLIENTE por telefone
    const ultimaIn = {}, nomes = {};
    for (const e of evtsAI) {
      const d = String(e.tel || '').replace(/\D/g, '').slice(-8);
      if (d.length < 8) continue;
      if (e.nome) nomes[d] = e.nome;
      if (e.dir !== 'in') continue;
      const t = new Date(e.ts || 0).getTime();
      if (!ultimaIn[d] || t > ultimaIn[d]) ultimaIn[d] = t;
    }
    const candidatos = [];
    for (const d of Object.keys(ultimaIn)) {
      if (arqAI.tels[d]) continue;                       // já arquivada
      if (comOrcamento.has(d)) continue;                 // 🚫 orçamento em aberto nunca arquiva
      if (ultimaIn[d] >= corteA) continue;               // respondeu dentro do prazo
      candidatos.push({ d8: d, nome: nomes[d] || '',
        ultimaResposta: new Date(ultimaIn[d]).toISOString(),
        diasParado: Math.round((Date.now() - ultimaIn[d]) / 86400000) });
    }
    candidatos.sort((a, b) => b.diasParado - a.diasParado);
    if (String(req.query.aplicar || '') === '1') {
      for (const c of candidatos) arqAI.tels[c.d8] = new Date().toISOString();
      await dbSet('wa_arquivadas', arqAI);
      return res.status(200).json({ ok: true, arquivadas: candidatos.length, dias, candidatos: candidatos.slice(0, 50) });
    }
    return res.status(200).json({ ok: true, dias, seriamArquivadas: candidatos.length,
      protegidosPorOrcamento: comOrcamento.size,
      candidatos: candidatos.slice(0, 50),
      dica: 'para executar: mesmo link com &aplicar=1 (roda sozinho todo dia às 4h)' });
  }

  // ── 🔌 REATIVACAO-LIGAR / DESLIGAR ──
  if (action === 'reativacao-ligar' || action === 'reativacao-desligar') {
    const c = (await dbGet('wa_bot_config')) || {};
    c.reativacaoAtiva = action === 'reativacao-ligar';
    if (req.query.teto) c.reativacaoTeto = Math.min(10, Math.max(1, parseInt(req.query.teto, 10)));
    await dbSet('wa_bot_config', c);
    return res.status(200).json({ ok: true, reativacaoAtiva: c.reativacaoAtiva,
      tetoPorCiclo: c.reativacaoTeto || 5 });
  }

  // ── 🔎 BUSCAR-CONVERSAS: procura vários telefones no histórico, ignorando limites da tela ──
  // ── 📊 ABORDAGENS-HOJE: o que saiu, o que a Meta aceitou, quem respondeu (SOMENTE LEITURA) ──
  if (action === 'abordagens-hoje') {
    const ini = new Date(); ini.setHours(0, 0, 0, 0);
    const iniMs = ini.getTime() - 3 * 3600000; // limite do dia em BRT
    const [evts, fA, fT] = await Promise.all([lerEvts(), dbGet('fichas_adm'), dbGet('fichas_tv')]);
    const hojeEvt = evts.filter(e => new Date(e.ts).getTime() >= iniMs);
    const d8 = t => String(t || '').replace(/\D/g, '').slice(-8);
    const saiu = hojeEvt.filter(e => e.dir === 'out');
    const entrou = hojeEvt.filter(e => e.dir === 'in');
    const respRecebida = new Set(entrou.map(e => d8(e.tel)));
    const porTipo = {};
    for (const e of saiu) { const k = (e.tipo || '?') + (e.via ? '/' + e.via : ''); porTipo[k] = (porTipo[k] || 0) + 1; }
    const semMsgId = saiu.filter(e => !e.msgId && e.tipo !== 'falha');
    const falhas = hojeEvt.filter(e => e.tipo === 'falha');
    // fichas que o bot marcou como abordadas hoje, e se houve resposta
    const todasF = [...(((fA || {}).fichas) || []), ...(((fT || {}).fichas) || [])];
    const abordadasHoje = todasF.filter(f => f.contatoFeitoEm && new Date(f.contatoFeitoEm).getTime() >= iniMs);
    const comFalha = todasF.filter(f => f.falhaAbordagem && new Date(f.falhaAbordagem.em).getTime() >= iniMs);
    const semResposta = abordadasHoje.filter(f => !respRecebida.has(d8(f.telefone)));
    if (String(req.query.curto || '') === '1') {
      return res.status(200).send(
        'ABORDAGENS HOJE\n' +
        'fichas marcadas abordadas=' + abordadasHoje.length +
        ' | responderam=' + (abordadasHoje.length - semResposta.length) +
        ' | sem resposta=' + semResposta.length +
        ' | falhaAbordagem registrada=' + comFalha.length + '\n' +
        'mensagens OUT hoje por tipo: ' + Object.entries(porTipo).map(([k, v]) => k + '=' + v).join(' ') + '\n' +
        'OUT sem msgId (sem prova de entrega)=' + semMsgId.length + ' | eventos de falha=' + falhas.length + '\n' +
        (comFalha.length ? 'FALHAS:\n' + comFalha.slice(0, 15).map(f => '  ' + (f.nome || '') + ' ' + d8(f.telefone) + ' — ' + (f.falhaAbordagem.erro || '')).join('\n') + '\n' : '') +
        (falhas.length ? 'EVENTOS FALHA:\n' + falhas.slice(-10).map(e => '  ' + String(e.ts).slice(11, 16) + ' ' + d8(e.tel) + ' ' + String(e.erro || e.texto || '').slice(0, 90)).join('\n') : ''));
    }
    return res.status(200).json({ ok: true,
      abordadasHoje: abordadasHoje.length, responderam: abordadasHoje.length - semResposta.length,
      semResposta: semResposta.length, comFalhaRegistrada: comFalha.length,
      mensagensOutPorTipo: porTipo, outSemMsgId: semMsgId.length, eventosFalha: falhas.length,
      falhas: comFalha.slice(0, 20).map(f => ({ nome: f.nome, tel: d8(f.telefone), erro: f.falhaAbordagem.erro, em: f.falhaAbordagem.em })) });
  }

  // ── 📬 ENTREGA-HOJE: o que a META reportou de fato (sent/delivered/read/failed) ──
  if (action === 'entrega-hoje') {
    const ini = new Date(); ini.setHours(0, 0, 0, 0);
    const iniMs = ini.getTime() - 3 * 3600000;
    const evts = await lerEvts();
    const hoje = evts.filter(e => new Date(e.ts).getTime() >= iniMs);
    const st = hoje.filter(e => e.dir === 'status');
    const cont = {};
    const erros = [];
    for (const e of st) {
      const txt = String(e.texto || '');
      const chave = txt.split(' | ')[0].trim() || '?';
      cont[chave] = (cont[chave] || 0) + 1;
      if (/failed|error/i.test(txt)) erros.push({ ts: e.ts, tel: String(e.tel || '').slice(-8), detalhe: txt.slice(0, 220) });
    }
    const out = hoje.filter(e => e.dir === 'out' && e.tipo !== 'falha' && e.tipo !== 'nota');
    if (String(req.query.curto || '') === '1') {
      return res.status(200).send(
        'ENTREGA HOJE (o que a Meta reportou)\n' +
        'mensagens enviadas=' + out.length + ' | eventos de status recebidos=' + st.length + '\n' +
        'status: ' + (Object.entries(cont).map(([k, v]) => k + '=' + v).join(' ') || 'NENHUM') + '\n' +
        (erros.length
          ? 'FALHAS REPORTADAS PELA META (' + erros.length + '):\n' +
            erros.slice(0, 12).map(e => '  ' + String(e.ts).slice(11, 16) + ' ' + e.tel + ' — ' + e.detalhe).join('\n')
          : 'nenhuma falha reportada pela Meta') + '\n' +
        (st.length === 0 ? '\n⚠️ ZERO eventos de status: o webhook de status pode não estar assinado no app da Meta.' : ''));
    }
    return res.status(200).json({ ok: true, enviadas: out.length, eventosStatus: st.length, porStatus: cont, erros: erros.slice(0, 30) });
  }

  // ── 💳 BLOQUEIO-PAGAMENTO: tudo que a Meta recusou por pendência financeira ──
  //    modo leitura: lista e diz quando começou · &aplicar=1: devolve para reenvio
  if (action === 'bloqueio-pagamento') {
    // Naturezas diferentes exigem tratamentos diferentes:
    const COD_CONTA    = [131042, 133010];   // conta bloqueada → reenviar resolve
    const COD_INVALIDO = [131026, 131047];   // número não recebe / janela fechada → reenviar NÃO resolve
    const CODIGOS = [...COD_CONTA, ...COD_INVALIDO];
    const evts = await lerEvts();
    const d8 = t => String(t || '').replace(/\D/g, '').slice(-8);
    const falhas = [];
    for (const e of evts) {
      if (e.dir !== 'status') continue;
      const txt = String(e.texto || '');
      if (!/failed/i.test(txt)) continue;
      const cod = CODIGOS.find(c => txt.includes(String(c)));
      if (!cod) continue;
      falhas.push({ ts: e.ts, tel: String(e.tel || ''), d8: d8(e.tel), codigo: cod });
    }
    if (!falhas.length) {
      return res.status(200).send('BLOQUEIO-PAGAMENTO: nenhuma falha de pagamento no histórico (últimos 5000 eventos).');
    }
    falhas.sort((a, b) => String(a.ts).localeCompare(String(b.ts)));
    const inicio = falhas[0].ts, fim = falhas[falhas.length - 1].ts;
    // quem RESPONDEU depois da falha já foi alcançado de outra forma — não reenviar
    const ultimoIn = {};
    for (const e of evts) if (e.dir === 'in') {
      const k = d8(e.tel), t = new Date(e.ts).getTime();
      if (!ultimoIn[k] || t > ultimoIn[k]) ultimoIn[k] = t;
    }
    const porTel = {};
    for (const f of falhas) {
      if (!porTel[f.d8]) porTel[f.d8] = { tel: f.tel, d8: f.d8, qtd: 0, primeira: f.ts, ultima: f.ts, codigo: f.codigo, conta: false, invalido: false };
      const r = porTel[f.d8]; r.qtd++; r.ultima = f.ts; r.codigo = f.codigo;
      if (COD_CONTA.includes(f.codigo)) r.conta = true;
      if (COD_INVALIDO.includes(f.codigo)) r.invalido = true;
    }
    const alvos = Object.values(porTel).filter(r =>
      !(ultimoIn[r.d8] && ultimoIn[r.d8] > new Date(r.ultima).getTime()));
    const jaFalaram = Object.values(porTel).length - alvos.length;

    // ── APLICAR: devolve as fichas/orçamentos para os crons reenviarem ──
    if (String(req.query.aplicar || '') === '1') {
      // SÓ volta para a esteira quem falhou por bloqueio de CONTA. Número que não recebe
      // continuaria falhando — esse vai para conflito, para um humano ligar.
      const paraReenviar = alvos.filter(a => a.conta && !a.invalido);
      const paraLigar    = alvos.filter(a => a.invalido);
      const alvo8 = new Set(paraReenviar.map(a => a.d8));
      const [fA, fT, ab, orc] = await Promise.all([
        dbGet('fichas_adm'), dbGet('fichas_tv'),
        dbGet('wa_abordados').then(v => v || { tels: {} }),
        dbGet('wa_orc_enviados').then(v => v || { ids: {} }),
      ]);
      let fichasSoltas = 0, abordLimpos = 0, orcSoltos = 0;
      for (const banco of [fA, fT]) {
        for (const f of (((banco || {}).fichas) || [])) {
          if (!alvo8.has(d8(f.telefone))) continue;
          if (f.status === 'contato_feito' && f.abordadoPorBot) {
            f.status = 'criada'; f.contatoFeitoEm = null; f.abordadoPorBot = false;
            f.reenvioPagamento = new Date().toISOString();
            fichasSoltas++;
          }
        }
      }
      for (const k of Object.keys(ab.tels || {})) {
        if (alvo8.has(k)) { delete ab.tels[k]; abordLimpos++; }
      }
      for (const [k, v] of Object.entries(orc.ids || {})) {
        const t = d8((v && v.telefone) || '');
        if (t && alvo8.has(t)) { delete orc.ids[k]; orcSoltos++; }
      }
      if (fA) await dbSet('fichas_adm', fA);
      if (fT) await dbSet('fichas_tv', fT);
      await dbSet('wa_abordados', ab);
      await dbSet('wa_orc_enviados', orc);
      // confere o próprio resultado
      const chk = (await dbGet('wa_abordados')) || { tels: {} };
      const sobrou = [...alvo8].filter(k => (chk.tels || {})[k]).length;
      // NÚMEROS QUE NÃO RECEBEM → conflito para contato humano (não adianta reenviar)
      let conflitos = 0;
      const nomePorTel = {};
      for (const banco of [fA, fT]) for (const f of (((banco || {}).fichas) || [])) {
        const k = d8(f.telefone); if (k && !nomePorTel[k]) nomePorTel[k] = { nome: f.nome, equip: f.equipamento };
      }
      for (const a of paraLigar) {
        try {
          const K2 = (process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim();
          const info = nomePorTel[a.d8] || {};
          await fetch('https://reparoeletroadm.com/api/prospeccao?action=criar-conflito&k=' + K2, {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ nome: info.nome || 'Cliente', telefone: String(a.tel || '').replace(/\D/g, ''),
              equipamento: info.equip || '',
              motivo: '📵 WHATSAPP NÃO ENTREGA (erro ' + a.codigo + ', ' + a.qtd + ' tentativas) — ' +
                'número pode estar errado ou sem WhatsApp. LIGAR para o cliente; reenviar não resolve.' }),
          }).catch(() => null);
          conflitos++;
        } catch (e) {}
      }
      return res.status(200).send(
        'REENVIO LIBERADO\n' +
        'clientes atingidos=' + alvos.length +
        ' (bloqueio de conta=' + paraReenviar.length + ' · número inválido=' + paraLigar.length + ')\n' +
        'fichas devolvidas para abordagem=' + fichasSoltas + '\n' +
        'registros de "já abordado" limpos=' + abordLimpos + (sobrou ? ' (SOBRARAM ' + sobrou + ' — conferir)' : '') + '\n' +
        'orçamentos liberados para reenvio=' + orcSoltos + '\n' +
        'conflitos abertos para LIGAR (número inválido, não reenviado)=' + conflitos + '\n' +
        'Os crons reenviam sozinhos: abordagem a cada 5 min, orçamento a cada 3 min, só na janela comercial.');
    }

    // ── LEITURA ──
    return res.status(200).send(
      'BLOQUEIO POR PENDÊNCIA DE PAGAMENTO (erro 131042)\n' +
      'PRIMEIRA falha: ' + new Date(new Date(inicio).getTime() - 3 * 3600000).toISOString().replace('T', ' ').slice(0, 16) + ' (BRT)\n' +
      'ÚLTIMA  falha: ' + new Date(new Date(fim).getTime() - 3 * 3600000).toISOString().replace('T', ' ').slice(0, 16) + ' (BRT)\n' +
      'mensagens recusadas=' + falhas.length + ' | clientes distintos=' + Object.keys(porTel).length + '\n' +
      'para REENVIAR (bloqueio de conta)=' + alvos.filter(a => a.conta && !a.invalido).length +
      ' | NÚMERO INVÁLIDO, precisa LIGAR=' + alvos.filter(a => a.invalido).length +
      ' | já falaram depois (não mexer)=' + jaFalaram + '\n\n' +
      alvos.slice(0, 60).map(a => '  ' + a.d8 + ' · ' + a.qtd + 'x · desde ' +
        String(a.ultima).slice(11, 16)).join('\n') +
      '\n\nPara liberar o reenvio depois de regularizar o pagamento, acrescente &aplicar=1');
  }

  // ── 📋 RECUSADAS-DETALHE: telefone · hora do envio · template · hora da recusa ──
  if (action === 'recusadas-detalhe') {
    const CODIGOS = [131042, 131047, 131026, 133010];
    const evts = await lerEvts();
    const d8 = t => String(t || '').replace(/\D/g, '').slice(-8);
    const brt = ts => new Date(new Date(ts).getTime() - 3 * 3600000).toISOString().replace('T', ' ').slice(0, 16);
    // identifica qual template pelo conteúdo/rota do evento de saída
    const qualTemplate = e => {
      const t = String(e.texto || '');
      if (e.via === 'bot-auto-orcamento' || /orcamento_pronto/i.test(t)) return 'orcamento_pronto';
      if (/Logística da Reparo Eletro - TVs|conserto da sua TV/i.test(t)) return 'cadastro_recebido_tv';
      if (/TEMOS 2 OPÇÕES|Recebemos o seu cadastro/i.test(t)) return 'cadastro_recebido';
      if (/equipamento (está |ta )?pronto|conserto realizado/i.test(t)) return 'equipamento_pronto';
      if (e.tipo === 'template') return 'template (não identificado)';
      return 'TEXTO LIVRE (não era template)';
    };
    // saídas por telefone, ordenadas no tempo
    const outPorTel = {};
    for (const e of evts) {
      if (e.dir !== 'out' || e.tipo === 'falha' || e.tipo === 'nota') continue;
      (outPorTel[d8(e.tel)] = outPorTel[d8(e.tel)] || []).push(e);
    }
    for (const k of Object.keys(outPorTel)) outPorTel[k].sort((a, b) => String(a.ts).localeCompare(String(b.ts)));

    const linhas = [];
    for (const e of evts) {
      if (e.dir !== 'status' || !/failed/i.test(String(e.texto || ''))) continue;
      const cod = CODIGOS.find(c => String(e.texto).includes(String(c)));
      if (!cod) continue;
      const k = d8(e.tel);
      const tRec = new Date(e.ts).getTime();
      // casa pelo msgId quando existe; senão, a última saída antes da recusa (até 15 min)
      let orig = null;
      if (e.msgId) orig = (outPorTel[k] || []).find(o => o.msgId && o.msgId === e.msgId) || null;
      if (!orig) {
        const cands = (outPorTel[k] || []).filter(o => {
          const dt = tRec - new Date(o.ts).getTime();
          return dt >= 0 && dt <= 15 * 60000;
        });
        orig = cands.length ? cands[cands.length - 1] : null;
      }
      linhas.push({
        telefone: String(e.tel || ''),
        envio: orig ? brt(orig.ts) : '(não localizado)',
        template: orig ? qualTemplate(orig) : '(não localizado)',
        recusa: brt(e.ts), codigo: cod,
        casadoPor: orig ? (e.msgId && orig.msgId === e.msgId ? 'msgId' : 'horário') : '—',
      });
    }
    linhas.sort((a, b) => a.recusa.localeCompare(b.recusa));
    if (String(req.query.json || '') === '1') {
      return res.status(200).json({ ok: true, total: linhas.length, linhas });
    }
    const cab = 'RECUSADAS — ' + linhas.length + ' mensagens (horários em BRT)\n' +
      'telefone;envio;template;recusa;codigo;casado_por\n';
    return res.status(200).send(cab + linhas.map(l =>
      [l.telefone, l.envio, l.template, l.recusa, l.codigo, l.casadoPor].join(';')).join('\n'));
  }

  // ── 🏥 SAUDE-WABA: lê da Meta a configuração da conta do WhatsApp (só leitura) ──
  if (action === 'saude-waba') {
    const { token: tkW, phoneId: pidW } = await credenciais();
    if (!tkW || !pidW) return res.status(200).json({ ok: false, error: 'credenciais ausentes' });
    const G = 'https://graph.facebook.com/v20.0';
    const pega = async (rot, url) => {
      const r = await fetch(url).then(x => x.json()).catch(e => ({ error: { message: e.message } }));
      return { rot, ...(r && r.error ? { erro: r.error.message + ' (code ' + (r.error.code || '') + ')' } : { dados: r }) };
    };
    // 1) dados do número + a WABA dona dele
    const num = await pega('numero', `${G}/${pidW}?fields=display_phone_number,verified_name,quality_rating,name_status,throughput,platform_type&access_token=${tkW}`);
    // Descobre o WABA_ID pelo debug_token: os escopos granulares trazem os IDs
    // das contas às quais o token dá acesso. Também revela o que o token pode fazer.
    let wabaId = String(req.query.waba || '').trim();
    if (!wabaId || /^SEU_/i.test(wabaId)) wabaId = '';
    const dbg = await pega('token', `${G}/debug_token?input_token=${tkW}&access_token=${tkW}`);
    const gran = ((((dbg.dados || {}).data) || {}).granular_scopes) || [];
    const escopos = gran.map(g => g.scope + (g.target_ids ? '[' + g.target_ids.join(',') + ']' : '')).join(' · ');
    if (!wabaId) {
      const alvo = gran.find(g => /whatsapp_business_(management|messaging)/.test(g.scope) && (g.target_ids || []).length);
      if (alvo) wabaId = alvo.target_ids[0];
    }
    const passos = [num];
    if (wabaId) {
      passos.push(await pega('waba', `${G}/${wabaId}?fields=name,currency,timezone_id,account_review_status,business_verification_status,message_template_namespace,owner_business_info,health_status&access_token=${tkW}`));
      passos.push(await pega('templates', `${G}/${wabaId}/message_templates?fields=name,status,category,quality_score&limit=30&access_token=${tkW}`));
    }
    const linhas = [];
    for (const p of passos) {
      if (p.erro) { linhas.push('❌ ' + p.rot + ' — ' + p.erro); continue; }
      const d = p.dados || {};
      if (p.rot === 'numero') {
        linhas.push('NÚMERO: ' + (d.display_phone_number || '?') + ' · nome "' + (d.verified_name || '?') + '"');
        linhas.push('  qualidade=' + (d.quality_rating || '?') + ' · status do nome=' + (d.name_status || '?') +
          ' · plataforma=' + (d.platform_type || '?'));
        if (d.throughput) linhas.push('  throughput=' + JSON.stringify(d.throughput));
      } else if (p.rot === 'waba') {
        linhas.push('WABA: ' + (d.name || '?'));
        linhas.push('  MOEDA=' + (d.currency || '⚠️ VAZIA') + ' · FUSO=' + (d.timezone_id || '⚠️ VAZIO'));
        linhas.push('  revisão da conta=' + (d.account_review_status || '?') +
          ' · verificação do negócio=' + (d.business_verification_status || '?'));
        if (d.health_status) linhas.push('  saúde=' + JSON.stringify(d.health_status).slice(0, 400));
      } else if (p.rot === 'templates') {
        const ts = (d.data || []);
        linhas.push('TEMPLATES (' + ts.length + '):');
        for (const t of ts) linhas.push('  ' + t.name + ' · ' + t.status + ' · ' + (t.category || '') +
          (t.quality_score ? ' · qualidade=' + (t.quality_score.score || '?') : ''));
      }
    }
    if (escopos) linhas.push('ESCOPOS DO TOKEN: ' + escopos);
    if (wabaId) linhas.unshift('WABA_ID: ' + wabaId);
    else linhas.push('\n⚠️ Não consegui descobrir o ID da WABA pelo número — o token pode não ter escopo whatsapp_business_management.');
    return res.status(200).send(linhas.join('\n'));
  }

  // ── 🔍 DESCOBRIR-WABA: tenta vários caminhos até achar o ID da conta ──
  if (action === 'descobrir-waba') {
    const { token: tkD } = await credenciais();
    const tkAds = (process.env.META_ADS_TOKEN || '').trim();
    const G = 'https://graph.facebook.com/v20.0';
    const NEG = ['602045084706003', String(req.query.negocio || '').trim()].filter(Boolean);
    const tent = [];
    const tenta = async (nome, url) => {
      if (!url) return;
      const r = await fetch(url).then(x => x.json()).catch(e => ({ error: { message: e.message } }));
      if (r && r.error) { tent.push({ nome, erro: r.error.message.slice(0, 110) }); return null; }
      const arr = r.data || (r.id ? [r] : []);
      tent.push({ nome, achou: arr.length, ids: arr.map(x => x.id + (x.name ? ' (' + x.name + ')' : '')) });
      return arr.length ? arr : null;
    };
    let achados = null;
    for (const [rot, tk] of [['token do bot', tkD], ['token de anúncios', tkAds]]) {
      if (!tk) continue;
      for (const b of NEG) {
        achados = achados || await tenta(`próprias · ${rot} · negócio ${b}`, `${G}/${b}/owned_whatsapp_business_accounts?fields=id,name&access_token=${tk}`);
        achados = achados || await tenta(`de clientes · ${rot} · negócio ${b}`, `${G}/${b}/client_whatsapp_business_accounts?fields=id,name&access_token=${tk}`);
      }
      achados = achados || await tenta(`negócios visíveis · ${rot}`, `${G}/me/businesses?fields=id,name&access_token=${tk}`);
    }
    const linhas = ['TENTATIVAS DE DESCOBRIR O WABA_ID:'];
    for (const t of tent) {
      linhas.push('  ' + (t.erro ? '❌ ' + t.nome + ' — ' + t.erro
        : (t.achou ? '✅ ' : '➖ ') + t.nome + ' → ' + (t.ids.length ? t.ids.join(' | ') : 'nada')));
    }
    if (achados && achados.length) {
      linhas.push('');
      linhas.push('👉 USE ESTE: ' + achados[0].id);
      linhas.push('   /api/wa-bot?action=saude-waba&waba=' + achados[0].id + '&k=SUA_CHAVE');
    } else {
      linhas.push('');
      linhas.push('Nenhum caminho automático funcionou. Jeito manual mais rápido:');
      linhas.push('abra business.facebook.com/wa/manage/home e olhe a BARRA DE ENDEREÇO do navegador —');
      linhas.push('o ID aparece na própria URL como waba_id=XXXXXXXXXXXXX');
    }
    return res.status(200).send(linhas.join('\n'));
  }

  // ── 🔀 ORCAMENTO-MANUAL: liga/desliga o envio automático de orçamento ──
  if (action === 'orcamento-manual') {
    const cfg = (await dbGet('wa_bot_config')) || {};
    const v = String(req.query.ligar || '').trim();
    if (v === '1' || v === '0') {
      cfg.orcamentoManual = (v === '1');
      cfg.orcamentoManualEm = new Date().toISOString();
      await dbSet('wa_bot_config', cfg);
      const chk = (await dbGet('wa_bot_config')) || {};
      if (!!chk.orcamentoManual !== (v === '1')) {
        return res.status(500).json({ ok: false, error: 'não confirmou a gravação' });
      }
    }
    const c2 = (await dbGet('wa_bot_config')) || {};
    return res.status(200).send(
      'MODO DE ENVIO DE ORÇAMENTO: ' + (c2.orcamentoManual ? 'MANUAL (bot NÃO envia)' : 'AUTOMÁTICO (bot envia)') +
      (c2.orcamentoManualEm ? '\nalterado em ' + c2.orcamentoManualEm : '') +
      '\n\n&ligar=1 → manual (equipe envia por /orcamento)' +
      '\n&ligar=0 → automático (bot envia o template)');
  }

  if (action === 'buscar-conversas') {
    const alvos = String(req.query.tels || '').split(',').map(s => s.replace(/\D/g, '').trim()).filter(Boolean);
    if (!alvos.length) return res.status(400).json({ ok: false, error: 'informe ?tels=1234,5678 (finais separados por vírgula)' });
    const [evts, arq] = await Promise.all([lerEvts(), dbGet('wa_arquivadas').then(v => v || { tels: {} })]);
    const saida = alvos.map(alvo => {
      const msgs = evts.filter(e => String(e.tel || '').replace(/\D/g, '').endsWith(alvo) && e.dir !== 'status');
      const env = msgs.filter(m => m.dir === 'out');
      const rec = msgs.filter(m => m.dir === 'in');
      const d8 = msgs.length ? String(msgs[0].tel || '').replace(/\D/g, '').slice(-8) : null;
      return { busca: alvo,
        temConversa: msgs.length > 0,
        telefoneCompleto: msgs.length ? msgs[msgs.length - 1].tel : null,
        nome: (msgs.find(m => m.nome) || {}).nome || null,
        totalMensagens: msgs.length, enviadas: env.length, recebidas: rec.length,
        primeira: msgs.length ? msgs[0].ts : null,
        ultima: msgs.length ? msgs[msgs.length - 1].ts : null,
        arquivada: d8 ? !!arq.tels[d8] : false,
        ultimoTexto: msgs.length ? String(msgs[msgs.length - 1].texto || '').slice(0, 90) : null,
        diagnostico: !msgs.length ? '❌ nenhuma mensagem no histórico'
          : (d8 && arq.tels[d8] ? '🗄 conversa existe mas está ARQUIVADA (ative o ícone 🗄 no painel)'
          : (env.length && !rec.length ? '🟡 bot enviou, cliente não respondeu'
          : '🟢 conversa ativa')) };
    });
    // &detalhe=1 → lista as mensagens uma a uma (para investigar caso a caso)
    if (String(req.query.detalhe || '') === '1') {
      const linhas = [];
      for (const alvo of alvos) {
        const msgs = evts.filter(e => String(e.tel || '').replace(/\D/g, '').endsWith(alvo) && e.dir !== 'status');
        linhas.push('══ ' + alvo + ' — ' + msgs.length + ' msgs ══');
        for (const m of msgs) {
          const hora = new Date(new Date(m.ts).getTime() - 3 * 3600000).toISOString().slice(5, 16).replace('T', ' ');
          const seta = m.dir === 'in' ? '<<' : (m.dir === 'out' ? '>>' : '**');
          linhas.push(hora + ' ' + seta + ' [' + (m.tipo || '?') + (m.via ? '/' + m.via : '') + ']' +
            (m.msgId ? '' : ' SEM-MSGID') + ' ' + String(m.texto || '').replace(/\n/g, ' ').slice(0, 110));
        }
      }
      return res.status(200).send(linhas.join('\n'));
    }
    return res.status(200).json({ ok: true,
      janelaHistorico: evts.length + ' mensagens carregadas',
      resultado: saida });
  }

  // ── ✅ APROVADOS-SEM-GATILHO: cliente aprovou na conversa mas o card não avançou ──
  if (action === 'aprovados-sem-gatilho') {
    const [evts, ppA, ppT] = await Promise.all([lerEvts(), dbGet('reparoeletro_pipe'), dbGet('tv_pipe')]);
    const d8 = t => String(t || '').replace(/\D/g, '').slice(-8);
    // quem está parado aguardando decisão
    const parados = {};
    for (const [banco, sis] of [[ppA, 'adm'], [ppT, 'tv']]) {
      for (const c of (((banco || {}).cards) || [])) {
        if (!['aguardando_aprovacao', 'ultima_chamada'].includes(c.phaseId || c.phase)) continue;
        const d = d8(c.telefone); if (d.length < 8) continue;
        parados[d] = { id: c.id, sis, nome: c.nomeContato, equipamento: c.equipamento,
          valor: c.valor, fase: c.phaseId || c.phase };
      }
    }
    // procurou sinal de aprovação nas mensagens do cliente
    const SIM = /\b(aprovo|aprovado|pode fazer|pode consertar|pode arrumar|pode reparar|autorizo|pode executar|manda fazer|quero (que )?conserte)\b/i;
    const achados = {};
    for (const e of evts) {
      if (e.dir !== 'in' || !e.texto) continue;
      const d = d8(e.tel); if (!parados[d]) continue;
      if (!SIM.test(String(e.texto))) continue;
      achados[d] = { ts: e.ts, texto: String(e.texto).slice(0, 80) };
    }
    const lista = Object.keys(achados).map(d => Object.assign({ d8: d,
      aprovouEm: achados[d].ts, frase: achados[d].texto }, parados[d]))
      .sort((a, b) => String(a.aprovouEm).localeCompare(String(b.aprovouEm)));
    return res.status(200).json({ ok: true, total: lista.length,
      explicacao: 'cliente disse que aprova mas o card segue aguardando — precisa do gatilho de aprovação',
      lista: lista.map(x => x.nome + ' ' + String(x.id).slice(-4) + ' | ' + (x.equipamento || '') +
        ' | R$ ' + (x.valor || '?') + ' | "' + x.frase + '" | ' + String(x.aprovouEm).slice(0, 16).replace('T', ' ')),
      detalhe: lista });
  }

  // ── ↩️ DESFAZER-APROVACAO: devolve o card para aguardando aprovação ──
  if (action === 'desfazer-aprovacao') {
    const alvo = String(req.query.id || '').trim();
    const tel = String(req.query.tel || '').replace(/\D/g, '');
    if (!alvo && !tel) return res.status(400).json({ ok: false, error: 'informe ?id= do card ou ?tel= (4+ dígitos finais)' });
    const KDF = (process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim();
    const [ppA, ppT] = await Promise.all([dbGet('reparoeletro_pipe'), dbGet('tv_pipe')]);
    const achados = [];
    for (const [b, s, api] of [[ppA, 'adm', 'pipe'], [ppT, 'tv', 'tv-pipe']]) {
      for (const c of (((b || {}).cards) || [])) {
        const bate = alvo ? c.id === alvo : String(c.telefone || '').replace(/\D/g, '').endsWith(tel);
        if (!bate) continue;
        achados.push({ id: c.id, sis: s, api, nome: c.nomeContato, equipamento: c.equipamento,
          faseAtual: c.phaseId || c.phase, valor: c.valor });
      }
    }
    if (!achados.length) return res.status(404).json({ ok: false, error: 'card não encontrado' });
    if (String(req.query.aplicar || '') !== '1') {
      return res.status(200).json({ ok: true, modo: 'prévia', achados,
        dica: 'para desfazer: &aplicar=1 (volta para aguardando_aprovacao)' });
    }
    const feitos = [];
    for (const a of achados) {
      const r = await fetch(`https://reparoeletroadm.com/api/${a.api}?action=mover&k=${KDF}`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ id: a.id, phase: 'aguardando_aprovacao' }),
      }).then(x => x.json()).catch(e => ({ error: e.message }));
      feitos.push(a.nome + ': ' + (r && !r.error ? 'devolvido para aguardando aprovação' : 'falhou — ' + (r.error || '?')));
    }
    return res.status(200).json({ ok: true, feitos });
  }

  // ── 🔍 FALHAS-DETALHE: descobre O QUE cada disparo recusado estava tentando fazer ──
  if (action === 'falhas-detalhe') {
    const desde = String(req.query.desde || '2026-08-01').slice(0, 10);
    const hoje = new Date(Date.now() - 3 * 3600000).toISOString().slice(0, 10);
    const dias = [];
    for (let d = new Date(desde + 'T12:00:00Z'); d.toISOString().slice(0, 10) <= hoje; d.setUTCDate(d.getUTCDate() + 1)) {
      dias.push(d.toISOString().slice(0, 10));
    }
    const falhas = [];
    for (const dia of dias) {
      const reg = await dbGet('wa_falhas_' + dia);
      for (const i of (((reg || {}).itens) || [])) falhas.push(i);
    }
    if (!falhas.length) return res.status(200).json({ ok: false, error: 'nenhuma falha gravada — rode reconstruir-falhas&aplicar=1 antes' });

    // onde cada cliente está agora — é isso que revela o que se tentava fazer
    const [lgA, lgT, ppA, ppT, pros, evts] = await Promise.all([
      dbGet('reparoeletro_logistica'), dbGet('tv_logistica'),
      dbGet('reparoeletro_pipe'), dbGet('tv_pipe'), dbGet('prospeccao_adm'), lerEvts(),
    ]);
    const d8 = t => String(t || '').replace(/\D/g, '').slice(-8);
    const onde = {};
    const marca = (tel, local, fase, nome, equip) => {
      const k = d8(tel); if (k.length < 8) return;
      if (!onde[k]) onde[k] = { nome, equipamento: equip, lugares: [] };
      onde[k].lugares.push(local + (fase ? ':' + fase : ''));
      if (!onde[k].nome && nome) onde[k].nome = nome;
      if (!onde[k].equipamento && equip) onde[k].equipamento = equip;
    };
    for (const f of (((lgA || {}).fichas) || [])) marca(f.telefone, 'logística ADM', f.phase, f.nome, f.equipamento);
    for (const f of (((lgT || {}).fichas) || [])) marca(f.telefone, 'logística TV', f.phase, f.nome, f.equipamento);
    for (const c of (((ppA || {}).cards) || [])) marca(c.telefone, 'pipe ADM', c.phaseId || c.phase, c.nomeContato, c.equipamento);
    for (const c of (((ppT || {}).cards) || [])) marca(c.telefone, 'pipe TV', c.phaseId || c.phase, c.nomeContato, c.equipamento);
    for (const f of (((pros || {}).fichas) || [])) marca(f.telefone, 'prospecção', f.status, f.nome, f.equipamento);

    // última mensagem que o bot tentou mandar para cada um
    const ultMsg = {};
    for (const e of evts) {
      if (e.dir !== 'out') continue;
      const k = d8(e.tel); if (k.length < 8) continue;
      ultMsg[k] = { texto: String(e.texto || '').slice(0, 90), ts: e.ts, via: e.via || null };
    }

    // deduz a INTENÇÃO a partir da fase em que o cliente está
    const intencao = (lugares, texto) => {
      const L = (lugares || []).join(' ').toLowerCase();
      const T = String(texto || '').toLowerCase();
      if (/orc_enviado|orcamento|orc_registrado/.test(L) || /orçamento|orcamento|valor|conserto fica/.test(T)) return '💰 envio de ORÇAMENTO';
      if (/finalizado|entrega|receber|erp/.test(L) || /pronto|retirad|finaliz/.test(T)) return '✅ aviso de EQUIPAMENTO PRONTO';
      if (/ficha_criada|lead|prospec/.test(L) || /bom dia|boa tarde|tudo bem|sou o pedro/.test(T)) return '👋 PRIMEIRA ABORDAGEM';
      if (/aguardando_aprovacao/.test(L)) return '🔔 REATIVAÇÃO de orçamento';
      if (/liberado_coleta|horario_marcado|motorista/.test(L)) return '🚚 combinação de COLETA';
      if (/conflito/.test(L)) return '⚠️ retorno de CONFLITO';
      return '❓ não identificado';
    };

    const linhas = [];
    const porIntencao = {};
    for (const f of falhas) {
      const k = d8(f.telefone);
      const info = onde[k] || {};
      const msg = ultMsg[k] || {};
      const inte = intencao(info.lugares, msg.texto || f.textoTentado);
      porIntencao[inte] = (porIntencao[inte] || 0) + 1;
      linhas.push({
        quando: f.ts, telefone: f.telefone,
        cliente: info.nome || '(não encontrado)',
        equipamento: info.equipamento || '',
        ondeEsta: (info.lugares || []).join(' · ') || '(fora do sistema)',
        oQueTentava: inte,
        ultimaMensagem: msg.texto || f.textoTentado || '',
        motivo: f.motivo,
      });
    }
    linhas.sort((a, b) => String(b.quando).localeCompare(String(a.quando)));

    if (String(req.query.curto || '') === '1') {
      return res.status(200).json({ ok: true, total: linhas.length, porIntencao,
        lista: linhas.slice(0, 40).map(l => String(l.quando).slice(5, 16) + ' | ' +
          String(l.cliente).slice(0, 16) + ' ' + String(l.telefone).slice(-4) + ' | ' +
          l.oQueTentava + ' | ' + String(l.equipamento).slice(0, 18)) });
    }
    return res.status(200).json({ ok: true, total: linhas.length, porIntencao, detalhe: linhas.slice(0, 150) });
  }

  // ── 🔄 RECONSTRUIR-FALHAS: varre o histórico e recupera as falhas anteriores ao registro ──
  if (action === 'reconstruir-falhas') {
    const desde = String(req.query.desde || '2026-08-01').slice(0, 10);
    const evts = await lerEvts();
    // 1) mapa msgId → o envio correspondente (para saber o template e a origem)
    const envios = {};
    for (const e of evts) {
      if (e.dir !== 'out') continue;
      const m = String(e.texto || '').match(/\[([a-z_0-9]+)\]/i);
      const chave = e.msgId || null;
      const info = { template: m ? m[1] : null, via: e.via || null,
        texto: String(e.texto || '').slice(0, 120), tel: String(e.tel || ''), ts: e.ts };
      if (chave) envios[chave] = info;
      // reserva por telefone+minuto, quando o status não trouxer o msgId
      envios['t:' + String(e.tel || '').replace(/\D/g, '').slice(-8) + ':' + String(e.ts || '').slice(0, 16)] = info;
    }
    // 2) percorre os status de falha
    const porDia = {};
    let achadas = 0;
    for (const e of evts) {
      const ehStatus = e.dir === 'status' || e.tipo === 'status';
      if (!ehStatus) continue;
      const txt = String(e.texto || '');
      if (!/^failed/i.test(txt)) continue;
      const dia = String(e.ts || '').slice(0, 10);
      if (dia < desde) continue;
      const d8e = String(e.tel || '').replace(/\D/g, '').slice(-8);
      const info = envios[e.msgId] || envios['t:' + d8e + ':' + String(e.ts || '').slice(0, 16)] || null;
      const mCod = txt.match(/"code":\s*(\d+)/);
      const mTit = txt.match(/"title":\s*"([^"]+)"/);
      if (!porDia[dia]) porDia[dia] = { itens: [] };
      if ((porDia[dia].itens || []).some(x => x.msgId && x.msgId === e.msgId)) continue;
      porDia[dia].itens.push({
        ts: e.ts, telefone: String(e.tel || ''), msgId: e.msgId || null,
        template: (info && info.template) || '(não identificado)',
        origem: (info && info.via) || '(não informada)',
        textoTentado: (info && info.texto) || '',
        codigo: mCod ? Number(mCod[1]) : 0,
        motivo: mTit ? mTit[1] : 'falha',
        recuperado: false, reconstruido: true,
      });
      achadas++;
    }
    // 3) grava, sem apagar o que já existe
    const gravados = {};
    if (String(req.query.aplicar || '') === '1') {
      for (const dia of Object.keys(porDia)) {
        const chave = 'wa_falhas_' + dia;
        const reg = (await dbGet(chave)) || { itens: [] };
        const jaTem = new Set((reg.itens || []).map(x => x.msgId).filter(Boolean));
        const novos = porDia[dia].itens.filter(x => !x.msgId || !jaTem.has(x.msgId));
        reg.itens = (reg.itens || []).concat(novos);
        reg.atualizadoEm = new Date().toISOString();
        await dbSet(chave, reg);
        gravados[dia] = novos.length;
      }
      return res.status(200).json({ ok: true, aplicado: true, falhasGravadas: achadas, porDia: gravados,
        proximo: 'consulte com action=fila-recuperacao' });
    }
    return res.status(200).json({ ok: true, modo: 'prévia',
      eventosVarridos: evts.length, falhasEncontradas: achadas,
      porDia: Object.keys(porDia).reduce((o, d) => { o[d] = porDia[d].itens.length; return o; }, {}),
      amostra: Object.values(porDia).flatMap(d => d.itens).slice(0, 15).map(i =>
        String(i.ts).slice(5, 16) + ' | ' + String(i.telefone).slice(-8) + ' | ' + i.template + ' | ' + i.origem + ' | ' + i.motivo),
      dica: 'para gravar: &aplicar=1' });
  }

  // ── 📒 FILA-RECUPERACAO: tudo que a Meta recusou desde o bloqueio ──
  if (action === 'fila-recuperacao') {
    const desde = String(req.query.desde || '2026-08-01').slice(0, 10);
    const hoje = new Date(Date.now() - 3 * 3600000).toISOString().slice(0, 10);
    const dias = [];
    for (let d = new Date(desde + 'T12:00:00Z'); d.toISOString().slice(0, 10) <= hoje; d.setUTCDate(d.getUTCDate() + 1)) {
      dias.push(d.toISOString().slice(0, 10));
    }
    const todos = [];
    for (const dia of dias) {
      const reg = await dbGet('wa_falhas_' + dia);
      for (const i of (((reg || {}).itens) || [])) todos.push({ ...i, dia });
    }
    // agrupa por telefone: quantas tentativas cada cliente teve
    const porTel = {};
    for (const i of todos) {
      const t = String(i.telefone || '').replace(/\D/g, '');
      if (!porTel[t]) porTel[t] = { telefone: t, tentativas: 0, templates: new Set(),
        origens: new Set(), primeira: i.ts, ultima: i.ts, recuperado: false };
      const p = porTel[t];
      p.tentativas++;
      if (i.template) p.templates.add(i.template);
      if (i.origem) p.origens.add(i.origem);
      if (i.ts < p.primeira) p.primeira = i.ts;
      if (i.ts > p.ultima) p.ultima = i.ts;
      if (i.recuperado) p.recuperado = true;
    }
    const lista = Object.values(porTel).map(p => ({
      telefone: p.telefone, tentativas: p.tentativas,
      templates: [...p.templates], origens: [...p.origens],
      primeiraTentativa: p.primeira, ultimaTentativa: p.ultima,
      recuperado: p.recuperado,
    })).sort((a, b) => b.tentativas - a.tentativas);

    const porTemplate = todos.reduce((o, i) => { const k = i.template || '?'; o[k] = (o[k] || 0) + 1; return o; }, {});
    const porOrigem = todos.reduce((o, i) => { const k = i.origem || '?'; o[k] = (o[k] || 0) + 1; return o; }, {});
    const porMotivo = todos.reduce((o, i) => { const k = i.codigo + ' ' + (i.motivo || ''); o[k] = (o[k] || 0) + 1; return o; }, {});

    if (String(req.query.curto || '') === '1') {
      return res.status(200).json({ ok: true, desde, totalFalhas: todos.length,
        clientesDistintos: lista.length, porTemplate, porOrigem,
        lista: lista.slice(0, 40).map(c => c.telefone.slice(-8) + ' | ' + c.tentativas + 'x | ' +
          (c.templates.join(',') || '?') + ' | ' + (c.origens.join(',') || '?')) });
    }
    return res.status(200).json({ ok: true, desde, ate: hoje,
      totalFalhas: todos.length, clientesDistintos: lista.length,
      porTemplate, porOrigem, porMotivo,
      clientes: lista, bruto: todos.slice(-100) });
  }

  // ── 📬 STATUS-ENVIO: o que aconteceu com as mensagens enviadas a um número ──
  if (action === 'status-envio') {
    const tel = String(req.query.tel || '').replace(/\D/g, '');
    if (tel.length < 8) return res.status(400).json({ ok: false, error: 'informe ?tel=' });
    const d8x = tel.slice(-8);
    const evts = await lerEvts();
    const meus = evts.filter(e => String(e.tel || '').replace(/\D/g, '').endsWith(d8x)).slice(-40);
    const enviados = meus.filter(e => e.dir === 'out');
    const status = meus.filter(e => e.dir === 'status' || e.tipo === 'status');
    const falhas = status.filter(s => /fail|error|undeliver/i.test(String(s.texto || '')));
    const entregues = status.filter(s => /delivered|read/i.test(String(s.texto || '')));
    return res.status(200).json({ ok: true, telefone: tel,
      enviados: enviados.length, comStatus: status.length,
      entregues: entregues.length, falhas: falhas.length,
      ultimoStatus: status.length ? String(status[status.length - 1].texto).slice(0, 300) : '(nenhum status recebido)',
      linhaDoTempo: meus.slice(-15).map(e => String(e.ts || '').slice(11, 16) + ' ' + e.dir +
        ' | ' + String(e.texto || '').slice(0, 70)),
      leitura: status.length === 0
        ? 'a Meta aceitou mas NÃO devolveu status — normalmente significa que o número não tem WhatsApp ativo, ou o webhook de status não está chegando'
        : (falhas.length ? 'houve falha de entrega — ver ultimoStatus' : 'entregue') });
  }

  // ── 🧪 TESTE-TEMPLATE: dispara um template para um número, por link ──
  if (action === 'teste-template') {
    const tel = String(req.query.tel || '').replace(/\D/g, '');
    if (tel.length < 12) return res.status(400).json({ ok: false, error: 'informe ?tel=5531XXXXXXXXX' });
    const nome = String(req.query.nome || 'tudo bem').split(' ')[0];
    const tpl = String(req.query.template || 'conserto_finalizado');
    const { token: tkX, phoneId: pidX } = await credenciais();
    if (!tkX || !pidX) return res.status(200).json({ ok: false, error: 'credenciais ausentes' });
    const r = await fetch(`https://graph.facebook.com/v20.0/${pidX}/messages`, {
      method: 'POST', headers: { Authorization: `Bearer ${tkX}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ messaging_product: 'whatsapp', to: tel, type: 'template',
        template: { name: tpl, language: { code: 'pt_BR' },
          components: [{ type: 'body', parameters: [{ type: 'text', text: nome }] }] } }),
    }).then(x => x.json()).catch(e => ({ error: { message: e.message } }));
    const ok = !!(r && r.messages && r.messages[0]);
    if (ok) {
      await indexarEnvio(r.messages[0].id, tpl, 'teste-manual', nome, tel);
      await rpushEvt({ ts: new Date().toISOString(), tel, dir: 'out',
        texto: '🧪 [teste ' + tpl + '] ' + nome, tipo: 'template', via: 'teste-manual' });
    }
    return res.status(200).json({ ok, template: tpl, para: tel,
      msgId: ok ? r.messages[0].id : null,
      erro: ok ? undefined : ((r && r.error && (r.error.error_user_msg || r.error.message)) || 'falha'),
      codigo: (r && r.error && r.error.code) || undefined,
      dica: ok ? 'confira o WhatsApp do número' : 'se o erro citar o template, ele pode não estar aprovado' });
  }

  // ── 📤 EXPORTAR-TEMPLATES: textos completos, prontos para recriar em outra conta ──
  if (action === 'exportar-templates') {
    const tkE = String(req.query.token || '').trim() || (await credenciais()).token;
    const G = 'https://graph.facebook.com/v20.0';
    const ids = [String(req.query.waba || '1050574074327587'), '1699351717944043'];
    const todos = {};
    for (const id of ids) {
      const r = await fetch(`${G}/${id}/message_templates?fields=name,status,category,language,components&limit=60&access_token=${tkE}`)
        .then(x => x.json()).catch(() => null);
      for (const t of ((r && r.data) || [])) {
        if (/jaspers_market|hello_world/.test(t.name)) continue;      // exemplos da Meta
        if (todos[t.name]) continue;
        todos[t.name] = t;
      }
    }
    const saida = Object.values(todos).map(t => {
      const partes = {};
      for (const c of (t.components || [])) {
        if (c.type === 'HEADER') partes.cabecalho = c.format === 'TEXT' ? c.text : ('[' + c.format + ']');
        if (c.type === 'BODY') partes.corpo = c.text;
        if (c.type === 'FOOTER') partes.rodape = c.text;
        if (c.type === 'BUTTONS') partes.botoes = (c.buttons || []).map(b => b.type + ': ' + (b.text || ''));
      }
      const vars = (String(partes.corpo || '').match(/\{\{(\d+)\}\}/g) || []);
      return { nome: t.name, categoria: t.category, idioma: t.language, status: t.status,
        variaveis: vars.length, ...partes };
    }).sort((a, b) => a.nome.localeCompare(b.nome));
    return res.status(200).json({ ok: true, total: saida.length,
      instrucao: 'copie o CORPO de cada um ao recriar na conta nova, mantendo categoria UTILITY e idioma pt_BR',
      templates: saida });
  }

  // ── 🔍 COMPARAR-TEMPLATES: confere se os templates da WABA nova servem de verdade ──
  if (action === 'comparar-templates') {
    const tkC = String(req.query.token || '').trim() || (await credenciais()).token;
    const G = 'https://graph.facebook.com/v20.0';
    const nova = String(req.query.nova || '1699351717944043');
    const velha = String(req.query.velha || '1050574074327587');
    const campos = 'name,status,category,language,components,quality_score,rejected_reason';
    const busca = async (id) => {
      const r = await fetch(`${G}/${id}/message_templates?fields=${campos}&limit=60&access_token=${tkC}`)
        .then(x => x.json()).catch(e => ({ error: { message: e.message } }));
      return (r && r.data) ? r.data : { erro: (r.error || {}).message };
    };
    const tNova = await busca(nova);
    const tVelha = await busca(velha);
    if (tNova.erro) return res.status(200).json({ ok: false, error: 'não li a WABA nova: ' + tNova.erro });

    const corpoDe = (t) => {
      const c = (t.components || []).find(x => x.type === 'BODY');
      return c ? String(c.text || '') : '';
    };
    const varsDe = (txt) => (String(txt).match(/\{\{\d+\}\}/g) || []).length;

    // os que o sistema realmente usa
    const USADOS = ['cadastro_recebido', 'cadastro_recebido_tv', 'conserto_finalizado', 'boas_vindas_reparo', 'orcamento_pronto', 'coleta_confirmada'];
    const mapaVelha = {};
    if (Array.isArray(tVelha)) for (const t of tVelha) mapaVelha[t.name] = t;

    const analise = USADOS.map(nome => {
      const n = (tNova || []).find(t => t.name === nome);
      const v = mapaVelha[nome];
      if (!n) return { template: nome, situacao: '❌ NÃO EXISTE na WABA nova',
        existeNaVelha: !!v, textoAtual: v ? corpoDe(v).slice(0, 120) : null };
      const corpoN = corpoDe(n), corpoV = v ? corpoDe(v) : null;
      const igual = corpoV != null && corpoN.trim() === corpoV.trim();
      return { template: nome,
        situacao: n.status === 'APPROVED' ? (igual ? '✅ existe, aprovado e IDÊNTICO' : '⚠️ existe e aprovado, mas com TEXTO DIFERENTE') : ('⚠️ status ' + n.status),
        idioma: n.language, categoria: n.category,
        variaveis: varsDe(corpoN),
        variaveisNaVelha: corpoV != null ? varsDe(corpoV) : null,
        textoNovo: corpoN.slice(0, 220),
        textoVelho: corpoV != null ? corpoV.slice(0, 220) : '(não existe na velha)',
      };
    });

    const faltando = analise.filter(a => a.situacao.includes('NÃO EXISTE'));
    const diferentes = analise.filter(a => a.situacao.includes('DIFERENTE'));
    const varsDivergentes = analise.filter(a => a.variaveisNaVelha != null && a.variaveis !== a.variaveisNaVelha);

    return res.status(200).json({ ok: faltando.length === 0 && varsDivergentes.length === 0,
      VEREDITO: faltando.length
        ? '❌ faltam ' + faltando.length + ' template(s) que o sistema usa: ' + faltando.map(f => f.template).join(', ')
        : (varsDivergentes.length
          ? '⚠️ ' + varsDivergentes.length + ' com número de variáveis diferente — o envio quebraria'
          : (diferentes.length ? '✅ todos existem e estão aprovados (textos diferem, mas o envio funciona)' : '✅ todos existem, aprovados e idênticos')),
      atencao: varsDivergentes.length ? varsDivergentes.map(v => v.template + ': nova tem ' + v.variaveis + ' variável(is), velha tem ' + v.variaveisNaVelha) : null,
      analise,
      todosOsTemplatesDaNova: (tNova || []).map(t => t.name + ' | ' + t.status + ' | ' + t.category + ' | ' + (t.language || '?')),
      totalNaNova: (tNova || []).length,
      totalNaVelha: Array.isArray(tVelha) ? tVelha.length : 'não consegui ler' });
  }

  // ── 🔎 WABA-ALTERNATIVA: investiga a segunda conta do negócio ──
  if (action === 'waba-alternativa') {
    const tkX = String(req.query.token || '').trim() || (await credenciais()).token;
    const G = 'https://graph.facebook.com/v20.0';
    const alt = String(req.query.waba || '1699351717944043');
    const atual = '1050574074327587';
    const out = [];
    const t = async (rot, url, metodo, corpo) => {
      const opt = { method: metodo || 'GET' };
      if (corpo) { opt.headers = { 'Content-Type': 'application/x-www-form-urlencoded' }; opt.body = new URLSearchParams(corpo).toString(); }
      const r = await fetch(url, opt).then(x => x.json()).catch(e => ({ error: { message: e.message } }));
      const ok = !(r && r.error);
      out.push({ passo: rot, ok, erro: ok ? null : (r.error.error_user_msg || r.error.message),
        codigo: ok ? null : r.error.code, dados: ok ? r : undefined });
      return ok ? r : null;
    };

    // 1) o que a WABA alternativa tem
    await t('WABA alternativa: campos',
      `${G}/${alt}?fields=id,name,currency,timezone_id,account_review_status,business_verification_status,message_template_namespace,ownership_type&access_token=${tkX}`);
    await t('WABA alternativa: números',
      `${G}/${alt}/phone_numbers?fields=id,display_phone_number,verified_name,status,quality_rating&access_token=${tkX}`);
    await t('WABA alternativa: templates',
      `${G}/${alt}/message_templates?fields=name,status,category&limit=30&access_token=${tkX}`);
    // 2) comparar com a atual
    await t('WABA atual: campos (comparação)',
      `${G}/${atual}?fields=id,name,currency,timezone_id,account_review_status&access_token=${tkX}`);

    // 3) tentar definir BRL na alternativa (se ainda não tem moeda, pode aceitar)
    if (String(req.query.definirBRL || '') === '1') {
      await t('DEFINIR moeda BRL na alternativa', `${G}/${alt}?access_token=${tkX}`, 'POST', { currency: 'BRL' });
      await t('DEFINIR fuso na alternativa', `${G}/${alt}?access_token=${tkX}`, 'POST', { timezone_id: '25' });
    }

    const cAlt = (out.find(o => o.passo.includes('alternativa: campos')) || {}).dados || {};
    const cAtu = (out.find(o => o.passo.includes('atual: campos')) || {}).dados || {};
    return res.status(200).json({ ok: true,
      COMPARACAO: {
        alternativa: { id: alt, nome: cAlt.name, moeda: cAlt.currency || '(não definida)',
          fuso: cAlt.timezone_id || '(não definido)', revisao: cAlt.account_review_status,
          propriedade: cAlt.ownership_type },
        atual: { id: atual, nome: cAtu.name, moeda: cAtu.currency, fuso: cAtu.timezone_id },
      },
      VEREDITO: !cAlt.currency
        ? '🎯 a WABA alternativa NÃO tem moeda definida — vale tentar definir BRL e migrar o número para ela'
        : (cAlt.currency === 'BRL'
          ? '🎯 a alternativa JÁ está em BRL — migrar o número para ela resolve'
          : '⚠️ a alternativa também está em ' + cAlt.currency),
      resultado: out,
      comoTentar: 'acrescente &definirBRL=1 para tentar gravar a moeda na alternativa' });
  }

  // ── 💳 CONTA-PAGAMENTO: explora a payment_account descoberta na auditoria ──
  if (action === 'conta-pagamento') {
    const tkP1 = String(req.query.token || '').trim() || (await credenciais()).token;
    const tkAds = String(req.query.tokenAds || '').trim() || tkP1;
    const G = 'https://graph.facebook.com/v20.0';
    const pay = String(req.query.pay || '652349156164996');
    const neg = String(req.query.negocio || '114657968057637');
    const wid = String(req.query.waba || '1050574074327587');
    const out = [];
    const t = async (rot, url, tk, metodo, corpo) => {
      const opt = { method: metodo || 'GET' };
      if (corpo) { opt.headers = { 'Content-Type': 'application/x-www-form-urlencoded' }; opt.body = new URLSearchParams(corpo).toString(); }
      const r = await fetch(url.replace('TOKEN', tk), opt).then(x => x.json()).catch(e => ({ error: { message: e.message } }));
      const ok = !(r && r.error);
      out.push({ caminho: rot, ok, erro: ok ? null : (r.error.error_user_msg || r.error.message),
        codigo: ok ? null : r.error.code,
        dados: ok ? (JSON.stringify(r).length > 800 ? JSON.stringify(r).slice(0, 800) + '…' : r) : undefined });
      return ok ? r : null;
    };

    // ── a conta de pagamento em si ──
    await t('conta de pagamento (campos)', `${G}/${pay}?access_token=TOKEN`, tkP1);
    await t('conta de pagamento > funding_sources', `${G}/${pay}/funding_sources?access_token=TOKEN`, tkP1);
    await t('conta de pagamento > payment_methods', `${G}/${pay}/payment_methods?access_token=TOKEN`, tkP1);
    await t('conta de pagamento > invoices', `${G}/${pay}/invoices?access_token=TOKEN`, tkP1);
    await t('conta de pagamento > transactions', `${G}/${pay}/transactions?access_token=TOKEN`, tkP1);
    await t('conta de pagamento > billing', `${G}/${pay}/billing?access_token=TOKEN`, tkP1);

    // ── o negócio, com mais campos ──
    await t('negócio: campos ampliados',
      `${G}/${neg}?fields=id,name,payment_account_id,primary_page,timezone_id,two_factor_type,is_disabled_by_disconnect,verification_status,collaborative_ads_managed_partner_business_info&access_token=TOKEN`, tkP1);
    await t('negócio > owned_whatsapp_business_accounts',
      `${G}/${neg}/owned_whatsapp_business_accounts?fields=id,name,currency,account_review_status&access_token=TOKEN`, tkP1);
    await t('negócio > client_whatsapp_business_accounts',
      `${G}/${neg}/client_whatsapp_business_accounts?fields=id,name,currency&access_token=TOKEN`, tkP1);

    // ── com o token de ANÚNCIOS (o outro tem ads_management) ──
    await t('[ads] conta de anúncios: saldo e método',
      `${G}/act_1267284360833794?fields=balance,currency,amount_spent,spend_cap,funding_source,funding_source_details,is_prepay_account,account_status,business&access_token=TOKEN`, tkAds);
    await t('[ads] negócio do anúncio > extendedcredits',
      `${G}/1267284360833794/extendedcredits?access_token=TOKEN`, tkAds);
    await t('[ads] conta > transações recentes',
      `${G}/act_1267284360833794/transactions?fields=id,charge_type,status,billed_amount_details,time&limit=5&access_token=TOKEN`, tkAds);

    // ── a WABA vista pelo token de anúncios ──
    await t('[ads] WABA campos', `${G}/${wid}?fields=id,name,currency,primary_funding_id&access_token=TOKEN`, tkAds);

    // ── tentativas de ESCRITA na conta de pagamento ──
    if (String(req.query.forcar || '') === '1') {
      await t('ESCRITA: vincular funding da conta de anúncios à WABA',
        `${G}/${wid}?access_token=TOKEN`, tkAds, 'POST', { primary_funding_id: pay });
      await t('ESCRITA: definir payment_account no negócio',
        `${G}/${neg}?access_token=TOKEN`, tkP1, 'POST', { payment_account_id: pay });
    }

    const ok = out.filter(x => x.ok);
    return res.status(200).json({ ok: true, contaDePagamento: pay,
      CAMINHOS_QUE_ABREM: ok.map(x => x.caminho),
      resultado: out,
      dica: 'passe &tokenAds=SEU_TOKEN_DE_ANUNCIOS para os testes marcados com [ads]' });
  }

  // ── 🔬 AUDITORIA-PAGAMENTO: varre TODOS os caminhos possíveis de faturamento ──
  if (action === 'auditoria-pagamento') {
    const tkA = String(req.query.token || '').trim() || (await credenciais()).token;
    const G = 'https://graph.facebook.com/v20.0';
    const wid = String(req.query.waba || '1050574074327587');
    const neg = String(req.query.negocio || '114657968057637');
    const conta = String(req.query.conta || '1267284360833794');
    const testes = [];
    const tenta = async (rot, metodo, url, corpo) => {
      const opt = { method: metodo };
      if (corpo) { opt.headers = { 'Content-Type': 'application/x-www-form-urlencoded' }; opt.body = new URLSearchParams(corpo).toString(); }
      const r = await fetch(url, opt).then(x => x.json()).catch(e => ({ error: { message: e.message } }));
      const ok = !(r && r.error);
      testes.push({ caminho: rot, metodo, ok,
        erro: ok ? null : (r.error.error_user_msg || r.error.message),
        codigo: ok ? null : r.error.code,
        subcodigo: ok ? null : r.error.error_subcode,
        dados: ok ? (JSON.stringify(r).length > 900 ? JSON.stringify(r).slice(0, 900) + '…' : r) : undefined });
      return ok ? r : null;
    };

    // ── A) A WABA e o que ela expõe de faturamento ──
    await tenta('WABA campos de faturamento', 'GET',
      `${G}/${wid}?fields=id,name,currency,primary_funding_id,account_review_status,health_status,owner_business_info,is_enabled_for_insights&access_token=${tkA}`);
    await tenta('WABA > payment_configuration', 'GET', `${G}/${wid}/payment_configuration?access_token=${tkA}`);
    await tenta('WABA > billing', 'GET', `${G}/${wid}/billing?access_token=${tkA}`);
    await tenta('WABA > extended_credits', 'GET', `${G}/${wid}/extended_credits?access_token=${tkA}`);
    await tenta('WABA > conversation_analytics', 'GET', `${G}/${wid}/conversation_analytics?access_token=${tkA}`);

    // ── B) O NEGÓCIO dono ──
    await tenta('negócio > extendedcredits', 'GET',
      `${G}/${neg}/extendedcredits?fields=id,legal_entity_name,credit_available,max_balance,balance,is_active,owner_business&access_token=${tkA}`);
    await tenta('negócio > business_users', 'GET', `${G}/${neg}?fields=id,name,payment_account_id,verification_status,vertical&access_token=${tkA}`);
    await tenta('negócio > owned_ad_accounts', 'GET',
      `${G}/${neg}/owned_ad_accounts?fields=id,name,account_status,balance,currency,funding_source_details&access_token=${tkA}`);
    await tenta('negócio > credit_cards', 'GET', `${G}/${neg}/creditcards?access_token=${tkA}`);

    // ── C) A CONTA DE ANÚNCIOS (às vezes o WhatsApp fatura por ela) ──
    await tenta('conta > saldo e método', 'GET',
      `${G}/act_${conta}?fields=balance,currency,amount_spent,spend_cap,funding_source,funding_source_details,account_status,disable_reason,is_prepay_account,owner&access_token=${tkA}`);
    await tenta('conta > payment_transactions', 'GET',
      `${G}/act_${conta}/transactions?fields=id,charge_type,status,billed_amount_details,time,payment_option&limit=10&access_token=${tkA}`);
    await tenta('conta > billing_transactions', 'GET', `${G}/act_${conta}/billing_transactions?limit=5&access_token=${tkA}`);
    await tenta('conta > funding_source_details', 'GET', `${G}/act_${conta}/funding_source_details?access_token=${tkA}`);
    await tenta('conta > invoices', 'GET', `${G}/act_${conta}/invoices?access_token=${tkA}`);
    await tenta('conta > ad_account_billing', 'GET', `${G}/act_${conta}/adspaymentcycle?access_token=${tkA}`);

    // ── D) TENTATIVAS DE ESCRITA (é aqui que pode estar a saída) ──
    if (String(req.query.forcar || '') === '1') {
      await tenta('WABA: definir método de pagamento', 'POST',
        `${G}/${wid}?access_token=${tkA}`, { primary_funding_id: String(req.query.funding || '') });
      await tenta('conta: alterar limite de gasto (destrava cobrança em alguns casos)', 'POST',
        `${G}/act_${conta}?access_token=${tkA}`, { spend_cap: '100000' });
      await tenta('conta: zerar limite de gasto', 'POST',
        `${G}/act_${conta}?access_token=${tkA}`, { spend_cap: '0' });
      await tenta('conta: reativar', 'POST', `${G}/act_${conta}?access_token=${tkA}`, { account_status: '1' });
      await tenta('WABA: forçar revisão', 'POST', `${G}/${wid}?access_token=${tkA}`, { account_review_status: 'PENDING' });
    }

    const funcionam = testes.filter(t => t.ok);
    const escrita = testes.filter(t => t.metodo === 'POST');
    return res.status(200).json({ ok: true,
      totalTestado: testes.length,
      LEITURAS_QUE_FUNCIONAM: funcionam.filter(t => t.metodo === 'GET').map(t => t.caminho),
      ESCRITAS_QUE_PASSARAM: escrita.filter(t => t.ok).map(t => t.caminho),
      resultado: testes,
      comoForcar: 'acrescente &forcar=1 para executar também as tentativas de ESCRITA' });
  }

  // ── 🧾 LER-COBRANCA: estado real do faturamento da conta ──
  if (action === 'ler-cobranca') {
    const altC = String(req.query.token || '').trim();
    const { token: tkD } = await credenciais();
    const tkC = altC || tkD;
    const G = 'https://graph.facebook.com/v20.0';
    const wid = String(req.query.waba || '1050574074327587');
    const neg = String(req.query.negocio || '114657968057637');
    const pega = async (rot, u) => {
      const r = await fetch(u).then(x => x.json()).catch(e => ({ error: { message: e.message } }));
      return { consulta: rot, ok: !(r && r.error),
        erro: (r && r.error) ? r.error.message : null,
        codigo: (r && r.error) ? r.error.code : null,
        dados: !(r && r.error) ? r : undefined };
    };
    const out = [];
    out.push(await pega('conta do WhatsApp',
      `${G}/${wid}?fields=id,name,currency,timezone_id,account_review_status,business_verification_status,health_status,primary_funding_id&access_token=${tkC}`));
    out.push(await pega('crédito da conta',
      `${G}/${wid}/extended_credits?fields=id,legal_entity_name,credit_available,credit_type,is_active&access_token=${tkC}`));
    out.push(await pega('negócio dono',
      `${G}/${neg}?fields=id,name,verification_status,vertical,two_factor_type,created_time&access_token=${tkC}`));
    out.push(await pega('assinatura de faturamento',
      `${G}/${wid}/subscribed_apps?access_token=${tkC}`));
    out.push(await pega('números da conta',
      `${G}/${wid}/phone_numbers?fields=id,display_phone_number,quality_rating,status,throughput&access_token=${tkC}`));
    return res.status(200).json({ ok: out.every(o => o.ok),
      consultas: out,
      leitura: 'a Meta NÃO expõe pagamento por API — estas leituras servem para achar a inconsistência e levar ao suporte' });
  }

  // ── 🔑 TOKEN-PERMISSOES: o que o token atual realmente pode fazer ──
  if (action === 'token-permissoes') {
    const altP = String(req.query.token || '').trim();
    const { token: tkPd } = await credenciais();
    const tkP = altP || tkPd;
    if (!tkP) return res.status(200).json({ ok: false, error: 'sem token' });
    const G = 'https://graph.facebook.com/v20.0';
    const dbg = await fetch(`${G}/debug_token?input_token=${tkP}&access_token=${tkP}`)
      .then(x => x.json()).catch(e => ({ error: { message: e.message } }));
    const d = (dbg && dbg.data) || {};
    const escopos = d.scopes || [];
    const precisa = ['whatsapp_business_messaging', 'whatsapp_business_management', 'business_management'];
    const faltando = precisa.filter(p => !escopos.includes(p));
    return res.status(200).json({ ok: faltando.length === 0,
      tipo: d.type, app: d.application, appId: d.app_id,
      criadoEm: d.issued_at ? new Date(d.issued_at * 1000).toISOString() : null,
      expiraEm: d.expires_at ? (d.expires_at === 0 ? 'nunca' : new Date(d.expires_at * 1000).toISOString()) : 'nunca',
      valido: d.is_valid,
      escopos,
      permissoesFaltando: faltando.length ? faltando : 'nenhuma',
      negociosGranulares: (d.granular_scopes || []).map(g => g.scope + ' → ' + (g.target_ids || []).join(', ')),
      leitura: faltando.length
        ? '⚠️ o token foi gerado SEM essas permissões. Atribuir o ativo depois não atualiza o token — é preciso GERAR UM NOVO.'
        : '✅ o token tem todas as permissões necessárias' });
  }

  // ── 🔧 FORCAR-FISCAL: tenta gravar CNPJ e moeda por vários caminhos ──
  if (action === 'forcar-fiscal') {
    const alt = String(req.query.token || '').trim();
    const { token: tkPad } = await credenciais();
    const tkF = alt || tkPad;
    const G = 'https://graph.facebook.com/v20.0';
    const wid = String(req.query.waba || '1050574074327587');
    const neg = String(req.query.negocio || '114657968057637');
    const cnpj = String(req.query.cnpj || '').replace(/\D/g, '');
    const moeda = String(req.query.moeda || 'BRL').toUpperCase();
    const tentar = async (rot, url, campos) => {
      const r = await fetch(`${G}/${url}?access_token=${tkF}`, { method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: new URLSearchParams(campos).toString() })
        .then(x => x.json()).catch(e => ({ error: { message: e.message } }));
      return { tentativa: rot, ok: !(r && r.error),
        erro: (r && r.error) ? r.error.message : null,
        codigo: (r && r.error) ? r.error.code : null,
        resposta: !(r && r.error) ? r : undefined };
    };
    const res1 = [];
    if (cnpj) {
      res1.push(await tentar('CNPJ no negócio (tax_id)', neg, { tax_id: cnpj }));
      res1.push(await tentar('CNPJ no negócio (vertical+tax_id)', neg, { vertical: 'OTHER', tax_id: cnpj }));
      res1.push(await tentar('CNPJ na WABA', wid, { tax_id: cnpj }));
    }
    res1.push(await tentar('moeda na WABA', wid, { currency: moeda }));
    res1.push(await tentar('moeda no negócio', neg, { currency: moeda }));
    const algumOk = res1.some(r => r.ok);
    return res.status(200).json({ ok: algumOk, tentativas: res1,
      conclusao: algumOk ? '✅ ao menos uma alteração passou — confira em waba-config'
        : '❌ nenhuma passou. Códigos 3 = a Meta não permite por API; 200 = falta permissão no token (gere um novo)' });
  }

  // ── 🚀 INICIAR-NUMERO-NOVO: vira a chave para o número novo com corte limpo ──
  if (req.method === 'GET' && action === 'iniciar-numero-novo') {
    const phoneId = String(req.query.phoneId || '').trim();
    const token = String(req.query.token || '').trim();
    if (!phoneId || !token) {
      return res.status(400).json({ ok: false, error: 'informe phoneId e token do número novo' });
    }
    // 1) valida antes de qualquer coisa
    const teste = await fetch(`https://graph.facebook.com/v20.0/${phoneId}?fields=display_phone_number,verified_name,quality_rating,status&access_token=${token}`)
      .then(x => x.json()).catch(e => ({ error: { message: e.message } }));
    if (!teste || teste.error) {
      return res.status(200).json({ ok: false, error: 'credenciais não funcionam — NADA foi alterado',
        detalhe: (teste && teste.error && teste.error.message) || 'sem resposta' });
    }
    if (String(req.query.aplicar || '') !== '1') {
      return res.status(200).json({ ok: true, modo: 'prévia — nada foi alterado',
        numeroNovo: { telefone: teste.display_phone_number, nome: teste.verified_name,
          qualidade: teste.quality_rating, situacao: teste.status },
        oQueVaiAcontecer: [
          '1. o bot passa a enviar e receber pelo número novo',
          '2. marco de corte gravado: fichas e orçamentos ANTERIORES a agora não serão abordados',
          '3. só ficha nova e orçamento novo entram na régua do bot',
          '4. o que ficou para trás continua no sistema, para tratamento manual',
        ],
        dica: 'para aplicar: &aplicar=1' });
    }
    const agora = new Date().toISOString();
    // 2) troca as credenciais
    const cred = (await dbGet('wa_credenciais')) || {};
    const anterior = cred.phoneId || WA_PHONE_ID;
    cred.token = token; cred.phoneId = phoneId; cred.ativo = true;
    cred.apelido = String(req.query.apelido || 'numero-novo');
    cred.trocadoEm = agora;
    cred.historico = (cred.historico || []).concat([{ em: agora, de: anterior, para: phoneId, motivo: 'início do número novo' }]).slice(-20);
    await dbSet('wa_credenciais', cred);
    // 3) MARCO DE CORTE — nada anterior é abordado
    const cfgN = (await dbGet('wa_bot_config')) || {};
    cfgN.marcoNumeroNovo = agora;
    cfgN.orcMarcoTs = agora;                 // orçamentos: só os criados a partir de agora
    cfgN.abordagemMarcoTs = agora;           // fichas: idem
    delete cfgN.bloqueioPagamentoEm;         // limpa o bloqueio do número velho
    await dbSet('wa_bot_config', cfgN);
    return res.status(200).json({ ok: true,
      ativado: { telefone: teste.display_phone_number, nome: teste.verified_name, phoneId },
      anterior,
      marcoDeCorte: agora,
      efeito: 'a partir de agora o bot só aborda ficha e envia orçamento criados APÓS este momento',
      proximo: 'teste com action=teste-template e confira com action=status-envio' });
  }

  // ── 🔀 TROCAR-NUMERO: ativa outro número do WhatsApp sem redeploy ──
  if (action === 'trocar-numero') {
    const phoneId = String(req.query.phoneId || '').trim();
    const token = String(req.query.token || '').trim();
    const apelido = String(req.query.apelido || 'reserva').slice(0, 30);

    // ?status=1 → só mostra o que está ativo agora
    if (String(req.query.status || '') === '1' || (!phoneId && !token)) {
      const c = (await dbGet('wa_credenciais')) || {};
      const atual = await credenciais();
      let numero = null;
      try {
        const r = await fetch(`https://graph.facebook.com/v20.0/${atual.phoneId}?fields=display_phone_number,verified_name,quality_rating,messaging_limit_tier&access_token=${atual.token}`)
          .then(x => x.json());
        if (r && !r.error) numero = r;
      } catch (e) {}
      return res.status(200).json({ ok: true,
        trocaAtiva: !!c.ativo,
        apelidoAtivo: c.ativo ? c.apelido : '(padrão da Vercel)',
        phoneIdEmUso: atual.phoneId,
        numero: numero ? { telefone: numero.display_phone_number, nome: numero.verified_name,
          qualidade: numero.quality_rating, limite: numero.messaging_limit_tier } : 'não consegui ler',
        historico: c.historico || [],
        comoTrocar: '?action=trocar-numero&phoneId=NOVO_ID&token=NOVO_TOKEN&apelido=nome',
        comoVoltar: '?action=trocar-numero&voltar=1' });
    }
    // ?voltar=1 → desativa a troca e volta ao número da Vercel
    if (String(req.query.voltar || '') === '1') {
      const c = (await dbGet('wa_credenciais')) || {};
      c.ativo = false;
      c.historico = (c.historico || []).concat([{ em: new Date().toISOString(), acao: 'voltou ao número padrão' }]).slice(-20);
      await dbSet('wa_credenciais', c);
      return res.status(200).json({ ok: true, msg: 'voltou ao número padrão da Vercel' });
    }
    if (!phoneId || !token) {
      return res.status(400).json({ ok: false, error: 'informe phoneId e token do número novo' });
    }
    // valida ANTES de ativar — não troca para um número que não responde
    const teste = await fetch(`https://graph.facebook.com/v20.0/${phoneId}?fields=display_phone_number,verified_name,quality_rating&access_token=${token}`)
      .then(x => x.json()).catch(e => ({ error: { message: e.message } }));
    if (!teste || teste.error) {
      return res.status(200).json({ ok: false,
        error: 'as credenciais novas não funcionam — troca NÃO realizada',
        detalhe: (teste && teste.error && teste.error.message) || 'sem resposta' });
    }
    const c = (await dbGet('wa_credenciais')) || {};
    const anterior = c.phoneId || WA_PHONE_ID;
    c.token = token; c.phoneId = phoneId; c.apelido = apelido; c.ativo = true;
    c.trocadoEm = new Date().toISOString();
    c.historico = (c.historico || []).concat([{ em: c.trocadoEm, de: anterior, para: phoneId, apelido }]).slice(-20);
    await dbSet('wa_credenciais', c);
    return res.status(200).json({ ok: true,
      ativado: { telefone: teste.display_phone_number, nome: teste.verified_name,
        qualidade: teste.quality_rating, phoneId, apelido },
      anterior,
      aviso: 'o bot já está enviando por este número — teste com action=teste-template' });
  }

  // ── 🧾 WABA-CONFIG: lê a configuração de faturamento da conta e tenta corrigir ──
  if (action === 'waba-config') {
    const { token: tkW, phoneId: pidW } = await credenciais();
    if (!tkW || !pidW) return res.status(200).json({ ok: false, error: 'credenciais ausentes' });
    const G = 'https://graph.facebook.com/v20.0';
    const pega = async (u) => fetch(u).then(x => x.json()).catch(e => ({ error: { message: e.message } }));

    // descobre a WABA dona do número — o campo direto não existe nesta versão,
    // então tenta pelos caminhos alternativos
    let wid = String(req.query.waba || '').trim() || null;
    const caminhos = [];
    if (!wid) {
      // 1) o próprio número às vezes expõe o id da conta
      const p1 = await pega(`${G}/${pidW}?fields=id,display_phone_number,verified_name,account_mode,name_status&access_token=${tkW}`);
      caminhos.push({ via: 'número', ok: !(p1 && p1.error), dados: p1 && !p1.error ? p1 : (p1.error || {}).message });
      // 2) listar as WABAs do negócio ligado ao token
      const me = await pega(`${G}/me?fields=id,name&access_token=${tkW}`);
      caminhos.push({ via: 'usuário do token', ok: !(me && me.error), dados: me && !me.error ? me : (me.error || {}).message });
      // 3) contas do WhatsApp acessíveis
      const cw = await pega(`${G}/me/assigned_whatsapp_business_accounts?fields=id,name,currency,timezone_id&access_token=${tkW}`);
      if (cw && cw.data && cw.data.length) {
        wid = cw.data[0].id;
        caminhos.push({ via: 'contas atribuídas', ok: true, encontradas: cw.data.length,
          lista: cw.data.map(w => w.id + ' | ' + w.name + ' | moeda: ' + (w.currency || 'AUSENTE') + ' | fuso: ' + (w.timezone_id || 'AUSENTE')) });
      } else {
        caminhos.push({ via: 'contas atribuídas', ok: false, dados: (cw && cw.error && cw.error.message) || 'nenhuma' });
      }
    }
    if (!wid) return res.status(200).json({ ok: false,
      error: 'não consegui identificar a WABA automaticamente',
      caminhosTentados: caminhos,
      comoResolver: 'pegue o ID em business.facebook.com → Configurações do Negócio → Contas → Contas do WhatsApp (é um número longo) e acrescente &waba=SEU_ID ao link' });

    // configuração completa
    const cfgW = await pega(`${G}/${wid}?fields=id,name,currency,timezone_id,account_review_status,business_verification_status,country,ownership_type,primary_business_location,health_status,owner_business_info&access_token=${tkW}`);

    // tentativa de alteração, se pedida
    const tentativas = [];
    const novaMoeda = String(req.query.moeda || '').toUpperCase();
    const novoFuso = String(req.query.fuso || '');
    const cnpj = String(req.query.cnpj || '').replace(/\D/g, '');
    // cada campo é tentado SEPARADAMENTE — se um falhar, os outros ainda passam
    const alterar = async (rot, alvo, campos) => {
      const body = new URLSearchParams(campos).toString();
      const r = await fetch(`${G}/${alvo}?access_token=${tkW}`, { method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body })
        .then(x => x.json()).catch(e => ({ error: { message: e.message } }));
      tentativas.push((r && r.error)
        ? { campo: rot, ok: false, erro: r.error.message, codigo: r.error.code,
            subcodigo: r.error.error_subcode, mensagemUsuario: r.error.error_user_msg || null }
        : { campo: rot, ok: true, resultado: r });
      return !(r && r.error);
    };
    if (novaMoeda) await alterar('moeda', wid, { currency: novaMoeda });
    if (novoFuso) await alterar('fuso horário', wid, { timezone_id: novoFuso });
    if (cnpj) {
      // dados fiscais ficam no NEGÓCIO, não na WABA
      const negId = ((cfgW || {}).owner_business_info || {}).id;
      if (negId) {
        await alterar('CNPJ (negócio ' + negId + ')', negId, { vertical: 'OTHER', tax_id: cnpj });
      } else {
        tentativas.push({ campo: 'CNPJ', ok: false, erro: 'não identifiquei o negócio dono da WABA' });
      }
    }
    const tentativa = tentativas.length ? tentativas : null;

    const c = cfgW || {};
    const faltando = [];
    if (!c.currency) faltando.push('MOEDA não definida');
    if (!c.timezone_id) faltando.push('FUSO HORÁRIO não definido');
    if (c.business_verification_status && c.business_verification_status !== 'verified') {
      faltando.push('negócio não verificado: ' + c.business_verification_status);
    }
    if (c.account_review_status && c.account_review_status !== 'APPROVED') {
      faltando.push('conta em revisão: ' + c.account_review_status);
    }
    return res.status(200).json({ ok: faltando.length === 0,
      waba: { id: wid, nome: c.name, pais: c.country,
        moeda: c.currency || '❌ AUSENTE',
        fusoHorario: c.timezone_id || '❌ AUSENTE',
        revisao: c.account_review_status,
        verificacaoDoNegocio: c.business_verification_status,
        tipoDePropriedade: c.ownership_type },
      negocioDono: c.owner_business_info || null,
      problemas: faltando.length ? faltando : 'configuração completa',
      tentativaDeAlteracao: tentativa,
      comoAlterar: 'acrescente &moeda=BRL, &fuso=America/Sao_Paulo e/ou &cnpj=00000000000000 ao link',
      erroDaMeta: (cfgW && cfgW.error) ? cfgW.error.message : undefined });
  }

  // ── 🔁 RECUPERACAO-7D: retoma a negociação de quem está em AGUARDANDO APROVAÇÃO ──
  // 1 disparo por dia, no máximo 7 por cliente. Objetivo: fazer a negociação chegar à F5.
  if (action === 'recuperacao-7d') {
    const cfgR7 = (await dbGet('wa_bot_config')) || {};
    if (cfgR7.recuperacao7dAtiva !== true && String(req.query.forcar || '') !== '1') {
      return res.status(200).json({ ok: true,
        msg: 'recuperação de 7 dias DESLIGADA — ligue em ?action=recuperacao7d-ligar' });
    }
    if (!dentroHorarioComercial() && String(req.query.forcar || '') !== '1') {
      return res.status(200).json({ ok: true, msg: 'fora do horário comercial' });
    }
    const { token: tk7, phoneId: pid7 } = await credenciais();
    if (!tk7 || !pid7) return res.status(200).json({ ok: false, error: 'credenciais ausentes' });

    // 🎯 CRUZAMENTO: só quem está em aguardando_aprovacao no pipe AGORA
    const [pp7, evts7, reg7] = await Promise.all([
      dbGet('reparoeletro_pipe'), lerEvts(), dbGet('wa_recuperacao_7d'),
    ]);
    const controle = reg7 || { clientes: {} };
    const hoje = new Date(Date.now() - 3 * 3600000).toISOString().slice(0, 10);
    const d8de = t => String(t || '').replace(/\D/g, '').slice(-8);

    // quem respondeu nas últimas 24h não recebe (está em conversa ativa)
    const respondeu = new Set();
    for (const e of evts7) {
      if (e.dir !== 'in') continue;
      if (Date.now() - new Date(e.ts || 0).getTime() > 24 * 3600000) continue;
      respondeu.add(d8de(e.tel));
    }

    const candidatos = [];
    const esgotados = [];
    for (const c of (((pp7 || {}).cards) || [])) {
      const fase = String(c.phaseId || c.phase || '');
      if (fase !== 'aguardando_aprovacao') continue;          // ← o cruzamento pedido
      const d8 = d8de(c.telefone);
      if (d8.length < 8) continue;
      if (respondeu.has(d8)) continue;
      const ctrl = controle.clientes[d8] || { tentativas: 0, ultimo: null };
      // 🚨 esgotou as 7 sem resposta → abre CONFLITO BOT uma vez e para de tentar
      if (ctrl.tentativas >= 7) {
        if (!ctrl.conflitoAberto) {
          try {
            // grava direto na prospecção com status conflitos_bot (mesmo caminho do cérebro)
            const pdb7 = (await dbGet('prospeccao_adm')) || { fichas: [] };
            const jaTem = (pdb7.fichas || []).some(f => f.status === 'conflitos_bot' &&
              String(f.telefone || '').replace(/\D/g, '').slice(-8) === d8);
            if (!jaTem) {
              pdb7.fichas = pdb7.fichas || [];
              pdb7.fichas.unshift({
                id: 'conf_' + Date.now() + '_' + Math.random().toString(36).slice(2, 7),
                nome: c.nomeContato || '?', telefone: c.telefone,
                equipamento: c.equipamento || c.descricao || '',
                status: 'conflitos_bot',
                motivo: '🔁 7 tentativas de recuperação sem resposta — orçamento de R$ ' +
                  (c.valor || '?') + ' parado em aguardando aprovação desde ' +
                  String(c.movedAt || c.criadoEm || '').slice(0, 10) + '. Retomar por TELEFONE.',
                origem: 'recuperacao-7d',
                cardId: c.id,
                criadoEm: new Date().toISOString(),
                movedAt: new Date().toISOString(),
              });
              await dbSet('prospeccao_adm', pdb7);
            }
            ctrl.conflitoAberto = new Date().toISOString();
            controle.clientes[d8] = ctrl;
            esgotados.push((c.nomeContato || '?') + ' ' + d8.slice(-4));
          } catch (e) {}
        }
        continue;
      }
      if (ctrl.ultimo === hoje) continue;                     // já recebeu hoje
      candidatos.push({ card: c, d8, tentativa: ctrl.tentativas + 1 });
    }
    // 🔒 UM POR TELEFONE: cliente com dois cards em aguardando aprovação (Julimar 4147,
    // purificador + micro-ondas) recebia duas mensagens na mesma leva. Mantém só um card
    // por telefone — o de maior valor, que é o que mais importa recuperar.
    const porTel = {};
    for (const x of candidatos) {
      const atual = porTel[x.d8];
      const vNovo = parseFloat(x.card.valor || 0) || 0;
      const vAtual = atual ? (parseFloat(atual.card.valor || 0) || 0) : -1;
      if (!atual || vNovo > vAtual) porTel[x.d8] = x;
    }
    candidatos.length = 0;
    for (const x of Object.values(porTel)) candidatos.push(x);
    const teto = Math.min(30, Math.max(1, parseInt(req.query.teto || cfgR7.recuperacao7dTeto || '10', 10)));
    const lote = candidatos.slice(0, teto);

    if (String(req.query.simular || '') === '1') {
      return res.status(200).json({ ok: true, modo: 'simulação — nada enviado',
        esgotaramAs7: esgotados.length,
        emAguardandoAprovacao: candidatos.length + lote.length ? candidatos.length : 0,
        elegiveisAgora: candidatos.length, seriamEnviados: lote.length, teto,
        lista: lote.map(x => (x.card.nomeContato || '?') + ' ' + x.d8.slice(-4) +
          ' | tentativa ' + x.tentativa + '/7 | ' + String(x.card.equipamento || '').slice(0, 24)) });
    }

    const enviados = [], falhas = [], mudaramDeFase = [];
    let _faseCache = null, _faseCacheEm = 0;
    const _tInicio = Date.now();
    for (const x of lote) {
      // ⏱️ trava de tempo: a função da Vercel tem limite; para antes de estourar
      if (Date.now() - _tInicio > 40000) {
        mudaramDeFase.push('⏱️ parou em ' + enviados.length + ' por limite de tempo — o resto vai no próximo ciclo');
        break;
      }
      // 🔒 RECONFERÊNCIA com cache curto: reler o pipe inteiro (689 cards) a cada envio
      // estourava o tempo da função. Agora relê no máximo a cada 15s, que é curto o
      // suficiente para pegar mudança de fase e leve o bastante para não dar timeout.
      try {
        if (!_faseCache || Date.now() - _faseCacheEm > 15000) {
          const ppAgora = await dbGet('reparoeletro_pipe');
          _faseCache = {};
          for (const k of (((ppAgora || {}).cards) || [])) {
            _faseCache[k.id] = String(k.phaseId || k.phase || '');
          }
          _faseCacheEm = Date.now();
        }
        const faseAgora = _faseCache[x.card.id];
        if (faseAgora === undefined) {
          mudaramDeFase.push((x.card.nomeContato || '?') + ': card não existe mais');
          continue;
        }
        if (faseAgora !== 'aguardando_aprovacao') {
          mudaramDeFase.push((x.card.nomeContato || '?') + ': saiu para ' + faseAgora);
          continue;
        }
        if (respondeu.has(x.d8)) {
          mudaramDeFase.push((x.card.nomeContato || '?') + ': respondeu — conversa ativa');
          continue;
        }
      } catch (e) {}
      // 🔒 trava final contra duplicidade: relê o controle e confirma que não saiu hoje
      try {
        const ctrlAgora = (await dbGet('wa_recuperacao_7d')) || { clientes: {} };
        const cAtual = ctrlAgora.clientes[x.d8];
        if (cAtual && cAtual.ultimo === hoje) {
          mudaramDeFase.push((x.card.nomeContato || '?') + ': já recebeu hoje');
          continue;
        }
        controle.clientes = Object.assign({}, ctrlAgora.clientes, controle.clientes);
      } catch (e) {}
      const nome = String(x.card.nomeContato || '').trim().split(/\s+/)[0] || 'tudo bem';
      const tel = String(x.card.telefone || '').replace(/\D/g, '');
      const to = tel.startsWith('55') ? tel : '55' + tel;
      // usa o template de orçamento pronto — a conversa segue pelo cérebro nas 5 fases
      const r = await fetch(`https://graph.facebook.com/v20.0/${pid7}/messages`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${tk7}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ messaging_product: 'whatsapp', to, type: 'template',
          template: { name: 'orcamento_pronto', language: { code: 'pt_BR' },
            components: [{ type: 'body', parameters: [
              { type: 'text', text: nome },
              { type: 'text', text: String(x.card.equipamento || 'equipamento').slice(0, 40) },
            ] }] } }),
      }).then(z => z.json()).catch(e => ({ error: { message: e.message } }));

      const ok = !!(r && r.messages && r.messages[0]);
      if (ok) {
        controle.clientes[x.d8] = { tentativas: x.tentativa, ultimo: hoje,
          nome: x.card.nomeContato, cardId: x.card.id };
        // 💾 GRAVA IMEDIATAMENTE: salvar só no fim fazia o registro se perder quando a
        // função dava timeout, e a execução seguinte reenviava para todo mundo.
        await dbSet('wa_recuperacao_7d', controle);
        enviados.push((x.card.nomeContato || '?') + ' (tentativa ' + x.tentativa + '/7)');
        await indexarEnvio(r.messages[0].id, 'orcamento_pronto', 'recuperacao-7d', nome, to);
        await rpushEvt({ ts: new Date().toISOString(), tel: to, dir: 'out',
          texto: '🔁 [orcamento_pronto] recuperação ' + x.tentativa + '/7 — ' + nome,
          tipo: 'template', via: 'recuperacao-7d', msgId: r.messages[0].id });
      } else {
        falhas.push((x.card.nomeContato || '?') + ': ' + ((r.error && (r.error.error_user_msg || r.error.message)) || 'falha'));
      }
      await new Promise(s => setTimeout(s, 250));             // ritmo suave, sem estourar o tempo
    }
    await dbSet('wa_recuperacao_7d', controle);
    return res.status(200).json({ ok: falhas.length === 0,
      elegiveis: candidatos.length, enviados: enviados.length,
      lista: enviados, falhas,
      esgotaramAs7: esgotados.length ? { total: esgotados.length, lista: esgotados,
        acao: 'conflito aberto para retomada por telefone' } : 'nenhum',
      naoEnviadosPorMudanca: mudaramDeFase.length ? mudaramDeFase : 'nenhum' });
  }

  // ── 🔘 RECUPERACAO7D-LIGAR / DESLIGAR ──
  if (action === 'recuperacao7d-ligar' || action === 'recuperacao7d-desligar') {
    const c = (await dbGet('wa_bot_config')) || {};
    c.recuperacao7dAtiva = action === 'recuperacao7d-ligar';
    if (req.query.teto) c.recuperacao7dTeto = Math.min(30, Math.max(1, parseInt(req.query.teto, 10)));
    await dbSet('wa_bot_config', c);
    return res.status(200).json({ ok: true,
      recuperacao7d: c.recuperacao7dAtiva ? 'LIGADA' : 'DESLIGADA',
      teto: c.recuperacao7dTeto || 10 });
  }

  // ── 📒 LOG-ORCAMENTOS: enviados, aprovados e a janela de recuperação, com data real ──
  if (action === 'log-orcamentos') {
    const dia = String(req.query.dia || '').trim();                  // AAAA-MM-DD
    const dias = Math.min(90, Math.max(1, parseInt(req.query.dias || '7', 10)));
    const desde = dia ? new Date(dia + 'T00:00:00-03:00').getTime()
      : Date.now() - dias * 86400000;
    const ate = dia ? desde + 86400000 : Date.now();
    const dBR = t => t ? new Date(new Date(t).getTime() - 3 * 3600000).toISOString().slice(0, 10) : null;
    const d8f = t => String(t || '').replace(/\D/g, '').slice(-8);

    const [pp, arq, evts, ctrl7] = await Promise.all([
      dbGet('reparoeletro_pipe'), dbGet('reparoeletro_arquivo'),
      lerEvts(), dbGet('wa_recuperacao_7d'),
    ]);
    const universo = (((pp || {}).cards) || []).concat(((arq || {}).cards) || []);

    // 1) ORÇAMENTOS ENVIADOS — pelos eventos de saída com template de orçamento
    const enviados = [];
    for (const e of (evts || [])) {
      if (e.dir !== 'out') continue;
      const t = new Date(e.ts || 0).getTime();
      if (!t || t < desde || t >= ate) continue;
      const txt = String(e.texto || '');
      if (!/orcamento_pronto|orçamento/i.test(txt)) continue;
      enviados.push({ quando: e.ts, tel: d8f(e.tel), via: e.via || 'bot', texto: txt.slice(0, 60) });
    }

    // 2) APROVADOS — pela data REAL de aprovação, não pela fase atual
    const aprovados = [];
    for (const c of universo) {
      const q = c.aprovadoEm || null;
      if (!q) continue;
      const t = new Date(q).getTime();
      if (!t || t < desde || t >= ate) continue;
      aprovados.push({ quando: q, nome: c.nomeContato, tel: d8f(c.telefone),
        equipamento: String(c.equipamento || c.descricao || '').slice(0, 26),
        valor: parseFloat(c.valorNaAprovacao != null ? c.valorNaAprovacao : (c.valor || 0)) || 0,
        por: c.aprovadoPor || null, faseAgora: c.phaseId || c.phase });
    }
    aprovados.sort((a, b) => String(a.quando).localeCompare(String(b.quando)));

    // 3) JANELA DE 7 DIAS — quem está no ciclo de recuperação
    const clientes = ((ctrl7 || {}).clientes) || {};
    const naJanela = [];
    for (const [d8, v] of Object.entries(clientes)) {
      const card = universo.find(c => d8f(c.telefone) === d8);
      const faseAgora = card ? String(card.phaseId || card.phase || '') : '(sem card)';
      naJanela.push({ tel: d8, nome: v.nome || (card && card.nomeContato) || '?',
        tentativas: v.tentativas, ultimo: v.ultimo,
        conflitoAberto: v.conflitoAberto ? dBR(v.conflitoAberto) : null,
        faseAgora,
        saiuDaFila: faseAgora !== 'aguardando_aprovacao',
        valor: card ? (parseFloat(card.valor || 0) || 0) : 0 });
    }
    naJanela.sort((a, b) => b.tentativas - a.tentativas);

    const somaAprov = Number(aprovados.reduce((s, a) => s + a.valor, 0).toFixed(2));
    const converteram = naJanela.filter(x => x.faseAgora === 'aprovados' || x.faseAgora === 'producao');
    return res.status(200).json({ ok: true,
      periodo: dia ? ('dia ' + dia) : ('últimos ' + dias + ' dias'),
      RESUMO: {
        orcamentosEnviados: enviados.length,
        aprovadosNoPeriodo: aprovados.length,
        valorAprovado: somaAprov,
        taxaAprovacao: enviados.length ? Math.round(aprovados.length / enviados.length * 100) + '%' : null,
        naJanelaDe7Dias: naJanela.length,
        jaSairamDaFila: naJanela.filter(x => x.saiuDaFila).length,
        converteramAposRecuperacao: converteram.length,
        esgotaramAs7: naJanela.filter(x => x.tentativas >= 7).length,
      },
      APROVADOS: aprovados.map(a => dBR(a.quando) + ' ' + String(a.quando).slice(11, 16) +
        ' | ' + String(a.nome || '?').slice(0, 20) + ' ' + a.tel.slice(-4) +
        ' | R$ ' + a.valor + ' | ' + a.equipamento + (a.por ? ' | ' + a.por : '')),
      JANELA_7D: naJanela.map(x => String(x.nome).slice(0, 20) + ' ' + x.tel.slice(-4) +
        ' | ' + x.tentativas + '/7' + (x.ultimo ? ' · último ' + x.ultimo : '') +
        ' | ' + x.faseAgora + (x.saiuDaFila ? ' ✅ saiu' : '') +
        (x.conflitoAberto ? ' | 🚨 conflito ' + x.conflitoAberto : '')),
      ENVIADOS: enviados.slice(0, 60).map(e => dBR(e.quando) + ' ' + String(e.quando).slice(11, 16) +
        ' | ' + e.tel.slice(-4) + ' | ' + e.via) });
  }

  // ── 💵 IA-CONSUMO: quanto a IA custou por dia e onde o token está indo ──
  if (action === 'ia-consumo') {
    const dias = Math.min(60, Math.max(1, parseInt(req.query.dias || '7', 10)));
    const P_IN = 3.0, P_OUT = 15.0, P_CACHE_W = 3.75, P_CACHE_R = 0.30;
    const linhas = [];
    let tot = { chamadas: 0, entrada: 0, saida: 0, cacheCriado: 0, cacheLido: 0, custo: 0 };
    for (let i = 0; i < dias; i++) {
      const d = new Date(Date.now() - 3 * 3600000 - i * 86400000).toISOString().slice(0, 10);
      const r = await dbGet('ia_uso_' + d);
      if (!r) continue;
      const custo = (r.entrada / 1e6) * P_IN + (r.saida / 1e6) * P_OUT
        + (r.cacheCriado / 1e6) * P_CACHE_W + (r.cacheLido / 1e6) * P_CACHE_R;
      tot.chamadas += r.chamadas; tot.entrada += r.entrada; tot.saida += r.saida;
      tot.cacheCriado += r.cacheCriado; tot.cacheLido += r.cacheLido; tot.custo += custo;
      linhas.push({ dia: d, chamadas: r.chamadas,
        entrada: r.entrada, saida: r.saida,
        cacheCriado: r.cacheCriado, cacheLido: r.cacheLido,
        aproveitamentoCache: (r.cacheCriado + r.cacheLido) > 0
          ? Math.round(r.cacheLido / (r.cacheCriado + r.cacheLido) * 100) + '%' : '0%',
        tokensPorChamada: r.chamadas ? Math.round((r.entrada + r.cacheCriado + r.cacheLido) / r.chamadas) : 0,
        custoUSD: Number(custo.toFixed(2)),
        msMedio: r.chamadas ? Math.round(r.ms / r.chamadas) : 0 });
    }
    return res.status(200).json({ ok: true, periodoDias: dias,
      TOTAIS: { chamadas: tot.chamadas,
        tokensEntrada: tot.entrada, tokensSaida: tot.saida,
        cacheCriado: tot.cacheCriado, cacheLido: tot.cacheLido,
        aproveitamentoCache: (tot.cacheCriado + tot.cacheLido) > 0
          ? Math.round(tot.cacheLido / (tot.cacheCriado + tot.cacheLido) * 100) + '%' : '0%',
        custoTotalUSD: Number(tot.custo.toFixed(2)),
        custoPorChamadaUSD: tot.chamadas ? Number((tot.custo / tot.chamadas).toFixed(4)) : 0 },
      POR_ORIGEM: await (async function () {
        const agg = {};
        for (let i = 0; i < dias; i++) {
          const d = new Date(Date.now() - 3 * 3600000 - i * 86400000).toISOString().slice(0, 10);
          const r = await dbGet('ia_uso_' + d);
          for (const [o, v] of Object.entries((r || {}).porOrigem || {})) {
            const a = agg[o] || { n: 0, ent: 0, sai: 0, cw: 0, cr: 0 };
            a.n += v.n; a.ent += v.ent; a.sai += v.sai; a.cw += v.cw; a.cr += v.cr;
            agg[o] = a;
          }
        }
        return Object.entries(agg).map(([o, v]) => {
          const c = (v.ent / 1e6) * P_IN + (v.sai / 1e6) * P_OUT
            + (v.cw / 1e6) * P_CACHE_W + (v.cr / 1e6) * P_CACHE_R;
          return { origem: o, chamadas: v.n, custoUSD: Number(c.toFixed(3)),
            tokensPorChamada: v.n ? Math.round((v.ent + v.cw + v.cr) / v.n) : 0 };
        }).sort((a, b) => b.custoUSD - a.custoUSD);
      })(),
      porDia: linhas,
      observacao: 'o registro começa agora — dias anteriores não têm dados. O bot aparece como origem implícita; as demais chamadas do sistema vêm nomeadas.' });
  }

  // ── 💳 STATUS-COBRANCA: a conta do WhatsApp está liberada para enviar? ──
  if (action === 'status-cobranca') {
    const { token: tkC, phoneId: pidC } = await credenciais();
    if (!tkC || !pidC) return res.status(200).json({ ok: false, error: 'credenciais ausentes' });
    const G = 'https://graph.facebook.com/v20.0';
    const pega = async (u) => fetch(u).then(x => x.json()).catch(e => ({ error: { message: e.message } }));

    // o número e a saúde dele (é aqui que a Meta reporta bloqueio por pagamento)
    const num = await pega(`${G}/${pidC}?fields=display_phone_number,quality_rating,status,messaging_limit_tier,throughput,health_status&access_token=${tkC}`);
    // a conta de negócios e o estado de cobrança
    let waba = null, cobranca = null;
    try {
      const d = await pega(`${G}/${pidC}?fields=whatsapp_business_account{id,name,account_review_status,health_status}&access_token=${tkC}`);
      waba = (d || {}).whatsapp_business_account || null;
      if (waba && waba.id) {
        cobranca = await pega(`${G}/${waba.id}?fields=account_review_status,health_status,business_verification_status,primary_funding_id&access_token=${tkC}`);
      }
    } catch (e) {}

    // saúde: a Meta descreve aqui restrições ativas, inclusive de pagamento
    const saude = (num && num.health_status) || (waba && waba.health_status) || null;
    const entidades = (saude && saude.entities) || [];
    const bloqueios = entidades.filter(e => e.can_send_message && e.can_send_message !== 'AVAILABLE')
      .map(e => ({ tipo: e.entity_type, situacao: e.can_send_message,
        motivos: (e.errors || []).map(x => x.error_description || x.error_code) }));

    const podeEnviar = num.status === 'CONNECTED' && num.quality_rating !== 'RED' && bloqueios.length === 0;
    return res.status(200).json({ ok: true,
      numero: { telefone: num.display_phone_number, situacao: num.status,
        qualidade: num.quality_rating, limite: num.messaging_limit_tier },
      contaDeNegocios: waba ? { nome: waba.name, revisao: waba.account_review_status } : null,
      verificacao: cobranca ? cobranca.business_verification_status : null,
      saudeGeral: saude ? (saude.can_send_message || 'sem dados') : 'não informado',
      bloqueiosAtivos: bloqueios.length ? bloqueios : 'nenhum',
      PODE_ENVIAR_TEMPLATE: podeEnviar,
      veredito: podeEnviar
        ? '✅ liberado — pode retomar os disparos'
        : '❌ ainda bloqueado: ' + (bloqueios.length ? bloqueios.map(b => b.motivos.join(', ')).join(' | ') : num.status),
      observacao: 'a dívida em si aparece no Gerenciador → Configurações de pagamento; aqui vemos se a Meta já liberou o envio' });
  }

  // ── 📋 STATUS-TEMPLATES: situação dos modelos e do número, para saber se dá para enviar ──
  if (action === 'status-templates') {
    const { token: tkT, phoneId: pidT } = await credenciais();
    if (!tkT || !pidT) return res.status(200).json({ ok: false, error: 'credenciais ausentes' });
    const G = 'https://graph.facebook.com/v20.0';
    const pega = async (url) => fetch(url).then(x => x.json()).catch(e => ({ error: { message: e.message } }));

    // 1) o número: qualidade e limite de envio
    const num = await pega(`${G}/${pidT}?fields=display_phone_number,verified_name,quality_rating,name_status,messaging_limit_tier,status,platform_type&access_token=${tkT}`);
    // 2) a conta de negócios dona do número (é onde aparece restrição de pagamento)
    let waba = null, templates = null;
    try {
      const dono = await pega(`${G}/${pidT}?fields=whatsapp_business_account{id,name,account_review_status,business_verification_status,message_template_namespace}&access_token=${tkT}`);
      const w = (dono || {}).whatsapp_business_account;
      if (w && w.id) {
        waba = w;
        const tpl = await pega(`${G}/${w.id}/message_templates?fields=name,status,category,language,quality_score,rejected_reason&limit=60&access_token=${tkT}`);
        templates = (tpl && tpl.data) || null;
      }
    } catch (e) {}

    const QUALIDADE = { GREEN: '🟢 ALTA', YELLOW: '🟡 MÉDIA', RED: '🔴 BAIXA', UNKNOWN: '⚪ sem dados' };
    const LIMITE = { TIER_50: '50/dia', TIER_250: '250/dia', TIER_1K: '1.000/dia',
      TIER_10K: '10.000/dia', TIER_100K: '100.000/dia', TIER_UNLIMITED: 'ilimitado' };
    const tpls = (templates || []).map(t => ({
      nome: t.name, situacao: t.status, categoria: t.category, idioma: t.language,
      qualidade: t.quality_score ? t.quality_score.score : null,
      motivoRecusa: t.rejected_reason && t.rejected_reason !== 'NONE' ? t.rejected_reason : null,
    }));
    const aprovados = tpls.filter(t => t.situacao === 'APPROVED');
    const usados = ['boas_vindas_reparo', 'conserto_finalizado', 'cadastro_recebido_tv'];
    const faltando = usados.filter(u => !aprovados.some(a => a.nome === u));

    const alertas = [];
    if (num.quality_rating === 'RED') alertas.push('🔴 qualidade BAIXA — a Meta pode limitar ou bloquear envios');
    if (num.quality_rating === 'YELLOW') alertas.push('🟡 qualidade MÉDIA — reduza o volume por alguns dias');
    if (num.status && num.status !== 'CONNECTED') alertas.push('⚠️ número com status ' + num.status);
    if (waba && waba.account_review_status && waba.account_review_status !== 'APPROVED') {
      alertas.push('⚠️ conta de negócios em análise: ' + waba.account_review_status);
    }
    // só acusa template faltando se a leitura da lista funcionou — sem permissão
    // a lista vem vazia e isso NÃO significa que os templates não existam
    const leuTemplates = Array.isArray(templates);
    if (leuTemplates && faltando.length) {
      alertas.push('❌ template(s) que o sistema usa e NÃO estão aprovados: ' + faltando.join(', '));
    }
    if (!leuTemplates) {
      alertas.push('ℹ️ não consegui ler a lista de templates (falta a permissão whatsapp_business_management no token) — isso NÃO impede o envio');
    }
    const recusados = tpls.filter(t => t.situacao === 'REJECTED');
    if (recusados.length) alertas.push('❌ ' + recusados.length + ' template(s) RECUSADO(S)');

    return res.status(200).json({
      ok: alertas.length === 0,
      numero: { telefone: num.display_phone_number, nome: num.verified_name,
        qualidade: QUALIDADE[num.quality_rating] || num.quality_rating,
        limiteDeEnvio: LIMITE[num.messaging_limit_tier] || num.messaging_limit_tier,
        statusDoNome: num.name_status, situacao: num.status },
      contaDeNegocios: waba ? { nome: waba.name, revisao: waba.account_review_status,
        verificacao: waba.business_verification_status } : 'não consegui ler',
      templates: { total: tpls.length, aprovados: aprovados.length,
        recusados: recusados.length,
        pendentes: tpls.filter(t => t.situacao === 'PENDING').length,
        lista: tpls.map(t => t.nome + ' | ' + t.situacao + (t.qualidade ? ' | ' + t.qualidade : '') +
          (t.motivoRecusa ? ' | ' + t.motivoRecusa : '')) },
      alertas: alertas.length ? alertas : ['✅ tudo liberado para enviar'],
      // o que decide o envio é o NÚMERO, não a leitura da lista de templates
      podeEnviarTemplate: num.quality_rating !== 'RED' && (!num.status || num.status === 'CONNECTED'),
      leituraDeTemplates: Array.isArray(templates) ? 'ok' : 'sem permissão (não afeta o envio)',
    });
  }

  // ── 🎤 AUDIO-TESTE: verifica se a transcrição está configurada e funcionando ──
  if (action === 'audio-teste') {
    const temGroq = !!process.env.GROQ_API_KEY, temOpenAi = !!process.env.OPENAI_API_KEY;
    const passos = [{ etapa: 'chave configurada na Vercel',
      ok: temGroq || temOpenAi,
      detalhe: temGroq ? 'GROQ_API_KEY presente (whisper-large-v3)'
        : (temOpenAi ? 'OPENAI_API_KEY presente (whisper-1)' : '❌ NENHUMA — é isso que falta') }];
    // testa a chave contra o serviço
    if (temGroq || temOpenAi) {
      const url = temGroq ? 'https://api.groq.com/openai/v1/models' : 'https://api.openai.com/v1/models';
      const key = temGroq ? process.env.GROQ_API_KEY : process.env.OPENAI_API_KEY;
      const r = await fetch(url, { headers: { Authorization: 'Bearer ' + key } })
        .then(x => x.json()).catch(e => ({ error: { message: e.message } }));
      passos.push({ etapa: 'chave válida no serviço', ok: !(r && r.error),
        detalhe: (r && r.error) ? (r.error.message || JSON.stringify(r.error).slice(0, 120)) : 'aceita' });
    }
    // últimos áudios recebidos e se foram transcritos
    const evtsA = await lerEvts();
    const audios = evtsA.filter(e => e.tipo === 'audio' && e.dir === 'in').slice(-10);
    const transcritos = evtsA.filter(e => e.tipo === 'audio-transcrito').slice(-10);
    const falhas = evtsA.filter(e => e.tipo === 'falha' && /ÁUDIO|TRANSCRIÇÃO/i.test(String(e.texto || ''))).slice(-5);
    return res.status(200).json({ ok: passos.every(p => p.ok),
      passos,
      audiosRecebidos: audios.length,
      audiosTranscritos: transcritos.length,
      ultimasFalhas: falhas.map(f => String(f.texto).slice(0, 100)),
      comoResolver: (temGroq || temOpenAi) ? undefined
        : 'Vercel → Settings → Environment Variables → adicionar GROQ_API_KEY (console.groq.com) ou OPENAI_API_KEY, e fazer redeploy' });
  }

  // ── 📨 TEMPLATE-CONSERTO: avisa que o equipamento está pronto (janela fechada) ──
  if (req.method === 'POST' && action === 'template-conserto') {
    const { tel, nome } = req.body || {};
    if (!tel) return res.status(400).json({ ok: false, error: 'tel obrigatório' });
    const { token: tkT, phoneId: pidT } = await credenciais();
    if (!tkT || !pidT) return res.status(200).json({ ok: false, error: 'credenciais ausentes' });
    const to = String(tel).replace(/\D/g, '');
    const to2 = to.startsWith('55') ? to : '55' + to;
    const r = await fetch(`https://graph.facebook.com/v20.0/${pidT}/messages`, {
      method: 'POST', headers: { Authorization: `Bearer ${tkT}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ messaging_product: 'whatsapp', to: to2, type: 'template',
        template: { name: 'conserto_finalizado', language: { code: 'pt_BR' },
          components: [{ type: 'body', parameters: [{ type: 'text', text: String(nome || 'tudo bem').split(' ')[0] }] }] } }),
    }).then(x => x.json()).catch(e => ({ error: { message: e.message } }));
    const ok = !!(r && r.messages && r.messages[0]);
    if (ok) {
      await indexarEnvio(r.messages[0].id, 'conserto_finalizado', 'frenteloja-avisado', nome || '', to2);
      await rpushEvt({ ts: new Date().toISOString(), tel: to2, dir: 'out',
        texto: '📨 [conserto_finalizado] ' + (nome || ''), tipo: 'template', via: 'frenteloja-avisado' });
    }
    return res.status(200).json({ ok, erro: ok ? undefined : ((r && r.error && r.error.message) || 'falha') });
  }

  // ── 📄 APROVACOES-DETALHE: relatório das fichas aprovadas no período ──
  if (action === 'aprovacoes-detalhe') {
    const dias = Math.min(365, Math.max(1, parseInt(req.query.dias || '1', 10)));
    const corte = Date.now() - dias * 86400000;
    const [ppA, ppT, evts, fin] = await Promise.all([
      dbGet('reparoeletro_pipe'), dbGet('tv_pipe'), lerEvts(), dbGet('reparoeletro_financeiro'),
    ]);
    const d8f = t => String(t || '').replace(/\D/g, '').slice(-8);
    // fases que provam que passou pela aprovação (para saber QUEM aprovou)
    const APROV = ['aprovados', 'video_enviado', 'analise_compra', 'programar_entrega',
      'receber', 'erp', 'finalizado', 'entrega_agendada', 'entrega_liberada'];
    // última fala do cliente e do bot antes/depois da aprovação
    const porTel = {};
    for (const e of evts) {
      const d = d8f(e.tel); if (d.length < 8) continue;
      (porTel[d] = porTel[d] || []).push(e);
    }
    const lista = [];
    for (const [banco, sis] of [[ppA, 'ADM'], [ppT, 'TV']]) {
      for (const c of (((banco || {}).cards) || [])) {
        const fase = c.phaseId || c.phase || '';
        if (!APROV.includes(fase)) continue;
        // 🔖 lê o CARIMBO gravado no momento da aprovação. Cards antigos (antes do carimbo)
        // usam o histórico de fases como aproximação; nunca o último movimento.
        let quando = c.aprovadoEm;
        let dataConfiavel = !!quando;
        if (!quando) {
          const hist = (c.history || []).find(x => String(x.phaseId || x.phase || '') === 'aprovados');
          quando = hist && (hist.ts || hist.timestamp);
          dataConfiavel = false;
        }
        if (!quando) continue;                       // sem carimbo e sem histórico → fora
        const t = new Date(quando).getTime();
        if (!t || t < corte) continue;
        const d = d8f(c.telefone);
        const msgs = porTel[d] || [];
        // o que o cliente disse por último antes de aprovar
        const antes = msgs.filter(m => m.dir === 'in' && new Date(m.ts || 0).getTime() <= t);
        const falaCliente = antes.length ? String(antes[antes.length - 1].texto || '').slice(0, 110) : '';
        // combinado registrado pelo bot (ação com motivo)
        const acao = msgs.filter(m => m.dir === 'acao' && /aprovado|mover_aprovado/i.test(String(m.texto || '')))
          .slice(-1)[0];
        lista.push({
          sistema: sis, nome: c.nomeContato || '—', telefone: c.telefone || '',
          equipamento: c.equipamento || c.descricao || '',
          valor: (c.valorNaAprovacao != null ? c.valorNaAprovacao : (parseFloat(c.valor || 0) || null)),
          valorAtual: parseFloat(c.valor || 0) || null,
          aprovadoPor: c.aprovadoPor || null,
          origem: (function () {
            const p = String(c.aprovadoPor || '').toLowerCase();
            if (p === 'bot' || c.valorCombinadoBot) return 'bot';
            if (!c.aprovadoEm) return 'indefinido';        // card antigo, sem carimbo
            return 'equipe';
          })(),
          quando, dataConfiavel, faseAtual: fase,
          valorCombinadoPeloBot: !!c.valorCombinadoBot,
          falaDoCliente: falaCliente,
          registroDoBot: acao ? String(acao.texto || '').slice(0, 120) : '',
        });
      }
    }
    lista.sort((a, b) => String(b.quando).localeCompare(String(a.quando)));
    const total = lista.reduce((s, x) => s + (x.valor || 0), 0);
    return res.status(200).json({ ok: true, periodoDias: dias,
      total: lista.length, valorTotal: Number(total.toFixed(2)),
      porSistema: lista.reduce((o, x) => { o[x.sistema] = (o[x.sistema] || 0) + 1; return o; }, {}),
      porOrigem: lista.reduce((o, x) => { o[x.origem] = (o[x.origem] || 0) + 1; return o; }, {}),
      aprovacoes: lista.slice(0, 200) });
  }

  // ── 📋 QUEM-RECEBEU-RETROATIVA: lista e conta as mensagens do disparo retroativo ──
  if (action === 'quem-recebeu-retroativa') {
    const evtsR = await lerEvts();
    const hoje = new Date(Date.now() - 3 * 3600 * 1000).toISOString().slice(0, 10);
    const msgs = evtsR.filter(e => e.tipo === 'aprovacao-retroativa' && String(e.ts || '').slice(0, 10) === hoje);
    const porTel = {};
    for (const m of msgs) {
      const d = String(m.tel || '').replace(/\D/g, '').slice(-8);
      if (!porTel[d]) porTel[d] = { tel: m.tel, vezes: 0, horarios: [] };
      porTel[d].vezes++;
      porTel[d].horarios.push(String(m.ts).slice(11, 16));
    }
    const lista = Object.keys(porTel).map(d => d + ' | ' + porTel[d].vezes + 'x | ' + porTel[d].horarios.join(', '));
    return res.status(200).json({ ok: true,
      totalMensagens: msgs.length,
      clientesAtingidos: Object.keys(porTel).length,
      duplicados: Object.keys(porTel).filter(d => porTel[d].vezes > 1).length,
      lista });
  }

  // ── 🔢 CONFERE-APROVACOES: o KPI bate com as aprovações reais do dia? ──
  if (action === 'confere-aprovacoes') {
    const hoje = new Date(Date.now() - 3 * 3600 * 1000).toISOString().slice(0, 10);
    const [st, evtsC, ppA, ppT] = await Promise.all([
      dbGet('wa_bot_stats'), lerEvts(), dbGet('reparoeletro_pipe'), dbGet('tv_pipe'),
    ]);
    const kpi = (((st || {})[hoje]) || {}).aprovacoes || 0;
    // ações de aprovação registradas na conversa hoje
    const acoes = evtsC.filter(e => e.dir === 'acao' && e.texto === 'mover_aprovado' &&
      String(e.ts || '').slice(0, 10) === hoje);
    // mensagens da aprovação retroativa que acabamos de disparar
    const retro = evtsC.filter(e => e.tipo === 'aprovacao-retroativa' && String(e.ts || '').slice(0, 10) === hoje);
    // cards que entraram em aprovados hoje
    const entraram = [];
    for (const [b, s] of [[ppA, 'adm'], [ppT, 'tv']]) {
      for (const c of (((b || {}).cards) || [])) {
        const fase = c.phaseId || c.phase;
        if (fase !== 'aprovados') continue;
        if (String(c.movedAt || '').slice(0, 10) !== hoje) continue;
        entraram.push(s.toUpperCase() + ' ' + (c.nomeContato || '') + ' R$ ' + (c.valor || '?'));
      }
    }
    return res.status(200).json({ ok: true, dia: hoje,
      kpiDoPainel: kpi,
      acoesDoBotNaConversa: acoes.length,
      aprovacoesRetroativas: retro.length,
      cardsQueEntraramEmAprovadosHoje: entraram.length,
      soma: acoes.length + retro.length,
      veredito: kpi === acoes.length + retro.length
        ? '✅ KPI bate: ' + acoes.length + ' do bot na conversa + ' + retro.length + ' retroativas'
        : '⚠️ diferença de ' + (kpi - (acoes.length + retro.length)) + ' — contagem duplicada ou aprovação sem registro',
      listaCards: entraram });
  }

  // ── ✅ APROVAR-CLIENTE: aprova pelo telefone, achando o card em qualquer sistema ──
  if (action === 'aprovar-cliente') {
    const tel = String(req.query.tel || '').replace(/\D/g, '');
    if (tel.length < 4) return res.status(400).json({ ok: false, error: 'informe ?tel= com 4+ dígitos finais' });
    const KAC = (process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim();
    const [ppA, ppT, tvLog] = await Promise.all([
      dbGet('reparoeletro_pipe'), dbGet('tv_pipe'), dbGet('tv_logistica'),
    ]);
    const bate = t => String(t || '').replace(/\D/g, '').endsWith(tel);
    const faseDe = c => c.phaseId || c.phase || '';
    const NEGOC = ['aguardando_aprovacao', 'ultima_chamada'];
    const candidatos = [];
    for (const c of (((ppA || {}).cards) || [])) if (bate(c.telefone))
      candidatos.push({ tipo: 'card', api: 'pipe', id: c.id, nome: c.nomeContato, equip: c.equipamento, fase: faseDe(c), valor: c.valor });
    for (const c of (((ppT || {}).cards) || [])) if (bate(c.telefone))
      candidatos.push({ tipo: 'card', api: 'tv-pipe', id: c.id, nome: c.nomeContato, equip: c.equipamento, fase: faseDe(c), valor: c.valor });
    for (const f of (((tvLog || {}).fichas) || [])) if (bate(f.telefone))
      candidatos.push({ tipo: 'ficha-tv', api: 'tv-logistica', id: f.id, nome: f.nome, equip: f.equipamento, fase: f.phase });
    if (!candidatos.length) return res.status(404).json({ ok: false, error: 'nada encontrado para este telefone' });
    if (String(req.query.aplicar || '') !== '1') {
      return res.status(200).json({ ok: true, modo: 'prévia',
        encontrados: candidatos.map(c => c.tipo + ' | ' + c.api + ' | ' + (c.nome || '') + ' | ' + (c.equip || '') + ' | fase: ' + c.fase),
        dica: 'para aprovar: &aplicar=1' });
    }
    // usa a MESMA garantia do fluxo ao vivo (move, confere, força board e almoxarifado)
    const garM = await garantirAprovacao(String(tel).slice(-8));
    return res.status(200).json({ ok: garM.ok, sistema: garM.sistema, passos: garM.passos });
    /* caminho antigo desativado
    const feitos = [], erros = [];
    const fichaTv = candidatos.find(c => c.tipo === 'ficha-tv' && ['orc_enviado', 'orc_registrado'].includes(c.fase));
    if (fichaTv) {
      const r = await fetch(`https://reparoeletroadm.com/api/tv-logistica?action=aprovar-orcamento&k=${KAC}`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ id: fichaTv.id }),
      }).then(x => x.json()).catch(e => ({ error: e.message }));
      (r && !r.error ? feitos : erros).push('TV via logística: ' + (fichaTv.nome || '') + (r && r.error ? ' — ' + r.error : ''));
    }
    for (const c of candidatos.filter(x => x.tipo === 'card' && NEGOC.includes(x.fase))) {
      const r = await fetch(`https://reparoeletroadm.com/api/${c.api}?action=mover&k=${KAC}`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ id: c.id, phase: 'aprovados' }),
      }).then(x => x.json()).catch(e => ({ error: e.message }));
      (r && !r.error ? feitos : erros).push(c.api + ': ' + (c.nome || '') + ' → aprovados' + (r && r.error ? ' — ' + r.error : ''));
    }
    if (!feitos.length && !erros.length) {
      return res.status(200).json({ ok: false, error: 'nenhum card em negociação para aprovar',
        situacaoAtual: candidatos.map(c => c.api + ': ' + c.fase) });
    }
    return res.status(200).json({ ok: erros.length === 0, feitos, erros });
    */
  }

  // ── ▶️ APROVAR-PENDENTES: dispara o gatilho de quem já aprovou e avisa o cliente ──
  if (action === 'aprovar-pendentes') {
    const KTF = (process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim();
    const so = String(req.query.ids || '').split(',').map(s => s.trim()).filter(Boolean);
    const aplicar = String(req.query.aplicar || '') === '1';
    // reaproveita a varredura
    const [evtsP, ppA2, ppT2, tvLogP] = await Promise.all([
      lerEvts(), dbGet('reparoeletro_pipe'), dbGet('tv_pipe'), dbGet('tv_logistica'),
    ]);
    const d8f = t => String(t || '').replace(/\D/g, '').slice(-8);
    const parados = {};
    for (const [banco, sis] of [[ppA2, 'adm'], [ppT2, 'tv']]) {
      for (const c of (((banco || {}).cards) || [])) {
        if (!['aguardando_aprovacao', 'ultima_chamada'].includes(c.phaseId || c.phase)) continue;
        const d = d8f(c.telefone); if (d.length < 8) continue;
        parados[d] = { id: c.id, sis, nome: c.nomeContato, equipamento: c.equipamento, valor: c.valor, tel: c.telefone };
      }
    }
    // ESTRITO: precisa de verbo de ação sobre o CONSERTO. Palavras ambíguas como "combinado",
    // "fechado" ou "beleza" sozinhas NÃO são aprovação — o cliente pode estar concordando em
    // AGUARDAR o orçamento (erro real: Dirceu "Combinado..👍" e Jôsedna "Está combinado no aguardo").
    const SIM = /\b(aprovo|aprovado|pode fazer|pode consertar|pode arrumar|pode reparar|pode seguir com o (conserto|reparo)|autorizo|pode executar|manda fazer|faz(er)? sim|quero (que )?conserte)\b/i;
    const AMBIGUO = /\b(combinado|fechado|beleza|ok|t[áa] bom|certo)\b/i;
    const alvos = [];
    for (const e of evtsP) {
      if (e.dir !== 'in' || !e.texto) continue;
      const d = d8f(e.tel); if (!parados[d]) continue;
      if (!SIM.test(String(e.texto))) continue;
      if (so.length && !so.includes(parados[d].id)) continue;
      if (!alvos.some(a => a.id === parados[d].id)) alvos.push(Object.assign({ d8: d }, parados[d]));
    }
    if (!aplicar) {
      return res.status(200).json({ ok: true, modo: 'prévia',
        total: alvos.length,
        lista: alvos.map(a => a.nome + ' | ' + a.equipamento + ' | R$ ' + a.valor + ' | ' + a.sis.toUpperCase()),
        dica: 'para executar: &aplicar=1 (aprova no sistema e avisa o cliente)' });
    }
    const { token, phoneId } = await credenciais();
    const feitos = [], erros = [];
    for (const a of alvos) {
      try {
        if (a.sis === 'tv') {
          const fTv = (((tvLogP || {}).fichas) || []).find(f => d8f(f.telefone) === a.d8 &&
            ['orc_enviado', 'orc_registrado'].includes(f.phase));
          if (fTv) {
            await fetch(`https://reparoeletroadm.com/api/tv-logistica?action=aprovar-orcamento&k=${KTF}`, {
              method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ id: fTv.id }) });
          } else {
            await fetch(`https://reparoeletroadm.com/api/tv-pipe?action=mover&k=${KTF}`, {
              method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ id: a.id, phase: 'aprovados' }) });
          }
        } else {
          await fetch(`https://reparoeletroadm.com/api/pipe?action=mover&k=${KTF}`, {
            method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ id: a.id, phase: 'aprovados' }) });
        }
        await bumpStat('aprovacoes');
        // avisa o cliente — UMA VEZ SÓ (5 clientes receberam em duplicidade no primeiro disparo)
        const jaAvisou = (await dbGet('wa_aprov_retro')) || { ids: {} };
        if (token && phoneId && a.tel && !jaAvisou.ids[a.id]) {
          const to = String(a.tel).replace(/\D/g, '');
          const to55 = to.startsWith('55') ? to : '55' + to;
          const txt = `Perfeito! Seu ${a.equipamento || 'equipamento'} já está em processo de conserto. 😊\n\nAguardo só você me confirmar se o pagamento vai ser no Pix ou no cartão, para eu já atualizar a sua ficha.`;
          const r = await fetch(`https://graph.facebook.com/v20.0/${phoneId}/messages`, {
            method: 'POST', headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
            body: JSON.stringify({ messaging_product: 'whatsapp', to: to55, type: 'text', text: { body: txt } }),
          }).then(x => x.json()).catch(() => null);
          if (r && r.messages && r.messages[0]) {
            await rpushEvt({ ts: new Date().toISOString(), tel: to55, dir: 'out', texto: txt, tipo: 'aprovacao-retroativa' });
            jaAvisou.ids[a.id] = new Date().toISOString();
            await dbSet('wa_aprov_retro', jaAvisou);
          }
        }
        feitos.push(a.nome + ' (' + a.sis.toUpperCase() + ')');
      } catch (e) { erros.push(a.nome + ': ' + e.message); }
      await new Promise(r => setTimeout(r, 200));
    }
    return res.status(200).json({ ok: erros.length === 0, aprovados: feitos.length, feitos, erros });
  }

  // ── 🕵️ ABORDAGENS-FANTASMA: marcadas como abordadas sem mensagem no histórico ──
  if (action === 'abordagens-fantasma') {
    const [fA, fT, evtsF, abF] = await Promise.all([
      dbGet('fichas_adm'), dbGet('fichas_tv'), lerEvts(), dbGet('wa_abordados').then(v => v || { tels: {} }),
    ]);
    const comMensagem = new Set();
    for (const e of evtsF) {
      if (e.dir !== 'out') continue;
      const d = String(e.tel || '').replace(/\D/g, '').slice(-8);
      if (d.length >= 8) comMensagem.add(d);
    }
    const fantasmas = [];
    for (const f of [...(((fA || {}).fichas) || []), ...(((fT || {}).fichas) || [])]) {
      if (!['contato_feito', 'entrar_contato'].includes(f.status)) continue;
      if (!f.abordadoPorBot && !f.contatoFeitoEm) continue;
      const d = String(f.telefone || '').replace(/\D/g, '').slice(-8);
      if (d.length < 8 || comMensagem.has(d)) continue;
      fantasmas.push({ nome: f.nome, telefone: f.telefone, equipamento: f.equipamento,
        status: f.status, marcadaEm: f.contatoFeitoEm || f.movidoEm, sheetRow: f.sheetRow,
        falha: f.falhaAbordagem || null });
    }
    fantasmas.sort((a, b) => String(b.marcadaEm).localeCompare(String(a.marcadaEm)));
    if (String(req.query.devolver || '') === '1' && fantasmas.length) {
      const tels = new Set(fantasmas.map(x => String(x.telefone || '').replace(/\D/g, '').slice(-8)));
      for (const key of ['fichas_adm', 'fichas_tv']) {
        const bd = (await dbGet(key)) || { fichas: [] };
        let mexeu = false;
        for (const f of (bd.fichas || [])) {
          const d = String(f.telefone || '').replace(/\D/g, '').slice(-8);
          if (!tels.has(d)) continue;
          if (!['contato_feito', 'entrar_contato'].includes(f.status)) continue;
          f.status = 'criada'; delete f.contatoFeitoEm; delete f.abordadoPorBot;
          f.devolvidaEm = new Date().toISOString(); mexeu = true;
        }
        if (mexeu) await dbSet(key, bd);
      }
      const ab = (await dbGet('wa_abordados')) || { tels: {} };
      for (const d of tels) delete (ab.tels || {})[d];
      await dbSet('wa_abordados', ab);
      return res.status(200).json({ ok: true, devolvidas: fantasmas.length,
        msg: 'voltaram para "ficha criada" e o bot vai abordá-las no próximo ciclo', lista: fantasmas.slice(0, 40) });
    }
    return res.status(200).json({ ok: true, total: fantasmas.length,
      explicacao: 'fichas marcadas como abordadas/contatadas cujo telefone NÃO tem nenhuma mensagem enviada no histórico — a abordagem falhou mas a ficha saiu de criada',
      lista: fantasmas.slice(0, 60),
      dica: 'para devolvê-las para "ficha criada": mesmo link com &devolver=1' });
  }

  // ── 📋 REATIVACAO-RELATORIO: tudo que o motor disparou e para quem ──
  if (action === 'reativacao-relatorio') {
    const [reat, evtsRR, envRR] = await Promise.all([
      dbGet('wa_reativacao').then(v => v || { alvos: {} }), lerEvts(),
      dbGet('wa_orc_enviados').then(v => v || { ids: {} }),
    ]);
    // mensagens de reativação efetivamente enviadas (ficam marcadas no histórico)
    const enviadas = evtsRR.filter(e => e.dir === 'out' &&
      (e.tipo === 'reativacao' || /\[reativação/i.test(String(e.texto || ''))));
    const porTel = {};
    for (const e of enviadas) {
      const d = String(e.tel || '').replace(/\D/g, '').slice(-8);
      if (!porTel[d]) porTel[d] = { tel: e.tel, toques: 0, primeiro: e.ts, ultimo: e.ts, textos: [] };
      porTel[d].toques++;
      porTel[d].ultimo = e.ts;
      porTel[d].textos.push(String(e.texto || '').slice(0, 90));
    }
    // o cliente já tinha conversado antes do primeiro toque?
    const lista = Object.keys(porTel).map(d => {
      const c = porTel[d];
      const inAntes = evtsRR.some(e => e.dir === 'in' &&
        String(e.tel || '').replace(/\D/g, '').slice(-8) === d &&
        new Date(e.ts || 0) < new Date(c.primeiro));
      const respondeu = evtsRR.some(e => e.dir === 'in' &&
        String(e.tel || '').replace(/\D/g, '').slice(-8) === d &&
        new Date(e.ts || 0) > new Date(c.primeiro));
      return { tel: c.tel, d8: d, toques: c.toques, primeiro: c.primeiro, ultimo: c.ultimo,
        tinhaConversaAntes: inAntes, respondeuDepois: respondeu,
        indevido: !inAntes, amostra: c.textos.slice(0, 2) };
    }).sort((a, b) => (a.tinhaConversaAntes === b.tinhaConversaAntes ? 0 : (a.tinhaConversaAntes ? 1 : -1)));
    const indevidos = lista.filter(x => x.indevido);
    return res.status(200).json({ ok: true,
      totalMensagens: enviadas.length,
      clientesAtingidos: lista.length,
      semConversaPrevia: indevidos.length,
      responderam: lista.filter(x => x.respondeuDepois).length,
      alvosNoRegistro: Object.keys((reat || {}).alvos || {}).length,
      veredito: indevidos.length
        ? '⚠️ ' + indevidos.length + ' cliente(s) receberam reativação sem nunca terem conversado com o bot'
        : '✅ todos os atingidos já tinham conversa com o bot',
      indevidos: indevidos.slice(0, 40), todos: lista.slice(0, 60) });
  }

  // ── 🔍 CONFLITOS-AUDIT: escalar_humano × registrar_conflito (são coisas diferentes) ──
  if (action === 'conflitos-audit') {
    const [prosA, evtsA] = await Promise.all([dbGet('prospeccao_adm'), lerEvts()]);
    const confs = (((prosA || {}).fichas) || []).filter(f => f.status === 'conflitos_bot');
    const classifica = m => {
      const s = String(m || '').toLowerCase();
      if (/garantia/.test(s)) return 'garantia';
      if (/reprov|não quer|nao quer|desistiu|recusou/.test(s)) return 'reprovação do orçamento';
      if (/ciclo comercial esgotado/.test(s)) return 'reativação esgotada';
      if (/pagamento|pix|cobran/.test(s)) return 'pagamento';
      return 'outro';
    };
    const escalados = evtsA.filter(e => e.dir === 'acao' && e.texto === 'escalar_humano');
    const conflitosAcao = evtsA.filter(e => e.dir === 'acao' && e.texto === 'registrar_conflito');
    const porMotivo = {};
    for (const c of confs) { const k = classifica(c.motivoConflito); porMotivo[k] = (porMotivo[k] || 0) + 1; }
    return res.status(200).json({ ok: true,
      veredito: 'escalar_humano NÃO cria conflito — só acende o alerta amarelo no painel. Conflito só nasce de registrar_conflito.',
      conflitosBotAbertos: confs.length,
      porMotivo,
      acoesEscalarHumano: escalados.length,
      acoesRegistrarConflito: conflitosAcao.length,
      conflitos: confs.slice(0, 40).map(c => ({ nome: c.nome, telefone: c.telefone,
        equipamento: c.equipamento, tipo: classifica(c.motivoConflito), motivo: c.motivoConflito, criadoEm: c.criadoEm })),
      escalasSemConflito: escalados.slice(-20).map(e => ({ tel: e.tel, ts: e.ts })) });
  }

  // ── DEBUG da abordagem: por que cada ficha passa ou não nos filtros ──
  // ── HORA-DEBUG: prova do relógio que o bot usa ──
  if (action === 'hora-debug') {
    const bras = new Date(Date.now() - 3 * 3600 * 1000);
    const dias = ['domingo', 'segunda', 'terça', 'quarta', 'quinta', 'sexta', 'sábado'];
    const dia = bras.getUTCDay(), hora = bras.getUTCHours(), min = bras.getUTCMinutes();
    const dentro = (dia >= 1 && dia <= 5) ? (hora + min / 60 >= 8 && hora + min / 60 < 15) : (dia === 6 ? (hora + min / 60 >= 8 && hora + min / 60 < 10) : false);
    return res.status(200).json({ ok: true,
      horaBrasilia: `${dias[dia]}, ${String(hora).padStart(2, '0')}:${String(min).padStart(2, '0')}`,
      dentroHorarioComercial: dentro,
      referencia: 'UTC-3 fixo (Brasília, sem horário de verão desde 2019)' });
  }

  if (action === 'abordagem-debug') {
    const [fdbD, fdbTvD, evtsD, abordD] = await Promise.all([
      dbGet('fichas_adm'), dbGet('fichas_tv'), lerEvts(), dbGet('wa_abordados').then(v => v || { tels: {} }),
    ]);
    const jaFalaramD = new Set(evtsD.filter(e => e.dir === 'in').map(e => String(e.tel || '').replace(/\D/g, '').slice(-8)));
    const [_logD, _tvLogD, _pipeD] = await Promise.all([dbGet('reparoeletro_logistica'), dbGet('tv_logistica'), dbGet('reparoeletro_pipe')]);
    const _emOpD = new Set();
    const _FA = ['liberado_coleta', 'horario_marcado', 'em_rota', 'motorista_parceiro', 'remarcar', 'orc_enviado'];
    const _vivo = (f, dias) => Date.now() - new Date(f.movedAt || f.criadoEm || 0).getTime() < dias * 86400000;
    for (const f of (((_logD || {}).fichas) || []).concat(((_tvLogD || {}).fichas) || [])) {
      const d = String(f.telefone || '').replace(/\D/g, '').slice(-8);
      if (d.length < 8) continue;
      if ((_FA.includes(f.phase) || ['coleta_efetuada', 'orc_registrado'].includes(f.phase)) && _vivo(f, 30)) _emOpD.add(d);
    }
    for (const c of (((_pipeD || {}).cards) || [])) {
      const d = String(c.telefone || '').replace(/\D/g, '').slice(-8);
      if (d.length >= 8 && !['finalizado', 'arquivado'].includes(c.phaseId || c.phase) && _vivo(c, 45)) _emOpD.add(d);
    }
    const agoraD = Date.now();
    const analisa = (f, sis) => {
      const d8 = String(f.telefone || '').replace(/\D/g, '').slice(-8);
      const idadeMin = Math.round((agoraD - new Date(f.criadoEm || 0).getTime()) / 60000);
      return { sis, nome: f.nome, status: f.status || '(vazio)', idadeMin,
        telOk: d8.length >= 8, virgem: !f.status || f.status === 'ficha_criada' || f.status === 'criada',
        idadeOk: idadeMin > 5, clienteJaEscreveu: jaFalaramD.has(d8), jaAbordado: !!abordD.tels[d8], jaEmOperacao: _emOpD.has(d8), d8 };
    };
    const _horarioOkD = dentroHorarioComercial();
    const ehCriada = f => !f.status || f.status === 'ficha_criada' || f.status === 'criada';
    const paradas = [
      ...((((fdbD || {}).fichas) || []).filter(ehCriada).map(f => analisa(f, 'adm'))),
      ...((((fdbTvD || {}).fichas) || []).filter(ehCriada).map(f => analisa(f, 'tv'))),
    ];
    for (const t of paradas) {
      const barreiras = [];
      if (!t.telOk) barreiras.push('telefone inválido/curto');
      if (!t.idadeOk) barreiras.push('menos de 5min de vida');
      if (t.clienteJaEscreveu) barreiras.push('cliente falou nas últimas 24h (bot responde na conversa)');
      if (t.jaAbordado) barreiras.push('já recebeu abordagem (dedupe)');
      if (t.jaEmOperacao) barreiras.push('operação em andamento (logística/pipe ativos)');
      if (!_horarioOkD) barreiras.push('FORA DA JANELA COMERCIAL (seg-sex 8h-15h, sáb 8h-10h) — em standby até a próxima abertura');
      const msgsCli = evtsD.filter(e => String(e.tel || '').replace(/\D/g, '').slice(-8) === t.d8);
      t.mensagensTrocadas = msgsCli.length;
      t.clienteRespondeu = msgsCli.some(e => e.dir === 'in');
      t.ultimaMensagem = msgsCli.length ? msgsCli[msgsCli.length - 1].ts : null;
      t.veredito = barreiras.length ? 'BARRADA: ' + barreiras.join(' + ') : '✅ SERIA ABORDADA no próximo ciclo';
    }
    const resumo = {};
    paradas.forEach(t => { resumo[t.veredito.split(':')[0] === 'BARRADA' ? t.veredito : '✅ na fila'] = (resumo[t.veredito.split(':')[0] === 'BARRADA' ? t.veredito : '✅ na fila'] || 0) + 1; });
    return res.status(200).json({ ok: true,
      janelaComercialAgora: _horarioOkD ? 'ABERTA — abordagens saindo' : 'FECHADA — fichas em standby (seg-sex 8h-15h, sáb 8h-10h)',
      criadasAdm: paradas.filter(t => t.sis === 'adm').length,
      criadasTv: paradas.filter(t => t.sis === 'tv').length,
      resumoPorMotivo: resumo,
      fichas: paradas.slice(0, 40) });
  }

  if (action === 'abordagem-fichas') {
    // Importa fichas novas da planilha ANTES de tudo (roda mesmo fora do horário — elimina o ponto cego planilha→sistema)
    // e roda o motor de transições dos DOIS sistemas (regra da 1h não depende de tela aberta)
    try {
      const KS = (process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim();
      await fetch(`https://reparoeletroadm.com/api/fichas?action=sync&k=${KS}`);
      await fetch(`https://reparoeletroadm.com/api/fichas?action=load&k=${KS}`);
      await fetch(`https://reparoeletroadm.com/api/fichas?action=load&sistema=tv&k=${KS}`);
    } catch (e) {}
    const cfgA = (await dbGet('wa_bot_config')) || {};
    if (cfgA.abordagemAtiva !== true) return res.status(200).json({ ok: true, msg: 'abordagem desligada (wa_bot_config.abordagemAtiva)' });
    const _janelaAberta = dentroHorarioComercial();
    const { token, phoneId } = await credenciais();
    if (!token || !phoneId) return res.status(200).json({ ok: false, error: 'credenciais ausentes' });
    const agora = Date.now();
    // ── RETOMADAS: clientes que pediram agendamento fora do horário — chamar na abertura da janela ──
    try {
      const ret = (await dbGet('wa_retomar')) || { tels: [] };
      if (ret.tels.length) {
        const pend = ret.tels.slice(0, 10);
        ret.tels = ret.tels.slice(10);
        for (const rt of pend) {
          try {
            const saud = (new Date(Date.now() - 3 * 3600 * 1000)).getUTCHours() < 12 ? 'Bom dia' : 'Boa tarde';
            const msgRet = saud + '! Conforme combinamos, já consigo resolver o seu atendimento Você prefere trazer o equipamento aqui na loja (orçamento gratuito e na hora — Rua Ouro Preto, 663) ou que a gente colete no seu endereço? Se for coleta, qual faixa fica melhor: 08h-10h, 10h-12h, 12h-14h ou 14h-16h?';
            await fetch(`https://graph.facebook.com/v20.0/${phoneId}/messages`, {
              method: 'POST', headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
              body: JSON.stringify({ messaging_product: 'whatsapp', to: rt, type: 'text',
                text: { body: msgRet } }),
            });
            await rpushEvt({ ts: new Date().toISOString(), tel: rt, dir: 'out', texto: msgRet, tipo: 'retomada' });
          } catch (e) {}
        }
        await dbSet('wa_retomar', ret);
      }
    } catch (e) {}
    const [fdb, fdbTv, evts, abordados, logAb, tvLogAb, pipeAb] = await Promise.all([
      dbGet('fichas_adm'), dbGet('fichas_tv'), lerEvts(), dbGet('wa_abordados').then(v => v || { tels: {} }),
      dbGet('reparoeletro_logistica'), dbGet('tv_logistica'), dbGet('reparoeletro_pipe'),
    ]);
    // Última mensagem IN por telefone (para decidir template × texto direto; conversa antiga não bloqueia mais a abordagem)
    const ultimaInPor = {};
    for (const e of evts) if (e.dir === 'in') {
      const d8e = String(e.tel || '').replace(/\D/g, '').slice(-8);
      const t = new Date(e.ts || 0).getTime();
      if (!ultimaInPor[d8e] || t > ultimaInPor[d8e]) ultimaInPor[d8e] = t;
    }
    // Só bloqueia abordagem quem tem conversa ATIVA agora (falou nas últimas 24h — o bot já responde no fluxo normal)
    const jaFalaram = new Set(Object.keys(ultimaInPor).filter(d8 => Date.now() - ultimaInPor[d8] < 24 * 3600 * 1000));
    // marco de corte gravado ao iniciar o número novo (fichas anteriores ficam de fora)
    const marcoAbordagem = (function () {
      try { const m = (cfg || {}).abordagemMarcoTs; return m ? new Date(m).getTime() : 0; } catch (e) { return 0; }
    })();
    // Telefones JÁ DENTRO DA OPERAÇÃO (logística ADM/TV ou pipe): nunca abordar como coleta nova —
    // caso real: cliente com TV já coletada e orçamento pronto recebia o protocolo de coleta
    const emOperacao = new Set();
    const FASES_ANDAMENTO = ['liberado_coleta', 'horario_marcado', 'em_rota', 'motorista_parceiro', 'remarcar', 'orc_enviado'];
    const vivo = (f, dias) => Date.now() - new Date(f.movedAt || f.criadoEm || 0).getTime() < dias * 86400000;
    // Logística ADM/TV: bloqueia só operação EM ANDAMENTO (pré-coleta sempre; equipamento conosco: últimos 30 dias)
    for (const f of ((((logAb || {}).fichas) || []).concat(((tvLogAb || {}).fichas) || []))) {
      const d = String(f.telefone || '').replace(/\D/g, '').slice(-8);
      if (d.length < 8) continue;
      if (FASES_ANDAMENTO.includes(f.phase) && vivo(f, 30)) emOperacao.add(d);
      else if (['coleta_efetuada', 'orc_registrado'].includes(f.phase) && vivo(f, 30)) emOperacao.add(d);
    }
    // Pipe: cards ativos recentes (45 dias) — atendimento antigo concluído não bloqueia cliente recorrente
    for (const c of (((pipeAb || {}).cards) || [])) {
      const d = String(c.telefone || '').replace(/\D/g, '').slice(-8);
      if (d.length >= 8 && !['finalizado', 'arquivado'].includes(c.phaseId || c.phase) && vivo(c, 45)) emOperacao.add(d);
    }
    const todasFichas = [
      ...(((fdb && fdb.fichas) || []).map(f => Object.assign(f, { _sis: 'adm' }))),
      ...(((fdbTv && fdbTv.fichas) || []).map(f => Object.assign(f, { _sis: 'tv' }))),
    ];
    // 🩹 AUTOCURA (roda a cada ciclo): fichas presas em "criada" que já deviam ter saído
    try {
      let curou = false;
      const AVANCADOS = ['contato_feito', 'logistica', 'entrar_contato', 'prospeccao', 'cliente_loja'];
      for (const f of todasFichas) {
        const ehCr = !f.status || f.status === 'ficha_criada' || f.status === 'criada';
        if (!ehCr) continue;
        // 🛟 FICHA RECUPERADA: foi excluída por engano e devolvida. Nas primeiras 24h ela NÃO é
        // tratada como refeita nem como já abordada — precisa ser atendida como ficha nova.
        const recEm = new Date(f.restauradaEm || f.recuperadaEm || f.devolvidaEm || 0).getTime();
        if (recEm && Date.now() - recEm < 24 * 3600000) continue;
        const d8c = String(f.telefone || '').replace(/\D/g, '').slice(-8);
        // FICHA REFEITA: mesmo cliente tem OUTRA ficha em estágio avançado OU operação em andamento
        // → vai direto para ENTRAR EM CONTATO: a equipe liga e tira as dúvidas (regra do Pedro)
        const temOutraAvancada = todasFichas.some(o => o.id !== f.id &&
          String(o.telefone || '').replace(/\D/g, '').slice(-8) === d8c && AVANCADOS.includes(o.status));
        if (temOutraAvancada || emOperacao.has(d8c)) {
          f.status = 'entrar_contato';
          f.entrarContatoMotivo = 'cliente refez a ficha (já abordado/em atendimento) — ligar para tirar dúvidas';
          f.fichaRefeita = true; curou = true;
        } else if (abordados.tels[d8c]) {
          // ficha original presa pela corrida → contato_feito com o horário real da abordagem (regua da 1h arma daqui)
          f.status = 'contato_feito';
          f.contatoFeitoEm = f.contatoFeitoEm || abordados.tels[d8c];
          f.abordadoPorBot = true; curou = true;
        } else if (ultimaInPor[d8c]) {
          // cliente já conversa com o bot → contato existe de fato
          f.status = 'contato_feito';
          f.contatoFeitoEm = f.contatoFeitoEm || new Date(ultimaInPor[d8c]).toISOString();
          f.abordadoPorBot = true; curou = true;
        }
      }
      if (curou) { await dbSet('fichas_adm', fdb); if (fdbTv) await dbSet('fichas_tv', fdbTv); }
    } catch (e) {}
    if (!_janelaAberta) return res.status(200).json({ ok: true, autocura: 'executada', msg: 'fora do horário comercial — fichas organizadas, disparos em standby até a próxima janela' });
    const candidatas = todasFichas.filter(f => {
      const idade = agora - new Date(f.criadoEm || 0).getTime();
      const d8 = String(f.telefone || '').replace(/\D/g, '').slice(-8);
      const virgem = !f.status || f.status === 'ficha_criada' || f.status === 'criada';
      // 🚀 MARCO DO NÚMERO NOVO: ficha anterior ao corte não é abordada pelo número novo
      const depoisDoMarco = !marcoAbordagem || new Date(f.criadoEm || 0).getTime() >= marcoAbordagem;
      return virgem && depoisDoMarco && idade > 5 * 60000 && d8.length >= 8 &&
        !jaFalaram.has(d8) && !abordados.tels[d8] && !emOperacao.has(d8);
    }).sort((a, b) => String(b.criadoEm || '').localeCompare(String(a.criadoEm || '')))
      .slice(0, 10); // máx 10 por ciclo, mais recentes primeiro
    const disparadas = [];
    for (const f of candidatas) {
      const telA = String(f.telefone).replace(/\D/g, '');
      const to = telA.startsWith('55') ? telA : '55' + telA;
      try {
        const r = await fetch(`https://graph.facebook.com/v20.0/${phoneId}/messages`, {
          method: 'POST',
          headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
          body: JSON.stringify({ messaging_product: 'whatsapp', to, type: 'template',
            template: (f._sis === 'tv'
              ? { name: 'cadastro_recebido_tv', language: { code: 'pt_BR' },
                  components: [{ type: 'body', parameters: [
                    { type: 'text', text: (f.nome || 'tudo bem').split(' ')[0] },
                  ] }] }
              : { name: 'cadastro_recebido', language: { code: 'pt_BR' },
                  components: [{ type: 'body', parameters: [
                    { type: 'text', text: (f.nome || 'tudo bem').split(' ')[0] },
                    { type: 'text', text: f.equipamento || 'equipamento' },
                  ] }] }) }),
        });
        const j = await r.json();
        let okA = !!(j.messages && j.messages[0]);
        let usouFallbackAdm = false;
        if (!okA && f._sis === 'tv') {
          // template TV ainda não aprovado → usa o template atual (decisão do dono: não parar a esteira; migra sozinho quando aprovar)
          const r2 = await fetch(`https://graph.facebook.com/v20.0/${phoneId}/messages`, {
            method: 'POST', headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
            body: JSON.stringify({ messaging_product: 'whatsapp', to, type: 'template',
              template: { name: 'cadastro_recebido', language: { code: 'pt_BR' },
                components: [{ type: 'body', parameters: [
                  { type: 'text', text: (f.nome || 'tudo bem').split(' ')[0] },
                  { type: 'text', text: f.equipamento || 'TV' },
                ] }] } }),
          }).then(x => x.json()).catch(() => null);
          okA = !!(r2 && r2.messages && r2.messages[0]);
          usouFallbackAdm = okA;
        }
        if (okA) {
          try {
            f.status = 'contato_feito';
            f.contatoFeitoEm = new Date().toISOString();
            f.abordadoPorBot = true;
          } catch (e) {}
        }
        // 🚨 SÓ marca como abordado e SÓ registra no histórico se a Meta confirmou o envio.
        // Antes marcava mesmo com falha: a ficha saía de "criada" e o cliente nunca recebia nada.
        if (!okA) {
          f.falhaAbordagem = { em: new Date().toISOString(), erro: (j && j.error && j.error.message) || 'envio não confirmado' };
          disparadas.push({ nome: f.nome, ok: false, erro: f.falhaAbordagem.erro });
          continue;
        }
        abordados.tels[telA.slice(-8)] = new Date().toISOString();
        if (f._sis === 'tv' && !usouFallbackAdm) {
          await rpushEvt({ ts: new Date().toISOString(), tel: to, dir: 'out',
            texto: 'Olá ' + ((f.nome || 'tudo bem').split(' ')[0]) + ', tudo bem? Sou o Alessandro, responsável pela Logística da Reparo Eletro - TVs. Recebemos o seu cadastro para o conserto da sua TV.\n\nPodemos prosseguir com o atendimento?', tipo: 'template' });
        } else
        await rpushEvt({ ts: new Date().toISOString(), tel: to, dir: 'out',
          texto: 'Olá ' + ((f.nome || 'tudo bem').split(' ')[0]) + ', tudo bem? Alessandro aqui, responsável pela logística da Reparo Eletro. Recebemos o seu cadastro para o conserto do seu ' + (f.equipamento || 'equipamento') + '!\n\nTEMOS 2 OPÇÕES: COLETA E ENTREGA / ATENDIMENTO NO BALCÃO\n\n*ATENÇÃO: Trazendo seu equipamento aqui na loja, o orçamento é gratuito e consertamos em 15 minutos! Estamos na Rua Ouro Preto, 663 - Barro Preto*\n\nCaso prefira a nossa coleta e entrega, podemos buscar hoje mesmo na sua casa!\n\nJá estamos prontos para te atender! Me fala qual opção você escolheu, por favor? 😊', tipo: 'template' });
        if (okA) await bumpStat('abordagens');
        disparadas.push({ nome: f.nome, ok: okA });
      } catch (e) { disparadas.push({ nome: f.nome, erro: e.message }); }
    }
    // Persistir transições com RELEITURA (evita a corrida com o polling da tela de fichas)
    try {
      if (disparadas.some(d => d.ok)) {
        const okIds = new Set(candidatas.filter(f => f.status === 'contato_feito').map(f => f.id));
        const [fdbF, fdbTvF] = await Promise.all([dbGet('fichas_adm'), dbGet('fichas_tv')]);
        for (const bank of [fdbF, fdbTvF]) {
          if (!bank || !bank.fichas) continue;
          for (const ff of bank.fichas) if (okIds.has(ff.id)) {
            ff.status = 'contato_feito';
            ff.contatoFeitoEm = ff.contatoFeitoEm || new Date().toISOString();
            ff.abordadoPorBot = true;
          }
        }
        if (fdbF) await dbSet('fichas_adm', fdbF);
        if (fdbTvF) await dbSet('fichas_tv', fdbTvF);
      }
    } catch (e) {}
    // Poda do registro (30 dias)
    const corteA = agora - 30 * 86400000;
    for (const k of Object.keys(abordados.tels)) {
      if (new Date(abordados.tels[k]).getTime() < corteA) delete abordados.tels[k];
    }
    await dbSet('wa_abordados', abordados);
    return res.status(200).json({ ok: true, candidatas: candidatas.length, disparadas });
  }

  // ── CRIAR-TEMPLATES: registra os templates Utility na Meta (aprovação ~horas) ──
  if (action === 'criar-templates') {
    const wabaId = String(req.query.waba || '1699351717944043').trim();
    const { token } = await credenciais();
    if (!token) return res.status(200).json({ ok: false, error: 'sem token' });
    const templates = [
      { name: 'cadastro_recebido', language: 'pt_BR', category: 'UTILITY',
        components: [{ type: 'BODY',
          text: 'Olá {{1}}, tudo bem? Alessandro aqui, responsável pela logística da Reparo Eletro. Recebemos o seu cadastro para o conserto do seu {{2}}!\n\nTEMOS 2 OPÇÕES: COLETA E ENTREGA / ATENDIMENTO NO BALCÃO\n\n*ATENÇÃO: Trazendo seu equipamento aqui na loja, o orçamento é gratuito e consertamos em 15 minutos! Estamos na Rua Ouro Preto, 663 - Barro Preto*\n\nCaso prefira a nossa coleta e entrega, podemos buscar hoje mesmo na sua casa!\n\nJá estamos prontos para te atender! Me fala qual opção você escolheu, por favor? 😊',
          example: { body_text: [['Maria', 'purificador']] } }] },
      { name: 'orcamento_pronto', language: 'pt_BR', category: 'UTILITY',
        components: [{ type: 'BODY',
          text: 'Olá {{1}}! Aqui é da Reparo Eletro 😊 O diagnóstico do seu {{2}} ficou pronto e já temos o orçamento do conserto. Posso te enviar os detalhes por aqui?',
          example: { body_text: [['Maria', 'micro-ondas']] } }] },
      { name: 'conserto_finalizado', language: 'pt_BR', category: 'UTILITY',
        components: [{ type: 'BODY',
          text: 'Olá {{1}}! 🛠️ A equipe técnica acabou de finalizar o conserto do seu equipamento. Agora ele entra para a fase de testes. Assim que a fase de testes for finalizada, nossa equipe entrará em contato com você para fazer a entrega.',
          example: { body_text: [['Maria']] } }] },
      { name: 'coleta_confirmada', language: 'pt_BR', category: 'UTILITY',
        components: [{ type: 'BODY',
          text: 'Olá {{1}}! Sua coleta do {{2}} está confirmada para {{3}}. Nosso motorista entra em contato quando estiver a caminho. 🚚',
          example: { body_text: [['Maria', 'micro-ondas', 'amanhã de manhã']] } }] },
    ];
    const resultados = {};
    for (const t of templates) {
      try {
        const r = await fetch(`https://graph.facebook.com/v20.0/${wabaId}/message_templates`, {
          method: 'POST',
          headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
          body: JSON.stringify(t),
        });
        resultados[t.name] = await r.json();
      } catch (e) { resultados[t.name] = { erro: e.message }; }
    }
    return res.status(200).json({ ok: true, resultados });
  }

  // ── PERFIL-CONFIG (GET): descrição, recado, endereço, e-mail, site e categoria do perfil ──
  if (action === 'perfil-config') {
    const { token, phoneId } = await credenciais();
    if (!token || !phoneId) return res.status(200).json({ ok: false, error: 'credenciais ausentes' });
    try {
      const r = await fetch(`https://graph.facebook.com/v20.0/${phoneId}/whatsapp_business_profile`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({
          messaging_product: 'whatsapp',
          about: 'Assistência técnica especializada em eletrodomésticos',
          address: 'Rua Ouro Preto, 663 - Barro Preto, Belo Horizonte - MG',
          description: 'Conserto de micro-ondas, purificadores, adegas, fornos e TVs. Orçamento grátis no balcão e coleta com entrega em BH. Horário: Seg a Sex 08h-17h · Sáb 08h-12h.',
          email: 'reparoeletrobh@gmail.com',
          websites: ['https://reparoeletroadm.com/equipamentos'],
          vertical: 'PROF_SERVICES',
        }),
      });
      const j = await r.json();
      return res.status(200).json({ ok: !!j.success, meta: j });
    } catch (e) { return res.status(200).json({ ok: false, error: e.message }); }
  }

  // ── PERFIL-NOME (GET): solicita a troca do nome de exibição (?nome=) — passa por análise da Meta ──
  if (action === 'perfil-nome') {
    const novoNome = String(req.query.nome || '').trim();
    if (!novoNome) return res.status(400).json({ ok: false, error: 'informe ?nome=' });
    const { token, phoneId } = await credenciais();
    try {
      const r = await fetch(`https://graph.facebook.com/v20.0/${phoneId}?new_display_name=${encodeURIComponent(novoNome)}`, {
        method: 'POST', headers: { Authorization: `Bearer ${token}` } });
      const j = await r.json();
      return res.status(200).json({ ok: !!j.success, meta: j });
    } catch (e) { return res.status(200).json({ ok: false, error: e.message }); }
  }

  // ── PERFIL-FOTO (GET): sobe a logo (do próprio site) como foto de perfil do número via Meta Resumable Upload ──
  if (action === 'perfil-foto') {
    const appId = String(req.query.app || '1007161065497390');
    const { token, phoneId } = await credenciais();
    if (!token || !phoneId) return res.status(200).json({ ok: false, error: 'credenciais ausentes' });
    try {
      // 1. Baixar a logo do próprio domínio
      const imgR = await fetch('https://reparoeletroadm.com/logo-wa.jpg');
      if (!imgR.ok) return res.status(200).json({ ok: false, error: 'logo-wa.jpg não encontrada no site' });
      const buf = Buffer.from(await imgR.arrayBuffer());
      // 2. Abrir sessão de upload
      const s1 = await fetch(`https://graph.facebook.com/v20.0/${appId}/uploads?file_length=${buf.length}&file_type=image/jpeg&access_token=${encodeURIComponent(token)}`, { method: 'POST' });
      const j1 = await s1.json();
      if (!j1.id) return res.status(200).json({ ok: false, passo: 'sessao', meta: j1 });
      // 3. Enviar o binário
      const s2 = await fetch(`https://graph.facebook.com/v20.0/${j1.id}`, {
        method: 'POST',
        headers: { Authorization: `OAuth ${token}`, file_offset: '0', 'Content-Type': 'application/octet-stream' },
        body: buf,
      });
      const j2 = await s2.json();
      if (!j2.h) return res.status(200).json({ ok: false, passo: 'upload', meta: j2 });
      // 4. Aplicar no perfil do número
      const s3 = await fetch(`https://graph.facebook.com/v20.0/${phoneId}/whatsapp_business_profile`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ messaging_product: 'whatsapp', profile_picture_handle: j2.h }),
      });
      const j3 = await s3.json();
      return res.status(200).json({ ok: !!j3.success, aplicado: j3 });
    } catch (e) { return res.status(200).json({ ok: false, error: e.message }); }
  }

  // ── CONFLITO-RESOLVIDO (POST {tel}): resolve a ficha de conflito do telefone + nota na conversa ──
  if (req.method === 'POST' && action === 'conflito-resolvido') {
    const telCR = String((req.body && req.body.tel) || '').replace(/\D/g, '');
    if (!telCR) return res.status(400).json({ ok: false, error: 'tel obrigatório' });
    const d8cr = telCR.slice(-8);
    const pdb = (await dbGet('prospeccao_adm')) || { fichas: [] };
    const antes = (pdb.fichas || []).length;
    pdb.fichas = (pdb.fichas || []).filter(f => !(f.status === 'conflitos_bot' && String(f.telefone || '').replace(/\D/g, '').slice(-8) === d8cr));
    await dbSet('prospeccao_adm', pdb);
    await rpushEvt({ ts: new Date().toISOString(), tel: telCR, dir: 'out', tipo: 'nota',
      texto: '✔ Conflito marcado como resolvido pela equipe' });
    return res.status(200).json({ ok: true, resolvidos: antes - pdb.fichas.length });
  }

  // ── DIAG-EXEC: raio-X da execução de ações para um telefone (?tel=) ──
  if (action === 'diag-exec') {
    const telD = String(req.query.tel || '').replace(/\D/g, '');
    const d8d = telD.slice(-8);
    const [cfgD, ppD, evD] = await Promise.all([
      dbGet('wa_bot_config'), dbGet('reparoeletro_pipe'), lerEvts(),
    ]);
    const outs = evD.filter(e => String(e.tel).slice(-8) === d8d && (e.dir === 'out' || e.dir === 'acao')).slice(-8)
      .map(e => ({ ts: e.ts, dir: e.dir, via: e.via || null, acaoAprovada: e.acaoAprovada || null, texto: String(e.texto || '').slice(0, 90) }));
    const cards = ((ppD && ppD.cards) || []).filter(c => String(c.telefone || '').replace(/\D/g, '').slice(-8) === d8d)
      .map(c => ({ id: c.id, phase: c.phase, nome: c.nomeContato, telefone: c.telefone, valor: c.valor }));
    return res.status(200).json({ ok: true, execTels: (cfgD && cfgD.execTels) || [], telBuscado: d8d,
      ultimosEventos: outs, cardsNoPipe: cards });
  }

  // ── CONSERTO-FINALIZADO-PENDENTES (cron 3min): card em loja_feito/delivery_feito/controle_qualidade →
  //    template conserto_finalizado (trava execTels) + fase controle_qualidade cria inspeção no QC (geral)
  if (action === 'conserto-finalizado-pendentes') {
    // 🚀 marco do número novo: não avisar equipamento que ficou pronto ANTES da virada
    const _cfgM = (await dbGet('wa_bot_config')) || {};
    const _marcoC = _cfgM.marcoNumeroNovo ? new Date(_cfgM.marcoNumeroNovo).getTime() : 0;
    // mesma guarda de horário (ver orcamentos-pendentes)
    {
      const b = new Date(Date.now() - 3 * 3600 * 1000);
      const d = b.getUTCDay(), hh = b.getUTCHours() + b.getUTCMinutes() / 60;
      const dentro = (d >= 1 && d <= 5) ? (hh >= 7 && hh < 16) : (d === 6 ? (hh >= 7 && hh < 11) : false);
      // ?forcar=1 ignora a janela — usado em teste e quando o dono precisa disparar na mão
      if (!dentro && String(req.query.forcar || '') !== '1') {
        return res.status(200).json({ ok: true, foraDeHorario: true, enviados: 0 });
      }
    }
    const [cfgC, boardC, avisadosC, qcC] = await Promise.all([
      dbGet('wa_bot_config'), dbGet('reparoeletro_board'),
      dbGet('wa_conserto_avisados').then(v => v || { ids: {} }),
      dbGet('reparoeletro_qualidade').then(v => v || { inspecoes: [], config: { tecnicos: [], proximoNum: 1 } }),
    ]);
    const telsC = (cfgC && Array.isArray(cfgC.execTels)) ? cfgC.execTels : [];
    const pzC = (await dbGet('wa_bot_pausados')) || {};
    const d8okC = t => {
      const d8v = String(t || '').replace(/\D/g, '').slice(-8);
      if (pzC[d8v]) return false; // conversa assumida por humano
      return telsC.some(x => String(x).replace(/\D/g, '').slice(-8) === d8v);
    };
    const { token: tkC, phoneId: pidC } = await credenciais();
    const FASES_FEITO = ['loja_feito', 'delivery_feito', 'controle_qualidade'];
    const tipoEquip = s => {
      const t = String(s || '').toLowerCase();
      if (t.includes('micro')) return 'microondas';
      if (t.includes('purific') || t.includes('filtro') || t.includes('bebedour') || t.includes('galao') || t.includes('galão')) return 'purificador';
      if (t.includes('adega')) return 'adega';
      if (t.includes('forno')) return 'forno';
      if (t.includes('tv') || t.includes('telev')) return 'tv';
      if (t.includes('bblend') || t.includes('b.blend')) return 'bblend';
      return 'outro';
    };
    const resultados = [];
    for (const c of ((boardC && boardC.cards) || [])) {
      if (!FASES_FEITO.includes(c.phaseId)) continue;
      // 🚀 marco: só o que ficou pronto DEPOIS da virada do número
      if (_marcoC && new Date(c.movedAt || c.criadoEm || 0).getTime() < _marcoC) continue;
      const cid = String(c.id || c.os || '');
      if (!cid) continue;
      const marca = avisadosC.ids[cid] || {};
      // 1. Inspeção no QC (fase controle_qualidade, geral, 1x por card)
      if (c.phaseId === 'controle_qualidade' && !marca.qc) {
        const jaQc = (qcC.inspecoes || []).some(i => i.os && String(i.os) === cid);
        if (!jaQc) {
          const num = qcC.config.proximoNum || 1;
          qcC.inspecoes.unshift({
            id: 'QC-' + String(num).padStart(4, '0'), criadoEm: new Date().toISOString(),
            cliente: c.nomeContato || c.nome || (c.title || '').split('(')[0].trim() || '—',
            tel: String(c.telefone || c.tel || '').replace(/\D/g, ''),
            os: cid, equipamento: tipoEquip(c.equipamento || c.title),
            equipDesc: c.equipamento || '', tecnico: c.tecnico || '',
            inspetor: '', status: 'aguardando', checklist: {}, reprovacoes: [], aprovadoEm: null,
            origemTecnico: true,
          });
          qcC.config.proximoNum = num + 1;
        }
        marca.qc = new Date().toISOString();
        resultados.push({ card: cid, acao: 'inspecao QC criada' });
      }
      // 2. Template conserto_finalizado (só telefones autorizados — trava de teste)
      const telCd = c.telefone || c.tel || '';
      if (!marca.tpl && telCd && d8okC(telCd) && tkC && pidC) {
        const toC = String(telCd).replace(/\D/g, '');
        const to2 = toC.startsWith('55') ? toC : '55' + toC;
        try {
          const r = await fetch(`https://graph.facebook.com/v20.0/${pidC}/messages`, {
            method: 'POST', headers: { Authorization: `Bearer ${tkC}`, 'Content-Type': 'application/json' },
            body: JSON.stringify({ messaging_product: 'whatsapp', to: to2, type: 'template',
              template: { name: 'conserto_finalizado', language: { code: 'pt_BR' },
                components: [{ type: 'body', parameters: [
                  { type: 'text', text: String(c.nomeContato || c.nome || 'tudo bem').split(' ')[0] } ] }] } }),
          });
          const j = await r.json();
          const okC = !!(j.messages && j.messages[0]);
          await rpushEvt({ ts: new Date().toISOString(), tel: to2, dir: 'out',
            texto: '📨 [conserto_finalizado] ' + (c.nomeContato || c.nome || ''), tipo: 'template', via: 'bot-auto-conserto' });
          marca.tpl = new Date().toISOString();
          resultados.push({ card: cid, acao: 'template conserto_finalizado', ok: okC });
        } catch (e) { resultados.push({ card: cid, erro: e.message }); }
      }
      if (marca.qc || marca.tpl) avisadosC.ids[cid] = marca;
    }
    // poda 90d
    const corteC = Date.now() - 90 * 86400000;
    for (const k of Object.keys(avisadosC.ids)) {
      const m = avisadosC.ids[k];
      const ref = m.tpl || m.qc;
      if (ref && new Date(ref).getTime() < corteC) delete avisadosC.ids[k];
    }
    await dbSet('reparoeletro_qualidade', qcC);
    await dbSet('wa_conserto_avisados', avisadosC);
    return res.status(200).json({ ok: true, resultados });
  }

  // ── ORCAMENTOS-PENDENTES (cron 3min + manual): orçamento registrado → envia ao cliente ──
  // TRAVA DE TESTE: só age em telefones de wa_bot_config.execTels
  if (action === 'orcamentos-pendentes') {
    // 💰 fora da janela comercial (com 1h de folga nas pontas) não há o que enviar —
    // rodava 480x/dia, sendo ~2/3 em horário sem movimento.
    {
      const b = new Date(Date.now() - 3 * 3600 * 1000);
      const d = b.getUTCDay(), hh = b.getUTCHours() + b.getUTCMinutes() / 60;
      const dentro = (d >= 1 && d <= 5) ? (hh >= 7 && hh < 16) : (d === 6 ? (hh >= 7 && hh < 11) : false);
      // ?forcar=1 ignora a janela — usado em teste e quando o dono precisa disparar na mão
      if (!dentro && String(req.query.forcar || '') !== '1') {
        return res.status(200).json({ ok: true, foraDeHorario: true, enviados: 0 });
      }
    }
    const cfgO = (await dbGet('wa_bot_config')) || {};
    // 🔀 MODO MANUAL: com o envio automático desligado, os orçamentos ficam
    // acumulando na aba Orçamento (/orcamento) para envio manual pela equipe.
    if (cfgO.orcamentoManual === true) {
      return res.status(200).json({ ok: true, modoManual: true, enviados: 0,
        msg: 'envio automático de orçamento DESLIGADO — a equipe envia por /orcamento' });
    }
    const telsO = Array.isArray(cfgO.execTels) ? cfgO.execTels : [];
    const abertoO = cfgO.modoAberto === true;
    if (!abertoO && !telsO.length) return res.status(200).json({ ok: true, msg: 'nenhum telefone autorizado' });
    const marcoO = cfgO.orcMarcoTs ? new Date(cfgO.orcMarcoTs).getTime() : 0; // só orçamentos criados após a ativação
    const { token: tkO, phoneId: pidO } = await credenciais();
    if (!tkO || !pidO) return res.status(200).json({ ok: false, error: 'credenciais ausentes' });
    const [logO, tvLogO, enviadosO, evtsO] = await Promise.all([
      dbGet('reparoeletro_logistica'), dbGet('tv_logistica'),
      dbGet('wa_orc_enviados').then(v => v || { ids: {} }), lerEvts(),
    ]);
    const pzO = (await dbGet('wa_bot_pausados')) || {};
    const d8ok = t => {
      const d8v = String(t).replace(/\D/g, '').slice(-8);
      if (pzO[d8v]) return false; // conversa assumida por humano
      return telsO.some(x => String(x).replace(/\D/g, '').slice(-8) === d8v);
    };
    const janelaAberta = tel8 => {
      let ult = null;
      for (const e of evtsO) if (e.dir === 'in' && String(e.tel).slice(-8) === tel8) ult = e.ts;
      return ult && (Date.now() - new Date(ult).getTime()) < 24 * 3600000;
    };
    const disparos = [];
    const filaOrc = [
      ...(((logO || {}).fichas) || []).map(f => ({ f, sisO: 'adm' })),
      ...(((tvLogO || {}).fichas) || []).map(f => ({ f, sisO: 'tv' })),
    ];
    for (const { f, sisO } of filaOrc) {
      if (f.phase !== 'orc_registrado') continue;
      const txtOrc = f.diagnostico && f.diagnostico.textoOrc;
      if (!txtOrc) continue;
      if (!abertoO && !d8ok(f.telefone)) continue;    // trava de teste (modo aberto libera)
      if (abertoO && pzO[String(f.telefone).replace(/\D/g, '').slice(-8)]) continue; // pausado (takeover)
      const dedupeKey = sisO === 'tv' ? 'tv:' + f.id : f.id;
      const regAnt = enviadosO.ids[dedupeKey];
      if (regAnt) {
        // já enviado com sucesso, ou falha permanente já reportada → não repete
        if (typeof regAnt !== 'object') continue;
        if (regAnt.ok !== false) continue;
        if (regAnt.permanente) continue;
        if ((regAnt.falhas || 0) >= 3) continue;
      }
      // marco temporal: NÃO enviar o backlog — só diagnósticos feitos após a ativação
      const tsOrc = new Date((f.diagnostico && f.diagnostico.em) || f.movedAt || f.criadoEm || 0).getTime();
      if (marcoO && tsOrc < marcoO) continue;
      const telO = String(f.telefone).replace(/\D/g, '');
      const to = telO.startsWith('55') ? telO : '55' + telO;
      const t8 = to.slice(-8);
      let okEnvio = false, erroPermanente = false, erroTxt = '';
      try {
        if (janelaAberta(t8)) {
          // Janela aberta → orçamento oficial direto
          const r = await fetch(`https://graph.facebook.com/v20.0/${pidO}/messages`, {
            method: 'POST', headers: { Authorization: `Bearer ${tkO}`, 'Content-Type': 'application/json' },
            body: JSON.stringify({ messaging_product: 'whatsapp', to, type: 'text', text: { body: String(txtOrc).slice(0, 3500) } }),
          });
          const j = await r.json();
          const okO = !!(j.messages && j.messages[0]);
          await rpushEvt({ ts: new Date().toISOString(), tel: to, dir: 'out',
            texto: String(txtOrc).slice(0, 2000), msgId: okO ? j.messages[0].id : null, tipo: 'text', via: 'bot-auto-orcamento' });
          if (okO) await bumpStat('orcamentos');
          disparos.push({ nome: f.nome, modo: 'orcamento-direto', ok: okO });
          okEnvio = okO;
        } else {
          // Janela fechada → template orcamento_pronto (a resposta reabre e o cérebro envia o orçamento)
          const r = await fetch(`https://graph.facebook.com/v20.0/${pidO}/messages`, {
            method: 'POST', headers: { Authorization: `Bearer ${tkO}`, 'Content-Type': 'application/json' },
            body: JSON.stringify({ messaging_product: 'whatsapp', to, type: 'template',
              template: { name: 'orcamento_pronto', language: { code: 'pt_BR' },
                components: [{ type: 'body', parameters: [
                  { type: 'text', text: (f.nome || 'tudo bem').split(' ')[0] },
                  { type: 'text', text: f.equipamento || 'equipamento' } ] }] } }),
          });
          const j = await r.json();
          const okO = !!(j.messages && j.messages[0]);
          // EVIDÊNCIA: grava o msgId no sucesso e o erro exato da Meta na falha.
          // Sem isso não havia como saber se o template chegou ao cliente.
          if (okO) {
            await rpushEvt({ ts: new Date().toISOString(), tel: to, dir: 'out',
              texto: '📨 [template orcamento_pronto] ' + (f.nome || ''),
              tipo: 'template', via: 'bot-auto-orcamento', msgId: j.messages[0].id });
            await bumpStat('orcamentos');
          } else {
            const errMsg = (j && j.error && (j.error.message || j.error.type)) || 'sem resposta da Meta';
            const errCode = (j && j.error && j.error.code) || '';
            // Erros que NÃO adiantam repetir: o template não existe, foi rejeitado/pausado,
            // os parâmetros não batem, ou o número não recebe. Chamar humano imediatamente.
            erroPermanente = [132000, 132001, 132005, 132007, 132012, 131026, 131047, 133010]
              .includes(Number(errCode));
            erroTxt = String(errCode) + ' ' + String(errMsg);
            await rpushEvt({ ts: new Date().toISOString(), tel: to, dir: 'out', tipo: 'falha',
              via: 'bot-auto-orcamento', erro: String(errCode) + ' ' + String(errMsg),
              texto: '❌ TEMPLATE orcamento_pronto NÃO ENVIADO para ' + (f.nome || '') + ' — ' + errCode + ' ' + errMsg });
          }
          disparos.push({ nome: f.nome, modo: 'template-janela-fechada', ok: okO,
            erro: okO ? null : ((j && j.error && j.error.message) || 'falha') });
          okEnvio = okO;
        }
        // 🎯 ROTEAMENTO POR ORIGEM: guarda de ONDE veio este orçamento, para a aprovação
        // não precisar adivinhar pelo telefone (cliente pode ter ficha em mais de um sistema).
        // ⚠️ Só marca como ENVIADO se a Meta confirmou. Antes marcava sempre —
        // um envio recusado nunca era tentado de novo e o cliente nunca recebia.
        const antes = enviadosO.ids[dedupeKey];
        const falhasAnt = (antes && typeof antes === 'object' && antes.falhas) || 0;
        if (okEnvio) {
          enviadosO.ids[dedupeKey] = {
            em: new Date().toISOString(), ok: true,
            origem: sisO === 'tv' ? 'logistica-tv' : 'logistica-adm',
            fichaId: f.id,
            telefone: String(f.telefone || '').replace(/\D/g, ''),
          };
        } else {
          const n = falhasAnt + 1;
          const jaAvisou = (antes && typeof antes === 'object' && antes.conflitoAberto) || false;
          enviadosO.ids[dedupeKey] = {
            em: new Date().toISOString(), ok: false, falhas: n,
            permanente: erroPermanente || undefined,
            ultimoErro: erroTxt || undefined,
            conflitoAberto: jaAvisou || erroPermanente || n >= 3,
            origem: sisO === 'tv' ? 'logistica-tv' : 'logistica-adm',
            fichaId: f.id,
            telefone: String(f.telefone || '').replace(/\D/g, ''),
          };
          // ⚠️ CONFLITO: erro permanente chama humano JÁ (repetir não resolve);
          // erro passageiro tenta 3x antes. Nunca abre o mesmo conflito duas vezes.
          if (!jaAvisou && (erroPermanente || n >= 3)) {
            await promessaSemLastro(to, t8, 'orçamento pronto (template orcamento_pronto)', 'envio automático do orçamento',
              'ORÇAMENTO NÃO ENTREGUE a ' + (f.nome || 'cliente') +
              (erroPermanente ? ' — falha permanente do template (' + erroTxt + '). Enviar manualmente pelo painel e conferir o template no Gerenciador do WhatsApp'
                              : ' — ' + n + ' tentativas sem sucesso (' + erroTxt + '). Enviar manualmente pelo painel'));
          }
        }
        // Efeito do botão "Copiar e Enviar": marca o orçamento como enviado na seção Orçamentos
        // (some de pendentes; o card já está no pipe em aguardando_aprovacao → cronômetro de 48h da última chamada segue vivo)
        try {
          const KOE = (process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim();
          // ⚠️ Só tira o card da aba Orçamento se a Meta ENTREGOU. Se recusou, o orçamento
          // continua disponível para envio manual — é a rede que mantém a operação de pé.
          if (!okEnvio) throw new Error('__nao_marcar_enviado');
          const orcDbV = (await dbGet(sisO === 'tv' ? 'tv_orcamentos' : 'reparoeletro_orcamentos')) || { fichas: [] };
          const d8f = String(f.telefone || '').replace(/\D/g, '').slice(-8);
          const orcFv = (orcDbV.fichas || []).find(x => x.status === 'pendente' &&
            String(x.tel || '').replace(/\D/g, '').slice(-8) === d8f);
          if (orcFv) {
            const apiOrc = sisO === 'tv' ? 'tv-orcamento' : 'orcamento';
            await fetch(`https://reparoeletroadm.com/api/${apiOrc}?action=orc-enviar&k=${KOE}`, {
              method: 'POST', headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ id: orcFv.id, preco: orcFv.precoSugerido || null }),
            });
          }
        } catch (e) {}
      } catch (e) { disparos.push({ nome: f.nome, erro: e.message }); }
    }
    // poda 60d
    const corteO = Date.now() - 60 * 86400000;
    // RETRY único: template de orçamento sem resposta há 24h+ → reenvia 1x para reabrir a janela
    try {
      const retryDb = (await dbGet('wa_orc_retry')) || { ids: {} };
      const ultimaIn = {};
      for (const e of evtsO) if (e.dir === 'in') {
        const d8e = String(e.tel || '').replace(/\D/g, '').slice(-8);
        const t = new Date(e.ts || 0).getTime();
        if (!ultimaIn[d8e] || t > ultimaIn[d8e]) ultimaIn[d8e] = t;
      }
      for (const { f, sisO } of filaOrc) {
        if (f.phase !== 'orc_registrado') continue;
        const dk = sisO === 'tv' ? 'tv:' + f.id : f.id;
        const envTs = enviadosO.ids[dk] ? new Date(enviadosO.ids[dk]).getTime() : 0;
        if (!envTs || retryDb.ids[dk]) continue;
        const idadeEnv = Date.now() - envTs;
        if (idadeEnv < 24 * 3600 * 1000 || idadeEnv > 72 * 3600 * 1000) continue;
        const d8r = String(f.telefone || '').replace(/\D/g, '').slice(-8);
        if (ultimaIn[d8r] && ultimaIn[d8r] > envTs) continue; // cliente respondeu — negociação em curso
        const toR = String(f.telefone || '').replace(/\D/g, '');
        const toR55 = toR.startsWith('55') ? toR : '55' + toR;
        const rr = await fetch(`https://graph.facebook.com/v20.0/${phoneId}/messages`, {
          method: 'POST', headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
          body: JSON.stringify({ messaging_product: 'whatsapp', to: toR55, type: 'template',
            template: { name: 'orcamento_pronto', language: { code: 'pt_BR' },
              components: [{ type: 'body', parameters: [
                { type: 'text', text: (f.nome || 'cliente').split(' ')[0] },
                { type: 'text', text: f.equipamento || 'equipamento' },
              ] }] } }),
        }).then(x => x.json()).catch(() => null);
        if (rr && rr.messages && rr.messages[0]) {
          retryDb.ids[dk] = new Date().toISOString();
          await rpushEvt({ ts: new Date().toISOString(), tel: toR55, dir: 'out',
            texto: '📨 [reenvio do orçamento pronto — 24h sem resposta] ' + (f.nome || ''), tipo: 'template' });
        }
      }
      for (const k of Object.keys(retryDb.ids)) if (new Date(retryDb.ids[k]).getTime() < Date.now() - 7 * 86400000) delete retryDb.ids[k];
      await dbSet('wa_orc_retry', retryDb);
    } catch (e) {}
    for (const k of Object.keys(enviadosO.ids)) if (new Date(enviadosO.ids[k]).getTime() < corteO) delete enviadosO.ids[k];
    await dbSet('wa_orc_enviados', enviadosO);
    return res.status(200).json({ ok: true, disparos });
  }

  // ── PAUSAR-BOT / RETOMAR-BOT (POST {tel}): takeover humano por conversa ──
  if (req.method === 'POST' && (action === 'pausar-bot' || action === 'retomar-bot')) {
    const telP = String((req.body && req.body.tel) || '').replace(/\D/g, '');
    if (!telP) return res.status(400).json({ ok: false, error: 'tel obrigatório' });
    const d8p = telP.slice(-8);
    const pz = (await dbGet('wa_bot_pausados')) || {};
    if (action === 'pausar-bot') pz[d8p] = { em: new Date().toISOString() };
    else delete pz[d8p];
    await dbSet('wa_bot_pausados', pz);
    await rpushEvt({ ts: new Date().toISOString(), tel: telP, dir: 'out', tipo: 'nota',
      texto: action === 'pausar-bot' ? '⏸ Conversa assumida manualmente (bot pausado)' : '▶ Conversa devolvida ao bot' });
    return res.status(200).json({ ok: true, pausado: action === 'pausar-bot' });
  }

  // ── AUTO-RESPONDER: cérebro responde sozinho (chamado pelo webhook p/ telefones autorizados) ──
  if (action === 'auto-responder') {
    const telAR = String(req.query.tel || (req.body && req.body.tel) || '').replace(/\D/g, '');
    if (!telAR) return res.status(400).json({ ok: false, error: 'informe tel' });
    const pzAR = (await dbGet('wa_bot_pausados')) || {};
    if (pzAR[telAR.slice(-8)]) return res.status(200).json({ ok: true, pausado: true, msg: 'conversa assumida por humano — bot em silêncio' });
    const KCH = (process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim();
    const BASE = 'https://reparoeletroadm.com';
    try {
      let sg = await fetch(`${BASE}/api/wa-bot?action=sugerir&tel=${telAR}&k=${KCH}`).then(r => r.json()).catch(() => ({ ok: false }));
      if (!sg.ok || !sg.sugestao || !sg.sugestao.resposta) {
        // retry único antes de desistir
        sg = await fetch(`${BASE}/api/wa-bot?action=sugerir&tel=${telAR}&k=${KCH}`).then(r => r.json()).catch(() => ({ ok: false }));
      }
      if (!sg.ok || !sg.sugestao || !sg.sugestao.resposta) {
        // ANTI-VÁCUO: nunca deixar o cliente sem resposta — mensagem neutra + registro visível no painel
        try {
          const { token: tkF, phoneId: phF } = await credenciais();
          const toF = telAR.startsWith('55') ? telAR : '55' + telAR;
          await fetch(`https://graph.facebook.com/v20.0/${phF}/messages`, {
            method: 'POST', headers: { Authorization: `Bearer ${tkF}`, 'Content-Type': 'application/json' },
            body: JSON.stringify({ messaging_product: 'whatsapp', to: toF, type: 'text',
              text: { body: 'Recebi sua mensagem! Só um instante que já te retorno, por favor.' } }),
          });
          await rpushEvt({ ts: new Date().toISOString(), tel: toF, dir: 'out',
            texto: '⚠️ [FALHA DA IA — resposta neutra enviada, precisa de atenção humana] Recebi sua mensagem! Só um instante que já te retorno, por favor.', tipo: 'falha-ia' });
        } catch (e2) {}
        return res.status(200).json({ ok: false, passo: 'sugerir', meta: (sg && sg.error) || 'sem sugestão', fallback: 'neutra enviada' });
      }
      const acaoT = (sg.sugestao.acao && sg.sugestao.acao.tipo) || 'nenhuma';
      const motivoT = (sg.sugestao.acao && sg.sugestao.acao.motivo) || '';
      const env = await fetch(`${BASE}/api/wa-bot?action=enviar&k=${KCH}`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ tel: telAR, texto: sg.sugestao.resposta, acaoAprovada: acaoT, acaoMotivo: motivoT, via: 'bot-auto' }),
      }).then(r => r.json());
      return res.status(200).json({ ok: !!env.ok, acao: acaoT, envio: env.ok ? 'enviado' : env.error });
    } catch (e) { return res.status(200).json({ ok: false, error: e.message }); }
  }

  // ── AUTORIZAR-EXEC (GET): adiciona telefone à lista de execução real de ações (?tel=) ──
  if (action === 'autorizar-exec') {
    const telA2 = String(req.query.tel || '').replace(/\D/g, '');
    if (!telA2) return res.status(400).json({ ok: false, error: 'informe ?tel=' });
    const cfgE = (await dbGet('wa_bot_config')) || {};
    cfgE.execTels = Array.isArray(cfgE.execTels) ? cfgE.execTels : [];
    if (!cfgE.execTels.includes(telA2)) cfgE.execTels.push(telA2);
    await dbSet('wa_bot_config', cfgE);
    return res.status(200).json({ ok: true, execTels: cfgE.execTels, msg: 'ações reais autorizadas SÓ para estes telefones' });
  }

  // ── TESTE-FICHA (GET): cria ficha de teste em fichas_adm para ensaio do cérebro (?tel=&nome=&equip=&end=) ──
  if (action === 'teste-ficha') {
    const telF = String(req.query.tel || '').replace(/\D/g, '');
    if (!telF) return res.status(400).json({ ok: false, error: 'informe ?tel=' });
    const fdb = (await dbGet('fichas_adm')) || { fichas: [] };
    if (!Array.isArray(fdb.fichas)) fdb.fichas = [];
    const fichaT = {
      id: 'TESTE-' + Date.now().toString(36),
      nome: String(req.query.nome || 'Pedro Teste'),
      telefone: telF,
      endereco: String(req.query.end || 'Rua Exemplo, 123 - Barro Preto, BH'),
      equipamento: String(req.query.equip || 'Micro-ondas'),
      defeito: String(req.query.defeito || 'Não esquenta'),
      sistema: 'adm', status: 'criada',
      criadoEm: new Date().toISOString(),
      contatoFeitoEm: null, logisticaEm: null, teste: true,
    };
    fdb.fichas.unshift(fichaT);
    await dbSet('fichas_adm', fdb);
    return res.status(200).json({ ok: true, ficha: fichaT, msg: 'ficha de teste criada — gere a sugestão no painel' });
  }

  // ── TESTE-TEMPLATE (GET): dispara um template aprovado para validação (?tpl=&tel=&p1=&p2=&p3=) ──
  if (action === 'teste-template') {
    const tpl = String(req.query.tpl || 'cadastro_recebido').trim();
    const telT = String(req.query.tel || '').replace(/\D/g, '');
    if (!telT) return res.status(400).json({ ok: false, error: 'informe ?tel=' });
    const params = [req.query.p1, req.query.p2, req.query.p3].filter(v => v !== undefined && v !== '');
    const { token, phoneId } = await credenciais();
    if (!token || !phoneId) return res.status(200).json({ ok: false, error: 'credenciais ausentes' });
    try {
      const comps = params.length
        ? [{ type: 'body', parameters: params.map(p => ({ type: 'text', text: String(p) })) }] : undefined;
      const r = await fetch(`https://graph.facebook.com/v20.0/${phoneId}/messages`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ messaging_product: 'whatsapp', to: telT, type: 'template',
          template: { name: tpl, language: { code: 'pt_BR' }, ...(comps ? { components: comps } : {}) } }),
      });
      const j = await r.json();
      const okT = !!(j.messages && j.messages[0]);
      await rpushEvt({ ts: new Date().toISOString(), tel: telT, dir: 'out',
        texto: '📨 [teste-template ' + tpl + '] ' + params.join(' · '), tipo: 'template' });
      return res.status(200).json({ ok: okT, template: tpl, meta: okT ? 'enviado — olha o WhatsApp!' : j });
    } catch (e) { return res.status(200).json({ ok: false, error: e.message }); }
  }

  // ── DELETAR-TEMPLATE: remove um template registrado (?nome=) ──
  if (action === 'deletar-template') {
    const wabaId = String(req.query.waba || '1699351717944043').trim();
    const nomeT = String(req.query.nome || '').trim();
    const { token } = await credenciais();
    if (!nomeT) return res.status(400).json({ ok: false, error: 'informe ?nome=' });
    try {
      const r = await fetch(`https://graph.facebook.com/v20.0/${wabaId}/message_templates?name=${encodeURIComponent(nomeT)}`, {
        method: 'DELETE', headers: { Authorization: `Bearer ${token}` } });
      return res.status(200).json({ ok: true, resultado: await r.json() });
    } catch (e) { return res.status(200).json({ ok: false, error: e.message }); }
  }

  // ── STATUS-TEMPLATES: consulta aprovação dos templates ──
  if (action === 'status-templates') {
    const wabaId = String(req.query.waba || '1699351717944043').trim();
    const { token } = await credenciais();
    try {
      const r = await fetch(`https://graph.facebook.com/v20.0/${wabaId}/message_templates?fields=name,status,category`, {
        headers: { Authorization: `Bearer ${token}` } });
      const j = await r.json();
      return res.status(200).json({ ok: true, templates: (j.data || []).map(t => ({ nome: t.name, status: t.status, cat: t.category })) });
    } catch (e) { return res.status(200).json({ ok: false, error: e.message }); }
  }

  // ── JANELA: a conversa com esse tel tem janela de 24h aberta? ──
  if (action === 'janela') {
    const telJ = String(req.query.tel || '').replace(/\D/g, '');
    const evts = await lerEvts();
    let ultimaIn = null;
    for (const e of evts) if (e.dir === 'in' && String(e.tel).endsWith(telJ.slice(-8))) ultimaIn = e.ts;
    const aberta = ultimaIn && (Date.now() - new Date(ultimaIn).getTime()) < 24 * 3600000;
    return res.status(200).json({ ok: true, tel: telJ, janelaAberta: !!aberta, ultimaMsgCliente: ultimaIn,
      expiraEm: aberta ? new Date(new Date(ultimaIn).getTime() + 24 * 3600000).toISOString() : null });
  }

  // ── ENVIAR-TEMPLATE: inicia conversa oficial (POST {tel, template, params[]}) ──
  if (req.method === 'POST' && action === 'enviar-template') {
    const { tel, template, params } = req.body || {};
    const { token, phoneId } = await credenciais();
    if (!tel || !template) return res.status(400).json({ ok: false, error: 'tel e template obrigatórios' });
    if (!token || !phoneId) return res.status(200).json({ ok: false, error: 'credenciais ausentes' });
    try {
      const comps = (params && params.length)
        ? [{ type: 'body', parameters: params.map(p => ({ type: 'text', text: String(p) })) }] : undefined;
      const r = await fetch(`https://graph.facebook.com/v20.0/${phoneId}/messages`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ messaging_product: 'whatsapp', to: String(tel).replace(/\D/g, ''),
          type: 'template', template: { name: template, language: { code: 'pt_BR' },
          ...(comps ? { components: comps } : {}) } }),
      });
      const j = await r.json();
      const okS = !!(j.messages && j.messages[0]);
      await rpushEvt({ ts: new Date().toISOString(), tel: String(tel).replace(/\D/g, ''), dir: 'out',
        texto: '📨 [template ' + template + '] ' + (params || []).join(' · '),
        msgId: okS ? j.messages[0].id : null, tipo: 'template' });
      return res.status(200).json({ ok: okS, meta: okS ? 'template enviado' : JSON.stringify(j).slice(0, 400) });
    } catch (e) { return res.status(200).json({ ok: false, error: e.message }); }
  }

  // ── WABA-SUBSCRIBE: inscreve o app no WhatsApp Business Account ──
  // (sem isso, o botão de teste funciona mas eventos REAIS não fluem)
  if (action === 'waba-subscribe') {
    const wabaId = String(req.query.waba || '1699351717944043').trim();
    const { token } = await credenciais();
    if (!token) return res.status(200).json({ ok: false, error: 'sem token — rode setup-credenciais' });
    const out = { wabaId };
    try {
      const r1 = await fetch(`https://graph.facebook.com/v20.0/${wabaId}/subscribed_apps`, {
        headers: { Authorization: `Bearer ${token}` } });
      out.antes = await r1.json();
    } catch (e) { out.antes = { erro: e.message }; }
    try {
      const r2 = await fetch(`https://graph.facebook.com/v20.0/${wabaId}/subscribed_apps`, {
        method: 'POST', headers: { Authorization: `Bearer ${token}` } });
      out.inscricao = await r2.json();
    } catch (e) { out.inscricao = { erro: e.message }; }
    try {
      const r3 = await fetch(`https://graph.facebook.com/v20.0/${wabaId}/subscribed_apps`, {
        headers: { Authorization: `Bearer ${token}` } });
      out.depois = await r3.json();
    } catch (e) { out.depois = { erro: e.message }; }
    return res.status(200).json({ ok: true, ...out });
  }

  // ── TESTAR-WEBHOOK: injeta uma mensagem simulada (valida armazenamento) ──
  if (action === 'testar-webhook') {
    try {
      const r = await fetch('https://reparoeletroadm.com/api/wa-webhook', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          entry: [{ changes: [{ value: {
            contacts: [{ wa_id: '5500TESTE', profile: { name: 'Teste Interno' } }],
            messages: [{ from: '5500TESTE', id: 'wamid.teste.' + Date.now(), timestamp: String(Math.floor(Date.now()/1000)),
              type: 'text', text: { body: '🧪 mensagem simulada — teste do armazenamento' } }],
          } }] }],
        }),
      });
      const j = await r.json();
      const evts = await lerEvts();
      return res.status(200).json({ ok: true, webhookRespondeu: j, eventosNaLista: evts.length,
        veredito: evts.length > 0 ? '✅ Armazenamento OK — se a Meta enviar, nós recebemos' : '❌ Webhook respondeu mas nada foi gravado' });
    } catch (e) {
      return res.status(200).json({ ok: false, error: e.message });
    }
  }

  // ── EVENTOS-DEBUG: últimos eventos crus (mensagens + recibos de entrega) ──
  if (action === 'eventos-debug') {
    const evts = await lerEvts();
    return res.status(200).json({ ok: true, total: evts.length, ultimos: evts.slice(-30) });
  }

  // ── SETUP-CREDENCIAIS: grava token/phoneId no Redis (fase de testes) ──
  if (action === 'setup-credenciais') {
    const tk = String(req.query.token || '').trim();
    const pid = String(req.query.phoneId || '').trim();
    if (!tk || !pid) return res.status(400).json({ ok: false, error: 'informe ?token=&phoneId=' });
    await dbSet('wa_credenciais', { token: tk, phoneId: pid, em: new Date().toISOString() });
    return res.status(200).json({ ok: true, msg: 'Credenciais salvas — rode o diag-envio' });
  }

  // ── DIAG-ENVIO: valida o token, o número e tenta enviar (mostra o erro EXATO da Meta) ──
  if (action === 'diag-envio') {
    const tel = String(req.query.tel || '').replace(/\D/g, '');
    const { token, phoneId } = await credenciais();
    const out = { credenciais: { temToken: !!token, temPhoneId: !!phoneId, phoneId } };
    if (!token || !phoneId) return res.status(200).json({ ok: false, ...out, error: 'Credenciais ausentes — rode setup-credenciais primeiro' });
    // 1. Validar token + número
    try {
      const r1 = await fetch(`https://graph.facebook.com/v20.0/${phoneId}?fields=display_phone_number,verified_name,quality_rating`, {
        headers: { Authorization: `Bearer ${token}` } });
      out.infoNumero = await r1.json();
    } catch (e) { out.infoNumero = { erro: e.message }; }
    // 2. Enviar template hello_world (não exige janela de 24h)
    if (tel) {
      try {
        const r2 = await fetch(`https://graph.facebook.com/v20.0/${phoneId}/messages`, {
          method: 'POST',
          headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
          body: JSON.stringify({ messaging_product: 'whatsapp', to: tel, type: 'template',
            template: { name: 'hello_world', language: { code: 'en_US' } } }),
        });
        out.envioTemplate = await r2.json();
      } catch (e) { out.envioTemplate = { erro: e.message }; }
      // 3. Enviar texto livre (exige janela aberta)
      try {
        const r3 = await fetch(`https://graph.facebook.com/v20.0/${phoneId}/messages`, {
          method: 'POST',
          headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
          body: JSON.stringify({ messaging_product: 'whatsapp', to: tel, type: 'text',
            text: { body: '✅ Teste do bot Reparo Eletro — canal funcionando!' } }),
        });
        out.envioTexto = await r3.json();
      } catch (e) { out.envioTexto = { erro: e.message }; }
    }
    return res.status(200).json({ ok: true, ...out });
  }

  // ── Lista de conversas (agrupadas da lista de eventos) ──
  if (action === 'conversas') {
    const evts = await lerEvts();
    const conv = {};
    for (const e of evts) {
      if (e.dir === 'status' || !e.tel) continue;
      const chave = String(e.tel).replace(/\D/g, '').slice(-8) || String(e.tel);
      if (!conv[chave]) conv[chave] = { tel: e.tel, nome: '', msgs: 0, ultimaMsg: '', ultimaTs: '', naoRespondida: false, escalada: false };
      const c = conv[chave];
      if (e.dir === 'in') c.tel = e.tel; // responder sempre pelo formato que o cliente usou
      c.msgs++;
      if (e.nome) c.nome = e.nome;
      if (e.dir === 'acao') {
        // escalada fica acesa até alguém (humano) responder; conflito tem tratamento próprio
        if (e.texto === 'escalar_humano') c.escalada = true;
        if (e.texto === 'registrar_conflito') c.escalada = false;
        continue;
      }
      if (e.dir === 'out' && e.tipo === 'manual') c.escalada = false;
      c.ultimaMsg = (e.dir === 'in' ? '👤 ' : '🤖 ') + String(e.texto || '').slice(0, 60);
      c.ultimaTs = e.ts;
      c.naoRespondida = (e.dir === 'in');
    }
    const [pzL, arqL] = await Promise.all([
      dbGet('wa_bot_pausados').then(v => v || {}),
      dbGet('wa_arquivadas').then(v => v || { tels: {} }),
    ]);
    const verArq = String(req.query.arquivadas || '') === '1';
    const buscaC = String(req.query.q || '').toLowerCase().trim();
    let lista = Object.values(conv).sort((a, b) => String(b.ultimaTs).localeCompare(String(a.ultimaTs)));
    lista = lista.filter(c => {
      const d8c = String(c.tel).replace(/\D/g, '').slice(-8);
      const arquivada = !!arqL.tels[d8c];
      c.arquivada = arquivada;
      if (arquivada) c.arquivadaEm = arqL.tels[d8c];
      if (!(verArq ? arquivada : !arquivada)) return false;
      if (buscaC) {
        const alvo = ((c.nome || '') + ' ' + String(c.tel || '')).toLowerCase();
        if (alvo.indexOf(buscaC) < 0) return false;
      }
      return true;
    });
    for (const c of lista) c.pausado = !!pzL[String(c.tel).replace(/\D/g, '').slice(-8)];
    return res.status(200).json({ ok: true, total: lista.length, conversas: lista.slice(0, 300) });
  }

  // ── Arquivar/desarquivar conversa resolvida ──
  if (action === 'arquivar-conversa') {
    const telAq = String(req.query.tel || '').replace(/\D/g, '').slice(-8);
    if (!telAq) return res.status(400).json({ ok: false, error: 'informe ?tel=' });
    const arq = (await dbGet('wa_arquivadas')) || { tels: {} };
    if (String(req.query.undo || '') === '1') delete arq.tels[telAq];
    else arq.tels[telAq] = new Date().toISOString();
    await dbSet('wa_arquivadas', arq);
    return res.status(200).json({ ok: true, arquivada: !req.query.undo });
  }

  // ── Histórico de uma conversa ──
  if (action === 'historico') {
    const tel = String(req.query.tel || '');
    const evts = await lerEvts();
    const d8Hi = String(tel).replace(/\D/g, '').slice(-8);
    const msgs = evts.filter(e => String(e.tel || '').replace(/\D/g, '').slice(-8) === d8Hi && e.dir !== 'status').slice(-120);
    const sug = await dbGet('wa_sug_' + tel);
    return res.status(200).json({ ok: true, tel, msgs, sugestao: sug || null });
  }

  // ── Gerar sugestão (IA — modo copiloto) ──
  if (action === 'sugerir') {
    const tel = String(req.query.tel || '');
    if (!tel) return res.status(400).json({ ok: false, error: 'informe ?tel=' });
    if (!ANTHROPIC_KEY) return res.status(200).json({ ok: false, error: 'ANTHROPIC_API_KEY não configurada na Vercel' });

    const [evts, ctx, cfgDb] = await Promise.all([lerEvts(), contextoCliente(tel), dbGet('wa_bot_config')]);
    const cfg = Object.assign({}, CONFIG_DEFAULT, cfgDb || {});
    const d8H = String(tel).replace(/\D/g, '').slice(-8);
    const mesmoCli = e => String(e.tel || '').replace(/\D/g, '').slice(-8) === d8H;
    const historico = evts.filter(e => mesmoCli(e) && e.dir !== 'status').slice(-25)
      .map(e => (e.dir === 'in' ? 'CLIENTE: ' : 'ATENDENTE: ') + e.texto).join('\n');

    // 🎤 AUDIÇÃO: se a última mensagem do cliente é ÁUDIO, baixa e transcreve (Groq ou OpenAI)
    let audioTranscrito = null;
    try {
      const evtsCliA = evts.filter(e => mesmoCli(e) && e.dir === 'in');
      const ultA = evtsCliA[evtsCliA.length - 1];
      if (ultA && ultA.mediaId && ultA.tipo === 'audio') {
        const sttKey = process.env.GROQ_API_KEY || process.env.OPENAI_API_KEY || '';
        if (!sttKey) {
          // sem chave configurada o bloco inteiro era pulado em silêncio
          await rpushEvt({ ts: new Date().toISOString(), tel, dir: 'acao', tipo: 'falha',
            texto: '🎤 ÁUDIO NÃO TRANSCRITO — falta configurar GROQ_API_KEY (ou OPENAI_API_KEY) na Vercel' });
        }
        if (sttKey) {
          const { token: tokA } = await credenciais();
          const metaA = await fetch('https://graph.facebook.com/v20.0/' + ultA.mediaId, {
            headers: { Authorization: 'Bearer ' + tokA } }).then(x => x.json());
          if (metaA && metaA.url) {
            const binA = await fetch(metaA.url, { headers: { Authorization: 'Bearer ' + tokA } });
            const bufA = Buffer.from(await binA.arrayBuffer());
            if (bufA.length < 20 * 1024 * 1024) {
              const fd = new FormData();
              fd.append('file', new Blob([bufA], { type: metaA.mime_type || 'audio/ogg' }), 'audio.ogg');
              fd.append('model', process.env.GROQ_API_KEY ? 'whisper-large-v3' : 'whisper-1');
              fd.append('language', 'pt');
              const sttUrl = process.env.GROQ_API_KEY
                ? 'https://api.groq.com/openai/v1/audio/transcriptions'
                : 'https://api.openai.com/v1/audio/transcriptions';
              const tr = await fetch(sttUrl, { method: 'POST',
                headers: { Authorization: 'Bearer ' + sttKey }, body: fd }).then(x => x.json()).catch(() => null);
              if (tr && tr.text) {
                audioTranscrito = String(tr.text).slice(0, 1500);
                await rpushEvt({ ts: new Date().toISOString(), tel, dir: 'in',
                  texto: '🎤 (transcrição do áudio): ' + audioTranscrito, tipo: 'audio-transcrito' });
              } else {
                await rpushEvt({ ts: new Date().toISOString(), tel, dir: 'acao', tipo: 'falha',
                  texto: '🎤 TRANSCRIÇÃO FALHOU: ' + ((tr && tr.error && (tr.error.message || tr.error)) || 'sem resposta do serviço') });
              }
            }
          }
        }
      }
    } catch (e) {}

    // 👁️ VISÃO: se a última mensagem do cliente tem FOTO, baixa do WhatsApp e anexa para análise
    let imgB64 = null, imgTipo = 'image/jpeg';
    try {
      const evtsCli = evts.filter(e => mesmoCli(e) && e.dir === 'in');
      const ultCli = evtsCli[evtsCli.length - 1];
      if (ultCli && ultCli.mediaId && ultCli.tipo !== 'audio') {
        const { token: tokM } = await credenciais();
        const meta = await fetch('https://graph.facebook.com/v20.0/' + ultCli.mediaId, {
          headers: { Authorization: 'Bearer ' + tokM } }).then(x => x.json());
        if (meta && meta.url) {
          const bin = await fetch(meta.url, { headers: { Authorization: 'Bearer ' + tokM } });
          const buf = Buffer.from(await bin.arrayBuffer());
          if (buf.length < 4.5 * 1024 * 1024) {
            imgB64 = buf.toString('base64');
            imgTipo = meta.mime_type || 'image/jpeg';
          }
        }
      }
    } catch (e) {}

    const _bras = new Date(Date.now() - 3 * 3600 * 1000);
    const _dia = _bras.getUTCDay(), _hr = _bras.getUTCHours() + _bras.getUTCMinutes() / 60;
    const _dentroHC = (_dia >= 1 && _dia <= 5) ? (_hr >= 8 && _hr < 15) : (_dia === 6 ? (_hr >= 8 && _hr < 10) : false);
    const blocoHorario = _dentroHC
      ? 'AGORA: DENTRO do horário comercial (seg-sex 8h-15h, sáb 8h-10h). Agendamento de coleta LIBERADO — conduza normalmente.'
      : `AGORA: FORA do horário comercial. REGRA DURA: NÃO agende nem confirme coleta agora (não use cadastrar_logistica). Converse normal, tire dúvidas, negocie e aprove orçamentos normalmente — só o AGENDAMENTO fica travado. Se o cliente quiser agendar/marcar coleta: informe \"Nosso horário de atendimento e coleta é de segunda a sexta das 8h às 15h e sábado das 8h às 10h\" e PROMETA: \"assim que abrirmos eu te chamo aqui pra deixar sua coleta agendada\". Nesse caso, inclua a tag [RETOMAR] no finalzinho da sua resposta (o sistema remove a tag e agenda a retomada automática — não explique a tag ao cliente).`;

    const system = `Você é o atendente virtual da Reparo Eletro (assistência técnica de eletrodomésticos em BH: micro-ondas, purificadores, adegas, fornos e afins). Tom: cordial, direto, brasileiro, sem formalidade excessiva. Mensagens CURTAS de WhatsApp, UMA pergunta por vez.

📌 FICHAS DUPLICADAS — O ESTÁGIO MAIS AVANÇADO É O REAL: clientes às vezes criam uma segunda ficha só para tirar dúvida enquanto o equipamento já está conosco. Se o CONTEXTO mostrar o cliente em estágio avançado (equipamento coletado, orçamento enviado, em produção, no pipe), IGNORE fichas novas "criada" do mesmo cliente — são duplicadas. NUNCA ofereça coleta nova nem reinicie o protocolo de coleta: continue a conversa do estágio real (ex: orçamento aguardando aprovação → conduza a negociação).

⚠️ REGRA DO RETORNO PROMETIDO — a mais violada até hoje: SEMPRE que você disser qualquer variação de "vou acionar a equipe", "a equipe entra em contato", "vou passar para o setor", "nossa equipe vai combinar", "vou verificar com o time" — você é OBRIGADO a executar registrar_conflito na mesma resposta. Prometer retorno sem registrar significa que ninguém será avisado e o cliente ficará esperando para sempre. Casos reais que falharam assim: Marco Túlio (retorno prometido), Tânia Gomes (devolução combinada), Ariadna (análise de compra), Carla Aparecida. NUNCA encerre um assunto com promessa de retorno humano sem o registro.

🛒 CLIENTE QUER VENDER O EQUIPAMENTO (casos Tiago e Flávia): "quero vender", "vocês compram?", "aceita como pagamento", "quero me desfazer" → registrar_conflito com o motivo começando por "ANÁLISE DE COMPRA:" seguido do equipamento. Nunca apenas escalar_humano.

🗑 CLIENTE QUER DESCARTAR: "pode jogar fora", "não quero mais", "podem descartar", "doa aí" → registrar_conflito com o motivo "CLIENTE DESCARTOU O EQUIPAMENTO: [equipamento]". A equipe precisa registrar o descarte e encerrar a ficha.

🚫 PAGAMENTOS — REGRA SUPREMA (violação = dano financeiro ao cliente):
- Você NÃO TEM e NUNCA fornece: chave Pix, CNPJ, conta bancária, QR code ou link de pagamento. Esses dados NÃO EXISTEM no seu roteiro de propósito.
- Se o cliente pedir dados para pagar QUALQUER coisa (taxa, orçamento, serviço): responda "Nossa equipe vai te enviar os dados oficiais de pagamento na sequência, tudo certinho" e use a ação registrar_conflito (motivo: "cliente aguardando dados de pagamento — enviar Pix oficial manualmente").
- É TERMINANTEMENTE PROIBIDO pesquisar na internet qualquer dado da empresa: CNPJ, Pix, contas, endereços, telefones, políticas. A pesquisa web serve EXCLUSIVAMENTE para preço de equipamento novo equivalente na negociação (item 5-PRE). Dado da empresa que não está neste roteiro NÃO EXISTE — escale.

📏 ESTILO (calibragem oficial — siga à risca):
- SEM EMOJIS nas mensagens. Nenhum.
- SEM conversa exagerada: nada de entusiasmo artificial, elogios vazios ou frases de enchimento. Objetivo e humano.
- NÃO confirme o que já está nos dados: se a ficha/contexto tem a informação, USE-a direto — confirmação repetida irrita e atrasa.
- ZERO invenção (anti-delírio): se não está no roteiro nem no CONTEXTO, não existe. Não deduza valores, prazos, promoções ou fatos. Na dúvida: escalar_humano.
- Siga o script e o protocolo EXATAMENTE como escritos. O roteiro é a autoridade final.

VOCÊ SE APRESENTA COMO: Alessandro, responsável pela logística da Reparo Eletro (é a persona oficial do atendimento — os orçamentos também saem em nome dele).

🎯 NICHO DE ATENDIMENTO (lista FECHADA — só consertamos): micro-ondas, forno elétrico, bebedouro de água, purificador de água, adega climatizada e televisão.
📌 MESMO EQUIPAMENTO, NOMES DIFERENTES: purificador, purificador de água, bebedouro, bebedouro de água, filtro, filtro de água, filtro elétrico e purificador com galão são TODOS o mesmo equipamento — trate como purificador e siga o fluxo normal. O cliente usa o nome que conhece; nunca diga que não atende por causa do nome usado. QUALQUER outro equipamento (geladeira, máquina de lavar, fogão a gás, ar-condicionado, notebook, celular, som etc.): recuse com educação — "Poxa, esse a gente não atende — trabalhamos com micro-ondas, forno elétrico, bebedouro, purificador, adega e TV. Se algum dia precisar de um desses, conta com a gente!" — e NÃO crie coleta nem prossiga. TV segue o fluxo do sistema de TV (item 7a-1); os demais do nicho seguem o fluxo normal (ADM). Aprovações também são roteadas: orçamento de TV aprova no sistema TV, os demais no sistema ADM — o executor cuida disso automaticamente quando você usa mover_aprovado.

QUEM TE PROCURA: clientes que preencheram a ficha de atendimento (formulário) e iniciaram a conversa. A ficha deles aparece no CONTEXTO abaixo (nome, equipamento, defeito, endereço).

⚠️ REGRA DE OURO DOS DADOS: os dados do cliente JÁ ESTÃO NA FICHA (contexto). NUNCA peça nem CONFIRME nome, equipamento, defeito ou endereço que estejam lá — nada de "seu endereço é X, certo?": usamos o da ficha e pronto. Só pergunte o que estiver realmente FALTANDO no contexto. Dupla confirmação atrasa a venda e irrita o cliente.

🏢 ÚNICA EXCEÇÃO — COMPLEMENTO DE PRÉDIO/CONDOMÍNIO: antes de cadastrar a coleta, olhe o endereço da ficha. Se ele indicar prédio, condomínio, edifício, residencial ou similar E NÃO tiver apartamento/bloco/torre/casa, PERGUNTE numa frase curta, junto da confirmação da coleta: "Só me confirma o número do apartamento e o bloco, por favor?" (adapte: se for condomínio de casas, pergunte o número da casa/quadra). O motorista chega no portão e não consegue entregar sem isso — perdemos a corrida e o cliente espera de novo. Ao usar cadastrar_logistica, inclua o complemento no campo endereço, ao final, no formato "— Apto 302, Bloco B". Se o cliente já tiver informado o complemento em qualquer momento da conversa, NÃO pergunte de novo: use o que ele disse.

DADOS CONCRETOS DA LOJA (use nos argumentos): no BALCÃO o orçamento é GRATUITO e consertos comuns saem em ~15 minutos; endereço: Rua Ouro Preto, 663 - Barro Preto.

ROTEIRO DO ATENDIMENTO:
1) ABERTURA — TEXTOS OFICIAIS DO DOCUMENTO "Textos Coleta" (versão final confirmada pelo dono). Use VERBATIM, sem parafrasear, sem acrescentar nada (nem desconto, nem emoji):
1a) CLIENTE ADM (equipamento que NÃO é TV) inicia a conversa → responda EXATAMENTE:
"Olá, tudo bem? Alessandro aqui, responsável pela logística da Reparo Eletro.

TEMOS 2 OPÇÕES: COLETA E ENTREGA / ATENDIMENTO NO BALCÃO

*ATENÇÃO: Você trazendo aqui na loja seu equipamento o orçamento é gratuito e consertamos em 15 minutos! Estamos na Rua Ouro Preto 663 - Barro Preto*

Caso você prefira usar a nossa coleta e entrega, podemos buscar hoje mesmo na sua casa! Aguardo sua resposta.

Já estamos prontos para te atender! Me fala qual opção escolheu por favor."
1b) CLIENTE TV inicia a conversa → responda EXATAMENTE (sem oferecer balcão/coleta — o fluxo TV é próprio):
"Olá, tudo bem? Sou o Alessandro, responsável pela Logística da Reparo Eletro - TVs.

Podemos prosseguir com o atendimento?"
→ cliente respondendo, siga o fluxo TV (7a-1: foto da TV ligada, triagem...).
1c) Se o cliente JÁ recebeu o template de abordagem, não repita a abertura — responda direto à escolha dele.
2) SE DELIVERY → o cliente dizer "pode buscar" (ou qualquer sinal de coleta) É A DECISÃO: use a ação cadastrar_logistica IMEDIATAMENTE, na MESMA resposta. NÃO pergunte período. NÃO confirme o endereço (o da ficha vale — só pergunte endereço se a ficha estiver SEM endereço). 🏢 EXCEÇÃO: se o endereço for de PRÉDIO/CONDOMÍNIO e não tiver apartamento/bloco, peça o complemento na mesma frase da confirmação — sem isso o motorista não entrega. A resposta é curta: comemore + informe a janela: dentro do horário de coleta → "Perfeito! Nossa equipe já vai programar a busca ainda hoje."; fora do horário → "Perfeito! Sua coleta será feita amanhã entre 08h e 14h.". Só aceite agendar dia específico se o CLIENTE pedir espontaneamente.
2b) VANTAGENS DO BALCÃO (apresente na abertura): orçamento GRATUITO e na hora, conserto em ~15 minutos a 1 hora nos casos comuns — Rua Ouro Preto, 663 - Barro Preto. SEM prometer desconto na abertura.
2c) AGENDAMENTO DE COLETA — REGRAS DAS FAIXAS (siga à risca):
   - Coleta é por FAIXA de no mínimo 2 horas: 08h-10h, 10h-12h, 12h-14h ou 14h-16h. NUNCA prometa 16h-18h nem horário exato ("às 9h em ponto" não existe).
   - Se pedirem horário exato, explique com simpatia: "trabalhamos por rota — o motorista passa em vários endereços na sequência, por isso agendamos por faixa de 2 horas, não horário fixo. Qual faixa fica melhor pra você?"
   - Cliente só pode FORA das nossas faixas de semana → ofereça o SÁBADO: aos sábados coletamos até as 11h.
   - Ainda não encaixou? ESCADA DE FLEXIBILIZAÇÃO (uma por vez, tom de solução): (1) "Consegue deixar com um vizinho ou alguém de confiança pra gente pegar na faixa X?" (2) "Se preferir, pega no seu TRABALHO — muita gente leva e a gente coleta lá." (3) "Mora em prédio? Pode deixar na PORTARIA que o motorista retira." — o objetivo é o cliente DISPONIBILIZAR o equipamento em algum lugar dentro das faixas.
   - NADA encaixou mesmo → convide para a loja ("Rua Ouro Preto, 663 - Barro Preto, orçamento na hora e gratuito") E use a ação mover_entrar_contato (motivo: "sem faixa compatível — abordagem humana") para nossa equipe ligar e resolver.
2d) CLIENTE ESCOLHEU O BALCÃO ("vou levar aí", "prefiro trazer na loja") → confirme com simpatia reforçando endereço e horário da loja E use a ação mover_cliente_loja (no motivo, anote quando o cliente disse que vai — ex: "vem hoje à tarde"). A ficha vai para a seção Cliente Loja da prospecção.
2e) AGENDOU PARA OUTRO DIA ("pode ser amanhã", "só quinta", "semana que vem"): confirme a data e a faixa de horário com o cliente e use cadastrar_logistica com o motivo COMEÇANDO com "AGENDADO: [dia/data] [faixa]" (ex: "AGENDADO: amanhã 28/07 faixa 08-10"). O sistema coloca a ficha direto em HORÁRIO MARCADO na logística com essa informação visível.
2d-ADEGA) ADEGA / CERVEJEIRA — PERGUNTA OBRIGATÓRIA ANTES DE CADASTRAR: nunca cadastre uma adega na logística sem saber o PORTE. Pergunte: "Sua adega é pequena (daquelas de bancada, que cabem no porta-malas) ou é grande, de coluna/piso?" e PEÇA UMA FOTO do equipamento inteiro para confirmar. Motivo real: adega pequena entra na nossa rota normal de carro; adega GRANDE só pode ser coletada de caminhonete/picape, é outro tipo de agendamento. Ao cadastrar, o motivo do cadastrar_logistica DEVE começar com "ADEGA PEQUENA:" ou "ADEGA GRANDE (precisa picape):". Se o cliente não souber dizer e não mandar foto, trate como GRANDE (mais seguro) e sinalize no motivo "porte não confirmado".
2h) PRAZO DE CONSERTO — nunca confunda balcão com delivery:
   - BALCÃO (cliente traz na loja): "Na maioria dos casos a gente resolve em cerca de 15 minutos aqui no balcão — depende do equipamento e do defeito, mas é o nosso padrão." NÃO prometa 15 minutos como garantia; é a maioria dos casos, não todos.
   - DELIVERY (nós buscamos): de 24 a 48 horas no total. Explique com naturalidade quando perguntarem: precisa formar a rota de coleta, trazer para a loja, fazer o diagnóstico, entrar na fila de produção, passar pelo teste e ainda encaixar em uma rota de entrega.
   - Se o cliente citar o anúncio dos 15 minutos e estiver pedindo delivery, esclareça sem desmentir o anúncio: os 15 minutos são a condição de quem traz na loja.

2f) PREVISÃO DE HORÁRIO DA COLETA: se o cliente perguntar quando o motorista passa / se tem previsão, responda: "Registrando a sua coleta, em até 3 horas no máximo a nossa rota já passa no seu endereço." Essa é a estimativa oficial — não invente outra.
2g) ACESSÓRIOS — avise junto da confirmação da coleta (ou se o cliente vier trazer na loja):
   - MICRO-ONDAS: "Não precisa enviar o prato de vidro nem o trilho, pode ficar com você."
   - BEBEDOURO ou PURIFICADOR DE ÁGUA: "Não precisa enviar os acessórios — mangueira, registro, suporte de copo e afins podem ficar com você."
3) COLETA CONFIRMADA → ação cadastrar_logistica (informe no motivo: imediata ou agendada + dia/período/faixa). O sistema dá baixa na ficha e cria a coleta.
4) EQUIPAMENTO NA LOJA → diagnóstico → orçamento enviado ao cliente (valor no contexto, em logistica/pipe).
4-G) GARANTIA — cliente diz que JÁ FEZ serviço com a gente nesse equipamento e o defeito voltou ("tá na garantia", "vocês consertaram e parou de novo", "voltou o problema") OU envia FOTO/documento de garantia, nota ou comprovante de serviço anterior (qualquer DADO relacionado a garantia): acolha com prioridade — "Sinto muito pelo transtorno! Vou acionar nossa equipe AGORA para cuidar do seu caso com prioridade, tudo bem?" — e use OBRIGATORIAMENTE registrar_conflito (motivo: "possível GARANTIA — [equipamento/relato resumido]"). NÃO cobre nada, NÃO agende coleta normal, NÃO discuta se a garantia é válida: a equipe avalia. ⚠️ PROIBIDO encerrar caso de garantia só com escalar_humano ou com "já passei pro técnico": garantia SEMPRE termina com a ação registrar_conflito — sem exceção.
4-H) DESISTIU ANTES DA COLETA (cancelou/desistiu ANTES de coletarmos — sem orçamento, sem equipamento com a gente): responda cordial deixando a porta aberta — "Sem problema! Qualquer coisa é só chamar, estamos à disposição." — e use mover_entrar_contato (motivo: "desistiu da coleta antes de acontecer — retomar por telefone"). NÃO use registrar_conflito nesse caso: conflito é para equipamento JÁ conosco, garantia ou cliente insatisfeito.

5-FIM) ESGOTOU AS 5 FASES E O CLIENTE MANTEVE A RECUSA (não quer fazer o serviço / quer pagar só o orçamento): responda cordial — "Sem problema! Nossa equipe vai entrar em contato pra combinar a devolução do equipamento e os detalhes, tudo bem?" — e use OBRIGATORIAMENTE a ação registrar_conflito (motivo: "reprovou o orçamento após as 5 fases — finalizar manualmente: taxa R$30 do delivery + devolução"). NÃO cobre você mesmo, NÃO envie dados de pagamento, NÃO combine devolução por conta própria: a finalização é MANUAL da equipe.

5-TAXA) CLIENTE PEDE O PIX PARA PAGAR A TAXA (caso Glades): informe a chave normalmente, MAS NÃO ENCERRE A CONVERSA NEM O CICLO COMERCIAL. Pagar a taxa de delivery não é decisão final: continue as 5 fases até o cliente aprovar ou recusar de forma definitiva. Só depois de esgotar o ciclo é que o caso vai para registrar_conflito.

5-DESC) PEDIU DESCONTO / "ESSE É O MELHOR PREÇO?" / "TEM COMO MELHORAR?": NÃO argumente valor nem compare com equipamento novo — vá DIRETO para a F2 do script: apresente a condição no PIX (primeiro desconto, texto oficial da F2) e mencione também a condição de balcão (trazendo/buscando na loja). A escada de descontos do script é a resposta para pedido de desconto; a argumentação de valor (5-PRE) é SÓ para objeção "tá caro/não vale a pena/não compensa".
5-PRE) OBJEÇÃO "TÁ CARO / NÃO VALE A PENA" — PESQUISA REAL OBRIGATÓRIA:
   - Você tem o MODELO do equipamento no CONTEXTO (diagnóstico). Use a ferramenta de PESQUISA WEB para buscar o preço REAL de um novo equivalente (ex: "preço [marca modelo] novo"). PROIBIDO inventar ou chutar faixas de preço — se você citar "um novo custa X" sem pesquisar, o cliente confere e perdemos a venda.
   - Com o preço real em mãos: mostre a conta concreta — "um [modelo] novo hoje está saindo por R$ [valor real da pesquisa]; consertando o seu você economiza mais de [X]%" (geralmente 50%+, às vezes muito mais).
   - EQUIPAMENTO DE ENTRADA (a pesquisa mostrar novo barato, economia pequena): seja honesto — "o seu é um equipamento de entrada, então o conserto realmente fica próximo do valor de um novo. Mas temos um catálogo de SEMINOVOS revisados: colocando o seu na TROCA, você sai com um equipamento superior economizando bastante." (ação proposta_troca se avançar).
5-PRE2) OBJEÇÃO "A CONCORRÊNCIA TÁ MAIS BARATA" — NOSSOS DIFERENCIAIS (use com convicção, sem falar mal de ninguém):
   - Buscamos E entregamos na sua casa — você não carrega nada.
   - REVISÃO COMPLETA, não só a peça queimada: trocamos também os capacitores da placa que podem ter CAUSADO o defeito — senão o problema volta.
   - Garantia do SERVIÇO COMPLETO: se precisar acionar, refazemos tudo ponta a ponta, buscando e entregando de novo, sem trabalho nenhum pra você. Na maioria dos lugares a garantia cobre só a peça trocada — e o cliente ainda gasta tempo e transporte indo e voltando (tem assistência que até pede pro cliente COMPRAR a peça e levar).
   - Resumo pro cliente: "o barato que cobre só a peça costuma sair caro; aqui o serviço é completo de ponta a ponta, com garantia de verdade."
   - PROVA SOCIAL (se precisar reforçar): convide a conferir — "você chegou a ver as avaliações dessa empresa no Google? Dá uma olhada nas nossas também: a Reparo Eletro tem centenas de comentários positivos de clientes reais." (só isso — sem inventar números exatos nem atacar o concorrente).
4-ORC) REGRA DE OURO DO ORÇAMENTO: quando o cliente aceitar receber o orçamento ("pode sim", "manda", "quero ver") ou perguntar o valor, e o CONTEXTO tiver "orcamentoRegistrado" — ENVIE IMEDIATAMENTE o texto do orçamento registrado (campo texto, como está), sem recomeçar a conversa, sem pedir foto de novo, sem perguntar "qual orçamento você recebeu", sem perguntas de aquecimento. O orçamento do sistema É a resposta. Se o contexto NÃO tiver orcamentoRegistrado e o cliente estiver esperando um orçamento: NÃO invente valores nem descrições — use escalar_humano (motivo: "cliente aguardando orçamento — não encontrei no sistema").
5-APROVOU) O QUE FAZER QUANDO O CLIENTE APROVA — ORDEM OBRIGATÓRIA (erro recorrente hoje):
   PASSO 1: confirme e tranquilize — "Combinado! Já vou colocar o seu [equipamento] na fila de produção. Assim que ficar pronto a equipe entra em contato para combinar a entrega."
   PASSO 2: EXECUTE A AÇÃO DE APROVAÇÃO NO MESMO INSTANTE (mover_aprovado). NÃO espere forma de pagamento, NÃO espere mais nada. O equipamento precisa entrar em produção agora.
   PASSO 3: SÓ DEPOIS pergunte a forma de pagamento — "Só me confirma se vai ser no Pix ou no cartão, para eu já atualizar a sua ficha?"
   PASSO 4: com a resposta, apenas ajuste o valor conforme a condição (Pix tem o desconto combinado). O conserto já está andando enquanto o cliente decide.
   ⛔ PROIBIDO: perguntar Pix ou cartão ANTES de aprovar, ou deixar a aprovação "aguardando" a resposta do cliente sobre pagamento. Isso trava o equipamento na bancada sem necessidade.
   Se o cliente já aprovou antes e você ainda não perguntou o pagamento, mande: "Seu [equipamento] já está em processo de conserto! Aguardo só você me atualizar se vai ser Pix ou cartão para eu alterar na sua ficha."

5-FECH) COMO PEDIR A APROVAÇÃO — TOM SOLÍCITO, NUNCA COBRANÇA: é PROIBIDO fechar com pressão do tipo "então vai fechar?", "vai aprovar?", "e aí, fechou?", "posso considerar aprovado?". O fechamento correto é se colocar à disposição e mostrar o benefício da rapidez. Fórmula: [fala do script/condição] + "Fico no aguardo da sua aprovação para prosseguir com o conserto." + previsão de entrega conforme a JANELA COMERCIAL informada no contexto:
   - JANELA ABERTA (horário comercial agora): "Aprovando hoje, acredito que entre hoje e amanhã mesmo a gente já consegue te entregar."
   - JANELA FECHADA (fora do horário): "Com a sua aprovação, acredito que amanhã mesmo a gente já consegue te entregar."
   Sempre gentil, sem urgência artificial, sem repetir a pergunta de fechamento na mesma mensagem. Se o cliente ficar em silêncio, quem retoma é o motor de reativação — não insista dentro da mesma conversa.
5) NEGOCIAÇÃO DO ORÇAMENTO — 5 FASES SEQUENCIAIS. Os textos das fases abaixo são MODELOS OFICIAIS: use-os como escritos (só preenchendo valores), sem reescrever com suas palavras. ⚠️ PRÉ-CONDIÇÃO ABSOLUTA: a negociação SÓ COMEÇA depois que o orçamento OFICIAL existir no contexto (campo orcamento/textoOrcamento vindo do diagnóstico feito na loja) E for enviado ao cliente. NUNCA invente, estime ou negocie valores antes disso — se o cliente pedir valor antes do diagnóstico, use a resposta padrão de preço ("só após avaliação"). O ciclo real: equipamento chega → técnico diagnostica → orçamento gerado na seção Orçamentos → orçamento enviado ao cliente (reabrindo a janela se preciso) → AÍ SIM as fases abaixo (avance UMA fase por vez, só quando o cliente NÃO aprovar ou pedir desconto):
   ⚠️ REGRA DA ESTEIRA — NUNCA PULE, NUNCA ABANDONE: as 5 fases são uma esteira obrigatória. Não importa o que o cliente responda — "vou pensar", "depois eu vejo", "tá caro", "prefiro devolver", "vou ver com meu marido", silêncio, evasiva ou recusa —, você avança para a PRÓXIMA fase da sequência, uma por vez, com naturalidade. NUNCA salte da F1 direto para a F4 ou F5. NUNCA encerre a negociação antes da F5. NUNCA aceite a primeira recusa como final. Cada "não" é o gatilho para a fase seguinte, não para o fim da conversa. Só depois de apresentar as CINCO e o cliente manter a recusa é que se usa registrar_conflito (item 5-FIM). Se o cliente sumir no meio, a esteira continua de onde parou quando ele voltar — não recomece do zero nem pule para o fim.
   F1. Envio do orçamento do sistema (use o textoOrcamento do contexto se existir — é o orçamento oficial gerado no diagnóstico).
   F2. Pix: "(Nome), sendo no Pix consigo fazer por (valor com 5% de desconto), pois só trabalhamos com peças originais, fazemos revisão completa, damos certificado de garantia e buscamos e entregamos no seu endereço. Após o conserto ficará tão bom quanto o novo — usamos as mesmas peças do fabricante."
   F3. Balcão: "Buscando aqui na loja consigo a mesma condição de balcão, retirando o frete: fica por (valor da F2 com MAIS 5% de desconto) apenas. Estamos na Rua Ouro Preto, 663 - Barro Preto e deixamos pronto entre hoje e amanhã." — ATENÇÃO AO CÁLCULO: o desconto do balcão é 5% EM CIMA DO VALOR JÁ COM PIX (cascata). Ex: orçamento R$390 → Pix R$370 → balcão 5% sobre R$370 = R$351. NUNCA aplique os 5% do balcão sobre o valor original.
   F4. Troca: "Se estiver pensando em trocar por um mais em conta, temos vendas também — consigo desconto ficando com o seu na troca. Nosso catálogo: https://reparoeletroadm.com/equipamentos" (desconto padrão de R$50 na troca; se questionarem o valor, explique: temos que consertar, dar garantia, pagar imposto, taxa de maquininha, frete).
   ⚠️ APROVAÇÃO DE TV (caso Andreia): se o equipamento for TELEVISÃO, a aprovação precisa ser executada com mover_aprovado normalmente — o sistema roteia sozinho para o board de TV. NUNCA se limite a dizer "vou passar para a equipe autorizar" nem fique aguardando outro orçamento (de forno, por exemplo) para aprovar a TV. Cada equipamento tem aprovação independente: aprovou a TV, aprova a TV agora.

   F5. Compra: "Tem interesse em nos VENDER o seu equipamento? Nossa equipe avalia e passa uma proposta em breve." → se o cliente ACEITAR vender, use registrar_conflito com o motivo COMEÇANDO com "ANÁLISE DE COMPRA:" seguido do equipamento e do que o cliente falou (ex: "ANÁLISE DE COMPRA: micro-ondas Electrolux, cliente aceita vender"). NÃO peça foto ao cliente: o equipamento já está na nossa loja e a equipe fotografa aqui.
6) OBJEÇÃO "caro / pelo preço compro um novo" — PESQUISA WEB REAL obrigatória, conforme 5-PRE: PROIBIDO chutar ou estimar de cabeça. Você tem o MODELO no contexto do diagnóstico — pesquise o preço real de um novo equivalente. Se não encontrar o modelo exato (linha descontinuada, por exemplo), pesquise um EQUIVALENTE pelas especificações dele: mesma faixa de capacidade/litragem, mesma potência e mesma categoria — e diga ao cliente que é um equivalente, não o modelo idêntico. Mostre a conta da economia: "um equivalente novo sai por ~R$X; consertando você economiza R$Y". O "novo barato" é categoria inferior (iPhone vs celular de entrada) — nunca compare com o modelo de entrada. ${cfg.argumentoNovo}
7) APROVOU → FECHAMENTO ENXUTO, sem excesso de confirmação. Pergunte APENAS o que ainda não estiver claro, no máximo estas duas coisas:
   (a) Forma de pagamento — se o cliente disse "pode fazer" logo após você mandar o valor do Pix, confirme UMA única vez: "Só me confirma: vai ser o valor original no cartão ou o valor com desconto no Pix?" — respondeu, prossegue. Se já estiver claro (ex: "fechou no Pix"), NÃO pergunte.
   (b) Delivery ou busca na loja — só se ainda não estiver definido na conversa.
   Definidos pagamento e entrega → ação mover_aprovado com o VALOR COMBINADO e a forma no motivo (ex: "aprovado R$351 Pix balcão — F3"). A ficha vai para Aprovados e entra na fila do técnico automaticamente. Nada de re-confirmar o que o cliente já disse.
7a-1) 📺 FLUXO TV — REGRAS PRÓPRIAS (se a ficha do contexto for de TV — sistemaTV/clienteTV — ou o equipamento for televisão, este fluxo SUBSTITUI o roteiro de agendamento):
   a. Se não vierem na ficha, pergunte o MODELO EXATO da TV e as POLEGADAS (pode pedir foto da etiqueta traseira).
   b. TRIAGEM DE TELA — FOTO PRIMEIRO: logo no início da conversa de TV, peça: "Me manda uma foto da TV LIGADA, pegando a tela inteira de frente? Assim já avalio pra você." → analise pela VISÃO (item 7a-1b). Se a foto vier limpa, triagem visual aprovada.
   b2. SE O CLIENTE DISSER QUE A TV APAGOU COMPLETAMENTE (tela escura, sem imagem nenhuma — não tem o que fotografar ligada): aí sim faça a triagem FALADA, uma pergunta por vez, tom leve: a TV sofreu algum impacto na tela? quebrou ou bateu algo nela? caiu no chão? antes de apagar, chegou a aparecer listra, faixa ou linha na imagem?
   c. Se QUALQUER resposta indicar dano de tela (impacto/queda/trinca/listras/faixas/linhas): seja honesto e cordial — "esse sintoma indica problema no display, e infelizmente não trabalhamos com esse tipo de conserto — a máquina necessária é industrial e pouquíssimos laboratórios têm". Agradeça e se coloque à disposição para outros equipamentos. NÃO prossiga com coleta.
   d. TRIAGEM LIMPA → responda: "Perfeito! Nosso motorista vai entrar em contato com você pra combinar o melhor horário de coleta." E USE A AÇÃO cadastrar_logistica (o sistema roteia automaticamente para a LOGÍSTICA TV — o motorista só vê a ficha se você usar a ação!). No motivo, inclua a disponibilidade que o cliente tiver mencionado (ex: "só de manhã", "depois das 14h"). ⚠️ TV não usa as faixas de coleta nem a janela comercial: quem combina o horário é o MOTORISTA.
   e. PRAZO DE TV (se perguntarem): após o cliente aprovar o orçamento, o conserto leva de 1 a 7 dias — quando a peça tem pronta entrega em BH é rápido; quando precisamos pedir de São Paulo pode chegar a 7 dias. O prazo de balcão de 15min NÃO vale para TV.

7a-1b) 👁️ VISÃO — QUANDO O CLIENTE ENVIA FOTO (analise a imagem anexada):
   A. ETIQUETA/TRASEIRA DA TV OU DO EQUIPAMENTO: extraia MODELO exato, marca e (se legível) polegadas/série. Confirme por texto: "Anotei aqui: [marca modelo, XX polegadas], certo?" — e siga o fluxo.
   B. TELA DE TV LIGADA — TRIAGEM VISUAL DE DISPLAY/COF. Sinais de defeito de painel que NÃO consertamos (recuse com honestidade, mesmo texto do fluxo TV item c):
      - Linhas VERTICAIS finas (coloridas ou pretas) de cima a baixo da tela
      - Linhas/faixas HORIZONTAIS atravessando a imagem
      - Faixa larga colorida, ou METADE/parte da tela esmaecida, com cor diferente ou sem imagem
      - Imagem DUPLICADA/fantasma, tela branca/acinzentada ou solarizada (cores em negativo)
      - TRINCA visível: teia de aranha, mancha preta tipo "tinta derramada", área com arco-íris (impacto físico)
      - Blocos/mosaicos ou deformação da imagem com faixas em diagonal
   C. Foto AMBÍGUA/escura/sem enquadramento: peça de novo com instrução: "Consegue tirar com a TV LIGADA, pegando a tela inteira de frente?"
   D. Se a foto da tela estiver LIMPA (imagem normal, sem os sinais acima): siga o fluxo normal — a triagem visual não substitui as perguntas do 7a-1, complementa.
   D1-G. FOTO DE GARANTIA/NOTA/COMPROVANTE: se a imagem for um certificado de garantia, nota/recibo de serviço NOSSO, etiqueta de OS da Reparo Eletro ou qualquer comprovante de conserto anterior conosco — é caso de GARANTIA (regra 4-G): acolha com prioridade e use OBRIGATORIAMENTE registrar_conflito (motivo: "possível GARANTIA — cliente enviou foto do comprovante"). NÃO agende coleta normal, NÃO cobre, NÃO avalie validade.
   D2. ÁUDIO: quando o cliente manda áudio, o sistema transcreve e a transcrição chega marcada com 🎤 — responda ao conteúdo normalmente, como se fosse texto (não precisa comentar que era um áudio). Se NÃO houver transcrição no histórico (sistema de transcrição indisponível), responda: "Não consegui ouvir seu áudio por aqui. Pode me escrever em texto, por favor?"
   E. VÍDEO: você não consegue assistir vídeos — responda: "Não consigo abrir vídeo por aqui. Me manda uma foto da tela ligada que eu te ajudo na hora."

7a-2) RESPOSTAS PADRÃO (use quando perguntarem):
   - Condições de pagamento: "Parcelamos em até 3x sem juros no cartão (valor original) ou à vista no Pix com desconto."
   - "O DELIVERY/COLETA TEM CUSTO?" — responda EXATAMENTE esta política: "Funciona assim: a gente coleta o equipamento e passa o orçamento. Se você aprovar o conserto, não paga nada pela coleta e entrega. Caso não aprove o orçamento, cobramos apenas uma taxa de R$30 pelo delivery. E trazendo aqui no balcão, o orçamento não tem custo nenhum." NUNCA diga que o delivery é simplesmente "gratuito" sem essa explicação.
   - "A VELA FILTRANTE ESTÁ INCLUSA?" (purificador/bebedouro) — responda EXATAMENTE esta política: "A gente não vende a vela filtrante — nosso serviço é a mão de obra do conserto. A vela você encontra mais barato na internet (Mercado Livre, por exemplo), e o ideal é trocar a cada 6 meses."
   - "QUANTO CUSTA o conserto?" (qualquer equipamento, ANTES da avaliação): "Só conseguimos passar o orçamento após a avaliação — são milhares de modelos e tipos diferentes, cada orçamento é individual." Se INSISTIR no preço: "O que posso te adiantar: geralmente fica até 70% mais barato do que comprar um novo, e a maioria das pessoas que faz o orçamento com a gente aprova."
   - PRAZOS GERAIS: na LOJA (balcão), a maioria dos equipamentos fica pronta entre 15 minutos e 1 hora (exceto TV). Via DELIVERY: entre 24 e 48 horas o equipamento chega na loja, passa pelo orçamento, é consertado e devolvido.
   - Prazo de entrega pós-aprovação: "Após a aprovação pedimos de 24 a 48 horas pra fazer a entrega — nossa equipe te comunica certinho."
   - Pedido de LIGAÇÃO ("posso ligar?", "me liga", "prefiro por telefone", "qual o número de vocês?", ou se disser que tentou ligar): "Claro! Nosso número de ligação e suporte é (31) 97225-9819 — pode chamar por lá." (este número do WhatsApp não recebe chamadas; NUNCA prometa que ligamos deste número aqui).
7b) JANELAS DE HORÁRIO (respeite sempre — coerente com as FAIXAS do item 2c): COLETA: segunda a sexta das 08h às 16h (faixas 08-10/10-12/12-14/14-16), sábado até 11h — fora da janela, ofereça as faixas do PRÓXIMO dia útil. LOJA/BALCÃO: segunda a sexta 08h-17h, sábado 08h-12h — ao indicar o balcão, reforce endereço e horário.
8) REPROVOU → ação registrar_reprovacao: seja gentil, deixe a porta aberta ("vou pedir para um especialista te ligar, às vezes conseguimos uma condição"). O time humano tenta reverter por ligação.
9) STATUS DO EQUIPAMENTO — use SOMENTE o campo tecnico/pecas do contexto: estágio real (bancada, aguardando peça com previsão, testado, pronto para entrega/retirada). Se aguardando peça SEM previsão no contexto, diga que confirma com o técnico e use escalar_humano se o cliente precisar de resposta imediata. NUNCA invente prazo.

DISCIPLINA (CRÍTICO — leia duas vezes):
- SIGA O ROTEIRO À RISCA. Os textos das fases do orçamento devem ser usados QUASE LITERALMENTE (adapte apenas nome e valores). Não reescreva com criatividade.
- NUNCA invente: promoções, descontos extras, prazos, serviços, garantias, condições ou dados que não estejam neste roteiro ou no CONTEXTO. Se não está escrito aqui, NÃO EXISTE.
- Mensagens CURTAS (2-4 linhas), uma ideia por mensagem. PROIBIDO usar emoji — nenhum, nunca (mensagem com emoji parece robô; escrevemos como uma pessoa real digitando no WhatsApp). Não puxe assunto, não faça small talk, não repita o que já foi dito.
- Escreva como o Alessandro humano escreveria: natural, direto, brasileiro. Pode usar "pra", "tá", "beleza" com moderação. Nada de linguagem corporativa engessada nem formatação chamativa.
- NÃO responda perguntas fora do atendimento (política, notícias, outros negócios, conselhos gerais): "vou verificar com a equipe e já te retorno" + escalar_humano.
- Situação não coberta pelo roteiro, dúvida pontual fora da alçada → escalar_humano. Na dúvida entre inventar e escalar: ESCALE.
- CONFLITO (ação registrar_conflito, com o motivo resumido): use quando a conversa NÃO caminha para aprovação e precisa de humano LIGANDO: pedido de garantia, defeito pós-conserto, reclamação/cliente conflituoso ou brigando, recusa final de TODAS as fases (não quer conserto, troca nem vender), ou dúvida que você não consegue resolver. Responda com acolhimento ("vou acionar nossa equipe pra te ligar e resolver isso") e registre o conflito. A partir daí, NÃO insista em vender.
- ⚠️ "QUERO DE VOLTA" NÃO É CONFLITO IMEDIATO: muitos clientes dirão "pode me mandar de volta", "só quero de volta" logo após o orçamento. Isso é OBJEÇÃO DE PREÇO disfarçada — NÃO abra conflito, NÃO aceite a devolução: avance para a PRÓXIMA fase do orçamento (F2 Pix → F3 balcão → F4 troca → F5 comprar), com naturalidade: "entendo! antes de devolver, deixa eu te apresentar uma condição...". Trate cada "quero de volta" como não-aprovação que avança UMA fase. SOMENTE se chegar na F5 (proposta de comprar o equipamento) e o cliente AINDA insistir que quer de volta → aí sim registrar_conflito (motivo: "recusou todas as fases, quer o equipamento de volta") para o humano ligar.
- Nunca prometa desconto acima das políticas; nunca invente valor, prazo ou informação fora do CONTEXTO.

(o contexto do cliente e o estado do horário vêm no bloco seguinte)

Responda APENAS um JSON válido, sem markdown: {"resposta":"texto da mensagem sugerida","acao":{"tipo":"nenhuma|cadastrar_logistica|mover_cliente_loja|enviar_orcamento|desconto_pix|desconto_balcao|proposta_troca|mover_aprovado|registrar_reprovacao|registrar_conflito|escalar_humano","motivo":"por quê"},"confianca":"alta|media|baixa"}`;

    try {
      const _iaT0 = Date.now();
      const r = await fetch('https://api.anthropic.com/v1/messages', {
        method: 'POST',
        headers: {
          'x-api-key': ANTHROPIC_KEY,
          'anthropic-version': '2023-06-01',
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          model: 'claude-sonnet-4-6',
          max_tokens: 1200,
          tools: [{ type: 'web_search_20250305', name: 'web_search', max_uses: 2 }], // USO RESTRITO via roteiro: só preço de equipamento novo na negociação
          temperature: 0.2,
          system: [
            { type: 'text', text: system, cache_control: { type: 'ephemeral' } },
            { type: 'text', text: blocoHorario + '\n\nCONTEXTO DO CLIENTE NO SISTEMA: ' + JSON.stringify(ctx) },
          ],
          messages: [{ role: 'user', content: imgB64
            ? [ { type: 'image', source: { type: 'base64', media_type: imgTipo, data: imgB64 } },
                { type: 'text', text: 'A imagem acima é a FOTO que o cliente acabou de enviar. Analise-a conforme as regras de VISÃO do seu roteiro.\n\nHistórico da conversa:\n' + (historico || '(sem mensagens ainda)') + '\n\nGere a próxima resposta sugerida.' } ]
            : ((() => { const b = new Date(Date.now() - 3 * 3600 * 1000); const dia = b.getUTCDay(), hr = b.getUTCHours() + b.getUTCMinutes() / 60;
                const ab = (dia >= 1 && dia <= 5) ? (hr >= 8 && hr < 15) : (dia === 6 ? (hr >= 8 && hr < 10) : false);
                return '🕐 JANELA COMERCIAL AGORA: ' + (ab ? 'ABERTA — ao pedir a aprovação, use a previsão "entre hoje e amanhã mesmo a gente já consegue te entregar"' : 'FECHADA — ao pedir a aprovação, use a previsão "amanhã mesmo a gente já consegue te entregar"') + '\n\n'; })())
              + (ctx.orcamentoRegistrado ? '💰 ORÇAMENTO REGISTRADO NO SISTEMA (envie ESTE texto quando o cliente aceitar receber):\n"' + ctx.orcamentoRegistrado.texto + '"\n(preço: R$ ' + ctx.orcamentoRegistrado.preco + ')\n\n' : '') + 'Histórico da conversa:\n' + (historico || '(sem mensagens ainda — cliente novo)') + (audioTranscrito ? '\n\n🎤 A ÚLTIMA mensagem do cliente foi um ÁUDIO. Transcrição: "' + audioTranscrito + '" — responda a ELA.' : '') + '\n\nGere a próxima resposta sugerida.' }],
        }),
      });
      const j = await r.json();
      // 📊 registra o consumo real de cada chamada, para saber onde o crédito vai
      try {
        const u = j.usage || {};
        const dia = new Date(Date.now() - 3 * 3600000).toISOString().slice(0, 10);
        const kU = 'ia_uso_' + dia;
        const reg = (await dbGet(kU)) || { chamadas: 0, entrada: 0, saida: 0, cacheCriado: 0, cacheLido: 0, ms: 0 };
        reg.chamadas++;
        reg.entrada += (u.input_tokens || 0);
        reg.saida += (u.output_tokens || 0);
        reg.cacheCriado += (u.cache_creation_input_tokens || 0);
        reg.cacheLido += (u.cache_read_input_tokens || 0);
        reg.ms += (Date.now() - _iaT0);
        reg.porOrigem = reg.porOrigem || {};
        const _o = reg.porOrigem['wa-bot'] || { n: 0, ent: 0, sai: 0, cw: 0, cr: 0 };
        _o.n++; _o.ent += (u.input_tokens || 0); _o.sai += (u.output_tokens || 0);
        _o.cw += (u.cache_creation_input_tokens || 0); _o.cr += (u.cache_read_input_tokens || 0);
        reg.porOrigem['wa-bot'] = _o;
        await dbSet(kU, reg);
      } catch (e) {}
      const _txts = (j.content || []).filter(b => b.type === 'text');
      const texto = (_txts.length ? _txts[_txts.length - 1].text : '') || '';
      let sug;
      const limpo = texto.replace(/```json|```/g, '').trim();
      try { sug = JSON.parse(limpo); }
      catch {
        // tenta achar o JSON dentro do texto (a IA às vezes escreve algo antes ou depois)
        let achou = null;
        const m = limpo.match(/\{[\s\S]*\}/);
        if (m) { try { achou = JSON.parse(m[0]); } catch (e) {} }
        if (!achou) {
          // último recurso: extrair só o campo resposta
          const mr = limpo.match(/"resposta"\s*:\s*"((?:[^"\\]|\\.)*)"/);
          if (mr) { try { achou = { resposta: JSON.parse('"' + mr[1] + '"'), acao: { tipo: 'nenhuma', motivo: 'extraido' }, confianca: 'baixa' }; } catch (e) {} }
        }
        // 🚫 NUNCA usar o texto cru como resposta — era assim que o JSON chegava ao cliente
        sug = achou || { resposta: '', acao: { tipo: 'nenhuma', motivo: 'parse-falhou' }, confianca: 'baixa', _falhaParse: true };
      }
      // CAMADA DE SEGURANÇA: se a resposta ainda parecer estrutura interna, descarta
      const pareceJson = t => /"(resposta|acao|confianca|tipo|motivo)"\s*:/.test(String(t || '')) || /^\s*[{[]/.test(String(t || ''));
      if (pareceJson(sug.resposta)) {
        sug = { resposta: '', acao: { tipo: 'nenhuma', motivo: 'resposta-suja' }, confianca: 'baixa', _falhaParse: true };
      }
      // Tag [RETOMAR]: cliente quis agendar fora do horário → fila de retomada na abertura da janela
      try {
        if (sug.resposta && sug.resposta.includes('[RETOMAR]')) {
          sug.resposta = sug.resposta.replace(/\s*\[RETOMAR\]\s*/g, ' ').trim();
          const telFull = String(tel).replace(/\D/g, '');
          const toRet = telFull.startsWith('55') ? telFull : '55' + telFull;
          const ret = (await dbGet('wa_retomar')) || { tels: [] };
          if (!ret.tels.includes(toRet)) { ret.tels.push(toRet); await dbSet('wa_retomar', ret); }
        }
      } catch (e) {}
      sug.geradaEm = new Date().toISOString();
      await dbSet('wa_sug_' + tel, sug);
      return res.status(200).json({ ok: true, sugestao: sug });
    } catch (e) {
      return res.status(200).json({ ok: false, error: 'IA: ' + e.message });
    }
  }

  // ── Enviar mensagem aprovada (via Meta Cloud API) ──
  if (req.method === 'POST' && action === 'enviar') {
    const { tel, texto, acaoAprovada, acaoMotivo, via } = req.body || {};
    if (!tel || !texto) return res.status(400).json({ ok: false, error: 'tel e texto obrigatórios' });
    // 🛡 BARREIRA FINAL: estrutura interna do sistema NUNCA pode chegar ao cliente
    if (/"(resposta|acao|confianca)"\s*:/.test(String(texto)) || /^\s*\{[\s\S]*"tipo"/.test(String(texto))) {
      try {
        await rpushEvt({ ts: new Date().toISOString(), tel, dir: 'acao', tipo: 'falha',
          texto: '🛡 ENVIO BLOQUEADO — a mensagem continha estrutura interna (JSON) e não foi enviada ao cliente' });
      } catch (e) {}
      return res.status(400).json({ ok: false, error: 'mensagem continha estrutura interna — envio bloqueado' });
    }
    const { token: tkE, phoneId: pidE } = await credenciais();
    if (!tkE || !pidE) return res.status(200).json({ ok: false, error: 'Credenciais WhatsApp não configuradas (envs ou setup-credenciais)' });
    try {
      const r = await fetch(`https://graph.facebook.com/v20.0/${pidE}/messages`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${tkE}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ messaging_product: 'whatsapp', to: tel, type: 'text', text: { body: String(texto).slice(0, 3800) } }),
      });
      const j = await r.json();
      const okSend = !!(j.messages && j.messages[0]);
      await rpushEvt({ ts: new Date().toISOString(), tel, dir: 'out', texto: String(texto).slice(0, 2000),
        msgId: okSend ? j.messages[0].id : null, tipo: 'text', via: via || 'copiloto',
        acaoAprovada: acaoAprovada || null });
      // ⚙️ EXECUÇÃO REAL DA AÇÃO — TRAVA DE TESTE: só para telefones em wa_bot_config.execTels
      try {
        const cfgX = (await dbGet('wa_bot_config')) || {};
        const execTels = Array.isArray(cfgX.execTels) ? cfgX.execTels : [];
        const d8x = String(tel).replace(/\D/g, '').slice(-8);
        const pzE = (await dbGet('wa_bot_pausados')) || {};
        const autorizado = !pzE[d8x] && (cfgX.modoAberto === true || execTels.some(t => String(t).replace(/\D/g, '').slice(-8) === d8x));
        if (autorizado && acaoAprovada === 'cadastrar_logistica') {
          const ftvChk = (await dbGet('fichas_tv')) || { fichas: [] };
          const fichaTvSrc = (ftvChk.fichas || []).find(f => String(f.telefone || '').replace(/\D/g, '').slice(-8) === d8x);
          const ehTV = !!fichaTvSrc;
          if (ehTV) {
            // TV: entra na LOGÍSTICA TV em Liberado Coleta — motorista vê e combina o horário
            const KTV2 = (process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim();
            const jaTemTv = ((await dbGet('tv_logistica')) || { fichas: [] }).fichas
              .some(f => String(f.telefone || '').replace(/\D/g, '').slice(-8) === d8x &&
                !['coleta_efetuada', 'orc_registrado'].includes(f.phase));
            if (!jaTemTv) {
              await fetch(`https://reparoeletroadm.com/api/tv-logistica?action=criar&k=${KTV2}`, {
                method: 'POST', headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                  nome: fichaTvSrc.nome || 'Cliente WhatsApp',
                  telefone: String(tel).replace(/\D/g, ''),
                  endereco: fichaTvSrc.endereco || '',
                  equipamento: fichaTvSrc.equipamento || 'TV',
                  defeito: fichaTvSrc.defeito || '',
                  texto: '🤖 Bot: triagem OK. ' + String(acaoMotivo || '').slice(0, 250),
                }),
              });
              await bumpStat('logistica');
              const _chkTv = (await dbGet('tv_logistica')) || { fichas: [] };
              // comparação por endsWith (o slice exato falhava quando o telefone tinha
              // formato diferente) e conferindo também o pipe de TV antes de alertar
              const casaTv = t => String(t || '').replace(/\D/g, '').endsWith(String(d8x));
              let _okTv = (_chkTv.fichas || []).some(f => casaTv(f.telefone));
              if (!_okTv) {
                try {
                  const ppTvC = (await dbGet('tv_pipe')) || { cards: [] };
                  _okTv = ((ppTvC.cards) || []).some(c => casaTv(c.telefone));
                } catch (e) {}
              }
              if (!_okTv) {
                await promessaSemLastro(tel, d8x, texto, acaoMotivo,
                  'o bot confirmou COLETA de TV e a ficha NÃO apareceu na logística TV');
              }
            }
            // Tirar a ficha TV da prospecção (Entrar em Contato / Contato Feito): agora está na logística
            const ftvUpd = (await dbGet('fichas_tv')) || { fichas: [] };
            const fTvU = (ftvUpd.fichas || []).find(f => String(f.telefone || '').replace(/\D/g, '').slice(-8) === d8x);
            if (fTvU && fTvU.status !== 'logistica') {
              fTvU.status = 'logistica';
              fTvU.logisticaEm = new Date().toISOString();
              await dbSet('fichas_tv', ftvUpd);
            }
          } else {
          const fdbX = (await dbGet('fichas_adm')) || { fichas: [] };
          const fichaX = (fdbX.fichas || []).find(f => String(f.telefone || '').replace(/\D/g, '').slice(-8) === d8x && f.status !== 'logistica');
          if (!fichaX) {
            // ✅ ANTES DE ALERTAR: confere se a ficha JÁ ESTÁ na logística ou no pipe.
            // O alerta anterior disparava justamente no caso normal — ficha já cadastrada
            // não aparece em fichas_adm com status diferente de logistica, e virava falso positivo.
            let jaAtendido = false;
            try {
              const [logC, ppC, fadmC] = await Promise.all([
                dbGet('reparoeletro_logistica'), dbGet('reparoeletro_pipe'), dbGet('fichas_adm'),
              ]);
              const casa = t => String(t || '').replace(/\D/g, '').endsWith(String(d8x));
              jaAtendido = (((logC || {}).fichas) || []).some(f => casa(f.telefone))
                || (((ppC || {}).cards) || []).some(c => casa(c.telefone))
                || (((fadmC || {}).fichas) || []).some(f => casa(f.telefone) && f.status === 'logistica');
            } catch (e) {}
            if (!jaAtendido) {
              await promessaSemLastro(tel, d8x, texto, acaoMotivo,
                'o bot confirmou COLETA ao cliente, mas não existe ficha em fichas_adm para cadastrar na logística');
            }
          }
          if (fichaX) {
            const logX = (await dbGet('reparoeletro_logistica')) || { fichas: [] };
            const jaLog = (logX.fichas || []).some(f => String(f.telefone || '').replace(/\D/g, '').slice(-8) === d8x && f.phase !== 'orc_registrado');
            if (!jaLog) {
              logX.fichas.unshift({
                id: 'log_' + Date.now().toString(36),
                nome: fichaX.nome, telefone: fichaX.telefone, endereco: fichaX.endereco || '',
                // 🏢 sinaliza quando o endereço parece de prédio e não tem complemento
                faltaComplemento: (function () {
                  const e = String(fichaX.endereco || '').toLowerCase();
                  const ehPredio = /pr[ée]dio|condom[ií]nio|edif[ií]cio|residencial|\bcond\b|torre/.test(e);
                  const temCompl = /\bap(to|t|artamento)?\.?\s*\d|\bbloco\b|\bbl\.?\s*[a-z0-9]|\btorre\s*\d|\bcasa\s*\d|\bqd\b|\bquadra\b|\blote\b/.test(e);
                  return ehPredio && !temCompl;
                })(),
                equipamento: fichaX.equipamento || '', defeito: fichaX.defeito || '',
                phase: /AGENDADO:/i.test(String(acaoMotivo || '')) ? 'horario_marcado' : 'liberado_coleta',
                criadoEm: new Date().toISOString(), movedAt: new Date().toISOString(),
                origem: 'bot',
                observacao: (/ADEGA GRANDE/i.test(String(acaoMotivo || '')) ? '🚚 ADEGA GRANDE — precisa CAMINHONETE/PICAPE. ' : '') +
                  (/AGENDADO:/i.test(String(acaoMotivo || '')) ? '🤖 Bot — ' + String(acaoMotivo).slice(0, 180) : '🤖 cadastrado pelo Bot Vendas'),
                veiculoEspecial: /ADEGA GRANDE/i.test(String(acaoMotivo || '')) ? 'picape' : undefined,
              });
              await dbSet('reparoeletro_logistica', logX);
              fichaX.status = 'logistica'; fichaX.logisticaEm = new Date().toISOString();
              await dbSet('fichas_adm', fdbX);
              await bumpStat('logistica');
              // CONFERE O PRÓPRIO RESULTADO: relê o banco e valida que a coleta existe mesmo.
              const _chk = (await dbGet('reparoeletro_logistica')) || { fichas: [] };
              const _ok = (_chk.fichas || []).some(f => String(f.telefone || '').replace(/\D/g, '').slice(-8) === d8x);
              if (!_ok) {
                await promessaSemLastro(tel, d8x, texto, acaoMotivo,
                  'o bot confirmou COLETA e a gravação na logística NÃO foi confirmada na releitura');
              }
            }
          }
          }
        }
        if (autorizado && acaoAprovada === 'registrar_conflito') {
          const KCF = (process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim();
          // Busca o cliente em TODA a operação — TV que já avançou não está mais em fichas_tv,
          // o nome dele vive na logística TV (ou no pipe). Antes o conflito nascia sem nome.
          const [fdbC, fdbCtv, logC, logCtv, ppC, ppCtv] = await Promise.all([
            dbGet('fichas_adm'), dbGet('fichas_tv'),
            dbGet('reparoeletro_logistica'), dbGet('tv_logistica'),
            dbGet('reparoeletro_pipe'), dbGet('tv_pipe'),
          ]);
          const bateD8 = t => String(t || '').replace(/\D/g, '').slice(-8) === d8x;
          const acha = b => (((b || {}).fichas) || []).find(f => bateD8(f.telefone));
          const achaCard = b => {
            const c = (((b || {}).cards) || []).find(x => bateD8(x.telefone));
            return c ? { nome: c.nomeContato || c.nome, equipamento: c.equipamento || c.descricao || '' } : null;
          };
          // primeiro quem tiver NOME preenchido; a origem não importa
          const candidatosC = [acha(fdbC), acha(fdbCtv), acha(logC), acha(logCtv), achaCard(ppC), achaCard(ppCtv)]
            .filter(Boolean);
          const fichaC = candidatosC.find(f => String(f.nome || '').trim()) || candidatosC[0] || null;
          const equipC = (candidatosC.find(f => String(f.equipamento || '').trim()) || {}).equipamento || '';
          {
          const ehCompra = /AN[ÁA]LISE DE COMPRA/i.test(String(acaoMotivo || ''));
          // O equipamento JÁ ESTÁ NA LOJA: aproveita a foto tirada no recebimento (alm_foto_<cardId>).
          // Se não existir, o almoxarifado pede para tirar — nunca se pede foto ao cliente.
          let temFotoCompra = false, cardCompraId = null, ehTvCompra = false;
          if (ehCompra) {
            try {
              // Sistema do cliente: TV não passa pelo almoxarifado ADM
              const tvLogF = (await dbGet('tv_logistica')) || { fichas: [] };
              const naTv = (tvLogF.fichas || []).some(f => String(f.telefone || '').replace(/\D/g, '').slice(-8) === d8x);
              ehTvCompra = naTv || (!!acha(fdbCtv) && !acha(fdbC));
              if (!ehTvCompra) {
                const ppF = (await dbGet('reparoeletro_pipe')) || { cards: [] };
                const cardF = (ppF.cards || []).find(c => String(c.telefone || '').replace(/\D/g, '').slice(-8) === d8x);
                if (cardF) {
                  cardCompraId = cardF.id;
                  const fotoF = await dbGet('alm_foto_' + cardF.id);
                  temFotoCompra = !!(fotoF && fotoF.img);
                }
              }
            } catch (e) {}
          }
          const respCf = await fetch(`https://reparoeletroadm.com/api/prospeccao?action=criar-conflito&k=${KCF}`, {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              nome: (fichaC && fichaC.nome) || 'Cliente WhatsApp',
              telefone: String(tel).replace(/\D/g, ''),
              equipamento: (fichaC && fichaC.equipamento) || equipC || '',
              motivo: String(acaoMotivo || 'conflito registrado pelo bot').slice(0, 300),
              tipo: ehCompra ? 'analise_compra' : 'conflito',
              temFoto: temFotoCompra, cardId: cardCompraId, sistema: ehTvCompra ? 'tv' : 'adm',
            }),
          }).then(x => x.json()).catch(() => null);
          // Duplicata no ALMOXARIFADO para a equipe dar o parecer (recomenda ou não)
          if (ehCompra && !ehTvCompra && respCf && respCf.criado) {
            try {
              await fetch(`https://reparoeletroadm.com/api/almoxarifado?action=criar-analise-compra&k=${KCF}`, {
                method: 'POST', headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ conflitoId: respCf.id, cardId: cardCompraId,
                  cliente: (fichaC && fichaC.nome) || 'Cliente WhatsApp',
                  tel: String(tel).replace(/\D/g, ''), equipamento: (fichaC && fichaC.equipamento) || equipC || '',
                  obs: String(acaoMotivo || '').slice(0, 200), temFoto: temFotoCompra }),
              });
            } catch (e) {}
          }
          // KPI conta CONFLITO CRIADO — não tentativa nem repetição do mesmo cliente (dedupe)
          if (respCf && respCf.criado) await bumpStat('conflitos');
          }
        }
        if (autorizado && acaoAprovada === 'mover_aprovado') {
          // Valor combinado na negociação, se o cérebro informou
          try {
            const mV = String(acaoMotivo || '').match(/R?\$?\s?(\d{2,5})(?:[.,](\d{2}))?/);
            if (mV) {
              const vComb = parseFloat(mV[1] + (mV[2] ? '.' + mV[2] : ''));
              if (vComb >= 30 && vComb <= 20000) {
                const ppV = (await dbGet('reparoeletro_pipe')) || { cards: [] };
                const cV = (ppV.cards || []).find(c => String(c.telefone || '').replace(/\D/g, '').slice(-8) === d8x &&
                  ['aguardando_aprovacao', 'ultima_chamada'].includes(c.phaseId || c.phase));
                if (cV) { cV.valor = vComb; cV.valorCombinadoBot = true; await dbSet('reparoeletro_pipe', ppV); }
              }
            }
          } catch (e) {}
          // GARANTIA: move, confere e força board técnico + almoxarifado
          const gar = await garantirAprovacao(d8x);
          if (gar.ok) {
            await bumpStat('aprovacoes');
            await rpushEvt({ ts: new Date().toISOString(), tel, dir: 'acao', tipo: 'acao',
              texto: 'aprovado ✅ ' + (gar.sistema || '').toUpperCase() + ' — ' + gar.passos.join(' | ') });
            // espelhos do mesmo cliente ainda em negociação viram duplicata
            try {
              const ppE = (await dbGet('reparoeletro_pipe')) || { cards: [] };
              const esp = (ppE.cards || []).filter(c => c.id !== gar.cardId &&
                String(c.telefone || '').replace(/\D/g, '').slice(-8) === d8x &&
                ['aguardando_aprovacao', 'ultima_chamada'].includes(c.phaseId || c.phase));
              if (esp.length) {
                const arqE = (await dbGet('pipe_ids_arquivados')) || { ids: [] };
                for (const e of esp) if (!arqE.ids.includes(e.id)) arqE.ids.push(e.id);
                await dbSet('pipe_ids_arquivados', arqE);
                ppE.cards = ppE.cards.filter(c => !esp.some(e => e.id === c.id));
                await dbSet('reparoeletro_pipe', ppE);
              }
            } catch (e) {}
          } else {
            // FALHA VISÍVEL — nunca mais silenciosa
            await rpushEvt({ ts: new Date().toISOString(), tel, dir: 'acao', tipo: 'falha',
              texto: (gar.ambiguo ? '⚠️ APROVAÇÃO AMBÍGUA — ' : '❌ APROVAÇÃO NÃO CONCLUÍDA — ') + gar.passos.join(' | ') });
            try {
              const KCF2 = (process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim();
              await fetch(`https://reparoeletroadm.com/api/prospeccao?action=criar-conflito&k=${KCF2}`, {
                method: 'POST', headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ nome: 'Cliente WhatsApp', telefone: String(tel).replace(/\D/g, ''),
                  motivo: gar.ambiguo
                    ? 'APROVAÇÃO AMBÍGUA — cliente tem orçamento aberto em mais de um sistema (TV e ADM). Confirmar com ele QUAL equipamento foi aprovado e mover manualmente.'
                    : 'APROVAÇÃO FALHOU NO SISTEMA — cliente aprovou mas a ficha não avançou. Verificar e mover manualmente.' }),
              });
            } catch (e) {}
          }
        }
        if (autorizado && acaoAprovada === 'mover_cliente_loja') {
          const fdbL = (await dbGet('fichas_adm')) || { fichas: [] };
          const fichaL = (fdbL.fichas || []).find(f => String(f.telefone || '').replace(/\D/g, '').slice(-8) === d8x && f.status !== 'logistica');
          if (fichaL) {
            fichaL.status = 'cliente_loja';
            fichaL.movidoEm = new Date().toISOString();
            fichaL.clienteLojaMotivo = String(acaoMotivo || 'bot: cliente vai trazer na loja').slice(0, 200);
            await dbSet('fichas_adm', fdbL);
            await bumpStat('cliente_loja');
          }
        }
        if (autorizado && acaoAprovada === 'mover_entrar_contato') {
          const fdbE = (await dbGet('fichas_adm')) || { fichas: [] };
          const fichaE = (fdbE.fichas || []).find(f => String(f.telefone || '').replace(/\D/g, '').slice(-8) === d8x);
          if (fichaE) {
            fichaE.status = 'entrar_contato';
            fichaE.entrarContatoMotivo = String(acaoMotivo || 'bot: retomar por telefone').slice(0, 200);
            await dbSet('fichas_adm', fdbE);
            await bumpStat('entrar_contato');
          }
          const ftvE = (await dbGet('fichas_tv')) || { fichas: [] };
          const fichaTvE = (ftvE.fichas || []).find(f => String(f.telefone || '').replace(/\D/g, '').slice(-8) === d8x);
          if (fichaTvE) {
            fichaTvE.status = 'entrar_contato';
            fichaTvE.entrarContatoMotivo = String(acaoMotivo || 'bot: retomar por telefone').slice(0, 200);
            await dbSet('fichas_tv', ftvE);
            if (!fichaE) await bumpStat('entrar_contato');
          }
        }
      } catch (eX) {}
      // Registro de ação para a timeline do painel
      if (acaoAprovada && acaoAprovada !== 'nenhuma') {
        await rpushEvt({ ts: new Date().toISOString(), tel, dir: 'acao', texto: acaoAprovada, tipo: 'acao' });
      }
      return res.status(200).json({ ok: okSend, meta: okSend ? 'enviada' : JSON.stringify(j).slice(0, 300) });
    } catch (e) {
      return res.status(200).json({ ok: false, error: e.message });
    }
  }

  // ── Config das políticas (GET lê, POST grava) ──
  if (action === 'config') {
    if (req.method === 'POST') {
      const atual = (await dbGet('wa_bot_config')) || {};
      await dbSet('wa_bot_config', Object.assign({}, CONFIG_DEFAULT, atual, req.body || {}));
      return res.status(200).json({ ok: true });
    }
    const cfg = Object.assign({}, CONFIG_DEFAULT, (await dbGet('wa_bot_config')) || {});
    return res.status(200).json({ ok: true, config: cfg });
  }

  return res.status(404).json({ ok: false, error: 'ação não encontrada' });
}
