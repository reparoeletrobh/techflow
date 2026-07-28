// api/wa-bot.js — Cérebro do bot (FASE 1: COPILOTO — sugere, humano aprova)
// actions: conversas | historico&tel= | sugerir&tel= | enviar (POST) | config
const U = (process.env.UPSTASH_URL || '').replace(/['"]/g, '').trim();
const T = (process.env.UPSTASH_TOKEN || '').replace(/[\n\r'"]/g, '').trim();
let WA_TOKEN = (process.env.WA_TOKEN || '').trim();
let WA_PHONE_ID = (process.env.WA_PHONE_ID || '').trim();
async function credenciais() {
  // Envs da Vercel têm prioridade; fallback: chave wa_credenciais no Redis
  if (WA_TOKEN && WA_PHONE_ID) return { token: WA_TOKEN, phoneId: WA_PHONE_ID };
  const c = await dbGet('wa_credenciais');
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
    const r = await fetch(`${U}/lrange/${EVT_LIST}/-1500/-1`, { headers: { Authorization: `Bearer ${T}` } });
    const j = await r.json();
    const out = [];
    for (const s of (j.result || [])) { try { out.push(JSON.parse(s)); } catch (_) {} }
    return out;
  } catch { return []; }
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
  // ── 💰 ORÇAMENTOS-ABERTOS: contagem VIVA — entrou em orçamento e ainda não saiu ──
  // Sai quando: aprovou (pipe avançou) OU virou Conflitos Bot. Enquanto isso, fica aqui.
  if (action === 'orcamentos-abertos') {
    const [logA, tvA, envA, pipeA, pipeT, pros] = await Promise.all([
      dbGet('reparoeletro_logistica'), dbGet('tv_logistica'),
      dbGet('wa_orc_enviados').then(v => v || { ids: {} }),
      dbGet('reparoeletro_pipe'), dbGet('tv_pipe'), dbGet('prospeccao_adm'),
    ]);
    const d8 = t => String(t || '').replace(/\D/g, '').slice(-8);
    // Quem JÁ SAIU: aprovou/avançou no pipe, ou está em Conflitos Bot
    const APROVOU = ['aprovados', 'video_enviado', 'analise_compra', 'equipamento_comprado',
      'programar_entrega', 'solicitar_entrega', 'entrega_solicitada', 'rota_em_andamento',
      'receber', 'erp', 'finalizado', 'descarte', 'garantia'];
    const saiu = new Set();
    for (const c of [...(((pipeA || {}).cards) || []), ...(((pipeT || {}).cards) || [])]) {
      const fase = c.phaseId || c.phase;
      if (APROVOU.includes(fase)) { const d = d8(c.telefone); if (d.length >= 8) saiu.add(d); }
    }
    for (const f of (((pros || {}).fichas) || [])) {
      if (f.status === 'conflitos_bot') { const d = d8(f.telefone); if (d.length >= 8) saiu.add(d); }
    }
    // Quem ESTÁ EM ORÇAMENTO agora
    const abertos = [];
    const vistos = new Set();
    const registra = (tel, nome, sis, quando, onde, equipamento, valor) => {
      const d = d8(tel);
      if (d.length < 8 || saiu.has(d) || vistos.has(d)) return;
      vistos.add(d);
      const ts = quando ? new Date(quando).getTime() : 0;
      abertos.push({ tel: String(tel || '').replace(/\D/g, ''), nome: nome || 'Cliente', sis, onde,
        equipamento: equipamento || '', valor: valor || null,
        enviadoEm: quando || null,
        horasParado: ts ? Number(((Date.now() - ts) / 3600000).toFixed(1)) : null });
    };
    // SOMENTE orçamentos que O BOT enviou (registro wa_orc_enviados) e que seguem sem desfecho.
    // Orçamento enviado por humano/outro canal NÃO entra aqui.
    for (const f of (((logA || {}).fichas) || [])) {
      if (!['orc_registrado', 'orc_enviado'].includes(f.phase)) continue;
      if (!envA.ids[f.id]) continue;
      registra(f.telefone, f.nome, 'adm', envA.ids[f.id], 'aguardando resposta', f.equipamento);
    }
    for (const f of (((tvA || {}).fichas) || [])) {
      if (!['orc_registrado', 'orc_enviado'].includes(f.phase)) continue;
      const quando = envA.ids['tv:' + f.id] || envA.ids[f.id];
      if (!quando) continue;
      registra(f.telefone, f.nome, 'tv', quando, 'aguardando resposta', f.equipamento);
    }
    abertos.sort((a, b) => (b.horasParado || 0) - (a.horasParado || 0));
    const valorEmJogo = abertos.reduce((s, a) => s + (a.valor || 0), 0);
    return res.status(200).json({ ok: true, total: abertos.length,
      valorEmJogo: Number(valorEmJogo.toFixed(2)),
      porOnde: abertos.reduce((o, a) => { o[a.onde] = (o[a.onde] || 0) + 1; return o; }, {}),
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
    if (cfgR.reativacaoAtiva === false) return res.status(200).json({ ok: true, msg: 'reativação desligada (wa_bot_config.reativacaoAtiva=false)' });
    if (!dentroHorarioComercial()) return res.status(200).json({ ok: true, msg: 'fora do horário comercial — reativações em standby' });
    const { token, phoneId } = await credenciais();
    if (!token || !phoneId) return res.status(200).json({ ok: false, error: 'credenciais ausentes' });

    const [logR, tvLogR, evtsR, reatR] = await Promise.all([
      dbGet('reparoeletro_logistica'), dbGet('tv_logistica'), lerEvts(),
      dbGet('wa_reativacao').then(v => v || { alvos: {} }),
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
    // Alvos: quem tem orçamento na mesa e ainda não decidiu
    const alvos = [
      ...(((logR || {}).fichas) || []).filter(f => ['orc_registrado', 'orc_enviado'].includes(f.phase)).map(f => ({ f, sis: 'adm' })),
      ...(((tvLogR || {}).fichas) || []).filter(f => ['orc_registrado', 'orc_enviado'].includes(f.phase)).map(f => ({ f, sis: 'tv' })),
    ];
    const ESCADA = [
      { h: 6,  txt: (n, eq) => `Oi ${n}! Conseguiu dar uma olhada no orçamento do seu ${eq}? Qualquer dúvida sobre o serviço eu te explico, é só me chamar 😊` },
      { h: 24, txt: (n) => `${n}, uma condição que costuma ajudar: pagando no Pix a gente consegue um valor melhor pra você. Quer que eu veja isso?` },
      { h: 48, txt: (n, eq) => `${n}, seu ${eq} já está aqui com a gente e fico no aguardo da sua aprovação para prosseguir com o conserto. Com a sua confirmação, acredito que entre hoje e amanhã mesmo a gente já consegue te entregar 😊` },
      { h: 72, txt: (n, eq) => `${n}, só passando para me colocar à disposição sobre o seu ${eq}. Se quiser seguir com o conserto, é só me avisar que já encaminho para a bancada. E se preferir deixar para outro momento, também tudo bem — me avisa que a equipe organiza a devolução com você.` },
    ];
    const agoraR = Date.now();
    const feitos = [];
    // TRAVA: só reativa quem O BOT atendeu (tem conversa) E para quem O BOT enviou o orçamento.
    // Sem isso o motor escrevia para clientes que nunca falaram com o bot.
    const enviadosR = (await dbGet('wa_orc_enviados')) || { ids: {} };
    const teveConversa = new Set();
    for (const e of evtsR) {
      if (e.dir !== 'in') continue;
      const d = String(e.tel || '').replace(/\D/g, '').slice(-8);
      if (d.length >= 8) teveConversa.add(d);
    }
    const pulados = [];
    for (const { f, sis } of alvos) {
      const d8r = String(f.telefone || '').replace(/\D/g, '').slice(-8);
      if (d8r.length < 8) continue;
      const chave = (sis === 'tv' ? 'tv:' : '') + f.id;
      if (!enviadosR.ids[chave] && !enviadosR.ids[f.id]) {
        pulados.push({ nome: f.nome, motivo: 'orçamento não foi enviado pelo bot' }); continue;
      }
      if (!teveConversa.has(d8r)) {
        pulados.push({ nome: f.nome, motivo: 'cliente nunca conversou com o bot' }); continue;
      }
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
      }
    }
    for (const k of Object.keys(reatR.alvos)) {
      if (agoraR - (reatR.alvos[k].ultimo || 0) > 20 * 86400000) delete reatR.alvos[k];
    }
    await dbSet('wa_reativacao', reatR);
    return res.status(200).json({ ok: true, alvosAtivos: alvos.length, acoes: feitos.length, feitos,
      pulados: pulados.length, motivosPulados: pulados.slice(0, 20) });
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
      return virgem && idade > 5 * 60000 && d8.length >= 8 &&
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
      if (t.includes('purific') || t.includes('filtro')) return 'purificador';
      if (t.includes('adega')) return 'adega';
      if (t.includes('forno')) return 'forno';
      if (t.includes('tv') || t.includes('telev')) return 'tv';
      if (t.includes('bblend') || t.includes('b.blend')) return 'bblend';
      return 'outro';
    };
    const resultados = [];
    for (const c of ((boardC && boardC.cards) || [])) {
      if (!FASES_FEITO.includes(c.phaseId)) continue;
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
    const cfgO = (await dbGet('wa_bot_config')) || {};
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
      if (enviadosO.ids[dedupeKey]) continue;         // dedupe
      // marco temporal: NÃO enviar o backlog — só diagnósticos feitos após a ativação
      const tsOrc = new Date((f.diagnostico && f.diagnostico.em) || f.movedAt || f.criadoEm || 0).getTime();
      if (marcoO && tsOrc < marcoO) continue;
      const telO = String(f.telefone).replace(/\D/g, '');
      const to = telO.startsWith('55') ? telO : '55' + telO;
      const t8 = to.slice(-8);
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
          await rpushEvt({ ts: new Date().toISOString(), tel: to, dir: 'out',
            texto: '📨 [template orcamento_pronto] ' + (f.nome || ''), tipo: 'template', via: 'bot-auto-orcamento' });
          if (okO) await bumpStat('orcamentos');
          disparos.push({ nome: f.nome, modo: 'template-janela-fechada', ok: okO });
        }
        enviadosO.ids[dedupeKey] = new Date().toISOString();
        // Efeito do botão "Copiar e Enviar": marca o orçamento como enviado na seção Orçamentos
        // (some de pendentes; o card já está no pipe em aguardando_aprovacao → cronômetro de 48h da última chamada segue vivo)
        try {
          const KOE = (process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim();
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
    let lista = Object.values(conv).sort((a, b) => String(b.ultimaTs).localeCompare(String(a.ultimaTs)));
    lista = lista.filter(c => {
      const d8c = String(c.tel).replace(/\D/g, '').slice(-8);
      const arquivada = !!arqL.tels[d8c];
      c.arquivada = arquivada;
      return verArq ? arquivada : !arquivada;
    });
    for (const c of lista) c.pausado = !!pzL[String(c.tel).replace(/\D/g, '').slice(-8)];
    return res.status(200).json({ ok: true, total: lista.length, conversas: lista.slice(0, 100) });
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
    const msgs = evts.filter(e => String(e.tel || '').replace(/\D/g, '').slice(-8) === d8Hi && e.dir !== 'status').slice(-60);
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

🎯 NICHO DE ATENDIMENTO (lista FECHADA — só consertamos): micro-ondas, forno elétrico, bebedouro de água, purificador de água, adega climatizada e televisão. QUALQUER outro equipamento (geladeira, máquina de lavar, fogão a gás, ar-condicionado, notebook, celular, som etc.): recuse com educação — "Poxa, esse a gente não atende — trabalhamos com micro-ondas, forno elétrico, bebedouro, purificador, adega e TV. Se algum dia precisar de um desses, conta com a gente!" — e NÃO crie coleta nem prossiga. TV segue o fluxo do sistema de TV (item 7a-1); os demais do nicho seguem o fluxo normal (ADM). Aprovações também são roteadas: orçamento de TV aprova no sistema TV, os demais no sistema ADM — o executor cuida disso automaticamente quando você usa mover_aprovado.

QUEM TE PROCURA: clientes que preencheram a ficha de atendimento (formulário) e iniciaram a conversa. A ficha deles aparece no CONTEXTO abaixo (nome, equipamento, defeito, endereço).

⚠️ REGRA DE OURO DOS DADOS: os dados do cliente JÁ ESTÃO NA FICHA (contexto). NUNCA peça nem CONFIRME nome, equipamento, defeito ou endereço que estejam lá — nada de "seu endereço é X, certo?": usamos o da ficha e pronto. Só pergunte o que estiver realmente FALTANDO no contexto. Dupla confirmação atrasa a venda e irrita o cliente.

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
2) SE DELIVERY → o cliente dizer "pode buscar" (ou qualquer sinal de coleta) É A DECISÃO: use a ação cadastrar_logistica IMEDIATAMENTE, na MESMA resposta. NÃO pergunte período. NÃO confirme o endereço (o da ficha vale — só pergunte endereço se a ficha estiver SEM endereço). A resposta é curta: comemore + informe a janela: dentro do horário de coleta → "Perfeito! Nossa equipe já vai programar a busca ainda hoje."; fora do horário → "Perfeito! Sua coleta será feita amanhã entre 08h e 14h.". Só aceite agendar dia específico se o CLIENTE pedir espontaneamente.
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
2f) PREVISÃO DE HORÁRIO DA COLETA: se o cliente perguntar quando o motorista passa / se tem previsão, responda: "Registrando a sua coleta, em até 3 horas no máximo a nossa rota já passa no seu endereço." Essa é a estimativa oficial — não invente outra.
2g) ACESSÓRIOS — avise junto da confirmação da coleta (ou se o cliente vier trazer na loja):
   - MICRO-ONDAS: "Não precisa enviar o prato de vidro nem o trilho, pode ficar com você."
   - BEBEDOURO ou PURIFICADOR DE ÁGUA: "Não precisa enviar os acessórios — mangueira, registro, suporte de copo e afins podem ficar com você."
3) COLETA CONFIRMADA → ação cadastrar_logistica (informe no motivo: imediata ou agendada + dia/período/faixa). O sistema dá baixa na ficha e cria a coleta.
4) EQUIPAMENTO NA LOJA → diagnóstico → orçamento enviado ao cliente (valor no contexto, em logistica/pipe).
4-G) GARANTIA — cliente diz que JÁ FEZ serviço com a gente nesse equipamento e o defeito voltou ("tá na garantia", "vocês consertaram e parou de novo", "voltou o problema") OU envia FOTO/documento de garantia, nota ou comprovante de serviço anterior (qualquer DADO relacionado a garantia): acolha com prioridade — "Sinto muito pelo transtorno! Vou acionar nossa equipe AGORA para cuidar do seu caso com prioridade, tudo bem?" — e use OBRIGATORIAMENTE registrar_conflito (motivo: "possível GARANTIA — [equipamento/relato resumido]"). NÃO cobre nada, NÃO agende coleta normal, NÃO discuta se a garantia é válida: a equipe avalia. ⚠️ PROIBIDO encerrar caso de garantia só com escalar_humano ou com "já passei pro técnico": garantia SEMPRE termina com a ação registrar_conflito — sem exceção.
4-H) DESISTIU ANTES DA COLETA (cancelou/desistiu ANTES de coletarmos — sem orçamento, sem equipamento com a gente): responda cordial deixando a porta aberta — "Sem problema! Qualquer coisa é só chamar, estamos à disposição." — e use mover_entrar_contato (motivo: "desistiu da coleta antes de acontecer — retomar por telefone"). NÃO use registrar_conflito nesse caso: conflito é para equipamento JÁ conosco, garantia ou cliente insatisfeito.

5-FIM) ESGOTOU AS 5 FASES E O CLIENTE MANTEVE A RECUSA (não quer fazer o serviço / quer pagar só o orçamento): responda cordial — "Sem problema! Nossa equipe vai entrar em contato pra combinar a devolução do equipamento e os detalhes, tudo bem?" — e use OBRIGATORIAMENTE a ação registrar_conflito (motivo: "reprovou o orçamento após as 5 fases — finalizar manualmente: taxa R$30 do delivery + devolução"). NÃO cobre você mesmo, NÃO envie dados de pagamento, NÃO combine devolução por conta própria: a finalização é MANUAL da equipe.

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
5-FECH) COMO PEDIR A APROVAÇÃO — TOM SOLÍCITO, NUNCA COBRANÇA: é PROIBIDO fechar com pressão do tipo "então vai fechar?", "vai aprovar?", "e aí, fechou?", "posso considerar aprovado?". O fechamento correto é se colocar à disposição e mostrar o benefício da rapidez. Fórmula: [fala do script/condição] + "Fico no aguardo da sua aprovação para prosseguir com o conserto." + previsão de entrega conforme a JANELA COMERCIAL informada no contexto:
   - JANELA ABERTA (horário comercial agora): "Aprovando hoje, acredito que entre hoje e amanhã mesmo a gente já consegue te entregar."
   - JANELA FECHADA (fora do horário): "Com a sua aprovação, acredito que amanhã mesmo a gente já consegue te entregar."
   Sempre gentil, sem urgência artificial, sem repetir a pergunta de fechamento na mesma mensagem. Se o cliente ficar em silêncio, quem retoma é o motor de reativação — não insista dentro da mesma conversa.
5) NEGOCIAÇÃO DO ORÇAMENTO — 5 FASES SEQUENCIAIS. Os textos das fases abaixo são MODELOS OFICIAIS: use-os como escritos (só preenchendo valores), sem reescrever com suas palavras. ⚠️ PRÉ-CONDIÇÃO ABSOLUTA: a negociação SÓ COMEÇA depois que o orçamento OFICIAL existir no contexto (campo orcamento/textoOrcamento vindo do diagnóstico feito na loja) E for enviado ao cliente. NUNCA invente, estime ou negocie valores antes disso — se o cliente pedir valor antes do diagnóstico, use a resposta padrão de preço ("só após avaliação"). O ciclo real: equipamento chega → técnico diagnostica → orçamento gerado na seção Orçamentos → orçamento enviado ao cliente (reabrindo a janela se preciso) → AÍ SIM as fases abaixo (avance UMA fase por vez, só quando o cliente NÃO aprovar ou pedir desconto):
   F1. Envio do orçamento do sistema (use o textoOrcamento do contexto se existir — é o orçamento oficial gerado no diagnóstico).
   F2. Pix: "(Nome), sendo no Pix consigo fazer por (valor com 5% de desconto), pois só trabalhamos com peças originais, fazemos revisão completa, damos certificado de garantia e buscamos e entregamos no seu endereço. Após o conserto ficará tão bom quanto o novo — usamos as mesmas peças do fabricante."
   F3. Balcão: "Buscando aqui na loja consigo a mesma condição de balcão, retirando o frete: fica por (valor da F2 com MAIS 5% de desconto) apenas. Estamos na Rua Ouro Preto, 663 - Barro Preto e deixamos pronto entre hoje e amanhã." — ATENÇÃO AO CÁLCULO: o desconto do balcão é 5% EM CIMA DO VALOR JÁ COM PIX (cascata). Ex: orçamento R$390 → Pix R$370 → balcão 5% sobre R$370 = R$351. NUNCA aplique os 5% do balcão sobre o valor original.
   F4. Troca: "Se estiver pensando em trocar por um mais em conta, temos vendas também — consigo desconto ficando com o seu na troca. Nosso catálogo: https://reparoeletroadm.com/equipamentos" (desconto padrão de R$50 na troca; se questionarem o valor, explique: temos que consertar, dar garantia, pagar imposto, taxa de maquininha, frete).
   F5. Compra: "Tem interesse em nos VENDER o seu equipamento? Nossa equipe avalia e passa uma proposta em breve." → se o cliente ACEITAR vender, use registrar_conflito com o motivo COMEÇANDO com "ANÁLISE DE COMPRA:" seguido do equipamento e do que o cliente falou (ex: "ANÁLISE DE COMPRA: micro-ondas Electrolux, cliente aceita vender"). NÃO peça foto ao cliente: o equipamento já está na nossa loja e a equipe fotografa aqui.
6) OBJEÇÃO "caro / pelo preço compro um novo" — pesquise mentalmente o preço REAL de um equipamento novo EQUIVALENTE ao modelo dele (mesma categoria/qualidade — não o modelo de entrada) e mostre a conta da economia: "um equivalente novo sai por ~R$X; consertando você economiza R$Y". Seja honesto se não souber o modelo exato: peça o modelo ou use a faixa da categoria. O "novo barato" é categoria inferior (iPhone vs celular de entrada). ${cfg.argumentoNovo}
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
      const _txts = (j.content || []).filter(b => b.type === 'text');
      const texto = (_txts.length ? _txts[_txts.length - 1].text : '') || '';
      let sug;
      try { sug = JSON.parse(texto.replace(/```json|```/g, '').trim()); }
      catch { sug = { resposta: texto.slice(0, 800), acao: { tipo: 'nenhuma', motivo: 'parse' }, confianca: 'baixa' }; }
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
          if (fichaX) {
            const logX = (await dbGet('reparoeletro_logistica')) || { fichas: [] };
            const jaLog = (logX.fichas || []).some(f => String(f.telefone || '').replace(/\D/g, '').slice(-8) === d8x && f.phase !== 'orc_registrado');
            if (!jaLog) {
              logX.fichas.unshift({
                id: 'log_' + Date.now().toString(36),
                nome: fichaX.nome, telefone: fichaX.telefone, endereco: fichaX.endereco || '',
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
            }
          }
          }
        }
        if (autorizado && acaoAprovada === 'registrar_conflito') {
          const KCF = (process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim();
          const [fdbC, fdbCtv] = await Promise.all([dbGet('fichas_adm'), dbGet('fichas_tv')]);
          const acha = b => (((b || {}).fichas) || []).find(f => String(f.telefone || '').replace(/\D/g, '').slice(-8) === d8x);
          const fichaC = acha(fdbC) || acha(fdbCtv);
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
              equipamento: (fichaC && fichaC.equipamento) || '',
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
                  tel: String(tel).replace(/\D/g, ''), equipamento: (fichaC && fichaC.equipamento) || '',
                  obs: String(acaoMotivo || '').slice(0, 200), temFoto: temFotoCompra }),
              });
            } catch (e) {}
          }
          // KPI conta CONFLITO CRIADO — não tentativa nem repetição do mesmo cliente (dedupe)
          if (respCf && respCf.criado) await bumpStat('conflitos');
          }
        }
        if (autorizado && acaoAprovada === 'mover_aprovado') {
          // TV aprova no sistema TV; ADM aprova no pipe ADM
          const tvLogX = (await dbGet('tv_logistica')) || { fichas: [] };
          const fichaTvX = (tvLogX.fichas || []).find(f => String(f.telefone || '').replace(/\D/g, '').slice(-8) === d8x &&
            ['orc_enviado', 'orc_registrado'].includes(f.phase));
          if (fichaTvX) {
            const KTV = (process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim();
            await fetch(`https://reparoeletroadm.com/api/tv-logistica?action=aprovar-orcamento&k=${KTV}`, {
              method: 'POST', headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ id: fichaTvX.id }),
            });
            await bumpStat('aprovacoes');
          }
          const ppX = (await dbGet('reparoeletro_pipe')) || { cards: [] };
          if (fichaTvX) { /* já aprovado no TV — não mexe no pipe ADM */ } else {
          const cardX = (ppX.cards || []).find(c => String(c.telefone || '').replace(/\D/g, '').slice(-8) === d8x && c.phase !== 'aprovados');
          if (cardX) {
            // Valor combinado na negociação (regra do Fluxo Bot Vendas) — antes do mover oficial
            const mV = String(acaoMotivo || '').match(/R?\$?\s?(\d{2,5})(?:[.,](\d{2}))?/);
            if (mV) {
              const vComb = parseFloat(mV[1] + (mV[2] ? '.' + mV[2] : ''));
              if (vComb >= 30 && vComb <= 20000) { cardX.valor = vComb; cardX.valorCombinadoBot = true; await dbSet('reparoeletro_pipe', ppX); }
            }
            // Mover pela ACTION OFICIAL do pipe → dispara os mesmos gatilhos do mover manual (sessão técnico etc.)
            const KMV = (process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim();
            await fetch(`https://reparoeletroadm.com/api/pipe?action=mover&k=${KMV}`, {
              method: 'POST', headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ id: cardX.id, phase: 'aprovados' }),
            });
            await bumpStat('aprovacoes');
            // ESPELHOS: outros cards do MESMO telefone ainda em aguardando/última chamada são duplicatas → arquivar
            try {
              const ppE = (await dbGet('reparoeletro_pipe')) || { cards: [] };
              const espelhos = (ppE.cards || []).filter(c => c.id !== cardX.id &&
                String(c.telefone || '').replace(/\D/g, '').slice(-8) === d8x &&
                ['aguardando_aprovacao', 'ultima_chamada'].includes(c.phaseId || c.phase));
              if (espelhos.length) {
                const arqE = (await dbGet('pipe_ids_arquivados')) || { ids: [] };
                for (const e of espelhos) if (!arqE.ids.includes(e.id)) arqE.ids.push(e.id);
                await dbSet('pipe_ids_arquivados', arqE);
                ppE.cards = ppE.cards.filter(c => !espelhos.some(e => e.id === c.id));
                await dbSet('reparoeletro_pipe', ppE);
              }
            } catch (e) {}
          }
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
