// ═══════════════════════════════════════════════════════════════════
// KPIs — o funil inteiro, do dinheiro investido até o faturamento.
// Investimento → Conversas → Fichas → Logística → Orçamentos → Aprovados
// ═══════════════════════════════════════════════════════════════════
const U = (process.env.UPSTASH_URL || '').replace(/['"]/g, '').trim();
const T = (process.env.UPSTASH_TOKEN || '').replace(/[\n\r'"]/g, '').trim();
const GRAPH = 'https://graph.facebook.com/v20.0';
// 🔑 os nomes usados no projeto são META_ADS_ACCOUNT e META_ADS_TOKEN — com os
// nomes errados o investimento vinha sempre zerado
const CONTA = String(process.env.META_ADS_ACCOUNT || '1267284360833794').trim().replace(/^act_/, '');
const TOKEN = String(process.env.META_ADS_TOKEN || '').trim();

async function dbGet(k) {
  try {
    const r = await fetch(`${U}/get/${k}`, { headers: { Authorization: `Bearer ${T}` } }).then(x => x.json());
    return r && r.result ? JSON.parse(r.result) : null;
  } catch (e) { return null; }
}
// 📨 os eventos do WhatsApp ficam numa LISTA do Redis, não numa chave comum
async function lerEventos() {
  try {
    const r = await fetch(`${U}/lrange/wa_evt_list/-8000/-1`,
      { headers: { Authorization: `Bearer ${T}` } }).then(x => x.json());
    const out = [];
    for (const s of (r.result || [])) { try { out.push(JSON.parse(s)); } catch (e) {} }
    return out;
  } catch (e) { return []; }
}
const d8 = t => String(t || '').replace(/\D/g, '').slice(-8);
const soDia = d => String(d || '').slice(0, 10);

// janela pedida, sempre em horário de Brasília
function janela(q) {
  const hoje = new Date(Date.now() - 3 * 3600000);
  const hojeStr = hoje.toISOString().slice(0, 10);
  const p = String(q.periodo || 'dia');
  let de, ate = hojeStr, rotulo = '', cicloSabado = null;
  if (p === 'personalizado' && q.de && q.ate) {
    de = String(q.de).slice(0, 10); ate = String(q.ate).slice(0, 10);
    rotulo = 'de ' + de.split('-').reverse().join('/') + ' a ' + ate.split('-').reverse().join('/');
  } else if (p === 'semana') {
    // 📅 o ciclo comercial vai de sábado 13h ao sábado seguinte 13h — é o intervalo
    // em que a verba é montada e consumida, então investimento e faturamento
    // precisam ser medidos nessa mesma janela
    const agoraBR = hoje;                       // já está em horário de Brasília
    const dia = agoraBR.getUTCDay();            // 6 = sábado
    const hora = agoraBR.getUTCHours() + agoraBR.getUTCMinutes() / 60;
    let diasDesdeSabado = (dia - 6 + 7) % 7;    // quantos dias desde o último sábado
    if (dia === 6 && hora < 13) diasDesdeSabado = 7;   // sábado antes das 13h ainda é o ciclo anterior
    const inicio = new Date(agoraBR);
    inicio.setUTCDate(agoraBR.getUTCDate() - diasDesdeSabado);
    de = inicio.toISOString().slice(0, 10);
    cicloSabado = { de, hIni: 13, hFim: 13 };
    rotulo = 'ciclo de ' + de.slice(8) + '/' + de.slice(5, 7) + ' 13h até sábado 13h';
  } else if (p === 'mes') {
    de = hojeStr.slice(0, 8) + '01'; rotulo = 'este mês';
  } else { de = hojeStr; rotulo = 'hoje'; }
  // no ciclo de sábado a hora importa: começa às 13h e termina 7 dias depois às 13h
  if (cicloSabado) {
    const ini = new Date(cicloSabado.de + 'T13:00:00-03:00').getTime();
    return { de, ate, rotulo, periodo: p, cicloSabado: true,
      ini, fim: Math.min(Date.now(), ini + 7 * 86400000) };
  }
  return { de, ate, rotulo, periodo: p, cicloSabado: false,
    ini: new Date(de + 'T00:00:00-03:00').getTime(),
    fim: new Date(ate + 'T23:59:59-03:00').getTime() };
}

// gasto real na Meta no período, separado por frente
// 💰 gasto e conversas iniciadas no período, direto da Meta.
// O relatório por período traz TODA campanha que teve entrega naquelas datas,
// inclusive as já pausadas ou encerradas depois — é o gasto real do período,
// não o que está ativo agora.
const ehTvNome = n => /\btv\b|televis|tela|led|barramento|apagad|imagem/i.test(String(n || ''));

async function investimento(de, ate) {
  const vazio = { adm: 0, tv: 0, total: 0,
    convAdm: 0, convTv: 0, convTotal: 0,
    fonte: 'conta de anúncios não conectada', campanhas: 0 };
  if (!TOKEN || !CONTA) return vazio;
  try {
    // 📨 a Meta informa quantas conversas cada campanha iniciou
    const CONV = ['onsite_conversion.messaging_conversation_started_7d',
      'onsite_conversion.total_messaging_connection'];
    let url = `${GRAPH}/act_${CONTA}/insights`
      + `?level=campaign&fields=campaign_name,spend,actions`
      + `&time_range=${encodeURIComponent(JSON.stringify({ since: de, until: ate }))}`
      + `&action_report_time=conversion&use_account_attribution_setting=true`
      + `&limit=500&access_token=${TOKEN}`;
    let adm = 0, tv = 0, cAdm = 0, cTv = 0, n = 0;
    let paginas = 0;
    while (url && paginas < 6) {
      const r = await fetch(url).then(x => x.json());
      if (!r || r.error) return { ...vazio, fonte: (r && r.error && r.error.message) || 'sem retorno' };
      for (const c of (r.data || [])) {
        n++;
        const gasto = parseFloat(c.spend || 0) || 0;
        let conv = 0;
        for (const a of (c.actions || [])) {
          if (CONV.includes(String(a.action_type))) { conv += parseInt(a.value || 0, 10) || 0; break; }
        }
        if (ehTvNome(c.campaign_name)) { tv += gasto; cTv += conv; }
        else { adm += gasto; cAdm += conv; }
      }
      url = (r.paging && r.paging.next) || null;
      paginas++;
    }
    return { adm: +adm.toFixed(2), tv: +tv.toFixed(2), total: +(adm + tv).toFixed(2),
      convAdm: cAdm, convTv: cTv, convTotal: cAdm + cTv,
      campanhas: n, fonte: 'Meta Ads · ' + n + ' campanha(s) com entrega no período' };
  } catch (e) { return { ...vazio, fonte: e.message }; }
}

// 📆 desde quando cada fonte tem dado confiável
async function desdeQuando() {
  const r = { };
  const menor = arr => arr.filter(Boolean).sort()[0] || null;
  try {
    const [fa, ft, ppA, ppT, lgA] = await Promise.all([
      dbGet('fichas_adm'), dbGet('fichas_tv'),
      dbGet('reparoeletro_pipe'), dbGet('tv_pipe'), dbGet('reparoeletro_logistica'),
    ]);
    r.fichas = menor([...(((fa || {}).fichas) || []), ...(((ft || {}).fichas) || [])]
      .map(f => String(f.criadoEm || f.registradoEm || '').slice(0, 10)));
    r.logistica = menor((((lg = lgA) || {}).fichas || []).map(f => String(f.criadoEm || '').slice(0, 10)));
    const cards = [...(((ppA || {}).cards) || []), ...(((ppT || {}).cards) || [])];
    r.aprovacoes = menor(cards.map(c => String(c.aprovadoEm || '').slice(0, 10)));
    r.cards = menor(cards.map(c => String(c.criadoEm || '').slice(0, 10)));
  } catch (e) {}
  try {
    const ev = await fetch(`${U}/lrange/wa_evt_list/0/0`,
      { headers: { Authorization: `Bearer ${T}` } }).then(x => x.json());
    const primeiro = (ev.result || [])[0];
    if (primeiro) { const e = JSON.parse(primeiro); r.conversas = String(e.ts || '').slice(0, 10); }
    const tam = await fetch(`${U}/llen/wa_evt_list`,
      { headers: { Authorization: `Bearer ${T}` } }).then(x => x.json());
    r.mensagensGuardadas = (tam && tam.result) || 0;
  } catch (e) {}
  return r;
}
let lg;

module.exports = async function handler(req, res) {
  const _k = (req.query && req.query.k) || req.headers['x-tf-key'] || '';
  if (_k !== ((process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim())) {
    return res.status(401).json({ ok: false, error: 'não autorizado' });
  }
  res.setHeader('Access-Control-Allow-Origin', 'https://reparoeletroadm.com');
  const J = janela(req.query || {});
  const dentro = d => { const t = new Date(d || 0).getTime(); return t >= J.ini && t <= J.fim; };

  const [fA, fT, lgA, lgT, ppA, ppT, evts, inv] = await Promise.all([
    dbGet('fichas_adm'), dbGet('fichas_tv'),
    dbGet('reparoeletro_logistica'), dbGet('tv_logistica'),
    dbGet('reparoeletro_pipe'), dbGet('tv_pipe'),
    lerEventos(), investimento(J.de, J.ate),
  ]);

  // conversas iniciadas: telefones cujo PRIMEIRO contato caiu no período
  const primeiraMsg = {};
  const listaEv = Array.isArray(evts) ? evts : [];
  for (const e of listaEv) {
    if (e.dir !== 'in') continue;
    const t = d8(e.tel); if (!t) continue;
    const q = new Date(e.ts || 0).getTime();
    if (!q) continue;
    if (!primeiraMsg[t] || q < primeiraMsg[t]) primeiraMsg[t] = q;
  }
  const conversasNoPeriodo = Object.entries(primeiraMsg)
    .filter(([, q]) => q >= J.ini && q <= J.fim).map(([t]) => t);
  const setConversas = new Set(conversasNoPeriodo);

  function montar(fichasDb, logDb, pipeDb, telsTv) {
    const ehRetorno = f => ['remarcar', 'reagendamento'].includes(String(f.origem || '')) ||
      f.reagendarColeta === true ||
      String(f.id || '').startsWith('rem_') || String(f.id || '').startsWith('fic_reag_');
    // fichas
    const fichas = (((fichasDb || {}).fichas) || [])
      .filter(f => dentro(f.criadoEm || f.registradoEm) && !ehRetorno(f));
    // logística
    const logs = (((logDb || {}).fichas) || []).filter(f => dentro(f.criadoEm));
    const porBot = logs.filter(f => /bot/i.test(String(f.origem || '') + ' ' + String(f.criadoPor || '')));
    // pipe: orçamentos e aprovados
    const cards = ((pipeDb || {}).cards) || [];
    const orcs = cards.filter(c => dentro(c.orcamentoEm || c.criadoEm) &&
      (Number(c.valor || 0) > 0 || c.orcamentoEm));
    // ✅ contam os que PASSARAM pela aprovação no período, mesmo que já tenham
    // seguido para produção, entrega ou finalizado — olhar só quem está parado
    // na coluna hoje esconderia todo card que avançou depois
    const quandoAprovou = c => {
      if (c.aprovadoEm) return new Date(c.aprovadoEm).getTime();
      const h = (c.history || [])
        .filter(x => String(x.phase || x.phaseId || '') === 'aprovados')
        .map(x => new Date(x.ts || x.timestamp || 0).getTime())
        .filter(Boolean).sort((a, b) => a - b);
      return h.length ? h[0] : 0;
    };
    const aprov = cards.filter(c => {
      const t = quandoAprovou(c);
      return t && t >= J.ini && t <= J.fim;
    });
    const aprovBot = aprov.filter(c => /bot/i.test(String(c.aprovadoPor || '')));
    const faturamento = aprov.reduce((s, c) => s + (Number(c.valor || 0) || 0), 0);
    return {
      fichas: fichas.length,
      logistica: { total: logs.length, bot: porBot.length, manual: logs.length - porBot.length,
        pctBot: logs.length ? Math.round(porBot.length / logs.length * 100) : 0 },
      orcamentos: orcs.length,
      aprovados: { total: aprov.length, bot: aprovBot.length, manual: aprov.length - aprovBot.length,
        pctBot: aprov.length ? Math.round(aprovBot.length / aprov.length * 100) : 0 },
      faturamento: +faturamento.toFixed(2),
      ticketMedio: aprov.length ? +(faturamento / aprov.length).toFixed(2) : 0,
    };
  }

  // conversas por frente: pelo telefone da ficha correspondente
  const telFichasTv = new Set((((fT || {}).fichas) || []).map(f => d8(f.telefone)));
  const convTv = conversasNoPeriodo.filter(t => telFichasTv.has(t)).length;
  const convAdm = conversasNoPeriodo.length - convTv;

  const adm = montar(fA, lgA, ppA);
  const tv = montar(fT, lgT, ppT);
  // 📨 a contagem da Meta é a oficial de conversas iniciadas pelo tráfego;
  // o histórico do WhatsApp serve de reserva quando a Meta não responde
  const usarMeta = inv.convTotal > 0;
  const convAdmFinal = usarMeta ? inv.convAdm : convAdm;
  const convTvFinal = usarMeta ? inv.convTv : convTv;
  const taxa = (a, b) => b ? Math.round(a / b * 100) : 0;
  const custo = (v, n) => n ? +(v / n).toFixed(2) : 0;

  const enriquecer = (d, gasto, conversas) => ({
    investimento: gasto,
    conversas,
    custoPorConversa: custo(gasto, conversas),
    fichas: d.fichas,
    custoPorFicha: custo(gasto, d.fichas),
    conversaParaFicha: taxa(d.fichas, conversas),
    logistica: d.logistica,
    fichaParaLogistica: taxa(d.logistica.total, d.fichas),
    orcamentos: d.orcamentos,
    logisticaParaOrcamento: taxa(d.orcamentos, d.logistica.total),
    aprovados: d.aprovados,
    orcamentoParaAprovado: taxa(d.aprovados.total, d.orcamentos),
    faturamento: d.faturamento,
    ticketMedio: d.ticketMedio,
    custoPorAprovado: custo(gasto, d.aprovados.total),
    retorno: gasto ? +(d.faturamento / gasto).toFixed(2) : 0,
  });

  return res.status(200).json({ ok: true,
    periodo: { de: J.de, ate: J.ate, rotulo: J.rotulo, tipo: J.periodo,
      cicloFechado: !!J.cicloSabado,
      inicioExato: new Date(J.ini - 3 * 3600000).toISOString().slice(0, 16).replace('T', ' ') + ' BRT',
      fimExato: new Date(J.fim - 3 * 3600000).toISOString().slice(0, 16).replace('T', ' ') + ' BRT' },
    geradoEm: new Date(Date.now() - 3 * 3600000).toISOString().slice(0, 16).replace('T', ' ') + ' BRT',
    fonteInvestimento: inv.fonte,
    DIAGNOSTICO: {
      tokenDaMetaConfigurado: !!TOKEN,
      contaDeAnuncios: CONTA ? 'act_' + CONTA : '(não configurada)',
      eventosLidos: listaEv.length,
      conversasNoPeriodo: conversasNoPeriodo.length,
    },
    // 📖 de onde sai cada número, para conferência
    DESDE_QUANDO: await desdeQuando(),
    FONTES: {
      investimento: 'Meta Ads · gasto real das datas escolhidas, incluindo campanhas já pausadas ou encerradas depois · campanha com TV, televisão, tela, LED ou barramento no nome conta como TV; o resto como ADM',
      conversas: usarMeta
        ? 'Meta Ads · conversas iniciadas pelo anúncio no período, atribuídas à campanha que as gerou'
        : 'histórico do WhatsApp · primeiro contato de cada telefone no período (a Meta não retornou dados)',
      conversasPorFrente: usarMeta ? 'pela campanha que gerou a conversa'
        : 'a conversa é de TV se o telefone tiver ficha em fichas_tv',
      fichas: 'fichas_adm e fichas_tv · pela data de criação · não conta retorno do remarcar, que já foi contado na primeira entrada',
      logistica: 'reparoeletro_logistica e tv_logistica · pela data de criação · é do bot quando a origem ou quem cadastrou menciona bot',
      orcamentos: 'cards do pipe com data de orçamento ou valor preenchido, dentro do período',
      aprovados: 'cards que PASSARAM pela fase de aprovação dentro do período, pelo carimbo ou pelo histórico — inclui os que já avançaram para produção, entrega ou finalizado',
      faturamento: 'soma do valor desses mesmos cards, na data em que passaram pela aprovação',
      custoPorAprovado: 'investimento ÷ aprovados',
      atencao: 'as etapas medem coisas que acontecem em momentos diferentes: uma ficha criada hoje pode ser aprovada semana que vem, então as taxas entre etapas não são de um mesmo grupo de clientes',
    },
    origemDasConversas: usarMeta ? 'Meta Ads (conversas iniciadas pelo anúncio)'
      : 'histórico do WhatsApp (primeiro contato de cada cliente)',
    ADM: enriquecer(adm, inv.adm, convAdmFinal),
    TV: enriquecer(tv, inv.tv, convTvFinal),
    TOTAL: {
      investimento: inv.total,
      faturamento: +(adm.faturamento + tv.faturamento).toFixed(2),
      aprovados: adm.aprovados.total + tv.aprovados.total,
      retorno: inv.total ? +((adm.faturamento + tv.faturamento) / inv.total).toFixed(2) : 0,
    } });
};
