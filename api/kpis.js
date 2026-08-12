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
  let de, ate = hojeStr, rotulo = '';
  if (p === 'personalizado' && q.de && q.ate) {
    de = String(q.de).slice(0, 10); ate = String(q.ate).slice(0, 10);
    rotulo = 'de ' + de.split('-').reverse().join('/') + ' a ' + ate.split('-').reverse().join('/');
  } else if (p === 'semana') {
    const ds = hoje.getUTCDay();
    const seg = new Date(hoje); seg.setUTCDate(hoje.getUTCDate() - ((ds === 0) ? 6 : (ds - 1)));
    de = seg.toISOString().slice(0, 10); rotulo = 'esta semana (desde segunda)';
  } else if (p === 'mes') {
    de = hojeStr.slice(0, 8) + '01'; rotulo = 'este mês';
  } else { de = hojeStr; rotulo = 'hoje'; }
  return { de, ate, rotulo, periodo: p,
    ini: new Date(de + 'T00:00:00-03:00').getTime(),
    fim: new Date(ate + 'T23:59:59-03:00').getTime() };
}

// gasto real na Meta no período, separado por frente
async function investimento(de, ate) {
  const vazio = { adm: 0, tv: 0, total: 0, fonte: 'sem token da Meta' };
  if (!TOKEN) return vazio;
  try {
    const url = `${GRAPH}/act_${CONTA}/insights`
      + `?level=campaign&fields=campaign_name,spend`
      + `&time_range=${encodeURIComponent(JSON.stringify({ since: de, until: ate }))}`
      + `&limit=500&access_token=${TOKEN}`;
    const r = await fetch(url).then(x => x.json());
    if (!r || !r.data) return { ...vazio, fonte: (r && r.error && r.error.message) || 'sem retorno' };
    let adm = 0, tv = 0;
    for (const c of r.data) {
      const v = parseFloat(c.spend || 0) || 0;
      if (/\btv\b|televis|tela |led |barramento|apagad/i.test(String(c.campaign_name || ''))) tv += v;
      else adm += v;
    }
    return { adm: +adm.toFixed(2), tv: +tv.toFixed(2), total: +(adm + tv).toFixed(2), fonte: 'Meta Ads' };
  } catch (e) { return { ...vazio, fonte: e.message }; }
}

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
    const aprov = cards.filter(c => c.aprovadoEm && dentro(c.aprovadoEm));
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
    periodo: { de: J.de, ate: J.ate, rotulo: J.rotulo, tipo: J.periodo },
    geradoEm: new Date(Date.now() - 3 * 3600000).toISOString().slice(0, 16).replace('T', ' ') + ' BRT',
    fonteInvestimento: inv.fonte,
    DIAGNOSTICO: {
      tokenDaMetaConfigurado: !!TOKEN,
      contaDeAnuncios: CONTA ? 'act_' + CONTA : '(não configurada)',
      eventosLidos: listaEv.length,
      conversasNoPeriodo: conversasNoPeriodo.length,
    },
    ADM: enriquecer(adm, inv.adm, convAdm),
    TV: enriquecer(tv, inv.tv, convTv),
    TOTAL: {
      investimento: inv.total,
      faturamento: +(adm.faturamento + tv.faturamento).toFixed(2),
      aprovados: adm.aprovados.total + tv.aprovados.total,
      retorno: inv.total ? +((adm.faturamento + tv.faturamento) / inv.total).toFixed(2) : 0,
    } });
};
