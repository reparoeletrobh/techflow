// ═══ 📈 TRÁFEGO — Conector Meta Ads (Fase 2, leitura) ═══
const U = (process.env.UPSTASH_URL || process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL || '').replace(/['"]/g, '').trim();
const T = (process.env.UPSTASH_TOKEN || process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN || '').replace(/[\n\r'"]/g, '').trim();
async function dbGet(k) {
  const r = await fetch(`${U}/get/${k}`, { headers: { Authorization: `Bearer ${T}` } }).then(x => x.json()).catch(() => null);
  try { return r && r.result ? JSON.parse(r.result) : null; } catch (e) { return null; }
}
async function dbSet(k, v) {
  await fetch(`${U}/set/${k}`, { method: 'POST', headers: { Authorization: `Bearer ${T}`, 'Content-Type': 'application/json' }, body: JSON.stringify(v) });
}
const GRAPH = 'https://graph.facebook.com/v20.0';
function brlS(v) { return 'R$ ' + Number(v || 0).toFixed(2).replace('.', ','); }

// Busca paginada com páginas PEQUENAS (a Meta recusa requisições grandes:
// "Please reduce the amount of data you're asking for")
async function pegarTudo(url, maxPaginas) {
  const out = []; let prox = url, n = 0, erro = null;
  while (prox && n < (maxPaginas || 6)) {
    const j = await fetch(prox).then(x => x.json()).catch(e => ({ error: { message: e.message } }));
    if (j && j.error) { erro = j.error.message; break; }
    for (const d of ((j || {}).data || [])) out.push(d);
    prox = ((j || {}).paging || {}).next || null;
    n++;
    if (prox) await new Promise(r => setTimeout(r, 120)); // respiro entre páginas
  }
  return { data: out, erro };
}

module.exports = async function handler(req, res) {
  try {
  res.setHeader('Access-Control-Allow-Origin', '*');
  const action = (req.query.action || '').trim();
  const _tfk = (req.query && req.query.k) || req.headers['x-tf-key'] || '';
  if (_tfk !== ((process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim())) {
    return res.status(401).json({ ok: false, error: 'não autorizado' });
  }
  const TOKEN = (process.env.META_ADS_TOKEN || '').trim();
  const CONTA = String(process.env.META_ADS_ACCOUNT || '').trim().replace(/^act_/, '');
  if (!TOKEN) return res.status(200).json({ ok: false, error: 'META_ADS_TOKEN não configurado na Vercel (Settings → Environment Variables → Redeploy)' });

  // ── TESTE + AUTODIAGNÓSTICO: valida o token e lista as contas acessíveis ──
  if (action === 'meta-teste') {
    const eu = await fetch(`${GRAPH}/me?fields=id,name&access_token=${TOKEN}`).then(x => x.json()).catch(e => ({ error: { message: e.message } }));
    if (eu.error) return res.status(200).json({ ok: false, passo: 'token', erro: eu.error.message, dica: 'token inválido/expirado — gera outro no usuário do sistema' });
    const contas = await fetch(`${GRAPH}/me/adaccounts?fields=id,name,account_status,currency,amount_spent&access_token=${TOKEN}`).then(x => x.json()).catch(e => ({ error: { message: e.message } }));
    if (contas.error) return res.status(200).json({ ok: false, passo: 'adaccounts', erro: contas.error.message, dica: 'o usuário do sistema precisa da conta de anúncios atribuída (Gerenciar campanhas) + permissão ads_read no token' });
    const lista = (contas.data || []).map(c => ({ id: c.id, nome: c.name, status: c.account_status === 1 ? 'ativa' : 'status ' + c.account_status, moeda: c.currency, gastoTotal: c.amount_spent }));
    const configuradaOk = lista.some(c => c.id === 'act_' + CONTA);
    return res.status(200).json({ ok: true,
      tokenValido: true, usuarioSistema: eu.name || eu.id,
      contaConfigurada: CONTA ? 'act_' + CONTA : '(META_ADS_ACCOUNT vazio)',
      contaConfiguradaEstaAcessivel: configuradaOk,
      veredito: configuradaOk ? '✅ TUDO CERTO — pode seguir' : '⚠️ o ID configurado não está entre as contas do token — usa um dos IDs da lista abaixo (o número após act_)',
      contasAcessiveis: lista });
  }

  // ═══ CONFIG: metas de custo por conversa e verba semanal ═══
  const CFG_PADRAO = {
    metas: { tv: 2, microondas: 5, forno: 5, purificador: 8, adega: 10, institucional: 8, outros: 8 },
    verba: { adm: 5000, tv: 500, aproveitamento: 0.87 },
    cicloInicio: { diaSemana: 6, hora: 13 }, // sábado 13h
  };
  async function cfgTrafego() {
    const c = (await dbGet('trafego_config')) || {};
    return {
      metas: Object.assign({}, CFG_PADRAO.metas, c.metas || {}),
      verba: Object.assign({}, CFG_PADRAO.verba, c.verba || {}),
      cicloInicio: Object.assign({}, CFG_PADRAO.cicloInicio, c.cicloInicio || {}),
      apelidos: c.apelidos || {},
      custoViagem: c.custoViagem != null ? c.custoViagem : 25,
    };
  }

  // Apelidos ensinados pelo dono (trafego_config.apelidos): { "reforma": "microondas", ... }
  let APELIDOS = {};
  function categoriaDe(nome) {
    const s = String(nome || '').toLowerCase();
    for (const termo of Object.keys(APELIDOS)) {         // o que você ensinou vem primeiro
      if (termo && s.includes(termo)) return APELIDOS[termo];
    }
    if (/tvs?\b|\btvs?\d|televis|barramento|tela quebrad|quebrar tv|polegada/.test(s)) return 'tv';
    if (/micro-?\s?ondas|reforma|\binflu\b|\bantigo\b/.test(s)) return 'microondas';
    if (/\bforn(o|inho)/.test(s)) return 'forno';
    if (/purificador|bebedouro|\bfiltro\b|vela|[áa]gua/.test(s)) return 'purificador';
    if (/adega|cervejeir|climatiz|vinho/.test(s)) return 'adega';
    if (/criativo loja|reuniao externa|reunião externa|\bnovo\b|vitrine|institucional/.test(s)) return 'institucional';
    return 'outros';
  }
  try { const _c = await cfgTrafego(); APELIDOS = _c.apelidos || {}; } catch (e) {}
  // Início do ciclo (último sábado 13h, horário de Brasília) em data ISO
  function inicioCiclo(cfg) {
    const agoraBrt = new Date(Date.now() - 3 * 3600 * 1000);
    const d = new Date(agoraBrt);
    const diff = (d.getUTCDay() - cfg.cicloInicio.diaSemana + 7) % 7;
    d.setUTCDate(d.getUTCDate() - diff);
    if (diff === 0 && agoraBrt.getUTCHours() < cfg.cicloInicio.hora) d.setUTCDate(d.getUTCDate() - 7);
    return d.toISOString().slice(0, 10);
  }

  if (action === 'config') {
    if (req.method === 'POST') {
      const atual = (await dbGet('trafego_config')) || {};
      const b = req.body || {};
      if (b.metas) atual.metas = Object.assign({}, atual.metas || {}, b.metas);
      if (b.verba) atual.verba = Object.assign({}, atual.verba || {}, b.verba);
      if (b.apelidos) atual.apelidos = Object.assign({}, atual.apelidos || {}, b.apelidos);
      if (b.custoViagem != null) atual.custoViagem = Number(b.custoViagem);
      await dbSet('trafego_config', atual);
      return res.status(200).json({ ok: true, config: await cfgTrafego() });
    }
    return res.status(200).json({ ok: true, config: await cfgTrafego() });
  }

  // ═══ 📊 PAINEL: anúncios com criativo, verba e desempenho do ciclo ═══
  if (action === 'painel') {
    if (!CONTA) return res.status(200).json({ ok: false, error: 'META_ADS_ACCOUNT não configurado' });
    const cfg = await cfgTrafego();
    const desde = inicioCiclo(cfg);
    const hoje = new Date(Date.now() - 3 * 3600 * 1000).toISOString().slice(0, 10);
    const periodo = ['hoje', '7d', 'ciclo'].includes(String(req.query.periodo || '')) ? String(req.query.periodo) : 'ciclo';
    const soAtivos = String(req.query.todos || '') !== '1';
    // Janela idêntica à do Gerenciador de Anúncios
    const janela = periodo === 'hoje' ? 'date_preset=today'
      : periodo === '7d' ? 'date_preset=last_7d'
      : 'time_range=' + encodeURIComponent(JSON.stringify({ since: desde, until: hoje }));
    const chaveCache = 'trafego_painel_cache_' + periodo + (soAtivos ? '' : '_todos');
    const cache = await dbGet(chaveCache);
    if (cache && cache.em && (Date.now() - new Date(cache.em).getTime() < 30 * 60000)
        && String(req.query.forcar || '') !== '1' && cache.desde === desde) {
      return res.status(200).json(Object.assign({}, cache.dados, { cacheDe: cache.em, doCache: true }));
    }
    // SOMENTE ANÚNCIOS ATIVOS + campos mínimos + páginas de 25 (evita o erro de volume da Meta)
    const filtroAtivo = soAtivos
      ? '&filtering=' + encodeURIComponent(JSON.stringify([{ field: 'effective_status', operator: 'IN', value: ['ACTIVE'] }]))
      : '';
    const base = `${GRAPH}/act_${CONTA}/`;
    const tk = `&access_token=${TOKEN}`;
    // SEQUENCIAL e com páginas grandes: a Meta limita o NÚMERO de chamadas, não o total de itens
    // 1º os anúncios ativos; 2º SÓ os pais deles, buscados por id (antes eu varria a conta inteira e
    // batia no teto de paginação antes de alcançar os conjuntos/campanhas certos)
    const ads = await pegarTudo(`${base}ads?fields=id,name,effective_status,adset_id,campaign_id,creative{thumbnail_url}&limit=100${filtroAtivo}${tk}`, 8);
    if (ads.erro && !ads.data.length) return res.status(200).json({ ok: false, erro: ads.erro });
    const idsAdset = [...new Set((ads.data || []).map(a => a.adset_id).filter(Boolean))];
    const idsCamp = [...new Set((ads.data || []).map(a => a.campaign_id).filter(Boolean))];
    const porLote = async (ids, campos) => {
      const out = []; let erro = null;
      for (let i = 0; i < ids.length; i += 50) {
        const lote = ids.slice(i, i + 50).join(',');
        const j = await fetch(`${GRAPH}/?ids=${lote}&fields=${campos}&access_token=${TOKEN}`)
          .then(x => x.json()).catch(e => ({ error: { message: e.message } }));
        if (j && j.error) { erro = j.error.message; break; }
        for (const k of Object.keys(j || {})) out.push(j[k]);
        if (i + 50 < ids.length) await new Promise(r => setTimeout(r, 120));
      }
      return { data: out, erro };
    };
    const adsets = await porLote(idsAdset, 'id,name,effective_status,daily_budget,lifetime_budget,end_time');
    if (adsets.erro && !adsets.data.length) {
      return res.status(200).json({ ok: false, erro: 'não consegui ler os conjuntos: ' + adsets.erro +
        (/limit reached/i.test(adsets.erro) ? ' — a Meta limitou as consultas por alguns minutos; tente de novo em instantes.' : '') });
    }
    const camps2 = await porLote(idsCamp, 'id,name,effective_status,daily_budget,lifetime_budget');
    const ins = await pegarTudo(`${base}insights?level=ad&${janela}&use_unified_attribution_setting=true&fields=ad_id,spend,clicks,ctr,actions&limit=100${tk}`, 8);
    const camps = camps2;
    const porAd = {};
    for (const i of (ins.data || [])) porAd[i.ad_id] = i;
    const porAdset = {};
    for (const a of (adsets.data || [])) porAdset[a.id] = a;
    const porCamp = {};
    for (const c of (camps.data || [])) porCamp[c.id] = c;
    const cent = v => (Number(v) > 0 ? Number(v) / 100 : null);


    // Ordem de prioridade REAL: a métrica do Gerenciador é "conversas por mensagem iniciadas"
    const CONV = ['onsite_conversion.messaging_conversation_started_7d', 'onsite_conversion.messaging_first_reply', 'onsite_conversion.total_messaging_connection', 'lead'];
    const contaConversa = (acoes) => {
      for (const tipo of CONV) {                     // respeita a ordem: pega a primeira que existir
        const a = (acoes || []).find(x => x.action_type === tipo);
        if (a) return { valor: Number(a.value || 0), metrica: tipo };
      }
      return { valor: 0, metrica: null };
    };
    // Diagnóstico do que a Meta devolveu (o filtro dela nem sempre é respeitado)
    const porStatus = {};
    for (const ad of (ads.data || [])) {
      const s = ad.effective_status || 'SEM_STATUS';
      porStatus[s] = (porStatus[s] || 0) + 1;
    }
    // TRAVA local: anúncio ativo E conjunto ativo. Sem isso entram pausados e "ativos" de conjunto parado.
    // Só entra quem PROVA que está rodando nos três níveis (anúncio, conjunto e campanha).
    // Sem prova → fica de fora (antes eu deixava passar quando não sabia, e por isso inflava).
    const motivosCorte = {};
    const corta = (m) => { motivosCorte[m] = (motivosCorte[m] || 0) + 1; return false; };
    const agoraMs = Date.now();
    const brutos = (ads.data || []).filter(ad => {
      if (!soAtivos) return true;
      if (ad.effective_status !== 'ACTIVE') return corta('anúncio: ' + (ad.effective_status || 'sem status'));
      const st = porAdset[ad.adset_id];
      if (!st) return corta('conjunto não localizado');
      if (st.effective_status !== 'ACTIVE') return corta('conjunto: ' + st.effective_status);
      // ENCERRADO: a Meta mantém status ACTIVE depois que a veiculação termina — o prazo é quem decide
      if (st.end_time && new Date(st.end_time).getTime() < agoraMs) return corta('veiculação encerrada (prazo terminou)');
      return true;
    });
    const anuncios = brutos.map(ad => {
      const i = porAd[ad.id] || {};
      const st = porAdset[ad.adset_id] || {};
      const cv = contaConversa(i.actions);
      const conversas = cv.valor;
      const gasto = Number(i.spend || 0);
      const cat = categoriaDe(ad.name + ' ' + (st.name || ''));
      const meta = cfg.metas[cat] || cfg.metas.outros;
      const cpa = conversas > 0 ? gasto / conversas : null;
      // distância da meta: <1 abaixo (bom), >1 acima (ruim); sem conversa com gasto = pior caso
      const razao = cpa != null ? cpa / meta : (gasto > 0 ? 3 : null);
      let situacao = 'sem-dados';
      if (razao != null) situacao = razao <= 1 ? 'campeao' : (razao <= 1.3 ? 'atencao' : 'ralo');
      return {
        id: ad.id, nome: ad.name, ativo: ad.effective_status === 'ACTIVE',
        status: ad.effective_status,
        thumb: (ad.creative || {}).thumbnail_url || null,
        adsetId: ad.adset_id, adsetNome: st.name || '',
        orcamentoDiario: cent(st.daily_budget) || cent((porCamp[ad.campaign_id] || {}).daily_budget),
        orcamentoTotal: cent(st.lifetime_budget) || cent((porCamp[ad.campaign_id] || {}).lifetime_budget),
        verbaEm: cent(st.daily_budget) || cent(st.lifetime_budget) ? 'conjunto'
          : (cent((porCamp[ad.campaign_id] || {}).daily_budget) || cent((porCamp[ad.campaign_id] || {}).lifetime_budget) ? 'campanha' : 'desconhecida'),
        campanhaId: ad.campaign_id || null,
        terminaEm: st.end_time || null,
        categoria: cat, meta,
        gasto: Number(gasto.toFixed(2)), conversas, metricaConversa: cv.metrica,
        cpa: cpa != null ? Number(cpa.toFixed(2)) : null,
        razaoMeta: razao != null ? Number(razao.toFixed(2)) : null,
        cliques: Number(i.clicks || 0),
        ctr: i.ctr ? Number(Number(i.ctr).toFixed(2)) : null,
        situacao,
      };
    }).sort((a, b) => (a.razaoMeta == null ? 9 : a.razaoMeta) - (b.razaoMeta == null ? 9 : b.razaoMeta));

    // Termômetro da semana: verba real (87%) × gasto do ciclo
    const gastoTv = anuncios.filter(a => a.categoria === 'tv').reduce((s, a) => s + a.gasto, 0);
    const gastoAdm = anuncios.filter(a => a.categoria !== 'tv').reduce((s, a) => s + a.gasto, 0);
    const realAdm = cfg.verba.adm * cfg.verba.aproveitamento;
    const realTv = cfg.verba.tv * cfg.verba.aproveitamento;
    const porCategoria = {};
    for (const c of ['tv', 'microondas', 'forno', 'purificador', 'adega', 'institucional', 'outros']) {
      const lista = anuncios.filter(a => a.categoria === c && a.ativo);
      const gc = lista.reduce((s, a) => s + a.gasto, 0);
      const cc = lista.reduce((s, a) => s + a.conversas, 0);
      porCategoria[c] = { anuncios: lista.length, ativos: lista.filter(a => a.ativo).length,
        gasto: Number(gc.toFixed(2)), conversas: cc,
        cpa: cc > 0 ? Number((gc / cc).toFixed(2)) : null, meta: cfg.metas[c] || cfg.metas.outros,
        campeoes: lista.filter(a => a.situacao === 'campeao').length,
        ralos: lista.filter(a => a.situacao === 'ralo').length };
    }
    const ativos = anuncios.filter(a => a.ativo);
    const gastoTotal = anuncios.reduce((s, a) => s + a.gasto, 0);
    const convTotal = anuncios.reduce((s, a) => s + a.conversas, 0);
    const metricasVistas = [...new Set(anuncios.map(a => a.metricaConversa).filter(Boolean))];
    const dados = { ok: true, ciclo: { desde, ate: hoje },
      periodo,
      periodoLabel: periodo === 'hoje' ? 'Hoje' : periodo === '7d' ? 'Últimos 7 dias' : 'Ciclo (desde ' + desde.split('-').reverse().join('/') + ')',
      totais: { gasto: Number(gastoTotal.toFixed(2)), conversas: convTotal,
        cpa: convTotal > 0 ? Number((gastoTotal / convTotal).toFixed(2)) : null },
      calculo: { formula: 'custo por conversa = valor gasto ÷ conversas por mensagem iniciadas',
        metricaMeta: metricasVistas.join(', ') || 'nenhuma conversa no período',
        atribuicao: 'configuração unificada da conta (a mesma do Gerenciador de Anúncios)' },
      ativos: ativos.length, pausados: anuncios.length - ativos.length,
      verba: {
        adm: { depositado: cfg.verba.adm, real: Number(realAdm.toFixed(2)), gasto: Number(gastoAdm.toFixed(2)), saldo: Number((realAdm - gastoAdm).toFixed(2)) },
        tv: { depositado: cfg.verba.tv, real: Number(realTv.toFixed(2)), gasto: Number(gastoTv.toFixed(2)), saldo: Number((realTv - gastoTv).toFixed(2)) },
        aproveitamento: cfg.verba.aproveitamento },
      metas: cfg.metas, porCategoria, totalAnuncios: anuncios.length, anuncios,
      recebidosDaMeta: (ads.data || []).length, exibidos: anuncios.length,
      verbaOrigem: anuncios.reduce((o, a) => { o[a.verbaEm] = (o[a.verbaEm] || 0) + 1; return o; }, {}),
      statusRecebidos: porStatus, motivosCorte,
      conjuntosLidos: (adsets.data || []).length, campanhasLidas: (camps.data || []).length,
      avisos: [ads.erro, adsets.erro, camps.erro, ins.erro].filter(Boolean) };
    try { await dbSet(chaveCache, { em: new Date().toISOString(), desde, dados }); } catch (e) {}
    if (periodo === 'ciclo') { try { await dbSet('trafego_painel_cache', { em: new Date().toISOString(), desde, dados }); } catch (e) {} }
    return res.status(200).json(dados);
  }

  // ── CAMPANHAS + desempenho (últimos 7 dias) ──
  if (action === 'meta-campanhas') {
    if (!CONTA) return res.status(200).json({ ok: false, error: 'META_ADS_ACCOUNT não configurado' });
    const camps = await fetch(`${GRAPH}/act_${CONTA}/campaigns?fields=id,name,status,daily_budget,lifetime_budget&limit=50&access_token=${TOKEN}`).then(x => x.json()).catch(e => ({ error: { message: e.message } }));
    if (camps.error) return res.status(200).json({ ok: false, erro: camps.error.message });
    const ins = await fetch(`${GRAPH}/act_${CONTA}/insights?level=campaign&date_preset=last_7d&fields=campaign_id,campaign_name,spend,impressions,clicks,ctr,cpc,actions&limit=50&access_token=${TOKEN}`).then(x => x.json()).catch(() => ({ data: [] }));
    const porCamp = {};
    for (const i of (ins.data || [])) porCamp[i.campaign_id] = i;
    const resultado = (camps.data || []).map(c => {
      const i = porCamp[c.id] || {};
      const leads = ((i.actions || []).find(a => ['lead', 'onsite_conversion.messaging_conversation_started_7d'].includes(a.action_type)) || {}).value || 0;
      return { id: c.id, nome: c.name, status: c.status,
        orcamentoDiario: c.daily_budget ? (c.daily_budget / 100).toFixed(2) : null,
        gasto7d: i.spend || '0', impressoes7d: i.impressions || '0', cliques7d: i.clicks || '0',
        ctr: i.ctr ? Number(i.ctr).toFixed(2) + '%' : '—', cpc: i.cpc ? 'R$ ' + Number(i.cpc).toFixed(2) : '—',
        conversas7d: leads };
    });
    try { if (U && T) await dbSet('trafego_meta_cache', { em: new Date().toISOString(), campanhas: resultado }); } catch (e) {}
    return res.status(200).json({ ok: true, conta: 'act_' + CONTA, periodo: 'últimos 7 dias', campanhas: resultado });
  }

  // ═══ 🧭 COPILOTO: oportunidades do dia (o que pausar, o que escalar, onde sobra verba) ═══
  if (action === 'copiloto') {
    const cfg = await cfgTrafego();
    const per = ['hoje', '7d', 'ciclo'].includes(String(req.query.periodo || '')) ? String(req.query.periodo) : 'ciclo';
    const base = await dbGet('trafego_painel_cache_' + per) || await dbGet('trafego_painel_cache');
    if (!base || !base.dados) return res.status(200).json({ ok: false, error: 'abra o painel primeiro para carregar os dados' });
    const ads = (base.dados.anuncios || []).filter(a => a.ativo);
    const diasRestantes = (function () {
      const b = new Date(Date.now() - 3 * 3600 * 1000);
      const faltam = (6 - b.getUTCDay() + 7) % 7;
      return faltam === 0 ? 7 : faltam;
    })();
    // Verba que AINDA seria gasta por este anúncio até o fim do ciclo
    const verbaRestante = (a) => {
      if (a.orcamentoTotal) return Math.max(0, a.orcamentoTotal - a.gasto);       // verba total: sobra o que não gastou
      if (a.orcamentoDiario) return a.orcamentoDiario * diasRestantes;            // verba diária: dias que faltam
      return null;                                                                // não dá para saber
    };
    const semVerbaConhecida = ads.filter(a => verbaRestante(a) == null).length;

    // ── OPORTUNIDADE 1: cortar quem queima acima da meta ──
    const pausar = ads.filter(a => {
      if (a.conversas === 0 && a.gasto >= a.meta * 2) return true;
      return a.razaoMeta != null && a.razaoMeta > 1.3 && a.conversas > 0;
    }).map(a => {
      const vr = verbaRestante(a);
      return { id: a.id, nome: a.nome, categoria: a.categoria, thumb: a.thumb, adsetId: a.adsetId,
        cpa: a.cpa, meta: a.meta, gasto: a.gasto, conversas: a.conversas,
        orcamentoDiario: a.orcamentoDiario, verbaEm: a.verbaEm,
        liberaria: vr != null ? Number(vr.toFixed(2)) : null,
        motivo: a.conversas === 0
          ? 'queimou ' + brlS(a.gasto) + ' sem nenhuma conversa'
          : 'CPA ' + brlS(a.cpa) + ' — ' + Math.round((a.razaoMeta - 1) * 100) + '% acima da meta de ' + brlS(a.meta),
        desperdicioDiario: (a.cpa != null && a.orcamentoDiario)
          ? Number((a.orcamentoDiario * (1 - 1 / a.razaoMeta)).toFixed(2)) : null,
      };
    }).sort((x, y) => (y.liberaria || 0) - (x.liberaria || 0));
    const liberado = Number(pausar.reduce((s, p) => s + (p.liberaria || 0), 0).toFixed(2));
    const liberadoIncerto = pausar.filter(p => p.liberaria == null).length;

    // ── OPORTUNIDADE 2: escalar quem está bem abaixo da meta ──
    const idsPausar = new Set(pausar.map(p => p.id));
    const campeoes = ads.filter(a => !idsPausar.has(a.id) && a.situacao === 'campeao' && a.conversas >= 3);
    const pesoDe = a => (1 / Math.max(0.15, a.razaoMeta)) * Math.log10(10 + a.conversas);
    const somaPeso = campeoes.reduce((s, a) => s + pesoDe(a), 0) || 1;
    const distribuir = campeoes.map(a => {
      const fatia = liberado > 0 ? Number((liberado * (pesoDe(a) / somaPeso)).toFixed(2)) : 0;
      const diarioExtra = diasRestantes > 0 ? fatia / diasRestantes : 0;
      return { id: a.id, nome: a.nome, categoria: a.categoria, thumb: a.thumb, adsetId: a.adsetId,
        cpa: a.cpa, meta: a.meta, conversas: a.conversas, verbaEm: a.verbaEm,
        pesoPct: Number(((pesoDe(a) / somaPeso) * 100).toFixed(1)),
        receber: fatia,
        orcamentoDiarioAtual: a.orcamentoDiario,
        orcamentoDiarioNovo: a.orcamentoDiario != null ? Number((a.orcamentoDiario + diarioExtra).toFixed(2)) : null,
        conversasEstimadas: (a.cpa && fatia > 0) ? Math.round(fatia / a.cpa) : 0,
      };
    }).sort((x, y) => y.receber - x.receber || (x.cpa || 9e9) - (y.cpa || 9e9));
    const conversasGanhas = distribuir.reduce((s, d) => s + (d.conversasEstimadas || 0), 0);

    // ── OPORTUNIDADE 3: sem entrega (ativo e parado) ──
    const semEntrega = ads.filter(a => a.gasto === 0 && a.cliques === 0)
      .map(a => ({ id: a.id, nome: a.nome, categoria: a.categoria, thumb: a.thumb }));

    // ── OPORTUNIDADE 4: ritmo da verba do ciclo ──
    const v = base.dados.verba || {};
    const ritmo = ['adm', 'tv'].map(k => {
      const d = v[k] || {};
      const diasCorridos = Math.max(1, 7 - diasRestantes);
      const porDia = (d.gasto || 0) / diasCorridos;
      const projecao = (d.gasto || 0) + porDia * diasRestantes;
      const sobra = (d.real || 0) - projecao;
      return { frente: k.toUpperCase(), gasto: d.gasto || 0, verba: d.real || 0,
        projecao: Number(projecao.toFixed(2)), sobra: Number(sobra.toFixed(2)),
        alerta: sobra > (d.real || 0) * 0.15 ? 'vai sobrar verba — dinheiro parado é venda perdida'
          : (sobra < -(d.real || 0) * 0.05 ? 'ritmo acima da verba — vai estourar antes do fim' : 'ritmo saudável') };
    });

    const oportunidades = [];
    if (pausar.length) oportunidades.push({ tipo: 'cortar', titulo: pausar.length + ' anúncio(s) acima da meta', impacto: liberado });
    if (semEntrega.length) oportunidades.push({ tipo: 'sem-entrega', titulo: semEntrega.length + ' ativo(s) sem entrega', impacto: 0 });
    for (const r of ritmo) if (r.alerta !== 'ritmo saudável') oportunidades.push({ tipo: 'ritmo', titulo: r.frente + ': ' + r.alerta, impacto: Math.abs(r.sobra) });

    let resumo;
    if (!pausar.length) resumo = 'Nenhum anúncio ativo passou do limite de corte — a verba está bem alocada agora.';
    else if (liberado > 0) resumo = 'Cortando ' + pausar.length + ' anúncio(s) você recupera ' + brlS(liberado) +
      (distribuir.length ? '. Redistribuindo entre os ' + distribuir.length + ' campeões, a estimativa é de mais ' + conversasGanhas + ' conversas com a mesma verba.' : '.');
    else resumo = pausar.length + ' anúncio(s) merecem corte, mas não consegui calcular a verba a recuperar: ' +
      (semVerbaConhecida ? 'a Meta não expôs o orçamento de ' + semVerbaConhecida + ' anúncio(s) (verba pode estar em nível que o token não lê).' : 'orçamento não informado.') +
      ' O corte segue valendo pelo desperdício: cada um está pagando acima da meta.';

    return res.status(200).json({ ok: true, periodo: per,
      ciclo: base.dados.ciclo, diasRestantes, analisados: ads.length,
      oportunidades, pausar, liberado, liberadoIncerto, semVerbaConhecida,
      distribuir, semEntrega, ritmo, resumo, conversasEstimadasGanhas: conversasGanhas });
  }

  // ── ▶️ APLICAR: executa pausa/verba na Meta (só com confirmação explícita, modo copiloto) ──
  if (req.method === 'POST' && action === 'aplicar') {
    const { pausarIds, orcamentos, confirmar } = req.body || {};
    if (confirmar !== true) return res.status(400).json({ ok: false, error: 'confirmação explícita obrigatória' });
    const feitos = [], erros = [];
    for (const id of (pausarIds || [])) {
      const r = await fetch(`${GRAPH}/${id}`, { method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ status: 'PAUSED', access_token: TOKEN }) }).then(x => x.json()).catch(e => ({ error: { message: e.message } }));
      if (r && r.error) erros.push({ id, acao: 'pausar', erro: r.error.message });
      else feitos.push({ id, acao: 'pausado' });
    }
    for (const o of (orcamentos || [])) {
      if (!o.adsetId || !o.diario) continue;
      const centavos = Math.round(Number(o.diario) * 100);
      const r = await fetch(`${GRAPH}/${o.adsetId}`, { method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ daily_budget: centavos, access_token: TOKEN }) }).then(x => x.json()).catch(e => ({ error: { message: e.message } }));
      if (r && r.error) erros.push({ id: o.adsetId, acao: 'orcamento', erro: r.error.message });
      else feitos.push({ id: o.adsetId, acao: 'orçamento diário → R$ ' + Number(o.diario).toFixed(2) });
    }
    try {
      const lg = (await dbGet('trafego_log')) || { movs: [] };
      lg.movs.unshift({ ts: new Date().toISOString(), feitos, erros });
      lg.movs = lg.movs.slice(0, 200);
      await dbSet('trafego_log', lg);
      await dbSet('trafego_painel_cache', null);
    } catch (e) {}
    return res.status(200).json({ ok: erros.length === 0, feitos, erros });
  }

  // ═══ 🔬 ANÁLISE: padrões dos criativos campeões + 20 sugestões da semana ═══
  if (action === 'analise') {
    const AK = (process.env.ANTHROPIC_API_KEY || '').trim();
    const cfg = await cfgTrafego();
    const base = await dbGet('trafego_painel_cache');
    if (!base || !base.dados) return res.status(200).json({ ok: false, error: 'abra o painel primeiro para carregar os dados do ciclo' });
    const semana = (function () { const d = new Date(Date.now() - 3 * 3600 * 1000); const on = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
      return d.getUTCFullYear() + '-S' + Math.ceil((((d - on) / 86400000) + on.getUTCDay() + 1) / 7); })();
    const cacheS = await dbGet('trafego_sugestoes_' + semana);
    if (cacheS && String(req.query.gerar || '') !== '1') return res.status(200).json(Object.assign({ ok: true, semana, doCache: true }, cacheS));
    if (!AK) return res.status(200).json({ ok: false, error: 'ANTHROPIC_API_KEY não configurada na Vercel' });

    const ativos = (base.dados.anuncios || []).filter(a => a.ativo && a.conversas > 0);
    const topC = ativos.filter(a => a.situacao === 'campeao').slice(0, 15);
    const topR = ativos.filter(a => a.situacao === 'ralo').slice(0, 10);
    // Texto do criativo só para estes (lotes de 10 — requisição pequena)
    const copys = {};
    try {
      const alvos = [...topC, ...topR].map(a => a.id);
      for (let i = 0; i < alvos.length; i += 10) {
        const lote = alvos.slice(i, i + 10).join(',');
        const j = await fetch(`${GRAPH}/?ids=${lote}&fields=creative{body,object_story_spec{video_data{message},link_data{message}}}&access_token=${TOKEN}`)
          .then(x => x.json()).catch(() => null);
        for (const k of Object.keys(j || {})) {
          const c = ((j[k] || {}).creative) || {}, os = c.object_story_spec || {};
          copys[k] = String((os.video_data || {}).message || (os.link_data || {}).message || c.body || '').slice(0, 300);
        }
      }
    } catch (e) {}
    const campeoes = topC.map(a => ({ nome: a.nome, categoria: a.categoria, cpa: a.cpa, meta: a.meta, conversas: a.conversas, ctr: a.ctr, copy: copys[a.id] || '' }));
    const ralos = topR.map(a => ({ nome: a.nome, categoria: a.categoria, cpa: a.cpa, meta: a.meta, conversas: a.conversas, copy: copys[a.id] || '' }));

    const prompt = `Você é o estrategista de tráfego pago da Reparo Eletro, assistência técnica de eletrodomésticos em Belo Horizonte. Os anúncios são Click-to-WhatsApp: o objetivo de cada criativo é fazer a pessoa abrir conversa no WhatsApp para agendar conserto.

NOSSOS CRIATIVOS CAMPEÕES desta semana (CPA = custo por conversa; meta por categoria: TV R$${cfg.metas.tv}, micro-ondas R$${cfg.metas.microondas}, purificador R$${cfg.metas.purificador}, adega R$${cfg.metas.adega}):
${JSON.stringify(campeoes)}

NOSSOS PIORES desta semana (o que evitar):
${JSON.stringify(ralos)}

TAREFA — responda em duas partes:
1) PADRÕES: analise o que os campeões têm em comum e o que os piores erram (ganchos, dor tratada, promessa, tom, formato, uso de preço/urgência/prova). Seja específico e baseado nos dados acima, não genérico.
2) SUGESTÕES: pesquise na web o que está funcionando hoje em anúncios de conserto de eletrodomésticos e de adegas/cervejeiras no Brasil (concorrentes, ganchos comuns, formatos), e proponha EXATAMENTE 20 ideias novas de criativo em VÍDEO, assim distribuídas: 10 de ADEGA, 4 de TV, 3 de MICRO-ONDAS, 3 de PURIFICADOR.

Cada sugestão precisa de: titulo (curto), categoria, gancho (a primeira frase/cena, os 3 primeiros segundos), roteiro (3 a 5 cenas descritas para gravar com celular na própria loja), legenda (texto do anúncio, até 250 caracteres, tom próximo do que funciona nos nossos campeões), cta, porque (em que padrão ou evidência essa ideia se apoia).

Responda APENAS um JSON válido, sem markdown:
{"padroes":{"campeoes":["..."],"erros":["..."],"mercado":["..."]},"sugestoes":[{"titulo":"","categoria":"adega|tv|microondas|purificador","gancho":"","roteiro":["",""],"legenda":"","cta":"","porque":""}]}`;

    try {
      const r = await fetch('https://api.anthropic.com/v1/messages', {
        method: 'POST',
        headers: { 'x-api-key': AK, 'anthropic-version': '2023-06-01', 'Content-Type': 'application/json' },
        body: JSON.stringify({ model: 'claude-sonnet-4-6', max_tokens: 8000,
          tools: [{ type: 'web_search_20250305', name: 'web_search' }],
          messages: [{ role: 'user', content: prompt }] }),
      });
      const j = await r.json();
      const txt = ((j && j.content) || []).filter(b => b.type === 'text').map(b => b.text).join('\n');
      let parsed = null;
      try { parsed = JSON.parse(txt.replace(/```json|```/g, '').trim()); } catch (e) {
        const m = txt.match(/\{[\s\S]*\}/); if (m) { try { parsed = JSON.parse(m[0]); } catch (e2) {} }
      }
      if (!parsed || !parsed.sugestoes) return res.status(200).json({ ok: false, error: 'não consegui montar as sugestões desta vez', bruto: txt.slice(0, 300) });
      const saida = { padroes: parsed.padroes || {}, sugestoes: parsed.sugestoes.slice(0, 20),
        baseadoEm: { campeoes: campeoes.length, ralos: ralos.length }, geradoEm: new Date().toISOString() };
      await dbSet('trafego_sugestoes_' + semana, saida);
      return res.status(200).json(Object.assign({ ok: true, semana }, saida));
    } catch (e) {
      return res.status(200).json({ ok: false, error: e.message });
    }
  }

  // ── 🏷️ NAO-CLASSIFICADOS: nomes que caem em "outros" (anúncios + equipamentos da operação) ──
  if (action === 'nao-classificados') {
    const [cacheP, fA, fT, lgA, lgT] = await Promise.all([
      dbGet('trafego_painel_cache_7d').then(v => v || dbGet('trafego_painel_cache')),
      dbGet('fichas_adm'), dbGet('fichas_tv'), dbGet('reparoeletro_logistica'), dbGet('tv_logistica'),
    ]);
    const contaAnuncio = {}, contaEquip = {};
    for (const a of ((((cacheP || {}).dados) || {}).anuncios || [])) {
      if (categoriaDe(a.nome) === 'outros') contaAnuncio[a.nome] = (contaAnuncio[a.nome] || 0) + 1;
    }
    const corte90 = Date.now() - 90 * 86400000;
    const brancosPorBanco = {};
    const bancos = [['fichas_adm', fA], ['fichas_tv', fT], ['logistica_adm', lgA], ['logistica_tv', lgT]];
    for (const [nomeBanco, banco] of bancos) {
      for (const fi of (((banco || {}).fichas) || [])) {
        if (new Date(fi.criadoEm || 0).getTime() < corte90) continue;
        const eq = String(fi.equipamento || '').trim();
        if (!eq) {
          contaEquip['(equipamento em branco)'] = (contaEquip['(equipamento em branco)'] || 0) + 1;
          brancosPorBanco[nomeBanco] = (brancosPorBanco[nomeBanco] || 0) + 1;
          continue;
        }
        if (categoriaDe(eq) === 'outros') contaEquip[eq] = (contaEquip[eq] || 0) + 1;
      }
    }
    const ordena = o => Object.keys(o).map(k => ({ nome: k, vezes: o[k] })).sort((a, b) => b.vezes - a.vezes).slice(0, 40);
    const cfgN = await cfgTrafego();
    return res.status(200).json({ ok: true,
      apelidosAtivos: cfgN.apelidos,
      anunciosLidosDoCache: ((((cacheP || {}).dados) || {}).anuncios || []).length,
      brancosPorBanco,
      anunciosSemCategoria: ordena(contaAnuncio),
      equipamentosSemCategoria: ordena(contaEquip),
      comoEnsinar: 'POST em ?action=config com {"apelidos":{"vitrine opa":"purificador","gabriel":"microondas"}} — o termo é procurado dentro do nome, em minúsculas' });
  }

  // ── 🩺 REDIS-DEBUG: prova que o banco está acessível ──
  if (action === 'redis-debug') {
    const testeChave = 'trafego_teste_' + Date.now();
    let escreveu = false, leu = null;
    try { await dbSet(testeChave, { ok: true, em: new Date().toISOString() }); escreveu = true; } catch (e) {}
    try { leu = await dbGet(testeChave); } catch (e) {}
    const fichas = await dbGet('fichas_adm');
    const pipe = await dbGet('reparoeletro_pipe');
    return res.status(200).json({ ok: true,
      urlConfigurada: !!U, tokenConfigurado: !!T,
      escreveu, leuDeVolta: !!(leu && leu.ok),
      fichasAdmEncontradas: (((fichas || {}).fichas) || []).length,
      cardsPipeEncontrados: (((pipe || {}).cards) || []).length,
      veredito: (leu && leu.ok) ? '✅ Redis acessível' : '❌ Redis NÃO acessível — confira UPSTASH_URL/UPSTASH_TOKEN na Vercel' });
  }

  // ═══ 🧠 INTELIGÊNCIA: funil real por categoria (operação + Meta) ═══
  // Pesado: lê os bancos da operação inteira. Roda 1x/dia por cron e serve snapshot.
  if (action === 'inteligencia') {
    const cacheI = await dbGet('trafego_inteligencia');
    if (cacheI && String(req.query.recalcular || '') !== '1') {
      return res.status(200).json(Object.assign({ ok: true, doCache: true }, cacheI));
    }
    const cfgI = await cfgTrafego();
    const dias = Math.min(180, Math.max(7, parseInt(req.query.dias || '60', 10)));
    const corte = Date.now() - dias * 86400000;
    const dentro = (d) => { const t = new Date(d || 0).getTime(); return t > 0 && t >= corte; };

    const [fAdm, fTv, logA, logT, pipe, pipeTv] = await Promise.all([
      dbGet('fichas_adm'), dbGet('fichas_tv'),
      dbGet('reparoeletro_logistica'), dbGet('tv_logistica'),
      dbGet('reparoeletro_pipe'), dbGet('tv_pipe'),
    ]);
    const CATS = ['tv', 'microondas', 'forno', 'purificador', 'adega', 'institucional', 'outros'];
    const vazio = () => ({ entradas: 0, clienteLoja: 0, naLoja: 0,
      orcados: 0, aprovados: 0, reprovados: 0, negociando: 0, faturado: 0, tickets: [] });
    const f = {}; for (const c of CATS) f[c] = vazio();

    // ═══ ENTRADA (coorte da logística): equipamentos que entraram no processo ═══
    const NA_LOJA = ['coleta_efetuada', 'orc_registrado', 'orc_enviado', 'finalizado_rs'];
    for (const fi of [...(((logA || {}).fichas) || []), ...(((logT || {}).fichas) || [])]) {
      if (!dentro(fi.criadoEm)) continue;
      const c = categoriaDe(fi.equipamento || '');
      f[c].entradas++;
      if (NA_LOJA.includes(fi.phase)) f[c].naLoja++;
    }
    // Cliente Loja: também é entrada (o cliente traz o equipamento) e também veio de conversa
    for (const fi of [...(((fAdm || {}).fichas) || []), ...(((fTv || {}).fichas) || [])]) {
      if (!dentro(fi.criadoEm)) continue;
      if (fi.status !== 'cliente_loja') continue;
      const c = categoriaDe(fi.equipamento || '');
      f[c].entradas++; f[c].clienteLoja++; f[c].naLoja++;
    }

    // ═══ DESFECHO (coorte do pipe ADM + TV): quem recebeu orçamento e o que aconteceu ═══
    // Regra: conta cada card UMA vez pelo ponto MAIS AVANÇADO que alcançou (usa o histórico de fases)
    const APROV = ['aprovados', 'video_enviado', 'analise_compra', 'equipamento_comprado',
                   'programar_entrega', 'receber', 'erp', 'finalizado', 'garantia'];
    const REPROV = ['solicitar_entrega', 'entrega_solicitada', 'rota_em_andamento', 'descarte'];
    const NEGOC = ['aguardando_aprovacao', 'ultima_chamada'];
    const fasesDoCard = (cd) => {
      const hist = (cd.history || []).map(x => x.phaseId || x.phase).filter(Boolean);
      return new Set([...hist, cd.phaseId || cd.phase].filter(Boolean));
    };
    for (const cd of [...(((pipe || {}).cards) || []), ...(((pipeTv || {}).cards) || [])]) {
      if (!dentro(cd.criadoEm || cd.createdAt || cd.movedAt)) continue;
      const fases = fasesDoCard(cd);
      // só entra na conta quem chegou a receber orçamento
      const recebeuOrc = [...NEGOC, ...APROV, ...REPROV].some(p => fases.has(p));
      if (!recebeuOrc) continue;
      const c = categoriaDe(cd.equipamento || cd.descricao || cd.nomeContato || '');
      f[c].orcados++;
      const aprovou = APROV.some(p => fases.has(p));
      if (aprovou) {
        f[c].aprovados++;
        const v = parseFloat(cd.valor || 0) || 0;
        if (v > 0) { f[c].faturado += v; f[c].tickets.push(v); }
      } else if (REPROV.some(p => fases.has(p))) {
        f[c].reprovados++;
      } else {
        f[c].negociando++;
      }
    }
    // 4) CONVERSAS dos anúncios (do cache do painel, sem gastar requisição da Meta)
    const cachePainel = await dbGet('trafego_painel_cache_7d') || await dbGet('trafego_painel_cache');
    const convPorCat = {}; const gastoPorCat = {};
    for (const c of CATS) {
      const pc = ((((cachePainel || {}).dados) || {}).porCategoria || {})[c] || {};
      convPorCat[c] = pc.conversas || 0; gastoPorCat[c] = pc.gasto || 0;
    }

    const margem = 1 - 0.125; // peça consome 10-15% → margem de contribuição ~87,5%
    const custoViagem = Number(cfgI.custoViagem || 25); // coleta + devolução do reprovado
    const saida = CATS.map(c => {
      const d = f[c];
      const ticket = d.tickets.length ? d.tickets.reduce((a, b) => a + b, 0) / d.tickets.length : 0;
      const decididos = d.aprovados + d.reprovados;             // quem já respondeu ao orçamento
      const taxaAprov = decididos ? d.aprovados / decididos : null;
      const taxaEntradaLoja = d.entradas ? d.naLoja / d.entradas : null;
      const lucroBruto = ticket * margem;
      // lucro esperado de CADA orçamento enviado (já desconta a viagem perdida de quem reprova)
      const lucroPorOrcamento = taxaAprov != null
        ? (taxaAprov * lucroBruto) - (custoViagem * (1 - taxaAprov)) : null;
      return { categoria: c,
        entradas: d.entradas, clienteLoja: d.clienteLoja, naLoja: d.naLoja,
        orcados: d.orcados, aprovados: d.aprovados, reprovados: d.reprovados, negociando: d.negociando,
        faturado: Number(d.faturado.toFixed(2)),
        ticketMedio: Number(ticket.toFixed(2)),
        comValor: d.tickets.length,
        taxaEntradaLoja: taxaEntradaLoja != null ? Number((taxaEntradaLoja * 100).toFixed(1)) : null,
        taxaAprovacao: taxaAprov != null ? Number((taxaAprov * 100).toFixed(1)) : null,
        lucroPorOrcamento: lucroPorOrcamento != null ? Number(lucroPorOrcamento.toFixed(2)) : null,
        conversas7d: convPorCat[c], gasto7d: gastoPorCat[c],
        metaAtual: cfgI.metas[c] || cfgI.metas.outros,
        confianca: decididos >= 20 ? 'boa' : (decididos >= 8 ? 'fraca' : 'insuficiente'),
        // prova aritmética: tem que fechar sempre
        confere: (d.aprovados + d.reprovados + d.negociando) === d.orcados,
      };
    });
    const dadosI = { ok: true, periodoDias: dias, margemUsada: margem, custoViagem,
      geradoEm: new Date().toISOString(), categorias: saida,
      nota: 'ENTRADAS vêm da logística + cliente loja; ORÇADOS/APROVADOS vêm do pipe ADM e TV, contando cada card uma vez pelo ponto mais avançado que alcançou. São populações distintas — a taxa de aprovação é confiável; a razão entre elas é aproximada.',
      fechaConta: saida.every(x => x.confere) };
    try { await dbSet('trafego_inteligencia', dadosI); } catch (e) {}
    return res.status(200).json(dadosI);
  }

  // ── 🩺 PAINEL-DEBUG: testa cada requisição isoladamente ──
  if (action === 'painel-debug') {
    const tk = `&access_token=${TOKEN}`;
    const base = `${GRAPH}/act_${CONTA}/`;
    const testes = {};
    const t1 = await fetch(`${base}ads?fields=id,name,effective_status&limit=5${tk}`).then(x => x.json()).catch(e => ({ error: { message: e.message } }));
    testes.ads = t1.error ? '✗ ' + t1.error.message : '✓ ' + ((t1.data || []).length) + ' anúncios';
    const t2 = await fetch(`${base}adsets?fields=id,daily_budget&limit=5${tk}`).then(x => x.json()).catch(e => ({ error: { message: e.message } }));
    testes.adsets = t2.error ? '✗ ' + t2.error.message : '✓ ' + ((t2.data || []).length) + ' conjuntos';
    const cfgD = await cfgTrafego();
    const t3 = await fetch(`${base}insights?level=ad&time_range=${encodeURIComponent(JSON.stringify({ since: inicioCiclo(cfgD), until: new Date(Date.now() - 3 * 3600 * 1000).toISOString().slice(0, 10) }))}&fields=ad_id,spend&limit=5${tk}`).then(x => x.json()).catch(e => ({ error: { message: e.message } }));
    testes.insights = t3.error ? '✗ ' + t3.error.message : '✓ ' + ((t3.data || []).length) + ' linhas';
    const t4 = await fetch(`${base}ads?fields=id,creative{thumbnail_url}&limit=3${tk}`).then(x => x.json()).catch(e => ({ error: { message: e.message } }));
    testes.criativos = t4.error ? '✗ ' + t4.error.message : '✓ miniaturas ok';
    return res.status(200).json({ ok: true, conta: 'act_' + CONTA, cicloDesde: inicioCiclo(cfgD), testes });
  }

  // ── 🎯 ORIGENS: conversas com anúncio de origem capturado (base da atribuição de funil) ──
  if (action === 'origens') {
    const org = (await dbGet('wa_origem_anuncio')) || { por: {} };
    const tels = Object.keys(org.por || {});
    const porAd = {};
    for (const t of tels) {
      const o = org.por[t];
      const k = o.adId || 'sem-id';
      if (!porAd[k]) porAd[k] = { adId: o.adId, titulo: o.titulo, conversas: 0, comClid: 0 };
      porAd[k].conversas++;
      if (o.ctwaClid) porAd[k].comClid++;
    }
    return res.status(200).json({ ok: true,
      conversasComOrigem: tels.length,
      anunciosDistintos: Object.keys(porAd).length,
      porAnuncio: Object.values(porAd).sort((a, b) => b.conversas - a.conversas).slice(0, 50),
      obs: 'a origem só chega na PRIMEIRA mensagem de conversas vindas de anúncio (Click-to-WhatsApp)' });
  }

  return res.status(404).json({ ok: false, error: 'ação não encontrada (meta-teste | meta-campanhas | painel | origens | config)' });
  } catch (e) {
    return res.status(200).json({ ok: false, error: 'erro interno: ' + e.message });
  }
};
