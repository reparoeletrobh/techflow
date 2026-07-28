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
  // contexto: 'anuncio' → o que não identificar vira institucional (regra do dono)
  //           'equipamento' → tenta pelo código do modelo antes de desistir
  function categoriaDe(nome, contexto) {
    const s = String(nome || '').toLowerCase();
    for (const termo of Object.keys(APELIDOS)) {         // o que você ensinou vem primeiro
      if (termo && s.includes(termo)) return APELIDOS[termo];
    }
    if (/tvs?\b|\btvs?\d|televis|barramento|tela quebrad|quebrar tv|polegada/.test(s)) return 'tv';
    // micro-ondas: nome por extenso (tolerando erros de digitação) + códigos de modelo
    if (/mic?r?o\s?-?\s?o?nd?as|microodas|micro ?ond|\bmicro\b|reforma|\binflu\b|\bantigo\b/.test(s)) return 'microondas';
    if (/\bme[fovs]?\d{2}|\bmto\d|\bpms?\d{2}|\bpme\d|\bpm0\d|\bmtae?g?\d{2}|\bmi-?\d{4}|\bnn-?st?\d{2}|\bms\d{4}|\bmh\d{4}|\bbm[a-z]?\d{2}|\bcm[a-z]?\d{2}|\bmg\d{2}/.test(s)) return 'microondas';
    if (/\bforn(o|inho)/.test(s)) return 'forno';
    // purificador/bebedouro: nome + linhas Electrolux (PE/PA/PH4/PC4) e Consul (CPB/CPC)
    if (/purifi[a-z]*dor|purifiador|bebedouro|\bfiltro\b|\bvela\b|[áa]gua/.test(s)) return 'purificador';
    if (/\bibbl\b|\blatina\b|colorma[qc]|coloma[qc]|esmaltec|\blibell\b|\bsoft\b|masterfrio|karina/.test(s)) return 'purificador';
    if (/\bp[ea]\d{2}[a-z]?\b|\bph4\d|\bpc4\d|\bcp[bc]\d{2}/.test(s)) return 'purificador';
    // refrigeração de bebida: adega, cervejeira, frigobar, mini geladeira
    if (/adega|cerve|cervi|chopeir|b\.?\s?blend|climatiz|vinho|frigobar|min[ie]\s?\s?geladeira|gelad?eira|\bbz[a-z]?\d{2}|co ?²|co ?2/.test(s)) return 'adega';
    if (/criativo loja|reuniao externa|reunião externa|\bnovo\b|vitrine|institucional/.test(s)) return 'institucional';
    // sem identificação: anúncio vira institucional; equipamento fica marcado para ensinarmos
    return contexto === 'anuncio' ? 'institucional' : 'outros';
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
      const cat = categoriaDe(ad.name + ' ' + (st.name || ''), 'anuncio');
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

  // ═══ 🧭 COPILOTO: plano de verba POR CATEGORIA (realocação dentro da própria categoria) ═══
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
    const verbaTotalDe = a => a.orcamentoTotal || (a.orcamentoDiario ? a.orcamentoDiario * 7 : null);
    const restanteDe = a => { const v = verbaTotalDe(a); return v != null ? Math.max(0, v - a.gasto) : null; };

    const CATS = ['tv', 'microondas', 'purificador', 'adega', 'forno', 'institucional', 'outros'];
    const planos = [];
    for (const cat of CATS) {
      const lista = ads.filter(a => a.categoria === cat);
      if (!lista.length) continue;
      const meta = (cfg.metas[cat] != null ? cfg.metas[cat] : cfg.metas.outros);
      const gasto = lista.reduce((s, a) => s + a.gasto, 0);
      const conversas = lista.reduce((s, a) => s + a.conversas, 0);
      const verbaAlocada = lista.reduce((s, a) => s + (verbaTotalDe(a) || 0), 0);
      const restante = lista.reduce((s, a) => s + (restanteDe(a) || 0), 0);
      const cpaCat = conversas > 0 ? gasto / conversas : null;

      // perdedores da categoria
      const cortar = lista.filter(a => (a.conversas === 0 && a.gasto >= meta * 2)
        || (a.razaoMeta != null && a.razaoMeta > 1.3 && a.conversas > 0))
        .map(a => ({ id: a.id, nome: a.nome, thumb: a.thumb, adsetId: a.adsetId, campanhaId: a.campanhaId,
          cpa: a.cpa, conversas: a.conversas, gasto: a.gasto,
          libera: restanteDe(a), verbaEm: a.verbaEm,
          motivo: a.conversas === 0 ? 'sem nenhuma conversa'
            : Math.round((a.razaoMeta - 1) * 100) + '% acima da meta' }))
        .sort((x, y) => (y.libera || 0) - (x.libera || 0));
      const libera = Number(cortar.reduce((s, c) => s + (c.libera || 0), 0).toFixed(2));

      // campeões da MESMA categoria recebem
      const idsCortar = new Set(cortar.map(c => c.id));
      const campeoes = lista.filter(a => !idsCortar.has(a.id) && a.cpa != null && a.cpa <= meta && a.conversas >= 3);
      const peso = a => (meta / Math.max(0.2, a.cpa)) * Math.log10(10 + a.conversas);
      const soma = campeoes.reduce((s, a) => s + peso(a), 0) || 1;
      const reforcar = campeoes.map(a => {
        const fatia = libera > 0 ? Number((libera * (peso(a) / soma)).toFixed(2)) : 0;
        const atual = verbaTotalDe(a);
        return { id: a.id, nome: a.nome, thumb: a.thumb, adsetId: a.adsetId, campanhaId: a.campanhaId,
          cpa: a.cpa, conversas: a.conversas, verbaEm: a.verbaEm,
          pesoPct: Number(((peso(a) / soma) * 100).toFixed(1)),
          receber: fatia,
          verbaAtual: atual, verbaNova: atual != null ? Number((atual + fatia).toFixed(2)) : null,
          alvoId: a.verbaEm === 'campanha' ? a.campanhaId : a.adsetId,
          campoVerba: a.orcamentoTotal ? 'lifetime_budget' : 'daily_budget',
          valorNovo: a.orcamentoTotal ? Number((atual + fatia).toFixed(2)) : null,
          diarioNovo: atual != null && diasRestantes > 0
            ? Number(((Math.max(0, atual - a.gasto) + fatia) / diasRestantes).toFixed(2)) : null,
          conversasEstimadas: (a.cpa && fatia > 0) ? Math.round(fatia / a.cpa) : 0 };
      }).sort((x, y) => y.receber - x.receber);

      const todos = lista.map(a => ({ id: a.id, nome: a.nome, thumb: a.thumb,
        cpa: a.cpa, conversas: a.conversas, gasto: a.gasto,
        verba: verbaTotalDe(a), restante: restanteDe(a),
        situacao: idsCortar.has(a.id) ? 'cortar'
          : (a.cpa != null && a.cpa <= meta && a.conversas >= 3 ? 'campeao'
          : (a.cpa != null && a.cpa <= meta ? 'ok-pouco-volume' : 'observar')) }))
        .sort((x, y) => (x.cpa == null ? 9e9 : x.cpa) - (y.cpa == null ? 9e9 : y.cpa));
      planos.push({ categoria: cat, meta, anuncios: lista.length, todos,
        verbaAlocada: Number(verbaAlocada.toFixed(2)), gasto: Number(gasto.toFixed(2)),
        restante: Number(restante.toFixed(2)), conversas,
        cpa: cpaCat != null ? Number(cpaCat.toFixed(2)) : null,
        acimaDaMeta: cpaCat != null ? cpaCat > meta : null,
        cortar, libera, reforcar,
        ganhoEstimado: reforcar.reduce((s, r) => s + (r.conversasEstimadas || 0), 0),
        aplicado: Number(reforcar.reduce((s, r) => s + (r.receber || 0), 0).toFixed(2)),
        semDestino: libera > 0 && !campeoes.length,
        frentePropria: cat === 'tv' });
    }

    const admPlanos = planos.filter(p => !p.frentePropria);
    const totalAdm = admPlanos.reduce((s, p) => s + p.verbaAlocada, 0) || 1;
    for (const p of admPlanos) p.fatiaDaVerbaAdm = Number((p.verbaAlocada / totalAdm * 100).toFixed(1));

    const globalAtivos = ads.length;
    const globalAlocado = Number(ads.reduce((s, a) => s + (verbaTotalDe(a) || 0), 0).toFixed(2));
    const globalGasto = Number(ads.reduce((s, a) => s + a.gasto, 0).toFixed(2));
    const globalRestante = Number(ads.reduce((s, a) => s + (restanteDe(a) || 0), 0).toFixed(2));
    const globalConversas = ads.reduce((s, a) => s + a.conversas, 0);
    const totalCortes = planos.reduce((s, p) => s + p.cortar.length, 0);
    const totalLibera = Number(planos.reduce((s, p) => s + p.libera, 0).toFixed(2));
    const totalGanho = planos.reduce((s, p) => s + p.ganhoEstimado, 0);
    const resumo = totalCortes
      ? 'Cortando ' + totalCortes + ' criativo(s) você recupera ' + brlS(totalLibera) +
        ', realocado dentro de cada categoria — estimativa de mais ' + totalGanho + ' conversas com a mesma verba.'
      : 'Nenhum criativo ativo passou do limite de corte agora.';

    return res.status(200).json({ ok: true, periodo: per, diasRestantes,
      ciclo: base.dados.ciclo,
      global: { ativos: globalAtivos, alocado: globalAlocado, gasto: globalGasto,
        restante: globalRestante, conversas: globalConversas,
        cpaMedio: globalConversas > 0 ? Number((globalGasto / globalConversas).toFixed(2)) : null,
        percentualGasto: globalAlocado > 0 ? Number((globalGasto / globalAlocado * 100).toFixed(1)) : null },
      verba: base.dados.verba, planos, totalCortes, totalLibera, totalGanho, resumo,
      regra: 'a verba de um criativo perdedor vai para os campeões da MESMA categoria; TV é frente própria e não troca verba com as demais' });
  }

  // ── ▶️ APLICAR: executa na Meta (só com confirmação explícita) ──
  if (req.method === 'POST' && action === 'aplicar') {
    const { pausarIds, orcamentos, confirmar } = req.body || {};
    if (confirmar !== true) return res.status(400).json({ ok: false, error: 'confirmação explícita obrigatória' });
    // A Meta espera form-urlencoded com o token na query — JSON no corpo dá "parâmetro inválido"
    const postMeta = async (id, campos) => {
      const corpo = new URLSearchParams(campos).toString();
      return fetch(`${GRAPH}/${id}?access_token=${TOKEN}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: corpo,
      }).then(x => x.json()).catch(e => ({ error: { message: e.message } }));
    };
    const feitos = [], erros = [];
    // A verba destas campanhas é TOTAL e fica na CAMPANHA — pausar o anúncio não libera nada
    // e a Meta recusa (código 100). Pausamos na campanha; se falhar, tentamos conjunto e anúncio.
    for (const item of (pausarIds || [])) {
      const alvos = typeof item === 'string'
        ? [{ nivel: 'anúncio', id: item }]
        : [
            item.campanhaId ? { nivel: 'campanha', id: item.campanhaId } : null,
            item.adsetId ? { nivel: 'conjunto', id: item.adsetId } : null,
            item.id ? { nivel: 'anúncio', id: item.id } : null,
          ].filter(Boolean);
      let ok = false; const tentativas = [];
      for (const alvo of alvos) {
        const r = await postMeta(alvo.id, { status: 'PAUSED' });
        if (r && r.error) { tentativas.push(alvo.nivel + ': ' + r.error.message + ' (cód ' + r.error.code + ')'); }
        else { feitos.push({ id: alvo.id, acao: 'pausado no nível ' + alvo.nivel, nome: item.nome }); ok = true; break; }
        await new Promise(r2 => setTimeout(r2, 120));
      }
      if (!ok) erros.push({ id: (item.id || item), acao: 'pausar', nome: item.nome || '',
        erro: tentativas.join(' | ') || 'nenhum destino válido', codigo: 100 });
      await new Promise(r2 => setTimeout(r2, 120));
    }
    for (const o of (orcamentos || [])) {
      // o front informa ONDE está a verba e QUAL campo usar (total x diária)
      const alvo = o.alvoId || o.adsetId;
      const campo = o.campo || (o.diario ? 'daily_budget' : 'lifetime_budget');
      const valor = o.valor != null ? o.valor : o.diario;
      if (!alvo || valor == null) { erros.push({ id: alvo || '?', acao: 'orçamento', erro: 'destino ou valor ausente' }); continue; }
      const centavos = Math.round(Number(valor) * 100);
      if (!(centavos > 0)) { erros.push({ id: alvo, acao: 'orçamento', erro: 'valor inválido' }); continue; }
      const r = await postMeta(alvo, { [campo]: String(centavos) });
      if (r && r.error) erros.push({ id: alvo, acao: 'orçamento (' + campo + ')', erro: r.error.message, codigo: r.error.code });
      else feitos.push({ id: alvo, acao: campo + ' → R$ ' + Number(valor).toFixed(2) });
      await new Promise(r2 => setTimeout(r2, 120));
    }
    try {
      const lg = (await dbGet('trafego_log')) || { movs: [] };
      lg.movs.unshift({ ts: new Date().toISOString(), feitos, erros });
      lg.movs = lg.movs.slice(0, 200);
      await dbSet('trafego_log', lg);
      for (const p of ['hoje', '7d', 'ciclo']) await dbSet('trafego_painel_cache_' + p, null);
      await dbSet('trafego_painel_cache', null);
    } catch (e) {}
    return res.status(200).json({ ok: erros.length === 0, feitos, erros });
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

  // ═══ 📰 RELATÓRIO DIÁRIO DO COPILOTO — roda 1x/dia, compara com o histórico ═══
  if (action === 'relatorio-diario') {
    const guardado = await dbGet('trafego_relatorio_diario');
    if (guardado && String(req.query.gerar || '') !== '1') {
      return res.status(200).json(Object.assign({ ok: true, doCache: true }, guardado));
    }
    if (!CONTA) return res.status(200).json({ ok: false, error: 'META_ADS_ACCOUNT não configurado' });
    const cfg = await cfgTrafego();
    const base = `${GRAPH}/act_${CONTA}/`;
    const tk = `&access_token=${TOKEN}`;
    const filtroAtivo = '&filtering=' + encodeURIComponent(JSON.stringify([{ field: 'effective_status', operator: 'IN', value: ['ACTIVE'] }]));

    // 1) Anúncios ativos + insights de ONTEM
    const ads = await pegarTudo(`${base}ads?fields=id,name,effective_status,adset_id,campaign_id&limit=100${filtroAtivo}${tk}`, 6);
    if (ads.erro && !ads.data.length) return res.status(200).json({ ok: false, erro: ads.erro });
    const ins = await pegarTudo(`${base}insights?level=ad&date_preset=yesterday&use_unified_attribution_setting=true&fields=ad_id,spend,clicks,actions&limit=100${tk}`, 6);
    const CONV = ['onsite_conversion.messaging_conversation_started_7d', 'onsite_conversion.messaging_first_reply', 'onsite_conversion.total_messaging_connection', 'lead'];
    const contaConv = (acoes) => { for (const t of CONV) { const a = (acoes || []).find(x => x.action_type === t); if (a) return Number(a.value || 0); } return 0; };
    const porAd = {}; for (const i of (ins.data || [])) porAd[i.ad_id] = i;

    // 2) Grava o dia de ontem no histórico
    const ontem = new Date(Date.now() - 3 * 3600 * 1000 - 86400000).toISOString().slice(0, 10);
    const hist = (await dbGet('trafego_historico_diario')) || { dias: {} };
    const doDia = {};
    const snapshot = [];
    for (const ad of (ads.data || [])) {
      const i = porAd[ad.id] || {};
      const gasto = Number(i.spend || 0), conv = contaConv(i.actions);
      if (gasto === 0 && conv === 0) continue;
      const cat = categoriaDe(ad.name, 'anuncio');
      const cpa = conv > 0 ? Number((gasto / conv).toFixed(2)) : null;
      doDia[ad.id] = { n: ad.name, c: cat, g: Number(gasto.toFixed(2)), v: conv, cpa };
      snapshot.push({ id: ad.id, nome: ad.name, categoria: cat, gasto: Number(gasto.toFixed(2)), conversas: conv, cpa });
    }
    hist.dias[ontem] = doDia;
    const corteH = new Date(Date.now() - 45 * 86400000).toISOString().slice(0, 10);
    for (const d of Object.keys(hist.dias)) if (d < corteH) delete hist.dias[d];
    try { await dbSet('trafego_historico_diario', hist); } catch (e) {}

    // 3) Compara cada criativo com a própria média histórica (dias anteriores)
    const diasOrd = Object.keys(hist.dias).sort();
    const anteriores = diasOrd.filter(d => d < ontem);
    const mediaDe = (adId) => {
      const vals = [];
      for (const d of anteriores) { const r = hist.dias[d][adId]; if (r && r.cpa != null) vals.push(r.cpa); }
      return vals.length ? { media: vals.reduce((a, b) => a + b, 0) / vals.length, dias: vals.length } : null;
    };
    const melhorando = [], piorando = [], estaveis = [], novos = [], semConversa = [];
    for (const s of snapshot) {
      const meta = cfg.metas[s.categoria] != null ? cfg.metas[s.categoria] : cfg.metas.outros;
      if (s.cpa == null) { semConversa.push(Object.assign({ meta }, s)); continue; }
      const m = mediaDe(s.id);
      if (!m || m.dias < 2) { novos.push(Object.assign({ meta }, s)); continue; }
      const varia = (s.cpa - m.media) / m.media;
      const item = Object.assign({ meta, mediaAnterior: Number(m.media.toFixed(2)),
        variacaoPct: Number((varia * 100).toFixed(0)), diasHistorico: m.dias }, s);
      if (varia <= -0.15) melhorando.push(item);
      else if (varia >= 0.15) piorando.push(item);
      else estaveis.push(item);
    }
    melhorando.sort((a, b) => a.variacaoPct - b.variacaoPct);
    piorando.sort((a, b) => b.variacaoPct - a.variacaoPct);

    // 4) Totais do dia + por categoria
    const gastoOntem = snapshot.reduce((s, a) => s + a.gasto, 0);
    const convOntem = snapshot.reduce((s, a) => s + a.conversas, 0);
    const porCat = {};
    for (const s of snapshot) {
      const c = s.categoria;
      if (!porCat[c]) porCat[c] = { gasto: 0, conversas: 0, meta: cfg.metas[c] != null ? cfg.metas[c] : cfg.metas.outros };
      porCat[c].gasto += s.gasto; porCat[c].conversas += s.conversas;
    }
    for (const c of Object.keys(porCat)) {
      porCat[c].gasto = Number(porCat[c].gasto.toFixed(2));
      porCat[c].cpa = porCat[c].conversas > 0 ? Number((porCat[c].gasto / porCat[c].conversas).toFixed(2)) : null;
      porCat[c].acimaDaMeta = porCat[c].cpa != null ? porCat[c].cpa > porCat[c].meta : null;
    }

    // 5) Texto do copiloto (curto e direto)
    let mensagem = '';
    const AK = (process.env.ANTHROPIC_API_KEY || '').trim();
    if (AK) {
      try {
        const prompt = `Você é o copiloto de tráfego da Reparo Eletro. Escreva o recado do dia para o dono, em português do Brasil, tom direto de quem trabalha com ele.

ONTEM (${ontem}): R$ ${gastoOntem.toFixed(2)} gastos, ${convOntem} conversas.
POR CATEGORIA: ${JSON.stringify(porCat)}
MELHORANDO (CPA caiu vs a média do próprio criativo): ${JSON.stringify(melhorando.slice(0, 6))}
PIORANDO (CPA subiu): ${JSON.stringify(piorando.slice(0, 6))}
ESTÁVEIS: ${estaveis.length} criativos
SEM CONVERSA ONTEM: ${JSON.stringify(semConversa.slice(0, 5))}
NOVOS (sem histórico): ${novos.length}

Escreva 3 a 5 frases curtas: comece pelo que mudou de ontem para cá, cite criativos pelo nome quando relevante, aponte o que merece ação hoje e o que já está bom. Sem saudação, sem despedida, sem markdown. Se algo estiver com pouca amostra, diga que é cedo para concluir.`;
        const r = await fetch('https://api.anthropic.com/v1/messages', {
          method: 'POST',
          headers: { 'x-api-key': AK, 'anthropic-version': '2023-06-01', 'Content-Type': 'application/json' },
          body: JSON.stringify({ model: 'claude-sonnet-4-6', max_tokens: 600, messages: [{ role: 'user', content: prompt }] }),
        }).then(x => x.json());
        mensagem = ((r && r.content) || []).filter(b => b.type === 'text').map(b => b.text).join(' ').trim();
      } catch (e) {}
    }
    if (!mensagem) {
      mensagem = `Ontem foram ${convOntem} conversas por R$ ${gastoOntem.toFixed(2)}` +
        (convOntem > 0 ? ` (R$ ${(gastoOntem / convOntem).toFixed(2)} por conversa).` : '.') +
        (piorando.length ? ` ${piorando.length} criativo(s) pioraram.` : '') +
        (melhorando.length ? ` ${melhorando.length} melhoraram.` : '');
    }

    const saida = { dia: ontem, geradoEm: new Date().toISOString(), mensagem,
      totais: { gasto: Number(gastoOntem.toFixed(2)), conversas: convOntem,
        cpa: convOntem > 0 ? Number((gastoOntem / convOntem).toFixed(2)) : null },
      porCategoria: porCat, melhorando, piorando,
      estaveis: estaveis.length, novos: novos.length, semConversa,
      diasDeHistorico: diasOrd.length };
    try { await dbSet('trafego_relatorio_diario', saida); } catch (e) {}
    return res.status(200).json(Object.assign({ ok: true }, saida));
  }

  // ═══ 💵 ROAS por categoria — faturamento REALMENTE PAGO ÷ investimento em anúncios ═══
  if (action === 'roas') {
    const diasCache = Math.min(120, Math.max(15, parseInt(req.query.dias || '56', 10)));
    const guardado = await dbGet('trafego_roas_' + diasCache);
    if (guardado && String(req.query.gerar || '') !== '1') {
      return res.status(200).json(Object.assign({ ok: true, doCache: true }, guardado));
    }
    if (!CONTA) return res.status(200).json({ ok: false, error: 'META_ADS_ACCOUNT não configurado' });
    const cfg = await cfgTrafego();
    // 8 semanas = 56 dias (é assim que o dono mede). 60 dias inflava ~7%.
    const dias = Math.min(120, Math.max(15, parseInt(req.query.dias || '56', 10)));
    const ate = new Date(Date.now() - 3 * 3600 * 1000).toISOString().slice(0, 10);
    const desde = new Date(Date.now() - 3 * 3600 * 1000 - dias * 86400000).toISOString().slice(0, 10);

    // ── 1) INVESTIMENTO por categoria (todos os anúncios, ativos ou não) ──
    const janela = 'time_range=' + encodeURIComponent(JSON.stringify({ since: desde, until: ate }));
    const ins = await pegarTudo(`${GRAPH}/act_${CONTA}/insights?level=ad&${janela}&fields=ad_id,ad_name,spend&limit=100&access_token=${TOKEN}`, 12);
    if (ins.erro && !ins.data.length) return res.status(200).json({ ok: false, erro: ins.erro });
    const investido = {};
    let investidoTotal = 0;
    for (const i of (ins.data || [])) {
      const cat = categoriaDe(i.ad_name || '', 'anuncio');
      const v = Number(i.spend || 0);
      investido[cat] = (investido[cat] || 0) + v;
      investidoTotal += v;
    }

    // ── 2) FATURAMENTO PAGO por categoria ──
    // Só entra o que comprovadamente passou pelo pagamento: fase de pagamento confirmado,
    // histórico com passagem por pagamento_confirmado, ou comprovante analisado e aprovado.
    // FONTE DO PAGO = passagem por ERP ("RP"), nos dois sistemas.
    // Regra do dono: pago vai para ERP; na segunda migra para Finalizado, que é POLUÍDO
    // (garantia e reprovado também caem lá) — por isso conta a PASSAGEM POR ERP, não a fase atual.
    const [ppA, ppT, boardA, fin] = await Promise.all([
      dbGet('reparoeletro_pipe'), dbGet('tv_pipe'),
      dbGet('reparoeletro_board'), dbGet('reparoeletro_financeiro'),
    ]);
    const dentroJanela = (d) => {
      const t = new Date(d || 0).getTime();
      return t > 0 && t >= Date.now() - dias * 86400000;
    };
    const faturado = {}; const detalhe = {}; let faturadoTotal = 0; let semCategoria = 0;
    const contados = new Set();
    const assinaturas = new Set();
    const porFonte = {};
    const amostras = {};
    const amostraOutros = [];
    const registrar = (chave, cat, valor, obj) => {
      const fonte = String(chave).split(':')[0];
      if (contados.has(chave)) return;                 // dedupe por identificador
      // dedupe por ASSINATURA: o mesmo serviço aparece no pipe, no board e no arquivo com ids diferentes
      if (obj) {
        const d8 = String(obj.telefone || obj.tel || '').replace(/\D/g, '').slice(-8);
        // telefone + valor identificam o serviço mesmo quando o código difere entre pipe,
        // financeiro e arquivo. Recompra do mesmo cliente com valor idêntico é rara o bastante
        // para valer menos que a duplicação que isso evita.
        const sig = d8 + '|' + valor.toFixed(2);
        if (d8.length >= 8) {
          if (assinaturas.has(sig)) { duplicados++; return; }
          assinaturas.add(sig);
        }
      }
      contados.add(chave);
      if (!porFonte[fonte]) porFonte[fonte] = { itens: 0, valor: 0, semTelefone: 0 };
      porFonte[fonte].itens++;
      porFonte[fonte].valor = Number((porFonte[fonte].valor + valor).toFixed(2));
      if (!String((obj || {}).telefone || (obj || {}).tel || '').replace(/\D/g, '').slice(-8)) porFonte[fonte].semTelefone++;
      if (cat === 'outros' && amostraOutros.length < 15) amostraOutros.push({
        nome: (obj || {}).nomeContato || (obj || {}).nome || '',
        equip: (obj || {}).equipamento || (obj || {}).descricao || (obj || {}).title || '(vazio)',
        valor, fonte });
      if (!amostras[fonte]) amostras[fonte] = [];
      if (amostras[fonte].length < 3) amostras[fonte].push({
        nome: (obj || {}).nomeContato || (obj || {}).nome || (obj || {}).title || '',
        equip: (obj || {}).equipamento || (obj || {}).descricao || '',
        tel: (obj || {}).telefone || (obj || {}).tel || '(sem)',
        valor, fase: (obj || {}).phaseId || (obj || {}).phase || '',
        data: (obj || {}).finalizadoEm || (obj || {}).movedAt || (obj || {}).criadoEm || '' });
      faturado[cat] = (faturado[cat] || 0) + valor;
      detalhe[cat] = (detalhe[cat] || 0) + 1;
      faturadoTotal += valor;
    };
    const passouPorErp = (obj) => {
      const fases = [(obj.phaseId || obj.phase), ...((obj.history || []).map(x => x.phaseId || x.phase))]
        .filter(Boolean).map(String);
      return fases.some(f => f === 'erp' || f.startsWith('erp'));
    };
    const quandoErp = (obj) => {
      const h = (obj.history || []).find(x => String(x.phaseId || x.phase || '').startsWith('erp'));
      return (h && (h.ts || h.timestamp)) || obj.movedAt || obj.criadoEm || obj.createdAt;
    };
    // REGRA DO DONO: vale o que está em FINALIZADO, tirando RS, garantia e reprovado.
    // (ERP também conta — é a antessala do finalizado, ainda não migrou na segunda.)
    const ehExcluido = (c) => {
      const txt = [c.nomeContato, c.title, c.descricao, c.equipamento, c.osCode, c.nome]
        .filter(Boolean).join(' ').toLowerCase();
      return /\brs\b|\br\.?\s?s\.?\b|\brs[-–—\s]|garantia|garant|reprovad|retorno|refaz|revisit/.test(txt);
    };
    let excluidosRsGarantia = 0;
    let naoVendidos = 0;
    let duplicados = 0;
    const FASES_VALEM = ['finalizado', 'erp'];
    for (const [banco, sis] of [[ppA, 'adm'], [ppT, 'tv']]) {
      for (const c of (((banco || {}).cards) || [])) {
        const fase = String(c.phaseId || c.phase || '');
        const valeAgora = FASES_VALEM.includes(fase);
        if (!valeAgora && !passouPorErp(c)) continue;
        const valor = parseFloat(c.valor || c.total || 0) || 0;
        if (!(valor > 0)) continue;
        if (ehExcluido(c)) { excluidosRsGarantia++; continue; }        // 🚫 RS / garantia / reprovado
        const quando = quandoErp(c) || c.movedAt || c.criadoEm;
        if (!dentroJanela(quando)) continue;
        const cat = categoriaDe(c.equipamento || c.descricao || c.nomeContato || '');
        if (cat === 'outros') semCategoria++;
        registrar(sis + ':' + c.id, cat, valor, c);
      }
    }
    // ARQUIVO MORTO: cards finalizados são REMOVIDOS do pipe/board e guardados aqui —
    // é onde mora a maior parte do faturamento histórico.
    const [bTv, arqA, arqT, arqFin] = await Promise.all([
      dbGet('tv_board'), dbGet('reparoeletro_arquivo'), dbGet('tv_arquivo'),
      dbGet('reparoeletro_financeiro_arquivo'),
    ]);
    for (const [banco, sis] of [[arqA, 'arqAdm'], [arqT, 'arqTv'], [arqFin, 'arqFin']]) {
      const itens = (((banco || {}).cards) || []).concat(((banco || {}).records) || [])
        .concat(((banco || {}).fichas) || []);
      for (const c of itens) {
        const valor = parseFloat(c.valor || c.total || 0) || 0;
        if (!(valor > 0)) continue;
        if (ehExcluido(c)) { excluidosRsGarantia++; continue; }
        const fase = String(c.phaseId || c.phase || '');
        // 🚫 SÓ VENDA: no arquivo entram cards de todas as fases, inclusive última chamada e
        // aguardando aprovação (orçamento sem decisão). Vale apenas finalizado/ERP ou quem passou por ERP.
        const ehVenda = FASES_VALEM.includes(fase)
          || ['entrega_liberada', 'entrega_agendada', 'entrega_realizada', 'equip_retirado', 'pagamento_confirmado'].includes(fase)
          || passouPorErp(c);
        if (!ehVenda) { naoVendidos++; continue; }
        // ⚠️ arquivadoEm NÃO serve como data do serviço: arquivamos em lote esta semana,
        // o que traria serviços antigos para dentro da janela. Vale a data real de conclusão.
        const quando = c.finalizadoEm || c.pagoEm || quandoErp(c) || c.movedAt || c.criadoEm || c.createdAt;
        if (!dentroJanela(quando)) continue;
        const cat = categoriaDe(c.equipamento || c.descricao || c.title || c.nomeContato || c.nome || '');
        if (cat === 'outros') semCategoria++;
        registrar(sis + ':' + (c.id || c.pipefyId || c.osCode), cat, valor, c);
      }
    }
    for (const [banco, sis] of [[boardA, 'boardAdm'], [bTv, 'boardTv']]) {
      for (const c of (((banco || {}).cards) || [])) {
        const fase = String(c.phaseId || c.phase || '');
        if (!FASES_VALEM.includes(fase)) continue;
        const valor = parseFloat(c.valor || c.total || 0) || 0;
        if (!(valor > 0)) continue;
        if (ehExcluido(c)) { excluidosRsGarantia++; continue; }
        const quando = c.movedAt || c.syncedAt || c.criadoEm;
        if (!dentroJanela(quando)) continue;
        const cat = categoriaDe(c.descricao || c.equipamento || c.title || c.nomeContato || '');
        if (cat === 'outros') semCategoria++;
        registrar(sis + ':' + (c.pipefyId || c.osCode || c.id), cat, valor, c);
      }
    }
    // 2) metaLog do board: entradas em ERP com valor e data reais
    for (const m of (((boardA || {}).metaLog) || [])) {
      if (String(m.phaseId || '') !== 'erp_entrada') continue;
      const valor = parseFloat(m.valor || 0) || 0;
      if (!(valor > 0) || !dentroJanela(m.timestamp)) continue;
      const cat = categoriaDe(m.equipamento || m.descricao || m.titulo || '');
      if (cat === 'outros') semCategoria++;
      registrar('board:' + (m.pipefyId || m.id), cat, valor, m);
    }
    // 3) financeiro: complementa o que tiver comprovante aprovado e não veio das fontes acima
    for (const r of (((fin || {}).records) || [])) {
      const valor = parseFloat(r.valor || r.total || 0) || 0;
      if (!(valor > 0)) continue;
      const ok = passouPorErp(r) || (r.comprovanteAnalise && r.comprovanteAnalise.veredito === 'verde');
      if (!ok) continue;
      const quando = r.pagoEm || r.confirmadoEm || quandoErp(r);
      if (!dentroJanela(quando)) continue;
      const cat = categoriaDe(r.equipamento || r.descricao || r.titulo || '');
      if (cat === 'outros') semCategoria++;
      registrar('fin:' + r.id, cat, valor, r);
    }

    // ── 3) ROAS ──
    const CATS = ['tv', 'microondas', 'forno', 'purificador', 'adega', 'institucional', 'outros'];
    const linhas = CATS.map(c => {
      const inv = Number((investido[c] || 0).toFixed(2));
      const fat = Number((faturado[c] || 0).toFixed(2));
      const roas = inv > 0 ? Number((fat / inv).toFixed(2)) : null;
      const meta = cfg.metas[c] != null ? cfg.metas[c] : cfg.metas.outros;
      return { categoria: c, investido: inv, faturado: fat, vendas: detalhe[c] || 0,
        roas, ticketMedio: (detalhe[c] || 0) > 0 ? Number((fat / detalhe[c]).toFixed(2)) : null,
        metaCpaAtual: meta,
        // quanto se pode pagar por conversa mantendo o ROAS desejado
        confianca: (detalhe[c] || 0) >= 15 ? 'boa' : ((detalhe[c] || 0) >= 5 ? 'fraca' : 'insuficiente') };
    }).filter(l => l.investido > 0 || l.faturado > 0)
      .sort((a, b) => (b.roas || 0) - (a.roas || 0));

    // FRENTES SEPARADAS: TV tem verba própria e não se mistura com o ADM
    const somaFrente = (filtro) => {
      const ls = linhas.filter(filtro);
      const inv = ls.reduce((s, l) => s + l.investido, 0);
      const fat = ls.reduce((s, l) => s + l.faturado, 0);
      const vds = ls.reduce((s, l) => s + l.vendas, 0);
      return { investido: Number(inv.toFixed(2)), faturado: Number(fat.toFixed(2)), vendas: vds,
        roas: inv > 0 ? Number((fat / inv).toFixed(2)) : null,
        ticketMedio: vds > 0 ? Number((fat / vds).toFixed(2)) : null,
        categorias: ls.map(l => l.categoria) };
    };
    const frentes = {
      tv: somaFrente(l => l.categoria === 'tv'),
      adm: somaFrente(l => l.categoria !== 'tv'),
    };
    const saida = { periodoDias: dias, de: desde, ate, frentes,
      geradoEm: new Date().toISOString(),
      totais: { investido: Number(investidoTotal.toFixed(2)), faturado: Number(faturadoTotal.toFixed(2)),
        roas: investidoTotal > 0 ? Number((faturadoTotal / investidoTotal).toFixed(2)) : null },
      linhas, semCategoria,
      criterioFaturamento: 'FINALIZADO + ERP, excluindo tudo que tenha RS, garantia ou reprovado no nome/descrição (regra do dono); fontes: pipe ADM, pipe TV, board ADM, board TV e metaLog de ERP, com dedupe',
      excluidosRsGarantia,
      descartadosSemVenda: naoVendidos,
      duplicadosRemovidos: duplicados,
      fontes: porFonte,
      amostraPorFonte: amostras,
      equipamentosNaoClassificados: amostraOutros.slice(0, 25),
      totalContado: contados.size,
      alerta: 'investimento vem da Meta por nome do anúncio; faturamento vem do financeiro por equipamento — categorias mal nomeadas nos dois lados distorcem o ROAS' };
    try { await dbSet('trafego_roas_' + dias, saida); if (dias === 56) await dbSet('trafego_roas', saida); } catch (e) {}
    return res.status(200).json(Object.assign({ ok: true }, saida));
  }

  // ── 🩺 ROAS-DIAGNOSTICO: onde estão os serviços pagos que ainda não entram na conta ──
  if (action === 'roas-diagnostico') {
    const dias = Math.min(120, Math.max(15, parseInt(req.query.dias || '60', 10)));
    const corte = Date.now() - dias * 86400000;
    const dentro = d => { const t = new Date(d || 0).getTime(); return t > 0 && t >= corte; };
    const [ppA, ppT, board, fin, balcao, arq, boardTv] = await Promise.all([
      dbGet('reparoeletro_pipe'), dbGet('tv_pipe'), dbGet('reparoeletro_board'),
      dbGet('reparoeletro_financeiro'), dbGet('reparoeletro_balcao'),
      dbGet('pipe_ids_arquivados'), dbGet('tv_board'),
    ]);
    const temErp = o => [(o.phaseId || o.phase), ...((o.history || []).map(x => x.phaseId || x.phase))]
      .filter(Boolean).some(f => String(f).startsWith('erp'));
    const contar = (arr, filtro) => (arr || []).filter(filtro).length;
    const somar = (arr, filtro, campo) => (arr || []).filter(filtro)
      .reduce((s, x) => s + (parseFloat(x[campo] || x.valor || 0) || 0), 0);

    const cardsA = ((ppA || {}).cards) || [];
    const cardsT = ((ppT || {}).cards) || [];
    const metaLog = ((board || {}).metaLog) || [];
    const erpLog = metaLog.filter(m => String(m.phaseId || '') === 'erp_entrada');
    const datas = erpLog.map(m => m.timestamp).filter(Boolean).sort();
    const recs = ((fin || {}).records) || [];
    const bal = Array.isArray(balcao) ? balcao : (((balcao || {}).itens) || []);

    return res.status(200).json({ ok: true, periodoDias: dias,
      pipeAdm: { total: cardsA.length, comErp: contar(cardsA, temErp),
        comErpNoPeriodo: contar(cardsA, c => temErp(c) && dentro(c.movedAt || c.criadoEm)),
        comValor: contar(cardsA, c => parseFloat(c.valor || 0) > 0) },
      pipeTv: { total: cardsT.length, comErp: contar(cardsT, temErp),
        comValor: contar(cardsT, c => parseFloat(c.valor || 0) > 0) },
      boardMetaLog: { totalEntradas: metaLog.length, entradasErp: erpLog.length,
        erpNoPeriodo: erpLog.filter(m => dentro(m.timestamp)).length,
        comValor: erpLog.filter(m => parseFloat(m.valor || 0) > 0).length,
        valorSomado: Number(erpLog.filter(m => dentro(m.timestamp)).reduce((s, m) => s + (parseFloat(m.valor || 0) || 0), 0).toFixed(2)),
        maisAntiga: datas[0] || null, maisRecente: datas[datas.length - 1] || null },
      financeiro: { registros: recs.length, comValor: contar(recs, r => parseFloat(r.valor || r.total || 0) > 0),
        comErp: contar(recs, temErp) },
      balcao: { registros: bal.length,
        pagos: contar(bal, b => b.pagoEm || b.status === 'pago'),
        pagosNoPeriodo: contar(bal, b => (b.pagoEm || b.status === 'pago') && dentro(b.pagoEm || b.entradaEm)) },
      cardsArquivados: (((arq || {}).ids) || []).length,
      arquivos: await (async () => {
        const [a1, a2, a3, a4] = await Promise.all([
          dbGet('reparoeletro_arquivo'), dbGet('tv_arquivo'),
          dbGet('reparoeletro_financeiro_arquivo'), dbGet('reparoeletro_logistica_arquivo')]);
        const conta = (b) => { const it = (((b||{}).cards)||[]).concat(((b||{}).records)||[]).concat(((b||{}).fichas)||[]);
          return { itens: it.length, comValor: it.filter(x => parseFloat(x.valor||x.total||0) > 0).length,
            valorNoPeriodo: Number(it.filter(x => parseFloat(x.valor||x.total||0) > 0 && dentro(x.arquivadoEm||x.movedAt||x.criadoEm))
              .reduce((s,x)=>s+(parseFloat(x.valor||x.total||0)||0),0).toFixed(2)) }; };
        return { reparoeletro_arquivo: conta(a1), tv_arquivo: conta(a2),
          financeiro_arquivo: conta(a3), logistica_arquivo: conta(a4) };
      })(),
      tvBoard: { existe: !!boardTv, cards: (((boardTv || {}).cards) || []).length },
      leitura: 'compare com ~1300 serviços pagos em 60 dias: a fonte com número próximo disso é onde mora a verdade' });
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
