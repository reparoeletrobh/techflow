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
    // Encerramento sábado 11h: limite de CAPACIDADE DE ATENDIMENTO, não de mídia.
    // As 2h de folga evitam que o lead que chega depois pense que há coleta no mesmo dia.
    cicloFim: { diaSemana: 6, hora: 11 },
  };
  async function cfgTrafego() {
    const c = (await dbGet('trafego_config')) || {};
    return {
      metas: Object.assign({}, CFG_PADRAO.metas, c.metas || {}),
      verba: Object.assign({}, CFG_PADRAO.verba, c.verba || {}),
      cicloInicio: Object.assign({}, CFG_PADRAO.cicloInicio, c.cicloInicio || {}),
      cicloFim: Object.assign({}, CFG_PADRAO.cicloFim, c.cicloFim || {}),
      modelosFixados: c.modelosFixados || {},
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
    if (/criativo loja 1\b/.test(s)) return 'microondas';   // confirmado pelo dono
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
      if (b.modelosFixados) atual.modelosFixados = Object.assign({}, atual.modelosFixados || {}, b.modelosFixados);
      if (b.modelosExcluir) atual.modelosExcluir = b.modelosExcluir;
      if (b.ciclo) atual.ciclo = Object.assign({}, atual.ciclo || {}, b.ciclo);
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

  // ── ⛔ EXCLUIR/LIBERAR modelo pelo nome (link simples, sem POST) ──
  if (action === 'modelo-excluir' || action === 'modelo-liberar') {
    const termo = String(req.query.nome || '').toLowerCase().trim();
    if (!termo) return res.status(400).json({ ok: false, error: 'informe ?nome=trecho do nome do anúncio' });
    const c = (await dbGet('trafego_config')) || {};
    let lista = c.modelosExcluir || ['reforma', 'pintura', 'restaura'];
    if (action === 'modelo-excluir') { if (!lista.includes(termo)) lista.push(termo); }
    else lista = lista.filter(x => x !== termo);
    c.modelosExcluir = lista;
    await dbSet('trafego_config', c);
    return res.status(200).json({ ok: true, acao: action === 'modelo-excluir' ? 'excluído' : 'liberado',
      termo, listaAtual: lista,
      proximo: 'rode ?action=modelos&curto=1 para ver o novo campeão' });
  }

  // ── 👤 QUEM-APROVA: usuários e permissões da conta (quem pode liberar) ──
  if (action === 'quem-aprova') {
    if (!CONTA) return res.status(200).json({ ok: false, error: 'conta não configurada' });
    const pega = async (rot, url) => {
      const r = await fetch(url).then(x => x.json()).catch(e => ({ error: { message: e.message } }));
      return r && r.error ? { rot, erro: r.error.message } : { rot, dados: r.data || r };
    };
    const [conta, users, sys, eu] = await Promise.all([
      pega('conta', `${GRAPH}/act_${CONTA}?fields=name,account_status,business,owner,disable_reason,funding_source_details&access_token=${TOKEN}`),
      pega('usuarios', `${GRAPH}/act_${CONTA}/assigned_users?fields=id,name,tasks&limit=50&access_token=${TOKEN}`),
      pega('meu_usuario', `${GRAPH}/me?fields=id,name&access_token=${TOKEN}`),
      pega('permissoes', `${GRAPH}/act_${CONTA}/users?fields=id,name,role,permissions&limit=50&access_token=${TOKEN}`),
    ]);
    const c = (conta.dados) || {};
    const MOTIVO = { 0:'sem restrição', 1:'ATIVIDADE INCOMUM', 2:'ANÚNCIOS RECUSADOS', 3:'POLÍTICAS DE ANÚNCIO',
      4:'PAGAMENTO PENDENTE', 5:'CONTA FECHADA', 6:'REVISÃO DE RISCO', 7:'ATRASO NO PAGAMENTO',
      8:'FALHA NO PAGAMENTO', 9:'IDENTIDADE NÃO CONFIRMADA', 10:'VIOLAÇÃO DE PERMISSÃO' };
    const STATUS = { 1:'ATIVA', 2:'DESATIVADA', 3:'CANCELADA', 7:'EM ANÁLISE', 9:'PERÍODO DE GRAÇA', 101:'FECHADA' };
    const donos = ((users.dados) || []).map(u => ({
      nome: u.name, id: u.id,
      papel: (u.tasks || []).includes('MANAGE') ? '👑 ADMINISTRADOR (pode liberar)'
        : ((u.tasks || []).includes('ADVERTISE') ? 'anunciante' : (u.tasks || []).join(', ')),
      tarefas: u.tasks }));
    return res.status(200).json({ ok: true,
      conta: { nome: c.name, status: STATUS[c.account_status] || c.account_status,
        motivoRestricao: MOTIVO[c.disable_reason] != null ? MOTIVO[c.disable_reason] : c.disable_reason,
        negocio: c.business ? c.business.name : null, negocioId: c.business ? c.business.id : null,
        dono: c.owner || null,
        formaPagamento: c.funding_source_details ? (c.funding_source_details.display_string || c.funding_source_details.type) : '(não informada)' },
      tokenUsadoPeloSistema: (sys.dados || {}).name || sys.erro,
      QUEM_PODE_LIBERAR: donos.filter(d => String(d.papel).includes('ADMIN')),
      todosOsUsuarios: donos,
      permissoes: permissoesErro(eu),
      dica: 'o administrador da conta e o admin do Gerenciador de Negócios são quem recebem a notificação de confirmação' });
  }
  function permissoesErro(r) { return r && r.erro ? ('sem acesso: ' + r.erro) : ((r && r.dados) || []); }

  // ── ✅ CONFERIR-APLICACAO: as últimas alterações do Copiloto valeram na Meta? ──
  if (action === 'conferir-aplicacao') {
    if (!CONTA) return res.status(200).json({ ok: false, error: 'conta não configurada' });
    const lg = (await dbGet('trafego_log')) || { movs: [] };
    const ultimo = (lg.movs || []).find(m => m.acao === 'aplicar' || m.pausas || m.verbas);
    if (!ultimo) return res.status(200).json({ ok: false, error: 'nenhuma aplicação registrada no log' });

    // estado atual na Meta
    const camps = await pegarTudo(`${GRAPH}/act_${CONTA}/campaigns?fields=id,name,effective_status,daily_budget,lifetime_budget&limit=200&access_token=${TOKEN}`, 8);
    const sets = await pegarTudo(`${GRAPH}/act_${CONTA}/adsets?fields=id,name,effective_status,daily_budget,lifetime_budget,campaign{id}&limit=200&access_token=${TOKEN}`, 8);
    const porCamp = {}; for (const c of (camps.data || [])) porCamp[c.id] = c;
    const setsDe = {};
    for (const s of (sets.data || [])) {
      const cid = (s.campaign || {}).id; if (!cid) continue;
      (setsDe[cid] = setsDe[cid] || []).push(s);
    }
    const verbaReal = (cid) => {
      const c = porCamp[cid];
      if (!c) return null;
      if (c.lifetime_budget) return { valor: Number(c.lifetime_budget) / 100, onde: 'campanha (total)' };
      if (c.daily_budget) return { valor: Number(c.daily_budget) / 100, onde: 'campanha (diária)' };
      let soma = 0, tipo = null;
      for (const s of (setsDe[cid] || [])) {
        if (s.lifetime_budget) { soma += Number(s.lifetime_budget) / 100; tipo = 'conjunto (total)'; }
        else if (s.daily_budget) { soma += Number(s.daily_budget) / 100; tipo = 'conjunto (diária)'; }
      }
      return soma ? { valor: Number(soma.toFixed(2)), onde: tipo } : null;
    };

    const conferePausa = (p) => {
      const cid = p.campanhaId || p.campaignId || p.id;
      const c = porCamp[cid];
      const pausadaNaCampanha = c && ['PAUSED', 'CAMPAIGN_PAUSED', 'ADSET_PAUSED'].includes(c.effective_status);
      const setsPausados = (setsDe[cid] || []).every(s => ['PAUSED', 'ADSET_PAUSED', 'CAMPAIGN_PAUSED'].includes(s.effective_status));
      const ok = pausadaNaCampanha || ((setsDe[cid] || []).length > 0 && setsPausados);
      return { nome: p.nome || (c && c.name) || cid, id: cid,
        situacaoAtual: c ? c.effective_status : '(não encontrada)',
        pausouDeVerdade: !!ok };
    };
    const confereVerba = (v) => {
      const cid = v.campanhaId || v.campaignId || v.id;
      const c = porCamp[cid];
      const real = verbaReal(cid);
      const esperado = Number(v.nova || v.novaVerba || v.valor || 0);
      const bate = real && esperado ? Math.abs(real.valor - esperado) < 1 : null;
      return { nome: v.nome || (c && c.name) || cid, id: cid,
        verbaEsperada: esperado || null,
        verbaNaMeta: real ? real.valor : null,
        onde: real ? real.onde : null,
        situacao: c ? c.effective_status : '(não encontrada)',
        aplicou: bate };
    };

    const pausas = (ultimo.pausas || ultimo.pausados || []).map(conferePausa);
    const verbas = (ultimo.verbas || ultimo.realocacoes || []).map(confereVerba);
    const falhouPausa = pausas.filter(p => !p.pausouDeVerdade);
    const falhouVerba = verbas.filter(v => v.aplicou === false);

    return res.status(200).json({
      ok: falhouPausa.length === 0 && falhouVerba.length === 0,
      aplicadoEm: ultimo.ts,
      pausas: { total: pausas.length, confirmadas: pausas.length - falhouPausa.length, detalhe: pausas },
      verbas: { total: verbas.length, confirmadas: verbas.length - falhouVerba.length, detalhe: verbas },
      alertas: [
        ...(falhouPausa.length ? ['❌ ' + falhouPausa.length + ' NÃO pausou: ' + falhouPausa.map(p => p.nome).join(', ')] : []),
        ...(falhouVerba.length ? ['❌ ' + falhouVerba.length + ' com verba divergente'] : []),
      ],
      resumo: pausas.map(p => (p.pausouDeVerdade ? '⏸️ ' : '❌ ') + String(p.nome).slice(0, 30) + ' | ' + p.situacaoAtual)
        .concat(verbas.map(v => (v.aplicou ? '💰 ' : '⚠️ ') + String(v.nome).slice(0, 30) +
          ' | esperado R$ ' + v.verbaEsperada + ' · na Meta R$ ' + v.verbaNaMeta + ' (' + (v.onde || '?') + ')')),
    });
  }

  // ── 🚨 ERROS-CONJUNTOS: problemas no nível do conjunto e do anúncio ──
  if (action === 'erros-conjuntos') {
    if (!CONTA) return res.status(200).json({ ok: false, error: 'conta não configurada' });
    const brt = d => d ? new Date(new Date(d).getTime() - 3 * 3600000).toISOString().slice(0, 16).replace('T', ' ') : null;
    // conjuntos com issues_info
    const sets = await pegarTudo(`${GRAPH}/act_${CONTA}/adsets?fields=id,name,status,effective_status,issues_info,start_time,end_time,daily_budget,lifetime_budget,campaign{id,name},targeting,promoted_object&limit=100&access_token=${TOKEN}`, 8);
    if (sets.erro && !sets.data.length) return res.status(200).json({ ok: false, erro: sets.erro });
    const recentes = (sets.data || []).filter(s => {
      const i = String(s.start_time || '').slice(0, 10);
      return i >= new Date(Date.now() - 3 * 3600000 - 2 * 86400000).toISOString().slice(0, 10);
    });
    const comProblema = [], ok = [];
    for (const s of recentes) {
      const issues = (s.issues_info || []).map(x => ({
        resumo: x.error_summary, mensagem: x.error_message,
        tipo: x.level, codigo: x.error_code,
      }));
      const item = { conjunto: s.name, id: s.id,
        campanha: (s.campaign || {}).name,
        situacao: s.effective_status,
        inicio: brt(s.start_time), fim: brt(s.end_time),
        verba: s.lifetime_budget ? Number(s.lifetime_budget) / 100 : (s.daily_budget ? Number(s.daily_budget) / 100 : null),
        destinoWhats: s.promoted_object ? (s.promoted_object.page_id ? 'página ' + s.promoted_object.page_id : JSON.stringify(s.promoted_object).slice(0, 60)) : '(sem destino!)',
        problemas: issues };
      (issues.length || s.effective_status !== 'ACTIVE' ? comProblema : ok).push(item);
    }
    // anúncios com problema
    const ads = await pegarTudo(`${GRAPH}/act_${CONTA}/ads?fields=id,name,effective_status,issues_info,adset{name}&limit=100&access_token=${TOKEN}`, 6);
    const adsRuins = (ads.data || []).filter(a => (a.issues_info || []).length || ['DISAPPROVED', 'WITH_ISSUES', 'PENDING_REVIEW'].includes(a.effective_status))
      .map(a => ({ anuncio: a.name, situacao: a.effective_status,
        conjunto: (a.adset || {}).name,
        problemas: (a.issues_info || []).map(x => x.error_summary || x.error_message) }));

    // separa o aviso inofensivo (agendado) dos problemas de verdade
    const ehAvisoNormal = p => /não está sendo veiculado, mas você não precisa fazer nada/i.test(String(p || ''));
    const graves = adsRuins.filter(a => (a.problemas || []).some(p => !ehAvisoNormal(p)));
    const soAgendados = adsRuins.length - graves.length;
    return res.status(200).json({ ok: comProblema.length === 0 && graves.length === 0,
      conjuntosDoCiclo: recentes.length,
      conjuntosOk: ok.length, conjuntosComProblema: comProblema.length,
      anunciosAgendadosOk: soAgendados,
      anunciosComProblemaReal: graves.length,
      PROBLEMAS_REAIS: graves.map(a => a.anuncio + ' | conjunto: ' + (a.conjunto || '?') +
        ' | ' + a.problemas.filter(p => !ehAvisoNormal(p)).join(' · ')),
      detalheConjuntos: comProblema.slice(0, 15),
      resumoProblemas: [...new Set([
        ...comProblema.flatMap(c => c.problemas.map(p => p.resumo || p.mensagem)),
        ...adsRuins.flatMap(a => a.problemas),
      ].filter(Boolean))].slice(0, 10) });
  }

  // ── 🔎 CICLO-AGORA: o que está no ar neste momento, com gasto e entrega ──
  if (action === 'ciclo-agora') {
    if (!CONTA) return res.status(200).json({ ok: false, error: 'conta não configurada' });
    const camps = await pegarTudo(`${GRAPH}/act_${CONTA}/campaigns?fields=id,name,status,effective_status,configured_status,daily_budget,lifetime_budget,start_time,stop_time,issues_info&limit=100&access_token=${TOKEN}`, 8);
    if (camps.erro && !camps.data.length) return res.status(200).json({ ok: false, erro: camps.erro });
    const brt = d => d ? new Date(new Date(d).getTime() - 3 * 3600000).toISOString().slice(0, 16).replace('T', ' ') : null;
    const hoje = new Date(Date.now() - 3 * 3600000).toISOString().slice(0, 10);
    // só as do ciclo atual (começaram hoje ou ontem)
    const doCiclo = (camps.data || []).filter(c => {
      const i = String(c.start_time || '').slice(0, 10);
      return i >= new Date(Date.now() - 3 * 3600000 - 2 * 86400000).toISOString().slice(0, 10);
    });
    // gasto de hoje por campanha
    const gastos = {};
    try {
      const ins = await pegarTudo(`${GRAPH}/act_${CONTA}/insights?level=campaign&date_preset=today&fields=campaign_id,spend,impressions&limit=100&access_token=${TOKEN}`, 4);
      for (const i of (ins.data || [])) gastos[i.campaign_id] = { gasto: Number(i.spend || 0), impressoes: Number(i.impressions || 0) };
    } catch (e) {}
    const linhas = doCiclo.map(c => {
      const g = gastos[c.id] || { gasto: 0, impressoes: 0 };
      const problemas = (c.issues_info || []).map(x => x.error_summary || x.error_message).filter(Boolean);
      return { nome: c.name, id: c.id,
        situacao: c.effective_status, configurado: c.configured_status,
        inicio: brt(c.start_time), fim: brt(c.stop_time),
        gastoHoje: g.gasto, impressoes: g.impressoes,
        entregando: g.impressoes > 0,
        problemas: problemas.length ? problemas : null };
    }).sort((a, b) => b.impressoes - a.impressoes);
    const entregando = linhas.filter(l => l.entregando).length;
    const comProblema = linhas.filter(l => l.problemas);
    const naoAtivas = linhas.filter(l => l.situacao !== 'ACTIVE');
    return res.status(200).json({ ok: true, agoraBRT: new Date(Date.now() - 3 * 3600000).toISOString().slice(0, 16).replace('T', ' '),
      totalDoCiclo: linhas.length,
      entregando, semEntrega: linhas.length - entregando,
      gastoHojeTotal: Number(linhas.reduce((s, l) => s + l.gastoHoje, 0).toFixed(2)),
      situacoes: linhas.reduce((o, l) => { o[l.situacao] = (o[l.situacao] || 0) + 1; return o; }, {}),
      alertas: [
        ...(naoAtivas.length ? ['⚠️ ' + naoAtivas.length + ' não estão ativas: ' + [...new Set(naoAtivas.map(n => n.situacao))].join(', ')] : []),
        ...(comProblema.length ? ['❌ ' + comProblema.length + ' com problema informado pela Meta'] : []),
        ...(entregando === 0 ? ['❌ NENHUMA entregando impressões'] : []),
      ],
      problemas: comProblema.map(c => c.nome + ' → ' + c.problemas.join(' | ')).slice(0, 10),
      lista: linhas.map(l => String(l.nome).slice(0, 30) + ' | ' + l.situacao +
        ' | R$ ' + l.gastoHoje.toFixed(2) + ' | ' + l.impressoes + ' impr') });
  }

  // ── ✅ CHECKUP-PROGRAMACAO: confere o que foi agendado para o próximo ciclo ──
  if (action === 'checkup-programacao') {
    if (!CONTA) return res.status(200).json({ ok: false, error: 'conta não configurada' });
    const cfgC = await cfgTrafego();
    // busca campanhas com agendamento futuro
    const camps = await pegarTudo(`${GRAPH}/act_${CONTA}/campaigns?fields=id,name,status,effective_status,daily_budget,lifetime_budget,start_time,stop_time,created_time&limit=100&access_token=${TOKEN}`, 8);
    if (camps.erro && !camps.data.length) return res.status(200).json({ ok: false, erro: camps.erro });
    const agora = Date.now();
    const futuras = (camps.data || []).filter(c => {
      const ini = c.start_time ? new Date(c.start_time).getTime() : 0;
      return ini > agora - 6 * 3600000;                      // começa agora ou no futuro
    });
    const brt = d => d ? new Date(new Date(d).getTime() - 3 * 3600000).toISOString().slice(0, 16).replace('T', ' ') : null;
    // ⚠️ a verba pode estar na CAMPANHA (CBO) ou no CONJUNTO. Buscar os conjuntos das
    // campanhas que vieram sem valor — senão elas aparecem como "sem verba" sem estar.
    const semNaCamp = futuras.filter(c => !c.lifetime_budget && !c.daily_budget).map(c => c.id);
    const verbaConj = {}, agendaConj = {};
    for (let i = 0; i < semNaCamp.length; i += 20) {
      const lote = semNaCamp.slice(i, i + 20).join(',');
      const j = await fetch(`${GRAPH}/?ids=${lote}&fields=adsets{id,daily_budget,lifetime_budget,start_time,end_time}&access_token=${TOKEN}`)
        .then(x => x.json()).catch(() => null);
      for (const k of Object.keys(j || {})) {
        const sets = (((j[k] || {}).adsets) || {}).data || [];
        let soma = 0, tipo = null, ini = null, fim = null;
        for (const s of sets) {
          if (s.lifetime_budget) { soma += Number(s.lifetime_budget) / 100; tipo = 'total (conjunto)'; }
          else if (s.daily_budget) { soma += Number(s.daily_budget) / 100; tipo = 'diária (conjunto)'; }
          if (s.start_time && !ini) ini = s.start_time;
          if (s.end_time && !fim) fim = s.end_time;
        }
        if (soma > 0) verbaConj[k] = { soma: Number(soma.toFixed(2)), tipo, conjuntos: sets.length };
        if (ini || fim) agendaConj[k] = { ini, fim };
      }
      await new Promise(r => setTimeout(r, 120));
    }
    const linhas = futuras.map(c => {
      let verba = c.lifetime_budget ? Number(c.lifetime_budget) / 100
        : (c.daily_budget ? Number(c.daily_budget) / 100 : null);
      let tipoVerba = c.lifetime_budget ? 'total (campanha)' : (c.daily_budget ? 'diária (campanha)' : null);
      let onde = 'campanha';
      if (verba == null && verbaConj[c.id]) {
        verba = verbaConj[c.id].soma; tipoVerba = verbaConj[c.id].tipo; onde = 'conjunto';
      }
      const ag = agendaConj[c.id] || {};
      return { nome: c.name, id: c.id, status: c.effective_status || c.status,
        verba, tipoVerba: tipoVerba || 'sem verba', verbaEm: verba != null ? onde : null,
        inicio: brt(c.start_time || ag.ini), fim: brt(c.stop_time || ag.fim),
        categoria: categoriaDe(c.name || '', 'anuncio') };
    }).sort((a, b) => String(a.inicio).localeCompare(String(b.inicio)));

    // validações
    const alertas = [];
    const verbas = [...new Set(linhas.map(l => l.verba))];
    if (verbas.length > 1) alertas.push('⚠️ verbas diferentes entre campanhas: ' + verbas.map(v => 'R$ ' + v).join(', '));
    const semVerba = linhas.filter(l => !l.verba);
    if (semVerba.length) alertas.push('❌ ' + semVerba.length + ' campanha(s) SEM verba definida');
    const inicios = [...new Set(linhas.map(l => l.inicio))];
    if (inicios.length > 1) alertas.push('⚠️ horários de início diferentes: ' + inicios.join(' · '));
    const fins = [...new Set(linhas.map(l => l.fim))];
    if (fins.length > 1) alertas.push('⚠️ horários de término diferentes: ' + fins.join(' · '));
    const semFim = linhas.filter(l => !l.fim);
    if (semFim.length) alertas.push('❌ ' + semFim.length + ' campanha(s) SEM data de término — não vão encerrar sozinhas');
    const paradas = linhas.filter(l => !['ACTIVE', 'SCHEDULED', 'PENDING_REVIEW', 'IN_PROCESS'].includes(l.status));
    if (paradas.length) alertas.push('⚠️ ' + paradas.length + ' com status inesperado: ' + [...new Set(paradas.map(p => p.status))].join(', '));

    const total = linhas.reduce((s, l) => s + (l.verba || 0), 0);
    const porCat = linhas.reduce((o, l) => { o[l.categoria] = (o[l.categoria] || 0) + 1; return o; }, {});
    return res.status(200).json({ ok: alertas.length === 0,
      campanhasAgendadas: linhas.length,
      verbaTotal: Number(total.toFixed(2)),
      verbaPorCampanha: verbas.length === 1 ? verbas[0] : verbas,
      inicio: inicios.length === 1 ? inicios[0] : inicios,
      fim: fins.length === 1 ? fins[0] : fins,
      porCategoria: porCat,
      alertas: alertas.length ? alertas : ['✅ nenhum problema encontrado'],
      verbaEm: linhas.reduce((o, l) => { const k = l.verbaEm || 'sem verba'; o[k] = (o[k] || 0) + 1; return o; }, {}),
      lista: linhas.map(l => (l.categoria || '?').slice(0, 4).toUpperCase() + ' | ' + String(l.nome).slice(0, 28) +
        ' | R$ ' + (l.verba || 0) + ' (' + (l.verbaEm || '—') + ') | ' + l.status) });
  }

  // ── 📊 CUSTO-POR-FICHA: fichas recebidas x investimento, por semana e por frente ──
  if (action === 'custo-por-ficha') {
    if (!CONTA) return res.status(200).json({ ok: false, error: 'conta não configurada' });
    const dias = Math.min(180, Math.max(7, parseInt(req.query.dias || '60', 10)));
    const ate = new Date(Date.now() - 3 * 3600 * 1000).toISOString().slice(0, 10);
    const desde = new Date(Date.now() - 3 * 3600 * 1000 - dias * 86400000).toISOString().slice(0, 10);
    const detalhado = String(req.query.detalhado || '') === '1';

    // ── investimento por dia e categoria ──
    const janela = 'time_range=' + encodeURIComponent(JSON.stringify({ since: desde, until: ate }));
    const ins = await pegarTudo(`${GRAPH}/act_${CONTA}/insights?level=ad&${janela}&time_increment=1&fields=ad_name,spend,date_start&limit=200&access_token=${TOKEN}`, 20);
    if (ins.erro && !ins.data.length) return res.status(200).json({ ok: false, erro: ins.erro });

    const semanaDe = (iso) => {
      const d = new Date(iso + 'T12:00:00Z');
      const dow = d.getUTCDay();                       // ciclo começa no sábado
      const ini = new Date(d); ini.setUTCDate(d.getUTCDate() - ((dow + 1) % 7));
      return ini.toISOString().slice(0, 10);
    };
    const sem = {};
    const garante = (s) => { if (!sem[s]) sem[s] = {
      semana: s, invAdm: 0, invTv: 0, fichasAdm: 0, fichasTv: 0, porCategoria: {} }; return sem[s]; };

    for (const i of (ins.data || [])) {
      const cat = categoriaDe(i.ad_name || '', 'anuncio');
      const v = Number(i.spend || 0);
      const s = garante(semanaDe(i.date_start));
      if (cat === 'tv') s.invTv += v; else s.invAdm += v;
      s.porCategoria[cat] = s.porCategoria[cat] || { investido: 0, fichas: 0 };
      s.porCategoria[cat].investido += v;
    }

    // ── fichas recebidas por dia ──
    const [fA, fT, lgA, lgT, arqA, arqT] = await Promise.all([
      dbGet('fichas_adm'), dbGet('fichas_tv'),
      dbGet('reparoeletro_logistica'), dbGet('tv_logistica'),
      dbGet('reparoeletro_arquivo'), dbGet('tv_arquivo'),
    ]);
    const corteMs = Date.now() - dias * 86400000;
    const vistos = new Set();
    const contaFicha = (f, ehTv) => {
      const q = new Date(f.criadoEm || f.entradaEm || f.createdAt || 0).getTime();
      if (!q || q < corteMs) return;
      const d8 = String(f.telefone || '').replace(/\D/g, '').slice(-8);
      const chave = d8 + '|' + new Date(q).toISOString().slice(0, 10);
      if (d8.length >= 8 && vistos.has(chave)) return;   // dedupe: mesma pessoa no mesmo dia
      if (d8.length >= 8) vistos.add(chave);
      const s = garante(semanaDe(new Date(q - 3 * 3600000).toISOString().slice(0, 10)));
      if (ehTv) s.fichasTv++; else s.fichasAdm++;
      const cat = categoriaDe(f.equipamento || f.descricao || '', 'equipamento');
      s.porCategoria[cat] = s.porCategoria[cat] || { investido: 0, fichas: 0 };
      s.porCategoria[cat].fichas++;
    };
    for (const b of [fA, lgA, arqA]) for (const f of (((b || {}).fichas) || (((b || {}).cards) || []))) contaFicha(f, false);
    for (const b of [fT, lgT, arqT]) for (const f of (((b || {}).fichas) || (((b || {}).cards) || []))) contaFicha(f, true);

    const semanas = Object.values(sem).sort((a, b) => a.semana.localeCompare(b.semana))
      .map(s => ({
        semana: s.semana,
        adm: { investido: Number(s.invAdm.toFixed(2)), fichas: s.fichasAdm,
          custoPorFicha: s.fichasAdm ? Number((s.invAdm / s.fichasAdm).toFixed(2)) : null },
        tv: { investido: Number(s.invTv.toFixed(2)), fichas: s.fichasTv,
          custoPorFicha: s.fichasTv ? Number((s.invTv / s.fichasTv).toFixed(2)) : null },
        ...(detalhado ? { categorias: Object.keys(s.porCategoria).reduce((o, c) => {
          const x = s.porCategoria[c];
          o[c] = { investido: Number(x.investido.toFixed(2)), fichas: x.fichas,
            custoPorFicha: x.fichas ? Number((x.investido / x.fichas).toFixed(2)) : null };
          return o; }, {}) } : {}),
      }));

    const tot = semanas.reduce((o, s) => ({
      invAdm: o.invAdm + s.adm.investido, fAdm: o.fAdm + s.adm.fichas,
      invTv: o.invTv + s.tv.investido, fTv: o.fTv + s.tv.fichas,
    }), { invAdm: 0, fAdm: 0, invTv: 0, fTv: 0 });

    return res.status(200).json({ ok: true, periodoDias: dias, de: desde, ate,
      totais: {
        adm: { investido: Number(tot.invAdm.toFixed(2)), fichas: tot.fAdm,
          custoPorFicha: tot.fAdm ? Number((tot.invAdm / tot.fAdm).toFixed(2)) : null },
        tv: { investido: Number(tot.invTv.toFixed(2)), fichas: tot.fTv,
          custoPorFicha: tot.fTv ? Number((tot.invTv / tot.fTv).toFixed(2)) : null },
      },
      semanas: semanas.map(s => s.semana + ' | ADM ' + s.adm.fichas + ' fichas · ' + brlNum(s.adm.investido) +
        ' · ' + (s.adm.custoPorFicha != null ? 'R$ ' + s.adm.custoPorFicha + '/ficha' : '—') +
        ' || TV ' + s.tv.fichas + ' fichas · ' + brlNum(s.tv.investido) +
        ' · ' + (s.tv.custoPorFicha != null ? 'R$ ' + s.tv.custoPorFicha + '/ficha' : '—')),
      detalhe: semanas,
      observacao: 'semana começa no sábado, acompanhando o ciclo de anúncios · fichas contadas sem duplicar o mesmo cliente no mesmo dia · inclui fichas orgânicas (GMB, indicação), então o custo por ficha é conservador' });
  }
  function brlNum(v) { return 'R$ ' + Number(v || 0).toFixed(2); }

  // ── 🩺 META-SAUDE: testa a comunicação com a Meta ponta a ponta ──
  if (action === 'meta-saude') {
    const t0 = Date.now();
    const passos = [];
    const chamar = async (rot, url) => {
      const ini = Date.now();
      const r = await fetch(url).then(x => x.json()).catch(e => ({ error: { message: e.message } }));
      const ms = Date.now() - ini;
      if (r && r.error) {
        passos.push({ etapa: rot, ok: false, ms, erro: r.error.message, codigo: r.error.code, subcodigo: r.error.error_subcode });
        return null;
      }
      passos.push({ etapa: rot, ok: true, ms });
      return r;
    };
    // 1) credenciais configuradas?
    passos.push({ etapa: 'variáveis de ambiente', ok: !!(TOKEN && CONTA),
      detalhe: (TOKEN ? 'token presente' : 'TOKEN AUSENTE') + ' · ' + (CONTA ? 'conta act_' + CONTA : 'CONTA AUSENTE') });
    if (!TOKEN || !CONTA) return res.status(200).json({ ok: false, passos, veredito: '❌ credenciais ausentes na Vercel' });
    // 2) o token é válido?
    const eu = await chamar('token válido', `${GRAPH}/me?fields=id,name&access_token=${TOKEN}`);
    // 3) a conta responde?
    const conta = await chamar('conta de anúncios', `${GRAPH}/act_${CONTA}?fields=name,account_status,currency,balance,spend_cap&access_token=${TOKEN}`);
    // 4) leitura de campanhas
    const camp = await chamar('ler campanhas', `${GRAPH}/act_${CONTA}/campaigns?fields=id&limit=1&access_token=${TOKEN}`);
    // 5) leitura de métricas
    const ins = await chamar('ler métricas (insights)', `${GRAPH}/act_${CONTA}/insights?date_preset=today&fields=spend&access_token=${TOKEN}`);
    // 6) biblioteca de vídeos
    const vid = await chamar('biblioteca de vídeos', `${GRAPH}/act_${CONTA}/advideos?fields=id&limit=1&access_token=${TOKEN}`);
    // 7) permissão de escrita (sem alterar nada: pede o campo status de uma campanha)
    let escrita = { etapa: 'permissão de escrita', ok: null, detalhe: 'não testada' };
    try {
      const c1 = (camp && camp.data && camp.data[0]) ? camp.data[0].id : null;
      if (c1) {
        const r = await fetch(`${GRAPH}/${c1}?fields=status,effective_status&access_token=${TOKEN}`).then(x => x.json());
        escrita = r && !r.error
          ? { etapa: 'permissão de escrita', ok: true, detalhe: 'acesso de gestão confirmado' }
          : { etapa: 'permissão de escrita', ok: false, erro: (r.error || {}).message };
      }
    } catch (e) { escrita = { etapa: 'permissão de escrita', ok: false, erro: e.message }; }
    passos.push(escrita);

    const cont = conta || {};
    const falhas = passos.filter(p => p.ok === false);
    const STATUS = { 1: 'ATIVA', 2: 'DESATIVADA', 3: 'CANCELADA', 7: 'EM ANÁLISE', 9: 'PERÍODO DE GRAÇA', 101: 'FECHADA' };
    return res.status(200).json({ ok: falhas.length === 0,
      conta: cont.name ? { nome: cont.name, status: STATUS[cont.account_status] || cont.account_status,
        moeda: cont.currency, saldo: cont.balance ? (cont.balance / 100).toFixed(2) : null,
        tetoDeGasto: cont.spend_cap ? (cont.spend_cap / 100).toFixed(2) : 'sem teto' } : null,
      usuario: eu ? eu.name : null,
      passos, tempoTotalMs: Date.now() - t0,
      veredito: falhas.length === 0
        ? '✅ comunicação com a Meta saudável'
        : '❌ ' + falhas.length + ' falha(s): ' + falhas.map(f => f.etapa).join(', ') });
  }

  // ── 🎬 VIDEOS-META: lista a biblioteca de vídeos da conta de anúncios ──
  if (action === 'videos-meta') {
    if (!CONTA) return res.status(200).json({ ok: false, error: 'META_ADS_ACCOUNT não configurado' });
    const r = await pegarTudo(`${GRAPH}/act_${CONTA}/advideos?fields=id,title,created_time,status&limit=100&access_token=${TOKEN}`, 4);
    if (r.erro && !r.data.length) return res.status(200).json({ ok: false, erro: r.erro });
    const vids = (r.data || []).map(v => ({
      id: v.id, titulo: v.title || '(sem título)',
      criadoEm: v.created_time,
      pronto: !v.status || v.status.video_status === 'ready',
      categoria: categoriaDe(v.title || '', 'equipamento'),
    })).sort((a, b) => String(b.criadoEm).localeCompare(String(a.criadoEm)));
    if (String(req.query.curto || '') === '1') {
      return res.status(200).json({ ok: true, total: vids.length,
        lista: vids.slice(0, 40).map(v => v.titulo.slice(0, 40) + ' | ' + v.categoria + ' | ' +
          (v.pronto ? 'pronto' : 'processando') + ' | ' + String(v.criadoEm).slice(0, 10)) });
    }
    return res.status(200).json({ ok: true, total: vids.length,
      porCategoria: vids.reduce((o, v) => { o[v.categoria] = (o[v.categoria] || 0) + 1; return o; }, {}),
      videos: vids });
  }

  // ── 📥 REGISTRAR-DA-META: usa os vídeos já subidos na Meta como criativos da semana ──
  // ── 🔗 REGISTRAR-SEMANA: link simples, sem acentos nem espaços na URL ──
  // Uso: ?action=registrar-semana&desde=2026-07-31&naoclassificado=adega
  if (action === 'registrar-semana') {
    const desde = String(req.query.desde || '').slice(0, 10);
    if (!/^\d{4}-\d{2}-\d{2}$/.test(desde)) {
      return res.status(400).json({ ok: false, error: 'informe ?desde=AAAA-MM-DD' });
    }
    const fallback = String(req.query.naoclassificado || '').toLowerCase().trim();
    const CATS_OK = ['tv', 'microondas', 'purificador', 'adega'];
    if (fallback && !CATS_OK.includes(fallback)) {
      return res.status(400).json({ ok: false, error: 'naoclassificado deve ser: ' + CATS_OK.join(', ') });
    }
    const r = await pegarTudo(`${GRAPH}/act_${CONTA}/advideos?fields=id,title,created_time&limit=100&access_token=${TOKEN}`, 6);
    const doDia = (r.data || []).filter(v => String(v.created_time || '').slice(0, 10) >= desde);
    const chave = 'trafego_criativos_' + (req.query.semana || 'atual');
    const atual = String(req.query.substituir || '1') === '1' ? { itens: [] } : ((await dbGet(chave)) || { itens: [] });
    const aceitos = [], recusados = [];
    for (const v of doDia) {
      let cat = categoriaDe(v.title || '', 'equipamento');
      if (!CATS_OK.includes(cat)) cat = fallback;              // não classificou → vai para o fallback
      if (!CATS_OK.includes(cat)) { recusados.push(String(v.title || v.id).slice(0, 45)); continue; }
      if (atual.itens.some(x => x.videoId === v.id)) continue;
      aceitos.push({ nome: String(v.title || v.id).slice(0, 80), categoria: cat,
        videoId: v.id, url: null, tipo: 'video-meta', registradoEm: new Date().toISOString() });
    }
    atual.itens = atual.itens.concat(aceitos);
    atual.atualizadoEm = new Date().toISOString();
    await dbSet(chave, atual);
    return res.status(200).json({ ok: true, videosNoPeriodo: doDia.length, aceitos: aceitos.length,
      recusados,
      porCategoria: atual.itens.reduce((o, x) => { o[x.categoria] = (o[x.categoria] || 0) + 1; return o; }, {}),
      lista: aceitos.map(a => a.categoria + ' | ' + a.nome.slice(0, 42)) });
  }

  // ── 🔗 REGISTRAR-DA-META-LINK: mesma coisa, mas por link simples (sem POST) ──
  // Uso: ?action=registrar-da-meta-link&desde=2026-07-31&adega=titulo1|titulo2
  if (action === 'registrar-da-meta-link') {
    const desde = String(req.query.desde || '').slice(0, 10);
    if (!/^\d{4}-\d{2}-\d{2}$/.test(desde)) {
      return res.status(400).json({ ok: false, error: 'informe ?desde=AAAA-MM-DD (recorte obrigatório)' });
    }
    const cats = {};
    for (const c of ['tv', 'microondas', 'purificador', 'adega']) {
      const v = String(req.query[c] || '').trim();
      if (v) for (const t of v.split('|')) if (t.trim()) cats[t.trim()] = c;
    }
    const r = await fetch(`https://reparoeletroadm.com/api/trafego?action=registrar-da-meta&k=${(process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim()}`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ desde, substituir: String(req.query.substituir || '1') === '1', categorias: cats }),
    }).then(x => x.json()).catch(e => ({ ok: false, error: e.message }));
    return res.status(200).json(r);
  }

  if (req.method === 'POST' && action === 'registrar-da-meta') {
    const { semana, ids, substituir, desde, categorias } = req.body || {};
    // 🚫 TRAVA: a biblioteca tem centenas de vídeos antigos. Exige recorte explícito
    // (lista de ids OU data), senão registraria o histórico inteiro como criativo da semana.
    if (!(Array.isArray(ids) && ids.length) && !desde) {
      return res.status(400).json({ ok: false,
        error: 'informe "ids" (lista) ou "desde" (data AAAA-MM-DD) — sem recorte eu registraria toda a biblioteca' });
    }
    const chave = 'trafego_criativos_' + (semana || 'atual');
    const atual = substituir ? { itens: [] } : ((await dbGet(chave)) || { itens: [] });
    const r = await pegarTudo(`${GRAPH}/act_${CONTA}/advideos?fields=id,title,created_time&limit=100&access_token=${TOKEN}`, 6);
    const todos = r.data || [];
    let filtro = todos;
    if (Array.isArray(ids) && ids.length) filtro = todos.filter(v => ids.includes(v.id));
    else if (desde) filtro = todos.filter(v => String(v.created_time || '').slice(0, 10) >= String(desde));
    const CATS_OK = ['tv', 'microondas', 'purificador', 'adega'];
    const aceitos = [], semCategoria = [];
    const overrides = categorias || {};              // correção manual por título ou id
    for (const v of filtro) {
      let over = overrides[v.id] || overrides[v.title];
      if (!over) {                                  // casa por trecho do título também
        for (const k of Object.keys(overrides)) {
          if (k && String(v.title || '').toLowerCase().includes(String(k).toLowerCase())) { over = overrides[k]; break; }
        }
      }
      const cat = String(over || categoriaDe(v.title || '', 'equipamento')).toLowerCase();
      if (!CATS_OK.includes(cat)) { semCategoria.push({ id: v.id, titulo: v.title || '(sem título)' }); continue; }
      if (atual.itens.some(x => x.videoId === v.id)) continue;
      aceitos.push({ nome: String(v.title || v.id).slice(0, 80), categoria: cat,
        videoId: v.id, url: null, tipo: 'video-meta', registradoEm: new Date().toISOString() });
    }
    atual.itens = atual.itens.concat(aceitos);
    atual.atualizadoEm = new Date().toISOString();
    await dbSet(chave, atual);
    return res.status(200).json({ ok: true, aceitos: aceitos.length,
      naoClassificados: semCategoria.slice(0, 20),
      totalNaSemana: atual.itens.length,
      porCategoria: atual.itens.reduce((o, x) => { o[x.categoria] = (o[x.categoria] || 0) + 1; return o; }, {}) });
  }

  // ── 📥 CRIATIVOS-REGISTRAR: cadastra os criativos da semana (nome, categoria, URL do vídeo) ──
  if (req.method === 'POST' && action === 'criativos-registrar') {
    const { semana, criativos, substituir } = req.body || {};
    if (!Array.isArray(criativos)) return res.status(400).json({ ok: false, error: 'criativos deve ser uma lista' });
    const chave = 'trafego_criativos_' + (semana || 'atual');
    const atual = substituir ? { itens: [] } : ((await dbGet(chave)) || { itens: [] });
    const CATS_OK = ['tv', 'microondas', 'purificador', 'adega'];
    const aceitos = [], recusados = [];
    for (const c of criativos) {
      const cat = String(c.categoria || '').toLowerCase();
      if (!CATS_OK.includes(cat)) { recusados.push({ nome: c.nome, motivo: 'categoria inválida: ' + cat }); continue; }
      if (!c.url) { recusados.push({ nome: c.nome, motivo: 'sem URL do vídeo' }); continue; }
      if (atual.itens.some(x => x.url === c.url)) { recusados.push({ nome: c.nome, motivo: 'já cadastrado' }); continue; }
      aceitos.push({ nome: String(c.nome || 'sem nome').slice(0, 80), categoria: cat,
        url: String(c.url), tipo: c.tipo || 'video', registradoEm: new Date().toISOString() });
    }
    atual.itens = atual.itens.concat(aceitos);
    atual.atualizadoEm = new Date().toISOString();
    await dbSet(chave, atual);
    return res.status(200).json({ ok: true, aceitos: aceitos.length, recusados,
      totalNaSemana: atual.itens.length,
      porCategoria: atual.itens.reduce((o, x) => { o[x.categoria] = (o[x.categoria] || 0) + 1; return o; }, {}) });
  }

  // ── 📋 PLANO-SEMANA: campeões + novos, rateio da verba, prévia para revisão ──
  if (action === 'plano-semana') {
    const cfgP = await cfgTrafego();
    const semana = String(req.query.semana || 'atual');
    const novos = (await dbGet('trafego_criativos_' + semana)) || { itens: [] };
    const base = await dbGet('trafego_painel_cache_7d') || await dbGet('trafego_painel_cache');
    if (!base || !base.dados) return res.status(200).json({ ok: false, error: 'abra o painel primeiro' });

    // campeões atuais por categoria (mesmo critério da action modelos)
    const cfgX = (await dbGet('trafego_config')) || {};
    const fora = (cfgX.modelosExcluir || ['reforma', 'pintura', 'restaura']).map(s => String(s).toLowerCase());
    const ativos = (base.dados.anuncios || []).filter(a => a.ativo && a.conversas >= 3 && a.cpa != null
      && !fora.some(p => String(a.nome || '').toLowerCase().includes(p)));
    const CATS = ['tv', 'microondas', 'purificador', 'adega'];
    const plano = [];
    for (const cat of CATS) {
      const daCat = ativos.filter(a => a.categoria === cat).sort((a, b) => (a.razaoMeta || 9) - (b.razaoMeta || 9));
      // ✅ REGRA DO DONO: entra no próximo ciclo TODO anúncio dentro da meta de custo,
      // não apenas o melhor. O campeão serve só como modelo para clonar os criativos novos.
      const mantidos = daCat.filter(a => (a.razaoMeta || 9) <= 1);
      const campeao = daCat[0] || null;
      const novosCat = (novos.itens || []).filter(x => x.categoria === cat);
      plano.push({ categoria: cat,
        mantidos: mantidos.map(a => ({ nome: a.nome, cpa: a.cpa, meta: a.meta, conversas: a.conversas,
          anuncioId: a.id, campanhaId: a.campanhaId })),
        campeao: campeao ? { nome: campeao.nome, cpa: campeao.cpa, conversas: campeao.conversas,
          anuncioId: campeao.id, adsetId: campeao.adsetId, campanhaId: campeao.campanhaId } : null,
        novos: novosCat.map(x => ({ nome: x.nome, url: x.url })),
        totalAnuncios: mantidos.length + novosCat.length,
        foraDaMeta: daCat.filter(a => (a.razaoMeta || 9) > 1)
          .map(a => ({ nome: a.nome, cpa: a.cpa, meta: a.meta })),
      });
    }
    // rateio: TV é frente própria; as demais dividem a verba do ADM
    const vTv = cfgP.verba.tv * cfgP.verba.aproveitamento;
    const vAdm = cfgP.verba.adm * cfgP.verba.aproveitamento;
    const nTv = plano.find(p => p.categoria === 'tv').totalAnuncios || 0;
    const nAdm = plano.filter(p => p.categoria !== 'tv').reduce((s, p) => s + p.totalAnuncios, 0);
    const porAnuncioTv = nTv ? Number((vTv / nTv).toFixed(2)) : 0;
    const porAnuncioAdm = nAdm ? Number((vAdm / nAdm).toFixed(2)) : 0;
    for (const p of plano) p.verbaPorAnuncio = p.categoria === 'tv' ? porAnuncioTv : porAnuncioAdm;

    return res.status(200).json({ ok: true, semana,
      ciclo: 'sábado 13h → sábado 11h',
      verba: { adm: { total: Number(vAdm.toFixed(2)), anuncios: nAdm, porAnuncio: porAnuncioAdm },
               tv:  { total: Number(vTv.toFixed(2)),  anuncios: nTv,  porAnuncio: porAnuncioTv } },
      plano,
      totalMantidos: plano.reduce((s, p) => s + p.mantidos.length, 0),
      totalSubir: plano.reduce((s, p) => s + p.novos.length, 0),
      foraDaMeta: plano.reduce((s, p) => s + p.foraDaMeta.length, 0),
      resumo: 'Vão rodar ' + (nAdm + nTv) + ' anúncios: ' +
        plano.reduce((s, p) => s + p.mantidos.length, 0) + ' dentro da meta que continuam e ' +
        plano.reduce((s, p) => s + p.novos.length, 0) + ' criativos novos. ' +
        plano.reduce((s, p) => s + p.foraDaMeta.length, 0) + ' ficaram fora da meta e não entram.' });
  }

  // ── 📅 MONTAR-SEMANA: cria os anúncios do próximo ciclo, agendados ──
  // Não pausa nada: as campanhas atuais têm término no sábado 11h e encerram sozinhas.
  if (req.method === 'POST' && action === 'montar-semana') {
    const { confirmar, semana, apenasUm } = req.body || {};
    const cfgM = await cfgTrafego();
    const novos = (await dbGet('trafego_criativos_' + (semana || 'atual'))) || { itens: [] };
    const base = await dbGet('trafego_painel_cache_7d') || await dbGet('trafego_painel_cache');
    if (!base || !base.dados) return res.status(200).json({ ok: false, error: 'abra o painel primeiro' });

    // datas do ciclo: próximo sábado 13h → sábado seguinte 11h (horário de Brasília = UTC-3)
    const agoraB = new Date(Date.now() - 3 * 3600 * 1000);
    const diasAteSab = (6 - agoraB.getUTCDay() + 7) % 7 || 7;
    const inicio = new Date(Date.UTC(agoraB.getUTCFullYear(), agoraB.getUTCMonth(), agoraB.getUTCDate() + diasAteSab, 16, 0, 0)); // 13h BRT
    const fim = new Date(inicio.getTime() + (6 * 86400000) + (22 * 3600000));                                                     // sáb 11h BRT
    const iniISO = Math.floor(inicio.getTime() / 1000), fimISO = Math.floor(fim.getTime() / 1000);

    // campeão (modelo) por categoria
    const cfgX = (await dbGet('trafego_config')) || {};
    const fora = (cfgX.modelosExcluir || ['reforma', 'pintura', 'restaura']).map(s => String(s).toLowerCase());
    const ativos = (base.dados.anuncios || []).filter(a => a.ativo && a.conversas >= 3 && a.cpa != null
      && !fora.some(p => String(a.nome || '').toLowerCase().includes(p)));
    const modeloDe = cat => ativos.filter(a => a.categoria === cat)
      .sort((a, b) => (a.razaoMeta || 9) - (b.razaoMeta || 9))[0] || null;

    // lista final: campeão de cada categoria (recriado) + criativos novos
    const CATS = ['tv', 'microondas', 'purificador', 'adega'];
    const fila = [];
    for (const cat of CATS) {
      const mod = modeloDe(cat);
      // TODOS os que estão dentro da meta continuam no próximo ciclo
      const dentroDaMeta = ativos.filter(a => a.categoria === cat && (a.razaoMeta || 9) <= 1)
        .sort((a, b) => (a.razaoMeta || 9) - (b.razaoMeta || 9));
      const selo = ' [C' + String(new Date().getDate()).padStart(2, '0') + ']';
      for (const a of dentroDaMeta) {
        fila.push({ cat, tipo: 'mantido', nome: a.nome + selo, url: null, modelo: a });
      }
      if (!mod) continue;
      for (const n of (novos.itens || []).filter(x => x.categoria === cat)) {
        fila.push({ cat, tipo: 'novo', nome: n.nome, url: n.url, videoId: n.videoId || null, modelo: mod });
      }
    }
    const nTv = fila.filter(f => f.cat === 'tv').length;
    const nAdm = fila.length - nTv;
    const vbTv = nTv ? (cfgM.verba.tv * cfgM.verba.aproveitamento) / nTv : 0;
    const vbAdm = nAdm ? (cfgM.verba.adm * cfgM.verba.aproveitamento) / nAdm : 0;
    for (const f of fila) f.verba = Number((f.cat === 'tv' ? vbTv : vbAdm).toFixed(2));

    if (confirmar !== true) {
      return res.status(200).json({ ok: true, modo: 'prévia',
        inicio: inicio.toISOString(), fim: fim.toISOString(),
        total: fila.length, tv: nTv, adm: nAdm,
        verbaPorAnuncio: { tv: Number(vbTv.toFixed(2)), adm: Number(vbAdm.toFixed(2)) },
        itens: fila.map(f => f.cat.toUpperCase() + ' | ' + f.tipo + ' | ' + String(f.nome).slice(0, 34) +
          ' | R$ ' + f.verba + (f.modelo ? ' | modelo: ' + String(f.modelo.nome).slice(0, 22) : ' | SEM MODELO')),
        observacao: 'nenhuma campanha atual será pausada — elas encerram sozinhas no sábado 11h' });
    }

    // ── EXECUÇÃO ──
    const alvo = apenasUm ? fila.filter(f => f.tipo === 'novo').slice(0, 1) : fila;
    if (String((req.body || {}).explicar || '') === '1') {
      return res.status(200).json({ ok: true, modo: 'explicação (nada foi enviado à Meta)',
        exemplo: alvo.map(f => ({
          nome: f.nome, categoria: f.cat,
          videoJaNaMeta: f.videoId || '(nenhum)',
          campanhaModelo: f.modelo ? f.modelo.campanhaId : '(sem modelo)',
          modeloNome: f.modelo ? f.modelo.nome : null,
          verbaCentavos: Math.round(f.verba * 100),
          inicioUnix: iniISO, fimUnix: fimISO,
          inicioLegivel: inicio.toISOString(), fimLegivel: fim.toISOString(),
        })) });
    }
    const feitos = [], erros = [];
    const postForm = async (path, campos) => {
      const corpo = new URLSearchParams(campos).toString();
      const r = await fetch(`${GRAPH}/${path}?access_token=${TOKEN}`, {
        method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body: corpo,
      }).then(x => x.json()).catch(e => ({ error: { message: e.message } }));
      if (r && r.error) {
        // guarda o detalhe completo para diagnóstico (a Meta manda 'parâmetro inválido' genérico)
        r._detalhe = {
          endpoint: path,
          enviado: campos,
          mensagem: r.error.message,
          usuario: r.error.error_user_msg || null,
          titulo: r.error.error_user_title || null,
          codigo: r.error.code, subcodigo: r.error.error_subcode,
          campo: r.error.error_data ? JSON.stringify(r.error.error_data).slice(0, 200) : null,
        };
      }
      return r;
    };
    const erroDe = (r, etapa) => etapa + ' → ' + [
      (r._detalhe && r._detalhe.mensagem) || 'sem mensagem',
      r._detalhe && r._detalhe.usuario ? '(' + r._detalhe.usuario + ')' : '',
      r._detalhe ? 'cód ' + r._detalhe.codigo + (r._detalhe.subcodigo ? '/' + r._detalhe.subcodigo : '') : '',
      r._detalhe && r._detalhe.campo ? '| ' + r._detalhe.campo : '',
    ].filter(Boolean).join(' ');
    for (const f of alvo) {
      try {
        if (!f.modelo || !f.modelo.campanhaId) { erros.push(f.nome + ': sem campanha modelo'); continue; }
        // 1) vídeo novo (a Meta baixa da URL — arquivo grande não passa pelo nosso servidor)
        let videoId = f.videoId || null;                 // já subido na Meta → usa direto
        if (!videoId && f.url) {
          const v = await postForm('act_' + CONTA + '/advideos', { file_url: f.url, name: f.nome });
          if (v && v.error) { erros.push(f.nome + ' | ' + erroDe(v, 'upload do vídeo')); continue; }
          videoId = v && v.id;
        }
        // 2) duplica a campanha campeã (leva conjunto, segmentação e destino WhatsApp)
        const cp = await postForm(f.modelo.campanhaId + '/copies', {
          deep_copy: 'true', status_option: 'PAUSED', rename_options: JSON.stringify({ rename_strategy: 'NO_RENAME' }),
        });
        if (cp && cp.error) { erros.push(f.nome + ' | ' + erroDe(cp, 'copiar campanha ' + f.modelo.campanhaId)); continue; }
        const novaCampanha = cp && (cp.copied_campaign_id || cp.id);
        // 3) verba e janela do ciclo
        const up = await postForm(novaCampanha, {
          name: f.nome, lifetime_budget: String(Math.round(f.verba * 100)),
          start_time: String(iniISO), stop_time: String(fimISO), status: 'ACTIVE',
        });
        if (up && up.error) { erros.push(f.nome + ' | ' + erroDe(up, 'aplicar verba e agenda')); continue; }
        feitos.push({ nome: f.nome, categoria: f.cat, campanha: novaCampanha, verba: f.verba, videoId });
      } catch (e) { erros.push(f.nome + ': ' + e.message); }
      await new Promise(r => setTimeout(r, 400));
    }
    try {
      const lg = (await dbGet('trafego_log')) || { movs: [] };
      lg.movs.unshift({ ts: new Date().toISOString(), acao: 'montar-semana', feitos, erros });
      await dbSet('trafego_log', lg);
    } catch (e) {}
    return res.status(200).json({ ok: erros.length === 0, criados: feitos.length, feitos, erros,
      inicio: inicio.toISOString(), fim: fim.toISOString() });
  }

  // ═══ 🏆 MODELOS: o anúncio campeão de cada categoria, pronto para ser clonado ═══
  if (action === 'modelos') {
    const cfg = await cfgTrafego();
    const per = ['hoje', '7d', 'ciclo'].includes(String(req.query.periodo || '')) ? String(req.query.periodo) : '7d';
    const base = await dbGet('trafego_painel_cache_' + per) || await dbGet('trafego_painel_cache');
    if (!base || !base.dados) return res.status(200).json({ ok: false, error: 'abra o painel primeiro para carregar os dados' });
    // Criativos que NÃO servem de modelo: reforma é outro serviço (pintura/restauração),
    // não a base do conserto padrão. Lista ampliável pela config.
    const cfgX = (await dbGet('trafego_config')) || {};
    const padroesFora = (cfgX.modelosExcluir || ['reforma', 'pintura', 'restaura']).map(s => String(s).toLowerCase());
    const foraDeModelo = (nome) => padroesFora.some(p => String(nome || '').toLowerCase().includes(p));
    const ads = (base.dados.anuncios || []).filter(a =>
      a.ativo && a.conversas >= 3 && a.cpa != null && !foraDeModelo(a.nome));
    const CATS = ['tv', 'microondas', 'purificador', 'adega', 'forno'];   // institucional fora: não será clonado
    const modelos = {};
    for (const cat of CATS) {
      const lista = ads.filter(a => a.categoria === cat)
        .sort((a, b) => (a.razaoMeta || 9) - (b.razaoMeta || 9));   // mais abaixo da meta primeiro
      if (!lista.length) { modelos[cat] = null; continue; }
      const c = lista[0];
      modelos[cat] = {
        anuncioId: c.id, nome: c.nome,
        conjuntoId: c.adsetId, campanhaId: c.campanhaId,
        cpa: c.cpa, meta: c.meta, conversas: c.conversas, gasto: c.gasto,
        verbaEm: c.verbaEm, orcamentoTotal: c.orcamentoTotal, orcamentoDiario: c.orcamentoDiario,
        thumb: c.thumb,
        porQue: 'melhor custo por conversa da categoria no período (' + c.conversas + ' conversas, ' +
          Math.round((1 - (c.razaoMeta || 1)) * 100) + '% abaixo da meta)',
        vice: lista[1] ? { nome: lista[1].nome, cpa: lista[1].cpa, anuncioId: lista[1].id } : null,
      };
    }
    const cfgM = (await dbGet('trafego_config')) || {};
    // ?curto=1 → uma linha por categoria (cabe no chat)
    if (String(req.query.curto || '') === '1') {
      const linhas = Object.keys(modelos).map(c => {
        const m = modelos[c];
        if (!m) return c.toUpperCase() + ': sem campeão (nenhum ativo com 3+ conversas)';
        return c.toUpperCase() + ': ' + String(m.nome).slice(0, 32) +
          ' | R$ ' + Number(m.cpa).toFixed(2) + '/conversa (meta ' + m.meta + ')' +
          ' | ' + m.conversas + ' conversas' +
          (m.vice ? ' | vice: ' + String(m.vice.nome).slice(0, 24) + ' R$ ' + Number(m.vice.cpa).toFixed(2) : '');
      });
      return res.status(200).json({ ok: true, periodo: per, campeoes: linhas,
        excluidosDaEscolha: padroesFora.join(', ') + ' (não servem de base para clonagem)' });
    }
    return res.status(200).json({ ok: true, periodo: per,
      criterio: 'anúncio ATIVO com no mínimo 3 conversas e menor custo por conversa em relação à meta da categoria',
      modelos,
      modelosFixados: cfgM.modelosFixados || {},
      comoFixar: 'POST em ?action=config com {"modelosFixados":{"adega":"ID_DO_ANUNCIO"}} para travar um modelo manualmente' });
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
    // a copy não vem no payload enxuto do painel — busca só a dos selecionados, em lotes de 10
    const copys = {};
    try {
      const alvos = [...topC, ...topR].map(a => a.id);
      for (let i = 0; i < alvos.length; i += 10) {
        const lote = alvos.slice(i, i + 10).join(',');
        const j = await fetch(`${GRAPH}/?ids=${lote}&fields=creative{body,object_story_spec{video_data{message},link_data{message}}}&access_token=${TOKEN}`)
          .then(x => x.json()).catch(() => null);
        for (const k of Object.keys(j || {})) {
          const cc = ((j[k] || {}).creative) || {}, os = cc.object_story_spec || {};
          copys[k] = String((os.video_data || {}).message || (os.link_data || {}).message || cc.body || '').slice(0, 300);
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
