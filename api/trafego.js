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


// ── 📊 medidor de consumo da IA (chave única da Anthropic) ──
async function _regIA(origem, j) {
  try {
    const U2 = (process.env.UPSTASH_URL || '').replace(/['"]/g, '').trim();
    const T2 = (process.env.UPSTASH_TOKEN || '').replace(/[\n\r'"]/g, '').trim();
    if (!U2 || !T2 || !j) return;
    const u = j.usage || {};
    const dia = new Date(Date.now() - 3 * 3600000).toISOString().slice(0, 10);
    const k = 'ia_uso_' + dia;
    const rr = await fetch(`${U2}/get/${k}`, { headers: { Authorization: `Bearer ${T2}` } })
      .then(x => x.json()).catch(() => null);
    let reg = { chamadas: 0, entrada: 0, saida: 0, cacheCriado: 0, cacheLido: 0, ms: 0, porOrigem: {} };
    try { if (rr && rr.result) reg = Object.assign(reg, JSON.parse(rr.result)); } catch (e) {}
    reg.chamadas++;
    reg.entrada += (u.input_tokens || 0);
    reg.saida += (u.output_tokens || 0);
    reg.cacheCriado += (u.cache_creation_input_tokens || 0);
    reg.cacheLido += (u.cache_read_input_tokens || 0);
    reg.porOrigem = reg.porOrigem || {};
    const o = reg.porOrigem[origem] || { n: 0, ent: 0, sai: 0, cw: 0, cr: 0 };
    o.n++; o.ent += (u.input_tokens || 0); o.sai += (u.output_tokens || 0);
    o.cw += (u.cache_creation_input_tokens || 0); o.cr += (u.cache_read_input_tokens || 0);
    reg.porOrigem[origem] = o;
    await fetch(`${U2}/set/${k}/${encodeURIComponent(JSON.stringify(reg))}`,
      { headers: { Authorization: `Bearer ${T2}` } });
  } catch (e) {}
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
    verba: { adm: 2500, tv: 870, aproveitamento: 1 },   // valores DISPONÍVEIS (definidos 06/08)
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
    if (/tvs?\b|\btvs?\d|televis|barramento|tela quebrad|quebrar tv|polegada|\bled\b|tela lavad|tela apagad|\bt-?con\b|placa fonte|som mais? n[aã]o|sem imagem|n[aã]o d[aá] imagem/.test(s)) return 'tv';
    // micro-ondas: nome por extenso (tolerando erros de digitação) + códigos de modelo
    if (/criativo loja 1\b/.test(s)) return 'microondas';   // confirmado pelo dono
    if (/mic?r?o\s?-?\s?o?nd?as|microodas|micro ?ond|\bmicro\b|reforma|\binflu\b|\bantigo\b/.test(s)) return 'microondas';
    if (/\bme[fovs]?\d{2}|\bmto\d|\bpms?\d{2}|\bpme\d|\bpm0\d|\bmtae?g?\d{2}|\bmi-?\d{4}|\bnn-?st?\d{2}|\bms\d{4}|\bmh\d{4}|\bbm[a-z]?\d{2}|\bcm[a-z]?\d{2}|\bmg\d{2}/.test(s)) return 'microondas';
    if (/\bforn(o|inho)/.test(s)) return 'forno';
    // purificador/bebedouro: nome + linhas Electrolux (PE/PA/PH4/PC4) e Consul (CPB/CPC)
    if (/purifi[a-z]*dor|purifiador|bebe[a-z]*douro|bebedor|\bfiltro\b|\bvela\b|[áa]gua/.test(s)) return 'purificador';
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
    // 🎯 metas de verba do ciclo — o painel passa a mostrar o consumo sobre elas
    const metaAdm = (cfg.verba && cfg.verba.adm) || 2500;
    const metaTv = (cfg.verba && cfg.verba.tv) || 870;
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
    // 💰 o gasto real do ciclo vem das CAMPANHAS — somar só os anúncios que passam na
    // trava de 3 níveis deixava de fora o que foi gasto por anúncio já encerrado,
    // e o painel mostrava menos que o extrato (R$1.992 contra R$2.251 em 06/08).
    let gastoTotal = anuncios.reduce((s, a) => s + a.gasto, 0);
    let gastoPorFrente = null;
    try {
      const desdeG = (function () {
        const b = new Date(Date.now() - 3 * 3600000);
        const d = new Date(b); d.setUTCDate(b.getUTCDate() - ((b.getUTCDay() + 1) % 7));
        return d.toISOString().slice(0, 10);
      })();
      const jnG = 'time_range=' + encodeURIComponent(JSON.stringify({ since: desdeG, until: new Date(Date.now() - 3 * 3600000).toISOString().slice(0, 10) }));
      const insG = await pegarTudo(`${GRAPH}/act_${CONTA}/insights?level=campaign&${jnG}&fields=campaign_id,campaign_name,spend&limit=200&access_token=${TOKEN}`, 6);
      // 📅 só campanhas DESTE ciclo — as de ciclos anteriores ainda gastaram nesta janela
      // (terminaram sábado 11h) e inflavam o total em ~R$350
      const cicloIds = new Set();
      try {
        const cG = await pegarTudo(`${GRAPH}/act_${CONTA}/campaigns?fields=id,start_time&limit=200&access_token=${TOKEN}`, 6);
        for (const c of (cG.data || [])) {
          if (String(c.start_time || '').slice(0, 10) >= desdeG) cicloIds.add(String(c.id));
        }
      } catch (e) {}
      let gTv = 0, gAdm = 0, gTot = 0;
      for (const i of (insG.data || [])) {
        if (cicloIds.size && !cicloIds.has(String(i.campaign_id))) continue;
        const v = Number(i.spend || 0);
        gTot += v;
        if (categoriaDe(i.campaign_name || '', 'anuncio') === 'tv') gTv += v; else gAdm += v;
      }
      if (gTot > 0) {
        gastoTotal = gTot;
        gastoPorFrente = { tv: Number(gTv.toFixed(2)), adm: Number(gAdm.toFixed(2)) };
      }
    } catch (e) {}
    const convTotal = anuncios.reduce((s, a) => s + a.conversas, 0);
    const metricasVistas = [...new Set(anuncios.map(a => a.metricaConversa).filter(Boolean))];
    const dados = { ok: true, ciclo: { desde, ate: hoje },
      periodo,
      periodoLabel: periodo === 'hoje' ? 'Hoje' : periodo === '7d' ? 'Últimos 7 dias' : 'Ciclo (desde ' + desde.split('-').reverse().join('/') + ')',
      totais: { gasto: Number(gastoTotal.toFixed(2)), conversas: convTotal,
        cpa: convTotal > 0 ? Number((gastoTotal / convTotal).toFixed(2)) : null },
      // 💰 verba REALMENTE alocada nas campanhas do ciclo (mesma fonte do extrato)
      verbaAlocadaReal: await (async function () {
        try {
          const desdeV = inicioCiclo(cfg);
          const cV = await pegarTudo(`${GRAPH}/act_${CONTA}/campaigns?fields=id,name,effective_status,daily_budget,lifetime_budget,start_time&limit=200&access_token=${TOKEN}`, 6);
          const sV = await pegarTudo(`${GRAPH}/act_${CONTA}/adsets?fields=id,daily_budget,lifetime_budget,campaign{id}&limit=200&access_token=${TOKEN}`, 6);
          const sPor = {};
          for (const s of (sV.data || [])) { const ci = (s.campaign || {}).id; if (ci) (sPor[ci] = sPor[ci] || []).push(s); }
          let vTv = 0, vAdm = 0;
          for (const c of (cV.data || [])) {
            if (c.effective_status !== 'ACTIVE') continue;
            if (String(c.start_time || '').slice(0, 10) < desdeV) continue;
            let v = 0;
            if (c.lifetime_budget) v = Number(c.lifetime_budget) / 100;
            else if (c.daily_budget) v = Number(c.daily_budget) / 100;
            else for (const s of (sPor[c.id] || [])) {
              if (s.lifetime_budget) v += Number(s.lifetime_budget) / 100;
              else if (s.daily_budget) v += Number(s.daily_budget) / 100;
            }
            if (categoriaDe(c.name || '', 'anuncio') === 'tv') vTv += v; else vAdm += v;
          }
          return { adm: Number(vAdm.toFixed(2)), tv: Number(vTv.toFixed(2)),
            total: Number((vAdm + vTv).toFixed(2)) };
        } catch (e) { return null; }
      })(),
      // 🎯 consumo sobre a verba DISPONÍVEL de cada frente
      metaVerba: {
        adm: { disponivel: metaAdm,
          gasto: gastoPorFrente ? gastoPorFrente.adm : Number((gastoTotal - gastoTv).toFixed(2)),
          restante: Number((metaAdm - (gastoPorFrente ? gastoPorFrente.adm : gastoTotal - gastoTv)).toFixed(2)),
          pct: Math.round(((gastoPorFrente ? gastoPorFrente.adm : gastoTotal - gastoTv) / metaAdm) * 100) },
        tv: { disponivel: metaTv,
          gasto: gastoPorFrente ? gastoPorFrente.tv : Number(gastoTv.toFixed(2)),
          restante: Number((metaTv - (gastoPorFrente ? gastoPorFrente.tv : gastoTv)).toFixed(2)),
          pct: Math.round(((gastoPorFrente ? gastoPorFrente.tv : gastoTv) / metaTv) * 100) },
      },
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
    // 🔄 ?recarregar=1 → busca os dados frescos na Meta antes de decidir,
    // nos DOIS períodos (ciclo para o momento, 7 dias para a média de proteção)
    if (String(req.query.recarregar || '') === '1') {
      const KRC = (process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim();
      for (const p of [per, '7d']) {
        try {
          await fetch(`https://reparoeletroadm.com/api/trafego?action=painel&periodo=${p}&forcar=1&k=${KRC}`)
            .then(x => x.json()).catch(() => null);
        } catch (e) {}
      }
    }
    let base = await dbGet('trafego_painel_cache_' + per) || await dbGet('trafego_painel_cache');
    if (!base || !base.dados) {
      // cache apagado (acontece após criar ou alterar anúncios) → refaz sozinho
      try {
        const KRB = (process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim();
        await fetch(`https://reparoeletroadm.com/api/trafego?action=painel&periodo=${per}&forcar=1&k=${KRB}`)
          .then(x => x.json()).catch(() => null);
        base = await dbGet('trafego_painel_cache_' + per);
      } catch (e) {}
    }
    if (!base || !base.dados) return res.status(200).json({ ok: false, error: 'abra o painel primeiro para carregar os dados' });
    // 📆 REGRA DO DONO: o corte não pode se basear só no dia ou no início do ciclo.
    // Cruza com a janela de 7 dias — criativo dentro da meta na semana NÃO é cortado,
    // mesmo que hoje esteja ruim (variação diária é normal).
    const base7 = await dbGet('trafego_painel_cache_7d');
    const cpaSemana = {};
    for (const a of (((base7 || {}).dados || {}).anuncios || [])) {
      if (a.cpa != null) cpaSemana[a.id] = { cpa: a.cpa, conversas: a.conversas, razao: a.razaoMeta };
    }
    let temSemana = Object.keys(cpaSemana).length > 0;
    // 🔄 sem a média de 7 dias, RECARREGA sozinho em vez de recusar — o cache é apagado
    // sempre que criamos ou alteramos anúncios, e a tela ficava vazia sem explicação.
    if (!temSemana && per !== '7d') {
      try {
        const KR7 = (process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim();
        await fetch(`https://reparoeletroadm.com/api/trafego?action=painel&periodo=7d&forcar=1&k=${KR7}`)
          .then(x => x.json()).catch(() => null);
        const b7 = await dbGet('trafego_painel_cache_7d');
        for (const a of (((b7 || {}).dados || {}).anuncios || [])) {
          if (a.cpa != null) cpaSemana[a.id] = { cpa: a.cpa, conversas: a.conversas, razao: a.razaoMeta };
        }
        temSemana = Object.keys(cpaSemana).length > 0;
      } catch (e) {}
      if (!temSemana) {
        return res.status(200).json({ ok: false,
          error: 'não consegui carregar a média de 7 dias',
          comoResolver: 'abra o painel no período "7 dias" e volte ao Copiloto' });
      }
    }
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
      const meta = (cfg.metas[cat] != null ? cfg.metas[cat] : cfg.metas.outros);
      // 📌 categoria SEM anúncio ativo não some do painel — aparece com o aviso,
      // senão some da tela quando todos forem pausados e ninguém percebe.
      if (!lista.length) {
        const pausadosCat = (base.dados.anuncios || []).filter(a => !a.ativo && a.categoria === cat);
        if (pausadosCat.length) {
          out.push({ categoria: cat, meta, semAtivos: true,
            anunciosPausados: pausadosCat.length,
            aviso: '⚠️ nenhum anúncio ATIVO nesta categoria — ' + pausadosCat.length + ' pausado(s)',
            pausados: pausadosCat.slice(0, 10).map(a => ({ nome: a.nome, cpa: a.cpa, conversas: a.conversas })),
            cortar: [], reforcar: [], libera: 0 });
        }
        continue;
      }
      const gasto = lista.reduce((s, a) => s + a.gasto, 0);
      const conversas = lista.reduce((s, a) => s + a.conversas, 0);
      const verbaAlocada = lista.reduce((s, a) => s + (verbaTotalDe(a) || 0), 0);
      const restante = lista.reduce((s, a) => s + (restanteDe(a) || 0), 0);
      const cpaCat = conversas > 0 ? gasto / conversas : null;

      // perdedores da categoria
      // 📌 TODOS os ativos aparecem — inclusive os dentro da meta, marcados como tal.
      // Antes a categoria sumia quando não havia nada a cortar.
      const dentroDaMeta = lista.filter(a => a.cpa != null && a.cpa <= meta)
        .map(a => ({ id: a.id, nome: a.nome, cpa: a.cpa, conversas: a.conversas,
          gasto: a.gasto, verba: verbaTotalDe(a) }))
        .sort((x, y) => (x.cpa || 9) - (y.cpa || 9));
      const cortar = lista.filter(a => {
        const ruimAgora = (a.conversas === 0 && a.gasto >= meta * 2)
          || (a.razaoMeta != null && a.razaoMeta > 1.3 && a.conversas > 0);
        if (!ruimAgora) return false;
        // 🛡 protegido pela semana: se nos 7 dias está dentro da meta, não corta
        const sem = cpaSemana[a.id];
        if (sem && sem.conversas >= 3 && sem.cpa <= meta) return false;
        return true;
      })
        .map(a => ({ id: a.id, nome: a.nome, thumb: a.thumb, adsetId: a.adsetId, campanhaId: a.campanhaId,
          cpa: a.cpa, conversas: a.conversas, gasto: a.gasto,
          libera: restanteDe(a), verbaEm: a.verbaEm,
          cpaSemana: (cpaSemana[a.id] || {}).cpa || null,
          motivo: a.conversas === 0 ? 'sem nenhuma conversa'
            : Math.round((a.razaoMeta - 1) * 100) + '% acima da meta' +
              (cpaSemana[a.id] ? ' (7 dias: ' + cpaSemana[a.id].cpa + ')' : '') }))
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
        dentroDaMeta, cortar, libera, reforcar,
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

    // ⏸️ panorama dos PAUSADOS — o Copiloto só falava dos ativos
    let panoramaPausados = null;
    try {
      const cAll = await pegarTudo(`${GRAPH}/act_${CONTA}/campaigns?fields=id,name,effective_status,daily_budget,lifetime_budget,start_time&limit=200&access_token=${TOKEN}`, 6);
      const sAll = await pegarTudo(`${GRAPH}/act_${CONTA}/adsets?fields=id,effective_status,daily_budget,lifetime_budget,campaign{id}&limit=200&access_token=${TOKEN}`, 6);
      const setsPor = {};
      for (const s of (sAll.data || [])) { const ci = (s.campaign || {}).id; if (ci) (setsPor[ci] = setsPor[ci] || []).push(s); }
      const desdeC = (function () {
        const b = new Date(Date.now() - 3 * 3600000);
        const d = new Date(b); d.setUTCDate(b.getUTCDate() - ((b.getUTCDay() + 1) % 7));
        return d.toISOString().slice(0, 10);
      })();
      const pausadas = (cAll.data || []).filter(c => c.effective_status !== 'ACTIVE'
        && String(c.start_time || '').slice(0, 10) >= desdeC);
      let verbaPresa = 0;
      const lista = pausadas.map(c => {
        let v = 0;
        if (c.lifetime_budget) v = Number(c.lifetime_budget) / 100;
        else if (c.daily_budget) v = Number(c.daily_budget) / 100;
        else for (const s of (setsPor[c.id] || [])) {
          if (s.lifetime_budget) v += Number(s.lifetime_budget) / 100;
          else if (s.daily_budget) v += Number(s.daily_budget) / 100;
        }
        verbaPresa += v;
        return { nome: c.name, id: c.id, situacao: c.effective_status,
          categoria: categoriaDe(c.name || '', 'anuncio'), verba: Number(v.toFixed(2)) };
      }).sort((a, b) => b.verba - a.verba);
      panoramaPausados = { total: pausadas.length,
        verbaAlocadaNeles: Number(verbaPresa.toFixed(2)),
        aviso: pausadas.length ? '⏸️ ' + pausadas.length + ' campanha(s) pausada(s) neste ciclo, com R$ ' + verbaPresa.toFixed(2) + ' alocados — rode verba-orfa para devolver o que sobrou' : null,
        lista: lista.map(p => p.categoria.toUpperCase().slice(0, 4) + ' | ' + String(p.nome).slice(0, 30) + ' | R$ ' + p.verba + ' | ' + p.situacao) };
    } catch (e) {}
    // ⚠️ ATIVAS COM PROBLEMA — anúncios do ciclo que estão ativos mas não entregam
    let comProblema = null;
    try {
      const adsP = await pegarTudo(`${GRAPH}/act_${CONTA}/ads?fields=id,name,effective_status,issues_info,adset{id,name,effective_status,end_time},campaign{id,name,effective_status,start_time}&limit=300&access_token=${TOKEN}`, 8);
      const agoraMs = Date.now();
      // 📅 SÓ o ciclo atual — sem isso entravam anúncios de todos os ciclos anteriores (569!)
      const desdeCicloP = (function () {
        const b = new Date(Date.now() - 3 * 3600000);
        const d = new Date(b); d.setUTCDate(b.getUTCDate() - ((b.getUTCDay() + 1) % 7));
        return d.toISOString().slice(0, 10);
      })();
      const ehAvisoNormal = p => /não está sendo veiculado, mas você não precisa fazer nada/i.test(String(p || ''));
      const achados = [];
      for (const a of (adsP.data || [])) {
        const st = a.adset || {};
        const issues = (a.issues_info || []).map(x => x.error_summary || x.error_message).filter(Boolean);
        const graves = issues.filter(i => !ehAvisoNormal(i));
        const prazoVencido = st.end_time && new Date(st.end_time).getTime() < agoraMs;
        const conjParado = st.effective_status && st.effective_status !== 'ACTIVE';
        const reprovado = a.effective_status === 'DISAPPROVED';
        const emRevisao = ['PENDING_REVIEW', 'IN_PROCESS', 'WITH_ISSUES'].includes(a.effective_status) && graves.length;
        // só interessa se a CAMPANHA está ativa E é do ciclo atual
        if ((a.campaign || {}).effective_status !== 'ACTIVE') continue;
        if (String((a.campaign || {}).start_time || '').slice(0, 10) < desdeCicloP) continue;
        if (!graves.length && !prazoVencido && !conjParado && !reprovado && !emRevisao) continue;
        achados.push({
          anuncio: a.name, id: a.id,
          categoria: categoriaDe(a.name || '', 'anuncio'),
          situacao: a.effective_status,
          problema: reprovado ? '❌ REPROVADO pela Meta'
            : (graves.length ? '⚠️ ' + String(graves[0]).slice(0, 70)
            : (prazoVencido ? '⏰ prazo de veiculação ENCERRADO em ' + String(st.end_time).slice(0, 10)
            : (conjParado ? '⏸️ conjunto ' + st.effective_status : '⏳ em revisão'))),
          conjunto: st.name || null,
        });
      }
      // 🚨 CAMPANHA ATIVA SEM NENHUM ANÚNCIO ATIVO — verba parada, ninguém percebe
      try {
        const cSem = await pegarTudo(`${GRAPH}/act_${CONTA}/campaigns?fields=id,name,effective_status,daily_budget,lifetime_budget,start_time&limit=300&access_token=${TOKEN}`, 8);
        const sSem = await pegarTudo(`${GRAPH}/act_${CONTA}/adsets?fields=id,daily_budget,lifetime_budget,campaign{id}&limit=300&access_token=${TOKEN}`, 8);
        const setsSem = {};
        for (const s of (sSem.data || [])) { const ci = (s.campaign || {}).id; if (ci) (setsSem[ci] = setsSem[ci] || []).push(s); }
        const adsPorCamp = {};
        for (const a of (adsP.data || [])) {
          const ci = (a.campaign || {}).id; if (!ci) continue;
          adsPorCamp[ci] = adsPorCamp[ci] || { total: 0, ativos: 0 };
          adsPorCamp[ci].total++;
          if (a.effective_status === 'ACTIVE') adsPorCamp[ci].ativos++;
        }
        for (const c of (cSem.data || [])) {
          if (c.effective_status !== 'ACTIVE') continue;
          if (String(c.start_time || '').slice(0, 10) < desdeCicloP) continue;
          const cont = adsPorCamp[c.id] || { total: 0, ativos: 0 };
          if (cont.ativos > 0) continue;                    // tem anúncio rodando, tudo bem
          let v = 0;
          if (c.lifetime_budget) v = Number(c.lifetime_budget) / 100;
          else if (c.daily_budget) v = Number(c.daily_budget) / 100;
          else for (const s of (setsSem[c.id] || [])) {
            if (s.lifetime_budget) v += Number(s.lifetime_budget) / 100;
            else if (s.daily_budget) v += Number(s.daily_budget) / 100;
          }
          achados.push({
            anuncio: c.name, id: c.id,
            categoria: categoriaDe(c.name || '', 'anuncio'),
            situacao: 'ACTIVE',
            problema: '🚨 CAMPANHA ATIVA SEM ANÚNCIO RODANDO — R$ ' + v.toFixed(2) + ' de verba parada' +
              (cont.total ? ' (' + cont.total + ' anúncio(s), nenhum ativo)' : ' (nenhum anúncio criado)'),
            verbaParada: Number(v.toFixed(2)),
          });
        }
      } catch (e) {}
      achados.sort((a, b) => (b.verbaParada || 0) - (a.verbaParada || 0));
      const verbaParadaTotal = Number(achados.reduce((s, x) => s + (x.verbaParada || 0), 0).toFixed(2));
      comProblema = { total: achados.length,
        verbaParada: verbaParadaTotal,
        aviso: achados.length ? '⚠️ ' + achados.length + ' campanha(s)/anúncio(s) ATIVOS com problema' +
          (verbaParadaTotal > 0 ? ' — R$ ' + verbaParadaTotal.toFixed(2) + ' de verba parada' : ' — não estão entregando') : null,
        lista: achados.slice(0, 20).map(x => String(x.categoria || '?').toUpperCase().slice(0, 4) + ' | ' +
          String(x.anuncio).slice(0, 28) + ' | ' + x.problema),
        detalhe: achados.slice(0, 20) };
    } catch (e) {}
    return res.status(200).json({ ok: true, periodo: per, diasRestantes,
      ATIVOS_COM_PROBLEMA: comProblema,
      PAUSADOS: panoramaPausados,
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
    // 🐛 'cat' era usado adiante sem nunca ser declarado, e a aplicação inteira
    // falhava com "cat is not defined" — nenhum corte era executado.
    const cat = String((req.body && req.body.frente) || req.query.frente || req.query.cat || 'adm').toLowerCase();
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
    // 🚫 aqui havia um bloco inteiro de verificação de teto copiado do subir-agora,
    // usando verba, videos, desdeCiclo e TK — variáveis que só existem lá. A aplicação
    // morria antes de tocar na Meta. O aplicar pausa e ajusta; não cria anúncio.
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
      let alvo = o.alvoId || o.adsetId;
      const campo = o.campo || (o.diario ? 'daily_budget' : 'lifetime_budget');
      const valor = o.valor != null ? o.valor : o.diario;
      if (!alvo || valor == null) { erros.push({ id: alvo || '?', acao: 'orçamento', erro: 'destino ou valor ausente' }); continue; }
      const centavos = Math.round(Number(valor) * 100);
      if (!(centavos > 0)) { erros.push({ id: alvo, acao: 'orçamento', erro: 'valor inválido' }); continue; }
      let r = await postMeta(alvo, { [campo]: String(centavos) });
      // ↩️ nível errado? tenta o outro. A conta mistura verba em campanha e em conjunto,
      // e 20 aplicações falharam em 28/07 por isso, sem ninguém ser avisado.
      if (r && r.error) {
        const outro = (String(alvo) === String(o.adsetId)) ? o.campanhaId : o.adsetId;
        if (outro && String(outro) !== String(alvo)) {
          const r2 = await postMeta(outro, { [campo]: String(centavos) });
          if (!(r2 && r2.error)) { r = r2; alvo = outro; }
        }
      }
      // ainda falhou? tenta o outro CAMPO (total x diária)
      if (r && r.error) {
        const campoAlt = campo === 'lifetime_budget' ? 'daily_budget' : 'lifetime_budget';
        const r3 = await postMeta(alvo, { [campoAlt]: String(centavos) });
        if (!(r3 && r3.error)) r = r3;
      }
      if (r && r.error) erros.push({ id: alvo, nome: o.nome || null,
        acao: 'orçamento (' + campo + ')', erro: r.error.message, codigo: r.error.code });
      else {
        // 📝 log legível: nome, quanto recebeu e o saldo final
        const antes = o.verbaAntes != null ? Number(o.verbaAntes) : null;
        const recebeu = o.recebeu != null ? Number(o.recebeu)
          : (antes != null ? Number(valor) - antes : null);
        feitos.push({ id: alvo, nome: (o.nome || o.anuncio || null),
          acao: (antes != null ? 'R$ ' + antes.toFixed(2) + ' + R$ ' + (recebeu || 0).toFixed(2) + ' = ' : '') +
            'R$ ' + Number(valor).toFixed(2) + ' (' + (campo === 'daily_budget' ? 'diária' : 'total') + ')',
          verbaAntes: antes, recebeu, verbaFinal: Number(valor) });
      }
      await new Promise(r2 => setTimeout(r2, 120));
    }
    // ⛔ DEVOLUÇÃO AUTOMÁTICA DESLIGADA (05/08): verba de campanha PAUSADA não é gasto —
    // é só um número na configuração e o dinheiro nunca sai. Redistribuí-la aos ativos
    // INFLAVA o total da conta a cada ciclo com pausas. Pausou, esquece.
    // Para reequilibrar de propósito, use ajustar-teto com o valor desejado.
    const orfaFeitos = [];
    try {
      const lg = (await dbGet('trafego_log')) || { movs: [] };
      lg.movs.unshift({ ts: new Date().toISOString(),
        feitos: feitos.concat(orfaFeitos.map(f => ({ ...f, acao: '[órfã] ' + f.acao }))), erros });
      lg.movs = lg.movs.slice(0, 200);
      await dbSet('trafego_log', lg);
      for (const p of ['hoje', '7d', 'ciclo']) await dbSet('trafego_painel_cache_' + p, null);
      await dbSet('trafego_painel_cache', null);
    } catch (e) {}
    return res.status(200).json({ ok: erros.length === 0, feitos, erros,
      verbaOrfaDevolvida: orfaFeitos.length ? { anuncios: orfaFeitos.length, lista: orfaFeitos.map(f => f.nome + ' → ' + f.acao) } : 'nenhuma' });
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

  // ── 🚦 PENDENTES-APROVACAO: anúncios criados que ainda não estão veiculando ──
  if (action === 'pendentes-aprovacao') {
    if (!CONTA) return res.status(200).json({ ok: false, error: 'conta não configurada' });
    const tk = String(req.query.token || '').trim() || TOKEN;
    const dias = Math.min(30, Math.max(1, parseInt(req.query.dias || '3', 10)));
    const corte = Date.now() - dias * 86400000;
    const ads = await pegarTudo(`${GRAPH}/act_${CONTA}/ads?fields=id,name,effective_status,created_time,issues_info,adset{id,name,effective_status},campaign{id,name,effective_status,start_time}&limit=300&access_token=${tk}`, 10);
    if (ads.erro && !ads.data.length) return res.status(200).json({ ok: false, erro: ads.erro });

    // estados que significam "não está veiculando ainda"
    const ESPERANDO = ['PENDING_REVIEW', 'IN_PROCESS', 'PENDING_BILLING_INFO', 'PREAPPROVED', 'WITH_ISSUES'];
    const recentes = (ads.data || []).filter(a => new Date(a.created_time || 0).getTime() >= corte);
    const linhas = [];
    for (const a of recentes) {
      const st = a.effective_status;
      const issues = (a.issues_info || []).map(x => x.error_summary || x.error_message).filter(Boolean);
      // aviso de agendado é inofensivo; o resto merece atenção
      const soAgendado = issues.length === 1 && /não está sendo veiculado, mas você não precisa fazer nada/i.test(issues[0]);
      const precisaAcao = (ESPERANDO.includes(st) && !soAgendado) || st === 'DISAPPROVED';
      if (!precisaAcao) continue;
      const seguranca = issues.some(i => /revis|aprovad|publicar|seguran|autoriz/i.test(String(i)));
      linhas.push({
        anuncio: a.name, id: a.id,
        campanha: (a.campaign || {}).name,
        campanhaId: (a.campaign || {}).id,
        situacao: st,
        criadoEm: a.created_time,
        problemas: issues.length ? issues : null,
        tipo: st === 'DISAPPROVED' ? '❌ REPROVADO'
          : (seguranca ? '🔒 AGUARDA SUA APROVAÇÃO (recurso de segurança)' : '⏳ em revisão da Meta'),
        linkParaAprovar: 'https://adsmanager.facebook.com/adsmanager/manage/ads?act=' + CONTA +
          '&selected_ad_ids=' + a.id,
      });
    }
    linhas.sort((a, b) => String(b.criadoEm).localeCompare(String(a.criadoEm)));
    const seguranca = linhas.filter(l => l.tipo.includes('SEGURANÇA'));
    const reprovados = linhas.filter(l => l.tipo.includes('REPROVADO'));
    return res.status(200).json({ ok: linhas.length === 0,
      periodoDias: dias,
      total: linhas.length,
      aguardandoSuaAprovacao: seguranca.length,
      emRevisaoDaMeta: linhas.length - seguranca.length - reprovados.length,
      reprovados: reprovados.length,
      ACAO_NECESSARIA: seguranca.length
        ? '🔒 ' + seguranca.length + ' anúncio(s) esperando VOCÊ aprovar no Gerenciador — eles não gastam nem entregam até lá'
        : (reprovados.length ? '❌ ' + reprovados.length + ' reprovado(s) — verifique o motivo' : '✅ nada pendente'),
      lista: linhas.map(l => l.tipo + ' | ' + String(l.anuncio).slice(0, 30) + ' | ' + l.situacao +
        (l.problemas ? ' | ' + String(l.problemas[0]).slice(0, 60) : '')),
      detalhe: linhas.slice(0, 20) });
  }

  // ── 🔬 AUDITORIA-CRIACAO: TUDO que é preciso para criar um anúncio, verificado de uma vez ──
  if (action === 'auditoria-criacao') {
    const tk = String(req.query.token || '').trim() || TOKEN;
    const G2 = GRAPH;
    const chk = [];
    const pega = async (rot, url, critico) => {
      const r = await fetch(url).then(x => x.json()).catch(e => ({ error: { message: e.message } }));
      const ok = !(r && r.error);
      chk.push({ item: rot, ok, critico: !!critico,
        erro: ok ? null : (r.error.error_user_msg || r.error.message),
        codigo: ok ? null : r.error.code, dados: ok ? r : undefined });
      return ok ? r : null;
    };

    // 1) o token e seus escopos
    const dbg = await fetch(`${G2}/debug_token?input_token=${tk}&access_token=${tk}`).then(x => x.json()).catch(() => ({}));
    const d = (dbg && dbg.data) || {};
    const escopos = d.scopes || [];
    chk.push({ item: 'token válido', ok: !!d.is_valid, critico: true,
      erro: d.is_valid ? null : 'token inválido' });
    chk.push({ item: 'escopo ads_management', ok: escopos.includes('ads_management'), critico: true,
      erro: escopos.includes('ads_management') ? null : 'FALTA — sem ele não cria anúncio' });
    chk.push({ item: 'escopo pages_manage_ads', ok: escopos.includes('pages_manage_ads'), critico: false,
      erro: escopos.includes('pages_manage_ads') ? null : 'ausente (pode ser suprido pelo ativo Página)' });
    chk.push({ item: 'escopos do token', ok: true, dados: escopos });

    // 2) a conta de anúncios
    await pega('conta de anúncios', `${G2}/act_${CONTA}?fields=name,account_status,capabilities&access_token=${tk}`, true);

    // 3) as PÁGINAS que o token enxerga — é aqui que estava travando
    const pgs = await pega('páginas acessíveis pelo token', `${G2}/me/accounts?fields=id,name,tasks&access_token=${tk}`);
    const pgsConta = await pega('páginas ligadas à conta de anúncios', `${G2}/act_${CONTA}/promote_pages?fields=id,name&access_token=${tk}`);

    // 4) a Página usada pelo modelo
    let pageModelo = null;
    try {
      const base = await dbGet('trafego_painel_cache_ciclo') || await dbGet('trafego_painel_cache');
      const mod = ((base || {}).dados || {}).anuncios?.filter(a => a.ativo && a.categoria === (req.query.cat || 'tv'))[0];
      if (mod) {
        const cr = await fetch(`${G2}/${mod.id}?fields=creative{object_story_spec{page_id}}&access_token=${tk}`).then(x => x.json());
        pageModelo = (((cr.creative || {}).object_story_spec) || {}).page_id || null;
        chk.push({ item: 'Página usada pelos anúncios atuais', ok: !!pageModelo,
          critico: true, dados: pageModelo, erro: pageModelo ? null : 'não consegui ler' });
        if (pageModelo) {
          await pega('acesso à Página ' + pageModelo, `${G2}/${pageModelo}?fields=id,name,access_token&access_token=${tk}`, true);
        }
      }
    } catch (e) {}

    // 5) teste REAL de criação (cria e apaga em seguida)
    let testeCriacao = null;
    if (String(req.query.testar || '') === '1') {
      const nomeT = 'TESTE-PERMISSAO-' + Date.now();
      const c = await fetch(`${G2}/act_${CONTA}/campaigns?access_token=${tk}`, { method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: new URLSearchParams({ name: nomeT, objective: 'OUTCOME_ENGAGEMENT', status: 'PAUSED',
          special_ad_categories: '[]', is_adset_budget_sharing_enabled: 'false' }).toString() })
        .then(x => x.json()).catch(e => ({ error: { message: e.message } }));
      if (c && c.error) testeCriacao = { etapa: 'campanha', ok: false, erro: c.error.error_user_msg || c.error.message };
      else {
        testeCriacao = { etapa: 'campanha', ok: true, id: c.id };
        // apaga o teste
        await fetch(`${G2}/${c.id}?access_token=${tk}`, { method: 'DELETE' }).catch(() => {});
      }
    }

    const criticos = chk.filter(c => c.critico && !c.ok);
    return res.status(200).json({
      ok: criticos.length === 0,
      BLOQUEIOS: criticos.length ? criticos.map(c => '❌ ' + c.item + ' → ' + c.erro) : 'nenhum',
      paginaDosAnuncios: pageModelo,
      paginasQueOTokenVE: (pgs && pgs.data || []).map(p => p.id + ' | ' + p.name + ' | ' + (p.tasks || []).join(',')),
      paginasDaContaDeAnuncios: (pgsConta && pgsConta.data || []).map(p => p.id + ' | ' + p.name),
      testeCriacao,
      verificacoes: chk,
      comoUsar: 'acrescente &testar=1 para tentar criar uma campanha de teste (é apagada em seguida)' });
  }

  // ── ➕ SUBIR-AGORA: cria anúncios no ciclo ATUAL, com término definido ──
  if (action === 'subir-agora') {
    if (!CONTA) return res.status(200).json({ ok: false, error: 'conta não configurada' });
    const TK = String(req.query.token || '').trim() || TOKEN;   // token alternativo para teste
    const cat = String(req.query.cat || 'tv').toLowerCase();
    const verba = parseFloat(req.query.verba || '145');
    const desde = String(req.query.desde || '').slice(0, 10);      // vídeos a partir desta data
    const ids = String(req.query.ids || '').split(',').filter(Boolean);
    const apenasUm = String(req.query.apenasUm || '') === '1';
    if (!(verba > 0)) return res.status(400).json({ ok: false, error: 'verba inválida' });

    // 📅 término: sábado 11h BRT — SEMPRE no futuro. Rodando num sábado depois das 11h
    // o cálculo antigo apontava para hoje, e as campanhas nasciam já encerradas.
    const agoraB = new Date(Date.now() - 3 * 3600000);
    // 🕐 INÍCIO: 13h BRT do dia informado em ?desde (ou hoje). 16h UTC.
    const inicioCic = desde
      ? new Date(desde + 'T16:00:00Z')
      : new Date(Date.UTC(agoraB.getUTCFullYear(), agoraB.getUTCMonth(), agoraB.getUTCDate(), 16, 0, 0));
    // 🏁 FIM: sábado 11h BRT (14h UTC) da semana SEGUINTE ao início
    const fim = new Date(inicioCic);
    const diasAteSab = (6 - inicioCic.getUTCDay() + 7) % 7;
    fim.setUTCDate(inicioCic.getUTCDate() + (diasAteSab === 0 ? 7 : diasAteSab));
    fim.setUTCHours(14, 0, 0, 0);
    while (fim.getTime() <= inicioCic.getTime()) fim.setUTCDate(fim.getUTCDate() + 7);
    const fimUnix = Math.floor(fim.getTime() / 1000);
    const inicioUnix = Math.floor(Math.max(inicioCic.getTime(), Date.now() + 300000) / 1000);
    const horasRestantes = Math.round((fim.getTime() - Date.now()) / 3600000);

    // vídeos a usar
    const vids = await pegarTudo(`${GRAPH}/act_${CONTA}/advideos?fields=id,title,created_time&limit=100&access_token=${TK}`, 6);
    let escolhidos = (vids.data || []);
    if (ids.length) escolhidos = escolhidos.filter(v => ids.includes(v.id));
    else if (desde) escolhidos = escolhidos.filter(v => String(v.created_time || '').slice(0, 10) >= desde);
    else return res.status(400).json({ ok: false, error: 'informe &desde=AAAA-MM-DD ou &ids=' });
    if (!escolhidos.length) return res.status(200).json({ ok: false, error: 'nenhum vídeo encontrado no filtro' });

    // modelo: melhor anúncio ativo da categoria
    const base = await dbGet('trafego_painel_cache_ciclo') || await dbGet('trafego_painel_cache');
    if (!base || !base.dados) return res.status(200).json({ ok: false, error: 'abra o painel primeiro' });
    let modelo = (base.dados.anuncios || [])
      .filter(a => a.categoria === cat && a.campanhaId && a.adsetId)
      .sort((x, y) => {
        // prefere ATIVO; entre iguais, o de melhor CPA. O ciclo anterior termina
        // sábado 11h e os anúncios deixam de ser ativos — sem isso o sistema
        // ficava sem modelo justamente na hora de montar o ciclo novo.
        if (!!x.ativo !== !!y.ativo) return x.ativo ? -1 : 1;
        return (x.razaoMeta || 9) - (y.razaoMeta || 9);
      })[0];
    if (!modelo) modelo = await modeloDaMeta(cat, TK);
    if (!modelo) return res.status(200).json({ ok: false,
      error: 'nenhum anúncio de ' + cat + ' encontrado para usar de modelo',
      dica: 'recarregue o painel: /api/trafego?action=painel&periodo=ciclo&forcar=1' });

    let alvo = apenasUm ? escolhidos.slice(0, 1) : escolhidos;
    if (String(req.query.aplicar || '') !== '1') {
      return res.status(200).json({ ok: true, modo: 'prévia',
        categoria: cat, verbaPorAnuncio: verba, total: alvo.length,
        verbaTotal: Number((verba * alvo.length).toFixed(2)),
        terminaEm: new Date(fim.getTime() - 3 * 3600000).toISOString().slice(0, 16).replace('T', ' ') + ' BRT',
        horasDeVeiculacao: horasRestantes,
        modelo: modelo.nome + ' (CPA R$ ' + modelo.cpa + ')',
        videos: alvo.map(v => v.title),
        dica: 'para criar: &aplicar=1 · para testar só o primeiro: &apenasUm=1&aplicar=1' });
    }

    const postForm = async (path, campos) => {
      const r = await fetch(`${GRAPH}/${path}?access_token=${TK}`, { method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: new URLSearchParams(campos).toString() })
        .then(x => x.json()).catch(e => ({ error: { message: e.message } }));
      return r;
    };
    // 🎯 só os vídeos da CATEGORIA pedida — sem isso os 16 da pasta entravam todos
    // em cada rodada, criando anúncio de adega com texto de micro-ondas
    alvo = alvo.filter(v => categoriaDe(String(v.title || ''), 'anuncio') === cat);
    if (!alvo.length) return res.status(200).json({ ok: false,
      error: 'nenhum vídeo de ' + cat + ' entre os recentes',
      dica: 'os vídeos são classificados pelo NOME do arquivo' });
    const feitos = [], erros = [];
    // 🛡 nomes já usados no ciclo de destino, para não duplicar campanha
    const nomesNoCiclo = new Set();
    try {
      const cicloDest = String(req.query.desde || '').slice(0, 10);
      const cJa = await pegarTudo(`${GRAPH}/act_${CONTA}/campaigns?fields=id,name,start_time&limit=300&access_token=${TK}`, 8);
      for (const c of (cJa.data || [])) {
        if (cicloDest && String(c.start_time || '').slice(0, 10) < cicloDest) continue;
        nomesNoCiclo.add(String(c.name || '').toLowerCase());
      }
    } catch (e) {}
    for (const v of alvo) {
      try {
        // 🛡 DUPLICATA: não recriar campanha que já existe com o mesmo nome no ciclo.
        const nomePrev = nomeComData(v.title);
        if (nomesNoCiclo.has(nomePrev.toLowerCase())) {
          erros.push(v.title + ': já existe a campanha "' + nomePrev + '" — pulei para não duplicar');
          continue;
        }
        // 1) tenta duplicar; se o app não tiver permissão para /copies (código 10),
        // cria do ZERO lendo a configuração do modelo — mesmo resultado, endpoints padrão
        let nova = null;
        const cp = await postForm(modelo.campanhaId + '/copies', {
          deep_copy: 'true', status_option: 'PAUSED',
          rename_options: JSON.stringify({ rename_strategy: 'NO_RENAME' }),
        });
        if (cp && !cp.error) {
          nova = cp.copied_campaign_id || cp.id;
        } else {
          // ── caminho alternativo: replicar a estrutura ──
          const nome = nomeComData(v.title);
          // lê o modelo completo
          const mCamp = await fetch(`${GRAPH}/${modelo.campanhaId}?fields=objective,special_ad_categories,buying_type&access_token=${TK}`).then(x => x.json());
          const mSet = modelo.adsetId
            ? await fetch(`${GRAPH}/${modelo.adsetId}?fields=targeting,optimization_goal,billing_event,destination_type,promoted_object,bid_strategy&access_token=${TK}`).then(x => x.json())
            : {};
          if (mCamp.error || mSet.error) {
            erros.push(v.title + ' | ler modelo: ' + ((mCamp.error || mSet.error).message));
            continue;
          }
          // 1a) campanha
          const camposCamp = {
            name: nome, objective: mCamp.objective || 'OUTCOME_ENGAGEMENT',
            status: 'PAUSED', special_ad_categories: JSON.stringify(mCamp.special_ad_categories || []),
            // campo obrigatório quando a verba fica no CONJUNTO (é o nosso caso):
            // false = cada conjunto usa só o próprio orçamento, sem compartilhar
            is_adset_budget_sharing_enabled: 'false',
          };
          const c1 = await postForm('act_' + CONTA + '/campaigns', camposCamp);
          if (c1 && c1.error) {
            erros.push(v.title + ' | criar campanha: ' + c1.error.message + ' (cód ' + c1.error.code +
              ')' + (c1.error.error_user_msg ? ' · ' + c1.error.error_user_msg : '') +
              (c1.error.error_data ? ' · ' + JSON.stringify(c1.error.error_data).slice(0, 200) : '') +
              ' | ENVIADO: ' + JSON.stringify(camposCamp).slice(0, 200) +
              ' | MODELO: objetivo=' + (mCamp.objective || '?') + ' compra=' + (mCamp.buying_type || '?'));
            continue;
          }
          nova = c1.id;
          // 1b) conjunto, com a mesma segmentação e destino
          const camposSet = {
            name: nome + ' - conjunto', campaign_id: nova,
            targeting: JSON.stringify(mSet.targeting || {}),
            optimization_goal: mSet.optimization_goal || 'CONVERSATIONS',
            billing_event: mSet.billing_event || 'IMPRESSIONS',
            bid_strategy: mSet.bid_strategy || 'LOWEST_COST_WITHOUT_CAP',
            status: 'PAUSED',
            lifetime_budget: String(Math.round(verba * 100)),
            start_time: String(inicioUnix),
            end_time: String(fimUnix),
          };
          if (mSet.destination_type) camposSet.destination_type = mSet.destination_type;
          if (mSet.promoted_object) camposSet.promoted_object = JSON.stringify(mSet.promoted_object);
          const s1 = await postForm('act_' + CONTA + '/adsets', camposSet);
          if (s1 && s1.error) {
            erros.push(v.title + ' | criar conjunto: ' + s1.error.message + ' (cód ' + s1.error.code + ')' + (s1.error.error_user_msg ? ' · ' + s1.error.error_user_msg : '') + (s1.error.error_data ? ' · ' + JSON.stringify(s1.error.error_data).slice(0,150) : ''));
            continue;
          }
          // 1c) criativo com o VÍDEO NOVO
          const mAdC = await fetch(`${GRAPH}/${modelo.id}?fields=creative{object_story_spec,degrees_of_freedom_spec}&access_token=${TK}`).then(x => x.json());
          const oss = ((mAdC.creative || {}).object_story_spec) || {};
          const novoOss = { page_id: oss.page_id };
          if (oss.video_data) {
            novoOss.video_data = { ...oss.video_data, video_id: v.id };
            delete novoOss.video_data.image_url;
            // ✍️ TEXTO: o criativo nascia SEM título e SEM corpo (os 3 de TV de 05/08).
            // Gera a partir do nome do vídeo, que descreve o defeito, e mantém o
            // botão e o destino herdados do modelo.
            const txt = textoPorDefeito(String(v.title || ''), cat);
            if (txt) {
              novoOss.video_data.title = txt.titulo;
              novoOss.video_data.message = txt.corpo;
            }
          } else if (oss.link_data) {
            novoOss.link_data = oss.link_data;
          }
          // ⏳ vídeo recém-subido leva alguns segundos para processar; criar o criativo
          // antes disso faz a Meta recusar. Espera até ficar pronto (máx 40s).
          try {
            for (let tent = 0; tent < 8; tent++) {
              const st = await fetch(`${GRAPH}/${v.id}?fields=status&access_token=${TK}`)
                .then(x => x.json()).catch(() => null);
              const fase = st && st.status && (st.status.video_status || st.status);
              if (!fase || fase === 'ready') break;
              if (fase === 'error') { throw new Error('vídeo com erro de processamento'); }
              await new Promise(s => setTimeout(s, 5000));
            }
          } catch (e) {
            erros.push(v.title + ': ' + e.message);
            continue;
          }
          const cr = await postForm('act_' + CONTA + '/adcreatives', {
            name: nome + ' - criativo', object_story_spec: JSON.stringify(novoOss),
          });
          if (cr && cr.error) { erros.push(v.title + ' | criar criativo: ' + cr.error.message + ' (cód ' + cr.error.code + ')' + (cr.error.error_user_msg ? ' · ' + cr.error.error_user_msg : '') + (cr.error.error_data ? ' · ' + JSON.stringify(cr.error.error_data).slice(0,150) : '')); continue; }
          // 1d) anúncio
          const a1 = await postForm('act_' + CONTA + '/ads', {
            name: nome, adset_id: s1.id, creative: JSON.stringify({ creative_id: cr.id }), status: 'ACTIVE',
          });
          if (a1 && a1.error) { erros.push(v.title + ' | criar anúncio: ' + a1.error.message + ' (cód ' + a1.error.code + ')' + (a1.error.error_user_msg ? ' · ' + a1.error.error_user_msg : '') + (a1.error.error_data ? ' · ' + JSON.stringify(a1.error.error_data).slice(0,150) : '')); continue; }
          // ativa o conjunto e a campanha
          await postForm(s1.id, { status: 'ACTIVE' });
          await postForm(nova, { status: 'ACTIVE' });
          feitos.push({ video: v.title, campanha: nova, verba, videoId: v.id, via: 'criado do zero (com o vídeo novo)' });
          await new Promise(r => setTimeout(r, 500));
          continue;
        }
        // 2) nome, verba e término, já ativa
        const up = await postForm(nova, {
          name: nomeComData(v.title),
          lifetime_budget: String(Math.round(verba * 100)),
          stop_time: String(fimUnix),
          status: 'ACTIVE',
        });
        if (up && up.error) { erros.push(v.title + ' | verba/prazo: ' + up.error.message + ' (cód ' + up.error.code + ')'); continue; }
        feitos.push({ video: v.title, campanha: nova, verba, videoId: v.id });
      } catch (e) { erros.push(v.title + ' | ' + e.message); }
      await new Promise(r => setTimeout(r, 500));
    }
    try {
      const lg = (await dbGet('trafego_log')) || { movs: [] };
      lg.movs.unshift({ ts: new Date().toISOString(), acao: 'subir-agora',
        feitos: feitos.map(f => ({ id: f.campanha, nome: f.video, acao: 'criado com R$ ' + f.verba })), erros });
      await dbSet('trafego_log', lg);
      // recarrega em vez de apagar — apagar deixava o Copiloto sem dados
      const KRC = (process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim();
      for (const p of ['ciclo', '7d']) {
        fetch(`https://reparoeletroadm.com/api/trafego?action=painel&periodo=${p}&forcar=1&k=${KRC}`).catch(() => {});
      }
    } catch (e) {}
    // 🚦 confere se algum ficou aguardando aprovação (recurso de segurança da conta)
    let pendencias = null;
    if (feitos.length) {
      try {
        await new Promise(s => setTimeout(s, 2500));
        const KP = (process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim();
        const pr = await fetch(`https://reparoeletroadm.com/api/trafego?action=pendentes-aprovacao&dias=1&k=${KP}`)
          .then(x => x.json()).catch(() => null);
        if (pr && pr.total > 0) pendencias = { total: pr.total, aviso: pr.ACAO_NECESSARIA, lista: pr.lista };
      } catch (e) {}
    }
    return res.status(200).json({ ok: erros.length === 0, criados: feitos.length, feitos, erros,
      APROVACAO_PENDENTE: pendencias || 'nenhuma',
      terminaEm: new Date(fim.getTime() - 3 * 3600000).toISOString().slice(0, 16).replace('T', ' ') + ' BRT',
      aviso: erros.length ? 'veja os erros — o vídeo não foi trocado no criativo, apenas a campanha foi clonada' : undefined,
      proximoPasso: 'confira em /trafego e troque o vídeo no criativo se necessário' });
  }

  // ── 🔎 modelo direto da Meta: o cache do painel esvazia na virada do ciclo ──
  async function modeloDaMeta(cat, tk) {
    try {
      const ads = await pegarTudo(`${GRAPH}/act_${CONTA}/ads?fields=id,name,effective_status,adset{id},campaign{id,name,start_time}&limit=400&access_token=${tk}`, 10);
      const cands = (ads.data || [])
        .filter(a => (a.adset || {}).id && (a.campaign || {}).id)
        .filter(a => categoriaDe(a.name || (a.campaign || {}).name || '', 'anuncio') === cat)
        .sort((x, y) => String((y.campaign || {}).start_time || '').localeCompare(String((x.campaign || {}).start_time || '')));
      const m = cands[0];
      if (!m) return null;
      return { id: m.id, nome: m.name, adsetId: m.adset.id, campanhaId: m.campaign.id,
        categoria: cat, viaMeta: true };
    } catch (e) { return null; }
  }

  // ── 🧹 LIMPAR-ORFAS: remove campanhas criadas sem anúncio dentro ──
  if (action === 'limpar-orfas') {
    if (!CONTA) return res.status(200).json({ ok: false, error: 'conta não configurada' });
    const TKO = String(req.query.token || '').trim() || TOKEN;
    const desde = String(req.query.desde || '').slice(0, 10);
    const [camps, ads] = await Promise.all([
      pegarTudo(`${GRAPH}/act_${CONTA}/campaigns?fields=id,name,effective_status,daily_budget,lifetime_budget,start_time&limit=300&access_token=${TKO}`, 8),
      pegarTudo(`${GRAPH}/act_${CONTA}/ads?fields=id,campaign{id}&limit=400&access_token=${TKO}`, 10),
    ]);
    const comAnuncio = new Set();
    for (const a of (ads.data || [])) { const ci = (a.campaign || {}).id; if (ci) comAnuncio.add(String(ci)); }
    const orfas = (camps.data || []).filter(c => {
      if (desde && String(c.start_time || '').slice(0, 10) < desde) return false;
      return !comAnuncio.has(String(c.id));
    });
    if (String(req.query.aplicar || '') !== '1') {
      return res.status(200).json({ ok: true, modo: 'prévia',
        campanhasSemAnuncio: orfas.length,
        lista: orfas.map(c => c.name + ' | ' + c.effective_status +
          ' | R$ ' + ((Number(c.lifetime_budget || c.daily_budget || 0) / 100).toFixed(2))),
        dica: 'para excluir: &aplicar=1' });
    }
    const feitos = [], erros = [];
    for (const c of orfas) {
      const r = await fetch(`${GRAPH}/${c.id}?access_token=${TKO}`, { method: 'DELETE' })
        .then(x => x.json()).catch(e => ({ error: { message: e.message } }));
      if (r && r.error) erros.push(c.name + ': ' + r.error.message);
      else feitos.push(c.name);
      await new Promise(s => setTimeout(s, 250));
    }
    return res.status(200).json({ ok: erros.length === 0, excluidas: feitos.length, feitos, erros });
  }

  // ── ♻️ RENOVAR-CICLO: duplica os campeões para o ciclo novo, com verba e data novas ──
  if (action === 'renovar-ciclo') {
    if (!CONTA) return res.status(200).json({ ok: false, error: 'conta não configurada' });
    const TKR = String(req.query.token || '').trim() || TOKEN;
    const frente = String(req.query.frente || 'adm').toLowerCase();
    const verba = parseFloat(req.query.verba || '0');
    const desde = String(req.query.desde || '').slice(0, 10);
    if (!verba || !desde) return res.status(400).json({ ok: false,
      error: 'informe &verba=X&desde=AAAA-MM-DD' });

    // término: sábado 11h BRT da semana do início
    // 🕐 início 13h BRT do dia informado · 🏁 fim sábado 11h BRT da semana seguinte
    const ini = new Date(desde + 'T16:00:00Z');
    const fim = new Date(ini);
    const dSab = (6 - ini.getUTCDay() + 7) % 7;
    fim.setUTCDate(ini.getUTCDate() + (dSab === 0 ? 7 : dSab));
    fim.setUTCHours(14, 0, 0, 0);
    while (fim.getTime() <= ini.getTime()) fim.setUTCDate(fim.getUTCDate() + 7);
    const fimUnix = Math.floor(fim.getTime() / 1000);
    const iniUnix = Math.floor(Math.max(ini.getTime(), Date.now() + 300000) / 1000);

    // campeões do ciclo ANTERIOR = ativos que não estão marcados para corte
    const base = await dbGet('trafego_painel_cache_7d') || await dbGet('trafego_painel_cache_ciclo');
    let todos = (((base || {}).dados || {}).anuncios || [])
      .filter(a => a.campanhaId && a.adsetId);
    // o cache esvazia na virada do ciclo — busca direto na Meta nesse caso
    if (!todos.length) {
      try {
        // 🎯 SÓ O CICLO ANTERIOR: sem esse recorte vinham os 394 anúncios do histórico
        // inteiro da conta, incluindo campanhas de outros negócios de anos atrás.
        const ini = new Date(desde + 'T00:00:00-03:00');
        const inicioAnterior = new Date(ini.getTime() - 7 * 86400000).toISOString().slice(0, 10);
        const ads = await pegarTudo(`${GRAPH}/act_${CONTA}/ads?fields=id,name,effective_status,adset{id},campaign{id,name,start_time,effective_status}&limit=400&access_token=${TKR}`, 10);
        const vistos = new Set();
        todos = (ads.data || [])
          .filter(a => (a.adset || {}).id && (a.campaign || {}).id)
          .filter(a => {
            const dt = String((a.campaign || {}).start_time || '').slice(0, 10);
            if (!(dt >= inicioAnterior && dt < desde)) return false;   // só o ciclo que acabou
            // ⛔ não renovar quem foi PAUSADO no ciclo — foram cortes por desempenho
            const stC = String((a.campaign || {}).effective_status || '');
            if (stC === 'PAUSED' || stC === 'DELETED' || stC === 'ARCHIVED') return false;
            return true;
          })
          .filter(a => { const n = String(a.name || '').toLowerCase(); if (vistos.has(n)) return false; vistos.add(n); return true; })
          .map(a => ({ id: a.id, nome: a.name, adsetId: a.adset.id, campanhaId: a.campaign.id,
            categoria: categoriaDe(a.name || '', 'anuncio'), cpa: null,
            inicio: (a.campaign || {}).start_time }))
          .sort((x, y) => String(y.inicio || '').localeCompare(String(x.inicio || '')));
      } catch (e) {}
    }
    const daFrente = todos.filter(a => frente === 'tv' ? a.categoria === 'tv' : a.categoria !== 'tv');
    const cfgR = await cfgTrafego();
    const campeoes = daFrente.filter(a => {
      const meta = cfgR.metas[a.categoria] != null ? cfgR.metas[a.categoria] : cfgR.metas.outros;
      if (a.cpa == null) return true;                       // sem dado: mantém
      return a.cpa <= meta * 1.3;                           // até 30% acima da meta continua
    });
    const cortados = daFrente.filter(a => !campeoes.includes(a));

    // 🛡 trava: número absurdo indica que o filtro falhou
    if (campeoes.length > 40 && String(req.query.forcar || '') !== '1') {
      return res.status(200).json({ ok: false,
        error: '🛡 ' + campeoes.length + ' anúncios seriam renovados — isso indica que o filtro de ciclo falhou',
        verbaQueSeriaAlocada: Number((verba * campeoes.length).toFixed(2)),
        oQueFazer: 'confira o painel; se estiver certo mesmo, use &forcar=1',
        amostra: campeoes.slice(0, 10).map(a => a.nome + ' | ' + a.categoria) });
    }
    if (String(req.query.aplicar || '') !== '1') {
      return res.status(200).json({ ok: true, modo: 'prévia — nada criado',
        frente: frente.toUpperCase(), cicloNovo: desde,
        comecaEm: new Date(iniUnix * 1000 - 3 * 3600000).toISOString().slice(0, 16).replace('T', ' ') + ' BRT',
        terminaEm: new Date(fim.getTime() - 3 * 3600000).toISOString().slice(0, 16).replace('T', ' ') + ' BRT',
        campeoesQueSeraoRenovados: campeoes.length,
        naoRenovados: cortados.length,
        verbaPorAnuncio: verba,
        verbaTotal: Number((verba * campeoes.length).toFixed(2)),
        RENOVAR: campeoes.map(a => a.nome + ' | CPA ' + (a.cpa ?? '?') + ' | ' + a.categoria),
        NAO_RENOVAR: cortados.map(a => a.nome + ' | CPA ' + (a.cpa ?? '?') + ' — acima de 30% da meta'),
        dica: 'para criar: &aplicar=1' });
    }

    const postF = async (id, campos) => fetch(`${GRAPH}/${id}?access_token=${TKR}`, { method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body: new URLSearchParams(campos).toString() })
      .then(x => x.json()).catch(e => ({ error: { message: e.message } }));
    const postNovo = async (caminho, campos) => fetch(`${GRAPH}/${caminho}?access_token=${TKR}`, { method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body: new URLSearchParams(campos).toString() })
      .then(x => x.json()).catch(e => ({ error: { message: e.message } }));

    const feitos = [], erros = [];
    for (const a of campeoes) {
      try {
        const nome = nomeComData(a.nome);
        const [mC, mS, mA] = await Promise.all([
          fetch(`${GRAPH}/${a.campanhaId}?fields=objective,special_ad_categories&access_token=${TKR}`).then(x => x.json()),
          fetch(`${GRAPH}/${a.adsetId}?fields=optimization_goal,billing_event,destination_type,promoted_object,bid_strategy,targeting{geo_locations,age_min,age_max,genders,locales,flexible_spec,custom_audiences,excluded_custom_audiences,publisher_platforms,device_platforms,targeting_automation}&access_token=${TKR}`).then(x => x.json()),
          fetch(`${GRAPH}/${a.id}?fields=creative{id,object_story_spec}&access_token=${TKR}`).then(x => x.json()),
        ]);
        if (mC.error || mS.error || mA.error) { erros.push(a.nome + ': ' + ((mC.error || mS.error || mA.error).message)); continue; }
        const c1 = await postNovo('act_' + CONTA + '/campaigns', {
          name: nome, objective: mC.objective || 'OUTCOME_ENGAGEMENT', status: 'PAUSED',
          special_ad_categories: JSON.stringify(mC.special_ad_categories || []),
          is_adset_budget_sharing_enabled: 'false',
        });
        if (c1.error) { erros.push(a.nome + ' (campanha): ' + c1.error.message); continue; }
        const camposS = { name: nome + ' - conjunto', campaign_id: c1.id, status: 'PAUSED',
          targeting: JSON.stringify(mS.targeting || {}),
          optimization_goal: mS.optimization_goal || 'CONVERSATIONS',
          billing_event: mS.billing_event || 'IMPRESSIONS',
          bid_strategy: mS.bid_strategy || 'LOWEST_COST_WITHOUT_CAP',
          lifetime_budget: String(Math.round(verba * 100)),
          start_time: String(iniUnix), end_time: String(fimUnix) };
        if (mS.destination_type) camposS.destination_type = mS.destination_type;
        if (mS.promoted_object) camposS.promoted_object = JSON.stringify(mS.promoted_object);
        const s1 = await postNovo('act_' + CONTA + '/adsets', camposS);
        if (s1.error) { erros.push(a.nome + ' (conjunto): ' + s1.error.message); continue; }
        // ♻️ REUTILIZA o criativo original: recriar a partir do object_story_spec falhava
        // com "Invalid parameter" porque a Meta devolve campos que não aceita de volta.
        const cid = (mA.creative || {}).id;
        if (!cid) { erros.push(a.nome + ': não consegui ler o criativo original'); continue; }
        const a1 = await postNovo('act_' + CONTA + '/ads', {
          name: nome, adset_id: s1.id, creative: JSON.stringify({ creative_id: cid }), status: 'ACTIVE' });
        if (a1.error) { erros.push(a.nome + ' (anúncio): ' + a1.error.message); continue; }
        await postF(s1.id, { status: 'ACTIVE' });
        await postF(c1.id, { status: 'ACTIVE' });
        feitos.push(nome + ' → R$ ' + verba);
        await new Promise(s => setTimeout(s, 500));
      } catch (e) { erros.push(a.nome + ': ' + e.message); }
    }
    return res.status(200).json({ ok: erros.length === 0,
      renovados: feitos.length, feitos, erros,
      naoRenovados: cortados.map(a => a.nome),
      terminaEm: new Date(fim.getTime() - 3 * 3600000).toISOString().slice(0, 16).replace('T', ' ') + ' BRT' });
  }

  // ── 🎯 VER-SEGMENTACAO: mostra em português o que o modelo tem configurado ──
  if (action === 'ver-segmentacao') {
    if (!CONTA) return res.status(200).json({ ok: false, error: 'conta não configurada' });
    const tkS = String(req.query.token || '').trim() || TOKEN;
    const cat = String(req.query.cat || 'tv').toLowerCase();
    const base = await dbGet('trafego_painel_cache_ciclo') || await dbGet('trafego_painel_cache');
    const alvoNome = String(req.query.anuncio || '').toLowerCase();
    const cands = ((base || {}).dados || {}).anuncios
      ?.filter(a => a.ativo && a.adsetId && (alvoNome
        ? String(a.nome || '').toLowerCase().includes(alvoNome)
        : a.categoria === cat)) || [];
    const mod = alvoNome ? cands[0]
      : cands.sort((x, y) => (x.razaoMeta || 9) - (y.razaoMeta || 9))[0];
    if (!mod) return res.status(200).json({ ok: false,
      error: 'nenhum modelo encontrado',
      disponiveis: (((base || {}).dados || {}).anuncios || [])
        .filter(a => a.ativo).map(a => a.nome + ' | ' + a.categoria + ' | conjunto: ' + (a.adsetNome || '?')) });

    const [camp, set] = await Promise.all([
      fetch(`${GRAPH}/${mod.campanhaId}?fields=name,objective,special_ad_categories&access_token=${tkS}`).then(x => x.json()),
      fetch(`${GRAPH}/${mod.adsetId}?fields=name,optimization_goal,billing_event,destination_type,targeting{geo_locations,age_min,age_max,genders,locales,flexible_spec,custom_audiences,excluded_custom_audiences,publisher_platforms,device_platforms,targeting_automation}&access_token=${tkS}`).then(x => x.json()),
    ]);
    if (camp.error || set.error) return res.status(200).json({ ok: false,
      error: (camp.error || set.error).message });
    const t = set.targeting || {};
    const g = t.geo_locations || {};
    const traduzGen = { 1: 'homens', 2: 'mulheres' };
    // 📍 a Meta pode usar cities OU custom_locations (ponto no mapa + raio) — ler os dois
    const cidades = (g.cities || []).map(c => c.name + (c.radius ? ' + ' + c.radius + (c.distance_unit || 'km') : ''));
    const pontos = (g.custom_locations || []).map(p =>
      'ponto ' + p.latitude + ', ' + p.longitude + ' + raio de ' + p.radius + ' ' +
      (p.distance_unit === 'kilometer' ? 'km' : (p.distance_unit || '')) +
      (p.country ? ' (' + p.country + ')' : '') +
      ' · mapa: https://maps.google.com/?q=' + p.latitude + ',' + p.longitude);
    const tiposLocal = (g.location_types || []).map(x =>
      x === 'home' ? 'quem mora na área' : (x === 'recent' ? 'quem esteve na área recentemente' : x));
    const regioes = (g.regions || []).map(r => r.name);
    const paises = g.countries || [];
    const semGeo = !(g.cities || []).length && !(g.regions || []).length &&
      !(g.countries || []).length && !(g.custom_locations || []).length;
    return res.status(200).json({ ok: true,
      modeloUsado: mod.nome + ' (CPA R$ ' + (mod.cpa ?? '?') + ')',
      conjunto: set.name || mod.adsetNome || '?',
      ALERTA: semGeo
        ? '🚨 SEM GEOLOCALIZAÇÃO — este conjunto não tem país, região nem cidade definidos. Confira no Gerenciador; se estiver mesmo aberto, o anúncio pode estar sendo mostrado fora da sua área de atendimento.'
        : null,
      CAMPANHA: {
        objetivo: camp.objective,
        oQueSignifica: camp.objective === 'OUTCOME_ENGAGEMENT'
          ? 'otimizar para ENGAJAMENTO — no seu caso, conversas iniciadas no WhatsApp' : camp.objective,
        categoriasEspeciais: (camp.special_ad_categories || []).length
          ? camp.special_ad_categories
          : 'NENHUMA — anúncio comum, sem restrição de segmentação',
        oQueSaoCategoriasEspeciais: 'a Meta obriga a declarar quando o anúncio é de CRÉDITO, EMPREGO, MORADIA, política ou questões sociais. Nesses casos ela PROÍBE segmentar por idade, gênero e CEP, para evitar discriminação. Conserto de eletrodoméstico não se encaixa em nenhuma — por isso a lista vem vazia, e é o correto.',
      },
      SEGMENTACAO: {
        idade: (t.age_min || '?') + ' a ' + (t.age_max || '?') + ' anos',
        genero: (t.genders || []).length ? (t.genders || []).map(x => traduzGen[x] || x).join(', ') : 'todos',
        paises, regioes, cidades,
        pontosNoMapa: pontos,
        quemVe: tiposLocal.length ? tiposLocal : 'não especificado',
        publicoAmpliado: (t.targeting_automation && t.targeting_automation.advantage_audience === 1)
          ? '⚡ Advantage+ LIGADO — a Meta amplia o público além do definido quando acha que converte melhor'
          : 'desligado',
        idiomas: t.locales || 'não restrito',
        interesses: ((t.flexible_spec || []).flatMap(f => (f.interests || []).map(i => i.name))) || [],
        publicoPersonalizado: (t.custom_audiences || []).map(a => a.name || a.id),
        excluidos: (t.excluded_custom_audiences || []).map(a => a.name || a.id),
        posicionamentos: t.publisher_platforms || 'automático (todos)',
        dispositivos: t.device_platforms || 'todos',
        otimizacao: set.optimization_goal,
        cobranca: set.billing_event,
        destino: set.destination_type,
      },
      TARGETING_CRU: t,
      observacao: 'tudo isso é copiado IGUAL para cada anúncio novo — só mudam vídeo, texto, verba e nome' });
  }

  // ── ✅ CHECAR-COPILOTO: confirma que o Copiloto vai ler o ciclo certo ──
  if (action === 'checar-copiloto') {
    if (!CONTA) return res.status(200).json({ ok: false, error: 'conta não configurada' });
    const TKC = String(req.query.token || '').trim() || TOKEN;
    const b = new Date(Date.now() - 3 * 3600000);
    const d = new Date(b); d.setUTCDate(b.getUTCDate() - ((b.getUTCDay() + 1) % 7));
    const inicioCiclo = d.toISOString().slice(0, 10);
    const camps = await pegarTudo(`${GRAPH}/act_${CONTA}/campaigns?fields=id,name,effective_status,start_time,stop_time&limit=300&access_token=${TKC}`, 8);
    const doCiclo = (camps.data || []).filter(c =>
      String(c.start_time || '').slice(0, 10) >= inicioCiclo);
    const anteriores = (camps.data || []).filter(c =>
      String(c.start_time || '').slice(0, 10) < inicioCiclo &&
      c.effective_status === 'ACTIVE');
    // já há gasto no ciclo?
    const ins = await fetch(`${GRAPH}/act_${CONTA}/insights?level=account&fields=spend,actions&time_range=${encodeURIComponent(JSON.stringify({ since: inicioCiclo, until: new Date(Date.now() - 3 * 3600000).toISOString().slice(0, 10) }))}&access_token=${TKC}`)
      .then(x => x.json()).catch(() => null);
    const g = ((ins || {}).data || [])[0] || {};
    const conv = ((g.actions || []).find(a => /messaging_conversation_started/.test(a.action_type)) || {}).value;
    const horasRodando = Math.max(0, (Date.now() - new Date(inicioCiclo + 'T16:00:00Z').getTime()) / 3600000);
    return res.status(200).json({ ok: true,
      cicloComecaEm: inicioCiclo + ' (sábado)',
      horasJaRodadas: Number(horasRodando.toFixed(1)),
      campanhasDoCiclo: doCiclo.length,
      campanhasDeCiclosAnterioresAindaAtivas: anteriores.length,
      avisoAnteriores: anteriores.length
        ? '⚠️ ' + anteriores.length + ' campanha(s) antiga(s) ainda ACTIVE — o Copiloto as descarta por prazo vencido, mas confira se alguma deveria ter parado'
        : '✅ nenhuma campanha antiga ativa',
      gastoNoCiclo: Number((parseFloat(g.spend || 0) || 0).toFixed(2)),
      conversasNoCiclo: parseInt(conv || 0, 10) || 0,
      PRONTO_PARA_DECIDIR: horasRodando >= 36 && (parseInt(conv || 0, 10) || 0) >= 30
        ? '✅ sim — dados suficientes para o Copiloto sugerir cortes'
        : '⏳ ainda cedo — o Copiloto precisa de ~36h e 30+ conversas para uma decisão confiável',
      listaDoCiclo: doCiclo.slice(0, 45).map(c => c.name + ' | ' + c.effective_status),
      listaAnteriores: anteriores.slice(0, 20).map(c => c.name + ' | ' + c.effective_status) });
  }

  // ── ⏱️ ULTIMAS-24H: o ciclo novo comparado com a média das semanas anteriores ──
  if (action === 'ultimas-24h') {
    if (!CONTA) return res.status(200).json({ ok: false, error: 'conta não configurada' });
    const TKH = String(req.query.token || '').trim() || TOKEN;
    const horas = Math.min(72, Math.max(1, parseInt(req.query.horas || '24', 10)));
    const corte = Date.now() - horas * 3600000;
    const hoje = new Date(Date.now() - 3 * 3600000).toISOString().slice(0, 10);
    const ontem = new Date(Date.now() - 3 * 3600000 - 86400000).toISOString().slice(0, 10);

    // gasto e conversas de ontem e hoje, por anúncio
    const ins = await pegarTudo(`${GRAPH}/act_${CONTA}/insights?level=ad&fields=ad_id,ad_name,spend,actions&time_range=${encodeURIComponent(JSON.stringify({ since: ontem, until: hoje }))}&time_increment=1&limit=500&access_token=${TKH}`, 12);
    const novos = {}, antigos = {};
    for (const r of (ins.data || [])) {
      const nome = String(r.ad_name || '');
      const ehNovo = /08082026$/.test(nome.trim());
      const alvo = ehNovo ? novos : antigos;
      const cat = categoriaDe(nome, 'anuncio');
      alvo[cat] = alvo[cat] || { gasto: 0, conversas: 0, anuncios: new Set() };
      alvo[cat].gasto += parseFloat(r.spend || 0) || 0;
      alvo[cat].anuncios.add(r.ad_id);
      const conv = ((r.actions || []).find(a =>
        /messaging_conversation_started/.test(a.action_type)) || {}).value;
      alvo[cat].conversas += parseInt(conv || 0, 10) || 0;
    }
    // fichas e leads criados nas últimas N horas
    const U3 = (process.env.UPSTASH_URL || '').replace(/['"]/g, '').trim();
    const T3 = (process.env.UPSTASH_TOKEN || '').replace(/[\n\r'"]/g, '').trim();
    const ler = async (k) => { try {
      const r = await fetch(`${U3}/get/${k}`, { headers: { Authorization: `Bearer ${T3}` } }).then(x => x.json());
      return r && r.result ? JSON.parse(r.result) : null; } catch (e) { return null; } };
    const [fA, fT, pA, pT] = await Promise.all([ler('fichas_adm'), ler('fichas_tv'), ler('prospeccao_adm'), ler('prospeccao_tv')]);
    const contar = (lista, filtro) => {
      const o = {};
      for (const f of (lista || [])) {
        if (filtro && !filtro(f)) continue;
        const t = new Date(f.criadoEm || f.data || 0).getTime();
        if (!t || t < corte) continue;
        const c = categoriaDe(String(f.equipamento || f.descricao || ''), 'anuncio');
        o[c] = (o[c] || 0) + 1;
      }
      return o;
    };
    const fichas = contar((((fA || {}).fichas) || []).concat(((fT || {}).fichas) || []));
    const leads = contar((((pA || {}).fichas) || []).concat(((pT || {}).fichas) || []),
      f => !['ficha', 'convertido'].includes(String(f.status || '')));

    const cats = [...new Set([...Object.keys(novos), ...Object.keys(antigos), ...Object.keys(fichas), ...Object.keys(leads)])];
    const linhas = cats.map(c => {
      const n = novos[c] || { gasto: 0, conversas: 0, anuncios: new Set() };
      const a = antigos[c] || { gasto: 0, conversas: 0, anuncios: new Set() };
      const gTot = n.gasto + a.gasto, cTot = n.conversas + a.conversas;
      return { categoria: c,
        criativosNovos: n.anuncios.size, gastoNovos: Number(n.gasto.toFixed(2)), conversasNovos: n.conversas,
        cpaNovos: n.conversas ? Number((n.gasto / n.conversas).toFixed(2)) : null,
        criativosAntigos: a.anuncios.size, gastoAntigos: Number(a.gasto.toFixed(2)), conversasAntigos: a.conversas,
        cpaAntigos: a.conversas ? Number((a.gasto / a.conversas).toFixed(2)) : null,
        fichas: fichas[c] || 0, leads: leads[c] || 0,
        custoPorFicha: fichas[c] ? Number((gTot / fichas[c]).toFixed(2)) : null,
        custoPorLead: leads[c] ? Number((gTot / leads[c]).toFixed(2)) : null };
    }).sort((x, y) => y.gastoNovos - x.gastoNovos);

    const somaN = linhas.reduce((s, l) => s + l.gastoNovos, 0);
    const somaA = linhas.reduce((s, l) => s + l.gastoAntigos, 0);
    const convN = linhas.reduce((s, l) => s + l.conversasNovos, 0);
    const convA = linhas.reduce((s, l) => s + l.conversasAntigos, 0);
    return res.status(200).json({ ok: true, periodo: 'últimas ' + horas + 'h',
      CICLO_NOVO_08082026: { gasto: Number(somaN.toFixed(2)), conversas: convN,
        cpa: convN ? Number((somaN / convN).toFixed(2)) : null },
      CICLO_ANTERIOR: { gasto: Number(somaA.toFixed(2)), conversas: convA,
        cpa: convA ? Number((somaA / convA).toFixed(2)) : null },
      totalFichas: Object.values(fichas).reduce((a, b) => a + b, 0),
      totalLeads: Object.values(leads).reduce((a, b) => a + b, 0),
      TABELA: linhas.map(l => l.categoria.padEnd(14) +
        ' | NOVO: ' + String(l.criativosNovos).padStart(2) + ' anún R$ ' + String(l.gastoNovos).padStart(7) +
        ' ' + String(l.conversasNovos).padStart(3) + ' conv CPA ' + String(l.cpaNovos || '—').padStart(6) +
        ' | ANTIGO: ' + String(l.conversasAntigos).padStart(3) + ' conv CPA ' + String(l.cpaAntigos || '—').padStart(6) +
        ' | ' + String(l.fichas).padStart(2) + ' fichas ' + String(l.leads).padStart(3) + ' leads'),
      detalhe: linhas });
  }

  // ── 🧠 INTELIGENCIA-SEMANAS: investimento × fichas × leads, por semana e categoria ──
  if (action === 'inteligencia-semanas') {
    if (!CONTA) return res.status(200).json({ ok: false, error: 'conta não configurada' });
    const TKI = String(req.query.token || '').trim() || TOKEN;
    const semanas = Math.min(12, Math.max(1, parseInt(req.query.semanas || '8', 10)));
    const ini = new Date(Date.now() - 3 * 3600000 - semanas * 7 * 86400000).toISOString().slice(0, 10);
    const fim = new Date(Date.now() - 3 * 3600000).toISOString().slice(0, 10);

    // 1) GASTO por dia e por anúncio
    const ins = await pegarTudo(`${GRAPH}/act_${CONTA}/insights?level=ad&fields=ad_id,ad_name,spend,actions&time_range=${encodeURIComponent(JSON.stringify({ since: ini, until: fim }))}&time_increment=1&limit=500&access_token=${TKI}`, 20);
    const semanaDe = (d) => {
      const dias = Math.floor((new Date(fim + 'T12:00:00Z') - new Date(d + 'T12:00:00Z')) / 86400000);
      return Math.floor(dias / 7);
    };
    const gasto = {};      // [semana][categoria] = valor
    const conversas = {};
    for (const r of (ins.data || [])) {
      const s = semanaDe(String(r.date_start || '').slice(0, 10));
      if (s < 0 || s >= semanas) continue;
      const cat = categoriaDe(r.ad_name || '', 'anuncio');
      gasto[s] = gasto[s] || {}; conversas[s] = conversas[s] || {};
      gasto[s][cat] = (gasto[s][cat] || 0) + (parseFloat(r.spend || 0) || 0);
      const conv = ((r.actions || []).find(a =>
        /onsite_conversion.messaging_conversation_started|messaging_conversation_started/.test(a.action_type)) || {}).value;
      conversas[s][cat] = (conversas[s][cat] || 0) + (parseInt(conv || 0, 10) || 0);
    }

    // 2) FICHAS e LEADS por semana e categoria
    const U2 = (process.env.UPSTASH_URL || '').replace(/['"]/g, '').trim();
    const T2 = (process.env.UPSTASH_TOKEN || '').replace(/[\n\r'"]/g, '').trim();
    const ler = async (k) => {
      try {
        const r = await fetch(`${U2}/get/${k}`, { headers: { Authorization: `Bearer ${T2}` } }).then(x => x.json());
        return r && r.result ? JSON.parse(r.result) : null;
      } catch (e) { return null; }
    };
    const [fA, fT, prosA, prosT] = await Promise.all([
      ler('fichas_adm'), ler('fichas_tv'), ler('prospeccao_adm'), ler('prospeccao_tv'),
    ]);
    const catDoEquip = (txt) => categoriaDe(String(txt || ''), 'anuncio');
    const fichas = {}, leads = {};
    const contar = (lista, alvo) => {
      for (const f of (lista || [])) {
        const d = String(f.criadoEm || f.data || '').slice(0, 10);
        if (!d || d < ini) continue;
        const s = semanaDe(d);
        if (s < 0 || s >= semanas) continue;
        const cat = catDoEquip(f.equipamento || f.descricao || '');
        alvo[s] = alvo[s] || {};
        alvo[s][cat] = (alvo[s][cat] || 0) + 1;
      }
    };
    contar(((fA || {}).fichas) || [], fichas);
    contar(((fT || {}).fichas) || [], fichas);
    // leads = quem está em prospecção e NÃO virou ficha
    contar((((prosA || {}).fichas) || []).filter(f => !['ficha', 'convertido'].includes(String(f.status || ''))), leads);
    contar((((prosT || {}).fichas) || []).filter(f => !['ficha', 'convertido'].includes(String(f.status || ''))), leads);

    // 3) monta a tabela
    const cats = [...new Set([...Object.values(gasto), ...Object.values(fichas), ...Object.values(leads)]
      .flatMap(o => Object.keys(o || {})))];
    const linhas = [];
    for (let s = 0; s < semanas; s++) {
      const rot = s === 0 ? 'esta semana' : (s === 1 ? 'semana passada' : s + ' semanas atrás');
      const g = gasto[s] || {}, fi = fichas[s] || {}, le = leads[s] || {}, co = conversas[s] || {};
      const totG = Object.values(g).reduce((a, b) => a + b, 0);
      const totF = Object.values(fi).reduce((a, b) => a + b, 0);
      const totL = Object.values(le).reduce((a, b) => a + b, 0);
      const totC = Object.values(co).reduce((a, b) => a + b, 0);
      linhas.push({ semana: s, rotulo: rot,
        gasto: Number(totG.toFixed(2)), conversas: totC, fichas: totF, leads: totL,
        custoPorConversa: totC ? Number((totG / totC).toFixed(2)) : null,
        custoPorFicha: totF ? Number((totG / totF).toFixed(2)) : null,
        custoPorLead: totL ? Number((totG / totL).toFixed(2)) : null,
        taxaFichaSobreConversa: totC ? Math.round(totF / totC * 100) + '%' : null,
        porCategoria: cats.map(c => ({ categoria: c,
          gasto: Number((g[c] || 0).toFixed(2)), conversas: co[c] || 0,
          fichas: fi[c] || 0, leads: le[c] || 0,
          custoPorFicha: fi[c] ? Number(((g[c] || 0) / fi[c]).toFixed(2)) : null,
          custoPorLead: le[c] ? Number(((g[c] || 0) / le[c]).toFixed(2)) : null })).filter(x => x.gasto || x.fichas || x.leads),
      });
    }
    return res.status(200).json({ ok: true, periodo: ini + ' a ' + fim,
      TABELA: linhas.map(l => l.rotulo.padEnd(17) + ' | R$ ' + String(l.gasto).padStart(8) +
        ' | ' + String(l.conversas).padStart(4) + ' conv | ' + String(l.fichas).padStart(3) + ' fichas | ' +
        String(l.leads).padStart(3) + ' leads | ficha R$ ' + String(l.custoPorFicha || '—').padStart(7) +
        ' | lead R$ ' + String(l.custoPorLead || '—').padStart(7)),
      detalhe: linhas });
  }

  // ── 📞 DESTINO-ANUNCIOS: para qual WhatsApp cada anúncio manda o cliente ──
  if (action === 'destino-anuncios') {
    if (!CONTA) return res.status(200).json({ ok: false, error: 'conta não configurada' });
    const TKD = String(req.query.token || '').trim() || TOKEN;
    const desde = String(req.query.desde || '').slice(0, 10);
    const ads = await pegarTudo(`${GRAPH}/act_${CONTA}/ads?fields=id,name,effective_status,creative,campaign{id,name,start_time}&limit=400&access_token=${TKD}`, 10);
    const filtrados = (ads.data || [])
      .filter(a => !desde || String((a.campaign || {}).start_time || '').slice(0, 10) >= desde)
      .slice(0, 60);
    // o object_story_spec não vem na listagem — busca criativo a criativo
    const specs = {};
    for (const a of filtrados) {
      const cid = (a.creative || {}).id;
      if (!cid || specs[cid]) continue;
      const cr = await fetch(`${GRAPH}/${cid}?fields=object_story_spec&access_token=${TKD}`)
        .then(x => x.json()).catch(() => null);
      specs[cid] = (cr && cr.object_story_spec) || {};
      await new Promise(s => setTimeout(s, 120));
    }
    const lista = filtrados
      .map(a => {
        const oss = specs[(a.creative || {}).id] || {};
        const vd = oss.video_data || oss.link_data || {};
        const cta = vd.call_to_action || {};
        const val = cta.value || {};
        const num = val.whatsapp_number || val.app_destination || null;
        const link = val.link || null;
        return { anuncio: a.name, situacao: a.effective_status,
          pagina: oss.page_id || null,
          botao: cta.type || null,
          numeroWhatsApp: num, link,
          inicio: String((a.campaign || {}).start_time || '').slice(0, 10) };
      });
    const porNumero = {}, porPagina = {}, porBotao = {};
    for (const l of lista) {
      const k = l.numeroWhatsApp || (l.link ? 'via link: ' + String(l.link).slice(0, 40) : '(não definido)');
      porNumero[k] = (porNumero[k] || 0) + 1;
      porPagina[l.pagina || '?'] = (porPagina[l.pagina || '?'] || 0) + 1;
      porBotao[l.botao || '?'] = (porBotao[l.botao || '?'] || 0) + 1;
    }
    const esperado = String(req.query.esperado || '3099').replace(/\D/g, '');
    const errados = lista.filter(l => l.numeroWhatsApp &&
      !String(l.numeroWhatsApp).replace(/\D/g, '').endsWith(esperado));
    return res.status(200).json({ ok: errados.length === 0, totalAnuncios: lista.length,
      numeroEsperado: '...' + esperado,
      ALERTA: errados.length
        ? '🚨 ' + errados.length + ' anúncio(s) apontando para número DIFERENTE do esperado'
        : '✅ nenhum anúncio com número divergente',
      DIVERGENTES: errados.map(l => l.anuncio + ' → ' + l.numeroWhatsApp),
      POR_NUMERO_DE_DESTINO: porNumero,
      POR_PAGINA: porPagina,
      POR_BOTAO: porBotao,
      observacao: 'quando o destino é a Página (sem número explícito), a Meta usa o WhatsApp vinculado à Página — confira em Configurações da Página',
      LISTA: lista.slice(0, 60).map(l => l.inicio + ' | ' + String(l.anuncio).slice(0, 30) +
        ' | ' + (l.botao || '?') + ' | ' + (l.numeroWhatsApp || l.link || 'destino pela Página')) });
  }

  // ── 📐 AUDITORIA-VERBA: a linha do tempo da verba no ciclo ──
  if (action === 'auditoria-verba') {
    const TK = String(req.query.token || '').trim() || TOKEN;
    const desde = String(req.query.desde || '2026-08-08').slice(0, 10);
    const hoje = new Date(Date.now() - 3 * 3600000).toISOString().slice(0, 10);

    // 1) o que a Meta diz hoje: verba e situação de cada campanha do ciclo
    const ads = await pegarTudo(`${GRAPH}/act_${CONTA}/ads`
      + `?fields=id,name,effective_status,adset{id,name,effective_status,daily_budget,lifetime_budget},`
      + `campaign{id,name,effective_status,start_time,daily_budget,lifetime_budget}`
      + `&limit=400&access_token=${TK}`, 10);
    const camp = {};
    for (const a of (ads.data || [])) {
      const c = a.campaign || {}, cj = a.adset || {};
      if (!c.id || String(c.start_time || '').slice(0, 10) < desde) continue;
      if (!camp[c.id]) camp[c.id] = { id: c.id, nome: c.name,
        situacao: c.effective_status,
        verba: (Number(cj.lifetime_budget || c.lifetime_budget || 0) / 100) ||
               (Number(cj.daily_budget || c.daily_budget || 0) / 100),
        ehTv: /\btv\b|televis|tela|led|barramento|apagad|imagem/i.test(String(c.name || '')) };
    }
    // 2) gasto de cada uma no ciclo
    let url = `${GRAPH}/act_${CONTA}/insights?level=campaign&fields=campaign_id,spend`
      + `&time_range=${encodeURIComponent(JSON.stringify({ since: desde, until: hoje }))}`
      + `&limit=400&access_token=${TK}`;
    let p = 0;
    while (url && p < 5) {
      const r = await fetch(url).then(x => x.json());
      if (!r || r.error) break;
      for (const c of (r.data || [])) if (camp[c.campaign_id]) camp[c.campaign_id].gasto = parseFloat(c.spend || 0) || 0;
      url = (r.paging && r.paging.next) || null; p++;
    }
    for (const c of Object.values(camp)) c.gasto = c.gasto || 0;

    // 3) o histórico de aplicações do ciclo: quem pausou e quem recebeu verba
    let aplicacoes = [];
    try {
      const lg = (await dbGet('trafego_log')) || { itens: [] };
      aplicacoes = (lg.itens || []).filter(x => String(x.ts || '').slice(0, 10) >= desde)
        .map(x => ({ quando: String(x.ts || '').slice(5, 16).replace('T', ' '),
          pausados: (x.PAUSADOS || []).length,
          verbasAjustadas: (x.VERBAS || []).length,
          detalhePausas: x.PAUSADOS || [], detalheVerbas: x.VERBAS || [] }));
    } catch (e) {}

    const ativas = Object.values(camp).filter(c => c.situacao === 'ACTIVE');
    const pausadas = Object.values(camp).filter(c => c.situacao !== 'ACTIVE');
    const soma = (arr, campo) => +arr.reduce((s, c) => s + (c[campo] || 0), 0).toFixed(2);
    const vAt = soma(ativas, 'verba'), gAt = soma(ativas, 'gasto');
    const vPa = soma(pausadas, 'verba'), gPa = soma(pausadas, 'gasto');
    // teto informado (ou o padrão do projeto)
    const tetoAdm = Number(req.query.tetoAdm || 2484);
    const tetoTv = Number(req.query.tetoTv || 870);
    const teto = tetoAdm + tetoTv;
    const naoGastoNasPausadas = +(vPa - gPa).toFixed(2);
    const previsaoFinal = +(gPa + vAt).toFixed(2);

    return res.status(200).json({ ok: true,
      ciclo: { de: desde, ate: hoje },
      ETAPA_1_INICIO_DO_CICLO: {
        campanhas: Object.keys(camp).length,
        tetoPrevisto: teto,
        observacao: 'teto informado: ADM ' + tetoAdm + ' + TV ' + tetoTv,
      },
      ETAPA_2_O_CORTE: {
        campanhasPausadas: pausadas.length,
        verbaQueTinham: vPa,
        jaHaviamGastado: gPa,
        faltavamGastar: naoGastoNasPausadas,
        L: pausadas.map(c => c.nome.slice(0, 40).padEnd(40) +
          ' verba ' + c.verba.toFixed(2).padStart(8) +
          ' gasto ' + c.gasto.toFixed(2).padStart(8) +
          ' sobrou ' + (c.verba - c.gasto).toFixed(2).padStart(8)),
      },
      ETAPA_3_A_REDISTRIBUICAO: {
        campanhasAtivas: ativas.length,
        verbaAtualDelas: vAt,
        aplicacoesNoCiclo: aplicacoes.length,
        HISTORICO: aplicacoes.map(a => a.quando + ' | ' + a.pausados + ' pausada(s) · ' +
          a.verbasAjustadas + ' verba(s) ajustada(s)'),
        DETALHE_VERBAS: aplicacoes.flatMap(a => a.detalheVerbas).slice(0, 60),
      },
      ETAPA_4_A_CONTA: {
        verbaSomadaHoje: +(vAt + vPa).toFixed(2),
        tetoPrevisto: teto,
        diferencaSobreOTeto: +((vAt + vPa) - teto).toFixed(2),
        gastoAteAgora: +(gAt + gPa).toFixed(2),
        faltaGastarNasAtivas: +(vAt - gAt).toFixed(2),
        previsaoDeFechamento: previsaoFinal,
        ficaraSemUso: +(teto - previsaoFinal).toFixed(2),
      },
      COMO_DEVERIA_SER: {
        regra: 'a verba da pausada NÃO se mexe. O que ela deixou de gastar deve aumentar a verba das ativas, para o total investido no fim do ciclo igualar o teto',
        contaCorreta: 'ativas deveriam ter: teto (' + teto + ') − gasto das pausadas (' + gPa + ') = ' + (+(teto - gPa).toFixed(2)),
        verbaRealDasAtivas: vAt,
        faltouDistribuir: +((teto - gPa) - vAt).toFixed(2),
      },
      ATIVAS: ativas.map(c => c.nome.slice(0, 40).padEnd(40) +
        ' verba ' + c.verba.toFixed(2).padStart(8) + ' gasto ' + c.gasto.toFixed(2).padStart(8)),
    });
  }

  // ── 🩺 SAUDE-DO-CICLO: onde a verba está indo, e onde não está ──
  if (action === 'saude-ciclo') {
    const TK = String(req.query.token || '').trim() || TOKEN;
    const desde = String(req.query.desde || '2026-08-08').slice(0, 10);
    const hoje = new Date(Date.now() - 3 * 3600000).toISOString().slice(0, 10);
    // 1) situação atual de cada anúncio
    const ads = await pegarTudo(`${GRAPH}/act_${CONTA}/ads`
      + `?fields=id,name,status,effective_status,issues_info,`
      + `adset{id,name,status,effective_status,daily_budget,lifetime_budget,end_time},`
      + `campaign{id,name,status,effective_status,start_time,stop_time,daily_budget,lifetime_budget}`
      + `&limit=400&access_token=${TK}`, 10);
    // 2) gasto de hoje e do ciclo, por campanha
    const gastos = async (de, ate) => {
      const m = {};
      let url = `${GRAPH}/act_${CONTA}/insights?level=campaign&fields=campaign_id,campaign_name,spend,impressions`
        + `&time_range=${encodeURIComponent(JSON.stringify({ since: de, until: ate }))}`
        + `&limit=400&access_token=${TK}`;
      let p = 0;
      while (url && p < 5) {
        const r = await fetch(url).then(x => x.json());
        if (!r || r.error) break;
        for (const c of (r.data || [])) {
          m[c.campaign_id] = { gasto: parseFloat(c.spend || 0) || 0,
            impressoes: parseInt(c.impressions || 0, 10) || 0, nome: c.campaign_name };
        }
        url = (r.paging && r.paging.next) || null; p++;
      }
      return m;
    };
    const [gHoje, gCiclo] = await Promise.all([gastos(hoje, hoje), gastos(desde, hoje)]);

    const porCampanha = {};
    for (const a of (ads.data || [])) {
      const c = a.campaign || {}, cj = a.adset || {};
      if (String(c.start_time || '').slice(0, 10) < desde) continue;
      const id = c.id;
      porCampanha[id] = porCampanha[id] || { nome: c.name, id,
        situacao: c.effective_status, statusProprio: c.status,
        fim: (c.stop_time || cj.end_time || '').slice(0, 16).replace('T', ' '),
        verba: (Number(cj.lifetime_budget || c.lifetime_budget || 0) / 100) ||
               (Number(cj.daily_budget || c.daily_budget || 0) / 100),
        tipoVerba: (cj.lifetime_budget || c.lifetime_budget) ? 'total' : 'diária',
        anuncios: [], problemas: [] };
      const p = porCampanha[id];
      const probs = (a.issues_info || []).map(x => x.error_summary || x.error_message).filter(Boolean);
      p.anuncios.push({ nome: a.name, status: a.status, efetivo: a.effective_status,
        conjunto: cj.effective_status, problemas: probs });
      for (const x of probs) if (!p.problemas.includes(x)) p.problemas.push(x);
    }

    const L = [], alertas = [];
    let totHoje = 0, totCiclo = 0, verbaTotal = 0;
    for (const [id, c] of Object.entries(porCampanha)) {
      const gh = (gHoje[id] || {}).gasto || 0;
      const gc = (gCiclo[id] || {}).gasto || 0;
      const imp = (gHoje[id] || {}).impressoes || 0;
      totHoje += gh; totCiclo += gc; verbaTotal += c.verba;
      const ativa = c.situacao === 'ACTIVE';
      const anunciosAtivos = c.anuncios.filter(a => a.efetivo === 'ACTIVE').length;
      // 🚩 sinais de que a verba não está sendo consumida
      if (ativa && gh === 0) alertas.push('🔴 ' + c.nome.slice(0, 40) + ' — ATIVA mas gastou R$ 0 hoje' +
        (imp === 0 ? ' e sem impressões' : ''));
      if (ativa && anunciosAtivos === 0) alertas.push('🔴 ' + c.nome.slice(0, 40) +
        ' — campanha ativa com ZERO anúncio entregando');
      if (c.problemas.length) alertas.push('🟠 ' + c.nome.slice(0, 40) + ' — ' + c.problemas[0].slice(0, 70));
      const falta = c.verba - gc;
      if (ativa && c.tipoVerba === 'total' && falta > c.verba * 0.5)
        alertas.push('🟡 ' + c.nome.slice(0, 40) + ' — gastou só ' +
          Math.round(gc / c.verba * 100) + '% da verba do ciclo');
      L.push((ativa ? '🟢' : '⏸️') + ' ' + c.nome.slice(0, 38).padEnd(38) +
        ' | verba ' + c.verba.toFixed(0).padStart(4) + ' (' + c.tipoVerba + ')' +
        ' | hoje ' + gh.toFixed(2).padStart(7) +
        ' | ciclo ' + gc.toFixed(2).padStart(8) +
        ' | anúncios ' + anunciosAtivos + '/' + c.anuncios.length +
        (c.fim ? ' | até ' + c.fim : '') +
        (c.problemas.length ? ' | 🚩 ' + c.problemas[0].slice(0, 40) : ''));
    }
    // ritmo esperado
    const diasCorridos = Math.max(1, Math.ceil((Date.now() - new Date(desde + 'T13:00:00-03:00').getTime()) / 86400000));
    const esperadoAteAgora = verbaTotal * (diasCorridos / 7);
    return res.status(200).json({ ok: alertas.length === 0,
      ciclo: { desde, hoje, diaDoCiclo: diasCorridos + ' de 7' },
      RESUMO: {
        campanhas: L.length,
        ativas: Object.values(porCampanha).filter(c => c.situacao === 'ACTIVE').length,
        verbaTotalDoCiclo: +verbaTotal.toFixed(2),
        gastoHoje: +totHoje.toFixed(2),
        gastoNoCiclo: +totCiclo.toFixed(2),
        esperadoAteAgora: +esperadoAteAgora.toFixed(2),
        diferenca: +(totCiclo - esperadoAteAgora).toFixed(2),
        mediaDiariaEsperada: +(verbaTotal / 7).toFixed(2),
      },
      VEREDITO: alertas.length
        ? '🚨 ' + alertas.length + ' ponto(s) de atenção'
        : '✅ todas as campanhas ativas estão gastando',
      ALERTAS: alertas,
      CAMPANHAS: L.sort() });
  }

  // ── 📚 HISTORICO: todas as execuções, com o detalhe de cada anúncio ──
  if (action === 'historico') {
    const dias = Math.min(60, Math.max(1, parseInt(req.query.dias || '14', 10)));
    const corte = Date.now() - dias * 86400000;
    const lg = (await dbGet('trafego_log')) || { movs: [] };
    const hh = d => d ? new Date(new Date(d).getTime() - 3 * 3600000).toISOString().slice(5, 16).replace('T', ' ') : '?';
    const execs = (lg.movs || []).filter(m => new Date(m.ts || 0).getTime() >= corte);
    const detalhado = execs.map((m, idx) => {
      const feitos = m.feitos || [], erros = m.erros || [];
      const pausas = feitos.filter(f => /pausad/i.test(String(f.acao || '')));
      const criados = feitos.filter(f => /criad|novo/i.test(String(f.acao || '')));
      // verbas: exclui os criados, que também mencionam valor e apareciam duas vezes
      const verbas = feitos.filter(f => /budget/i.test(String(f.acao || '')) && !criados.includes(f));
      return {
        n: idx + 1, quando: hh(m.ts),
        resumo: [pausas.length ? pausas.length + ' pausado(s)' : null,
          verbas.length ? verbas.length + ' verba(s) ajustada(s)' : null,
          criados.length ? criados.length + ' criado(s)' : null,
          erros.length ? erros.length + ' erro(s)' : null].filter(Boolean).join(' · ') || 'sem alterações',
        PAUSADOS: pausas.map(f => (f.nome || f.id) + (f.acao ? ' — ' + f.acao : '')),
        VERBAS: verbas.map(f => (f.nome ? f.nome : 'campanha ' + String(f.id || '').slice(-6)) + ' → ' + f.acao),
        CRIADOS: criados.map(f => (f.nome || f.id) + ' — ' + f.acao),
        ERROS: erros.map(e => typeof e === 'string' ? e
          : ((e.nome || e.id || e.video || '?') + (e.acao ? ' | ' + e.acao : '') +
             ' | ' + (e.erro || e.error || e.message || JSON.stringify(e).slice(0, 80)))),
      };
    });
    // 📅 agrupa por dia, para o painel abrir por data
    const porDia = {};
    for (const d of detalhado) {
      const dia = d.quando.slice(0, 5);   // MM-DD
      porDia[dia] = porDia[dia] || { execucoes: 0, pausados: 0, verbas: 0, criados: 0, erros: 0, itens: [] };
      porDia[dia].execucoes++;
      porDia[dia].pausados += d.PAUSADOS.length;
      porDia[dia].verbas += d.VERBAS.length;
      porDia[dia].criados += d.CRIADOS.length;
      porDia[dia].erros += d.ERROS.length;
      porDia[dia].itens.push(d);
    }
    return res.status(200).json({ ok: true, periodoDias: dias,
      execucoes: detalhado.length,
      POR_DIA: Object.entries(porDia).map(([dia, v]) => ({
        dia, execucoes: v.execucoes,
        resumo: [v.pausados ? v.pausados + ' pausado(s)' : null,
          v.verbas ? v.verbas + ' verba(s)' : null,
          v.criados ? v.criados + ' criado(s)' : null,
          v.erros ? v.erros + ' erro(s)' : null].filter(Boolean).join(' · ') || 'sem alterações',
        pausados: v.pausados, verbas: v.verbas, criados: v.criados, erros: v.erros,
        execucoesDoDia: v.itens })),
      LINHA_DO_TEMPO: detalhado.map(d => '#' + d.n + ' · ' + d.quando + ' · ' + d.resumo),
      DETALHE: detalhado });
  }

  // ── 📜 ULTIMA-APLICACAO: o que a última execução do Copiloto fez ──
  if (action === 'ultima-aplicacao') {
    const lg = (await dbGet('trafego_log')) || { movs: [] };
    const n = Math.min(5, Math.max(1, parseInt(req.query.n || '1', 10)));
    return res.status(200).json({ ok: true,
      execucoes: (lg.movs || []).slice(0, n).map(m => ({
        quando: new Date(new Date(m.ts).getTime() - 3 * 3600000).toISOString().slice(0, 16).replace('T', ' '),
        totalFeitos: (m.feitos || []).length,
        totalErros: (m.erros || []).length,
        PAUSAS: (m.feitos || []).filter(f => /pausado/i.test(String(f.acao || '')))
          .map(f => (f.nome || f.id) + ' → ' + f.acao),
        VERBAS: (m.feitos || []).filter(f => /budget/i.test(String(f.acao || '')))
          .map(f => (f.nome || f.id) + ' → ' + f.acao),
        ERROS: (m.erros || []).map(e => (e.nome || e.id) + ' | ' + e.acao + ' | ' + e.erro),
      })) });
  }

  // ── 🔧 REATIVAR-ANUNCIOS: liga os anúncios de campanhas ativas que ficaram sem ──
  if (action === 'reativar-anuncios') {
    const TKR2 = String(req.query.token || '').trim() || TOKEN;
    const desde = String(req.query.desde || '2026-08-08').slice(0, 10);
    const ads = await pegarTudo(`${GRAPH}/act_${CONTA}/ads?fields=id,name,status,effective_status,issues_info,adset{id,status,effective_status},campaign{id,name,status,effective_status,start_time}&limit=400&access_token=${TKR2}`, 10);
    const alvo = (ads.data || []).filter(a => {
      const c = a.campaign || {};
      if (String(c.start_time || '').slice(0, 10) < desde) return false;
      if (String(c.effective_status || '') !== 'ACTIVE') return false;   // campanha ativa
      return String(a.status || '') === 'PAUSED';                        // anúncio pausado
    });
    if (String(req.query.aplicar || '') !== '1') {
      // 🔍 o MOTIVO real: alteração de verba coloca o anúncio em revisão, e isso
      // aparece como WITH_ISSUES sem que ninguém o tenha pausado de fato
      return res.status(200).json({ ok: true, modo: 'prévia',
        anunciosPausadosEmCampanhaAtiva: alvo.length,
        L: alvo.map(a => String((a.campaign || {}).name || '?').slice(0, 30) +
          ' | status próprio: ' + a.status +
          ' | efetivo: ' + a.effective_status +
          ' | conjunto: ' + ((a.adset || {}).status || '?') +
          ' | motivo: ' + ((a.issues_info || []).map(x => x.error_summary || x.error_message)
            .filter(Boolean).join(' · ').slice(0, 90) || '(nenhum informado)')),
        LEITURA: 'se o motivo for "não está sendo veiculado, mas você não precisa fazer nada", ' +
          'é o aviso benigno de revisão — NÃO religue, a Meta reativa sozinha',
        dica: 'para religar: &aplicar=1' });
    }
    const feitos = [], erros = [];
    for (const a of alvo) {
      const corpo = new URLSearchParams({ status: 'ACTIVE' }).toString();
      const r = await fetch(`${GRAPH}/${a.id}?access_token=${TKR2}`, {
        method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body: corpo,
      }).then(x => x.json()).catch(e => ({ error: { message: e.message } }));
      if (r && r.error) erros.push(String(a.name || a.id).slice(0, 30) + ': ' + r.error.message);
      else feitos.push(String((a.campaign || {}).name || a.name).slice(0, 34));
      // o conjunto também pode estar pausado
      const ads2 = (a.adset || {});
      if (ads2.id && String(ads2.status || '') === 'PAUSED') {
        await fetch(`${GRAPH}/${ads2.id}?access_token=${TKR2}`, {
          method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
          body: new URLSearchParams({ status: 'ACTIVE' }).toString() }).catch(() => {});
      }
      await new Promise(s => setTimeout(s, 150));
    }
    return res.status(200).json({ ok: erros.length === 0, religados: feitos.length, feitos, erros });
  }

  // ── 🔎 STATUS-CICLO: situação real de cada anúncio do ciclo novo ──
  if (action === 'status-ciclo') {
    if (!CONTA) return res.status(200).json({ ok: false, error: 'conta não configurada' });
    const TKS = String(req.query.token || '').trim() || TOKEN;
    const desde = String(req.query.desde || new Date(Date.now() - 3 * 3600000).toISOString().slice(0, 10));
    const ads = await pegarTudo(`${GRAPH}/act_${CONTA}/ads?fields=id,name,status,effective_status,issues_info,adset{id,status,effective_status,start_time,end_time},campaign{id,name,status,effective_status,start_time}&limit=400&access_token=${TKS}`, 10);
    const doCiclo = (ads.data || []).filter(a =>
      String((a.campaign || {}).start_time || '').slice(0, 10) >= desde);
    const porStatus = {};
    const linhas = doCiclo.map(a => {
      const st = a.effective_status;
      porStatus[st] = (porStatus[st] || 0) + 1;
      const iss = (a.issues_info || []).map(x => x.error_summary || x.error_message).filter(Boolean);
      return { anuncio: a.name, situacao: st, statusProprio: a.status,
        conjunto: (a.adset || {}).effective_status,
        campanha: (a.campaign || {}).effective_status,
        comeca: (a.adset || {}).start_time,
        avisos: iss.length ? iss : null };
    });
    const TRADUZ = {
      ACTIVE: '✅ rodando',
      PENDING_REVIEW: '⏳ em revisão da Meta',
      IN_PROCESS: '⏳ processando',
      CAMPAIGN_PAUSED: '⏸️ campanha pausada',
      ADSET_PAUSED: '⏸️ conjunto pausado',
      PAUSED: '⏸️ pausado',
      DISAPPROVED: '❌ reprovado',
      WITH_ISSUES: '⚠️ com problema',
      PENDING_BILLING_INFO: '💳 falta pagamento',
    };
    const agendados = doCiclo.filter(a => {
      const s = (a.adset || {}).start_time;
      return s && new Date(s).getTime() > Date.now();
    }).length;
    return res.status(200).json({ ok: true, cicloDesde: desde,
      totalAnuncios: doCiclo.length,
      agendadosParaComecar: agendados,
      POR_SITUACAO: Object.entries(porStatus).map(([s, n]) =>
        (TRADUZ[s] || s) + ': ' + n),
      DIAGNOSTICO: agendados === doCiclo.length
        ? '✅ todos AGENDADOS — vão ativar automaticamente no horário de início'
        : (porStatus.ACTIVE === doCiclo.length ? '✅ todos rodando'
        : '⚠️ situações mistas — veja a lista'),
      LISTA: linhas.slice(0, 50).map(l => (TRADUZ[l.situacao] || l.situacao) + ' | ' +
        String(l.anuncio).slice(0, 34) +
        (l.comeca ? ' | começa ' + String(l.comeca).slice(5, 16).replace('T', ' ') : '') +
        (l.avisos ? ' | ' + String(l.avisos[0]).slice(0, 50) : '')) });
  }

  // ── 🤖 CICLO-AUTOMATICO: monta o ciclo inteiro num comando ──
  // R2 → Meta → renova campeões → cria os novos → devolve o relatório do que foi feito
  if (action === 'ciclo-automatico') {
    if (!CONTA) return res.status(200).json({ ok: false, error: 'conta não configurada' });
    const TKA = String(req.query.token || '').trim() || TOKEN;
    const KA = (process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim();
    const pasta = String(req.query.pasta || 'Criativos Reparo Eletro');
    const tetoAdm = parseFloat(req.query.tetoAdm || '2500');
    const tetoTv = parseFloat(req.query.tetoTv || '870');
    const desde = String(req.query.desde || new Date(Date.now() - 3 * 3600000).toISOString().slice(0, 10));
    const base = 'https://reparoeletroadm.com/api/trafego';
    const chamar = async (qs) => fetch(base + '?' + qs + '&k=' + KA).then(x => x.json()).catch(e => ({ ok: false, error: e.message }));
    const etapas = [];
    const reg = (nome, r, resumo) => etapas.push({ etapa: nome, ok: !!(r && r.ok), resumo, detalhe: r });

    // ── 1) o que existe no R2 ──
    const noR2 = await chamar('action=da-pasta&pasta=' + encodeURIComponent(pasta) + '&verba=1');
    const videos = ((noR2 || {}).PLANO || []);
    const porCat = ((noR2 || {}).POR_CATEGORIA) || {};
    reg('1. Ler o R2', noR2, videos.length + ' vídeo(s) encontrado(s)');

    // ── 2) quantos campeões renovam ──
    const prevAdm = await chamar('action=renovar-ciclo&frente=adm&verba=1&desde=' + desde);
    const prevTv = await chamar('action=renovar-ciclo&frente=tv&verba=1&desde=' + desde);
    const nAdmCamp = (prevAdm || {}).campeoesQueSeraoRenovados || 0;
    const nTvCamp = (prevTv || {}).campeoesQueSeraoRenovados || 0;
    // vídeos novos por frente
    const novosTv = Object.entries(porCat).filter(([c]) => c === 'tv').reduce((s, [, n]) => s + n, 0);
    const novosAdm = videos.length - novosTv;
    const totalAdm = nAdmCamp + novosAdm;
    const totalTv = nTvCamp + novosTv;
    const verbaAdm = totalAdm ? Math.floor((tetoAdm / totalAdm) * 100) / 100 : 0;
    const verbaTv = totalTv ? Math.floor((tetoTv / totalTv) * 100) / 100 : 0;
    reg('2. Calcular a verba', { ok: true },
      'ADM: ' + totalAdm + ' anúncios × R$ ' + verbaAdm + ' · TV: ' + totalTv + ' × R$ ' + verbaTv);

    if (String(req.query.aplicar || '') !== '1') {
      return res.status(200).json({ ok: true, modo: 'PRÉVIA — nada foi criado',
        cicloComeca: desde + ' 13:00 BRT',
        PLANO: {
          videosNoR2: videos.length, porCategoria: porCat,
          campeoesAdm: nAdmCamp, novosAdm, totalAdm, verbaPorAnuncioAdm: verbaAdm,
          campeoesTv: nTvCamp, novosTv, totalTv, verbaPorAnuncioTv: verbaTv,
          verbaTotalAdm: Number((verbaAdm * totalAdm).toFixed(2)),
          verbaTotalTv: Number((verbaTv * totalTv).toFixed(2)),
        },
        VIDEOS: videos,
        etapas,
        dica: 'para executar tudo: &aplicar=1' });
    }

    // ── 3) sobe os vídeos do R2 para a Meta ──
    const subiu = await chamar('action=da-pasta&pasta=' + encodeURIComponent(pasta) + '&verba=1&aplicar=1');
    reg('3. Subir vídeos para a Meta', subiu, ((subiu || {}).subidosParaMeta || 0) + ' vídeo(s)');

    // ── 4) renova os campeões ──
    if (nAdmCamp) {
      const r = await chamar('action=renovar-ciclo&frente=adm&verba=' + verbaAdm + '&desde=' + desde + '&aplicar=1');
      reg('4. Renovar campeões ADM', r, (r.renovados || 0) + ' renovado(s)');
    }
    if (nTvCamp) {
      const r = await chamar('action=renovar-ciclo&frente=tv&verba=' + verbaTv + '&desde=' + desde + '&aplicar=1');
      reg('5. Renovar campeões TV', r, (r.renovados || 0) + ' renovado(s)');
    }

    // ── 5) cria os novos, categoria por categoria ──
    for (const [cat, qtd] of Object.entries(porCat)) {
      if (!qtd) continue;
      const v = cat === 'tv' ? verbaTv : verbaAdm;
      const r = await chamar('action=subir-agora&cat=' + cat + '&verba=' + v + '&desde=' + desde + '&aplicar=1');
      reg('6. Criar novos de ' + cat, r, (r.criados || 0) + ' criado(s)' +
        ((r.erros || []).length ? ' · ' + r.erros.length + ' erro(s)' : ''));
      await new Promise(s => setTimeout(s, 800));
    }

    // ── 6) confere o resultado ──
    const extrato = await chamar('action=extrato-verba');
    const pend = await chamar('action=pendentes-aprovacao&dias=1');
    reg('7. Conferir', extrato, ((extrato.TOTAIS || {}).geral || {}).ativos + ' anúncio(s) ativo(s)');

    // 📜 registra a montagem do ciclo no histórico do tráfego
    try {
      const lgC = (await dbGet('trafego_log')) || { movs: [] };
      const feitosC = [];
      for (const e of etapas) {
        for (const f of (((e.detalhe || {}).feitos) || [])) {
          feitosC.push({ nome: typeof f === 'string' ? f : (f.video || f.campanha || '?'),
            acao: e.etapa.replace(/^\d+\.\s*/, '') });
        }
      }
      lgC.movs.unshift({ ts: new Date().toISOString(), origem: 'ciclo-automatico',
        feitos: feitosC, erros: etapas.flatMap(e => ((e.detalhe || {}).erros || [])) });
      lgC.movs = lgC.movs.slice(0, 200);
      await dbSet('trafego_log', lgC);
    } catch (e) {}
    const criados = etapas.filter(e => e.etapa.startsWith('6.')).reduce((s, e) => s + ((e.detalhe || {}).criados || 0), 0);
    const renovados = etapas.filter(e => e.etapa.startsWith('4.') || e.etapa.startsWith('5.'))
      .reduce((s, e) => s + ((e.detalhe || {}).renovados || 0), 0);
    const errosTotais = etapas.flatMap(e => ((e.detalhe || {}).erros || []));
    return res.status(200).json({ ok: errosTotais.length === 0,
      RESUMO: {
        videosSubidos: (subiu || {}).subidosParaMeta || 0,
        campeoesRenovados: renovados, criativosNovos: criados,
        totalNoAr: ((extrato.TOTAIS || {}).geral || {}).ativos || 0,
        verbaAdm: ((extrato.TOTAIS || {}).adm || {}).verba || 0,
        verbaTv: ((extrato.TOTAIS || {}).tv || {}).verba || 0,
        aprovacaoPendente: (pend || {}).aguardandoSuaAprovacao || 0,
        erros: errosTotais.length,
      },
      DISTRIBUICAO: (extrato || {}).porCategoria || {},
      ETAPAS: etapas.map(e => (e.ok ? '✅ ' : '⚠️ ') + e.etapa + ' — ' + e.resumo),
      ERROS: errosTotais.length ? errosTotais : 'nenhum',
      ANUNCIOS: (extrato || {}).ATIVOS || [] });
  }

  // ── 📤 R2-LISTAR / R2-UPLOAD: gerenciar os vídeos do bucket pela tela ──
  if (action === 'r2-listar') {
    const pasta = String(req.query.pasta || 'Criativos Reparo Eletro');
    const prefixo = pasta ? (pasta.replace(/^\/|\/$/g, '') + '/') : '';
    const bucket = (process.env.R2_BUCKET || 'reparo-criativos').trim();
    const R2_PUB = (process.env.R2_PUBLIC_URL || 'https://pub-2e45a0631d27491ea1b38cdd5520b4ea.r2.dev').replace(/\/$/, '');
    const q = { 'list-type': '2', 'max-keys': '200' };
    if (prefixo) q.prefix = prefixo;
    const ass = await assinarR2('GET', '/' + bucket, q);
    const xml = await fetch(ass.url, { headers: ass.headers }).then(x => x.text()).catch(() => '');
    const chaves = [...String(xml).matchAll(/<Key>([^<]+)<\/Key>/g)].map(m => m[1]);
    const tams = [...String(xml).matchAll(/<Size>(\d+)<\/Size>/g)].map(m => Number(m[1]));
    const datas = [...String(xml).matchAll(/<LastModified>([^<]+)<\/LastModified>/g)].map(m => m[1]);
    const arqs = chaves.map((k, i) => ({ chave: k, nome: k.split('/').pop(),
      mb: tams[i] ? Math.round(tams[i] / 1048576) : null,
      quando: datas[i] || null,
      url: R2_PUB + '/' + k.split('/').map(encodeURIComponent).join('/'),
      categoria: categoriaDe(k.split('/').pop(), 'anuncio'),
      texto: textoPorDefeito(k.split('/').pop(), '') }))
      .filter(a => /\.(mp4|mov|avi|mkv|webm)$/i.test(a.nome));
    return res.status(200).json({ ok: true, pasta: prefixo || '(raiz)',
      total: arqs.length,
      totalMB: arqs.reduce((s, a) => s + (a.mb || 0), 0),
      porCategoria: arqs.reduce((o, a) => { o[a.categoria] = (o[a.categoria] || 0) + 1; return o; }, {}),
      arquivos: arqs });
  }
  // gera a URL assinada para o navegador enviar o arquivo direto ao R2
  if (action === 'r2-url-upload') {
    const nome = String(req.query.nome || '').trim();
    const pasta = String(req.query.pasta || 'Criativos Reparo Eletro');
    if (!nome) return res.status(400).json({ ok: false, error: 'informe ?nome=arquivo.mov' });
    const bucket = (process.env.R2_BUCKET || 'reparo-criativos').trim();
    const chave = (pasta ? pasta.replace(/^\/|\/$/g, '') + '/' : '') + nome;
    const caminho = '/' + bucket + '/' + chave.split('/').map(encodeURIComponent).join('/');
    const ass = await assinarR2('PUT', caminho, {});
    return res.status(200).json({ ok: true, url: ass.url, headers: ass.headers, chave });
  }
  // apaga um arquivo específico
  if (action === 'r2-apagar') {
    const chave = String(req.query.chave || '').trim();
    if (!chave) return res.status(400).json({ ok: false, error: 'informe ?chave=' });
    const bucket = (process.env.R2_BUCKET || 'reparo-criativos').trim();
    const caminho = '/' + bucket + '/' + chave.split('/').map(encodeURIComponent).join('/');
    const ass = await assinarR2('DELETE', caminho, {});
    const r = await fetch(ass.url, { method: 'DELETE', headers: ass.headers })
      .then(x => ({ ok: x.status === 204 || x.status === 200, st: x.status }))
      .catch(e => ({ ok: false, st: e.message }));
    return res.status(200).json({ ok: r.ok, chave, http: r.st });
  }

  // ── 🧽 LIMPAR-R2: apaga do bucket os vídeos que já viraram anúncio ──
  if (action === 'limpar-r2') {
    const TKL = String(req.query.token || '').trim() || TOKEN;
    const pasta = String(req.query.pasta || 'Criativos Reparo Eletro');
    const prefixo = pasta ? (pasta.replace(/^\/|\/$/g, '') + '/') : '';
    const bucket = (process.env.R2_BUCKET || 'reparo-criativos').trim();
    // 1) o que está no bucket
    const q = { 'list-type': '2', 'max-keys': '200' };
    if (prefixo) q.prefix = prefixo;
    const ass = await assinarR2('GET', '/' + bucket, q);
    const xml = await fetch(ass.url, { headers: ass.headers }).then(x => x.text()).catch(() => '');
    const chaves = [...String(xml).matchAll(/<Key>([^<]+)<\/Key>/g)].map(m => m[1])
      .filter(k => /\.(mp4|mov|avi|mkv|webm)$/i.test(k));
    if (!chaves.length) return res.status(200).json({ ok: true, msg: 'bucket já está vazio' });
    // 2) quais desses viraram anúncio na Meta
    const ads = await pegarTudo(`${GRAPH}/act_${CONTA}/ads?fields=id,name,effective_status&limit=400&access_token=${TKL}`, 10);
    const nomesNaMeta = new Set((ads.data || []).map(a => String(a.name || '').toLowerCase()));
    const semExt = s => String(s).replace(/\.(mp4|mov|avi|mkv|webm)$/i, '').toLowerCase();
    const analise = chaves.map(k => {
      const nome = k.split('/').pop();
      const baseNome = semExt(nome);
      const virouAnuncio = [...nomesNaMeta].some(n => n.includes(baseNome.slice(0, 24)));
      return { chave: k, arquivo: nome, virouAnuncio };
    });
    const podeApagar = analise.filter(a => a.virouAnuncio);
    const naoApagar = analise.filter(a => !a.virouAnuncio);
    if (String(req.query.aplicar || '') !== '1') {
      return res.status(200).json({ ok: true, modo: 'prévia',
        noBucket: analise.length,
        podeApagar: podeApagar.length, naoApagar: naoApagar.length,
        SEGUROS_PARA_APAGAR: podeApagar.map(a => a.arquivo),
        NAO_VIRARAM_ANUNCIO: naoApagar.map(a => '⚠️ ' + a.arquivo),
        dica: 'para apagar os que já viraram anúncio: &aplicar=1' });
    }
    const feitos = [], erros = [];
    for (const a of podeApagar) {
      const del = await assinarR2('DELETE', '/' + bucket + '/' + a.chave.split('/').map(encodeURIComponent).join('/'), {});
      const r = await fetch(del.url, { method: 'DELETE', headers: del.headers })
        .then(x => ({ ok: x.status === 204 || x.status === 200, st: x.status })).catch(e => ({ ok: false, st: e.message }));
      if (r.ok) feitos.push(a.arquivo); else erros.push(a.arquivo + ' (HTTP ' + r.st + ')');
      await new Promise(s => setTimeout(s, 200));
    }
    return res.status(200).json({ ok: erros.length === 0,
      apagados: feitos.length, feitos, erros,
      mantidos: naoApagar.map(a => a.arquivo),
      observacao: 'só foram apagados os vídeos que já existem como anúncio na Meta' });
  }

  // ── 🔑 assinatura AWS SigV4 — necessária para listar o bucket do R2 ──
  async function assinarR2(metodo, caminho, query) {
    const AK = (process.env.R2_ACCESS_KEY || '31a48286ed15896e6201edadfa35aa87').trim();
    const SK = (process.env.R2_SECRET_KEY || 'c3abe45aec10e95cdf1b65209b11122b1b5fcf3ca92c2947d216c508b823042c').trim();
    const host = (process.env.R2_HOST || '1cef61647aff00cef531b60af8dbdf2b.r2.cloudflarestorage.com').trim();
    const cr = await import('node:crypto');
    const sha256 = (x) => cr.createHash('sha256').update(x).digest('hex');
    const hmac = (k, x) => cr.createHmac('sha256', k).update(x).digest();
    const agora = new Date();
    const amz = agora.toISOString().replace(/[:-]|\.\d{3}/g, '');
    const dia = amz.slice(0, 8);
    const vazio = sha256('');
    const canonQ = Object.keys(query || {}).sort()
      .map(k => encodeURIComponent(k) + '=' + encodeURIComponent(query[k])).join('&');
    const canon = [metodo, caminho, canonQ,
      'host:' + host, 'x-amz-content-sha256:' + vazio, 'x-amz-date:' + amz, '',
      'host;x-amz-content-sha256;x-amz-date', vazio].join('\n');
    const escopo = dia + '/auto/s3/aws4_request';
    const paraAssinar = ['AWS4-HMAC-SHA256', amz, escopo, sha256(canon)].join('\n');
    let k = hmac('AWS4' + SK, dia);
    k = hmac(k, 'auto'); k = hmac(k, 's3'); k = hmac(k, 'aws4_request');
    const assinatura = cr.createHmac('sha256', k).update(paraAssinar).digest('hex');
    return {
      url: 'https://' + host + caminho + (canonQ ? '?' + canonQ : ''),
      headers: {
        Authorization: 'AWS4-HMAC-SHA256 Credential=' + AK + '/' + escopo +
          ', SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=' + assinatura,
        'x-amz-content-sha256': vazio, 'x-amz-date': amz,
      },
    };
  }

  // ── 📁 DA-PASTA: pega vídeos de uma pasta pública do Drive e cria os anúncios ──
  // A Meta baixa o vídeo sozinha pelo file_url — não precisa passar o arquivo por aqui.
  if (action === 'da-pasta') {
    if (!CONTA) return res.status(200).json({ ok: false, error: 'conta não configurada' });
    const tkP = String(req.query.token || '').trim() || TOKEN;
    const pasta = String(req.query.pasta || '').trim();      // subpasta no R2 (opcional)
    const R2_PUB = (process.env.R2_PUBLIC_URL || 'https://pub-2e45a0631d27491ea1b38cdd5520b4ea.r2.dev').replace(/\/$/, '');
    const R2_S3 = (process.env.R2_S3_ENDPOINT || 'https://1cef61647aff00cef531b60af8dbdf2b.r2.cloudflarestorage.com/reparo-criativos').replace(/\/$/, '');
    const cat = String(req.query.cat || 'tv').toLowerCase();
    const verba = parseFloat(req.query.verba || '145');
    // 1) LISTA os vídeos: o bucket público do R2 responde a ?list-type=2 em XML
    let arquivos = [];
    const prefixo = pasta ? (pasta.replace(/^\/|\/$/g, '') + '/') : '';
    try {
      const bucket = (process.env.R2_BUCKET || 'reparo-criativos').trim();
      const q = { 'list-type': '2', 'max-keys': '100' };
      if (prefixo) q.prefix = prefixo;
      const ass = await assinarR2('GET', '/' + bucket, q);
      const xml = await fetch(ass.url, { headers: ass.headers })
        .then(x => x.text()).catch(() => '');
      const chaves = [...String(xml).matchAll(/<Key>([^<]+)<\/Key>/g)].map(m => m[1]);
      const tams = [...String(xml).matchAll(/<Size>(\d+)<\/Size>/g)].map(m => Number(m[1]));
      arquivos = chaves.map((k, i) => ({ chave: k, name: k.split('/').pop(), size: tams[i] || null }))
        .filter(a => /\.(mp4|mov|avi|mkv|webm)$/i.test(a.name));
    } catch (e) {}
    // se a listagem não vier (bucket sem permissão de list), aceita os nomes por parâmetro
    if (!arquivos.length) {
      const nomes = String(req.query.arquivos || '').split(',').map(x => x.trim()).filter(Boolean);
      if (nomes.length) {
        arquivos = nomes.map(n => ({ chave: prefixo + n, name: n.split('/').pop(), size: null }));
      }
    }
    if (!arquivos.length) {
      return res.status(200).json({ ok: false,
        error: 'nenhum vídeo encontrado no bucket',
        bucket: R2_PUB, prefixoUsado: prefixo || '(raiz)',
        alternativa: 'se o bucket não permite listagem, passe os nomes: &arquivos=Led queimado.mov,Tela lavada.mov',
        dica: 'confira também se o "URL de desenvolvimento público" está ativado no R2' });
    }
    // 2) prévia: mostra o que será criado, com o texto de cada um
    const plano = arquivos.map(a => {
      const t = textoPorDefeito(a.name, cat);
      const catReal = categoriaDe(a.name, 'anuncio');
      return { arquivo: a.name, url: R2_PUB + '/' + a.chave.split('/').map(encodeURIComponent).join('/'),
        categoria: catReal,
        frente: catReal === 'tv' ? 'TV' : 'ADM',
        titulo: t.titulo, corpo: t.corpo,
        verba, tamanhoMB: a.size ? Math.round(Number(a.size) / 1048576) : null };
    });
    const porCat = plano.reduce((o, p) => { o[p.categoria] = (o[p.categoria] || 0) + 1; return o; }, {});
    const porFrente = plano.reduce((o, p) => { o[p.frente] = (o[p.frente] || 0) + 1; return o; }, {});
    if (String(req.query.aplicar || '') !== '1') {
      return res.status(200).json({ ok: true, modo: 'prévia — nada foi criado',
        bucket: R2_PUB, pasta: prefixo || '(raiz)', videos: plano.length,
        POR_CATEGORIA: porCat, POR_FRENTE: porFrente,
        verbaTotal: Number((verba * plano.length).toFixed(2)),
        PLANO: plano.map(p => '[' + p.categoria.toUpperCase() + '] ' + p.arquivo +
          (p.tamanhoMB ? ' (' + p.tamanhoMB + 'MB)' : '') +
          '\n   ↳ "' + p.titulo + '"\n   ↳ ' + p.corpo),
        dica: 'para criar tudo: &aplicar=1' });
    }
    // 3) sobe cada vídeo para a Meta pelo link direto do Drive
    const subidos = [], falhas = [];
    for (const p of plano) {
      const urlDireta = p.url;                              // link público direto do R2
      const up = await fetch(`${GRAPH}/act_${CONTA}/advideos?access_token=${tkP}`, {
        method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: new URLSearchParams({ file_url: urlDireta, title: p.arquivo }).toString(),
      }).then(x => x.json()).catch(e => ({ error: { message: e.message } }));
      if (up && up.error) { falhas.push(p.arquivo + ': ' + (up.error.error_user_msg || up.error.message)); continue; }
      subidos.push({ arquivo: p.arquivo, videoId: up.id, titulo: p.titulo, corpo: p.corpo });
      await new Promise(s => setTimeout(s, 800));
    }
    return res.status(200).json({ ok: falhas.length === 0,
      subidosParaMeta: subidos.length,
      videos: subidos.map(s => s.arquivo + ' → id ' + s.videoId),
      falhas,
      proximoPasso: subidos.length
        ? 'agora rode: action=subir-agora&cat=' + cat + '&verba=' + verba + '&aplicar=1 — os vídeos já estão na biblioteca da Meta e vão entrar com o texto certo'
        : 'nenhum vídeo subiu' });
  }

  // ── 📅 selo de data no nome da campanha: DDMMAAAA, para rastreabilidade ──
  // Antes o código usava '08' fixo como mês, o que funcionaria só em agosto.
  function seloData() {
    const b = new Date(Date.now() - 3 * 3600000);          // horário de Brasília
    const dd = String(b.getUTCDate()).padStart(2, '0');
    const mm = String(b.getUTCMonth() + 1).padStart(2, '0');
    return dd + mm + b.getUTCFullYear();
  }
  function nomeComData(base) {
    const limpo = String(base || '').replace(/\.(mov|mp4|avi|mkv|webm)$/i, '').trim();
    const selo = seloData();
    // não duplica se já terminar com um selo de 8 dígitos
    if (new RegExp('\\b' + selo + '$').test(limpo)) return limpo;
    return limpo.replace(/\s+\d{4,8}$/, '') + ' ' + selo;
  }

  // ── ✍️ textos por DEFEITO: cada criativo fala do problema que mostra ──
  function textoPorDefeito(nomeArquivo, categoria) {
    const s = String(nomeArquivo || '').toLowerCase().replace(/\.(mov|mp4|avi)$/i, '');
    const TV = [
      { re: /som.*(n[aã]o|sem).*(imagem|v[ií]deo)|sem imagem|n[aã]o d[aá] imagem/,
        titulo: 'Sua TV tem som mas não tem imagem?',
        corpo: 'A tela fica preta mas o som continua funcionando? Na maioria das vezes é a placa ou os LEDs — conserto rápido e com garantia. Chama no WhatsApp!' },
      { re: /led queimad|led/,
        titulo: 'LED queimado na sua TV?',
        corpo: 'Tela escura, manchada ou com faixas? Os LEDs queimam com o tempo. Fazemos a troca completa com peças originais e garantia. Fala com a gente!' },
      { re: /tela lavad|lavada|desbotad|apagad/,
        titulo: 'A imagem da sua TV ficou lavada?',
        corpo: 'Cores desbotadas, imagem clara demais ou esbranquiçada? Tem conserto — e sai bem mais em conta que uma TV nova. Chama no WhatsApp!' },
      { re: /tela azul|azulad/,
        titulo: 'Sua TV está com a tela azulada?',
        corpo: 'Sua TV perdeu o brilho? Você consegue ver a imagem lá no fundo, ou ela fica toda azul? Podemos fazer a troca dos LEDs rapidamente!' },
      { re: /n[aã]o liga|nao liga|morta/,
        titulo: 'Sua TV não liga?',
        corpo: 'Não acende nem a luzinha? Costuma ser a fonte — um dos consertos mais simples que fazemos. Orçamento após avaliação, sem compromisso.' },
      { re: /listra|risco|linha/,
        titulo: 'TV com listras ou linhas na tela?',
        corpo: 'Faixas coloridas, linhas verticais ou horizontais? É defeito conhecido e tem solução. Coletamos na sua casa e devolvemos consertada!' },
      { re: /quebrad|trincad|rachad/,
        titulo: 'Quebrou a tela da sua TV?',
        corpo: 'Antes de comprar outra, faça um orçamento com a gente. Avaliamos sem compromisso e você decide. Chama no WhatsApp!' },
      { re: /t-?con|barrament/,
        titulo: 'Imagem falhando na sua TV?',
        corpo: 'Imagem tremendo, piscando ou com defeito na placa? Consertamos com peças originais e garantia. Fala com a gente pelo WhatsApp!' },
    ];
    const MICRO = [
      { re: /n[aã]o liga.*n[aã]o esquenta|n[aã]o esquenta/,
        titulo: 'Seu micro-ondas não esquenta?',
        corpo: 'Liga, gira o prato, mas a comida sai fria? Costuma ser a válvula magnetron — trocamos com peça original e garantia. Chama no WhatsApp!' },
      { re: /enferrujad|ferrugem/,
        titulo: 'Micro-ondas enferrujado por dentro?',
        corpo: 'Ferrugem na cavidade é comum e tem conserto — fazemos o reparo e a pintura própria para micro-ondas. Bem mais barato que comprar outro!' },
      { re: /qualquer marca|todas as marcas/,
        titulo: 'Consertamos micro-ondas de qualquer marca',
        corpo: 'Brastemp, Electrolux, Consul, Philco, LG, Panasonic e outras. Orçamento após avaliação, sem compromisso. Chama no WhatsApp!' },
      { re: /hor[aá]rio de almo[cç]o|almo[cç]o/,
        titulo: 'Sem micro-ondas na hora do almoço?',
        corpo: 'A gente busca, conserta e devolve funcionando. Peças originais e garantia. Fala com a gente pelo WhatsApp!' },
      { re: /90 por cento|90%|maioria dos casos/,
        titulo: 'Vale a pena consertar seu micro-ondas?',
        corpo: 'Na maioria dos casos o conserto sai por uma fração do preço de um novo — e o seu é de linha superior. Avaliamos sem compromisso!' },
      { re: /fa[ií]sca|estala|barulho/,
        titulo: 'Micro-ondas fazendo barulho ou faísca?',
        corpo: 'Estalos e faíscas costumam ser a mica ou o prato — conserto rápido e seguro, com garantia. Chama no WhatsApp!' },
    ];
    const ADEGA = [
      { re: /volume de adega|quantas garrafa|tamanho/,
        titulo: 'Sua adega parou de gelar?',
        corpo: 'Seja de 8, 12 ou 30 garrafas, consertamos com peças originais e garantia. Coletamos na sua casa. Chama no WhatsApp!' },
      { re: /15 minutos|quinze minutos/,
        titulo: 'Conserto de adega em BH',
        corpo: 'Adega ou cervejeira que não gela, faz barulho ou não liga? Avaliamos sem compromisso e consertamos com garantia. Fala com a gente!' },
      { re: /interessante|segredo|olha (que|s[oó])/,
        titulo: 'Adega com defeito? Tem conserto',
        corpo: 'Antes de comprar outra, faça um orçamento com a gente. Peças originais, garantia e coleta na sua casa. Chama no WhatsApp!' },
      { re: /n[aã]o gela|esquentou/,
        titulo: 'Adega não está gelando?',
        corpo: 'Costuma ser o compressor ou o termostato — os dois têm conserto. Avaliamos sem compromisso e você decide. Chama no WhatsApp!' },
    ];
    const PURI = [
      { re: /beber [aá]gua|[aá]gua quente|calor/,
        titulo: 'Purificador parou de gelar?',
        corpo: 'Água saindo quente ou sem sair? Consertamos com peças originais e garantia, e ainda trocamos o filtro. Chama no WhatsApp!' },
      { re: /fecha registro|vazand|vazament|pingand/,
        titulo: 'Purificador vazando?',
        corpo: 'Vazamento costuma ser mangueira, válvula ou vedação — conserto rápido e com garantia. Coletamos na sua casa. Fala com a gente!' },
      { re: /bebedouro torre|torre|empresa/,
        titulo: 'Bebedouro da sua empresa parou?',
        corpo: 'Atendemos empresas com conserto de bebedouro torre e de coluna. Coleta, conserto e entrega, com nota fiscal. Chama no WhatsApp!' },
      { re: /tipos de bebedouro|bebedouro/,
        titulo: 'Consertamos bebedouros e purificadores',
        corpo: 'De coluna, de torre ou de bancada, qualquer marca. Peças originais e garantia. Fala com a gente pelo WhatsApp!' },
    ];
    const GENERICO = {
      tv: { titulo: 'Consertamos sua TV', corpo: 'Conserto de TV rápido em BH, com peças originais e garantia. Coletamos na sua casa e devolvemos funcionando. Chama no WhatsApp!' },
      microondas: { titulo: 'Seu micro-ondas parou?', corpo: 'Não esquenta, não liga ou faz barulho? Consertamos com peças originais e garantia. Coletamos na sua casa. Chama no WhatsApp!' },
      purificador: { titulo: 'Purificador com problema?', corpo: 'Não gela, vaza ou parou de sair água? Consertamos rápido, com garantia e coleta na sua casa. Fala com a gente!' },
      adega: { titulo: 'Sua adega parou de gelar?', corpo: 'Adega ou cervejeira com defeito? Consertamos com peças originais e garantia. Chama no WhatsApp!' },
      forno: { titulo: 'Forno elétrico com defeito?', corpo: 'Não esquenta, não liga ou o timer parou? Consertamos com peças originais e garantia. Chama no WhatsApp!' },
    };
    // 🎯 a CATEGORIA vem do nome do arquivo, não do parâmetro — os 16 vídeos de 08/08
    // eram de micro-ondas, adega e purificador, e recebiam texto de TV
    const cat2 = categoriaDe(s, 'anuncio');
    if (cat2 === 'tv') { for (const t of TV) if (t.re.test(s)) return { titulo: t.titulo, corpo: t.corpo }; return GENERICO.tv; }
    if (cat2 === 'microondas') { for (const t of MICRO) if (t.re.test(s)) return { titulo: t.titulo, corpo: t.corpo }; return GENERICO.microondas; }
    if (cat2 === 'adega') { for (const t of ADEGA) if (t.re.test(s)) return { titulo: t.titulo, corpo: t.corpo }; return GENERICO.adega; }
    if (cat2 === 'purificador') { for (const t of PURI) if (t.re.test(s)) return { titulo: t.titulo, corpo: t.corpo }; return GENERICO.purificador; }
    if (cat2 === 'forno') return GENERICO.forno;
    return GENERICO[categoria] || GENERICO.microondas;
  }

  // ── ✍️ TEXTOS-CRIATIVOS: compara e replica os textos dos campeões nos novos ──
  if (action === 'textos-criativos') {
    if (!CONTA) return res.status(200).json({ ok: false, error: 'conta não configurada' });
    const tkT = String(req.query.token || '').trim() || TOKEN;
    const cat = String(req.query.cat || 'tv').toLowerCase();
    const ads = await pegarTudo(`${GRAPH}/act_${CONTA}/ads?fields=id,name,effective_status,creative{id,object_story_spec,title,body,call_to_action_type},campaign{id,name,start_time}&limit=300&access_token=${tkT}`, 8);
    const desdeC = (function () {
      const b = new Date(Date.now() - 3 * 3600000);
      const d = new Date(b); d.setUTCDate(b.getUTCDate() - ((b.getUTCDay() + 1) % 7));
      return d.toISOString().slice(0, 10);
    })();
    const doCiclo = (ads.data || []).filter(a => {
      const c = a.campaign || {};
      if (String(c.start_time || '').slice(0, 10) < desdeC) return false;
      return categoriaDe(a.name || c.name || '', 'anuncio') === cat;
    });
    const lidos = doCiclo.map(a => {
      const cr = a.creative || {};
      const oss = cr.object_story_spec || {};
      const vd = oss.video_data || {};
      const ld = oss.link_data || {};
      const cta = vd.call_to_action || ld.call_to_action || {};
      return {
        anuncio: a.name, id: a.id, criativoId: cr.id,
        situacao: a.effective_status,
        titulo: vd.title || ld.name || cr.title || null,
        corpo: vd.message || ld.message || cr.body || null,
        descricao: vd.link_description || ld.description || null,
        botao: cta.type || cr.call_to_action_type || null,
        destino: (cta.value && (cta.value.link || cta.value.whatsapp_number)) || null,
        pageId: oss.page_id || null,
      };
    });
    const completos = lidos.filter(x => x.corpo && x.titulo);
    const vazios = lidos.filter(x => !x.corpo || !x.titulo);
    const modelo = completos.sort((a, b) => (b.corpo || '').length - (a.corpo || '').length)[0] || null;

    if (String(req.query.aplicar || '') !== '1') {
      return res.status(200).json({ ok: true, categoria: cat,
        totalNoCiclo: lidos.length,
        comTexto: completos.length, semTexto: vazios.length,
        MODELO: modelo ? { anuncio: modelo.anuncio, titulo: modelo.titulo,
          corpo: modelo.corpo, descricao: modelo.descricao, botao: modelo.botao } : null,
        SEM_TEXTO: vazios.map(v => {
          const t = textoPorDefeito(v.anuncio, cat);
          return v.anuncio + '\n     ↳ título: "' + (t ? t.titulo : '?') + '"\n     ↳ corpo: "' +
            (t ? t.corpo : '?') + '"';
        }),
        observacao: 'cada criativo recebe o texto do DEFEITO que ele mostra, não uma cópia do campeão',
        detalhe: lidos,
        dica: modelo && vazios.length
          ? 'para replicar o texto do modelo nos vazios: &aplicar=1'
          : (vazios.length ? 'nenhum criativo completo para servir de modelo' : 'todos já têm texto') });
    }
    if (!modelo) return res.status(200).json({ ok: false, error: 'nenhum criativo com texto para servir de modelo' });

    const feitos = [], erros = [];
    for (const v of vazios) {
      // lê o criativo atual para preservar o vídeo
      const at = await fetch(`${GRAPH}/${v.criativoId}?fields=object_story_spec&access_token=${tkT}`)
        .then(x => x.json()).catch(e => ({ error: { message: e.message } }));
      if (at && at.error) { erros.push(v.anuncio + ': ' + at.error.message); continue; }
      const oss = (at.object_story_spec) || {};
      if (!oss.video_data) { erros.push(v.anuncio + ': criativo sem vídeo — não mexi'); continue; }
      const novoOss = { page_id: oss.page_id, video_data: { ...oss.video_data } };
      // usa o texto do DEFEITO que o vídeo mostra — copiar o texto do campeão
      // colocaria "tela azulada" num anúncio de "sai som", derrubando o desempenho
      const txtD = textoPorDefeito(v.anuncio, cat);
      novoOss.video_data.title = (txtD && txtD.titulo) || modelo.titulo;
      novoOss.video_data.message = (txtD && txtD.corpo) || modelo.corpo;
      if (modelo.descricao) novoOss.video_data.link_description = modelo.descricao;
      delete novoOss.video_data.image_url;
      // cria um criativo novo com o mesmo vídeo e o texto do modelo
      const cr = await fetch(`${GRAPH}/act_${CONTA}/adcreatives?access_token=${tkT}`, {
        method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: new URLSearchParams({ name: v.anuncio + ' - criativo com texto',
          object_story_spec: JSON.stringify(novoOss) }).toString(),
      }).then(x => x.json()).catch(e => ({ error: { message: e.message } }));
      if (cr && cr.error) { erros.push(v.anuncio + ' (criar criativo): ' + cr.error.message); continue; }
      const up = await fetch(`${GRAPH}/${v.id}?access_token=${tkT}`, {
        method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: new URLSearchParams({ creative: JSON.stringify({ creative_id: cr.id }) }).toString(),
      }).then(x => x.json()).catch(e => ({ error: { message: e.message } }));
      if (up && up.error) erros.push(v.anuncio + ' (aplicar): ' + up.error.message);
      else feitos.push(v.anuncio + ' → texto do modelo aplicado');
      await new Promise(s => setTimeout(s, 400));
    }
    return res.status(200).json({ ok: erros.length === 0,
      modeloUsado: modelo.anuncio, aplicados: feitos.length, feitos, erros });
  }

  // ── 🔬 AUDITORIA-CICLO: puxa TUDO da Meta e cruza com o que cada painel mostra ──
  if (action === 'auditoria-ciclo') {
    if (!CONTA) return res.status(200).json({ ok: false, error: 'conta não configurada' });
    const cfgA = await cfgTrafego();
    const desde = inicioCiclo(cfgA);
    const ate = new Date(Date.now() - 3 * 3600000).toISOString().slice(0, 10);

    // ── 1) FONTE PRIMÁRIA: campanhas, conjuntos, anúncios e gastos ──
    const [camps, sets, ads] = await Promise.all([
      pegarTudo(`${GRAPH}/act_${CONTA}/campaigns?fields=id,name,effective_status,daily_budget,lifetime_budget,start_time,stop_time&limit=300&access_token=${TOKEN}`, 10),
      pegarTudo(`${GRAPH}/act_${CONTA}/adsets?fields=id,name,effective_status,daily_budget,lifetime_budget,end_time,campaign{id}&limit=300&access_token=${TOKEN}`, 10),
      pegarTudo(`${GRAPH}/act_${CONTA}/ads?fields=id,name,effective_status,adset{id},campaign{id}&limit=400&access_token=${TOKEN}`, 12),
    ]);
    const jn = 'time_range=' + encodeURIComponent(JSON.stringify({ since: desde, until: ate }));
    const insC = await pegarTudo(`${GRAPH}/act_${CONTA}/insights?level=campaign&${jn}&fields=campaign_id,campaign_name,spend&limit=300&access_token=${TOKEN}`, 10);
    const gastoCamp = {};
    for (const i of (insC.data || [])) gastoCamp[i.campaign_id] = Number(i.spend || 0);

    const setsDe = {};
    for (const s of (sets.data || [])) { const ci = (s.campaign || {}).id; if (ci) (setsDe[ci] = setsDe[ci] || []).push(s); }
    const adsDe = {};
    for (const a of (ads.data || [])) { const ci = (a.campaign || {}).id; if (ci) (adsDe[ci] = adsDe[ci] || []).push(a); }

    // ── 2) CAMPANHAS DO CICLO ──
    const doCiclo = (camps.data || []).filter(c => String(c.start_time || '').slice(0, 10) >= desde);
    const tabela = doCiclo.map(c => {
      let verba = 0, onde = null;
      if (c.lifetime_budget) { verba = Number(c.lifetime_budget) / 100; onde = 'campanha(total)'; }
      else if (c.daily_budget) { verba = Number(c.daily_budget) / 100; onde = 'campanha(diária)'; }
      else {
        for (const s of (setsDe[c.id] || [])) {
          if (s.lifetime_budget) { verba += Number(s.lifetime_budget) / 100; onde = 'conjunto(total)'; }
          else if (s.daily_budget) { verba += Number(s.daily_budget) / 100; onde = 'conjunto(diária)'; }
        }
      }
      const g = Number((gastoCamp[c.id] || 0).toFixed(2));
      const adsAtivos = (adsDe[c.id] || []).filter(a => a.effective_status === 'ACTIVE').length;
      return { campanha: c.name, id: c.id,
        categoria: categoriaDe(c.name || '', 'anuncio'),
        frente: categoriaDe(c.name || '', 'anuncio') === 'tv' ? 'TV' : 'ADM',
        situacao: c.effective_status,
        ativa: c.effective_status === 'ACTIVE',
        verba: Number(verba.toFixed(2)), verbaEm: onde,
        gasto: g, falta: Number(Math.max(0, verba - g).toFixed(2)),
        anunciosAtivos: adsAtivos,
        inicio: String(c.start_time || '').slice(0, 10),
        fim: c.stop_time ? String(c.stop_time).slice(0, 16).replace('T', ' ') : null };
    }).sort((a, b) => b.verba - a.verba);

    const ativas = tabela.filter(t => t.ativa);
    const pausadas = tabela.filter(t => !t.ativa);
    const som = (arr, k) => Number(arr.reduce((s, x) => s + (x[k] || 0), 0).toFixed(2));
    const porFrente = (f) => {
      const a = ativas.filter(x => x.frente === f);
      return { campanhas: a.length, verba: som(a, 'verba'), gasto: som(a, 'gasto'), falta: som(a, 'falta') };
    };

    // ── 3) O QUE O PAINEL/CACHE ESTÁ MOSTRANDO ──
    const cache = await dbGet('trafego_painel_cache_ciclo');
    const doCache = ((cache || {}).dados) || null;
    const cacheDiz = doCache ? {
      geradoEm: cache.em || null,
      anunciosExibidos: doCache.exibidos ?? doCache.totalAnuncios,
      gastoTotal: (doCache.totais || {}).gasto,
      metaVerbaAdmGasto: ((doCache.metaVerba || {}).adm || {}).gasto,
      metaVerbaTvGasto: ((doCache.metaVerba || {}).tv || {}).gasto,
      verbaAlocadaReal: doCache.verbaAlocadaReal || null,
      blocoVerbaLegado: { adm: ((doCache.verba || {}).adm || {}).gasto, tv: ((doCache.verba || {}).tv || {}).gasto },
      motivosCorte: doCache.motivosCorte || null,
    } : 'sem cache';

    // ── 4) CRUZAMENTO ──
    const verdade = {
      admVerba: porFrente('ADM').verba, admGasto: porFrente('ADM').gasto,
      tvVerba: porFrente('TV').verba, tvGasto: porFrente('TV').gasto,
      gastoTotalCiclo: Number((porFrente('ADM').gasto + porFrente('TV').gasto).toFixed(2)),
    };
    const divergencias = [];
    if (doCache) {
      const cmp = (rot, valorPainel, valorReal) => {
        if (valorPainel == null) { divergencias.push(rot + ': painel não informa'); return; }
        const d = Number((valorPainel - valorReal).toFixed(2));
        if (Math.abs(d) > 1) divergencias.push(rot + ': painel ' + valorPainel + ' × real ' + valorReal + ' → diferença ' + (d > 0 ? '+' : '') + d);
      };
      cmp('gasto total do ciclo', cacheDiz.gastoTotal, verdade.gastoTotalCiclo);
      cmp('gasto ADM', cacheDiz.metaVerbaAdmGasto, verdade.admGasto);
      cmp('gasto TV', cacheDiz.metaVerbaTvGasto, verdade.tvGasto);
      if (cacheDiz.verbaAlocadaReal) {
        cmp('verba alocada ADM', cacheDiz.verbaAlocadaReal.adm, verdade.admVerba);
        cmp('verba alocada TV', cacheDiz.verbaAlocadaReal.tv, verdade.tvVerba);
      }
      cmp('bloco VERBA legado (ADM)', cacheDiz.blocoVerbaLegado.adm, verdade.admGasto);
    }

    return res.status(200).json({ ok: divergencias.length === 0,
      ciclo: { desde, ate },
      VERDADE_DA_META: {
        ADM: porFrente('ADM'), TV: porFrente('TV'),
        totalAtivas: ativas.length, totalPausadas: pausadas.length,
        verbaTotalAtivas: som(ativas, 'verba'),
        gastoTotalAtivas: som(ativas, 'gasto'),
        faltaGastar: som(ativas, 'falta'),
        pausadas: { verba: som(pausadas, 'verba'), gasto: som(pausadas, 'gasto') },
      },
      O_QUE_O_PAINEL_MOSTRA: cacheDiz,
      DIVERGENCIAS: divergencias.length ? divergencias : '✅ nenhuma',
      TABELA_ATIVAS: ativas.map(t => t.frente + ' | ' + String(t.campanha).slice(0, 32).padEnd(32) +
        ' | verba ' + String(t.verba).padStart(8) + ' | gasto ' + String(t.gasto).padStart(8) +
        ' | falta ' + String(t.falta).padStart(8) + ' | ' + t.verbaEm + ' | ' + t.anunciosAtivos + ' anúncio(s)'),
      TABELA_PAUSADAS: pausadas.map(t => t.frente + ' | ' + String(t.campanha).slice(0, 32) +
        ' | verba ' + t.verba + ' | gasto ' + t.gasto + ' | ' + t.situacao),
      detalhe: tabela });
  }

  // ── 🎯 AJUSTAR-TETO: redistribui a verba de uma frente para fechar num teto exato ──
  if (action === 'ajustar-teto') {
    if (!CONTA) return res.status(200).json({ ok: false, error: 'conta não configurada' });
    const frente = String(req.query.frente || 'adm').toLowerCase();      // adm | tv
    const teto = parseFloat(req.query.teto || '2500');
    if (!(teto > 0)) return res.status(400).json({ ok: false, error: 'teto inválido' });

    const [camps, sets] = await Promise.all([
      pegarTudo(`${GRAPH}/act_${CONTA}/campaigns?fields=id,name,effective_status,daily_budget,lifetime_budget,start_time&limit=200&access_token=${TOKEN}`, 8),
      pegarTudo(`${GRAPH}/act_${CONTA}/adsets?fields=id,name,effective_status,daily_budget,lifetime_budget,campaign{id}&limit=200&access_token=${TOKEN}`, 8),
    ]);
    const setsDe = {};
    for (const s of (sets.data || [])) { const ci = (s.campaign || {}).id; if (ci) (setsDe[ci] = setsDe[ci] || []).push(s); }
    const verbaDe = (c) => {
      if (c.lifetime_budget) return { v: Number(c.lifetime_budget) / 100, alvo: c.id, campo: 'lifetime_budget', onde: 'campanha' };
      if (c.daily_budget) return { v: Number(c.daily_budget) / 100, alvo: c.id, campo: 'daily_budget', onde: 'campanha' };
      for (const s of (setsDe[c.id] || [])) {
        if (s.lifetime_budget) return { v: Number(s.lifetime_budget) / 100, alvo: s.id, campo: 'lifetime_budget', onde: 'conjunto' };
        if (s.daily_budget) return { v: Number(s.daily_budget) / 100, alvo: s.id, campo: 'daily_budget', onde: 'conjunto' };
      }
      return { v: 0, alvo: null, campo: null, onde: null };
    };
    const desdeCiclo = (function () {
      const b = new Date(Date.now() - 3 * 3600000);
      const d = new Date(b); d.setUTCDate(b.getUTCDate() - ((b.getUTCDay() + 1) % 7));
      return d.toISOString().slice(0, 10);
    })();
    const gastos = {};
    try {
      const jn = 'time_range=' + encodeURIComponent(JSON.stringify({ since: desdeCiclo, until: new Date(Date.now() - 3 * 3600000).toISOString().slice(0, 10) }));
      const ins = await pegarTudo(`${GRAPH}/act_${CONTA}/insights?level=campaign&${jn}&fields=campaign_id,spend&limit=200&access_token=${TOKEN}`, 6);
      for (const i of (ins.data || [])) gastos[i.campaign_id] = Number(i.spend || 0);
    } catch (e) {}

    // desempenho por campanha (para pesar a distribuição)
    const base = await dbGet('trafego_painel_cache_ciclo') || await dbGet('trafego_painel_cache');
    const perf = {};
    for (const a of (((base || {}).dados || {}).anuncios || [])) {
      if (a.campanhaId) perf[a.campanhaId] = { cpa: a.cpa, conversas: a.conversas || 0, categoria: a.categoria };
    }

    const doCiclo = (camps.data || []).filter(c => String(c.start_time || '').slice(0, 10) >= desdeCiclo);
    const daFrente = doCiclo.filter(c => {
      const cat = categoriaDe(c.name || '', 'anuncio');
      return frente === 'tv' ? cat === 'tv' : cat !== 'tv';
    });
    const ativos = [], pausados = [];
    for (const c of daFrente) {
      const vb = verbaDe(c);
      const g = Number((gastos[c.id] || 0).toFixed(2));
      const item = { nome: c.name, id: c.id, categoria: categoriaDe(c.name || '', 'anuncio'),
        verbaAtual: vb.v, gasto: g, alvoId: vb.alvo, campo: vb.campo, onde: vb.onde,
        cpa: (perf[c.id] || {}).cpa ?? null, conversas: (perf[c.id] || {}).conversas ?? 0 };
      (c.effective_status === 'ACTIVE' ? ativos : pausados).push(item);
    }
    if (!ativos.length) return res.status(200).json({ ok: false, error: 'nenhuma campanha ativa em ' + frente });

    // 💰 o que já foi gasto nos PAUSADOS não volta — desconta do teto
    const gastoPausados = Number(pausados.reduce((s, p) => s + p.gasto, 0).toFixed(2));
    const gastoAtivos = Number(ativos.reduce((s, a) => s + a.gasto, 0).toFixed(2));
    const disponivel = Number((teto - gastoPausados).toFixed(2));   // teto para os ativos
    if (disponivel < gastoAtivos) {
      return res.status(200).json({ ok: false,
        error: 'o teto é menor do que já foi gasto',
        teto, gastoAtivos, gastoPausados,
        explicacao: 'não dá para reduzir abaixo do que os anúncios já consumiram' });
    }
    // sobra a distribuir por desempenho, respeitando o já gasto como piso
    const aDistribuir = Number((disponivel - gastoAtivos).toFixed(2));
    const cfgT = await cfgTrafego();
    const peso = a => {
      const meta = cfgT.metas[a.categoria] != null ? cfgT.metas[a.categoria] : cfgT.metas.outros;
      if (a.cpa == null || a.conversas < 1) return 0.5;           // sem dado: peso baixo
      return (meta / Math.max(0.2, a.cpa)) * Math.log10(10 + a.conversas);
    };
    const somaPesos = ativos.reduce((s, a) => s + peso(a), 0) || 1;
    const plano = ativos.map(a => {
      const fatia = Number((aDistribuir * peso(a) / somaPesos).toFixed(2));
      const nova = Number((a.gasto + fatia).toFixed(2));
      return { ...a, verbaNova: nova, correcao: Number((nova - a.verbaAtual).toFixed(2)) };
    }).sort((x, y) => y.verbaNova - x.verbaNova);

    const somaNova = Number(plano.reduce((s, p) => s + p.verbaNova, 0).toFixed(2));
    if (String(req.query.aplicar || '') !== '1') {
      return res.status(200).json({ ok: true, modo: 'prévia', frente: frente.toUpperCase(), teto,
        CONFERENCIA: {
          verbaAtualNosAtivos: Number(ativos.reduce((s, a) => s + a.verbaAtual, 0).toFixed(2)),
          jaGastoNosAtivos: gastoAtivos,
          jaGastoNosPausados: gastoPausados,
          disponivelParaOsAtivos: disponivel,
          somaDepoisDoAjuste: somaNova,
          totalFinal: Number((somaNova + gastoPausados).toFixed(2)),
          bate: Math.abs(somaNova + gastoPausados - teto) < 1 ? '✅ fecha no teto' : '⚠️ diferença',
        },
        PLANO: plano.map(p => String(p.nome).slice(0, 30).padEnd(30) +
          ' | gasto ' + String(p.gasto).padStart(7) +
          ' | de ' + String(p.verbaAtual).padStart(7) +
          ' → ' + String(p.verbaNova).padStart(7) +
          ' | ' + (p.correcao >= 0 ? '+' : '') + p.correcao +
          (p.cpa != null ? ' | CPA ' + p.cpa : '')),
        pausados: pausados.map(p => String(p.nome).slice(0, 30) + ' | alocado ' + p.verbaAtual + ' · gasto ' + p.gasto),
        detalhe: plano,
        dica: 'para aplicar: &aplicar=1' });
    }
    // aplica
    const feitos = [], erros = [];
    const postF = async (id, campos) => fetch(`${GRAPH}/${id}?access_token=${TOKEN}`, { method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body: new URLSearchParams(campos).toString() })
      .then(x => x.json()).catch(e => ({ error: { message: e.message } }));
    for (const p of plano) {
      if (Math.abs(p.correcao) < 0.5) continue;                   // sem mudança relevante
      const centavos = Math.round(p.verbaNova * 100);
      let r = await postF(p.alvoId, { [p.campo]: String(centavos) });
      if (r && r.error) {
        const outro = p.onde === 'conjunto' ? p.id : (setsDe[p.id] || [])[0]?.id;
        if (outro) { const r2 = await postF(outro, { [p.campo]: String(centavos) }); if (!(r2 && r2.error)) r = r2; }
      }
      if (r && r.error) erros.push(p.nome + ': ' + r.error.message);
      else feitos.push({ id: p.alvoId, nome: p.nome, acao: 'verba → R$ ' + p.verbaNova.toFixed(2) });
      await new Promise(s => setTimeout(s, 150));
    }
    try {
      const lg = (await dbGet('trafego_log')) || { movs: [] };
      lg.movs.unshift({ ts: new Date().toISOString(), acao: 'ajustar-teto ' + frente + ' R$' + teto, feitos, erros });
      await dbSet('trafego_log', lg);
      const KRT = (process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim();
      for (const pp of ['ciclo', '7d']) fetch(`https://reparoeletroadm.com/api/trafego?action=painel&periodo=${pp}&forcar=1&k=${KRT}`).catch(() => {});
    } catch (e) {}
    return res.status(200).json({ ok: erros.length === 0, frente: frente.toUpperCase(), teto,
      aplicados: feitos.length, totalFinal: Number((somaNova + gastoPausados).toFixed(2)),
      feitos: feitos.map(f => f.nome + ' → ' + f.acao), erros });
  }

  // ── 💸 REALOCAR-ORFA: aplica a redistribuição da verba presa nos pausados ──
  if (action === 'realocar-orfa') {
    const KRO = (process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim();
    const diag = await fetch(`https://reparoeletroadm.com/api/trafego?action=verba-orfa&k=${KRO}`)
      .then(x => x.json()).catch(e => ({ ok: false, error: e.message }));
    if (!diag || !diag.ok) return res.status(200).json({ ok: false, error: 'não consegui calcular a verba órfã' });
    const alvos = [];
    for (const p of (diag.propostas || [])) {
      for (const d of (p.destinos || [])) {
        if (d.receber > 0) alvos.push({ ...d, categoria: p.categoria });
      }
    }
    if (!alvos.length) return res.status(200).json({ ok: true, nada: true, msg: 'nenhuma verba órfã para realocar' });
    if (String(req.query.aplicar || '') !== '1') {
      return res.status(200).json({ ok: true, modo: 'prévia', total: alvos.length,
        verbaRealocada: Number(alvos.reduce((s, a) => s + a.receber, 0).toFixed(2)),
        lista: alvos.map(a => a.categoria.toUpperCase().slice(0, 4) + ' | ' + String(a.nome).slice(0, 28) +
          ' | R$ ' + a.verbaAtual + ' → R$ ' + a.verbaNova + ' (+' + a.receber + ')'),
        dica: 'para aplicar: &aplicar=1' });
    }
    const feitos = [], erros = [];
    const postMetaO = async (id, campos) => {
      const corpo = new URLSearchParams(campos).toString();
      return fetch(`${GRAPH}/${id}?access_token=${TOKEN}`, { method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body: corpo })
        .then(x => x.json()).catch(e => ({ error: { message: e.message } }));
    };
    // ⚖️ SOMA ZERO: antes de dar a verba aos ativos, REDUZIR a dos pausados para o já gasto.
    // Sem isso a verba total inflava — o pausado continuava com o valor cheio e os ativos
    // recebiam a sobra por cima, criando dinheiro que não existia.
    try {
      const diagP = await fetch(`https://reparoeletroadm.com/api/trafego?action=verba-orfa&k=${KRO}`)
        .then(x => x.json()).catch(() => null);
      for (const linha of ((diagP && diagP.pausados) || [])) {
        // recalcula pelo próprio diagnóstico: alocado e gasto vêm no texto
        const m = String(linha).match(/alocado R\$ ([\d.]+) · gasto R\$ ([\d.]+)/);
        if (!m) continue;
      }
      // usa o detalhe estruturado, mais confiável que o texto
      const camps2 = await pegarTudo(`${GRAPH}/act_${CONTA}/campaigns?fields=id,name,effective_status,daily_budget,lifetime_budget,start_time&limit=200&access_token=${TOKEN}`, 6);
      const sets2 = await pegarTudo(`${GRAPH}/act_${CONTA}/adsets?fields=id,effective_status,daily_budget,lifetime_budget,campaign{id}&limit=200&access_token=${TOKEN}`, 6);
      const setsP = {};
      for (const s of (sets2.data || [])) { const ci = (s.campaign || {}).id; if (ci) (setsP[ci] = setsP[ci] || []).push(s); }
      const desdeP = (function () {
        const b = new Date(Date.now() - 3 * 3600000);
        const d = new Date(b); d.setUTCDate(b.getUTCDate() - ((b.getUTCDay() + 1) % 7));
        return d.toISOString().slice(0, 10);
      })();
      const jnP = 'time_range=' + encodeURIComponent(JSON.stringify({ since: desdeP, until: new Date(Date.now() - 3 * 3600000).toISOString().slice(0, 10) }));
      const insP = await pegarTudo(`${GRAPH}/act_${CONTA}/insights?level=campaign&${jnP}&fields=campaign_id,spend&limit=200&access_token=${TOKEN}`, 6);
      const gastoP = {};
      for (const i of (insP.data || [])) gastoP[i.campaign_id] = Number(i.spend || 0);
      for (const c of (camps2.data || [])) {
        if (c.effective_status === 'ACTIVE') continue;
        if (String(c.start_time || '').slice(0, 10) < desdeP) continue;
        const g = Number((gastoP[c.id] || 0).toFixed(2));
        let alvoP = null, campoP = null, atualP = 0;
        if (c.lifetime_budget) { alvoP = c.id; campoP = 'lifetime_budget'; atualP = Number(c.lifetime_budget) / 100; }
        else if (c.daily_budget) { alvoP = c.id; campoP = 'daily_budget'; atualP = Number(c.daily_budget) / 100; }
        else for (const s of (setsP[c.id] || [])) {
          if (s.lifetime_budget) { alvoP = s.id; campoP = 'lifetime_budget'; atualP = Number(s.lifetime_budget) / 100; break; }
          if (s.daily_budget) { alvoP = s.id; campoP = 'daily_budget'; atualP = Number(s.daily_budget) / 100; break; }
        }
        // reduz para o já gasto (mínimo aceito pela Meta), liberando a sobra de verdade
        const novo = Math.max(g, 1);
        if (alvoP && atualP > novo + 0.5) {
          const rr = await postMetaO(alvoP, { [campoP]: String(Math.round(novo * 100)) });
          if (rr && rr.error) erros.push('reduzir pausado ' + c.name + ': ' + rr.error.message);
          else feitos.push({ id: alvoP, nome: c.name, acao: 'pausado reduzido para R$ ' + novo.toFixed(2) });
          await new Promise(s => setTimeout(s, 120));
        }
      }
    } catch (e) {}
    for (const a of alvos) {
      const centavos = Math.round(a.verbaNova * 100);
      let alvo = a.alvoId, r = await postMetaO(alvo, { [a.campo]: String(centavos) });
      if (r && r.error && a.campanhaId && String(alvo) !== String(a.campanhaId)) {
        const r2 = await postMetaO(a.campanhaId, { [a.campo]: String(centavos) });
        if (!(r2 && r2.error)) { r = r2; alvo = a.campanhaId; }
      }
      if (r && r.error) erros.push({ id: alvo, nome: a.nome, erro: r.error.message });
      else feitos.push({ id: alvo, nome: a.nome, acao: a.campo + ' → R$ ' + a.verbaNova.toFixed(2) });
      await new Promise(s => setTimeout(s, 150));
    }
    try {
      const lg = (await dbGet('trafego_log')) || { movs: [] };
      lg.movs.unshift({ ts: new Date().toISOString(), acao: 'realocar-orfa', feitos, erros });
      lg.movs = lg.movs.slice(0, 200);
      await dbSet('trafego_log', lg);
      for (const p of ['hoje', '7d', 'ciclo']) await dbSet('trafego_painel_cache_' + p, null);
    } catch (e) {}
    return res.status(200).json({ ok: erros.length === 0, aplicados: feitos.length, feitos, erros });
  }

  // ── 💸 VERBA-ORFA: o que foi pausado e não voltou para a operação ──
  if (action === 'verba-orfa') {
    if (!CONTA) return res.status(200).json({ ok: false, error: 'conta não configurada' });
    const [camps, sets] = await Promise.all([
      pegarTudo(`${GRAPH}/act_${CONTA}/campaigns?fields=id,name,effective_status,daily_budget,lifetime_budget,start_time&limit=200&access_token=${TOKEN}`, 8),
      pegarTudo(`${GRAPH}/act_${CONTA}/adsets?fields=id,name,effective_status,daily_budget,lifetime_budget,campaign{id}&limit=200&access_token=${TOKEN}`, 8),
    ]);
    const setsDe = {};
    for (const s of (sets.data || [])) { const cid = (s.campaign || {}).id; if (cid) (setsDe[cid] = setsDe[cid] || []).push(s); }
    const verbaDe = (c) => {
      if (c.lifetime_budget) return { v: Number(c.lifetime_budget) / 100, onde: 'campanha', alvo: c.id, campo: 'lifetime_budget' };
      if (c.daily_budget) return { v: Number(c.daily_budget) / 100, onde: 'campanha', alvo: c.id, campo: 'daily_budget' };
      for (const s of (setsDe[c.id] || [])) {
        if (s.lifetime_budget) return { v: Number(s.lifetime_budget) / 100, onde: 'conjunto', alvo: s.id, campo: 'lifetime_budget' };
        if (s.daily_budget) return { v: Number(s.daily_budget) / 100, onde: 'conjunto', alvo: s.id, campo: 'daily_budget' };
      }
      return { v: 0, onde: null, alvo: null, campo: null };
    };
    // gasto por campanha no ciclo (a verba já consumida não pode ser realocada)
    const desdeCiclo = (function () {
      const b = new Date(Date.now() - 3 * 3600000);
      const d = new Date(b); d.setUTCDate(b.getUTCDate() - ((b.getUTCDay() + 1) % 7));
      return d.toISOString().slice(0, 10);
    })();
    const gastos = {};
    try {
      const jn = 'time_range=' + encodeURIComponent(JSON.stringify({ since: desdeCiclo, until: new Date(Date.now() - 3 * 3600000).toISOString().slice(0, 10) }));
      const ins = await pegarTudo(`${GRAPH}/act_${CONTA}/insights?level=campaign&${jn}&fields=campaign_id,spend&limit=200&access_token=${TOKEN}`, 6);
      for (const i of (ins.data || [])) gastos[i.campaign_id] = Number(i.spend || 0);
    } catch (e) {}

    const doCiclo = (camps.data || []).filter(c => String(c.start_time || '').slice(0, 10) >= desdeCiclo);
    const pausadas = [], ativas = [];
    for (const c of doCiclo) {
      const vb = verbaDe(c);
      const gasto = Number((gastos[c.id] || 0).toFixed(2));
      const item = { nome: c.name, id: c.id, categoria: categoriaDe(c.name || '', 'anuncio'),
        verba: vb.v, verbaEm: vb.onde, alvoId: vb.alvo, campo: vb.campo,
        gasto, sobrou: Number(Math.max(0, vb.v - gasto).toFixed(2)) };
      (c.effective_status === 'ACTIVE' ? ativas : pausadas).push(item);
    }

    // por categoria: quanto sobrou nos pausados e para quem vai
    const cfgO = await cfgTrafego();
    const base = await dbGet('trafego_painel_cache_ciclo') || await dbGet('trafego_painel_cache');
    const cpaDe = {};
    for (const a of (((base || {}).dados || {}).anuncios || [])) {
      if (a.campanhaId && a.cpa != null) cpaDe[a.campanhaId] = { cpa: a.cpa, conversas: a.conversas, nome: a.nome };
    }
    const propostas = [];
    const cats = [...new Set(pausadas.map(p => p.categoria))];
    for (const cat of cats) {
      const orfa = Number(pausadas.filter(p => p.categoria === cat)
        .reduce((s, p) => s + p.sobrou, 0).toFixed(2));
      if (orfa < 1) continue;
      const meta = cfgO.metas[cat] != null ? cfgO.metas[cat] : cfgO.metas.outros;
      // destinos: ativos da MESMA categoria, dentro da meta, priorizando menor CPA
      const destinos = ativas.filter(a => a.categoria === cat)
        .map(a => ({ ...a, cpa: (cpaDe[a.id] || {}).cpa ?? null, conversas: (cpaDe[a.id] || {}).conversas ?? 0 }))
        .filter(a => a.cpa != null && a.cpa <= meta && a.conversas >= 3)
        .sort((x, y) => x.cpa - y.cpa);
      if (!destinos.length) {
        propostas.push({ categoria: cat, verbaOrfa: orfa, destinos: [],
          aviso: '⚠️ nenhum destino dentro da meta nesta categoria' });
        continue;
      }
      const peso = a => (meta / Math.max(0.2, a.cpa)) * Math.log10(10 + a.conversas);
      const soma = destinos.reduce((s, a) => s + peso(a), 0) || 1;
      propostas.push({ categoria: cat, verbaOrfa: orfa, meta,
        destinos: destinos.map(a => {
          const fatia = Number((orfa * peso(a) / soma).toFixed(2));
          return { nome: a.nome, alvoId: a.alvoId, campanhaId: a.id, campo: a.campo,
            verbaEm: a.verbaEm, cpa: a.cpa, conversas: a.conversas,
            verbaAtual: a.verba, receber: fatia,
            verbaNova: Number((a.verba + fatia).toFixed(2)) };
        }) });
    }
    const totalOrfa = Number(propostas.reduce((s, p) => s + p.verbaOrfa, 0).toFixed(2));
    return res.status(200).json({ ok: true, cicloDesde: desdeCiclo,
      resumo: {
        campanhasPausadas: pausadas.length,
        verbaAlocadaNosPausados: Number(pausadas.reduce((s, p) => s + p.verba, 0).toFixed(2)),
        jaGastoAntesDePausar: Number(pausadas.reduce((s, p) => s + p.gasto, 0).toFixed(2)),
        VERBA_ORFA: totalOrfa,
      },
      pausados: pausadas.map(p => (p.categoria || '?').toUpperCase().slice(0, 4) + ' | ' +
        String(p.nome).slice(0, 30) + ' | alocado R$ ' + p.verba + ' · gasto R$ ' + p.gasto +
        ' · SOBROU R$ ' + p.sobrou),
      propostas,
      comoAplicar: 'confira as propostas e aplique pelo Copiloto, ou pelo link realocar-orfa&aplicar=1' });
  }

  // ── 📋 EXTRATO-VERBA: todos os anúncios do ciclo, verba e situação, somados por frente ──
  if (action === 'extrato-verba') {
    if (!CONTA) return res.status(200).json({ ok: false, error: 'conta não configurada' });
    const brt = d => d ? new Date(new Date(d).getTime() - 3 * 3600000).toISOString().slice(0, 16).replace('T', ' ') : null;
    const [camps, sets, ads] = await Promise.all([
      pegarTudo(`${GRAPH}/act_${CONTA}/campaigns?fields=id,name,effective_status,daily_budget,lifetime_budget,start_time,stop_time&limit=200&access_token=${TOKEN}`, 8),
      pegarTudo(`${GRAPH}/act_${CONTA}/adsets?fields=id,name,effective_status,daily_budget,lifetime_budget,end_time,campaign{id}&limit=200&access_token=${TOKEN}`, 8),
      pegarTudo(`${GRAPH}/act_${CONTA}/ads?fields=id,name,effective_status,adset{id},campaign{id}&limit=300&access_token=${TOKEN}`, 10),
    ]);
    const porCamp = {}; for (const c of (camps.data || [])) porCamp[c.id] = c;
    const setsDe = {};
    for (const s of (sets.data || [])) { const cid = (s.campaign || {}).id; if (cid) (setsDe[cid] = setsDe[cid] || []).push(s); }

    // gasto do ciclo por campanha
    const desdeCiclo = (function () {
      const b = new Date(Date.now() - 3 * 3600000);
      const d = new Date(b); d.setUTCDate(b.getUTCDate() - ((b.getUTCDay() + 1) % 7));
      return d.toISOString().slice(0, 10);
    })();
    const gastos = {};
    try {
      const jn = 'time_range=' + encodeURIComponent(JSON.stringify({ since: desdeCiclo, until: new Date(Date.now() - 3 * 3600000).toISOString().slice(0, 10) }));
      const ins = await pegarTudo(`${GRAPH}/act_${CONTA}/insights?level=campaign&${jn}&fields=campaign_id,spend&limit=200&access_token=${TOKEN}`, 6);
      for (const i of (ins.data || [])) gastos[i.campaign_id] = Number(i.spend || 0);
    } catch (e) {}

    // só campanhas deste ciclo
    const doCiclo = (camps.data || []).filter(c => String(c.start_time || '').slice(0, 10) >= desdeCiclo);
    const linhas = doCiclo.map(c => {
      let verba = null, onde = null;
      if (c.lifetime_budget) { verba = Number(c.lifetime_budget) / 100; onde = 'campanha'; }
      else if (c.daily_budget) { verba = Number(c.daily_budget) / 100; onde = 'campanha (diária)'; }
      else {
        let soma = 0;
        for (const s of (setsDe[c.id] || [])) {
          if (s.lifetime_budget) soma += Number(s.lifetime_budget) / 100;
          else if (s.daily_budget) soma += Number(s.daily_budget) / 100;
        }
        if (soma) { verba = Number(soma.toFixed(2)); onde = 'conjunto'; }
      }
      const ativa = ['ACTIVE'].includes(c.effective_status);
      return { nome: c.name, id: c.id,
        categoria: categoriaDe(c.name || '', 'anuncio'),
        situacao: c.effective_status, ativa,
        verba, verbaEm: onde,
        gasto: Number((gastos[c.id] || 0).toFixed(2)),
        inicio: brt(c.start_time), fim: brt(c.stop_time) };
    }).sort((a, b) => (b.verba || 0) - (a.verba || 0));

    const ativos = linhas.filter(l => l.ativa);
    const pausados = linhas.filter(l => !l.ativa);
    const somaTv = ativos.filter(l => l.categoria === 'tv');
    const somaAdm = ativos.filter(l => l.categoria !== 'tv');
    const soma = (arr, campo) => Number(arr.reduce((s, x) => s + (x[campo] || 0), 0).toFixed(2));

    // ⏳ quanto falta até o fim do ciclo (sábado 11h BRT)
    const agoraB = new Date(Date.now() - 3 * 3600000);
    const diasAteSab = (6 - agoraB.getUTCDay() + 7) % 7;
    const fimCiclo = new Date(Date.UTC(agoraB.getUTCFullYear(), agoraB.getUTCMonth(),
      agoraB.getUTCDate() + (diasAteSab === 0 && agoraB.getUTCHours() >= 14 ? 7 : diasAteSab), 14, 0, 0));
    const horasRestantes = Math.max(0, (fimCiclo.getTime() - Date.now()) / 3600000);
    const horasDecorridas = Math.max(1, (Date.now() - new Date(desdeCiclo + 'T16:00:00Z').getTime()) / 3600000);
    const projeta = (arr) => {
      const g = soma(arr, 'gasto'), v = soma(arr, 'verba');
      const ritmo = g / horasDecorridas;                       // por hora
      const projetado = Number((g + ritmo * horasRestantes).toFixed(2));
      return { gasto: g, verba: v, restante: Number((v - g).toFixed(2)),
        porDia: Number((ritmo * 24).toFixed(2)),
        projecaoAteOFim: projetado,
        vaiSobrar: Number((v - projetado).toFixed(2)) };
    };
    return res.status(200).json({ ok: true, cicloDesde: desdeCiclo,
      fimDoCiclo: new Date(fimCiclo.getTime() - 3 * 3600000).toISOString().slice(0, 16).replace('T', ' ') + ' BRT',
      horasRestantes: Math.round(horasRestantes),
      TOTAIS: {
        tv: { anuncios: somaTv.length, ...projeta(somaTv) },
        adm: { anuncios: somaAdm.length, ...projeta(somaAdm) },
        geral: { ativos: ativos.length, pausados: pausados.length,
          verbaAtiva: soma(ativos, 'verba'), gastoTotal: soma(linhas, 'gasto'),
          verbaEmPausados: soma(pausados, 'verba'),
          gastoNosPausados: soma(pausados, 'gasto'),
          presoNosPausados: Number((soma(pausados, 'verba') - soma(pausados, 'gasto')).toFixed(2)) },
      },
      porCategoria: ativos.reduce((o, l) => {
        const k = l.categoria; o[k] = o[k] || { anuncios: 0, verba: 0, gasto: 0 };
        o[k].anuncios++; o[k].verba = Number((o[k].verba + (l.verba || 0)).toFixed(2));
        o[k].gasto = Number((o[k].gasto + l.gasto).toFixed(2)); return o; }, {}),
      ...(String(req.query.mini || '') === '1' ? {} : {}),
      ATIVOS: ativos.map(l => (l.categoria || '?').slice(0, 4).toUpperCase().padEnd(4) + ' | ' +
        String(l.nome).slice(0, 30).padEnd(30) + ' | R$ ' + String(l.verba || 0).padEnd(7) +
        ' | gasto R$ ' + l.gasto + ' (' + (l.verbaEm || '?') + ')'),
      PAUSADOS: pausados.map(l => (l.categoria || '?').slice(0, 4).toUpperCase().padEnd(4) + ' | ' +
        String(l.nome).slice(0, 30).padEnd(30) + ' | R$ ' + String(l.verba || 0) + ' | ' + l.situacao),
    });
  }

  // ── 🩺 LOG-BRUTO: o que está gravado no log do tráfego, sem interpretação ──
  if (action === 'log-bruto') {
    const lg = (await dbGet('trafego_log')) || { movs: [] };
    const n = Math.min(20, Math.max(1, parseInt(req.query.n || '10', 10)));
    return res.status(200).json({ ok: true,
      totalRegistros: (lg.movs || []).length,
      registros: (lg.movs || []).slice(0, n).map(m => ({
        quando: m.ts,
        acao: m.acao || '(sem rótulo)',
        qtdFeitos: (m.feitos || []).length,
        qtdErros: (m.erros || []).length,
        feitos: (m.feitos || []).map(f => (f.nome || f.id) + ' → ' + f.acao),
        erros: (m.erros || []).map(e => (e.id || '?') + ' → ' + (e.erro || '')),
      })) });
  }

  // ── 📜 HISTORICO-COPILOTO: o que o Copiloto fez no período ──
  if (action === 'historico-copiloto') {
    const dias = Math.min(60, Math.max(1, parseInt(req.query.dias || '7', 10)));
    const corte = Date.now() - dias * 86400000;
    const lg = (await dbGet('trafego_log')) || { movs: [] };
    const movs = (lg.movs || []).filter(m => new Date(m.ts || 0).getTime() >= corte);
    // nomes atuais das campanhas, para o log não mostrar só identificadores
    const nomes = {};
    try {
      const camps = await pegarTudo(`${GRAPH}/act_${CONTA}/campaigns?fields=id,name&limit=200&access_token=${TOKEN}`, 6);
      for (const c of (camps.data || [])) nomes[c.id] = c.name;
      const ads = await pegarTudo(`${GRAPH}/act_${CONTA}/ads?fields=id,name&limit=300&access_token=${TOKEN}`, 8);
      for (const a of (ads.data || [])) nomes[a.id] = a.name;
    } catch (e) {}

    const eventos = [];
    for (const m of movs) {
      const quando = m.ts;
      for (const f of (m.feitos || [])) {
        const ehPausa = /pausa|paused|status/i.test(String(f.acao || ''));
        const mv = String(f.acao || '').match(/R\$\s*([\d.,]+)/);
        const valor = mv ? Number(String(mv[1]).replace(/\./g, '').replace(',', '.')) : null;
        eventos.push({ quando,
          tipo: ehPausa ? 'pausa' : 'verba',
          nome: f.nome || nomes[f.id] || f.id,
          id: f.id,
          valor: !ehPausa && valor ? (valor > 5000 ? valor / 100 : valor) : null,
          acao: f.acao });
      }
      for (const e of (m.erros || [])) {
        eventos.push({ quando, tipo: 'erro', nome: nomes[e.id] || e.id, id: e.id,
          acao: e.acao, erro: e.erro });
      }
    }
    eventos.sort((a, b) => String(b.quando).localeCompare(String(a.quando)));

    const pausas = eventos.filter(e => e.tipo === 'pausa');
    const verbas = eventos.filter(e => e.tipo === 'verba');
    const erros = eventos.filter(e => e.tipo === 'erro');
    // agrupa por dia
    const porDia = {};
    for (const e of eventos) {
      const d = String(e.quando).slice(0, 10);
      if (!porDia[d]) porDia[d] = { pausas: 0, verbas: 0, totalVerba: 0, erros: 0 };
      if (e.tipo === 'pausa') porDia[d].pausas++;
      else if (e.tipo === 'verba') { porDia[d].verbas++; porDia[d].totalVerba += (e.valor || 0); }
      else porDia[d].erros++;
    }
    return res.status(200).json({ ok: true, periodoDias: dias,
      resumo: { aplicacoes: movs.length, pausas: pausas.length, realocacoes: verbas.length,
        verbaRealocada: Number(verbas.reduce((s, v) => s + (v.valor || 0), 0).toFixed(2)),
        erros: erros.length },
      porDia: Object.keys(porDia).sort().reverse().reduce((o, d) => {
        const x = porDia[d];
        o[d] = x.pausas + ' pausa(s) · ' + x.verbas + ' verba(s) · R$ ' + x.totalVerba.toFixed(2) +
          (x.erros ? ' · ' + x.erros + ' erro(s)' : '');
        return o; }, {}),
      linhaDoTempo: eventos.slice(0, 60).map(e =>
        String(e.quando).slice(5, 16).replace('T', ' ') + ' | ' +
        (e.tipo === 'pausa' ? '⏸️ PAUSOU' : (e.tipo === 'verba' ? '💰 VERBA' : '❌ ERRO')) + ' | ' +
        String(e.nome).slice(0, 30) +
        (e.valor ? ' | R$ ' + e.valor.toFixed(2) : '') +
        (e.erro ? ' | ' + String(e.erro).slice(0, 50) : '')),
      eventos: eventos.slice(0, 100) });
  }

  // ── 🔎 POR-QUE-FORA: quais anúncios ativos na Meta ficaram fora do painel, e por quê ──
  if (action === 'por-que-fora') {
    const cat = String(req.query.cat || '').toLowerCase();
    const ads = await pegarTudo(`${GRAPH}/act_${CONTA}/ads?fields=id,name,effective_status,adset{id,name,effective_status,end_time},campaign{id,name,effective_status}&limit=300&access_token=${TOKEN}`, 10);
    const agoraMs = Date.now();
    const linhas = [];
    for (const a of (ads.data || [])) {
      const c = categoriaDe(a.name || '', 'anuncio');
      if (cat && c !== cat) continue;
      if (a.effective_status !== 'ACTIVE') continue;          // só os que a Meta diz ATIVO
      const st = a.adset || {};
      let motivo = null;
      if (st.effective_status !== 'ACTIVE') motivo = 'conjunto ' + (st.effective_status || '?');
      else if (st.end_time && new Date(st.end_time).getTime() < agoraMs) {
        motivo = 'veiculação encerrada em ' + new Date(new Date(st.end_time).getTime() - 3 * 3600000).toISOString().slice(0, 16).replace('T', ' ');
      }
      linhas.push({ nome: a.name, id: a.id, categoria: c,
        conjunto: st.name || '', fimDoConjunto: st.end_time || null,
        campanha: (a.campaign || {}).name || '',
        entraNoPainel: !motivo, motivoDoCorte: motivo });
    }
    const dentro = linhas.filter(l => l.entraNoPainel);
    const fora = linhas.filter(l => !l.entraNoPainel);
    const porMotivo = fora.reduce((o, l) => { const k = l.motivoDoCorte; o[k] = (o[k] || 0) + 1; return o; }, {});
    return res.status(200).json({ ok: true, categoria: cat || 'todas',
      ativosNaMeta: linhas.length, entramNoPainel: dentro.length, ficamDeFora: fora.length,
      porMotivo,
      listaFora: fora.slice(0, 30).map(l => String(l.nome).slice(0, 30) + ' | ' + l.motivoDoCorte +
        ' | camp: ' + String(l.campanha).slice(0, 24)),
      listaDentro: dentro.slice(0, 30).map(l => String(l.nome).slice(0, 30) + ' | camp: ' + String(l.campanha).slice(0, 24)) });
  }

  // ── 🔬 DIAG-CATEGORIA: por que uma categoria some do Copiloto ──
  if (action === 'diag-categoria') {
    const cat = String(req.query.cat || 'microondas').toLowerCase();
    const base = await dbGet('trafego_painel_cache_7d') || await dbGet('trafego_painel_cache');
    if (!base || !base.dados) return res.status(200).json({ ok: false, error: 'abra o painel primeiro' });
    const todos = base.dados.anuncios || [];
    const daCat = todos.filter(a => a.categoria === cat);
    // e o que a Meta diz AGORA (o cache pode estar velho)
    const ads = await pegarTudo(`${GRAPH}/act_${CONTA}/ads?fields=id,name,effective_status,adset{id,name,effective_status},campaign{id,name,effective_status}&limit=200&access_token=${TOKEN}`, 8);
    const naMeta = (ads.data || []).filter(a => categoriaDe(a.name || '', 'anuncio') === cat);
    const ativosMeta = naMeta.filter(a => a.effective_status === 'ACTIVE');
    return res.status(200).json({ ok: true, categoria: cat,
      cacheDoPainel: {
        geradoEm: base.em || base.geradoEm || '(sem data)',
        total: daCat.length,
        ativos: daCat.filter(a => a.ativo).length,
        inativos: daCat.filter(a => !a.ativo).length,
        lista: daCat.map(a => (a.ativo ? '🟢 ' : '⏸️ ') + String(a.nome).slice(0, 30) +
          ' | cpa ' + (a.cpa != null ? a.cpa : '—') + ' | conv ' + a.conversas),
      },
      naMetaAgora: {
        total: naMeta.length,
        ativos: ativosMeta.length,
        lista: naMeta.slice(0, 30).map(a => (a.effective_status === 'ACTIVE' ? '🟢 ' : '⏸️ ') +
          String(a.name).slice(0, 30) + ' | ' + a.effective_status +
          ' | conj: ' + ((a.adset || {}).effective_status || '?') +
          ' | camp: ' + ((a.campaign || {}).effective_status || '?')),
      },
      diagnostico: ativosMeta.length > 0 && daCat.filter(a => a.ativo).length === 0
        ? '⚠️ o CACHE do painel está desatualizado — há ' + ativosMeta.length + ' ativo(s) na Meta. Recarregue com forcar=1'
        : (ativosMeta.length === 0 ? 'não há nenhum anúncio ativo desta categoria na Meta'
          : 'cache e Meta concordam'),
      comoCorrigir: '/api/trafego?action=painel&periodo=ciclo&forcar=1' });
  }

  // ── ✅ CONFERIR-APLICACAO: as últimas alterações do Copiloto valeram na Meta? ──
  if (action === 'conferir-aplicacao') {
    if (!CONTA) return res.status(200).json({ ok: false, error: 'conta não configurada' });
    const lg = (await dbGet('trafego_log')) || { movs: [] };
    // o log grava em { feitos: [{id, acao}], erros: [] } — ler nesse formato
    const ultimo = (lg.movs || []).find(m => (m.feitos && m.feitos.length) || m.pausas || m.verbas);
    if (!ultimo) return res.status(200).json({ ok: false,
      error: 'nenhuma aplicação registrada no log',
      registrosNoLog: (lg.movs || []).length,
      ultimoRegistro: (lg.movs || [])[0] || null });

    // separa pausas de realocações a partir do texto da ação
    const feitos = ultimo.feitos || [];
    const pausasLog = feitos.filter(f => /pausa|paused|status/i.test(String(f.acao || '')))
      .map(f => ({ campanhaId: f.id, nome: f.nome || f.id }));
    const verbasLog = feitos.filter(f => /budget|orçamento|orcamento|R\$/i.test(String(f.acao || '')))
      .map(f => {
        const m = String(f.acao || '').match(/R\$\s*([\d.,]+)/);
        return { campanhaId: f.id, nome: f.nome || f.id,
          nova: m ? Number(String(m[1]).replace(/\./g, '').replace(',', '.')) : null };
      });

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
      // o log grava em CENTAVOS quando o valor vem da chamada à Meta — normalizar
      const espReais = esperado > 5000 ? esperado / 100 : esperado;
      const bate = real && espReais ? Math.abs(real.valor - espReais) < 1 : null;
      return { nome: (c && c.name) || v.nome || cid, id: cid,
        verbaEsperada: espReais || null,
        verbaNaMeta: real ? real.valor : null,
        onde: real ? real.onde : null,
        situacao: c ? c.effective_status : '(não encontrada)',
        aplicou: bate };
    };

    const pausas = (ultimo.pausas || ultimo.pausados || pausasLog).map(conferePausa);
    const verbas = (ultimo.verbas || ultimo.realocacoes || verbasLog).map(confereVerba);
    const falhouPausa = pausas.filter(p => !p.pausouDeVerdade);
    const falhouVerba = verbas.filter(v => v.aplicou === false);

    return res.status(200).json({
      ok: falhouPausa.length === 0 && falhouVerba.length === 0,
      aplicadoEm: ultimo.ts,
      errosNaAplicacao: (ultimo.erros || []).length ? ultimo.erros : 'nenhum',
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
      // ⚠️ bloco legado — mantido para compatibilidade, mas a tela usa metaVerba
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
      _regIA('trafego', j).catch(() => {});
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
