// ═══ 📈 TRÁFEGO — Conector Meta Ads (Fase 2, leitura) ═══
const U = process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL;
const T = process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN;
async function dbGet(k) {
  const r = await fetch(`${U}/get/${k}`, { headers: { Authorization: `Bearer ${T}` } }).then(x => x.json()).catch(() => null);
  try { return r && r.result ? JSON.parse(r.result) : null; } catch (e) { return null; }
}
async function dbSet(k, v) {
  await fetch(`${U}/set/${k}`, { method: 'POST', headers: { Authorization: `Bearer ${T}`, 'Content-Type': 'application/json' }, body: JSON.stringify(v) });
}
const GRAPH = 'https://graph.facebook.com/v20.0';

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
    metas: { tv: 2, microondas: 5, purificador: 8, adega: 10, outros: 8 },
    verba: { adm: 5000, tv: 500, aproveitamento: 0.87 },
    cicloInicio: { diaSemana: 6, hora: 13 }, // sábado 13h
  };
  async function cfgTrafego() {
    const c = (await dbGet('trafego_config')) || {};
    return {
      metas: Object.assign({}, CFG_PADRAO.metas, c.metas || {}),
      verba: Object.assign({}, CFG_PADRAO.verba, c.verba || {}),
      cicloInicio: Object.assign({}, CFG_PADRAO.cicloInicio, c.cicloInicio || {}),
    };
  }
  function categoriaDe(nome) {
    const s = String(nome || '').toLowerCase();
    if (/\btvs?\b|televis|barramento|tela quebrad|quebrar tv/.test(s)) return 'tv';
    if (/micro-?\s?ondas/.test(s)) return 'microondas';
    if (/purificador|bebedouro|\bfiltro\b|vela|[áa]gua/.test(s)) return 'purificador';
    if (/adega|cervejeir|climatiz|vinho/.test(s)) return 'adega';
    return 'outros';
  }
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
    // Cache de 30 min (a conta não muda de minuto em minuto)
    const cache = await dbGet('trafego_painel_cache');
    if (cache && cache.em && (Date.now() - new Date(cache.em).getTime() < 30 * 60000)
        && String(req.query.forcar || '') !== '1' && cache.desde === desde) {
      return res.status(200).json(Object.assign({}, cache.dados, { cacheDe: cache.em, doCache: true }));
    }
    const g = (p) => fetch(`${GRAPH}/act_${CONTA}/${p}&access_token=${TOKEN}`).then(x => x.json()).catch(e => ({ error: { message: e.message } }));
    const [ads, adsets, ins] = await Promise.all([
      g('ads?fields=id,name,status,effective_status,adset_id,campaign_id,creative{id,thumbnail_url,body,title,object_story_spec{video_data{message,title},link_data{message,name,description}}}&limit=250'),
      g('adsets?fields=id,name,status,daily_budget,lifetime_budget,campaign_id&limit=250'),
      g(`insights?level=ad&time_range={"since":"${desde}","until":"${hoje}"}&fields=ad_id,campaign_name,spend,impressions,clicks,ctr,cpc,actions&limit=400`),
    ]);
    if (ads.error) return res.status(200).json({ ok: false, erro: ads.error.message });
    const porAd = {};
    for (const i of ((ins || {}).data || [])) porAd[i.ad_id] = i;
    const porAdset = {};
    for (const a of ((adsets || {}).data || [])) porAdset[a.id] = a;

    const CONV = ['onsite_conversion.messaging_conversation_started_7d', 'lead', 'onsite_conversion.total_messaging_connection'];
    const anuncios = ((ads || {}).data || []).map(ad => {
      const i = porAd[ad.id] || {};
      const st = porAdset[ad.adset_id] || {};
      const conversas = Number(((i.actions || []).find(a => CONV.includes(a.action_type)) || {}).value || 0);
      const gasto = Number(i.spend || 0);
      const cat = categoriaDe(ad.name + ' ' + (i.campaign_name || '') + ' ' + (st.name || ''));
      const meta = cfg.metas[cat] || cfg.metas.outros;
      const cpa = conversas > 0 ? gasto / conversas : null;
      // distância da meta: <1 abaixo (bom), >1 acima (ruim); sem conversa com gasto = pior caso
      const razao = cpa != null ? cpa / meta : (gasto > 0 ? 3 : null);
      let situacao = 'sem-dados';
      if (razao != null) situacao = razao <= 1 ? 'campeao' : (razao <= 1.3 ? 'atencao' : 'ralo');
      return {
        id: ad.id, nome: ad.name, ativo: (ad.effective_status || ad.status) === 'ACTIVE',
        status: ad.effective_status || ad.status,
        thumb: (ad.creative || {}).thumbnail_url || null,
        copy: (function () {
          const c = ad.creative || {}, os = c.object_story_spec || {};
          const vd = os.video_data || {}, ld = os.link_data || {};
          return String(vd.message || ld.message || c.body || vd.title || ld.name || c.title || '').slice(0, 500);
        })(),
        adsetId: ad.adset_id, adsetNome: st.name || '',
        orcamentoDiario: st.daily_budget ? Number(st.daily_budget) / 100 : null,
        orcamentoTotal: st.lifetime_budget ? Number(st.lifetime_budget) / 100 : null,
        categoria: cat, meta,
        gasto: Number(gasto.toFixed(2)), conversas,
        cpa: cpa != null ? Number(cpa.toFixed(2)) : null,
        razaoMeta: razao != null ? Number(razao.toFixed(2)) : null,
        impressoes: Number(i.impressions || 0), cliques: Number(i.clicks || 0),
        ctr: i.ctr ? Number(Number(i.ctr).toFixed(2)) : null,
        cpc: i.cpc ? Number(Number(i.cpc).toFixed(2)) : null,
        situacao,
      };
    }).sort((a, b) => (a.razaoMeta == null ? 9 : a.razaoMeta) - (b.razaoMeta == null ? 9 : b.razaoMeta));

    // Termômetro da semana: verba real (87%) × gasto do ciclo
    const gastoTv = anuncios.filter(a => a.categoria === 'tv').reduce((s, a) => s + a.gasto, 0);
    const gastoAdm = anuncios.filter(a => a.categoria !== 'tv').reduce((s, a) => s + a.gasto, 0);
    const realAdm = cfg.verba.adm * cfg.verba.aproveitamento;
    const realTv = cfg.verba.tv * cfg.verba.aproveitamento;
    const porCategoria = {};
    for (const c of ['tv', 'microondas', 'purificador', 'adega', 'outros']) {
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
    const dados = { ok: true, ciclo: { desde, ate: hoje },
      ativos: ativos.length, pausados: anuncios.length - ativos.length,
      verba: {
        adm: { depositado: cfg.verba.adm, real: Number(realAdm.toFixed(2)), gasto: Number(gastoAdm.toFixed(2)), saldo: Number((realAdm - gastoAdm).toFixed(2)) },
        tv: { depositado: cfg.verba.tv, real: Number(realTv.toFixed(2)), gasto: Number(gastoTv.toFixed(2)), saldo: Number((realTv - gastoTv).toFixed(2)) },
        aproveitamento: cfg.verba.aproveitamento },
      metas: cfg.metas, porCategoria, totalAnuncios: anuncios.length, anuncios };
    try { await dbSet('trafego_painel_cache', { em: new Date().toISOString(), desde, dados }); } catch (e) {}
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

  // ═══ 🧭 COPILOTO: o que pausar, quanto libera e para quem vai (proporcional ao desempenho) ═══
  if (action === 'copiloto') {
    const cfg = await cfgTrafego();
    const base = await dbGet('trafego_painel_cache');
    if (!base || !base.dados) return res.status(200).json({ ok: false, error: 'abra o painel primeiro para carregar os dados do ciclo' });
    const ads = (base.dados.anuncios || []).filter(a => a.ativo);
    const diasRestantes = (function () {
      const b = new Date(Date.now() - 3 * 3600 * 1000);
      const faltam = (6 - b.getUTCDay() + 7) % 7;
      return faltam === 0 ? 7 : faltam;
    })();
    const semanalDe = a => a.orcamentoDiario != null ? a.orcamentoDiario * 7 : (a.orcamentoTotal || 0);
    // PAUSAR: acima de 30% da meta, ou queimando sem nenhuma conversa
    const pausar = ads.filter(a => {
      if (a.conversas === 0 && a.gasto >= (a.meta * 2)) return true;
      return a.razaoMeta != null && a.razaoMeta > 1.3;
    }).map(a => ({
      id: a.id, nome: a.nome, categoria: a.categoria, thumb: a.thumb,
      cpa: a.cpa, meta: a.meta, gasto: a.gasto, conversas: a.conversas,
      orcamentoDiario: a.orcamentoDiario, adsetId: a.adsetId,
      liberaria: Number(Math.max(0, semanalDe(a) - a.gasto).toFixed(2)),
      motivo: a.conversas === 0 ? 'queimou ' + a.gasto.toFixed(2) + ' sem nenhuma conversa'
        : 'CPA R$ ' + a.cpa + ' — ' + Math.round((a.razaoMeta - 1) * 100) + '% acima da meta de R$ ' + a.meta,
    })).sort((x, y) => y.liberaria - x.liberaria);
    const liberado = Number(pausar.reduce((s, p) => s + p.liberaria, 0).toFixed(2));
    // REFORÇAR: campeões, peso proporcional ao desempenho (quanto melhor o CPA vs meta, mais peso)
    const idsPausar = new Set(pausar.map(p => p.id));
    const campeoes = ads.filter(a => !idsPausar.has(a.id) && a.situacao === 'campeao' && a.conversas > 0);
    const pesoDe = a => (1 / a.razaoMeta) * Math.log10(10 + a.conversas); // desempenho × consistência
    const somaPeso = campeoes.reduce((s, a) => s + pesoDe(a), 0) || 1;
    const distribuir = campeoes.map(a => {
      const fatia = Number((liberado * (pesoDe(a) / somaPeso)).toFixed(2));
      const diarioExtra = diasRestantes > 0 ? fatia / diasRestantes : 0;
      return {
        id: a.id, nome: a.nome, categoria: a.categoria, thumb: a.thumb, adsetId: a.adsetId,
        cpa: a.cpa, meta: a.meta, conversas: a.conversas,
        pesoPct: Number(((pesoDe(a) / somaPeso) * 100).toFixed(1)),
        receber: fatia,
        orcamentoDiarioAtual: a.orcamentoDiario,
        orcamentoDiarioNovo: a.orcamentoDiario != null ? Number((a.orcamentoDiario + diarioExtra).toFixed(2)) : Number(diarioExtra.toFixed(2)),
        conversasEstimadas: a.cpa ? Math.round(fatia / a.cpa) : null,
      };
    }).sort((x, y) => y.receber - x.receber);
    const conversasGanhas = distribuir.reduce((s, d) => s + (d.conversasEstimadas || 0), 0);
    return res.status(200).json({ ok: true,
      ciclo: base.dados.ciclo, diasRestantes,
      analisados: ads.length, pausar, liberado, distribuir,
      resumo: pausar.length
        ? `Pausando ${pausar.length} anúncio(s) você recupera R$ ${liberado.toFixed(2)}. Redistribuindo entre os ${distribuir.length} campeões, a estimativa é de mais ${conversasGanhas} conversas com a mesma verba.`
        : 'Nenhum anúncio ativo passou do limite de corte agora — a verba está bem alocada.',
      conversasEstimadasGanhas: conversasGanhas });
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
    const campeoes = ativos.filter(a => a.situacao === 'campeao').slice(0, 25)
      .map(a => ({ nome: a.nome, categoria: a.categoria, cpa: a.cpa, meta: a.meta, conversas: a.conversas, ctr: a.ctr, copy: (a.copy || '').slice(0, 300) }));
    const ralos = ativos.filter(a => a.situacao === 'ralo').slice(0, 15)
      .map(a => ({ nome: a.nome, categoria: a.categoria, cpa: a.cpa, meta: a.meta, conversas: a.conversas, copy: (a.copy || '').slice(0, 200) }));

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
