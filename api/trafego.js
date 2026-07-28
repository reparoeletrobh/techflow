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
      g('ads?fields=id,name,status,effective_status,adset_id,campaign_id,creative{id,thumbnail_url}&limit=250'),
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
      const lista = anuncios.filter(a => a.categoria === c);
      const gc = lista.reduce((s, a) => s + a.gasto, 0);
      const cc = lista.reduce((s, a) => s + a.conversas, 0);
      porCategoria[c] = { anuncios: lista.length, ativos: lista.filter(a => a.ativo).length,
        gasto: Number(gc.toFixed(2)), conversas: cc,
        cpa: cc > 0 ? Number((gc / cc).toFixed(2)) : null, meta: cfg.metas[c] || cfg.metas.outros,
        campeoes: lista.filter(a => a.situacao === 'campeao').length,
        ralos: lista.filter(a => a.situacao === 'ralo').length };
    }
    const dados = { ok: true, ciclo: { desde, ate: hoje },
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

  return res.status(404).json({ ok: false, error: 'ação não encontrada (meta-teste | meta-campanhas)' });
  } catch (e) {
    return res.status(200).json({ ok: false, error: 'erro interno: ' + e.message });
  }
};
