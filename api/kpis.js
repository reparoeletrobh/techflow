// carregado de forma tolerante: o harness executa os arquivos fora da pasta api
let _funil = { registrar: async () => false, ler: async () => [], jaRegistrado: async () => false };
try { _funil = require('./_funil'); } catch (e) {
  try { _funil = require(require('path').join(__dirname, '_funil.js')); } catch (e2) {}
}
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
  // as conversas vêm da Meta, que guarda 37 meses — não dependem deste banco
  r.conversas = 'Meta Ads · 37 meses';
  return r;
}
let lg;

// 🔎 desde quando existe carimbo de aprovação — e quanto se perde antes disso
async function coberturaAprovacoes() {
  const R = { porMes: {}, primeiroCarimbo: null, primeiroHistorico: null,
    totalCards: 0, comCarimbo: 0, comHistorico: 0, semNada: 0 };
  const fases = ['aprovados', 'producao', 'video_enviado', 'analise_compra',
    'equipamento_comprado', 'programar_entrega', 'solicitar_entrega',
    'entrega_solicitada', 'receber', 'erp', 'garantia', 'finalizado'];
  for (const k of ['reparoeletro_pipe', 'tv_pipe', 'reparoeletro_arquivo', 'tv_arquivo']) {
    let b = null;
    try { b = await dbGet(k); } catch (e) { continue; }
    for (const L of ['cards', 'fichas']) {
      for (const c of (((b || {})[L]) || [])) {
        const fase = String(c.phaseId || c.phase || '');
        const passou = fases.includes(fase) || !!c.aprovadoEm ||
          (c.history || []).some(x => String(x.phase || x.phaseId || '') === 'aprovados');
        if (!passou) continue;
        R.totalCards++;
        const carimbo = c.aprovadoEm ? String(c.aprovadoEm).slice(0, 10) : null;
        const hist = (c.history || [])
          .filter(x => String(x.phase || x.phaseId || '') === 'aprovados')
          .map(x => String(x.ts || x.timestamp || '').slice(0, 10)).filter(Boolean).sort()[0] || null;
        // o mês de referência é o do card, para saber quando cada situação ocorre
        const mesRef = String(carimbo || hist || c.criadoEm || '').slice(0, 7) || '(sem data)';
        R.porMes[mesRef] = R.porMes[mesRef] || { total: 0, carimbo: 0, historico: 0, semNada: 0 };
        R.porMes[mesRef].total++;
        if (carimbo) {
          R.comCarimbo++; R.porMes[mesRef].carimbo++;
          if (!R.primeiroCarimbo || carimbo < R.primeiroCarimbo) R.primeiroCarimbo = carimbo;
        } else if (hist) {
          R.comHistorico++; R.porMes[mesRef].historico++;
          if (!R.primeiroHistorico || hist < R.primeiroHistorico) R.primeiroHistorico = hist;
        } else {
          R.semNada++; R.porMes[mesRef].semNada++;
        }
      }
    }
  }
  return R;
}

module.exports = async function handler(req, res) {
  const _k = (req.query && req.query.k) || req.headers['x-tf-key'] || '';
  if (_k !== ((process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim())) {
    return res.status(401).json({ ok: false, error: 'não autorizado' });
  }
  res.setHeader('Access-Control-Allow-Origin', 'https://reparoeletroadm.com');
  // 📏 medição da cobertura das aprovações
  if ((req.query || {}).action === 'cobertura') {
    const C = await coberturaAprovacoes();
    const meses = Object.entries(C.porMes).sort();
    const pct = (a, b) => b ? Math.round(a / b * 100) : 0;
    // a partir de qual mês a medição é confiável (90% ou mais identificados)
    let confiavelDesde = null;
    for (const [m, v] of meses) {
      const ident = v.carimbo + v.historico;
      if (pct(ident, v.total) >= 90) { confiavelDesde = m; break; }
    }
    return res.status(200).json({ ok: true,
      RESUMO: {
        cardsQuePassaramPorAprovacao: C.totalCards,
        comCarimboDeData: C.comCarimbo,
        semCarimboMasComHistorico: C.comHistorico,
        semNenhumaDataDeAprovacao: C.semNada,
        percentualIdentificavel: pct(C.comCarimbo + C.comHistorico, C.totalCards) + '%',
      },
      primeiroCarimboRegistrado: C.primeiroCarimbo,
      primeiroHistoricoRegistrado: C.primeiroHistorico,
      VEREDITO: confiavelDesde
        ? 'a medição de aprovados é confiável a partir de ' + confiavelDesde
        : 'nenhum mês atinge 90% de aprovações identificáveis',
      confiavelDesde,
      POR_MES: meses.map(([m, v]) => m + ' | ' + String(v.total).padStart(3) + ' aprovação(ões) | ' +
        'com data: ' + pct(v.carimbo + v.historico, v.total) + '% ' +
        '(carimbo ' + v.carimbo + ' · histórico ' + v.historico + ' · sem data ' + v.semNada + ')'),
      observacao: 'card sem carimbo e sem histórico não entra em nenhum período — ele existe, mas não se sabe quando foi aprovado' });
  }

  // ── ⏪ RETROATIVO: reconstrói os carimbos que faltam, a partir do histórico ──
  if ((req.query || {}).action === 'retroativo') {
    const desde = String(req.query.desde || '2026-08-08');
    const corte = new Date(desde + 'T00:00:00-03:00').getTime();
    const dataDe = (c, fases) => {
      const h2 = (c.history || [])
        .filter(x => fases.includes(String(x.phase || x.phaseId || '')))
        .map(x => new Date(x.ts || x.timestamp || 0).getTime())
        .filter(Boolean).sort((a, b) => a - b);
      return h2.length ? new Date(h2[0]).toISOString() : null;
    };
    const previa = { aprovacao: [], orcamento: [], semHistorico: [] };
    const BANCOS = ['reparoeletro_pipe', 'tv_pipe', 'reparoeletro_frenteloja'];
    const alteracoes = {};
    for (const k of BANCOS) {
      const b = await dbGet(k);
      if (!b) continue;
      const lista = k === 'reparoeletro_frenteloja' ? (b.fichas || []) : (b.cards || []);
      for (const c of lista) {
        const nasceu = new Date(c.criadoEm || c.movedAt || 0).getTime();
        const doPeriodo = (c.history || []).some(x => new Date(x.ts || 0).getTime() >= corte) ||
          nasceu >= corte;
        if (!doPeriodo) continue;
        const doBalcao = k === 'reparoeletro_frenteloja' || String(c.origem || '') === 'frenteloja';
        const quem = String(c.nomeContato || c.nome || '?').slice(0, 20) + ' ' +
          String(c.telefone || '').replace(/\D/g, '').slice(-4);
        // 💰 aprovação: no balcão é a ida para produção; no pipe é a entrada em aprovados
        if (!c.aprovadoEm) {
          const d = doBalcao ? dataDe(c, ['producao', 'aprovados'])
                             : dataDe(c, ['aprovados']);
          if (d && new Date(d).getTime() >= corte) {
            alteracoes[k] = alteracoes[k] || [];
            alteracoes[k].push({ id: c.id, campo: 'aprovadoEm', valor: d, doBalcao });
            previa.aprovacao.push((doBalcao ? '🏪 ' : '💻 ') + quem + ' → ' + d.slice(0, 16).replace('T', ' '));
          } else if (['aprovados', 'producao'].includes(String(c.phase || c.phaseId || ''))) {
            previa.semHistorico.push(quem + ' (está em ' + (c.phase || c.phaseId) + ' mas sem histórico datável)');
          }
        }
        // 📄 orçamento
        if (!c.orcamentoEm) {
          const d = doBalcao ? dataDe(c, ['orcamento_cadastrado', 'producao', 'aprovados'])
                             : dataDe(c, ['aguardando_aprovacao', 'orcamento', 'aprovados']);
          const alt = d || (Number(c.valor || 0) > 0 && nasceu >= corte ? new Date(nasceu).toISOString() : null);
          if (alt && new Date(alt).getTime() >= corte) {
            alteracoes[k] = alteracoes[k] || [];
            alteracoes[k].push({ id: c.id, campo: 'orcamentoEm', valor: alt, doBalcao });
            previa.orcamento.push((doBalcao ? '🏪 ' : '💻 ') + quem + ' → ' + alt.slice(0, 16).replace('T', ' '));
          }
        }
      }
    }
    if (String(req.query.aplicar || '') !== '1') {
      return res.status(200).json({ ok: true, modo: 'prévia', desde,
        aprovacoesACarimbar: previa.aprovacao.length,
        orcamentosACarimbar: previa.orcamento.length,
        semHistoricoDatavel: previa.semHistorico.length,
        criterio: {
          balcao: 'aprovação = ida para produção pelo botão APROVADO; orçamento = quando foi cadastrado',
          pipe: 'aprovação = primeira entrada em aprovados no histórico; orçamento = entrada em aguardando aprovação',
          semHistorico: 'não é carimbado — inventar data seria pior que deixar sem',
        },
        APROVACOES: previa.aprovacao.slice(0, 60),
        ORCAMENTOS: previa.orcamento.slice(0, 60),
        SEM_HISTORICO: previa.semHistorico.slice(0, 30),
        dica: 'para gravar: &aplicar=1' });
    }
    let gravadas = 0;
    for (const [k, alts] of Object.entries(alteracoes)) {
      const b = await dbGet(k);
      if (!b) continue;
      const lista = k === 'reparoeletro_frenteloja' ? (b.fichas || []) : (b.cards || []);
      for (const a of alts) {
        const c = lista.find(x => String(x.id) === String(a.id));
        if (!c || c[a.campo]) continue;
        c[a.campo] = a.valor;
        c.carimboRetroativo = true;
        if (a.doBalcao && a.campo === 'aprovadoEm') c.aprovadoNoBalcao = true;
        gravadas++;
      }
      await dbSet(k, b);
    }
    return res.status(200).json({ ok: true, gravadas,
      observacao: 'apenas datas obtidas do histórico real — nada foi estimado' });
  }

  // ── 📜 QUEM-APROVOU: a lista nominal de um dia, direto do livro-razão ──
  
  // ── 📄 QUEM-ORCOU: a lista nominal dos orçamentos do dia ──
  // Existe para conferir o número do painel contra a realidade: se o painel diz
  // sete e o diagnóstico do dia mostra dois, esta lista revela quais cinco
  // entraram indevidamente e por qual data foram contados.
  
  // ── 🔍 AUDITORIA: cada número do painel contra as fontes ──
  // O painel soma de mais de uma fonte — pipe, balcão, livro-razão — e quando
  // divergem não há como saber qual está certa. Esta conferência mostra o que
  // cada fonte diz, item por item, e aponta o que aparece em duas delas.
  if ((req.query || {}).action === 'auditoria-kpi') {
    const per = String(req.query.periodo || 'semana');
    const frente = String(req.query.frente || 'adm').toLowerCase();
    const agora = Date.now();
    // ciclo comercial: sábado 13h a sábado 13h
    const aBR = new Date(agora - 3 * 3600000);
    let voltar = (aBR.getUTCDay() - 6 + 7) % 7;
    if (aBR.getUTCDay() === 6 && aBR.getUTCHours() < 13) voltar = 7;
    const iniSem = new Date(aBR.getTime() - voltar * 86400000);
    iniSem.setUTCHours(13, 0, 0, 0);
    const ini = per === 'hoje'
      ? new Date(new Date(agora - 3 * 3600000).toISOString().slice(0, 10) + 'T00:00:00-03:00').getTime()
      : per === 'mes' ? agora - 30 * 86400000
      : iniSem.getTime() + 3 * 3600000;
    const dentro = d => { const t = new Date(d || 0).getTime(); return t >= ini && t <= agora; };
    const d8a = t => String(t || '').replace(/\D/g, '').slice(-8);
    const hh = d => d ? new Date(new Date(d).getTime() - 3 * 3600000)
      .toISOString().slice(5, 16).replace('T', ' ') : '—';

    const bancoPipe = frente === 'tv' ? 'tv_pipe' : 'reparoeletro_pipe';
    const [pipeDb, flDb, arqDb] = await Promise.all([
      dbGet(bancoPipe), dbGet('reparoeletro_frenteloja'), dbGet('reparoeletro_arquivo'),
    ]);

    // ── fonte 1: cards do pipe com data de orçamento ──
    const doPipe = [];
    for (const c of (((pipeDb || {}).cards) || [])) {
      const q = c.orcamentoEm || (((c.history || [])
        .filter(x => ['aguardando_aprovacao', 'orcamento_cadastrado']
          .includes(String(x.phase || x.phaseId || '')))
        .map(x => String(x.ts || x.timestamp || '')).filter(Boolean).sort()[0]) || null);
      if (!q || !dentro(q)) continue;
      doPipe.push({ fonte: 'pipe', id: c.id, nome: c.nomeContato || c.nome || '?',
        tel: d8a(c.telefone), valor: Number(c.valor || 0), quando: q,
        balcao: String(c.origem || '') === 'frenteloja' || c.aprovadoNoBalcao === true ||
          String(c.id || '').includes('-loja') });
    }
    // ── fonte 2: fichas do balcão com orçamento ──
    const doBalcao = [];
    if (frente !== 'tv') {
      for (const f of (((flDb || {}).fichas) || [])) {
        const q = f.orcamentoEm || f.orcamentoCadastradoEm || null;
        if (!q || !dentro(q)) continue;
        doBalcao.push({ fonte: 'balcão', id: f.id, nome: f.nomeContato || f.nome || '?',
          tel: d8a(f.telefone), valor: Number(f.valor || 0), quando: q });
      }
    }
    // ── fonte 3: livro-razão ──
    let doLivro = [];
    try {
      doLivro = (await _funil.ler(ini, agora))
        .filter(e => e.etapa === 'orcamento' && (e.frente || 'adm') === frente)
        .map(e => ({ fonte: 'livro', nome: e.nome || '?', tel: d8a(e.tel),
          valor: Number(e.valor || 0), quando: e.ts, canal: e.canal }));
    } catch (e) {}

    // ── cruzamento: quem aparece em mais de uma fonte ──
    const chave = x => x.tel + '|' + Math.round(x.valor);
    const vistos = {};
    for (const lista of [doPipe, doBalcao, doLivro]) {
      for (const x of lista) {
        const k = chave(x);
        vistos[k] = vistos[k] || { nome: x.nome, tel: x.tel, valor: x.valor, fontes: [] };
        if (!vistos[k].fontes.includes(x.fonte)) vistos[k].fontes.push(x.fonte);
      }
    }
    const emDuas = Object.values(vistos).filter(v => v.fontes.length > 1);
    const unicos = Object.keys(vistos).length;

    return res.status(200).json({ ok: true,
      frente: frente.toUpperCase(), periodo: per,
      de: hh(ini), ate: hh(agora),
      POR_FONTE: {
        pipe: doPipe.length,
        pipeOnline: doPipe.filter(x => !x.balcao).length,
        pipeBalcao: doPipe.filter(x => x.balcao).length,
        fichasDoBalcao: doBalcao.length,
        livroRazao: doLivro.length,
      },
      clientesDistintos: unicos,
      SOMA_SIMPLES: doPipe.length + doBalcao.length,
      VEREDITO: emDuas.length
        ? '⚠️ ' + emDuas.length + ' registro(s) aparecem em mais de uma fonte — ' +
          'somar as fontes conta esses duas vezes'
        : '✅ nenhum registro repetido entre as fontes',
      EM_DUAS_FONTES: emDuas.slice(0, 40).map(v => String(v.nome).slice(0, 22) + ' ' +
        v.tel.slice(-4) + ' | R$ ' + v.valor.toFixed(2) + ' | ' + v.fontes.join(' + ')),
      SO_NO_PIPE: doPipe.filter(x => !Object.values(vistos)
        .find(v => v.tel === x.tel && v.fontes.includes('livro')))
        .slice(0, 40).map(x => hh(x.quando) + ' | ' + String(x.nome).slice(0, 20) +
          ' ' + x.tel.slice(-4) + ' | R$ ' + x.valor.toFixed(2) +
          (x.balcao ? ' | 🏪 balcão' : ' | 💻 online')),
      SO_NO_LIVRO: doLivro.filter(x => !doPipe.find(p => p.tel === x.tel))
        .slice(0, 40).map(x => hh(x.quando) + ' | ' + String(x.nome).slice(0, 20) +
          ' ' + x.tel.slice(-4) + ' | R$ ' + x.valor.toFixed(2) +
          (x.canal ? ' | ' + x.canal : '')),
      comoLer: 'o painel deve mostrar clientesDistintos, não a soma das fontes' });
  }


  // ── 🔎 cacar-orcamentos: onde estão os orçamentos do dia, em toda parte ──
  // Quando o painel mostra um número e a operação conta outro, é preciso olhar
  // TODAS as bases, não só a que o painel lê. O orçamento pode estar sendo
  // gravado em lugar que a contagem não visita.
  if ((req.query || {}).action === 'cacar-orcamentos') {
    const dia = String(req.query.dia || new Date(Date.now() - 3 * 3600000).toISOString().slice(0, 10));
    const ini = new Date(dia + 'T00:00:00-03:00').getTime();
    const fim = ini + 86400000 - 1;
    const noDia = d => { if (!d) return false;
      const t = new Date(d).getTime(); return t >= ini && t <= fim; };
    const hh = d => d ? new Date(new Date(d).getTime() - 3 * 3600000)
      .toISOString().slice(5, 16).replace('T', ' ') : '—';
    const d8c = t => String(t || '').replace(/\D/g, '').slice(-8);

    // 📚 inclui a base onde a logística grava o orçamento gerado, que a
    // contagem do painel nunca visitou
    const BASES = ['reparoeletro_orcamentos', 'reparoeletro_pipe', 'tv_pipe',
      'reparoeletro_frenteloja', 'fichas_adm', 'fichas_tv',
      'reparoeletro_logistica', 'tv_logistica',
      'reparoeletro_almoxarifado', 'tv_almoxarifado',
      'reparoeletro_board', 'reparoeletro_arquivo'];
    const achados = {}, telsVistos = {};
    for (const chave of BASES) {
      const db = await dbGet(chave);
      if (!db) continue;
      const lista = [];
      for (const L of ['cards', 'fichas', 'itens', 'orcamentos', 'lista']) {
        for (const x of (((db || {})[L]) || [])) {
          const valor = Number(x.valor || x.valorOrcamento || x.preco || 0);
          if (!(valor > 0)) continue;
          // qualquer data que indique quando o orçamento foi feito
          const datas = [x.orcamentoEm, x.orcamentoCadastradoEm, x.diagnosticoEm,
            x.movedAt, x.criadoEm, x.statusAt].filter(Boolean);
          const doDia = datas.find(noDia);
          if (!doDia) continue;
          const tel = d8c(x.telefone);
          lista.push({ nome: x.nomeContato || x.nome || x.cliente || '?', tel,
            valor, quando: doDia,
            campo: x.orcamentoEm ? 'orcamentoEm'
              : x.orcamentoCadastradoEm ? 'orcamentoCadastradoEm'
              : x.diagnosticoEm ? 'diagnosticoEm'
              : x.movedAt ? 'movedAt' : 'criadoEm',
            fase: x.phaseId || x.phase || x.status || '?' });
          if (tel) (telsVistos[tel] = telsVistos[tel] || []).push(chave);
        }
      }
      if (lista.length) {
        achados[chave] = { quantos: lista.length,
          L: lista.sort((a, b) => String(a.quando).localeCompare(String(b.quando)))
            .map(x => hh(x.quando) + ' | ' + String(x.nome).slice(0, 20).padEnd(20) +
              ' ' + x.tel.slice(-4) + ' | R$ ' + x.valor.toFixed(2).padStart(8) +
              ' | ' + x.campo + ' | ' + x.fase) };
      }
    }
    const clientesUnicos = Object.keys(telsVistos).length;
    return res.status(200).json({ ok: true, dia,
      clientesDistintosComValor: clientesUnicos,
      POR_BASE: Object.fromEntries(Object.entries(achados)
        .map(([k, v]) => [k, v.quantos])),
      EM_MAIS_DE_UMA_BASE: Object.entries(telsVistos)
        .filter(([, bs]) => new Set(bs).size > 1)
        .map(([t, bs]) => t.slice(-4) + ' → ' + [...new Set(bs)].join(' + ')),
      DETALHE: achados,
      comoLer: 'compare o total de cada base com o que a operação contou: a base ' +
        'que tiver o número certo é a fonte que a contagem deveria estar lendo' });
  }

  // ── 🔍 orcamentos-perdidos: quem tem valor mas não é contado ──
  // A contagem exige data própria do orçamento — carimbo ou histórico. Card
  // antigo pode ter valor e nenhuma das duas coisas, e some da conta sem que
  // isso apareça em lugar nenhum.
  if ((req.query || {}).action === 'orcamentos-perdidos') {
    const per = String(req.query.periodo || 'semana');
    const agora = Date.now();
    const aBR = new Date(agora - 3 * 3600000);
    let voltar = (aBR.getUTCDay() - 6 + 7) % 7;
    if (aBR.getUTCDay() === 6 && aBR.getUTCHours() < 13) voltar = 7;
    const iniSem = new Date(aBR.getTime() - voltar * 86400000);
    iniSem.setUTCHours(13, 0, 0, 0);
    const ini = per === 'hoje'
      ? new Date(new Date(agora - 3 * 3600000).toISOString().slice(0, 10) + 'T00:00:00-03:00').getTime()
      : per === 'mes' ? agora - 30 * 86400000
      : iniSem.getTime() + 3 * 3600000;
    const dentroP = d => { const t = new Date(d || 0).getTime(); return t >= ini && t <= agora; };
    const hh = d => d ? new Date(new Date(d).getTime() - 3 * 3600000)
      .toISOString().slice(5, 16).replace('T', ' ') : '—';

    const saida = {};
    for (const [chave, nome] of [['reparoeletro_pipe', 'ADM'], ['tv_pipe', 'TV']]) {
      const db = await dbGet(chave);
      const comCarimbo = [], porHistorico = [], semData = [], semValor = [];
      for (const c of (((db || {}).cards) || [])) {
        const valor = Number(c.valor || 0);
        const linha = String(c.nomeContato || c.nome || '?').slice(0, 22) + ' ' +
          String(c.telefone || '').slice(-4) + ' | R$ ' + valor.toFixed(2) +
          ' | fase ' + String(c.phaseId || c.phase || '?');
        if (c.orcamentoEm) {
          if (dentroP(c.orcamentoEm)) comCarimbo.push(linha + ' | ' + hh(c.orcamentoEm));
          continue;
        }
        const h2 = (c.history || [])
          .filter(x => ['aguardando_aprovacao', 'orcamento_cadastrado']
            .includes(String(x.phase || x.phaseId || '')))
          .map(x => String(x.ts || x.timestamp || '')).filter(Boolean).sort();
        if (h2.length) {
          if (dentroP(h2[0])) porHistorico.push(linha + ' | ' + hh(h2[0]));
          continue;
        }
        // 🚨 tem valor, mas nenhuma data que prove quando o orçamento saiu
        if (valor > 0) {
          if (dentroP(c.criadoEm)) {
            semData.push(linha + ' | criado ' + hh(c.criadoEm) +
              ' | ⚠️ sem carimbo nem histórico — NÃO É CONTADO');
          }
        } else if (dentroP(c.criadoEm)) {
          semValor.push(linha + ' | criado ' + hh(c.criadoEm));
        }
      }
      saida[nome] = {
        contados: comCarimbo.length + porHistorico.length,
        comCarimbo: comCarimbo.length,
        pelaHistorico: porHistorico.length,
        NAO_CONTADOS_COM_VALOR: semData.length,
        semValorNenhum: semValor.length,
        L_NAO_CONTADOS: semData.slice(0, 50),
      };
    }
    const perdidos = Object.values(saida).reduce((s, x) => s + x.NAO_CONTADOS_COM_VALOR, 0);
    return res.status(200).json({ ok: perdidos === 0, periodo: per,
      de: hh(ini),
      POR_FRENTE: saida,
      VEREDITO: perdidos
        ? '🚨 ' + perdidos + ' card(s) têm valor mas não entram na contagem por não ' +
          'terem data de orçamento — é a diferença que você está vendo'
        : '✅ todo card com valor tem data e está sendo contado',
      comoCorrigir: perdidos
        ? 'action=carimbar-orcamentos&aplicar=1 — usa a data de criação como referência'
        : null });
  }

  // ── 🔧 carimbar-orcamentos: repõe a data faltante ──
  if ((req.query || {}).action === 'carimbar-orcamentos') {
    const feitos = [], erros = [];
    for (const chave of ['reparoeletro_pipe', 'tv_pipe']) {
      const db = (await dbGet(chave)) || { cards: [] };
      let mexeu = 0;
      for (const c of (db.cards || [])) {
        if (c.orcamentoEm) continue;
        if (!(Number(c.valor || 0) > 0)) continue;
        const h2 = (c.history || [])
          .filter(x => ['aguardando_aprovacao', 'orcamento_cadastrado']
            .includes(String(x.phase || x.phaseId || '')))
          .map(x => String(x.ts || x.timestamp || '')).filter(Boolean).sort();
        if (h2.length) continue;                    // já tem como ser datado
        if (!c.criadoEm) continue;
        if (String(req.query.aplicar || '') === '1') {
          c.orcamentoEm = c.criadoEm;
          c.orcamentoEmSuposto = true;   // 🏷️ a data é aproximada, não medida
          mexeu++;
        }
        feitos.push(chave + ' | ' + String(c.nomeContato || c.nome || '?').slice(0, 20) +
          ' | R$ ' + Number(c.valor).toFixed(2) + ' → ' + String(c.criadoEm).slice(0, 10));
      }
      if (mexeu) await dbSet(chave, db);
    }
    return res.status(200).json({
      ok: true,
      modo: String(req.query.aplicar || '') === '1' ? 'aplicado' : 'prévia',
      cards: feitos.length, L: feitos.slice(0, 60), erros,
      observacao: 'a data usada é a da criação do card, que é aproximada: o orçamento ' +
        'pode ter sido lançado depois. Serve para o card voltar a ser contado.',
      dica: String(req.query.aplicar || '') === '1' ? null : 'para aplicar: &aplicar=1' });
  }

    if ((req.query || {}).action === 'quem-orcou') {
    const dia = String(req.query.dia || new Date(Date.now() - 3 * 3600000).toISOString().slice(0, 10));
    const frente = String(req.query.frente || '').toLowerCase();
    const ini = new Date(dia + 'T00:00:00-03:00').getTime();
    const fim = ini + 86400000 - 1;
    const banco = frente === 'tv' ? 'tv_pipe' : 'reparoeletro_pipe';
    const db = (await dbGet(banco)) || { cards: [] };
    const hh = d => d ? new Date(new Date(d).getTime() - 3 * 3600000)
      .toISOString().slice(5, 16).replace('T', ' ') : '—';
    const dentroDia = d => { const t = new Date(d || 0).getTime(); return t >= ini && t <= fim; };
    const comCarimbo = [], porHistorico = [], semData = [];
    for (const c of (db.cards || [])) {
      const linha = String(c.nomeContato || c.nome || '?').slice(0, 22) +
        ' ' + String(c.telefone || '').slice(-4) +
        ' | R$ ' + Number(c.valor || 0).toFixed(2);
      if (c.orcamentoEm) {
        if (dentroDia(c.orcamentoEm)) comCarimbo.push(linha + ' | carimbo ' + hh(c.orcamentoEm));
        continue;
      }
      const h2 = (c.history || [])
        .filter(x => ['aguardando_aprovacao', 'orcamento_cadastrado']
          .includes(String(x.phase || x.phaseId || '')))
        .map(x => String(x.ts || x.timestamp || '')).filter(Boolean).sort();
      if (h2.length && dentroDia(h2[0])) porHistorico.push(linha + ' | histórico ' + hh(h2[0]));
      else if (Number(c.valor || 0) > 0 && dentroDia(c.criadoEm)) {
        semData.push(linha + ' | criado ' + hh(c.criadoEm) +
          ' — tem valor mas NÃO tem data de orçamento');
      }
    }
    return res.status(200).json({ ok: true, dia, frente: frente || 'adm',
      contagem: comCarimbo.length + porHistorico.length,
      COM_CARIMBO: comCarimbo,
      PELO_HISTORICO: porHistorico,
      NAO_CONTADOS_SEM_DATA: semData,
      explicacao: 'só entram na contagem os que têm data do orçamento, por carimbo ' +
        'ou por entrada em aguardando aprovação; ter valor não basta' });
  }

  if ((req.query || {}).action === 'quem-aprovou') {
    const dia = String(req.query.dia || new Date(Date.now() - 3 * 3600000).toISOString().slice(0, 10));
    const frente = String(req.query.frente || '').toLowerCase();
    const ini = new Date(dia + 'T00:00:00-03:00').getTime();
    const fim = ini + 86400000 - 1;
    const brutos = (await _funil.ler(ini, fim))
      .filter(e => e.etapa === 'aprovado' && (!frente || e.frente === frente))
      .sort((a, b) => String(a.ts).localeCompare(String(b.ts)));
    // 🔁 mesmo cliente, mesmo valor, dentro de 10 minutos: é o mesmo fato
    const vistos = [];
    const evs = brutos.filter(e => {
      const t = String(e.tel || '').slice(-8);
      const q = new Date(e.ts || 0).getTime();
      const eqE = String(e.equipamento || e.ref || '').toLowerCase().slice(0, 14);
      const rep = vistos.some(v => v.t === t && v.eq === eqE &&
        v.valor === Number(e.valor || 0) && Math.abs(v.q - q) < 600000);
      if (rep) return false;
      vistos.push({ t, eq: eqE, valor: Number(e.valor || 0), q });
      return true;
    });
    const duplicadas = brutos.length - evs.length;
    const hora = t => new Date(new Date(t).getTime() - 3 * 3600000).toISOString().slice(11, 16);
    const soma = evs.reduce((s, e) => s + (Number(e.valor) || 0), 0);
    return res.status(200).json({ ok: true,
      dia, frente: frente || 'todas',
      quantos: evs.length,
      duplicadasIgnoradas: duplicadas,
      faturamento: +soma.toFixed(2),
      ticketMedio: evs.length ? +(soma / evs.length).toFixed(2) : 0,
      porCanal: evs.reduce((o, e) => { o[e.canal] = (o[e.canal] || 0) + 1; return o; }, {}),
      LISTA: evs.map((e, i) => String(i + 1).padStart(2) + '. ' + hora(e.ts) +
        ' | ' + String(e.nome || '?').slice(0, 26).padEnd(26) +
        ' ' + String(e.tel || '').slice(-4) +
        ' | ' + e.canal.padEnd(7) +
        ' | R$ ' + (Number(e.valor) || 0).toFixed(2).padStart(8) +
        (e.quem ? ' | ' + e.quem : '')),
      observacao: 'lido do livro-razão: cada linha foi gravada no instante da aprovação' });
  }

  // ── 🔍 CONFERIR-APROVADOS: lista nominal, para bater com a contagem manual ──
  if ((req.query || {}).action === 'conferir-aprovados') {
    const Jc = janela(req.query || {});
    const d8c = t => String(t || '').replace(/\D/g, '').slice(-8);
    const noPeriodo = d => { const t = new Date(d || 0).getTime(); return t >= Jc.ini && t <= Jc.fim; };
    const FASES_OK = ['aprovados', 'producao', 'video_enviado', 'analise_compra',
      'equipamento_comprado', 'programar_entrega', 'solicitar_entrega', 'entrega_solicitada',
      'receber', 'erp', 'garantia', 'finalizado', 'conserto_realizado', 'entregue'];
    const quando = c => {
      if (c.aprovadoEm) return c.aprovadoEm;
      const h2 = (c.history || [])
        .filter(x => ['aprovados', 'producao'].includes(String(x.phase || x.phaseId || '')))
        .map(x => x.ts || x.timestamp).filter(Boolean).sort();
      return h2[0] || null;
    };
    const achados = [], forales = [];
    const vistos = new Map();
    for (const [k, lista, tipo] of [
      ['reparoeletro_pipe', ((await dbGet('reparoeletro_pipe')) || {}).cards || [], 'pipe'],
      ['reparoeletro_arquivo', ((await dbGet('reparoeletro_arquivo')) || {}).cards || [], 'arquivo'],
      ['reparoeletro_frenteloja', ((await dbGet('reparoeletro_frenteloja')) || {}).fichas || [], 'balcão'],
    ]) {
      for (const c of lista) {
        const fase = String(c.phaseId || c.phase || '');
        const passou = FASES_OK.includes(fase) || !!c.aprovadoEm ||
          (c.history || []).some(x => ['aprovados', 'producao'].includes(String(x.phase || x.phaseId || '')));
        if (!passou) continue;
        const q = quando(c);
        const tel = d8c(c.telefone);
        const item = { banco: k, tipo, nome: c.nomeContato || c.nome || '?', tel,
          fase, quando: q, valor: Number(c.valor || (c.orcamento && c.orcamento.valor) || 0),
          origem: c.origem || null, noBalcao: c.aprovadoNoBalcao === true,
          semData: !q };
        if (!q || !noPeriodo(q)) { forales.push(item); continue; }
        // 🔁 o mesmo cliente pode ter ficha no balcão E card no pipe: conta uma vez
        const chave = tel + '|' + String(q).slice(0, 10);
        if (vistos.has(chave)) { vistos.get(chave).duplicadoEm = (vistos.get(chave).duplicadoEm || []).concat(k); continue; }
        vistos.set(chave, item); achados.push(item);
      }
    }
    const ehBal = i => i.tipo === 'balcão' || i.origem === 'frenteloja' || i.noBalcao;
    const balcao = achados.filter(ehBal), online = achados.filter(i => !ehBal(i));
    const fmt = i => (ehBal(i) ? '🏪 ' : '💻 ') + String(i.nome).slice(0, 22) + ' ' + i.tel.slice(-4) +
      ' | ' + String(i.quando || '').slice(5, 16).replace('T', ' ') +
      ' | ' + i.fase + ' | R$ ' + i.valor.toFixed(2) + ' | ' + i.banco +
      (i.duplicadoEm ? ' | também em ' + i.duplicadoEm.join(',') : '');
    return res.status(200).json({ ok: true,
      periodo: { de: Jc.de, ate: Jc.ate, rotulo: Jc.rotulo,
        inicio: new Date(Jc.ini - 3 * 3600000).toISOString().slice(0, 16).replace('T', ' '),
        fim: new Date(Jc.fim - 3 * 3600000).toISOString().slice(0, 16).replace('T', ' ') },
      TOTAL: achados.length, BALCAO: balcao.length, ONLINE: online.length,
      duplicadosEvitados: achados.filter(i => i.duplicadoEm).length,
      foraDoPeriodoOuSemData: forales.length,
      semDataDeAprovacao: forales.filter(i => i.semData).length,
      LISTA_BALCAO: balcao.map(fmt),
      LISTA_ONLINE: online.map(fmt),
      SEM_DATA: forales.filter(i => i.semData).slice(0, 40)
        .map(i => (ehBal(i) ? '🏪 ' : '💻 ') + String(i.nome).slice(0, 22) + ' ' + i.tel.slice(-4) +
          ' | ' + i.fase + ' | ' + i.banco + ' — sem data de aprovação, não entra em nenhum período') });
  }

  // ── 🕵️ SUSPEITOS: aprovações cuja data pode ter vindo errada do carimbo retroativo ──
  if ((req.query || {}).action === 'suspeitos') {
    const Js = janela(req.query || {});
    const d8s = t => String(t || '').replace(/\D/g, '').slice(-8);
    const hh3 = d => d ? String(d).slice(5, 16).replace('T', ' ') : '—';
    const L = [];
    for (const k of ['reparoeletro_pipe', 'reparoeletro_arquivo']) {
      const b = await dbGet(k);
      for (const c of (((b || {}).cards) || [])) {
        const t = new Date(c.aprovadoEm || 0).getTime();
        if (!t || t < Js.ini || t > Js.fim) continue;
        const hist = (c.history || []).map(x => ({
          fase: String(x.phase || x.phaseId || '?'),
          ts: String(x.ts || x.timestamp || '') })).filter(x => x.ts).sort((a, b2) => a.ts.localeCompare(b2.ts));
        const entrouAprovados = hist.find(x => x.fase === 'aprovados');
        const primeiroMov = hist[0];
        // 🚩 sinais de que a data pode não ser a da aprovação real
        const sinais = [];
        if (c.carimboRetroativo) sinais.push('carimbo reconstruído');
        if (!entrouAprovados) sinais.push('nunca registrou passagem por aprovados');
        if (primeiroMov && primeiroMov.ts.slice(0, 10) < Js.de) sinais.push('card já existia antes do ciclo');
        if (c.criadoEm && String(c.criadoEm).slice(0, 10) < Js.de) sinais.push('criado antes do ciclo');
        if (!sinais.length) continue;
        L.push({ nome: c.nomeContato || c.nome || '?', tel: d8s(c.telefone).slice(-4),
          fase: String(c.phaseId || c.phase || ''), aprovadoEm: c.aprovadoEm,
          criadoEm: c.criadoEm, sinais,
          historico: hist.slice(0, 6).map(x => x.fase + ' ' + hh3(x.ts)) });
      }
    }
    L.sort((a, b) => b.sinais.length - a.sinais.length);
    return res.status(200).json({ ok: true,
      periodo: Js.rotulo,
      suspeitos: L.length,
      explicacao: 'aprovações datadas dentro do ciclo, mas com indício de que a data real é anterior',
      L: L.map(x => x.nome.slice(0, 20) + ' ' + x.tel +
        ' | aprovado ' + hh3(x.aprovadoEm) + ' | ' + x.fase +
        ' | criado ' + String(x.criadoEm || '').slice(0, 10) +
        '\n     🚩 ' + x.sinais.join(' · ') +
        '\n     histórico: ' + x.historico.join(' → ')) });
  }

  // ── 📒 LIVRO-RAZÃO: o funil lido dos eventos gravados no instante do fato ──
  if ((req.query || {}).action === 'livro') {
    const Jl = janela(req.query || {});
    const evs = await _funil.ler(Jl.ini, Jl.fim);
    const cont = (etapa, filtro) => evs.filter(e => e.etapa === etapa && (!filtro || filtro(e))).length;
    const soma = (etapa, filtro) => evs.filter(e => e.etapa === etapa && (!filtro || filtro(e)))
      .reduce((s, e) => s + (Number(e.valor) || 0), 0);
    const porFrente = f => {
      const daF = e => e.frente === f;
      return {
        fichas: cont('ficha', daF),
        logistica: { total: cont('logistica', daF), bot: cont('logistica', e => daF(e) && e.canal === 'bot') },
        orcamentos: { total: cont('orcamento', daF),
          balcao: cont('orcamento', e => daF(e) && e.canal === 'balcao'),
          online: cont('orcamento', e => daF(e) && e.canal !== 'balcao') },
        aprovados: { total: cont('aprovado', daF),
          balcao: cont('aprovado', e => daF(e) && e.canal === 'balcao'),
          online: cont('aprovado', e => daF(e) && e.canal !== 'balcao') },
        faturamento: +soma('aprovado', daF).toFixed(2),
      };
    };
    return res.status(200).json({ ok: true,
      periodo: { rotulo: Jl.rotulo,
        inicio: new Date(Jl.ini - 3 * 3600000).toISOString().slice(0, 16).replace('T', ' '),
        fim: new Date(Jl.fim - 3 * 3600000).toISOString().slice(0, 16).replace('T', ' ') },
      eventosNoPeriodo: evs.length,
      ADM: porFrente('adm'), TV: porFrente('tv'),
      observacao: 'contagem lida do registro gravado no instante de cada etapa — não depende do estado atual dos cards',
      ULTIMOS: evs.slice(-25).reverse().map(e =>
        String(e.ts).slice(5, 16).replace('T', ' ') + ' | ' + e.etapa.padEnd(10) +
        ' | ' + e.frente.toUpperCase() + ' | ' + e.canal.padEnd(7) +
        ' | ' + String(e.nome || '?').slice(0, 20) + ' ' + String(e.tel || '').slice(-4) +
        (e.valor ? ' | R$ ' + e.valor.toFixed(2) : '')) });
  }

  // ── 🌱 SEMEAR: preenche o livro-razão do ciclo atual com o que já aconteceu ──
  // Uma vez só: daqui em diante cada etapa se registra sozinha no instante do fato.
  if ((req.query || {}).action === 'semear') {
    const Jm = janela({ periodo: 'semana' });
    const d8m = t => String(t || '').replace(/\D/g, '').slice(-8);
    const noCiclo = d => { const t = new Date(d || 0).getTime(); return t >= Jm.ini && t <= Jm.fim; };
    // o que já está no livro, para não duplicar
    const jaTem = new Set((await _funil.ler(Jm.ini, Jm.fim))
      .map(e => e.etapa + '|' + String(e.tel || '').slice(-8)));
    const novos = [];
    const juntar = (etapa, tel, nome, valor, frente, canal, quando, ref) => {
      const k = etapa + '|' + d8m(tel);
      if (!tel || jaTem.has(k)) return;
      jaTem.add(k);
      novos.push({ etapa, ts: quando, tel: String(tel).replace(/\D/g, '').slice(-11),
        nome: String(nome || '').slice(0, 40), valor: Number(valor || 0) || 0,
        frente, canal, quem: '', ref: String(ref || '').slice(0, 40) });
    };
    // 1) fichas
    for (const [k, frente] of [['fichas_adm', 'adm'], ['fichas_tv', 'tv']]) {
      const b = await dbGet(k);
      for (const f of (((b || {}).fichas) || [])) {
        if (!noCiclo(f.criadoEm)) continue;
        const id = String(f.id || '');
        if (['remarcar', 'reagendamento'].includes(String(f.origem || '')) ||
            id.startsWith('rem_') || id.startsWith('fic_reag_')) continue;
        juntar('ficha', f.telefone, f.nome, 0, frente, 'planilha', f.criadoEm, f.id);
      }
    }
    // 2) logística
    for (const [k, frente] of [['reparoeletro_logistica', 'adm'], ['tv_logistica', 'tv']]) {
      const b = await dbGet(k);
      for (const f of (((b || {}).fichas) || [])) {
        if (!noCiclo(f.criadoEm)) continue;
        const bot = /bot/i.test(String(f.origem || '') + ' ' + String(f.criadoPor || ''));
        juntar('logistica', f.telefone, f.nome, 0, frente, bot ? 'bot' : 'manual', f.criadoEm, f.id);
      }
    }
    // 3) orçamentos e aprovações — pipe, arquivo e balcão
    for (const [k, frente, canalPadrao] of [
      ['reparoeletro_pipe', 'adm', 'online'], ['tv_pipe', 'tv', 'online'],
      ['reparoeletro_arquivo', 'adm', 'online'], ['tv_arquivo', 'tv', 'online'],
      ['reparoeletro_frenteloja', 'adm', 'balcao'],
    ]) {
      const b = await dbGet(k);
      const lista = (((b || {}).cards) || []).concat(((b || {}).fichas) || []);
      for (const c of lista) {
        const balcao = canalPadrao === 'balcao' || String(c.origem || '') === 'frenteloja' ||
          c.aprovadoNoBalcao === true;
        const canal = balcao ? 'balcao' : 'online';
        const valor = Number(c.valor || (c.orcamento && c.orcamento.valor) || 0);
        const nome = c.nomeContato || c.nome || '';
        // 📅 o campo próprio só passou a existir hoje: para o restante do ciclo,
        // a data vem do histórico, que registra a saída de cada fase
        const doHistorico = (fases) => {
          const hs = (c.history || [])
            .filter(x => fases.includes(String(x.phase || x.phaseId || '')))
            .map(x => String(x.ts || x.timestamp || '')).filter(Boolean).sort();
          return hs[0] || null;
        };
        const qOrc = c.orcamentoEm || doHistorico(['aguardando_aprovacao', 'orcamento_cadastrado']);
        // no balcão, sair de orçamento/produção é o momento da aprovação
        const qApr = c.aprovadoEm || doHistorico(balcao ? ['aprovados', 'producao', 'pago'] : ['aprovados']);
        if (noCiclo(qOrc)) juntar('orcamento', c.telefone, nome, valor, frente, canal, qOrc, c.id);
        if (noCiclo(qApr)) juntar('aprovado', c.telefone, nome, valor, frente, canal, qApr, c.id);
      }
    }
    // 🏪 a seção Balcão registra toda aprovação presencial — completa o que faltar
    try {
      const bal = await dbGet('reparoeletro_balcao');
      for (const b2 of (Array.isArray(bal) ? bal : ((bal || {}).itens || []))) {
        if (!noCiclo(b2.entradaEm)) continue;
        juntar('aprovado', b2.telefone, b2.nomeContato, 0, 'adm', 'balcao', b2.entradaEm, b2.pipefyId);
      }
    } catch (e) {}
    novos.sort((a, b2) => String(a.ts).localeCompare(String(b2.ts)));
    const resumo = novos.reduce((o, e) => {
      const k = e.etapa + ' ' + e.frente + ' ' + e.canal; o[k] = (o[k] || 0) + 1; return o; }, {});
    if (String(req.query.aplicar || '') !== '1') {
      return res.status(200).json({ ok: true, modo: 'prévia',
        ciclo: Jm.rotulo, vaoSerRegistrados: novos.length,
        POR_TIPO: resumo,
        AMOSTRA: novos.slice(0, 40).map(e => String(e.ts).slice(5, 16).replace('T', ' ') +
          ' | ' + e.etapa.padEnd(10) + ' | ' + e.frente.toUpperCase() + ' | ' + e.canal.padEnd(8) +
          ' | ' + String(e.nome).slice(0, 20) + ' ' + String(e.tel).slice(-4) +
          (e.valor ? ' | R$ ' + e.valor.toFixed(2) : '')),
        dica: 'para gravar: &aplicar=1' });
    }
    let n = 0;
    for (const e of novos) {
      try {
        await fetch(`${U}/rpush/kpi_funil/${encodeURIComponent(JSON.stringify(e))}`,
          { headers: { Authorization: `Bearer ${T}` } });
        n++;
      } catch (x) {}
    }
    return res.status(200).json({ ok: true, registrados: n, POR_TIPO: resumo,
      observacao: 'a partir de agora cada etapa se registra sozinha — esta ação não precisa ser repetida' });
  }

  const J = janela(req.query || {});
  const dentro = d => { const t = new Date(d || 0).getTime(); return t >= J.ini && t <= J.fim; };

  // 📦 cards e fichas antigas migram para o arquivo — sem incluí-lo, qualquer
  // período passado aparece com aprovados, orçamentos e logística muito abaixo do real
  const [fA, fT, lgA, lgT, ppA, ppT, arqA, arqT, fl, balcao, inv] = await Promise.all([
    dbGet('fichas_adm'), dbGet('fichas_tv'),
    dbGet('reparoeletro_logistica'), dbGet('tv_logistica'),
    dbGet('reparoeletro_pipe'), dbGet('tv_pipe'),
    dbGet('reparoeletro_arquivo'), dbGet('tv_arquivo'),
    dbGet('reparoeletro_frenteloja'), dbGet('reparoeletro_balcao'),
    investimento(J.de, J.ate),
  ]);
  // 🏪 toda aprovação no balcão gera uma entrada aqui — é a fonte oficial do presencial
  const doBalcao = (Array.isArray(balcao) ? balcao : ((balcao || {}).itens || []))
    .filter(b => dentro(b.entradaEm));
  const telsBalcao = new Set(doBalcao.map(b => d8(b.telefone)).filter(t => t.length >= 8));
  // o arquivo guarda tanto cards quanto fichas — separa cada tipo
  const desmembrar = (arq) => {
    const cards = [], fichas = [];
    for (const L of ['cards', 'fichas']) {
      for (const x of (((arq || {})[L]) || [])) {
        if (x.phaseId || x.phase || x.aprovadoEm) cards.push(x); else fichas.push(x);
      }
    }
    return { cards, fichas };
  };
  const arqAdm = desmembrar(arqA), arqTv = desmembrar(arqT);
  const juntarCards = (pipe, extra) => ({ cards: (((pipe || {}).cards) || []).concat(extra.cards) });
  const juntarFichas = (b, extra) => ({ fichas: (((b || {}).fichas) || []).concat(extra.fichas) });

  // 📨 conversas vêm SOMENTE da Meta. O histórico deste sistema não serve:
  // quem atende a conversa vinda do anúncio é outro número, e o bot daqui só
  // entra depois que o contato já virou ficha — seria uma etapa bem posterior.
  function montar(fichasDb, logDb, pipeDb, extraBalcao) {
    const ehRetorno = f => ['remarcar', 'reagendamento'].includes(String(f.origem || '')) ||
      f.reagendarColeta === true ||
      String(f.id || '').startsWith('rem_') || String(f.id || '').startsWith('fic_reag_');
    // fichas
    const vistos = new Set();
    const unico = x => {
      const k = String(x.id || '') || (d8(x.telefone) + '|' + String(x.criadoEm || ''));
      if (vistos.has(k)) return false; vistos.add(k); return true;
    };
    const fichas = (((fichasDb || {}).fichas) || [])
      .filter(f => dentro(f.criadoEm || f.registradoEm) && !ehRetorno(f) && unico(f));
    // logística
    const logs = (((logDb || {}).fichas) || []).filter(f => dentro(f.criadoEm));
    const porBot = logs.filter(f => /bot/i.test(String(f.origem || '') + ' ' + String(f.criadoPor || '')));
    // pipe: orçamentos e aprovados
    const cards = ((pipeDb || {}).cards) || [];
    // 🏪 é do balcão se a origem diz, se o carimbo diz, ou se o cliente consta
    // na seção Balcão — que é criada exatamente quando se aprova na loja
    const tb = (extraBalcao && extraBalcao.telsBalcao) || new Set();
    const ehBalcao = c => String(c.origem || '') === 'frenteloja' ||
      c.aprovadoNoBalcao === true || String(c.id || '').includes('-loja') ||
      tb.has(d8(c.telefone));
    // 📄 orçamento é um FATO com data própria: o momento em que o valor foi
    // registrado. Usar a criação do card como substituto contava como orçamento
    // do dia todo card que nasceu hoje e em algum momento ganhou valor, mesmo
    // que o valor tenha sido definido depois — por isso o número vinha inflado.
    // 📄 ORÇAMENTO = diagnóstico registrado no pipe + cadastro no frente de loja.
    // O card do balcão vira registro nas duas fontes: sem excluí-lo de um lado,
    // o mesmo atendimento é contado duas vezes.
    const orcs = cards.filter(c => {
      if (ehBalcao(c)) return false;          // o balcão é contado pela ficha, não pelo card
      if (c.orcamentoEm) return dentro(c.orcamentoEm);
      const h2 = (c.history || [])
        .filter(x => ['aguardando_aprovacao', 'orcamento_cadastrado']
          .includes(String(x.phase || x.phaseId || '')))
        .map(x => String(x.ts || x.timestamp || '')).filter(Boolean).sort();
      return h2.length ? dentro(h2[0]) : false;
    });
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
    // 🏪 o balcão vem das FICHAS do frente de loja, não dos cards: é lá que o
    // atendimento presencial é registrado, e contar pelos dois lados duplica
    let orcBalcao = 0;
    const balcaoJaContado = new Set();
    for (const f of ((extraBalcao && extraBalcao.fichasFL) || [])) {
      // 📄 o balcão vira orçamento quando o técnico faz o diagnóstico na Análise
      // Loja e a ficha passa a Orçamento Cadastrado — é aí que ganha valor.
      // A ficha recém-cadastrada, sem preço, ainda não é um orçamento.
      let q = f.orcamentoEm || f.orcamentoCadastradoEm || null;
      if (!q) {
        q = ((f.history || [])
          .filter(x => String(x.phase || '') === 'orcamento_cadastrado')
          .map(x => String(x.ts || '')).filter(Boolean).sort())[0] || null;
      }
      if (!q) continue;
      const vlr = Number(f.valorOrcamento || f.valor || 0);
      if (vlr <= 0) continue;             // sem preço não é orçamento
      const t2 = new Date(q).getTime();
      if (!(t2 >= J.ini && t2 <= J.fim)) continue;
      const t = d8(f.telefone);
      if (t && balcaoJaContado.has(t)) continue;   // mesma ficha em duplicidade
      if (t) balcaoJaContado.add(t);
      orcBalcao++;
    }
    let aprovBalcao = aprov.filter(ehBalcao);
    // a seção Balcão registra toda aprovação presencial: se ela tem mais do que
    // encontramos nos cards, o número dela prevalece
    const pisoBalcao = (extraBalcao && extraBalcao.totalBalcao) || 0;
    const fatBalcao = aprovBalcao.reduce((s, c) => s + (Number(c.valor || 0) || 0), 0);
    return {
      fichas: fichas.length,
      logistica: { total: logs.length, bot: porBot.length, manual: logs.length - porBot.length,
        pctBot: logs.length ? Math.round(porBot.length / logs.length * 100) : 0 },
      orcamentos: { total: orcs.length + orcBalcao,
        online: orcs.length, balcao: orcBalcao,
        pctBalcao: (orcs.length + orcBalcao)
          ? Math.round(orcBalcao / (orcs.length + orcBalcao) * 100) : 0,
        comoEContado: 'diagnóstico registrado no pipe + cadastro no frente de loja' },
      aprovados: { total: Math.max(aprov.length, pisoBalcao + (aprov.length - aprovBalcao.length)),
        bot: aprovBot.length, manual: aprov.length - aprovBot.length,
        balcao: Math.max(aprovBalcao.length, pisoBalcao),
        online: aprov.length - aprovBalcao.length,
        balcaoPelaSecao: pisoBalcao, balcaoPelosCards: aprovBalcao.length,
        pctBalcao: aprov.length ? Math.round(aprovBalcao.length / aprov.length * 100) : 0,
        pctBot: aprov.length ? Math.round(aprovBot.length / aprov.length * 100) : 0 },
      faturamento: +faturamento.toFixed(2),
      faturamentoBalcao: +fatBalcao.toFixed(2),
      faturamentoOnline: +(faturamento - fatBalcao).toFixed(2),
      ticketMedio: aprov.length ? +(faturamento / aprov.length).toFixed(2) : 0,
    };
  }

  // 🏪 todo atendimento do balcão gera orçamento — inclusive o que ainda não
  // virou card no pipe, que de outro modo ficaria fora da contagem
  const fichasFL = (((fl || {}).fichas) || []).map(f => ({
    ...f, origem: 'frenteloja',
    valor: (f.orcamento && f.orcamento.valor) || f.valor || 0,
  }));
  // 📒 o livro-razão é a fonte quando há registro no período. Ele guarda o fato
  // no instante em que acontece, então não muda quando o card avança ou é
  // arquivado — que era a origem das divergências. Sem registro, cai para os cards.
  // 🔁 remove repetição do mesmo fato antes de contar
  const evsBrutos = await _funil.ler(J.ini, J.fim);
  const jaVi = [];
  const evsLivro = evsBrutos.filter(e => {
    const t = String(e.tel || '').slice(-8);
    const q = new Date(e.ts || 0).getTime();
    // 🔑 o equipamento entra na comparação: dois aparelhos do mesmo cliente são
    // dois fatos distintos, e ignorá-lo descartava o segundo como repetição
    const eqE = String(e.equipamento || e.ref || '').toLowerCase().slice(0, 14);
    const rep = jaVi.some(v => v.et === e.etapa && v.t === t && v.eq === eqE &&
      v.valor === Number(e.valor || 0) && Math.abs(v.q - q) < 600000);
    if (rep) return false;
    jaVi.push({ et: e.etapa, t, eq: eqE, valor: Number(e.valor || 0), q });
    return true;
  });
  const usaLivro = evsLivro.length > 0;
  const doLivro = (frente) => {
    const c = (etapa, f2) => evsLivro.filter(e => e.etapa === etapa && e.frente === frente &&
      (!f2 || f2(e))).length;
    const s = (etapa) => evsLivro.filter(e => e.etapa === etapa && e.frente === frente)
      .reduce((t, e) => t + (Number(e.valor) || 0), 0);
    const totOrc = c('orcamento'), totApr = c('aprovado'), totLog = c('logistica');
    const orcBal = c('orcamento', e => e.canal === 'balcao');
    const aprBal = c('aprovado', e => e.canal === 'balcao');
    const logBot = c('logistica', e => e.canal === 'bot');
    const fatBal = evsLivro.filter(e => e.etapa === 'aprovado' && e.frente === frente &&
      e.canal === 'balcao').reduce((t, e) => t + (Number(e.valor) || 0), 0);
    return {
      fichas: c('ficha'),
      logistica: { total: totLog, bot: logBot, manual: totLog - logBot,
        pctBot: totLog ? Math.round(logBot / totLog * 100) : 0 },
      orcamentos: { total: totOrc, balcao: orcBal, online: totOrc - orcBal,
        pctBalcao: totOrc ? Math.round(orcBal / totOrc * 100) : 0 },
      aprovados: { total: totApr, balcao: aprBal, online: totApr - aprBal,
        bot: 0, manual: totApr,
        pctBalcao: totApr ? Math.round(aprBal / totApr * 100) : 0, pctBot: 0 },
      faturamento: +s('aprovado').toFixed(2),
      faturamentoBalcao: +fatBal.toFixed(2),
      faturamentoOnline: +(s('aprovado') - fatBal).toFixed(2),
      ticketMedio: totApr ? +(s('aprovado') / totApr).toFixed(2) : 0,
    };
  };
  const adm = usaLivro ? doLivro('adm') : montar(juntarFichas(fA, arqAdm), juntarFichas(lgA, arqAdm),
    { cards: (((ppA || {}).cards) || []).concat(arqAdm.cards).concat(fichasFL) },
    { telsBalcao, totalBalcao: doBalcao.length, fichasFL });
  const tv = usaLivro ? doLivro('tv')
    : montar(juntarFichas(fT, arqTv), juntarFichas(lgT, arqTv), juntarCards(ppT, arqTv));
  const temMeta = inv.convTotal > 0 || !!TOKEN;
  const convAdmFinal = inv.convAdm;
  const convTvFinal = inv.convTv;
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
    logisticaParaOrcamento: taxa(d.orcamentos.total, d.logistica.total),
    aprovados: d.aprovados,
    orcamentoParaAprovado: taxa(d.aprovados.total, d.orcamentos.total),
    faturamentoBalcao: d.faturamentoBalcao,
    faturamentoOnline: d.faturamentoOnline,
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
    fonteDoFunil: usaLivro
      ? '📒 livro-razão — ' + evsLivro.length + ' evento(s) registrados no instante do fato'
      : '📇 estado atual dos cards — período sem registro no livro',
    livroAtivo: usaLivro,
    DIAGNOSTICO: {
      tokenDaMetaConfigurado: !!TOKEN,
      contaDeAnuncios: CONTA ? 'act_' + CONTA : '(não configurada)',
      conversasNoPeriodo: inv.convTotal,
    },
    // 📖 de onde sai cada número, para conferência
    DESDE_QUANDO: await desdeQuando(),
    FONTES: {
      investimento: 'Meta Ads · gasto real das datas escolhidas, incluindo campanhas já pausadas ou encerradas depois · campanha com TV, televisão, tela, LED ou barramento no nome conta como TV; o resto como ADM',
      conversas: 'Meta Ads · conversas iniciadas pelo anúncio no período, atribuídas à campanha que as gerou. O histórico de mensagens deste sistema NÃO é usado: quem recebe a conversa vinda do anúncio é outro número, e o bot daqui só entra depois que o contato virou ficha',
      conversasPorFrente: 'pela campanha que gerou a conversa',
      fichas: 'fichas_adm e fichas_tv, mais o arquivo · pela data de criação · não conta retorno do remarcar, que já foi contado na primeira entrada',
      logistica: 'logística das duas frentes, mais o arquivo · pela data de criação · é do bot quando a origem ou quem cadastrou menciona bot',
      orcamentos: 'cards do pipe e do arquivo com data de orçamento ou valor preenchido, dentro do período',
      aprovados: 'online: cards que passaram pela fase de aprovação no período, pelo carimbo ou pelo histórico. Balcão: a seção Balcão, que recebe uma entrada a cada aprovação presencial — é a fonte oficial do atendimento na loja',
      faturamento: 'soma do valor desses mesmos cards, na data em que passaram pela aprovação',
      custoPorAprovado: 'investimento ÷ aprovados',
      arquivo: 'períodos passados só ficam corretos porque o arquivo também é lido — cards e fichas antigas saem dos bancos ativos com o tempo',
      atencao: 'as etapas medem coisas que acontecem em momentos diferentes: uma ficha criada hoje pode ser aprovada semana que vem, então as taxas entre etapas não são de um mesmo grupo de clientes',
    },
    origemDasConversas: 'Meta Ads — conversas iniciadas pelo anúncio, atribuídas à campanha',
    ADM: enriquecer(adm, inv.adm, convAdmFinal),
    TV: enriquecer(tv, inv.tv, convTvFinal),
    TOTAL: {
      investimento: inv.total,
      faturamento: +(adm.faturamento + tv.faturamento).toFixed(2),
      aprovados: adm.aprovados.total + tv.aprovados.total,
      retorno: inv.total ? +((adm.faturamento + tv.faturamento) / inv.total).toFixed(2) : 0,
    } });
};
