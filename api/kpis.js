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
    const orcBalcao = orcs.filter(ehBalcao).length;
    let aprovBalcao = aprov.filter(ehBalcao);
    // a seção Balcão registra toda aprovação presencial: se ela tem mais do que
    // encontramos nos cards, o número dela prevalece
    const pisoBalcao = (extraBalcao && extraBalcao.totalBalcao) || 0;
    const fatBalcao = aprovBalcao.reduce((s, c) => s + (Number(c.valor || 0) || 0), 0);
    return {
      fichas: fichas.length,
      logistica: { total: logs.length, bot: porBot.length, manual: logs.length - porBot.length,
        pctBot: logs.length ? Math.round(porBot.length / logs.length * 100) : 0 },
      orcamentos: { total: orcs.length, balcao: orcBalcao, online: orcs.length - orcBalcao,
        pctBalcao: orcs.length ? Math.round(orcBalcao / orcs.length * 100) : 0 },
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
  const adm = montar(juntarFichas(fA, arqAdm), juntarFichas(lgA, arqAdm),
    { cards: (((ppA || {}).cards) || []).concat(arqAdm.cards).concat(fichasFL) },
    { telsBalcao, totalBalcao: doBalcao.length });
  const tv = montar(juntarFichas(fT, arqTv), juntarFichas(lgT, arqTv), juntarCards(ppT, arqTv));
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
