let _gravar = { alterar: async () => ({ ok: false, motivo: 'módulo indisponível' }),
  acrescentar: async () => ({ ok: false, motivo: 'módulo indisponível' }) };
try { _gravar = require('./_gravar'); } catch (e) {
  try { _gravar = require(require('path').join(__dirname, '_gravar.js')); } catch (e2) {}
}
// ═══ CONTROLE DE QUALIDADE — API (beta) ═══
// Fila de inspeção + checklists + aprovação/reprovação + técnicos + certificado

const U = (process.env.UPSTASH_URL || '').replace(/['"]/g, '').trim();
const T = (process.env.UPSTASH_TOKEN || '').replace(/[\n\r'"]/g, '').trim();
const KEY = 'reparoeletro_qualidade';

async function dbGet(k) {
  try {
    const r = await fetch(`${U}/get/${k}`, { headers: { Authorization: `Bearer ${T}` } });
    const j = await r.json();
    return j.result ? JSON.parse(j.result) : null;
  } catch (e) { return null; }
}
async function dbSet(k, v) {
  const r = await fetch(`${U}/set/${k}`, {
    method: 'POST', headers: { Authorization: `Bearer ${T}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(v),
  });
  return (await r.json()).result === 'OK';
}

function defaultDB() {
  return {
    inspecoes: [],
    config: { tecnicos: [], proximoNum: 1 },
  };
}

export default async function handler(req, res) {
  // 🔐 TF-AUTH (Fase 1): chave obrigatória em toda chamada
  const _tfk = (req.query && req.query.k) || req.headers['x-tf-key'] || '';
  if (_tfk !== ((process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim())) {
    return res.status(401).json({ ok: false, error: 'não autorizado' });
  }
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type,x-tf-key');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const action = req.query.action || '';

  // ── 📊 PAINEL: origem, técnicos e fila do setor técnico ──
  if (action === 'painel') {
    const hoje = new Date(Date.now() - 3 * 3600000).toISOString().slice(0, 10);
    const per = String(req.query.periodo || 'hoje');
    const iniPer = per === 'semana' ? Date.now() - 7 * 86400000
      : per === 'mes' ? Date.now() - 30 * 86400000
      : new Date(hoje + 'T00:00:00-03:00').getTime();
    const dentro = d => d && new Date(d).getTime() >= iniPer;
    const hh = d => d ? new Date(new Date(d).getTime() - 3 * 3600000)
      .toISOString().slice(5, 16).replace('T', ' ') : '—';

    const q = (await dbGet('reparoeletro_qualidade')) || { inspecoes: [] };
    const insp = q.inspecoes || [];
    const bd = (await dbGet('reparoeletro_board')) || { cards: [] };

    const origemDe = i => (/garantia/i.test(String(i.origem || '') + ' ' + String(i.tipo || ''))
      ? 'garantia' : (i.avulsa === true || String(i.origem || '') === 'avulsa') ? 'avulsa' : 'tecnico');

    // ── 1) três blocos por origem ──
    const bloco = (nome) => {
      const meus = insp.filter(i => origemDe(i) === nome);
      const entraram = meus.filter(i => dentro(i.criadoEm));
      const aprovadas = meus.filter(i => i.status === 'aprovado' && dentro(i.aprovadoEm));
      const reprovadas = meus.filter(i => i.status === 'reprovado' && dentro(i.reprovadoEm));
      return { entraram: entraram.length, aprovadas: aprovadas.length,
        reprovadas: reprovadas.length,
        aguardando: meus.filter(i => i.status === 'aguardando').length,
        taxaAprovacao: (aprovadas.length + reprovadas.length)
          ? Math.round(aprovadas.length / (aprovadas.length + reprovadas.length) * 100) : null };
    };
    const ORIGENS = { tecnico: bloco('tecnico'), garantia: bloco('garantia'), avulsa: bloco('avulsa') };

    // ── 2) as metas contam SÓ o que vem do setor técnico ──
    // A diária zera todo dia; a semanal acompanha o ciclo comercial, de sábado
    // 13h a sábado 13h, para poder ser lida junto com o resultado da semana.
    const doTecnicoHoje = insp.filter(i => origemDe(i) === 'tecnico' &&
      String(i.criadoEm || '').slice(0, 10) === hoje).length;
    const META = 25;
    const META_SEMANA = 150;
    const aBR = new Date(Date.now() - 3 * 3600000);
    let voltarD = (aBR.getUTCDay() - 6 + 7) % 7;          // dias desde o último sábado
    if (aBR.getUTCDay() === 6 && aBR.getUTCHours() < 13) voltarD = 7;
    const iniSem = new Date(aBR.getTime() - voltarD * 86400000);
    iniSem.setUTCHours(13, 0, 0, 0);
    const iniSemMs = iniSem.getTime() + 3 * 3600000;      // de volta para UTC real
    const doTecnicoSemana = insp.filter(i => origemDe(i) === 'tecnico' &&
      i.criadoEm && new Date(i.criadoEm).getTime() >= iniSemMs).length;
    // 📅 quanto do ciclo já passou, para saber se o ritmo dá conta
    const diasCorridos = Math.max(0.1, (Date.now() - iniSemMs) / 86400000);
    const esperadoAgora = Math.round(META_SEMANA * Math.min(1, diasCorridos / 7));

    // ── 3) produção por técnico, separada por categoria ──
    const porTec = {};
    for (const i of insp) {
      const fim = i.aprovadoEm || i.reprovadoEm;
      if (!dentro(fim)) continue;
      const t = String(i.tecnico || '(sem técnico)');
      porTec[t] = porTec[t] || { tecnico: 0, garantia: 0, avulsa: 0, aprov: 0, reprov: 0 };
      porTec[t][origemDe(i)]++;
      if (i.status === 'aprovado') porTec[t].aprov++;
      if (i.status === 'reprovado') porTec[t].reprov++;
    }
    const TECNICOS = Object.entries(porTec)
      .map(([nome, v]) => ({ nome, ...v, total: v.tecnico + v.garantia + v.avulsa,
        taxa: (v.aprov + v.reprov) ? Math.round(v.aprov / (v.aprov + v.reprov) * 100) : null }))
      .sort((a, b) => b.total - a.total);

    // ── 4) fila do setor técnico, por coluna, com tempo de espera ──
    const COLUNAS = [
      ['aprovado', 'Aprovado'], ['producao', 'Produção'],
      ['reforma_cliente', 'Reforma Cliente'], ['reforma_loja', 'Reforma Loja'],
      ['os_atrasada', 'OS em Atraso'], ['comprar_peca', 'Comprar Peça'],
      ['aguardando_peca', 'Aguardando Peça'], ['peca_disponivel', 'Peça Disponível'],
    ];
    const LIMITE_H = 48;
    const fila = {}, fichasPorColuna = {};
    let totalFila = 0, atrasadas = 0;
    for (const [id] of COLUNAS) { fila[id] = 0; fichasPorColuna[id] = []; }
    for (const c of (bd.cards || [])) {
      const f = String(c.phaseId || '');
      if (fila[f] === undefined) continue;
      fila[f]++; totalFila++;
      const desde = c.entrouTecnicoEm || c.movedAt || c.criadoEm;
      const horas = desde ? (Date.now() - new Date(desde).getTime()) / 3600000 : null;
      if (horas != null && horas > LIMITE_H) atrasadas++;
      fichasPorColuna[f].push({
        id: c.id, cliente: c.nomeContato || c.nome || '—',
        telefone: String(c.telefone || '').slice(-4),
        equipamento: String(c.equipamento || c.descricao || '').slice(0, 34),
        tecnico: c.tecnico || c.tecnicoServico || null,
        aprovadoEm: c.entrouTecnicoEm || null,
        desde, horas: horas != null ? +horas.toFixed(1) : null,
        atrasada: horas != null && horas > LIMITE_H,
      });
    }
    for (const k of Object.keys(fichasPorColuna)) {
      fichasPorColuna[k].sort((a, b) => (b.horas || 0) - (a.horas || 0));
    }

    return res.status(200).json({ ok: true,
      periodo: per, limiteAtrasoHoras: LIMITE_H,
      META_DIARIA: { feitas: doTecnicoHoje, meta: META,
        percentual: Math.round(doTecnicoHoje / META * 100),
        faltam: Math.max(0, META - doTecnicoHoje),
        observacao: 'conta apenas o que veio do setor técnico; garantia e avulsa ' +
          'são creditadas ao técnico mas ficam fora da meta' },
      META_SEMANAL: { feitas: doTecnicoSemana, meta: META_SEMANA,
        percentual: Math.round(doTecnicoSemana / META_SEMANA * 100),
        faltam: Math.max(0, META_SEMANA - doTecnicoSemana),
        cicloComecou: new Date(iniSemMs - 3 * 3600000).toISOString().slice(5, 16).replace('T', ' '),
        diaDoCiclo: Math.min(7, Math.ceil(diasCorridos)) + ' de 7',
        esperadoAteAgora: esperadoAgora,
        situacao: doTecnicoSemana >= esperadoAgora
          ? 'no ritmo' : 'atrás em ' + (esperadoAgora - doTecnicoSemana),
        faltamPorDia: (() => {
          const diasRestantes = Math.max(0.5, 7 - diasCorridos);
          return Math.ceil(Math.max(0, META_SEMANA - doTecnicoSemana) / diasRestantes);
        })() },
      ORIGENS,
      TECNICOS: TECNICOS.map(t => ({ nome: t.nome, total: t.total,
        tecnico: t.tecnico, garantia: t.garantia, avulsa: t.avulsa,
        taxaAprovacao: t.taxa })),
      FILA_TECNICO: {
        total: totalFila, atrasadas,
        colunas: COLUNAS.map(([id, nome]) => ({ id, nome, quantas: fila[id] })),
      },
      FICHAS_POR_COLUNA: fichasPorColuna,
      RESUMO_ATRASO: atrasadas
        ? '🔴 ' + atrasadas + ' ficha(s) há mais de ' + LIMITE_H + 'h no setor técnico'
        : '✅ nenhuma ficha acima de ' + LIMITE_H + 'h' });
  }

  // ── 📈 REGISTRAR-RITMO: fotografia diária de entrada e vazão ──
  // Roda algumas vezes por dia e guarda o que entrou, o que saiu e quanto tempo
  // cada serviço levou. Sem uma semana de medição, qualquer previsão de prazo
  // seria chute — este log é o que vai permitir calcular com base em fato.
  if (action === 'registrar-ritmo') {
    const hoje = new Date(Date.now() - 3 * 3600000).toISOString().slice(0, 10);
    const agora = new Date().toISOString();
    const q = (await dbGet('reparoeletro_qualidade')) || { inspecoes: [] };
    const insp = q.inspecoes || [];
    const bd = (await dbGet('reparoeletro_board')) || { cards: [] };
    const cards = bd.cards || [];

    const origemDe = i => (/garantia/i.test(String(i.origem || '') + ' ' + String(i.tipo || ''))
      ? 'garantia' : (i.avulsa === true || String(i.origem || '') === 'avulsa') ? 'avulsa' : 'tecnico');
    const doDia = (d) => String(d || '').slice(0, 10) === hoje;

    // ── entradas de hoje, por origem ──
    const entradas = { tecnico: 0, garantia: 0, avulsa: 0 };
    const saidas = { tecnico: 0, garantia: 0, avulsa: 0 };
    const tempos = [];
    for (const i of insp) {
      const o = origemDe(i);
      if (doDia(i.criadoEm)) entradas[o]++;
      const fim = i.aprovadoEm || i.reprovadoEm;
      if (doDia(fim)) {
        saidas[o]++;
        // ⏱️ o tempo que interessa é o da PRODUÇÃO: da aprovação no setor
        // técnico até o serviço chegar ao controle de qualidade. O intervalo
        // entre entrar no CQ e ser inspecionado mede a inspeção, não o serviço.
        const inicio = i.entrouTecnicoEm;
        if (inicio) {
          const ref = i.criadoEm || fim;   // criadoEm da inspeção = entrada no CQ
          const hProd = (new Date(ref).getTime() - new Date(inicio).getTime()) / 3600000;
          if (hProd >= 0 && hProd < 24 * 90) {
            tempos.push({ origem: o, horas: +hProd.toFixed(1), tecnico: i.tecnico || '?',
              medida: 'produção (aprovado → controle de qualidade)' });
          }
        }
      }
    }

    // ── fila do técnico agora, por coluna ──
    const FASES = ['aprovado', 'producao', 'reforma_cliente', 'reforma_loja',
      'comprar_peca', 'aguardando_peca', 'peca_disponivel', 'os_atrasada'];
    const fila = {};
    for (const f of FASES) fila[f] = 0;
    let aprovadosHoje = 0;
    for (const c of cards) {
      const f = String(c.phaseId || '');
      if (fila[f] !== undefined) fila[f]++;
      if (f === 'aprovado' && doDia(c.movedAt || c.criadoEm)) aprovadosHoje++;
    }

    // ── produção por técnico hoje ──
    const porTecnico = {};
    for (const i of insp) {
      const fim = i.aprovadoEm || i.reprovadoEm;
      if (!doDia(fim)) continue;
      const t = String(i.tecnico || '?');
      porTecnico[t] = porTecnico[t] || { tecnico: 0, garantia: 0, avulsa: 0 };
      porTecnico[t][origemDe(i)]++;
    }

    const foto = { em: agora, dia: hoje,
      entradas, saidas, porTecnico,
      naFila: { ...fila, total: Object.values(fila).reduce((a, b) => a + b, 0) },
      aguardandoInspecao: insp.filter(i => i.status === 'aguardando').length,
      aprovadosHojeNoTecnico: aprovadosHoje,
      tempoMedioProducaoHoras: tempos.length
        ? +(tempos.reduce((s, t) => s + t.horas, 0) / tempos.length).toFixed(1) : null,
      oQueMede: 'horas entre a aprovação no setor técnico e a entrada no controle de qualidade',
      semCarimboDeEntrada: insp.filter(i => doDia(i.aprovadoEm || i.reprovadoEm) &&
        !i.entrouTecnicoEm).length,
      amostraTempos: tempos.slice(0, 40) };

    // guarda uma linha por leitura, sem sobrescrever: o histórico é o valor
    const kR = 'qualidade_ritmo';
    const log = (await dbGet(kR)) || { fotos: [] };
    log.fotos = (log.fotos || []).concat([foto]).slice(-400);   // ~2 meses de leituras
    await dbSet(kR, log);
    return res.status(200).json({ ok: true, registrado: hoje,
      leiturasGuardadas: log.fotos.length, foto });
  }

  // ── 📊 RITMO: o que a medição já mostra ──
  if (action === 'ritmo') {
    const log = (await dbGet('qualidade_ritmo')) || { fotos: [] };
    const fotos = log.fotos || [];
    if (!fotos.length) return res.status(200).json({ ok: false,
      error: 'ainda não há medição — a coleta roda automaticamente e leva alguns dias' });
    // uma linha por dia: a última leitura de cada dia
    const porDia = {};
    for (const f of fotos) porDia[f.dia] = f;
    const dias = Object.keys(porDia).sort();
    const linhas = dias.map(d => {
      const f = porDia[d];
      const ent = Object.values(f.entradas || {}).reduce((a, b) => a + b, 0);
      const sai = Object.values(f.saidas || {}).reduce((a, b) => a + b, 0);
      return { dia: d, entrou: ent, saiu: sai, saldo: ent - sai,
        fila: (f.naFila || {}).total || 0,
        tempoMedio: f.tempoMedioProducaoHoras != null ? f.tempoMedioProducaoHoras : f.tempoMedioHoras,
        tecnico: (f.entradas || {}).tecnico || 0,
        garantia: (f.entradas || {}).garantia || 0,
        avulsa: (f.entradas || {}).avulsa || 0 };
    });
    const comDados = linhas.filter(l => l.entrou || l.saiu);
    const mediaEnt = comDados.length
      ? +(comDados.reduce((s, l) => s + l.entrou, 0) / comDados.length).toFixed(1) : 0;
    const mediaSai = comDados.length
      ? +(comDados.reduce((s, l) => s + l.saiu, 0) / comDados.length).toFixed(1) : 0;
    const filaAtual = linhas.length ? linhas[linhas.length - 1].fila : 0;
    return res.status(200).json({ ok: true,
      diasMedidos: comDados.length,
      maduro: comDados.length >= 5,
      aviso: comDados.length < 5
        ? '⏳ medição em andamento: com menos de 5 dias úteis qualquer previsão é frágil'
        : null,
      MEDIA_POR_DIA: { entram: mediaEnt, saem: mediaSai, saldo: +(mediaEnt - mediaSai).toFixed(1) },
      filaAtual,
      previsaoDias: (mediaSai > mediaEnt && filaAtual)
        ? +(filaAtual / (mediaSai - mediaEnt)).toFixed(1) : null,
      POR_DIA: linhas.map(l => l.dia + ' | entrou ' + String(l.entrou).padStart(3) +
        ' (téc ' + l.tecnico + ' · gar ' + l.garantia + ' · avl ' + l.avulsa + ')' +
        ' | saiu ' + String(l.saiu).padStart(3) +
        ' | saldo ' + (l.saldo > 0 ? '+' : '') + l.saldo +
        ' | fila ' + l.fila +
        (l.tempoMedio != null ? ' | ' + l.tempoMedio + 'h médias' : '')) });
  }
  let db = (await dbGet(KEY)) || defaultDB();
  if (!Array.isArray(db.inspecoes)) db.inspecoes = [];
  if (!db.config) db.config = { tecnicos: [], proximoNum: 1 };

  // ── LOAD ──
  // ── 🩻 POR-QUE-NAO-CRIOU: o card registra o que houve ao tentar criar a inspeção ──
  if (action === 'por-que-nao-criou') {
    const dias = Math.min(7, Math.max(1, parseInt(req.query.dias || '2', 10)));
    const corte = Date.now() - dias * 86400000;
    const L = [];
    for (const k of ['reparoeletro_board', 'reparoeletro_pipe', 'tv_pipe']) {
      try {
        const b = await dbGet(k);
        for (const c of (((b || {}).cards) || [])) {
          if (String(c.phaseId || c.phase || '') !== 'controle_qualidade') continue;
          const t = new Date(c.entrouCqEm || c.movedAt || 0).getTime();
          if (!t || t < corte) continue;
          L.push({ banco: k,
            cliente: c.nomeContato || c.nome || c.title || '?',
            temId: !!c.id, id: c.id || '(sem id)',
            telefone: c.telefone || '(sem telefone)',
            tecnicoServico: c.tecnicoServico || null,
            inspecaoCriada: c.inspecaoCriada || null,
            inspecaoConfirmada: c.inspecaoConfirmada,
            inspecaoErro: c.inspecaoErro || null,
            entrouCqEm: c.entrouCqEm || null,
            movedAt: c.movedAt });
        }
      } catch (e) {}
    }
    const semTentativa = L.filter(x => !x.inspecaoCriada && !x.inspecaoErro);
    const comErro = L.filter(x => x.inspecaoErro);
    const semId = L.filter(x => !x.temId);
    return res.status(200).json({ ok: comErro.length === 0 && semTentativa.length === 0,
      cardsNoCQ: L.length,
      semNenhumaTentativa: semTentativa.length,
      comErroRegistrado: comErro.length,
      semIdentificador: semId.length,
      DIAGNOSTICO: semId.length ? '🚨 há card sem identificador — a inspeção não consegue se vincular'
        : comErro.length ? '🚨 a criação falhou com erro registrado'
        : semTentativa.length ? '⚠️ o gatilho não chegou a rodar para estes cards'
        : '✅ todos registraram criação de inspeção',
      L: L.map(x => String(x.cliente).slice(0, 22) + ' | ' + x.banco +
        ' | id: ' + String(x.id).slice(0, 14) +
        ' | tel: ' + String(x.telefone).slice(-4) +
        ' | téc: ' + (x.tecnicoServico || '—') +
        ' | inspeção: ' + (x.inspecaoCriada || '❌ nenhuma') +
        (x.inspecaoConfirmada === false ? ' (não confirmada)' : '') +
        (x.inspecaoErro ? ' | 🚨 ' + x.inspecaoErro : '')) });
  }

  // ── 🔎 CONFERIR-HOJE: o que existe e o que sumiu do controle de qualidade ──
  if (action === 'conferir-hoje') {
    const dia = String(req.query.dia || new Date(Date.now() - 3 * 3600000).toISOString().slice(0, 10));
    const ini = new Date(dia + 'T00:00:00-03:00').getTime();
    const fim = ini + 86400000;
    const insp = db.inspecoes || [];
    const doDia = insp.filter(i => {
      const t = new Date(i.criadoEm || 0).getTime();
      return t >= ini && t < fim;
    });
    // cards que entraram na fase hoje — a referência do que DEVERIA existir
    const d8q = t => String(t || '').replace(/\D/g, '').slice(-8);
    const comInsp = new Set(insp.map(i => String(i.cardId || '')).filter(Boolean));
    const telComInsp = new Set(insp.map(i => d8q(i.telefone)).filter(t => t.length >= 8));
    const cardsHoje = [], semInspecao = [];
    for (const k of ['reparoeletro_board', 'reparoeletro_pipe', 'tv_pipe']) {
      try {
        const b = await dbGet(k);
        for (const c of (((b || {}).cards) || [])) {
          if (String(c.phaseId || c.phase || '') !== 'controle_qualidade') continue;
          const t = new Date(c.entrouCqEm || c.movedAt || 0).getTime();
          if (!t || t < ini || t >= fim) continue;
          const item = { banco: k, id: c.id, cliente: c.nomeContato || c.nome || '?',
            tel: d8q(c.telefone), equipamento: c.equipamento || c.descricao || '',
            tecnico: c.tecnicoServico || c.tecnico || null,
            entrou: c.entrouCqEm || c.movedAt };
          cardsHoje.push(item);
          if (!comInsp.has(String(c.id)) && !telComInsp.has(item.tel)) semInspecao.push(item);
        }
      } catch (e) {}
    }
    const hh3 = d => d ? new Date(new Date(d).getTime() - 3 * 3600000).toISOString().slice(11, 16) : '?';
    return res.status(200).json({ ok: semInspecao.length === 0,
      dia,
      inspecoesDoDia: doDia.length,
      cardsQueEntraramHoje: cardsHoje.length,
      semInspecao: semInspecao.length,
      VEREDITO: semInspecao.length === 0
        ? '✅ todo card que entrou hoje tem inspeção — nada foi removido indevidamente'
        : '🚨 ' + semInspecao.length + ' card(s) de hoje sem inspeção',
      INSPECOES_DE_HOJE: doDia.map(i => hh3(i.criadoEm) + ' | ' + (i.os || '') +
        ' | ' + String(i.cliente || '?').slice(0, 22) +
        ' | ' + String(i.equipamentoTexto || i.equipamento || '').slice(0, 24) +
        ' | téc: ' + (i.tecnico || '—') + ' | ' + i.status +
        (i.recuperada ? ' | ⚠️ recuperada' : '') + (i.avulsa ? ' | avulsa' : '')),
      SEM_INSPECAO: semInspecao.map(c => hh3(c.entrou) + ' | ' + String(c.cliente).slice(0, 22) +
        ' ' + c.tel.slice(-4) + ' | ' + String(c.equipamento).slice(0, 24) +
        ' | téc: ' + (c.tecnico || '—')) });
  }

  // ── 🧹 LIMPAR-DUPLICADAS: mesma inspeção gravada mais de uma vez ──
  if (action === 'limpar-duplicadas') {
    const lista = db.inspecoes || [];
    const porId = {}, porConteudo = {};
    const ficam = [], removidas = [];
    for (const i of lista) {
      const chaveId = String(i.id || '');
      const chaveC = String(i.os || '') + '|' + String(i.cardId || '') + '|' +
        String(i.cliente || '') + '|' + String(i.equipamentoTexto || i.equipamento || '') + '|' +
        String(i.tecnico || '');
      // mantém a primeira; se alguma já foi inspecionada, ela tem preferência
      const jaTemId = porId[chaveId], jaTemC = porConteudo[chaveC];
      if (!jaTemId && !jaTemC) {
        porId[chaveId] = i; porConteudo[chaveC] = i; ficam.push(i); continue;
      }
      const guardada = jaTemId || jaTemC;
      const estaTratada = i.status === 'aprovado' || i.status === 'reprovado';
      const guardadaTratada = guardada.status === 'aprovado' || guardada.status === 'reprovado';
      if (estaTratada && !guardadaTratada) {
        // troca: a tratada vale mais
        const ix = ficam.indexOf(guardada);
        if (ix >= 0) ficam[ix] = i;
        porId[chaveId] = i; porConteudo[chaveC] = i;
        removidas.push((guardada.os || guardada.id) + ' | ' + String(guardada.cliente || '?').slice(0, 20));
      } else {
        removidas.push((i.os || i.id) + ' | ' + String(i.cliente || '?').slice(0, 20) +
          ' | ' + String(i.equipamentoTexto || i.equipamento || '').slice(0, 22));
      }
    }
    if (String(req.query.aplicar || '') !== '1') {
      return res.status(200).json({ ok: removidas.length === 0,
        modo: 'prévia', total: lista.length,
        duplicadas: removidas.length, ficam: ficam.length,
        L: removidas.slice(0, 50),
        criterio: 'mesma OS, mesmo card, mesmo cliente, equipamento e técnico · inspeção já tratada tem preferência',
        dica: removidas.length ? 'para limpar: &aplicar=1' : undefined });
    }
    db.inspecoes = ficam;
    await dbSet(KEY, db);
    return res.status(200).json({ ok: true, removidas: removidas.length, restam: ficam.length });
  }

  // ── ↩️ DESFAZER-RECUPERADAS: remove as inspeções criadas em massa por engano ──
  if (action === 'desfazer-recuperadas') {
    const alvo = (db.inspecoes || []).filter(i => i.recuperada === true &&
      i.status !== 'aprovado' && i.status !== 'reprovado');
    if (String(req.query.aplicar || '') !== '1') {
      return res.status(200).json({ ok: true, modo: 'prévia',
        vaoSerRemovidas: alvo.length,
        L: alvo.map(i => (i.os || i.id) + ' | ' + String(i.cliente || '?').slice(0, 22) +
          ' | ' + String(i.equipamentoTexto || i.equipamento || '').slice(0, 26) +
          ' | téc: ' + (i.tecnico || '—')),
        observacao: 'só remove as recuperadas que ainda não foram inspecionadas',
        dica: 'para remover: &aplicar=1' });
    }
    const ids = new Set(alvo.map(i => i.id));
    db.inspecoes = (db.inspecoes || []).filter(i => !ids.has(i.id));
    await dbSet(KEY, db);
    return res.status(200).json({ ok: true, removidas: ids.size, restam: db.inspecoes.length });
  }

  // ── 🔍 FALTANDO: cards no Controle de Qualidade sem inspeção criada ──
  if (action === 'faltando') {
    const dias = Math.min(30, Math.max(1, parseInt(req.query.dias || '3', 10)));
    const corte = Date.now() - dias * 86400000;
    const insp = db.inspecoes || [];
    const comInsp = new Set(insp.map(i => String(i.cardId || '')).filter(Boolean));
    // 🔍 inspeções antigas podem não ter cardId — casar também por telefone evita
    // considerar como faltante um card que já foi inspecionado
    const d8q = t => String(t || '').replace(/\D/g, '').slice(-8);
    const telComInsp = new Set(insp.map(i => d8q(i.telefone)).filter(t => t.length >= 8));
    const faltando = [];
    for (const k of ['reparoeletro_board', 'reparoeletro_pipe', 'tv_pipe']) {
      try {
        const b = await dbGet(k);
        for (const c of (((b || {}).cards) || [])) {
          const fase = String(c.phaseId || c.phase || '');
          if (fase !== 'controle_qualidade') continue;
          const q = new Date(c.entrouCqEm || c.movedAt || 0).getTime();
          if (!q || q < corte) continue;
          if (comInsp.has(String(c.id))) continue;
          if (telComInsp.has(d8q(c.telefone))) continue;   // já tem inspeção deste cliente
          faltando.push({ banco: k, id: c.id,
            cliente: c.nomeContato || c.nome || '?',
            telefone: c.telefone || '',
            equipamento: c.equipamento || c.descricao || '',
            tecnico: c.tecnicoServico || c.tecnico || null,
            entrouEm: c.entrouCqEm || c.movedAt });
        }
      } catch (e) {}
    }
    if (String(req.query.aplicar || '') !== '1') {
      return res.status(200).json({ ok: faltando.length === 0,
        cardsSemInspecao: faltando.length,
        L: faltando.map(f => String(f.cliente).slice(0, 22) + ' ' +
          String(f.telefone).slice(-4) + ' | ' + String(f.equipamento).slice(0, 26) +
          ' | téc: ' + (f.tecnico || '(sem técnico)') +
          ' | entrou ' + String(f.entrouEm || '').slice(5, 16).replace('T', ' ')),
        dica: faltando.length ? 'para criar as inspeções: &aplicar=1' : undefined });
    }
    const criadas = [];
    for (const f of faltando) {
      const num = (db.config.proximoNum || (db.inspecoes.length + 1));
      const txt = String(f.equipamento).toLowerCase();
      const tipo = /micro-?ondas|magnetron/.test(txt) ? 'microondas'
        : /purificador|bebedouro/.test(txt) ? 'purificador'
        : /adega/.test(txt) ? 'adega' : /forno/.test(txt) ? 'forno'
        : /\btv\b|televis/.test(txt) ? 'tv' : 'outro';
      db.inspecoes.unshift({
        id: 'insp_rec_' + Date.now().toString(36) + Math.random().toString(36).slice(2, 5),
        os: 'CQ-' + String(num).padStart(4, '0'),
        cardId: f.id, cliente: f.cliente, telefone: f.telefone,
        equipamento: tipo, equipamentoTexto: f.equipamento,
        tecnico: f.tecnico, valor: 0, status: 'aguardando', checklist: {},
        criadoEm: f.entrouEm || new Date().toISOString(),
        recuperada: true,
      });
      db.config.proximoNum = num + 1;
      criadas.push(String(f.cliente).slice(0, 22) + ' — ' + String(f.equipamento).slice(0, 24));
      await new Promise(s => setTimeout(s, 60));
    }
    if (criadas.length) await dbSet(KEY, db);
    return res.status(200).json({ ok: true, criadas: criadas.length, L: criadas });
  }

  // ── 👨‍🔧 EQUIPE: a lista oficial de técnicos, igual à do Mover OS ──
  if (action === 'equipe') {
    const EQUIPE_OFICIAL = ['Lucas', 'Diego', 'Kassio', 'Roberto', 'Carlos', 'Arthur'];
    const salvos = (db.config && db.config.tecnicos) || [];
    // junta os oficiais com quaisquer outros já cadastrados, sem repetir
    const todos = EQUIPE_OFICIAL.concat(salvos.filter(t => !EQUIPE_OFICIAL.includes(t)));
    return res.status(200).json({ ok: true, tecnicos: todos, oficiais: EQUIPE_OFICIAL });
  }

  // ── ➕ FICHA AVULSA: produção do técnico que não veio da esteira ──
  // Ex.: equipamento de venda, reforma de magnetron. Conta para o técnico,
  // mas NÃO dispara nenhum comunicado ao cliente — não há cliente envolvido.
  if (req.method === 'POST' && action === 'criar-avulsa') {
    const b = req.body || {};
    const tecnico = String(b.tecnico || '').trim();
    const descricao = String(b.descricao || '').trim();
    if (!tecnico) return res.status(400).json({ ok: false, error: 'informe o técnico' });
    if (!descricao) return res.status(400).json({ ok: false, error: 'descreva o que foi feito' });
    const txt = descricao.toLowerCase();
    const tipo = /micro-?ondas|magnetron/.test(txt) ? 'microondas'
      : /purificador|bebedouro/.test(txt) ? 'purificador'
      : /adega/.test(txt) ? 'adega'
      : /forno/.test(txt) ? 'forno'
      : /\btv\b|televis/.test(txt) ? 'tv' : 'outro';
    const num = db.config.proximoNum || ((db.inspecoes || []).length + 1);
    const insp = {
      id: 'insp_avl_' + Date.now().toString(36) + Math.random().toString(36).slice(2, 5),
      os: 'AVL-' + String(num).padStart(4, '0'),
      cardId: null,
      // 🏷️ sem cliente, o cabeçalho mostra o que foi feito, não um traço vazio
      cliente: String(b.cliente || '').trim() || descricao.slice(0, 40),
      telefone: '',
      equipamento: tipo,
      equipamentoTexto: descricao.slice(0, 120),
      tecnico,
      categoria: String(b.categoria || 'producao_interna'),   // venda · reforma · producao_interna · garantia
      // 🔗 quando o serviço é de garantia, o vínculo permite baixar a fila
      // automaticamente ao aprovar — sem ele o item fica esperando algo já feito
      garantiaId: b.garantiaId ? String(b.garantiaId) : null,
      origem: b.categoria === 'garantia' ? 'garantia' : undefined,
      obsTecnica: String(b.obs || '').slice(0, 400) || null,
      valor: Number(b.valor || 0) || 0,
      avulsa: true,
      semComunicado: true,          // 🔇 nunca avisa cliente
      status: 'aguardando',
      checklist: {},
      criadoEm: new Date().toISOString(),
      criadaPor: String(b.criadaPor || 'operação').slice(0, 30),
    };
    db.inspecoes.unshift(insp);
    db.config.proximoNum = num + 1;
    await dbSet(KEY, db);
    // confirma que persistiu
    const conf = (await dbGet(KEY)) || {};
    const ok2 = ((conf.inspecoes) || []).some(x => x.id === insp.id);
    if (!ok2) return res.status(200).json({ ok: false, error: 'a gravação não persistiu — tente de novo' });
    return res.status(200).json({ ok: true, inspecao: insp, filaGarantia,
      aviso: 'ficha avulsa criada — nenhum comunicado será enviado' });
  }

  // ── 📊 CONTADORES: por dia, semana e por técnico ──
  if (action === 'contadores') {
    const insp = db.inspecoes || [];
    const bras = new Date(Date.now() - 3 * 3600000);
    const hoje = bras.toISOString().slice(0, 10);
    const diaSem = bras.getUTCDay();
    const seg = new Date(bras); seg.setUTCDate(bras.getUTCDate() - ((diaSem === 0) ? 6 : (diaSem - 1)));
    const iniSemana = seg.toISOString().slice(0, 10);
    // 📅 a data gravada está em UTC e o dia de referência é o de Brasília:
    // comparar direto fazia todo registro feito depois das 21h cair no dia seguinte
    const dia = d => { const t = new Date(d || 0).getTime();
      return t ? new Date(t - 3 * 3600000).toISOString().slice(0, 10) : ''; };

    // 🎯 a meta de 25/dia NÃO conta as que vieram de garantia
    const deGarantia = i => /garantia/i.test(String(i.origem || '') + ' ' + String(i.tipo || ''));
    const ehAvulsaC = i => i.avulsa === true;
    const entrouHoje = insp.filter(i => dia(i.criadoEm) === hoje);
    const entrouSemana = insp.filter(i => dia(i.criadoEm) >= iniSemana);
    // 🎯 a meta de 25/dia mede a produção que veio da esteira do técnico:
    // não conta garantia nem ficha avulsa, que é registro manual de outra origem
    const contaNaMeta = i => !deGarantia(i) && i.avulsa !== true;
    const hojeSemGarantia = entrouHoje.filter(contaNaMeta);
    const semanaSemGarantia = entrouSemana.filter(contaNaMeta);
    const concluiHoje = insp.filter(i => i.aprovadoEm && dia(i.aprovadoEm) === hoje);
    const concluiSemana = insp.filter(i => i.aprovadoEm && dia(i.aprovadoEm) >= iniSemana);
    const reprovHoje = insp.filter(i => i.reprovadoEm && dia(i.reprovadoEm) === hoje);

    const porTecnico = {};
    for (const i of insp) {
      const t = String(i.tecnico || '(sem técnico)');
      porTecnico[t] = porTecnico[t] || { hoje: 0, semana: 0, total: 0, aprovadas: 0, reprovadas: 0 };
      porTecnico[t].total++;
      if (dia(i.criadoEm) === hoje) porTecnico[t].hoje++;
      if (dia(i.criadoEm) >= iniSemana) porTecnico[t].semana++;
      if (i.status === 'aprovado') porTecnico[t].aprovadas++;
      if (i.reprovadoEm) porTecnico[t].reprovadas++;
    }
    return res.status(200).json({ ok: true,
      hoje, semanaComecaEm: iniSemana,
      META_DIA: 25,
      ENTRARAM: { hoje: entrouHoje.length, semana: entrouSemana.length, total: insp.length,
        hojeSemGarantia: hojeSemGarantia.length,
        semanaSemGarantia: semanaSemGarantia.length,
        deGarantiaHoje: entrouHoje.length - hojeSemGarantia.length },
      AVULSAS: { hoje: entrouHoje.filter(ehAvulsaC).length,
        semana: entrouSemana.filter(ehAvulsaC).length,
        obs: 'produção interna: venda, reforma — conta para o técnico, mas fica FORA da meta de 25' },
      META: { alvo: 25, feito: hojeSemGarantia.length,
        falta: Math.max(0, 25 - hojeSemGarantia.length),
        percentual: Math.round(hojeSemGarantia.length / 25 * 100) + '%' },
      CONCLUIDAS: { hoje: concluiHoje.length, semana: concluiSemana.length,
        total: insp.filter(i => i.status === 'aprovado').length },
      AGUARDANDO: insp.filter(i => i.status === 'aguardando').length,
      REPROVADAS_HOJE: reprovHoje.length,
      POR_TECNICO: Object.entries(porTecnico)
        .sort((a, b) => b[1].semana - a[1].semana)
        .map(([t, v]) => t.padEnd(12) + ' | hoje ' + String(v.hoje).padStart(2) +
          ' | semana ' + String(v.semana).padStart(2) +
          ' | aprovadas ' + String(v.aprovadas).padStart(2) +
          (v.reprovadas ? ' | reprovadas ' + v.reprovadas : '')),
      // 📋 as OSs de cada técnico, para ver quais são e não só quantas
      OSS_POR_TECNICO: Object.fromEntries(Object.keys(porTecnico).map(t => [t,
        insp.filter(i => String(i.tecnico || '(sem técnico)') === t &&
            dia(i.criadoEm) >= iniSemana)
          .sort((a, b) => String(b.criadoEm).localeCompare(String(a.criadoEm)))
          .map(i => (i.os || i.id) + ' | ' + String(i.cliente || '?').slice(0, 20) +
            ' | ' + String(i.equipamentoTexto || i.equipamento || '').slice(0, 24) +
            ' | ' + (i.status === 'aprovado' ? '✅ aprovada'
              : i.reprovadoEm ? '🔧 retrabalho' : '⏳ aguardando') +
            ' | ' + String(i.criadoEm || '').slice(5, 10).split('-').reverse().join('/'))])),
      detalhePorTecnico: porTecnico });
  }

  // ── 📊 RELATORIO: inspeções por período, por técnico, aprovados x reprovados ──
  if (action === 'relatorio') {
    const dias = Math.min(365, Math.max(1, parseInt(req.query.dias || '30', 10)));
    const corte = Date.now() - dias * 86400000;
    const dbR = (await dbGet(KEY)) || { inspecoes: [] };
    const todas = (dbR.inspecoes || []);
    const noPeriodo = todas.filter(i => new Date(i.criadoEm || 0).getTime() >= corte);

    const horas = i => {
      const ini = new Date(i.criadoEm || 0).getTime();
      const fim = new Date(i.aprovadoEm || i.reprovadoEm || 0).getTime();
      return (ini && fim && fim > ini) ? Number(((fim - ini) / 3600000).toFixed(1)) : null;
    };
    const media = a => a.length ? Number((a.reduce((s, x) => s + x, 0) / a.length).toFixed(1)) : null;

    // por TÉCNICO que fez o serviço
    const porTec = {};
    for (const i of noPeriodo) {
      const t = (i.tecnico || '(não informado)').trim();
      if (!porTec[t]) porTec[t] = { tecnico: t, total: 0, aprovados: 0, reprovados: 0, aguardando: 0, tempos: [] };
      const p = porTec[t];
      p.total++;
      if (i.status === 'aprovado') p.aprovados++;
      else if (i.status === 'reprovado') p.reprovados++;
      else p.aguardando++;
      const hh = horas(i); if (hh != null) p.tempos.push(hh);
    }
    const ranking = Object.values(porTec).map(p => ({
      tecnico: p.tecnico, inspecoes: p.total,
      aprovados: p.aprovados, reprovados: p.reprovados, aguardando: p.aguardando,
      taxaAprovacao: (p.aprovados + p.reprovados) ? Math.round(p.aprovados / (p.aprovados + p.reprovados) * 100) : null,
      horasMedias: media(p.tempos),
    })).sort((a, b) => b.inspecoes - a.inspecoes);

    const porDia = {};
    for (const i of noPeriodo) {
      const d = new Date(new Date(i.criadoEm).getTime() - 3 * 3600000).toISOString().slice(0, 10);
      porDia[d] = (porDia[d] || 0) + 1;
    }
    const aprovados = noPeriodo.filter(i => i.status === 'aprovado').length;
    const reprovados = noPeriodo.filter(i => i.status === 'reprovado').length;

    if (String(req.query.mini || '') === '1') {
      return res.status(200).json({ dias,
        entraram: noPeriodo.length, aprovados, reprovados,
        aguardando: noPeriodo.filter(i => i.status === 'aguardando').length,
        taxaAprovacao: (aprovados + reprovados) ? Math.round(aprovados / (aprovados + reprovados) * 100) + '%' : null,
        porTecnico: ranking.reduce((o, r) => { o[r.tecnico] = r.inspecoes; return o; }, {}) });
    }
    return res.status(200).json({ ok: true, periodoDias: dias,
      RESUMO: {
        entraramNoCQ: noPeriodo.length,
        aprovados, reprovados,
        aguardandoAgora: todas.filter(i => i.status === 'aguardando').length,
        taxaAprovacao: (aprovados + reprovados) ? Math.round(aprovados / (aprovados + reprovados) * 100) + '%' : null,
        horasMedias: media(noPeriodo.map(horas).filter(x => x != null)),
      },
      POR_TECNICO: ranking.map(r => r.tecnico + ' | ' + r.inspecoes + ' inspeção(ões) | ✅ ' +
        r.aprovados + ' · ❌ ' + r.reprovados +
        (r.taxaAprovacao != null ? ' | ' + r.taxaAprovacao + '% aprovação' : '') +
        (r.aguardando ? ' | ' + r.aguardando + ' na fila' : '')),
      porDia,
      detalhePorTecnico: ranking,
      reprovadosDetalhe: noPeriodo.filter(i => i.status === 'reprovado').map(i => ({
        os: i.os, cliente: i.cliente, equipamento: i.equipamento,
        tecnico: i.tecnico, inspetor: i.inspetor,
        motivo: i.motivoReprovacao || null, quando: i.reprovadoEm })) });
  }

  if (action === 'load') {
    return res.status(200).json({ ok: true, inspecoes: db.inspecoes, config: db.config });
  }

  // ── SEED-TESTE: 10 fichas simulando a chegada do técnico (leque completo) ──
  if (action === 'seed-teste') {
    const TEL = '5531997856023';
    const seeds = [
      { cliente: 'Pedro 1',  equipamento: 'adega',       equipDesc: 'Adega termoelétrica 12 garrafas — troca da pastilha Peltier + cooler', tecnico: 'Técnico A', os: 'PIPE-TESTE-01' },
      { cliente: 'Pedro 2',  equipamento: 'adega',       equipDesc: 'Adega de compressor 33 garrafas — carga de gás + termostato', tecnico: 'Técnico B', os: 'PIPE-TESTE-02' },
      { cliente: 'Pedro 3',  equipamento: 'purificador', equipDesc: 'Purificador termoelétrico (gela) — troca do módulo + filtro novo', tecnico: 'Técnico A', os: 'PIPE-TESTE-03' },
      { cliente: 'Pedro 4',  equipamento: 'purificador', equipDesc: 'Purificador natural (sem refrigeração) — troca de dutos, conexões e filtro', tecnico: 'Técnico B', os: 'PIPE-TESTE-04' },
      { cliente: 'Pedro 5',  equipamento: 'microondas',  equipDesc: 'Micro-ondas 30L — capacitor + fusível de alta + micro chave', tecnico: 'Técnico A', os: 'FL-TESTE-05' },
      { cliente: 'Pedro 6',  equipamento: 'microondas',  equipDesc: 'Micro-ondas — REFORMA completa: pintura da cavidade + placa mica + acabamento', tecnico: 'Técnico B', os: 'FL-TESTE-06' },
      { cliente: 'Pedro 7',  equipamento: 'forno',       equipDesc: 'Forno elétrico pequeno — troca do termostato + chave seletora', tecnico: 'Técnico A', os: 'FL-TESTE-07' },
      { cliente: 'Pedro 8',  equipamento: 'forno',       equipDesc: 'Forno grande — resistência superior + reoperação elétrica', tecnico: 'Técnico B', os: 'FL-TESTE-08' },
      { cliente: 'Pedro 9',  equipamento: 'tv',          equipDesc: 'TV 50" — TU + barramento (recuperação da placa)', tecnico: 'Técnico A', os: 'TV-TESTE-09' },
      { cliente: 'Pedro 10', equipamento: 'bblend',      equipDesc: 'B.blend — reparo do circuito hidráulico + higienização do sistema', tecnico: 'Técnico B', os: 'PIPE-TESTE-10' },
    ];
    const EQUIPE = ['Lucas', 'Diego', 'Kassio', 'Roberto', 'Carlos', 'Arthur'];  // Kassio é a grafia usada no sistema
    if (!db.config.tecnicos || !db.config.tecnicos.length) db.config.tecnicos = EQUIPE.slice();
    // garante que todos da equipe estejam na lista, sem apagar nomes já cadastrados
    for (const t of EQUIPE) if (!db.config.tecnicos.includes(t)) db.config.tecnicos.push(t);
    const criadas = [];
    for (const s of seeds) {
      if (db.inspecoes.some(i => i.cliente === s.cliente && i.tel === TEL)) continue; // dedupe
      const num = db.config.proximoNum || 1;
      db.inspecoes.unshift({
        id: 'QC-' + String(num).padStart(4, '0'), criadoEm: new Date().toISOString(),
        cliente: s.cliente, tel: TEL, os: s.os, equipamento: s.equipamento, equipDesc: s.equipDesc,
        tecnico: s.tecnico, inspetor: '', status: 'aguardando', checklist: {}, reprovacoes: [], aprovadoEm: null,
        teste: true,
      });
      db.config.proximoNum = num + 1;
      criadas.push(s.cliente);
    }
    await dbSet(KEY, db);
    return res.status(200).json({ ok: true, criadas, jaExistiam: seeds.length - criadas.length });
  }

  // ── LIMPAR-TESTES: remove as fichas de teste ──
  if (action === 'limpar-testes') {
    const antes = db.inspecoes.length;
    db.inspecoes = db.inspecoes.filter(i => !i.teste);
    await dbSet(KEY, db);
    return res.status(200).json({ ok: true, removidas: antes - db.inspecoes.length });
  }

  // ── CRIAR inspeção (entrada manual na beta; depois virá do técnico) ──
  if (req.method === 'POST' && action === 'criar') {
    const { cliente, tel, os, equipamento, equipDesc, tecnico,
            diagnosticoOriginal, obsQualidade, valor } = req.body || {};
    if (!cliente || !equipamento) return res.status(400).json({ ok: false, error: 'cliente e equipamento obrigatórios' });
    const num = db.config.proximoNum || 1;
    const insp = {
      id: 'QC-' + String(num).padStart(4, '0'),
      criadoEm: new Date().toISOString(),
      cliente: String(cliente).trim(),
      tel: String(tel || '').trim(),
      os: String(os || '').trim(),
      equipamento: String(equipamento).trim(),
      equipDesc: String(equipDesc || '').trim(),
      tecnico: String(tecnico || '').trim(),
      // 📋 contexto que o inspetor precisa: o que foi diagnosticado quando o equipamento
      // chegou, e a observação que o técnico deixou ao mandar para o CQ (uso INTERNO)
      diagnosticoOriginal: String(diagnosticoOriginal || '').trim().slice(0, 600),
      obsQualidade: String(obsQualidade || '').trim().slice(0, 500),
      valor: parseFloat(valor || 0) || null,
      inspetor: '',
      status: 'aguardando',
      checklist: {},
      reprovacoes: [],
      aprovadoEm: null,
    };
    db.config.proximoNum = num + 1;
    db.inspecoes.unshift(insp);
    await dbSet(KEY, db);
    return res.status(200).json({ ok: true, inspecao: insp });
  }

  // ── SALVAR checklist parcial (auto-save durante a inspeção) ──
  if (req.method === 'POST' && action === 'salvar-checklist') {
    const { id, checklist, inspetor } = req.body || {};
    const insp = db.inspecoes.find(i => i.id === id);
    if (!insp) return res.status(404).json({ ok: false, error: 'inspeção não encontrada' });
    if (checklist) insp.checklist = checklist;
    if (inspetor !== undefined) insp.inspetor = String(inspetor);
    await dbSet(KEY, db);
    return res.status(200).json({ ok: true });
  }

  // ── APROVAR ──
  // ── 🧹 ZERAR: limpa as inspeções (fichas de teste) ──
  if (action === 'zerar') {
    const antes = (db.inspecoes || []).length;
    if (String(req.query.aplicar || '') !== '1') {
      return res.status(200).json({ ok: true, modo: 'prévia',
        inspecoes: antes,
        porStatus: (db.inspecoes || []).reduce((o, i) => {
          const s = String(i.status || '?'); o[s] = (o[s] || 0) + 1; return o; }, {}),
        amostra: (db.inspecoes || []).slice(0, 10).map(i =>
          (i.os || i.id) + ' | ' + String(i.cliente || i.nome || '?').slice(0, 18) + ' | ' + i.status),
        dica: 'para apagar: &aplicar=1' });
    }
    // guarda cópia antes de apagar
    try { await dbSet('qualidade_lixeira', { em: new Date().toISOString(), inspecoes: db.inspecoes || [] }); } catch (e) {}
    db.inspecoes = [];
    if (db.config) db.config.proximoNum = 1;
    await dbSet(KEY, db);
    return res.status(200).json({ ok: true, apagadas: antes,
      backup: 'cópia guardada em qualidade_lixeira' });
  }

  if (req.method === 'POST' && action === 'aprovar') {
    const { id, checklist, inspetor } = req.body || {};
    const insp = db.inspecoes.find(i => i.id === id);
    if (!insp) return res.status(404).json({ ok: false, error: 'inspeção não encontrada' });
    if (checklist) insp.checklist = checklist;
    if (inspetor) insp.inspetor = String(inspetor);
    insp.status = 'aprovado';
    insp.aprovadoEm = new Date().toISOString();
    await dbSet(KEY, db);

    // ── 🏪 FRENTE DE LOJA: aprovado no CQ → conserto realizado lá também ──
    // Antes o técnico marcava Loja Feito e a ficha ia direto para conserto realizado.
    // Agora ela passa pelo Controle de Qualidade, então quem move é a aprovação daqui.
    // 🛡️ inspeção de garantia aprovada: baixa o item da fila de tratamento
    let filaGarantia = null;
    if (insp.garantiaId || insp.origem === 'garantia') {
      try {
        const fl2 = (await dbGet('reparoeletro_garantia_fila')) || { itens: [] };
        const d8f2 = t => String(t || '').replace(/\D/g, '').slice(-8);
        const it = (fl2.itens || []).find(x => x.status !== 'resolvido' &&
          (String(x.id) === String(insp.garantiaId) ||
           (d8f2(x.telefone) && d8f2(x.telefone) === d8f2(insp.telefone))));
        if (it) {
          it.status = 'resolvido';
          it.destino = 'qc';
          it.resolvidoEm = new Date().toISOString();
          it.resolvidoPor = 'aprovação no controle de qualidade';
          it.inspecaoOs = insp.os || insp.id;
          await dbSet('reparoeletro_garantia_fila', fl2);
          filaGarantia = { baixado: true, item: it.nome || it.id };
        } else { filaGarantia = { baixado: false, motivo: 'item não encontrado na fila' }; }
      } catch (e) { filaGarantia = { baixado: false, erro: e.message }; }
    }
    let frenteLoja = null;
    try {
      const d8q = String(insp.telefone || '').replace(/\D/g, '').slice(-8);
      const nomeQ = String(insp.cliente || '').toLowerCase().trim();
      const ABERTAS = ['producao', 'analise', 'orcamento_cadastrado', 'aprovados', 'receber'];
      // 🏪 aprovado no CQ → conserto realizado no Frente de Loja, liberando o pagamento
      const rFL = await _gravar.alterar('reparoeletro_frenteloja',
        (FL) => {
          const lista = (FL && FL.fichas) || [];
          const f = lista.find(x => {
            if (String(x.id || '') === String(insp.flFichaId || '')) return true;
            const t = String(x.telefone || '').replace(/\D/g, '').slice(-8);
            const n = String(x.nomeContato || x.nome || '').toLowerCase().trim();
            if (!ABERTAS.includes(String(x.phase || ''))) return false;
            return (d8q.length >= 8 && t === d8q) || (nomeQ && n === nomeQ);
          });
          if (!f) return null;                       // nada a fazer
          const agora = new Date().toISOString();
          f.phase = 'conserto_realizado';
          f.movedAt = agora;
          f.consertoRealizadoEm = agora;
          f.viaControleQualidade = true;
          f.inspecaoOs = insp.os || insp.id;
          // 🏪 entra na régua de lembretes de retirada a partir de agora. As
          // fichas anteriores a esta marcação ficam de fora, para não cobrar
          // retirada de equipamento que provavelmente já foi buscado.
          f.reguaRetirada = true;
          f.reguaRetiradaDesde = agora;
          f.lembreteRetirada = { enviados: 0, ultimo: null };
          f.history = (f.history || []).concat([{ phase: 'conserto_realizado', ts: agora,
            via: 'controle_qualidade', inspecao: insp.os || insp.id }]);
          return FL;
        },
        (FL) => ((FL || {}).fichas || []).some(x =>
          String(x.inspecaoOs || '') === String(insp.os || insp.id) &&
          String(x.phase || '') === 'conserto_realizado'),
        { tentativas: 3, padrao: { fichas: [] } });
      frenteLoja = rFL.ok
        ? (rFL.motivo === 'nada a alterar'
            ? { movido: false, motivo: 'cliente não encontrado em fase aberta no Frente de Loja' }
            : { movido: true, para: 'conserto_realizado', detalhe: rFL.motivo })
        : { movido: false, erro: rFL.motivo };
    } catch (e) { frenteLoja = { movido: false, erro: e.message }; }

    // ── 📲 AVISA O CLIENTE com o resultado do controle de qualidade ──
    // 🔇 ficha avulsa é produção interna: não há cliente para avisar
    let avisoCliente = null;
    const ehAvulsa = insp.avulsa === true || insp.semComunicado === true;
    try {
      if (ehAvulsa) throw { _pular: true };
      const tel = String(insp.telefone || '').replace(/\D/g, '');
      if (tel.length >= 10) {
        const EQ = { microondas: 'micro-ondas', purificador: 'purificador',
          adega: 'adega', forno: 'forno', tv: 'TV', bblend: 'BBlend', outro: 'equipamento' };
        const aprovados = Object.entries(insp.checklist || {})
          .filter(([, v]) => v && v.v === 'ok').length;
        const primeiro = String(insp.cliente || '').trim().split(/\s+/)[0] || 'tudo bem';
        const equip = EQ[insp.equipamento] || 'equipamento';
        const texto =
          'Olá ' + primeiro + '! ✅ Seu ' + equip + ' passou pelo nosso Controle de Qualidade e foi APROVADO.\n\n' +
          (aprovados ? aprovados + ' verificação(ões) conferida(s) e aprovada(s).\n\n' : '') +
          'Agora ele segue para a etapa de *instalação e teste prático*, onde montamos e ligamos o equipamento para certificar que está pronto de verdade.\n\n' +
          'Assim que essa etapa terminar, nossa equipe do financeiro entra em contato pelo número *(31) 97225-9819* para emitir a nota fiscal, registrar a garantia e combinar a entrega.\n\n' +
          'Qualquer dúvida é só chamar! 😊';
        const KTF = (process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim();
        const r = await fetch('https://reparoeletroadm.com/api/wa-bot?action=enviar&k=' + KTF, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ tel, texto, via: 'controle-qualidade' }),
        }).then(x => x.json()).catch(e => ({ ok: false, error: e.message }));
        avisoCliente = r && r.ok ? 'enviado' : ('não enviado: ' + ((r && r.error) || '?'));
        insp.avisoClienteEm = new Date().toISOString();
        insp.avisoClienteStatus = avisoCliente;
        insp.textoEnviado = texto;
        await dbSet(KEY, db);
      } else { avisoCliente = 'inspeção sem telefone — cliente não avisado'; }
    } catch (e) { avisoCliente = 'falhou: ' + e.message; }

    return res.status(200).json({ ok: true, inspecao: insp, avisoCliente });
  }

  // ── REPROVAR (registra retrabalho) ──
  if (req.method === 'POST' && action === 'reprovar') {
    const { id, checklist, inspetor, itensFalhos } = req.body || {};
    const insp = db.inspecoes.find(i => i.id === id);
    if (!insp) return res.status(404).json({ ok: false, error: 'inspeção não encontrada' });
    if (checklist) insp.checklist = checklist;
    if (inspetor) insp.inspetor = String(inspetor);
    insp.status = 'reprovado';
    insp.reprovadoEm = new Date().toISOString();   // 📅 data e hora da reprovação
    insp.reprovacoes = insp.reprovacoes || [];
    insp.reprovacoes.push({
      em: new Date().toISOString(),
      inspetor: String(inspetor || ''),
      itensFalhos: Array.isArray(itensFalhos) ? itensFalhos : [],
    });
    await dbSet(KEY, db);
    return res.status(200).json({ ok: true, inspecao: insp });
  }

  // ── REINSPECIONAR (voltou do retrabalho → nova rodada) ──
  if (req.method === 'POST' && action === 'reinspecionar') {
    const { id } = req.body || {};
    const insp = db.inspecoes.find(i => i.id === id);
    if (!insp) return res.status(404).json({ ok: false, error: 'inspeção não encontrada' });
    insp.status = 'aguardando';
    insp.checklist = {};
    await dbSet(KEY, db);
    return res.status(200).json({ ok: true });
  }

  // ── EXCLUIR ──
  if (req.method === 'POST' && action === 'excluir') {
    const { id } = req.body || {};
    const antes = db.inspecoes.length;
    db.inspecoes = db.inspecoes.filter(i => i.id !== id);
    if (db.inspecoes.length === antes) return res.status(404).json({ ok: false, error: 'não encontrada' });
    await dbSet(KEY, db);
    return res.status(200).json({ ok: true });
  }

  // ── TÉCNICOS (config) ──
  if (req.method === 'POST' && action === 'config-tecnicos') {
    const { tecnicos } = req.body || {};
    if (!Array.isArray(tecnicos)) return res.status(400).json({ ok: false, error: 'tecnicos deve ser lista' });
    db.config.tecnicos = tecnicos.map(t => String(t).trim()).filter(Boolean);
    await dbSet(KEY, db);
    return res.status(200).json({ ok: true, tecnicos: db.config.tecnicos });
  }

  return res.status(400).json({ ok: false, error: 'action inválida' });
}
