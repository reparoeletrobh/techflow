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
      categoria: String(b.categoria || 'producao_interna'),   // venda · reforma · producao_interna
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
    return res.status(200).json({ ok: true, inspecao: insp,
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
