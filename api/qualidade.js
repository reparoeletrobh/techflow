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
  // ── 📊 CONTADORES: por dia, semana e por técnico ──
  if (action === 'contadores') {
    const insp = db.inspecoes || [];
    const bras = new Date(Date.now() - 3 * 3600000);
    const hoje = bras.toISOString().slice(0, 10);
    const diaSem = bras.getUTCDay();
    const seg = new Date(bras); seg.setUTCDate(bras.getUTCDate() - ((diaSem === 0) ? 6 : (diaSem - 1)));
    const iniSemana = seg.toISOString().slice(0, 10);
    const dia = d => String(d || '').slice(0, 10);

    // 🎯 a meta de 25/dia NÃO conta as que vieram de garantia
    const deGarantia = i => /garantia/i.test(String(i.origem || '') + ' ' + String(i.tipo || ''));
    const entrouHoje = insp.filter(i => dia(i.criadoEm) === hoje);
    const entrouSemana = insp.filter(i => dia(i.criadoEm) >= iniSemana);
    const hojeSemGarantia = entrouHoje.filter(i => !deGarantia(i));
    const semanaSemGarantia = entrouSemana.filter(i => !deGarantia(i));
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

    // ── 📲 AVISA O CLIENTE com o resultado do controle de qualidade ──
    let avisoCliente = null;
    try {
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
