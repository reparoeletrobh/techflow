// ═══════════════════════════════════════════════════════════════════
// DISCADOR
//
// Disca para dois clientes ao mesmo tempo e entrega ao atendente o primeiro
// que atender. O segundo é desligado imediatamente; se ele já tiver atendido
// antes de a chamada cair, ouve um recado curto avisando que retornaremos —
// silêncio na linha é pior que a mensagem.
//
// A conversa acontece no navegador, pelo mesmo popup da fila de ligação.
// ═══════════════════════════════════════════════════════════════════

const U = (process.env.UPSTASH_URL || process.env.KV_REST_API_URL || '')
  .replace(/['"]/g, '').trim();
const T = (process.env.UPSTASH_TOKEN || process.env.KV_REST_API_TOKEN || '')
  .replace(/[\n\r'"]/g, '').trim();

const SID = (process.env.TWILIO_ACCOUNT_SID || '').trim();
const TOKEN = (process.env.TWILIO_AUTH_TOKEN || '').trim();
const NUMERO = (process.env.TWILIO_NUMERO || '').trim();
const APP_SID = (process.env.TWILIO_TWIML_APP_SID || '').trim();
const API_KEY = (process.env.TWILIO_API_KEY || '').trim();
const API_SECRET = (process.env.TWILIO_API_SECRET || '').trim();
const BASE = 'https://reparoeletroadm.com/api/discador';

async function dbGet(chave) {
  try {
    const r = await fetch(`${U}/get/${chave}`, { headers: { Authorization: `Bearer ${T}` } })
      .then(x => x.json());
    if (!r || r.result == null) return null;
    return typeof r.result === 'string' ? JSON.parse(r.result) : r.result;
  } catch (e) { return null; }
}
async function dbSet(chave, valor) {
  try {
    await fetch(`${U}/set/${chave}`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${T}`, 'Content-Type': 'application/json' },
      body: JSON.stringify(valor),
    });
    return true;
  } catch (e) { return false; }
}

/** Chamada à API do Twilio, com a autenticação básica que ela espera. */
async function twilio(caminho, metodo, campos) {
  const url = `https://api.twilio.com/2010-04-01/Accounts/${SID}${caminho}`;
  const auth = Buffer.from(`${SID}:${TOKEN}`).toString('base64');
  const opc = { method: metodo, headers: { Authorization: `Basic ${auth}` } };
  if (campos) {
    opc.headers['Content-Type'] = 'application/x-www-form-urlencoded';
    opc.body = new URLSearchParams(campos).toString();
  }
  const r = await fetch(url, opc);
  const txt = await r.text();
  let corpo = null;
  try { corpo = JSON.parse(txt); } catch (e) { corpo = { raw: txt.slice(0, 300) }; }
  return { status: r.status, corpo };
}

/** Número no formato internacional que o Twilio exige. */
function paraE164(tel) {
  let d = String(tel || '').replace(/\D/g, '');
  if (!d) return null;
  if (d.length === 10 || d.length === 11) d = '55' + d;
  if (d.length < 12 || d.length > 13) return null;
  return '+' + d;
}

const d8 = (t) => String(t || '').replace(/\D/g, '').slice(-8);

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const action = String((req.query || {}).action || '').trim();

  // ── as respostas de voz são pedidas pelo Twilio, sem a nossa chave ──
  const PUBLICAS = ['voz-cliente', 'voz-excedente', 'status', 'voz-conectar'];
  if (!PUBLICAS.includes(action)) {
    const chave = (req.query && req.query.k) || req.headers['x-tf-key'] || '';
    if (chave !== ((process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim())) {
      return res.status(401).json({ ok: false, error: 'não autorizado' });
    }
  }

  // ── 🔎 pronto: o que falta para o discador funcionar ──
  if (action === 'pronto') {
    const falta = [];
    if (!SID) falta.push('TWILIO_ACCOUNT_SID');
    if (!TOKEN) falta.push('TWILIO_AUTH_TOKEN');
    if (!NUMERO) falta.push('TWILIO_NUMERO');
    if (!APP_SID) falta.push('TWILIO_TWIML_APP_SID (para atender no navegador)');
    if (!API_KEY) falta.push('TWILIO_API_KEY');
    if (!API_SECRET) falta.push('TWILIO_API_SECRET');
    let conta = null;
    if (SID && TOKEN) {
      try {
        const r = await twilio('.json', 'GET', null);
        conta = r.status === 200
          ? { nome: r.corpo.friendly_name, situacao: r.corpo.status }
          : { erro: 'credencial recusada (HTTP ' + r.status + ')' };
      } catch (e) { conta = { erro: e.message }; }
    }
    return res.status(200).json({ ok: falta.length === 0,
      faltando: falta, conta, numeroDeOrigem: NUMERO || null,
      ondeConfigurar: 'Vercel → Settings → Environment Variables → Redeploy',
      observacao: 'o número de origem precisa ser brasileiro e habilitado para voz' });
  }

  // ── 📞 chamar: disca para dois e entrega o primeiro que atender ──
  if (req.method === 'POST' && action === 'chamar') {
    if (!SID || !TOKEN || !NUMERO) {
      return res.status(400).json({ ok: false, error: 'credenciais do Twilio ausentes' });
    }
    const alvos = (req.body || {}).alvos || [];
    const operador = String((req.body || {}).operador || '').trim() || 'atendente';
    if (!alvos.length) return res.status(400).json({ ok: false, error: 'informe os alvos' });

    // 🔒 no máximo dois por vez: com três, a chance de dois atenderem juntos
    // cresce e mais gente recebe ligação que não vai ser atendida
    const lote = alvos.slice(0, 2);
    const sessaoId = 'ses_' + Date.now().toString(36) + Math.random().toString(36).slice(2, 5);
    const sessao = { id: sessaoId, operador, em: new Date().toISOString(),
      status: 'discando', chamadas: [], vencedor: null };

    for (const a of lote) {
      const para = paraE164(a.telefone);
      if (!para) { sessao.chamadas.push({ telefone: a.telefone, erro: 'número inválido' }); continue; }
      try {
        const r = await twilio('/Calls.json', 'POST', {
          To: para, From: NUMERO,
          Url: `${BASE}?action=voz-cliente&sessao=${sessaoId}&tel=${encodeURIComponent(d8(para))}`,
          StatusCallback: `${BASE}?action=status&sessao=${sessaoId}`,
          StatusCallbackEvent: 'initiated ringing answered completed',
          StatusCallbackMethod: 'POST',
          // 25 segundos: passa disso, quem atenderia já teria atendido
          Timeout: '25',
          MachineDetection: 'Enable',
        });
        if (r.status === 201) {
          sessao.chamadas.push({ sid: r.corpo.sid, telefone: para,
            nome: a.nome || '?', origem: a.rotulo || '', status: 'discando' });
        } else {
          sessao.chamadas.push({ telefone: para, nome: a.nome || '?',
            erro: (r.corpo && r.corpo.message) || ('HTTP ' + r.status) });
        }
      } catch (e) {
        sessao.chamadas.push({ telefone: para, nome: a.nome || '?', erro: e.message });
      }
    }
    const livro = (await dbGet('discador_sessoes')) || { sessoes: {} };
    livro.sessoes = livro.sessoes || {};
    livro.sessoes[sessaoId] = sessao;
    // guarda 30 dias
    const corte = Date.now() - 30 * 86400000;
    for (const [id, s] of Object.entries(livro.sessoes)) {
      if (new Date(s.em || 0).getTime() < corte) delete livro.sessoes[id];
    }
    await dbSet('discador_sessoes', livro);

    const okChamadas = sessao.chamadas.filter(c => c.sid).length;
    return res.status(200).json({ ok: okChamadas > 0, sessao: sessaoId,
      discando: okChamadas, chamadas: sessao.chamadas });
  }

  // ── ☎️ voz-cliente: o que o cliente ouve quando atende ──
  // O primeiro a atender é conectado ao atendente. O segundo — se atendeu antes
  // de a chamada ser cortada — ouve um recado curto: silêncio faria a pessoa
  // pensar que foi trote.
  if (action === 'voz-cliente') {
    const sessaoId = String(req.query.sessao || '');
    const livro = (await dbGet('discador_sessoes')) || { sessoes: {} };
    const s = (livro.sessoes || {})[sessaoId];
    const meuSid = String((req.body || {}).CallSid || req.query.CallSid || '');

    res.setHeader('Content-Type', 'text/xml; charset=utf-8');
    if (!s) {
      return res.status(200).send('<?xml version="1.0" encoding="UTF-8"?><Response>' +
        '<Say language="pt-BR" voice="Polly.Camila">Desculpe, houve um erro. ' +
        'Retornaremos em breve.</Say><Hangup/></Response>');
    }

    // 🥇 o primeiro a chegar leva: marca o vencedor de forma atômica
    if (!s.vencedor) {
      s.vencedor = meuSid;
      s.status = 'em_conversa';
      s.atendidoEm = new Date().toISOString();
      const c = (s.chamadas || []).find(x => x.sid === meuSid);
      if (c) { c.status = 'atendeu'; c.atendeuEm = s.atendidoEm; }
      await dbSet('discador_sessoes', livro);

      // 📴 derruba a outra chamada imediatamente
      for (const outra of (s.chamadas || [])) {
        if (!outra.sid || outra.sid === meuSid) continue;
        try {
          await twilio('/Calls/' + outra.sid + '.json', 'POST', { Status: 'completed' });
          outra.status = 'cancelada';
        } catch (e) {}
      }
      await dbSet('discador_sessoes', livro);

      // conecta ao atendente que está no navegador
      return res.status(200).send('<?xml version="1.0" encoding="UTF-8"?><Response>' +
        '<Dial timeout="30" answerOnBridge="true">' +
        '<Client>' + (s.operador || 'atendente').replace(/[^a-zA-Z0-9_-]/g, '') + '</Client>' +
        '</Dial></Response>');
    }

    // 🥈 chegou depois: recado curto e encerra
    const c2 = (s.chamadas || []).find(x => x.sid === meuSid);
    if (c2) { c2.status = 'excedente_avisado'; c2.avisadoEm = new Date().toISOString(); }
    await dbSet('discador_sessoes', livro);
    return res.status(200).send('<?xml version="1.0" encoding="UTF-8"?><Response>' +
      '<Say language="pt-BR" voice="Polly.Camila">Olá! Aqui é da Reparo Eletro. ' +
      'Nós tentamos falar com você agora e vamos retornar em instantes. ' +
      'Obrigado pela paciência.</Say><Hangup/></Response>');
  }

  // ── 📡 status: o Twilio avisa cada mudança da chamada ──
  if (action === 'status') {
    const sessaoId = String(req.query.sessao || '');
    const b = req.body || {};
    const livro = (await dbGet('discador_sessoes')) || { sessoes: {} };
    const s = (livro.sessoes || {})[sessaoId];
    if (s) {
      const c = (s.chamadas || []).find(x => x.sid === b.CallSid);
      if (c) {
        c.ultimoStatus = b.CallStatus || null;
        if (b.AnsweredBy) c.atendidoPor = b.AnsweredBy;   // human, machine
        if (b.CallDuration) c.duracaoSeg = Number(b.CallDuration);
        if (['completed', 'busy', 'no-answer', 'failed', 'canceled'].includes(b.CallStatus)) {
          c.encerradaEm = new Date().toISOString();
        }
      }
      const todasFim = (s.chamadas || []).every(x => !x.sid || x.encerradaEm);
      if (todasFim && s.status !== 'encerrada') {
        s.status = 'encerrada';
        s.encerradaEm = new Date().toISOString();
      }
      await dbSet('discador_sessoes', livro);
    }
    res.setHeader('Content-Type', 'text/xml; charset=utf-8');
    return res.status(200).send('<?xml version="1.0" encoding="UTF-8"?><Response/>');
  }

  // ── 👀 sessao: o popup pergunta o que está acontecendo ──
  if (action === 'sessao') {
    const id = String(req.query.id || '');
    const livro = (await dbGet('discador_sessoes')) || { sessoes: {} };
    const s = (livro.sessoes || {})[id];
    if (!s) return res.status(404).json({ ok: false, error: 'sessão não encontrada' });
    const venc = (s.chamadas || []).find(c => c.sid === s.vencedor);
    return res.status(200).json({ ok: true, status: s.status,
      atendeu: venc ? { nome: venc.nome, telefone: venc.telefone,
        origem: venc.origem, atendeuEm: venc.atendeuEm } : null,
      chamadas: (s.chamadas || []).map(c => ({ nome: c.nome, telefone: c.telefone,
        status: c.status || c.ultimoStatus || 'discando',
        atendidoPor: c.atendidoPor || null, erro: c.erro || null })) });
  }

  // ── ⏹️ encerrar: o atendente desliga ou desiste ──
  if (req.method === 'POST' && action === 'encerrar') {
    const id = String((req.body || {}).sessao || '');
    const livro = (await dbGet('discador_sessoes')) || { sessoes: {} };
    const s = (livro.sessoes || {})[id];
    if (!s) return res.status(404).json({ ok: false, error: 'sessão não encontrada' });
    for (const c of (s.chamadas || [])) {
      if (!c.sid || c.encerradaEm) continue;
      try { await twilio('/Calls/' + c.sid + '.json', 'POST', { Status: 'completed' }); }
      catch (e) {}
    }
    s.status = 'encerrada';
    s.encerradaEm = new Date().toISOString();
    await dbSet('discador_sessoes', livro);
    return res.status(200).json({ ok: true });
  }

  // ── 📊 relatorio: como o discador está se saindo ──
  if (action === 'relatorio') {
    const dias = Math.max(1, Math.min(30, parseInt(req.query.dias || '7', 10)));
    const desde = Date.now() - dias * 86400000;
    const livro = (await dbGet('discador_sessoes')) || { sessoes: {} };
    const ses = Object.values(livro.sessoes || {})
      .filter(s => new Date(s.em || 0).getTime() >= desde);
    const chamadas = ses.flatMap(s => s.chamadas || []).filter(c => c.sid);
    const atenderam = chamadas.filter(c => c.status === 'atendeu');
    const excedentes = chamadas.filter(c => c.status === 'excedente_avisado');
    const secretaria = chamadas.filter(c => c.atendidoPor === 'machine_start' ||
      String(c.atendidoPor || '').startsWith('machine'));
    const hh = d => d ? new Date(new Date(d).getTime() - 3 * 3600000)
      .toISOString().slice(5, 16).replace('T', ' ') : '—';
    const dur = atenderam.map(c => c.duracaoSeg).filter(x => x > 0);
    return res.status(200).json({ ok: true, periodoDias: dias,
      RESUMO: {
        sessoes: ses.length,
        chamadasFeitas: chamadas.length,
        conversas: atenderam.length,
        taxaDeAtendimento: chamadas.length
          ? Math.round(atenderam.length / chamadas.length * 100) + '%' : '—',
        // 🎯 é este número que diz se a rajada de dois compensa
        excedentesAvisados: excedentes.length,
        pctExcedentes: atenderam.length
          ? Math.round(excedentes.length / atenderam.length * 100) + '%' : '—',
        caixaPostal: secretaria.length,
        duracaoMediaSeg: dur.length
          ? Math.round(dur.reduce((a, b) => a + b, 0) / dur.length) : null,
      },
      ULTIMAS: ses.sort((a, b) => String(b.em).localeCompare(String(a.em))).slice(0, 30)
        .map(s => hh(s.em) + ' | ' + (s.operador || '?') + ' | ' + (s.status || '?') +
          ' | ' + (s.chamadas || []).map(c => (c.nome || '?').slice(0, 14) +
            '(' + (c.status || c.ultimoStatus || '?') + ')').join(' + ')) });
  }

  return res.status(404).json({ ok: false, error: 'ação não encontrada',
    disponiveis: ['pronto', 'chamar', 'sessao', 'encerrar', 'relatorio'] });
}
