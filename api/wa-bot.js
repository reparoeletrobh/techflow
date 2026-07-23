// api/wa-bot.js — Cérebro do bot (FASE 1: COPILOTO — sugere, humano aprova)
// actions: conversas | historico&tel= | sugerir&tel= | enviar (POST) | config
const U = (process.env.UPSTASH_URL || '').replace(/['"]/g, '').trim();
const T = (process.env.UPSTASH_TOKEN || '').replace(/[\n\r'"]/g, '').trim();
let WA_TOKEN = (process.env.WA_TOKEN || '').trim();
let WA_PHONE_ID = (process.env.WA_PHONE_ID || '').trim();
async function credenciais() {
  // Envs da Vercel têm prioridade; fallback: chave wa_credenciais no Redis
  if (WA_TOKEN && WA_PHONE_ID) return { token: WA_TOKEN, phoneId: WA_PHONE_ID };
  const c = await dbGet('wa_credenciais');
  return { token: (c && c.token) || WA_TOKEN, phoneId: (c && c.phoneId) || WA_PHONE_ID };
}
const ANTHROPIC_KEY = (process.env.ANTHROPIC_API_KEY || '').trim();

const EVT_LIST = 'wa_evt_list';

async function dbGet(key) {
  try {
    const r = await fetch(`${U}/get/${key}`, { headers: { Authorization: `Bearer ${T}` } });
    const j = await r.json();
    let v = j.result;
    if (typeof v === 'string') v = JSON.parse(v);
    if (typeof v === 'string') v = JSON.parse(v);
    return v;
  } catch { return null; }
}
async function dbSet(key, val) {
  await fetch(`${U}/set/${key}`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${T}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(val),
  });
}
async function lerEvts() {
  try {
    const r = await fetch(`${U}/lrange/${EVT_LIST}/0/-1`, { headers: { Authorization: `Bearer ${T}` } });
    const j = await r.json();
    const out = [];
    for (const s of (j.result || [])) { try { out.push(JSON.parse(s)); } catch (_) {} }
    return out;
  } catch { return []; }
}
async function rpushEvt(evt) {
  try {
    await fetch(`${U}/rpush/${EVT_LIST}/${encodeURIComponent(JSON.stringify(evt))}`,
      { headers: { Authorization: `Bearer ${T}` } });
  } catch (_) {}
}

// Contexto do cliente nos sistemas (por últimos 8 dígitos do telefone)
const FASE_TECNICO_LBL = {
  producao: 'em bancada (produção)', aguardando_peca: 'aguardando chegada de peça',
  conserto_concluido: 'conserto concluído', teste_realizado: 'testado e aprovado no teste de qualidade',
  aguardando_ret: 'pronto — aguardando retirada na loja', solicitar_entrega: 'pronto — entrega sendo agendada',
  entrega_realizada: 'entregue', coleta_solicitada: 'coleta a caminho', erp: 'finalizado (registro)',
};

async function contextoCliente(tel) {
  const d8 = String(tel).replace(/\D/g, '').slice(-8);
  const ctx = { fichas: [], logistica: [], pipe: [], tecnico: [], pecas: [] };
  try {
    const [fa, lg, pp, bd, pcs] = await Promise.all([
      dbGet('fichas_adm'), dbGet('reparoeletro_logistica'), dbGet('reparoeletro_pipe'),
      dbGet('reparoeletro_board'), dbGet('reparoeletro_compras_pecas'),
    ]);
    const bate = (t) => String(t || '').replace(/\D/g, '').endsWith(d8);
    for (const f of ((fa && fa.fichas) || [])) if (bate(f.telefone)) {
      ctx.fichas.push({ id: f.id, nome: f.nome, status: f.status, equipamento: f.equipamento, defeito: f.defeito });
    }
    for (const f of ((lg && lg.fichas) || [])) if (bate(f.telefone)) {
      ctx.logistica.push({ id: f.id, nome: f.nome, fase: f.phase, equipamento: f.equipamento,
        orcamento: (f.diagnostico && f.diagnostico.preco) || f.orcamentoValor || null,
        textoOrcamento: (f.diagnostico && f.diagnostico.textoOrc) || null });
    }
    for (const c of ((pp && pp.cards) || [])) if (bate(c.telefone)) {
      ctx.pipe.push({ id: c.id, nome: c.nomeContato, fase: c.phase, equipamento: c.equipamento, valor: c.valor || null });
    }
    // Board do técnico: estágio REAL da OS (produção/peça/teste/entrega)
    const nomesCliente = [];
    for (const c of ((bd && bd.cards) || [])) if (bate(c.telefone || c.tel)) {
      ctx.tecnico.push({ os: c.os || c.numero || c.id, equipamento: c.equipamento || c.title || '',
        estagio: FASE_TECNICO_LBL[c.phase] || c.phase, fluxo: c.fluxo || c.tipo || '' });
      if (c.nomeContato || c.nome) nomesCliente.push(String(c.nomeContato || c.nome).toLowerCase().split(' ')[0]);
    }
    // Peças ligadas às OSs do cliente (previsão de chegada)
    const ossCliente = new Set(ctx.tecnico.map(t => String(t.os)));
    for (const p of ((pcs && pcs.pecas) || [])) {
      const pos = String(p.os || p.osNum || '');
      const pnome = String(p.cliente || p.nome || '').toLowerCase();
      if ((pos && ossCliente.has(pos)) || (pnome && nomesCliente.some(n => n && pnome.includes(n)))) {
        ctx.pecas.push({ os: pos, peca: p.peca || p.descricao || '', status: p.status || '',
          previsao: p.previsao || p.prazo || p.chegadaPrevista || null });
      }
    }
  } catch (_) {}
  return ctx;
}

const CONFIG_DEFAULT = {
  descontoPix: 10,          // % à vista no Pix
  descontoBalcao: 15,       // % se trouxer/retirar na loja
  politicaTroca: 'Aceitamos seu equipamento na troca por um seminovo revisado com garantia — o valor dele vira desconto.',
  politicaCompra: 'Também compramos seu equipamento usado, mesmo com defeito.',
  argumentoNovo: 'Equipamentos novos de preço parecido geralmente são de linha inferior (menor potência, menos capacidade e vida útil menor). O conserto devolve a vida útil do SEU equipamento, que é superior.',
};

export default async function handler(req, res) {
  // 🔐 TF-AUTH (Fase 1): chave obrigatória em toda chamada
  const _tfk = (req.query && req.query.k) || req.headers['x-tf-key'] || '';
  if (_tfk !== ((process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim())) {
    return res.status(401).json({ ok: false, error: 'não autorizado' });
  }

  res.setHeader('Cache-Control', 'no-cache');
  const action = req.query.action || '';

  // ── ABORDAGEM-FICHAS (cron 5min): ficha criada há 5-60min sem conversa iniciada → template cadastro_recebido ──
  // Interruptor: wa_bot_config.abordagemAtiva (false por padrão — ligar quando o número real estiver ativo)
  if (action === 'abordagem-fichas') {
    const cfgA = (await dbGet('wa_bot_config')) || {};
    if (cfgA.abordagemAtiva !== true) return res.status(200).json({ ok: true, msg: 'abordagem desligada (wa_bot_config.abordagemAtiva)' });
    const { token, phoneId } = await credenciais();
    if (!token || !phoneId) return res.status(200).json({ ok: false, error: 'credenciais ausentes' });
    const agora = Date.now();
    const [fdb, evts, abordados] = await Promise.all([
      dbGet('fichas_adm'), lerEvts(), dbGet('wa_abordados').then(v => v || { tels: {} }),
    ]);
    // Telefones que JÁ iniciaram conversa (qualquer evento in)
    const jaFalaram = new Set(evts.filter(e => e.dir === 'in').map(e => String(e.tel).slice(-8)));
    const candidatas = ((fdb && fdb.fichas) || []).filter(f => {
      const idade = agora - new Date(f.criadoEm || 0).getTime();
      const d8 = String(f.telefone || '').replace(/\D/g, '').slice(-8);
      return idade > 5 * 60000 && idade < 60 * 60000 && d8.length >= 8 &&
        !jaFalaram.has(d8) && !abordados.tels[d8];
    }).slice(0, 10); // máx 10 por ciclo (segurança)
    const disparadas = [];
    for (const f of candidatas) {
      const telA = String(f.telefone).replace(/\D/g, '');
      const to = telA.startsWith('55') ? telA : '55' + telA;
      try {
        const r = await fetch(`https://graph.facebook.com/v20.0/${phoneId}/messages`, {
          method: 'POST',
          headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
          body: JSON.stringify({ messaging_product: 'whatsapp', to, type: 'template',
            template: { name: 'cadastro_recebido', language: { code: 'pt_BR' },
              components: [{ type: 'body', parameters: [
                { type: 'text', text: (f.nome || 'tudo bem').split(' ')[0] },
                { type: 'text', text: f.equipamento || 'equipamento' },
              ] }] } }),
        });
        const j = await r.json();
        const okA = !!(j.messages && j.messages[0]);
        abordados.tels[telA.slice(-8)] = new Date().toISOString();
        await rpushEvt({ ts: new Date().toISOString(), tel: to, dir: 'out',
          texto: '📨 [abordagem automática] cadastro_recebido — ' + (f.nome || ''), tipo: 'template' });
        disparadas.push({ nome: f.nome, ok: okA });
      } catch (e) { disparadas.push({ nome: f.nome, erro: e.message }); }
    }
    // Poda do registro (30 dias)
    const corteA = agora - 30 * 86400000;
    for (const k of Object.keys(abordados.tels)) {
      if (new Date(abordados.tels[k]).getTime() < corteA) delete abordados.tels[k];
    }
    await dbSet('wa_abordados', abordados);
    return res.status(200).json({ ok: true, candidatas: candidatas.length, disparadas });
  }

  // ── CRIAR-TEMPLATES: registra os templates Utility na Meta (aprovação ~horas) ──
  if (action === 'criar-templates') {
    const wabaId = String(req.query.waba || '1699351717944043').trim();
    const { token } = await credenciais();
    if (!token) return res.status(200).json({ ok: false, error: 'sem token' });
    const templates = [
      { name: 'cadastro_recebido', language: 'pt_BR', category: 'UTILITY',
        components: [{ type: 'BODY',
          text: 'Olá {{1}}, tudo bem? Alessandro aqui, responsável pela logística da Reparo Eletro. Recebemos o seu cadastro para o conserto do seu {{2}}!\n\nTEMOS 2 OPÇÕES: COLETA E ENTREGA / ATENDIMENTO NO BALCÃO\n\n*ATENÇÃO: Trazendo seu equipamento aqui na loja, o orçamento é gratuito e consertamos em 15 minutos! Estamos na Rua Ouro Preto, 663 - Barro Preto*\n\nCaso prefira a nossa coleta e entrega, podemos buscar hoje mesmo na sua casa!\n\nJá estamos prontos para te atender! Me fala qual opção você escolheu, por favor? 😊',
          example: { body_text: [['Maria', 'purificador']] } }] },
      { name: 'orcamento_pronto', language: 'pt_BR', category: 'UTILITY',
        components: [{ type: 'BODY',
          text: 'Olá {{1}}! Aqui é da Reparo Eletro 😊 O diagnóstico do seu {{2}} ficou pronto e já temos o orçamento do conserto. Posso te enviar os detalhes por aqui?',
          example: { body_text: [['Maria', 'micro-ondas']] } }] },
      { name: 'conserto_finalizado', language: 'pt_BR', category: 'UTILITY',
        components: [{ type: 'BODY',
          text: 'Olá {{1}}! 🛠️ A equipe técnica acabou de finalizar o conserto do seu equipamento. Agora ele entra para a fase de testes. Assim que a fase de testes for finalizada, nossa equipe entrará em contato com você para fazer a entrega.',
          example: { body_text: [['Maria']] } }] },
      { name: 'coleta_confirmada', language: 'pt_BR', category: 'UTILITY',
        components: [{ type: 'BODY',
          text: 'Olá {{1}}! Sua coleta do {{2}} está confirmada para {{3}}. Nosso motorista entra em contato quando estiver a caminho. 🚚',
          example: { body_text: [['Maria', 'micro-ondas', 'amanhã de manhã']] } }] },
    ];
    const resultados = {};
    for (const t of templates) {
      try {
        const r = await fetch(`https://graph.facebook.com/v20.0/${wabaId}/message_templates`, {
          method: 'POST',
          headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
          body: JSON.stringify(t),
        });
        resultados[t.name] = await r.json();
      } catch (e) { resultados[t.name] = { erro: e.message }; }
    }
    return res.status(200).json({ ok: true, resultados });
  }

  // ── PERFIL-FOTO (GET): sobe a logo (do próprio site) como foto de perfil do número via Meta Resumable Upload ──
  if (action === 'perfil-foto') {
    const appId = String(req.query.app || '1007161065497390');
    const { token, phoneId } = await credenciais();
    if (!token || !phoneId) return res.status(200).json({ ok: false, error: 'credenciais ausentes' });
    try {
      // 1. Baixar a logo do próprio domínio
      const imgR = await fetch('https://reparoeletroadm.com/logo-wa.jpg');
      if (!imgR.ok) return res.status(200).json({ ok: false, error: 'logo-wa.jpg não encontrada no site' });
      const buf = Buffer.from(await imgR.arrayBuffer());
      // 2. Abrir sessão de upload
      const s1 = await fetch(`https://graph.facebook.com/v20.0/${appId}/uploads?file_length=${buf.length}&file_type=image/jpeg&access_token=${encodeURIComponent(token)}`, { method: 'POST' });
      const j1 = await s1.json();
      if (!j1.id) return res.status(200).json({ ok: false, passo: 'sessao', meta: j1 });
      // 3. Enviar o binário
      const s2 = await fetch(`https://graph.facebook.com/v20.0/${j1.id}`, {
        method: 'POST',
        headers: { Authorization: `OAuth ${token}`, file_offset: '0', 'Content-Type': 'application/octet-stream' },
        body: buf,
      });
      const j2 = await s2.json();
      if (!j2.h) return res.status(200).json({ ok: false, passo: 'upload', meta: j2 });
      // 4. Aplicar no perfil do número
      const s3 = await fetch(`https://graph.facebook.com/v20.0/${phoneId}/whatsapp_business_profile`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ messaging_product: 'whatsapp', profile_picture_handle: j2.h }),
      });
      const j3 = await s3.json();
      return res.status(200).json({ ok: !!j3.success, aplicado: j3 });
    } catch (e) { return res.status(200).json({ ok: false, error: e.message }); }
  }

  // ── ORCAMENTOS-PENDENTES (cron 3min + manual): orçamento registrado → envia ao cliente ──
  // TRAVA DE TESTE: só age em telefones de wa_bot_config.execTels
  if (action === 'orcamentos-pendentes') {
    const cfgO = (await dbGet('wa_bot_config')) || {};
    const telsO = Array.isArray(cfgO.execTels) ? cfgO.execTels : [];
    if (!telsO.length) return res.status(200).json({ ok: true, msg: 'nenhum telefone autorizado' });
    const { token: tkO, phoneId: pidO } = await credenciais();
    if (!tkO || !pidO) return res.status(200).json({ ok: false, error: 'credenciais ausentes' });
    const [logO, enviadosO, evtsO] = await Promise.all([
      dbGet('reparoeletro_logistica'), dbGet('wa_orc_enviados').then(v => v || { ids: {} }), lerEvts(),
    ]);
    const d8ok = t => telsO.some(x => String(x).replace(/\D/g, '').slice(-8) === String(t).replace(/\D/g, '').slice(-8));
    const janelaAberta = tel8 => {
      let ult = null;
      for (const e of evtsO) if (e.dir === 'in' && String(e.tel).slice(-8) === tel8) ult = e.ts;
      return ult && (Date.now() - new Date(ult).getTime()) < 24 * 3600000;
    };
    const disparos = [];
    for (const f of ((logO && logO.fichas) || [])) {
      if (f.phase !== 'orc_registrado') continue;
      const txtOrc = f.diagnostico && f.diagnostico.textoOrc;
      if (!txtOrc) continue;
      if (!d8ok(f.telefone)) continue;               // trava de teste
      if (enviadosO.ids[f.id]) continue;              // dedupe
      const telO = String(f.telefone).replace(/\D/g, '');
      const to = telO.startsWith('55') ? telO : '55' + telO;
      const t8 = to.slice(-8);
      try {
        if (janelaAberta(t8)) {
          // Janela aberta → orçamento oficial direto
          const r = await fetch(`https://graph.facebook.com/v20.0/${pidO}/messages`, {
            method: 'POST', headers: { Authorization: `Bearer ${tkO}`, 'Content-Type': 'application/json' },
            body: JSON.stringify({ messaging_product: 'whatsapp', to, type: 'text', text: { body: String(txtOrc).slice(0, 3500) } }),
          });
          const j = await r.json();
          const okO = !!(j.messages && j.messages[0]);
          await rpushEvt({ ts: new Date().toISOString(), tel: to, dir: 'out',
            texto: String(txtOrc).slice(0, 2000), msgId: okO ? j.messages[0].id : null, tipo: 'text', via: 'bot-auto-orcamento' });
          disparos.push({ nome: f.nome, modo: 'orcamento-direto', ok: okO });
        } else {
          // Janela fechada → template orcamento_pronto (a resposta reabre e o cérebro envia o orçamento)
          const r = await fetch(`https://graph.facebook.com/v20.0/${pidO}/messages`, {
            method: 'POST', headers: { Authorization: `Bearer ${tkO}`, 'Content-Type': 'application/json' },
            body: JSON.stringify({ messaging_product: 'whatsapp', to, type: 'template',
              template: { name: 'orcamento_pronto', language: { code: 'pt_BR' },
                components: [{ type: 'body', parameters: [
                  { type: 'text', text: (f.nome || 'tudo bem').split(' ')[0] },
                  { type: 'text', text: f.equipamento || 'equipamento' } ] }] } }),
          });
          const j = await r.json();
          const okO = !!(j.messages && j.messages[0]);
          await rpushEvt({ ts: new Date().toISOString(), tel: to, dir: 'out',
            texto: '📨 [template orcamento_pronto] ' + (f.nome || ''), tipo: 'template', via: 'bot-auto-orcamento' });
          disparos.push({ nome: f.nome, modo: 'template-janela-fechada', ok: okO });
        }
        enviadosO.ids[f.id] = new Date().toISOString();
      } catch (e) { disparos.push({ nome: f.nome, erro: e.message }); }
    }
    // poda 60d
    const corteO = Date.now() - 60 * 86400000;
    for (const k of Object.keys(enviadosO.ids)) if (new Date(enviadosO.ids[k]).getTime() < corteO) delete enviadosO.ids[k];
    await dbSet('wa_orc_enviados', enviadosO);
    return res.status(200).json({ ok: true, disparos });
  }

  // ── AUTO-RESPONDER: cérebro responde sozinho (chamado pelo webhook p/ telefones autorizados) ──
  if (action === 'auto-responder') {
    const telAR = String(req.query.tel || (req.body && req.body.tel) || '').replace(/\D/g, '');
    if (!telAR) return res.status(400).json({ ok: false, error: 'informe tel' });
    const KCH = (process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim();
    const BASE = 'https://reparoeletroadm.com';
    try {
      const sg = await fetch(`${BASE}/api/wa-bot?action=sugerir&tel=${telAR}&k=${KCH}`).then(r => r.json());
      if (!sg.ok || !sg.sugestao || !sg.sugestao.resposta) {
        return res.status(200).json({ ok: false, passo: 'sugerir', meta: sg.error || 'sem sugestão' });
      }
      const acaoT = (sg.sugestao.acao && sg.sugestao.acao.tipo) || 'nenhuma';
      const env = await fetch(`${BASE}/api/wa-bot?action=enviar&k=${KCH}`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ tel: telAR, texto: sg.sugestao.resposta, acaoAprovada: acaoT, via: 'bot-auto' }),
      }).then(r => r.json());
      return res.status(200).json({ ok: !!env.ok, acao: acaoT, envio: env.ok ? 'enviado' : env.error });
    } catch (e) { return res.status(200).json({ ok: false, error: e.message }); }
  }

  // ── AUTORIZAR-EXEC (GET): adiciona telefone à lista de execução real de ações (?tel=) ──
  if (action === 'autorizar-exec') {
    const telA2 = String(req.query.tel || '').replace(/\D/g, '');
    if (!telA2) return res.status(400).json({ ok: false, error: 'informe ?tel=' });
    const cfgE = (await dbGet('wa_bot_config')) || {};
    cfgE.execTels = Array.isArray(cfgE.execTels) ? cfgE.execTels : [];
    if (!cfgE.execTels.includes(telA2)) cfgE.execTels.push(telA2);
    await dbSet('wa_bot_config', cfgE);
    return res.status(200).json({ ok: true, execTels: cfgE.execTels, msg: 'ações reais autorizadas SÓ para estes telefones' });
  }

  // ── TESTE-FICHA (GET): cria ficha de teste em fichas_adm para ensaio do cérebro (?tel=&nome=&equip=&end=) ──
  if (action === 'teste-ficha') {
    const telF = String(req.query.tel || '').replace(/\D/g, '');
    if (!telF) return res.status(400).json({ ok: false, error: 'informe ?tel=' });
    const fdb = (await dbGet('fichas_adm')) || { fichas: [] };
    if (!Array.isArray(fdb.fichas)) fdb.fichas = [];
    const fichaT = {
      id: 'TESTE-' + Date.now().toString(36),
      nome: String(req.query.nome || 'Pedro Teste'),
      telefone: telF,
      endereco: String(req.query.end || 'Rua Exemplo, 123 - Barro Preto, BH'),
      equipamento: String(req.query.equip || 'Micro-ondas'),
      defeito: String(req.query.defeito || 'Não esquenta'),
      sistema: 'adm', status: 'criada',
      criadoEm: new Date().toISOString(),
      contatoFeitoEm: null, logisticaEm: null, teste: true,
    };
    fdb.fichas.unshift(fichaT);
    await dbSet('fichas_adm', fdb);
    return res.status(200).json({ ok: true, ficha: fichaT, msg: 'ficha de teste criada — gere a sugestão no painel' });
  }

  // ── TESTE-TEMPLATE (GET): dispara um template aprovado para validação (?tpl=&tel=&p1=&p2=&p3=) ──
  if (action === 'teste-template') {
    const tpl = String(req.query.tpl || 'cadastro_recebido').trim();
    const telT = String(req.query.tel || '').replace(/\D/g, '');
    if (!telT) return res.status(400).json({ ok: false, error: 'informe ?tel=' });
    const params = [req.query.p1, req.query.p2, req.query.p3].filter(v => v !== undefined && v !== '');
    const { token, phoneId } = await credenciais();
    if (!token || !phoneId) return res.status(200).json({ ok: false, error: 'credenciais ausentes' });
    try {
      const comps = params.length
        ? [{ type: 'body', parameters: params.map(p => ({ type: 'text', text: String(p) })) }] : undefined;
      const r = await fetch(`https://graph.facebook.com/v20.0/${phoneId}/messages`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ messaging_product: 'whatsapp', to: telT, type: 'template',
          template: { name: tpl, language: { code: 'pt_BR' }, ...(comps ? { components: comps } : {}) } }),
      });
      const j = await r.json();
      const okT = !!(j.messages && j.messages[0]);
      await rpushEvt({ ts: new Date().toISOString(), tel: telT, dir: 'out',
        texto: '📨 [teste-template ' + tpl + '] ' + params.join(' · '), tipo: 'template' });
      return res.status(200).json({ ok: okT, template: tpl, meta: okT ? 'enviado — olha o WhatsApp!' : j });
    } catch (e) { return res.status(200).json({ ok: false, error: e.message }); }
  }

  // ── DELETAR-TEMPLATE: remove um template registrado (?nome=) ──
  if (action === 'deletar-template') {
    const wabaId = String(req.query.waba || '1699351717944043').trim();
    const nomeT = String(req.query.nome || '').trim();
    const { token } = await credenciais();
    if (!nomeT) return res.status(400).json({ ok: false, error: 'informe ?nome=' });
    try {
      const r = await fetch(`https://graph.facebook.com/v20.0/${wabaId}/message_templates?name=${encodeURIComponent(nomeT)}`, {
        method: 'DELETE', headers: { Authorization: `Bearer ${token}` } });
      return res.status(200).json({ ok: true, resultado: await r.json() });
    } catch (e) { return res.status(200).json({ ok: false, error: e.message }); }
  }

  // ── STATUS-TEMPLATES: consulta aprovação dos templates ──
  if (action === 'status-templates') {
    const wabaId = String(req.query.waba || '1699351717944043').trim();
    const { token } = await credenciais();
    try {
      const r = await fetch(`https://graph.facebook.com/v20.0/${wabaId}/message_templates?fields=name,status,category`, {
        headers: { Authorization: `Bearer ${token}` } });
      const j = await r.json();
      return res.status(200).json({ ok: true, templates: (j.data || []).map(t => ({ nome: t.name, status: t.status, cat: t.category })) });
    } catch (e) { return res.status(200).json({ ok: false, error: e.message }); }
  }

  // ── JANELA: a conversa com esse tel tem janela de 24h aberta? ──
  if (action === 'janela') {
    const telJ = String(req.query.tel || '').replace(/\D/g, '');
    const evts = await lerEvts();
    let ultimaIn = null;
    for (const e of evts) if (e.dir === 'in' && String(e.tel).endsWith(telJ.slice(-8))) ultimaIn = e.ts;
    const aberta = ultimaIn && (Date.now() - new Date(ultimaIn).getTime()) < 24 * 3600000;
    return res.status(200).json({ ok: true, tel: telJ, janelaAberta: !!aberta, ultimaMsgCliente: ultimaIn,
      expiraEm: aberta ? new Date(new Date(ultimaIn).getTime() + 24 * 3600000).toISOString() : null });
  }

  // ── ENVIAR-TEMPLATE: inicia conversa oficial (POST {tel, template, params[]}) ──
  if (req.method === 'POST' && action === 'enviar-template') {
    const { tel, template, params } = req.body || {};
    const { token, phoneId } = await credenciais();
    if (!tel || !template) return res.status(400).json({ ok: false, error: 'tel e template obrigatórios' });
    if (!token || !phoneId) return res.status(200).json({ ok: false, error: 'credenciais ausentes' });
    try {
      const comps = (params && params.length)
        ? [{ type: 'body', parameters: params.map(p => ({ type: 'text', text: String(p) })) }] : undefined;
      const r = await fetch(`https://graph.facebook.com/v20.0/${phoneId}/messages`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ messaging_product: 'whatsapp', to: String(tel).replace(/\D/g, ''),
          type: 'template', template: { name: template, language: { code: 'pt_BR' },
          ...(comps ? { components: comps } : {}) } }),
      });
      const j = await r.json();
      const okS = !!(j.messages && j.messages[0]);
      await rpushEvt({ ts: new Date().toISOString(), tel: String(tel).replace(/\D/g, ''), dir: 'out',
        texto: '📨 [template ' + template + '] ' + (params || []).join(' · '),
        msgId: okS ? j.messages[0].id : null, tipo: 'template' });
      return res.status(200).json({ ok: okS, meta: okS ? 'template enviado' : JSON.stringify(j).slice(0, 400) });
    } catch (e) { return res.status(200).json({ ok: false, error: e.message }); }
  }

  // ── WABA-SUBSCRIBE: inscreve o app no WhatsApp Business Account ──
  // (sem isso, o botão de teste funciona mas eventos REAIS não fluem)
  if (action === 'waba-subscribe') {
    const wabaId = String(req.query.waba || '1699351717944043').trim();
    const { token } = await credenciais();
    if (!token) return res.status(200).json({ ok: false, error: 'sem token — rode setup-credenciais' });
    const out = { wabaId };
    try {
      const r1 = await fetch(`https://graph.facebook.com/v20.0/${wabaId}/subscribed_apps`, {
        headers: { Authorization: `Bearer ${token}` } });
      out.antes = await r1.json();
    } catch (e) { out.antes = { erro: e.message }; }
    try {
      const r2 = await fetch(`https://graph.facebook.com/v20.0/${wabaId}/subscribed_apps`, {
        method: 'POST', headers: { Authorization: `Bearer ${token}` } });
      out.inscricao = await r2.json();
    } catch (e) { out.inscricao = { erro: e.message }; }
    try {
      const r3 = await fetch(`https://graph.facebook.com/v20.0/${wabaId}/subscribed_apps`, {
        headers: { Authorization: `Bearer ${token}` } });
      out.depois = await r3.json();
    } catch (e) { out.depois = { erro: e.message }; }
    return res.status(200).json({ ok: true, ...out });
  }

  // ── TESTAR-WEBHOOK: injeta uma mensagem simulada (valida armazenamento) ──
  if (action === 'testar-webhook') {
    try {
      const r = await fetch('https://reparoeletroadm.com/api/wa-webhook', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          entry: [{ changes: [{ value: {
            contacts: [{ wa_id: '5500TESTE', profile: { name: 'Teste Interno' } }],
            messages: [{ from: '5500TESTE', id: 'wamid.teste.' + Date.now(), timestamp: String(Math.floor(Date.now()/1000)),
              type: 'text', text: { body: '🧪 mensagem simulada — teste do armazenamento' } }],
          } }] }],
        }),
      });
      const j = await r.json();
      const evts = await lerEvts();
      return res.status(200).json({ ok: true, webhookRespondeu: j, eventosNaLista: evts.length,
        veredito: evts.length > 0 ? '✅ Armazenamento OK — se a Meta enviar, nós recebemos' : '❌ Webhook respondeu mas nada foi gravado' });
    } catch (e) {
      return res.status(200).json({ ok: false, error: e.message });
    }
  }

  // ── EVENTOS-DEBUG: últimos eventos crus (mensagens + recibos de entrega) ──
  if (action === 'eventos-debug') {
    const evts = await lerEvts();
    return res.status(200).json({ ok: true, total: evts.length, ultimos: evts.slice(-30) });
  }

  // ── SETUP-CREDENCIAIS: grava token/phoneId no Redis (fase de testes) ──
  if (action === 'setup-credenciais') {
    const tk = String(req.query.token || '').trim();
    const pid = String(req.query.phoneId || '').trim();
    if (!tk || !pid) return res.status(400).json({ ok: false, error: 'informe ?token=&phoneId=' });
    await dbSet('wa_credenciais', { token: tk, phoneId: pid, em: new Date().toISOString() });
    return res.status(200).json({ ok: true, msg: 'Credenciais salvas — rode o diag-envio' });
  }

  // ── DIAG-ENVIO: valida o token, o número e tenta enviar (mostra o erro EXATO da Meta) ──
  if (action === 'diag-envio') {
    const tel = String(req.query.tel || '').replace(/\D/g, '');
    const { token, phoneId } = await credenciais();
    const out = { credenciais: { temToken: !!token, temPhoneId: !!phoneId, phoneId } };
    if (!token || !phoneId) return res.status(200).json({ ok: false, ...out, error: 'Credenciais ausentes — rode setup-credenciais primeiro' });
    // 1. Validar token + número
    try {
      const r1 = await fetch(`https://graph.facebook.com/v20.0/${phoneId}?fields=display_phone_number,verified_name,quality_rating`, {
        headers: { Authorization: `Bearer ${token}` } });
      out.infoNumero = await r1.json();
    } catch (e) { out.infoNumero = { erro: e.message }; }
    // 2. Enviar template hello_world (não exige janela de 24h)
    if (tel) {
      try {
        const r2 = await fetch(`https://graph.facebook.com/v20.0/${phoneId}/messages`, {
          method: 'POST',
          headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
          body: JSON.stringify({ messaging_product: 'whatsapp', to: tel, type: 'template',
            template: { name: 'hello_world', language: { code: 'en_US' } } }),
        });
        out.envioTemplate = await r2.json();
      } catch (e) { out.envioTemplate = { erro: e.message }; }
      // 3. Enviar texto livre (exige janela aberta)
      try {
        const r3 = await fetch(`https://graph.facebook.com/v20.0/${phoneId}/messages`, {
          method: 'POST',
          headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
          body: JSON.stringify({ messaging_product: 'whatsapp', to: tel, type: 'text',
            text: { body: '✅ Teste do bot Reparo Eletro — canal funcionando!' } }),
        });
        out.envioTexto = await r3.json();
      } catch (e) { out.envioTexto = { erro: e.message }; }
    }
    return res.status(200).json({ ok: true, ...out });
  }

  // ── Lista de conversas (agrupadas da lista de eventos) ──
  if (action === 'conversas') {
    const evts = await lerEvts();
    const conv = {};
    for (const e of evts) {
      if (e.dir === 'status' || !e.tel) continue;
      if (!conv[e.tel]) conv[e.tel] = { tel: e.tel, nome: '', msgs: 0, ultimaMsg: '', ultimaTs: '', naoRespondida: false };
      const c = conv[e.tel];
      c.msgs++;
      if (e.nome) c.nome = e.nome;
      c.ultimaMsg = (e.dir === 'in' ? '👤 ' : '🤖 ') + String(e.texto || '').slice(0, 60);
      c.ultimaTs = e.ts;
      c.naoRespondida = (e.dir === 'in');
    }
    const lista = Object.values(conv).sort((a, b) => String(b.ultimaTs).localeCompare(String(a.ultimaTs)));
    return res.status(200).json({ ok: true, total: lista.length, conversas: lista.slice(0, 100) });
  }

  // ── Histórico de uma conversa ──
  if (action === 'historico') {
    const tel = String(req.query.tel || '');
    const evts = await lerEvts();
    const msgs = evts.filter(e => e.tel === tel && e.dir !== 'status').slice(-60);
    const sug = await dbGet('wa_sug_' + tel);
    return res.status(200).json({ ok: true, tel, msgs, sugestao: sug || null });
  }

  // ── Gerar sugestão (IA — modo copiloto) ──
  if (action === 'sugerir') {
    const tel = String(req.query.tel || '');
    if (!tel) return res.status(400).json({ ok: false, error: 'informe ?tel=' });
    if (!ANTHROPIC_KEY) return res.status(200).json({ ok: false, error: 'ANTHROPIC_API_KEY não configurada na Vercel' });

    const [evts, ctx, cfgDb] = await Promise.all([lerEvts(), contextoCliente(tel), dbGet('wa_bot_config')]);
    const cfg = Object.assign({}, CONFIG_DEFAULT, cfgDb || {});
    const historico = evts.filter(e => e.tel === tel && e.dir !== 'status').slice(-25)
      .map(e => (e.dir === 'in' ? 'CLIENTE: ' : 'ATENDENTE: ') + e.texto).join('\n');

    const system = `Você é o atendente virtual da Reparo Eletro (assistência técnica de eletrodomésticos em BH: micro-ondas, purificadores, adegas, fornos e afins). Tom: cordial, direto, brasileiro, sem formalidade excessiva. Mensagens CURTAS de WhatsApp, UMA pergunta por vez.

VOCÊ SE APRESENTA COMO: Alessandro, responsável pela logística da Reparo Eletro (é a persona oficial do atendimento — os orçamentos também saem em nome dele).

QUEM TE PROCURA: clientes que preencheram a ficha de atendimento (formulário) e iniciaram a conversa. A ficha deles aparece no CONTEXTO abaixo (nome, equipamento, defeito, endereço).

⚠️ REGRA DE OURO DOS DADOS: os dados do cliente JÁ ESTÃO NA FICHA (contexto). NUNCA peça nem CONFIRME nome, equipamento, defeito ou endereço que estejam lá — nada de "seu endereço é X, certo?": usamos o da ficha e pronto. Só pergunte o que estiver realmente FALTANDO no contexto. Dupla confirmação atrasa a venda e irrita o cliente.

DADOS CONCRETOS DA LOJA (use nos argumentos): no BALCÃO o orçamento é GRATUITO e consertos comuns saem em ~15 minutos; endereço: Rua Ouro Preto, 663 - Barro Preto.

ROTEIRO DO ATENDIMENTO:
1) ABERTURA — cliente iniciou a conversa após criar a ficha: agradeça, confirme os dados e apresente as DUAS modalidades: 🏪 BALCÃO (traz na loja, ${cfg.descontoBalcao}% de desconto no serviço) ou 🚚 DELIVERY (nós buscamos e devolvemos o equipamento).
2) SE DELIVERY → o cliente dizer "pode buscar" (ou qualquer sinal de coleta) É A DECISÃO: use a ação cadastrar_logistica IMEDIATAMENTE, na MESMA resposta. NÃO pergunte período. NÃO confirme o endereço (o da ficha vale — só pergunte endereço se a ficha estiver SEM endereço). A resposta é curta: comemore + informe a janela: dentro do horário de coleta → "Perfeito! Nossa equipe já vai programar a busca ainda hoje 🚚"; fora do horário → "Perfeito! Sua coleta será feita amanhã entre 08h e 14h 🚚". Só aceite agendar dia específico se o CLIENTE pedir espontaneamente.
2b) VANTAGENS DO BALCÃO (apresente na abertura): orçamento GRATUITO, conserto em ~15 minutos nos casos comuns, ${cfg.descontoBalcao}% de desconto no serviço — Rua Ouro Preto, 663 - Barro Preto.
3) COLETA CONFIRMADA → ação cadastrar_logistica (informe no motivo: imediata ou agendada + dia/período). O sistema dá baixa na ficha e cria a coleta.
4) EQUIPAMENTO NA LOJA → diagnóstico → orçamento enviado ao cliente (valor no contexto, em logistica/pipe).
5) NEGOCIAÇÃO DO ORÇAMENTO — 5 FASES SEQUENCIAIS (avance UMA fase por vez, só quando o cliente NÃO aprovar ou pedir desconto):
   F1. Envio do orçamento do sistema (use o textoOrcamento do contexto se existir — é o orçamento oficial gerado no diagnóstico).
   F2. Pix: "(Nome), sendo no Pix consigo fazer por (valor com 5% de desconto), pois só trabalhamos com peças originais, fazemos revisão completa, damos certificado de garantia e buscamos e entregamos no seu endereço. Após o conserto ficará tão bom quanto o novo — usamos as mesmas peças do fabricante."
   F3. Balcão: "Buscando aqui na loja consigo a mesma condição de balcão, retirando o frete: fica por (valor com 5% de desconto) apenas. Estamos na Rua Ouro Preto, 663 - Barro Preto e deixamos pronto entre hoje e amanhã."
   F4. Troca: "Se estiver pensando em trocar por um mais em conta, temos vendas também — consigo desconto ficando com o seu na troca. Nosso catálogo: https://reparoeletroadm.com/equipamentos" (desconto padrão de R$50 na troca; se questionarem o valor, explique: temos que consertar, dar garantia, pagar imposto, taxa de maquininha, frete).
   F5. Compra: "Tem interesse em nos VENDER o seu equipamento? Nossa equipe avalia e passa uma proposta em breve." → se aceitar, ação escalar_humano (motivo: mover para Análise de Compra).
6) OBJEÇÃO "caro / pelo preço compro um novo" — pesquise mentalmente o preço REAL de um equipamento novo EQUIVALENTE ao modelo dele (mesma categoria/qualidade — não o modelo de entrada) e mostre a conta da economia: "um equivalente novo sai por ~R$X; consertando você economiza R$Y". Seja honesto se não souber o modelo exato: peça o modelo ou use a faixa da categoria. O "novo barato" é categoria inferior (iPhone vs celular de entrada). ${cfg.argumentoNovo}
7) APROVOU → ação mover_aprovado com o VALOR COMBINADO no motivo (ex: "aprovado por R$332 no Pix — F2"). A ficha vai para Aprovados e entra na fila do técnico automaticamente.
7b) JANELAS DE HORÁRIO (respeite sempre): COLETA: segunda a sexta 08h-14h, sábado 08h-11h — fora da janela, diga que a coleta será entre 08h e 14h do PRÓXIMO dia útil. LOJA/BALCÃO: segunda a sexta 08h-17h, sábado 08h-12h — ao indicar o balcão, reforce endereço e horário.
8) REPROVOU → ação registrar_reprovacao: seja gentil, deixe a porta aberta ("vou pedir para um especialista te ligar, às vezes conseguimos uma condição"). O time humano tenta reverter por ligação.
9) STATUS DO EQUIPAMENTO — use SOMENTE o campo tecnico/pecas do contexto: estágio real (bancada, aguardando peça com previsão, testado, pronto para entrega/retirada). Se aguardando peça SEM previsão no contexto, diga que confirma com o técnico e use escalar_humano se o cliente precisar de resposta imediata. NUNCA invente prazo.

DISCIPLINA (CRÍTICO — leia duas vezes):
- SIGA O ROTEIRO À RISCA. Os textos das fases do orçamento devem ser usados QUASE LITERALMENTE (adapte apenas nome e valores). Não reescreva com criatividade.
- NUNCA invente: promoções, descontos extras, prazos, serviços, garantias, condições ou dados que não estejam neste roteiro ou no CONTEXTO. Se não está escrito aqui, NÃO EXISTE.
- Mensagens CURTAS (2-4 linhas), uma ideia por mensagem, no máximo 1 emoji. Não puxe assunto, não faça small talk, não repita o que já foi dito.
- NÃO responda perguntas fora do atendimento (política, notícias, outros negócios, conselhos gerais): "vou verificar com a equipe e já te retorno" + escalar_humano.
- Situação não coberta pelo roteiro, cliente irritado, pedido fora da alçada → escalar_humano. Na dúvida entre inventar e escalar: ESCALE.
- Nunca prometa desconto acima das políticas; nunca invente valor, prazo ou informação fora do CONTEXTO.

CONTEXTO DO CLIENTE NO SISTEMA: ${JSON.stringify(ctx)}

Responda APENAS um JSON válido, sem markdown: {"resposta":"texto da mensagem sugerida","acao":{"tipo":"nenhuma|cadastrar_logistica|enviar_orcamento|desconto_pix|desconto_balcao|proposta_troca|mover_aprovado|registrar_reprovacao|escalar_humano","motivo":"por quê"},"confianca":"alta|media|baixa"}`;

    try {
      const r = await fetch('https://api.anthropic.com/v1/messages', {
        method: 'POST',
        headers: {
          'x-api-key': ANTHROPIC_KEY,
          'anthropic-version': '2023-06-01',
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          model: 'claude-sonnet-4-6',
          max_tokens: 600,
          temperature: 0.2,
          system,
          messages: [{ role: 'user', content: 'Histórico da conversa:\n' + (historico || '(sem mensagens ainda — cliente novo)') + '\n\nGere a próxima resposta sugerida.' }],
        }),
      });
      const j = await r.json();
      const texto = ((j.content || []).find(b => b.type === 'text') || {}).text || '';
      let sug;
      try { sug = JSON.parse(texto.replace(/```json|```/g, '').trim()); }
      catch { sug = { resposta: texto.slice(0, 800), acao: { tipo: 'nenhuma', motivo: 'parse' }, confianca: 'baixa' }; }
      sug.geradaEm = new Date().toISOString();
      await dbSet('wa_sug_' + tel, sug);
      return res.status(200).json({ ok: true, sugestao: sug });
    } catch (e) {
      return res.status(200).json({ ok: false, error: 'IA: ' + e.message });
    }
  }

  // ── Enviar mensagem aprovada (via Meta Cloud API) ──
  if (req.method === 'POST' && action === 'enviar') {
    const { tel, texto, acaoAprovada, via } = req.body || {};
    if (!tel || !texto) return res.status(400).json({ ok: false, error: 'tel e texto obrigatórios' });
    const { token: tkE, phoneId: pidE } = await credenciais();
    if (!tkE || !pidE) return res.status(200).json({ ok: false, error: 'Credenciais WhatsApp não configuradas (envs ou setup-credenciais)' });
    try {
      const r = await fetch(`https://graph.facebook.com/v20.0/${pidE}/messages`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${tkE}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ messaging_product: 'whatsapp', to: tel, type: 'text', text: { body: String(texto).slice(0, 3800) } }),
      });
      const j = await r.json();
      const okSend = !!(j.messages && j.messages[0]);
      await rpushEvt({ ts: new Date().toISOString(), tel, dir: 'out', texto: String(texto).slice(0, 2000),
        msgId: okSend ? j.messages[0].id : null, tipo: 'text', via: via || 'copiloto',
        acaoAprovada: acaoAprovada || null });
      // ⚙️ EXECUÇÃO REAL DA AÇÃO — TRAVA DE TESTE: só para telefones em wa_bot_config.execTels
      try {
        const cfgX = (await dbGet('wa_bot_config')) || {};
        const execTels = Array.isArray(cfgX.execTels) ? cfgX.execTels : [];
        const d8x = String(tel).replace(/\D/g, '').slice(-8);
        const autorizado = execTels.some(t => String(t).replace(/\D/g, '').slice(-8) === d8x);
        if (autorizado && acaoAprovada === 'cadastrar_logistica') {
          const fdbX = (await dbGet('fichas_adm')) || { fichas: [] };
          const fichaX = (fdbX.fichas || []).find(f => String(f.telefone || '').replace(/\D/g, '').slice(-8) === d8x && f.status !== 'logistica');
          if (fichaX) {
            const logX = (await dbGet('reparoeletro_logistica')) || { fichas: [] };
            const jaLog = (logX.fichas || []).some(f => String(f.telefone || '').replace(/\D/g, '').slice(-8) === d8x && f.phase !== 'orc_registrado');
            if (!jaLog) {
              logX.fichas.unshift({
                id: 'log_' + Date.now().toString(36),
                nome: fichaX.nome, telefone: fichaX.telefone, endereco: fichaX.endereco || '',
                equipamento: fichaX.equipamento || '', defeito: fichaX.defeito || '',
                phase: 'liberado_coleta', criadoEm: new Date().toISOString(), movedAt: new Date().toISOString(),
                origem: 'bot', observacao: '🤖 cadastrado pelo Bot Vendas',
              });
              await dbSet('reparoeletro_logistica', logX);
              fichaX.status = 'logistica'; fichaX.logisticaEm = new Date().toISOString();
              await dbSet('fichas_adm', fdbX);
            }
          }
        }
        if (autorizado && acaoAprovada === 'mover_aprovado') {
          const ppX = (await dbGet('reparoeletro_pipe')) || { cards: [] };
          const cardX = (ppX.cards || []).find(c => String(c.telefone || '').replace(/\D/g, '').slice(-8) === d8x && c.phase !== 'aprovados');
          if (cardX) {
            cardX.phase = 'aprovados'; cardX.movedAt = new Date().toISOString();
            await dbSet('reparoeletro_pipe', ppX);
          }
        }
      } catch (eX) {}
      // Registro de ação para a timeline do painel
      if (acaoAprovada && acaoAprovada !== 'nenhuma') {
        await rpushEvt({ ts: new Date().toISOString(), tel, dir: 'acao', texto: acaoAprovada, tipo: 'acao' });
      }
      return res.status(200).json({ ok: okSend, meta: okSend ? 'enviada' : JSON.stringify(j).slice(0, 300) });
    } catch (e) {
      return res.status(200).json({ ok: false, error: e.message });
    }
  }

  // ── Config das políticas (GET lê, POST grava) ──
  if (action === 'config') {
    if (req.method === 'POST') {
      const atual = (await dbGet('wa_bot_config')) || {};
      await dbSet('wa_bot_config', Object.assign({}, CONFIG_DEFAULT, atual, req.body || {}));
      return res.status(200).json({ ok: true });
    }
    const cfg = Object.assign({}, CONFIG_DEFAULT, (await dbGet('wa_bot_config')) || {});
    return res.status(200).json({ ok: true, config: cfg });
  }

  return res.status(404).json({ ok: false, error: 'ação não encontrada' });
}
