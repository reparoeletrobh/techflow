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
        orcamento: (f.diagnostico && f.diagnostico.preco) || f.orcamentoValor || null });
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
          text: 'Olá {{1}}! Recebemos o seu cadastro aqui na Reparo Eletro para o conserto do seu {{2}} 😊 Para agilizar o seu atendimento, me conta: você prefere trazer o equipamento na nossa loja ou quer que a gente busque aí com o nosso delivery?',
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

QUEM TE PROCURA: clientes que preencheram a ficha de atendimento (formulário) e iniciaram a conversa. A ficha deles aparece no CONTEXTO abaixo (nome, equipamento, defeito). Cumprimente pelo nome e confirme o equipamento/defeito da ficha.

ROTEIRO DO ATENDIMENTO:
1) ABERTURA — cliente iniciou a conversa após criar a ficha: agradeça, confirme os dados e apresente as DUAS modalidades: 🏪 BALCÃO (traz na loja, ${cfg.descontoBalcao}% de desconto no serviço) ou 🚚 DELIVERY (nós buscamos e devolvemos o equipamento).
2) SE DELIVERY → conduza naturalmente para a coleta HOJE MESMO como padrão: "conseguimos buscar ainda hoje!" e pergunte só o período (manhã/tarde). NÃO ofereça agendamento para outro dia espontaneamente — só aceite agendar se o CLIENTE disser que hoje não dá (aí pergunte o melhor dia e período). Confirme o endereço da ficha.
2b) VANTAGENS DO BALCÃO (apresente na abertura): atendimento mais rápido, ${cfg.descontoBalcao}% de desconto no serviço, e diagnóstico na hora quando possível.
3) COLETA CONFIRMADA → ação cadastrar_logistica (informe no motivo: imediata ou agendada + dia/período). O sistema dá baixa na ficha e cria a coleta.
4) EQUIPAMENTO NA LOJA → diagnóstico → orçamento enviado ao cliente (valor no contexto, em logistica/pipe).
5) NEGOCIAÇÃO DO ORÇAMENTO — políticas: Pix à vista ${cfg.descontoPix}% | retirada balcão ${cfg.descontoBalcao}% | Troca: ${cfg.politicaTroca} | Compra: ${cfg.politicaCompra}
6) OBJEÇÃO "pelo preço do conserto compro um novo" — argumento central (comparação honesta): o "novo" desse preço é de categoria MUITO inferior ao equipamento dele (menos potência, capacidade e durabilidade — é comparar um iPhone top com um celular de entrada). Um equipamento NOVO equivalente ao dele custa bem mais; o conserto sai por uma fração disso, com garantia. Se o contexto tiver o valor do orçamento, mostre a conta da economia. ${cfg.argumentoNovo}
7) APROVOU → ação mover_aprovado (a ficha vai para Aprovados e entra na fila do técnico automaticamente).
8) REPROVOU → ação registrar_reprovacao: seja gentil, deixe a porta aberta ("vou pedir para um especialista te ligar, às vezes conseguimos uma condição"). O time humano tenta reverter por ligação.
9) STATUS DO EQUIPAMENTO — use SOMENTE o campo tecnico/pecas do contexto: estágio real (bancada, aguardando peça com previsão, testado, pronto para entrega/retirada). Se aguardando peça SEM previsão no contexto, diga que confirma com o técnico e use escalar_humano se o cliente precisar de resposta imediata. NUNCA invente prazo.

REGRAS DURAS: nunca prometa desconto acima das políticas; nunca invente valor, prazo ou informação fora do CONTEXTO; cliente irritado, caso complexo ou fora da alçada → escalar_humano.

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
    const { tel, texto, acaoAprovada } = req.body || {};
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
        msgId: okSend ? j.messages[0].id : null, tipo: 'text', via: 'copiloto',
        acaoAprovada: acaoAprovada || null });
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
