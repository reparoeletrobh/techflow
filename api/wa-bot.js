// api/wa-bot.js — Cérebro do bot (FASE 1: COPILOTO — sugere, humano aprova)
// actions: conversas | historico&tel= | sugerir&tel= | enviar (POST) | config
const U = (process.env.UPSTASH_URL || '').replace(/['"]/g, '').trim();
const T = (process.env.UPSTASH_TOKEN || '').replace(/[\n\r'"]/g, '').trim();
const WA_TOKEN = (process.env.WA_TOKEN || '').trim();
const WA_PHONE_ID = (process.env.WA_PHONE_ID || '').trim();
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
async function contextoCliente(tel) {
  const d8 = String(tel).replace(/\D/g, '').slice(-8);
  const ctx = { fichas: [], logistica: [], pipe: [] };
  try {
    const [fa, lg, pp] = await Promise.all([
      dbGet('fichas_adm'), dbGet('reparoeletro_logistica'), dbGet('reparoeletro_pipe'),
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

    const system = `Você é o atendente virtual da Reparo Eletro (assistência técnica de eletrodomésticos em BH: micro-ondas, purificadores, adegas, fornos). Tom: cordial, direto, brasileiro, sem formalidade excessiva. Mensagens CURTAS (WhatsApp).

FLUXO DO ATENDIMENTO: 1) Cliente fez atendimento e tem ficha aberta; oferecemos coleta DELIVERY (buscamos e devolvemos) ou atendimento BALCÃO (traz na loja). 2) Se autorizar a busca → ação cadastrar_logistica. 3) Após diagnóstico enviamos o orçamento. 4) Se aprovar → ação mover_aprovado. 

POLÍTICAS DE NEGOCIAÇÃO (use quando o cliente achar caro):
- Desconto Pix à vista: ${cfg.descontoPix}%
- Desconto retirando/trazendo na loja (balcão): ${cfg.descontoBalcao}%
- Troca: ${cfg.politicaTroca}
- Compra: ${cfg.politicaCompra}
- Comparação com novo: ${cfg.argumentoNovo}

REGRAS: nunca prometa desconto acima das políticas; nunca invente prazo ou valor de orçamento que não esteja no contexto; se o cliente pedir algo fora da alçada ou demonstrar irritação, use a ação escalar_humano.

CONTEXTO DO CLIENTE NO SISTEMA: ${JSON.stringify(ctx)}

Responda APENAS um JSON válido, sem markdown: {"resposta":"texto da mensagem sugerida","acao":{"tipo":"nenhuma|cadastrar_logistica|enviar_orcamento|desconto_pix|desconto_balcao|proposta_troca|mover_aprovado|escalar_humano","motivo":"por quê"},"confianca":"alta|media|baixa"}`;

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
    if (!WA_TOKEN || !WA_PHONE_ID) return res.status(200).json({ ok: false, error: 'WA_TOKEN/WA_PHONE_ID não configurados na Vercel' });
    try {
      const r = await fetch(`https://graph.facebook.com/v20.0/${WA_PHONE_ID}/messages`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${WA_TOKEN}`, 'Content-Type': 'application/json' },
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
