// ═══════════════════════════════════════════════════════════════════
// PESQUISA DE SATISFAÇÃO
//
// Pergunta, no dia seguinte à entrega, se está tudo certo. Quem elogia sem
// nenhuma ressalva recebe o pedido de avaliação no Google; quem relata
// problema vira conflito; quem faz observação sai da fila para a equipe ver.
//
// Vive em arquivo próprio: o módulo do bot passou de 470 mil caracteres e
// deixou de ser publicado pela plataforma.
// ═══════════════════════════════════════════════════════════════════

const U = (process.env.UPSTASH_URL || process.env.KV_REST_API_URL || '')
  .replace(/['"]/g, '').trim();
const T = (process.env.UPSTASH_TOKEN || process.env.KV_REST_API_TOKEN || '')
  .replace(/[\n\r'"]/g, '').trim();
const EVT_LIST = 'wa_evt_list';

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
      method: 'POST', headers: { Authorization: `Bearer ${T}`, 'Content-Type': 'application/json' },
      body: JSON.stringify(valor),
    });
    return true;
  } catch (e) { return false; }
}
/** Últimos eventos de conversa, para saber quem respondeu e quando. */
async function lerEvts() {
  try {
    const r = await fetch(`${U}/lrange/${EVT_LIST}/-6000/-1`,
      { headers: { Authorization: `Bearer ${T}` } }).then(x => x.json());
    const out = [];
    for (const s of (r.result || [])) { try { out.push(JSON.parse(s)); } catch (e) {} }
    return out;
  } catch (e) { return []; }
}


/**
 * Anota um acontecimento da pesquisa no diário.
 * Sem isso só se enxerga o estado atual de cada cliente — quem já foi avaliado,
 * quem não — e se perde a leitura do conjunto: quantos elogiaram, quantos
 * reclamaram, se a proporção está mudando de uma semana para outra.
 */
async function anotar(evento) {
  try {
    const k = 'satisfacao_diario';
    const d = (await dbGet(k)) || { eventos: [] };
    d.eventos = (d.eventos || []).concat([{ em: new Date().toISOString(), ...evento }]);
    // guarda 120 dias: o suficiente para comparar meses
    const corte = Date.now() - 120 * 86400000;
    d.eventos = d.eventos.filter(e => new Date(e.em || 0).getTime() >= corte).slice(-4000);
    await dbSet(k, d);
  } catch (e) {}
}

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const chave = (req.query && req.query.k) || req.headers['x-tf-key'] || '';
  if (chave !== ((process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim())) {
    return res.status(401).json({ ok: false, error: 'não autorizado' });
  }
  const action = String((req.query || {}).action || '').trim();

  // ── 🧹 baixar-perguntados: tira do painel quem já recebeu a pesquisa ──
  // A baixa acontece no momento do envio, mas quem foi perguntado antes disso
  // existir — ou cuja baixa falhou — continuou listado, e a equipe acabaria
  // abordando de novo quem o sistema já abordou.
  if (action === 'baixar-perguntados') {
    const aplicar = String(req.query.aplicar || '') === '1';
    const d8b = t => String(t || '').replace(/\D/g, '').slice(-8);
    const [pend, ctrl] = await Promise.all([
      dbGet('gmb_pendentes'), dbGet('wa_pesquisa_satisfacao'),
    ]);
    const clientes = ((ctrl || {}).clientes) || {};
    const alvos = [];
    for (const f of (((pend || {}).fichas) || [])) {
      const tel = d8b(f.tel || f.telefone);
      const c = clientes[tel];
      if (!c) continue;
      alvos.push({ id: f.id, nome: f.nome || f.nomeContato || '?', tel,
        telCheio: f.tel || f.telefone,
        perguntadoEm: c.em,
        situacao: c.avaliacaoPedida ? 'elogiou e recebeu o link'
          : c.reclamou ? 'reclamou — virou conflito'
          : c.teveRessalva ? 'fez ressalva'
          : 'aguardando resposta' });
    }
    if (!aplicar) {
      return res.status(200).json({ ok: true, modo: 'prévia',
        vaoSairDoPainel: alvos.length,
        L: alvos.map(a => String(a.nome).slice(0, 22).padEnd(22) + ' ' + a.tel.slice(-4) +
          ' | perguntado ' + String(a.perguntadoEm).slice(5, 16).replace('T', ' ') +
          ' | ' + a.situacao),
        dica: 'para aplicar: &aplicar=1' });
    }
    const feitos = [], erros = [];
    for (const a of alvos) {
      try {
        const r = await fetch('https://reparoeletroadm.com/api/orcamento' +
          '?action=gmb-marcar-enviado&k=' +
          ((process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim()), {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ id: a.id, nome: a.nome, tel: a.telCheio,
            desc: 'pesquisa de satisfação já enviada' }),
        }).then(x => x.json());
        if (r && r.ok) {
          if (clientes[a.tel]) clientes[a.tel].gmbBaixado = true;
          feitos.push(a.nome + ' ' + a.tel.slice(-4) +
            (r.removidosDoPainel ? ' (removeu ' + r.removidosDoPainel + ')' : ''));
        } else erros.push(a.nome + ': recusado');
      } catch (e) { erros.push(a.nome + ': ' + e.message); }
    }
    if (feitos.length) await dbSet('wa_pesquisa_satisfacao', ctrl);
    return res.status(200).json({ ok: erros.length === 0,
      baixados: feitos.length, L: feitos, erros });
  }

  // ── 🔀 conferir-gmb: por que alguém do GMB não recebeu a pesquisa ──
  // As duas listas nascem de lugares diferentes: o painel do Google lê apenas
  // o quadro da linha branca, enquanto a pesquisa lê o livro de entradas, que
  // registra as três origens. Comparar as duas mostra quem ficou de fora e por quê.
  if (action === 'conferir-gmb') {
    const d8g = t => String(t || '').replace(/\D/g, '').slice(-8);
    const hh = x => x ? new Date(new Date(x).getTime() - 3 * 3600000)
      .toISOString().slice(5, 16).replace('T', ' ') : '—';
    const [pend, env, livro, ctrl] = await Promise.all([
      dbGet('gmb_pendentes'), dbGet('gmb_enviados'),
      dbGet('erp_entradas'), dbGet('wa_pesquisa_satisfacao'),
    ]);
    const fichasPend = ((pend || {}).fichas) || [];
    const idsPend = new Set(((pend || {}).ids || []).map(String));
    const enviados = new Set((((env || {}).fichas) || []).map(f => d8g(f.tel || f.telefone)));
    const clientes = ((ctrl || {}).clientes) || {};
    const registros = Object.values(((livro || {}).registros) || {});
    const noLivro = {};
    for (const r of registros) { const d = d8g(r.telefone); if (d) noLivro[d] = r; }

    const semPesquisa = [], comPesquisa = [], semTelefone = [];
    for (const f of fichasPend) {
      const d = d8g(f.tel || f.telefone);
      const nome = String(f.nome || f.nomeContato || '?').slice(0, 22);
      if (!d || d.length < 8) { semTelefone.push(nome + ' — sem telefone utilizável'); continue; }
      const c = clientes[d];
      if (c) {
        comPesquisa.push(nome + ' ' + d.slice(-4) + ' | perguntado ' + hh(c.em) +
          (c.avaliacaoPedida ? ' | ✅ elogiou e recebeu o link'
            : c.reclamou ? ' | 🚨 reclamou — virou conflito'
            : c.teveRessalva ? ' | ⚠️ fez ressalva'
            : ' | aguardando resposta'));
      } else {
        const reg = noLivro[d];
        semPesquisa.push(nome + ' ' + d.slice(-4) +
          (reg ? ' | entrou no ERP em ' + reg.dia + ' — pesquisa ainda não alcançou'
               : ' | 🚨 NÃO está no livro de entradas: o ERP dele não foi registrado') +
          (enviados.has(d) ? ' | já foi contatado à mão' : ''));
      }
    }
    // e o contrário: quem a pesquisa alcançou mas o painel não lista
    const foraDoPainel = [];
    for (const [d, c] of Object.entries(clientes)) {
      const estaNoPainel = fichasPend.some(f => d8g(f.tel || f.telefone) === d);
      if (!estaNoPainel && !enviados.has(d)) {
        foraDoPainel.push(String(c.nome || '?').slice(0, 22) + ' ' + d.slice(-4) +
          ' | ' + (c.sis || '?') + ' | perguntado ' + hh(c.em));
      }
    }
    return res.status(200).json({ ok: semPesquisa.length === 0,
      noPainelGmb: fichasPend.length,
      RESUMO: {
        comPesquisaEnviada: comPesquisa.length,
        semPesquisa: semPesquisa.length,
        semTelefone: semTelefone.length,
        pesquisadosForaDoPainel: foraDoPainel.length,
      },
      VEREDITO: semPesquisa.length
        ? '🚨 ' + semPesquisa.length + ' no painel do Google sem pesquisa enviada'
        : '✅ todos os do painel já foram pesquisados',
      SEM_PESQUISA: semPesquisa,
      SEM_TELEFONE: semTelefone,
      COM_PESQUISA: comPesquisa,
      PESQUISADOS_FORA_DO_PAINEL: foraDoPainel,
      observacao: 'o painel do Google lê apenas o quadro da linha branca; a pesquisa ' +
        'lê o livro de entradas, que inclui televisão e balcão — por isso as listas diferem' });
  }

  // ── ⚡ tratar-resposta: classifica UM cliente na hora em que ele responde ──
  // A leitura em lote roda de hora em hora, mas o cérebro do bot responde em
  // segundos: quem elogiava recebia uma oferta de orçamento no lugar do pedido
  // de avaliação. Esta ação é chamada pelo webhook antes do cérebro e devolve
  // se a pesquisa assumiu a conversa.
  if (action === 'tratar-resposta') {
    const d8t = t => String(t || '').replace(/\D/g, '').slice(-8);
    const alvo = String(req.query.tel || '');
    const d = d8t(alvo);
    if (d.length < 8) return res.status(200).json({ ok: true, assumiu: false });
    const ctrl = (await dbGet('wa_pesquisa_satisfacao')) || { clientes: {} };
    const clientes = ctrl.clientes || {};
    const c = clientes[d];
    // não é uma resposta de pesquisa: o cérebro segue normalmente
    if (!c || !c.aguardandoResposta || c.avaliacaoPedida) {
      return res.status(200).json({ ok: true, assumiu: false });
    }
    // a última mensagem do cliente, posterior à pergunta
    let texto = null, quando = 0;
    try {
      for (const e of (await lerEvts())) {
        if (e.dir !== 'in') continue;
        if (d8t(e.tel) !== d) continue;
        const q = new Date(e.ts || 0).getTime();
        if (q <= new Date(c.em || 0).getTime()) continue;
        const t = String(e.texto || '').trim();
        if (!t || t.startsWith('🎤 [')) continue;
        if (q > quando) { quando = q; texto = t; }
      }
    } catch (e) {}
    if (!texto) return res.status(200).json({ ok: true, assumiu: false,
      motivo: 'ainda não há resposta registrada' });

    // 🧠 classifica: mesmo critério da leitura em lote
    const CHAVE = (process.env.ANTHROPIC_API_KEY || '').trim();
    let veredito = 'indefinido';
    if (CHAVE) {
      try {
        const rr = await fetch('https://api.anthropic.com/v1/messages', {
          method: 'POST',
          headers: { 'x-api-key': CHAVE, 'anthropic-version': '2023-06-01',
            'content-type': 'application/json' },
          body: JSON.stringify({ model: 'claude-sonnet-4-6', max_tokens: 12, temperature: 0,
            system: 'Você classifica a resposta de um cliente a uma pesquisa de satisfação ' +
              'de assistência técnica.\n\nResponda APENAS uma palavra:\n' +
              'RECLAMACAO — se relata que algo NÃO está funcionando, voltou a apresentar ' +
              'defeito, está insatisfeito ou cobra solução.\n' +
              'ELOGIO — SOMENTE se a mensagem inteira é satisfação ou agradecimento, ' +
              'sem NENHUM acréscimo. Exemplos que SÃO elogio: "Obrigado", "Tudo certo, ' +
              'muito obrigado", "Ficou ótimo".\n' +
              'RESSALVA — se há qualquer coisa além do agradecimento: observação, ' +
              'sugestão, dúvida, pedido, condição, promessa de responder depois, ou se ' +
              'a pessoa ainda não usou nem verificou o equipamento. Exemplos que NÃO são ' +
              'elogio: "Liguei ontem, ainda não olhei, mando informações depois", ' +
              '"Obrigado, mas tenho uma dúvida", "Está bom, só que...", ' +
              '"Ainda não testei, obrigada". O agradecimento junto de qualquer outra ' +
              'coisa é RESSALVA, não elogio.\n' +
              'INDEFINIDO — se é ambígua ou não responde à pergunta.\n\n' +
              'Na dúvida entre ELOGIO e RESSALVA, responda RESSALVA.',
            messages: [{ role: 'user', content: texto.slice(0, 600) }] }),
        }).then(x => x.json());
        const t = ((rr.content || []).filter(b => b.type === 'text')
          .map(b => b.text).join('') || '').trim().toUpperCase();
        if (t.includes('RECLAMACAO') || t.includes('RECLAMAÇÃO')) veredito = 'reclamacao';
        else if (t.includes('ELOGIO')) veredito = 'elogio';
        else if (t.includes('RESSALVA')) veredito = 'ressalva';
      } catch (e) {}
    }
    // 🔒 sem classificação, o cérebro assume: é melhor uma resposta boa de
    // negociação que o silêncio de uma pesquisa que não soube o que fazer
    if (veredito === 'indefinido') {
      return res.status(200).json({ ok: true, assumiu: false, veredito });
    }

    const cfg = (await dbGet('wa_credenciais')) || {};
    const pid = cfg.phoneId || process.env.WA_PHONE_ID;
    const tk = cfg.token || process.env.WA_TOKEN;
    const tel = String(c.telefone || '').replace(/\D/g, '') || d;

    if (veredito === 'elogio') {
      const LINK = 'https://g.page/r/CUDbfbB2xOBHEBM/review';
      const msg = 'Maravilha! A qualquer momento que precisar de algo pode me chamar aqui ' +
        'que estaremos prontos pra te atender.\n\n' +
        'Quero apenas te fazer um último pedido, que me ajuda demais a continuar fazendo ' +
        'um bom trabalho. Sua avaliação no nosso Google é muito importante pro nosso ' +
        'crescimento. Se possível nos avalie por favor:\n\n' + LINK;
      try {
        const r = await fetch('https://graph.facebook.com/v20.0/' + pid + '/messages', {
          method: 'POST',
          headers: { Authorization: 'Bearer ' + tk, 'Content-Type': 'application/json' },
          body: JSON.stringify({ messaging_product: 'whatsapp', to: tel,
            type: 'text', text: { body: msg, preview_url: true } }),
        }).then(x => x.json());
        if (!(r && r.messages && r.messages[0])) {
          return res.status(200).json({ ok: false, assumiu: false,
            erro: (r && r.error && r.error.message) || 'falha no envio' });
        }
      } catch (e) {
        return res.status(200).json({ ok: false, assumiu: false, erro: e.message });
      }
      c.avaliacaoPedida = true;
      c.avaliacaoPedidaEm = new Date().toISOString();
      c.aguardandoResposta = false;
      c.respostaCliente = texto.slice(0, 300);
      await dbSet('wa_pesquisa_satisfacao', ctrl);
      await anotar({ tipo: 'elogio', tel: d, nome: c.nome, sis: c.sis,
        equipamento: c.equipamento, resposta: texto.slice(0, 200),
        avaliacaoPedida: true, viaImediata: true });
      return res.status(200).json({ ok: true, assumiu: true, veredito: 'elogio' });
    }

    // reclamação e ressalva: a pesquisa NÃO responde, mas registra e libera o
    // cérebro para conduzir — quem apontou um problema precisa de conversa real
    c.aguardandoResposta = false;
    c.respostaCliente = texto.slice(0, 300);
    if (veredito === 'reclamacao') c.reclamou = true; else c.teveRessalva = true;
    await dbSet('wa_pesquisa_satisfacao', ctrl);
    await anotar({ tipo: veredito, tel: d, nome: c.nome, sis: c.sis,
      equipamento: c.equipamento, resposta: texto.slice(0, 300), viaImediata: true });
    return res.status(200).json({ ok: true, assumiu: false, veredito,
      observacao: 'registrado; o cérebro conduz a conversa' });
  }

  // ── 🔎 elogios-perdidos: quem elogiou e não recebeu o link ──
  // Enquanto o cérebro respondia por cima da pesquisa, o elogio virava conversa
  // comercial e o pedido de avaliação nunca saía. Esta varredura relê as
  // respostas de quem foi perguntado e ainda não recebeu o link.
  if (action === 'elogios-perdidos') {
    const aplicar = String(req.query.aplicar || '') === '1';
    const d8x = t => String(t || '').replace(/\D/g, '').slice(-8);
    const hh = x => x ? new Date(new Date(x).getTime() - 3 * 3600000)
      .toISOString().slice(5, 16).replace('T', ' ') : '—';
    const ctrl = (await dbGet('wa_pesquisa_satisfacao')) || { clientes: {} };
    const clientes = ctrl.clientes || {};

    // a resposta de cada cliente que foi perguntado e não recebeu o link
    const respostas = {};
    try {
      for (const e of (await lerEvts())) {
        if (e.dir !== 'in') continue;
        const d = d8x(e.tel); if (!d) continue;
        const c = clientes[d];
        if (!c || c.avaliacaoPedida) continue;
        const q = new Date(e.ts || 0).getTime();
        if (q <= new Date(c.em || 0).getTime()) continue;
        const txt = String(e.texto || '').trim();
        if (!txt || txt.startsWith('🎤 [')) continue;
        if (!respostas[d] || q > respostas[d].ts) respostas[d] = { ts: q, texto: txt, c };
      }
    } catch (e) {}

    const CHAVE = (process.env.ANTHROPIC_API_KEY || '').trim();
    const elogios = [], outros = [], reclamacoes = [];
    for (const [d, r] of Object.entries(respostas)) {
      let veredito = 'indefinido';
      if (CHAVE) {
        try {
          const rr = await fetch('https://api.anthropic.com/v1/messages', {
            method: 'POST',
            headers: { 'x-api-key': CHAVE, 'anthropic-version': '2023-06-01',
              'content-type': 'application/json' },
            body: JSON.stringify({ model: 'claude-sonnet-4-6', max_tokens: 12, temperature: 0,
              system: 'Classifique a resposta de um cliente a uma pesquisa de satisfação. ' +
                'Responda APENAS uma palavra: ELOGIO se diz que está tudo certo, ' +
                'funcionando, satisfeito, agradece ou elogia, E NADA MAIS. RESSALVA se há ' +
                'observação, dúvida, pedido, condição, ou se ainda não usou. RECLAMACAO se ' +
                'relata defeito ou insatisfação. INDEFINIDO se ambígua. Na dúvida entre ' +
                'ELOGIO e RESSALVA, responda RESSALVA.',
              messages: [{ role: 'user', content: r.texto.slice(0, 600) }] }),
          }).then(x => x.json());
          const t = ((rr.content || []).filter(b => b.type === 'text')
            .map(b => b.text).join('') || '').trim().toUpperCase();
          if (t.includes('ELOGIO')) veredito = 'elogio';
          else if (t.includes('RECLAMACAO') || t.includes('RECLAMAÇÃO')) veredito = 'reclamacao';
          else if (t.includes('RESSALVA')) veredito = 'ressalva';
        } catch (e) {}
      }
      const linha = String(r.c.nome || '?').slice(0, 22) + ' ' + d.slice(-4) +
        ' | perguntado ' + hh(r.c.em) +
        ' | respondeu "' + r.texto.slice(0, 60).replace(/\n/g, ' ') + '"';
      if (veredito === 'elogio') elogios.push({ d, r, linha });
      else {
        outros.push(linha + ' | ' + veredito);
        // 🚨 reclamação encontrada aqui nunca virou conflito, porque o cérebro
        // respondeu por cima e a classificação não chegou a rodar
        if (veredito === 'reclamacao') reclamacoes.push({ d, r, linha });
      }
    }

    if (!aplicar) {
      return res.status(200).json({ ok: true, modo: 'prévia',
        responderamSemLink: Object.keys(respostas).length,
        elogiosQueVaoReceber: elogios.length,
        reclamacoesQueViramConflito: reclamacoes.length,
        ELOGIOS: elogios.map(x => x.linha),
        RECLAMACOES_ABRIRAO_CONFLITO: reclamacoes.map(x => x.linha),
        NAO_SAO_ELOGIO: outros,
        dica: 'para enviar: &aplicar=1' });
    }

    const cfg = (await dbGet('wa_credenciais')) || {};
    const pid = cfg.phoneId || process.env.WA_PHONE_ID;
    const tk = cfg.token || process.env.WA_TOKEN;
    const LINK = 'https://g.page/r/CUDbfbB2xOBHEBM/review';
    const msg = 'Maravilha! A qualquer momento que precisar de algo pode me chamar aqui ' +
      'que estaremos prontos pra te atender.\n\n' +
      'Quero apenas te fazer um último pedido, que me ajuda demais a continuar fazendo ' +
      'um bom trabalho. Sua avaliação no nosso Google é muito importante pro nosso ' +
      'crescimento. Se possível nos avalie por favor:\n\n' + LINK;
    const feitos = [], erros = [];
    for (const x of elogios) {
      const cli = clientes[x.d] || {};
      const tel = String(cli.telefone || '').replace(/\D/g, '') || x.d;
      try {
        const r = await fetch('https://graph.facebook.com/v20.0/' + pid + '/messages', {
          method: 'POST',
          headers: { Authorization: 'Bearer ' + tk, 'Content-Type': 'application/json' },
          body: JSON.stringify({ messaging_product: 'whatsapp', to: tel,
            type: 'text', text: { body: msg, preview_url: true } }),
        }).then(y => y.json());
        if (r && r.messages && r.messages[0]) {
          cli.avaliacaoPedida = true;
          cli.avaliacaoPedidaEm = new Date().toISOString();
          cli.aguardandoResposta = false;
          cli.respostaCliente = String(x.r.texto).slice(0, 300);
          feitos.push(x.linha);
          await anotar({ tipo: 'elogio', tel: x.d, nome: cli.nome, sis: cli.sis,
            equipamento: cli.equipamento, resposta: String(x.r.texto).slice(0, 200),
            avaliacaoPedida: true, recuperado: true });
        } else {
          erros.push(x.linha + ' — ' + ((r && r.error && r.error.message) || 'falha'));
        }
      } catch (e) { erros.push(x.linha + ' — ' + e.message); }
      await new Promise(s => setTimeout(s, 400));
    }
    // 🚨 e as reclamações viram conflito: o cliente relatou um problema e
    // ninguém foi avisado, porque a classificação não chegou a rodar
    const conflitos = [];
    for (const x of reclamacoes) {
      const cli = clientes[x.d] || {};
      if (cli.reclamou) continue;                       // já registrado antes
      try {
        const r = await fetch('https://reparoeletroadm.com/api/conflitos?action=criar&k=' +
          ((process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim()), {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            titulo: 'Reclamação na pesquisa — ' + String(cli.nome || '?').slice(0, 30),
            tipo: 'qualidade', prioridade: 'alto',
            setor: cli.sis === 'TV' ? 'TV' : 'Assistência',
            ficha: String(cli.nome || '?') + ' ' + x.d.slice(-4),
            cliente: cli.nome || '?', telefone: cli.telefone || x.d,
            equipamento: cli.equipamento || '',
            descricao: 'Respondeu à pesquisa de satisfação: "' +
              String(x.r.texto).slice(0, 400) + '"' +
              (cli.equipamento ? '\n\nEquipamento: ' + cli.equipamento : ''),
            registradoPor: 'pesquisa de satisfação',
          }),
        }).then(y => y.json());
        if (r && r.ok) {
          cli.reclamou = true;
          cli.aguardandoResposta = false;
          cli.respostaCliente = String(x.r.texto).slice(0, 300);
          conflitos.push(x.linha);
          await anotar({ tipo: 'reclamacao', tel: x.d, nome: cli.nome, sis: cli.sis,
            equipamento: cli.equipamento, resposta: String(x.r.texto).slice(0, 300),
            conflitoAberto: true, recuperado: true });
        } else erros.push(x.linha + ' — conflito recusado');
      } catch (e) { erros.push(x.linha + ' — conflito: ' + e.message); }
    }
    if (feitos.length || conflitos.length) await dbSet('wa_pesquisa_satisfacao', ctrl);
    return res.status(200).json({ ok: erros.length === 0,
      enviados: feitos.length, L: feitos,
      conflitosAbertos: conflitos.length, CONFLITOS: conflitos, erros });
  }

  // ── 📊 diario — o que a pesquisa produziu, dia a dia ──
  // O controle guarda o estado de cada cliente; este diário guarda a leitura do
  // conjunto: quantos foram perguntados, quantos elogiaram, quantos apontaram
  // algo e quantos viraram conflito — que é o que mostra se a satisfação está
  // subindo ou caindo de uma semana para outra.
  if (action === 'diario') {
    const d = (await dbGet('satisfacao_diario')) || { eventos: [] };
    const evs = d.eventos || [];
    const dias = Math.max(1, Math.min(120, parseInt(req.query.dias || '30', 10)));
    const desde = Date.now() - dias * 86400000;
    const noPeriodo = evs.filter(e => new Date(e.em || 0).getTime() >= desde);
    const hh = x => x ? new Date(new Date(x).getTime() - 3 * 3600000)
      .toISOString().slice(5, 16).replace('T', ' ') : '—';
    const diaDe = x => new Date(new Date(x).getTime() - 3 * 3600000).toISOString().slice(0, 10);

    const conta = t => noPeriodo.filter(e => e.tipo === t).length;
    const perguntas = conta('pergunta');
    const elogios = conta('elogio');
    const ressalvas = conta('ressalva');
    const reclamacoes = conta('reclamacao');
    const responderam = elogios + ressalvas + reclamacoes;

    // por dia, para ver a evolução
    const porDia = {};
    for (const e of noPeriodo) {
      const k = diaDe(e.em);
      porDia[k] = porDia[k] || { pergunta: 0, elogio: 0, ressalva: 0, reclamacao: 0 };
      if (porDia[k][e.tipo] !== undefined) porDia[k][e.tipo]++;
    }

    const lista = t => noPeriodo.filter(e => e.tipo === t)
      .sort((a, b) => String(b.em).localeCompare(String(a.em)))
      .map(e => hh(e.em) + ' | ' + String(e.nome || '?').slice(0, 22).padEnd(22) +
        ' ' + String(e.tel || '').slice(-4) +
        (e.equipamento ? ' | ' + String(e.equipamento).slice(0, 24) : '') +
        (e.resposta ? ' | "' + String(e.resposta).replace(/\n/g, ' ').slice(0, 70) + '"' : '') +
        (e.baixadoNoGmb ? ' | ✅ baixado no GMB' : '') +
        (e.conflitoAberto ? ' | 🚨 conflito aberto' : ''));

    return res.status(200).json({ ok: true,
      periodoDias: dias,
      RESUMO: {
        perguntados: perguntas,
        responderam,
        taxaDeResposta: perguntas ? Math.round(responderam / perguntas * 100) + '%' : '—',
        elogios, ressalvas, reclamacoes,
        // 🎯 dos que responderam, quantos estavam plenamente satisfeitos
        satisfacao: responderam ? Math.round(elogios / responderam * 100) + '%' : '—',
        avaliacoesPedidas: elogios,
        semResposta: Math.max(0, perguntas - responderam),
      },
      POR_DIA: Object.entries(porDia).sort().reverse().map(([dia, v]) =>
        dia + ' | perguntou ' + String(v.pergunta).padStart(3) +
        ' | elogio ' + String(v.elogio).padStart(3) +
        ' | ressalva ' + String(v.ressalva).padStart(3) +
        ' | reclamação ' + String(v.reclamacao).padStart(3)),
      ELOGIOS: lista('elogio'),
      RESSALVAS: lista('ressalva'),
      RECLAMACOES: lista('reclamacao'),
      PERGUNTADOS: lista('pergunta').slice(0, 60) });
  }

  // ── 📒 REGISTRAR-ERP: livro de quem entrou no sistema de gestão ──
  // O histórico do cartão não serve como data de entrada: todo domingo à
  // meia-noite uma limpeza move os finalizados em massa, e o registro passa a
  // marcar a hora do processamento em vez do dia em que o cliente recebeu.
  // Este livro é escrito ao longo do dia, quando a entrada de fato acontece.
  if (action === 'registrar-erp') {
    const agora = new Date();
    const hojeBR = new Date(agora.getTime() - 3 * 3600000).toISOString().slice(0, 10);
    const d8e = t => String(t || '').replace(/\D/g, '').slice(-8);
    const [ppA, ppT, flE, livroAtual] = await Promise.all([
      dbGet('reparoeletro_pipe'), dbGet('tv_pipe'),
      dbGet('reparoeletro_frenteloja'), dbGet('erp_entradas'),
    ]);
    const livro = livroAtual || { registros: {} };
    livro.registros = livro.registros || {};

    // 🕛 a limpeza semanal roda domingo às 23h59: o que for visto nessa janela
    // não é entrada real e não deve ser gravado
    const bBR = new Date(agora.getTime() - 3 * 3600000);
    const ehLimpeza = bBR.getUTCDay() === 0 && bBR.getUTCHours() === 23;
    if (ehLimpeza && String(req.query.forcar || '') !== '1') {
      return res.status(200).json({ ok: true, ignorado: true,
        motivo: 'janela da limpeza semanal — movimentação em massa não é entrada real' });
    }

    let novos = 0;
    const registrados = [];
    for (const [db, sis, lista] of [[ppA, 'ADM', 'cards'], [ppT, 'TV', 'cards'],
                                     [flE, 'LOJA', 'fichas']]) {
      for (const c of (((db || {})[lista]) || [])) {
        if (String(c.phaseId || c.phase || '') !== 'erp') continue;
        const tel = d8e(c.telefone);
        if (tel.length < 8) continue;
        const id = String(c.id || tel);
        if (livro.registros[id]) continue;      // já registrado antes
        livro.registros[id] = {
          em: agora.toISOString(), dia: hojeBR, sis,
          cardId: String(c.id || ''),   // permite baixar a ficha certa no GMB
          nome: c.nomeContato || c.nome || '?',
          telefone: String(c.telefone || '').replace(/\D/g, ''),
          equipamento: String(c.equipamento || c.descricao || '').slice(0, 60),
          valor: Number(c.valor || 0),
        };
        novos++;
        registrados.push(sis + ' | ' + String(c.nomeContato || c.nome || '?').slice(0, 22) +
          ' ' + tel.slice(-4));
      }
    }
    // 🧹 o livro guarda 90 dias: passado isso a pesquisa já não interessa
    const corte = Date.now() - 90 * 86400000;
    for (const [id, r] of Object.entries(livro.registros)) {
      if (new Date(r.em || 0).getTime() < corte) delete livro.registros[id];
    }
    if (novos) await dbSet('erp_entradas', livro);
    return res.status(200).json({ ok: true, dia: hojeBR,
      novasEntradas: novos, totalNoLivro: Object.keys(livro.registros).length,
      L: registrados.slice(0, 40) });
  }

  // ── ⭐ RESPOSTAS-SATISFACAO: só quem elogiou sem ressalva recebe o pedido ──
  // O critério é estrito de propósito. Pedir avaliação a quem fez uma sugestão
  // ou relatou um problema, ainda que de leve, é convidar a pessoa a escrever
  // isso publicamente — e machuca justamente onde a nota importa.
  if (action === 'respostas-satisfacao') {
    const aplicar = String(req.query.aplicar || '') === '1';
    const d8p = t => String(t || '').replace(/\D/g, '').slice(-8);
    const LINK = 'https://g.page/r/CUDbfbB2xOBHEBM/review';
    const controle = (await dbGet('wa_pesquisa_satisfacao')) || { clientes: {} };
    const clientes = controle.clientes || {};

    // a última mensagem de cada cliente que ainda aguarda resposta
    const respostas = {};
    try {
      for (const e of (await lerEvts())) {
        if (e.dir !== 'in') continue;
        const d = d8p(e.tel); if (!d) continue;
        const c = clientes[d];
        if (!c || !c.aguardandoResposta || c.avaliacaoPedida) continue;
        const q = new Date(e.ts || 0).getTime();
        if (q <= new Date(c.em || 0).getTime()) continue;   // anterior à pergunta
        const txt = String(e.texto || '').trim();
        if (!txt || txt.startsWith('🎤 [')) continue;        // áudio sem transcrição
        if (!respostas[d] || q > respostas[d].ts) respostas[d] = { ts: q, texto: txt, c };
      }
    } catch (e) {}

    const CHAVE = (process.env.ANTHROPIC_API_KEY || '').trim();
    const elogios = [], comRessalva = [], indefinidos = [], reclamacoes = [];

    for (const [d, r] of Object.entries(respostas)) {
      let veredito = null;
      if (CHAVE) {
        try {
          const rr = await fetch('https://api.anthropic.com/v1/messages', {
            method: 'POST',
            headers: { 'x-api-key': CHAVE, 'anthropic-version': '2023-06-01',
              'content-type': 'application/json' },
            body: JSON.stringify({ model: 'claude-sonnet-4-6', max_tokens: 12, temperature: 0,
              system: 'Você classifica a resposta de um cliente a uma pesquisa de satisfação ' +
                'de assistência técnica.\n\n' +
                'Responda APENAS uma palavra:\n' +
                'ELOGIO — se a pessoa diz que está tudo certo, funcionando, satisfeita, ' +
                'agradece ou elogia, E NÃO acrescenta nada além disso.\n' +
                'RECLAMACAO — se a pessoa relata que algo NÃO está funcionando, voltou ' +
                'a apresentar defeito, está insatisfeita, ou cobra uma solução.\n' +
                'RESSALVA — se há observação, sugestão, dúvida, pedido, condição ' +
                '("está bom MAS...", "funcionando, só que..."), ou se ela ainda não ' +
                'usou o equipamento — mas sem relatar defeito nem insatisfação.\n' +
                'INDEFINIDO — se a resposta é ambígua, vazia de conteúdo, ou não responde ' +
                'à pergunta.\n\n' +
                'Na dúvida entre ELOGIO e RESSALVA, responda RESSALVA.',
              messages: [{ role: 'user', content: r.texto.slice(0, 600) }] }),
          }).then(x => x.json());
          const t = ((rr.content || []).filter(b => b.type === 'text')
            .map(b => b.text).join('') || '').trim().toUpperCase();
          if (t.includes('RECLAMACAO') || t.includes('RECLAMAÇÃO')) veredito = 'reclamacao';
          else if (t.includes('ELOGIO')) veredito = 'elogio';
          else if (t.includes('RESSALVA')) veredito = 'ressalva';
          else veredito = 'indefinido';
        } catch (e) { veredito = null; }
      }
      // 🔒 sem classificação não se pede avaliação: o silêncio é mais seguro
      if (veredito === null) veredito = 'indefinido';
      const linha = String(r.c.nome || '?').slice(0, 22) + ' ' + d.slice(-4) +
        ' → "' + r.texto.slice(0, 60).replace(/\n/g, ' ') + '"';
      if (veredito === 'elogio') elogios.push({ d, r, linha });
      else if (veredito === 'reclamacao') reclamacoes.push({ d, r, linha });
      else if (veredito === 'ressalva') comRessalva.push(linha);
      else indefinidos.push(linha);
    }

    if (!aplicar) {
      return res.status(200).json({ ok: true, modo: 'prévia',
        responderam: Object.keys(respostas).length,
        vaoReceberPedidoDeAvaliacao: elogios.length,
        ELOGIO_PURO: elogios.map(x => x.linha),
        RECLAMACOES_VAO_PARA_CONFLITOS: reclamacoes.map(x => x.linha),
        COM_RESSALVA_NAO_RECEBEM: comRessalva,
        INDEFINIDOS_NAO_RECEBEM: indefinidos,
        observacao: 'quem fez qualquer observação não recebe o pedido de avaliação; ' +
          'na dúvida, o sistema não pede',
        dica: 'para enviar: &aplicar=1' });
    }

    const cfg = (await dbGet('wa_credenciais')) || {};
    const pid = cfg.phoneId || process.env.WA_PHONE_ID;
    const tk = cfg.token || process.env.WA_TOKEN;
    if (!pid || !tk) return res.status(200).json({ ok: false, error: 'credenciais ausentes' });

    const texto = 'Maravilha! A qualquer momento que precisar de algo pode me chamar aqui ' +
      'que estaremos prontos pra te atender.\n\n' +
      'Quero apenas te fazer um último pedido, que me ajuda demais a continuar fazendo ' +
      'um bom trabalho. Sua avaliação no nosso Google é muito importante pro nosso ' +
      'crescimento. Se possível nos avalie por favor:\n\n' + LINK;

    const feitos = [], erros = [];
    for (const x of elogios) {
      let tel = x.d;
      // recompõe o número completo a partir do registro
      const cli = clientes[x.d] || {};
      const cheio = String(cli.telefone || '').replace(/\D/g, '');
      if (!cheio || cheio.length < 12) {
        erros.push(x.linha + ' — não tenho o número completo registrado');
        continue;
      }
      try {
        const r = await fetch('https://graph.facebook.com/v20.0/' + pid + '/messages', {
          method: 'POST',
          headers: { Authorization: 'Bearer ' + tk, 'Content-Type': 'application/json' },
          body: JSON.stringify({ messaging_product: 'whatsapp', to: cheio,
            type: 'text', text: { body: texto, preview_url: true } }),
        }).then(y => y.json());
        if (r && r.messages && r.messages[0]) {
          clientes[x.d].avaliacaoPedida = true;
          clientes[x.d].avaliacaoPedidaEm = new Date().toISOString();
          clientes[x.d].aguardandoResposta = false;
          clientes[x.d].respostaCliente = String(x.r.texto).slice(0, 300);
          // ⭐ a ficha já saiu do painel quando a pergunta foi feita; esta é uma
          // segunda tentativa para o caso de aquela ter falhado
          try {
            if (clientes[x.d] && clientes[x.d].gmbBaixado) throw new Error('já baixado');
            await fetch('https://reparoeletroadm.com/api/orcamento?action=gmb-marcar-enviado&k=' +
              ((process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim()), {
              method: 'POST', headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ id: cli.cardId || cli.id || x.d,
                nome: cli.nome || '?', tel: cheio,
                desc: (cli.equipamento || '') + ' · pedido enviado pela pesquisa de satisfação' }),
            });
            clientes[x.d].gmbBaixado = true;
          } catch (e) { clientes[x.d].gmbBaixado = false; }
          await anotar({ tipo: 'elogio', tel: x.d, nome: cli.nome, sis: cli.sis,
            equipamento: cli.equipamento, resposta: String(x.r.texto).slice(0, 200),
            avaliacaoPedida: true, baixadoNoGmb: clientes[x.d].gmbBaixado === true });
          feitos.push(x.linha);
        } else {
          erros.push(x.linha + ' — ' + ((r && r.error && r.error.message) || 'falha'));
        }
      } catch (e) { erros.push(x.linha + ' — ' + e.message); }
      await new Promise(s => setTimeout(s, 400));
    }
    // 🚨 reclamação na pesquisa é conflito: o cliente acabou de dizer que algo
    // não está certo, e isso precisa chegar a quem resolve, não morrer no log
    const conflitosAbertos = [];
    for (const x of reclamacoes) {
      const cli = clientes[x.d] || {};
      try {
        const r = await fetch('https://reparoeletroadm.com/api/conflitos?action=criar&k=' +
          ((process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim()), {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            // ⚠️ os nomes dos campos são os que o registro de conflitos espera:
            // título e prioridade são obrigatórios
            titulo: 'Reclamação na pesquisa — ' + String(cli.nome || '?').slice(0, 30),
            tipo: 'qualidade',
            prioridade: 'alto',
            setor: cli.sis === 'TV' ? 'TV' : 'Assistência',
            ficha: String(cli.nome || '?') + ' ' + String(cli.telefone || x.d).slice(-4),
            cliente: cli.nome || '?',
            telefone: cli.telefone || x.d,
            equipamento: cli.equipamento || '',
            descricao: 'Respondeu à pesquisa do dia seguinte à entrega: "' +
              String(x.r.texto).slice(0, 400) + '"' +
              (cli.equipamento ? '\n\nEquipamento: ' + cli.equipamento : ''),
            registradoPor: 'pesquisa de satisfação',
          }),
        }).then(y => y.json());
        if (r && r.ok) {
          clientes[x.d].aguardandoResposta = false;
          clientes[x.d].reclamou = true;
          clientes[x.d].conflitoId = r.id || r.conflito && r.conflito.id || null;
          clientes[x.d].respostaCliente = String(x.r.texto).slice(0, 300);
          await anotar({ tipo: 'reclamacao', tel: x.d, nome: cli.nome, sis: cli.sis,
            equipamento: cli.equipamento, resposta: String(x.r.texto).slice(0, 300),
            conflitoAberto: true });
          conflitosAbertos.push(x.linha);
        } else {
          erros.push(x.linha + ' — não consegui abrir o conflito: ' +
            ((r && r.error) || 'sem retorno'));
        }
      } catch (e) { erros.push(x.linha + ' — conflito: ' + e.message); }
    }

    // quem respondeu com ressalva sai da espera: o caso é da equipe, não do robô
    for (const linha of comRessalva) {
      const fin = linha.match(/\s(\d{4})\s→/);
      if (!fin) continue;
      for (const [d, c] of Object.entries(clientes)) {
        if (d.slice(-4) === fin[1] && c.aguardandoResposta) {
          c.aguardandoResposta = false; c.teveRessalva = true;
          await anotar({ tipo: 'ressalva', tel: d, nome: c.nome, sis: c.sis,
            equipamento: c.equipamento,
            resposta: String(linha.split('→ ')[1] || '').replace(/^"|"$/g, '').slice(0, 200) });
        }
      }
    }
    await dbSet('wa_pesquisa_satisfacao', controle);
    return res.status(200).json({ ok: erros.length === 0,
      pedidosEnviados: feitos.length, L: feitos, erros,
      conflitosAbertos: conflitosAbertos.length,
      RECLAMACOES: conflitosAbertos,
      comRessalvaParaAEquipe: comRessalva });
  }

  // ── 🔬 onde-esta-erp: mostra a estrutura real dos registros em ERP ──
  // Antes de decidir a data de entrada é preciso ver como cada base guarda
  // isso. Supor o formato levou a contar trinta clientes num dia e nenhum
  // no outro — esta consulta mostra os campos como eles são.
  if (action === 'onde-esta-erp') {
    const [ppA, ppT, flS] = await Promise.all([
      dbGet('reparoeletro_pipe'), dbGet('tv_pipe'), dbGet('reparoeletro_frenteloja'),
    ]);
    const amostra = (lista, rotulo, campoFase) => {
      const emErp = (lista || []).filter(x =>
        String(x[campoFase] || x.phase || x.phaseId || '') === 'erp');
      return { onde: rotulo, totalEmErp: emErp.length,
        exemplos: emErp.slice(0, 4).map(x => ({
          nome: String(x.nomeContato || x.nome || '?').slice(0, 24),
          movedAt: x.movedAt || null,
          criadoEm: x.criadoEm || null,
          camposComData: Object.keys(x).filter(k =>
            /Em$|At$|data|_ts/i.test(k) && x[k]).slice(0, 10),
          historico: (x.history || []).slice(-4).map(hh =>
            String(hh.phase || hh.phaseId || '?') + '@' +
            String(hh.ts || hh.timestamp || '?').slice(0, 16)),
          totalNoHistorico: (x.history || []).length,
        })) };
    };
    return res.status(200).json({ ok: true,
      PIPE_ADM: amostra(((ppA || {}).cards) || [], 'reparoeletro_pipe', 'phaseId'),
      PIPE_TV: amostra(((ppT || {}).cards) || [], 'tv_pipe', 'phaseId'),
      FRENTE_LOJA: amostra(((flS || {}).fichas) || [], 'reparoeletro_frenteloja', 'phase'),
      comoLer: 'veja em camposComData e historico onde a entrada no ERP fica registrada' });
  }

  if (action === 'pesquisa-satisfacao') {
    const aplicar = String(req.query.aplicar || '') === '1';
    const d8s = t => String(t || '').replace(/\D/g, '').slice(-8);
    // o dia anterior, em horário de Brasília
    // 🗓️ no domingo ninguém entra em ERP, então perguntar sobre "ontem" na
    // segunda não acharia nada — e os entregues no sábado ficariam sem pesquisa.
    // Recua até o último dia útil quando o anterior é domingo.
    // a data deixou de ser critério: o que define a fila é estar parado no
    // painel, e não a data em que o cartão entrou no sistema de gestão
    const ontem = String(req.query.dia || '');


    // 🎯 A FONTE É A ABA GOOGLE MEU NEGÓCIO: quem está parado lá é exatamente
    // quem recebeu o serviço e ainda não foi abordado. O livro de entradas
    // registra quando cada um entrou no sistema de gestão, mas a primeira
    // gravação recolheu de uma vez tudo que já estava lá, misturando quem
    // recebeu ontem com quem recebeu semanas atrás.
    const pend = (await dbGet('gmb_pendentes')) || { fichas: [], ids: [] };
    const enviadosGmb = (await dbGet('gmb_enviados')) || { fichas: [] };
    const jaContatado = new Set((enviadosGmb.fichas || [])
      .map(f => d8s(f.tel || f.telefone)).filter(t => t.length >= 8));
    const ctrlS = await dbGet('wa_pesquisa_satisfacao');
    const controle = ctrlS || { clientes: {} };
    const candidatos = [];
    for (const f of (pend.fichas || [])) {
      const tel = d8s(f.tel || f.telefone);
      if (tel.length < 8) continue;
      // quem a equipe já abordou à mão não precisa da pesquisa automática
      if (jaContatado.has(tel)) continue;
      candidatos.push({ sis: f.sis || 'ADM', nome: f.nome || f.nomeContato || '',
        cardId: String(f.id || ''),
        telefone: f.tel || f.telefone,
        equipamento: f.desc || f.equipamento || '',
        quando: f.entrouEm || f.criadoEm || null });
    }
    const semHistorico = [];


    // um por cliente, e nunca duas vezes
    const fila = [], jaPerguntado = [];
    const vistos = new Set();
    for (const c of candidatos) {
      const d = d8s(c.telefone);
      if (d.length < 8 || vistos.has(d)) continue;
      vistos.add(d);
      if ((controle.clientes || {})[d]) { jaPerguntado.push(c.nome + ' ' + d.slice(-4)); continue; }
      fila.push(c);
    }

    if (!aplicar) {
      return res.status(200).json({ ok: true, modo: 'prévia',
        fonte: 'aba Google Meu Negócio — quem está parado lá aguardando abordagem',
        noPainel: candidatos.length,
        jaAbordadosPelaEquipe: 'não entram: quem já consta como contatado à mão',
        vaoReceber: fila.length, jaPerguntado,
        L: fila.map(c => c.sis + ' | ' + String(c.nome).slice(0, 22).padEnd(22) +
          ' ' + String(c.telefone || '').slice(-4) + ' | ' +
          String(c.equipamento).slice(0, 28)),
        textoQueSai: 'Bom dia, NOME! Pedro aqui da Reparo Eletro.\n\nGostaria de saber ' +
          'se está tudo certinho com o seu equipamento e se você já conseguiu utilizar. ' +
          'Qualquer dúvida que tiver estou pronto pra te atender.\n\nAguardo sua resposta.',
        dica: 'para enviar: &aplicar=1' });
    }

    const cfg = (await dbGet('wa_credenciais')) || {};
    const pid = cfg.phoneId || process.env.WA_PHONE_ID;
    const tk = cfg.token || process.env.WA_TOKEN;
    if (!pid || !tk) return res.status(200).json({ ok: false, error: 'credenciais ausentes' });

    // quem tem janela aberta recebe texto; os demais, o modelo
    const ultimaIn = {};
    try {
      for (const e of (await lerEvts())) {
        if (e.dir !== 'in') continue;
        const d = d8s(e.tel); if (!d) continue;
        const q = new Date(e.ts || 0).getTime();
        if (!ultimaIn[d] || q > ultimaIn[d]) ultimaIn[d] = q;
      }
    } catch (e) {}

    const feitos = [], erros = [];
    controle.clientes = controle.clientes || {};
    for (const c of fila) {
      if (feitos.length >= 25) break;      // lote curto: a função tem tempo limitado
      let tel = String(c.telefone || '').replace(/\D/g, '');
      if (tel.length === 10 || tel.length === 11) tel = '55' + tel;
      if (tel.length < 12) { erros.push(c.nome + ': telefone inválido'); continue; }
      const primeiro = String(c.nome || '').trim().split(/\s+/)[0] || 'tudo bem';
      const d = d8s(tel);
      const janelaAberta = ultimaIn[d] && (Date.now() - ultimaIn[d]) < 24 * 3600000;
      const texto = 'Bom dia, ' + primeiro + '! Pedro aqui da Reparo Eletro.\n\n' +
        'Gostaria de saber se está tudo certinho com o seu equipamento e se você já ' +
        'conseguiu utilizar. Qualquer dúvida que tiver estou pronto pra te atender.\n\n' +
        'Aguardo sua resposta.';
      try {
        const corpo = janelaAberta
          ? { messaging_product: 'whatsapp', to: tel, type: 'text', text: { body: texto } }
          : { messaging_product: 'whatsapp', to: tel, type: 'template',
              template: { name: 'pesquisa_satisfacao', language: { code: 'pt_BR' },
                components: [{ type: 'body', parameters: [{ type: 'text', text: primeiro }] }] } };
        const r = await fetch('https://graph.facebook.com/v20.0/' + pid + '/messages', {
          method: 'POST',
          headers: { Authorization: 'Bearer ' + tk, 'Content-Type': 'application/json' },
          body: JSON.stringify(corpo),
        }).then(x => x.json());
        if (r && r.messages && r.messages[0]) {
          // 🏷️ marca que a pergunta foi feita: a resposta será classificada depois
          controle.clientes[d] = { em: new Date().toISOString(),
            nome: c.nome, equipamento: c.equipamento, sis: c.sis, cardId: c.cardId || '',
            telefone: tel,          // 📞 guarda o número completo: recompor por
                                    // DDD suposto erraria em cliente de fora

            via: janelaAberta ? 'mensagem' : 'modelo',
            aguardandoResposta: true, avaliacaoPedida: false };
          // ⭐ a pergunta saiu: a ficha sai da aba do Google Meu Negócio pelo
          // mesmo caminho do botão de copiar e enviar. O cliente já foi
          // abordado — deixá-lo lá faria a equipe abordar de novo.
          let baixou = false;
          try {
            const rb = await fetch('https://reparoeletroadm.com/api/orcamento' +
              '?action=gmb-marcar-enviado&k=' +
              ((process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim()), {
              method: 'POST', headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ id: c.cardId || d, nome: c.nome || '?', tel: tel,
                desc: (c.equipamento || '') + ' · pesquisa de satisfação enviada' }),
            }).then(x => x.json());
            baixou = !!(rb && rb.ok);
          } catch (e) {}
          controle.clientes[d].gmbBaixado = baixou;
          await anotar({ tipo: 'pergunta', tel: d, nome: c.nome, sis: c.sis,
            equipamento: c.equipamento, via: janelaAberta ? 'mensagem' : 'modelo',
            baixadoNoGmb: baixou });
          feitos.push(c.sis + ' | ' + String(c.nome).slice(0, 22) + ' ' + d.slice(-4) +
            (baixou ? ' | ✅ saiu do GMB' : ' | ⚠️ não consegui baixar no GMB'));
        } else {
          erros.push(String(c.nome).slice(0, 20) + ': ' +
            ((r && r.error && r.error.message) || 'falha no envio'));
        }
      } catch (e) { erros.push(String(c.nome).slice(0, 20) + ': ' + e.message); }
      await new Promise(s => setTimeout(s, 400));
    }
    if (feitos.length) await dbSet('wa_pesquisa_satisfacao', controle);
    return res.status(200).json({ ok: erros.length === 0,
      diaConsultado: ontem, enviados: feitos.length,
      faltam: Math.max(0, fila.length - feitos.length - erros.length),
      L: feitos, erros });
  }


  return res.status(404).json({ ok: false, error: 'ação não encontrada',
    disponiveis: ['pesquisa-satisfacao', 'respostas-satisfacao', 'onde-esta-erp'] });
}
