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
    let ontem = String(req.query.dia || '');
    if (!ontem) {
      const d = new Date(Date.now() - 3 * 3600000 - 86400000);
      if (d.getUTCDay() === 0) d.setUTCDate(d.getUTCDate() - 1);   // domingo → sábado
      ontem = d.toISOString().slice(0, 10);
    }
    const iniS = new Date(ontem + 'T00:00:00-03:00').getTime();
    const fimS = iniS + 86400000 - 1;
    const naData = d => { if (!d) return false;
      const t = new Date(d).getTime(); return t >= iniS && t <= fimS; };

    // 🎯 quem entrou em ERP ontem, nas duas frentes e no balcão
    const [ppA, ppT, flS, ctrlS] = await Promise.all([
      dbGet('reparoeletro_pipe'), dbGet('tv_pipe'),
      dbGet('reparoeletro_frenteloja'), dbGet('wa_pesquisa_satisfacao'),
    ]);
    const controle = ctrlS || { clientes: {} };
    const candidatos = [];
    // 📅 quando o card ENTROU no ERP: vem do histórico, que registra cada
    // passagem de fase. A última movimentação não serve — ela muda a cada
    // alteração posterior e faria o card parecer ter entrado hoje.
    const semHistorico = [];
    const entrouNoErp = (c) => {
      const hs = (c.history || [])
        .filter(x => String(x.phase || x.phaseId || '') === 'erp')
        .map(x => String(x.ts || x.timestamp || '')).filter(Boolean).sort();
      return hs.length ? hs[0] : null;
    };
    for (const [db, sis] of [[ppA, 'ADM'], [ppT, 'TV']]) {
      for (const c of (((db || {}).cards) || [])) {
        if (String(c.phaseId || c.phase || '') !== 'erp') continue;
        const q = entrouNoErp(c);
        if (!q) { semHistorico.push(sis + ' | ' + String(c.nomeContato || c.nome || '?').slice(0, 24) +
          ' — está em ERP mas o histórico não registra quando entrou'); continue; }
        if (!naData(q)) continue;
        candidatos.push({ sis, nome: c.nomeContato || c.nome || '', telefone: c.telefone,
          equipamento: c.equipamento || c.descricao || '', quando: q });
      }
    }
    for (const f of (((flS || {}).fichas) || [])) {
      if (String(f.phase || '') !== 'erp') continue;
      const q = entrouNoErp(f);
      if (!naData(q)) continue;
      candidatos.push({ sis: 'LOJA', nome: f.nomeContato || f.nome || '',
        telefone: f.telefone, equipamento: f.equipamento || '', quando: q });
    }

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
        diaConsultado: ontem, entraramNoErp: candidatos.length,
        // 🔍 de onde veio a data de cada um, para conferir contra a realidade
        COMO_FOI_DATADO: candidatos.map(c => c.sis + ' | ' +
          String(c.nome).slice(0, 22).padEnd(22) + ' | entrou no ERP em ' +
          String(c.quando).slice(0, 16).replace('T', ' ')),
        SEM_HISTORICO_DE_ERP: (semHistorico || []).length,
        LISTA_SEM_HISTORICO: (semHistorico || []).slice(0, 40),
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
            nome: c.nome, equipamento: c.equipamento, sis: c.sis,
            telefone: tel,          // 📞 guarda o número completo: recompor por
                                    // DDD suposto erraria em cliente de fora

            via: janelaAberta ? 'mensagem' : 'modelo',
            aguardandoResposta: true, avaliacaoPedida: false };
          feitos.push(c.sis + ' | ' + String(c.nome).slice(0, 22) + ' ' + d.slice(-4));
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
