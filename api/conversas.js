// ═══════════════════════════════════════════════════════════════════
// ARQUIVO DE CONVERSAS
//
// A lista de eventos do WhatsApp é uma janela corrente: as consultas leem os
// últimos milhares de registros e, com centenas de atendimentos por dia, isso
// cobre poucos dias. Quando alguém precisa saber o orçamento passado a um
// cliente semanas atrás, a conversa já saiu da janela.
//
// Este módulo guarda a conversa POR CLIENTE, em chave própria. Não há
// competição por espaço entre clientes: o histórico de um não empurra o de
// outro para fora. Cada cliente mantém as suas últimas 400 mensagens.
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
      method: 'POST',
      headers: { Authorization: `Bearer ${T}`, 'Content-Type': 'application/json' },
      body: JSON.stringify(valor),
    });
    return true;
  } catch (e) { return false; }
}
/** Lê a janela corrente de eventos — a fonte que este módulo arquiva. */
async function lerEvts(quantos) {
  try {
    const n = Math.max(1000, Math.min(20000, quantos || 8000));
    const r = await fetch(`${U}/lrange/${EVT_LIST}/-${n}/-1`,
      { headers: { Authorization: `Bearer ${T}` } }).then(x => x.json());
    const out = [];
    for (const s of (r.result || [])) { try { out.push(JSON.parse(s)); } catch (e) {} }
    return out;
  } catch (e) { return []; }
}

const d8 = (t) => String(t || '').replace(/\D/g, '').slice(-8);
const chaveDe = (tel) => 'wa_conv_' + d8(tel);

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

  // ── 💾 arquivar: move a janela corrente para o arquivo de cada cliente ──
  // Roda de hora em hora. Idempotente: mensagem já arquivada não duplica,
  // porque cada uma é identificada pelo carimbo de tempo e pelo id da Meta.
  if (action === 'arquivar') {
    // 📖 marca d'água: guarda até onde já foi arquivado, e só processa o que
    // veio depois. Sem isso cada passagem relia tudo de novo, gastava o tempo
    // da função e limitava a janela que dava para varrer.
    const marca = (await dbGet('wa_conv_marca')) || {};
    const desdeTs = String(marca.ultimoTs || '');
    // 🔭 a janela pode ser ampla porque só o novo é processado: cobrir mais
    // dias protege contra falhas de execução — se a passagem falhar por horas,
    // a seguinte ainda alcança o que ficou para trás
    const evtsBrutos = await lerEvts(Number(req.query.janela || 20000));
    if (!evtsBrutos.length) return res.status(200).json({ ok: true, msg: 'nada a arquivar' });
    const evts = desdeTs && String(req.query.tudo || '') !== '1'
      ? evtsBrutos.filter(e => String(e.ts || '') > desdeTs)
      : evtsBrutos;
    // 🕳️ detecta buraco: se o evento mais antigo da janela for POSTERIOR à
    // marca, houve período que a janela já não cobre e se perdeu de vez
    const maisAntigo = String((evtsBrutos[0] || {}).ts || '');
    const houveBuraco = !!(desdeTs && maisAntigo && maisAntigo > desdeTs);
    if (!evts.length) return res.status(200).json({ ok: true,
      msg: 'nada novo desde ' + desdeTs, houveBuraco });

    // agrupa por cliente antes de gravar: uma escrita por pessoa, não por mensagem
    const porTel = {};
    for (const e of evts) {
      const d = d8(e.tel);
      if (d.length < 8) continue;
      (porTel[d] = porTel[d] || []).push(e);
    }
    let clientes = 0, novas = 0;
    const nomes = {};
    for (const [d, lista] of Object.entries(porTel)) {
      const arq = (await dbGet(chaveDe(d))) || { tel: d, nome: null, msgs: [] };
      const jaTem = new Set((arq.msgs || [])
        .map(m => String(m.msgId || '') + '|' + String(m.ts || '')));
      let acrescentou = 0;
      for (const e of lista) {
        const id = String(e.msgId || '') + '|' + String(e.ts || '');
        if (jaTem.has(id)) continue;
        jaTem.add(id);
        arq.msgs.push({ ts: e.ts, dir: e.dir, texto: e.texto, tipo: e.tipo,
          msgId: e.msgId, via: e.via || null,
          // 📷 o identificador da mídia permite recuperar a foto depois: sem
          // ele a conversa registra que houve foto mas não há como vê-la
          mediaId: e.mediaId || null });
        acrescentou++;
        if (e.nome && !arq.nome) arq.nome = e.nome;
      }
      if (!acrescentou) continue;
      arq.msgs.sort((a, b) => String(a.ts).localeCompare(String(b.ts)));
      // 📦 400 mensagens por cliente: cobre meses de conversa de um mesmo
      // atendimento sem que um cliente falante ocupe o espaço dos outros
      // 📦 800 mensagens por cliente: um atendimento com áudios transcritos e
      // registros de status consome rápido, e 400 cortava conversas ainda vivas
      if (arq.msgs.length > 800) arq.msgs = arq.msgs.slice(-800);
      arq.atualizadoEm = new Date().toISOString();
      await dbSet(chaveDe(d), arq);
      // índice para poder buscar por nome depois
      nomes[d] = arq.nome || null;
      clientes++; novas += acrescentou;
    }
    // índice de quem tem arquivo, para busca por nome
    if (clientes) {
      const idx = (await dbGet('wa_conv_indice')) || { tels: {} };
      idx.tels = idx.tels || {};
      for (const [d, nome] of Object.entries(nomes)) {
        idx.tels[d] = { nome: nome || (idx.tels[d] || {}).nome || null,
          em: new Date().toISOString() };
      }
      await dbSet('wa_conv_indice', idx);
    }
    // guarda até onde chegou, para a próxima passagem continuar daqui
    const ultimo = evts.reduce((m, e) =>
      String(e.ts || '') > m ? String(e.ts || '') : m, desdeTs);
    if (ultimo) await dbSet('wa_conv_marca', { ultimoTs: ultimo,
      em: new Date().toISOString(), clientes, novas });
    return res.status(200).json({ ok: !houveBuraco,
      clientesAtualizados: clientes, mensagensNovas: novas,
      processados: evts.length, naJanela: evtsBrutos.length,
      arquivadoAte: ultimo ? ultimo.slice(0, 16).replace('T', ' ') : null,
      alerta: houveBuraco
        ? '🚨 houve intervalo sem arquivar maior que a janela — parte se perdeu'
        : null });
  }

  // ── 📖 ver: a conversa completa de um cliente, venha de onde vier ──
  if (action === 'ver') {
    const alvo = String(req.query.tel || '');
    if (d8(alvo).length < 4) return res.status(400).json({ ok: false,
      error: 'informe &tel= com ao menos 4 dígitos' });
    const d = d8(alvo);
    const hh = x => x ? new Date(new Date(x).getTime() - 3 * 3600000)
      .toISOString().slice(0, 16).replace('T', ' ') : '—';

    // o arquivo é a base; a janela corrente cobre o que ainda não foi arquivado
    let arq = await dbGet(chaveDe(d));
    if (!arq && d.length < 8) {
      // busca por final parcial, quando só se sabe os 4 últimos dígitos
      const idx = (await dbGet('wa_conv_indice')) || { tels: {} };
      const achou = Object.keys(idx.tels || {}).filter(t => t.endsWith(d));
      if (achou.length === 1) arq = await dbGet(chaveDe(achou[0]));
      else if (achou.length > 1) return res.status(200).json({ ok: false,
        error: 'mais de um cliente termina em ' + d,
        candidatos: achou.map(t => t + ' — ' + ((idx.tels[t] || {}).nome || '?')) });
    }
    const doArquivo = ((arq || {}).msgs) || [];
    const daJanela = (await lerEvts()).filter(e => d8(e.tel).endsWith(d) && e.dir !== 'status');
    const vistas = new Set(doArquivo.map(m => String(m.msgId || '') + '|' + String(m.ts || '')));
    const tudo = doArquivo.concat(daJanela
      .filter(e => !vistas.has(String(e.msgId || '') + '|' + String(e.ts || '')))
      .map(e => ({ ts: e.ts, dir: e.dir, texto: e.texto, tipo: e.tipo, msgId: e.msgId })));
    tudo.sort((a, b) => String(a.ts).localeCompare(String(b.ts)));

    if (!tudo.length) return res.status(200).json({ ok: false,
      error: 'nenhuma conversa encontrada para ' + d,
      dica: 'se o cliente é antigo, a conversa pode ser anterior ao arquivo' });

    // 💰 valores citados na conversa: é o que a operação mais procura
    const valores = [];
    for (const m of tudo) {
      const achados = String(m.texto || '').match(/R\$ ?[\d.]+,?\d{0,2}/g);
      if (achados) for (const v of achados) {
        valores.push(hh(m.ts) + ' | ' + (m.dir === 'out' ? 'nós' : 'cliente') + ' | ' + v);
      }
    }
    return res.status(200).json({ ok: true,
      telefone: d, nome: (arq || {}).nome || null,
      mensagens: tudo.length,
      doArquivo: doArquivo.length, daJanelaCorrente: tudo.length - doArquivo.length,
      primeira: hh(tudo[0].ts), ultima: hh(tudo[tudo.length - 1].ts),
      VALORES_CITADOS: valores,
      CONVERSA: tudo.map(m => hh(m.ts) + ' ' +
        (m.dir === 'out' ? '→' : '←') + ' ' +
        String(m.texto || '(' + (m.tipo || 'sem texto') + ')').replace(/\n/g, ' ⏎ ')) });
  }

  // ── 📷 foto: recupera uma imagem que o cliente enviou ──
  // A plataforma guarda a mídia por um tempo e entrega mediante o identificador
  // que veio no recebimento. Sem esta ação a conversa registrava que houve foto
  // e não havia como abri-la.
  if (action === 'foto') {
    const mid = String(req.query.mediaId || '').trim();
    const alvo = String(req.query.tel || '');
    const cfg = (await dbGet('wa_credenciais')) || {};
    const tk = cfg.token || process.env.WA_TOKEN;

    // sem identificador, lista as fotos daquele cliente para escolher
    if (!mid) {
      if (!alvo) return res.status(400).json({ ok: false,
        error: 'informe &tel= para listar as fotos, ou &mediaId= para abrir uma' });
      const arq = await dbGet(chaveDe(alvo));
      const msgs = ((arq || {}).msgs) || [];
      const fotos = msgs.filter(m => m.mediaId ||
        /📷|🎬|\[foto\]|\[vídeo\]/.test(String(m.texto || '')));
      return res.status(200).json({ ok: true,
        cliente: (arq || {}).nome || null,
        fotosEncontradas: fotos.length,
        FOTOS: fotos.map(m => ({
          quando: new Date(new Date(m.ts).getTime() - 3 * 3600000)
            .toISOString().slice(5, 16).replace('T', ' '),
          de: m.dir === 'in' ? 'cliente' : 'nós',
          texto: m.texto,
          mediaId: m.mediaId,
          link: m.mediaId
            ? 'https://reparoeletroadm.com/api/conversas?action=foto&mediaId=' +
              m.mediaId + '&k=' + ((process.env.TECHFLOW_KEY || '').trim())
            : '⚠️ sem identificador — foto anterior ao registro de mídia' })) });
    }

    try {
      const meta = await fetch('https://graph.facebook.com/v20.0/' + mid,
        { headers: { Authorization: 'Bearer ' + tk } }).then(x => x.json());
      if (!meta || !meta.url) {
        return res.status(404).json({ ok: false,
          error: (meta && meta.error && meta.error.message) ||
            'a plataforma não devolveu a mídia — pode ter expirado' });
      }
      const bin = await fetch(meta.url, { headers: { Authorization: 'Bearer ' + tk } });
      if (!bin.ok) return res.status(502).json({ ok: false,
        error: 'falha ao baixar: HTTP ' + bin.status });
      const buf = Buffer.from(await bin.arrayBuffer());
      res.setHeader('Content-Type', meta.mime_type || 'image/jpeg');
      res.setHeader('Cache-Control', 'private, max-age=300');
      return res.status(200).send(buf);
    } catch (e) {
      return res.status(500).json({ ok: false, error: e.message });
    }
  }

  // ── 📊 estado: o que o arquivo já cobre ──
  if (action === 'estado') {
    const idx = (await dbGet('wa_conv_indice')) || { tels: {} };
    const evts = await lerEvts();
    const hh = x => x ? String(x).slice(0, 16).replace('T', ' ') : '—';
    return res.status(200).json({ ok: true,
      clientesArquivados: Object.keys(idx.tels || {}).length,
      janelaCorrente: {
        eventos: evts.length,
        maisAntigo: evts.length ? hh(evts[0].ts) : null,
        maisRecente: evts.length ? hh(evts[evts.length - 1].ts) : null,
        cobertura: evts.length
          ? ((new Date(evts[evts.length - 1].ts) - new Date(evts[0].ts)) / 86400000).toFixed(1) + ' dias'
          : null,
      },
      observacao: 'a janela corrente é o que se perde; o arquivo guarda por cliente ' +
        'e não é disputado entre eles' });
  }

  return res.status(404).json({ ok: false, error: 'ação não encontrada',
    disponiveis: ['arquivar', 'ver', 'estado', 'foto'] });
}
