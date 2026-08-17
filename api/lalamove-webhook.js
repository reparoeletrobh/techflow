// ═══════════════════════════════════════════════════════════════════
// WEBHOOK DA LALAMOVE
//
// A corrida é criada à mão no aplicativo, com os mesmos endereços da rota
// montada no sistema. A plataforma avisa aqui cada mudança de estado, e é
// isso que permite descobrir quem pegou aquela rota sem ninguém digitar.
//
// O encontro entre a corrida e a rota é feito pelos TELEFONES dos destinos:
// são os mesmos clientes, então bastam dois em comum para não haver dúvida.
// Quando a corrida é coletada — o entregador já saiu da loja — os dados dele
// são gravados na rota, dispensando o preenchimento manual da saída.
// ═══════════════════════════════════════════════════════════════════

const U = (process.env.UPSTASH_URL || process.env.KV_REST_API_URL || '')
  .replace(/['"]/g, '').trim();
const T = (process.env.UPSTASH_TOKEN || process.env.KV_REST_API_TOKEN || '')
  .replace(/[\n\r'"]/g, '').trim();

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

/** Últimos oito dígitos: compara telefone escrito de formas diferentes. */
const d8 = (t) => String(t || '').replace(/\D/g, '').slice(-8);

/**
 * Encontra a rota do almoxarifado que corresponde a esta corrida.
 * O critério é a coincidência de telefones dos destinos: um só poderia ser
 * acaso, dois já identificam a rota com segurança. Rotas que já saíram ou
 * foram encerradas ficam de fora.
 */
function acharRota(rotas, telefonesDaCorrida) {
  const alvo = new Set(telefonesDaCorrida.map(d8).filter(t => t.length >= 8));
  if (!alvo.size) return null;
  let melhor = null, melhorPontos = 0;
  for (const r of (rotas || [])) {
    if (['finalizada', 'cancelada'].includes(String(r.status || ''))) continue;
    let pontos = 0;
    for (const it of (r.itens || [])) {
      if (alvo.has(d8(it.tel))) pontos++;
    }
    if (pontos > melhorPontos) { melhor = r; melhorPontos = pontos; }
  }
  // uma coincidência isolada pode ser cliente com duas entregas em rotas
  // diferentes; a partir de duas, é a rota certa
  if (melhorPontos >= 2) return { rota: melhor, coincidencias: melhorPontos };
  if (melhorPontos === 1 && (melhor.itens || []).length === 1) {
    return { rota: melhor, coincidencias: 1 };   // rota de uma entrega só
  }
  return null;
}

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type,X-Lalamove-Signature');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const action = String((req.query || {}).action || '').trim();

  // ── consultas administrativas, protegidas por chave ──
  if (action) {
    const chave = (req.query && req.query.k) || req.headers['x-tf-key'] || '';
    if (chave !== ((process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim())) {
      return res.status(401).json({ ok: false, error: 'não autorizado' });
    }

    // 📥 o que chegou da plataforma, para conferir o que está sendo recebido
    if (action === 'recebidos') {
      const log = (await dbGet('lalamove_webhook_log')) || { eventos: [] };
      const hh = d => d ? new Date(new Date(d).getTime() - 3 * 3600000)
        .toISOString().slice(5, 16).replace('T', ' ') : '—';
      const evs = (log.eventos || []).slice(-60).reverse();
      return res.status(200).json({ ok: true,
        recebidos: (log.eventos || []).length,
        ULTIMOS: evs.map(e => hh(e.em) + ' | ' + (e.tipo || '?') +
          ' | pedido ' + String(e.orderId || '?').slice(-8) +
          ' | ' + (e.status || '') +
          (e.motorista ? ' | ' + e.motorista : '') +
          (e.rotaCasada ? ' | → ' + e.rotaCasada : ' | sem rota correspondente')),
        DETALHE: evs.slice(0, 15) });
    }

    // 🔗 corridas conhecidas e a rota de cada uma
    if (action === 'vinculos') {
      const v = (await dbGet('lalamove_vinculos')) || { pedidos: {} };
      return res.status(200).json({ ok: true,
        vinculados: Object.keys(v.pedidos || {}).length,
        L: Object.entries(v.pedidos || {}).map(([id, p]) =>
          String(id).slice(-8) + ' | ' + (p.rotaId || 'sem rota') +
          ' | ' + (p.status || '?') +
          (p.motorista ? ' | ' + p.motorista.nome : '') +
          (p.gravadoNaRota ? ' | ✅ gravado na rota' : '')) });
    }

    return res.status(404).json({ ok: false, error: 'ação não encontrada',
      disponiveis: ['recebidos', 'vinculos'] });
  }

  // ── recepção do webhook: a plataforma envia POST sem a nossa chave ──
  if (req.method !== 'POST') {
    return res.status(200).json({ ok: true,
      pronto: 'aponte o webhook da Lalamove para este endereço',
      metodo: 'POST' });
  }

  const corpo = req.body || {};
  const agora = new Date().toISOString();
  // 🗒️ tudo que chega é registrado antes de qualquer processamento: se o
  // formato mudar, o que foi recebido continua disponível para inspeção
  const log = (await dbGet('lalamove_webhook_log')) || { eventos: [] };

  try {
    const dados = corpo.data || corpo;
    const ordem = dados.order || dados;
    const orderId = String(ordem.orderId || ordem.id || corpo.orderId || '');
    const status = String(ordem.status || dados.status || corpo.eventType || '');
    const motoristaBruto = dados.driver || ordem.driver || null;

    const evento = { em: agora, tipo: corpo.eventType || corpo.type || 'atualização',
      orderId, status,
      motorista: motoristaBruto ? (motoristaBruto.name || motoristaBruto.driverId) : null,
      rotaCasada: null };

    if (!orderId) {
      log.eventos = (log.eventos || []).concat([{ ...evento, erro: 'sem identificador' }]).slice(-500);
      await dbSet('lalamove_webhook_log', log);
      return res.status(200).json({ ok: true, ignorado: 'evento sem identificador de pedido' });
    }

    const vinc = (await dbGet('lalamove_vinculos')) || { pedidos: {} };
    vinc.pedidos = vinc.pedidos || {};
    const ja = vinc.pedidos[orderId] || { marcos: {} };

    // telefones dos destinos, que é o que liga a corrida à rota
    const tels = [];
    for (const s of (ordem.stops || dados.stops || [])) {
      if (s && s.phone) tels.push(s.phone);
      if (s && s.recipient && s.recipient.phone) tels.push(s.recipient.phone);
    }
    for (const r of (ordem.recipients || dados.recipients || [])) {
      if (r && r.phone) tels.push(r.phone);
    }

    // procura a rota correspondente, se ainda não estiver vinculada
    let rotaId = ja.rotaId || null, coincidencias = ja.coincidencias || 0;
    if (!rotaId && tels.length) {
      const rdb = (await dbGet('reparoeletro_almox_rotas')) || { rotas: [] };
      const achou = acharRota(rdb.rotas, tels);
      if (achou) { rotaId = achou.rota.id; coincidencias = achou.coincidencias; }
    }
    evento.rotaCasada = rotaId;

    const motorista = motoristaBruto ? {
      id: motoristaBruto.driverId || motoristaBruto.id || null,
      nome: motoristaBruto.name || null,
      telefone: motoristaBruto.phone || null,
      placa: motoristaBruto.plateNumber || motoristaBruto.plate || null,
    } : (ja.motorista || null);

    const marcos = Object.assign({}, ja.marcos || {});
    if (!marcos.primeiroAvisoEm) marcos.primeiroAvisoEm = agora;
    if (motorista && motorista.id && !marcos.aceitaEm) marcos.aceitaEm = agora;
    // 🚚 SAIU DA LOJA: é neste estado que o equipamento deixa a loja com o
    // entregador, e portanto o momento certo de gravar quem levou
    const COLETADO = ['PICKED_UP', 'ON_GOING'];
    if (COLETADO.includes(status) && !marcos.coletadaEm) marcos.coletadaEm = agora;
    if (status === 'COMPLETED' && !marcos.concluidaEm) marcos.concluidaEm = agora;

    let gravadoAgora = false;
    // grava na rota assim que a corrida é coletada e o entregador é conhecido
    if (rotaId && marcos.coletadaEm && motorista && motorista.nome && !ja.gravadoNaRota) {
      const rdb2 = (await dbGet('reparoeletro_almox_rotas')) || { rotas: [] };
      const rt = (rdb2.rotas || []).find(r => r.id === rotaId);
      if (rt) {
        rt.motorista = motorista.nome;
        rt.placa = motorista.placa || rt.placa || '';
        rt.telMotorista = motorista.telefone || rt.telMotorista || '';
        rt.saidaEm = rt.saidaEm || marcos.coletadaEm;
        rt.motoristaOrigem = 'Lalamove · corrida ' + orderId.slice(-8);
        rt.lalamoveOrderId = orderId;
        if (String(rt.status || '') === 'separacao' || !rt.status) rt.status = 'em_rota';
        await dbSet('reparoeletro_almox_rotas', rdb2);
        // confirma que persistiu antes de dar por gravado
        const conf = await dbGet('reparoeletro_almox_rotas');
        const rc = (((conf || {}).rotas) || []).find(r => r.id === rotaId);
        gravadoAgora = !!(rc && rc.motorista === motorista.nome);
      }
    }

    vinc.pedidos[orderId] = { orderId, rotaId, coincidencias, status, motorista, marcos,
      gravadoNaRota: ja.gravadoNaRota || gravadoAgora,
      atualizadoEm: agora };
    // guarda 90 dias de vínculos
    const corte = Date.now() - 90 * 86400000;
    for (const [id, p] of Object.entries(vinc.pedidos)) {
      if (new Date((p.marcos || {}).primeiroAvisoEm || p.atualizadoEm || 0).getTime() < corte) {
        delete vinc.pedidos[id];
      }
    }
    await dbSet('lalamove_vinculos', vinc);

    log.eventos = (log.eventos || []).concat([evento]).slice(-500);
    await dbSet('lalamove_webhook_log', log);

    return res.status(200).json({ ok: true, orderId, status,
      rotaVinculada: rotaId, gravadoNaRota: gravadoAgora });
  } catch (e) {
    log.eventos = (log.eventos || []).concat([{ em: agora, erro: e.message,
      corpoRecebido: JSON.stringify(corpo).slice(0, 600) }]).slice(-500);
    await dbSet('lalamove_webhook_log', log);
    // 200 de propósito: erro nosso não deve fazer a plataforma reenviar sem parar
    return res.status(200).json({ ok: false, error: e.message });
  }
}
