// compras-pecas.js — Gestão de Compra de Peças
const UPSTASH_URL   = process.env.UPSTASH_URL;
const UPSTASH_TOKEN = process.env.UPSTASH_TOKEN;
const KEY           = "reparoeletro_compras_pecas";
const TV_KEY = "reparoeletro_tv_compras";

async function dbGet(key) {
  const r = await fetch(`${UPSTASH_URL}/pipeline`, {
    method:"POST", headers:{Authorization:`Bearer ${UPSTASH_TOKEN}`,"Content-Type":"application/json"},
    body: JSON.stringify([["GET", key]])
  });
  const j = await r.json();
  return j[0]?.result ? JSON.parse(j[0].result) : null;
}
async function dbSet(key, val) {
  await fetch(`${UPSTASH_URL}/pipeline`, {
    method:"POST", headers:{Authorization:`Bearer ${UPSTASH_TOKEN}`,"Content-Type":"application/json"},
    body: JSON.stringify([["SET", key, JSON.stringify(val)]])
  });
}

function defaultDB() { return { pecas: [] }; }
function uid() { return Date.now().toString(36) + Math.random().toString(36).slice(2,6); }

module.exports = async (req, res) => {
  // 🔐 TF-AUTH (Fase 1): chave obrigatória em toda chamada
  const _tfk = (req.query && req.query.k) || req.headers['x-tf-key'] || '';
  if (_tfk !== ((process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim())) {
    return res.status(401).json({ ok: false, error: 'não autorizado' });
  }

  res.setHeader("Access-Control-Allow-Origin","*");
  res.setHeader("Access-Control-Allow-Methods","GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers","Content-Type");
  if (req.method === "OPTIONS") return res.status(200).end();

  const action = req.query.action || "";
  let db = await dbGet(KEY) || defaultDB();
  if (!Array.isArray(db.pecas)) db.pecas = [];

  // ── GET load ──────────────────────────────────────────────────
  if (action === "load") {
    return res.status(200).json({ ok:true, pecas: db.pecas });
  }

  // ── POST cadastrar — nova peça para comprar ───────────────────
  if (req.method === "POST" && action === "cadastrar") {
    const { descricao, os, quantidade, urgente, obs } = req.body || {};
    if (!descricao) return res.status(400).json({ ok:false, error:"descricao obrigatória" });
    const peca = {
      id: uid(), descricao, os: os||"", quantidade: parseInt(quantidade)||1,
      urgente: !!urgente, obs: obs||"",
      status: "pendente", // pendente → aguardando_pagamento → pago
      createdAt: new Date().toISOString(),
      fornecedor: null, tipoCompra: null, dadosPagamento: null,
      previsaoChegada: null, compradoEm: null, pagoEm: null,
      grupoId: null
    };
    db.pecas.unshift(peca);
    await dbSet(KEY, db);
    return res.status(200).json({ ok:true, peca });
  }

  // ── POST comprar — marca peças como compradas (um ou mais) ────
  if (req.method === "POST" && action === "comprar") {
    const { ids, fornecedor, tipoCompra, dadosPagamento, previsoes, valor, descricaoGrupo } = req.body || {};
    if (!ids?.length) return res.status(400).json({ ok:false, error:"ids obrigatórios" });
    const grupoId = ids.length > 1 ? uid() : null;
    const now = new Date().toISOString();
    for (const id of ids) {
      const p = db.pecas.find(x => x.id === id);
      if (!p) continue;
      p.status        = "aguardando_pagamento";
      p.fornecedor    = fornecedor || "";
      p.valor         = valor || null;
      p.descricaoGrupo= descricaoGrupo || "";
      p.tipoCompra    = tipoCompra || "loja"; // loja | online
      p.dadosPagamento= dadosPagamento || "";
      p.compradoEm    = now;
      p.grupoId       = grupoId;
      if (tipoCompra === "online" && previsoes) {
        p.previsaoChegada = previsoes[id] || null;
      }
    }
    await dbSet(KEY, db);

    // ── Auto-registro DRE: lança peca_cmv automaticamente ────────
    if (valor && parseFloat(valor) > 0) {
      try {
        const DRE_KEY = 'reparo_fin_despesas';
        const dreRaw  = await dbGet(DRE_KEY);
        const dreDesps = Array.isArray(dreRaw) ? dreRaw : [];
        const hoje = now.slice(0, 10);
        // Monta descrição: usa descricaoGrupo || fornecedor || nomes das peças
        const nomePecas = db.pecas
          .filter(p => ids.includes(p.id))
          .map(p => p.descricao).slice(0, 3).join(', ');
        const descDRE = (descricaoGrupo || fornecedor || nomePecas || 'Compra de Peças').slice(0, 40);
        dreDesps.push({
          id:             uid(),
          descricao:      descDRE,
          valor:          parseFloat(valor),
          categoria:      'peca_cmv',
          status:         'pago',
          data:           hoje,
          dataVencimento: hoje,
          fornecedor:     fornecedor || '',
          autoCompra:     true,       // flag: criado automaticamente
          comprasIds:     ids,        // referência às peças compradas
          grupoId:        grupoId,
          tipoCompra:     tipoCompra || 'loja',
        });
        await dbSet(DRE_KEY, dreDesps);
      } catch (dreErr) {
        // Não deixa falhar a compra por erro do DRE
        console.error('[DRE auto-registro] Erro:', dreErr.message);
      }
    }
    // ─────────────────────────────────────────────────────────────

    return res.status(200).json({ ok:true, grupoId });
  }

  // ── 💰 GET painel-ciclo: quanto já se gastou na semana comercial ──
  // O ciclo vai de sábado 13h a sábado 13h, o mesmo do comercial, para que a
  // compra de peças possa ser lida junto com o que a semana produziu.
  if (action === "painel-ciclo") {
    const TETO = Math.max(0, Number(req.query.teto || 4000));
    // início do ciclo: último sábado às 13h de Brasília
    const agoraBR = new Date(Date.now() - 3 * 3600000);
    const diaSem = agoraBR.getUTCDay();          // 0=dom … 6=sáb
    const hora = agoraBR.getUTCHours();
    let voltar = (diaSem - 6 + 7) % 7;           // dias desde o último sábado
    if (diaSem === 6 && hora < 13) voltar = 7;   // sábado antes das 13h: ciclo anterior
    const ini = new Date(agoraBR.getTime() - voltar * 86400000);
    ini.setUTCHours(13, 0, 0, 0);
    const iniMs = ini.getTime() + 3 * 3600000;   // de volta para UTC real
    const fimMs = iniMs + 7 * 86400000;

    const pecas = (db.pecas || []);
    const noCiclo = pecas.filter(p => {
      if (p.status !== 'pago' || !p.pagoEm) return false;
      const t = new Date(p.pagoEm).getTime();
      return t >= iniMs && t < fimMs;
    });
    const gasto = noCiclo.reduce((s, p) =>
      s + Number(p.valor || p.preco || p.custo || 0), 0);
    const aguardando = pecas.filter(p => p.status === 'aguardando_pagamento');
    const aguardandoValor = aguardando.reduce((s, p) =>
      s + Number(p.valor || p.preco || p.custo || 0), 0);

    // por dia, para ver onde o dinheiro saiu
    const porDia = {};
    for (const p of noCiclo) {
      const d = new Date(new Date(p.pagoEm).getTime() - 3 * 3600000).toISOString().slice(0, 10);
      porDia[d] = (porDia[d] || 0) + Number(p.valor || p.preco || p.custo || 0);
    }
    const hh = d => new Date(d).toISOString().slice(0, 16).replace('T', ' ');
    return res.status(200).json({ ok: true,
      cicloComecaEm: hh(iniMs) + ' BRT', cicloTerminaEm: hh(fimMs) + ' BRT',
      teto: TETO,
      gasto: +gasto.toFixed(2),
      restante: +(TETO - gasto).toFixed(2),
      percentual: TETO ? Math.round(gasto / TETO * 100) : 0,
      compras: noCiclo.length,
      AGUARDANDO_PAGAMENTO: { quantas: aguardando.length, valor: +aguardandoValor.toFixed(2),
        seTudoForPago: +(gasto + aguardandoValor).toFixed(2),
        estouraria: (gasto + aguardandoValor) > TETO },
      POR_DIA: Object.entries(porDia).sort()
        .map(([d, v]) => d + ' · R$ ' + v.toFixed(2)),
      MAIORES: noCiclo
        .sort((a, b) => Number(b.valor || b.preco || 0) - Number(a.valor || a.preco || 0))
        .slice(0, 12)
        .map(p => 'R$ ' + Number(p.valor || p.preco || p.custo || 0).toFixed(2).padStart(9) +
          ' | ' + String(p.nome || p.descricao || '?').slice(0, 32) +
          ' | ' + hh(p.pagoEm)) });
  }

  // ── POST confirmar-pagamento ───────────────────────────────────
  if (req.method === "POST" && action === "confirmar-pagamento") {
    const { ids } = req.body || {};
    if (!ids?.length) return res.status(400).json({ ok:false, error:"ids obrigatórios" });
    const agoraPg = new Date().toISOString();
    let somaPg = 0;
    const pagas = [];
    for (const id of ids) {
      const p = db.pecas.find(x => x.id === id);
      if (!p) continue;
      // 💰 não soma de novo o que já estava pago: clicar duas vezes contaria
      // o mesmo valor duas vezes no teto da semana
      if (p.status === 'pago' && p.pagoEm) { pagas.push({ ja: true, nome: p.nome }); continue; }
      p.status = "pago";
      p.pagoEm = agoraPg;
      const v = Number(p.valor || p.preco || p.custo || 0);
      somaPg += v;
      pagas.push({ nome: p.nome || p.descricao || '?', valor: v });
    }
    await dbSet(KEY, db);
    return res.status(200).json({ ok:true, confirmadas: pagas.length, valorConfirmado: +somaPg.toFixed(2) });
  }

  // ── POST previsao — atualiza previsão de chegada de peça online ─
  if (req.method === "POST" && action === "previsao") {
    const { id, previsaoChegada } = req.body || {};
    const p = db.pecas.find(x => x.id === id);
    if (!p) return res.status(404).json({ ok:false, error:"não encontrada" });
    p.previsaoChegada = previsaoChegada;
    await dbSet(KEY, db);
    return res.status(200).json({ ok:true });
  }

  // ── DELETE deletar ─────────────────────────────────────────────
  if (req.method === "POST" && action === "deletar") {
    const { id } = req.body || {};
    db.pecas = db.pecas.filter(p => p.id !== id);
    await dbSet(KEY, db);
    return res.status(200).json({ ok:true });
  }

  // ── POST marcar-caminho — marca peças como "a caminho" ────────
  if (req.method === "POST" && action === "marcar-caminho") {
    const { ids, previsoes } = req.body || {};
    if (!ids?.length) return res.status(400).json({ ok:false, error:"ids obrigatórios" });
    const now = new Date().toISOString();
    for (const id of ids) {
      const p = db.pecas.find(x => x.id === id);
      if (!p) continue;
      p.status = "a_caminho";
      p.aCaminhoEm = now;
      if (previsoes && previsoes[id]) p.previsaoChegada = previsoes[id];
    }
    await dbSet(KEY, db);
    return res.status(200).json({ ok:true });
  }

  // ── POST marcar-recebido — marca peças como recebidas ─────────
  if (req.method === "POST" && action === "marcar-recebido") {
    const { ids } = req.body || {};
    if (!ids?.length) return res.status(400).json({ ok:false, error:"ids obrigatórios" });
    for (const id of ids) {
      const p = db.pecas.find(x => x.id === id);
      if (p) { p.status = "recebido"; p.recebidoEm = new Date().toISOString(); }
    }
    await dbSet(KEY, db);
    return res.status(200).json({ ok:true });
  }

    if (req.method === "POST" && action === "purge-recebidos") {
    const antes = db.pecas.length;
    db.pecas = db.pecas.filter(p => p.status !== "recebido");
    const removidas = antes - db.pecas.length;
    if (removidas > 0) await dbSet(KEY, db);
    return res.status(200).json({ ok: true, removidas, restantes: db.pecas.length });
  }

  if(action==="limpar-tv-indevidos"){const antes=db.pecas.length;db.pecas=db.pecas.filter(p=>p.origem!=="tv_aprovado");const removidas=antes-db.pecas.length;if(removidas>0)await dbSet(KEY,db);return res.status(200).json({ok:true,removidas,total:db.pecas.length});}

  // ── TV actions — chave separada, não mistura com ADM ─────────
  if (action === "tv-load") {
    const tvDb = (await dbGet(TV_KEY)) || { pecas: [] };
    return res.status(200).json({ ok: true, pecas: tvDb.pecas || [] });
  }
  if (req.method === "POST" && action === "tv-atualizar") {
    const { id, status } = req.body || {};
    const tvDb = (await dbGet(TV_KEY)) || { pecas: [] };
    const p = (tvDb.pecas || []).find(x => x.id === id);
    if (!p) return res.status(404).json({ ok: false, error: "Não encontrado" });
    if (status) p.status = status;
    await dbSet(TV_KEY, tvDb);
    return res.status(200).json({ ok: true });
  }

    return res.status(404).json({ ok:false, error:"Ação não encontrada" });
};
