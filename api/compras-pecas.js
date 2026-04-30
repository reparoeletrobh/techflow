// compras-pecas.js — Gestão de Compra de Peças
const UPSTASH_URL   = process.env.UPSTASH_URL;
const UPSTASH_TOKEN = process.env.UPSTASH_TOKEN;
const KEY           = "reparoeletro_compras_pecas";

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

  // ── POST confirmar-pagamento ───────────────────────────────────
  if (req.method === "POST" && action === "confirmar-pagamento") {
    const { ids } = req.body || {};
    if (!ids?.length) return res.status(400).json({ ok:false, error:"ids obrigatórios" });
    for (const id of ids) {
      const p = db.pecas.find(x => x.id === id);
      if (p) { p.status = "pago"; p.pagoEm = new Date().toISOString(); }
    }
    await dbSet(KEY, db);
    return res.status(200).json({ ok:true });
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

    if(action==="limpar-tv-indevidos"){const antes=db.pecas.length;db.pecas=db.pecas.filter(p=>p.origem!=="tv_aprovado");const removidas=antes-db.pecas.length;if(removidas>0)await dbSet(KEY,db);return res.status(200).json({ok:true,removidas,total:db.pecas.length});}

  return res.status(404).json({ ok:false, error:"Ação não encontrada" });
};
