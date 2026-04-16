// api/rastrear.js — Busca global em todo o sistema Reparo Eletro
const UPSTASH_URL   = (process.env.UPSTASH_URL   || '').replace(/['"]/g,'').trim();
const UPSTASH_TOKEN = (process.env.UPSTASH_TOKEN || '').replace(/['"]/g,'').trim();

async function dbGet(key) {
  try {
    const r = await fetch(`${UPSTASH_URL}/pipeline`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${UPSTASH_TOKEN}`, 'Content-Type': 'application/json' },
      body: JSON.stringify([['GET', key]])
    });
    const j = await r.json();
    if (!j[0] || !j[0].result) return null;
    let p = JSON.parse(j[0].result);
    if (typeof p === 'string') p = JSON.parse(p);
    return (p && typeof p === 'object') ? p : null;
  } catch(e) { return null; }
}

// ── Fase labels para garantia ──
const GARANTIA_FASE = {
  producao:           { label: 'Em Produção',        cor: '#f5c800' },
  conserto_concluido: { label: 'Conserto Concluído',  cor: '#22c55e' },
  teste_realizado:    { label: 'Teste Realizado',     cor: '#22c55e' },
  equip_retirado:     { label: 'Equip. Retirado',     cor: '#a855f7' },
  coleta_solicitada:  { label: 'Coleta Solicitada',   cor: '#3b9eff' },
  solicitar_entrega:  { label: 'Solicitar Entrega',   cor: '#f97316' },
  entrega_realizada:  { label: 'Entrega Realizada',   cor: '#a855f7' },
  garantia_solicitada:{ label: 'Garantia Solicitada', cor: '#f97316' },
  equip_recolhido:    { label: 'Equip. Recolhido',    cor: '#f5c800' },
  conserto_realizado: { label: 'Conserto Realizado',  cor: '#22c55e' },
};
const GARANTIA_TIPO = {
  loja_imediata:      'Loja Imediata',
  loja_acompanhamento:'Loja Acompanhamento',
  delivery:           'Delivery',
  rua:                'RS Rua',
};

// ── Fase labels para OS/board ──
const BOARD_FASE = {
  orcamento:          { label: 'Orçamento',           cor: '#f5c800', url: '/tecnico' },
  aguardando_aprovacao:{ label: 'Ag. Aprovação',      cor: '#f97316', url: '/tecnico' },
  aprovado:           { label: 'Aprovado',            cor: '#22c55e', url: '/tecnico' },
  em_producao:        { label: 'Em Produção',         cor: '#3b9eff', url: '/tecnico' },
  comprar_peca:       { label: 'Comprar Peça',        cor: '#f97316', url: '/compras-pecas' },
  aguardando_peca:    { label: 'Aguardando Peça',     cor: '#3b9eff', url: '/compras-pecas' },
  peca_disponivel:    { label: 'Peça Disponível',     cor: '#22c55e', url: '/compras-pecas' },
  pronto:             { label: 'Pronto',              cor: '#22c55e', url: '/tecnico' },
  finalizado:         { label: 'Finalizado',          cor: '#a855f7', url: '/tecnico' },
  cancelado:          { label: 'Cancelado',           cor: '#ef4444', url: '/tecnico' },
};

// ── Fase labels para peças avulsas ──
const PECA_FASE = {
  pendente:             { label: 'Pendente',         cor: '#f5c800', url: '/compras-pecas' },
  aguardando_pagamento: { label: 'Ag. Pagamento',    cor: '#f97316', url: '/compras-pecas' },
  pago:                 { label: 'Pago',             cor: '#22c55e', url: '/compras-pecas' },
  a_caminho:            { label: 'A Caminho',        cor: '#3b9eff', url: '/compras-pecas' },
  recebido:             { label: 'Recebido',         cor: '#a855f7', url: '/compras-pecas' },
};

function match(term, ...fields) {
  const t = term.toLowerCase();
  return fields.some(f => f && String(f).toLowerCase().includes(t));
}

function faseInfo(map, key, fallback) {
  return map[key] || { label: fallback || key, cor: '#5a5a7a' };
}

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const q = (req.query.q || '').trim();
  if (!q || q.length < 2) return res.status(200).json({ ok: true, results: [] });

  try {
    const results = [];

    // ── 1. OS / Board ─────────────────────────────────────────────
    const board = await dbGet('reparoeletro_board');
    const cards = board?.cards || [];
    for (const c of cards) {
      if (match(q, c.nomeContato, c.title, c.osCode, c.descricao, c.telefone, c.pipefyId)) {
        const fase = faseInfo(BOARD_FASE, c.phaseId, c.phaseId);
        results.push({
          tipo:     'os',
          id:       c.pipefyId || c.id,
          label:    c.nomeContato || c.title || '—',
          sublabel: [c.osCode ? '#' + c.osCode : null, c.descricao].filter(Boolean).join(' · '),
          fase:     fase.label,
          cor:      fase.cor,
          url:      fase.url || '/tecnico',
          updatedAt: c.movedAt || c.createdAt || null,
        });
      }
    }

    // ── 2. Garantia ───────────────────────────────────────────────
    const garDB = await dbGet('reparoeletro_garantia_v2');
    const fichas = garDB?.fichas || [];
    for (const f of fichas) {
      if (match(q, f.nome, f.telefone, f.defeito, f.endereco, f.id)) {
        const fase = faseInfo(GARANTIA_FASE, f.faseId, f.faseId);
        const tipoLabel = GARANTIA_TIPO[f.tipo] || f.tipo;
        results.push({
          tipo:     'garantia',
          id:       f.id,
          label:    f.nome,
          sublabel: [tipoLabel, f.defeito].filter(Boolean).join(' · '),
          fase:     f.concluida ? 'Concluída' : fase.label,
          cor:      f.concluida ? '#a855f7' : fase.cor,
          url:      '/garantia',
          updatedAt: f.movidaEm || f.criadaEm || null,
        });
      }
    }

    // ── 3. Compras de Peças ───────────────────────────────────────
    const pecDB = await dbGet('reparoeletro_compras_pecas');
    const pecas = pecDB?.pecas || [];
    for (const p of pecas) {
      if (match(q, p.descricao, p.os, p.obs, p.fornecedor, p.id)) {
        const fase = faseInfo(PECA_FASE, p.status, p.status);
        results.push({
          tipo:     'peca',
          id:       p.id,
          label:    p.descricao,
          sublabel: [p.os ? 'OS: ' + p.os : null, p.fornecedor].filter(Boolean).join(' · '),
          fase:     fase.label,
          cor:      fase.cor,
          url:      '/compras-pecas',
          updatedAt: p.compradoEm || p.createdAt || null,
        });
      }
    }

    // Ordena por data decrescente (mais recente primeiro)
    results.sort((a, b) => {
      if (!a.updatedAt) return 1;
      if (!b.updatedAt) return -1;
      return new Date(b.updatedAt) - new Date(a.updatedAt);
    });

    return res.status(200).json({ ok: true, results: results.slice(0, 40) });

  } catch(e) {
    return res.status(200).json({ ok: false, error: e.message, results: [] });
  }
};
