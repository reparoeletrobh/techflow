// api/rastrear.js — Busca global com ficha completa e navegação precisa
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

// ── Fases do board OS ──────────────────────────────────────────
const BOARD_FASE = {
  aprovado:        { label: 'Aprovado',         cor: '#22c55e' },
  producao:        { label: 'Em Produção',       cor: '#3b9eff' },
  cliente_loja:    { label: 'Cliente Loja',      cor: '#f5c800' },
  urgencia:        { label: 'Urgência',          cor: '#ef4444' },
  comprar_peca:    { label: 'Comprar Peça',      cor: '#f97316' },
  aguardando_peca: { label: 'Aguardando Peça',   cor: '#3b9eff' },
  peca_disponivel: { label: 'Peça Disponível',   cor: '#22c55e' },
  loja_feito:      { label: 'Loja Feito',        cor: '#a855f7' },
  delivery_feito:  { label: 'Delivery Feito',    cor: '#a855f7' },
  aguardando_ret:  { label: 'Ag. Retirada',      cor: '#f97316' },
  rs_loja:         { label: 'RS Loja',           cor: '#f5c800' },
  rs_delivery:     { label: 'RS Delivery',       cor: '#f5c800' },
};
// Fases em compras-pecas → { url de destino, osFaseKey para osTab() }
const BOARD_COMPRAS = {
  comprar_peca:    { url: '/compras-pecas', osFaseKey: 'comprar',    elementPrefix: 'oscard-' },
  aguardando_peca: { url: '/compras-pecas', osFaseKey: 'aguardando', elementPrefix: 'oscard-' },
  peca_disponivel: { url: '/compras-pecas', osFaseKey: 'disponivel', elementPrefix: 'oscard-' },
};

// ── Fases da garantia ──────────────────────────────────────────
const GARANTIA_FASE = {
  producao:            { label: 'Em Produção',        cor: '#3b9eff' },
  conserto_concluido:  { label: 'Conserto Concluído', cor: '#22c55e' },
  teste_realizado:     { label: 'Teste Realizado',    cor: '#22c55e' },
  equip_retirado:      { label: 'Equip. Retirado',    cor: '#a855f7' },
  coleta_solicitada:   { label: 'Coleta Solicitada',  cor: '#f97316' },
  solicitar_entrega:   { label: 'Solicitar Entrega',  cor: '#f97316' },
  entrega_realizada:   { label: 'Entrega Realizada',  cor: '#a855f7' },
  garantia_solicitada: { label: 'Garantia Solicitada',cor: '#f5c800' },
  equip_recolhido:     { label: 'Equip. Recolhido',   cor: '#f5c800' },
  conserto_realizado:  { label: 'Conserto Realizado', cor: '#22c55e' },
};
const GARANTIA_TIPO = {
  loja_imediata:       { label: 'Loja Imediata',       cor: '#3b9eff' },
  loja_acompanhamento: { label: 'Loja Acompanhamento', cor: '#f5c800' },
  delivery:            { label: 'Delivery',            cor: '#f97316' },
  rua:                 { label: 'RS Rua',              cor: '#a855f7' },
};

// ── Fases de peças avulsas ─────────────────────────────────────
const PECA_FASE = {
  pendente:             { label: 'Pendente',       cor: '#f5c800', pecaTab: 'pendentes'  },
  aguardando_pagamento: { label: 'Ag. Pagamento',  cor: '#f97316', pecaTab: 'aguardando' },
  pago:                 { label: 'Pago',           cor: '#22c55e', pecaTab: 'pago'       },
  a_caminho:            { label: 'A Caminho',      cor: '#3b9eff', pecaTab: 'caminho'    },
  recebido:             { label: 'Recebido',       cor: '#a855f7', pecaTab: 'caminho'    },
};

function match(term, ...fields) {
  const t = term.toLowerCase();
  return fields.some(f => f && String(f).toLowerCase().includes(t));
}

function fmtPhone(tel) {
  if (!tel) return null;
  const d = String(tel).replace(/\D/g,'');
  if (d.length === 11) return `(${d.slice(0,2)}) ${d.slice(2,7)}-${d.slice(7)}`;
  if (d.length === 10) return `(${d.slice(0,2)}) ${d.slice(2,6)}-${d.slice(6)}`;
  return tel;
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
    const boardDB = await dbGet('reparoeletro_board');
    const cards   = boardDB?.cards || [];

    for (const c of cards) {
      if (!match(q, c.nomeContato, c.title, c.osCode, c.descricao, c.pipefyId)) continue;

      const fase      = BOARD_FASE[c.phaseId] || { label: c.phaseId, cor: '#5a5a7a' };
      const compras   = BOARD_COMPRAS[c.phaseId] || null;
      const isCompras = !!compras;

      results.push({
        tipo:       'os',
        id:         c.pipefyId,
        // Campos para buildCard
        label:      c.nomeContato || c.title || '—',
        sublabel:   c.osCode ? '#' + c.osCode : null,       // ref badge
        descricao:  c.descricao || c.title || null,          // texto descritivo
        telefone:   null,                                    // board não armazena
        urgente:    false,
        tipoCor:    null,
        fase:       fase.label,
        cor:        fase.cor,
        // Campos de navegação
        url:        isCompras ? compras.url : (c.phaseId === 'aguardando_ret' ? '/aguardando-retirada' : '/tecnico'),
        osFaseKey:  isCompras ? compras.osFaseKey : null,
        elementId:  isCompras ? (compras.elementPrefix + c.pipefyId) : ('card-' + c.pipefyId),
        pecaTab:    null,
        garantiaTipo: null,
      });
    }

    // ── 2. Garantia ───────────────────────────────────────────────
    const garDB  = await dbGet('reparoeletro_garantia_v2');
    const fichas = garDB?.fichas || [];

    for (const f of fichas) {
      if (!match(q, f.nome, f.telefone, f.defeito, f.endereco, f.id)) continue;

      const fase    = GARANTIA_FASE[f.faseId] || { label: f.faseId, cor: '#5a5a7a' };
      const tipo    = GARANTIA_TIPO[f.tipo]   || { label: f.tipo, cor: '#5a5a7a' };

      results.push({
        tipo:       'garantia',
        id:         f.id,
        // Campos para buildCard
        label:      f.nome,
        sublabel:   tipo.label,                    // tipo badge (Delivery, Loja, etc.)
        descricao:  f.defeito,
        telefone:   fmtPhone(f.telefone),
        urgente:    false,
        tipoCor:    tipo.cor,
        fase:       f.concluida ? 'Concluída' : fase.label,
        cor:        f.concluida ? '#a855f7' : fase.cor,
        // Campos de navegação
        url:        '/garantia',
        osFaseKey:  null,
        elementId:  'ficha-' + f.id,               // ID do card no garantia.html
        pecaTab:    null,
        garantiaTipo: f.tipo,                      // para setView() na garantia
      });
    }

    // ── 3. Compras de Peças ───────────────────────────────────────
    const pecDB = await dbGet('reparoeletro_compras_pecas');
    const pecas = pecDB?.pecas || [];

    for (const p of pecas) {
      if (!match(q, p.descricao, p.os, p.obs, p.fornecedor, p.id)) continue;

      const fase = PECA_FASE[p.status] || { label: p.status, cor: '#5a5a7a', pecaTab: 'pendentes' };

      // Previsão se a caminho
      let previsaoDesc = null;
      if (p.previsaoChegada) {
        const dias = Math.ceil((new Date(p.previsaoChegada) - new Date()) / 86400000);
        previsaoDesc = dias < 0 ? `Atrasada ${Math.abs(dias)}d` : dias === 0 ? 'Chega hoje' : `Chega em ${dias}d`;
      }

      const detalhe = [
        p.os       ? 'OS: ' + p.os : null,
        p.fornecedor || null,
        previsaoDesc || null,
      ].filter(Boolean).join(' · ');

      results.push({
        tipo:       'peca',
        id:         p.id,
        // Campos para buildCard
        label:      p.descricao,
        sublabel:   detalhe || null,                // ref + fornecedor + previsão
        descricao:  p.obs || null,
        telefone:   null,
        urgente:    p.urgente || false,
        tipoCor:    null,
        fase:       fase.label,
        cor:        fase.cor,
        // Campos de navegação
        url:        '/compras-pecas',
        osFaseKey:  null,
        elementId:  'ci-' + p.id,                  // ID do card em compras-pecas.html
        pecaTab:    fase.pecaTab,
        garantiaTipo: null,
      });
    }

    return res.status(200).json({ ok: true, results: results.slice(0, 60) });

  } catch(e) {
    return res.status(200).json({ ok: false, error: e.message, results: [] });
  }
};
