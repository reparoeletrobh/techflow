// api/precificacao.js — SOMENTE LEITURA. Não escreve em nenhuma chave.
// Extrai o histórico de orçamentos por modelo para análise de precificação:
// quanto foi cobrado, quantos aprovaram, e a taxa de aprovação por faixa de preço.
//
// Uso:
//   /api/precificacao?action=modelos&k=CHAVE&curto=1
//   /api/precificacao?action=faixas&k=CHAVE&curto=1
//   /api/precificacao?action=modelos&k=CHAVE&curto=1&min=3   (só modelos com 3+ casos)

const U = (process.env.UPSTASH_URL || '').replace(/['"]/g, '').trim();
const T = (process.env.UPSTASH_TOKEN || '').replace(/['"]/g, '').trim();

async function dbGet(k) {
  try {
    const r = await fetch(U + '/pipeline', {
      method: 'POST',
      headers: { Authorization: 'Bearer ' + T, 'Content-Type': 'application/json' },
      body: JSON.stringify([['GET', k]]),
    });
    const j = await r.json();
    return j[0]?.result ? JSON.parse(j[0].result) : null;
  } catch (e) { return null; }
}

// Fases que significam "o cliente aprovou" (aprovados e tudo que vem depois)
const APROVOU = ['aprovados', 'video_enviado', 'analise_compra', 'equipamento_comprado',
  'programar_entrega', 'solicitar_entrega', 'entrega_solicitada', 'receber', 'erp',
  'garantia', 'finalizado'];
// Fases que significam "ainda negociando ou perdeu"
const NAO = ['aguardando_aprovacao', 'ultima_chamada', 'descarte'];

const faseDe = c => c.phaseId || c.phase || '';
const norm = s => String(s || '').trim().toUpperCase().replace(/\s+/g, ' ').slice(0, 40);

export default async function handler(req, res) {
  const q = req.query || {};
  const chave = String(q.k || '').trim();
  if (chave !== ((process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim())) {
    return res.status(401).json({ ok: false, error: 'chave invalida' });
  }
  const action = q.action || 'modelos';
  const curto = q.curto === '1';
  const min = parseInt(q.min || '1') || 1;

  const [pipeA, fl] = await Promise.all([
    dbGet('reparoeletro_pipe'),
    dbGet('reparoeletro_frenteloja'),
  ]);

  // ── Reunir os casos: card do pipe ADM + fichas do frente de loja ──
  const casos = [];
  for (const c of ((pipeA || {}).cards || [])) {
    const fase = faseDe(c);
    const valor = parseFloat(c.valor) || 0;
    if (!valor) continue;
    const decidiu = APROVOU.includes(fase) || NAO.includes(fase);
    if (!decidiu) continue;
    casos.push({
      equip: norm(c.equipamento) || '(sem equipamento)',
      modelo: norm(c.modelo),
      valor,
      aprovou: APROVOU.includes(fase),
      origem: 'pipe',
    });
  }
  for (const f of ((fl || {}).fichas || [])) {
    const valor = parseFloat(f.valorOrcamento) || 0;
    if (!valor) continue;
    const eq = (f.diagnosticoLoja && f.diagnosticoLoja.equips && f.diagnosticoLoja.equips[0]) || {};
    const fase = f.phase || '';
    // no FL, "conserto realizado"/pago = aprovou; orçamento parado = não
    const aprovou = /conserto|pago|finaliz|entreg|pronto/i.test(fase);
    casos.push({
      equip: norm(eq.tipo || f.equipamento) || '(sem equipamento)',
      modelo: norm(eq.modelo),
      valor,
      aprovou,
      origem: 'loja',
    });
  }

  // ── Agrupador ──
  function agrupar(chaveFn) {
    const m = {};
    for (const c of casos) {
      const k = chaveFn(c);
      if (k === null) continue;
      (m[k] = m[k] || { n: 0, ap: 0, soma: 0, min: Infinity, max: 0 });
      m[k].n++; m[k].soma += c.valor;
      if (c.aprovou) m[k].ap++;
      if (c.valor < m[k].min) m[k].min = c.valor;
      if (c.valor > m[k].max) m[k].max = c.valor;
    }
    return Object.entries(m).map(([k, v]) => ({
      chave: k, n: v.n, aprov: v.ap,
      taxa: v.n ? Math.round((v.ap / v.n) * 100) : 0,
      medio: Math.round(v.soma / v.n), min: v.min === Infinity ? 0 : v.min, max: v.max,
    })).sort((a, b) => b.n - a.n);
  }

  if (action === 'modelos') {
    const lista = agrupar(c => c.modelo ? (c.equip + ' | ' + c.modelo) : null).filter(x => x.n >= min);
    const semModelo = casos.filter(c => !c.modelo).length;
    if (curto) {
      const linhas = lista.slice(0, 60).map(x =>
        `${x.chave};n=${x.n};med=${x.medio};min=${x.min};max=${x.max};aprov=${x.taxa}%`);
      return res.status(200).send(
        `MODELOS (min=${min})\ntotal_casos=${casos.length} com_modelo=${casos.length - semModelo} sem_modelo=${semModelo}\n` +
        linhas.join('\n'));
    }
    return res.status(200).json({ ok: true, total: casos.length, semModelo, modelos: lista });
  }

  if (action === 'faixas') {
    // taxa de aprovação por faixa de preço, por equipamento — mostra onde o cliente trava
    const faixa = v => v < 300 ? '<300' : v < 350 ? '300-349' : v < 400 ? '350-399'
      : v < 450 ? '400-449' : v < 500 ? '450-499' : v < 600 ? '500-599'
      : v < 800 ? '600-799' : v < 1000 ? '800-999' : '1000+';
    const lista = agrupar(c => c.equip + ' | ' + faixa(c.valor));
    if (curto) {
      const linhas = lista.slice(0, 60).map(x => `${x.chave};n=${x.n};aprov=${x.taxa}%;med=${x.medio}`);
      return res.status(200).send(`FAIXAS\ntotal_casos=${casos.length}\n` + linhas.join('\n'));
    }
    return res.status(200).json({ ok: true, total: casos.length, faixas: lista });
  }

  return res.status(400).json({ ok: false, error: 'action invalida (modelos|faixas)' });
}
