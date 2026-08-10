// ═══════════════════════════════════════════════════════════════════
// TRILHA — registro central de tudo que altera ou apaga dados
// Existe porque em 10/08 um botão apagou 59 equipamentos comprados e
// não havia como saber quem, quando, nem como desfazer.
// ═══════════════════════════════════════════════════════════════════
const U = (process.env.UPSTASH_URL || '').replace(/['"]/g, '').trim();
const T = (process.env.UPSTASH_TOKEN || '').replace(/[\n\r'"]/g, '').trim();

async function dbGet(k) {
  try {
    const r = await fetch(`${U}/get/${k}`, { headers: { Authorization: `Bearer ${T}` } }).then(x => x.json());
    return r && r.result ? JSON.parse(r.result) : null;
  } catch (e) { return null; }
}
async function dbSet(k, v) {
  try {
    await fetch(`${U}/set/${k}`, { method: 'POST',
      headers: { Authorization: `Bearer ${T}`, 'Content-Type': 'application/json' },
      body: JSON.stringify(v) });
    return true;
  } catch (e) { return false; }
}

// bancos que nunca podem sumir sem rastro
const CRITICOS = [
  'reparoeletro_pipe', 'tv_pipe', 'reparoeletro_logistica', 'tv_logistica',
  'reparoeletro_compra_equip', 'reparoeletro_almoxarifado', 'prospeccao_adm',
  'prospeccao_tv', 'fichas_adm', 'fichas_tv', 'reparoeletro_arquivo',
  'reparoeletro_orcamentos', 'reparoeletro_financeiro',
];
const LISTAS = ['fichas', 'cards', 'tarefas', 'itens', 'inspecoes'];

function contar(obj) {
  if (!obj) return { existe: false };
  const r = { existe: true };
  for (const L of LISTAS) {
    if (Array.isArray(obj[L])) {
      r[L] = obj[L].length;
      // contagem por status, que é onde os problemas aparecem
      const st = {};
      for (const x of obj[L]) {
        const s = String(x.status || x.phase || x.phaseId || '?');
        st[s] = (st[s] || 0) + 1;
      }
      r[L + '_status'] = st;
    }
  }
  return r;
}

module.exports = async function handler(req, res) {
  const _k = (req.query && req.query.k) || req.headers['x-tf-key'] || '';
  if (_k !== ((process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim())) {
    return res.status(401).json({ ok: false, error: 'não autorizado' });
  }
  res.setHeader('Access-Control-Allow-Origin', 'https://reparoeletroadm.com');
  const { action } = req.query;
  const hoje = new Date(Date.now() - 3 * 3600000).toISOString().slice(0, 10);

  // ── 📸 SNAPSHOT: fotografa o tamanho de cada banco ──
  if (action === 'snapshot') {
    const foto = { em: new Date().toISOString(), bancos: {} };
    for (const k of CRITICOS) foto.bancos[k] = contar(await dbGet(k));
    await dbSet('trilha_snap_' + hoje, foto);
    // guarda também o último, para comparação imediata
    await dbSet('trilha_snap_ultimo', foto);
    return res.status(200).json({ ok: true, em: foto.em,
      bancos: Object.entries(foto.bancos).map(([k, v]) =>
        k + ': ' + (v.existe ? LISTAS.filter(L => v[L] !== undefined).map(L => v[L] + ' ' + L).join(', ') : 'NÃO EXISTE')) });
  }

  // ── 🚨 COMPARAR: o que mudou desde o último snapshot ──
  if (action === 'comparar') {
    const dias = Math.min(30, Math.max(1, parseInt(req.query.dias || '1', 10)));
    const antes = await dbGet('trilha_snap_' +
      new Date(Date.now() - 3 * 3600000 - dias * 86400000).toISOString().slice(0, 10));
    if (!antes) return res.status(200).json({ ok: false,
      error: 'não há snapshot de ' + dias + ' dia(s) atrás para comparar' });
    const alertas = [], mudancas = [];
    for (const k of CRITICOS) {
      const ag = contar(await dbGet(k));
      const an = antes.bancos[k] || {};
      for (const L of LISTAS) {
        if (an[L] === undefined && ag[L] === undefined) continue;
        const a = an[L] || 0, b = ag[L] || 0;
        if (a === b) continue;
        const dif = b - a;
        const pct = a ? Math.round(Math.abs(dif) / a * 100) : 100;
        const linha = k + '.' + L + ': ' + a + ' → ' + b + ' (' + (dif > 0 ? '+' : '') + dif + ')';
        mudancas.push(linha);
        // queda de 20%+ ou de 10+ registros é sinal de perda
        if (dif < 0 && (pct >= 20 || Math.abs(dif) >= 10)) alertas.push('🚨 ' + linha + ' — queda de ' + pct + '%');
      }
      // mudanças por status, que revelam alteração em massa
      for (const L of LISTAS) {
        const stA = an[L + '_status'] || {}, stB = ag[L + '_status'] || {};
        for (const s of new Set([...Object.keys(stA), ...Object.keys(stB)])) {
          const a = stA[s] || 0, b = stB[s] || 0;
          if (a === b) continue;
          if (a >= 10 && b === 0) alertas.push('🚨 ' + k + '.' + L + ' status "' + s + '": ' + a + ' → 0 — SUMIU TUDO');
        }
      }
    }
    return res.status(200).json({ ok: alertas.length === 0,
      comparandoCom: antes.em,
      ALERTAS: alertas.length ? alertas : '✅ nenhuma perda detectada',
      todasAsMudancas: mudancas });
  }

  // ── 📜 REGISTRAR: grava uma ação destrutiva na trilha ──
  if (req.method === 'POST' && action === 'registrar') {
    const { oque, quem, banco, antes, depois, detalhe } = req.body || {};
    const chave = 'trilha_log_' + hoje;
    const log = (await dbGet(chave)) || { itens: [] };
    log.itens.unshift({ ts: new Date().toISOString(), oque, quem: quem || 'não identificado',
      banco, antes, depois, detalhe: String(detalhe || '').slice(0, 300) });
    log.itens = log.itens.slice(0, 500);
    await dbSet(chave, log);
    return res.status(200).json({ ok: true });
  }

  // ── 📖 HISTORICO: o que foi feito nos últimos dias ──
  if (action === 'historico') {
    const dias = Math.min(30, Math.max(1, parseInt(req.query.dias || '7', 10)));
    const itens = [];
    for (let i = 0; i < dias; i++) {
      const d = new Date(Date.now() - 3 * 3600000 - i * 86400000).toISOString().slice(0, 10);
      const l = await dbGet('trilha_log_' + d);
      for (const x of ((l && l.itens) || [])) itens.push(x);
    }
    return res.status(200).json({ ok: true, total: itens.length,
      L: itens.slice(0, 80).map(x =>
        new Date(new Date(x.ts).getTime() - 3 * 3600000).toISOString().slice(5, 16).replace('T', ' ') +
        ' | ' + x.oque + ' | ' + x.banco + ' | ' + x.antes + '→' + x.depois +
        (x.detalhe ? ' | ' + x.detalhe.slice(0, 50) : '')) });
  }

  // ── 💾 LIXEIRAS: o que está guardado e pode ser restaurado ──
  if (action === 'lixeiras') {
    const CHAVES = ['reparoeletro_compra_equip_lixeira', 'reparoeletro_pipe_archive',
      'wa_coleta_pendente', 'prospeccao_excluidos'];
    const r = [];
    for (const k of CHAVES) {
      const d = await dbGet(k);
      if (!d) { r.push(k + ' → vazia'); continue; }
      let n = 0;
      for (const L of LISTAS) if (Array.isArray(d[L])) n += d[L].length;
      if (d.itens && typeof d.itens === 'object') n += Object.keys(d.itens).length;
      r.push(k + ' → ' + n + ' registro(s)' + (d.em ? ' · ' + String(d.em).slice(0, 16).replace('T', ' ') : ''));
    }
    return res.status(200).json({ ok: true, LIXEIRAS: r });
  }

  return res.status(400).json({ ok: false,
    acoes: ['snapshot', 'comparar', 'registrar', 'historico', 'lixeiras'] });
};
