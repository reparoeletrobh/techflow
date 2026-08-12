// ═══════════════════════════════════════════════════════════════════
// SOCORRO — cópia de segurança dos bancos e restauração de emergência.
// Criado antes da refatoração da camada de gravação: se algo sair do
// controle, é por aqui que se volta ao estado anterior.
// ═══════════════════════════════════════════════════════════════════
const U = (process.env.UPSTASH_URL || '').replace(/['"]/g, '').trim();
const T = (process.env.UPSTASH_TOKEN || '').replace(/[\n\r'"]/g, '').trim();

async function bruto(k) {
  const r = await fetch(`${U}/get/${k}`, { headers: { Authorization: `Bearer ${T}` } }).then(x => x.json());
  return r && r.result ? r.result : null;   // string crua, sem interpretar
}
async function gravarBruto(k, valorString) {
  const r = await fetch(`${U}/set/${k}`, { method: 'POST',
    headers: { Authorization: `Bearer ${T}`, 'Content-Type': 'text/plain' },
    body: valorString });
  return (await r.json()).result === 'OK';
}

const CRITICOS = [
  'fichas_adm', 'fichas_tv', 'reparoeletro_logistica', 'tv_logistica',
  'reparoeletro_pipe', 'tv_pipe', 'reparoeletro_board', 'reparoeletro_frenteloja',
  'reparoeletro_garantia_v2', 'reparoeletro_garantia_fila', 'reparoeletro_qualidade',
  'prospeccao_adm', 'reparoeletro_balcao', 'reparoeletro_arquivo',
];

module.exports = async function handler(req, res) {
  const _k = (req.query && req.query.k) || req.headers['x-tf-key'] || '';
  if (_k !== ((process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim())) {
    return res.status(401).json({ ok: false, error: 'não autorizado' });
  }
  const { action } = req.query;

  // ── 💾 SALVAR: guarda uma cópia de cada banco crítico ──
  if (action === 'salvar') {
    const rotulo = String(req.query.rotulo || new Date(Date.now() - 3 * 3600000)
      .toISOString().slice(0, 16).replace(/[-:T]/g, '')).slice(0, 24);
    const feitos = [], erros = [];
    for (const k of CRITICOS) {
      try {
        const v = await bruto(k);
        if (!v) { erros.push(k + ': vazio'); continue; }
        const ok = await gravarBruto('bkp_' + rotulo + '_' + k, v);
        feitos.push(k + ' (' + (v.length / 1024).toFixed(0) + ' KB)' + (ok ? '' : ' ⚠️ falhou'));
      } catch (e) { erros.push(k + ': ' + e.message); }
      await new Promise(s => setTimeout(s, 80));
    }
    return res.status(200).json({ ok: erros.length === 0, rotulo,
      salvos: feitos.length, feitos, erros,
      comoRestaurar: '/api/socorro?action=restaurar&rotulo=' + rotulo + '&banco=NOME&aplicar=1' });
  }

  // ── 📋 LISTAR: que cópias existem ──
  if (action === 'listar') {
    try {
      const r = await fetch(`${U}/keys/bkp_*`, { headers: { Authorization: `Bearer ${T}` } })
        .then(x => x.json());
      const chaves = r.result || [];
      const porRotulo = {};
      for (const c of chaves) {
        const m = String(c).match(/^bkp_([^_]+)_(.+)$/);
        if (!m) continue;
        (porRotulo[m[1]] = porRotulo[m[1]] || []).push(m[2]);
      }
      return res.status(200).json({ ok: true, copias: Object.keys(porRotulo).length,
        POR_ROTULO: Object.entries(porRotulo).sort().reverse()
          .map(([r2, bs]) => r2 + ' → ' + bs.length + ' banco(s)') });
    } catch (e) { return res.status(200).json({ ok: false, error: e.message }); }
  }

  // ── ♻️ RESTAURAR: devolve um banco ao estado da cópia ──
  if (action === 'restaurar') {
    const rotulo = String(req.query.rotulo || '');
    const banco = String(req.query.banco || '');
    if (!rotulo || !banco) return res.status(400).json({ ok: false, error: 'informe rotulo e banco' });
    const copia = await bruto('bkp_' + rotulo + '_' + banco);
    if (!copia) return res.status(404).json({ ok: false, error: 'cópia não encontrada' });
    let qtd = 0;
    try { const o = JSON.parse(copia); qtd = (o.fichas || o.cards || o.inspecoes || o.itens || []).length; } catch (e) {}
    if (String(req.query.aplicar || '') !== '1') {
      return res.status(200).json({ ok: true, modo: 'prévia',
        banco, rotulo, tamanhoKB: (copia.length / 1024).toFixed(0), registrosNaCopia: qtd,
        atencao: 'restaurar SUBSTITUI o estado atual — tudo que mudou depois se perde',
        dica: 'para restaurar: &aplicar=1' });
    }
    // guarda o estado atual antes de sobrescrever
    try { const atual = await bruto(banco); if (atual) await gravarBruto('antes_restauro_' + banco, atual); } catch (e) {}
    const ok = await gravarBruto(banco, copia);
    return res.status(200).json({ ok, banco, rotulo, registros: qtd,
      observacao: 'o estado anterior à restauração ficou em antes_restauro_' + banco });
  }

  return res.status(400).json({ ok: false, acoes: ['salvar', 'listar', 'restaurar'],
    bancosProtegidos: CRITICOS.length });
};
