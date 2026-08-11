// ═══════════════════════════════════════════════════════════════════
// VIGIA — nenhuma ficha pode sair de uma coluna sem chegar em outra.
// Existe porque em 11/08 a Maria 1499 e o Kaio 8225 saíram do Remarcar
// e não apareceram no atendimento: a coluna foi liberada, a gravação da
// ficha nova não persistiu, e ninguém soube até o cliente ser esquecido.
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
const d8 = t => String(t || '').replace(/\D/g, '').slice(-8);
const hh = d => d ? new Date(new Date(d).getTime() - 3 * 3600000).toISOString().slice(5, 16).replace('T', ' ') : '?';

// onde uma ficha pode legitimamente estar
const BANCOS = [
  ['fichas_adm', 'fichas'], ['fichas_tv', 'fichas'],
  ['reparoeletro_logistica', 'fichas'], ['tv_logistica', 'fichas'],
  ['prospeccao_adm', 'fichas'], ['prospeccao_tv', 'fichas'],
  ['reparoeletro_pipe', 'cards'], ['tv_pipe', 'cards'],
  ['reparoeletro_arquivo', 'fichas'], ['tv_arquivo', 'fichas'],
  ['reparoeletro_garantia_v2', 'fichas'], ['reparoeletro_board', 'cards'],
];

async function ondeEstaCadaTelefone() {
  const mapa = {};
  for (const [k, L] of BANCOS) {
    try {
      const b = await dbGet(k);
      for (const x of ((b || {})[L] || [])) {
        const t = d8(x.telefone);
        if (t.length < 8) continue;
        (mapa[t] = mapa[t] || []).push({
          banco: k, fase: String(x.status || x.phase || x.phaseId || x.faseId || '?'),
          origem: x.origem || null, id: x.id,
        });
      }
    } catch (e) {}
  }
  return mapa;
}

module.exports = async function handler(req, res) {
  const _k = (req.query && req.query.k) || req.headers['x-tf-key'] || '';
  if (_k !== ((process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim())) {
    return res.status(401).json({ ok: false, error: 'não autorizado' });
  }
  res.setHeader('Access-Control-Allow-Origin', 'https://reparoeletroadm.com');
  const { action } = req.query;

  // ── 🚨 VARRER: procura fichas em estado inconsistente ──
  if (action === 'varrer' || !action) {
    const horas = Math.min(168, Math.max(1, parseInt(req.query.horas || '48', 10)));
    const corte = Date.now() - horas * 3600000;
    const mapa = await ondeEstaCadaTelefone();
    const alertas = [];

    // 1) marcada como devolvida do remarcar, mas sem ficha no atendimento
    for (const [k, L] of [['reparoeletro_logistica', 'adm'], ['tv_logistica', 'tv']]) {
      const b = await dbGet(k);
      for (const f of ((b || {}).fichas || [])) {
        const q = new Date(f.enviadoProspeccaoEm || f.remarcadoEm || 0).getTime();
        if (!q || q < corte) continue;
        const onde = mapa[d8(f.telefone)] || [];
        const chegou = onde.some(o => (o.banco === 'fichas_adm' || o.banco === 'fichas_tv') &&
          (o.origem === 'remarcar' || String(o.id || '').startsWith('rem_') ||
           String(o.id || '').startsWith('fic_reag_') || String(o.id || '').startsWith('rec_')));
        if (!chegou) alertas.push({ tipo: 'REMARCAR_SEM_DESTINO', sis: L,
          nome: f.nome, tel: d8(f.telefone).slice(-4), quando: hh(f.enviadoProspeccaoEm || f.remarcadoEm),
          detalhe: 'saiu da coluna Remarcar e não há ficha no atendimento',
          // 🔍 mostra onde o telefone aparece, para saber se é perda real ou falha de leitura
          ondeAparece: onde.map(o => o.banco + ':' + o.fase +
            (o.origem ? '(' + o.origem + ')' : '') + '[' + String(o.id || '').slice(0, 10) + ']'),
          telefoneNaLogistica: String(f.telefone || ''),
          motivo: f.motivoRemarcar || '(sem motivo)' });
      }
    }

    // 2) ficha em fase de saída sem nenhum registro em outro banco
    for (const [k, L] of [['reparoeletro_logistica', 'fichas'], ['tv_logistica', 'fichas']]) {
      const b = await dbGet(k);
      for (const f of ((b || {})[L] || [])) {
        if (String(f.phase || '') !== 'prospeccao') continue;
        const q = new Date(f.enviadoProspeccaoEm || f.movedAt || 0).getTime();
        if (!q || q < corte) continue;
        const onde = (mapa[d8(f.telefone)] || []).filter(o => o.banco !== k);
        if (!onde.length) alertas.push({ tipo: 'SEM_DESTINO', sis: k,
          nome: f.nome, tel: d8(f.telefone).slice(-4), quando: hh(f.enviadoProspeccaoEm || f.movedAt),
          detalhe: 'marcada como enviada mas não existe em nenhum outro banco' });
      }
    }

    // 3) ficha criada hoje sem telefone válido — nunca poderá ser contatada
    for (const [k, L] of [['fichas_adm', 'fichas'], ['fichas_tv', 'fichas']]) {
      const b = await dbGet(k);
      for (const f of ((b || {})[L] || [])) {
        const q = new Date(f.criadoEm || f.registradoEm || 0).getTime();
        if (!q || q < corte) continue;
        if (d8(f.telefone).length >= 8) continue;
        alertas.push({ tipo: 'SEM_TELEFONE', sis: k, nome: f.nome,
          tel: String(f.telefone || '(vazio)'), quando: hh(f.criadoEm),
          detalhe: 'ficha sem telefone válido — impossível contatar' });
      }
    }

    // 4) ficha de garantia sem tipo — não renderiza em coluna nenhuma
    try {
      const g = await dbGet('reparoeletro_garantia_v2');
      for (const f of ((g || {}).fichas || [])) {
        if (f.tipo) continue;
        alertas.push({ tipo: 'GARANTIA_SEM_TIPO', sis: 'garantia', nome: f.nome,
          tel: d8(f.telefone).slice(-4), quando: hh(f.criadaEm || f.criadoEm),
          detalhe: 'garantia sem tipo — invisível em todas as colunas' });
      }
    } catch (e) {}

    const porTipo = alertas.reduce((o, a) => { o[a.tipo] = (o[a.tipo] || 0) + 1; return o; }, {});
    // guarda o histórico para acompanhar a evolução
    try {
      const dia = new Date(Date.now() - 3 * 3600000).toISOString().slice(0, 10);
      const hist = (await dbGet('vigia_historico')) || { dias: {} };
      hist.dias[dia] = { em: new Date().toISOString(), total: alertas.length, porTipo };
      await dbSet('vigia_historico', hist);
    } catch (e) {}

    return res.status(200).json({ ok: alertas.length === 0,
      janelaHoras: horas,
      totalDeProblemas: alertas.length,
      POR_TIPO: porTipo,
      SITUACAO: alertas.length === 0
        ? '✅ nenhuma ficha perdida — todas as saídas têm destino'
        : '🚨 ' + alertas.length + ' ficha(s) em estado inconsistente',
      ALERTAS: alertas.map(a => a.tipo + ' | ' + a.sis + ' | ' +
        String(a.nome || '?').slice(0, 20) + ' ' + a.tel + ' | ' + a.quando +
        ' | ' + a.detalhe +
        (a.ondeAparece ? ' → ONDE: ' + (a.ondeAparece.join(' | ') || 'em lugar nenhum') : '') +
        (a.telefoneNaLogistica ? ' | tel: ' + a.telefoneNaLogistica : '')),
      comoCorrigir: {
        REMARCAR_SEM_DESTINO: '/api/logistica?action=recriar-perdidas&dia=HOJE&aplicar=1',
        GARANTIA_SEM_TIPO: 'atribuir o tipo na tela de garantia',
        SEM_TELEFONE: 'corrigir o telefone na ficha',
      } });
  }

  // ── 🧪 TESTE-GRAVACAO: o banco aceita escrita? qual o tamanho? ──
  if (action === 'teste-gravacao') {
    const alvo = String(req.query.banco || 'fichas_adm');
    const antes = await dbGet(alvo);
    const tamanho = JSON.stringify(antes || {}).length;
    const quantas = ((antes || {}).fichas || (antes || {}).cards || []).length;
    if (String(req.query.aplicar || '') !== '1') {
      return res.status(200).json({ ok: true, modo: 'só leitura',
        banco: alvo,
        registros: quantas,
        tamanhoBytes: tamanho,
        tamanhoMB: (tamanho / 1048576).toFixed(2),
        limiteUpstash: '1 MB por chave no plano gratuito · 100 MB no pago',
        alerta: tamanho > 900000 ? '🚨 PERTO DO LIMITE — gravações podem falhar em silêncio' :
                tamanho > 500000 ? '⚠️ banco grande, vale acompanhar' : '✅ tamanho tranquilo',
        dica: 'para testar uma gravação real: &aplicar=1' });
    }
    // grava um marcador e confere se persistiu
    const db = antes || { fichas: [] };
    const lista = db.fichas ? 'fichas' : (db.cards ? 'cards' : 'fichas');
    db[lista] = db[lista] || [];
    const marca = 'teste_vigia_' + Date.now().toString(36);
    db[lista].unshift({ id: marca, nome: 'TESTE DO VIGIA', telefone: '00000000000',
      status: 'teste', criadoEm: new Date().toISOString() });
    const gravou = await dbSet(alvo, db);
    await new Promise(s => setTimeout(s, 400));
    const depois = await dbGet(alvo);
    const achou = ((depois || {})[lista] || []).some(x => x.id === marca);
    // limpa o marcador
    if (achou) {
      const limpo = await dbGet(alvo);
      limpo[lista] = (limpo[lista] || []).filter(x => x.id !== marca);
      await dbSet(alvo, limpo);
    }
    return res.status(200).json({ ok: achou,
      banco: alvo, registros: quantas,
      tamanhoMB: (tamanho / 1048576).toFixed(2),
      dbSetRetornou: gravou,
      persistiu: achou,
      VEREDITO: achou
        ? '✅ o banco aceita gravação normalmente'
        : '🚨 A GRAVAÇÃO NÃO PERSISTIU — é aqui que as fichas se perdem' });
  }

  // ── 📈 HISTORICO: evolução dos problemas ao longo dos dias ──
  if (action === 'historico') {
    const h = (await dbGet('vigia_historico')) || { dias: {} };
    return res.status(200).json({ ok: true,
      L: Object.entries(h.dias).sort((a, b) => b[0].localeCompare(a[0])).slice(0, 30)
        .map(([d, v]) => d + ' | ' + v.total + ' problema(s) | ' + JSON.stringify(v.porTipo)) });
  }

  return res.status(400).json({ ok: false, acoes: ['varrer', 'historico'] });
};
