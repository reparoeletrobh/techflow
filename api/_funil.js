// ═══════════════════════════════════════════════════════════════════
// LIVRO-RAZÃO DO FUNIL — cada etapa registrada no instante em que acontece.
//
// Por que existe: até agora o KPI reconstruía o passado lendo o estado atual
// dos cards. Mas o estado muda — o card avança, é arquivado, perde histórico —
// e o que aconteceu ontem passa a ser deduzido em vez de lido. Cada dedução
// dessas gerou divergência. Aqui o evento é gravado uma vez, com a data do
// fato, e nunca mais é alterado.
// ═══════════════════════════════════════════════════════════════════
const U = (process.env.UPSTASH_URL || '').replace(/['"]/g, '').trim();
const T = (process.env.UPSTASH_TOKEN || '').replace(/[\n\r'"]/g, '').trim();

const LISTA = 'kpi_funil';   // lista append-only: nada é reescrito

/**
 * Registra um passo do funil.
 * @param {string} etapa   ficha | logistica | orcamento | aprovado
 * @param {object} dados   { telefone, nome, valor, frente, canal, quem, ref }
 *   frente: 'adm' | 'tv'
 *   canal:  'balcao' | 'online' | 'bot'
 */
async function registrar(etapa, dados = {}) {
  try {
    if (!U || !T) return false;
    const ev = {
      etapa: String(etapa),
      ts: new Date().toISOString(),
      tel: String(dados.telefone || '').replace(/\D/g, '').slice(-11),
      nome: String(dados.nome || '').slice(0, 40),
      valor: Number(dados.valor || 0) || 0,
      frente: String(dados.frente || 'adm'),
      canal: String(dados.canal || 'online'),
      quem: String(dados.quem || '').slice(0, 30),
      ref: String(dados.ref || '').slice(0, 40),
    };
    await fetch(`${U}/rpush/${LISTA}/${encodeURIComponent(JSON.stringify(ev))}`,
      { headers: { Authorization: `Bearer ${T}` } });
    return true;
  } catch (e) { return false; }
}

/** Lê os eventos de um intervalo. */
async function ler(iniMs, fimMs) {
  try {
    const r = await fetch(`${U}/lrange/${LISTA}/-20000/-1`,
      { headers: { Authorization: `Bearer ${T}` } }).then(x => x.json());
    const out = [];
    for (const s of (r.result || [])) {
      try {
        const e = JSON.parse(s);
        const t = new Date(e.ts || 0).getTime();
        if (!t) continue;
        if (iniMs && t < iniMs) continue;
        if (fimMs && t > fimMs) continue;
        out.push(e);
      } catch (x) {}
    }
    return out;
  } catch (e) { return []; }
}

/** Evita registrar o mesmo passo duas vezes para o mesmo cliente no mesmo dia. */
async function jaRegistrado(etapa, telefone, dia) {
  const d = String(telefone || '').replace(/\D/g, '').slice(-8);
  if (!d) return false;
  const evs = await ler(new Date(dia + 'T00:00:00-03:00').getTime(),
    new Date(dia + 'T23:59:59-03:00').getTime());
  return evs.some(e => e.etapa === etapa &&
    String(e.tel || '').slice(-8) === d);
}

module.exports = { registrar, ler, jaRegistrado, LISTA };
