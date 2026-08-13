// ═══════════════════════════════════════════════════════════════════
// LIVRO-RAZÃO DO FUNIL — cada etapa registrada no instante em que acontece.
//
// Por que existe: até agora o KPI reconstruía o passado lendo o estado atual
// dos cards. Mas o estado muda — o card avança, é arquivado, perde histórico —
// e o que aconteceu ontem passa a ser deduzido em vez de lido. Cada dedução
// dessas gerou divergência. Aqui o evento é gravado uma vez, com a data do
// fato, e nunca mais é alterado.
// ═══════════════════════════════════════════════════════════════════
// lidos a cada chamada: em teste as variáveis chegam depois do carregamento
const url = () => (process.env.UPSTASH_URL || '').replace(/['"]/g, '').trim();
const tok = () => (process.env.UPSTASH_TOKEN || '').replace(/[\n\r'"]/g, '').trim();

const LISTA = 'kpi_funil';   // lista append-only: nada é reescrito

/**
 * Registra um passo do funil.
 * @param {string} etapa   ficha | logistica | orcamento | aprovado
 * @param {object} dados   { telefone, nome, valor, frente, canal, quem, ref }
 *   frente: 'adm' | 'tv'
 *   canal:  'balcao' | 'online' | 'bot'
 */
/**
 * Registra um passo do funil.
 * Ignora repetição do mesmo passo, para o mesmo cliente, com o mesmo valor,
 * dentro de uma janela curta: dois cards do mesmo cliente ou um duplo clique
 * gravavam duas linhas e inflavam a contagem do dia.
 */
async function registrar(etapa, dados = {}) {
  try {
    if (!url() || !tok()) return false;
    // 🔁 trava contra registro repetido: 10 minutos
    const chaveT = String(dados.telefone || '').replace(/\D/g, '').slice(-8);
    if (chaveT) {
      // 🔑 o equipamento entra na chave: cliente que deixa dois aparelhos gera
      // dois registros legítimos, e sem isso o segundo era descartado como
      // repetição. O que se quer evitar é o MESMO fato gravado duas vezes.
      const eq = String(dados.ref || dados.equipamento || '')
        .toLowerCase().replace(/[^a-z0-9]/g, '').slice(0, 14);
      const trava = 'funil_trava_' + etapa + '_' + chaveT + '_' +
        Math.round(Number(dados.valor || 0)) + (eq ? '_' + eq : '');
      try {
        const r = await fetch(`${url()}/set/${trava}/1?NX=true&EX=600`,
          { headers: { Authorization: `Bearer ${tok()}` } }).then(x => x.json());
        // se a chave já existia, o banco não grava e devolve null: é repetição
        if (!r || r.result === null) return false;
      } catch (e) { /* sem a trava, segue e grava */ }
    }
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
      equipamento: String(dados.equipamento || '').slice(0, 40),
    };
    await fetch(`${url()}/rpush/${LISTA}/${encodeURIComponent(JSON.stringify(ev))}`,
      { headers: { Authorization: `Bearer ${tok()}` } });
    // ✂️ mantém os últimos 60 mil eventos: com o volume atual cobre meses, e
    // impede que a lista cresça sem limite até comprometer o banco
    try {
      if (Math.random() < 0.02) {          // poda esporádica, não a cada gravação
        await fetch(`${url()}/ltrim/${LISTA}/-60000/-1`,
          { headers: { Authorization: `Bearer ${tok()}` } });
      }
    } catch (e) {}
    return true;
  } catch (e) { return false; }
}

/** Lê os eventos de um intervalo. */
async function ler(iniMs, fimMs) {
  try {
    const r = await fetch(`${url()}/lrange/${LISTA}/-60000/-1`,
      { headers: { Authorization: `Bearer ${tok()}` } }).then(x => x.json());
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
