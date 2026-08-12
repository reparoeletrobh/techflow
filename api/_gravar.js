// ═══════════════════════════════════════════════════════════════════
// GRAVAÇÃO SEGURA — uma função para todo o sistema.
//
// O problema que resolve: em todo lugar o código faz "lê tudo → modifica →
// grava tudo de volta". Quando duas rotinas fazem isso ao mesmo tempo, a
// segunda grava por cima do trabalho da primeira e o registro desaparece
// sem erro nenhum. Foi assim que fichas do remarcar sumiram, que inspeções
// do controle de qualidade se perderam e que fichas ficaram travadas.
//
// Como resolve: a modificação é feita numa função que recebe o estado mais
// recente, e o resultado é conferido depois de gravar. Se não persistiu,
// tenta de novo lendo o estado atualizado. Nunca insere duas vezes, porque
// quem decide o que mudar é sempre a função, sobre o dado recém-lido.
// ═══════════════════════════════════════════════════════════════════
// 🔌 lidos a cada chamada, não no carregamento: em teste as variáveis são
// definidas depois que o módulo já foi importado
const url = () => (process.env.UPSTASH_URL || '').replace(/['"]/g, '').trim();
const tok = () => (process.env.UPSTASH_TOKEN || '').replace(/[\n\r'"]/g, '').trim();

async function ler(chave) {
  const r = await fetch(`${url()}/get/${chave}`, { headers: { Authorization: `Bearer ${tok()}` } })
    .then(x => x.json());
  return r && r.result ? (typeof r.result === 'string' ? JSON.parse(r.result) : r.result) : null;
}
async function escrever(chave, valor) {
  const r = await fetch(`${url()}/set/${chave}`, { method: 'POST',
    headers: { Authorization: `Bearer ${tok()}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(valor) }).then(x => x.json());
  return r && r.result === 'OK';
}

/**
 * Altera um banco com segurança contra gravação concorrente.
 *
 * @param {string}   chave     nome do banco
 * @param {function} mudar     recebe o estado atual, devolve o novo (ou null para desistir)
 * @param {function} conferir  recebe o estado relido, devolve true se a mudança está lá
 * @param {object}   opcoes    { tentativas, esperaMs, padrao }
 * @returns {object}  { ok, tentativas, motivo, estado }
 */
async function alterar(chave, mudar, conferir, opcoes = {}) {
  const maxTent = Math.max(1, opcoes.tentativas || 3);
  const espera = opcoes.esperaMs || 250;
  const padrao = opcoes.padrao !== undefined ? opcoes.padrao : { fichas: [] };
  let ultimoMotivo = 'não tentou';

  for (let tent = 1; tent <= maxTent; tent++) {
    let atual;
    try { atual = (await ler(chave)) || JSON.parse(JSON.stringify(padrao)); }
    catch (e) { ultimoMotivo = 'falha ao ler: ' + e.message; await pausa(espera * tent); continue; }

    // 🔍 antes de mexer: a mudança já está aplicada? (outra rotina pode ter feito)
    if (conferir) {
      try { if (conferir(atual)) return { ok: true, tentativas: tent, motivo: 'já estava aplicado', estado: atual }; }
      catch (e) {}
    }

    let novo;
    try { novo = mudar(atual); }
    catch (e) { return { ok: false, tentativas: tent, motivo: 'erro na alteração: ' + e.message }; }
    if (novo === null || novo === undefined) {
      return { ok: true, tentativas: tent, motivo: 'nada a alterar', estado: atual };
    }

    let gravou = false;
    try { gravou = await escrever(chave, novo); }
    catch (e) { ultimoMotivo = 'falha ao gravar: ' + e.message; }

    if (!gravou) { ultimoMotivo = 'o banco recusou a gravação'; await pausa(espera * tent); continue; }

    // ✅ confere lendo de novo — é aqui que a sobreposição é detectada
    if (!conferir) return { ok: true, tentativas: tent, motivo: 'gravado', estado: novo };
    await pausa(120);
    let relido;
    try { relido = await ler(chave); }
    catch (e) { return { ok: true, tentativas: tent, motivo: 'gravado, sem confirmar', estado: novo }; }
    try {
      if (conferir(relido)) return { ok: true, tentativas: tent, motivo: 'gravado e confirmado', estado: relido };
    } catch (e) {}
    ultimoMotivo = 'a gravação não persistiu — outra rotina gravou por cima';
    await pausa(espera * tent);
  }
  return { ok: false, tentativas: maxTent, motivo: ultimoMotivo };
}

const pausa = ms => new Promise(s => setTimeout(s, ms));

/** Acrescenta um item a uma lista, sem duplicar. */
async function acrescentar(chave, lista, item, ehIgual, opcoes = {}) {
  return alterar(chave,
    (db) => {
      db[lista] = db[lista] || [];
      if (db[lista].some(x => ehIgual(x, item))) return null;   // já existe: não faz nada
      db[lista].unshift(item);
      return db;
    },
    (db) => Array.isArray((db || {})[lista]) && db[lista].some(x => ehIgual(x, item)),
    { ...opcoes, padrao: opcoes.padrao || { [lista]: [] } });
}

module.exports = { alterar, acrescentar, ler, escrever };
