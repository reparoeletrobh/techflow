// ═══════════════════════════════════════════════════════════════════
// REGISTRO DE ENTRADA EM "ENTRAR EM CONTATO"
//
// Por que existe: a ficha guardava apenas o status atual, sem memória de
// quantas vezes já tinha passado por essa coluna. Quando 48 apareceram de
// uma vez, não havia como saber se era acúmulo represado ou retorno em
// ciclo — a diferença entre um dia de trabalho e um defeito grave.
//
// Toda passagem passa a ficar registrada na própria ficha.
// ═══════════════════════════════════════════════════════════════════

/**
 * Marca a ficha como Entrar em Contato guardando o registro da passagem.
 * @param {object} ficha   a ficha (alterada no lugar)
 * @param {string} origem  o que motivou: 'régua', 'remarcar', 'bot', 'manual'...
 * @param {string} motivo  detalhe opcional
 */
function marcarEntrarContato(ficha, origem, motivo) {
  if (!ficha) return ficha;
  const agora = new Date().toISOString();
  const anterior = String(ficha.status || '');
  ficha.status = 'entrar_contato';
  ficha.entrarContatoEm = agora;
  ficha.entrarContatoOrigem = String(origem || 'não informada');
  if (motivo) ficha.entrarContatoMotivo = String(motivo).slice(0, 160);
  ficha.passagensEntrarContato = (ficha.passagensEntrarContato || []).concat([{
    em: agora,
    origem: String(origem || 'não informada'),
    veioDe: anterior || '(nova)',
    motivo: motivo ? String(motivo).slice(0, 120) : null,
  }]).slice(-20);
  ficha.vezesEmEntrarContato = ficha.passagensEntrarContato.length;
  return ficha;
}

/** Quantas vezes a ficha já passou por lá. */
function vezes(ficha) {
  return ((ficha || {}).passagensEntrarContato || []).length;
}

/** Resumo legível das passagens, para diagnóstico. */
function resumo(ficha) {
  const ps = (ficha || {}).passagensEntrarContato || [];
  if (!ps.length) return 'sem registro';
  return ps.length + 'ª entrada · ' + ps.map(p =>
    String(p.em).slice(5, 16).replace('T', ' ') + ' (' + p.origem + ')').join(' → ');
}

module.exports = { marcarEntrarContato, vezes, resumo };
