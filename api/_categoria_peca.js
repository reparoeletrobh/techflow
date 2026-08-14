// ═══════════════════════════════════════════════════════════════════
// CATEGORIA DA PEÇA
//
// A peça é cadastrada com descrição livre — "magnetron LG", "MAGNETRON",
// "magnetrom 220v" são a mesma coisa escrita de três formas. Sem agrupar,
// não dá para saber em que tipo de componente o dinheiro está indo.
//
// A classificação é por palavra reconhecida na descrição. Quando nenhuma
// bate, a peça fica em "outros" — e essa lista é o que mostra quais
// termos precisam entrar no dicionário.
// ═══════════════════════════════════════════════════════════════════

const CATEGORIAS = [
  ['magnetron',      /magnetron|magnetrom|magnetro\b/i],
  ['placa',          /placa|pci\b|circuito|inverter|main\s*board|módulo eletr|modulo eletr/i],
  ['compressor',     /compressor|motor.*compress/i],
  ['display',        /display|tela|painel de vidro|lcd|led\s*(panel|barra)|backlight|barra de led/i],
  ['fonte',          /\bfonte\b|power\s*supply|transformador/i],
  ['sensor',         /sensor|term[oó]stato|termistor|ntc\b/i],
  ['resistencia',    /resist[êe]ncia|resistor de aquec/i],
  ['motor',          /\bmotor\b|ventilador|ventoinha|turbina/i],
  ['bomba',          /bomba|pressuriz/i],
  ['filtro',         /filtro|refil|elemento filtrante|vela\b/i],
  ['valvula',        /v[áa]lvula|solenoide|solen[óo]ide|registro/i],
  ['porta',          /porta|trava|dobradi[çc]a|fecho|ma[çc]aneta|puxador|borracha de veda|gaxeta/i],
  ['prato_giratorio',/prato|girat[óo]rio|acoplador|roldana|anel girat/i],
  ['gas',            /\bg[áa]s\b|r134|r600|carga de g[áa]s|fluido refrig/i],
  ['cabo',           /cabo|fia[çc][ãa]o|chicote|conector|plug\b|tomada/i],
  ['capacitor',      /capacitor|condensador el[ée]tr/i],
  ['controle',       /controle remoto|controle\b/i],
  ['bandeja',        /bandeja|gaveta|prateleira|suporte de vidro/i],
  ['torneira',       /torneira|bica|gatilho|acionador/i],
  ['teclado',        /teclado|membrana|painel de comando|bot[ãa]o/i],
  ['lampada',        /l[âa]mpada|luz interna/i],
  ['rele',           /rel[êe]|contator|protetor t[ée]rmico/i],
  ['ferramenta',     /ferramenta|alicate|chave de fenda|multímetro|multimetro|solda|estanho/i],
  ['insumo',         /parafuso|abra[çc]adeira|fita|cola|silicone|graxa|[óo]leo|limpeza|[áa]lcool|pasta t[ée]rmica/i],
];

/** Devolve a categoria da peça a partir da descrição. */
function categoriaDe(peca) {
  if (peca && peca.categoriaManual) return String(peca.categoriaManual);
  const txt = String((peca && (peca.descricao || peca.nome)) || '');
  if (!txt.trim()) return 'outros';
  for (const [nome, re] of CATEGORIAS) if (re.test(txt)) return nome;
  return 'outros';
}

/** Nome legível para exibição. */
const ROTULOS = {
  magnetron: 'Magnetron', placa: 'Placa eletrônica', compressor: 'Compressor',
  display: 'Display / tela', fonte: 'Fonte', sensor: 'Sensor / termostato',
  resistencia: 'Resistência', motor: 'Motor / ventilador', bomba: 'Bomba',
  filtro: 'Filtro / refil', valvula: 'Válvula', porta: 'Porta / vedação',
  prato_giratorio: 'Prato giratório', gas: 'Gás refrigerante', cabo: 'Cabo / conector',
  capacitor: 'Capacitor', controle: 'Controle remoto', bandeja: 'Bandeja / prateleira',
  torneira: 'Torneira', teclado: 'Teclado / painel', lampada: 'Lâmpada',
  rele: 'Relé', ferramenta: 'Ferramenta', insumo: 'Insumo', outros: 'Outros',
};
function rotulo(cat) { return ROTULOS[cat] || cat; }

module.exports = { categoriaDe, rotulo, CATEGORIAS, ROTULOS };
