// ═══════════════════════════════════════════════════════════════════
// TRIAGEM DE TV — decide se vale a pena coletar
// PRIORIDADE: modelo na lista de alto interesse
// NÃO COLETAR: tela lavada em marca que não seja LG ou TCL
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

// modelos de alto interesse — a lista cresce com o tempo
const PRIORIDADE_PADRAO = [
  'UN40J5200','UN32J5200','UN43J5200','UN40J5290','UN43J5290','UN32J5290',
  'UN43T5300','UN32T5300','UN49J5200','UN49J5290','UN49K5300','UN40K5300',
  'UN43N5200','UN32J4300','UN32N4300','UN32T4300','UN32M4500','UN43AU7700',
  'UN55AU7700','UN55RU7100','UN50BU8000','UN43BU8000','UN55BU8000','UN49NU7100',
  'UN49MU6100','UN50MU6100','UN50MU6300','UN55NU7100','UN50AU7700','UN50RU7100','UN50NU7100',
  '43LK5700','43LM6300','43UM7300','43UN7310','43UP7500','50UN7310','50UN8000',
  '50UP7500','50UP7750','50UQ8050','50UQ8000','55UN7310','55UP7750','65UP7750',
  '55UN8000','65UN8000','55UJ6525','55UK6520','65UK6520','49UJ6525','49UJ6300',
  '32LM625','32LM630','32LK615',
  '50P615','43P615','55P615','50P635','43P635','55P635','50P725','55P725',
  '43S6500','32S6500','50C725',
  'PTV50G70','PTV43G52','PTV55G52','PTV50F60','PTV50F30','PTV43E10','PTV58G70',
  'PTV32G70','PTV43VA4REGB','PTV50VA4REGB','PTV55VA4REGB','PTV65G70','PTV65F90',
  '43PFG6917','50PUG6654','50PUG7406','55PUG7406','58PUG6654',
  'TB005','TB007','TB008',
  '43SK8300','50SK8300','55SK8300',
];
// marcas que valem a pena em tela lavada (T-CON acessível)
const MARCAS_TELA_LAVADA = ['lg', 'tcl'];

function normaliza(s) {
  return String(s || '').toUpperCase().replace(/[\s\-_.]/g, '');
}
// ── como o CLIENTE descreve tela lavada (backlight aceso, sem imagem) ──
// Cuidado com dois defeitos que parecem iguais e NÃO são:
//  • LED/backlight queimado → tela ESCURA, mas dá para ver a imagem com lanterna. É reparável.
//  • Luz VERMELHA acesa e não liga → é o LED de standby, problema de fonte. Não é tela lavada.
function ehTelaLavada(txt) {
  const s = String(txt || '').toLowerCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '');   // tira acentos

  // 🚫 exclusões: descrições que parecem mas são outro defeito
  const ehLedQueimado = /lanterna|celular.{0,20}(ver|enxerg)|no escuro.{0,15}(ver|da pra)|imagem fraca|imagem escura mas|so ve.{0,12}(perto|escuro)|sombra da imagem|da pra ver a imagem/.test(s);
  if (ehLedQueimado) return false;
  const ehStandby = /luz vermelha|led vermelho|luzinha vermelha/.test(s) && !/tela (branca|clara|lavada|acesa)/.test(s);
  if (ehStandby) return false;

  // ✅ o jeito técnico
  if (/tela\s*lavada|backlight\s*(aceso|ligado|funcionando)|luz de fundo\s*(acesa|ligada|funcionando)|t-?con/.test(s)) return true;

  // ✅ como o cliente leigo descreve
  const PADROES = [
    // tela clara/branca/acesa sem imagem
    /tela\s*(fica\s*)?(branca|clara|esbranquicada|leitosa|acinzentada|cinza)/,
    /tela\s*(acesa|iluminada|clareia|clara).{0,25}sem imagem/,
    // acende / liga mas não mostra nada
    /(acende|liga|ligar|ligou).{0,30}(mas|porem|so que|;)?.{0,20}(nao|sem)\s*(da|de|aparece|mostra|tem|vem|forma|surge)?\s*(imagem|nada|figura|video|desenho|tela)/,
    /(acende|liga).{0,25}(imagem|tela).{0,15}nao (aparece|vem|forma)/,
    /lig(a|ou|ando).{0,40}tela\s*(fica\s*)?(preta|escura|apagada|em branco|branca|apagad)/,
    /tela\s*(fica|ficou)\s*(em branco|preta|apagada)/,
    // som sem imagem
    /(tem|sai|ouve|escuta|funciona).{0,15}(som|audio|barulho|voz).{0,30}(mas|porem)?.{0,20}(nao|sem)\s*(tem|da|aparece|vem)?\s*(imagem|video|figura)/,
    /som (normal|ok|funcionando).{0,25}(sem|nao).{0,10}imagem/,
    // formulações diretas
    /(nao|sem)\s*(da|de|aparece|mostra|tem|forma)\s*imagem/,
    /imagem\s*(nao|nunca)\s*(aparece|vem|forma|surge)/,
    /so\s*(a\s*)?(luz|claridade|iluminacao)/,
    /(fica|ficou)\s*(so\s*)?(a\s*)?(luz|clarinha|claro|iluminada)/,
    /apagou a imagem|sumiu a imagem|perdeu a imagem|imagem sumiu/,
    /aparece (a )?(marca|logo|logotipo).{0,20}(depois|e (ai|entao))?.{0,15}(some|apaga|escurece)/,
  ];
  return PADROES.some(re => re.test(s));
}
function marcaDe(txt) {
  const s = String(txt || '').toLowerCase();
  for (const m of ['lg', 'tcl', 'samsung', 'philco', 'philips', 'toshiba', 'semp',
    'aoc', 'panasonic', 'sony', 'britânia', 'britania', 'multilaser', 'hq', 'sanyo']) {
    if (new RegExp('\\b' + m + '\\b').test(s)) return m;
  }
  return null;
}

// ── a decisão ──
async function classificar(texto) {
  const cfg = (await dbGet('tv_triagem_config')) || {};
  const lista = Array.isArray(cfg.prioridade) && cfg.prioridade.length ? cfg.prioridade : PRIORIDADE_PADRAO;
  const naoColetar = Array.isArray(cfg.naoColetar) ? cfg.naoColetar : [];
  const marcasTL = Array.isArray(cfg.marcasTelaLavada) && cfg.marcasTelaLavada.length
    ? cfg.marcasTelaLavada : MARCAS_TELA_LAVADA;

  const t = normaliza(texto);
  const marca = marcaDe(texto);
  const lavada = ehTelaLavada(texto);

  // 1) lista explícita de não coletar
  const bloqueado = naoColetar.find(m => m && t.includes(normaliza(m)));
  if (bloqueado) {
    return { selo: 'NAO_COLETAR', motivo: 'modelo ' + bloqueado + ' está na lista de não coletar', marca, lavada };
  }
  // 2) tela lavada só em LG ou TCL
  if (lavada) {
    if (!marca) {
      return { selo: 'ATENCAO', motivo: 'tela lavada sem marca identificada — confirmar antes de coletar', marca, lavada };
    }
    if (!marcasTL.includes(marca)) {
      return { selo: 'NAO_COLETAR',
        motivo: 'tela lavada em ' + marca.toUpperCase() + ' — só coletamos ' + marcasTL.map(x => x.toUpperCase()).join(' ou '),
        marca, lavada };
    }
  }
  // 3) modelo de alto interesse
  const achado = lista.find(m => m && t.includes(normaliza(m)));
  if (achado) {
    return { selo: 'PRIORIDADE', motivo: 'modelo ' + achado + ' é de alto interesse', marca, lavada, modelo: achado };
  }
  return { selo: null, motivo: null, marca, lavada };
}

module.exports = async function handler(req, res) {
  const _k = (req.query && req.query.k) || req.headers['x-tf-key'] || '';
  if (_k !== ((process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim())) {
    return res.status(401).json({ ok: false, error: 'não autorizado' });
  }
  res.setHeader('Access-Control-Allow-Origin', 'https://reparoeletroadm.com');
  const { action } = req.query;

  // ── testar um texto ──
  if (action === 'classificar') {
    const txt = String(req.query.texto || '');
    if (!txt) return res.status(400).json({ ok: false, error: 'informe ?texto=' });
    return res.status(200).json({ ok: true, texto: txt, ...(await classificar(txt)) });
  }

  // ── ver e editar as listas ──
  if (action === 'listas') {
    const cfg = (await dbGet('tv_triagem_config')) || {};
    return res.status(200).json({ ok: true,
      prioridade: (cfg.prioridade && cfg.prioridade.length) ? cfg.prioridade : PRIORIDADE_PADRAO,
      naoColetar: cfg.naoColetar || [],
      marcasTelaLavada: cfg.marcasTelaLavada || MARCAS_TELA_LAVADA,
      total: ((cfg.prioridade && cfg.prioridade.length) ? cfg.prioridade : PRIORIDADE_PADRAO).length });
  }
  if (req.method === 'POST' && action === 'add-prioridade') {
    const { modelos } = req.body || {};
    const novos = Array.isArray(modelos) ? modelos : String(modelos || '').split(/[,\n;]/);
    const cfg = (await dbGet('tv_triagem_config')) || {};
    const atual = (cfg.prioridade && cfg.prioridade.length) ? cfg.prioridade : PRIORIDADE_PADRAO.slice();
    let add = 0;
    for (const m of novos.map(x => String(x).trim()).filter(Boolean)) {
      if (!atual.some(x => normaliza(x) === normaliza(m))) { atual.push(m); add++; }
    }
    cfg.prioridade = atual;
    await dbSet('tv_triagem_config', cfg);
    return res.status(200).json({ ok: true, adicionados: add, total: atual.length });
  }
  if (req.method === 'POST' && action === 'add-nao-coletar') {
    const { modelos } = req.body || {};
    const novos = Array.isArray(modelos) ? modelos : String(modelos || '').split(/[,\n;]/);
    const cfg = (await dbGet('tv_triagem_config')) || {};
    cfg.naoColetar = cfg.naoColetar || [];
    let add = 0;
    for (const m of novos.map(x => String(x).trim()).filter(Boolean)) {
      if (!cfg.naoColetar.some(x => normaliza(x) === normaliza(m))) { cfg.naoColetar.push(m); add++; }
    }
    await dbSet('tv_triagem_config', cfg);
    return res.status(200).json({ ok: true, adicionados: add, total: cfg.naoColetar.length });
  }

  // ── aplicar em massa: marca as fichas TV existentes ──
  if (action === 'aplicar-em-massa') {
    const BANCOS = [['tv_logistica', 'fichas'], ['fichas_tv', 'fichas'],
      ['prospeccao_tv', 'fichas'], ['prospeccao_adm', 'fichas'], ['tv_pipe', 'cards']];
    const resumo = { PRIORIDADE: 0, NAO_COLETAR: 0, ATENCAO: 0, semSelo: 0 };
    const exemplos = [];
    for (const [chave, lista] of BANCOS) {
      const db = await dbGet(chave);
      if (!db || !Array.isArray(db[lista])) continue;
      let mudou = 0;
      for (const f of db[lista]) {
        const txt = [f.equipamento, f.descricao, f.defeito, f.modelo].filter(Boolean).join(' ');
        if (!txt) continue;
        // só TV
        if (!/\btv\b|televis|polegada|\bpol\b|["”]/i.test(txt) && chave.indexOf('tv') < 0) continue;
        const c = await classificar(txt);
        if (c.selo !== (f.seloTriagem || null)) mudou++;
        f.seloTriagem = c.selo;
        f.motivoTriagem = c.motivo;
        resumo[c.selo || 'semSelo']++;
        if (c.selo && exemplos.length < 20) {
          exemplos.push(c.selo + ' | ' + String(f.nome || f.nomeContato || '?').slice(0, 16) +
            ' | ' + String(txt).slice(0, 30) + ' | ' + c.motivo);
        }
      }
      if (String(req.query.aplicar || '') === '1' && mudou) await dbSet(chave, db);
    }
    return res.status(200).json({ ok: true,
      modo: String(req.query.aplicar || '') === '1' ? 'aplicado' : 'prévia',
      RESUMO: resumo, exemplos });
  }

  return res.status(400).json({ ok: false,
    acoes: ['classificar', 'listas', 'add-prioridade', 'add-nao-coletar', 'aplicar-em-massa'] });
};
module.exports.classificar = classificar;
