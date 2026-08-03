// ═══════════════════════════════════════════════════════════════════
// TechFlow — Auditoria Estática Completa
// Roda: node test/auditoria.js          (modo completo)
//       node test/auditoria.js --curto  (uma linha por problema)
//
// NÃO substitui o harness. O harness testa comportamento (2 APIs);
// esta auditoria varre TODO o repositório atrás dos erros que já
// aconteceram: sintaxe quebrada, função chamada que não existe na
// tela, phase×phaseId, divergência de preço logística×loja, cron
// apontando para rota inexistente.
// ═══════════════════════════════════════════════════════════════════
const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

const RAIZ = path.join(__dirname, '..');
const CURTO = process.argv.includes('--curto');
let problemas = 0, avisos = 0, dividas = 0;

// Dívidas conhecidas: problemas antigos já decididos. Não bloqueiam, mas ficam visíveis.
let CONHECIDOS = [];
try { CONHECIDOS = (JSON.parse(fs.readFileSync(path.join(__dirname, 'dividas-conhecidas.json'), 'utf8')).dividas || []); } catch (e) {}
function ehDivida(msg) { return CONHECIDOS.find(d => msg.includes(d.onde) && d.o_que.split(' ').slice(0, 3).every(w => msg.includes(w) || true) && msg.includes(d.onde)); }

function erro(msg, chave)  {
  const d = CONHECIDOS.find(x => chave ? x.id === chave : false);
  if (d) { dividas++; console.log('📋 DÍVIDA CONHECIDA — ' + msg + '\n   └ ' + d.impacto); return; }
  problemas++; console.log('❌ ' + msg);
}
function aviso(msg) { avisos++;    if (!CURTO) console.log('⚠️  ' + msg); }
function ok(msg)    { if (!CURTO) console.log('✅ ' + msg); }

// ── 1. Sintaxe de TODAS as APIs ─────────────────────────────────────
const apis = fs.readdirSync(path.join(RAIZ, 'api')).filter(f => f.endsWith('.js'));
let apisOk = 0;
for (const a of apis) {
  const src = fs.readFileSync(path.join(RAIZ, 'api', a), 'utf8');
  const ehESM = /^\s*import\s/m.test(src);
  if (ehESM) {
    fs.writeFileSync('/tmp/_aud_api.mjs', src);
    try { execSync('node --check /tmp/_aud_api.mjs', { stdio: 'pipe' }); apisOk++; }
    catch (e) { erro(`api/${a} (ESM) — sintaxe: ${String(e.stderr).split('\n')[0].slice(0, 100)}`); }
  } else {
    try { new Function(src.replace(/export default/, 'module.exports =')); apisOk++; }
    catch (e) { erro(`api/${a} — sintaxe: ${e.message}`); }
  }
}
ok(`Sintaxe: ${apisOk}/${apis.length} APIs válidas`);

// ── 2. Telas: cada bloco <script> passa no node --check ─────────────
const telas = fs.readdirSync(RAIZ).filter(f => f.endsWith('.html'));
let telasComErro = 0;
for (const t of telas) {
  const h = fs.readFileSync(path.join(RAIZ, t), 'utf8');
  const blocos = [...h.matchAll(/<script>([\s\S]*?)<\/script>/g)].map(m => m[1]);
  blocos.forEach((b, i) => {
    fs.writeFileSync('/tmp/_aud_bloco.js', b);
    try { execSync('node --check /tmp/_aud_bloco.js', { stdio: 'pipe' }); }
    catch (e) {
      telasComErro++;
      erro(`${t} bloco ${i + 1} — ${String(e.stderr).split('\n')[0].slice(0, 100)}`);
    }
  });
}
ok(`Blocos JS das ${telas.length} telas: ${telasComErro === 0 ? 'todos válidos' : telasComErro + ' com erro'}`);

// ── 3. Telas: função chamada em on* que NÃO existe naquele arquivo ──
// (o erro mais recorrente do histórico — botão que não faz nada)
let orfas = 0;
for (const t of telas) {
  const h = fs.readFileSync(path.join(RAIZ, t), 'utf8');
  const chamadas = new Set([...h.matchAll(/on(?:click|change|input|submit|load)="\s*([a-zA-Z_][\w]*)\s*\(/g)].map(m => m[1]));
  const definidas = new Set([
    ...[...h.matchAll(/function\s+([a-zA-Z_][\w]*)\s*\(/g)].map(m => m[1]),
    ...[...h.matchAll(/(?:const|let|var)\s+([a-zA-Z_][\w]*)\s*=\s*(?:async\s*)?(?:function|\()/g)].map(m => m[1]),
    ...[...h.matchAll(/window\.([a-zA-Z_][\w]*)\s*=/g)].map(m => m[1]),
  ]);
  const nativas = new Set(['if', 'alert', 'confirm', 'location', 'history', 'print', 'event']);
  const faltando = [...chamadas].filter(c => !definidas.has(c) && !nativas.has(c));
  if (faltando.length) {
    // Só é dívida se TODAS as funções faltando já estiverem registradas. Uma nova = problema novo.
    const dv = CONHECIDOS.find(x => x.onde === t && Array.isArray(x.funcoes));
    const novas = dv ? faltando.filter(f => !dv.funcoes.includes(f)) : faltando;
    if (novas.length === 0 && dv) { erro(`${t} — chama função inexistente: ${faltando.join(', ')}`, dv.id); }
    else { orfas += novas.length; erro(`${t} — chama função inexistente: ${novas.join(', ')}`); }
  }
}
ok(`Funções órfãs em handlers on*: ${orfas === 0 ? 'nenhuma' : orfas}`);

// ── 4. phase × phaseId: quem lê cards do pipe só por .phase ─────────
// Cards do pipe guardam a fase em phaseId. Ler só c.phase = filtro morto.
for (const a of apis) {
  const src = fs.readFileSync(path.join(RAIZ, 'api', a), 'utf8');
  const lePipe = /reparoeletro_pipe|tv_pipe/.test(src);
  const usaPhase = /\.\s*phase\b(?!Id)/.test(src);
  const usaPhaseId = /phaseId/.test(src);
  if (lePipe && usaPhase && !usaPhaseId) aviso(`api/${a} — lê o pipe e usa .phase sem nenhum phaseId (risco de filtro morto)`);
}

// ── 5. Paridade de preço: logística × frente de loja ────────────────
// Extrai os valores literais das duas funções de preço e compara.
function extrairPrecos(arquivo, nomeFn) {
  const src = fs.readFileSync(path.join(RAIZ, 'api', arquivo), 'utf8');
  const ini = src.indexOf('function ' + nomeFn);
  if (ini < 0) return null;
  // corpo: até a próxima "function " no mesmo nível (aproximação suficiente p/ literais)
  const fim = src.indexOf('\n    }', src.indexOf('return { texto: null', ini));
  const corpo = src.slice(ini, fim > ini ? fim : ini + 15000);
  const precos = [...corpo.matchAll(/fica em (\d+) reais|preco\s*:\s*'(\d+)'/g)]
    .map(m => m[1] || m[2]);
  return precos.sort().join(',');
}
const pLog = extrairPrecos('logistica.js', 'gerarTexto');
const pLoja = extrairPrecos('frenteloja.js', 'gerarTextoLoja');
if (pLog === null || pLoja === null) erro('paridade de preço — não achei uma das funções (gerarTexto/gerarTextoLoja)');
else if (pLog !== pLoja) erro(`PREÇOS DIVERGENTES logística×loja — alterou um e esqueceu o outro?\n   logistica: ${pLog}\n   loja:      ${pLoja}`);
else ok('Paridade de preço logística × frente de loja: idênticas');

// ── 6. Crons do vercel.json apontam para rotas que existem ──────────
try {
  const vj = JSON.parse(fs.readFileSync(path.join(RAIZ, 'vercel.json'), 'utf8'));
  const crons = vj.crons || [];
  let cronsQuebrados = 0;
  for (const c of crons) {
    const arq = (c.path || '').split('?')[0].replace(/^\//, '') + '.js';
    if (!fs.existsSync(path.join(RAIZ, arq))) { cronsQuebrados++; erro(`cron ${c.path} — arquivo ${arq} não existe`); }
  }
  ok(`Crons: ${crons.length} configurados, ${cronsQuebrados === 0 ? 'todos com rota válida' : cronsQuebrados + ' quebrados'}`);
} catch (e) { erro('vercel.json — ' + e.message); }

// ── 7. Telas e APIs sem rota no vercel.json (o caso do /auditoria) ──
try {
  const vj = JSON.parse(fs.readFileSync(path.join(RAIZ, 'vercel.json'), 'utf8'));
  const destinos = new Set((vj.rewrites || []).map(r => String(r.destination || '').replace(/^\//, '').split('?')[0]));
  const semRota = [];
  for (const t of telas) if (!destinos.has(t)) semRota.push(t);
  const apisSemRota = [];
  for (const a of apis) if (!destinos.has('api/' + a)) apisSemRota.push('api/' + a);
  if (semRota.length) aviso(`${semRota.length} tela(s) sem rota no vercel.json: ${semRota.slice(0, 8).join(', ')}${semRota.length > 8 ? '…' : ''}`);
  if (apisSemRota.length) aviso(`${apisSemRota.length} API(s) sem rota no vercel.json: ${apisSemRota.slice(0, 8).join(', ')}${apisSemRota.length > 8 ? '…' : ''}`);
  if (!semRota.length && !apisSemRota.length) ok('Rotas: toda tela e API tem entrada no vercel.json');
} catch (e) { erro('checagem de rotas — ' + e.message); }

// ── 8. Rota apontando para arquivo que NÃO existe (rota morta) ──────
try {
  const vj = JSON.parse(fs.readFileSync(path.join(RAIZ, 'vercel.json'), 'utf8'));
  let mortas = 0, bloqueadas = 0, mortasConhecidas = 0;
  for (const r of (vj.rewrites || [])) {
    const dest = String(r.destination || '').split('?')[0].replace(/^\//, '');
    if (!dest || dest.startsWith('http')) continue;
    if (!fs.existsSync(path.join(RAIZ, dest))) {
      // bloqueio intencional dos sites de geladeira: destino prefixado com '1'
      const dv = CONHECIDOS.find(x => x.destinos_prefixo1) ;
      if (dv && /^1.+\.html$/.test(dest) && fs.existsSync(path.join(RAIZ, dest.slice(1)))) {
        bloqueadas++; continue;
      }
      const dvR = CONHECIDOS.find(x => Array.isArray(x.rotas) && x.rotas.includes(r.source));
      if (dvR) { mortasConhecidas++; continue; }
      mortas++; erro(`ROTA MORTA: ${r.source} aponta para ${dest} que não existe`);
    }
  }
  if (bloqueadas) { dividas++; console.log(`📋 DÍVIDA CONHECIDA — ${bloqueadas} rota(s) de site de geladeira bloqueadas de propósito (destino prefixado com '1')`); }
  if (mortasConhecidas) { dividas++; console.log(`📋 DÍVIDA CONHECIDA — ${mortasConhecidas} rota(s) de legado apontando para arquivo removido`); }
  if (!mortas) ok('Rotas: nenhum destino morto novo no vercel.json');
} catch (e) { erro('checagem de rotas mortas — ' + e.message); }

// ── Resultado ───────────────────────────────────────────────────────
console.log('\n═══════════════════════════════════');
if (problemas === 0) console.log(`🟢 AUDITORIA LIMPA — 0 problemas novos, ${dividas} dívida(s) conhecida(s), ${avisos} aviso(s).`);
else console.log(`🔴 ${problemas} PROBLEMA(S) NOVO(S), ${dividas} dívida(s), ${avisos} aviso(s). NÃO SUBIR.`);
console.log('═══════════════════════════════════\n');
process.exit(problemas === 0 ? 0 : 1);
