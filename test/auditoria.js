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
let problemas = 0, avisos = 0;

function erro(msg)  { problemas++; console.log('❌ ' + msg); }
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
  if (faltando.length) { orfas += faltando.length; erro(`${t} — chama função inexistente: ${faltando.join(', ')}`); }
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

// ── Resultado ───────────────────────────────────────────────────────
console.log('\n═══════════════════════════════════');
if (problemas === 0) console.log(`🟢 AUDITORIA LIMPA — 0 erros, ${avisos} avisos.`);
else console.log(`🔴 ${problemas} ERRO(S), ${avisos} avisos. Corrija antes de subir.`);
console.log('═══════════════════════════════════\n');
process.exit(problemas === 0 ? 0 : 1);
