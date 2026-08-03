// ═══════════════════════════════════════════════════════════════════
// TechFlow — Harness de Testes (ambiente fechado, Redis em memória)
// Roda: node test/harness.js
// Simula o Upstash e executa os handlers reais contra cenários críticos.
// Regra da casa: NENHUM deploy sem este harness VERDE.
// ═══════════════════════════════════════════════════════════════════
const fs = require('fs');
const path = require('path');

// ── Redis em memória (mock fiel do REST do Upstash) ──
const KV = {};
const LISTS = {};
process.env.UPSTASH_URL = 'https://mock.upstash.local';
process.env.UPSTASH_TOKEN = 'mock';
process.env.TECHFLOW_KEY = 'tfk-teste';

const fetchReal = global.fetch;
global.fetch = async function (url, opts) {
  const u = String(url);
  if (u.startsWith('https://mock.upstash.local')) {
    const p = u.replace('https://mock.upstash.local/', '');
    // Protocolo /pipeline (usado pelo frenteloja e handlers mais novos)
    if (p === 'pipeline' || p.startsWith('pipeline')) {
      const cmds = JSON.parse(opts.body);
      const results = cmds.map(([cmd, key, val]) => {
        const c = String(cmd).toUpperCase();
        if (c === 'GET') return { result: KV[key] !== undefined ? JSON.stringify(KV[key]) : null };
        if (c === 'SET') { KV[key] = JSON.parse(val); return { result: 'OK' }; }
        if (c === 'RPUSH') { (LISTS[key] = LISTS[key] || []).push(val); return { result: LISTS[key].length }; }
        if (c === 'LRANGE') return { result: LISTS[key] || [] };
        return { result: null };
      });
      return { json: async () => results, ok: true };
    }
    const [cmd, ...rest] = p.split('/');
    const key = decodeURIComponent(rest.join('/'));
    let result = null;
    if (cmd === 'get') result = KV[key] !== undefined ? JSON.stringify(KV[key]) : null;
    else if (cmd === 'set') { KV[key] = JSON.parse(opts.body); result = 'OK'; }
    else if (cmd === 'rpush') { const [k, val] = [rest[0], decodeURIComponent(rest.slice(1).join('/'))]; (LISTS[k] = LISTS[k] || []).push(val); result = LISTS[k].length; }
    else if (cmd === 'lrange') { const k = rest[0]; result = (LISTS[k] || []); }
    return { json: async () => ({ result }), ok: true };
  }
  // Chamadas externas (Graph/Anthropic/produção) NUNCA saem do harness:
  return { json: async () => ({ mocked: true, messages: [{ id: 'wamid.mock' }] }), ok: true, arrayBuffer: async () => new ArrayBuffer(8) };
};

// ── Carregador de handler (transforma ESM export default) ──
function carregarHandler(arquivo) {
  let src = fs.readFileSync(path.join(__dirname, '..', arquivo), 'utf8');
  src = src.replace(/export default/, 'module.exports =');
  const mod = { exports: {} };
  const fn = new Function('module', 'exports', 'require', 'process', 'fetch', 'Buffer', src);
  fn(mod, mod.exports, require, process, global.fetch, Buffer);
  return mod.exports;
}

// ── Req/Res fake ──
function req(query = {}, body = null, method) {
  return { method: method || (body ? 'POST' : 'GET'), query, body, headers: {} };
}
function res() {
  const r = { statusCode: 0, dado: null };
  r.setHeader = () => r;
  r.status = (c) => { r.statusCode = c; return r; };
  r.json = (d) => { r.dado = d; return r; };
  r.send = (d) => { r.dado = d; return r; };
  return r;
}

// ── Mini-runner ──
let passa = 0, falha = 0;
function check(nome, cond, extra) {
  if (cond) { passa++; console.log('  ✅', nome); }
  else { falha++; console.log('  ❌', nome, extra !== undefined ? '→ ' + JSON.stringify(extra).slice(0, 140) : ''); }
}

(async () => {
  console.log('\n🧪 TechFlow Harness — ambiente fechado (Redis simulado)\n');
  const K = { k: 'tfk-teste' };

  // ════ CENÁRIO 1: sync do almoxarifado NÃO duplica tarefas (o bug das 997) ════
  console.log('▶ Cenário 1 — sync roda 3x sem duplicar (anti-997)');
  const almox = carregarHandler('api/almoxarifado.js');
  KV['reparoeletro_pipe'] = { cards: [
    { id: 'P1', nomeContato: 'Cliente A', telefone: '5531990001111', equipamento: 'Micro', phase: 'aguardando_aprovacao' },
  ] };
  KV['reparoeletro_logistica'] = { fichas: [] };
  await almox(req({ action: 'sync', ...K }), res());           // 1ª sync: fotografa
  KV['reparoeletro_pipe'].cards[0].phase = 'aprovados';        // movimento real
  KV['reparoeletro_pipe'].cards[0].movedAt = new Date().toISOString();
  await almox(req({ action: 'sync', ...K }), res());           // detecta
  await almox(req({ action: 'sync', ...K }), res());           // repete
  await almox(req({ action: 'sync', ...K }), res());           // repete
  const t1 = KV['reparoeletro_almoxarifado'].tarefas.filter(t => t.cardId === 'P1');
  check('movimento gera exatamente 1 tarefa após 3 syncs', t1.length === 1, { total: t1.length });

  // ════ CENÁRIO 2: conclusão do usuário sobrevive a um sync (anti-corrida) ════
  console.log('▶ Cenário 2 — ação humana não é sobrescrita pelo sync');
  const tid = t1[0].id;
  const r2 = res();
  await almox(req({ action: 'concluir', ...K }, { id: tid, feitoPor: 'Teste' }), r2);
  check('concluir respondeu ok', r2.dado && r2.dado.ok === true, r2.dado);
  await almox(req({ action: 'sync', ...K }), res());
  const t2 = KV['reparoeletro_almoxarifado'].tarefas.find(t => t.id === tid);
  check('tarefa segue FEITA após novo sync', t2 && t2.status === 'feito', t2 && t2.status);

  // ════ CENÁRIO 3: coleta efetuada gera tarefa; fantasma antigo não entra ════
  console.log('▶ Cenário 3 — coleta efetuada (recente entra, velha de 3 dias não)');
  KV['reparoeletro_logistica'] = { fichas: [
    { id: 'LG1', nome: 'Coleta Nova', telefone: '5531990002222', equipamento: 'Forno', phase: 'coleta_efetuada', defeito: 'não esquenta', texto: 'obs teste', movedAt: new Date().toISOString() },
    { id: 'LG2', nome: 'Coleta Velha', telefone: '5531990003333', equipamento: 'Adega', phase: 'coleta_efetuada', movedAt: new Date(Date.now() - 72 * 3600000).toISOString() },
  ] };
  await almox(req({ action: 'sync', ...K }), res());
  const tar3 = KV['reparoeletro_almoxarifado'].tarefas;
  check('coleta recente gerou tarefa receber', tar3.some(t => t.cardId === 'LG1' && t.tipo === 'receber'));
  check('tarefa carrega defeito e observação', tar3.some(t => t.cardId === 'LG1' && t.defeito === 'não esquenta' && t.obs === 'obs teste'));
  check('coleta de 3 dias atrás NÃO gerou tarefa', !tar3.some(t => t.cardId === 'LG2'));

  // ════ CENÁRIO 4: auto-conclusão quando a ficha sai de coleta_efetuada ════
  console.log('▶ Cenário 4 — diagnóstico feito fecha a tarefa sozinho');
  KV['reparoeletro_logistica'].fichas[0].phase = 'orc_registrado';
  await almox(req({ action: 'sync', ...K }), res());
  const t4 = KV['reparoeletro_almoxarifado'].tarefas.find(t => t.cardId === 'LG1');
  check('tarefa receber auto-concluída', t4 && t4.status === 'feito' && !!t4.autoConcluida, t4 && t4.status);

  // ════ CENÁRIO 5: fusível anti-enxurrada (primeira sync com lote de cron) ════
  console.log('▶ Cenário 5 — fusível: 20 movimentos em massa na primeira sync = nada criado');
  delete KV['reparoeletro_almoxarifado'];
  KV['reparoeletro_logistica'] = { fichas: [] };
  KV['reparoeletro_pipe'] = { cards: Array.from({ length: 20 }, (_, i) => (
    { id: 'M' + i, nomeContato: 'Massa ' + i, phase: 'ultima_chamada', movedAt: new Date().toISOString() })) };
  await almox(req({ action: 'sync', ...K }), res());
  const t5 = (KV['reparoeletro_almoxarifado'].tarefas || []).filter(t => t.tipo === 'mover');
  check('fusível segurou a enxurrada (0 tarefas)', t5.length === 0, { criadas: t5.length });

  // ════ CENÁRIO 6: abordagem do bot → contato_feito + marca 1h + dedupe ════
  console.log('▶ Cenário 6 — abordagem: transição de ficha e dedupe');
  const wabot = carregarHandler('api/wa-bot.js');
  KV['wa_bot_config'] = { abordagemAtiva: true, execTels: [] };
  KV['wa_credenciais'] = { token: 'mock', phoneId: '123' };
  KV['fichas_adm'] = { fichas: [{ id: 'F1', nome: 'Ficha Bot', telefone: '31990005555', equipamento: 'Micro', status: 'ficha_criada', criadoEm: new Date(Date.now() - 10 * 60000).toISOString() }] };
  const r6 = res();
  await wabot(req({ action: 'abordagem-fichas', ...K }), r6);
  const f6 = KV['fichas_adm'].fichas[0];
  const dentroJanela = (() => { const b = new Date(Date.now() - 3 * 3600000); const d = b.getUTCDay(), hh = b.getUTCHours(); return (d >= 1 && d <= 5) ? (hh >= 8 && hh < 15) : (d === 6 ? (hh >= 8 && hh < 10) : false); })();
  if (dentroJanela) {
    check('ficha moveu para contato_feito com marca do bot', f6.status === 'contato_feito' && f6.abordadoPorBot === true, f6.status);
    const r6b = res();
    await wabot(req({ action: 'abordagem-fichas', ...K }), r6b);
    check('segunda rodada não re-aborda (dedupe)', (r6b.dado.disparadas || []).length === 0, r6b.dado.disparadas);
  } else {
    check('fora da janela: abordagem em standby (nada disparado)', String((r6.dado || {}).msg || '').includes('standby'), r6.dado);
  }

  // ════ CENÁRIO 7: tabela de preço — logística e frente de loja ════
  console.log('▶ Cenário 7 — precificação: valores da tabela e paridade loja (−10%)');
  const logi = carregarHandler('api/logistica.js');
  const floja = carregarHandler('api/frenteloja.js');

  // helper: gerar orçamento na logística para 1 equipamento
  async function precoLogistica(equip) {
    KV['reparoeletro_logistica'] = { fichas: [{ id: 'PRC1', nome: 'Teste Preco', telefone: '5531990009999', phase: 'coleta_efetuada', diagnostico: { equips: [equip] } }] };
    const r = res();
    await logi(req({ action: 'gerar-orcamento', ...K }, { id: 'PRC1' }), r);
    return r.dado && r.dado.ficha ? r.dado.ficha.diagnostico.preco : (r.dado && r.dado.error);
  }

  const casos = [
    ['micro-ondas elétrico = 350',            { tipo:'microondas', servicos:['Elétrico'] },                          '350'],
    ['micro-ondas Magnetron = 390',           { tipo:'microondas', servicos:['Magnetron'] },                         '390'],
    ['micro-ondas placa custo 150 = 2x = 300',{ tipo:'microondas', servicos:['Troca de Placa'], preco:'150' },       '300'],
    ['purificador Motor Gás = 490',           { tipo:'purificador', subtipo:'Motor', servicos:['Gás'] },             '490'],
    ['purificador Eletrônico Kit = 350',      { tipo:'purificador', subtipo:'Eletrônico', servicos:['Kit Termo Elétrico'] }, '350'],
    ['adega Motor Termostato = 490',          { tipo:'adega', subtipo:'Motor', servicos:['Termostato'] },            '490'],
    ['adega Eletrônico Sensor = 390',         { tipo:'adega', subtipo:'Eletrônico', servicos:['Sensor'] },           '390'],
    ['forno Grande elétrico = 790',           { tipo:'forno', subtipo:'Grande', servicos:['Elétrico'] },             '790'],
    ['bblend = 1490',                         { tipo:'bblend', servicos:['Motor'] },                                 '1490'],
    ['tabela dinâmica: equip 1000 = 400',     { tipo:'microondas', servicos:['Elétrico'], tabela:'dinamica', valorEquip:'1000' }, '400'],
  ];
  for (const [nome, equip, esperado] of casos) {
    const p = await precoLogistica(equip);
    check(nome, p === esperado, { obtido: p });
  }

  // multi-equipamento: 2 aparelhos = soma com 10% de desconto
  KV['reparoeletro_logistica'] = { fichas: [{ id: 'PRC2', nome: 'Teste Multi', telefone: '5531990008888', phase: 'coleta_efetuada', diagnostico: { equips: [ { tipo:'microondas', servicos:['Elétrico'] }, { tipo:'purificador', subtipo:'Motor', servicos:['Gás'] } ] } }] };
  const rM = res();
  await logi(req({ action: 'gerar-orcamento', ...K }, { id: 'PRC2' }), rM);
  const pM = rM.dado && rM.dado.ficha ? rM.dado.ficha.diagnostico.preco : null;
  check('2 equipamentos: (350+490) −10% = 756', pM === '756', { obtido: pM });

  // paridade frente de loja: mesmo equipamento, total igual e −10% aplicado
  KV['reparoeletro_frenteloja'] = { fichas: [{ id: 'FL1', nomeContato: 'Teste Loja', telefone: '5531990007777', phase: 'analise' }], seq: 1 };
  const rF = res();
  await floja(req({ action: 'diagnostico-loja', ...K }, { id: 'FL1', equips: [ { tipo:'microondas', servicos:['Magnetron'] } ] }), rF);
  check('loja: micro Magnetron total = 390 (mesma tabela da logística)', rF.dado && rF.dado.total === 390, rF.dado && (rF.dado.error || rF.dado.total));
  check('loja: desconto de 10% aplicado = 351', rF.dado && rF.dado.totalComDesconto === 351, rF.dado && (rF.dado.error || rF.dado.totalComDesconto));

  // ════ Resultado ════
  console.log('\n═══════════════════════════════════');
  const _mj = (() => { const b = new Date(Date.now() - 3 * 3600000); const d = b.getUTCDay(), hh = b.getUTCHours(); return (d >= 1 && d <= 5) ? (hh >= 8 && hh < 15) : (d === 6 ? (hh >= 8 && hh < 10) : false); })();
  console.log(`   Modo: ${_mj ? 'DENTRO da janela comercial (dedupe do bot TESTADO)' : 'FORA da janela comercial (dedupe do bot NÃO testado — rode tb. em horário comercial)'}`);
  console.log(falha === 0 ? `🟢 VERDE — ${passa} testes passaram. Liberado para a janela de deploy.` : `🔴 VERMELHO — ${falha} falha(s), ${passa} ok. NÃO SUBIR PARA PRODUÇÃO.`);
  console.log('═══════════════════════════════════\n');
  process.exit(falha === 0 ? 0 : 1);
})();
