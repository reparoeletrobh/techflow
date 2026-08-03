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
  (global.__fetchLog = global.__fetchLog || []).push(u);
  if (global.__forcarErroGraph && u.includes('graph.facebook.com')) {
    return { json: async () => global.__forcarErroGraph, ok: false, arrayBuffer: async () => new ArrayBuffer(8) };
  }
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
    // ── MICRO-ONDAS: piso R$370 (era 350). O que já era 390 permanece 390.
    ['micro elétrico 350 -> 370',              { tipo:'microondas', servicos:['Elétrico'] , modelo:'TESTE01' },                          '370'],
    ['micro haste 350 -> 370',                 { tipo:'microondas', servicos:['Haste'] , modelo:'TESTE01' },                             '370'],
    ['micro pintura 350 -> 370',               { tipo:'microondas', servicos:['Pintura'] , modelo:'TESTE01' },                           '370'],
    ['micro reforma 350 -> 370',               { tipo:'microondas', servicos:['Reforma'] , modelo:'TESTE01' },                           '370'],
    ['micro reforma+revisão MANTÉM 390',       { tipo:'microondas', servicos:['Reforma','Revisão'] , modelo:'TESTE01' },                 '390'],
    ['micro magnetron MANTÉM 390',             { tipo:'microondas', servicos:['Magnetron'] , modelo:'TESTE01' },                         '390'],
    ['micro placa custo 150: 2x=300 -> piso 370', { tipo:'microondas', servicos:['Troca de Placa'], preco:'150' , modelo:'TESTE01' },    '370'],
    ['micro placa custo 250: 2x=500 PREVALECE',{ tipo:'microondas', servicos:['Troca de Placa'], preco:'250' , modelo:'TESTE01' },       '500'],

    // ── FORNO: Grande 790 -> 890. Pequeno permanece 490.
    ['forno GRANDE 790 -> 890',                { tipo:'forno', subtipo:'Grande', servicos:['Resistência'] , modelo:'TESTE01' },          '890'],
    ['forno PEQUENO mantém 490',               { tipo:'forno', subtipo:'Pequeno', servicos:['Resistência'] , modelo:'TESTE01' },         '490'],

    // ── PURIFICADOR: intocado.
    ['purificador Motor Gás mantém 490',       { tipo:'purificador', subtipo:'Motor', servicos:['Gás'] , modelo:'TESTE01' },             '490'],
    ['purificador Eletrônico Kit mantém 350',  { tipo:'purificador', subtipo:'Eletrônico', servicos:['Kit Termo Elétrico'] , modelo:'TESTE01' }, '350'],

    // ── ADEGA modo NORMAL: sem porte, preços atuais mantidos.
    ['adega normal Motor Termostato = 490',    { tipo:'adega', subtipo:'Motor', servicos:['Termostato'] , modelo:'TESTE01' },            '490'],
    ['adega normal Eletrônico Kit = 350',      { tipo:'adega', subtipo:'Eletrônico', servicos:['Kit Termo Elétrico'] , modelo:'TESTE01' }, '350'],

    // ── ADEGA 8 GARRAFAS: piso R$390. Acima disso, o maior prevalece.
    ['adega8 Eletrônico Kit 350 -> 390',       { tipo:'adega', subtipo:'Eletrônico', servicos:['Kit Termo Elétrico'], tabela:'adega8' , modelo:'TESTE01' }, '390'],
    ['adega8 Eletrônico Sensor 390 -> 390',    { tipo:'adega', subtipo:'Eletrônico', servicos:['Sensor'], tabela:'adega8' , modelo:'TESTE01' },            '390'],
    ['adega8 Eletrônico TermoDuplo 490 MANTÉM',{ tipo:'adega', subtipo:'Eletrônico', servicos:['Termoelétrico Duplo'], tabela:'adega8' , modelo:'TESTE01' },'490'],
    ['adega8 Motor Termostato 490 MANTÉM',     { tipo:'adega', subtipo:'Motor', servicos:['Termostato'], tabela:'adega8' , modelo:'TESTE01' },             '490'],

    // ── ADEGA 12 GARRAFAS: piso R$450.
    ['adega12 Eletrônico Kit 350 -> 450',      { tipo:'adega', subtipo:'Eletrônico', servicos:['Kit Termo Elétrico'], tabela:'adega12' , modelo:'TESTE01' }, '450'],
    ['adega12 Eletrônico Sensor 390 -> 450',   { tipo:'adega', subtipo:'Eletrônico', servicos:['Sensor'], tabela:'adega12' , modelo:'TESTE01' },            '450'],
    ['adega12 Eletrônico TermoDuplo 490 MANTÉM',{tipo:'adega', subtipo:'Eletrônico', servicos:['Termoelétrico Duplo'], tabela:'adega12' , modelo:'TESTE01' },'490'],
    ['adega12 Motor Termostato 490 MANTÉM',    { tipo:'adega', subtipo:'Motor', servicos:['Termostato'], tabela:'adega12' , modelo:'TESTE01' },             '490'],
    ['adega12 placa custo 180: 2x=360 -> piso 450', { tipo:'adega', subtipo:'Motor', servicos:['Troca de Placa'], preco:'180', tabela:'adega12' , modelo:'TESTE01' }, '450'],

    // ── TABELA DINÂMICA: intocada, 40% do valor do equipamento.
    ['dinâmica equip 1000 = 400',              { tipo:'microondas', servicos:['Elétrico'], tabela:'dinamica', valorEquip:'1000' , modelo:'TESTE01' }, '400'],
    ['dinâmica adega equip 2000 = 800',        { tipo:'adega', subtipo:'Motor', servicos:['Gás'], tabela:'dinamica', valorEquip:'2000' , modelo:'TESTE01' }, '800'],
  ];
  for (const [nome, equip, esperado] of casos) {
    const p = await precoLogistica(equip);
    check(nome, p === esperado, { obtido: p });
  }

  // multi-equipamento: 2 aparelhos = soma com 10% de desconto
  KV['reparoeletro_logistica'] = { fichas: [{ id: 'PRC2', nome: 'Teste Multi', telefone: '5531990008888', phase: 'coleta_efetuada', diagnostico: { equips: [ { tipo:'microondas', servicos:['Elétrico'] , modelo:'TESTE01' }, { tipo:'purificador', subtipo:'Motor', servicos:['Gás'] , modelo:'TESTE01' } ] } }] };
  const rM = res();
  await logi(req({ action: 'gerar-orcamento', ...K }, { id: 'PRC2' }), rM);
  const pM = rM.dado && rM.dado.ficha ? rM.dado.ficha.diagnostico.preco : null;
  check('2 equipamentos: (370+490) −5% = 817', pM === '817', { obtido: pM });

  // paridade frente de loja: mesmo equipamento, total igual e −10% aplicado
  KV['reparoeletro_frenteloja'] = { fichas: [{ id: 'FL1', nomeContato: 'Teste Loja', telefone: '5531990007777', phase: 'analise' }], seq: 1 };
  const rF = res();
  await floja(req({ action: 'diagnostico-loja', ...K }, { id: 'FL1', equips: [ { tipo:'microondas', servicos:['Magnetron'] , modelo:'TESTE01' } ] }), rF);
  check('loja: micro Magnetron total = 390 (mesma tabela da logística)', rF.dado && rF.dado.total === 390, rF.dado && (rF.dado.error || rF.dado.total));
  check('loja: desconto de 10% aplicado = 351', rF.dado && rF.dado.totalComDesconto === 351, rF.dado && (rF.dado.error || rF.dado.totalComDesconto));
  // loja usa os preços NOVOS: micro elétrico 370 -> com 10% = 333
  KV['reparoeletro_frenteloja'] = { fichas: [{ id: 'FL2', nomeContato: 'Teste Loja 2', telefone: '5531990007766', phase: 'analise' }], seq: 1 };
  const rF2 = res();
  await floja(req({ action: 'diagnostico-loja', ...K }, { id: 'FL2', equips: [ { tipo:'microondas', servicos:['Elétrico'] , modelo:'TESTE01' } ] }), rF2);
  check('loja: micro elétrico usa o preço novo 370', rF2.dado && rF2.dado.total === 370, rF2.dado && (rF2.dado.error || rF2.dado.total));
  check('loja: 370 −10% = 333', rF2.dado && rF2.dado.totalComDesconto === 333, rF2.dado && (rF2.dado.error || rF2.dado.totalComDesconto));
  // loja: adega 8 garrafas com piso 390 -> com 10% = 351
  KV['reparoeletro_frenteloja'] = { fichas: [{ id: 'FL3', nomeContato: 'Teste Loja 3', telefone: '5531990007755', phase: 'analise' }], seq: 1 };
  const rF3 = res();
  await floja(req({ action: 'diagnostico-loja', ...K }, { id: 'FL3', equips: [ { tipo:'adega', subtipo:'Eletrônico', servicos:['Kit Termo Elétrico'], tabela:'adega8' , modelo:'TESTE01' } ] }), rF3);
  check('loja: adega8 aplica piso 390', rF3.dado && rF3.dado.total === 390, rF3.dado && (rF3.dado.error || rF3.dado.total));

  // ════ CENÁRIO 7b: orçamento NUNCA sai sem preço no texto ════
  console.log('▶ Cenário 7b — trava: texto sem preço não pode ser gravado nem enviado');
  // 7b.1 template customizado SEM [VALOR] -> tem que RECUSAR (bug do orçamento sem preço)
  KV['reparoeletro_orc_templates'] = { microondas_placa: { texto: 'Ola [NOME], vamos trocar a [peças]. Aprovando ja iniciamos.' } };
  KV['reparoeletro_frenteloja'] = { fichas: [{ id:'SP1', nomeContato:'Sem Preco', telefone:'5531990001111', phase:'analise' }], seq:1 };
  const rSP = res();
  await floja(req({ action:'diagnostico-loja', ...K }, { id:'SP1', equips:[{ tipo:'microondas', servicos:['Troca de Placa'], preco:'150', modelo:'M4' }] }), rSP);
  check('loja: template sem [VALOR] é RECUSADO', rSP.dado && rSP.dado.ok === false, rSP.dado && (rSP.dado.texto || rSP.dado.total));
  const fSP = KV['reparoeletro_frenteloja'].fichas[0];
  check('loja: ficha NÃO ficou com orçamento gravado', !fSP.diagnosticoLoja && !fSP.valorOrcamento, fSP.valorOrcamento);

  // 7b.2 mesma coisa na logística
  KV['reparoeletro_logistica'] = { fichas: [{ id:'SP2', nome:'Sem Preco Log', telefone:'5531990001122', phase:'coleta_efetuada', diagnostico:{ equips:[{ tipo:'microondas', servicos:['Troca de Placa'], preco:'150', modelo:'M4' }] } }] };
  const rSP2 = res();
  await logi(req({ action:'gerar-orcamento', ...K }, { id:'SP2' }), rSP2);
  check('logística: template sem [VALOR] é RECUSADO', rSP2.dado && rSP2.dado.ok === false, rSP2.dado && rSP2.dado.error);
  delete KV['reparoeletro_orc_templates'];

  // 7b.3 peça SEM custo informado: continua sendo recusado (o piso não pode mascarar)
  KV['reparoeletro_frenteloja'] = { fichas: [{ id:'SP3', nomeContato:'Sem Custo', telefone:'5531990001133', phase:'analise' }], seq:1 };
  const rSP3 = res();
  await floja(req({ action:'diagnostico-loja', ...K }, { id:'SP3', equips:[{ tipo:'microondas', servicos:['Troca de Placa'], modelo:'M5' }] }), rSP3);
  check('loja: peça sem custo informado é RECUSADA (piso não mascara)', rSP3.dado && rSP3.dado.ok === false, rSP3.dado && (rSP3.dado.total || rSP3.dado.error));

  KV['reparoeletro_logistica'] = { fichas: [{ id:'SP4', nome:'Sem Custo Log', telefone:'5531990001144', phase:'coleta_efetuada', diagnostico:{ equips:[{ tipo:'microondas', servicos:['Troca de Placa'], modelo:'M5' }] } }] };
  const rSP4 = res();
  await logi(req({ action:'gerar-orcamento', ...K }, { id:'SP4' }), rSP4);
  check('logística: peça sem custo informado é RECUSADA', rSP4.dado && rSP4.dado.ok === false, rSP4.dado && (rSP4.dado.error || 'passou'));

  // ════ CENÁRIO 7c: falha visível — nada morre em silêncio ════
  console.log('▶ Cenário 7c — a recusa aparece no log e abre conflito');
  KV['reparoeletro_orc_templates'] = { microondas_placa: { texto: 'Ola [NOME], vamos trocar a [peças].' } };
  KV['reparoeletro_log'] = [];
  global.__fetchLog.length = 0;
  KV['reparoeletro_frenteloja'] = { fichas: [{ id:'VIS1', nomeContato:'Cliente Visivel', telefone:'5531990005511', phase:'analise' }], seq:1 };
  await floja(req({ action:'diagnostico-loja', ...K }, { id:'VIS1', equips:[{ tipo:'microondas', servicos:['Troca de Placa'], preco:'150', modelo:'M4' }] }), res());
  const logFL = KV['reparoeletro_log'] || [];
  check('loja: recusa entrou no log de auditoria como erro',
    logFL.some(l => l.status === 'erro' && /sem o valor|SEM PREÇO|preço/i.test(String(l.detalhe) + String(l.gatilho) + String(l.acao))), logFL[0]);
  check('loja: abriu conflito visível (criar-conflito chamado)',
    global.__fetchLog.some(u => u.includes('criar-conflito')), global.__fetchLog);

  KV['reparoeletro_log'] = [];
  global.__fetchLog.length = 0;
  KV['reparoeletro_logistica'] = { fichas: [{ id:'VIS2', nome:'Cliente Log', telefone:'5531990005522', phase:'coleta_efetuada', diagnostico:{ equips:[{ tipo:'microondas', servicos:['Troca de Placa'], preco:'150', modelo:'M4' }] } }] };
  await logi(req({ action:'gerar-orcamento', ...K }, { id:'VIS2' }), res());
  const logLG = KV['reparoeletro_log'] || [];
  check('logística: recusa entrou no log de auditoria como erro',
    logLG.some(l => l.status === 'erro'), logLG[0]);
  check('logística: abriu conflito visível',
    global.__fetchLog.some(u => u.includes('criar-conflito')), global.__fetchLog);
  delete KV['reparoeletro_orc_templates'];

  // ════ CENÁRIO 7d: MODELO obrigatório (alimenta a objeção "compro um novo") ════
  console.log('▶ Cenário 7d — modelo do equipamento é obrigatório');
  // logística: sem modelo -> recusa
  KV['reparoeletro_logistica'] = { fichas: [{ id:'MD1', nome:'Sem Modelo', telefone:'5531990006611', phase:'coleta_efetuada', diagnostico:{ equips:[{ tipo:'microondas', servicos:['Elétrico'] }] } }] };
  const rMD1 = res();
  await logi(req({ action:'gerar-orcamento', ...K }, { id:'MD1' }), rMD1);
  check('logística: SEM modelo é recusado', rMD1.dado && rMD1.dado.ok === false && /modelo/i.test(String(rMD1.dado.error)), rMD1.dado && rMD1.dado.error);

  // logística: com modelo -> passa
  KV['reparoeletro_logistica'] = { fichas: [{ id:'MD2', nome:'Com Modelo', telefone:'5531990006622', phase:'coleta_efetuada', diagnostico:{ equips:[{ tipo:'microondas', servicos:['Elétrico'], modelo:'MEF41' }] } }] };
  const rMD2 = res();
  await logi(req({ action:'gerar-orcamento', ...K }, { id:'MD2' }), rMD2);
  check('logística: COM modelo passa normal', rMD2.dado && rMD2.dado.ok === true, rMD2.dado && rMD2.dado.error);

  // loja: sem modelo -> recusa
  KV['reparoeletro_frenteloja'] = { fichas: [{ id:'MD3', nomeContato:'Loja Sem Modelo', telefone:'5531990006633', phase:'analise' }], seq:1 };
  const rMD3 = res();
  await floja(req({ action:'diagnostico-loja', ...K }, { id:'MD3', equips:[{ tipo:'microondas', servicos:['Elétrico'] }] }), rMD3);
  check('loja: SEM modelo é recusado', rMD3.dado && rMD3.dado.ok === false && /modelo/i.test(String(rMD3.dado.error)), rMD3.dado && rMD3.dado.error);

  // loja: modelo da FICHA é herdado quando o equipamento não traz
  KV['reparoeletro_frenteloja'] = { fichas: [{ id:'MD4', nomeContato:'Loja Herda', telefone:'5531990006644', phase:'analise', modelo:'BZC12B' }], seq:1 };
  const rMD4 = res();
  await floja(req({ action:'diagnostico-loja', ...K }, { id:'MD4', equips:[{ tipo:'adega', subtipo:'Eletrônico', servicos:['Kit Termo Elétrico'], tabela:'adega12' }] }), rMD4);
  check('loja: herda o modelo da ficha do Frente de Loja', rMD4.dado && rMD4.dado.ok === true, rMD4.dado && rMD4.dado.error);
  const fMD4 = KV['reparoeletro_frenteloja'].fichas[0];
  check('loja: modelo herdado ficou gravado no equipamento',
    fMD4.diagnosticoLoja && fMD4.diagnosticoLoja.equips[0].modelo === 'BZC12B', fMD4.diagnosticoLoja && fMD4.diagnosticoLoja.equips[0].modelo);

  // criação de ficha no Frente de Loja: modelo obrigatório e gravado
  KV['reparoeletro_frenteloja'] = { fichas: [], seq:0 };
  const rMD5 = res();
  await floja(req({ action:'criar', ...K }, { nomeContato:'Novo Cliente', equipamento:'Micro-ondas', telefone:'31990006655' }), rMD5);
  check('FL criar: SEM modelo é recusado', rMD5.dado && rMD5.dado.ok === false && /modelo/i.test(String(rMD5.dado.error)), rMD5.dado && rMD5.dado.error);
  const rMD6 = res();
  await floja(req({ action:'criar', ...K }, { nomeContato:'Novo Cliente', equipamento:'Micro-ondas', telefone:'31990006655', modelo:'MEF41' }), rMD6);
  check('FL criar: COM modelo cria e grava', rMD6.dado && rMD6.dado.ok === true && rMD6.dado.ficha && rMD6.dado.ficha.modelo === 'MEF41', rMD6.dado && (rMD6.dado.error || (rMD6.dado.ficha||{}).modelo));

  // ════ CENÁRIO 8: isolamento do Frente de Loja ════
  // Regra inviolável: FL grava SÓ em reparoeletro_frenteloja. O bot não lê esse banco.
  console.log('▶ Cenário 8 — Frente de Loja não vaza para os bancos do bot');
  const sentinelaLog = JSON.stringify(KV['reparoeletro_logistica'] || null);
  const sentinelaTvLog = JSON.stringify(KV['tv_logistica'] || null);
  KV['reparoeletro_frenteloja'] = { fichas: [{ id: 'ISO1', nomeContato: 'Iso Teste', telefone: '5531990006666', phase: 'analise' }], seq: 1 };
  await floja(req({ action: 'diagnostico-loja', ...K }, { id: 'ISO1', equips: [ { tipo:'purificador', subtipo:'Motor', servicos:['Gás'], modelo:'ISO-M' } ] }), res());
  check('diagnóstico de loja NÃO tocou reparoeletro_logistica', JSON.stringify(KV['reparoeletro_logistica'] || null) === sentinelaLog);
  check('diagnóstico de loja NÃO tocou tv_logistica', JSON.stringify(KV['tv_logistica'] || null) === sentinelaTvLog);
  const fIso = KV['reparoeletro_frenteloja'].fichas[0];
  check('orçamento de loja nasce NÃO enviado (orcEnviadoWpp=false)', fIso.orcEnviadoWpp === false, fIso.orcEnviadoWpp);

  // ════ CENÁRIO 9: aprovação — trava anti-limbo e roteamento por origem ════
  console.log('▶ Cenário 9 — aprovação: ambiguidade recusa, origem roteia');
  // 9a: SEM origem + orçamento aberto em TV e ADM = recusa e abre conflito
  KV['wa_orc_enviados'] = { ids: {} };
  KV['reparoeletro_pipe'] = { cards: [{ id: 'AMB-ADM', nomeContato: 'Ambiguo', telefone: '5531990004444', phaseId: 'aguardando_aprovacao' }] };
  KV['tv_logistica'] = { fichas: [{ id: 'AMB-TV', nome: 'Ambiguo', telefone: '5531990004444', phase: 'orc_enviado' }] };
  KV['tv_pipe'] = { cards: [] };
  const r9a = res();
  await wabot(req({ action: 'aprovar-cliente', tel: '90004444', aplicar: '1', ...K }), r9a);
  check('cliente em 2 sistemas sem origem: bot RECUSA (não adivinha)', r9a.dado && r9a.dado.ok === false, r9a.dado && r9a.dado.passos);

  // 9b: origem logistica-adm registrada = NÃO toca na TV
  global.__fetchLog.length = 0;
  KV['wa_orc_enviados'] = { ids: { 'orc1': { telefone: '5531990004444', origem: 'logistica-adm', fichaId: 'AMB-ADM', em: new Date().toISOString() } } };
  await wabot(req({ action: 'aprovar-cliente', tel: '90004444', aplicar: '1', ...K }), res());
  const chamouTv = global.__fetchLog.some(u => u.includes('tv-logistica') && u.includes('aprovar-orcamento'));
  check('origem ADM: aprovação NÃO chamou a logística de TV', !chamouTv);

  // ════ CENÁRIO 10: aprovação de TV não cria tarefa no almoxarifado ADM ════
  console.log('▶ Cenário 10 — TV aprovada: board de TV sim, almoxarifado ADM não');
  delete KV['reparoeletro_almoxarifado'];
  global.__fetchLog.length = 0;
  KV['wa_orc_enviados'] = { ids: { 'orc2': { telefone: '5531990003333', origem: 'logistica-tv', fichaId: 'TVF1', em: new Date().toISOString() } } };
  KV['tv_logistica'] = { fichas: [{ id: 'TVF1', nome: 'Cliente TV', telefone: '5531990003333', phase: 'orc_enviado' }] };
  KV['tv_pipe'] = { cards: [{ id: 'TVC1', nomeContato: 'Cliente TV', telefone: '5531990003333', phaseId: 'aprovados', equipamento: 'TV 50' }] };
  KV['reparoeletro_pipe'] = { cards: [] };
  KV['tv_board'] = { cards: [] };
  const r10 = res();
  await wabot(req({ action: 'aprovar-cliente', tel: '90003333', aplicar: '1', ...K }), r10);
  check('TV: card entrou no board de TV', (KV['tv_board'].cards || []).some(c => c.osCode === 'TVC1'));
  const criouAlmox = global.__fetchLog.some(u => u.includes('almoxarifado') && u.includes('criar-mover'));
  check('TV: NENHUMA tarefa criada no almoxarifado ADM', !criouAlmox && !KV['reparoeletro_almoxarifado']);

  // ════ CENÁRIO 11: precificação — leitura correta e ZERO escrita ════
  console.log('▶ Cenário 11 — análise de precificação (somente leitura)');
  const prec = carregarHandler('api/precificacao.js');
  KV['reparoeletro_pipe'] = { cards: [
    { id:'A1', equipamento:'Micro-ondas', modelo:'Electrolux MEF41', valor:350, phaseId:'aprovados' },
    { id:'A2', equipamento:'Micro-ondas', modelo:'Electrolux MEF41', valor:350, phaseId:'finalizado' },
    { id:'A3', equipamento:'Micro-ondas', modelo:'Electrolux MEF41', valor:350, phaseId:'ultima_chamada' },
    { id:'A4', equipamento:'Adega',       modelo:'Philco PH8',       valor:490, phaseId:'aguardando_aprovacao' },
    { id:'A5', equipamento:'Micro-ondas', modelo:'',                 valor:350, phaseId:'aprovados' },
    { id:'A6', equipamento:'Forno',       modelo:'Fischer X',        valor:790, phaseId:'video_enviado' },
    { id:'A7', equipamento:'Micro-ondas', modelo:'Consul CM1',       valor:0,   phaseId:'aprovados' },
  ] };
  KV['reparoeletro_frenteloja'] = { fichas: [] };
  const snapAntes = JSON.stringify(KV);
  const r11 = res();
  await prec(req({ action:'modelos', curto:'1', ...K }), r11);
  const txt11 = String(r11.dado || '');
  check('agrupou por modelo e calculou taxa (MEF41: 3 casos, 67%)', /MEF41;n=3;.*aprov=67%/.test(txt11), txt11.split('\n').slice(0,4));
  check('card sem valor foi ignorado (CM1 fora)', !txt11.includes('CM1'), txt11);
  check('contou os sem modelo separadamente', /sem_modelo=1/.test(txt11), txt11.split('\n')[1]);
  check('fase pós-aprovação conta como aprovada (Fischer 100%)', /FISCHER X;n=1;.*aprov=100%/.test(txt11), txt11);
  check('LEITURA PURA: banco intacto após a análise', JSON.stringify(KV) === snapAntes);
  const r11b = res();
  await prec(req({ action:'faixas', curto:'1', ...K }), r11b);
  check('faixas: agrupou por faixa de preço', /350-399/.test(String(r11b.dado||'')), String(r11b.dado||'').slice(0,120));
  const r11c = res();
  await prec(req({ action:'modelos', k:'chave-errada' }), r11c);
  check('chave inválida é recusada', r11c.statusCode === 401);

  // ════ CENÁRIO 12: promessa não cumprida vira conflito para um humano ════
  console.log('▶ Cenário 12 — cadastrar_logistica que falha abre conflito');
  KV['wa_credenciais'] = { token:'mock', phoneId:'123' };
  KV['wa_bot_config'] = { modoAberto: true, execTels: [] };
  KV['wa_bot_pausados'] = {};

  // 12a: cliente SEM ficha em fichas_adm — o cadastro não tem como acontecer
  KV['fichas_adm'] = { fichas: [] };
  KV['fichas_tv'] = { fichas: [] };
  KV['reparoeletro_logistica'] = { fichas: [] };
  global.__fetchLog.length = 0;
  const r12a = res();
  await wabot(req({ action:'enviar', ...K }, { tel:'5531990007001',
    texto:'Perfeito! Sua coleta será feita amanhã entre 08h e 14h.',
    acaoAprovada:'cadastrar_logistica', acaoMotivo:'coleta imediata' }), r12a);
  check('sem ficha: abriu conflito para um humano',
    global.__fetchLog.some(u => u.includes('criar-conflito')), global.__fetchLog.slice(0,4));
  check('sem ficha: nada foi criado na logística',
    ((KV['reparoeletro_logistica']||{}).fichas||[]).length === 0);

  // 12b: cliente COM ficha — cadastra e NÃO abre conflito
  KV['fichas_adm'] = { fichas: [{ id:'FA1', nome:'Cliente OK', telefone:'5531990007002', equipamento:'Micro-ondas', endereco:'Rua X', status:'contato_feito' }] };
  KV['reparoeletro_logistica'] = { fichas: [] };
  global.__fetchLog.length = 0;
  await wabot(req({ action:'enviar', ...K }, { tel:'5531990007002',
    texto:'Perfeito! Nossa equipe já vai programar a busca.',
    acaoAprovada:'cadastrar_logistica', acaoMotivo:'coleta imediata' }), res());
  check('com ficha: criou na logística', ((KV['reparoeletro_logistica']||{}).fichas||[]).length === 1,
    (KV['reparoeletro_logistica']||{}).fichas);
  check('com ficha: NÃO abriu conflito (nada a alertar)',
    !global.__fetchLog.some(u => u.includes('criar-conflito')));

  // ════ CENÁRIO 13: integridade do roteiro do bot ════
  // Não julga a inteligência (nenhum teste faz isso) — garante que as âncoras
  // críticas continuam no prompt e que o arquivo não quebrou.
  console.log('▶ Cenário 13 — âncoras críticas do roteiro do bot');
  const _srcBot = require('fs').readFileSync(require('path').join(__dirname, '..', 'api/wa-bot.js'), 'utf8');
  const ancoras = [
    ['5-DESC: pedido de desconto NÃO compara com equipamento novo', '5-DESC)'],
    ['5-PRE: pesquisa real obrigatória continua no roteiro', '5-PRE)'],
    ['proibido pesquisar dados da empresa (CNPJ/Pix/contas)', 'TERMINANTEMENTE PROIBIDO pesquisar na internet'],
    ['limite de 2 pesquisas web', 'max_uses: 2'],
    ['proibido inventar ou chutar preço', 'PROIBIDO inventar ou chutar'],
    ['regra 6 usa PESQUISA WEB REAL (não "mentalmente")', 'PESQUISA WEB REAL'],
    ['regra 6 compara por especificação quando não acha o modelo', 'especificaç'],
    ['argumento de linha inferior preservado', 'cfg.argumentoNovo'],
    ['fora da janela: regra dura de não agendar', 'REGRA DURA'],
    ['retorno prometido gera conflito', 'registrar_conflito'],
    ['rede de segurança da promessa sem lastro', 'promessaSemLastro'],
  ];
  for (const [nome, agulha] of ancoras) check(nome, _srcBot.includes(agulha), agulha);
  check('regra 6 NÃO contém mais "pesquise mentalmente"', !_srcBot.includes('pesquise mentalmente'));
  // o roteiro vive dentro de template literal: qualquer crase/${ solto derruba o arquivo inteiro
  check('wa-bot.js compila (template literal íntegro)', (() => {
    try { new Function(_srcBot.replace(/export default/, 'module.exports =')); return true; } catch (e) { return false; }
  })());

  // ════ CENÁRIO 14: fila de ligação — prioridade e registro ════
  console.log('▶ Cenário 14 — fila de ligação (Fase 0)');
  const fila = carregarHandler('api/fila-ligacao.js');
  const agoraF = Date.now();
  const hAtras = h => new Date(agoraF - h * 3600000).toISOString();
  KV['prospeccao_adm'] = { fichas: [
    { id:'L1', status:'lead', nome:'Lead Frio', telefone:'5531990010001', criadoEm: hAtras(10) },
    { id:'R1', status:'retornar', nome:'Retornar Um', telefone:'5531990010002', movidoEm: hAtras(10) },
    { id:'E1', status:'entrar_contato', nome:'Entrar Contato', telefone:'5531990010003', equipamento:'Micro-ondas', criadoEm: hAtras(5) },
    { id:'C1', status:'conflitos_bot', nome:'Reprovou Orc', telefone:'5531990010004', motivoConflito:'reprovou o orçamento após as 5 fases — finalizar manualmente', criadoEm: hAtras(8) },
    { id:'C2', status:'conflitos_bot', nome:'Analise Compra', telefone:'5531990010005', motivoConflito:'ANÁLISE DE COMPRA — cliente quer vender o equipamento', criadoEm: hAtras(8) },
    { id:'C3', status:'conflitos_bot', nome:'Promessa', telefone:'5531990010006', motivoConflito:'⚠️ PROMESSA NÃO CUMPRIDA — VERIFICAR COM O CLIENTE', criadoEm: hAtras(2) },
    { id:'CL1', status:'cliente_loja', nome:'Loja Vermelho', telefone:'5531990010007', movidoEm: hAtras(72) },
    { id:'CL2', status:'cliente_loja', nome:'Loja No Prazo', telefone:'5531990010008', movidoEm: hAtras(10) },
  ] };
  KV['fichas_adm'] = { fichas: [] }; KV['fichas_tv'] = { fichas: [] };
  delete KV['fila_ligacao_log'];
  const qF1 = res();
  await fila(req({ action:'fila', ...K }), qF1);
  const lista = (qF1.dado && qF1.dado.fila) || [];
  const nomes = lista.map(i => i.nome);
  check('ordem: Entrar em Contato é o 1º', nomes[0] === 'Entrar Contato', nomes);
  check('ordem: conflito de REPROVAÇÃO é o 2º', nomes[1] === 'Reprovou Orc', nomes);
  check('ordem: Cliente Loja VERMELHO é o 3º', nomes[2] === 'Loja Vermelho', nomes);
  check('ordem: Retornar é o 4º', nomes[3] === 'Retornar Um', nomes);
  check('ordem: Lead é o último', nomes[4] === 'Lead Frio', nomes);
  check('conflito de ANÁLISE DE COMPRA fica FORA (resolve por mensagem)', !nomes.includes('Analise Compra'), nomes);
  check('conflito de PROMESSA fica FORA da fila de ligação', !nomes.includes('Promessa'), nomes);
  check('Cliente Loja dentro do prazo fica FORA', !nomes.includes('Loja No Prazo'), nomes);

  // desfecho grava e confirma
  const qF2 = res();
  await fila(req({ action:'desfecho', ...K }, { id:'E1', telefone:'5531990010003', nome:'Entrar Contato',
    atendeu:'sim', resultado:'cadastrou_coleta', duracaoSeg: 180 }), qF2);
  check('desfecho gravado e confirmado', qF2.dado && qF2.dado.ok === true, qF2.dado);
  check('desfecho sem "atendeu" é recusado', await (async () => {
    const r = res(); await fila(req({ action:'desfecho', ...K }, { id:'E1' }), r); return r.dado && r.dado.ok === false; })());

  // quem já foi ligado hoje sai da fila
  const qF3 = res();
  await fila(req({ action:'fila', ...K }), qF3);
  check('cliente já ligado hoje sai da fila', !((qF3.dado.fila)||[]).some(i => i.id === 'E1'),
    ((qF3.dado||{}).fila||[]).map(i => i.nome));

  const qF4 = res();
  await fila(req({ action:'relatorio', ...K }), qF4);
  check('relatório conta a ligação e o tempo', qF4.dado && qF4.dado.ligacoes === 1 && qF4.dado.tempoMedioSeg === 180, qF4.dado);
  check('fila: chave inválida recusada', await (async () => {
    const r = res(); await fila(req({ action:'fila', k:'errada' }), r); return r.statusCode === 401; })());

  // ════ CENÁRIO 15: template de orçamento — falha não pode virar "enviado" ════
  console.log('▶ Cenário 15 — template orcamento_pronto: evidência e reenvio');
  KV['wa_credenciais'] = { token:'mock', phoneId:'123' };
  KV['wa_bot_config'] = { modoAberto:true, execTels:[] };
  KV['wa_bot_pausados'] = {};
  KV['tv_logistica'] = { fichas: [] };
  const fichaOrc = () => ({ id:'ORC1', nome:'Bruna Teste', telefone:'5531987537200',
    equipamento:'Purificador', phase:'orc_registrado',
    diagnostico:{ equips:[{tipo:'purificador',modelo:'PE11X',servicos:['Gás']}], textoOrc:'Orcamento: fica em 350 reais', em:new Date().toISOString() } });

  // 15a: Meta RECUSA o template -> não pode marcar como enviado
  KV['reparoeletro_logistica'] = { fichas: [fichaOrc()] };
  KV['wa_orc_enviados'] = { ids: {} };
  global.__forcarErroGraph = { error: { message: 'Template name does not exist', code: 132001 } };
  await wabot(req({ action:'orcamentos-pendentes', ...K }), res());
  delete global.__forcarErroGraph;
  const reg15 = (KV['wa_orc_enviados']||{}).ids || {};
  check('template recusado: NÃO marca como enviado com sucesso',
    !reg15['ORC1'] || reg15['ORC1'].ok !== true, reg15['ORC1']);
  const evt15 = (LISTS['wa_evt_list']||[]).map(x => { try { return JSON.parse(x); } catch(e){ return {}; } });
  check('template recusado: erro da Meta fica registrado no histórico',
    evt15.some(e => e.tipo === 'falha' && /132001|Template name/.test(String(e.texto) + String(e.erro))), evt15.slice(-2));

  // 15b: Meta ACEITA -> grava msgId e marca enviado
  LISTS['wa_evt_list'] = [];
  KV['reparoeletro_logistica'] = { fichas: [fichaOrc()] };
  KV['wa_orc_enviados'] = { ids: {} };
  await wabot(req({ action:'orcamentos-pendentes', ...K }), res());
  const reg15b = (KV['wa_orc_enviados']||{}).ids || {};
  const evt15b = (LISTS['wa_evt_list']||[]).map(x => { try { return JSON.parse(x); } catch(e){ return {}; } });
  check('template aceito: marca enviado com ok=true', reg15b['ORC1'] && reg15b['ORC1'].ok === true, reg15b['ORC1']);
  check('template aceito: msgId gravado como evidência',
    evt15b.some(e => e.tipo === 'template' && e.msgId), evt15b.filter(e => e.tipo === 'template'));

  // ════ CENÁRIO 16: falha de template vira conflito para um humano ════
  console.log('▶ Cenário 16 — template não entregue abre conflito');
  KV['wa_credenciais'] = { token:'mock', phoneId:'123' };
  KV['wa_bot_config'] = { modoAberto:true, execTels:[] };
  KV['wa_bot_pausados'] = {}; KV['tv_logistica'] = { fichas: [] };
  const fOrc = (id, nome) => ({ id, nome, telefone:'55319875372' + id.slice(-2),
    equipamento:'Purificador', phase:'orc_registrado',
    diagnostico:{ equips:[{tipo:'purificador',modelo:'PE11X',servicos:['Gás']}],
      textoOrc:'Orcamento: fica em 350 reais', em:new Date().toISOString() } });

  // 16a: erro PERMANENTE (template não existe) -> conflito NA HORA, sem insistir
  KV['reparoeletro_logistica'] = { fichas: [fOrc('OC10','Bruna')] };
  KV['wa_orc_enviados'] = { ids: {} };
  global.__fetchLog.length = 0;
  global.__forcarErroGraph = { error: { message: 'Template name does not exist in the translation', code: 132001 } };
  await wabot(req({ action:'orcamentos-pendentes', ...K }), res());
  delete global.__forcarErroGraph;
  check('erro permanente: abre conflito na PRIMEIRA falha',
    global.__fetchLog.some(u => u.includes('criar-conflito')), global.__fetchLog.slice(-3));
  const r16 = ((KV['wa_orc_enviados']||{}).ids||{})['OC10'];
  check('erro permanente: marca para NÃO insistir', r16 && r16.permanente === true, r16);

  // 16b: erro PASSAGEIRO -> tenta de novo, não abre conflito na primeira
  KV['reparoeletro_logistica'] = { fichas: [fOrc('OC11','Carla')] };
  KV['wa_orc_enviados'] = { ids: {} };
  global.__fetchLog.length = 0;
  global.__forcarErroGraph = { error: { message: 'Rate limit hit', code: 130429 } };
  await wabot(req({ action:'orcamentos-pendentes', ...K }), res());
  check('erro passageiro: NÃO abre conflito na 1ª falha',
    !global.__fetchLog.some(u => u.includes('criar-conflito')));
  const r16b = ((KV['wa_orc_enviados']||{}).ids||{})['OC11'];
  check('erro passageiro: contou a falha para tentar de novo', r16b && r16b.falhas === 1, r16b);
  // 2ª e 3ª tentativa -> conflito
  await wabot(req({ action:'orcamentos-pendentes', ...K }), res());
  await wabot(req({ action:'orcamentos-pendentes', ...K }), res());
  delete global.__forcarErroGraph;
  check('erro passageiro: abre conflito após 3 tentativas',
    global.__fetchLog.some(u => u.includes('criar-conflito')), global.__fetchLog.slice(-3));

  // 16c: sucesso não abre conflito nenhum
  KV['reparoeletro_logistica'] = { fichas: [fOrc('OC12','Denise')] };
  KV['wa_orc_enviados'] = { ids: {} };
  global.__fetchLog.length = 0;
  await wabot(req({ action:'orcamentos-pendentes', ...K }), res());
  check('sucesso: nenhum conflito aberto', !global.__fetchLog.some(u => u.includes('criar-conflito')));

  // ════ CENÁRIO 17: reenvio após bloqueio de pagamento da Meta ════
  console.log('▶ Cenário 17 — bloqueio de pagamento: levanta e devolve para reenvio');
  const falhaTxt = 'failed | [{"code":131042,"title":"Business eligibility payment issue"}]';
  LISTS['wa_evt_list'] = [
    { ts:'2026-08-03T13:01:00.000Z', tel:'5531990001111', dir:'status', texto: falhaTxt },
    { ts:'2026-08-03T13:02:00.000Z', tel:'5531990002222', dir:'status', texto: falhaTxt },
    { ts:'2026-08-03T13:03:00.000Z', tel:'5531990003333', dir:'status', texto: falhaTxt },
    // 3333 respondeu DEPOIS da falha → já foi alcançado, não deve reenviar
    { ts:'2026-08-03T14:00:00.000Z', tel:'5531990003333', dir:'in', texto:'oi' },
    { ts:'2026-08-03T13:04:00.000Z', tel:'5531990004444', dir:'status', texto:'failed | [{"code":131047}]' },
    { ts:'2026-08-03T13:05:00.000Z', tel:'5531990005555', dir:'status', texto:'delivered' },
  ].map(o => JSON.stringify(o));
  KV['fichas_adm'] = { fichas: [
    { id:'FP1', nome:'Um',   telefone:'5531990001111', status:'contato_feito', abordadoPorBot:true, contatoFeitoEm:'2026-08-03T13:00:00.000Z' },
    { id:'FP2', nome:'Dois', telefone:'5531990002222', status:'contato_feito', abordadoPorBot:true, contatoFeitoEm:'2026-08-03T13:00:00.000Z' },
    { id:'FP3', nome:'Tres', telefone:'5531990003333', status:'contato_feito', abordadoPorBot:true, contatoFeitoEm:'2026-08-03T13:00:00.000Z' },
    { id:'FP5', nome:'Cinco',telefone:'5531990005555', status:'contato_feito', abordadoPorBot:true, contatoFeitoEm:'2026-08-03T13:00:00.000Z' },
  ] };
  KV['fichas_tv'] = { fichas: [] };
  KV['wa_abordados'] = { tels: { '90001111':'x', '90002222':'x', '90003333':'x', '90005555':'x' } };
  KV['wa_orc_enviados'] = { ids: { 'O1': { telefone:'5531990001111', ok:true }, 'O9': { telefone:'5531990005555', ok:true } } };

  const q17 = res();
  await wabot(req({ action:'bloqueio-pagamento', ...K }), q17);
  const t17 = String(q17.dado || '');
  check('leitura: identifica as falhas de pagamento', /131042|PENDÊNCIA DE PAGAMENTO/.test(t17), t17.slice(0,80));
  check('leitura: NÃO altera nada (fichas intactas)',
    KV['fichas_adm'].fichas.every(f => f.status === 'contato_feito'));
  check('leitura: quem respondeu depois fica de fora do reenvio', /REENVIAR \(bloqueio de conta\)=2/.test(t17) && /j[áa] falaram depois \(n[ãa]o mexer\)=1/.test(t17), t17.split('\n')[4]);

  const q17b = res();
  await wabot(req({ action:'bloqueio-pagamento', aplicar:'1', ...K }), q17b);
  const t17b = String(q17b.dado || '');
  const fichas17 = KV['fichas_adm'].fichas;
  check('aplicar: ficha atingida volta para "criada"',
    fichas17.find(f => f.id === 'FP1').status === 'criada', fichas17.find(f => f.id === 'FP1'));
  check('aplicar: quem respondeu depois NÃO é mexido',
    fichas17.find(f => f.id === 'FP3').status === 'contato_feito', fichas17.find(f => f.id === 'FP3'));
  check('aplicar: quem foi ENTREGUE não é mexido',
    fichas17.find(f => f.id === 'FP5').status === 'contato_feito', fichas17.find(f => f.id === 'FP5'));
  check('aplicar: registro de "já abordado" limpo só dos atingidos',
    !KV['wa_abordados'].tels['90001111'] && !!KV['wa_abordados'].tels['90005555'], KV['wa_abordados'].tels);
  check('aplicar: orçamento do atingido liberado, o entregue mantido',
    !KV['wa_orc_enviados'].ids['O1'] && !!KV['wa_orc_enviados'].ids['O9'], KV['wa_orc_enviados'].ids);
  check('aplicar: relatório confirma o que foi feito', /fichas devolvidas para abordagem=2/.test(t17b), t17b);

  // ════ CENÁRIO 18: reenvio separa pagamento de número inválido ════
  console.log('▶ Cenário 18 — 131042 reenvia · 131026 vira conflito (não insiste)');
  const fPag = 'failed | [{"code":131042,"title":"Business eligibility payment issue"}]';
  const fInv = 'failed | [{"code":131026,"title":"Message undeliverable"}]';
  LISTS['wa_evt_list'] = [
    { ts:'2026-08-03T11:00:00.000Z', tel:'5531990001111', dir:'status', texto: fPag },
    { ts:'2026-08-03T11:00:00.000Z', tel:'5531999451058', dir:'status', texto: fInv },
    { ts:'2026-08-03T11:05:00.000Z', tel:'5531999451058', dir:'status', texto: fInv },
  ].map(o => JSON.stringify(o));
  KV['fichas_adm'] = { fichas: [
    { id:'PG1', nome:'Pagamento', telefone:'5531990001111', status:'contato_feito', abordadoPorBot:true, contatoFeitoEm:'2026-08-03T10:59:00.000Z' },
    { id:'IN1', nome:'Invalido',  telefone:'5531999451058', status:'contato_feito', abordadoPorBot:true, contatoFeitoEm:'2026-08-03T10:59:00.000Z' },
  ] };
  KV['fichas_tv'] = { fichas: [] };
  KV['wa_abordados'] = { tels: { '90001111':'x', '99451058':'x' } };
  KV['wa_orc_enviados'] = { ids: {} };
  global.__fetchLog.length = 0;

  const q18 = res();
  await wabot(req({ action:'bloqueio-pagamento', aplicar:'1', ...K }), q18);
  const t18 = String(q18.dado || '');
  const f18 = KV['fichas_adm'].fichas;
  check('131042: ficha volta para reenvio automático',
    f18.find(f => f.id === 'PG1').status === 'criada', f18.find(f => f.id === 'PG1'));
  check('131026: ficha NÃO volta para a esteira (reenviar não resolve)',
    f18.find(f => f.id === 'IN1').status === 'contato_feito', f18.find(f => f.id === 'IN1'));
  check('131026: abre conflito para alguém LIGAR',
    global.__fetchLog.some(u => u.includes('criar-conflito')), global.__fetchLog);
  check('131026: continua bloqueado para o bot não insistir',
    !!KV['wa_abordados'].tels['99451058'], KV['wa_abordados'].tels);
  check('relatório separa os dois grupos', /inv[áa]lid/i.test(t18) && /reenvio/i.test(t18), t18.slice(0, 200));

  // ════ CENÁRIO 19: orçamento só sai da fila manual se a Meta entregou ════
  console.log('▶ Cenário 19 — envio recusado mantém o orçamento na aba Orçamento');
  KV['wa_credenciais'] = { token:'mock', phoneId:'123' };
  KV['wa_bot_config'] = { modoAberto:true, execTels:[] };
  KV['wa_bot_pausados'] = {}; KV['tv_logistica'] = { fichas: [] };
  const fOrc19 = () => ({ id:'OR19', nome:'Cliente 19', telefone:'5531990019999',
    equipamento:'Purificador', phase:'orc_registrado',
    diagnostico:{ equips:[{tipo:'purificador',modelo:'PE11X',servicos:['Gás']}],
      textoOrc:'Orcamento: fica em 350 reais', em:new Date().toISOString() } });

  // 19a: Meta RECUSA → o card NÃO pode ser marcado como enviado
  KV['reparoeletro_logistica'] = { fichas: [fOrc19()] };
  KV['reparoeletro_orcamentos'] = { fichas: [{ id:'ORC-19', tel:'5531990019999', nome:'Cliente 19', status:'pendente', precoSugerido:'350', textoOrc:'x' }], syncedIds: [] };
  KV['wa_orc_enviados'] = { ids: {} };
  global.__fetchLog.length = 0;
  global.__forcarErroGraph = { error: { message: 'unsettled payments', code: 131042 } };
  await wabot(req({ action:'orcamentos-pendentes', ...K }), res());
  delete global.__forcarErroGraph;
  check('recusado: NÃO chamou orc-enviar (card fica na aba Orçamento)',
    !global.__fetchLog.some(u => u.includes('orc-enviar')), global.__fetchLog.filter(u => u.includes('orc')));

  // 19b: Meta ACEITA → marca como enviado normalmente
  KV['reparoeletro_logistica'] = { fichas: [fOrc19()] };
  KV['reparoeletro_orcamentos'] = { fichas: [{ id:'ORC-19', tel:'5531990019999', nome:'Cliente 19', status:'pendente', precoSugerido:'350', textoOrc:'x' }], syncedIds: [] };
  KV['wa_orc_enviados'] = { ids: {} };
  global.__fetchLog.length = 0;
  await wabot(req({ action:'orcamentos-pendentes', ...K }), res());
  check('aceito: chamou orc-enviar (card vai para Enviado)',
    global.__fetchLog.some(u => u.includes('orc-enviar')), global.__fetchLog.filter(u => u.includes('orc')));

  // ════ CENÁRIO 20: devolver à fila manual os orçamentos que a Meta recusou ════
  console.log('▶ Cenário 20 — devolver orçamentos marcados como enviados por engano');
  const orc20 = carregarHandler('api/orcamento.js');
  const fal = 'failed | [{"code":131042,"title":"Business eligibility payment issue"}]';
  LISTS['wa_evt_list'] = [
    { ts:'2026-08-01T11:00:00.000Z', tel:'5531990020001', dir:'status', texto: fal },
    { ts:'2026-08-02T11:00:00.000Z', tel:'5531990020002', dir:'status', texto: fal },
    { ts:'2026-08-03T11:00:00.000Z', tel:'5531990020004', dir:'status', texto:'delivered' },
    // 0005 falhou mas o cliente respondeu depois — já foi alcançado
    { ts:'2026-08-03T11:00:00.000Z', tel:'5531990020005', dir:'status', texto: fal },
    { ts:'2026-08-03T12:00:00.000Z', tel:'5531990020005', dir:'in', texto:'oi' },
  ].map(o => JSON.stringify(o));
  KV['reparoeletro_orcamentos'] = { fichas: [
    { id:'O1', tel:'5531990020001', nome:'Falhou Um',  status:'enviado', enviadoAt:'2026-08-01T11:00:05.000Z', preco:'350' },
    { id:'O2', tel:'5531990020002', nome:'Falhou Dois',status:'enviado', enviadoAt:'2026-08-02T11:00:05.000Z', preco:'370' },
    { id:'O3', tel:'5531990020003', nome:'Antes',      status:'enviado', enviadoAt:'2026-07-25T10:00:00.000Z', preco:'350' },
    { id:'O4', tel:'5531990020004', nome:'Entregue',   status:'enviado', enviadoAt:'2026-08-03T11:00:05.000Z', preco:'350' },
    { id:'O5', tel:'5531990020005', nome:'Respondeu',  status:'enviado', enviadoAt:'2026-08-03T11:00:05.000Z', preco:'350' },
    { id:'O6', tel:'5531990020006', nome:'Ja pendente',status:'pendente', preco:null },
  ], syncedIds: [] };
  KV['tv_orcamentos'] = { fichas: [] };

  const s20 = res();
  await orc20(req({ action:'devolver-nao-entregues', ...K }), s20);
  const t20 = String(s20.dado || '');
  check('leitura: identifica os 2 que falharam', /devolver=2/.test(t20), t20.split('\n')[0]);
  check('leitura: NÃO altera nada', KV['reparoeletro_orcamentos'].fichas.find(f => f.id === 'O1').status === 'enviado');

  const s20b = res();
  await orc20(req({ action:'devolver-nao-entregues', aplicar:'1', ...K }), s20b);
  const fx = id => KV['reparoeletro_orcamentos'].fichas.find(f => f.id === id);
  check('aplicar: falhou 01/08 volta para pendente', fx('O1').status === 'pendente', fx('O1'));
  check('aplicar: falhou 02/08 volta para pendente', fx('O2').status === 'pendente', fx('O2'));
  check('aplicar: enviado ANTES do bloqueio não é mexido', fx('O3').status === 'enviado', fx('O3'));
  check('aplicar: ENTREGUE não é mexido', fx('O4').status === 'enviado', fx('O4'));
  check('aplicar: cliente que respondeu depois não é mexido', fx('O5').status === 'enviado', fx('O5'));
  check('aplicar: já pendente continua pendente', fx('O6').status === 'pendente', fx('O6'));
  check('aplicar: guarda o motivo da devolução', !!fx('O1').devolvidoEm && /131042|recus/i.test(String(fx('O1').devolvidoMotivo||'')), fx('O1'));

  // ════ Resultado ════
  console.log('\n═══════════════════════════════════');
  const _mj = (() => { const b = new Date(Date.now() - 3 * 3600000); const d = b.getUTCDay(), hh = b.getUTCHours(); return (d >= 1 && d <= 5) ? (hh >= 8 && hh < 15) : (d === 6 ? (hh >= 8 && hh < 10) : false); })();
  console.log(`   Modo: ${_mj ? 'DENTRO da janela comercial (dedupe do bot TESTADO)' : 'FORA da janela comercial (dedupe do bot NÃO testado — rode tb. em horário comercial)'}`);
  console.log(falha === 0 ? `🟢 VERDE — ${passa} testes passaram. Liberado para a janela de deploy.` : `🔴 VERMELHO — ${falha} falha(s), ${passa} ok. NÃO SUBIR PARA PRODUÇÃO.`);
  console.log('═══════════════════════════════════\n');
  process.exit(falha === 0 ? 0 : 1);
})();
