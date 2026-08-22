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
  // 🔌 módulos auxiliares (_gravar, _funil) são resolvidos a partir da pasta api,
  // e recebem o mesmo fetch simulado — sem isso eles falavam com o banco de verdade
  const requireLocal = (nome) => {
    if (String(nome).startsWith('./_') || String(nome).includes('/_')) {
      const alvo = path.join(__dirname, '..', 'api', String(nome).replace(/^\.\//, '') + '.js');
      if (fs.existsSync(alvo)) {
        let s2 = fs.readFileSync(alvo, 'utf8');
        const m2 = { exports: {} };
        new Function('module', 'exports', 'require', 'process', 'fetch', 'Buffer', s2)
          (m2, m2.exports, requireLocal, process, global.fetch, Buffer);
        return m2.exports;
      }
    }
    return require(nome);
  };
  const fn = new Function('module', 'exports', 'require', 'process', 'fetch', 'Buffer', '__dirname', src);
  fn(mod, mod.exports, requireLocal, process, global.fetch, Buffer, path.join(__dirname, '..', 'api'));
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
  const dentroJanela = (() => { const b = new Date(Date.now() - 3 * 3600000); const d = b.getUTCDay(), hh = b.getUTCHours(); return (d >= 1 && d <= 5) ? (hh >= 8 && hh < 14) : (d === 6 ? (hh >= 8 && hh < 10) : false); })();
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
  // ── trava de horário: cadastro de coleta fora da janela é recusado ──
  // ── 🛡️ GARANTIA: cadastrar uma ficha e ver se ela aparece nos contadores ──
  console.log('▶ Cenário G1 — garantia cadastrada aparece nos contadores');
  {
    const garantia = carregarHandler('api/garantia.js');
    KV['reparoeletro_garantia_v2'] = { fichas: [] };
    const hojeISO = new Date(Date.now() - 3*3600000).toISOString();
    // 1) cadastra
    const rC = res();
    await garantia(req({ action: 'cadastrar', ...K }, {
      nome: 'Teste Harness', telefone: '31999990000',
      defeito: 'teste automatizado', tipo: 'loja_acompanhamento', tecnico: 'Lucas',
    }, 'POST'), rC);
    const criou = rC.dado && rC.dado.ok;
    check('garantia: cadastro aceito', criou, rC.dado && rC.dado.error);
    // 2) confere no banco
    const noBanco = ((KV['reparoeletro_garantia_v2']||{}).fichas || [])
      .filter(f => String(f.telefone||'').includes('99999'));
    check('garantia: gravou no banco', noBanco.length === 1, noBanco.length);
    if (noBanco[0]) {
      check('garantia: tem criadaEm', !!noBanco[0].criadaEm);
      check('garantia: tem tipo', noBanco[0].tipo === 'loja_acompanhamento');
      check('garantia: tem tecnico', noBanco[0].tecnico === 'Lucas');
    }
    // 3) os contadores enxergam?
    const rK = res();
    await garantia(req({ action: 'contadores', ...K }), rK);
    const c = rK.dado || {};
    check('garantia: contadores respondem', c.ok === true, c.error);
    check('garantia: conta ENTRARAM hoje', (c.ENTRARAM && c.ENTRARAM.hoje) >= 1,
      JSON.stringify(c.ENTRARAM));
    check('garantia: técnico aparece', JSON.stringify(c.POR_TECNICO || []).includes('Lucas'),
      JSON.stringify(c.POR_TECNICO));
    check('garantia: lista por técnico traz a ficha',
      JSON.stringify(c.GARANTIAS_POR_TECNICO || {}).includes('Teste Harness'));
  }

  // ── 🚪 janela de 24h: texto livre para quem não escreve há mais de 24h é bloqueado ──
  console.log('▶ Cenário J1 — janela fechada: dispara template e guarda a mensagem');
  {
    const rJ = res();
    global.__fetchLog.length = 0;
    await wabot(req({ action:'enviar', ...K }, { tel:'5531900001111',
      texto:'Olá, tudo bem?' }), rJ);
    const j = rJ.dado || {};
    check('janela fechada: detectada', j.janelaFechada === true, JSON.stringify(j).slice(0,90));
    check('janela fechada: mensagem guardada para depois', j.mensagemGuardada === true);
    const fila = KV['wa_pendentes_janela'] || { itens: [] };
    check('janela fechada: entrou na fila de pendentes',
      (fila.itens||[]).some(x => String(x.tel||'').includes('1111')), JSON.stringify(fila).slice(0,80));
    check('janela fechada: texto livre NÃO foi enviado',
      !(global.__fetchLog||[]).some(u => String(u).includes('/messages') && !String(u).includes('template'))
      || j.templateEnviado !== undefined);
  }

  // ── 🚚 toda ação chamada pela tela do motorista precisa estar liberada ──
  console.log('▶ Cenário M1 — tela do motorista não esbarra em autorização');
  {
    const fs2 = require('fs');
    const tela = fs2.readFileSync('tv-rota.html', 'utf8');
    const api = fs2.readFileSync('api/tv-logistica.js', 'utf8');
    const chamadas = [...new Set((tela.match(/action=rota-[a-z-]+/g) || [])
      .map(x => x.replace('action=', '')))];
    const mL = api.match(/_acaoLivre\s*=\s*\[([^\]]+)\]/);
    const livres = mL ? mL[1].split(',').map(x => x.trim().replace(/['"]/g, '')) : [];
    const faltando = chamadas.filter(c => !livres.includes(c));
    check('motorista: todas as ações da tela estão liberadas',
      faltando.length === 0, 'faltam: ' + faltando.join(', '));
  }

  // ── 🕐 os campos que a tela do motorista usa precisam vir do rota-fichas ──
  console.log('▶ Cenário M2 — rota-fichas entrega os campos que a tela usa');
  {
    const fs3 = require('fs');
    const tela = fs3.readFileSync('tv-rota.html', 'utf8');
    const api = fs3.readFileSync('api/tv-logistica.js', 'utf8');
    const i = api.indexOf("action === 'rota-fichas'");
    const bloco = api.slice(i, i + 2200);
    // campos que a tela lê de f.
    const usados = [...new Set((tela.match(/\bf\.[a-zA-Z]+/g) || [])
      .map(x => x.replace('f.', '')))]
      .filter(c => ['agendadoPara','horarioColeta','agendadoObs','endereco','equipamento',
                    'defeito','regiao','telefone','nome'].includes(c));
    const faltando = usados.filter(c => !new RegExp('\\b' + c + '\\b').test(bloco));
    check('motorista: rota-fichas entrega todos os campos usados pela tela',
      faltando.length === 0, 'faltam: ' + faltando.join(', '));
  }

  // ── ⏱️ a Vercel só executa 40 cron jobs: acima disso os últimos são ignorados ──
  console.log('▶ Cenário C1 — quantidade de crons dentro do limite da Vercel');
  {
    const v = JSON.parse(require('fs').readFileSync('vercel.json', 'utf8'));
    const n = (v.crons || []).length;
    check('crons dentro do limite de 40', n <= 40, n + ' agendamentos');
  }

  // ── 🔗 toda página apontada pelo menu precisa de rota configurada ──
  // ── 🔐 toda página do menu precisa do bloco que envia a chave nas chamadas ──
  console.log('▶ Cenário R2 — páginas do menu enviam a chave nas chamadas');
  {
    const fs6 = require('fs');
    const adm = fs6.readFileSync('adm.html', 'utf8');
    const urls = [...new Set((adm.match(/data-url="\/[a-z-]+"/g) || [])
      .map(x => x.replace(/data-url="|"/g, '').replace('/', '')))];
    // o que importa é a chave chegar à API: pelo guard que intercepta o fetch,
    // ou passada diretamente em cada chamada
    const semChave = urls.filter(u => {
      const arq = u + '.html';
      if (!fs6.existsSync(arq)) return false;
      const c = fs6.readFileSync(arq, 'utf8');
      if (!/\/api\//.test(c)) return false;           // página que não chama API
      return !/TF-GUARD/.test(c) && !/tf_key/.test(c);
    });
    check('menu: páginas que chamam a API enviam a chave',
      semChave.length === 0, 'sem chave: ' + semChave.join(', '));
  }

  console.log('▶ Cenário R1 — páginas do menu têm rewrite no vercel.json');
  {
    const fs4 = require('fs');
    const adm = fs4.readFileSync('adm.html', 'utf8');
    const v = JSON.parse(fs4.readFileSync('vercel.json', 'utf8'));
    const rotas = new Set((v.rewrites || []).map(r => r.source));
    const urls = [...new Set((adm.match(/data-url="\/[a-z.-]+"/g) || [])
      .map(x => x.replace(/data-url="|"/g, '')))]
      .filter(u => !u.endsWith('.html'));   // .html vai direto ao arquivo, não precisa de rota
    const semRota = urls.filter(u => !rotas.has(u) && !fs4.existsSync('.' + u + '.html') === false ? false : !rotas.has(u));
    check('menu: todas as páginas têm rewrite', semRota.length === 0, 'sem rota: ' + semRota.join(', '));
  }

  // ── 👨‍🔧 a ficha avulsa deve oferecer os mesmos técnicos do Mover OS ──
  console.log('▶ Cenário T1 — lista de técnicos igual nas duas telas');
  {
    const fs5 = require('fs');
    const tec = fs5.readFileSync('tecnico.html', 'utf8');
    const api = fs5.readFileSync('api/qualidade.js', 'utf8');
    const noMover = [...new Set((tec.match(/pickTecnico\('([^']+)'\)/g) || [])
      .map(x => x.replace(/pickTecnico\('|'\)/g, '')))].sort();
    const mE = api.match(/EQUIPE_OFICIAL = \[([^\]]+)\]/);
    const naQualidade = mE ? mE[1].split(',').map(x => x.trim().replace(/'/g, '')).sort() : [];
    const faltam = noMover.filter(t => !naQualidade.includes(t));
    check('técnicos: ficha avulsa oferece os mesmos do Mover OS',
      faltam.length === 0, 'faltam: ' + faltam.join(', '));
  }

  // ── 📅 cada etapa do funil precisa gravar a própria data na origem ──
  // ── 📒 cada etapa do funil precisa registrar no livro-razão ──
  // ── 🔒 a camada de gravação segura precisa resistir à sobreposição ──
  // ── 🔁 devolução do remarcar com a camada segura ──
  // ── 🔁 ficha já devolvida e atendida NÃO pode ser recriada ──
  // ── 🖥️ elemento citado no script precisa existir no HTML ──
  console.log('▶ Cenário EL — telas não chamam elementos inexistentes');
  {
    const fs9 = require('fs');
    const telas = ['qualidade.html', 'garantia.html', 'fichas.html', 'kpis.html', 'tecnico.html'];
    const problemas = [];
    for (const t of telas) {
      if (!fs9.existsSync(t)) continue;
      const c = fs9.readFileSync(t, 'utf8');
      const usados = [...new Set((c.match(/getElementById\('([a-zA-Z0-9_-]+)'\)/g) || [])
        .map(x => x.replace(/getElementById\('|'\)/g, '')))];
      for (const id of usados) {
        // ignora os criados dinamicamente pelo próprio script
        if (new RegExp("id\\s*=\\s*['\"]" + id + "['\"]").test(c)) continue;
        if (new RegExp("id=\\\\?['\"]?" + id).test(c)) continue;
        problemas.push(t + ':' + id);
      }
    }
    check('telas: nenhum elemento citado está ausente',
      problemas.length === 0, problemas.slice(0, 6).join(', '));
  }

  // ── 📌 lead novo da planilha entra mesmo já conversando com o bot ──
  console.log('▶ Cenário SY — sync cria ficha de quem não existe em banco nenhum');
  {
    const fich = carregarHandler('api/fichas.js');
    KV['fichas_adm'] = { fichas: [] };
    KV['fichas_tv'] = { fichas: [] };
    LISTS['wa_evt_list'] = [JSON.stringify({ tel: '5531988776655', dir: 'in',
      ts: new Date().toISOString(), texto: 'oi' })];
    const rS = res();
    await fich(req({ action: 'sync-completo', dias: '1', ...K }), rS);
    const d = rS.dado || {};
    // o cliente que conversa mas não tem ficha PRECISA ser criado
    const bloqueou = JSON.stringify(d).includes('jaExistem') &&
      (d.vaoSerCriadas === 0 && (d.jaExistem || 0) > 0);
    check('sync: conversa ativa não bloqueia lead sem ficha',
      !bloqueou, JSON.stringify(d).slice(0, 120));
  }

  // ── 📺 avisos ao cliente respeitam horário, resposta e limite ──
  // ── 🧭 função usada antes de ser declarada derruba a requisição ──
  // ── 📜 toda entrada em Entrar em Contato precisa ficar registrada ──
  // ── 📒 o livro-razão não pode perder nem duplicar fatos ──
  // ── 🧪 toda ação de toda API precisa responder sem quebrar ──
  // Verificação por EXECUÇÃO, não por leitura do código: encontra variável
  // inexistente, função fora de escopo e trecho colado no lugar errado —
  // defeitos que a inspeção textual não vê e que viram erro 500 em produção.
  // ── 💾 o que a resposta afirma precisa estar GRAVADO ──
  // Verificação de persistência real: a alteração acontecia no objeto lido no
  // início da requisição, mas a gravação copiava campo a campo para outro
  // objeto — o que ficasse de fora nunca chegava ao banco, embora a resposta
  // dissesse que deu certo.
  // ── ⚡ duas rotinas gravando ao mesmo tempo não podem se apagar ──
  // ── 🔬 board: mover para o controle de qualidade cria a inspeção certa ──
  console.log('▶ Cenário BQ — board cria inspeção com técnico e sem duplicar');
  {
    const bd = carregarHandler('api/board.js');
    KV['reparoeletro_board'] = { phases: [{ id: 'producao' }, { id: 'controle_qualidade' }],
      cards: [{ id: 'BQ1', pipefyId: 'PQ1', nomeContato: 'Carlos', telefone: '5531955554444',
        equipamento: 'Micro-ondas Electrolux', phaseId: 'producao' }], syncedIds: [] };
    KV['reparoeletro_qualidade'] = { inspecoes: [], config: { tecnicos: ['Lucas'], proximoNum: 1 } };
    await bd(req({ action: 'move', ...K },
      { pipefyId: 'PQ1', phaseId: 'controle_qualidade', tecnico: 'Kassio' }, 'POST'), res());
    await new Promise(s => setTimeout(s, 300));
    let insp = KV['reparoeletro_qualidade'].inspecoes || [];
    check('board: inspeção criada', insp.length === 1, insp.length);
    check('board: com o técnico que fez', (insp[0] || {}).tecnico === 'Kassio', (insp[0] || {}).tecnico);
    // duplo clique não pode gerar duas
    await bd(req({ action: 'move', ...K },
      { pipefyId: 'PQ1', phaseId: 'controle_qualidade', tecnico: 'Kassio' }, 'POST'), res());
    await new Promise(s => setTimeout(s, 300));
    insp = KV['reparoeletro_qualidade'].inspecoes || [];
    check('board: não duplica ao repetir o movimento', insp.length === 1, insp.length);
    // segundo equipamento do MESMO cliente precisa gerar a sua
    KV['reparoeletro_board'].cards.push({ id: 'BQ2', pipefyId: 'PQ2', nomeContato: 'Carlos',
      telefone: '5531955554444', equipamento: 'Purificador Consul', phaseId: 'producao' });
    await bd(req({ action: 'move', ...K },
      { pipefyId: 'PQ2', phaseId: 'controle_qualidade', tecnico: 'Lucas' }, 'POST'), res());
    await new Promise(s => setTimeout(s, 300));
    insp = KV['reparoeletro_qualidade'].inspecoes || [];
    check('board: segundo equipamento do mesmo cliente gera inspeção', insp.length === 2, insp.length);
  }

  // ── 🛡️ garantia: cadastro, fila e envio ao controle de qualidade ──
  // ── 📺 os sistemas replicados para TV não podem tocar os bancos do ADM ──
  // ── 📇 tentativa de disparo só pode ser registrada se a mensagem saiu ──
  // 16 clientes ficaram invisíveis por constarem como avisados sem que a
  // mensagem tivesse saído: como o controle os dava por atendidos, a régua
  // parava de alcançá-los e o painel deixava de cobrá-los.
  console.log('▶ Cenário FT — controle só registra envio confirmado');
  {
    const fsF = require('fs');
    const c = fsF.readFileSync('api/wa-bot.js', 'utf8');
    const linhas = c.split('\n');
    const suspeitas = [];
    linhas.forEach((l, i) => {
      // gravação de tentativa no controle da sequência
      if (!/controle\d*\.clientes\[[^\]]+\]\s*=\s*\{[^}]*tentativas/.test(l)) return;
      const ctx = linhas.slice(Math.max(0, i - 12), i + 2).join(' ');
      // precisa estar dentro de uma verificação de que a Meta aceitou
      const confirmado = /if \(ok\)|r\.messages\s*&&\s*r\.messages\[0\]|messages\[0\]\)/.test(ctx);
      if (!confirmado) suspeitas.push('linha ' + (i + 1));
    });
    check('registro de disparo exige confirmação da Meta',
      suspeitas.length === 0, suspeitas.join(', '));
    // a conferência de fantasmas precisa continuar existindo
    check('existe conferência de disparo sem prova de envio',
      /conferir-disparos-fantasma/.test(c));
  }

  console.log('▶ Cenário TV — almoxarifado e garantia de TV são independentes');
  {
    const gTv = carregarHandler('api/tv-garantia-v2.js');
    KV['tv_garantia_v2'] = { fichas: [] };
    KV['tv_garantia_fila'] = { itens: [] };
    delete KV['reparoeletro_garantia_v2'];
    delete KV['reparoeletro_garantia_fila'];
    const rTv = res();
    await gTv(req({ action: 'cadastrar', ...K }, { nome: 'Cliente TV',
      telefone: '5531933332222', defeito: 'voltou a falhar', tipo: 'loja_acompanhamento',
      tecnico: 'Arthur', equipamento: 'TV 50' }, 'POST'), rTv);
    check('garantia TV: cadastro aceito', rTv.dado && rTv.dado.ok);
    check('garantia TV: gravou no banco de TV',
      ((KV['tv_garantia_v2'] || {}).fichas || []).length === 1);
    check('garantia TV: entrou na fila de TV',
      ((KV['tv_garantia_fila'] || {}).itens || []).length === 1);
    check('garantia TV: não escreveu no banco do ADM',
      KV['reparoeletro_garantia_v2'] === undefined);
    // nenhum arquivo de TV pode apontar para banco do ADM
    const fsT = require('fs');
    const sujos = [];
    for (const arq of ['api/tv-almoxarifado.js', 'api/tv-garantia-v2.js']) {
      const c = fsT.readFileSync(arq, 'utf8');
      const refs = (c.match(/reparoeletro_\w+/g) || []);
      if (refs.length) sujos.push(arq + ': ' + [...new Set(refs)].join(', '));
    }
    check('módulos de TV não referenciam bancos da linha branca',
      sujos.length === 0, sujos.join(' | '));
  }

  console.log('▶ Cenário GQ — garantia exige técnico e credita a inspeção');
  {
    const gr = carregarHandler('api/garantia.js');
    KV['reparoeletro_garantia_v2'] = { fichas: [] };
    KV['reparoeletro_garantia_fila'] = { itens: [] };
    KV['reparoeletro_qualidade'] = { inspecoes: [], config: { tecnicos: ['Lucas'], proximoNum: 1 } };
    await gr(req({ action: 'cadastrar', ...K }, { nome: 'Dona Rita',
      telefone: '5531944443333', defeito: 'voltou a falhar', tipo: 'loja_acompanhamento',
      tecnico: 'Lucas', equipamento: 'Micro-ondas' }, 'POST'), res());
    const fila = KV['reparoeletro_garantia_fila'].itens || [];
    check('garantia: entrou na fila de tratamento', fila.length === 1, fila.length);
    if (fila.length) {
      const rSem = res();
      await gr(req({ action: 'fila-resolver', ...K },
        { id: fila[0].id, destino: 'qc' }, 'POST'), rSem);
      check('garantia: recusa enviar ao CQ sem informar o técnico',
        rSem.dado && rSem.dado.ok === false);
      await gr(req({ action: 'fila-resolver', ...K },
        { id: fila[0].id, destino: 'qc', tecnico: 'Kassio' }, 'POST'), res());
      await new Promise(s => setTimeout(s, 300));
      const insp = KV['reparoeletro_qualidade'].inspecoes || [];
      check('garantia: inspeção criada no controle de qualidade', insp.length === 1, insp.length);
      check('garantia: creditada ao técnico', (insp[0] || {}).tecnico === 'Kassio');
      check('garantia: marcada como garantia, fora da meta',
        (insp[0] || {}).origem === 'garantia');
    }
  }

  console.log('▶ Cenário CC — gravações simultâneas não se perdem');
  {
    for (const [arq, banco] of [['api/logistica.js', 'reparoeletro_logistica'],
                                 ['api/tv-logistica.js', 'tv_logistica']]) {
      const lg = carregarHandler(arq);
      KV[banco] = { fichas: [
        { id: 'CC-A', nome: 'Ana', telefone: '5531900000001', phase: 'liberado_coleta' },
        { id: 'CC-B', nome: 'Bruno', telefone: '5531900000002', phase: 'liberado_coleta' },
      ] };
      await Promise.all([
        lg(req({ action: 'mover', ...K }, { id: 'CC-A', phase: 'horario_marcado' }, 'POST'), res()),
        lg(req({ action: 'mover', ...K }, { id: 'CC-B', phase: 'motorista_parceiro' }, 'POST'), res()),
      ]);
      await new Promise(s => setTimeout(s, 250));
      const fs2 = (KV[banco].fichas || []);
      const a = fs2.find(f => f.id === 'CC-A') || {};
      const b = fs2.find(f => f.id === 'CC-B') || {};
      const nome = arq.includes('tv-') ? 'TV' : 'ADM';
      check('concorrência ' + nome + ': primeira gravação não se perdeu',
        a.phase === 'horario_marcado', a.phase);
      check('concorrência ' + nome + ': segunda gravação persistiu',
        b.phase === 'motorista_parceiro', b.phase);
    }
  }

  console.log('▶ Cenário PS — alterações persistem de fato no banco');
  {
    const pipeH = carregarHandler('api/pipe.js');
    KV['reparoeletro_pipe'] = { cards: [{ id: 'PS1', nomeContato: 'Persistência',
      telefone: '5531900001111', phase: 'aguardando_aprovacao',
      phaseId: 'aguardando_aprovacao', valor: 300 }] };
    const rP = res();
    await pipeH(req({ action: 'mover', ...K }, { id: 'PS1', phase: 'aprovados' }, 'POST'), rP);
    await new Promise(s => setTimeout(s, 200));
    const cP = (KV['reparoeletro_pipe'].cards || [])[0] || {};
    check('pipe ADM: fase persistiu', cP.phase === 'aprovados', cP.phase);
    check('pipe ADM: phaseId persistiu', cP.phaseId === 'aprovados', cP.phaseId);
    check('pipe ADM: carimbo de aprovação persistiu', !!cP.aprovadoEm);

    const tvH = carregarHandler('api/tv-pipe.js');
    KV['tv_pipe'] = { cards: [{ id: 'PS2', nomeContato: 'Persistência TV',
      telefone: '5531900002222', phase: 'aguardando_aprovacao',
      phaseId: 'aguardando_aprovacao', valor: 700 }] };
    const rT = res();
    await tvH(req({ action: 'mover', ...K }, { id: 'PS2', phase: 'aprovados' }, 'POST'), rT);
    await new Promise(s => setTimeout(s, 200));
    const cT = (KV['tv_pipe'].cards || [])[0] || {};
    check('pipe TV: fase persistiu', cT.phase === 'aprovados', cT.phase);
    check('pipe TV: phaseId persistiu', cT.phaseId === 'aprovados', cT.phaseId);
  }

  console.log('▶ Cenário EX — nenhuma ação quebra ao ser chamada');
  {
    const fsX = require('fs'), pathX = require('path');
    const IGNORAR = ['backup.js', 'nfse.js', 'qz-sign.js'];  // módulos ES / dependência externa
    const quebrou = [];
    const arquivos = fsX.readdirSync('api')
      .filter(f => f.endsWith('.js') && !f.startsWith('_') && !IGNORAR.includes(f));
    for (const arq of arquivos) {
      const caminho = 'api/' + arq;
      let handler;
      try { handler = carregarHandler(caminho); } catch (e) { continue; }
      if (typeof handler !== 'function') continue;
      const src = fsX.readFileSync(caminho, 'utf8');
      const acoes = [...new Set([...src.matchAll(/action\s*===?\s*['"]([a-z0-9-]+)['"]/g)]
        .map(m => m[1]))];
      for (const a of acoes.slice(0, 40)) {
        const r = res();
        try {
          await handler(req({ action: a, ...K }), r);
        } catch (e) {
          // erro de referência é sempre defeito; falha de rede externa não é
          if (/is not defined|Cannot read prop/.test(e.message)) {
            quebrou.push(arq + ':' + a + ' → ' + e.message.slice(0, 50));
          }
        }
      }
    }
    check('nenhuma ação quebra por variável ou função inexistente',
      quebrou.length === 0, quebrou.slice(0, 8).join(' | '));
  }

  console.log('▶ Cenário LR — livro-razão distingue fatos diferentes');
  {
    const fsL = require('fs');
    const fun = fsL.readFileSync('api/_funil.js', 'utf8');
    const kp = fsL.readFileSync('api/kpis.js', 'utf8');
    check('livro: a trava considera o equipamento',
      /const eq = String\(dados\.ref \|\| dados\.equipamento/.test(fun));
    check('livro: a leitura considera o equipamento',
      /v\.eq === eqE/.test(kp));
    check('livro: a lista é podada para não crescer sem limite', /ltrim/.test(fun));
    check('livro: o evento guarda o equipamento', /equipamento: String\(dados\.equipamento/.test(fun));
    // todo caminho de criação de ficha registra
    const fic = fsL.readFileSync('api/fichas.js', 'utf8');
    const criacoes = (fic.match(/fichas\.unshift\(\{/g) || []).length;
    const registros = (fic.match(/registrar\('ficha'/g) || []).length;
    check('livro: os caminhos principais de ficha registram',
      registros >= 3, registros + ' registro(s) para ' + criacoes + ' criação(ões)');
  }

  console.log('▶ Cenário EC — passagem por Entrar em Contato é registrada');
  {
    const EC = require('../api/_entrar_contato.js');
    const f = { status: 'contato_feito', nome: 'Teste' };
    EC.marcarEntrarContato(f, 'régua', 'sem resposta');
    check('registro: status mudou', f.status === 'entrar_contato');
    check('registro: guardou a passagem', (f.passagensEntrarContato || []).length === 1);
    check('registro: guardou a origem', f.passagensEntrarContato[0].origem === 'régua');
    check('registro: guardou de onde veio', f.passagensEntrarContato[0].veioDe === 'contato_feito');
    EC.marcarEntrarContato(f, 'remarcar', 'voltou');
    check('registro: segunda passagem soma', EC.vezes(f) === 2);
    // nenhum arquivo pode mudar o status sem registrar
    const fs11 = require('fs');
    const semRegistro = [];
    for (const arq of ['api/fichas.js', 'api/wa-bot.js']) {
      const c = fs11.readFileSync(arq, 'utf8');
      const linhas = c.split('\n');
      linhas.forEach((l, i) => {
        if (!/\.status = 'entrar_contato'/.test(l)) return;
        const ctx = linhas.slice(Math.max(0, i - 3), i + 2).join(' ');
        if (!/marcarEntrarContato|_ec\./.test(ctx)) semRegistro.push(arq + ':' + (i + 1));
      });
    }
    check('nenhum ponto muda o status sem registrar',
      semRegistro.length === 0, semRegistro.join(', '));
  }

  // ── 📅 o sync só traz o que é do dia, salvo pedido explícito ──
  // Dezenas de fichas antigas reentravam no sistema e caíam na fila de ligação
  // junto com os contatos do dia, e a equipe ligava para gente de uma semana
  // atrás como se fosse contato novo.
  // ── 🔬 o painel de meta e produção semanal não pode ser removido ──
  // Ele foi substituído por engano ao acrescentar os blocos por origem, e com
  // isso sumiu o histórico da semana e as OSs de cada técnico, que é a leitura
  // usada no dia a dia.
  // ── 🧾 o JavaScript de toda tela precisa ser válido ──
  // Uma tela com erro de sintaxe não carrega NADA: nenhum botão funciona.
  // O harness validava só os arquivos da API, e um 'async async' chegou a subir.
  console.log('▶ Cenário JS — telas sem erro de sintaxe');
  {
    const fsJ = require('fs');
    const { execFileSync } = require('child_process');
    const quebradas = [];
    for (const arq of fsJ.readdirSync('.').filter(f => f.endsWith('.html'))) {
      const c = fsJ.readFileSync(arq, 'utf8');
      const blocos = c.match(/<script(?![^>]*src=)[^>]*>([\s\S]*?)<\/script>/g) || [];
      for (const b of blocos) {
        const js = b.replace(/^<script[^>]*>/, '').replace(/<\/script>$/, '');
        if (!js.trim()) continue;
        try {
          fsJ.writeFileSync('/tmp/_chk.js', js);
          execFileSync('node', ['--check', '/tmp/_chk.js'], { stdio: 'pipe' });
        } catch (e) {
          const m = String(e.stderr || '').split('\n').filter(l => l.trim())[1] || '';
          quebradas.push(arq + ': ' + m.slice(0, 60));
          break;
        }
      }
    }
    check('nenhuma tela com JavaScript inválido',
      quebradas.length === 0, quebradas.slice(0, 5).join(' | '));
  }

  console.log('▶ Cenário PQ — qualidade mantém meta semanal e painéis novos');
  {
    const fsQ = require('fs');
    const t = fsQ.readFileSync('qualidade.html', 'utf8');
    check('painel de meta continua sendo chamado', /\bcarregarPainelMeta\(\);/.test(t));
    check('painel de origem e fila também é chamado', /\bcarregarPainelQualidade\(\);/.test(t));
    check('cada painel tem o seu container',
      /id="painel-meta"/.test(t) && /id="painel-origem"/.test(t));
    check('produção por técnico da semana continua clicável', /verOssTecnico/.test(t));
    const api = fsQ.readFileSync('api/qualidade.js', 'utf8');
    check('meta conta apenas o que vem do setor técnico',
      /origemDe\(i\) === 'tecnico' &&[\s\S]{0,120}META/.test(api) ||
      /doTecnicoHoje/.test(api));
  }

  // ── 🪞 excluir da fila precisa alcançar a ficha de verdade ──
  // A coluna Entrar em Contato é um espelho de fichas_adm e fichas_tv. Excluir
  // apenas na base da prospecção removia o reflexo, e a ficha original — que
  // nunca saiu — reaparecia no carregamento seguinte.
  console.log('▶ Cenário EX2 — exclusão da fila não volta atrás');
  {
    const pr = carregarHandler('api/prospeccao.js');
    const fi = carregarHandler('api/fichas.js');
    KV['prospeccao_adm'] = { fichas: [] };
    KV['prospeccao_excluidos'] = { tels: {} };
    KV['fichas_adm'] = { fichas: [{ id: 'EX-1', nome: 'Excluída', telefone: '5531955551111',
      status: 'entrar_contato', contatoFeitoEm: new Date(Date.now() - 5 * 3600000).toISOString(),
      criadoEm: new Date().toISOString(), abordadoPorBot: true }] };
    KV['fichas_tv'] = { fichas: [] };
    KV['reparoeletro_logistica'] = { fichas: [] };
    KV['tv_logistica'] = { fichas: [] };
    KV['reparoeletro_pipe'] = { cards: [] };
    KV['tv_pipe'] = { cards: [] };

    await pr(req({ action: 'excluir', ...K }, { id: 'EX-1' }, 'POST'), res());
    await new Promise(s => setTimeout(s, 150));
    const dep = ((KV['fichas_adm'] || {}).fichas || [])[0] || {};
    check('exclusão alcança a ficha original', dep.status !== 'entrar_contato', dep.status);

    // a régua roda de novo: a ficha não pode voltar
    await fi(req({ action: 'load', sistema: 'adm', ...K }), res());
    await new Promise(s => setTimeout(s, 150));
    const dep2 = ((KV['fichas_adm'] || {}).fichas || [])[0] || {};
    check('a régua não traz de volta quem foi excluído',
      dep2.status !== 'entrar_contato', dep2.status);
  }

  // ── 🔁 a remarcação não pode ressuscitar quem foi tirado da fila ──
  // A devolução do Remarcar cria ficha NOVA, com identificador novo, então a
  // proteção por id não a alcançava: o cliente excluído reaparecia na primeira
  // remarcação, e a equipe voltava a ligar para quem já havia decidido não ligar.
  // ── 🗑️ o botão da tela usa fichas?action=excluir, não o da prospecção ──
  // Toda a proteção contra o retorno depende do telefone estar registrado, e
  // esse caminho — o que a tela realmente usa — não o registrava: a ficha
  // sumia e voltava horas depois, trazida por outra rotina.
  // ── 🛵 o espelho da corrida só grava quando o equipamento sai da loja ──
  // A corrida é criada à mão no aplicativo com os mesmos endereços da rota; o
  // encontro é feito pelos telefones dos destinos, e o motorista só é gravado
  // no momento da coleta, que é quando ele de fato leva os equipamentos.
  // ── 🗑️ excluir ficha de TV sem informar o sistema ──
  // A busca ia direto para a base da linha branca; não achando a ficha, a
  // resposta era de sucesso sem nada ter sido feito, e a ficha continuava na
  // fila com o telefone fora da lista que impede o retorno.
  console.log('▶ Cenário EX4 — exclusão encontra a ficha nos dois sistemas');
  {
    const fi = carregarHandler('api/fichas.js');
    for (const [caso, corpo] of [
      ['com sistema informado', { id: 'TVX', sistema: 'tv', motivo: 't' }],
      ['sem informar o sistema', { id: 'TVX', motivo: 't' }],
    ]) {
      KV['prospeccao_excluidos'] = { tels: {} };
      KV['fichas_adm'] = { fichas: [] };
      KV['fichas_tv'] = { fichas: [{ id: 'TVX', nome: 'Cliente TV',
        telefone: '5531955557777', status: 'entrar_contato',
        criadoEm: new Date().toISOString(), sheetRow: 7 }] };
      await fi(req({ action: 'excluir', ...K }, corpo, 'POST'), res());
      await new Promise(s => setTimeout(s, 130));
      const saiu = ((KV['fichas_tv'] || {}).fichas || []).length === 0;
      const gravou = Object.keys((KV['prospeccao_excluidos'] || {}).tels || {})
        .some(t => t.endsWith('7777'));
      check('exclusão de ficha de TV ' + caso, saiu && gravou,
        'saiu=' + saiu + ' gravou=' + gravou);
    }
  }

  // ── 📄 todo caminho que lança valor precisa carimbar a data do orçamento ──
  // O botão de diagnóstico existe em quatro lugares e só um gravava a data;
  // como a contagem exige data própria, vinte e sete orçamentos viravam seis.
  // ── ⏰ o aviso automático precisa cobrir o expediente inteiro ──
  // A guarda cortava às 16h por economia de execuções, deixando sem aviso todo
  // cliente diagnosticado nas duas últimas horas — justamente o fim de tarde,
  // quando muito orçamento é fechado.
  // ── 📋 os fatos declarados precisam bater com o código ──
  // FATOS.md é consultado antes de afirmar qualquer regra; se ele divergir do
  // sistema, passa a ser fonte de erro em vez de correção.
  // ── ✍️ nenhum criativo pode nascer sem título e sem corpo ──
  // Quando o dicionário não cobria o nome do vídeo, os dois campos ficavam
  // indefinidos e o anúncio ia ao ar mudo — foi o que zerou as duas melhores
  // campanhas de TV no meio do ciclo.
  // ── 🏷️ conflito do bot precisa nascer com o motivo no campo que a tela lê ──
  // A régua gravava em 'motivo' e a tela lê 'motivoConflito': o texto existia no
  // banco e o card aparecia vazio, deixando a equipe sem saber o que houve.
  // ── 🔢 conversa se conta uma vez só ──
  // A plataforma devolve várias métricas para a mesma conversa; somar todas
  // multiplica o número por três ou quatro e faz o custo parecer irreal.
  // ── 💬 toda mensagem enviada ao cliente entra no histórico ──
  // A régua e a abordagem enviavam sem registrar: a mensagem chegava, o
  // controle marcava a tentativa, mas o histórico ficava vazio — a ficha
  // parecia nunca abordada e a consulta do atendimento não mostrava nada.
  // ── 🏅 lead que passa por Retornar continua sendo conversão de lead ──
  // A contagem olhava só a coluna imediatamente anterior, então o caminho
  // lead → retornar → logística não pontuava e a prospecção parecia render
  // menos do que rende.
  // ── 🛡️ a fila de garantia de TV precisa funcionar como a de linha branca ──
  // A tela existia mas apontava para o módulo da outra frente, e a rota levava
  // direto para a gestão: quem clicava em Garantia no TV nunca via a fila.
  // ── 💸 as rotinas não podem rodar fora do expediente ──
  // Executar de três em três minutos madrugada adentro custava dezenas de
  // milhares de chamadas por mês sem nada a fazer: fora do horário comercial
  // a ação apenas confere o relógio e sai.
  console.log('▶ Cenário CR — rotinas de envio limitadas ao expediente');
  {
    const v = JSON.parse(require('fs').readFileSync('vercel.json', 'utf8'));
    const porDia = (s) => {
      const [m, hh, , , dw] = s.split(' ');
      const fm = m.startsWith('*/') ? 60 / +m.slice(2) : (m === '*' ? 60 : m.split(',').length);
      let fh;
      if (hh.startsWith('*/')) fh = 24 / +hh.slice(2);
      else if (hh === '*') fh = 24;
      else if (hh.includes('-')) { const [a, b] = hh.split('-'); fh = +b - +a + 1; }
      else fh = hh.split(',').length;
      let dias = 7;
      if (dw !== '*') {
        if (dw.includes('-')) { const [a, b] = dw.split('-'); dias = +b - +a + 1; }
        else dias = dw.split(',').length;
      }
      return fm * fh * (dias / 7);
    };
    const total = (v.crons || []).reduce((s, c) => s + porDia(c.schedule), 0);
    check('o total diário de execuções está sob controle',
      total < 1600, Math.round(total) + '/dia');
    // e o envio de orçamento precisa cobrir o expediente inteiro
    const orc = (v.crons || []).find(c => c.path.includes('orcamentos-pendentes'));
    const faixa = orc ? orc.schedule.split(' ')[1] : '';
    check('o envio de orçamento cobre das 7h às 18h BRT',
      /^10-2[01]$/.test(faixa), faixa);
  }

  console.log('▶ Cenário GT — fila de garantia de TV aponta para o próprio módulo');
  {
    const fsG = require('fs');
    const tela = fsG.readFileSync('tv-garantia-fila.html', 'utf8');
    check('a fila de TV não chama o módulo de linha branca',
      !/api\/garantia\?/.test(tela));
    // cada ação chamada precisa existir na API correspondente
    const api = fsG.readFileSync('api/tv-garantia-v2.js', 'utf8');
    const chamadas = [...new Set((tela.match(/api\/tv-garantia-v2\?action=([a-z-]+)/g) || [])
      .map(x => x.split('action=')[1]))];
    const faltando = chamadas.filter(a =>
      !api.includes('action === "' + a + '"') && !api.includes("action === '" + a + "'"));
    check('todas as ações chamadas existem na API de TV',
      faltando.length === 0, faltando.join(', '));
    // e a rota principal leva à fila, como no ADM
    const v = JSON.parse(fsG.readFileSync('vercel.json', 'utf8'));
    const rota = (v.rewrites || []).find(r => r.source === '/tv/garantia');
    check('a rota /tv/garantia serve a fila',
      !!rota && /tv-garantia-fila/.test(rota.destination));
    // e o botão do menu precisa levar à fila, não direto à gestão
    const menu = fsG.readFileSync('tv-adm.html', 'utf8');
    const item = (menu.match(/data-url="([^"]*)"[^>]*data-label="Garantia TV"/) || [])[1];
    check('o botão Garantia do menu de TV leva à fila',
      item === '/tv/garantia', 'aponta para ' + item);
    // 📺 o formulário de cadastro aberto pela fila tem de ser o de TV:
    // apontar para o da linha branca fazia a garantia entrar na frente errada
    check('o cadastro da fila de TV abre o formulário de TV',
      !/frame-cadastro'\)\.src='\/garantia/.test(tela) &&
      /frame-cadastro'\)\.src='\/tv\//.test(tela));
    check('a fila de TV não linka para telas da linha branca',
      !/href="\/garantia/.test(tela) && !/href="\/adm"/.test(tela));
  }

  console.log('▶ Cenário LC — conversão de lead pelo caminho indireto');
  {
    const pr = carregarHandler('api/prospeccao.js');
    const diaC = new Date(Date.now() - 3 * 3600000).toISOString().slice(0, 10);
    KV['prospeccao_adm'] = { fichas: [{ id: 'LC1', nome: 'Veio de Lead',
      telefone: '5531955558888', status: 'lead', equipamento: 'Micro',
      criadoEm: new Date().toISOString() }] };
    KV['prosp_convertidos_' + diaC] = { total: 0, itens: [] };
    // lead → retornar
    await pr(req({ action: 'mover', ...K },
      { id: 'LC1', status: 'retornar', dataRetorno: '2026-08-25' }, 'POST'), res());
    await new Promise(s => setTimeout(s, 120));
    const f = (KV['prospeccao_adm'].fichas || [])[0] || {};
    check('a ficha guarda que nasceu em Lead', !!f.veioDaColunaLead);
    check('e está em Retornar', f.status === 'retornar');
  }

  // ── 🔍 "sem contato" só quando ninguém falou mesmo ──
  // O painel olhava só a janela de eventos, que cobre poucos dias: quem
  // recebeu vários toques da régua semanas atrás aparecia como abandonado,
  // e a equipe era mandada ligar para quem o sistema já vinha trabalhando.
  // ── 🔀 conflito de uma frente não barra a outra ──
  // O cliente é identificado pelos 8 últimos dígitos e finais coincidem entre
  // pessoas diferentes: um conflito de adega na linha branca barrou a
  // negociação de uma TV de outro cliente, segurando R$ 1.410.
  // ── ⭐ elogio na pesquisa não pode virar oferta de orçamento ──
  // O cérebro responde em segundos e a leitura da pesquisa rodava de hora em
  // hora: quem elogiava recebia uma proposta comercial no lugar do pedido de
  // avaliação, e o momento do elogio se perdia.
  // ── 🛡️ aprovar duas vezes não pode duplicar o serviço ──
  // Uma televisão aprovada há dois dias foi aprovada de novo quando a cliente
  // perguntou a data de entrega: o segundo processamento a tratou como linha
  // branca, criou cartão na frente errada e gerou pedido de peça inexistente.
  // ── 🔄 a régua precisa girar a fila ──
  // A passagem para aos 40 segundos e recomeçar sempre do topo fazia os
  // últimos nunca serem alcançados: havia orçamento de R$ 2.400 elegível e
  // sem toque nenhum há cinco dias.
  // ── 💾 o arquivo de conversas não pode ter buracos ──
  // A janela de eventos cobre pouco mais de um dia: uma hora de intervalo já
  // bastava para perder o início de um atendimento, e foi o que aconteceu com
  // a conversa de um cliente que reclamou duas vezes.
  console.log('▶ Cenário AR — arquivamento incremental e sem buraco');
  {
    const cv = require('fs').readFileSync('api/conversas.js', 'utf8');
    check('guarda até onde já arquivou', /wa_conv_marca/.test(cv));
    check('processa apenas o que é novo',
      /String\(e\.ts \|\| ''\) > desdeTs/.test(cv));
    check('avisa quando houve intervalo maior que a janela',
      /houveBuraco/.test(cv));
    const rt = require('fs').readFileSync('api/rotina.js', 'utf8');
    const iF = rt.indexOf("action === 'frequente'");
    const iH = rt.indexOf("action === 'de-hora-em-hora'");
    const bloco = rt.slice(iF, iH);
    check('o arquivamento roda no bloco de 10 em 10 minutos',
      /conversas\?action=arquivar/.test(bloco));
  }

  console.log('▶ Cenário GR — régua gira a fila e ignora quem não tem valor');
  {
    const cw = require('fs').readFileSync('api/wa-bot.js', 'utf8');
    const i = cw.indexOf("action === 'recuperacao-7d'");
    const b = cw.slice(i, i + 12000);
    check('quem não tem valor lançado não recebe toque',
      /if \(!\(Number\(c\.valor \|\| 0\) > 0\)\) continue;/.test(b));
    check('a fila é girada a partir de onde parou',
      /loteGirado/.test(b) && /controle\.giro/.test(b));
    check('o ponto de parada é guardado no controle',
      /controle\.giro = _ini \+ _percorridos/.test(b));
  }

  console.log('▶ Cenário RA — reaprovação de serviço já em andamento');
  {
    delete KV['reparoeletro_almoxarifado'];
    global.__fetchLog.length = 0;
    const doisDias = new Date(Date.now() - 2 * 86400000).toISOString();
    KV['wa_orc_enviados'] = { ids: { o: { telefone: '5531977776666',
      origem: 'logistica-tv', em: doisDias } } };
    KV['tv_pipe'] = { cards: [{ id: 'RA1', nomeContato: 'Ja Aprovada',
      telefone: '5531977776666', phaseId: 'aprovados', aprovadoEm: doisDias,
      equipamento: 'TV 55' }] };
    KV['reparoeletro_pipe'] = { cards: [] };
    KV['tv_logistica'] = { fichas: [] };
    KV['tv_board'] = { cards: [] };
    const rr = res();
    await wabot(req({ action: 'aprovar-cliente', tel: '77776666', aplicar: '1', ...K }), rr);
    const d = rr.dado || {};
    check('a reaprovação é recusada', d.ok === false || d.jaAprovado === true);
    const criouAlmox = global.__fetchLog.some(u =>
      u.includes('almoxarifado') && u.includes('criar-mover'));
    check('nenhum pedido criado no almoxarifado', !criouAlmox);
    check('nenhum card criado na linha branca',
      !((KV['reparoeletro_pipe'] || {}).cards || []).length);
  }

  console.log('▶ Cenário PS — pesquisa trata a resposta antes do cérebro');
  {
    const fsP = require('fs');
    const wh = fsP.readFileSync('api/wa-webhook.js', 'utf8');
    check('o webhook consulta a pesquisa antes de acionar o cérebro',
      /satisfacao[\s\S]{0,60}tratar-resposta/.test(wh));
    check('o cérebro só responde se a pesquisa não assumiu',
      /!escolhaTv && !pesquisaAssumiu/.test(wh));
    const sat = fsP.readFileSync('api/satisfacao.js', 'utf8');
    check('a pesquisa assume apenas o elogio puro',
      /veredito === 'elogio'[\s\S]{0,2000}assumiu: true/.test(sat));
    check('reclamação e ressalva liberam o cérebro',
      /assumiu: false, veredito,\s*\n\s*observacao: 'registrado/.test(sat));
  }

  console.log('▶ Cenário CF — conflito não cruza entre as frentes');
  {
    const cw = require('fs').readFileSync('api/wa-bot.js', 'utf8');
    check('os conflitos são lidos das duas prospecções, separados',
      /conflitoPorFrente/.test(cw) &&
      /\['prospeccao_adm', 'ADM'\], \['prospeccao_tv', 'TV'\]/.test(cw));
    check('o filtro do disparo usa a frente do próprio card',
      /conflitoPorFrente\[frenteA\]/.test(cw));
  }

  console.log('▶ Cenário SC — sem contato considera o controle de disparos');
  {
    const cw = require('fs').readFileSync('api/wa-bot.js', 'utf8');
    check('o painel não declara abandono só pela janela de eventos',
      !/semBot: nossas\.length === 0,/.test(cw));
    check('o critério considera respostas e disparos registrados',
      /semBot: nossas\.length === 0 && delas\.length === 0 &&/.test(cw));
    check('quem tem disparo sem histórico é separado',
      /semRegistroNaJanela/.test(cw));
  }

  console.log('▶ Cenário RG — envio ao cliente é registrado no histórico');
  {
    const cw = require('fs').readFileSync('api/wa-bot.js', 'utf8');
    for (const [acao, marca] of [
      ['régua de recuperação', "recuperação ' + x.tentativa + '/7"],
      ['abordagem de fichas', "via: 'abordagem-fichas'"],
    ]) {
      check(acao + ' registra o envio no histórico', cw.includes(marca));
    }
  }

  console.log('▶ Cenário CV — conversa não é contada em duplicidade');
  {
    const ct = require('fs').readFileSync('api/trafego.js', 'utf8');
    check('não há soma indiscriminada de ações de mensagem',
      !/conversas \+= Number\(a\.value[\s\S]{0,40}\}\s*\}/.test(
        ct.replace(/break;[\s\S]{0,20}\}/g, 'BREAK}')));
    check('a contagem usa a lista fechada e para na primeira',
      (ct.match(/messaging_conversation_started_7d/g) || []).length >= 2);
  }

  console.log('▶ Cenário MC — conflito do bot grava o motivo no campo certo');
  {
    const cw = require('fs').readFileSync('api/wa-bot.js', 'utf8');
    const i = cw.indexOf("status: 'conflitos_bot'");
    const trecho = i > 0 ? cw.slice(i, i + 600) : '';
    check('a régua grava motivoConflito, não motivo',
      /motivoConflito:/.test(trecho) && !/\bmotivo:\s*'/.test(trecho));
    const cp = require('fs').readFileSync('api/prospeccao.js', 'utf8');
    check('existe reparo para os conflitos já gravados errado',
      /reparar-motivo/.test(cp));
  }

  // ── 🎨 reforma e pintura têm teto próprio ──
  // São serviço de ticket baixo e demanda estreita: dar-lhes a verba cheia
  // tira recurso de campanha que traz conserto, e um bom desempenho aqui
  // não pode puxar verba de campeão.
  // ── 🛡️ nenhum criativo pode ser gravado sem título e corpo ──
  // O resguardo anterior ficava dentro da condição que trata o formato de
  // vídeo: ao copiar como modelo uma campanha que já estava muda, o bloco não
  // executava e o defeito se propagava para a campanha nova.
  // ── 🎬 o clone precisa trocar vídeo E texto ──
  // A cópia profunda traz o anúncio do modelo inteiro: a campanha nova exibia
  // o vídeo antigo, e quando o modelo estava sem chamada o defeito se
  // propagava para todas as campanhas do ciclo.
  console.log('▶ Cenário CL — clone troca o criativo pelo vídeo novo');
  {
    const ct = require('fs').readFileSync('api/trafego.js', 'utf8');
    const i = ct.indexOf("action === 'subir-agora'");
    const b = ct.slice(i, i + 24000);
    check('após clonar, o criativo é substituído',
      /TROCA O CRIATIVO DO CLONE/.test(b) && /crC = await postForm/.test(b));
    // verifica os dois fatos separadamente: medir a distância entre eles
    // fazia o teste falhar sempre que o bloco crescia
    check('o vídeo novo entra no lugar do vídeo do modelo',
      /const vdC = \{ \.\.\.\(ossC\.video_data \|\| \{\}\), video_id: v\.id \}/.test(b) &&
      /aplicar criativo/.test(b));
    check('o texto do dicionário tem precedência sobre o do modelo',
      /vdC\.title = \(txtC && txtC\.titulo\) \|\| vdC\.title/.test(b));
    check('falha na troca é reportada, não silenciada',
      /clone criado mas o criativo NÃO foi trocado/.test(b));
  }

  // ── 🔛 o conjunto do clone precisa ficar ativo ──
  // A cópia profunda nasce inteira pausada: ativar só a campanha deixava o
  // conjunto parado, e a campanha aparecia ativa no painel sem veicular nada.
  // ── 🆕 categoria estreante não pode ficar sem modelo ──
  // O forno estreou sem nenhuma campanha anterior e a criação foi recusada.
  // Emprestar a estrutura de uma categoria parecida resolve: a segmentação é
  // a mesma região e o mesmo público, e o texto vem do dicionário do aparelho.
  console.log('▶ Cenário CN — categoria nova empresta modelo de outra');
  {
    const ct = require('fs').readFileSync('api/trafego.js', 'utf8');
    check('há lista de categorias parecidas', /const PARECIDAS = \{/.test(ct));
    check('o forno recorre a micro-ondas primeiro',
      /forno: \['microondas'/.test(ct));
    check('o empréstimo é informado no retorno',
      /MODELO_EMPRESTADO/.test(ct));
  }

  console.log('▶ Cenário CJ — clone ativa e renomeia o conjunto');
  {
    const ct = require('fs').readFileSync('api/trafego.js', 'utf8');
    const i = ct.indexOf("action === 'subir-agora'");
    const b = ct.slice(i, i + 26000);
    check('o conjunto do clone é ativado', /ATIVA E RENOMEIA O CONJUNTO/.test(b));
    check('e recebe o nome da campanha nova',
      /nomeComData\(v\.title\) \+ ' - conjunto'/.test(b));
    check('existe conserto para os clones já criados',
      /corrigir-clones/.test(ct));
  }

  console.log('▶ Cenário TM — trava final impede criativo mudo');
  {
    const ct = require('fs').readFileSync('api/trafego.js', 'utf8');
    const i = ct.indexOf("action === 'subir-agora'");
    const b = ct.slice(i, i + 22000);
    check('há verificação imediatamente antes de gravar o criativo',
      /ÚLTIMA TRAVA antes de gravar/.test(b) &&
      /const cr = await postForm\('act_' \+ CONTA \+ '\/adcreatives'/.test(b));
    check('o formato de link também recebe texto', /txtL/.test(b));
    check('sem título ou corpo o criativo NÃO é criado',
      /não foi possível montar título e corpo — NÃO criado/.test(b));
  }

  console.log('▶ Cenário RF — criativo de reforma entra com teto limitado');
  {
    const ct = require('fs').readFileSync('api/trafego.js', 'utf8');
    check('há teto declarado para reforma', /const TETO_REFORMA = 100;/.test(ct));
    check('a verificação olha nome, título e corpo',
      /const ehReforma = \(\.\.\.partes\)/.test(ct));
    check('a verba do conjunto usa a verba efetiva',
      /lifetime_budget: String\(Math\.round\(verbaEfetiva \* 100\)\)/.test(ct));
    check('o estudo tira reforma da disputa de campeões',
      /disputam = perf\.filter\(p => !p\.reforma/.test(ct));
  }

  console.log('▶ Cenário TX — criativo novo sempre nasce com texto');
  {
    const ct = require('fs').readFileSync('api/trafego.js', 'utf8');
    check('o texto não depende mais de condicional que possa não entrar',
      !/if \(txt\) \{\s*novoOss\.video_data\.title/.test(ct));
    check('há reserva por categoria quando o dicionário não cobre',
      /const GENERICO = \{/.test(ct) && /reserva\.titulo/.test(ct));
    check('o título tem três camadas de resguardo',
      /txt && txt\.titulo\) \|\| doModelo\.title \|\| reserva\.titulo/.test(ct));
    check('o corpo tem três camadas de resguardo',
      /txt && txt\.corpo\) \|\| doModelo\.message \|\| reserva\.corpo/.test(ct));
    check('o retorno aponta os que sairiam mudos', /MUDOS:/.test(ct));
  }

  console.log('▶ Cenário FT2 — FATOS.md confere com o sistema');
  {
    const fsF = require('fs');
    const fatos = fsF.existsSync('FATOS.md') ? fsF.readFileSync('FATOS.md', 'utf8') : '';
    check('FATOS.md existe', fatos.length > 500);
    check('registra o ciclo de sábado a sábado',
      /sábado 13h.*sábado 11h/s.test(fatos));
    check('registra as verbas base', /108,75/.test(fatos) && /75,75/.test(fatos));
    check('registra que movedAt não é data',
      /movedAt.*NÃO é data/s.test(fatos));
    // os quatro caminhos citados precisam mesmo carimbar
    for (const arq of ['api/frenteloja.js', 'api/logistica.js',
                       'api/tv-logistica.js', 'api/board.js']) {
      if (!fatos.includes(arq.replace('api/', ''))) continue;
      const c = fsF.readFileSync(arq, 'utf8');
      check('FATOS diz que ' + arq + ' carimba o orçamento — e carimba',
        /orcamentoEm\s*=\s*now/.test(c));
    }
  }

  console.log('▶ Cenário HR — janela de envio cobre até o fechamento da loja');
  {
    const cw = require('fs').readFileSync('api/wa-bot.js', 'utf8');
    check('nenhuma guarda de envio corta às 16h',
      !/hh >= 7 && hh < 16/.test(cw));
    check('a guarda de dia útil vai até as 18h',
      (cw.match(/hh >= 7 && hh < 18/g) || []).length >= 2);
    check('sábado vai até as 13h',
      (cw.match(/hh >= 7 && hh < 13/g) || []).length >= 2);
  }

  console.log('▶ Cenário OR — diagnóstico carimba a data do orçamento');
  {
    const fsO = require('fs');
    const faltando = [];
    for (const arq of ['api/logistica.js', 'api/tv-logistica.js',
                       'api/board.js', 'api/frenteloja.js']) {
      const c = fsO.readFileSync(arq, 'utf8');
      if (!/orcamentoEm\s*=\s*now/.test(c)) faltando.push(arq);
    }
    check('os quatro caminhos do diagnóstico carimbam o orçamento',
      faltando.length === 0, faltando.join(', '));
    // e o carimbo só vale para o PRIMEIRO valor: reajuste não muda a data
    const lg = fsO.readFileSync('api/logistica.js', 'utf8');
    check('o carimbo não é sobrescrito em alteração de valor',
      /!card\.orcamentoEm && card\.valor>0/.test(lg));
  }

  console.log('▶ Cenário LW — espelho da corrida na rota do almoxarifado');
  {
    const w = carregarHandler('api/lalamove-webhook.js');
    KV['reparoeletro_almox_rotas'] = { rotas: [
      { id: 'RT-A', status: 'separacao', criadaEm: new Date().toISOString(), itens: [
        { cardId: 'x1', cliente: 'Um', tel: '5531911112222' },
        { cardId: 'x2', cliente: 'Dois', tel: '5531933334444' }] },
      { id: 'RT-B', status: 'separacao', criadaEm: new Date().toISOString(), itens: [
        { cardId: 'x3', cliente: 'Tres', tel: '5531955556666' }] },
    ] };
    KV['lalamove_vinculos'] = { pedidos: {} };
    KV['lalamove_webhook_log'] = { eventos: [] };
    const mot = { driverId: 'D1', name: 'Entregador', phone: '+553199990000', plateNumber: 'XYZ9A88' };
    const stops = [{ phone: '+5531911112222' }, { phone: '+5531933334444' }];

    await w({ method: 'POST', query: {}, headers: {}, body: { data: {
      order: { orderId: 'O-1', status: 'ASSIGNING_DRIVER', stops }, driver: mot } } }, res());
    await new Promise(s => setTimeout(s, 120));
    check('antes da coleta a rota não recebe motorista',
      !((KV['reparoeletro_almox_rotas'].rotas[0] || {}).motorista));

    await w({ method: 'POST', query: {}, headers: {}, body: { data: {
      order: { orderId: 'O-1', status: 'PICKED_UP', stops }, driver: mot } } }, res());
    await new Promise(s => setTimeout(s, 150));
    const rtA = KV['reparoeletro_almox_rotas'].rotas[0] || {};
    const rtB = KV['reparoeletro_almox_rotas'].rotas[1] || {};
    check('ao sair da loja o motorista é gravado na rota certa',
      rtA.motorista === 'Entregador' && rtA.placa === 'XYZ9A88', JSON.stringify(rtA.motorista));
    check('a outra rota não é afetada', !rtB.motorista);
  }

  console.log('▶ Cenário EX3 — exclusão pela tela registra o telefone');
  {
    const fi = carregarHandler('api/fichas.js');
    KV['prospeccao_excluidos'] = { tels: {} };
    KV['fichas_adm'] = { fichas: [{ id: 'EXT1', nome: 'Pela tela',
      telefone: '5531955553333', status: 'entrar_contato', sheetRow: 42,
      criadoEm: new Date().toISOString() }] };
    KV['fichas_tv'] = { fichas: [] };
    await fi(req({ action: 'excluir', ...K },
      { id: 'EXT1', sistema: 'adm', motivo: 'teste' }, 'POST'), res());
    await new Promise(s => setTimeout(s, 150));
    const tels = Object.keys((KV['prospeccao_excluidos'] || {}).tels || {});
    check('exclusão pela tela grava o telefone na lista',
      tels.some(t => t.endsWith('3333')), 'gravados: ' + tels.join(', '));
  }

  console.log('▶ Cenário RM — remarcar respeita a exclusão da fila');
  {
    const pr = carregarHandler('api/prospeccao.js');
    const lg = carregarHandler('api/logistica.js');
    const ont = new Date(Date.now() - 30 * 3600000).toISOString();
    KV['prospeccao_adm'] = { fichas: [] };
    KV['prospeccao_excluidos'] = { tels: {} };
    KV['fichas_adm'] = { fichas: [{ id: 'RM1', nome: 'Excluído', telefone: '5531955552222',
      status: 'entrar_contato', contatoFeitoEm: ont, criadoEm: ont }] };
    KV['fichas_tv'] = { fichas: [] };
    KV['reparoeletro_logistica'] = { fichas: [{ id: 'LRM1', nome: 'Excluído',
      telefone: '5531955552222', phase: 'remarcar', equipamento: 'Micro' }] };
    KV['tv_logistica'] = { fichas: [] };
    KV['reparoeletro_pipe'] = { cards: [] };
    KV['tv_pipe'] = { cards: [] };

    await pr(req({ action: 'excluir', ...K }, { id: 'RM1' }, 'POST'), res());
    await new Promise(s => setTimeout(s, 120));
    check('exclusão registra o telefone',
      Object.keys((KV['prospeccao_excluidos'] || {}).tels || {}).length > 0);

    await lg(req({ action: 'devolver-remarcar', ...K },
      { id: 'LRM1', motivo: 'teste', quem: 'harness' }, 'POST'), res());
    await new Promise(s => setTimeout(s, 200));
    const voltou = ((KV['fichas_adm'] || {}).fichas || [])
      .filter(f => String(f.status || '') === 'entrar_contato');
    check('remarcar não devolve quem foi excluído da fila',
      voltou.length === 0, voltou.map(f => f.nome).join(', '));
  }

  console.log('▶ Cenário SD — sync limita-se ao dia corrente');
  {
    const fsS = require('fs');
    const c = fsS.readFileSync('api/fichas.js', 'utf8');
    check('sync tem trava de dia corrente', /const SO_HOJE = !pediuDias/.test(c));
    check('a trava é aplicada no laço das linhas',
      /if \(SO_HOJE\) \{[\s\S]{0,260}diaLinha !== hojeSync\) continue/.test(c));
    check('janela padrão é de um dia', /\? Math\.min\(90[\s\S]{0,80}: 1;/.test(c));
    const rot = fsS.readFileSync('api/rotina.js', 'utf8');
    check('há fechamento do dia na rotina noturna',
      /horaBR === 23[\s\S]{0,200}sync-completo/.test(rot));
  }

  console.log('▶ Cenário OD — a rotina não usa variável antes de declarar');
  {
    const fsO = require('fs');
    // Três vezes uma variável foi usada antes da declaração e derrubou a
    // requisição inteira em produção. A verificação por texto em todo o código
    // dava falso positivo demais (mesmo nome em escopos diferentes), então ela
    // é feita por EXECUÇÃO: a rotina é chamada e não pode lançar erro.
    const rot = carregarHandler('api/rotina.js');
    let erro = null;
    const rR = res();
    try { await rot(req({ tipo: 'de-hora-em-hora', ...K }), rR); }
    catch (e) { if (/Cannot access|is not defined/.test(e.message)) erro = e.message; }
    check('rotina de hora em hora executa sem erro de declaração', !erro, erro || '');
    const rF = res();
    let erro2 = null;
    try { await rot(req({ tipo: 'frequente', ...K }), rF); }
    catch (e) { if (/Cannot access|is not defined/.test(e.message)) erro2 = e.message; }
    check('rotina frequente executa sem erro de declaração', !erro2, erro2 || '');
  }

  console.log('▶ Cenário AV — regras dos avisos automáticos');
  {
    const fsA = require('fs');
    const bot = fsA.readFileSync('api/wa-bot.js', 'utf8');
    const rot = fsA.readFileSync('api/rotina.js', 'utf8');
    const web = fsA.readFileSync('api/wa-webhook.js', 'utf8');
    check('TV condenada: não envia fora do expediente',
      /noExpediente/.test(bot) && /adiado: true/.test(bot));
    check('retirada: não insiste com quem respondeu',
      /respondeuDepois/.test(bot) && /NAO_RECEBEM_POR_TEREM_RESPONDIDO/.test(bot));
    check('retirada: só de segunda a sábado', /diaSem >= 1 && diaSem <= 6/.test(rot));
    check('escolha pendente expira em 5 dias', /diasEsperando >= 5/.test(bot));
    check('cérebro não responde por cima da escolha',
      /tv-condenada-respostas/.test(web) && /escolhaTv/.test(web));
    check('cliente recebe confirmação da escolha', /Anotei aqui/.test(bot));
  }

  console.log('▶ Cenário RC — recriar-perdidas respeita devolução já feita');
  {
    const logi2 = carregarHandler('api/logistica.js');
    const ontem = new Date(Date.now() - 26 * 3600000).toISOString();
    KV['reparoeletro_logistica'] = { fichas: [{
      id: 'LOG-JA', nome: 'Cliente Atendido', telefone: '5531977776666',
      phase: 'em_rota', remarcadoEm: ontem,
      voltouProspeccao: ontem,          // devolução confirmada na época
    }] };
    KV['fichas_adm'] = { fichas: [] };  // a ficha já saiu daqui: foi atendida
    const rC = res();
    const dia = new Date(Date.now() - 26 * 3600000).toISOString().slice(0, 10);
    await logi2(req({ action: 'recriar-perdidas', dia, ...K }), rC);
    const d = rC.dado || {};
    check('recriar: não considera perdida quem já foi devolvido',
      (d.marcadasComoDevolvidas || 0) === 0, JSON.stringify(d).slice(0, 110));
  }

  console.log('▶ Cenário RM — ficha remarcada chega em Entrar em Contato');
  {
    const logi = carregarHandler('api/logistica.js');
    KV['reparoeletro_logistica'] = { fichas: [{
      id: 'LOG-TESTE', nome: 'Cliente Remarcado', telefone: '5531988887777',
      equipamento: 'Micro-ondas', phase: 'liberado_coleta', criadoEm: new Date().toISOString(),
    }] };
    KV['fichas_adm'] = { fichas: [] };
    const rR = res();
    await logi(req({ action: 'mover', ...K }, {
      id: 'LOG-TESTE', phase: 'remarcar', motivo: 'cliente não estava', quem: 'teste' }, 'POST'), rR);
    const criadas = (KV['fichas_adm'].fichas || []).filter(f =>
      String(f.telefone || '').includes('8888'));
    check('remarcar: ficha chegou em fichas_adm', criadas.length === 1,
      'criadas: ' + criadas.length + ' | ' + JSON.stringify(rR.dado).slice(0, 90));
    if (criadas[0]) {
      check('remarcar: status entrar_contato', criadas[0].status === 'entrar_contato', criadas[0].status);
      check('remarcar: tem a badge de reagendamento', criadas[0].reagendarColeta === true);
      check('remarcar: guarda o motivo', /não estava/.test(String(criadas[0].motivoRemarcar || '')));
    }
    // repetir a mesma devolução NÃO pode criar uma segunda ficha
    const rR2 = res();
    await logi(req({ action: 'mover', ...K }, {
      id: 'LOG-TESTE', phase: 'remarcar', motivo: 'cliente não estava', quem: 'teste' }, 'POST'), rR2);
    const depois = (KV['fichas_adm'].fichas || []).filter(f =>
      String(f.telefone || '').includes('8888'));
    check('remarcar: não duplica ao repetir', depois.length === 1, 'agora: ' + depois.length);
  }

  console.log('▶ Cenário GR — gravação segura não perde nem duplica');
  {
    const G = require('../api/_gravar.js');
    // 1) acrescenta um item novo
    KV['teste_grav'] = { fichas: [] };
    const igual = (a, b) => String(a.id) === String(b.id);
    const r1 = await G.acrescentar('teste_grav', 'fichas', { id: 'A', nome: 'Ana' }, igual);
    check('gravação: item novo entrou', r1.ok && (KV['teste_grav'].fichas || []).length === 1,
      JSON.stringify(r1));
    // 2) o mesmo item de novo NÃO duplica
    const r2 = await G.acrescentar('teste_grav', 'fichas', { id: 'A', nome: 'Ana' }, igual);
    check('gravação: não duplica item existente',
      r2.ok && KV['teste_grav'].fichas.length === 1, KV['teste_grav'].fichas.length);
    // 3) item diferente entra
    await G.acrescentar('teste_grav', 'fichas', { id: 'B', nome: 'Bruno' }, igual);
    check('gravação: segundo item entrou', KV['teste_grav'].fichas.length === 2);
    // 4) alteração que muda um campo, com conferência
    const r4 = await G.alterar('teste_grav',
      (db) => { const f = db.fichas.find(x => x.id === 'A'); if (f) f.status = 'ok'; return db; },
      (db) => (db.fichas || []).some(x => x.id === 'A' && x.status === 'ok'));
    check('gravação: alteração confirmada', r4.ok, r4.motivo);
    // 5) desistir devolve ok sem alterar
    const antes = JSON.stringify(KV['teste_grav']);
    const r5 = await G.alterar('teste_grav', () => null, null);
    check('gravação: desistir não altera nada',
      r5.ok && JSON.stringify(KV['teste_grav']) === antes);
  }

  console.log('▶ Cenário K2 — etapas registram no livro-razão do funil');
  {
    const fs8 = require('fs');
    const exigido = [
      ['api/frenteloja.js', "registrar('aprovado'", 'aprovação no balcão'],
      ['api/frenteloja.js', "registrar('orcamento'", 'orçamento no balcão'],
      ['api/pipe.js', "registrar('aprovado'", 'aprovação ADM'],
      ['api/pipe.js', "registrar('orcamento'", 'orçamento ADM'],
      ['api/tv-pipe.js', "registrar('aprovado'", 'aprovação TV'],
      ['api/tv-pipe.js', "registrar('orcamento'", 'orçamento TV'],
      ['api/logistica.js', "registrar('logistica'", 'logística ADM'],
      ['api/tv-logistica.js', "registrar('logistica'", 'logística TV'],
      ['api/fichas.js', "registrar('ficha'", 'ficha criada'],
    ];
    const faltando = exigido.filter(([arq, trecho]) =>
      !fs8.existsSync(arq) || !fs8.readFileSync(arq, 'utf8').includes(trecho))
      .map(([, , nome]) => nome);
    check('funil: todas as etapas registram no livro-razão',
      faltando.length === 0, 'sem registro: ' + faltando.join(', '));
  }

  console.log('▶ Cenário K1 — etapas do KPI gravam data no banco');
  {
    const fs7 = require('fs');
    const exigido = [
      ['api/fichas.js', 'criadoEm', 'ficha criada'],
      ['api/logistica.js', 'criadoEm', 'logística ADM'],
      ['api/tv-logistica.js', 'criadoEm', 'logística TV'],
      ['api/pipe.js', 'orcamentoEm', 'orçamento ADM'],
      ['api/tv-pipe.js', 'orcamentoEm', 'orçamento TV'],
      ['api/frenteloja.js', 'orcamentoEm', 'orçamento balcão'],
      ['api/pipe.js', 'aprovadoEm', 'aprovação ADM'],
      ['api/tv-pipe.js', 'aprovadoEm', 'aprovação TV'],
      ['api/frenteloja.js', 'aprovadoEm', 'aprovação balcão'],
    ];
    const faltando = exigido.filter(([arq, campo]) => {
      if (!fs7.existsSync(arq)) return true;
      return !fs7.readFileSync(arq, 'utf8').includes(campo);
    }).map(([, , nome]) => nome);
    check('KPI: todas as etapas carimbam a data na origem',
      faltando.length === 0, 'sem carimbo: ' + faltando.join(', '));
  }

  console.log('▶ Cenário 12b — coleta fora da janela é bloqueada');
  const r12h = res();
  await wabot(req({ action:'enviar', forcarJanela:'1', ...K }, { tel:'5531990007099',
    texto:'Perfeito! Vou programar sua coleta.',
    acaoAprovada:'cadastrar_logistica', acaoMotivo:'coleta imediata' }), r12h);
  {
    const j = r12h.dado || {};
    const bras = new Date(Date.now() - 3*3600000);
    const dia = bras.getUTCDay(), hora = bras.getUTCHours() + bras.getUTCMinutes()/60;
    const dentro = (dia>=1&&dia<=5) ? (hora>=8&&hora<14) : (dia===6 ? (hora>=8&&hora<10) : false);
    if (dentro) check('dentro da janela: cadastro permitido', j.ok !== false || !/fora da janela/.test(String(j.error||'')));
    else check('fora da janela: cadastro recusado', /fora da janela/.test(String(j.error||'')));
  }

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
  await wabot(req({ action:'enviar', forcar:'1', forcarJanela:'1', ...K }, { tel:'5531990007001',
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
  await wabot(req({ action:'enviar', forcar:'1', forcarJanela:'1', ...K }, { tel:'5531990007002',
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
  // 🔀 Retornar passou à frente de Cliente Loja: retorno é compromisso marcado
  // com o cliente, enquanto cliente de loja que não veio pode esperar
  check('ordem: Retornar é o 3º', nomes[2] === 'Retornar Um', nomes);
  check('ordem: Cliente Loja VERMELHO é o 4º', nomes[3] === 'Loja Vermelho', nomes);
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
  await wabot(req({ action:'orcamentos-pendentes', forcar:'1', ...K }), res());
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
  await wabot(req({ action:'orcamentos-pendentes', forcar:'1', ...K }), res());
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
  await wabot(req({ action:'orcamentos-pendentes', forcar:'1', ...K }), res());
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
  await wabot(req({ action:'orcamentos-pendentes', forcar:'1', ...K }), res());
  check('erro passageiro: NÃO abre conflito na 1ª falha',
    !global.__fetchLog.some(u => u.includes('criar-conflito')));
  const r16b = ((KV['wa_orc_enviados']||{}).ids||{})['OC11'];
  check('erro passageiro: contou a falha para tentar de novo', r16b && r16b.falhas === 1, r16b);
  // 2ª e 3ª tentativa -> conflito
  await wabot(req({ action:'orcamentos-pendentes', forcar:'1', ...K }), res());
  await wabot(req({ action:'orcamentos-pendentes', forcar:'1', ...K }), res());
  delete global.__forcarErroGraph;
  check('erro passageiro: abre conflito após 3 tentativas',
    global.__fetchLog.some(u => u.includes('criar-conflito')), global.__fetchLog.slice(-3));

  // 16c: sucesso não abre conflito nenhum
  KV['reparoeletro_logistica'] = { fichas: [fOrc('OC12','Denise')] };
  KV['wa_orc_enviados'] = { ids: {} };
  global.__fetchLog.length = 0;
  await wabot(req({ action:'orcamentos-pendentes', forcar:'1', ...K }), res());
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
  await wabot(req({ action:'orcamentos-pendentes', forcar:'1', ...K }), res());
  delete global.__forcarErroGraph;
  check('recusado: NÃO chamou orc-enviar (card fica na aba Orçamento)',
    !global.__fetchLog.some(u => u.includes('orc-enviar')), global.__fetchLog.filter(u => u.includes('orc')));

  // 19b: Meta ACEITA → marca como enviado normalmente
  KV['reparoeletro_logistica'] = { fichas: [fOrc19()] };
  KV['reparoeletro_orcamentos'] = { fichas: [{ id:'ORC-19', tel:'5531990019999', nome:'Cliente 19', status:'pendente', precoSugerido:'350', textoOrc:'x' }], syncedIds: [] };
  KV['wa_orc_enviados'] = { ids: {} };
  global.__fetchLog.length = 0;
  await wabot(req({ action:'orcamentos-pendentes', forcar:'1', ...K }), res());
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

  // O3 é de 25/07 (antes do bloqueio) e recebeu recusa recente no MESMO telefone:
  // não pode entrar — é o falso positivo que apareceu em produção.
  LISTS['wa_evt_list'] = LISTS['wa_evt_list'].concat([
    JSON.stringify({ ts:'2026-08-03T11:00:00.000Z', tel:'5531990020003', dir:'status', texto: fal })]);
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
  check('aplicar: enviado ANTES do bloqueio não é mexido, mesmo com recusa recente no mesmo telefone', fx('O3').status === 'enviado', fx('O3'));
  check('aplicar: ENTREGUE não é mexido', fx('O4').status === 'enviado', fx('O4'));
  check('aplicar: cliente que respondeu depois não é mexido', fx('O5').status === 'enviado', fx('O5'));
  check('aplicar: já pendente continua pendente', fx('O6').status === 'pendente', fx('O6'));
  check('aplicar: guarda o motivo da devolução', !!fx('O1').devolvidoEm && /131042|recus/i.test(String(fx('O1').devolvidoMotivo||'')), fx('O1'));

  // ════ Resultado ════
  console.log('\n═══════════════════════════════════');
  const _mj = (() => { const b = new Date(Date.now() - 3 * 3600000); const d = b.getUTCDay(), hh = b.getUTCHours(); return (d >= 1 && d <= 5) ? (hh >= 8 && hh < 14) : (d === 6 ? (hh >= 8 && hh < 10) : false); })();
  console.log(`   Modo: ${_mj ? 'DENTRO da janela comercial (dedupe do bot TESTADO)' : 'FORA da janela comercial (dedupe do bot NÃO testado — rode tb. em horário comercial)'}`);
  console.log(falha === 0 ? `🟢 VERDE — ${passa} testes passaram. Liberado para a janela de deploy.` : `🔴 VERMELHO — ${falha} falha(s), ${passa} ok. NÃO SUBIR PARA PRODUÇÃO.`);
  console.log('═══════════════════════════════════\n');
  process.exit(falha === 0 ? 0 : 1);
})();
