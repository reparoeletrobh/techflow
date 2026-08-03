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
    ['micro elétrico 350 -> 370',              { tipo:'microondas', servicos:['Elétrico'] },                          '370'],
    ['micro haste 350 -> 370',                 { tipo:'microondas', servicos:['Haste'] },                             '370'],
    ['micro pintura 350 -> 370',               { tipo:'microondas', servicos:['Pintura'] },                           '370'],
    ['micro reforma 350 -> 370',               { tipo:'microondas', servicos:['Reforma'] },                           '370'],
    ['micro reforma+revisão MANTÉM 390',       { tipo:'microondas', servicos:['Reforma','Revisão'] },                 '390'],
    ['micro magnetron MANTÉM 390',             { tipo:'microondas', servicos:['Magnetron'] },                         '390'],
    ['micro placa custo 150: 2x=300 -> piso 370', { tipo:'microondas', servicos:['Troca de Placa'], preco:'150' },    '370'],
    ['micro placa custo 250: 2x=500 PREVALECE',{ tipo:'microondas', servicos:['Troca de Placa'], preco:'250' },       '500'],

    // ── FORNO: Grande 790 -> 890. Pequeno permanece 490.
    ['forno GRANDE 790 -> 890',                { tipo:'forno', subtipo:'Grande', servicos:['Resistência'] },          '890'],
    ['forno PEQUENO mantém 490',               { tipo:'forno', subtipo:'Pequeno', servicos:['Resistência'] },         '490'],

    // ── PURIFICADOR: intocado.
    ['purificador Motor Gás mantém 490',       { tipo:'purificador', subtipo:'Motor', servicos:['Gás'] },             '490'],
    ['purificador Eletrônico Kit mantém 350',  { tipo:'purificador', subtipo:'Eletrônico', servicos:['Kit Termo Elétrico'] }, '350'],

    // ── ADEGA modo NORMAL: sem porte, preços atuais mantidos.
    ['adega normal Motor Termostato = 490',    { tipo:'adega', subtipo:'Motor', servicos:['Termostato'] },            '490'],
    ['adega normal Eletrônico Kit = 350',      { tipo:'adega', subtipo:'Eletrônico', servicos:['Kit Termo Elétrico'] }, '350'],

    // ── ADEGA 8 GARRAFAS: piso R$390. Acima disso, o maior prevalece.
    ['adega8 Eletrônico Kit 350 -> 390',       { tipo:'adega', subtipo:'Eletrônico', servicos:['Kit Termo Elétrico'], tabela:'adega8' }, '390'],
    ['adega8 Eletrônico Sensor 390 -> 390',    { tipo:'adega', subtipo:'Eletrônico', servicos:['Sensor'], tabela:'adega8' },            '390'],
    ['adega8 Eletrônico TermoDuplo 490 MANTÉM',{ tipo:'adega', subtipo:'Eletrônico', servicos:['Termoelétrico Duplo'], tabela:'adega8' },'490'],
    ['adega8 Motor Termostato 490 MANTÉM',     { tipo:'adega', subtipo:'Motor', servicos:['Termostato'], tabela:'adega8' },             '490'],

    // ── ADEGA 12 GARRAFAS: piso R$450.
    ['adega12 Eletrônico Kit 350 -> 450',      { tipo:'adega', subtipo:'Eletrônico', servicos:['Kit Termo Elétrico'], tabela:'adega12' }, '450'],
    ['adega12 Eletrônico Sensor 390 -> 450',   { tipo:'adega', subtipo:'Eletrônico', servicos:['Sensor'], tabela:'adega12' },            '450'],
    ['adega12 Eletrônico TermoDuplo 490 MANTÉM',{tipo:'adega', subtipo:'Eletrônico', servicos:['Termoelétrico Duplo'], tabela:'adega12' },'490'],
    ['adega12 Motor Termostato 490 MANTÉM',    { tipo:'adega', subtipo:'Motor', servicos:['Termostato'], tabela:'adega12' },             '490'],
    ['adega12 placa custo 180: 2x=360 -> piso 450', { tipo:'adega', subtipo:'Motor', servicos:['Troca de Placa'], preco:'180', tabela:'adega12' }, '450'],

    // ── TABELA DINÂMICA: intocada, 40% do valor do equipamento.
    ['dinâmica equip 1000 = 400',              { tipo:'microondas', servicos:['Elétrico'], tabela:'dinamica', valorEquip:'1000' }, '400'],
    ['dinâmica adega equip 2000 = 800',        { tipo:'adega', subtipo:'Motor', servicos:['Gás'], tabela:'dinamica', valorEquip:'2000' }, '800'],
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
  check('2 equipamentos: (370+490) −5% = 817', pM === '817', { obtido: pM });

  // paridade frente de loja: mesmo equipamento, total igual e −10% aplicado
  KV['reparoeletro_frenteloja'] = { fichas: [{ id: 'FL1', nomeContato: 'Teste Loja', telefone: '5531990007777', phase: 'analise' }], seq: 1 };
  const rF = res();
  await floja(req({ action: 'diagnostico-loja', ...K }, { id: 'FL1', equips: [ { tipo:'microondas', servicos:['Magnetron'] } ] }), rF);
  check('loja: micro Magnetron total = 390 (mesma tabela da logística)', rF.dado && rF.dado.total === 390, rF.dado && (rF.dado.error || rF.dado.total));
  check('loja: desconto de 10% aplicado = 351', rF.dado && rF.dado.totalComDesconto === 351, rF.dado && (rF.dado.error || rF.dado.totalComDesconto));
  // loja usa os preços NOVOS: micro elétrico 370 -> com 10% = 333
  KV['reparoeletro_frenteloja'] = { fichas: [{ id: 'FL2', nomeContato: 'Teste Loja 2', telefone: '5531990007766', phase: 'analise' }], seq: 1 };
  const rF2 = res();
  await floja(req({ action: 'diagnostico-loja', ...K }, { id: 'FL2', equips: [ { tipo:'microondas', servicos:['Elétrico'] } ] }), rF2);
  check('loja: micro elétrico usa o preço novo 370', rF2.dado && rF2.dado.total === 370, rF2.dado && (rF2.dado.error || rF2.dado.total));
  check('loja: 370 −10% = 333', rF2.dado && rF2.dado.totalComDesconto === 333, rF2.dado && (rF2.dado.error || rF2.dado.totalComDesconto));
  // loja: adega 8 garrafas com piso 390 -> com 10% = 351
  KV['reparoeletro_frenteloja'] = { fichas: [{ id: 'FL3', nomeContato: 'Teste Loja 3', telefone: '5531990007755', phase: 'analise' }], seq: 1 };
  const rF3 = res();
  await floja(req({ action: 'diagnostico-loja', ...K }, { id: 'FL3', equips: [ { tipo:'adega', subtipo:'Eletrônico', servicos:['Kit Termo Elétrico'], tabela:'adega8' } ] }), rF3);
  check('loja: adega8 aplica piso 390', rF3.dado && rF3.dado.total === 390, rF3.dado && (rF3.dado.error || rF3.dado.total));

  // ════ CENÁRIO 8: isolamento do Frente de Loja ════
  // Regra inviolável: FL grava SÓ em reparoeletro_frenteloja. O bot não lê esse banco.
  console.log('▶ Cenário 8 — Frente de Loja não vaza para os bancos do bot');
  const sentinelaLog = JSON.stringify(KV['reparoeletro_logistica'] || null);
  const sentinelaTvLog = JSON.stringify(KV['tv_logistica'] || null);
  KV['reparoeletro_frenteloja'] = { fichas: [{ id: 'ISO1', nomeContato: 'Iso Teste', telefone: '5531990006666', phase: 'analise' }], seq: 1 };
  await floja(req({ action: 'diagnostico-loja', ...K }, { id: 'ISO1', equips: [ { tipo:'purificador', subtipo:'Motor', servicos:['Gás'] } ] }), res());
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

  // ════ Resultado ════
  console.log('\n═══════════════════════════════════');
  const _mj = (() => { const b = new Date(Date.now() - 3 * 3600000); const d = b.getUTCDay(), hh = b.getUTCHours(); return (d >= 1 && d <= 5) ? (hh >= 8 && hh < 15) : (d === 6 ? (hh >= 8 && hh < 10) : false); })();
  console.log(`   Modo: ${_mj ? 'DENTRO da janela comercial (dedupe do bot TESTADO)' : 'FORA da janela comercial (dedupe do bot NÃO testado — rode tb. em horário comercial)'}`);
  console.log(falha === 0 ? `🟢 VERDE — ${passa} testes passaram. Liberado para a janela de deploy.` : `🔴 VERMELHO — ${falha} falha(s), ${passa} ok. NÃO SUBIR PARA PRODUÇÃO.`);
  console.log('═══════════════════════════════════\n');
  process.exit(falha === 0 ? 0 : 1);
})();
