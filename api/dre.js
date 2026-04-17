// api/dre.js — Sistema Financeiro Isolado — Reparo Eletro
// Acessado via /dre  |  Não aparece em ADM nem TV
// Redis keys: reparo_fin_*

const U = (process.env.UPSTASH_URL   || '').replace(/['"]/g,'').trim();
const T = (process.env.UPSTASH_TOKEN || '').replace(/['"]/g,'').trim();

// ── DB helpers ────────────────────────────────────────────────
async function dbPipeline(...cmds) {
  const r = await fetch(`${U}/pipeline`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${T}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(cmds)
  });
  return r.json();
}

async function dbGet(...keys) {
  const res = await dbPipeline(...keys.map(k => ['GET', k]));
  return res.map(item => {
    if (!item?.result) return null;
    try { let p = JSON.parse(item.result); if (typeof p === 'string') p = JSON.parse(p); return p; }
    catch { return null; }
  });
}

async function dbSet(key, val) {
  await dbPipeline(['SET', key, JSON.stringify(val)]);
}

function uid() { return Date.now().toString(36) + Math.random().toString(36).slice(2,6); }
function today() { return new Date().toISOString().slice(0,10); }

// ── Config defaults ────────────────────────────────────────────
const CFG_DEFAULT = { metaMensal: 0, impostoPct: 6, nomeEmpresa: 'Reparo Eletro' };

// ── Fases do painel Financeiro (reparoeletro_financeiro) ────────
const PHASE_LABEL = {
  aguardando_dados:'Aguardando Dados',
  emitir_nf:'Emitir NF',          // fase legada ativa no sistema
  nf_emitida:'NF Emitida',
  faturamento:'Faturamento',
  pagamento_agendado:'Pagamento Agendado',
  analise_pagamento:'Análise de Pagamento',
  pagamento_confirmado:'Pagamento Confirmado',
  // pós-pagamento (não aparecem como recebíveis)
  entrega_agendada:'Entrega Agendada',
  entrega_liberada:'Entrega Liberada',
  rota_criada:'Rota Criada',
  item_coletado:'Item Coletado'
};

// Apenas fichas NESTAS fases aparecem como Recebíveis
// Fases exatas definidas pelo usuário — emitir_nf excluído propositalmente
const FIN_PHASES = new Set([
  'aguardando_dados',
  'nf_emitida',
  'faturamento',
  'pagamento_agendado',
  'analise_pagamento',
  'pagamento_confirmado'
]);

// Urgentes = próximos do pagamento
const URGENTES = new Set(['pagamento_agendado','analise_pagamento','pagamento_confirmado']);

// ── Categorias ─────────────────────────────────────────────────
const CAT_RECEITA = {
  servico:'Serviço de Reparo', peca:'Venda de Peças',
  delivery:'Coleta/Entrega', outro:'Outros'
};
const CAT_DESPESA = {
  peca_cmv:'Compra de Peças (CMV)',   // ← alimenta CMV diretamente
  aluguel:'Aluguel', energia:'Energia Elétrica', agua:'Água',
  telefone:'Telefone/Internet', salario:'Salário/Pró-labore',
  material:'Material/Suprimentos', marketing:'Marketing',
  contabilidade:'Contabilidade', transporte:'Transporte',
  manutencao:'Manutenção', outros:'Outros'
};

// ── Financial calculations ─────────────────────────────────────
function calcDRE(receitas, despesas, config, mes) {
  const cfg = { ...CFG_DEFAULT, ...config };

  // Receitas recebidas no mês
  const recs = (receitas||[]).filter(x => x.data?.startsWith(mes) && x.status === 'recebido');
  const recGroups = {};
  for (const k of Object.keys(CAT_RECEITA)) recGroups[k] = 0;
  recs.forEach(r => { recGroups[r.categoria||'outro'] = (recGroups[r.categoria||'outro']||0) + (r.valor||0); });
  const receitaBruta = recs.reduce((s,r) => s + (r.valor||0), 0);
  const impostos     = +(receitaBruta * (cfg.impostoPct/100)).toFixed(2);
  const receitaLiq   = receitaBruta - impostos;

  // CMV = despesas categoria 'peca_cmv' pagas no mês (100% manual)
  const cmv = +(despesas||[])
    .filter(x => x.categoria==='peca_cmv' && (x.data||x.dataVencimento||'').startsWith(mes) && x.status==='pago')
    .reduce((s,x) => s + (x.valor||0), 0)
    .toFixed(2);
  const lucroBruto = receitaLiq - cmv;

  // Despesas pagas no mês
  const desps = (despesas||[]).filter(x => (x.data||x.dataVencimento||'').startsWith(mes) && x.status==='pago');
  const despGroups = {};
  for (const k of Object.keys(CAT_DESPESA)) despGroups[k] = 0;
  desps.forEach(d => { despGroups[d.categoria||'outros'] = (despGroups[d.categoria||'outros']||0) + (d.valor||0); });

  const despFixas = desps.filter(x=>x.fixaRef).reduce((s,x) => s+(x.valor||0), 0);
  const despVar   = desps.filter(x=>!x.fixaRef).reduce((s,x) => s+(x.valor||0), 0);
  const totalDesp = despFixas + despVar;

  const lucroLiq = lucroBruto - totalDesp;
  const mBruta   = receitaBruta > 0 ? +(lucroBruto/receitaBruta*100).toFixed(1) : 0;
  const mLiquida = receitaBruta > 0 ? +(lucroLiq/receitaBruta*100).toFixed(1) : 0;

  return {
    receitaBruta: +receitaBruta.toFixed(2), recGroups,
    impostos, receitaLiq: +receitaLiq.toFixed(2),
    cmv: +cmv.toFixed(2), lucroBruto: +lucroBruto.toFixed(2),
    despFixas: +despFixas.toFixed(2), despVar: +despVar.toFixed(2),
    despGroups, totalDesp: +totalDesp.toFixed(2),
    lucroLiq: +lucroLiq.toFixed(2),
    margemBruta: mBruta, margemLiquida: mLiquida,
    impostoPct: cfg.impostoPct
  };
}

function calcFluxo(receitas, despesas, mes) {
  const [ano, m] = mes.split('-').map(Number);
  const diasNoMes = new Date(ano, m, 0).getDate();

  const txs = [];
  (receitas||[]).filter(x => x.data?.startsWith(mes) && x.status==='recebido')
    .forEach(x => txs.push({ data:x.data, valor:+(x.valor||0), tipo:'E', desc:x.descricao||'Receita' }));
  (despesas||[]).filter(x => (x.data||x.dataVencimento||'').startsWith(mes) && x.status==='pago')
    .forEach(x => txs.push({ data:(x.data||x.dataVencimento), valor:+(x.valor||0), tipo:'S', desc:x.descricao||'Despesa' }));
  txs.sort((a,b) => a.data.localeCompare(b.data));

  const weeks = [];
  let acc = 0;
  let weekIdx = 0;
  while (weekIdx*7+1 <= diasNoMes) {
    const dIni = weekIdx*7+1, dFim = Math.min((weekIdx+1)*7, diasNoMes);
    const wTxs = txs.filter(t => { const d = parseInt((t.data||'').split('-')[2]||0); return d>=dIni && d<=dFim; });
    const entradas = wTxs.filter(t=>t.tipo==='E').reduce((s,t)=>s+t.valor, 0);
    const saidas   = wTxs.filter(t=>t.tipo==='S').reduce((s,t)=>s+t.valor, 0);
    const saldo    = entradas - saidas;
    acc += saldo;
    weeks.push({
      label: `${String(dIni).padStart(2,'0')}-${String(dFim).padStart(2,'0')}/${String(m).padStart(2,'0')}`,
      entradas: +entradas.toFixed(2), saidas: +saidas.toFixed(2),
      saldo: +saldo.toFixed(2), acumulado: +acc.toFixed(2)
    });
    weekIdx++;
  }
  return weeks;
}

function calcKPIs(receitas, despesas, fixas, config, finRecords, mes) {
  const cfg = { ...CFG_DEFAULT, ...config };
  const recs  = (receitas||[]).filter(x => x.data?.startsWith(mes) && x.status==='recebido');
  const desps = (despesas||[]).filter(x => (x.data||x.dataVencimento||'').startsWith(mes) && x.status==='pago');
  const pend  = (despesas||[]).filter(x => (x.dataVencimento||x.data||'').startsWith(mes) && x.status==='pendente');

  const receitaMes  = +recs.reduce((s,x)=>s+(x.valor||0),0).toFixed(2);
  const despesasMes = +desps.reduce((s,x)=>s+(x.valor||0),0).toFixed(2);
  const impostos    = +(receitaMes*(cfg.impostoPct/100)).toFixed(2);
  const lucroLiq    = +(receitaMes - despesasMes - impostos).toFixed(2);
  const margemLiq   = receitaMes>0 ? +(lucroLiq/receitaMes*100).toFixed(1) : 0;
  const metaPct     = cfg.metaMensal>0 ? +(Math.min(receitaMes/cfg.metaMensal*100,999)).toFixed(1) : 0;
  const ticket      = recs.length>0 ? +(receitaMes/recs.length).toFixed(2) : 0;

  // Receivables = fichas do financeiro nas fases corretas, não confirmadas
  const confirmed  = new Set((receitas||[]).filter(x=>x.osRef && x.status==='recebido').map(x=>x.osRef));
  const recebiveis = (finRecords||[]).filter(c => FIN_PHASES.has(c.phaseId) && !confirmed.has(c.pipefyId||c.id));
  const recebiveisValor$ = +recebiveis.reduce((s,c) => s + (parseFloat(c.valor)||0), 0).toFixed(2);

  // A pagar esta semana
  const now = Date.now();
  const em7d = now + 7*86400000;
  const semana = pend.filter(x => {
    const t = new Date(x.dataVencimento||x.data).getTime();
    return t >= now && t <= em7d;
  }).reduce((s,x)=>s+(x.valor||0), 0);

  // Fixas não pagas no mês
  const fixasPagas = new Set((despesas||[]).filter(x=>x.fixaRef && (x.data||x.dataVencimento||'').startsWith(mes) && x.status==='pago').map(x=>x.fixaRef));
  const fixasPend  = (fixas||[]).filter(f=>f.ativo && !fixasPagas.has(f.id));
  const totalFixasPend = +fixasPend.reduce((s,f)=>s+(f.valor||0),0).toFixed(2);
  const totalFixasMes  = +(fixas||[]).filter(f=>f.ativo).reduce((s,f)=>s+(f.valor||0),0).toFixed(2);

  return {
    receitaMes, despesasMes, impostos, lucroLiq, margemLiq,
    metaPct, metaMensal: cfg.metaMensal,
    ticket, countReceitas: recs.length,
    recebiveisCount: recebiveis.length,
    recebiveisValor$,
    apagarSemana: +semana.toFixed(2),
    fixasPendentes: fixasPend.length, totalFixasPend, totalFixasMes
  };
}

// ── MAIN ──────────────────────────────────────────────────────
module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin','*');
  res.setHeader('Access-Control-Allow-Methods','GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers','Content-Type');
  if (req.method==='OPTIONS') return res.status(200).end();

  const action = req.query.action||'';
  const mes    = req.query.mes || new Date().toISOString().slice(0,7);

  try {

    // ── LOAD (all data + computed metrics) ──────────────────────
    if (action === 'load') {
      const [rD,dD,fD,cD,finD] = await dbGet(
        'reparo_fin_receitas','reparo_fin_despesas','reparo_fin_fixas',
        'reparo_fin_config','reparoeletro_financeiro'
      );
      const receitas = rD?.receitas || (Array.isArray(rD)?rD:[]);
      const despesas = dD?.despesas || (Array.isArray(dD)?dD:[]);
      const fixas    = fD?.fixas    || (Array.isArray(fD)?fD:[]);
      const config   = { ...CFG_DEFAULT, ...(cD||{}) };
      // reparoeletro_financeiro pode guardar { records:[...] } ou array direto
      const finRecords = finD?.records || (Array.isArray(finD)?finD:[]);

      // Receivables = fichas do painel financeiro nas fases corretas, não confirmadas
      const confirmed = new Set((receitas||[]).filter(x=>x.osRef && x.status==='recebido').map(x=>x.osRef));
      const recebiveis = finRecords
        .filter(c => FIN_PHASES.has(c.phaseId) && !confirmed.has(c.pipefyId))
        .map(c=>({
          pipefyId:   c.pipefyId || c.id,
          osCode:     c.osCode,
          nomeContato:c.nomeContato,
          title:      c.title || c.descricao,
          valor:      parseFloat(c.valor)||0,
          telefone:   c.telefone,
          phaseId:    c.phaseId,
          phaseLabel: PHASE_LABEL[c.phaseId] || c.phaseId,
          addedAt:    c.createdAt || c.movedAt
        }))
        .sort((a,b) => (a.nomeContato||'').localeCompare(b.nomeContato||''));

      // Soma total dos recebíveis
      const recebiveisTotal$ = +recebiveis.reduce((s,r) => s + r.valor, 0).toFixed(2);

      // A pagar: fixas do mês + despesas pendentes
      const fixasPagas = new Set(despesas.filter(x=>x.fixaRef && (x.data||x.dataVencimento||'').startsWith(mes) && x.status==='pago').map(x=>x.fixaRef));
      const fixasPend  = fixas.filter(f=>f.ativo && !fixasPagas.has(f.id)).map(f=>({
        ...f, tipo:'fixa', vencimento:`${mes}-${String(f.diaVencimento||1).padStart(2,'0')}`
      }));
      const despPend = despesas.filter(x=>(x.dataVencimento||x.data||'').startsWith(mes) && x.status==='pendente');

      return res.status(200).json({
        ok:true, mes, catReceita:CAT_RECEITA, catDespesa:CAT_DESPESA,
        receitas: [...receitas].reverse(),
        despesas: [...despesas].reverse(),
        fixas, config, recebiveis,
        recebiveisTotal$,
        aPagar: { fixas:fixasPend, despesas:despPend },
        dre:   calcDRE(receitas,despesas,config,mes),
        fluxo: calcFluxo(receitas,despesas,mes),
        kpis:  calcKPIs(receitas,despesas,fixas,config,finRecords,mes)
      });
    }

    // ── RECEITAS CRUD ────────────────────────────────────────────
    if (req.method==='POST' && action==='add-receita') {
      const b=req.body||{};
      if (!b.descricao||!b.valor) return res.status(200).json({ok:false,error:'descricao e valor obrigatórios'});
      const [rD]=await dbGet('reparo_fin_receitas');
      const lista=(rD?.receitas||(Array.isArray(rD)?rD:[]));
      lista.unshift({id:uid(),descricao:b.descricao,valor:parseFloat(b.valor)||0,
        data:b.data||today(),categoria:b.categoria||'servico',
        status:b.status||'recebido',osRef:b.osRef||null,cliente:b.cliente||'',
        criadoEm:new Date().toISOString()});
      await dbSet('reparo_fin_receitas',{receitas:lista});
      return res.status(200).json({ok:true});
    }
    if (req.method==='POST' && action==='edit-receita') {
      const b=req.body||{};
      const [rD]=await dbGet('reparo_fin_receitas');
      const lista=(rD?.receitas||(Array.isArray(rD)?rD:[]));
      const i=lista.findIndex(x=>x.id===b.id);
      if(i<0) return res.status(200).json({ok:false,error:'não encontrado'});
      lista[i]={...lista[i],...b,id:lista[i].id};
      await dbSet('reparo_fin_receitas',{receitas:lista});
      return res.status(200).json({ok:true});
    }
    if (req.method==='POST' && action==='del-receita') {
      const {id}=req.body||{};
      const [rD]=await dbGet('reparo_fin_receitas');
      const lista=(rD?.receitas||(Array.isArray(rD)?rD:[])).filter(x=>x.id!==id);
      await dbSet('reparo_fin_receitas',{receitas:lista});
      return res.status(200).json({ok:true});
    }

    // ── DESPESAS CRUD ────────────────────────────────────────────
    if (req.method==='POST' && action==='add-despesa') {
      const b=req.body||{};
      if (!b.descricao||!b.valor) return res.status(200).json({ok:false,error:'descricao e valor obrigatórios'});
      const [dD]=await dbGet('reparo_fin_despesas');
      const lista=(dD?.despesas||(Array.isArray(dD)?dD:[]));
      lista.unshift({id:uid(),descricao:b.descricao,valor:parseFloat(b.valor)||0,
        data:b.data||today(),dataVencimento:b.dataVencimento||b.data||today(),
        categoria:b.categoria||'outros',status:b.status||'pago',
        fixaRef:b.fixaRef||null,criadoEm:new Date().toISOString()});
      await dbSet('reparo_fin_despesas',{despesas:lista});
      return res.status(200).json({ok:true});
    }
    if (req.method==='POST' && action==='edit-despesa') {
      const b=req.body||{};
      const [dD]=await dbGet('reparo_fin_despesas');
      const lista=(dD?.despesas||(Array.isArray(dD)?dD:[]));
      const i=lista.findIndex(x=>x.id===b.id);
      if(i<0) return res.status(200).json({ok:false,error:'não encontrado'});
      lista[i]={...lista[i],...b,id:lista[i].id};
      await dbSet('reparo_fin_despesas',{despesas:lista});
      return res.status(200).json({ok:true});
    }
    if (req.method==='POST' && action==='del-despesa') {
      const {id}=req.body||{};
      const [dD]=await dbGet('reparo_fin_despesas');
      const lista=(dD?.despesas||(Array.isArray(dD)?dD:[])).filter(x=>x.id!==id);
      await dbSet('reparo_fin_despesas',{despesas:lista});
      return res.status(200).json({ok:true});
    }

    // ── CONTAS FIXAS CRUD ────────────────────────────────────────
    if (req.method==='POST' && action==='add-fixa') {
      const b=req.body||{};
      if (!b.nome||!b.valor) return res.status(200).json({ok:false,error:'nome e valor obrigatórios'});
      const [fD]=await dbGet('reparo_fin_fixas');
      const lista=(fD?.fixas||(Array.isArray(fD)?fD:[]));
      lista.push({id:uid(),nome:b.nome,valor:parseFloat(b.valor)||0,
        diaVencimento:parseInt(b.diaVencimento)||5,categoria:b.categoria||'outros',
        ativo:true,criadoEm:new Date().toISOString()});
      await dbSet('reparo_fin_fixas',{fixas:lista});
      return res.status(200).json({ok:true});
    }
    if (req.method==='POST' && action==='edit-fixa') {
      const b=req.body||{};
      const [fD]=await dbGet('reparo_fin_fixas');
      const lista=(fD?.fixas||(Array.isArray(fD)?fD:[]));
      const i=lista.findIndex(x=>x.id===b.id);
      if(i<0) return res.status(200).json({ok:false,error:'não encontrado'});
      lista[i]={...lista[i],...b,id:lista[i].id};
      await dbSet('reparo_fin_fixas',{fixas:lista});
      return res.status(200).json({ok:true});
    }
    if (req.method==='POST' && action==='del-fixa') {
      const {id}=req.body||{};
      const [fD]=await dbGet('reparo_fin_fixas');
      const lista=(fD?.fixas||(Array.isArray(fD)?fD:[])).filter(x=>x.id!==id);
      await dbSet('reparo_fin_fixas',{fixas:lista});
      return res.status(200).json({ok:true});
    }
    if (req.method==='POST' && action==='toggle-fixa') {
      const {id}=req.body||{};
      const [fD]=await dbGet('reparo_fin_fixas');
      const lista=(fD?.fixas||(Array.isArray(fD)?fD:[]));
      const f=lista.find(x=>x.id===id);
      if(f) f.ativo=!f.ativo;
      await dbSet('reparo_fin_fixas',{fixas:lista});
      return res.status(200).json({ok:true,ativo:f?.ativo});
    }

    // ── CONFIG ────────────────────────────────────────────────────
    if (req.method==='POST' && action==='config') {
      const b=req.body||{};
      await dbSet('reparo_fin_config',{
        metaMensal:parseFloat(b.metaMensal)||0,
        impostoPct:parseFloat(b.impostoPct)||6,
        nomeEmpresa:b.nomeEmpresa||'Reparo Eletro'
      });
      return res.status(200).json({ok:true});
    }

    // ── CONFIRMAR OS ──────────────────────────────────────────────
    if (req.method==='POST' && action==='confirmar-os') {
      const b=req.body||{};
      if (!b.osRef||!b.valor) return res.status(200).json({ok:false,error:'osRef e valor obrigatórios'});
      const [rD]=await dbGet('reparo_fin_receitas');
      const lista=(rD?.receitas||(Array.isArray(rD)?rD:[]));
      if (lista.find(x=>x.osRef===b.osRef && x.status==='recebido'))
        return res.status(200).json({ok:false,error:'OS já confirmada'});
      lista.unshift({
        id:uid(),
        descricao:b.descricao||`OS #${b.osCode||''} — ${b.nomeContato||'Cliente'}`,
        valor:parseFloat(b.valor)||0,
        data:b.data||today(),
        categoria:b.categoria||'servico',
        status:'recebido',
        osRef:b.osRef,
        cliente:b.nomeContato||'',
        criadoEm:new Date().toISOString()
      });
      await dbSet('reparo_fin_receitas',{receitas:lista});
      return res.status(200).json({ok:true});
    }

    // ── PAGAR CONTA FIXA ──────────────────────────────────────────
    if (req.method==='POST' && action==='pagar-fixa') {
      const b=req.body||{};
      if (!b.fixaId) return res.status(200).json({ok:false,error:'fixaId obrigatório'});
      const [fD,dD]=await dbGet('reparo_fin_fixas','reparo_fin_despesas');
      const fixas=(fD?.fixas||(Array.isArray(fD)?fD:[]));
      const fixa=fixas.find(x=>x.id===b.fixaId);
      if (!fixa) return res.status(200).json({ok:false,error:'conta não encontrada'});
      const despesas=(dD?.despesas||(Array.isArray(dD)?dD:[]));
      despesas.unshift({
        id:uid(), descricao:fixa.nome, valor:fixa.valor,
        data:b.data||today(), dataVencimento:b.data||today(),
        categoria:fixa.categoria, status:'pago',
        fixaRef:fixa.id, criadoEm:new Date().toISOString()
      });
      await dbSet('reparo_fin_despesas',{despesas});
      return res.status(200).json({ok:true});
    }

    // ── AI PARSE — interpreta texto de gasto server-side ──────────────────
    if (action === 'ai-parse') {
      const { texto } = req.body || {};
      if (!texto) return res.status(200).json({ok:false,error:'texto obrigatório'});
      const hoje = new Date().toISOString().slice(0,10);
      const sys = 'Você é assistente financeiro de uma assistência técnica brasileira. ' +
        'Analise o texto de gasto/despesa e retorne APENAS JSON válido sem markdown: ' +
        '{descricao(string capitalize máx 40 chars),valor(número positivo),categoria(peca_cmv|aluguel|energia|agua|telefone|salario|material|marketing|contabilidade|transporte|manutencao|outros),status(pago|pendente),data(YYYY-MM-DD)}. ' +
        'Hoje:' + hoje + '. Status padrão=pago. peca_cmv=peças/componentes eletrônicos. ' +
        'Se mencionar valor total gasto em peças num período, use esse valor total. ' +
        'Exemplos: "Fortec 120"→{"descricao":"Fortec","valor":120,"categoria":"peca_cmv","data":"' + hoje + '","status":"pago"} ' +
        '"gastamos 13464 com peças"→{"descricao":"Compra de Peças","valor":13464,"categoria":"peca_cmv","data":"' + hoje + '","status":"pago"} ' +
        '"conta de luz 280"→{"descricao":"Energia Elétrica","valor":280,"categoria":"energia","data":"' + hoje + '","status":"pago"}';
      try {
        const r = await fetch('https://api.anthropic.com/v1/messages', {
          method:'POST',
          headers:{'Content-Type':'application/json'},
          body:JSON.stringify({model:'claude-haiku-4-5-20251001',max_tokens:200,system:sys,messages:[{role:'user',content:texto}]})
        });
        const d = await r.json();
        const raw = (d.content?.[0]?.text||'').trim().replace(/```json?\n?/gi,'').replace(/```/g,'').trim();
        const parsed = JSON.parse(raw);
        if (!parsed.valor || parsed.valor <= 0) throw new Error('valor inválido');
        return res.status(200).json({ok:true, parsed});
      } catch(e) {
        return res.status(200).json({ok:false, error:'parse falhou: '+e.message});
      }
    }

    return res.status(200).json({ok:false,error:'Ação não encontrada'});

  } catch(e) {
    return res.status(200).json({ok:false, error:e.message});
  }
};
