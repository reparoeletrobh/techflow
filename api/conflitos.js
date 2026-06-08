// api/conflitos.js — Módulo Conflitos | Reparo Eletro BH TechFlow
const U=(process.env.UPSTASH_URL||'').replace(/['"]/g,'').trim();
const T=(process.env.UPSTASH_TOKEN||'').replace(/['"]/g,'').trim();
const KEY='reparoeletro_conflitos';

async function dbGet(k){
  try{const r=await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});const j=await r.json();const v=j[0]?.result;if(!v)return null;let x=JSON.parse(v);if(typeof x==='string')x=JSON.parse(x);return x;}catch(e){return null;}
}
async function dbSet(k,v){
  try{await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});}catch(e){}
}

function defaultDB(){ return { conflitos: [] }; }

function scoreOrdem(c){
  if(c.status==='resolvido') return 0;
  const p = c.prioridade==='critico'?3:c.prioridade==='alto'?2:1;
  return p;
}

module.exports = async function handler(req,res){
  res.setHeader('Access-Control-Allow-Origin','*');
  res.setHeader('Access-Control-Allow-Methods','GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers','Content-Type');
  if(req.method==='OPTIONS') return res.status(200).end();

  const action = req.query.action||'';
  const db = await dbGet(KEY) || defaultDB();
  if(!db.conflitos) db.conflitos=[];

  // ── GET listar ──────────────────────────────────────────────────────────────
  if(action==='listar'){
    const lista = [...db.conflitos].sort((a,b)=>scoreOrdem(b)-scoreOrdem(a)||new Date(b.criadoEm)-new Date(a.criadoEm));
    const abertos   = db.conflitos.filter(c=>c.status!=='resolvido');
    const criticos  = abertos.filter(c=>c.prioridade==='critico').length;
    const semResp   = abertos.filter(c=>!c.responsavel).length;
    return res.status(200).json({ok:true,conflitos:lista,total:db.conflitos.length,criticos,semResp,abertos:abertos.length});
  }

  // ── GET badge — só o contador para o header ─────────────────────────────────
  if(action==='badge'){
    const criticos = db.conflitos.filter(c=>c.status!=='resolvido'&&c.prioridade==='critico').length;
    const abertos  = db.conflitos.filter(c=>c.status!=='resolvido').length;
    return res.status(200).json({ok:true,criticos,abertos});
  }

  // ── POST criar ──────────────────────────────────────────────────────────────
  if(req.method==='POST'&&action==='criar'){
    const{titulo,tipo,prioridade,setor,ficha,responsavel,descricao,registradoPor}=req.body||{};
    if(!titulo||!prioridade) return res.status(400).json({ok:false,error:'titulo e prioridade obrigatórios'});
    const novo={
      id:'conf-'+Date.now().toString(36),
      titulo, tipo:tipo||'Outro', prioridade,
      setor:setor||'', ficha:ficha||'', responsavel:responsavel||'',
      descricao:descricao||'', registradoPor:registradoPor||'',
      status:'aberto', criadoEm:new Date().toISOString(),
      atualizadoEm:new Date().toISOString(),
      resolvidoEm:null, solucao:'', acaoPreventiva:''
    };
    db.conflitos.unshift(novo);
    await dbSet(KEY,db);
    return res.status(200).json({ok:true,conflito:novo});
  }

  // ── POST assumir ────────────────────────────────────────────────────────────
  if(req.method==='POST'&&action==='assumir'){
    const{id,responsavel}=req.body||{};
    const c=db.conflitos.find(x=>x.id===id);
    if(!c) return res.status(404).json({ok:false,error:'não encontrado'});
    c.responsavel=responsavel||c.responsavel;
    c.status='andamento'; c.atualizadoEm=new Date().toISOString();
    await dbSet(KEY,db);
    return res.status(200).json({ok:true,conflito:c});
  }

  // ── POST resolver ───────────────────────────────────────────────────────────
  if(req.method==='POST'&&action==='resolver'){
    const{id,solucao,acaoPreventiva}=req.body||{};
    const c=db.conflitos.find(x=>x.id===id);
    if(!c) return res.status(404).json({ok:false,error:'não encontrado'});
    c.status='resolvido'; c.solucao=solucao||'';
    c.acaoPreventiva=acaoPreventiva||'';
    c.resolvidoEm=new Date().toISOString();
    c.atualizadoEm=new Date().toISOString();
    await dbSet(KEY,db);
    return res.status(200).json({ok:true,conflito:c});
  }

  // ── POST editar ─────────────────────────────────────────────────────────────
  if(req.method==='POST'&&action==='editar'){
    const{id,...campos}=req.body||{};
    const c=db.conflitos.find(x=>x.id===id);
    if(!c) return res.status(404).json({ok:false,error:'não encontrado'});
    ['titulo','tipo','prioridade','setor','ficha','responsavel','descricao'].forEach(k=>{
      if(campos[k]!==undefined) c[k]=campos[k];
    });
    c.atualizadoEm=new Date().toISOString();
    await dbSet(KEY,db);
    return res.status(200).json({ok:true,conflito:c});
  }

  // ── POST excluir ────────────────────────────────────────────────────────────
  if(req.method==='POST'&&action==='excluir'){
    const{id}=req.body||{};
    db.conflitos=db.conflitos.filter(x=>x.id!==id);
    await dbSet(KEY,db);
    return res.status(200).json({ok:true});
  }

  // ── GET relatorio ───────────────────────────────────────────────────────────
  if(action==='relatorio'){
    const hoje=new Date();
    const semAgo=new Date(hoje.getTime()-7*24*3600*1000);
    const todos=db.conflitos;
    const daSemana=todos.filter(c=>new Date(c.criadoEm)>=semAgo);
    const resolvidosSem=daSemana.filter(c=>c.status==='resolvido');
    // Tempo médio resolução
    const tempos=resolvidosSem.filter(c=>c.resolvidoEm).map(c=>(new Date(c.resolvidoEm)-new Date(c.criadoEm))/3600000);
    const tmedio=tempos.length?Math.round(tempos.reduce((a,b)=>a+b,0)/tempos.length*10)/10:0;
    // Por tipo
    const porTipo={};
    daSemana.forEach(c=>{porTipo[c.tipo]=(porTipo[c.tipo]||0)+1;});
    const tiposOrdenados=Object.entries(porTipo).sort((a,b)=>b[1]-a[1]);
    return res.status(200).json({
      ok:true,
      periodo:{de:semAgo.toISOString().slice(0,10),ate:hoje.toISOString().slice(0,10)},
      abertos:todos.filter(c=>c.status!=='resolvido').length,
      resolvidosSemana:resolvidosSem.length,
      totalSemana:daSemana.length,
      tempoMedioHoras:tmedio,
      semResponsavel:todos.filter(c=>c.status!=='resolvido'&&!c.responsavel).length,
      criticos:todos.filter(c=>c.status!=='resolvido'&&c.prioridade==='critico').length,
      tiposOrdenados,
    });
  }

  return res.status(404).json({ok:false,error:'ação não encontrada: '+action});
};
