// api/agendamento.js — Sistema de Agendamentos Reparo Eletro BH
export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin','*');
  res.setHeader('Access-Control-Allow-Methods','GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers','Content-Type');
  if (req.method==='OPTIONS') return res.status(200).end();

  const U=(process.env.UPSTASH_URL||'').replace(/['"]/g,'').trim();
  const T=(process.env.UPSTASH_TOKEN||'').replace(/['"]/g,'').trim();
  const KEY='agendamentos';

  async function dbGet(k){
    try{const r=await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});const j=await r.json();let v=j[0]?.result;if(!v)return null;try{let x=JSON.parse(v);if(typeof x==='string')x=JSON.parse(x);return x;}catch(e){return null;}}catch(e){return null;}
  }
  async function dbSet(k,v){await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});}

  const action=req.query.action||'';
  const db=(await dbGet(KEY))||{agendamentos:[]};

  // ── LISTAR ────────────────────────────────────────────────────────────────
  if(action==='listar'){
    return res.status(200).json({ok:true,agendamentos:db.agendamentos||[]});
  }

  // ── CRIAR ─────────────────────────────────────────────────────────────────
  if(req.method==='POST'&&action==='criar'){
    const b=req.body||{};
    const novo={
      id:'ag-'+Date.now(),
      nome:b.nome||'',
      telefone:b.telefone||'',
      endereco:b.endereco||'',
      equipamento:b.equipamento||'',
      defeito:b.defeito||'',
      taxa:b.taxa||'',
      preco:b.preco||'',
      status:'pendente',
      criadoEm:new Date().toISOString(),
      atualizadoEm:new Date().toISOString()
    };
    db.agendamentos.unshift(novo);
    await dbSet(KEY,db);
    return res.status(200).json({ok:true,agendamento:novo});
  }

  // ── ORÇAMENTO ENVIADO (toggle) ──────────────────────────────────────────
  if(req.method==='POST'&&action==='orc-enviado'){
    const{id}=req.body||{};
    const ag=db.agendamentos.find(a=>a.id===id);
    if(!ag) return res.status(404).json({ok:false,error:'Não encontrado'});
    ag.orcEnviado=!ag.orcEnviado;
    ag.orcEnviadoEm=ag.orcEnviado?new Date().toISOString():null;
    ag.atualizadoEm=new Date().toISOString();
    await dbSet(KEY,db);
    return res.status(200).json({ok:true,orcEnviado:ag.orcEnviado});
  }

  // ── APROVAR: marca linha como aprovada (fica verde na tabela) ──────────────
  if(req.method==='POST'&&action==='aprovar'){
    const{id}=req.body||{};
    const ag=db.agendamentos.find(a=>a.id===id);
    if(!ag) return res.status(404).json({ok:false,error:'Não encontrado'});
    ag.aprovado=!ag.aprovado; // toggle — clica de novo para desmarcar
    ag.aprovadoEm=ag.aprovado?new Date().toISOString():null;
    ag.atualizadoEm=new Date().toISOString();
    await dbSet(KEY,db);
    return res.status(200).json({ok:true,aprovado:ag.aprovado});
  }

  // ── AGENDAR: setar data de agendamento ──────────────────────────────────
  if(req.method==='POST'&&action==='agendar'){
    const {id,dataAgendada}=req.body||{};
    const ag=db.agendamentos.find(a=>a.id===id);
    if(!ag) return res.status(404).json({ok:false,error:'Não encontrado'});
    ag.status='agendado';
    ag.dataAgendada=dataAgendada;
    ag.atualizadoEm=new Date().toISOString();
    await dbSet(KEY,db);
    return res.status(200).json({ok:true});
  }

  // ── EDITAR FICHA COMPLETA ──────────────────────────────────────────────────
  if(req.method==='POST'&&action==='editar'){
    const b=req.body||{};
    const ag=db.agendamentos.find(a=>a.id===b.id);
    if(!ag) return res.status(404).json({ok:false,error:'Não encontrado'});
    if(b.nome!==undefined)        ag.nome=b.nome;
    if(b.telefone!==undefined)    ag.telefone=b.telefone;
    if(b.endereco!==undefined)    ag.endereco=b.endereco;
    if(b.equipamento!==undefined) ag.equipamento=b.equipamento;
    if(b.defeito!==undefined)     ag.defeito=b.defeito;
    if(b.taxa!==undefined)        ag.taxa=b.taxa;
    if(b.preco!==undefined)       ag.preco=b.preco;
    ag.atualizadoEm=new Date().toISOString();
    await dbSet(KEY,db);
    return res.status(200).json({ok:true,agendamento:ag});
  }

  // ── ATUALIZAR PREÇO ───────────────────────────────────────────────────────
  if(req.method==='POST'&&action==='atualizar-preco'){
    const {id,preco}=req.body||{};
    const ag=db.agendamentos.find(a=>a.id===id);
    if(!ag) return res.status(404).json({ok:false,error:'Não encontrado'});
    ag.preco=preco;
    ag.atualizadoEm=new Date().toISOString();
    await dbSet(KEY,db);
    return res.status(200).json({ok:true});
  }

  // ── FINALIZAR (realizado ou cancelado) ────────────────────────────────────
  if(req.method==='POST'&&action==='finalizar'){
    const {id,status,preco,obs}=req.body||{};
    const ag=db.agendamentos.find(a=>a.id===id);
    if(!ag) return res.status(404).json({ok:false,error:'Não encontrado'});
    ag.status=status; // 'realizado' | 'cancelado'
    ag.finalizadoEm=new Date().toISOString();
    ag.atualizadoEm=new Date().toISOString();
    // Limpar flags especiais ao finalizar
    ag.orcEnviado=false; ag.orcEnviadoEm=null;
    ag.aprovado=false;   ag.aprovadoEm=null;
    if(preco!==undefined) ag.preco=preco;
    if(obs) ag.obs=obs;
    await dbSet(KEY,db);
    return res.status(200).json({ok:true});
  }

  // ── RELATÓRIO ─────────────────────────────────────────────────────────────
  if(action==='relatorio'){
    const ags=db.agendamentos||[];
    const pendentes  =ags.filter(a=>a.status==='pendente'||a.status==='agendado');
    const realizados =ags.filter(a=>a.status==='realizado');
    const cancelados =ags.filter(a=>a.status==='cancelado');
    const totalRealizado=realizados.reduce((s,a)=>s+parseFloat((a.preco||'0').replace(/[^\d.,]/g,'').replace(',','.'))||0,0);
    return res.status(200).json({ok:true,pendentes:pendentes.length,realizados:realizados.length,cancelados:cancelados.length,totalRealizado,agendamentos:ags});
  }

  return res.status(404).json({ok:false,error:'Ação não encontrada'});
}
