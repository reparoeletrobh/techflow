// api/pj-fornecedores.js — Clientes PJ + Faturamento | Reparo Eletro BH
const U=(process.env.UPSTASH_URL||'').replace(/['"]/g,'').trim();
const T=(process.env.UPSTASH_TOKEN||'').replace(/['"]/g,'').trim();
const KEY='pj_fornecedores';

async function dbGet(k){try{const r=await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});const j=await r.json();const v=j[0]?.result;if(!v)return null;let x=JSON.parse(v);if(typeof x==='string')x=JSON.parse(x);return x;}catch(e){return null;}}
async function dbSet(k,v){await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});}

function defaultDB(){return{fornecedores:[],nextId:1,nextCobId:1};}

module.exports=async function handler(req,res){
  res.setHeader('Access-Control-Allow-Origin','https://reparoeletroadm.com');
  res.setHeader('Access-Control-Allow-Methods','GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers','Content-Type');
  if(req.method==='OPTIONS')return res.status(200).end();
  const action=req.query.action||'';

  // ── GET load ─────────────────────────────────────────────────────────────────
  if(action==='load'){
    const db=await dbGet(KEY)||defaultDB();
    return res.status(200).json({ok:true,fornecedores:db.fornecedores||[]});
  }

  // ── GET faturamento — todas as cobranças de todos os clientes ─────────────────
  if(action==='faturamento'){
    const db=await dbGet(KEY)||defaultDB();
    const hoje=new Date().toISOString().slice(0,10);
    const cobranças=[];
    (db.fornecedores||[]).forEach(function(f){
      (f.cobrancas||[]).forEach(function(c){
        const venc=c.vencimento||'';
        const status=c.status==='recebido'?'recebido':venc&&venc<hoje?'vencido':'pendente';
        cobranças.push({...c,status,clienteId:f.id,clienteNome:f.razaoSocial,clienteResp:f.responsavel,clienteTel:f.telefone});
      });
    });
    // Métricas
    const pendentes=cobranças.filter(c=>c.status!=='recebido');
    const totalPendente=pendentes.reduce((s,c)=>s+(parseFloat(c.valor)||0),0);
    const vencidas=cobranças.filter(c=>c.status==='vencido');
    const totalVencido=vencidas.reduce((s,c)=>s+(parseFloat(c.valor)||0),0);
    const recebidas=cobranças.filter(c=>c.status==='recebido');
    const totalRecebido=recebidas.reduce((s,c)=>s+(parseFloat(c.valor)||0),0);
    return res.status(200).json({ok:true,cobranças,totalPendente,totalVencido,totalRecebido,qtdPendentes:pendentes.length,qtdVencidas:vencidas.length});
  }

  // ── POST criar ────────────────────────────────────────────────────────────────
  if(req.method==='POST'&&action==='criar'){
    const b=req.body||{};
    if(!b.razaoSocial)return res.status(400).json({ok:false,error:'razão social obrigatória'});
    const db=await dbGet(KEY)||defaultDB();
    const id='FOR-'+String(db.nextId||1).padStart(4,'0');
    db.fornecedores.unshift({
      id,
      razaoSocial:b.razaoSocial||'',
      cnpj:b.cnpj||'',
      inscricaoEstadual:b.inscricaoEstadual||'',
      responsavel:b.responsavel||'',
      cargo:b.cargo||'',
      email:b.email||'',
      telefone:b.telefone||'',
      prazoAcordado:b.prazoAcordado||30,
      obsNF:b.obsNF||'',
      segmento:b.segmento||'',
      obs:b.obs||'',
      origem:b.origem||'',
      leadId:b.leadId||'',
      ativo:true,
      cobrancas:[],
      criadoEm:new Date().toISOString()
    });
    db.nextId=(db.nextId||1)+1;
    await dbSet(KEY,db);
    return res.status(200).json({ok:true,id});
  }

  // ── POST atualizar ────────────────────────────────────────────────────────────
  if(req.method==='POST'&&action==='atualizar'){
    const{id,...campos}=req.body||{};
    if(!id)return res.status(400).json({ok:false,error:'id obrigatório'});
    const db=await dbGet(KEY)||defaultDB();
    const idx=db.fornecedores.findIndex(x=>x.id===id);
    if(idx<0)return res.status(404).json({ok:false,error:'cliente não encontrado'});
    // Campos editáveis (não sobrescreve id, cobrancas, criadoEm)
    const editaveis=['razaoSocial','cnpj','inscricaoEstadual','responsavel','cargo','email','telefone','prazoAcordado','obsNF','segmento','obs'];
    editaveis.forEach(k=>{if(campos[k]!==undefined)db.fornecedores[idx][k]=campos[k];});
    db.fornecedores[idx].atualizadoEm=new Date().toISOString();
    await dbSet(KEY,db);
    return res.status(200).json({ok:true});
  }

  // ── POST gerar-cobranca ───────────────────────────────────────────────────────
  if(req.method==='POST'&&action==='gerar-cobranca'){
    const{clienteId,valor,vencimento,descricao}=req.body||{};
    if(!clienteId||!valor||!vencimento)return res.status(400).json({ok:false,error:'clienteId, valor e vencimento obrigatórios'});
    const db=await dbGet(KEY)||defaultDB();
    const cli=db.fornecedores.find(x=>x.id===clienteId);
    if(!cli)return res.status(404).json({ok:false,error:'cliente não encontrado'});
    if(!cli.cobrancas)cli.cobrancas=[];
    const cobId='COB-'+String(db.nextCobId||1).padStart(5,'0');
    db.nextCobId=(db.nextCobId||1)+1;
    const cob={id:cobId,valor:parseFloat(valor),vencimento,descricao:descricao||'',status:'pendente',criadoEm:new Date().toISOString(),recebidoEm:null,pipeCardId:null};
    cli.cobrancas.push(cob);
    await dbSet(KEY,db);
    return res.status(200).json({ok:true,cobranca:cob});
  }

  // ── POST marcar-recebido ──────────────────────────────────────────────────────
  if(req.method==='POST'&&action==='marcar-recebido'){
    const{clienteId,cobId}=req.body||{};
    const db=await dbGet(KEY)||defaultDB();
    const cli=db.fornecedores.find(x=>x.id===clienteId);
    if(!cli)return res.status(404).json({ok:false,error:'cliente não encontrado'});
    const cob=(cli.cobrancas||[]).find(x=>x.id===cobId);
    if(!cob)return res.status(404).json({ok:false,error:'cobrança não encontrada'});
    cob.status='recebido';
    cob.recebidoEm=new Date().toISOString();

    // Criar/atualizar ficha no pipe — fase receber
    let pipeOk=false,pipeId=null;
    try{
      const _PU=(process.env.UPSTASH_URL||'').replace(/['"]/g,'').trim();
      const _PT=(process.env.UPSTASH_TOKEN||'').replace(/['"]/g,'').trim();
      async function _pg(k){const r=await fetch(_PU+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+_PT,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});const j=await r.json();const v=j[0]?.result;if(!v)return null;try{let x=JSON.parse(v);if(typeof x==='string')x=JSON.parse(x);return x;}catch(e){return null;}}
      async function _ps(k,v){await fetch(_PU+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+_PT,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});}
      const pipeDb=await _pg('reparoeletro_pipe');
      if(pipeDb&&Array.isArray(pipeDb.cards)){
        const now=new Date().toISOString();
        // Verificar se já existe card para esta cobrança
        const existing=cob.pipeCardId?pipeDb.cards.find(c=>c.id===cob.pipeCardId):null;
        if(existing){
          existing.phase='receber';existing.movedAt=now;
          existing.valor=cob.valor;existing.obs='Recebido de '+cli.razaoSocial+' — '+cob.descricao;
          pipeId=existing.id;
        } else {
          const newId='PIPE-PJ-'+Date.now().toString(36).toUpperCase();
          const novoCard={id:newId,nomeContato:cli.razaoSocial,telefone:cli.telefone||'',equipamento:'Cobrança PJ — '+cob.descricao,valor:cob.valor,phase:'receber',origem:'pj_faturamento',criadoEm:now,movedAt:now,obs:'Cobrança '+cobId+' | Venc: '+cob.vencimento,history:[]};
          pipeDb.cards.unshift(novoCard);
          cob.pipeCardId=newId;pipeId=newId;
        }
        pipeDb.lastSync=now;
        await _ps('reparoeletro_pipe',pipeDb);
        pipeOk=true;
      }
    }catch(pe){console.error('pipe PJ:',pe.message);}

    await dbSet(KEY,db);
    return res.status(200).json({ok:true,pipeOk,pipeId});
  }

  // ── POST excluir-cobranca ─────────────────────────────────────────────────────
  if(req.method==='POST'&&action==='excluir-cobranca'){
    const{clienteId,cobId}=req.body||{};
    const db=await dbGet(KEY)||defaultDB();
    const cli=db.fornecedores.find(x=>x.id===clienteId);
    if(!cli)return res.status(404).json({ok:false,error:'não encontrado'});
    cli.cobrancas=(cli.cobrancas||[]).filter(x=>x.id!==cobId);
    await dbSet(KEY,db);
    return res.status(200).json({ok:true});
  }

  return res.status(404).json({ok:false,error:'action não encontrada: '+action});
};
