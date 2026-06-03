// api/pj-prospects.js — CRUD base de prospects PJ
const U = (process.env.UPSTASH_URL   || '').replace(/['"]/g,'').trim();
const T = (process.env.UPSTASH_TOKEN || '').replace(/['"]/g,'').trim();
const KEY = 'pj_prospects';

async function dbGet(k){try{const r=await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});const j=await r.json();const v=j[0]?.result;if(!v)return null;let x=JSON.parse(v);if(typeof x==='string')x=JSON.parse(x);return x;}catch(e){return null;}}
async function dbSet(k,v){await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});}

module.exports = async function handler(req,res){
  res.setHeader('Access-Control-Allow-Origin','*');
  if(req.method==='OPTIONS')return res.status(200).end();
  const action=req.query.action||'';

  // GET load — lista todos
  if(action==='load'){
    const db=await dbGet(KEY)||{prospects:[],nextId:1};
    return res.status(200).json({ok:true,prospects:db.prospects||[]});
  }

  // POST criar
  if(req.method==='POST'&&action==='criar'){
    const{empresa,cnpj,telefone,email,responsavel,cargo,cidade,uf,segmento,obs}=req.body||{};
    if(!empresa)return res.status(400).json({ok:false,error:'empresa obrigatória'});
    const db=await dbGet(KEY)||{prospects:[],nextId:1};
    const id='PJ-'+String(db.nextId||1).padStart(4,'0');
    db.prospects.unshift({id,empresa,cnpj:cnpj||'',telefone:telefone||'',email:email||'',
      responsavel:responsavel||'',cargo:cargo||'',cidade:cidade||'',uf:uf||'',
      segmento:segmento||'',obs:obs||'',status:'novo',
      historico:[],criadoEm:new Date().toISOString()});
    db.nextId=(db.nextId||1)+1;
    await dbSet(KEY,db);
    return res.status(200).json({ok:true,id});
  }

  // POST atualizar-status
  if(req.method==='POST'&&action==='atualizar-status'){
    const{id,status,obs}=req.body||{};
    const db=await dbGet(KEY)||{prospects:[],nextId:1};
    const p=db.prospects.find(x=>x.id===id);
    if(!p)return res.status(404).json({ok:false,error:'não encontrado'});
    p.status=status;
    if(obs){p.historico=p.historico||[];p.historico.unshift({ts:new Date().toISOString(),texto:obs,tipo:'status'});}
    await dbSet(KEY,db);
    return res.status(200).json({ok:true});
  }

  // POST add-historico
  if(req.method==='POST'&&action==='add-historico'){
    const{id,texto,tipo}=req.body||{};
    const db=await dbGet(KEY)||{prospects:[],nextId:1};
    const p=db.prospects.find(x=>x.id===id);
    if(!p)return res.status(404).json({ok:false,error:'não encontrado'});
    p.historico=p.historico||[];
    p.historico.unshift({ts:new Date().toISOString(),texto,tipo:tipo||'nota'});
    await dbSet(KEY,db);
    return res.status(200).json({ok:true});
  }

  // DELETE deletar
  if(req.method==='DELETE'&&action==='deletar'){
    const{id}=req.body||{};
    const db=await dbGet(KEY)||{prospects:[],nextId:1};
    db.prospects=db.prospects.filter(x=>x.id!==id);
    await dbSet(KEY,db);
    return res.status(200).json({ok:true});
  }

  // ── Listas ──────────────────────────────────────────────────────────────
  if(action==='load-listas'){
    const db=await dbGet(KEY)||{prospects:[],nextId:1,listas:[]};
    return res.status(200).json({ok:true,listas:db.listas||[]});
  }
  if(req.method==='POST'&&action==='criar-lista'){
    const{nome}=req.body||{};
    if(!nome)return res.status(400).json({ok:false,error:'nome obrigatório'});
    const db=await dbGet(KEY)||{prospects:[],nextId:1,listas:[]};
    if(!db.listas)db.listas=[];
    const id='LST-'+Date.now().toString(36).toUpperCase();
    db.listas.push({id,nome,criadoEm:new Date().toISOString()});
    await dbSet(KEY,db);
    return res.status(200).json({ok:true,id,nome});
  }
  if(req.method==='POST'&&action==='deletar-lista'){
    const{id}=req.body||{};
    const db=await dbGet(KEY)||{prospects:[],nextId:1,listas:[]};
    db.listas=(db.listas||[]).filter(l=>l.id!==id);
    // Remover lista dos prospects
    (db.prospects||[]).forEach(p=>{if(p.listaId===id)p.listaId=null;});
    await dbSet(KEY,db);
    return res.status(200).json({ok:true});
  }
  if(req.method==='POST'&&action==='atribuir-lista'){
    const{id,listaId}=req.body||{};
    const db=await dbGet(KEY)||{prospects:[],nextId:1,listas:[]};
    const p=db.prospects.find(x=>x.id===id);
    if(!p)return res.status(404).json({ok:false,error:'não encontrado'});
    p.listaId=listaId||null;
    await dbSet(KEY,db);
    return res.status(200).json({ok:true});
  }
    return res.status(404).json({ok:false,error:'action não encontrada'});
};
