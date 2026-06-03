// api/pj-fornecedores.js — Cadastro de fornecedores/clientes PJ qualificados
const U = (process.env.UPSTASH_URL   || '').replace(/['"]/g,'').trim();
const T = (process.env.UPSTASH_TOKEN || '').replace(/['"]/g,'').trim();
const KEY = 'pj_fornecedores';

async function dbGet(k){try{const r=await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});const j=await r.json();const v=j[0]?.result;if(!v)return null;let x=JSON.parse(v);if(typeof x==='string')x=JSON.parse(x);return x;}catch(e){return null;}}
async function dbSet(k,v){await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});}

module.exports = async function handler(req,res){
  res.setHeader('Access-Control-Allow-Origin','*');
  if(req.method==='OPTIONS')return res.status(200).end();
  const action=req.query.action||'';

  if(action==='load'){
    const db=await dbGet(KEY)||{fornecedores:[],nextId:1};
    return res.status(200).json({ok:true,fornecedores:db.fornecedores||[]});
  }

  if(req.method==='POST'&&action==='criar'){
    const b=req.body||{};
    if(!b.razaoSocial||!b.cnpj)return res.status(400).json({ok:false,error:'razão social e CNPJ obrigatórios'});
    const db=await dbGet(KEY)||{fornecedores:[],nextId:1};
    const id='FOR-'+String(db.nextId||1).padStart(4,'0');
    db.fornecedores.unshift({id,...b,ativo:true,criadoEm:new Date().toISOString()});
    db.nextId=(db.nextId||1)+1;
    await dbSet(KEY,db);
    return res.status(200).json({ok:true,id});
  }

  if(req.method==='POST'&&action==='atualizar'){
    const{id,...campos}=req.body||{};
    const db=await dbGet(KEY)||{fornecedores:[],nextId:1};
    const idx=db.fornecedores.findIndex(x=>x.id===id);
    if(idx<0)return res.status(404).json({ok:false,error:'não encontrado'});
    db.fornecedores[idx]={...db.fornecedores[idx],...campos};
    await dbSet(KEY,db);
    return res.status(200).json({ok:true});
  }

  return res.status(404).json({ok:false,error:'action não encontrada'});
};
