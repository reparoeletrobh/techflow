// api/tv-conflitos.js — Conflitos exclusivo do sistema TV (separado do ADM)
const U=(process.env.UPSTASH_URL||'').replace(/['"]/g,'').trim();
const T=(process.env.UPSTASH_TOKEN||'').replace(/['"]/g,'').trim();
const KEY='tv_conflitos'; // ← chave separada do ADM (reparoeletro_conflitos)

async function dbGet(k){try{const r=await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});const j=await r.json();const v=j[0]?.result;if(!v)return null;let x=JSON.parse(v);if(typeof x==='string')x=JSON.parse(x);return x;}catch(e){return null;}}
async function dbSet(k,v){try{await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});}catch(e){}}

export default async function handler(req,res){
  res.setHeader('Access-Control-Allow-Origin','*');
  res.setHeader('Access-Control-Allow-Methods','GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers','Content-Type');
  if(req.method==='OPTIONS') return res.status(200).end();

  const action=req.query.action||'';
  const db=(await dbGet(KEY))||{conflitos:[]};
  if(!db.conflitos) db.conflitos=[];

  // ── LISTAR ────────────────────────────────────────────────────────────────
  if(action==='listar'){
    const sorted=[...db.conflitos].sort((a,b)=>{
      const p=x=>x.prioridade==='critico'?3:x.prioridade==='alto'?2:1;
      return p(b)-p(a)||new Date(b.criadoEm)-new Date(a.criadoEm);
    });
    const abertos=db.conflitos.filter(c=>c.status!=='resolvido');
    const criticos=abertos.filter(c=>c.prioridade==='critico').length;
    return res.status(200).json({ok:true,conflitos:sorted,abertos:abertos.length,criticos});
  }

  // ── BADGE ─────────────────────────────────────────────────────────────────
  if(action==='badge'){
    const criticos=db.conflitos.filter(c=>c.status!=='resolvido'&&c.prioridade==='critico').length;
    const abertos =db.conflitos.filter(c=>c.status!=='resolvido').length;
    return res.status(200).json({ok:true,criticos,abertos});
  }

  // ── CRIAR ─────────────────────────────────────────────────────────────────
  if(req.method==='POST'&&action==='criar'){
    const{titulo,prioridade,setor,descricao,registradoPor}=req.body||{};
    if(!titulo||!prioridade) return res.status(400).json({ok:false,error:'titulo e prioridade obrigatórios'});
    const novo={
      id:'tvc-'+Date.now().toString(36),
      titulo, prioridade, setor:setor||'', descricao:descricao||'',
      registradoPor:registradoPor||'', status:'aberto',
      criadoEm:new Date().toISOString(), atualizadoEm:new Date().toISOString(),
      notas:[]
    };
    db.conflitos.unshift(novo);
    await dbSet(KEY,db);
    return res.status(200).json({ok:true,conflito:novo});
  }

  // ── RESOLVER ──────────────────────────────────────────────────────────────
  if(req.method==='POST'&&action==='resolver'){
    const{id}=req.body||{};
    const c=db.conflitos.find(x=>x.id===id);
    if(!c) return res.status(404).json({ok:false,error:'não encontrado'});
    c.status='resolvido'; c.resolvidoEm=new Date().toISOString(); c.atualizadoEm=new Date().toISOString();
    await dbSet(KEY,db);
    return res.status(200).json({ok:true});
  }

  // ── EXCLUIR ───────────────────────────────────────────────────────────────
  if(req.method==='POST'&&action==='excluir'){
    const{id}=req.body||{};
    db.conflitos=db.conflitos.filter(c=>c.id!==id);
    await dbSet(KEY,db);
    return res.status(200).json({ok:true});
  }

  // ── ADICIONAR NOTA ────────────────────────────────────────────────────────
  if(req.method==='POST'&&action==='adicionar-nota'){
    const{id,texto,vencimento,autor}=req.body||{};
    const c=db.conflitos.find(x=>x.id===id);
    if(!c) return res.status(404).json({ok:false,error:'não encontrado'});
    if(!c.notas) c.notas=[];
    c.notas.push({nid:'n-'+Date.now().toString(36),texto:texto||'',vencimento:vencimento||'',autor:autor||'',criadaEm:new Date().toISOString()});
    c.atualizadoEm=new Date().toISOString();
    await dbSet(KEY,db);
    return res.status(200).json({ok:true});
  }

  // ── EXCLUIR NOTA ──────────────────────────────────────────────────────────
  if(req.method==='POST'&&action==='excluir-nota'){
    const{id,nid}=req.body||{};
    const c=db.conflitos.find(x=>x.id===id);
    if(!c) return res.status(404).json({ok:false,error:'não encontrado'});
    c.notas=(c.notas||[]).filter(n=>n.nid!==nid);
    c.atualizadoEm=new Date().toISOString();
    await dbSet(KEY,db);
    return res.status(200).json({ok:true});
  }

  return res.status(404).json({ok:false,error:'ação não encontrada'});
};
