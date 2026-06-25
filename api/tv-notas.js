// api/tv-notas.js — Sistema de Notas TV + ADM
export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin','*');
  res.setHeader('Access-Control-Allow-Methods','GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers','Content-Type');
  if (req.method==='OPTIONS') return res.status(200).end();

  const U=(process.env.UPSTASH_URL||'').replace(/['"]/g,'').trim();
  const T=(process.env.UPSTASH_TOKEN||'').replace(/['"]/g,'').trim();
  const KEY='tv_notas';

  async function dbGet(k){try{const r=await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});const j=await r.json();let v=j[0]?.result;if(!v)return null;try{let x=JSON.parse(v);if(typeof x==='string')x=JSON.parse(x);return x;}catch(e){return null;}}catch(e){return null;}}
  async function dbSet(k,v){await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});}

  const action=req.query.action||'';
  const db=(await dbGet(KEY))||{notas:[]};

  // ── LISTAR ────────────────────────────────────────────────────────────────
  if(action==='listar'){
    const notas=(db.notas||[]).sort((a,b)=>a.vencimento.localeCompare(b.vencimento));
    return res.status(200).json({ok:true,notas});
  }

  // ── CRIAR ─────────────────────────────────────────────────────────────────
  if(req.method==='POST'&&action==='criar'){
    const {texto,vencimento,autor}=req.body||{};
    if(!texto||!vencimento) return res.status(400).json({ok:false,error:'Texto e vencimento obrigatórios'});
    const nota={
      id:'nota-'+Date.now(),
      texto,
      vencimento,
      autor:autor||'',
      criadaEm:new Date().toISOString(),
      status:'ativa'
    };
    db.notas.unshift(nota);
    await dbSet(KEY,db);
    return res.status(200).json({ok:true,nota});
  }

  // ── CONCLUIR ──────────────────────────────────────────────────────────────
  if(req.method==='POST'&&action==='concluir'){
    const {id}=req.body||{};
    const nota=db.notas.find(n=>n.id===id);
    if(!nota) return res.status(404).json({ok:false,error:'Nota não encontrada'});
    nota.status='concluida';
    nota.concluidaEm=new Date().toISOString();
    await dbSet(KEY,db);
    return res.status(200).json({ok:true});
  }

  // ── EXCLUIR ───────────────────────────────────────────────────────────────
  if(req.method==='POST'&&action==='excluir'){
    const {id}=req.body||{};
    db.notas=db.notas.filter(n=>n.id!==id);
    await dbSet(KEY,db);
    return res.status(200).json({ok:true});
  }

  return res.status(404).json({ok:false,error:'Ação não encontrada'});
}
