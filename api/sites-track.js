// api/sites-track.js — tracking de acessos/cliques das páginas /autorizada
// Chave Redis: sites_track = { dias: { "YYYY-MM-DD": { "dominio|pagina": {a,c} } } }
const U=(process.env.UPSTASH_URL||'').replace(/['"]/g,'').trim();
const T=(process.env.UPSTASH_TOKEN||'').replace(/[\n\r'"]/g,'').trim();
const KEY='sites_track';

async function dbGet(key){
  try{
    const r=await fetch(`${U}/get/${key}`,{headers:{Authorization:`Bearer ${T}`}});
    const j=await r.json();
    return j.result?JSON.parse(j.result):null;
  }catch{return null;}
}
async function dbSet(key,val){
  try{
    await fetch(`${U}/set/${key}`,{
      method:'POST',
      headers:{Authorization:`Bearer ${T}`,'Content-Type':'application/json'},
      body:JSON.stringify(val)
    });return true;
  }catch{return false;}
}

function hojeBRT(){
  return new Date(Date.now()-3*3600000).toISOString().slice(0,10);
}

export default async function handler(req,res){
  // 🔐 TF-AUTH (Fase 1): chave obrigatória em toda chamada
  const _tfk = (req.query && req.query.k) || req.headers['x-tf-key'] || '';
  if (_tfk !== ((process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim())) {
    return res.status(401).json({ ok: false, error: 'não autorizado' });
  }

  res.setHeader('Access-Control-Allow-Origin','*'); // páginas rodam em 4 domínios
  res.setHeader('Cache-Control','no-cache');
  if(req.method==='OPTIONS') return res.status(200).end();

  const q=req.query||{};
  const action=q.action||'';

  // ── HIT / CLICK ───────────────────────────────────────────────────────────
  if(action==='hit'||action==='click'){
    const pagina=String(q.p||'desconhecida').substring(0,60);
    const dominio=String(q.d||'desconhecido').substring(0,60);
    const dia=hojeBRT();
    const db=(await dbGet(KEY))||{dias:{}};
    if(!db.dias)db.dias={};
    if(!db.dias[dia])db.dias[dia]={};
    const k=dominio+'|'+pagina;
    if(!db.dias[dia][k])db.dias[dia][k]={a:0,c:0};
    if(action==='hit')db.dias[dia][k].a++;
    else db.dias[dia][k].c++;

    // Poda: manter só últimos 90 dias
    const dias=Object.keys(db.dias).sort();
    while(dias.length>90){ delete db.dias[dias.shift()]; }

    await dbSet(KEY,db);
    return res.status(200).json({ok:true});
  }

  // ── STATS: agregado hoje / 7 dias / total ────────────────────────────────
  if(action==='stats'){
    const db=(await dbGet(KEY))||{dias:{}};
    const dia=hojeBRT();
    const seteAtras=new Date(Date.now()-3*3600000-6*86400000).toISOString().slice(0,10);

    const agg={}; // "dominio|pagina" → {hoje:{a,c}, semana:{a,c}, total:{a,c}}
    for(const d of Object.keys(db.dias||{})){
      for(const k of Object.keys(db.dias[d])){
        if(!agg[k])agg[k]={hoje:{a:0,c:0},semana:{a:0,c:0},total:{a:0,c:0}};
        const v=db.dias[d][k];
        agg[k].total.a+=v.a; agg[k].total.c+=v.c;
        if(d>=seteAtras){ agg[k].semana.a+=v.a; agg[k].semana.c+=v.c; }
        if(d===dia){ agg[k].hoje.a+=v.a; agg[k].hoje.c+=v.c; }
      }
    }

    const linhas=Object.keys(agg).map(k=>{
      const [dominio,pagina]=k.split('|');
      return {dominio,pagina,...agg[k]};
    });
    return res.status(200).json({ok:true,linhas});
  }

  return res.status(404).json({ok:false,error:'Ação não encontrada'});
}
