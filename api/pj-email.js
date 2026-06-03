// api/pj-email.js — Disparos de email via Resend + leitura da inbox
const RESEND_KEY = (process.env.RESEND_API_KEY||'').trim();
const U = (process.env.UPSTASH_URL   ||'').replace(/['"]/g,'').trim();
const T = (process.env.UPSTASH_TOKEN ||'').replace(/['"]/g,'').trim();
const INBOX_KEY = 'pj_inbox';
const CAMP_KEY  = 'pj_campanhas';

async function dbGet(k){try{const r=await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});const j=await r.json();const v=j[0]?.result;if(!v)return null;let x=JSON.parse(v);if(typeof x==='string')x=JSON.parse(x);return x;}catch(e){return null;}}
async function dbSet(k,v){await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});}

module.exports = async function handler(req,res){
  res.setHeader('Access-Control-Allow-Origin','*');
  if(req.method==='OPTIONS')return res.status(200).end();
  const action=req.query.action||'';

  // GET inbox
  if(action==='inbox'){
    const db=await dbGet(INBOX_KEY)||{emails:[]};
    const naoLidos=(db.emails||[]).filter(e=>!e.lido).length;
    return res.status(200).json({ok:true,emails:db.emails||[],naoLidos});
  }

  // POST marcar-lido
  if(req.method==='POST'&&action==='marcar-lido'){
    const{id}=req.body||{};
    const db=await dbGet(INBOX_KEY)||{emails:[]};
    const e=db.emails.find(x=>x.id===id);
    if(e) e.lido=true;
    await dbSet(INBOX_KEY,db);
    return res.status(200).json({ok:true});
  }

  // POST enviar — envia email para um ou vários destinatários
  if(req.method==='POST'&&action==='enviar'){
    if(!RESEND_KEY)return res.status(400).json({ok:false,error:'RESEND_API_KEY não configurado'});
    const{de,para,assunto,html,texto,remetente}=req.body||{};
    if(!para||!assunto)return res.status(400).json({ok:false,error:'para e assunto obrigatórios'});
    const destinatarios=Array.isArray(para)?para:[para];
    const resultados=[];
    for(const dest of destinatarios){
      try{
        const r=await fetch('https://api.resend.com/emails',{
          method:'POST',
          headers:{'Content-Type':'application/json','Authorization':'Bearer '+RESEND_KEY},
          body:JSON.stringify({
            from: remetente||de||'Pedro <pedro@comercial.reparoeletrobh.com.br>',
            to:[dest.email||dest],
            subject:assunto.replace(/\{\{empresa\}\}/g,dest.empresa||'').replace(/\{\{responsavel\}\}/g,dest.responsavel||''),
            html:(html||'').replace(/\{\{empresa\}\}/g,dest.empresa||'').replace(/\{\{responsavel\}\}/g,dest.responsavel||'').replace(/\{\{cidade\}\}/g,dest.cidade||''),
            text:(texto||'').replace(/\{\{empresa\}\}/g,dest.empresa||'').replace(/\{\{responsavel\}\}/g,dest.responsavel||''),
          })
        });
        const j=await r.json();
        resultados.push({email:dest.email||dest,ok:!j.statusCode,id:j.id,erro:j.message});
      }catch(e){resultados.push({email:dest.email||dest,ok:false,erro:e.message});}
    }
    // Salvar campanha no log
    const cDb=await dbGet(CAMP_KEY)||{campanhas:[]};
    cDb.campanhas.unshift({id:Date.now().toString(36),assunto,total:destinatarios.length,
      enviados:resultados.filter(r=>r.ok).length,falhas:resultados.filter(r=>!r.ok).length,
      criadoEm:new Date().toISOString()});
    cDb.campanhas=cDb.campanhas.slice(0,100);
    await dbSet(CAMP_KEY,cDb);
    return res.status(200).json({ok:true,resultados,total:destinatarios.length,enviados:resultados.filter(r=>r.ok).length});
  }

  // GET campanhas
  if(action==='campanhas'){
    const db=await dbGet(CAMP_KEY)||{campanhas:[]};
    return res.status(200).json({ok:true,campanhas:db.campanhas||[]});
  }

  return res.status(404).json({ok:false,error:'action não encontrada'});
};
