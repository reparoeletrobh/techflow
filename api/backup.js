// api/backup.js — Backup diário completo das chaves operacionais
// 1) Snapshot no Redis: backup_full_<0-6> (rotação semanal, não cresce)
// 2) E-mail via Resend com o JSON anexado (cópia off-site)
// Cron: diário 03:30 BRT (06:30 UTC) — ver vercel.json
import { gzipSync } from 'zlib';
const U=(process.env.UPSTASH_URL||'').replace(/['"]/g,'').trim();
const T=(process.env.UPSTASH_TOKEN||'').replace(/[\n\r'"]/g,'').trim();
const RESEND_KEY=process.env.RESEND_API_KEY||'';
const BACKUP_EMAIL=process.env.BACKUP_EMAIL||'reparoeletrobh@gmail.com';

// Chaves operacionais críticas (dados que não podem ser perdidos)
const CHAVES=[
  'reparoeletro_pipe','tv_pipe',
  'reparoeletro_financeiro','tv_financeiro',
  'reparoeletro_board','tv_board',
  'reparoeletro_logistica','tv_logistica',
  'reparoeletro_balcao',
  'reparoeletro_frenteloja','tv_frenteloja',
  'reparoeletro_orcamentos','tv_orcamentos',
  'reparoeletro_vendas','tv_vendas',
  'reparoeletro_checkout_vendas','tv_checkout_vendas',
  'reparoeletro_compra_equip',
  'fichas_adm','fichas_tv','fichas_sheet_cursor',
  'prospeccao_adm',
  'agendamentos',
  'pj_enviados','pj_inbox',
  'gmb_enviados','gmb_pendentes',
  'sites_track',
];

// GET cru (sem parse — preserva o valor exatamente como está)
async function dbGetRaw(key){
  try{
    const r=await fetch(`${U}/get/${key}`,{headers:{Authorization:`Bearer ${T}`}});
    const j=await r.json();
    return j.result??null;
  }catch{return null;}
}
async function dbSetRaw(key,rawStr){
  try{
    const r=await fetch(`${U}/set/${key}`,{
      method:'POST',
      headers:{Authorization:`Bearer ${T}`,'Content-Type':'application/json'},
      body:rawStr
    });
    if(!r.ok)return false;
    const j=await r.json().catch(()=>null);
    return !!(j&&(j.result==='OK'||j.result));
  }catch{return false;}
}

export default async function handler(req,res){
  res.setHeader('Cache-Control','no-cache');
  const action=req.query.action||'';

  // ── RUN: executa o backup completo ───────────────────────────────────────
  if(action==='run'){
    try{
      const valores=await Promise.all(CHAVES.map(k=>dbGetRaw(k)));
      const snapshot={ geradoEm:new Date().toISOString(), chaves:{} };
      let vazias=0;
      CHAVES.forEach((k,i)=>{
        snapshot.chaves[k]=valores[i];
        if(valores[i]==null)vazias++;
      });

      const json=JSON.stringify(snapshot);
      const bytes=Buffer.byteLength(json,'utf8');

      // 1) Snapshot no Redis POR CHAVE (rotação de 2 dias: par/ímpar)
      //    — chaves individuais têm o mesmo tamanho que o sistema já grava
      //    normalmente, então nenhum SET estoura limite de request
      const dow=new Date(Date.now()-3*3600000).getUTCDay();
      const slot=dow%2; // 0 ou 1 → mantém ontem e hoje (retenção longa fica no e-mail)
      let gravadas=0, falhas=[];
      for(let i=0;i<CHAVES.length;i++){
        if(valores[i]==null)continue;
        const okSet=await dbSetRaw(`bk_${slot}_${CHAVES[i]}`,
          JSON.stringify({em:snapshot.geradoEm,v:valores[i]}));
        if(okSet)gravadas++; else falhas.push(CHAVES[i]);
      }

      // 2) E-mail com anexo comprimido (.json.gz — ~10x menor)
      let emailStatus='nao_enviado', gzBytes=0;
      if(RESEND_KEY){
        try{
          const gz=gzipSync(Buffer.from(json));
          gzBytes=gz.length;
          if(gzBytes<15*1024*1024){
            const dia=new Date(Date.now()-3*3600000).toISOString().slice(0,10);
            const er=await fetch('https://api.resend.com/emails',{
              method:'POST',
              headers:{'Authorization':'Bearer '+RESEND_KEY,'Content-Type':'application/json'},
              body:JSON.stringify({
                from:'Backup TechFlow <pedro@comercial.reparoeletroadm.com>',
                to:[BACKUP_EMAIL],
                subject:`💾 Backup diário TechFlow — ${dia} (${(bytes/1048576).toFixed(1)} MB → ${(gzBytes/1048576).toFixed(1)} MB comprimido)`,
                html:`<p>Backup automático dos dados operacionais do sistema.</p>
<p><b>Data:</b> ${dia}<br><b>Chaves com dados:</b> ${CHAVES.length-vazias} de ${CHAVES.length}<br><b>Tamanho:</b> ${(bytes/1048576).toFixed(1)} MB (anexo comprimido: ${(gzBytes/1048576).toFixed(1)} MB)</p>
<p>O anexo é um .json.gz — descompacte com qualquer ferramenta de zip para acessar o JSON completo. Guarde este e-mail: ele permite restaurar o sistema em caso de perda de dados.</p>`,
                attachments:[{filename:`backup-techflow-${dia}.json.gz`,content:gz.toString('base64')}]
              })
            });
            emailStatus=er.ok?'enviado':`erro_http_${er.status}`;
          } else { emailStatus='muito_grande_mesmo_comprimido'; }
        }catch(e){emailStatus='erro_'+e.message;}
      } else { emailStatus='sem_chave_resend'; }

      return res.status(200).json({ok:true,chaves:CHAVES.length,comDados:CHAVES.length-vazias,
        bytes,gzBytes,slot,gravadasRedis:gravadas,falhasRedis:falhas,email:emailStatus});
    }catch(e){
      return res.status(200).json({ok:false,error:e.message});
    }
  }

  // ── STATUS: resumo dos snapshots por slot ────────────────────────────────
  if(action==='status'){
    const out=[];
    for(const slot of [0,1]){
      let chavesOk=0, totalBytes=0, geradoEm=null;
      const vals=await Promise.all(CHAVES.map(k=>dbGetRaw(`bk_${slot}_${k}`)));
      vals.forEach(v=>{
        if(v==null)return;
        chavesOk++; totalBytes+=Buffer.byteLength(String(v),'utf8');
        if(!geradoEm){ try{ geradoEm=JSON.parse(v).em; }catch{} }
      });
      out.push({slot,chaves:chavesOk,mb:+(totalBytes/1048576).toFixed(2),geradoEm});
    }
    return res.status(200).json({ok:true,slots:out,emailDestino:BACKUP_EMAIL});
  }

  return res.status(404).json({ok:false,error:'Ação não encontrada'});
}
