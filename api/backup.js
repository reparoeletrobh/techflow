// api/backup.js — Backup diário completo das chaves operacionais
// 1) Snapshot no Redis: backup_full_<0-6> (rotação semanal, não cresce)
// 2) E-mail via Resend com o JSON anexado (cópia off-site)
// Cron: diário 03:30 BRT (06:30 UTC) — ver vercel.json
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
    await fetch(`${U}/set/${key}`,{
      method:'POST',
      headers:{Authorization:`Bearer ${T}`,'Content-Type':'application/json'},
      body:rawStr
    });return true;
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

      // 1) Snapshot no Redis com rotação semanal (backup_full_0 a backup_full_6)
      const dow=new Date(Date.now()-3*3600000).getUTCDay(); // dia da semana BRT
      await dbSetRaw(`backup_full_${dow}`,json);

      // 2) E-mail com anexo (só se < 10MB — acima disso o snapshot Redis basta)
      let emailStatus='nao_enviado';
      if(RESEND_KEY && bytes<10*1024*1024){
        const dia=new Date(Date.now()-3*3600000).toISOString().slice(0,10);
        try{
          const er=await fetch('https://api.resend.com/emails',{
            method:'POST',
            headers:{'Authorization':'Bearer '+RESEND_KEY,'Content-Type':'application/json'},
            body:JSON.stringify({
              from:'Backup TechFlow <pedro@comercial.reparoeletroadm.com>',
              to:[BACKUP_EMAIL],
              subject:`💾 Backup diário TechFlow — ${dia} (${(bytes/1024).toFixed(0)} KB, ${CHAVES.length-vazias}/${CHAVES.length} chaves)`,
              html:`<p>Backup automático dos dados operacionais do sistema.</p>
<p><b>Data:</b> ${dia}<br><b>Chaves com dados:</b> ${CHAVES.length-vazias} de ${CHAVES.length}<br><b>Tamanho:</b> ${(bytes/1024).toFixed(0)} KB</p>
<p>Guarde este e-mail — o anexo permite restaurar o sistema em caso de perda de dados. Também há um snapshot rotativo dos últimos 7 dias dentro do próprio banco.</p>`,
              attachments:[{filename:`backup-techflow-${dia}.json`,content:Buffer.from(json).toString('base64')}]
            })
          });
          emailStatus=er.ok?'enviado':`erro_http_${er.status}`;
        }catch(e){emailStatus='erro_'+e.message;}
      } else if(!RESEND_KEY){ emailStatus='sem_chave_resend'; }
      else { emailStatus='muito_grande_so_redis'; }

      return res.status(200).json({ok:true,chaves:CHAVES.length,comDados:CHAVES.length-vazias,bytes,slotRedis:`backup_full_${dow}`,email:emailStatus});
    }catch(e){
      return res.status(200).json({ok:false,error:e.message});
    }
  }

  // ── STATUS: lista os snapshots existentes ────────────────────────────────
  if(action==='status'){
    const slots=await Promise.all([0,1,2,3,4,5,6].map(async d=>{
      const raw=await dbGetRaw(`backup_full_${d}`);
      if(!raw)return{slot:d,existe:false};
      let geradoEm=null;
      try{ geradoEm=JSON.parse(raw).geradoEm; }catch{}
      return{slot:d,existe:true,bytes:Buffer.byteLength(String(raw),'utf8'),geradoEm};
    }));
    return res.status(200).json({ok:true,slots,emailDestino:BACKUP_EMAIL});
  }

  return res.status(404).json({ok:false,error:'Ação não encontrada'});
}
