// api/perf.js — raio-X de performance: tamanho de cada chave (STRLEN, O(1))
const U=(process.env.UPSTASH_URL||'').replace(/['"]/g,'').trim();
const T=(process.env.UPSTASH_TOKEN||'').replace(/[\n\r'"]/g,'').trim();

const CHAVES=[
  // Operacionais quentes
  'reparoeletro_pipe','tv_pipe',
  'reparoeletro_board','tv_board',
  'reparoeletro_financeiro','tv_financeiro',
  'reparoeletro_logistica','tv_logistica',
  'fichas_adm','fichas_tv','prospeccao_adm',
  'reparoeletro_frenteloja','tv_frenteloja',
  'gmb_pendentes','gmb_enviados',
  'agendamentos','sites_track','prospeccao_eventos',
  // Arquivos e backups internos (suspeitos de inchaço)
  'reparoeletro_arquivo','tv_arquivo',
  'reparoeletro_pipe_bak_pre_arquivo','tv_pipe_bak_pre_arquivo',
  'reparoeletro_backup_index','tv_backup_index',
  'reparoeletro_financeiro_backup','tv_financeiro_backup',
  // Backups diários (2 slots × chaves grandes)
  'bk_0_reparoeletro_pipe','bk_1_reparoeletro_pipe',
  'bk_0_reparoeletro_board','bk_1_reparoeletro_board',
  'bk_0_reparoeletro_financeiro','bk_1_reparoeletro_financeiro',
];

async function strlen(key){
  try{
    const r=await fetch(`${U}/strlen/${key}`,{headers:{Authorization:`Bearer ${T}`}});
    const j=await r.json();
    return typeof j.result==='number'?j.result:0;
  }catch{return -1;}
}
async function llen(key){
  try{
    const r=await fetch(`${U}/llen/${key}`,{headers:{Authorization:`Bearer ${T}`}});
    const j=await r.json();
    return typeof j.result==='number'?j.result:0;
  }catch{return -1;}
}

export default async function handler(req,res){
  res.setHeader('Cache-Control','no-cache');
  const action=req.query.action||'sizes';

  if(action==='sizes'){
    const tamanhos=await Promise.all(CHAVES.map(k=>strlen(k)));
    const evtLen=await llen('prospeccao_evt_list');

    const linhas=CHAVES.map((k,i)=>({chave:k,mb:+(tamanhos[i]/1048576).toFixed(2),bytes:tamanhos[i]}))
      .filter(l=>l.bytes>0)
      .sort((a,b)=>b.bytes-a.bytes);
    const totalMb=+(linhas.reduce((s,l)=>s+l.bytes,0)/1048576).toFixed(1);

    return res.status(200).json({ok:true,totalMb,
      eventosProspeccao:evtLen,
      top:linhas.slice(0,15),
      todas:linhas});
  }

  // Contagem por fase do pipe (mais pesado — 1 leitura do pipe)
  if(action==='pipe-fases'){
    try{
      const r=await fetch(`${U}/get/reparoeletro_pipe`,{headers:{Authorization:`Bearer ${T}`}});
      const j=await r.json();
      let v=j.result;
      if(typeof v==='string')v=JSON.parse(v);
      if(typeof v==='string')v=JSON.parse(v);
      const porPhase={};
      (v?.cards||[]).forEach(c=>{porPhase[c.phase]=(porPhase[c.phase]||0)+1;});
      return res.status(200).json({ok:true,total:(v?.cards||[]).length,porPhase});
    }catch(e){
      return res.status(200).json({ok:false,error:e.message});
    }
  }

  return res.status(404).json({ok:false,error:'Ação não encontrada'});
}
