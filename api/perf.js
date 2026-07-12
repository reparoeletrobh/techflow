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

export default async function handler(req,res){
  res.setHeader('Cache-Control','no-cache');
  const action=req.query.action||'sizes';

  if(action==='sizes'){
    // PIPELINE: todos os STRLENs + LLEN em UMA request (robusto sob throttling)
    const cmds=CHAVES.map(k=>['STRLEN',k]).concat([['LLEN','prospeccao_evt_list']]);
    let results=[];
    try{
      const r=await fetch(`${U}/pipeline`,{
        method:'POST',
        headers:{Authorization:`Bearer ${T}`,'Content-Type':'application/json'},
        body:JSON.stringify(cmds)
      });
      results=await r.json();
    }catch(e){
      return res.status(200).json({ok:false,error:'pipeline: '+e.message});
    }
    const tamanhos=CHAVES.map((k,i)=>Number(results[i]?.result)||0);
    const evtLen=Number(results[CHAVES.length]?.result)||0;

    const linhas=CHAVES.map((k,i)=>({chave:k,mb:+(tamanhos[i]/1048576).toFixed(2),bytes:tamanhos[i]}))
      .filter(l=>l.bytes>0)
      .sort((a,b)=>b.bytes-a.bytes);
    const totalMb=+(linhas.reduce((s,l)=>s+l.bytes,0)/1048576).toFixed(1);

    return res.status(200).json({ok:true,totalMb,
      eventosProspeccao:evtLen,
      top:linhas.slice(0,15),
      todas:linhas});
  }

  // Composição do financeiro: registros por fase, peso dos anexos
  if(action==='fin-composicao'){
    try{
      const r=await fetch(`${U}/get/reparoeletro_financeiro`,{headers:{Authorization:`Bearer ${T}`}});
      const j=await r.json();
      let v=j.result;
      if(typeof v==='string')v=JSON.parse(v);
      if(typeof v==='string')v=JSON.parse(v);
      const recs=v?.records||[];
      const porPhase={},pesoPorPhase={};
      let comAnexo=0,pesoAnexos=0,maiorAnexo=0;
      for(const rec of recs){
        const ph=rec.phaseId||'sem_fase';
        porPhase[ph]=(porPhase[ph]||0)+1;
        const peso=JSON.stringify(rec).length;
        pesoPorPhase[ph]=(pesoPorPhase[ph]||0)+peso;
        if(rec.anexo){
          comAnexo++;
          const pa=String(rec.anexo).length;
          pesoAnexos+=pa;
          if(pa>maiorAnexo)maiorAnexo=pa;
        }
      }
      Object.keys(pesoPorPhase).forEach(k=>{pesoPorPhase[k]=+(pesoPorPhase[k]/1048576).toFixed(2);});
      return res.status(200).json({ok:true,totalRegistros:recs.length,
        comAnexo,pesoAnexosMb:+(pesoAnexos/1048576).toFixed(2),
        maiorAnexoKb:Math.round(maiorAnexo/1024),
        porPhase,pesoMbPorPhase:pesoPorPhase});
    }catch(e){
      return res.status(200).json({ok:false,error:e.message});
    }
  }

  // Peso das propriedades raiz do financeiro — RESUMO compacto (agrupa por prefixo)
  if(action==='fin-raiz'){
    try{
      const r=await fetch(`${U}/get/reparoeletro_financeiro`,{headers:{Authorization:`Bearer ${T}`}});
      const j=await r.json();
      let v=j.result;
      if(typeof v==='string')v=JSON.parse(v);
      if(typeof v==='string')v=JSON.parse(v);
      const keys=Object.keys(v||{});
      // agrupar por prefixo (texto antes do primeiro dígito ou 12 primeiros chars)
      const grupos={};
      for(const k of keys){
        const pref=(k.match(/^[^0-9]*/)||[k])[0].slice(0,20)||k.slice(0,12);
        if(!grupos[pref])grupos[pref]={qtd:0,mb:0,exemplo:k};
        grupos[pref].qtd++;
        grupos[pref].mb+=JSON.stringify(v[k]).length/1048576;
      }
      const resumo=Object.keys(grupos).map(g=>({
        grupo:g, qtd:grupos[g].qtd, mb:+grupos[g].mb.toFixed(2), exemplo:grupos[g].exemplo
      })).sort((a,b)=>b.mb-a.mb).slice(0,15);
      return res.status(200).json({ok:true,totalPropriedades:keys.length,top15Grupos:resumo});
    }catch(e){
      return res.status(200).json({ok:false,error:e.message});
    }
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
