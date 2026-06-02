// api/atendimento.js v3.5 — orcamento + pagamento confirmado + fix backlog
const U=process.env.UPSTASH_URL,T=process.env.UPSTASH_TOKEN,PT=process.env.PIPEFY_TOKEN;
const PA='https://api.pipefy.com/graphql',PID='305832912',ERP_ID='339008925';
const CK='reparoeletro_compra_equip',VK='reparoeletro_vendas',CACHE='reparoeletro_atendimento_cache';
const ORC_KEY='reparoeletro_orcamentos',FIN_KEY='reparoeletro_financeiro',BOARD_KEY='reparoeletro_board';
async function dbG(k){const r=await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T},body:JSON.stringify([['GET',k]])});const j=await r.json();return j[0]?.result?JSON.parse(j[0].result):null;}
async function dbS(k,v){await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});}
async function pf(q){const r=await fetch(PA,{method:'POST',headers:{Authorization:'Bearer '+PT,'Content-Type':'application/json'},body:JSON.stringify({query:q})});const j=await r.json();if(j.errors?.length)throw new Error(j.errors[0].message);return j.data;}
function brtStartOf(unit){
  const now=new Date();
  const fmt=new Intl.DateTimeFormat('en-CA',{timeZone:'America/Sao_Paulo',year:'numeric',month:'2-digit',day:'2-digit'});
  const [y,m,d]=fmt.format(now).split('-').map(Number);
  if(unit==='day') return new Date(Date.UTC(y,m-1,d,3,0,0,0));
  if(unit==='week'){
    const brtDate=new Date(now.toLocaleString('en-US',{timeZone:'America/Sao_Paulo'}));
    const dow=brtDate.getDay();const daysToMon=dow===0?6:dow-1;
    return new Date(Date.UTC(y,m-1,d-daysToMon,3,0,0,0));
  }
  if(unit==='month') return new Date(Date.UTC(y,m-1,1,3,0,0,0));
  return new Date(0);
}
function fmtNome(f){return (f.nomeContato||f.title||'').split('—')[0].split('|')[0].trim().slice(0,60)||'Sem nome';}
export default async function handler(req,res){
  res.setHeader('Access-Control-Allow-Origin','*');
  res.setHeader('Access-Control-Allow-Methods','GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers','Content-Type');
  if(req.method==='OPTIONS')return res.status(200).end();
  if(req.query.action==='metrics'){
    try{
      const todayUTC=brtStartOf('day'),weekUTC=brtStartOf('week'),monthUTC=brtStartOf('month');
      const Q1='query{pipe(id:"'+PID+'"){cards_count} phase(id:"'+ERP_ID+'"){cards_count cards(first:50){edges{node{id fields{name value}}}pageInfo{hasNextPage}}}}';
      const Q2='query{allCards(pipeId:"'+PID+'",first:2000){edges{node{id title created_at}}}}';
      const[pd,cd,vd,ficQ,orcDb,finDb,boardDb]=await Promise.all([
        pf(Q1),dbG(CK),dbG(VK),pf(Q2).catch(()=>null),
        dbG(ORC_KEY),dbG(FIN_KEY),dbG(BOARD_KEY).catch(()=>null)
      ]);
      // FICHAS — logistica (reparoeletro_atend_logistica)
      const atendLogDb=await dbG('reparoeletro_atend_logistica').catch(()=>null);
      const atendFichas=atendLogDb?.fichas||[];
      const fichasTotal=atendFichas.length;
      const ficDateOf=f=>new Date(f.registradoEm||0).getTime();
      const ficHoje=atendFichas.filter(f=>ficDateOf(f)>=todayUTC.getTime()).length||null;
      const ficSem=atendFichas.filter(f=>ficDateOf(f)>=weekUTC.getTime()).length||null;
      const ficMes=atendFichas.filter(f=>ficDateOf(f)>=monthUTC.getTime()).length||null;
      const fichasHojeList=atendFichas.filter(f=>ficDateOf(f)>=todayUTC.getTime()).map(f=>({id:f.id,title:(f.nome||'')+(f.equipamento?' — '+f.equipamento:''),createdAt:f.registradoEm}));
      // ERP
      const ep=pd?.phase;
      const erpTotal=ep?.cards_count||0;
      const erpEdges=ep?.cards?.edges?.map(e=>e.node)||[];
      const erpSV=erpEdges.filter(card=>{const vf=card.fields?.find(f=>/(valor|preco|preço)/i.test(f.name||''));return !vf?.value||parseFloat(String(vf.value||'0').replace(/[^0-9.,]/g,'').replace(',','.'))||0===0;}).length;
      const erpMore=ep?.cards?.pageInfo?.hasNextPage?Math.max(0,erpTotal-50):0;
      const ERP_SK='reparoeletro_erp_seen';
      const erpSeen=await dbG(ERP_SK)||{};
      const nowISO=new Date().toISOString();
      let erpDirty=false;
      for(const card of erpEdges){if(!erpSeen[card.id]){erpSeen[card.id]=nowISO;erpDirty=true;}}
      if(erpDirty) await dbS(ERP_SK,erpSeen);
      const erpHoje=erpEdges.filter(c=>new Date(erpSeen[c.id]||0).getTime()>=todayUTC.getTime()).length||null;
      const erpSemana=erpEdges.filter(c=>new Date(erpSeen[c.id]||0).getTime()>=weekUTC.getTime()).length||null;
      // ORCAMENTO
      const orcFichas=orcDb?.fichas||[];
      const orcTs=orcFichas.map(f=>f.createdAt||f.syncedAt||null).filter(Boolean);
      const orcHoje=orcTs.filter(ts=>new Date(ts).getTime()>=todayUTC.getTime()).length||null;
      const orcSem=orcTs.filter(ts=>new Date(ts).getTime()>=weekUTC.getTime()).length||null;
      // PAGAMENTO CONFIRMADO
      const finRecords=finDb?.records||[];
      const pgTs=finRecords.map(r=>{
        const h=(r.history||[]).find(e=>e.phaseId==='pagamento_confirmado');
        return h?.ts||null;
      }).filter(Boolean);
      const pgHoje=pgTs.filter(ts=>new Date(ts).getTime()>=todayUTC.getTime()).length||null;
      const pgSem=pgTs.filter(ts=>new Date(ts).getTime()>=weekUTC.getTime()).length||null;
      // COMPRADOS
      const fichas=cd?.fichas||[];
      const comprados=fichas.filter(f=>f.status==='comprado');
      const tsOf=f=>new Date(f.statusAt||f.createdAt||0).getTime();
      const compH=comprados.filter(f=>tsOf(f)>=todayUTC.getTime()).length;
      const compS=comprados.filter(f=>tsOf(f)>=weekUTC.getTime()).length;
      const compMArr=comprados.filter(f=>tsOf(f)>=monthUTC.getTime()).sort((a,b)=>tsOf(b)-tsOf(a));
      const compMCount=compMArr.length;
      const compTotal=comprados.length;
      const compAntArr=comprados.filter(f=>tsOf(f)<monthUTC.getTime()).sort((a,b)=>tsOf(b)-tsOf(a));
      // CADASTRADOS / VENDIDOS
      const produtos=vd?.produtos||[];
      const cadOf=p=>new Date(p.createdAt||0).getTime();
      const cadH=produtos.filter(p=>cadOf(p)>=todayUTC.getTime()).length;
      const cadS=produtos.filter(p=>cadOf(p)>=weekUTC.getTime()).length;
      const cadM=produtos.filter(p=>cadOf(p)>=monthUTC.getTime()).length;
      const cadTotal=produtos.length;
      const soldOf=p=>new Date(p.soldAt||0).getTime();
      const vendidos=produtos.filter(p=>p.vendido||p.soldAt);
      const vendH=vendidos.filter(p=>soldOf(p)>=todayUTC.getTime()).length;
      const vendS=vendidos.filter(p=>soldOf(p)>=weekUTC.getTime()).length;
      const vendTotal=vendidos.length;
      // MONTHLY
      const fichasEsteMes=compMArr.map(f=>({nome:fmtNome(f),statusAt:f.statusAt||f.createdAt,cadastrado:!!f.cadastradoVendas}));
      const fichasAnteriores=compAntArr.map(f=>({nome:fmtNome(f),statusAt:f.statusAt||f.createdAt,cadastrado:!!f.cadastradoVendas}));
      const cadastradas=fichasEsteMes.filter(f=>f.cadastrado).length;
      const pendentes=fichasEsteMes.filter(f=>!f.cadastrado).length;
      const backlog=fichasAnteriores.filter(f=>!f.cadastrado).length;
      // Aprovados (board.movesLog phaseId=aprovado_entrada)
      const movesLog=(boardDb?.movesLog||[]);
      function toBRT2(d){return new Date(new Date(d).toLocaleString('en-US',{timeZone:'America/Sao_Paulo'}));}
      function dayStart2(d){const b=toBRT2(d);b.setHours(0,0,0,0);return new Date(b.getTime()+3*60*60*1000);}
      function weekStart2(d){const b=toBRT2(d);const dy=b.getDay();b.setDate(b.getDate()+(dy===0?-6:1-dy));b.setHours(0,0,0,0);return new Date(b.getTime()+3*60*60*1000);}
      const aprvLog=movesLog.filter(m=>m.phaseId==='aprovado_entrada');
      const aprvTs=aprvLog.map(m=>m.timestamp||m.ts).filter(Boolean);
      const todayA=dayStart2(new Date()),weekA=weekStart2(new Date());
      const aprvHoje=aprvTs.filter(t=>new Date(t)>=todayA).length||null;
      const aprvSem=aprvTs.filter(t=>new Date(t)>=weekA).length||null;
      const aprvTotal=aprvTs.length||null;
      const m={
        fichas:{total:fichasTotal,hoje:ficHoje,semana:ficSem,mes:ficMes},
        comprados:{total:compTotal,hoje:compH,semana:compS,mes:compMCount},
        cadastrados:{total:cadTotal,hoje:cadH,semana:cadS,mes:cadM},
        vendidos:{total:vendTotal,hoje:vendH,semana:vendS},
        disponiveis:cadTotal-vendTotal,
        erp:{total:erpTotal,semValor:erpSV+erpMore,hoje:erpHoje,semana:erpSemana},
        orcamento:{hoje:orcHoje,semana:orcSem,timestamps:orcTs},
        pagamento:{hoje:pgHoje,semana:pgSem,timestamps:pgTs},
        aprovados:{hoje:aprvHoje,semana:aprvSem,total:aprvTotal,timestamps:aprvTs},
        monthly:{comprados:compMCount,cadastrados:cadM,falta:pendentes,backlog,compAnteriores:compAntArr.length,fichasEsteMes,fichasAnteriores,cadastradas,pendentes},
        fichasHojeList,
        updatedAt:new Date().toISOString(),
      };
      await dbS(CACHE,{...m,cachedAt:new Date().toISOString()});
      return res.status(200).json({ok:true,...m});
    }catch(e){const c=await dbG(CACHE);if(c)return res.status(200).json({ok:true,...c,fromCache:true});return res.status(500).json({ok:false,error:e.message});}
  }
  if(req.query.action==='pipes-info'){try{
    const q='query{adm:pipe(id:"305832912"){id name phases{id name}} me{pipes(first:20){edges{node{id name phases{id name}}}}}}';
    const d=await pf(q);
    const otherPipes=(d?.me?.pipes?.edges||[]).map(e=>e.node).filter(p=>p.id!=='305832912');
    return res.status(200).json({ok:true,adm:d?.adm,other:otherPipes});
  }catch(e){return res.status(500).json({ok:false,error:e.message});}}
  return res.status(404).json({ok:false,error:'Ação não encontrada'});
}
