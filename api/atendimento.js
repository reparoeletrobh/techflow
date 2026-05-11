// api/atendimento.js v2.3
const U=process.env.UPSTASH_URL,T=process.env.UPSTASH_TOKEN,PT=process.env.PIPEFY_TOKEN;
const PA='https://api.pipefy.com/graphql',PID='305832912',ERP_ID='339008925';
const CK='reparoeletro_compra_equip',VK='reparoeletro_vendas';
const HIST='reparoeletro_atendimento_hist',CACHE='reparoeletro_atendimento_cache';
async function dbG(k){const r=await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T},body:JSON.stringify([['GET',k]])});const j=await r.json();return j[0]?.result?JSON.parse(j[0].result):null;}
async function dbS(k,v){await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});}
async function pf(q){const r=await fetch(PA,{method:'POST',headers:{Authorization:'Bearer '+PT,'Content-Type':'application/json'},body:JSON.stringify({query:q})});const j=await r.json();if(j.errors?.length)throw new Error(j.errors[0].message);return j.data;}
function toBRT(d){try{return new Date(new Date(d).toLocaleString('en-US',{timeZone:'America/Sao_Paulo'}));}catch{return new Date(0);}}
function startOf(unit){const d=toBRT(new Date());if(unit==='day'){d.setHours(0,0,0,0);}else if(unit==='week'){const wd=d.getDay();d.setDate(d.getDate()-(wd===0?6:wd-1));d.setHours(0,0,0,0);}else if(unit==='month'){return new Date(d.getFullYear(),d.getMonth(),1);}return d;}
function dStr(d){return toBRT(d||new Date()).toISOString().slice(0,10);}
function fmtTitle(t){return (t||'').split('—')[0].split('|')[0].trim().slice(0,50)||'Sem título';}
export default async function handler(req,res){
  res.setHeader('Access-Control-Allow-Origin','*');
  res.setHeader('Access-Control-Allow-Methods','GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers','Content-Type');
  if(req.method==='OPTIONS')return res.status(200).end();
  if(req.query.action==='metrics'){
    try{
      const todayBRT=startOf('day'),weekBRT=startOf('week'),monthBRT=startOf('month');
      const monthISO=monthBRT.toISOString();
      const todayStr=dStr();
      // Pipefy: total + ERP em Q1, fichas do mes via Q2 (try allCards totalCount with search)
      const Q1='query{pipe(id:"'+PID+'"){cards_count} phase(id:"'+ERP_ID+'"){cards_count cards(first:50){edges{node{id fields{name value}}}pageInfo{hasNextPage}}}}';
      // Try multiple Pipefy filter formats for month count
      const Q2a='query{allCards(pipeId:"'+PID+'",search:{title:"",created_at:{gt:"'+monthISO+'"}}){totalCount}}';
      const Q2b='query{allCards(pipeId:"'+PID+'",filter:{created_at_gt:"'+monthISO+'"}){totalCount}}';
      const Q2c='query{allCards(pipeId:"'+PID+'"){totalCount}}'; // just test if totalCount works
      const[pd,cd,vd,hr,ficQ]=await Promise.all([
        pf(Q1),dbG(CK),dbG(VK),dbG(HIST),
        // Try queries in sequence until one works
        pf(Q2a).catch(()=>pf(Q2b).catch(()=>pf(Q2c).catch(()=>null)))
      ]);
      const fichasTotal=pd?.pipe?.cards_count||0;
      // fichas mes: from query if available, else from snapshot delta
      const ficMesQuery=ficQ?.allCards?.totalCount;
      // Snapshots para hoje/semana/mes
      const hist=hr||{snapshots:[]};
      let ts=hist.snapshots.find(s=>s.date===todayStr);
      if(!ts){ts={date:todayStr,fichasTotal};hist.snapshots.push(ts);}else{ts.fichasTotal=fichasTotal;}
      hist.snapshots=hist.snapshots.sort((a,b)=>a.date.localeCompare(b.date)).slice(-35);
      dbS(HIST,hist);
      function prevSnap(beforeDate){return hist.snapshots.filter(s=>s.date<beforeDate).sort((a,b)=>b.date.localeCompare(a.date))[0];}
      const dp=prevSnap(todayStr),wp=prevSnap(dStr(weekBRT)),mp=prevSnap(dStr(monthBRT));
      const fichasHoje=fichasTotal-(dp?.fichasTotal??fichasTotal);
      const fichasSem=fichasTotal-(wp?.fichasTotal??fichasTotal);
      const fichasMes=ficMesQuery!=null?ficMesQuery:(fichasTotal-(mp?.fichasTotal??fichasTotal));
      // Comprados
      const fichas=cd?.fichas||[];
      const comprados=fichas.filter(f=>f.status==='comprado');
      const tsOf=f=>toBRT(f.statusAt||f.createdAt||0).getTime();
      const compH=comprados.filter(f=>tsOf(f)>=todayBRT.getTime()).length;
      const compS=comprados.filter(f=>tsOf(f)>=weekBRT.getTime()).length;
      const compMArr=comprados.filter(f=>tsOf(f)>=monthBRT.getTime());
      const compMCount=compMArr.length;
      const compTotal=comprados.length;
      const compAntArr=comprados.filter(f=>tsOf(f)<monthBRT.getTime());
      // Fichas list for UI
      const fichasEsteMes=compMArr.map(f=>({title:fmtTitle(f.title),statusAt:f.statusAt||f.createdAt})).sort((a,b)=>new Date(b.statusAt)-new Date(a.statusAt));
      const fichasAnteriores=compAntArr.map(f=>({title:fmtTitle(f.title),statusAt:f.statusAt||f.createdAt})).sort((a,b)=>new Date(b.statusAt)-new Date(a.statusAt));
      // Cadastrados / Vendidos
      const produtos=vd?.produtos||[];
      const cadOf=p=>toBRT(p.createdAt||0).getTime();
      const cadH=produtos.filter(p=>cadOf(p)>=todayBRT.getTime()).length;
      const cadS=produtos.filter(p=>cadOf(p)>=weekBRT.getTime()).length;
      const cadM=produtos.filter(p=>cadOf(p)>=monthBRT.getTime()).length;
      const cadTotal=produtos.length;
      const soldOf=p=>toBRT(p.soldAt||0).getTime();
      const vendidos=produtos.filter(p=>p.vendido||p.soldAt);
      const vendH=vendidos.filter(p=>soldOf(p)>=todayBRT.getTime()).length;
      const vendS=vendidos.filter(p=>soldOf(p)>=weekBRT.getTime()).length;
      const vendTotal=vendidos.length;
      // ERP
      const ep=pd?.phase;
      const erpTotal=ep?.cards_count||0;
      const erpCards=ep?.cards?.edges?.map(e=>e.node)||[];
      const erpSV=erpCards.filter(card=>{const vf=card.fields?.find(f=>/(valor|preco|preço)/i.test(f.name||''));return !vf?.value||parseFloat(String(vf.value||'0').replace(/[^0-9.,]/g,'').replace(',','.'))||0===0;}).length;
      const erpMore=ep?.cards?.pageInfo?.hasNextPage?Math.max(0,erpTotal-50):0;
      const m={
        fichas:{total:fichasTotal,hoje:fichasHoje,semana:fichasSem,mes:fichasMes},
        comprados:{total:compTotal,hoje:compH,semana:compS,mes:compMCount},
        cadastrados:{total:cadTotal,hoje:cadH,semana:cadS,mes:cadM},
        vendidos:{total:vendTotal,hoje:vendH,semana:vendS},
        disponiveis:cadTotal-vendTotal,
        erp:{total:erpTotal,semValor:erpSV+erpMore},
        monthly:{comprados:compMCount,cadastrados:cadM,falta:compMCount,backlog:compAntArr.length,compAnteriores:compAntArr.length,fichasEsteMes,fichasAnteriores},
        updatedAt:new Date().toISOString(),
      };
      await dbS(CACHE,{...m,cachedAt:new Date().toISOString()});
      return res.status(200).json({ok:true,...m});
    }catch(e){const c=await dbG(CACHE);if(c)return res.status(200).json({ok:true,...c,fromCache:true});return res.status(500).json({ok:false,error:e.message});}
  }
  return res.status(404).json({ok:false,error:'Ação não encontrada'});
}
