// api/atendimento.js v3.1 — fichas via allCards + matching verde/vermelho
const U=process.env.UPSTASH_URL,T=process.env.UPSTASH_TOKEN,PT=process.env.PIPEFY_TOKEN;
const PA='https://api.pipefy.com/graphql',PID='305832912',ERP_ID='339008925';
const CK='reparoeletro_compra_equip',VK='reparoeletro_vendas',CACHE='reparoeletro_atendimento_cache';
async function dbG(k){const r=await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T},body:JSON.stringify([['GET',k]])});const j=await r.json();return j[0]?.result?JSON.parse(j[0].result):null;}
async function dbS(k,v){await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});}
async function pf(q){const r=await fetch(PA,{method:'POST',headers:{Authorization:'Bearer '+PT,'Content-Type':'application/json'},body:JSON.stringify({query:q})});const j=await r.json();if(j.errors?.length)throw new Error(j.errors[0].message);return j.data;}
function toBRT(d){try{return new Date(new Date(d).toLocaleString('en-US',{timeZone:'America/Sao_Paulo'}));}catch{return new Date(0);}}
function startOf(unit){const d=toBRT(new Date());if(unit==='day'){d.setHours(0,0,0,0);}else if(unit==='week'){const wd=d.getDay();d.setDate(d.getDate()-(wd===0?6:wd-1));d.setHours(0,0,0,0);}else if(unit==='month'){return new Date(d.getFullYear(),d.getMonth(),1);}return d;}
function fmtNome(f){return (f.nomeContato||f.title||'').split('—')[0].split('|')[0].trim().slice(0,60)||'Sem nome';}
// Matching por janela de data: comprado no dia X → procura cadastrado entre X e X+4 dias
function matchFichas(fichas,produtos){
  let avail=[...produtos].sort((a,b)=>new Date(a.createdAt||0)-new Date(b.createdAt||0));
  return fichas.map(f=>{
    const sd=new Date(f.statusAt||f.createdAt||0);
    const maxD=new Date(sd.getTime()+4*24*60*60*1000);
    const idx=avail.findIndex(p=>{const cd=new Date(p.createdAt||0);return cd>=sd&&cd<=maxD;});
    let matched=false;
    if(idx>=0){matched=true;avail.splice(idx,1);}
    return{nome:fmtNome(f),statusAt:f.statusAt||f.createdAt,cadastrado:matched};
  });
}
export default async function handler(req,res){
  res.setHeader('Access-Control-Allow-Origin','*');
  res.setHeader('Access-Control-Allow-Methods','GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers','Content-Type');
  if(req.method==='OPTIONS')return res.status(200).end();
  if(req.query.action==='metrics'){
    try{
      const todayBRT=startOf('day'),weekBRT=startOf('week'),monthBRT=startOf('month');
      // Q1: pipe total + ERP (com campos created_at nos cards)
      const Q1='query{pipe(id:"'+PID+'"){cards_count} phase(id:"'+ERP_ID+'"){cards_count cards(first:50){edges{node{id created_at fields{name value}}}pageInfo{hasNextPage}}}}';
      // Q2: allCards com created_at para contar fichas por periodo (pega os 500 mais recentes)
      const Q2='query{allCards(pipeId:"'+PID+'",first:500){edges{node{created_at}}}}';
      const[pd,cd,vd,ficQ]=await Promise.all([pf(Q1),dbG(CK),dbG(VK),pf(Q2).catch(()=>null)]);
      // ── FICHAS CONTAGEM por periodo ──────────────────────────────────────
      const fichasTotal=pd?.pipe?.cards_count||0;
      const allEdges=ficQ?.allCards?.edges||[];
      const ficDateOf=e=>new Date(e?.node?.created_at||0);
      const fichasHoje=allEdges.filter(e=>ficDateOf(e)>=todayBRT).length||null;
      const fichasSem=allEdges.filter(e=>ficDateOf(e)>=weekBRT).length||null;
      const fichasMes=allEdges.filter(e=>ficDateOf(e)>=monthBRT).length||null;
      // null se nenhum resultado (provavelmente allCards retornou cards antigos)
      const ficHoje=fichasHoje>0?fichasHoje:null;
      const ficSem=fichasSem>0?fichasSem:null;
      const ficMes=fichasMes>0?fichasMes:null;
      // ── ERP ──────────────────────────────────────────────────────────────
      const ep=pd?.phase;
      const erpTotal=ep?.cards_count||0;
      const erpEdges=ep?.cards?.edges?.map(e=>e.node)||[];
      const erpSV=erpEdges.filter(card=>{const vf=card.fields?.find(f=>/(valor|preco|preço)/i.test(f.name||''));return !vf?.value||parseFloat(String(vf.value||'0').replace(/[^0-9.,]/g,'').replace(',','.'))||0===0;}).length;
      const erpMore=ep?.cards?.pageInfo?.hasNextPage?Math.max(0,erpTotal-50):0;
      // ERP hoje/semana via created_at dos cards ERP
      const erpDateOf=c=>new Date(c.created_at||0);
      const erpHoje=erpEdges.filter(c=>erpDateOf(c)>=todayBRT).length||null;
      const erpSemana=erpEdges.filter(c=>erpDateOf(c)>=weekBRT).length||null;
      // ── COMPRADOS ────────────────────────────────────────────────────────
      const fichas=cd?.fichas||[];
      const comprados=fichas.filter(f=>f.status==='comprado');
      const tsOf=f=>toBRT(f.statusAt||f.createdAt||0).getTime();
      const compH=comprados.filter(f=>tsOf(f)>=todayBRT.getTime()).length;
      const compS=comprados.filter(f=>tsOf(f)>=weekBRT.getTime()).length;
      const compMArr=comprados.filter(f=>tsOf(f)>=monthBRT.getTime()).sort((a,b)=>tsOf(b)-tsOf(a));
      const compMCount=compMArr.length;
      const compTotal=comprados.length;
      const compAntArr=comprados.filter(f=>tsOf(f)<monthBRT.getTime()).sort((a,b)=>tsOf(b)-tsOf(a));
      // ── CADASTRADOS / VENDIDOS ───────────────────────────────────────────
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
      // ── MATCHING verde/vermelho para fichas deste mes ────────────────────
      const produtosMes=produtos.filter(p=>cadOf(p)>=monthBRT.getTime());
      const fichasEsteMes=matchFichas(compMArr,produtosMes);
      const fichasAnteriores=compAntArr.map(f=>({nome:fmtNome(f),statusAt:f.statusAt||f.createdAt}));
      const m={
        fichas:{total:fichasTotal,hoje:ficHoje,semana:ficSem,mes:ficMes},
        comprados:{total:compTotal,hoje:compH,semana:compS,mes:compMCount},
        cadastrados:{total:cadTotal,hoje:cadH,semana:cadS,mes:cadM},
        vendidos:{total:vendTotal,hoje:vendH,semana:vendS},
        disponiveis:cadTotal-vendTotal,
        erp:{total:erpTotal,semValor:erpSV+erpMore,hoje:erpHoje,semana:erpSemana},
        monthly:{comprados:compMCount,cadastrados:cadM,falta:compMCount,backlog:compAntArr.length,compAnteriores:compAntArr.length,fichasEsteMes,fichasAnteriores},
        updatedAt:new Date().toISOString(),
      };
      await dbS(CACHE,{...m,cachedAt:new Date().toISOString()});
      return res.status(200).json({ok:true,...m});
    }catch(e){const c=await dbG(CACHE);if(c)return res.status(200).json({ok:true,...c,fromCache:true});return res.status(500).json({ok:false,error:e.message});}
  }
  return res.status(404).json({ok:false,error:'Ação não encontrada'});
}
