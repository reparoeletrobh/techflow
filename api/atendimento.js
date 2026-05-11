// api/atendimento.js v3.2 — cadastrado via !!f.cadastradoVendas + lista fichas hoje
const U=process.env.UPSTASH_URL,T=process.env.UPSTASH_TOKEN,PT=process.env.PIPEFY_TOKEN;
const PA='https://api.pipefy.com/graphql',PID='305832912',ERP_ID='339008925';
const CK='reparoeletro_compra_equip',VK='reparoeletro_vendas',CACHE='reparoeletro_atendimento_cache';
async function dbG(k){const r=await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T},body:JSON.stringify([['GET',k]])});const j=await r.json();return j[0]?.result?JSON.parse(j[0].result):null;}
async function dbS(k,v){await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});}
async function pf(q){const r=await fetch(PA,{method:'POST',headers:{Authorization:'Bearer '+PT,'Content-Type':'application/json'},body:JSON.stringify({query:q})});const j=await r.json();if(j.errors?.length)throw new Error(j.errors[0].message);return j.data;}
function toBRT(d){try{return new Date(new Date(d).toLocaleString('en-US',{timeZone:'America/Sao_Paulo'}));}catch{return new Date(0);}}
function startOf(unit){const d=toBRT(new Date());if(unit==='day'){d.setHours(0,0,0,0);}else if(unit==='week'){const wd=d.getDay();d.setDate(d.getDate()-(wd===0?6:wd-1));d.setHours(0,0,0,0);}else if(unit==='month'){return new Date(d.getFullYear(),d.getMonth(),1);}return d;}
function fmtNome(f){return (f.nomeContato||f.title||'').split('—')[0].split('|')[0].trim().slice(0,60)||'Sem nome';}
export default async function handler(req,res){
  res.setHeader('Access-Control-Allow-Origin','*');
  res.setHeader('Access-Control-Allow-Methods','GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers','Content-Type');
  if(req.method==='OPTIONS')return res.status(200).end();
  if(req.query.action==='metrics'){
    try{
      const todayBRT=startOf('day'),weekBRT=startOf('week'),monthBRT=startOf('month');
      const Q1='query{pipe(id:"'+PID+'"){cards_count} phase(id:"'+ERP_ID+'"){cards_count cards(first:50){edges{node{id created_at fields{name value}}}pageInfo{hasNextPage}}}}';
      // Incluindo title para lista de fichas criadas hoje
      const Q2='query{allCards(pipeId:"'+PID+'",first:500){edges{node{id title created_at}}}}';
      const[pd,cd,vd,ficQ]=await Promise.all([pf(Q1),dbG(CK),dbG(VK),pf(Q2).catch(()=>null)]);
      // ── FICHAS CONTAGEM e LISTA ──────────────────────────────────────────
      const fichasTotal=pd?.pipe?.cards_count||0;
      const allEdges=ficQ?.allCards?.edges||[];
      const ficDateOf=e=>new Date(e?.node?.created_at||0);
      const ficHojeEdges=allEdges.filter(e=>ficDateOf(e)>=todayBRT);
      const ficSemEdges=allEdges.filter(e=>ficDateOf(e)>=weekBRT);
      const ficMesEdges=allEdges.filter(e=>ficDateOf(e)>=monthBRT);
      const ficHoje=ficHojeEdges.length||null;
      const ficSem=ficSemEdges.length||null;
      const ficMes=ficMesEdges.length||null;
      // Lista fichas criadas hoje (com título)
      const fichasHojeList=ficHojeEdges.map(e=>({id:e.node.id,title:e.node.title||'Sem título',createdAt:e.node.created_at}));
      // ── ERP ──────────────────────────────────────────────────────────────
      const ep=pd?.phase;
      const erpTotal=ep?.cards_count||0;
      const erpEdges=ep?.cards?.edges?.map(e=>e.node)||[];
      const erpSV=erpEdges.filter(card=>{const vf=card.fields?.find(f=>/(valor|preco|preço)/i.test(f.name||''));return !vf?.value||parseFloat(String(vf.value||'0').replace(/[^0-9.,]/g,'').replace(',','.'))||0===0;}).length;
      const erpMore=ep?.cards?.pageInfo?.hasNextPage?Math.max(0,erpTotal-50):0;
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
      // ── FICHAS ESTE MÊS — lógica CORRETA: !!f.cadastradoVendas ─────────
      const fichasEsteMes=compMArr.map(f=>({
        nome:fmtNome(f),
        statusAt:f.statusAt||f.createdAt,
        cadastrado:!!f.cadastradoVendas
      }));
      const fichasAnteriores=compAntArr.map(f=>({nome:fmtNome(f),statusAt:f.statusAt||f.createdAt}));
      const cadastradas=fichasEsteMes.filter(f=>f.cadastrado).length;
      const pendentes=fichasEsteMes.filter(f=>!f.cadastrado).length;
      const m={
        fichas:{total:fichasTotal,hoje:ficHoje,semana:ficSem,mes:ficMes},
        comprados:{total:compTotal,hoje:compH,semana:compS,mes:compMCount},
        cadastrados:{total:cadTotal,hoje:cadH,semana:cadS,mes:cadM},
        vendidos:{total:vendTotal,hoje:vendH,semana:vendS},
        disponiveis:cadTotal-vendTotal,
        erp:{total:erpTotal,semValor:erpSV+erpMore,hoje:erpHoje,semana:erpSemana},
        monthly:{comprados:compMCount,cadastrados:cadM,falta:pendentes,backlog:compAntArr.length,compAnteriores:compAntArr.length,fichasEsteMes,fichasAnteriores,cadastradas,pendentes},
        fichasHojeList,
        updatedAt:new Date().toISOString(),
      };
      await dbS(CACHE,{...m,cachedAt:new Date().toISOString()});
      return res.status(200).json({ok:true,...m});
    }catch(e){const c=await dbG(CACHE);if(c)return res.status(200).json({ok:true,...c,fromCache:true});return res.status(500).json({ok:false,error:e.message});}
  }
  return res.status(404).json({ok:false,error:'Ação não encontrada'});
}
