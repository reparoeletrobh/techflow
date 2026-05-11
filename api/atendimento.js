// api/atendimento.js v3.4 — fichasAnteriores com cadastrado + ERP phases_history + fichas allPhases
const U=process.env.UPSTASH_URL,T=process.env.UPSTASH_TOKEN,PT=process.env.PIPEFY_TOKEN;
const PA='https://api.pipefy.com/graphql',PID='305832912',ERP_ID='339008925';
const CK='reparoeletro_compra_equip',VK='reparoeletro_vendas',CACHE='reparoeletro_atendimento_cache';
async function dbG(k){const r=await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T},body:JSON.stringify([['GET',k]])});const j=await r.json();return j[0]?.result?JSON.parse(j[0].result):null;}
async function dbS(k,v){await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});}
async function pf(q){const r=await fetch(PA,{method:'POST',headers:{Authorization:'Bearer '+PT,'Content-Type':'application/json'},body:JSON.stringify({query:q})});const j=await r.json();if(j.errors?.length)throw new Error(j.errors[0].message);return j.data;}
// BRT = UTC-3 → meia-noite BRT = 03:00 UTC
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
      // Q1: pipe count + ERP phase com phases_history para data real de entrada na fase
      const Q1='query{pipe(id:"'+PID+'"){cards_count} phase(id:"'+ERP_ID+'"){cards_count cards(first:50){edges{node{id fields{name value}}}pageInfo{hasNextPage}}}}';
      // Q2: allCards para fichas criadas hoje (todos os cards do pipe, todas as fases)
      const Q2='query{allCards(pipeId:"'+PID+'",first:2000){edges{node{id title created_at}}}}';
      const[pd,cd,vd,ficQ]=await Promise.all([pf(Q1),dbG(CK),dbG(VK),pf(Q2).catch(()=>null)]);
      // ── FICHAS ──────────────────────────────────────────────────────────
      const fichasTotal=pd?.pipe?.cards_count||0;
      const allEdges=ficQ?.allCards?.edges||[];
      const ficDateOf=e=>new Date(e?.node?.created_at||0).getTime();
      const ficHojeEdges=allEdges.filter(e=>ficDateOf(e)>=todayUTC.getTime());
      const ficSemEdges=allEdges.filter(e=>ficDateOf(e)>=weekUTC.getTime());
      const ficMesEdges=allEdges.filter(e=>ficDateOf(e)>=monthUTC.getTime());
      const ficHoje=ficHojeEdges.length||null;
      const ficSem=ficSemEdges.length||null;
      const ficMes=ficMesEdges.length||null;
      const fichasHojeList=ficHojeEdges.map(e=>({id:e.node.id,title:e.node.title||'Sem título',createdAt:e.node.created_at}));
      // ── ERP — usar firstTimeIn da phases_history para data real de entrada ──
      const ep=pd?.phase;
      const erpTotal=ep?.cards_count||0;
      const erpEdges=ep?.cards?.edges?.map(e=>e.node)||[];
      // Data de entrada na fase ERP via phases_history
      const erpEntryOf=card=>{
        const ph=card.phases_history?.find(h=>h.phase?.id===ERP_ID);
        return ph?.firstTimeIn?new Date(ph.firstTimeIn).getTime():new Date(card.updated_at||0).getTime();
      };
      const erpSV=erpEdges.filter(card=>{const vf=card.fields?.find(f=>/(valor|preco|preço)/i.test(f.name||''));return !vf?.value||parseFloat(String(vf.value||'0').replace(/[^0-9.,]/g,'').replace(',','.'))||0===0;}).length;
      const erpMore=ep?.cards?.pageInfo?.hasNextPage?Math.max(0,erpTotal-50):0;
      // ERP hoje/semana removido: Pipefy não expõe phases_history de forma confiável
      // ── COMPRADOS ────────────────────────────────────────────────────────
      const fichas=cd?.fichas||[];
      const comprados=fichas.filter(f=>f.status==='comprado');
      const tsOf=f=>new Date(f.statusAt||f.createdAt||0).getTime();
      const compH=comprados.filter(f=>tsOf(f)>=todayUTC.getTime()).length;
      const compS=comprados.filter(f=>tsOf(f)>=weekUTC.getTime()).length;
      const compMArr=comprados.filter(f=>tsOf(f)>=monthUTC.getTime()).sort((a,b)=>tsOf(b)-tsOf(a));
      const compMCount=compMArr.length;
      const compTotal=comprados.length;
      const compAntArr=comprados.filter(f=>tsOf(f)<monthUTC.getTime()).sort((a,b)=>tsOf(b)-tsOf(a));
      // ── CADASTRADOS / VENDIDOS ───────────────────────────────────────────
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
      // ── FICHAS ESTE MÊS — !!f.cadastradoVendas ──────────────────────────
      const fichasEsteMes=compMArr.map(f=>({nome:fmtNome(f),statusAt:f.statusAt||f.createdAt,cadastrado:!!f.cadastradoVendas}));
      // ── FICHAS ANTERIORES — INCLUIR cadastrado para não mostrar errado ───
      const fichasAnteriores=compAntArr.map(f=>({nome:fmtNome(f),statusAt:f.statusAt||f.createdAt,cadastrado:!!f.cadastradoVendas}));
      const cadastradas=fichasEsteMes.filter(f=>f.cadastrado).length;
      const pendentes=fichasEsteMes.filter(f=>!f.cadastrado).length;
      // backlog = anteriores que ainda NÃO foram cadastradas
      const backlog=fichasAnteriores.filter(f=>!f.cadastrado).length;
      const m={
        fichas:{total:fichasTotal,hoje:ficHoje,semana:ficSem,mes:ficMes},
        comprados:{total:compTotal,hoje:compH,semana:compS,mes:compMCount},
        cadastrados:{total:cadTotal,hoje:cadH,semana:cadS,mes:cadM},
        vendidos:{total:vendTotal,hoje:vendH,semana:vendS},
        disponiveis:cadTotal-vendTotal,
        erp:{total:erpTotal,semValor:erpSV+erpMore},
        monthly:{comprados:compMCount,cadastrados:cadM,falta:pendentes,backlog,compAnteriores:compAntArr.length,fichasEsteMes,fichasAnteriores,cadastradas,pendentes},
        fichasHojeList,
        updatedAt:new Date().toISOString(),
      };
      await dbS(CACHE,{...m,cachedAt:new Date().toISOString()});
      return res.status(200).json({ok:true,...m});
    }catch(e){const c=await dbG(CACHE);if(c)return res.status(200).json({ok:true,...c,fromCache:true});return res.status(500).json({ok:false,error:e.message});}
  }
  return res.status(404).json({ok:false,error:'Ação não encontrada'});
}
