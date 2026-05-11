// api/atendimento.js
const U=process.env.UPSTASH_URL,T=process.env.UPSTASH_TOKEN,PT=process.env.PIPEFY_TOKEN;
const PA='https://api.pipefy.com/graphql',PID='305832912',ERP_ID='339008925';
const CK='reparoeletro_compra_equip',VK='reparoeletro_vendas',CACHE='reparoeletro_atendimento_cache';
async function dbG(k){const r=await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T},body:JSON.stringify([['GET',k]])});const j=await r.json();return j[0]?.result?JSON.parse(j[0].result):null;}
async function dbS(k,v){await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});}
async function pf(q){const r=await fetch(PA,{method:'POST',headers:{Authorization:'Bearer '+PT,'Content-Type':'application/json'},body:JSON.stringify({query:q})});const j=await r.json();if(j.errors?.length)throw new Error(j.errors[0].message);return j.data;}
export default async function handler(req,res){
  res.setHeader('Access-Control-Allow-Origin','*');
  res.setHeader('Access-Control-Allow-Methods','GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers','Content-Type');
  if(req.method==='OPTIONS')return res.status(200).end();
  if(req.query.action==='metrics'){
    try{
      const Q='query{pipe(id:"'+PID+'"){cards_count} phase(id:"'+ERP_ID+'"){cards_count cards(first:50){edges{node{id fields{name value}}}pageInfo{hasNextPage}}}}';
      const[pd,cd,vd]=await Promise.all([pf(Q),dbG(CK),dbG(VK)]);
      const fichasCriadas=pd?.pipe?.cards_count||0;
      const equipComprados=(cd?.fichas||[]).filter(f=>f.status==='comprado').length;
      const produtos=vd?.produtos||[];
      const equipCadastrados=produtos.length;
      const equipVendidos=produtos.filter(p=>p.vendido).length;
      const equipDisponiveis=equipCadastrados-equipVendidos;
      const ep=pd?.phase;
      const erpTotal=ep?.cards_count||0;
      const sv=(ep?.cards?.edges||[]).filter(e=>{const vf=e.node.fields?.find(f=>/(valor|preco|preço)/i.test(f.name||''));return !vf?.value||parseFloat(String(vf.value||'0').replace(/[^0-9.,]/g,'').replace(',','.'))||0===0;}).length;
      const more=ep?.cards?.pageInfo?.hasNextPage?Math.max(0,erpTotal-50):0;
      const m={fichasCriadas,equipComprados,equipCadastrados,equipDisponiveis,equipVendidos,erpTotal,erpSemValor:sv+more,updatedAt:new Date().toISOString()};
      await dbS(CACHE,{...m,cachedAt:new Date().toISOString()});
      return res.status(200).json({ok:true,...m});
    }catch(e){const cached=await dbG(CACHE);if(cached)return res.status(200).json({ok:true,...cached,fromCache:true});return res.status(500).json({ok:false,error:e.message});}
  }
  return res.status(404).json({ok:false,error:'Ação não encontrada'});
}
