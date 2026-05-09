// api/tv-checkout.js — Checkout VSL
const U=process.env.UPSTASH_URL,T=process.env.UPSTASH_TOKEN,CK='tv_checkout_config',VK='tv_checkout_vendas';
async function dbG(k){const r=await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T},body:JSON.stringify([['GET',k]])});const j=await r.json();return j[0]?.result?JSON.parse(j[0].result):null;}
async function dbS(k,v){await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});}
export default async function handler(req,res){
  res.setHeader('Access-Control-Allow-Origin','*');
  res.setHeader('Access-Control-Allow-Methods','GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers','Content-Type');
  if(req.method==='OPTIONS')return res.status(200).end();
  const a=req.query.action||'';
  if(a==='load-config'){const c=(await dbG(CK))||{};return res.status(200).json({ok:true,config:c});}
  if(req.method==='POST'&&a==='save-config'){const b=req.body||{},c=(await dbG(CK))||{};if(b.videoHtml!==undefined)c.videoHtml=b.videoHtml;if(b.pagamento!==undefined)c.pagamento=b.pagamento;c.updatedAt=new Date().toISOString();await dbS(CK,c);return res.status(200).json({ok:true});}
  if(req.method==='POST'&&a==='set-destaque'){const{id,desconto,badge,prioridade,ativo}=req.body||{};if(!id)return res.status(400).json({ok:false,error:'id obrigatorio'});const c=(await dbG(CK))||{};if(!c.destaques)c.destaques={};if(ativo===false){delete c.destaques[id];}else{c.destaques[id]={desconto:parseFloat(desconto)||0,badge:badge||'',prioridade:parseInt(prioridade)||0,ativo:true};}c.updatedAt=new Date().toISOString();await dbS(CK,c);return res.status(200).json({ok:true});}
  if(a==='load-equipamentos'){const p=req.headers['x-forwarded-proto']||'https',h=req.headers.host;const d=await fetch(p+'://'+h+'/api/vendas?action=load').then(r=>r.json());const pr=(d.produtos||[]).filter(x=>!x.vendido);const c=(await dbG(CK))||{};return res.status(200).json({ok:true,produtos:pr.map(x=>({...x,_destaque:c.destaques?.[x.id]||null}))});}
  if(req.method==='POST'&&a==='registrar-venda'){const{produto,comprador,valor,provedor}=req.body||{};if(!produto?.id)return res.status(400).json({ok:false,error:'produto obrigatorio'});const db=(await dbG(VK))||{vendas:[]};db.vendas.unshift({id:Date.now().toString(36),produto,comprador,valor,provedor:provedor||'whatsapp',criadoEm:new Date().toISOString()});await dbS(VK,db);return res.status(200).json({ok:true});}
  if(a==='load-vendas'){const db=(await dbG(VK))||{vendas:[]};const v=db.vendas||[];const t=v.reduce((s,x)=>s+(parseFloat(x.valor)||0),0);return res.status(200).json({ok:true,vendas:v,total:t,count:v.length});}
  return res.status(404).json({ok:false,error:'Ação não encontrada'});
    }
