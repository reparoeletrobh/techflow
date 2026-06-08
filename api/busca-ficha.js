// api/busca-ficha.js — busca temporária de ficha por número/nome
const U=(process.env.UPSTASH_URL||'').replace(/['"]/g,'').trim();
const T=(process.env.UPSTASH_TOKEN||'').replace(/['"]/g,'').trim();

async function dbGet(k){
  try{
    const r=await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});
    const j=await r.json();
    const v=j[0]?.result;
    if(!v)return null;
    let x=JSON.parse(v);
    if(typeof x==='string')x=JSON.parse(x);
    return x;
  }catch(e){return null;}
}

module.exports = async function(req,res){
  res.setHeader('Access-Control-Allow-Origin','*');
  const q=(req.query.q||'').toLowerCase().trim();
  if(!q) return res.status(400).json({ok:false,error:'q obrigatorio'});

  const results=[];

  // Buscar em reparoeletro_board (fichas principais)
  const board = await dbGet('reparoeletro_board');
  if(board){
    const allCards=[];
    Object.values(board).forEach(function(fase){
      if(Array.isArray(fase)) fase.forEach(function(c){allCards.push(c);});
    });
    allCards.forEach(function(c){
      const num=String(c.numero||c.id||'').toLowerCase();
      const nome=String(c.nome||c.cliente||'').toLowerCase();
      const tel=String(c.telefone||c.tel||c.phone||'');
      if(num.includes(q)||nome.includes(q)){
        results.push({fonte:'pipe',numero:c.numero||c.id,nome:c.nome||c.cliente,telefone:tel,status:c.status||c.fase||''});
      }
    });
  }

  // Buscar em reparoeletro_financeiro (fichas no financeiro)
  const fin = await dbGet('reparoeletro_financeiro');
  if(fin){
    const cards=Array.isArray(fin)?fin:(fin.cards||fin.fichas||[]);
    cards.forEach(function(c){
      const num=String(c.numero||c.id||'').toLowerCase();
      const nome=String(c.nome||c.cliente||'').toLowerCase();
      const tel=String(c.telefone||c.tel||c.phone||'');
      if(num.includes(q)||nome.includes(q)){
        results.push({fonte:'financeiro',numero:c.numero||c.id,nome:c.nome||c.cliente,telefone:tel,status:c.status||''});
      }
    });
  }

  return res.status(200).json({ok:true,q,total:results.length,results});
};
