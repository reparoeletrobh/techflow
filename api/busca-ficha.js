// api/busca-ficha.js — busca de ficha/cliente em todas as chaves Redis
const U=(process.env.UPSTASH_URL||'').replace(/['"]/g,'').trim();
const T=(process.env.UPSTASH_TOKEN||'').replace(/['"]/g,'').trim();

async function dbGet(k){
  try{
    const r=await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});
    const j=await r.json();const v=j[0]?.result;
    if(!v)return null;let x=JSON.parse(v);if(typeof x==='string')x=JSON.parse(x);return x;
  }catch(e){return null;}
}

function extrairCards(data) {
  if (!data) return [];
  if (Array.isArray(data)) return data;
  if (Array.isArray(data.cards)) return data.cards;
  if (Array.isArray(data.fichas)) return data.fichas;
  // board com fases
  const cards = [];
  Object.values(data).forEach(v => { if (Array.isArray(v)) v.forEach(c => { if(c&&(c.nome||c.cliente||c.nomeContato)) cards.push(c); }); });
  return cards;
}

function buscarEmCards(cards, q, fonte) {
  const results = [];
  cards.forEach(function(c) {
    if (!c) return;
    const nome  = String(c.nome||c.cliente||c.nomeContato||c.title||'').toLowerCase();
    const num   = String(c.numero||c.id||c.pipefyId||c.fichaId||'').toLowerCase();
    const tel   = String(c.telefone||c.tel||c.phone||c.fone||'');
    const osCode= String(c.osCode||c.os||'').toLowerCase();
    if (nome.includes(q) || num.includes(q) || osCode.includes(q)) {
      results.push({
        fonte, numero: c.numero||c.id||c.pipefyId||c.fichaId||'',
        nome: c.nome||c.cliente||c.nomeContato||c.title||'',
        telefone: tel, status: c.status||c.fase||c.phaseId||c.phase||'',
        osCode: c.osCode||c.os||''
      });
    }
  });
  return results;
}

module.exports = async function(req,res){
  res.setHeader('Access-Control-Allow-Origin','*');
  const q=(req.query.q||'').toLowerCase().trim();
  if(!q) return res.status(400).json({ok:false,error:'?q=nome_ou_numero'});

  const results = [];
  const CHAVES = [
    'reparoeletro_pipe','reparoeletro_frenteloja','reparoeletro_financeiro',
    'reparoeletro_board','reparoeletro_logistica','reparoeletro_balcao',
    'reparoeletro_arquivo','reparoeletro_pipe_archive','reparoeletro_orc',
    'reparoeletro_orcamentos','reparoeletro_compra_equip'
  ];

  for (const chave of CHAVES) {
    try {
      const data = await dbGet(chave);
      const cards = extrairCards(data);
      const found = buscarEmCards(cards, q, chave.replace('reparoeletro_',''));
      results.push(...found);
    } catch(e) {}
  }

  return res.status(200).json({ok:true, q, total:results.length, results});
};
