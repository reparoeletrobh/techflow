// api/frenteloja.js — Sistema Frente de Loja
const PIPEFY_API = 'https://api.pipefy.com/graphql';
const PIPE_ID    = '305832912';
const FL_KEY     = 'reparoeletro_frenteloja';
const BALCAO_KEY = 'reparoeletro_balcao';

const U = process.env.UPSTASH_URL;
const T = process.env.UPSTASH_TOKEN;
const PT = (process.env.PIPEFY_TOKEN||'').trim();

async function dbGet(k){
  try{const r=await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});const j=await r.json();return j[0]?.result?JSON.parse(j[0].result):null;}catch(e){return null;}
}
async function dbSet(k,v){
  try{await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});}catch(e){}
}
async function pipefyQ(q){
  const r=await fetch(PIPEFY_API,{method:'POST',headers:{Authorization:'Bearer '+PT,'Content-Type':'application/json'},body:JSON.stringify({query:q})});
  const j=await r.json();if(j.errors?.length)throw new Error(j.errors[0].message);return j.data;
}

function defaultDB(){return {fichas:[],seq:0};}
function nextId(db){db.seq=(db.seq||0)+1;return 'FL-'+String(db.seq).padStart(4,'0');}
function brtNow(){return new Date(new Date().toLocaleString('en-US',{timeZone:'America/Sao_Paulo'}));}
function brtStartOfDay(){
  const d=brtNow();d.setHours(0,0,0,0);
  return new Date(Date.UTC(d.getFullYear(),d.getMonth(),d.getDate(),3,0,0,0));
}

async function getPipefyPhaseId(keyword){
  const d=await pipefyQ('query{pipe(id:"'+PIPE_ID+'"){phases{id name}}}');
  const phases=d?.pipe?.phases||[];
  const ph=phases.find(p=>p.name.toLowerCase().includes(keyword.toLowerCase()));
  return ph?.id||null;
}

async function createPipefyCard(fields, phaseId){
  const fieldsArg=fields.map(f=>'{field_id:"'+f.id+'",field_value:"'+String(f.value).replace(/"/g,'\\"')+'"}').join(',');
  const phaseArg=phaseId?'phase_id:"'+phaseId+'",' :'';
  const q='mutation{createCard(input:{pipe_id:"'+PIPE_ID+'",'+phaseArg+'fields_attributes:['+fieldsArg+']}){card{id}}}';
  const d=await pipefyQ(q);
  return d?.createCard?.card?.id||null;
}

async function movePipefyCard(cardId, phaseId){
  const q='mutation{moveCardToPhase(input:{card_id:"'+cardId+'",destination_phase_id:"'+phaseId+'"}){card{id}}}';
  await pipefyQ(q);
}

export default async function handler(req,res){
  res.setHeader('Access-Control-Allow-Origin','*');
  res.setHeader('Access-Control-Allow-Methods','GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers','Content-Type');
  if(req.method==='OPTIONS')return res.status(200).end();
  const action=req.query.action;

  if(req.method==='GET'&&action==='load'){
    const db=await dbGet(FL_KEY)||defaultDB();
    const todayStart=brtStartOfDay();
    db.fichas.forEach(f=>{
      if(f.phase==='liberado_hoje'&&new Date(f.movedAt)<todayStart){
        f.phase='conserto_realizado';f.liberadoHoje=false;
      }
    });
    await dbSet(FL_KEY,db);
    return res.status(200).json({ok:true,fichas:db.fichas});
  }

  if(req.method==='POST'&&action==='criar'){
    const {nomeContato,equipamento,telefone,descricao}=req.body||{};
    if(!nomeContato||!equipamento)return res.status(400).json({ok:false,error:'Nome e equipamento obrigatórios'});
    const db=await dbGet(FL_KEY)||defaultDB();
    const id=nextId(db);const now=new Date().toISOString();
    const ficha={id,nomeContato,equipamento,telefone:(telefone||'').replace(/[^0-9]/g,''),descricao:descricao||'',phase:'analise',createdAt:now,movedAt:now,history:[{phase:'analise',ts:now}]};
    db.fichas.unshift(ficha);await dbSet(FL_KEY,db);
    return res.status(200).json({ok:true,ficha});
  }

  if(req.method==='POST'&&action==='analise'){
    const {id,descricaoTecnica}=req.body||{};
    if(!id||!descricaoTecnica)return res.status(400).json({ok:false,error:'id e descrição obrigatórios'});
    const db=await dbGet(FL_KEY)||defaultDB();
    const ficha=db.fichas.find(f=>f.id===id);
    if(!ficha)return res.status(404).json({ok:false,error:'Não encontrada'});
    const now=new Date().toISOString();
    ficha.descricaoTecnica=descricaoTecnica;ficha.phase='orcamento_cadastrado';ficha.movedAt=now;
    ficha.history=(ficha.history||[]).concat([{phase:'orcamento_cadastrado',ts:now}]);
    await dbSet(FL_KEY,db);return res.status(200).json({ok:true,ficha});
  }

  if(req.method==='POST'&&action==='passar-orcamento'){
    const {id,valor,formaPagamento,decisao}=req.body||{};
    if(!id)return res.status(400).json({ok:false,error:'id obrigatório'});
    const db=await dbGet(FL_KEY)||defaultDB();
    const ficha=db.fichas.find(f=>f.id===id);
    if(!ficha)return res.status(404).json({ok:false,error:'Não encontrada'});
    const now=new Date().toISOString();
    ficha.orcamento={valor:parseFloat(valor)||0,formaPagamento:formaPagamento||'pix',status:decisao};

    // Bug 1 fix: reprovado remove a ficha do banco definitivamente
    if(decisao==='reprovado'){
      db.fichas=db.fichas.filter(f=>f.id!==id);
      await dbSet(FL_KEY,db);
      return res.status(200).json({ok:true,ficha:{...ficha,phase:'encerrado'}});
    }

    if(decisao==='aprovado'){
      ficha.phase='producao';ficha.movedAt=now;
      ficha.history=(ficha.history||[]).concat([{phase:'producao',ts:now}]);

      // Salvar no Redis IMEDIATAMENTE e responder — Pipefy roda em background
      await dbSet(FL_KEY,db);

      // Pipefy + sync em background (não bloqueia a resposta)
      (async()=>{
        try{
          // Evitar duplicidade: checar se já tem pipefyCardId antes de criar
          const dbCheck=await dbGet(FL_KEY)||defaultDB();
          const fichaCheck=dbCheck.fichas.find(f=>f.id===ficha.id);
          if(fichaCheck?.pipefyCardId){
            console.log('[FrenteLoja] Card já existe:',fichaCheck.pipefyCardId,'— pulando criação');
            return;
          }
          const aprovadoPhaseId=await getPipefyPhaseId('aprovad');
          if(!aprovadoPhaseId) throw new Error('Fase Aprovado nao encontrada');
          const titleCompleto=(ficha.nomeContato+' (Loja) - '+ficha.equipamento+
            ' | '+(ficha.descricao||'')+' | Diag: '+(ficha.descricaoTecnica||'')+
            ' | R$'+String(parseFloat(ficha.orcamento?.valor||0).toFixed(2))+
            ' '+(ficha.orcamento?.formaPagamento||'pix')+' OS:'+ficha.id
          ).replace(/"/g,"'").slice(0,255);
          const nomeCard=(ficha.nomeContato+' (Loja)').replace(/"/g,"'").slice(0,255);
          const telCard=(ficha.telefone||'').replace(/"/g,"'").slice(0,100);
          const data=await pipefyQ(
            'mutation { createCard(input: { pipe_id: "'+PIPE_ID+'" phase_id: "'+aprovadoPhaseId+'" title: "'+titleCompleto+'" fields_attributes: [ { field_id: "nome_do_contato" field_value: "'+nomeCard+'" }, { field_id: "telefone" field_value: "'+telCard+'" } ] }) { card { id } } }'
          );
          const pipefyId=data?.createCard?.card?.id||null;
          console.log('[FrenteLoja] Card criado:',pipefyId);
          if(pipefyId){
            // Atualizar pipefyCardId no Redis
            const db2=await dbGet(FL_KEY)||defaultDB();
            const f2=db2.fichas.find(f=>f.id===ficha.id);
            if(f2){
              f2.pipefyCardId=pipefyId;
              const vn=String(parseFloat(ficha.orcamento?.valor||0).toFixed(2));
              await pipefyQ('mutation{updateCardField(input:{card_id:"'+pipefyId+'" field_id:"valor_de_contrato" new_value:"'+vn+'"}){success}}').catch(e=>console.error('[FL] valor:',e.message));
              await dbSet(FL_KEY,db2);
            }
            // Board sync para regra loja
            await fetch('https://reparoeletroadm.com/api/board?action=sync').catch(e=>console.error('[FL] sync:',e.message));
          }
        }catch(e){console.error('[FrenteLoja] BG Pipefy:',e.message);}
      })();

      return res.status(200).json({ok:true,ficha});
    }
  }

  if(req.method==='POST'&&action==='conserto-realizado'){
    const {pipefyCardId}=req.body||{};
    if(!pipefyCardId)return res.status(400).json({ok:false,error:'pipefyCardId obrigatório'});
    const db=await dbGet(FL_KEY)||defaultDB();
    const ficha=db.fichas.find(f=>f.pipefyCardId===String(pipefyCardId));
    if(!ficha)return res.status(404).json({ok:false,error:'Ficha não encontrada'});
    const now=new Date().toISOString();
    ficha.phase='conserto_realizado';ficha.liberadoHoje=true;ficha.movedAt=now;
    ficha.history=(ficha.history||[]).concat([{phase:'conserto_realizado',ts:now}]);
    await dbSet(FL_KEY,db);return res.status(200).json({ok:true,ficha});
  }

  if(req.method==='POST'&&action==='programar-entrega'){
    const {id}=req.body||{};
    const db=await dbGet(FL_KEY)||defaultDB();
    const ficha=db.fichas.find(f=>f.id===id);
    if(!ficha)return res.status(404).json({ok:false,error:'Não encontrada'});
    try{const phId=await getPipefyPhaseId('programar entrega');if(ficha.pipefyCardId&&phId)await movePipefyCard(ficha.pipefyCardId,phId);}catch(e){}
    return res.status(200).json({ok:true});
  }

  if(req.method==='POST'&&action==='liberar'){
    const {id,valor,formaPagamento}=req.body||{};
    if(!id)return res.status(400).json({ok:false,error:'id obrigatório'});
    const db=await dbGet(FL_KEY)||defaultDB();
    const ficha=db.fichas.find(f=>f.id===id);
    if(!ficha)return res.status(404).json({ok:false,error:'Não encontrada'});
    const now=new Date().toISOString();
    ficha.phase='pago';ficha.pagoEm=now;ficha.pagoValor=parseFloat(valor)||ficha.orcamento?.valor||0;
    ficha.pagoPor=formaPagamento||ficha.orcamento?.formaPagamento||'pix';ficha.movedAt=now;
    ficha.history=(ficha.history||[]).concat([{phase:'pago',ts:now}]);
    try{const phId=await getPipefyPhaseId('receber');if(ficha.pipefyCardId&&phId)await movePipefyCard(ficha.pipefyCardId,phId);}catch(e){}
    await dbSet(FL_KEY,db);return res.status(200).json({ok:true,ficha});
  }

  if(action==='rastrear'){
    const q=(req.query.q||'').trim().toLowerCase();
    if(!q)return res.status(400).json({ok:false,error:'Query obrigatória'});
    const db=await dbGet(FL_KEY)||defaultDB();
    const found=db.fichas.filter(f=>(f.id+' '+f.nomeContato+' '+f.telefone+' '+f.equipamento).toLowerCase().includes(q));
    return res.status(200).json({ok:true,fichas:found});
  }

  if(req.method==='POST'&&action==='mover'){
    const {id,phase,dados}=req.body||{};
    if(!id||!phase)return res.status(400).json({ok:false,error:'id e phase obrigatórios'});
    const db=await dbGet(FL_KEY)||defaultDB();
    const ficha=db.fichas.find(f=>f.id===id);
    if(!ficha)return res.status(404).json({ok:false,error:'Ficha não encontrada'});
    const now=new Date().toISOString();
    ficha.phase=phase;ficha.movedAt=now;
    ficha.history=(ficha.history||[]).concat([{phase,ts:now}]);
    if(dados&&phase==='endereco')ficha.endereco=dados;
    if(phase==='liberado_hoje')ficha.liberadoHoje=true;
    await dbSet(FL_KEY,db);return res.status(200).json({ok:true,ficha});
  }

  return res.status(404).json({ok:false,error:'Ação não encontrada'});
}
