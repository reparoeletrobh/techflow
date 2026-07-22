// api/recovery.js — Diagnóstico e recuperação de dados Redis
const U=(process.env.UPSTASH_URL||'').replace(/['"]/g,'').trim();
const T=(process.env.UPSTASH_TOKEN||'').replace(/['"]/g,'').trim();

async function dbGet(k){try{const r=await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});const j=await r.json();const v=j[0]?.result;if(!v)return null;let x=JSON.parse(v);if(typeof x==='string')x=JSON.parse(x);return x;}catch(e){return null;}}
async function dbSet(k,v){try{const r=await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});const j=await r.json();return j[0]?.result==='OK';}catch(e){return false;}}
async function dbKeys(pattern){try{const r=await fetch(U+'/keys/'+encodeURIComponent(pattern),{headers:{Authorization:'Bearer '+T}});const j=await r.json();return j.result||[];}catch(e){return [];}}

module.exports = async function handler(req,res){
  // 🔐 TF-AUTH (Fase 1): chave obrigatória em toda chamada
  const _tfk = (req.query && req.query.k) || req.headers['x-tf-key'] || '';
  if (_tfk !== ((process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim())) {
    return res.status(401).json({ ok: false, error: 'não autorizado' });
  }

  res.setHeader('Access-Control-Allow-Origin','https://reparoeletroadm.com');
  const action=req.query.action||'';

  // Diagnóstico: ver quais chaves existem e tamanho dos dados
  if(action==='diagnostico'){
    const chaves=['reparoeletro_pipe','reparoeletro_board','reparoeletro_conflitos',
                  'pj_fornecedores','reparoeletro_logistica','reparoeletro_frenteloja',
                  'reparoeletro_financeiro','reparoeletro_backup_index'];
    const resultado={};
    for(const ch of chaves){
      try{
        const d=await dbGet(ch);
        if(!d){resultado[ch]={status:'VAZIO/NULL'};}
        else if(d.cards){resultado[ch]={status:'OK',cards:d.cards.length};}
        else if(d.fichas){resultado[ch]={status:'OK',fichas:d.fichas.length};}
        else if(d.fornecedores){resultado[ch]={status:'OK',clientes:d.fornecedores.length};}
        else if(d.conflitos){resultado[ch]={status:'OK',conflitos:d.conflitos.length};}
        else if(Array.isArray(d)){resultado[ch]={status:'OK',items:d.length};}
        else{resultado[ch]={status:'OK',keys:Object.keys(d).join(',')};}
      }catch(e){resultado[ch]={status:'ERRO',msg:e.message};}
    }
    return res.status(200).json({ok:true,resultado});
  }

  // Listar backups disponíveis
  if(action==='listar-backups'){
    const idx=await dbGet('reparoeletro_backup_index')||[];
    return res.status(200).json({ok:true,total:idx.length,backups:idx.slice(-10).reverse()});
  }

  // Restaurar chave a partir de backup
  if(req.method==='POST'&&action==='restaurar'){
    const{chave,backupKey}=req.body||{};
    if(!chave||!backupKey) return res.status(400).json({ok:false,error:'chave e backupKey obrigatórios'});
    const backup=await dbGet(backupKey);
    if(!backup) return res.status(404).json({ok:false,error:'backup não encontrado: '+backupKey});
    // Salvar backup da versão atual antes de restaurar
    const agora=new Date().toISOString().replace(/[:.]/g,'-').slice(0,16);
    const atualBak=chave+'_antes_restauracao_'+agora;
    const atual=await dbGet(chave);
    if(atual) await dbSet(atualBak,atual);
    // Restaurar
    const ok=await dbSet(chave,backup);
    return res.status(200).json({ok,chave,backupKey,backupDaSalvo:atualBak,msg:ok?'Restaurado com sucesso':'Falha ao escrever'});
  }

  return res.status(200).json({ok:false,error:'ação não encontrada. Use: diagnostico, listar-backups, restaurar (POST)'});
};
