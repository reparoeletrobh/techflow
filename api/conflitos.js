// api/conflitos.js — Módulo Conflitos | Reparo Eletro BH TechFlow
const U=(process.env.UPSTASH_URL||'').replace(/['"]/g,'').trim();
const T=(process.env.UPSTASH_TOKEN||'').replace(/['"]/g,'').trim();
const KEY='reparoeletro_conflitos';

async function dbGet(k){
  try{const r=await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});const j=await r.json();const v=j[0]?.result;if(!v)return null;let x=JSON.parse(v);if(typeof x==='string')x=JSON.parse(x);return x;}catch(e){return null;}
}
async function dbSet(k,v){
  try{await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});}catch(e){}
}

function defaultDB(){ return { conflitos: [] }; }

function scoreOrdem(c){
  if(c.status==='resolvido') return 0;
  const p = c.prioridade==='critico'?3:c.prioridade==='alto'?2:1;
  return p;
}

module.exports = async function handler(req,res){
  // 🔐 TF-AUTH (Fase 1): chave obrigatória em toda chamada
  const _tfk = (req.query && req.query.k) || req.headers['x-tf-key'] || '';
  if (_tfk !== ((process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim())) {
    return res.status(401).json({ ok: false, error: 'não autorizado' });
  }

  // CORS restrito — apenas domínio autorizado
  res.setHeader('Access-Control-Allow-Origin', 'https://reparoeletroadm.com');
  // Limite de payload — rejeitar requisições > 512KB
  if (req.method === 'POST' && parseInt(req.headers['content-length']||0) > 524288) {
    return res.status(413).json({ok:false,error:'Payload muito grande (máx 512KB)'});
  }
  res.setHeader('X-Content-Type-Options','nosniff');
  res.setHeader('X-Frame-Options','SAMEORIGIN');
  res.setHeader('Access-Control-Allow-Methods','GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers','Content-Type');
  if(req.method==='OPTIONS') return res.status(200).end();

  const action = req.query.action||'';
  const db = await dbGet(KEY) || defaultDB();
  if(!db.conflitos) db.conflitos=[];

  // ── GET listar ──────────────────────────────────────────────────────────────
  if(action==='listar'){
    const lista = [...db.conflitos].sort((a,b)=>scoreOrdem(b)-scoreOrdem(a)||new Date(b.criadoEm)-new Date(a.criadoEm));
    const abertos   = db.conflitos.filter(c=>c.status!=='resolvido'&&c.status!=='relatar');
    const criticos  = abertos.filter(c=>c.prioridade==='critico').length;
    const semResp   = abertos.filter(c=>!c.responsavel).length;
    return res.status(200).json({ok:true,conflitos:lista,total:db.conflitos.length,criticos,semResp,abertos:abertos.length});
  }

  // ── GET badge — só o contador para o header ─────────────────────────────────
  if(action==='badge'){
    const criticos = db.conflitos.filter(c=>c.status!=='resolvido'&&c.status!=='relatar'&&c.prioridade==='critico').length;
    const abertos  = db.conflitos.filter(c=>c.status!=='resolvido'&&c.status!=='relatar').length;
    return res.status(200).json({ok:true,criticos,abertos});
  }

  // ── POST criar ──────────────────────────────────────────────────────────────
  if(req.method==='POST'&&action==='criar'){
    // vínculo com o pipe: quando informado, o conflito herda telefone e equipamento
    try{
      const v=(req.body||{}).vinculo;
      if(v&&v.telefone){
        req.body.telefone=String(v.telefone).replace(/\D/g,'');
        req.body.cardId=v.id||'';
        req.body.cardOrigem=(v.onde||'')+' · '+(v.id||'');   // rastreabilidade tecnica
        req.body.cardOnde=v.onde||'';
        if(!req.body.cliente&&v.nome)req.body.cliente=v.nome;
        if(!req.body.equipamento&&v.equipamento)req.body.equipamento=v.equipamento;
      }
    }catch(e){}
    const{titulo,tipo,prioridade,setor,ficha,responsavel,descricao,registradoPor}=req.body||{};
    if(!titulo||!prioridade) return res.status(400).json({ok:false,error:'titulo e prioridade obrigatórios'});
    const novo={
      id:'conf-'+Date.now().toString(36),
      titulo, tipo:tipo||'Outro', prioridade,
      setor:setor||'', ficha:ficha||'', responsavel:responsavel||'',
      descricao:descricao||'', registradoPor:registradoPor||'',
      status:'aberto', criadoEm:new Date().toISOString(),
      atualizadoEm:new Date().toISOString(),
      resolvidoEm:null, solucao:'', acaoPreventiva:''
    };
    db.conflitos.unshift(novo);
    await dbSet(KEY,db);
    return res.status(200).json({ok:true,conflito:novo});
  }

  // ── POST assumir ────────────────────────────────────────────────────────────
  if(req.method==='POST'&&action==='assumir'){
    const{id,responsavel}=req.body||{};
    const c=db.conflitos.find(x=>x.id===id);
    if(!c) return res.status(404).json({ok:false,error:'não encontrado'});
    c.responsavel=responsavel||c.responsavel;
    c.status='andamento'; c.atualizadoEm=new Date().toISOString();
    await dbSet(KEY,db);
    return res.status(200).json({ok:true,conflito:c});
  }

  // ── 🩺 STATUS-CONFLITOS: quantos há em cada estado (diagnóstico) ──
  if(action==='status-conflitos'){
    const cont={}, amostra={};
    for(const c of (db.conflitos||[])){
      const s=c.status||'(sem status)';
      cont[s]=(cont[s]||0)+1;
      if(!amostra[s])amostra[s]=[];
      if(amostra[s].length<3)amostra[s].push({
        id:c.id, titulo:String(c.titulo||'').slice(0,34),
        cliente:c.cliente||c.ficha||'', temSolucao:!!c.solucao,
        temTelefone:!!(c.telefone||c.tel), resolvidoEm:c.resolvidoEm||null });
    }
    return res.status(200).json({ok:true,total:(db.conflitos||[]).length,porStatus:cont,amostra});
  }

  // ── 🔧 MIGRAR-RELATAR: conflitos resolvidos sem retorno ao cliente ──
  if(action==='migrar-relatar'){
    const alvo=(db.conflitos||[]).filter(c=>c.status==='resolvido'&&!c.clienteRelatado);
    if(String(req.query.aplicar||'')==='1'&&alvo.length){
      for(const c of alvo){ c.status='relatar'; c.migradoEm=new Date().toISOString(); }
      await dbSet(KEY,db);
      return res.status(200).json({ok:true,migrados:alvo.length,
        lista:alvo.slice(0,20).map(c=>String(c.titulo||c.id).slice(0,40))});
    }
    return res.status(200).json({ok:true,seriamMigrados:alvo.length,
      lista:alvo.slice(0,20).map(c=>String(c.titulo||c.id).slice(0,40)),
      dica:'para aplicar: &aplicar=1'});
  }

  // ── 🔄 CORRIGIR-FICHAS: troca o código do card pelo NOME + 4 dígitos do cliente ──
  if(action==='corrigir-fichas'){
    // parece código de card? (PIPE-XXXX, GARANTIA-123, LOG-0007, prosp_xxx…)
    const ehCodigo = v => /^(PIPE-|GARANTIA-|LOG-|ALM-|prosp_|conf_|R-\d{8})/i.test(String(v||'').trim())
      || /^[A-Z]{2,}-[A-Z0-9]{4,}/i.test(String(v||'').trim());
    const [ppA,ppT,lgA,lgT,fA,fT,pros]=await Promise.all([
      dbGet('reparoeletro_pipe'),dbGet('tv_pipe'),
      dbGet('reparoeletro_logistica'),dbGet('tv_logistica'),
      dbGet('fichas_adm'),dbGet('fichas_tv'),dbGet('prospeccao_adm')]);
    // índice: id do card → nome e telefone
    const porId={}, porTel={};
    const guarda=(id,nome,tel)=>{
      const n=String(nome||'').trim();
      const t=String(tel||'').replace(/\D/g,'');
      if(id&&n) porId[String(id)]={nome:n,tel:t};
      if(t.length>=10&&n){ const d8=t.slice(-8); if(!porTel[d8])porTel[d8]={nome:n,tel:t}; }
    };
    for(const b of [ppA,ppT]) for(const c of (((b||{}).cards)||[])) guarda(c.id,c.nomeContato,c.telefone);
    for(const b of [lgA,lgT,fA,fT]) for(const f of (((b||{}).fichas)||[])) guarda(f.id,f.nome,f.telefone);
    for(const f of (((pros||{}).fichas)||[])) guarda(f.id,f.nome,f.telefone);

    const trocas=[], semAchar=[];
    for(const c of (db.conflitos||[])){
      const atual=String(c.ficha||'').trim();
      if(!atual||!ehCodigo(atual))continue;              // só mexe no que é código
      let info=porId[atual]||null;
      // se não achou pelo id, tenta pelo cardId ou pelo telefone já gravado
      if(!info&&c.cardId)info=porId[String(c.cardId)]||null;
      if(!info&&(c.telefone||c.tel)){
        const d8=String(c.telefone||c.tel).replace(/\D/g,'').slice(-8);
        info=porTel[d8]||null;
      }
      if(!info){ semAchar.push(atual+' | '+String(c.titulo||'').slice(0,34)); continue; }
      const legivel=info.nome+(info.tel?' '+info.tel.slice(-4):'');
      trocas.push({id:c.id, de:atual, para:legivel, titulo:String(c.titulo||'').slice(0,34)});
      if(String(req.query.aplicar||'')==='1'){
        c.fichaCodigo=atual;                             // guarda o código original
        c.ficha=legivel;
        if(!c.telefone&&info.tel)c.telefone=info.tel;
        if(!c.cliente)c.cliente=info.nome;
      }
    }
    if(String(req.query.aplicar||'')==='1'&&trocas.length){
      await dbSet(KEY,db);
      return res.status(200).json({ok:true,corrigidos:trocas.length,
        naoIdentificados:semAchar.length,
        lista:trocas.slice(0,40).map(t=>t.de+' → '+t.para)});
    }
    return res.status(200).json({ok:true,
      seriamCorrigidos:trocas.length,
      naoIdentificados:semAchar.length,
      lista:trocas.slice(0,40).map(t=>t.de+' → '+t.para),
      naoAchados:semAchar.slice(0,20),
      dica:'para aplicar: &aplicar=1'});
  }

  // ── 🔗 VINCULAR-FICHA: liga um conflito já existente a uma ficha do pipe ──
  if(req.method==='POST'&&action==='vincular-ficha'){
    const {id,vinculo}=req.body||{};
    if(!id||!vinculo||!vinculo.telefone){
      return res.status(400).json({ok:false,error:'informe o conflito e a ficha'});
    }
    const c=db.conflitos.find(x=>x.id===id);
    if(!c)return res.status(404).json({ok:false,error:'conflito não encontrado'});
    c.telefone=String(vinculo.telefone).replace(/\D/g,'');
    c.cardId=vinculo.id||'';
    c.cardOnde=vinculo.onde||'';
    if(vinculo.equipamento&&!c.equipamento)c.equipamento=vinculo.equipamento;
    c.vinculadoEm=new Date().toISOString();
    await dbSet(KEY,db);
    return res.status(200).json({ok:true,conflito:c});
  }

  // ── 🔎 BUSCAR-FICHA: procura no pipe para vincular ao conflito (nome, telefone ou OS) ──
  if(action==='buscar-ficha'){
    const q=String(req.query.q||'').toLowerCase().trim();
    if(q.length<3)return res.status(200).json({ok:false,error:'digite ao menos 3 caracteres'});
    const [ppA,ppT,lgA,lgT]=await Promise.all([
      dbGet('reparoeletro_pipe'),dbGet('tv_pipe'),
      dbGet('reparoeletro_logistica'),dbGet('tv_logistica')]);
    const so=t=>String(t||'').replace(/\D/g,'');
    const qNum=so(q);
    const casa=(nome,tel,id,equip)=>{
      const n=String(nome||'').toLowerCase();
      const t=so(tel);
      if(n.includes(q))return true;
      if(qNum.length>=4&&t.endsWith(qNum))return true;
      if(String(id||'').toLowerCase().includes(q))return true;
      if(String(equip||'').toLowerCase().includes(q))return true;
      return false;
    };
    const achados=[];
    for(const [b,onde,tipo] of [[ppA,'Pipe ADM','card'],[ppT,'Pipe TV','card'],
                                 [lgA,'Logística ADM','ficha'],[lgT,'Logística TV','ficha']]){
      const itens=tipo==='card'?(((b||{}).cards)||[]):(((b||{}).fichas)||[]);
      for(const i of itens){
        const nome=i.nomeContato||i.nome||'';
        const tel=i.telefone||i.tel||'';
        if(!casa(nome,tel,i.id,i.equipamento))continue;
        if(!so(tel))continue;                       // sem telefone não serve para o vínculo
        achados.push({ id:i.id, onde, nome, telefone:so(tel),
          equipamento:i.equipamento||i.descricao||'',
          fase:i.phaseId||i.phase||'', valor:i.valor||null,
          movidoEm:i.movedAt||i.criadoEm||null });
        if(achados.length>=25)break;
      }
      if(achados.length>=25)break;
    }
    return res.status(200).json({ok:true,total:achados.length,
      resultados:achados.map(a=>({...a,
        rotulo:a.nome+' · '+String(a.telefone).slice(-4)+' · '+String(a.equipamento).slice(0,26)+' · '+a.onde}))});
  }

  // ── 🧹 LIMPAR-TELEFONES-ERRADOS: remove os que não batem com os 4 dígitos do nome ──
  if(action==='limpar-telefones-errados'){
    const ruins=[];
    for(const c of (db.conflitos||[])){
      const tel=String(c.telefone||c.tel||'').replace(/\D/g,'');
      if(!tel)continue;
      const m4=String(c.cliente||c.ficha||'').match(/(\d{4})\s*$/);
      if(!m4)continue;
      if(!tel.endsWith(m4[1])){
        ruins.push((c.cliente||c.ficha)+' | tinha '+tel+' | esperado terminar em '+m4[1]);
        if(String(req.query.aplicar||'')==='1'){ delete c.telefone; delete c.tel; }
      }
    }
    if(String(req.query.aplicar||'')==='1'&&ruins.length){
      await dbSet(KEY,db);
      return res.status(200).json({ok:true,removidos:ruins.length,lista:ruins.slice(0,40)});
    }
    return res.status(200).json({ok:true,errados:ruins.length,lista:ruins.slice(0,40),
      dica:'para remover: &aplicar=1'});
  }

  // ── 📱 COMPLETAR-TELEFONES: busca o telefone do cliente em toda a operação ──
  if(action==='completar-telefones'){
    const [ppA,ppT,lgA,lgT,fA,fT]=await Promise.all([
      dbGet('reparoeletro_pipe'),dbGet('tv_pipe'),
      dbGet('reparoeletro_logistica'),dbGet('tv_logistica'),
      dbGet('fichas_adm'),dbGet('fichas_tv')]);
    const mapa={};
    const guarda=(nome,tel)=>{
      const n=String(nome||'').trim().toLowerCase();
      const t=String(tel||'').replace(/\D/g,'');
      if(n.length<3||t.length<10)return;
      if(!mapa[n])mapa[n]=t;
    };
    for(const b of [ppA,ppT]) for(const c of (((b||{}).cards)||[])) guarda(c.nomeContato,c.telefone);
    for(const b of [lgA,lgT,fA,fT]) for(const f of (((b||{}).fichas)||[])) guarda(f.nome,f.telefone);
    const achados=[];
    for(const c of (db.conflitos||[])){
      if(c.telefone||c.tel)continue;
      const alvo=String(c.cliente||c.ficha||'').trim().toLowerCase();
      if(alvo.length<3)continue;
      // 🎯 o nome do cliente já traz os 4 dígitos finais — usar isso como VERIFICAÇÃO.
      // Casar por nome parcial pegava outra pessoa: "Maria 8742" casava com qualquer Maria.
      const m4=String(c.cliente||c.ficha||'').match(/(\d{4})\s*$/);
      const d4=m4?m4[1]:null;
      let tel=mapa[alvo];
      if(tel&&d4&&!String(tel).endsWith(d4))tel=null;          // nome exato mas telefone divergente → descarta
      if(!tel&&d4){
        // procura quem realmente termina nesses 4 dígitos
        for(const k of Object.keys(mapa)){
          if(!String(mapa[k]).endsWith(d4))continue;
          if(k===alvo||k.includes(alvo)||alvo.includes(k)){tel=mapa[k];break;}
        }
      }
      if(!tel&&!d4){                                            // sem 4 dígitos no nome: só nome EXATO
        tel=mapa[alvo]||null;
      }
      if(tel){ c.telefone=tel; achados.push((c.cliente||c.ficha)+' → '+tel+(d4?(String(tel).endsWith(d4)?' ✓':' ⚠️'):' (sem verificação)')); }
    }
    if(String(req.query.aplicar||'')==='1'&&achados.length){
      await dbSet(KEY,db);
      return res.status(200).json({ok:true,completados:achados.length,lista:achados.slice(0,30)});
    }
    return res.status(200).json({ok:true,seriamCompletados:achados.length,
      semTelefone:(db.conflitos||[]).filter(c=>!c.telefone&&!c.tel).length,
      lista:achados.slice(0,30),dica:'para aplicar: &aplicar=1'});
  }

  // ── POST relatado: cliente avisado do desfecho → vai para FINALIZADOS ──
  if(req.method==='POST'&&action==='relatado'){
    const{id,por}=req.body||{};
    const c=db.conflitos.find(x=>x.id===id);
    if(!c) return res.status(404).json({ok:false,error:'não encontrado'});
    c.status='resolvido';
    c.clienteRelatado=true;
    c.relatadoEm=new Date().toISOString();
    c.relatadoPor=String(por||'').slice(0,40);
    c.atualizadoEm=c.relatadoEm;
    await dbSet(KEY,db);
    return res.status(200).json({ok:true,conflito:c});
  }

  // ── POST resolver ───────────────────────────────────────────────────────────
  if(req.method==='POST'&&action==='resolver'){
    const{id,solucao,acaoPreventiva}=req.body||{};
    const c=db.conflitos.find(x=>x.id===id);
    if(!c) return res.status(404).json({ok:false,error:'não encontrado'});
    // 📣 etapa intermediária: resolvido vai para RELATAR CLIENTE, não direto para finalizado
    c.status='relatar'; c.solucao=solucao||'';
    c.acaoPreventiva=acaoPreventiva||'';
    c.resolvidoEm=new Date().toISOString();
    c.atualizadoEm=new Date().toISOString();
    await dbSet(KEY,db);
    return res.status(200).json({ok:true,conflito:c,proximoPasso:'relatar ao cliente'});
  }

  // ── POST editar ─────────────────────────────────────────────────────────────
  if(req.method==='POST'&&action==='editar'){
    const{id,...campos}=req.body||{};
    const c=db.conflitos.find(x=>x.id===id);
    if(!c) return res.status(404).json({ok:false,error:'não encontrado'});
    ['titulo','tipo','prioridade','setor','ficha','responsavel','descricao'].forEach(k=>{
      if(campos[k]!==undefined) c[k]=campos[k];
    });
    c.atualizadoEm=new Date().toISOString();
    await dbSet(KEY,db);
    return res.status(200).json({ok:true,conflito:c});
  }

  // ── POST excluir ────────────────────────────────────────────────────────────
  if(req.method==='POST'&&action==='excluir'){
    const{id}=req.body||{};
    db.conflitos=db.conflitos.filter(x=>x.id!==id);
    await dbSet(KEY,db);
    return res.status(200).json({ok:true});
  }

  // ── GET relatorio ───────────────────────────────────────────────────────────
  if(action==='relatorio'){
    const hoje=new Date();
    const semAgo=new Date(hoje.getTime()-7*24*3600*1000);
    const todos=db.conflitos;
    const daSemana=todos.filter(c=>new Date(c.criadoEm)>=semAgo);
    const resolvidosSem=daSemana.filter(c=>c.status==='resolvido');
    // Tempo médio resolução
    const tempos=resolvidosSem.filter(c=>c.resolvidoEm).map(c=>(new Date(c.resolvidoEm)-new Date(c.criadoEm))/3600000);
    const tmedio=tempos.length?Math.round(tempos.reduce((a,b)=>a+b,0)/tempos.length*10)/10:0;
    // Por tipo
    const porTipo={};
    daSemana.forEach(c=>{porTipo[c.tipo]=(porTipo[c.tipo]||0)+1;});
    const tiposOrdenados=Object.entries(porTipo).sort((a,b)=>b[1]-a[1]);
    return res.status(200).json({
      ok:true,
      periodo:{de:semAgo.toISOString().slice(0,10),ate:hoje.toISOString().slice(0,10)},
      abertos:todos.filter(c=>c.status!=='resolvido').length,
      resolvidosSemana:resolvidosSem.length,
      totalSemana:daSemana.length,
      tempoMedioHoras:tmedio,
      semResponsavel:todos.filter(c=>c.status!=='resolvido'&&!c.responsavel).length,
      criticos:todos.filter(c=>c.status!=='resolvido'&&c.prioridade==='critico').length,
      tiposOrdenados,
    });
  }

  // ── POST adicionar-nota ─────────────────────────────────────────────────────
  if(req.method==='POST'&&action==='adicionar-nota'){
    const{id,texto,vencimento,autor}=req.body||{};
    const c=db.conflitos.find(x=>x.id===id);
    if(!c) return res.status(404).json({ok:false,error:'conflito não encontrado'});
    if(!c.notas) c.notas=[];
    c.notas.push({
      nid:'n-'+Date.now().toString(36),
      texto:texto||'',
      vencimento:vencimento||'',
      autor:autor||'',
      criadaEm:new Date().toISOString()
    });
    c.atualizadoEm=new Date().toISOString();
    await dbSet(KEY,db);
    return res.status(200).json({ok:true,conflito:c});
  }

  // ── POST excluir-nota ────────────────────────────────────────────────────────
  if(req.method==='POST'&&action==='excluir-nota'){
    const{id,nid}=req.body||{};
    const c=db.conflitos.find(x=>x.id===id);
    if(!c) return res.status(404).json({ok:false,error:'conflito não encontrado'});
    c.notas=(c.notas||[]).filter(n=>n.nid!==nid);
    c.atualizadoEm=new Date().toISOString();
    await dbSet(KEY,db);
    return res.status(200).json({ok:true});
  }

  return res.status(404).json({ok:false,error:'ação não encontrada: '+action});
};
