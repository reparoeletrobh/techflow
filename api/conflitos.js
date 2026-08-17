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
      // o nome no sistema já costuma terminar com os 4 dígitos — não duplicar
      const d4=info.tel?info.tel.slice(-4):'';
      const jaTem=d4&&new RegExp('\\b'+d4+'\\s*$').test(info.nome);
      const legivel=info.nome+((d4&&!jaTem)?' '+d4:'');
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

  // ── 🧹 FINALIZAR-RELATAR: move para finalizados sem avisar o cliente ──
  // Os casos acumulados na aba de relato são anteriores ao fluxo automático:
  // já foram tratados, e mandar aviso agora confundiria quem não espera mais
  // retorno. Vão para finalizados marcados como não avisados, para que o
  // disparo automático futuro não os alcance.
  if(action==='finalizar-relatar'){
    const alvo=(db.conflitos||[]).filter(c=>String(c.status||'')==='relatar');
    if(String(req.query.aplicar||'')!=='1'){
      return res.status(200).json({ok:true,modo:'prévia',
        vaoSerFinalizados:alvo.length,
        L:alvo.slice(0,60).map(c=>String(c.cliente||c.ficha||'?').slice(0,24)+' '
          +String(c.telefone||c.tel||'').slice(-4)+' | '+String(c.motivo||'').slice(0,38)),
        oQueVaiAcontecer:'vão para finalizados SEM nenhuma mensagem ao cliente',
        dica:'para aplicar: &aplicar=1'});
    }
    const agora=new Date().toISOString();
    let n=0;
    for(const c of alvo){
      c.status='resolvido';
      c.clienteRelatado=false;
      c.finalizadoSemAviso=true;
      c.finalizadoSemAvisoEm=agora;
      // 🔕 impede que o disparo automático alcance estes casos depois
      c.avisoAberturaEm=c.avisoAberturaEm||agora;
      c.avisoResolucaoEm=c.avisoResolucaoEm||agora;
      c.avisoAberturaVia='não enviado — caso anterior ao fluxo automático';
      c.avisoResolucaoVia='não enviado — caso anterior ao fluxo automático';
      c.atualizadoEm=agora;
      n++;
    }
    if(n) await dbSet(KEY,db);
    return res.status(200).json({ok:true,finalizados:n,
      observacao:'nenhuma mensagem foi enviada e estes casos não entram nos avisos futuros'});
  }

  // ── 📣 AVISAR-CLIENTE: disparo automático de abertura e de resolução ──
  // O cliente que gerou um conflito está insatisfeito e em silêncio. Saber que
  // um chamado foi aberto — e depois que foi resolvido — muda a percepção mais
  // do que a solução em si, que ele nem sempre enxerga.
  if(action==='avisar-clientes'){
    const aplicar=String(req.query.aplicar||'')==='1';
    const d8c=t=>String(t||'').replace(/\D/g,'').slice(-8);
    const lista=db.conflitos||[];

    const pendentes={abertura:[],resolucao:[]};
    for(const c of lista){
      const tel=String(c.telefone||c.tel||'').replace(/\D/g,'');
      if(tel.length<10) continue;
      // 1) conflito aberto e cliente ainda não avisado
      if(!c.avisoAberturaEm && ['aberto','andamento'].includes(String(c.status||''))){
        pendentes.abertura.push(c);
      }
      // 2) 📍 chegar em RELATAR CLIENTE é o marco do desfecho: é quando a
      // equipe dá o caso por encerrado e pronto para comunicar. O campo de
      // solução guarda o andamento da produção — "em teste", "na pintura" —
      // e usá-lo como critério avisava gente com serviço em andamento.
      if(!c.avisoResolucaoEm && String(c.status||'')==='relatar'){
        pendentes.resolucao.push(c);
      }
    }

    if(!aplicar){
      return res.status(200).json({ok:true,modo:'prévia',
        aberturaPendente:pendentes.abertura.length,
        resolucaoPendente:pendentes.resolucao.length,
        ABERTURA:pendentes.abertura.map(c=>String(c.cliente||c.ficha||'?').slice(0,22)+' '
          +String(c.telefone||c.tel||'').slice(-4)+' | '+String(c.motivo||'').slice(0,40)),
        RESOLUCAO:pendentes.resolucao.map(c=>String(c.cliente||c.ficha||'?').slice(0,22)+' '
          +String(c.telefone||c.tel||'').slice(-4)+' | '+String(c.solucao||'').slice(0,40)),
        dica:'para enviar: &aplicar=1'});
    }

    // ⏰ assunto delicado: não sai de madrugada nem no domingo
    const agoraBR=new Date(Date.now()-3*3600000);
    const hBR=agoraBR.getUTCHours(), dBR=agoraBR.getUTCDay();
    const expediente=(dBR>=1&&dBR<=5&&hBR>=8&&hBR<19)||(dBR===6&&hBR>=8&&hBR<13);
    if(!expediente&&String(req.query.forcar||'')!=='1'){
      return res.status(200).json({ok:true,adiado:true,
        motivo:'fora do expediente — os avisos saem no próximo horário comercial'});
    }

    const cfg=(await dbGet('wa_credenciais'))||{};
    const pid=cfg.phoneId||process.env.WA_PHONE_ID;
    const tk=cfg.token||process.env.WA_TOKEN;
    if(!pid||!tk) return res.status(200).json({ok:false,error:'credenciais do WhatsApp ausentes'});
    const CHAVE=(process.env.ANTHROPIC_API_KEY||'').trim();

    // janela de 24h: fora dela só modelo aprovado
    const ultimaIn={};
    try{
      const U2=(process.env.UPSTASH_URL||'').replace(/['"]/g,'').trim();
      const T2=(process.env.UPSTASH_TOKEN||'').replace(/[\n\r'"]/g,'').trim();
      const r=await fetch(U2+'/lrange/wa_evt_list/-6000/-1',
        {headers:{Authorization:'Bearer '+T2}}).then(x=>x.json());
      for(const s of (r.result||[])){
        try{const e=JSON.parse(s); if(e.dir!=='in')continue;
          const d=d8c(e.tel); if(!d)continue;
          const q=new Date(e.ts||0).getTime();
          if(!ultimaIn[d]||q>ultimaIn[d]) ultimaIn[d]=q;
        }catch(x){}
      }
    }catch(e){}

    const enviar=async(c,tipo)=>{
      let tel=String(c.telefone||c.tel||'').replace(/\D/g,'');
      if(tel.length===10||tel.length===11) tel='55'+tel;
      if(tel.length<12) return {ok:false,erro:'telefone inválido'};
      const primeiro=String(c.cliente||c.ficha||'').trim().split(/\s+/)[0]||'';
      const janelaAberta=ultimaIn[d8c(tel)]&&(Date.now()-ultimaIn[d8c(tel)])<24*3600000;

      if(janelaAberta){
        // texto livre, escrito a partir do caso
        let texto=tipo==='abertura'
          ? 'Oi'+(primeiro?' '+primeiro:'')+'! Aqui é da Reparo Eletro.\n\n'
            +'Abrimos um chamado prioritário para tratar da sua situação. '
            +'Nossa equipe já está cuidando disso e te dá um retorno em breve.\n\n'
            +'Se quiser falar com a gente, é só responder aqui.'
          : 'Oi'+(primeiro?' '+primeiro:'')+'! Voltando para te dar um retorno.\n\n'
            +'Sua situação foi resolvida. Qualquer dúvida é só chamar. 😊';
        if(CHAVE){
          try{
            const ctx=[c.motivo?('Motivo: '+c.motivo):'',c.descricao?('Descrição: '+c.descricao):'',
              (tipo==='resolucao'&&c.solucao)?('O que foi feito: '+c.solucao):''].filter(Boolean).join('\n');
            const rr=await fetch('https://api.anthropic.com/v1/messages',{method:'POST',
              headers:{'x-api-key':CHAVE,'anthropic-version':'2023-06-01','content-type':'application/json'},
              body:JSON.stringify({model:'claude-sonnet-4-6',max_tokens:350,temperature:0.4,
                system:'Você escreve mensagens de WhatsApp de uma assistência técnica ao cliente, '
                  +'em português do Brasil, máximo 4 linhas curtas.\n'
                  +(tipo==='abertura'
                    ? 'Avise que um chamado prioritário foi aberto para tratar da situação dele e '
                      +'que a equipe já está cuidando. Tom acolhedor, sem prometer prazo.'
                    : 'Avise que a situação foi resolvida, explicando em linguagem simples o que '
                      +'foi feito. Tom tranquilizador.')
                  +'\nNUNCA cite custo interno, nome de peça, culpa de funcionário ou fornecedor. '
                  +'Não invente o que não está no registro. Responda só com a mensagem.',
                messages:[{role:'user',content:'Cliente: '+(primeiro||'(sem nome)')+'\n\n'+ctx}]}),
            }).then(x=>x.json());
            const t=((rr.content||[]).filter(b=>b.type==='text').map(b=>b.text).join('')||'').trim();
            if(t) texto=t;
          }catch(e){}
        }
        const r=await fetch('https://graph.facebook.com/v20.0/'+pid+'/messages',{method:'POST',
          headers:{Authorization:'Bearer '+tk,'Content-Type':'application/json'},
          body:JSON.stringify({messaging_product:'whatsapp',to:tel,type:'text',text:{body:texto}}),
        }).then(x=>x.json());
        return (r&&r.messages&&r.messages[0])
          ? {ok:true,via:'mensagem',texto}
          : {ok:false,erro:(r&&r.error&&r.error.message)||'falha no envio'};
      }

      // janela fechada: modelo aprovado
      const modelo=tipo==='abertura'?'chamado_aberto':'chamado_resolvido';
      const r=await fetch('https://graph.facebook.com/v20.0/'+pid+'/messages',{method:'POST',
        headers:{Authorization:'Bearer '+tk,'Content-Type':'application/json'},
        body:JSON.stringify({messaging_product:'whatsapp',to:tel,type:'template',
          template:{name:modelo,language:{code:'pt_BR'},
            components:[{type:'body',parameters:[{type:'text',text:primeiro||'tudo bem'}]}]}}),
      }).then(x=>x.json());
      return (r&&r.messages&&r.messages[0])
        ? {ok:true,via:'modelo '+modelo}
        : {ok:false,erro:(r&&r.error&&r.error.message)||'modelo recusado'};
    };

    const feitos=[],erros=[];
    let mudou=false;
    for(const tipo of ['abertura','resolucao']){
      for(const c of pendentes[tipo]){
        if(feitos.length>=12) break;         // lote curto: a função tem tempo limitado
        const r=await enviar(c,tipo);
        if(r.ok){
          if(tipo==='abertura'){ c.avisoAberturaEm=new Date().toISOString(); c.avisoAberturaVia=r.via; }
          else { c.avisoResolucaoEm=new Date().toISOString(); c.avisoResolucaoVia=r.via;
                 if(r.texto) c.textoEnviadoAoCliente=String(r.texto).slice(0,600);
                 // 📌 cliente avisado do desfecho: o caso sai de Relatar Cliente
                 c.status='resolvido'; c.clienteRelatado=true;
                 c.relatadoEm=c.avisoResolucaoEm; c.relatadoPor='aviso automático'; }
          mudou=true;
          feitos.push(tipo+' | '+String(c.cliente||'?').slice(0,20)+' | '+r.via);
        } else {
          erros.push(tipo+' | '+String(c.cliente||'?').slice(0,20)+': '+r.erro);
        }
        await new Promise(s=>setTimeout(s,700));
      }
    }
    if(mudou) await dbSet(KEY,db);
    const faltam=(pendentes.abertura.length+pendentes.resolucao.length)-feitos.length-erros.length;
    return res.status(200).json({ok:erros.length===0,enviados:feitos.length,
      faltam:Math.max(0,faltam),L:feitos,erros});
  }

  // ── ✍️ POST mensagem-cliente: transforma a solução técnica em recado cordial ──
  // O que a equipe escreve na resolução é registro interno: cita peça, custo,
  // culpa e prazo em linguagem de bastidor. Enviar isso ao cliente soa seco e
  // às vezes expõe o que não deveria. A IA lê o caso e escreve o retorno.
  if(req.method==='POST'&&action==='mensagem-cliente'){
    const{id}=req.body||{};
    const c=db.conflitos.find(x=>x.id===id);
    if(!c) return res.status(404).json({ok:false,error:'conflito não encontrado'});
    const primeiro=String(c.cliente||c.ficha||'').trim().split(/\s+/)[0]||'';
    const CHAVE=(process.env.ANTHROPIC_API_KEY||'').trim();
    const reserva='Oi'+(primeiro?' '+primeiro:'')+'! Passando para te dar um retorno: '
      +'nossa equipe cuidou do seu caso e já está tudo resolvido. '
      +'Qualquer dúvida é só chamar aqui. 😊';
    if(!CHAVE) return res.status(200).json({ok:true,texto:reserva,via:'texto padrão'});
    const contexto=[
      c.motivo?('Motivo do conflito: '+c.motivo):'',
      c.descricao?('Descrição: '+c.descricao):'',
      c.solucao?('O que a equipe fez: '+c.solucao):'',
      c.equipamento?('Equipamento: '+c.equipamento):'',
    ].filter(Boolean).join('\n');
    try{
      const r=await fetch('https://api.anthropic.com/v1/messages',{
        method:'POST',
        headers:{'x-api-key':CHAVE,'anthropic-version':'2023-06-01','content-type':'application/json'},
        body:JSON.stringify({model:'claude-sonnet-4-6',max_tokens:400,temperature:0.4,
          system:'Você escreve mensagens de WhatsApp de uma assistência técnica para o cliente, '
            +'em português do Brasil. Recebe o registro interno de um problema e escreve o retorno '
            +'ao cliente.\n\nRegras:\n'
            +'- Tom cordial e tranquilizador, sem formalidade excessiva nem entusiasmo forçado\n'
            +'- No máximo 4 linhas curtas\n'
            +'- NUNCA mencione custo interno, nome de peça, culpa de funcionário, '
            +'falha de fornecedor ou qualquer detalhe de bastidor\n'
            +'- Não prometa prazo que o registro não garante\n'
            +'- Se o caso foi resolvido, diga com segurança; se ainda está em andamento, '
            +'diga que a equipe está cuidando e dará retorno\n'
            +'- Não invente informação que não esteja no registro\n'
            +'- Comece pelo primeiro nome do cliente quando houver\n'
            +'- Responda APENAS com a mensagem, sem aspas nem comentários',
          messages:[{role:'user',content:'Cliente: '+(primeiro||'(sem nome)')+'\n\n'+contexto
            +'\n\nEscreva o retorno para este cliente.'}]}),
      }).then(x=>x.json());
      const txt=((r.content||[]).filter(b=>b.type==='text').map(b=>b.text).join('')||'').trim();
      if(!txt) return res.status(200).json({ok:true,texto:reserva,via:'texto padrão — IA sem retorno'});
      return res.status(200).json({ok:true,texto:txt,via:'escrita pela IA'});
    }catch(e){
      return res.status(200).json({ok:true,texto:reserva,via:'texto padrão — '+e.message});
    }
  }

  // ── POST relatado: cliente avisado do desfecho → vai para FINALIZADOS ──
  if(req.method==='POST'&&action==='relatado'){
    const{id,por}=req.body||{};
    const c=db.conflitos.find(x=>x.id===id);
    if(!c) return res.status(404).json({ok:false,error:'não encontrado'});
    c.status='resolvido';
    c.clienteRelatado=true;
    c.relatadoEm=new Date().toISOString();
    // guarda o que foi dito ao cliente: se ele responder, quem atender sabe
    // exatamente qual retorno ele recebeu
    if((req.body||{}).texto) c.textoEnviadoAoCliente=String(req.body.texto).slice(0,600);
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
