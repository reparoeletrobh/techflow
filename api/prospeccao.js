// api/prospeccao.js — Prospecção (aba Criadas da planilha)
// Chave Redis: prospeccao_adm (completamente separado de fichas_adm/fichas_tv)
const U=(process.env.UPSTASH_URL||'').replace(/['"]/g,'').trim();
const T=(process.env.UPSTASH_TOKEN||'').replace(/[\n\r'"]/g,'').trim();

const SHEET_ID  = '1ovSEGZ7if5-wdNZpd1cbLlyg0PZpsrT9fQwOIzfG_mw';
const SHEET_CSV = `https://docs.google.com/spreadsheets/d/${SHEET_ID}/gviz/tq?tqx=out:csv&sheet=Criadas`;
const KEY       = 'prospeccao_adm';

async function dbGet(key){
  try{
    const r=await fetch(`${U}/get/${key}`,{headers:{Authorization:`Bearer ${T}`}});
    const j=await r.json();
    return j.result?JSON.parse(j.result):null;
  }catch{return null;}
}
async function dbSet(key,val){
  try{
    await fetch(`${U}/set/${key}`,{
      method:'POST',
      headers:{Authorization:`Bearer ${T}`,'Content-Type':'application/json'},
      body:JSON.stringify(val)
    });return true;
  }catch{return false;}
}

// Parser CSV robusto (suporta campos com quebras de linha dentro de aspas)
function parseCSV(text){
  const rows=[];const t=text.replace(/\r\n/g,'\n').replace(/\r/g,'\n');
  let i=0,cols=[],cur='',inQ=false;
  while(i<t.length){
    const c=t[i];
    if(inQ){
      if(c==='"'){if(t[i+1]==='"'){cur+='"';i+=2;}else{inQ=false;i++;}}
      else{cur+=c;i++;}
    }else{
      if(c==='"'){inQ=true;i++;}
      else if(c===','){cols.push(cur);cur='';i++;}
      else if(c==='\n'){
        cols.push(cur);
        if(cols.some(x=>x.trim()))rows.push(cols);
        cols=[];cur='';i++;
      }else{cur+=c;i++;}
    }
  }
  cols.push(cur);if(cols.some(x=>x.trim()))rows.push(cols);
  return rows;
}

function waNum(tel){
  const d=String(tel||'').replace(/\D/g,'');
  if(d.startsWith('55')&&d.length>=12)return d;
  return'55'+d;
}
function gerarId(tel,horario){
  return`prosp_${String(tel||'').replace(/\D/g,'').slice(-8)}_${Date.now().toString(36)}`;
}

// ── Log de eventos da prospecção (para o relatório) ──────────────────────
const EVT_KEY='prospeccao_eventos';
// Identidade contínua: ficha vinda do espelho mantém o id original de fichas
function idEvt(f){return (f&&(f.origemFichaId||f.id))||null;}
const EVT_LIST='prospeccao_evt_list'; // lista Redis — escrita ATÔMICA (RPUSH)
async function logEventos(lista){
  // lista: [{tipo, de?, sis, id, nome}] — ts adicionado aqui
  try{
    if(!lista||!lista.length)return;
    const ts=new Date().toISOString();
    for(const e of lista){
      const evt=JSON.stringify({ts,tipo:e.tipo,de:e.de??null,sis:e.sis==='tv'?'tv':'adm',id:e.id||null,nome:e.nome||null});
      // RPUSH é atômico: N requests simultâneos = N eventos, sem sobrescrita
      await fetch(`${U}/rpush/${EVT_LIST}/${encodeURIComponent(evt)}`,{headers:{Authorization:`Bearer ${T}`}});
    }
  }catch(_){}
}

async function rpushLote(eventos){
  // RPUSH com múltiplos valores via comando REST — lotes de 300 por request
  let gravados=0;
  for(let i=0;i<eventos.length;i+=300){
    const lote=eventos.slice(i,i+300).map(e=>JSON.stringify(e));
    try{
      const r=await fetch(U,{
        method:'POST',
        headers:{Authorization:`Bearer ${T}`,'Content-Type':'application/json'},
        body:JSON.stringify(['RPUSH',EVT_LIST,...lote])
      });
      if(r.ok)gravados+=lote.length;
    }catch(_){}
  }
  return gravados;
}

async function lerEventos(){
  let evs=[];
  try{
    const r=await fetch(`${U}/lrange/${EVT_LIST}/0/-1`,{headers:{Authorization:`Bearer ${T}`}});
    const j=await r.json();
    for(const s of (j.result||[])){try{evs.push(JSON.parse(s));}catch(_){}}
  }catch(_){}
  try{
    const old=(await dbGet(EVT_KEY))||{eventos:[]};
    evs=evs.concat(old.eventos||[]);
  }catch(_){}
  evs.sort((a,b)=>String(a.ts).localeCompare(String(b.ts)));
  return evs;
}

export default async function handler(req,res){
  res.setHeader('Access-Control-Allow-Origin','https://reparoeletroadm.com');
  res.setHeader('Cache-Control','no-cache');
  if(req.method==='OPTIONS')return res.status(200).end();

  const action=req.query.action||(req.body&&req.body.action)||'';

  // ── SYNC: importa todas as linhas da aba Criadas ────────────────────────
  if(action==='sync'){
    try{
      const resp=await fetch(SHEET_CSV,{redirect:'follow'});
      if(!resp.ok)return res.status(200).json({ok:false,error:`HTTP ${resp.status}`,novas:0});
      const text=await resp.text();
      const rows=parseCSV(text);
      // Linha 0 é header
      const dados=rows.slice(1).filter(r=>
        (String(r[0]||'').trim()||String(r[1]||'').trim())
      );

      const db=(await dbGet(KEY))||{fichas:[]};
      // Mapa de deduplicação por telefone (normalizado)
      const existentes=new Set(
        db.fichas.map(f=>String(f.telefone||'').replace(/\D/g,''))
      );

      // Parse do horário multi-formato → Date (UTC, assumindo entrada em BRT = UTC-3)
      function parseHorarioBR(s){
        const str=String(s||'').trim();
        // Formato BR: dd/mm/yy ou dd/mm/yyyy + hh:mm (com ou sem :ss)
        let m=str.match(/(\d{1,2})\/(\d{1,2})\/(\d{2,4})\s+(\d{1,2}):(\d{2})/);
        if(m){
          let ano=parseInt(m[3],10);if(ano<100)ano+=2000;
          return new Date(Date.UTC(ano,parseInt(m[2],10)-1,parseInt(m[1],10),parseInt(m[4],10)+3,parseInt(m[5],10)));
        }
        // Formato ISO: yyyy-mm-dd hh:mm ou yyyy-mm-ddThh:mm
        m=str.match(/(\d{4})-(\d{2})-(\d{2})[T\s](\d{1,2}):(\d{2})/);
        if(m){
          return new Date(Date.UTC(parseInt(m[1],10),parseInt(m[2],10)-1,parseInt(m[3],10),parseInt(m[4],10)+3,parseInt(m[5],10)));
        }
        // Formato gviz: Date(2026,4,15,23,3,0) — mês 0-based
        m=str.match(/Date\((\d{4}),(\d{1,2}),(\d{1,2}),(\d{1,2}),(\d{1,2})/);
        if(m){
          return new Date(Date.UTC(parseInt(m[1],10),parseInt(m[2],10),parseInt(m[3],10),parseInt(m[4],10)+3,parseInt(m[5],10)));
        }
        return null;
      }
      const DUAS_HORAS=2*60*60*1000;
      const agora=Date.now();

      let novas=0, aguardando=0, semHorario=0;
      const novasEvt=[];
      const debugHorarios=[];
      for(const row of dados){
        const tel  =String(row[0]||'').replace(/\D/g,'').trim();
        const nome =String(row[1]||'').trim();
        const equip=String(row[2]||'').trim();
        const def  =String(row[3]||'').trim();
        const end  =String(row[4]||'').trim();
        const hora =String(row[5]||'').trim(); // aba Criadas tem 6 colunas (sem Mensagem): horário é a col 5

        if(!tel&&!nome)continue;

        // REGRA: só importa se está na aba Criadas há MAIS de 2 horas
        const entradaEm=parseHorarioBR(hora);
        if(debugHorarios.length<3){
          debugHorarios.push({cru:hora, parseado:entradaEm?entradaEm.toISOString():null,
            idadeHoras:entradaEm?((agora-entradaEm.getTime())/3600000).toFixed(1):null,
            linhaCompleta:row.map(c=>String(c).substring(0,40))});
        }
        // FAIL-CLOSED: sem horário parseável → NÃO importa (aguarda), nunca importa às cegas
        if(!entradaEm){
          semHorario++;
          continue;
        }
        if((agora-entradaEm.getTime())<DUAS_HORAS){
          aguardando++;
          continue; // ainda não completou 2h na aba — aguarda próximo sync
        }

        // Deduplicação por telefone
        const telNorm=tel||'';
        if(existentes.has(telNorm))continue;
        existentes.add(telNorm);

        const novaFicha={
          id:      gerarId(tel,hora),
          telefone:tel, nome, equipamento:equip,
          defeito: def, endereco:end, horario:hora,
          waNum:   waNum(tel),
          status:  'lead',
          criadoEm:new Date().toISOString(),
          movidoEm:null,
        };
        db.fichas.unshift(novaFicha);
        novasEvt.push({tipo:'lead',sis:'adm',id:novaFicha.id,nome:novaFicha.nome});
        novas++;
      }

      if(novas>0)await dbSet(KEY,db);
      if(novasEvt.length)await logEventos(novasEvt);
      return res.status(200).json({ok:true,novas,aguardando2h:aguardando,semHorario,total:dados.length,naBase:db.fichas.length,header:(rows[0]||[]).map(c=>String(c).substring(0,30)),debugHorarios});
    }catch(e){
      return res.status(200).json({ok:false,error:e.message,novas:0});
    }
  }

  // ── BADGE: retorna contagem de leads novos (+ faz sync) ────────────────
  if(action==='badge'){
    // Badge apenas LÊ o Redis; sync fica com a página /prospeccao (2h).
    // Self-fetch removido: dobrava as invocations no Vercel.
    const db=(await dbGet(KEY))||{fichas:[]};
    const novas=(db.fichas||[]).filter(f=>f.status==='lead').length;
    return res.status(200).json({ok:true,novas});
  }

  // ── LOAD: retorna todas as prospecções ──────────────────────────────────
  if(action==='load'){
    // Carrega prospecção + ESPELHO das fichas em 'entrar_contato' (ADM e TV)
    const [db,fa,ft]=await Promise.all([dbGet(KEY),dbGet('fichas_adm'),dbGet('fichas_tv')]);
    const espelho=[];
    for(const [src,d] of [['adm',fa],['tv',ft]]){
      for(const f of (d?.fichas||[])){
        if(f.status==='entrar_contato') espelho.push({...f,origemSistema:src});
      }
    }
    return res.status(200).json({ok:true,fichas:(db?.fichas)||[],espelhoEntrar:espelho});
  }

  // ── MOVER: muda status (lead→retornar→cliente_loja) ───────────────────
  if(req.method==='POST'&&action==='mover'){
    const{id,status,dataRetorno,obsRetorno}=req.body||{};
    const db=(await dbGet(KEY))||{fichas:[]};
    const f=db.fichas.find(x=>x.id===id);
    if(!f)return res.status(404).json({ok:false,error:'Não encontrado'});
    const stAnt=f.status;
    f.status=status;f.movidoEm=new Date().toISOString();
    if(status==='retornar'){
      f.dataRetorno=dataRetorno||null;
      f.obsRetorno=obsRetorno||null;
      f.filaFinal=false; // reagendou → volta ao fluxo normal
    }
    await dbSet(KEY,db);
    if(status==='retornar'||status==='cliente_loja'){
      const tipoEvt=(status==='retornar'&&stAnt==='retornar')?'reagendar':status;
      if(tipoEvt!=='reagendar'||stAnt==='retornar'){
        await logEventos([{tipo:tipoEvt,de:stAnt||null,sis:f.origemSistema||'adm',id:idEvt(f),nome:f.nome}]);
      }
    }
    return res.status(200).json({ok:true});
  }

  // ── FIM-FILA: não conseguiu contato na data → final da fila + alerta ─────
  if(req.method==='POST'&&action==='fim-fila'){
    const{id}=req.body||{};
    const db=(await dbGet(KEY))||{fichas:[]};
    const f=db.fichas.find(x=>x.id===id);
    if(!f)return res.status(404).json({ok:false,error:'Não encontrado'});
    f.filaFinal=true;
    f.tentativas=(f.tentativas||0)+1;
    f.movidoEm=new Date().toISOString();
    await dbSet(KEY,db);
    await logEventos([{tipo:'fim_fila',de:(f.status==='cliente_loja'?'cliente_loja':'retornar'),sis:f.origemSistema||'adm',id:idEvt(f),nome:f.nome}]);
    return res.status(200).json({ok:true,tentativas:f.tentativas});
  }

  // ── ESPELHO-RETORNAR: ficha do espelho entra na cadência de Retornar ─────
  if(req.method==='POST'&&action==='espelho-retornar'){
    const{id,sistema,dataRetorno,obsRetorno}=req.body||{};
    const FKEY=sistema==='tv'?'fichas_tv':'fichas_adm';
    const fdb=(await dbGet(FKEY))||{fichas:[]};
    const orig=fdb.fichas.find(x=>x.id===id);
    if(!orig)return res.status(404).json({ok:false,error:'Ficha não encontrada'});

    // 1. Cria na prospecção com status retornar
    const db=(await dbGet(KEY))||{fichas:[]};
    const now=new Date().toISOString();
    db.fichas.unshift({
      id:'prosp_esp_'+Date.now().toString(36),
      telefone:orig.telefone||'', nome:orig.nome||'',
      equipamento:orig.equipamento||'', defeito:orig.defeito||'',
      endereco:orig.endereco||'', horario:orig.horario||'',
      waNum:orig.waNum||waNum(orig.telefone||''),
      status:'retornar', dataRetorno:dataRetorno||null, obsRetorno:obsRetorno||null, filaFinal:false,
      origemEspelho:true, origemSistema:sistema, origemFichaId:orig.id,
      criadoEm:now, movidoEm:now,
    });
    await dbSet(KEY,db);
    await logEventos([{tipo:'retornar',de:'entrar_contato',sis:sistema,id:orig.id,nome:orig.nome}]);

    // 2. Marca a origem — sai do espelho e das colunas de fichas (vive na prospecção)
    orig.status='prospeccao';
    orig.prospeccaoEm=now;
    await dbSet(FKEY,fdb);

    return res.status(200).json({ok:true});
  }

  // ── CADASTRAR-LOGISTICA ─────────────────────────────────────────────────
  if(req.method==='POST'&&action==='cadastrar-logistica'){
    const{id,sistema,tipoColeta,dataAgendada,faixaHorario,dados}=req.body||{};
    const db=(await dbGet(KEY))||{fichas:[]};
    const ficha=db.fichas.find(x=>x.id===id);
    if(!ficha)return res.status(404).json({ok:false,error:'Não encontrado'});
    // Dados conferidos/corrigidos no modal (principalmente endereço)
    if(dados&&typeof dados==='object'){
      if(dados.nome)ficha.nome=dados.nome;
      if(dados.telefone)ficha.telefone=String(dados.telefone).replace(/\D/g,'');
      if(dados.equipamento)ficha.equipamento=dados.equipamento;
      if(dados.defeito)ficha.defeito=dados.defeito;
      if(dados.endereco)ficha.endereco=dados.endereco;
    }

    const LOG_KEY=sistema==='tv'?'tv_logistica':'reparoeletro_logistica';
    const logDb=(await dbGet(LOG_KEY))||{fichas:[]};
    const phase=tipoColeta==='agendado'?'horario_marcado':'liberado_coleta';

    // Montar horarioColeta no formato que a logística usa (datetime-local)
    let horarioColeta=null;
    if(tipoColeta==='agendado'&&dataAgendada&&faixaHorario){
      const horaInicio=(faixaHorario.split(' - ')[0])||'08:00';
      horarioColeta=`${dataAgendada}T${horaInicio}`;
    }

    logDb.fichas.unshift({
      id:          'log_'+Date.now().toString(36),
      nome:        ficha.nome,
      telefone:    ficha.telefone,
      endereco:    ficha.endereco,
      equipamento: ficha.equipamento,
      defeito:     ficha.defeito,
      phase,
      dataAgendada:dataAgendada||null,
      faixaHorario:faixaHorario||null,
      horarioColeta,
      origem:      'prospeccao',
      origemTipo:  'ativa', // prospecção é sempre ação ativa
      criadoEm:    new Date().toISOString(),
      movedAt:     new Date().toISOString(),
    });
    await dbSet(LOG_KEY,logDb);

    // Marcar como cadastrado em logística
    const stAntLog=ficha.status;
    ficha.status='logistica';
    ficha.movidoEm=new Date().toISOString();
    ficha.logisticaEm=new Date().toISOString();
    ficha.logisticaTipo='ativa';
    ficha.logisticaSistema=sistema==='tv'?'tv':'adm';
    await logEventos([{tipo:'logistica',de:stAntLog||null,sis:sistema,id:idEvt(ficha),nome:ficha.nome}]);
    await dbSet(KEY,db);
    return res.status(200).json({ok:true});
  }

  // ── EXCLUIR ─────────────────────────────────────────────────────────────
  if(req.method==='POST'&&action==='excluir'){
    const{id}=req.body||{};
    const db=(await dbGet(KEY))||{fichas:[]};
    db.fichas=db.fichas.filter(x=>x.id!==id);
    await dbSet(KEY,db);
    return res.status(200).json({ok:true});
  }

  // ── CRIAR-MANUAL: cadastro manual de ficha (sempre conta como ATIVA) ─────
  if(req.method==='POST'&&action==='criar-manual'){
    const{nome,telefone,equipamento,defeito,endereco,destino,sistema,tipoColeta,dataAgendada,faixaHorario}=req.body||{};
    if(!nome||!String(nome).trim())return res.status(400).json({ok:false,error:'Nome é obrigatório'});
    const now=new Date().toISOString();
    const tel=String(telefone||'').replace(/\D/g,'');
    const db=(await dbGet(KEY))||{fichas:[]};

    const ficha={
      id:'prosp_man_'+Date.now().toString(36),
      telefone:tel, nome:String(nome).trim(),
      equipamento:String(equipamento||'').trim(),
      defeito:String(defeito||'').trim(),
      endereco:String(endereco||'').trim(),
      horario:'', waNum:waNum(tel),
      origemManual:true, logisticaTipo:'ativa',
      criadoEm:now, movidoEm:null,
    };

    if(destino==='logistica'){
      // Vai direto para a logística escolhida (mesma lógica do cadastrar-logistica)
      const LOG_KEY=sistema==='tv'?'tv_logistica':'reparoeletro_logistica';
      const logDb=(await dbGet(LOG_KEY))||{fichas:[]};
      const phase=tipoColeta==='agendado'?'horario_marcado':'liberado_coleta';
      let horarioColeta=null;
      if(tipoColeta==='agendado'&&dataAgendada&&faixaHorario){
        const horaInicio=(faixaHorario.split(' - ')[0])||'08:00';
        horarioColeta=`${dataAgendada}T${horaInicio}`;
      }
      logDb.fichas.unshift({
        id:'log_'+Date.now().toString(36),
        nome:ficha.nome, telefone:ficha.telefone, endereco:ficha.endereco,
        equipamento:ficha.equipamento, defeito:ficha.defeito,
        phase, dataAgendada:dataAgendada||null, faixaHorario:faixaHorario||null,
        horarioColeta, origem:'prospeccao_manual', origemTipo:'ativa',
        criadoEm:now, movedAt:now,
      });
      await dbSet(LOG_KEY,logDb);
      ficha.status='logistica';
      ficha.logisticaEm=now;
      ficha.logisticaSistema=sistema==='tv'?'tv':'adm';
    } else {
      // Cliente Loja
      ficha.status='cliente_loja';
      ficha.ativaManualEm=now;
      ficha.movidoEm=now;
    }

    db.fichas.unshift(ficha);
    await dbSet(KEY,db);
    const sisMan=destino==='logistica'?(sistema==='tv'?'tv':'adm'):'adm';
    await logEventos([
      {tipo:'manual',sis:sisMan,id:ficha.id,nome:ficha.nome},
      {tipo:destino==='logistica'?'logistica':'cliente_loja',de:'manual',sis:sisMan,id:ficha.id,nome:ficha.nome}
    ]);
    return res.status(200).json({ok:true,ficha});
  }

  // ── CONFIRMAR-LOJA: cliente confirmou que vai vir (inicia prazo de 7 dias) ─
  if(req.method==='POST'&&action==='confirmar-loja'){
    const{id}=req.body||{};
    const db=(await dbGet(KEY))||{fichas:[]};
    const f=db.fichas.find(x=>x.id===id);
    if(!f)return res.status(404).json({ok:false,error:'Não encontrado'});
    f.lojaConfirmouEm=new Date().toISOString();
    f.filaFinal=false; // progresso: sai do fim da fila
    await dbSet(KEY,db);
    await logEventos([{tipo:'confirmou_loja',de:'cliente_loja',sis:f.origemSistema||'adm',id:idEvt(f),nome:f.nome}]);
    return res.status(200).json({ok:true});
  }

  // ── MARCAR-FRENTELOJA: cliente loja virou cadastro no Frente de Loja ─────
  if(req.method==='POST'&&action==='marcar-frenteloja'){
    const{id}=req.body||{};
    const db=(await dbGet(KEY))||{fichas:[]};
    const f=db.fichas.find(x=>x.id===id);
    if(!f)return res.status(404).json({ok:false,error:'Não encontrado'});
    f.status='frenteloja';
    f.frentelojaEm=new Date().toISOString();
    f.logisticaTipo='ativa';
    f.movidoEm=f.frentelojaEm;
    await dbSet(KEY,db);
    await logEventos([{tipo:'frenteloja',de:'cliente_loja',sis:'adm',id:idEvt(f),nome:f.nome}]);
    return res.status(200).json({ok:true});
  }

  // ── ESPELHO-CLIENTE-LOJA: ficha do espelho vira Cliente Loja na prospecção ─
  if(req.method==='POST'&&action==='espelho-cliente-loja'){
    const{id,sistema}=req.body||{};
    const FKEY=sistema==='tv'?'fichas_tv':'fichas_adm';
    const fdb=(await dbGet(FKEY))||{fichas:[]};
    const orig=fdb.fichas.find(x=>x.id===id);
    if(!orig)return res.status(404).json({ok:false,error:'Ficha não encontrada'});

    const db=(await dbGet(KEY))||{fichas:[]};
    const now=new Date().toISOString();
    db.fichas.unshift({
      id:'prosp_esp_'+Date.now().toString(36),
      telefone:orig.telefone||'', nome:orig.nome||'',
      equipamento:orig.equipamento||'', defeito:orig.defeito||'',
      endereco:orig.endereco||'', horario:orig.horario||'',
      waNum:orig.waNum||waNum(orig.telefone||''),
      status:'cliente_loja', filaFinal:false,
      origemEspelho:true, origemSistema:sistema, origemFichaId:orig.id,
      criadoEm:now, movidoEm:now,
    });
    await dbSet(KEY,db);
    await logEventos([{tipo:'cliente_loja',de:'entrar_contato',sis:sistema,id:orig.id,nome:orig.nome}]);

    orig.status='prospeccao';
    orig.prospeccaoEm=now;
    await dbSet(FKEY,fdb);
    return res.status(200).json({ok:true});
  }

  // ── RELATORIO: contagens e conversões por período, separado ADM/TV ───────
  if(action==='relatorio'){
    const periodo=req.query.periodo||'hoje';
    let db_evt=(await dbGet(EVT_KEY))||{eventos:[]};

    // Backfill one-shot: reconstrói eventos dos timestamps já existentes
    if(!db_evt.backfillFeito){
      const [pr,fa,ft]=await Promise.all([dbGet(KEY),dbGet('fichas_adm'),dbGet('fichas_tv')]);
      const ev=db_evt.eventos||[];
      for(const f of (pr?.fichas||[])){
        const sisL=f.logisticaSistema||f.origemSistema||'adm';
        if(f.criadoEm&&!f.origemEspelho)ev.push({ts:f.criadoEm,tipo:'lead',sis:'adm',id:f.id,nome:f.nome,bf:1});
        if(f.criadoEm&&f.origemEspelho)ev.push({ts:f.criadoEm,tipo:f.status==='cliente_loja'?'cliente_loja':'retornar',sis:f.origemSistema||'adm',id:f.id,nome:f.nome,bf:1});
        if(f.logisticaEm)ev.push({ts:f.logisticaEm,tipo:'logistica',sis:sisL,id:f.id,nome:f.nome,bf:1});
        if(f.frentelojaEm)ev.push({ts:f.frentelojaEm,tipo:'frenteloja',sis:'adm',id:f.id,nome:f.nome,bf:1});
        if(f.ativaManualEm)ev.push({ts:f.ativaManualEm,tipo:'cliente_loja',sis:'adm',id:f.id,nome:f.nome,bf:1});
      }
      ev.sort((a,b)=>String(a.ts).localeCompare(String(b.ts)));
      db_evt={eventos:ev,backfillFeito:true};
      await dbSet(EVT_KEY,db_evt);
    }

    // Cortes de período (BRT)
    const agoraBRT=new Date(Date.now()-3*3600000);
    let corte;
    if(periodo==='hoje'){
      corte=new Date(Date.UTC(agoraBRT.getUTCFullYear(),agoraBRT.getUTCMonth(),agoraBRT.getUTCDate())+3*3600000);
    }else if(periodo==='mes'){
      corte=new Date(Date.UTC(agoraBRT.getUTCFullYear(),agoraBRT.getUTCMonth(),1)+3*3600000);
    }else{ // semana (desde domingo)
      const dom=new Date(Date.UTC(agoraBRT.getUTCFullYear(),agoraBRT.getUTCMonth(),agoraBRT.getUTCDate())+3*3600000);
      dom.setUTCDate(dom.getUTCDate()-agoraBRT.getUTCDay());
      corte=dom;
    }
    const corteISO=corte.toISOString();

    const TIPOS=['entrar_contato','lead','retornar','cliente_loja','frenteloja','logistica','fim_fila'];
    const out={adm:{},tv:{},total:{}};
    TIPOS.forEach(t=>{out.adm[t]=0;out.tv[t]=0;out.total[t]=0;});
    for(const e of (db_evt.eventos||[])){
      if(e.ts<corteISO)continue;
      if(!TIPOS.includes(e.tipo))continue;
      const s=e.sis==='tv'?'tv':'adm';
      out[s][e.tipo]++;out.total[e.tipo]++;
    }
    return res.status(200).json({ok:true,periodo,desde:corteISO,contagens:out});
  }

  // ── BACKFILL-HISTORICO: reconstrói eventos datados desde a criação ────────
  //    Fiel: usa apenas timestamps reais gravados nas fichas. Dedupe por (id,tipo).
  if(action==='backfill-historico'){
    const [pr,fa,ft,existentes]=await Promise.all([
      dbGet(KEY),dbGet('fichas_adm'),dbGet('fichas_tv'),lerEventos()
    ]);
    const ja=new Set(existentes.map(e=>`${e.id}|${e.tipo}`));
    const novos=[];
    function add(ts,tipo,sis,id,nome){
      if(!ts||!id)return;
      if(ja.has(`${id}|${tipo}`))return;
      ja.add(`${id}|${tipo}`);
      novos.push({ts,tipo,de:'hist',sis:sis==='tv'?'tv':'adm',id,nome:nome||null});
    }

    // 1. Fichas da prospecção
    for(const f of (pr?.fichas||[])){
      const id=f.origemFichaId||f.id;
      const sisBase=f.origemSistema||'adm';
      if(f.origemEspelho){
        add(f.criadoEm,'entrar_contato',sisBase,id,f.nome);
        // primeiro destino do espelho: retornar ou cliente_loja
        if(f.dataRetorno||f.status==='retornar')add(f.criadoEm,'retornar',sisBase,id,f.nome);
        else if(f.status==='cliente_loja'||f.frentelojaEm)add(f.criadoEm,'cliente_loja',sisBase,id,f.nome);
      }else if(f.origemManual){
        add(f.criadoEm,'manual','adm',id,f.nome);
      }else{
        add(f.criadoEm,'lead','adm',id,f.nome);
      }
      if(f.ativaManualEm)add(f.ativaManualEm,'cliente_loja','adm',id,f.nome);
      if(f.status==='retornar'&&!f.origemEspelho)add(f.movidoEm||f.criadoEm,'retornar',sisBase,id,f.nome);
      if(f.status==='cliente_loja'&&!f.ativaManualEm&&!f.origemEspelho)add(f.movidoEm||f.criadoEm,'cliente_loja',sisBase,id,f.nome);
      if(f.logisticaEm)add(f.logisticaEm,'logistica',f.logisticaSistema||'adm',id,f.nome);
      if(f.frentelojaEm)add(f.frentelojaEm,'frenteloja','adm',id,f.nome);
    }

    // 2. Fichas de fichas_adm/tv que foram trabalhadas pelo espelho
    for(const [sisF,db] of [['adm',fa],['tv',ft]]){
      for(const f of (db?.fichas||[])){
        // Estava em entrar_contato ao cadastrar logística? (regra 24h)
        if(f.logisticaEm&&f.contatoFeitoEm){
          const horas=(new Date(f.logisticaEm)-new Date(f.contatoFeitoEm))/3600000;
          if(horas>24){
            add(f.contatoFeitoEm,'entrar_contato',sisF,f.id,f.nome);
            add(f.logisticaEm,'logistica',sisF,f.id,f.nome);
          }
        }
        // Ficha atualmente na coluna espelhada
        if(f.status==='entrar_contato')add(f.contatoFeitoEm||f.criadoEm,'entrar_contato',sisF,f.id,f.nome);
      }
    }

    // Gravar na lista atômica
    novos.sort((a,b)=>String(a.ts).localeCompare(String(b.ts)));
    const gravados=await rpushLote(novos);
    const porTipo={};
    novos.forEach(e=>{porTipo[e.tipo]=(porTipo[e.tipo]||0)+1;});
    return res.status(200).json({ok:true,reconstruidos:gravados,porTipo,
      nota:'Marcos datados reais. Galhos intermediarios antigos (reagendar/fim de fila) nao possuem data gravada e ficam de fora.'});
  }

  // ── DEDUP-EVENTOS: remove duplicatas exatas (id+tipo+ts idênticos) ────────
  if(action==='dedup-eventos'){
    const todos=await lerEventos();
    const antes=todos.length;
    const vistos=new Set();
    const unicos=[];
    for(const e of todos){
      const chave=`${e.id}|${e.tipo}|${e.ts}`;
      if(vistos.has(chave))continue;
      vistos.add(chave);
      unicos.push(e);
    }
    if(unicos.length<antes){
      try{await fetch(`${U}/del/${EVT_LIST}`,{headers:{Authorization:`Bearer ${T}`}});}catch(_){}
      await rpushLote(unicos);
      await dbSet(EVT_KEY,{eventos:[],backfillFeito:true});
    }
    return res.status(200).json({ok:true,antes,depois:unicos.length,duplicatasRemovidas:antes-unicos.length});
  }

  // ── EVENTOS-DIAGNOSTICO: raio-x da base de eventos ────────────────────────
  if(action==='eventos-diagnostico'){
    const evs=await lerEventos();
    const db_evt={backfillFeito:true};
    const porDia={},porTipo={},semId=0,stats={backfill:0,aoVivoSemDe:0,completos:0};
    let _semId=0;
    for(const e of evs){
      const dia=String(e.ts||'').slice(0,10);
      porDia[dia]=(porDia[dia]||0)+1;
      porTipo[e.tipo]=(porTipo[e.tipo]||0)+1;
      if(!e.id)_semId++;
      if(e.bf)stats.backfill++;
      else if(e.de===undefined&&!['lead','entrar_contato','manual'].includes(e.tipo))stats.aoVivoSemDe++;
      else stats.completos++;
    }
    return res.status(200).json({ok:true,total:evs.length,semId:_semId,stats,porTipo,porDia,backfillFeito:!!db_evt.backfillFeito});
  }

  // ── EVENTOS-LIMPAR?modo=sujos|tudo — higieniza a base do relatório ────────
  if(action==='eventos-limpar'){
    const modo=req.query.modo||'sujos';
    const todos=await lerEventos();
    const antes=todos.length;
    let bons=[];
    if(modo!=='tudo'){
      bons=todos.filter(e=>{
        if(e.bf)return false;
        if(!e.id)return false;
        if((e.de===undefined||e.de===null)&&!['lead','entrar_contato','manual'].includes(e.tipo))return false;
        return true;
      });
    }
    // Consolidar tudo na LISTA atômica: DEL + RPUSH dos bons; zerar chave antiga
    try{await fetch(`${U}/del/${EVT_LIST}`,{headers:{Authorization:`Bearer ${T}`}});}catch(_){}
    if(bons.length)await rpushLote(bons);
    await dbSet(EVT_KEY,{eventos:[],backfillFeito:true});
    return res.status(200).json({ok:true,modo,antes,depois:bons.length,removidos:antes-bons.length});
  }

  // ── RELATORIO-ARVORE v2: 4 matrizes (entradas na etapa) + desmembramento
  //    recursivo + conversão final (logística/frente de loja) vs matriz ─────
  if(action==='relatorio-arvore'){
    const periodo=req.query.periodo||'hoje';
    const db_evt={eventos:await lerEventos()};

    const agoraBRT=new Date(Date.now()-3*3600000);
    let corte;
    if(periodo==='hoje'){
      corte=new Date(Date.UTC(agoraBRT.getUTCFullYear(),agoraBRT.getUTCMonth(),agoraBRT.getUTCDate())+3*3600000);
    }else if(periodo==='mes'){
      corte=new Date(Date.UTC(agoraBRT.getUTCFullYear(),agoraBRT.getUTCMonth(),1)+3*3600000);
    }else{
      const dom=new Date(Date.UTC(agoraBRT.getUTCFullYear(),agoraBRT.getUTCMonth(),agoraBRT.getUTCDate())+3*3600000);
      dom.setUTCDate(dom.getUTCDate()-agoraBRT.getUTCDay());
      corte=dom;
    }
    const corteISO=corte.toISOString();

    // Eventos por ficha, ordenados
    const porFicha={};
    for(const e of (db_evt.eventos||[])){
      if(!e.id)continue;
      if(!porFicha[e.id])porFicha[e.id]=[];
      porFicha[e.id].push(e);
    }
    Object.values(porFicha).forEach(l=>l.sort((a,b)=>String(a.ts).localeCompare(String(b.ts))));

    const MATRIZES=['entrar_contato','lead','retornar','cliente_loja'];
    function novoNo(){return {count:0,filhos:{}};}
    function novaMatriz(){return {count:0,filhos:{},convLog:0,convFl:0};}
    const out={adm:{},tv:{}};
    MATRIZES.forEach(m=>{out.adm[m]=novaMatriz();out.tv[m]=novaMatriz();});

    for(const id of Object.keys(porFicha)){
      const evs=porFicha[id];
      const sis=evs.some(e=>e.sis==='tv')?'tv':'adm';
      for(const M of MATRIZES){
        // primeira ENTRADA na etapa M dentro do período
        const iM=evs.findIndex(e=>e.tipo===M&&e.ts>=corteISO);
        if(iM<0)continue;
        const matriz=out[sis][M];
        matriz.count++;
        // sufixo da jornada a partir de M (sem repetir consecutivos, exceto reagendar)
        const suf=[];
        for(let k=iM+1;k<evs.length;k++){
          const t=evs[k].tipo;
          if(t===M&&suf.length===0)continue;
          if(suf.length&&t===suf[suf.length-1]&&t!=='reagendar')continue;
          suf.push(t);
        }
        // inserir na árvore da matriz
        let no=matriz;
        for(let k=0;k<suf.length&&k<5;k++){
          const t=suf[k];
          if(!no.filhos[t])no.filhos[t]=novoNo();
          no=no.filhos[t];
          no.count++;
        }
        // conversões finais vs matriz
        if(suf.includes('logistica'))matriz.convLog++;
        if(suf.includes('frenteloja'))matriz.convFl++;
      }
    }

    // ATIVIDADE em tempo real: eventos ocorridos no período (independente da jornada)
    const atividade={adm:{},tv:{}};
    for(const e of (db_evt.eventos||[])){
      if(e.ts<corteISO)continue;
      const s=e.sis==='tv'?'tv':'adm';
      atividade[s][e.tipo]=(atividade[s][e.tipo]||0)+1;
    }

    return res.status(200).json({ok:true,periodo,desde:corteISO,matrizes:out,atividade});
  }

  // ── STATS-PA: contagem semanal de fichas → logística por Passiva/Ativa ──
  if(action==='stats-pa'){
    // Início da semana (domingo 00:00 BRT = 03:00 UTC)
    const nowBRT=new Date(Date.now()-3*3600000);
    const iniSemana=new Date(Date.UTC(nowBRT.getUTCFullYear(),nowBRT.getUTCMonth(),nowBRT.getUTCDate()-nowBRT.getUTCDay(),3,0,0));
    const [fa,ft,pr]=await Promise.all([dbGet('fichas_adm'),dbGet('fichas_tv'),dbGet(KEY)]);
    // Breakdown por sistema: adm/tv × passiva/ativa
    const bk={adm:{passiva:0,ativa:0},tv:{passiva:0,ativa:0}};
    const conta=(db,fallback,extrator,sisFixo,sisExtrator)=>{
      for(const f of (db?.fichas||[])){
        const ts=extrator?extrator(f):f.logisticaEm;
        if(!ts)continue;
        if(new Date(ts)<iniSemana)continue;
        const t=(f.logisticaTipo||fallback)==='ativa'?'ativa':'passiva';
        const s=sisFixo||(sisExtrator?sisExtrator(f):'adm');
        bk[s==='tv'?'tv':'adm'][t]++;
      }
    };
    conta(fa,'passiva',null,'adm');
    conta(ft,'passiva',null,'tv');
    // Prospecção: logística (sistema gravado) | frente de loja (ADM) | manual cliente loja (ADM)
    conta(pr,'ativa',
      f=>f.logisticaEm||f.frentelojaEm||f.ativaManualEm,
      null,
      f=>f.logisticaEm?(f.logisticaSistema||'adm'):'adm');
    const passiva=bk.adm.passiva+bk.tv.passiva;
    const ativa=bk.adm.ativa+bk.tv.ativa;
    return res.status(200).json({ok:true,passiva,ativa,adm:bk.adm,tv:bk.tv});
  }

  // ── LIMPAR-TUDO: zera toda a prospecção (para reimportar corretamente) ──
  if(action==='limpar-tudo'){
    await dbSet(KEY,{fichas:[]});
    return res.status(200).json({ok:true,msg:'Prospecção zerada. Reimporte com action=sync após corrigir o gid.'});
  }

  return res.status(404).json({ok:false,error:'Ação não encontrada'});
}
