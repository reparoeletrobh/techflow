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

        db.fichas.unshift({
          id:      gerarId(tel,hora),
          telefone:tel, nome, equipamento:equip,
          defeito: def, endereco:end, horario:hora,
          waNum:   waNum(tel),
          status:  'lead',
          criadoEm:new Date().toISOString(),
          movidoEm:null,
        });
        novas++;
      }

      if(novas>0)await dbSet(KEY,db);
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
    const{id,status,dataRetorno}=req.body||{};
    const db=(await dbGet(KEY))||{fichas:[]};
    const f=db.fichas.find(x=>x.id===id);
    if(!f)return res.status(404).json({ok:false,error:'Não encontrado'});
    f.status=status;f.movidoEm=new Date().toISOString();
    if(status==='retornar'){
      f.dataRetorno=dataRetorno||null;
      f.filaFinal=false; // reagendou → volta ao fluxo normal
    }
    await dbSet(KEY,db);
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
    return res.status(200).json({ok:true,tentativas:f.tentativas});
  }

  // ── ESPELHO-RETORNAR: ficha do espelho entra na cadência de Retornar ─────
  if(req.method==='POST'&&action==='espelho-retornar'){
    const{id,sistema,dataRetorno}=req.body||{};
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
      status:'retornar', dataRetorno:dataRetorno||null, filaFinal:false,
      origemEspelho:true, origemSistema:sistema,
      criadoEm:now, movidoEm:now,
    });
    await dbSet(KEY,db);

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
    ficha.status='logistica';
    ficha.movidoEm=new Date().toISOString();
    ficha.logisticaEm=new Date().toISOString();
    ficha.logisticaTipo='ativa';
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
    } else {
      // Cliente Loja
      ficha.status='cliente_loja';
      ficha.ativaManualEm=now;
      ficha.movidoEm=now;
    }

    db.fichas.unshift(ficha);
    await dbSet(KEY,db);
    return res.status(200).json({ok:true,ficha});
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
      origemEspelho:true, origemSistema:sistema,
      criadoEm:now, movidoEm:now,
    });
    await dbSet(KEY,db);

    orig.status='prospeccao';
    orig.prospeccaoEm=now;
    await dbSet(FKEY,fdb);
    return res.status(200).json({ok:true});
  }

  // ── STATS-PA: contagem semanal de fichas → logística por Passiva/Ativa ──
  if(action==='stats-pa'){
    // Início da semana (domingo 00:00 BRT = 03:00 UTC)
    const nowBRT=new Date(Date.now()-3*3600000);
    const iniSemana=new Date(Date.UTC(nowBRT.getUTCFullYear(),nowBRT.getUTCMonth(),nowBRT.getUTCDate()-nowBRT.getUTCDay(),3,0,0));
    const [fa,ft,pr]=await Promise.all([dbGet('fichas_adm'),dbGet('fichas_tv'),dbGet(KEY)]);
    let passiva=0,ativa=0;
    const conta=(db,fallback,extrator)=>{
      for(const f of (db?.fichas||[])){
        const ts=extrator?extrator(f):f.logisticaEm;
        if(!ts)continue;
        if(new Date(ts)<iniSemana)continue;
        const t=f.logisticaTipo||fallback; // histórico sem campo: fichas→passiva, prospecção→ativa
        if(t==='ativa')ativa++;else passiva++;
      }
    };
    conta(fa,'passiva');conta(ft,'passiva');
    // Prospecção: logística OU frente de loja OU cadastro manual cliente loja — tudo ativa
    conta(pr,'ativa',f=>f.logisticaEm||f.frentelojaEm||f.ativaManualEm);
    return res.status(200).json({ok:true,passiva,ativa});
  }

  // ── LIMPAR-TUDO: zera toda a prospecção (para reimportar corretamente) ──
  if(action==='limpar-tudo'){
    await dbSet(KEY,{fichas:[]});
    return res.status(200).json({ok:true,msg:'Prospecção zerada. Reimporte com action=sync após corrigir o gid.'});
  }

  return res.status(404).json({ok:false,error:'Ação não encontrada'});
}
