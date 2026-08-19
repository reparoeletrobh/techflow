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
  // 🔐 TF-AUTH (Fase 1): chave obrigatória em toda chamada
  const _tfk = (req.query && req.query.k) || req.headers['x-tf-key'] || '';
  if (_tfk !== ((process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim())) {
    return res.status(401).json({ ok: false, error: 'não autorizado' });
  }

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
      // Tombstone: telefones excluídos manualmente não voltam da planilha (60 dias)
      try{
        const exc=(await dbGet('prospeccao_excluidos'))||{tels:{}};
        const corteExc=Date.now()-60*86400000;
        for(const t of Object.keys(exc.tels||{})){
          if(new Date(exc.tels[t]).getTime()<=corteExc)continue;
          existentes.add(t);
          const d=String(t).replace(/\D/g,'').slice(-8);
          if(d.length>=8)existentes.add(d);
        }
      }catch(_){}

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
        const telD8=String(tel||'').replace(/\D/g,'').slice(-8);
        if(existentes.has(telNorm)||(telD8.length>=8&&existentes.has(telD8)))continue;
        existentes.add(telNorm);
        if(telD8.length>=8)existentes.add(telD8);

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
  // ── 🔧 reparar-motivo: repõe o texto dos conflitos que aparecem vazios ──
  // A régua de recuperação gravava o texto num campo com outro nome; ele existe
  // no banco, mas a tela lê 'motivoConflito' e o card saía sem nada, deixando a
  // equipe sem saber por que aquele cliente virou conflito.
  if(action==='reparar-motivo'){
    const aplicar=String(req.query.aplicar||'')==='1';
    const reparados=[], semNada=[];
    for(const chave of ['prospeccao_adm','prospeccao_tv']){
      const db=(await dbGet(chave))||{fichas:[]};
      let mexeu=0;
      for(const f of (db.fichas||[])){
        if(String(f.status||'')!=='conflitos_bot') continue;
        if(String(f.motivoConflito||'').trim()) continue;     // já tem texto
        // o texto pode estar em qualquer um destes campos, conforme quem criou
        const achado=f.motivo||f.descricao||f.observacao||f.obs||null;
        const linha=String(f.nome||'?').slice(0,22)+' '+
          String(f.telefone||'').slice(-4)+
          (f.origem?' | origem '+f.origem:'');
        if(achado){
          if(aplicar){ f.motivoConflito=String(achado).slice(0,300); mexeu++; }
          reparados.push(linha+' → "'+String(achado).slice(0,70)+'"');
        } else {
          semNada.push(linha+' | nenhum texto guardado em campo nenhum');
        }
      }
      if(aplicar&&mexeu) await dbSet(chave,db);
    }
    return res.status(200).json({ok:true,
      modo:aplicar?'aplicado':'prévia',
      reparados:reparados.length, semTextoNenhum:semNada.length,
      L:reparados, SEM_TEXTO:semNada,
      dica:aplicar?null:'para aplicar: &aplicar=1'});
  }

  // ── 🔬 auditoria-leads: por que um lead da planilha não virou ficha ──
  // A importação recusa em silêncio em três situações — horário ilegível,
  // menos de duas horas na aba, telefone já conhecido — e nenhuma delas
  // aparece na tela. Um lead que não entra não é abordado por ninguém.
  if(action==='auditoria-leads'){
    const SHEET=(process.env.SHEET_LEADS_ID||process.env.GOOGLE_SHEET_ID||'').trim();
    const CHAVE=(process.env.GOOGLE_API_KEY||'').trim();
    const aba=String(req.query.aba||'Criadas');
    let linhas=[];
    try{
      const r=await fetch('https://sheets.googleapis.com/v4/spreadsheets/'+SHEET+
        '/values/'+encodeURIComponent(aba)+'?key='+CHAVE).then(x=>x.json());
      linhas=(r.values||[]).slice(1);
    }catch(e){
      return res.status(200).json({ok:false,error:'não consegui ler a planilha: '+e.message});
    }
    if(!linhas.length) return res.status(200).json({ok:false,
      error:'planilha vazia ou sem acesso', dica:'confira SHEET_LEADS_ID e GOOGLE_API_KEY'});

    const parseBR=(s)=>{
      const m=String(s||'').match(/(\d{2})\/(\d{2})\/(\d{4})[ ,]+(\d{1,2}):(\d{2})/);
      if(!m) return null;
      return new Date(Date.UTC(+m[3],+m[2]-1,+m[1],+m[4]+3,+m[5]));
    };
    const d8a=t=>String(t||'').replace(/\D/g,'').slice(-8);
    const desde=String(req.query.desde||'');
    const iniA=desde?new Date(desde+'T00:00:00-03:00').getTime():0;

    // tudo que o sistema conhece, para saber se o lead entrou de alguma forma
    const [pA,pT,fA,fT,exc]=await Promise.all([
      dbGet('prospeccao_adm'),dbGet('prospeccao_tv'),
      dbGet('fichas_adm'),dbGet('fichas_tv'),dbGet('prospeccao_excluidos'),
    ]);
    const noSistema={};
    for(const [db,onde] of [[pA,'prospecção ADM'],[pT,'prospecção TV'],
                            [fA,'fichas ADM'],[fT,'fichas TV']]){
      for(const f of (((db||{}).fichas)||[])){
        const d=d8a(f.telefone); if(!d) continue;
        (noSistema[d]=noSistema[d]||[]).push(onde+'/'+String(f.status||'?'));
      }
    }
    const excluidos=new Set(Object.keys(((exc||{}).tels)||{}));

    const DUAS=2*3600000, agora=Date.now();
    const R={entraram:[],aguardando2h:[],semHorario:[],jaExistia:[],excluido:[]};
    for(const row of linhas){
      const tel=String(row[0]||'').replace(/\D/g,'').trim();
      const nome=String(row[1]||'').trim();
      const equip=String(row[2]||'').trim();
      const hora=String(row[5]||'').trim();
      if(!tel&&!nome) continue;
      const dt=parseBR(hora);
      if(desde&&dt&&dt.getTime()<iniA) continue;
      if(desde&&!dt) continue;
      const d=d8a(tel);
      const linha=String(nome||'?').slice(0,22).padEnd(22)+' '+d.slice(-4)+
        ' | '+String(equip).slice(0,20).padEnd(20)+' | '+String(hora).slice(0,16);
      if(!dt){ R.semHorario.push(linha+' | 🚨 horário ilegível — NUNCA será importado'); continue; }
      if((agora-dt.getTime())<DUAS){ R.aguardando2h.push(linha+' | aguarda completar 2h'); continue; }
      if(excluidos.has(d)){ R.excluido.push(linha+' | foi excluído da fila à mão'); continue; }
      if(noSistema[d]){ R.entraram.push(linha+' | '+[...new Set(noSistema[d])].join(' · ')); continue; }
      R.jaExistia.push(linha+' | 🚨 NÃO está em lugar nenhum do sistema');
    }
    return res.status(200).json({ok:R.semHorario.length===0&&R.jaExistia.length===0,
      linhasNaPlanilha:linhas.length,
      periodo:desde?('a partir de '+desde):'planilha inteira',
      RESUMO:{entraramNoSistema:R.entraram.length,
        sumiram:R.jaExistia.length,
        horarioIlegivel:R.semHorario.length,
        aguardando2h:R.aguardando2h.length,
        excluidosAMao:R.excluido.length},
      VEREDITO:(R.jaExistia.length||R.semHorario.length)
        ?'🚨 '+R.jaExistia.length+' sumiram e '+R.semHorario.length+
         ' têm horário ilegível e nunca serão importados'
        :'✅ todos os leads da planilha estão no sistema ou aguardando',
      SUMIRAM:R.jaExistia.slice(0,80),
      HORARIO_ILEGIVEL:R.semHorario.slice(0,40),
      AGUARDANDO_2H:R.aguardando2h.slice(0,30),
      EXCLUIDOS_A_MAO:R.excluido.slice(0,30),
      ENTRARAM:R.entraram.slice(0,40)});
  }

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
  // ── 🏅 CONVERTIDOS: leads que viraram cadastro de logística ──
  if(action==='convertidos'){
    // 📅 por padrão conta a SEMANA COMERCIAL (segunda a domingo)
    const bras=new Date(Date.now()-3*3600*1000);
    const diaSem=bras.getUTCDay();
    const desdeSegunda=(diaSem===0)?7:diaSem;            // dias já corridos desta semana
    const dias=req.query.dias?Math.min(60,Math.max(1,parseInt(req.query.dias,10))):desdeSegunda;
    let total=0; const itens=[];
    for(let i=0;i<dias;i++){
      const d=new Date(Date.now()-3*3600000-i*86400000).toISOString().slice(0,10);
      const r=await dbGet('prosp_convertidos_'+d);
      if(!r)continue;
      total+=r.total||0;
      for(const x of (r.itens||[]))itens.push({...x,dia:d});
    }
    const hojeD=new Date(Date.now()-3*3600000).toISOString().slice(0,10);
    const rHoje=await dbGet('prosp_convertidos_'+hojeD);
    const seg=new Date(bras); seg.setUTCDate(bras.getUTCDate()-((diaSem===0)?6:(diaSem-1)));
    return res.status(200).json({ok:true,periodoDias:dias,
      semanaComecaEm:seg.toISOString().slice(0,10),
      hoje:(rHoje&&rHoje.total)||0,total,
      lista:itens.slice(0,60).map(x=>x.dia+' | '+String(x.nome||'?').slice(0,20)+' '+x.telefone+
        ' | '+String(x.equipamento||'').slice(0,20)+' | '+(x.sistema||'').toUpperCase()+' · '+(x.tipoColeta||''))});
  }

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
      observacao: (req.body.dados&&req.body.dados.observacao)?String(req.body.dados.observacao).trim():'',
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
    // 🏅 LEAD CONVERTIDO: cadastro de logística feito a partir da coluna LEAD conta ponto
    try {
      // ⚠️ ficha.status já virou 'logistica' algumas linhas acima — quem guarda a
      // coluna de ORIGEM é stAntLog. Conferir ficha.status aqui nunca dava 'lead'.
      if (String(stAntLog || '') === 'lead') {
        const dia = new Date(Date.now() - 3 * 3600000).toISOString().slice(0, 10);
        const kC = 'prosp_convertidos_' + dia;
        const reg = (await dbGet(kC)) || { total: 0, itens: [] };
        if (!reg.itens.some(x => x.id === ficha.id)) {
          reg.total++;
          reg.itens.push({ id: ficha.id, nome: ficha.nome,
            telefone: String(ficha.telefone || '').slice(-4),
            equipamento: ficha.equipamento || '', sistema,
            tipoColeta, em: new Date().toISOString() });
          await dbSet(kC, reg);
        }
        ficha.leadConvertidoEm = new Date().toISOString();
      }
    } catch (e) {}
    return res.status(200).json({ok:true});
  }

  // ── EXCLUIR ─────────────────────────────────────────────────────────────
  // ── 🧹 limpa excluídas que reapareceram (corrida do sync regravando snapshot velho) ──
  if(action==='limpar-excluidas-reaparecidas'){
    const [db,exc]=await Promise.all([dbGet(KEY),dbGet('prospeccao_excluidos')]);
    const tels=new Set();
    for(const t of Object.keys(((exc||{}).tels)||{})){
      const d=String(t).replace(/\D/g,'').slice(-8);
      if(d.length>=8)tels.add(d);
    }
    const antes=(((db||{}).fichas)||[]).length;
    const removidas=[];
    const restantes=(((db||{}).fichas)||[]).filter(f=>{
      const d=String(f.telefone||'').replace(/\D/g,'').slice(-8);
      if(d.length>=8&&tels.has(d)){removidas.push({nome:f.nome,telefone:f.telefone,status:f.status});return false;}
      return true;
    });
    // 🚫 DESATIVADO: comparar por telefone marcava ficha VIVA (cliente recorrente / duplicada
    // do mesmo numero) para exclusao — 20 das 31 estavam em logistica, com equipamento na loja.
    if(String(req.query.aplicar||'')==='1'){
      return res.status(400).json({ok:false,error:'execução desativada — a comparação por telefone removia fichas em atendimento'});
    }
    return res.status(200).json({ok:true,reapareceram:removidas.length,lista:removidas.slice(0,40),
      dica:'para remover: mesmo link com &aplicar=1'});
  }

  // ── 🔎 RASTREAR: onde este telefone aparece em toda a operação (investigação) ──
  if(action==='rastrear'){
    const alvo=String(req.query.tel||'').replace(/\D/g,'');
    if(alvo.length<4)return res.status(400).json({ok:false,error:'informe ?tel= com pelo menos 4 dígitos finais'});
    // casa pelo FINAL do número: 4 dígitos já servem para investigar
    const bate=t=>String(t||'').replace(/\D/g,'').endsWith(alvo);
    const [pros,fA,fT,lgA,lgT,ppA,exc,ppT]=await Promise.all([
      dbGet(KEY),dbGet('fichas_adm'),dbGet('fichas_tv'),
      dbGet('reparoeletro_logistica'),dbGet('tv_logistica'),
      dbGet('reparoeletro_pipe'),dbGet('prospeccao_excluidos'),dbGet('tv_pipe')]);
    const achados=[];
    const varre=(nome,arr,campos)=>{
      for(const x of (arr||[])){
        if(!bate(x.telefone))continue;
        achados.push(Object.assign({onde:nome,id:x.id},campos(x)));
      }
    };
    varre('prospecção',((pros||{}).fichas),x=>({nome:x.nome,equipamento:x.equipamento,status:x.status,criadoEm:x.criadoEm,sheetRow:x.sheetRow}));
    varre('fichas ADM',((fA||{}).fichas),x=>({nome:x.nome,equipamento:x.equipamento,status:x.status,criadoEm:x.criadoEm,sheetRow:x.sheetRow}));
    varre('fichas TV',((fT||{}).fichas),x=>({nome:x.nome,equipamento:x.equipamento,status:x.status,criadoEm:x.criadoEm,sheetRow:x.sheetRow}));
    varre('logística ADM',((lgA||{}).fichas),x=>({nome:x.nome,equipamento:x.equipamento,fase:x.phase,criadoEm:x.criadoEm}));
    varre('logística TV',((lgT||{}).fichas),x=>({nome:x.nome,equipamento:x.equipamento,fase:x.phase,criadoEm:x.criadoEm}));
    for(const c of (((ppA||{}).cards)||[])){
      if(!bate(c.telefone))continue;
      achados.push({onde:'pipe ADM',id:c.id,nome:c.nomeContato,equipamento:c.equipamento,fase:c.phaseId||c.phase,criadoEm:c.criadoEm||c.movedAt});
    }
    const tomb=Object.keys(((exc||{}).tels)||{}).filter(t=>String(t).replace(/\D/g,'').endsWith(alvo))
      .map(t=>({chave:t,excluidoEm:exc.tels[t]}));
    achados.sort((a,b)=>String(a.criadoEm||'').localeCompare(String(b.criadoEm||'')));
    return res.status(200).json({ok:true,telefone:alvo,ocorrencias:achados.length,
      registrosDeExclusao:tomb,linhaDoTempo:achados});
  }

  if(req.method==='POST'&&action==='excluir'){
    const{id}=req.body||{};
    const db=(await dbGet(KEY))||{fichas:[]};
    let fx=db.fichas.find(x=>x.id===id);

    // 🪞 A coluna Entrar em Contato é um ESPELHO de fichas_adm e fichas_tv:
    // a ficha não vive aqui. Excluir apenas nesta base removia o reflexo, e no
    // carregamento seguinte ele reaparecia — a ficha original nunca havia saído.
    // Agora a exclusão alcança a ficha de verdade, onde quer que ela esteja.
    let ondeEstava=null;
    if(!fx){
      for(const chave of ['fichas_adm','fichas_tv']){
        const fdb=(await dbGet(chave))||{fichas:[]};
        const alvo=(fdb.fichas||[]).find(x=>String(x.id)===String(id));
        if(!alvo) continue;
        fx=alvo; ondeEstava=chave;
        alvo.status='prospeccao';
        alvo.excluidoDaFilaEm=new Date().toISOString();
        alvo.excluidoDaFilaPor='exclusão manual na prospecção';
        await dbSet(chave,fdb);
        // confirma que persistiu: sem isso a ficha volta no próximo carregamento
        const conf=await dbGet(chave);
        const ainda=(((conf||{}).fichas)||[])
          .find(x=>String(x.id)===String(id)&&String(x.status||'')==='entrar_contato');
        if(ainda) return res.status(200).json({ok:false,
          error:'a exclusão não persistiu na ficha original — tente de novo'});
        break;
      }
    } else {
      // existe na prospecção, mas pode ter gêmea na base de fichas
      const d8e=String(fx.telefone||'').replace(/\D/g,'').slice(-8);
      if(d8e.length>=8){
        for(const chave of ['fichas_adm','fichas_tv']){
          const fdb=(await dbGet(chave))||{fichas:[]};
          let mexeu=false;
          for(const x of (fdb.fichas||[])){
            if(String(x.status||'')!=='entrar_contato') continue;
            if(String(x.telefone||'').replace(/\D/g,'').slice(-8)!==d8e) continue;
            x.status='prospeccao';
            x.excluidoDaFilaEm=new Date().toISOString();
            x.excluidoDaFilaPor='exclusão manual na prospecção';
            mexeu=true;
          }
          if(mexeu) await dbSet(chave,fdb);
        }
      }
    }
    // 🔒 mesmo sem achar a ficha, o telefone informado precisa entrar na lista
    // de excluídos: sem esse registro nada impede o retorno pela régua ou pela
    // remarcação, e a ficha reaparece horas depois como se nunca tivesse saído
    if(!fx){
      const telInformado=String((req.body||{}).telefone||'').replace(/\D/g,'');
      if(telInformado.length>=8){
        try{
          const excB=(await dbGet('prospeccao_excluidos'))||{tels:{}};
          if(!excB.tels)excB.tels={};
          excB.tels[telInformado.slice(-8)]=new Date().toISOString();
          await dbSet('prospeccao_excluidos',excB);
        }catch(_){}
        return res.status(200).json({ok:true,
          observacao:'a ficha não foi encontrada, mas o telefone entrou na lista de '+
            'excluídos e não voltará para a fila'});
      }
      return res.status(404).json({ok:false,error:'ficha não encontrada'});
    }
    // grava o tombstone ANTES de remover (se o sync atropelar, a trava já existe)
    if(fx&&fx.telefone){
      try{
        const excA=(await dbGet('prospeccao_excluidos'))||{tels:{}};
        if(!excA.tels)excA.tels={};
        const dA=String(fx.telefone).replace(/\D/g,'').slice(-8);
        if(dA.length>=8)excA.tels[dA]=new Date().toISOString();
        await dbSet('prospeccao_excluidos',excA);
        // confirma: sem esse registro a proteção contra o retorno não existe
        const confE=await dbGet('prospeccao_excluidos');
        if(dA.length>=8&&!(((confE||{}).tels)||{})[dA]){
          await dbSet('prospeccao_excluidos',excA);   // uma segunda tentativa
        }
      }catch(_){}
    }
    const dbF=(await dbGet(KEY))||db;   // relê: o sync pode ter gravado no meio
    dbF.fichas=(dbF.fichas||[]).filter(x=>x.id!==id);
    await dbSet(KEY,dbF);
    // Tombstone: excluída não volta pela planilha
    if(fx&&fx.telefone){
      try{
        const exc=(await dbGet('prospeccao_excluidos'))||{tels:{}};
        if(!exc.tels)exc.tels={};
        const d8x=String(fx.telefone||'').replace(/\D/g,'').slice(-8);
        if(d8x.length>=8)exc.tels[d8x]=new Date().toISOString();
        exc.tels[String(fx.telefone)]=new Date().toISOString();   // mantém o formato antigo também
        // Poda: manter só os últimos 60 dias / máx 5000
        const corteT=Date.now()-60*86400000;
        const keys=Object.keys(exc.tels);
        if(keys.length>5000){for(const k of keys){if(new Date(exc.tels[k]).getTime()<corteT)delete exc.tels[k];}}
        await dbSet('prospeccao_excluidos',exc);
      }catch(_){}
    }
    return res.status(200).json({ok:true});
  }

  // ── CRIAR-MANUAL: cadastro manual de ficha (sempre conta como ATIVA) ─────
  if(req.method==='POST'&&action==='criar-manual'){
    const{nome,telefone,equipamento,defeito,endereco,destino,sistema,tipoColeta,dataAgendada,faixaHorario,obs}=req.body||{};
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
      obs:String(obs||'').trim(),
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
  // ── CONFLITOS BOT: criação (pelo bot) e resolução (pela equipe) ──
  async function logConflito(tipo,dados){
    const lg=(await dbGet('prospeccao_conflitos_log'))||{movs:[]};
    lg.movs.unshift({ts:new Date().toISOString(),tipo:tipo,
      nome:String((dados||{}).nome||'').slice(0,40),
      telefone:String((dados||{}).telefone||'').replace(/\D/g,'').slice(-8),
      motivo:String((dados||{}).motivo||'').slice(0,120)});
    lg.movs=lg.movs.slice(0,500);
    await dbSet('prospeccao_conflitos_log',lg);
  }

  if(req.method==='POST'&&action==='criar-conflito'){
    const {nome,telefone,equipamento,motivo}=req.body||{};
    if(!telefone)return res.status(400).json({ok:false,error:'telefone obrigatório'});
    const db=(await dbGet(KEY))||{fichas:[]};
    if(!Array.isArray(db.fichas))db.fichas=[];
    const d8c=String(telefone).replace(/\D/g,'').slice(-8);
    // dedupe: conflito aberto do mesmo tel
    const jaTem=(db.fichas||[]).some(f=>f.status==='conflitos_bot'&&String(f.telefone||'').replace(/\D/g,'').slice(-8)===d8c);
    if(jaTem)return res.status(200).json({ok:true,criado:false,dedupe:true,msg:'conflito já aberto para este telefone'});
    const novoId='conf_'+Date.now().toString(36);
    const {tipo,temFoto}=req.body||{};
    db.fichas.unshift({
      id:novoId,
      nome:String(nome||'Cliente WhatsApp'),telefone:String(telefone).replace(/\D/g,''),
      equipamento:String(equipamento||''),defeito:'',endereco:'',
      status:'conflitos_bot',motivoConflito:String(motivo||'').slice(0,300),
      analiseCompra:tipo==='analise_compra',temFoto:!!temFoto,cardId:(req.body||{}).cardId||null,
      sistemaCompra:String((req.body||{}).sistema||'adm'),
      criadoEm:new Date().toISOString(),origemBot:true,
    });
    await dbSet(KEY,db);
    try{await logConflito(tipo==='analise_compra'?'criado-compra':'criado',{nome:nome,telefone:telefone,motivo:motivo});}catch(_){}
    return res.status(200).json({ok:true,criado:true,id:novoId});
  }

  // ── 📊 CONFLITOS-STATS: reconciliação (criados × abertos × desfechos) ──
  // ── 🩹 CORRIGIR-NOMES: preenche nome/equipamento dos conflitos que nasceram sem ──
  if(action==='corrigir-nomes-conflito'){
    const [db,lgA,lgT,fA,fT,ppA,ppT]=await Promise.all([
      dbGet(KEY),dbGet('reparoeletro_logistica'),dbGet('tv_logistica'),
      dbGet('fichas_adm'),dbGet('fichas_tv'),dbGet('reparoeletro_pipe'),dbGet('tv_pipe')]);
    const d8=t=>String(t||'').replace(/\D/g,'').slice(-8);
    const mapa={};
    const põe=(tel,nome,equip)=>{const d=d8(tel);if(d.length<8)return;
      if(!mapa[d])mapa[d]={};
      if(nome&&!mapa[d].nome)mapa[d].nome=nome;
      if(equip&&!mapa[d].equipamento)mapa[d].equipamento=equip;};
    for(const b of [lgA,lgT,fA,fT]) for(const f of (((b||{}).fichas)||[])) põe(f.telefone,f.nome,f.equipamento);
    for(const b of [ppA,ppT]) for(const c of (((b||{}).cards)||[])) põe(c.telefone,c.nomeContato||c.nome,c.equipamento||c.descricao);
    let corrigidos=0; const lista=[];
    for(const f of (((db||{}).fichas)||[])){
      if(f.status!=='conflitos_bot')continue;
      const m=mapa[d8(f.telefone)];if(!m)continue;
      const semNome=!f.nome||/^cliente( whatsapp)?$/i.test(String(f.nome).trim());
      if(semNome&&m.nome){f.nome=m.nome;corrigidos++;lista.push({tel:f.telefone,nome:m.nome});}
      if(!String(f.equipamento||'').trim()&&m.equipamento)f.equipamento=m.equipamento;
    }
    if(corrigidos)await dbSet(KEY,db);
    return res.status(200).json({ok:true,corrigidos,lista:lista.slice(0,40)});
  }

  if(action==='conflitos-stats'){
    const [dbS,lg]=await Promise.all([dbGet(KEY),dbGet('prospeccao_conflitos_log')]);
    const abertos=(((dbS||{}).fichas)||[]).filter(f=>f.status==='conflitos_bot');
    const movs=((lg||{}).movs)||[];
    const hoje=new Date(Date.now()-3*3600*1000).toISOString().slice(0,10);
    // 📅 converte para o fuso de Brasília antes de comparar o dia
    const emBRT=t=>{const x=new Date(t||0).getTime();return x?new Date(x-3*3600000).toISOString().slice(0,10):'';};
    const doDia=movs.filter(m=>emBRT(m.ts)===hoje);
    const cont=(arr,t)=>arr.filter(m=>m.tipo===t).length;
    return res.status(200).json({ok:true,
      abertosAgora:abertos.length,
      hoje:{criados:cont(doDia,'criado'),aprovados:cont(doDia,'aprovado'),reprovados:cont(doDia,'reprovado'),resolvidos:cont(doDia,'resolvido')},
      total:{criados:cont(movs,'criado'),aprovados:cont(movs,'aprovado'),reprovados:cont(movs,'reprovado'),resolvidos:cont(movs,'resolvido')},
      conferencia:'criados − (aprovados + reprovados + resolvidos) deve bater com abertosAgora',
      ultimos:movs.slice(0,30)});
  }
  // ── 📊 RETORNO DO REMARCAR: quantas coletas solicitadas voltaram (dia/semana, ADM e TV) ──
  if(action==='retorno-remarcar'){
    const cont=(await dbGet('prospeccao_retorno_remarcar'))||{dias:{}};
    const hoje=new Date(Date.now()-3*3600*1000).toISOString().slice(0,10);
    const d7=new Date(Date.now()-3*3600*1000-6*86400000).toISOString().slice(0,10);
    const zero={adm:0,tv:0,total:0};
    const soma=(de)=>{const r={adm:0,tv:0,total:0};
      for(const d of Object.keys(cont.dias||{})){ if(d<de) continue;
        r.adm+=(cont.dias[d].adm||0); r.tv+=(cont.dias[d].tv||0); }
      r.total=r.adm+r.tv; return r;};
    const hj=cont.dias[hoje]?{adm:cont.dias[hoje].adm||0,tv:cont.dias[hoje].tv||0,total:(cont.dias[hoje].adm||0)+(cont.dias[hoje].tv||0)}:zero;
    const ultimos=Object.keys(cont.dias||{}).sort().slice(-14).map(d=>({dia:d,adm:cont.dias[d].adm||0,tv:cont.dias[d].tv||0}));
    return res.status(200).json({ok:true,hoje:hj,semana:soma(d7),porDia:ultimos});
  }

  // ── 🏅 PONTOS: recuperações de orçamento no Conflitos Bot ──
  if(action==='pontos'){
    const pt=(await dbGet('reparoeletro_pontos'))||{pessoas:{}};
    const hoje=new Date(Date.now()-3*3600*1000).toISOString().slice(0,10);
    // 📅 SEMANA COMERCIAL: começa na SEGUNDA e zera domingo 23h59.
    // Antes o placar somava desde sempre e nunca reiniciava.
    const bras=new Date(Date.now()-3*3600*1000);
    const diaSem=bras.getUTCDay();                       // 0=dom 1=seg
    const voltar=(diaSem===0)?6:(diaSem-1);              // domingo pertence à semana que começou na segunda
    const segunda=new Date(bras); segunda.setUTCDate(bras.getUTCDate()-voltar);
    const iniSemana=segunda.toISOString().slice(0,10);
    const naSemana=x=>String(x.ts).slice(0,10)>=iniSemana;
    const rank=Object.keys(pt.pessoas||{}).map(nome=>{
      const p=pt.pessoas[nome];const h=(p.historico||[]);
      const hSem=h.filter(naSemana);
      return {nome,
        total:hSem.reduce((a,b)=>a+(b.pontos||0),0),      // placar da SEMANA
        totalHistorico:p.total||0,
        pontosHoje:h.filter(x=>new Date(new Date(x.ts||0).getTime()-3*3600000).toISOString().slice(0,10)===hoje).reduce((a,b)=>a+(b.pontos||0),0),
        pontosSemana:hSem.reduce((a,b)=>a+(b.pontos||0),0),
        recuperacoes:hSem.length,
        recuperacoesHistorico:h.length,
        valorRecuperado:hSem.reduce((a,b)=>a+(Number(b.valor)||0),0),
        ultimas:h.slice(0,10)};
    }).sort((a,b)=>b.total-a.total);
    return res.status(200).json({ok:true,semanaComecaEm:iniSemana,ranking:rank});
  }

  if(req.method==='POST'&&action==='conflito-aprovar'){
    const {id,valor,pagamento,obs}=req.body||{};
    const db=(await dbGet(KEY))||{fichas:[]};
    const f=(db.fichas||[]).find(x=>x.id===id);
    if(!f)return res.status(404).json({ok:false,error:'não encontrado'});
    const d8a=String(f.telefone||'').replace(/\D/g,'').slice(-8);
    // 📺 o cliente pode ser de TV — procurar nos DOIS pipes, senão a aprovação falha
    let pp=(await dbGet('reparoeletro_pipe'))||{cards:[]};
    let chavePipe='reparoeletro_pipe';
    // o card guarda a fase em phase OU phaseId conforme a origem — aceitar os dois
    const faseDe=c=>String(c.phaseId||c.phase||'');
    const mesmoTel=c=>String(c.telefone||'').replace(/\D/g,'').slice(-8)===d8a;
    let card=(pp.cards||[]).find(c=>mesmoTel(c)&&faseDe(c)!=='aprovados');
    const ppTv=(await dbGet('tv_pipe'))||{cards:[]};
    if(!card){
      const cTv=(ppTv.cards||[]).find(c=>mesmoTel(c)&&faseDe(c)!=='aprovados');
      if(cTv){ card=cTv; pp=ppTv; chavePipe='tv_pipe'; }
    }
    // ✅ card JÁ em aprovados: a aprovação segue mesmo assim, para aplicar o valor,
    // registrar a nota e tirar a ficha do Conflitos — antes travava e nada acontecia
    let jaEstavaAprovado=false;
    if(!card){
      const cA=(pp.cards||[]).find(mesmoTel);
      const cT=(ppTv.cards||[]).find(mesmoTel);
      if(cA){ card=cA; jaEstavaAprovado=true; }
      else if(cT){ card=cT; pp=ppTv; chavePipe='tv_pipe'; jaEstavaAprovado=true; }
    }
    if(!card){
      // 🔍 diagnóstico: o cliente existe em algum pipe? em que fase?
      const achadosAdm=(pp.cards||[]).filter(mesmoTel).map(c=>'ADM:'+faseDe(c));
      const achadosTv=(ppTv.cards||[]).filter(mesmoTel).map(c=>'TV:'+faseDe(c));
      const todos=achadosAdm.concat(achadosTv);
      return res.status(404).json({ok:false,
        error: todos.length
          ? 'o cliente existe no pipe, mas já está em fase que não permite aprovar: '+todos.join(', ')
          : 'card do cliente não encontrado em nenhum pipe (ADM ou TV)',
        telefoneProcurado:d8a.slice(-4),
        ondeEstaNoPipe: todos.length?todos:'em lugar nenhum',
        nomeNaProspeccao: f.nome||null});
    }
    const v=parseFloat(valor)||0;
    if(v>0)card.valor=v;
    const nota='✔ Aprovado via Conflitos Bot: R$'+(v||card.valor)+' ('+String(pagamento||'—')+')'+(obs?' — '+String(obs).slice(0,150):'');
    card.descricao=(card.descricao?card.descricao+'\n':'')+nota;
    await dbSet(chavePipe,pp);
    // Mover pela action oficial → dispara o gatilho do técnico.
    // 📺 cliente de TV precisa do endpoint de TV, senão o card não se move,
    // não sai do Conflitos e o valor não é aplicado.
    const KA=(process.env.TECHFLOW_KEY||'tfk-re2026-Bx7mQp9zKw4Y').trim();
    const apiMover = chavePipe==='tv_pipe' ? 'tv-pipe' : 'pipe';
    let moveuOk=false, moveuErro=null;
    try{
      const rm=await fetch('https://reparoeletroadm.com/api/'+apiMover+'?action=mover&k='+KA,{
        method:'POST',headers:{'Content-Type':'application/json'},
        body:JSON.stringify({id:card.id,phase:'aprovados'})}).then(x=>x.json());
      moveuOk=!!(rm&&rm.ok); if(!moveuOk) moveuErro=(rm&&rm.error)||'sem detalhe';
    }catch(e){ moveuErro=e.message; }
    // se o endpoint falhou, move direto no banco para não deixar o card parado
    if(!moveuOk){
      try{
        const pp2=(await dbGet(chavePipe))||{cards:[]};
        const c2=(pp2.cards||[]).find(x=>x.id===card.id);
        if(c2){ c2.phase='aprovados'; c2.phaseId='aprovados';
          c2.aprovadoEm=new Date().toISOString();
          if(v>0)c2.valor=v;
          await dbSet(chavePipe,pp2); moveuOk=true; }
      }catch(e){}
    }
    // 🏅 +2 pontos: orçamento teoricamente perdido foi recuperado no Conflitos Bot
    try{
      const quem=String((req.body||{}).responsavel||'André').trim().slice(0,30)||'André';
      const pt=(await dbGet('reparoeletro_pontos'))||{pessoas:{}};
      if(!pt.pessoas[quem]) pt.pessoas[quem]={total:0,historico:[]};
      // 🏅 1 ponto por recuperação, e nunca duas vezes o mesmo cliente:
      // aprovar de novo alguém que já contou inflava o placar
      const d8p=String(f.telefone||'').replace(/\D/g,'').slice(-8);
      const jaContou=Object.values(pt.pessoas).some(p=>(p.historico||[]).some(x=>
        String(x.telefone||'').replace(/\D/g,'').slice(-8)===d8p));
      if(!jaContou){
        pt.pessoas[quem].total=(pt.pessoas[quem].total||0)+1;
        pt.pessoas[quem].historico.unshift({ts:new Date().toISOString(),pontos:1,
          cliente:f.nome||'',telefone:f.telefone||'',valor:v||card.valor||0,
          eraReprovado:/reprov/i.test(String(f.motivoConflito||'')),
          motivo:'orçamento recuperado no Conflitos Bot'});
        pt.pessoas[quem].historico=pt.pessoas[quem].historico.slice(0,300);
        await dbSet('reparoeletro_pontos',pt);
      }
    }catch(_){}
    db.fichas=db.fichas.filter(x=>x.id!==id);
    await dbSet(KEY,db);
    try{await logConflito('aprovado',{nome:f.nome,telefone:f.telefone,motivo:'aprovado no Conflitos Bot'});}catch(_){}
    return res.status(200).json({ok:true,card:card.id});
  }
  if(req.method==='POST'&&action==='conflito-reprovar'){
    const {id}=req.body||{};
    const db=(await dbGet(KEY))||{fichas:[]};
    const f=(db.fichas||[]).find(x=>x.id===id);
    if(!f)return res.status(404).json({ok:false,error:'não encontrado'});
    const d8r=String(f.telefone||'').replace(/\D/g,'').slice(-8);
    let pp=(await dbGet('reparoeletro_pipe'))||{cards:[]};
    let chavePipeR='reparoeletro_pipe';
    const faseDeR=c=>String(c.phaseId||c.phase||'');
    const mesmoTelR=c=>String(c.telefone||'').replace(/\D/g,'').slice(-8)===d8r;
    let card=(pp.cards||[]).find(c=>mesmoTelR(c)&&faseDeR(c)!=='solicitar_entrega');
    const ppTvR=(await dbGet('tv_pipe'))||{cards:[]};
    if(!card){
      const cTvR=(ppTvR.cards||[]).find(c=>mesmoTelR(c)&&faseDeR(c)!=='solicitar_entrega');
      if(cTvR){ card=cTvR; pp=ppTvR; chavePipeR='tv_pipe'; }
    }
    if(!card){
      const todosR=(pp.cards||[]).filter(mesmoTelR).map(c=>'ADM:'+faseDeR(c))
        .concat((ppTvR.cards||[]).filter(mesmoTelR).map(c=>'TV:'+faseDeR(c)));
      return res.status(404).json({ok:false,
        error: todosR.length
          ? 'o cliente existe no pipe, mas em fase que não permite reprovar: '+todosR.join(', ')
          : 'card do cliente não encontrado em nenhum pipe (ADM ou TV)',
        telefoneProcurado:d8r.slice(-4), ondeEstaNoPipe: todosR.length?todosR:'em lugar nenhum'});
    }
    card.descricao='REPROVADO - '+String(card.descricao||'');
    await dbSet(chavePipeR,pp);
    const KR=(process.env.TECHFLOW_KEY||'tfk-re2026-Bx7mQp9zKw4Y').trim();
    await fetch('https://reparoeletroadm.com/api/pipe?action=mover&k='+KR,{
      method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({id:card.id,phase:'solicitar_entrega'})});
    db.fichas=db.fichas.filter(x=>x.id!==id);
    await dbSet(KEY,db);
    try{await logConflito('reprovado',{nome:f.nome,telefone:f.telefone,motivo:'reprovado no Conflitos Bot'});}catch(_){}
    return res.status(200).json({ok:true,card:card.id});
  }
  // ── 🔵 MARCAR-RECOMENDACAO: parecer do almoxarifado volta para o card de Análise de Compra ──
  // ── 📷 MARCAR-FOTO: a foto da análise de compra chegou pelo almoxarifado ──
  if(req.method==='POST'&&action==='marcar-foto'){
    const {id,cardId}=req.body||{};
    const db=(await dbGet(KEY))||{fichas:[]};
    const f=(db.fichas||[]).find(x=>x.id===id);
    if(!f)return res.status(404).json({ok:false,error:'conflito não encontrado'});
    f.temFoto=true; if(cardId)f.cardId=cardId;
    await dbSet(KEY,db);
    return res.status(200).json({ok:true});
  }

  if(req.method==='POST'&&action==='marcar-recomendacao'){
    const {id,parecer,preco,por}=req.body||{};
    const db=(await dbGet(KEY))||{fichas:[]};
    const f=(db.fichas||[]).find(x=>x.id===id);
    if(!f)return res.status(404).json({ok:false,error:'conflito não encontrado'});
    if((req.body||{}).temFoto)f.temFoto=true;
    f.recomendacaoCompra={parecer:parecer==='sim'?'sim':'nao',
      preco:preco?String(preco).slice(0,20):null,
      por:String(por||'Almoxarifado').slice(0,40),em:new Date().toISOString()};
    await dbSet(KEY,db);
    return res.status(200).json({ok:true});
  }

  if(req.method==='POST'&&action==='conflito-contatado'){
    const {id}=req.body||{};
    const db=(await dbGet(KEY))||{fichas:[]};
    const f=(db.fichas||[]).find(x=>x.id===id);
    if(!f)return res.status(404).json({ok:false,error:'não encontrado'});
    f.conflitoContatadoEm=new Date().toISOString();
    await dbSet(KEY,db);
    return res.status(200).json({ok:true});
  }
  // ── 📣 CONFLITO-RELATAR: etapa entre resolvido e finalizado — retorno ao cliente ──
  if(req.method==='POST'&&action==='conflito-relatar'){
    const {id,resumo}=req.body||{};
    if(!id)return res.status(400).json({ok:false,error:'id obrigatorio'});
    if(!resumo||String(resumo).trim().length<10){
      return res.status(400).json({ok:false,error:'escreva o resumo do que foi resolvido (mínimo 10 caracteres)'});
    }
    const db=(await dbGet(KEY))||{fichas:[]};
    const f=(db.fichas||[]).find(x=>x.id===id);
    if(!f)return res.status(404).json({ok:false,error:'conflito não encontrado'});
    f.aguardandoRelato=true;
    f.resumoResolucao=String(resumo).trim().slice(0,600);
    f.resolvidoEm=new Date().toISOString();
    await dbSet(KEY,db);
    return res.status(200).json({ok:true,ficha:f});
  }

  // ── ✅ CONFLITO-RELATADO: cliente avisado → vai para finalizado ──
  if(req.method==='POST'&&action==='conflito-relatado'){
    const {id}=req.body||{};
    if(!id)return res.status(400).json({ok:false,error:'id obrigatorio'});
    const db=(await dbGet(KEY))||{fichas:[]};
    const f=(db.fichas||[]).find(x=>x.id===id);
    if(!f)return res.status(404).json({ok:false,error:'conflito não encontrado'});
    f.clienteRelatado=true;
    f.relatadoEm=new Date().toISOString();
    delete f.aguardandoRelato;
    await dbSet(KEY,db);
    try{await logConflito('resolvido',{nome:f.nome,telefone:f.telefone,motivo:'resolvido e cliente avisado'});}catch(_){}
    // agora sim sai da fila de conflitos
    const dbF=(await dbGet(KEY))||db;
    dbF.fichas=(dbF.fichas||[]).filter(x=>x.id!==id);
    await dbSet(KEY,dbF);
    return res.status(200).json({ok:true,finalizado:true});
  }

  if(req.method==='POST'&&action==='resolver-conflito'){
    const {id}=req.body||{};
    const db=(await dbGet(KEY))||{fichas:[]};
    if(!Array.isArray(db.fichas))db.fichas=[];
    const f=(db.fichas||[]).find(x=>x.id===id);
    if(!f)return res.status(404).json({ok:false,error:'não encontrado'});
    db.fichas=db.fichas.filter(x=>x.id!==id);
    await dbSet(KEY,db);
    try{await logConflito('resolvido',{nome:f.nome,telefone:f.telefone,motivo:'resolvido/removido manualmente'});}catch(_){}
    return res.status(200).json({ok:true});
  }

  // Lista crua de eventos por tipo/dia (investigação)
  if(action==='eventos-listar'){
    const tipoQ=String(req.query.tipo||'');
    const diaQ=String(req.query.dia||'');
    let lista=await lerEventos();
    if(tipoQ)lista=lista.filter(e=>e.tipo===tipoQ);
    if(diaQ)lista=lista.filter(e=>String(e.ts||'').slice(0,10)===diaQ);
    return res.status(200).json({ok:true,total:lista.length,
      eventos:lista.slice(-50).map(e=>({ts:e.ts,tipo:e.tipo,de:e.de,nome:e.nome,sis:e.sis}))});
  }

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
