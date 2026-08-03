
// ── Pipefy é ESPELHO — nunca bloqueia o fluxo local ─────────────────────
async function pipefyBestEffort(fn) {
  try { return await fn(); }
  catch(e) { console.warn('[Pipefy best-effort]', e.message); return null; }
}


// ── fmt4dig: padrão Nome 4díg do telefone ────────────────────────────────
function fmt4dig(nome, tel) {
  if (!nome) return '';
  var n = String(nome).trim();
  if (/\s\d{4}$/.test(n)) return n;
  if (!tel) return n;
  var digits = String(tel).replace(/\D/g,'');
  var last4 = digits.slice(-4);
  if (last4.length < 4) return n;
  return n + ' ' + last4;
}

// ── Helper: gravar no log central ────────────────────────────────────────
async function logAction(entry) {
  try {
    const _U=(process.env.UPSTASH_URL||'').replace(/['"]/g,'').trim();
    const _T=(process.env.UPSTASH_TOKEN||'').replace(/['"]/g,'').trim();
    const _K='reparoeletro_log';
    const _r=await fetch(_U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+_T,'Content-Type':'application/json'},body:JSON.stringify([['GET',_K]])});
    const _j=await _r.json();const _v=_j[0]?.result;
    let _log=[];if(_v){try{_log=JSON.parse(_v);if(typeof _log==='string')_log=JSON.parse(_log);}catch(e){}}if(!Array.isArray(_log))_log=[];
    _log.unshift({ts:new Date().toISOString(),modulo:entry.modulo||'—',fichaId:entry.fichaId||'',ficha:entry.ficha||'',acao:entry.acao||'',de:entry.de||'',para:entry.para||'',gatilho:entry.gatilho||'',status:entry.status||'ok',detalhe:entry.detalhe||''});
    if(_log.length>500)_log.splice(500);
    await fetch(_U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+_T,'Content-Type':'application/json'},body:JSON.stringify([['SET',_K,JSON.stringify(_log)]])});
  }catch(e){}
}


// ── Helper: mover card no Pipe ADM pelo pipefyId ─────────────────────────
async function moverNoPipe(pipefyId, novaFase, dados) {
  if (!pipefyId) return;
  try {
    const PIPE_KEY = 'reparoeletro_pipe';
    const U = (process.env.UPSTASH_URL   || '').replace(/['"]/g,'').trim();
    const T = (process.env.UPSTASH_TOKEN || '').replace(/['"]/g,'').trim();
    async function _pipeGet(k) {
      const r = await fetch(U + '/pipeline', { method:'POST',
        headers:{ Authorization:'Bearer '+T,'Content-Type':'application/json' },
        body: JSON.stringify([['GET', k]]) });
      const j = await r.json();
      const v = j[0]?.result; if (!v) return null;
      let val = JSON.parse(v);
      if (typeof val === 'string') { try { val = JSON.parse(val); } catch(e) {} }
      return (val && typeof val === 'object') ? val : null;
    }
    async function _pipeSet(k, v) {
      await fetch(U + '/pipeline', { method:'POST',
        headers:{ Authorization:'Bearer '+T,'Content-Type':'application/json' },
        body: JSON.stringify([['SET', k, JSON.stringify(v)]]) });
    }
    const db   = (await _pipeGet(PIPE_KEY)) || { cards:[], syncedPipefyIds:[], lastSync:null };
    const card = (db.cards || []).find(c => c.pipefyId === String(pipefyId));
    if (!card) {
      // Card não existe — criar se dados fornecidos
      if (dados && dados.nomeContato) {
        const now = new Date().toISOString();
        db.cards.unshift({
          id:           'PIPE-' + String(db.cards.length + 1).padStart(4,'0'),
          pipefyId:     String(pipefyId),
          phase:        novaFase,
          nomeContato:  dados.nomeContato || '',
          telefone:     dados.telefone    || '',
          equipamento:  dados.equipamento || '',
          descricao:    dados.descricao   || '',
          valor:        parseFloat(dados.valor || 0) || 0,
          origem:       dados.origem      || 'sistema',
          criadoEm:     now, movedAt: now,
          aguardandoDesde: novaFase === 'aguardando_aprovacao' ? now : null,
          history: [], analiseCompra: false
        });
        await _pipeSet(PIPE_KEY, db);
      }
      return;
    }
    const now = new Date().toISOString();
    card.history = (card.history || []).concat([{ phase: card.phase, ts: now }]);
    card.phase   = novaFase;
    card.movedAt = now;
    if (dados) {
      if (dados.valor !== undefined) card.valor = parseFloat(dados.valor) || 0;
      if (dados.nomeContato)         card.nomeContato = dados.nomeContato;
    }
    await _pipeSet(PIPE_KEY, db);
  } catch(e) { console.error('[pipe-mover]', novaFase, e.message); }
}


// ── Hook: salvar no Pipe ADM quando nova ficha entra no FL ──────────────────
async function registrarNoPipe(dados) {
  try {
    const PIPE_KEY = 'reparoeletro_pipe';
    const pipeDb   = await dbGet(PIPE_KEY) || { cards:[], syncedPipefyIds:[], lastSync:null };
    if (!Array.isArray(pipeDb.cards)) pipeDb.cards = [];
    const pid = dados.pipefyId ? String(dados.pipefyId) : null;
    if (pid && pipeDb.cards.find(function(c){ return c.pipefyId === pid; })) return;
    // Para fichas sem pipefyId, verificar por fichaId local
    const fid = dados.fichaId ? String(dados.fichaId) : null;
    if (fid && pipeDb.cards.find(function(c){ return c.fichaId === fid; })) return;
    const now = new Date().toISOString();
    pipeDb.cards.unshift({
      id:              'PIPE-' + String(pipeDb.cards.length + 1).padStart(4,'0'),
      pipefyId:        pid,
      fichaId:         fid,
      phase:           dados.phase || 'aprovados',
      nomeContato:     dados.nomeContato || '',
      telefone:        dados.telefone || '',
      equipamento:     dados.equipamento || '',
      descricao:       dados.descricao || '',
      valor:           parseFloat(dados.valor || 0) || 0,
      origem:          'frenteloja',
      criadoEm:        now, movedAt: now,
      aguardandoDesde: null,
      history: [], analiseCompra: false
    });
    pipeDb.lastSync = now;
    await dbSet(PIPE_KEY, pipeDb);
  } catch(e) { console.error('[pipe-hook-fl]', e.message); }
}

// api/frenteloja.js — Sistema Frente de Loja
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

async function createPipefyCard() {
  return null; // Pipefy desconectado
}

async function movePipefyCard(cardId, phaseId){
  const q='mutation{moveCardToPhase(input:{card_id:"'+cardId+'",destination_phase_id:"'+phaseId+'"}){card{id}}}';
  await pipefyQ(q);
}

export default async function handler(req,res){
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
  if(req.method==='OPTIONS')return res.status(200).end();
  const action=req.query.action;

  if(req.method==='GET'&&action==='load'){
    const db=await dbGet(FL_KEY)||defaultDB();
    const todayStart=brtStartOfDay();
    let changed=false;
    db.fichas.forEach(f=>{
      if(f.liberadoHoje&&new Date(f.movedAt)<todayStart){
        f.liberadoHoje=false;changed=true;
      }
      if(f.phase==='reprovado'&&new Date(f.reprovadoEm||f.movedAt||0)<todayStart){
        f.phase='encerrado';changed=true;
      }
    });
    db.fichas=db.fichas.filter(f=>f.phase!=='encerrado');
    if(changed)await dbSet(FL_KEY,db);
    return res.status(200).json({ok:true,fichas:db.fichas});
  }

  if(req.method==='POST'&&action==='criar'){
    const {nomeContato,equipamento,telefone,descricao,cpf,email}=req.body||{};
    if(!nomeContato||!equipamento)return res.status(400).json({ok:false,error:'Nome e equipamento obrigatórios'});
    const db=await dbGet(FL_KEY)||defaultDB();
    // AUDITORIA: registrar quem criou (IP + dispositivo) — investigação de criações não reconhecidas
    try {
      const aud = (await dbGet('fl_criar_audit')) || { regs: [] };
      aud.regs.unshift({
        ts: new Date().toISOString(),
        nome: nomeContato || '', tel: (telefone||'').slice(-8),
        ip: String(req.headers['x-forwarded-for'] || req.headers['x-real-ip'] || '').split(',')[0].trim(),
        ua: String(req.headers['user-agent'] || '').slice(0, 160),
        ref: String(req.headers['referer'] || '').slice(0, 120),
      });
      aud.regs = aud.regs.slice(0, 200);
      await dbSet('fl_criar_audit', aud);
    } catch(e) {}
    // Idempotência: mesmo telefone+equipamento criado há <120s = duplo clique/retry → retorna a existente
    const telN=(telefone||'').replace(/[^0-9]/g,'');
    const jaExiste=(db.fichas||[]).find(f=>
      String(f.telefone||'')===telN && telN.length>=8 &&
      String(f.equipamento||'').trim().toLowerCase()===String(equipamento||'').trim().toLowerCase() &&
      (Date.now()-new Date(f.createdAt||0).getTime())<120000
    );
    if(jaExiste)return res.status(200).json({ok:true,ficha:jaExiste,duplicataEvitada:true});
    const id=nextId(db);const now=new Date().toISOString();
    const ficha={id,nomeContato,equipamento,telefone:telN,
      cpf:cpf||'',email:email||'',
      descricao:descricao||'',phase:'analise',createdAt:now,movedAt:now,
      history:[{phase:'analise',ts:now}]};
    db.fichas.unshift(ficha);await dbSet(FL_KEY,db);
    // Espelhar no board em analise_loja
    try{
      const _base=process.env.FL_BASE_URL||'https://reparoeletroadm.com';
      const _title=(ficha.nomeContato+' (Loja) - '+(ficha.equipamento||'')+' | '+(ficha.descricao||'')+' OS:'+ficha.id).replace(/"/g,"'").slice(0,255);
      await fetch(_base+'/api/board?action=add-loja-card&k=tfk-re2026-Bx7mQp9zKw4Y',{method:'POST',headers:{'Content-Type':'application/json'},
        body:JSON.stringify({flFichaId:ficha.id,pipefyId:null,title:_title,
          nomeContato:ficha.nomeContato||'',telefone:ficha.telefone||'',
          cpf:ficha.cpf||'',email:ficha.email||'',
          equipamento:ficha.equipamento||'',descricao:ficha.descricao||'',
          phaseId:'analise_loja'})
      }).catch(e=>console.error('[FL] criar→board:',e.message));
    }catch(e){}
    logAction({ modulo:'Frente de Loja', fichaId:ficha.id||'', ficha:ficha.nomeContato||'', acao:'Nova ficha criada', para:'analise', gatilho:'', status:'ok' }).catch(()=>{});
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
    await dbSet(FL_KEY,db);
    logAction({ modulo:'Frente de Loja', fichaId:ficha.id||'', ficha:ficha.nomeContato||'', acao:'Análise técnica', para:'orcamento_cadastrado', gatilho:'', status:'ok' }).catch(()=>{});
    return res.status(200).json({ok:true,ficha});
  }

  if(req.method==='POST'&&action==='passar-orcamento'){
    const {id,valor,formaPagamento,decisao}=req.body||{};
    if(!id)return res.status(400).json({ok:false,error:'id obrigatório'});
    const db=await dbGet(FL_KEY)||defaultDB();
    const ficha=db.fichas.find(f=>f.id===id);
    if(!ficha)return res.status(404).json({ok:false,error:'Não encontrada'});
    const now=new Date().toISOString();
    ficha.orcamento={valor:parseFloat(valor)||0,formaPagamento:formaPagamento||'pix',status:decisao};

    // Reprovado: mantém no banco com phase='reprovado' para exibir coluna hoje
    if(decisao==='reprovado'){
      ficha.phase='reprovado';
      ficha.reprovadoEm=now;
      ficha.motivo=req.body?.motivo||'';
      ficha.movedAt=now;
      await dbSet(FL_KEY,db);
      logAction({ modulo:'Frente de Loja', fichaId:ficha.id||'', ficha:ficha.nomeContato||'', acao:'Orçamento reprovado', para:'reprovado', gatilho:'', status:'ok' }).catch(()=>{});
    return res.status(200).json({ok:true,ficha});
    }

    if(decisao==='aprovado'){
      ficha.phase='producao';ficha.movedAt=now;
      ficha.history=(ficha.history||[]).concat([{phase:'producao',ts:now}]);

      const titleCompleto=(ficha.nomeContato+' (Loja) - '+ficha.equipamento+
        ' | '+(ficha.descricao||'')+' | Diag: '+(ficha.descricaoTecnica||'')+
        ' | R$'+String(parseFloat(ficha.orcamento?.valor||0).toFixed(2))+
        ' '+(ficha.orcamento?.formaPagamento||'pix')+' OS:'+ficha.id
      ).replace(/"/g,"'").slice(0,255);

      // ── PASSO 1: Criar card no Pipefy (SÍNCRONO, antes de responder) ─
      let pipefyId = null;
      try {
        const aprovadoPhaseId = await getPipefyPhaseId('aprovad');
        if (aprovadoPhaseId) {
          const nomeCard = (ficha.nomeContato+' (Loja)').replace(/"/g,"'").slice(0,255);
          const telCard  = (ficha.telefone||'').replace(/"/g,"'").slice(0,100);
          const data = await pipefyQ(
            'mutation{createCard(input:{pipe_id:"'+PIPE_ID+'" phase_id:"'+aprovadoPhaseId+'" title:"'+titleCompleto+'" fields_attributes:[{field_id:"nome_do_contato" field_value:"'+nomeCard+'"},{field_id:"telefone" field_value:"'+telCard+'"}]}){card{id}}}'
          );
          pipefyId = data?.createCard?.card?.id || null;
          if (pipefyId) {
            ficha.pipefyCardId = pipefyId;
            // Registrar no Pipe ADM
            await registrarNoPipe({ pipefyId, fichaId: ficha.id, phase: 'aprovados', nomeContato: ficha.nomeContato||'', telefone: ficha.telefone||'', equipamento: ficha.orcamento?.equipamento||'', valor: ficha.orcamento?.valor||0, origem:'frenteloja' }).catch(()=>{});
            console.log('[FL] Pipefy card criado:', pipefyId);
            // Atualizar valor no Pipefy (fire-and-forget)
            const vn = String(parseFloat(ficha.orcamento?.valor||0).toFixed(2));
            pipefyQ('mutation{updateCardField(input:{card_id:"'+pipefyId+'" field_id:"valor_de_contrato" new_value:"'+vn+'"}){success}}').catch(()=>{});
          }
        } else {
          console.error('[FL] Fase Aprovado nao encontrada no Pipefy');
          ficha.pipefyPending = true; // cron vai retentar
        }
      } catch(e) {
        console.error('[FL] Pipefy createCard:', e.message);
        ficha.pipefyPending = true; // cron vai retentar
      }

      // ── PASSO 2: Criar/mover card no board via endpoint (evita limite 5MB Upstash) ─
      try {
        const _boardBase = process.env.FL_BASE_URL || 'https://reparoeletroadm.com';
        const _boardR = await fetch(_boardBase+'/api/board?action=add-loja-card&k=tfk-re2026-Bx7mQp9zKw4Y', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            flFichaId:   ficha.id,
            pipefyId:    pipefyId || null,
            title:       titleCompleto,
            nomeContato: (ficha.nomeContato||'').replace(/\(Loja\)/g,'').trim(),
            telefone:    ficha.telefone||'',
            phaseId:     'cliente_loja',
          })
        }).then(r=>r.json()).catch(e=>({ ok:false, error:e.message }));
        if (!_boardR.ok) console.error('[FL] board via API:', _boardR.error||_boardR.msg);
        else console.log('[FL] board via API: OK —', _boardR.msg||'criado');
      } catch(e) { console.error('[FL] board via API exception:', e.message); }

      // ── PASSO 3: Registrar no Balcão (aguardando pagamento) ─────────
      try {
        const BALCAO_KEY = 'reparoeletro_balcao';
        const balcao = (await dbGet(BALCAO_KEY)) || [];
        const balcaoId = pipefyId ? String(pipefyId) : ficha.id;
        if (!balcao.find(b => b.pipefyId === balcaoId)) {
          balcao.unshift({
            pipefyId:    balcaoId,
            flFichaId:   ficha.id,
            nomeContato: ficha.nomeContato || '—',
            osCode:      ficha.id,
            descricao:   ficha.descricao || null,
            telefone:    ficha.telefone || null,
            tecnico:     null,
            entradaEm:   now,
            status:      'aguardando_pagamento',
            pagoEm:      null,
          });
          await dbSet(BALCAO_KEY, balcao);
          console.log('[FL] Balcao registrado:', balcaoId);
        }
      } catch(e) { console.error('[FL] balcao registro:', e.message); }

      // ── PASSO 4: Salvar FL e responder ───────────────────────────────
      await dbSet(FL_KEY, db);

      logAction({ modulo:'Frente de Loja', fichaId:ficha.id||'', ficha:ficha.nomeContato||'', acao:'Orçamento reprovado', para:'reprovado', gatilho:'', status:'ok' }).catch(()=>{});
    return res.status(200).json({ok:true,ficha});
    }
  }


  // ── GET buscar-finalizar: localiza ficha por nome+tel e retorna ID ──────────
  if (action === 'buscar-finalizar') {
    const nome = (req.query.nome || '').toLowerCase();
    const tel4 = (req.query.tel4 || '');
    const db   = await dbGet(FL_KEY) || defaultDB();
    const ficha = db.fichas.find(f =>
      (f.nomeContato || '').toLowerCase().startsWith(nome) &&
      (f.telefone || '').replace(/\D/g,'').slice(-4) === tel4 &&
      f.phase === 'producao'
    );
    if (!ficha) return res.status(404).json({ ok:false, error:'Ficha não encontrada em produção', nome, tel4 });
    return res.status(200).json({ ok:true, id:ficha.id, nome:ficha.nomeContato, fase:ficha.phase, pipefyCardId:ficha.pipefyCardId||null });
  }


  // ── GET finalizar-por-nome: localiza por nome+tel4 e move para conserto_realizado ──
  if (action === 'finalizar-por-nome') {
    const nome = (req.query.nome || '').toLowerCase();
    const tel4 = (req.query.tel4 || '');
    const db   = await dbGet(FL_KEY) || defaultDB();
    const ficha = db.fichas.find(f =>
      (f.nomeContato || '').toLowerCase().startsWith(nome) &&
      (f.telefone || '').replace(/\D/g,'').slice(-4) === tel4 &&
      f.phase === 'producao'
    );
    if (!ficha) return res.status(404).json({ ok:false, error:'Ficha não encontrada em produção', nome, tel4 });
    const now = new Date().toISOString();
    ficha.phase        = 'conserto_realizado';
    ficha._avisarPronto = true;
    ficha.liberadoHoje = true;
    ficha.movedAt      = now;
    ficha.history      = (ficha.history||[]).concat([{ phase:'conserto_realizado', ts:now, via:'admin_manual' }]);
    await processarAvisoPendente(ficha);
    await dbSet(FL_KEY, db);
    // Mover no Pipefy se tiver card
    if (ficha.pipefyCardId) {
      try {
        const phId = await getPipefyPhaseId('conserto');
        if (phId) await movePipefyCard(ficha.pipefyCardId, phId);
      } catch(pe) { console.error('[finalizar-por-nome] Pipefy:', pe.message); }
    }
    return res.status(200).json({ ok:true, finalizado:true, id:ficha.id, nome:ficha.nomeContato, fase:'conserto_realizado' });
  }


  // ── POST marcar-orc-whatsapp: marca orçamento como enviado pelo WhatsApp ──
  // Envia o aviso de "pronto para retirada" e devolve o resultado.
  // Usada tanto pelo botão manual quanto pelo aviso automático ao entrar na coluna.
  async function avisarProntoRetirada(ficha) {
    const tel = String(ficha.telefone || ficha.tel || '').replace(/\D/g, '');
    if (tel.length < 10) return { enviado: false, via: null, erro: 'telefone inválido ou ausente' };
    const to = tel.startsWith('55') ? tel : '55' + tel;
    const primeiro = String(ficha.nomeContato || 'tudo bem').trim().split(/\s+/)[0];
    const KAV = (process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim();
    const texto = `Oi ${primeiro}! Seu ${ficha.equipamento || 'equipamento'} já está pronto e pode ser retirado aqui na loja. Funcionamos de segunda a sexta das 8h às 17h e sábado das 8h às 12h. Te esperamos!`;
    try {
      const r = await fetch(`https://reparoeletroadm.com/api/wa-bot?action=enviar&k=${KAV}`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ tel: to, texto, via: 'frenteloja-avisado' }),
      }).then(x => x.json()).catch(e => ({ ok: false, error: e.message }));
      if (r && r.ok) return { enviado: true, via: 'mensagem', erro: null };
      const rt = await fetch(`https://reparoeletroadm.com/api/wa-bot?action=template-conserto&k=${KAV}`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ tel: to, nome: primeiro }),
      }).then(x => x.json()).catch(e => ({ ok: false, error: e.message }));
      return rt && rt.ok ? { enviado: true, via: 'template', erro: null }
        : { enviado: false, via: null, erro: (rt && (rt.error || rt.erro)) || 'falha no envio' };
    } catch (e) { return { enviado: false, via: null, erro: e.message }; }
  }
  // Registra o resultado do aviso na ficha (mesmo formato nos dois caminhos)
  function gravarAviso(ficha, envio) {
    ficha.avisadoPronto = envio.enviado;
    ficha.avisadoProntoEm = new Date().toISOString();
    ficha.avisadoVia = envio.via;
    if (envio.erro) ficha.avisadoErro = String(envio.erro).slice(0, 140);
    else delete ficha.avisadoErro;
  }

  // ═══ MESMA INTELIGÊNCIA DA LOGÍSTICA (templates, tabelas e regras de preço) ═══
  // Copiada da logística para o Frente de Loja usar o MESMO cálculo e os MESMOS textos,
  // porém gravando apenas no banco isolado — o bot continua sem enxergar nada daqui.
  function gerarTextoLoja(tipo, subtipo, servicos, precoInput, templates, nomeCliente) {
      const pn = String(nomeCliente || "cliente").trim().split(/\s+/)[0];
      const s  = servicos || [];
      const tem = (lista) => s.some(x => lista.includes(x));
      const pecas = (lista) => s.filter(x => lista.includes(x)).join(', ') || s.join(', ');
      const x2 = (v) => String(Math.round(parseFloat(v||0)*2));
      const T = templates || {};
      // Substituir placeholders num template
      function applyTpl(tpl, pecasStr, preco) {
        return tpl
          .replace(/\[NOME\]/g, pn)
          .replace(/\[peças\]/g, pecasStr || s.join(', '))
          .replace(/\[VALOR\]/g, preco || '');
      }

      if (tipo === 'microondas') {
        if (tem(['Troca de Placa','Display'])) {
          const p = pecas(['Troca de Placa','Display']);
          const tpl = T.microondas_placa?.texto || `Ola, [NOME] bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario fazer a troca do conjunto conjunto da [peças], será feito a reoperação eletrica tambem. Este conserto completo fica em [VALOR] reais apenas. Aprovando ja iniciamos o conserto.`;
          return { texto: applyTpl(tpl, p, x2(precoInput)), preco:x2(precoInput) };
        }
        if (tem(['Vidro','Porta'])) {
          const p = pecas(['Vidro','Porta']);
          const tpl = T.microondas_vidro?.texto || `Ola, [NOME] bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orçamento:\n\nPara fazer a desmontagem, instalação da [peças], montagem e regulagem consigo fazer para você por [VALOR] reais apenas. Aprovando ja iniciamos o conserto.`;
          return { texto: applyTpl(tpl, p, x2(precoInput)), preco:x2(precoInput) };
        }
        if (tem(['Haste'])) { const tpl = T.microondas_haste?.texto || `Ola, [NOME] bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orçamento:\n\nPara fazer a desmontagem, instalação da haste, montagem e regulagem consigo fazer para você por [VALOR] reais apenas. Aprovando ja iniciamos o conserto.`; return { texto: applyTpl(tpl, 'haste', '370'), preco:'370' }; }
        if (tem(['Pintura'])) { const tpl = T.microondas_pintura?.texto || `Ola, [NOME] bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orçamento:\n\nPara fazer a desmontagem, pintura, montagem, regulagem e revisão consigo fazer para você por [VALOR] reais apenas. Aprovando ja iniciamos o conserto.`; return { texto: applyTpl(tpl, 'pintura', '370'), preco:'370' }; }
        if (tem(['Reforma'])) {
          const comRevisao = tem(['Revisão']);
          const preco = comRevisao ? '390' : '370';
          const textoBase = `Olá, sou Alessandra, Reparo Eletro, e vou te passar seu orçamento agora. Fizemos as análises e para fazer a desmontagem do equipamento, lixar, pintar, refazer as instalações e montar novamente, fica em [VALOR] reais apenas. Aprovando, já iniciamos a reforma.`.replace('[VALOR]', preco);
          const textoFinal = textoBase + (comRevisao ? ` Observação: foi identificado que existem componentes que estão apresentando falhas e será necessário também fazer uma revisão completa para que além da reforma o equipamento fique em perfeito funcionamento. Esta revisão não terá nenhum custo adicional.` : '');
          return { texto: textoFinal, preco };
        }
        if (tem(['Magnetron'])) {
          const tpl = T.microondas_magnetron?.texto || `Ola, [NOME] bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario fazer a troca do Magnetron, peca responsavel pelo aquecimento do aparelho. Este conserto completo fica em [VALOR] reais apenas. Aprovando ja iniciamos o conserto.`;
          return { texto: applyTpl(tpl, 'Magnetron', '390'), preco:'390' };
        }
        const p = s.join(', ');
        const tpl = T.microondas_eletrico?.texto || `Ola, [NOME] bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario refazer a parte eletrica que causou danos no conjunto do [peças], as pecas serao trocadas tambem. Este conserto completo fica em [VALOR] reais apenas. Aprovando ja iniciamos o conserto.`;
        return { texto: applyTpl(tpl, p, '370'), preco:'370' };
      }
      if (tipo === 'bblend') {
        // Bblend — preço fixo R$ 1.490 independente das peças selecionadas
        const p = s.join(', ') || 'conjunto do motor';
        return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario fazer a troca do ${p}. Este conserto completo fica em 1490 reais apenas. Aprovando ja iniciamos o conserto.`, preco:'1490' };
      }

      if (tipo === 'tv') {
        // TV TU — preço fixo R$ 1.290 independente da opção (TU + Barramento ou TU + Placa)
        const opcao = s.join(', ') || subtipo || 'TU';
        return {
          texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario fazer a troca do ${opcao}. Este conserto completo fica em 1290 reais apenas. Aprovando ja iniciamos o conserto.`,
          preco:'1290'
        };
      }

      if (tipo === 'purificador') {
        if (subtipo === 'Motor') {
          if (tem(['Gás'])) return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario fazer a troca da valvula de gas, solda e recarga de gas refrigerante. Este conserto completo fica em 490 reais apenas. Aprovando ja iniciamos o conserto.`, preco:'490' };
          const p = s.join(', ');
          return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario fazer a troca do conjunto da ${p}. Este conserto completo fica em 490 reais apenas. Aprovando ja iniciamos o conserto.`, preco:'490' };
        }
        if (subtipo === 'Eletrônico') {
          if (tem(['Kit Termo Elétrico'])) return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario refazer a parte eletrica que causou danos no conjunto do cooler, placa de resfriamento e pasta termica, as pecas serao trocadas tambem. Este conserto completo fica em 350 reais apenas. Aprovando ja iniciamos o conserto.`, preco:'350' };
          if (tem(['Recuperação de Placa'])) return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario refazer a parte eletrica que causou danos no conjunto da placa principal, será feito a reoperação da placa tambem. Este conserto completo fica em 350 reais apenas. Aprovando ja iniciamos o conserto.`, preco:'350' };
          const p = s.join(', ');
          return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario fazer a troca do conjunto da ${p}. Este conserto completo fica em 350 reais apenas. Aprovando ja iniciamos o conserto.`, preco:'350' };
        }
      }
      if (tipo === 'adega') {
        if (subtipo === 'Motor') {
          if (tem(['Gás'])) return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario fazer a troca da valvula de gas, solda e recarga de gas refrigerante. Este conserto completo fica em 490 reais apenas. Aprovando ja iniciamos o conserto.`, preco:'490' };
          if (tem(['Termostato'])) return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario fazer a troca do Termostato da sua adega. Este conserto completo fica em 490 reais apenas. Aprovando ja iniciamos o conserto.`, preco:'490' };
          if (tem(['Kit de Partida do Motor'])) return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario fazer a troca do Kit de Partida do Motor da sua adega. Este conserto completo fica em 490 reais apenas. Aprovando ja iniciamos o conserto.`, preco:'490' };
          if (tem(['Sensor'])) return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario fazer a troca do Sensor da sua adega. Este conserto completo fica em 490 reais apenas. Aprovando ja iniciamos o conserto.`, preco:'490' };
          if (tem(['Controlador'])) return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario fazer a troca do Controlador da sua adega. Este conserto completo fica em 490 reais apenas. Aprovando ja iniciamos o conserto.`, preco:'490' };
          if (tem(['Porta'])) return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nPara fazer a desmontagem, instalacao da Porta, montagem e regulagem da sua adega consigo fazer para voce por ${x2(precoInput)} reais apenas. Aprovando ja iniciamos o conserto.`, preco:x2(precoInput) };
          if (tem(['Recuperação de Placa'])) return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario refazer a parte eletrica que causou danos no conjunto da placa principal, será feito a reoperação da placa tambem. Este conserto completo fica em 490 reais apenas. Aprovando ja iniciamos o conserto.`, preco:'490' };
          if (tem(['Troca de Placa'])) return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario fazer a troca do conjunto conjunto da Placa Principal, será feito a reoperação eletrica tambem. Este conserto completo fica em ${x2(precoInput)} reais apenas. Aprovando ja iniciamos o conserto.`, preco:x2(precoInput) };
        }
        if (subtipo === 'Eletrônico') {
          if (tem(['Kit Termo Elétrico'])) return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario refazer a parte eletrica que causou danos no conjunto do cooler, placa de resfriamento e pasta termica, as pecas serao trocadas tambem. Este conserto completo fica em 350 reais apenas. Aprovando ja iniciamos o conserto.`, preco:'350' };
          if (tem(['Sensor'])) return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario fazer a troca do Sensor da sua adega. Este conserto completo fica em 390 reais apenas. Aprovando ja iniciamos o conserto.`, preco:'390' };
          if (tem(['Termoelétrico Duplo'])) return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario fazer a troca do Termoeletrico Duplo da sua adega. Este conserto completo fica em 490 reais apenas. Aprovando ja iniciamos o conserto.`, preco:'490' };
          if (tem(['Recuperação de Placa'])) return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario refazer a parte eletrica que causou danos no conjunto da placa principal, será feito a reoperação da placa tambem. Este conserto completo fica em 350 reais apenas. Aprovando ja iniciamos o conserto.`, preco:'350' };
          if (tem(['Troca de Placa'])) return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario fazer a troca do conjunto conjunto da Placa Principal, será feito a reoperação eletrica tambem. Este conserto completo fica em ${x2(precoInput)} reais apenas. Aprovando ja iniciamos o conserto.`, preco:x2(precoInput) };
        }
      }
      if (tipo === 'forno') {
        const pb = subtipo === 'Grande' ? '890' : '490';
        if (tem(['Troca de Placa','Display'])) {
          const p = pecas(['Troca de Placa','Display']);
          return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario fazer a troca do conjunto conjunto do ${p}: será feito a reoperação eletrica tambem. Este conserto completo fica em ${x2(precoInput)} reais apenas. Aprovando ja iniciamos o conserto.`, preco:x2(precoInput) };
        }
        if (tem(['Porta','Vidro','Mola'])) {
          const p = pecas(['Porta','Vidro','Mola']);
          return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orçamento:\n\nPara fazer a desmontagem, instalação da ${p}, montagem e regulagem consigo fazer para você por ${x2(precoInput)} reais apenas. Aprovando ja iniciamos o conserto.`, preco:x2(precoInput) };
        }
        const p = s.join(', ');
        return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario refazer a parte eletrica que causou danos no conjunto da ${p}, será feito a reoperação da placa tambem. Este conserto completo fica em ${pb} reais apenas. Aprovando ja iniciamos o conserto.`, preco:pb };
      }
      return { texto: null, preco: null };
    }

  // ══════════════════════════════════════════════════════════════════════
  // 🔒 DIAGNÓSTICO DE LOJA — ISOLADO
  // Grava SOMENTE no banco do Frente de Loja. O bot lê reparoeletro_logistica
  // e tv_logistica; nada aqui encosta nesses bancos, então o orçamento NÃO sai
  // sozinho — só quando alguém clicar em "Enviar Orçamento WhatsApp".
  // ══════════════════════════════════════════════════════════════════════
  if (req.method === 'POST' && action === 'diagnostico-loja') {
    const { id, equips, observacao } = req.body || {};
    if (!id) return res.status(400).json({ ok:false, error:'id obrigatorio' });
    if (!Array.isArray(equips) || !equips.length) return res.status(400).json({ ok:false, error:'informe ao menos um equipamento' });
    const db = await dbGet(FL_KEY) || defaultDB();
    const ficha = db.fichas.find(f => f.id === id);
    if (!ficha) return res.status(404).json({ ok:false, error:'ficha nao encontrada' });

    // 🎯 USA A MESMA INTELIGÊNCIA DA LOGÍSTICA: templates por tipo/serviço e as regras
    // de preço já validadas na operação (dobro do custo, tabelas fixas, TV por polegada).
    let customTemplates = {};
    try { const t = await dbGet('reparoeletro_orc_templates'); if (t) customTemplates = t; } catch (e) {}

    const calc = [];
    const textos = [];
    for (let i = 0; i < equips.length; i++) {
      const e = equips[i] || {};
      const rot = equips.length > 1 ? ('Equipamento ' + (i+1)) : 'Equipamento';
      if (!e.tipo) return res.status(400).json({ ok:false, error:'selecione o tipo do ' + rot });
      if (!Array.isArray(e.servicos) || !e.servicos.length) return res.status(400).json({ ok:false, error:'selecione ao menos um serviço do ' + rot });
      if (e.tabela === 'dinamica' && !(parseFloat(String(e.valorEquip||'').replace(',','.')) > 0)) {
        return res.status(400).json({ ok:false, error:'informe o valor do equipamento (' + rot + ' — Tabela Dinâmica)' });
      }
      // preço de entrada: na tabela dinâmica o custo é 20% do valor do equipamento
      // (a função dobra depois, chegando aos 40% praticados)
      const entrada = e.tabela === 'dinamica'
        ? Math.round(parseFloat(String(e.valorEquip).replace(',','.')) * 0.2)
        : (parseFloat(String(e.preco||'').replace(',','.')) || null);

      const r = gerarTextoLoja(e.tipo, e.subtipo || null, e.servicos, entrada, customTemplates, ficha.nomeContato);
      // PISO (mesma regra da logística): adega8=390 · adega12=450 · micro-ondas=370.
      // A tabela dinâmica não recebe piso. Acima do piso, o maior prevalece.
      const _PISOS = { adega8: 390, adega12: 450 };
      const _pisoEq = e.tabela === 'dinamica' ? 0 : (_PISOS[e.tabela] || (e.tipo === 'microondas' ? 370 : 0));
      if (r && r.preco && _pisoEq) {
        const _at = parseInt(String(r.preco).replace(/\D/g,''), 10) || 0;
        if (_at < _pisoEq) {
          const _rx = new RegExp(String(r.preco).replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'g');
          r.texto = String(r.texto || '').replace(_rx, String(_pisoEq));
          r.preco = String(_pisoEq);
        }
      }
      const precoCalc = r && r.preco ? Math.round(parseFloat(String(r.preco).replace(/[^\d.,]/g,'').replace(',','.'))) : null;
      if (!precoCalc) {
        return res.status(400).json({ ok:false,
          error: 'não consegui calcular o preço do ' + rot + ' — informe o preço de custo ou revise os serviços selecionados' });
      }
      calc.push({ modelo:String(e.modelo||'').slice(0,60), tipo:e.tipo, subtipo:e.subtipo||null,
        servicos:e.servicos, tabela:e.tabela||'normal',
        valorEquip: e.tabela==='dinamica' ? parseFloat(String(e.valorEquip).replace(',','.')) : null,
        preco: precoCalc, textoOriginal: r.texto });
      textos.push(r.texto);
    }
    const total = calc.reduce((s,e) => s + (e.preco||0), 0);
    const comDesconto = total > 0 ? Math.round(total * 0.9) : null;   // 10% do cliente de loja
    if (!comDesconto) return res.status(400).json({ ok:false, error:'orçamento sem preço — revise os equipamentos' });

    // texto final: o mesmo da logística, com o valor JÁ COM DESCONTO no lugar do original
    const primeiro = String(ficha.nomeContato||'cliente').trim().split(/\s+/)[0];
    let texto;
    const listaTxt = calc.map(e => (e.modelo ? e.modelo + ' — ' : '') + (e.servicos||[]).join(', ')).join(' | ');
    if (calc.length === 1) {
      // troca o valor original pelo com desconto, preservando o template
      texto = String(textos[0]).replace(new RegExp('\\b' + calc[0].preco + '\\b'), String(comDesconto));
    } else {
      texto = `Ola, ${primeiro} bom dia, sou o Pedro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario: ${listaTxt}. Este conserto completo fica em ${comDesconto} reais apenas. Aprovando ja iniciamos o conserto.`;
    }

    const now = new Date().toISOString();
    ficha.diagnosticoLoja = { equips: calc, observacao: String(observacao||'').slice(0,600),
      total, totalComDesconto: comDesconto, descontoAplicado: '10%', texto, em: now };
    ficha.descricaoTecnica = listaTxt + (observacao ? ' · ' + observacao : '');
    ficha.valorOrcamento   = comDesconto;
    ficha.orcEnviadoWpp    = false;                 // 🔒 nasce NÃO enviado
    ficha.phase   = 'orcamento_cadastrado';
    ficha.movedAt = now;
    ficha.history = (ficha.history||[]).concat([{ phase:'orcamento_cadastrado', ts:now, via:'diagnostico-loja' }]);
    await dbSet(FL_KEY, db);
    // 🎯 tira o card da coluna Análise Loja no Técnico — o diagnóstico já foi feito
    let saiuDoTecnico = false;
    try {
      const KRB = (process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim();
      const rb = await fetch(`https://reparoeletroadm.com/api/board?action=remove-analise-card&k=${KRB}`, {
        method:'POST', headers:{'Content-Type':'application/json'},
        body: JSON.stringify({ flFichaId: ficha.id }),
      }).then(x=>x.json()).catch(e=>({ ok:false, error:e.message }));
      saiuDoTecnico = !!(rb && rb.ok);
      if (!saiuDoTecnico) ficha.avisoTecnico = 'card pode ter ficado na coluna Análise Loja';
    } catch(e) {}
    logAction({ modulo:'Frente de Loja', fichaId:ficha.id||'', ficha:ficha.nomeContato||'',
      acao:'Diagnóstico de loja', para:'orcamento_cadastrado',
      gatilho: saiuDoTecnico ? 'saiu do Técnico' : 'ATENÇÃO: não saiu do Técnico',
      status: saiuDoTecnico ? 'ok' : 'alerta' }).catch(()=>{});
    return res.status(200).json({ ok:true, ficha, total, totalComDesconto: comDesconto, texto,
      saiuDaColunaTecnico: saiuDoTecnico,
      aviso:'orçamento guardado — NÃO foi enviado ao cliente' });
  }

  // ── 📲 ENVIAR-ORCAMENTO-WPP: dispara o orçamento guardado para o cliente ──
  // É o ÚNICO ponto em que o orçamento de loja sai. Sem clicar aqui, nada é enviado.
  if (req.method === 'POST' && action === 'enviar-orcamento-wpp') {
    const { id } = req.body || {};
    if (!id) return res.status(400).json({ ok:false, error:'id obrigatorio' });
    const db = await dbGet(FL_KEY) || defaultDB();
    const ficha = db.fichas.find(f => f.id === id);
    if (!ficha) return res.status(404).json({ ok:false, error:'ficha nao encontrada' });
    if (ficha.orcEnviadoWpp) return res.status(200).json({ ok:true, jaEnviado:true, msg:'orçamento já foi enviado' });

    const diag = ficha.diagnosticoLoja || {};
    const texto = diag.texto;
    if (!texto) return res.status(400).json({ ok:false, error:'esta ficha não tem orçamento salvo — faça o diagnóstico primeiro' });

    const tel = String(ficha.telefone || ficha.tel || '').replace(/\D/g,'');
    if (tel.length < 10) {
      ficha.orcEnvioErro = 'telefone inválido ou ausente';
      ficha.orcEnvioTentadoEm = new Date().toISOString();
      await dbSet(FL_KEY, db);
      return res.status(200).json({ ok:false, error:'telefone inválido ou ausente na ficha', ficha });
    }
    const to = tel.startsWith('55') ? tel : '55' + tel;
    const KEV = (process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim();
    const primeiro = String(ficha.nomeContato||'cliente').trim().split(/\s+/)[0];

    // 1) tenta mensagem direta (janela aberta)
    let r = await fetch(`https://reparoeletroadm.com/api/wa-bot?action=enviar&k=${KEV}`, {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ tel: to, texto, via:'frenteloja-orcamento' }),
    }).then(x=>x.json()).catch(e=>({ ok:false, error:e.message }));
    let via = 'mensagem';

    // 2) janela fechada → abre com template e envia o orçamento em seguida
    if (!r || !r.ok) {
      const rt = await fetch(`https://reparoeletroadm.com/api/wa-bot?action=template-conserto&k=${KEV}`, {
        method:'POST', headers:{'Content-Type':'application/json'},
        body: JSON.stringify({ tel: to, nome: primeiro }),
      }).then(x=>x.json()).catch(e=>({ ok:false, error:e.message }));
      if (rt && rt.ok) {
        await new Promise(s => setTimeout(s, 1200));
        r = await fetch(`https://reparoeletroadm.com/api/wa-bot?action=enviar&k=${KEV}`, {
          method:'POST', headers:{'Content-Type':'application/json'},
          body: JSON.stringify({ tel: to, texto, via:'frenteloja-orcamento' }),
        }).then(x=>x.json()).catch(e=>({ ok:false, error:e.message }));
        via = 'template + mensagem';
      } else {
        r = { ok:false, error:(rt && (rt.error||rt.erro)) || 'não consegui abrir a conversa' };
      }
    }

    const agora = new Date().toISOString();
    if (r && r.ok) {
      ficha.orcEnviadoWpp   = true;
      ficha.orcEnviadoWppEm = agora;
      ficha.orcEnvioVia     = via;
      delete ficha.orcEnvioErro;
      // registra a ORIGEM para a aprovação rotear pela ferramenta do Frente de Loja
      try {
        const env = (await dbGet('wa_orc_enviados')) || { ids:{} };
        if (!env.ids) env.ids = {};
        env.ids['fl:' + ficha.id] = { em: agora, origem:'frenteloja', fichaId: ficha.id, telefone: to };
        await dbSet('wa_orc_enviados', env);
      } catch(e) {}
    } else {
      ficha.orcEnvioErro = String((r && r.error) || 'falha no envio').slice(0,140);
      ficha.orcEnvioTentadoEm = agora;
    }
    await dbSet(FL_KEY, db);
    logAction({ modulo:'Frente de Loja', fichaId:ficha.id, ficha:ficha.nomeContato||'',
      acao:'Enviar orçamento WhatsApp', gatilho: via, status: (r&&r.ok)?'ok':'erro' }).catch(()=>{});
    return res.status(200).json({ ok: !!(r && r.ok), via, ficha,
      valor: diag.totalComDesconto,
      error: (r && r.ok) ? undefined : ficha.orcEnvioErro });
  }

  // ── 📣 MARCAR-AVISADO: avisa o cliente pelo WhatsApp que o equipamento está pronto ──
  if (req.method === 'POST' && action === 'marcar-avisado') {
    const { id } = req.body || {};
    if (!id) return res.status(400).json({ ok:false, error:'id obrigatorio' });
    const db = await dbGet(FL_KEY) || defaultDB();
    const ficha = db.fichas.find(f => f.id === id);
    if (!ficha) return res.status(404).json({ ok:false, error:'ficha nao encontrada' });

    const envio = await avisarProntoRetirada(ficha);
    gravarAviso(ficha, envio);
    await dbSet(FL_KEY, db);
    return res.status(200).json({ ok:true, ficha, envio });
  }

  if (req.method === 'POST' && action === 'marcar-orc-whatsapp') {
    const { id } = req.body || {};
    if (!id) return res.status(400).json({ ok:false, error:'id obrigatorio' });
    const db = await dbGet(FL_KEY) || defaultDB();
    const ficha = db.fichas.find(f => f.id === id);
    if (!ficha) return res.status(404).json({ ok:false, error:'ficha nao encontrada' });
    ficha.orcEnviadoWpp     = true;
    ficha.orcEnviadoWppEm   = new Date().toISOString();
    await dbSet(FL_KEY, db);
    return res.status(200).json({ ok:true, id });
  }

  // Dispara o aviso pendente antes de salvar (marcado nas transições para conserto_realizado)
  async function processarAvisoPendente(ficha) {
    if (!ficha || !ficha._avisarPronto) return;
    delete ficha._avisarPronto;
    if (ficha.avisadoPronto) return;              // já avisado, não repete
    const envio = await avisarProntoRetirada(ficha);
    gravarAviso(ficha, envio);
  }

  if(req.method==='POST'&&action==='conserto-realizado'){
    const {pipefyCardId, fichaId}=req.body||{};
    if(!pipefyCardId && !fichaId)
      return res.status(400).json({ok:false,error:'pipefyCardId ou fichaId obrigatório'});
    const db=await dbGet(FL_KEY)||defaultDB();
    // Buscar por fichaId (preferencial, sem Pipefy) ou por pipefyCardId (legado)
    const ficha = fichaId
      ? db.fichas.find(f=>f.id===String(fichaId))
      : db.fichas.find(f=>f.pipefyCardId===String(pipefyCardId));
    if(!ficha)return res.status(404).json({ok:false,error:'Ficha não encontrada'});
    const now=new Date().toISOString();
    ficha.phase='conserto_realizado';ficha.liberadoHoje=true;ficha.movedAt=now;
    ficha.history=(ficha.history||[]).concat([{phase:'conserto_realizado',ts:now}]);
    await dbSet(FL_KEY,db);
    logAction({ modulo:'Frente de Loja', fichaId:ficha.id||'', ficha:ficha.nomeContato||'', acao:'Liberar equipamento', para:'receber', gatilho:'→ Pipe receber + Pipefy Receber$', status:'ok', detalhe:'Valor: R$'+(ficha.pagoValor||0)+' '+ficha.pagoPor }).catch(()=>{});
    return res.status(200).json({ok:true,ficha});
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
    // Mover no Pipefy para "Receber $"
    try {
      const phId = await getPipefyPhaseId('receber');
      if (phId) {
        // Tentar pelo pipefyCardId da ficha, senão buscar no board pelo flFichaId
        let cardId = ficha.pipefyCardId || null;
        if (!cardId) {
          const boardDb = await dbGet('reparoeletro_board');
          const card = (boardDb?.cards||[]).find(c => c.flFichaId === ficha.id && c.pipefyId && !String(c.pipefyId).startsWith('FL-'));
          if (card) cardId = card.pipefyId;
        }
        if (cardId) {
          await movePipefyCard(String(cardId), phId);
          console.log('[FL] liberar→Receber$:', ficha.id, cardId, phId);
        } else {
          console.warn('[FL] liberar: pipefyCardId nulo para', ficha.id);
        }
      } else {
        console.warn('[FL] liberar: fase Receber$ nao encontrada no Pipefy');
      }
    } catch(e) { console.error('[FL] liberar→Pipefy:', e.message); }
    await dbSet(FL_KEY,db);
    // Pipe ADM: mover/registrar em 'receber' — sempre, com ou sem pipefyCardId
    await registrarNoPipe({
      pipefyId:    ficha.pipefyCardId || null,
      fichaId:     ficha.id,
      phase:       'receber',
      nomeContato: ficha.nomeContato || '',
      telefone:    ficha.telefone    || '',
      equipamento: ficha.equipamento || (ficha.orcamento?.equipamento) || '',
      descricao:   ficha.descricaoTecnica || ficha.descricao || '',
      valor:       ficha.pagoValor || ficha.orcamento?.valor || 0,
      origem:      'frenteloja_liberar'
    }).catch(() => {});
    logAction({ modulo:'Frente de Loja', fichaId:ficha.id||'', ficha:ficha.nomeContato||'', acao:'Liberar equipamento', para:'receber', gatilho:'→ Pipe receber + Pipefy Receber$', status:'ok', detalhe:'Valor: R$'+(ficha.pagoValor||0)+' '+ficha.pagoPor }).catch(()=>{});
    return res.status(200).json({ok:true,ficha});
  }




  // ── GET fix-board-lote — corrige todas as fichas no balcao sem card no board ──
  if (action === 'fix-board-lote') {
    try {
      const flDb    = await dbGet(FL_KEY) || defaultDB();
      const balcao  = (await dbGet('reparoeletro_balcao')) || [];
      const boardDb = (await dbGet('reparoeletro_board')) || { cards: [] };
      const boardCards = boardDb.cards || [];

      // IDs das fichas que estão no balcão
      const idsBalcao = new Set(balcao.map(b => b.flFichaId).filter(Boolean));
      // IDs das fichas que JÁ estão no board
      const idsBoard  = new Set(boardCards.map(c => c.flFichaId).filter(Boolean));

      // Fichas que estão no balcão mas NÃO no board
      const fichasSemBoard = (flDb.fichas || []).filter(f =>
        idsBalcao.has(f.id) && !idsBoard.has(f.id)
      );

      const resultados = [];
      const base = process.env.FL_BASE_URL || 'https://reparoeletroadm.com';

      for (const ficha of fichasSemBoard) {
        const titulo = (
          ficha.nomeContato + ' (Loja) - ' + (ficha.equipamento||'') +
          ' | ' + (ficha.descricao||'') +
          ' | Diag: ' + (ficha.descricaoTecnica||'') +
          ' | R$' + String(parseFloat(ficha.orcamento?.valor||0).toFixed(2)) +
          ' OS:' + ficha.id
        ).replace(/"/g,"'").slice(0,255);

        const r = await fetch(base+'/api/board?action=add-loja-card&k=tfk-re2026-Bx7mQp9zKw4Y', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            flFichaId:   ficha.id,
            pipefyId:    ficha.pipefyCardId || null,
            title:       titulo,
            nomeContato: ficha.nomeContato || '',
            telefone:    ficha.telefone || '',
            phaseId:     'cliente_loja',
          })
        }).then(r=>r.json()).catch(e=>({ ok:false, error:e.message }));

        resultados.push({
          id:      ficha.id,
          nome:    ficha.nomeContato,
          tel:     ficha.telefone,
          board:   r.ok ? 'corrigido' : ('erro: ' + (r.error||r.msg||'?'))
        });
      }

      return res.status(200).json({
        ok: true,
        semBoard: fichasSemBoard.length,
        corrigidas: resultados.filter(r=>r.board==='corrigido').length,
        resultados
      });
    } catch(e) {
      return res.status(500).json({ ok:false, error: e.message });
    }
  }


  // ── GET remover-board-lote — remove do board as fichas adicionadas erroneamente ──
  if (action === 'remover-board-lote') {
    const IDS_REMOVER = ["FL-0226", "FL-0224", "FL-0223", "FL-0220", "FL-0219", "FL-0217", "FL-0215", "FL-0213", "FL-0212", "FL-0208", "FL-0205", "FL-0204", "FL-0200", "FL-0198", "FL-0197", "FL-0196", "FL-0194", "FL-0192", "FL-0190", "FL-0188", "FL-0183", "FL-0182", "FL-0180", "FL-0179", "FL-0178", "FL-0176", "FL-0175", "FL-0174", "FL-0173", "FL-0172", "FL-0171", "FL-0168", "FL-0165", "FL-0163", "FL-0161", "FL-0160", "FL-0159", "FL-0158", "FL-0157", "FL-0156", "FL-0155", "FL-0153", "FL-0151"];
    try {
      const boardDb = (await dbGet('reparoeletro_board')) || { cards: [] };
      const antes = (boardDb.cards || []).length;
      boardDb.cards = (boardDb.cards || []).filter(function(c){
        return !IDS_REMOVER.includes(c.flFichaId);
      });
      const removidos = antes - boardDb.cards.length;
      await dbSet('reparoeletro_board', boardDb);
      return res.status(200).json({ ok:true, removidos, totalRestante: boardDb.cards.length });
    } catch(e){ return res.status(500).json({ok:false,error:e.message}); }
  }


  // ── POST confirmar-pagamento-pipe — confirma pagamento e move para ERP no pipe ──
  if (req.method === 'POST' && action === 'confirmar-pagamento-pipe') {
    const { id, valor, formaPagamento } = req.body || {};
    if (!id) return res.status(400).json({ ok:false, error:'id obrigatório' });
    const db = await dbGet(FL_KEY) || defaultDB();
    const ficha = db.fichas.find(f => f.id === id);
    if (!ficha) return res.status(404).json({ ok:false, error:'Ficha não encontrada' });
    const now = new Date().toISOString();
    // Marcar pagamento na ficha FL
    ficha.pagamentoConfirmado = true;
    ficha.pagoEm = now;
    ficha.pagoValor = parseFloat(valor) || ficha.orcamento?.valor || 0;
    ficha.pagoPor = formaPagamento || ficha.orcamento?.formaPagamento || 'pix';
    ficha.phase = 'pago';
    ficha.movedAt = now;
    await dbSet(FL_KEY, db);
    // Criar/mover card no pipe ADM para ERP
    try {
      const _U=(process.env.UPSTASH_URL||'').replace(/['"]/g,'').trim();
      const _T=(process.env.UPSTASH_TOKEN||'').replace(/['"]/g,'').trim();
      async function _pg(k){const r=await fetch(_U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+_T,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});const j=await r.json();const v=j[0]?.result;if(!v)return null;try{let x=JSON.parse(v);if(typeof x==='string')x=JSON.parse(x);return x;}catch(e){return null;}}
      async function _ps(k,v){await fetch(_U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+_T,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});}
      const pipeDb = (await _pg('reparoeletro_pipe')) || { cards:[], lastSync:null };
      if (!Array.isArray(pipeDb.cards)) pipeDb.cards = [];
      // Verificar se já existe no pipe
      const jaExiste = pipeDb.cards.find(c => c.localId===id || c.flFichaId===id);
      if (jaExiste) {
        jaExiste.phase = 'erp';
        jaExiste.movedAt = now;
      } else {
        pipeDb.cards.unshift({
          id: 'PIPE-FL-'+Date.now().toString(36).toUpperCase()+'-'+Math.random().toString(36).slice(2,4).toUpperCase(),
          localId: id, flFichaId: id,
          phase: 'erp',
          nomeContato: ficha.nomeContato||'', telefone: ficha.telefone||'',
          equipamento: ficha.equipamento||'', descricao: ficha.descricao||ficha.descricaoTecnica||'',
          valor: ficha.pagoValor||0, origem: 'frenteloja_pago',
          criadoEm: now, movedAt: now, aguardandoDesde: null, history: [], analiseCompra: false
        });
      }
      pipeDb.lastSync = now;
      await _ps('reparoeletro_pipe', pipeDb);
      console.log('[FL] pago → pipe ERP:', id);
    } catch(ep){ console.error('[FL] pipe ERP:', ep.message); }
    return res.status(200).json({ ok:true, ficha });
  }


  // ── GET inserir-no-pipe — insere fichas do balcão no pipe em 'receber' ────
  if (action === 'inserir-no-pipe') {
    const db      = await dbGet(FL_KEY) || defaultDB();
    const balcao  = (await dbGet('reparoeletro_balcao')) || [];
    // Fichas no balcão que ainda não foram pagas
    const pendentes = balcao.filter(b => b.status !== 'pago');
    const resultados = [];
    for (const entry of pendentes) {
      const ficha = (db.fichas||[]).find(f => f.id === entry.flFichaId || f.id === entry.osCode);
      if (!ficha) { resultados.push({ id: entry.flFichaId, erro: 'ficha nao encontrada' }); continue; }
      try {
        await registrarNoPipe({
          pipefyId:    ficha.pipefyCardId || null,
          fichaId:     ficha.id,
          phase:       'receber',
          nomeContato: ficha.nomeContato || '',
          telefone:    ficha.telefone    || '',
          equipamento: ficha.equipamento || (ficha.orcamento?.equipamento) || '',
          descricao:   ficha.descricaoTecnica || ficha.descricao || '',
          valor:       ficha.pagoValor || ficha.orcamento?.valor || 0,
          origem:      'frenteloja_balcao'
        });
        resultados.push({ id: ficha.id, nome: ficha.nomeContato, status: 'inserido' });
      } catch(e) {
        resultados.push({ id: ficha.id, nome: ficha.nomeContato, erro: e.message });
      }
    }
    return res.status(200).json({ ok:true, total: pendentes.length, resultados });
  }


  // ── GET fichas-antigas — lista fichas >48h em frente de caixa e balcão ──
  if (action === 'fichas-antigas') {
    const LIMITE_MS = 48 * 60 * 60 * 1000;
    const agora = Date.now();
    const flDb   = await dbGet(FL_KEY) || defaultDB();
    const balcao = (await dbGet('reparoeletro_balcao')) || [];
    const result = { frenteDeCaixa: [], balcao: [], geradoEm: new Date().toISOString() };

    // Frente de caixa: fichas ativas (não pagas, não entregues)
    const fasesAtivas = ['analise','orcamento','conserto','conserto_realizado','aguardando_aprovacao','aprovado'];
    (flDb.fichas||[]).forEach(f => {
      const fase = f.phase||'';
      const ativa = fasesAtivas.some(fa => fase.includes(fa)) || !['pago','entregue','cancelado','arquivado'].includes(fase);
      if (!ativa) return;
      const entrada = new Date(f.criadoEm||f.createdAt||f.addedAt||0).getTime();
      const horas = Math.floor((agora - entrada) / (1000*60*60));
      if (horas >= 48) {
        result.frenteDeCaixa.push({
          id:       f.id,
          nome:     f.nomeContato || f.nome || '—',
          telefone: f.telefone || '—',
          fase:     f.phase || '—',
          entrada:  f.criadoEm || f.createdAt || '—',
          horas,
        });
      }
    });

    // Balcão: fichas não pagas
    balcao.forEach(b => {
      if (b.status === 'pago') return;
      const entrada = new Date(b.addedAt||b.criadoEm||b.createdAt||0).getTime();
      const horas = Math.floor((agora - entrada) / (1000*60*60));
      if (horas >= 48) {
        result.balcao.push({
          id:       b.pipefyId || b.flFichaId || '—',
          nome:     b.nomeContato || '—',
          telefone: b.telefone || '—',
          entrada:  b.addedAt || b.criadoEm || '—',
          horas,
          descricao: b.descricao || '—',
        });
      }
    });

    // Ordenar por mais antigo primeiro
    result.frenteDeCaixa.sort((a,b) => b.horas - a.horas);
    result.balcao.sort((a,b) => b.horas - a.horas);
    result.totalFC = result.frenteDeCaixa.length;
    result.totalBalcao = result.balcao.length;

    return res.status(200).json({ ok: true, ...result });
  }

  // ── GET fix-board-por-nome — busca ficha por nome e força no board ───────
  if (action === 'fix-board-por-nome') {
    const busca = (req.query.nome || '').toLowerCase().trim();
    if (!busca) return res.status(400).json({ ok:false, error:'Informe ?nome=...' });
    const db = await dbGet(FL_KEY) || defaultDB();
    const fichas = (db.fichas || []).filter(f =>
      (f.nomeContato||'').toLowerCase().includes(busca) ||
      (f.telefone||'').includes(busca)
    );
    if (!fichas.length) return res.status(404).json({ ok:false, error:'Nenhuma ficha encontrada', busca });

    const resultados = [];
    for (const ficha of fichas) {
      const titulo = (ficha.nomeContato + ' (Loja) - ' + (ficha.equipamento||'') +
        ' | ' + (ficha.descricao||'') +
        ' | Diag: ' + (ficha.descricaoTecnica||'') +
        ' | R$' + String(parseFloat(ficha.orcamento?.valor||0).toFixed(2)) +
        ' OS:' + ficha.id
      ).replace(/"/g,"'").slice(0,255);
      const r = await fetch((process.env.FL_BASE_URL||'https://reparoeletroadm.com')+'/api/board?action=add-loja-card&k=tfk-re2026-Bx7mQp9zKw4Y', {
        method: 'POST',
        headers: {'Content-Type':'application/json'},
        body: JSON.stringify({
          flFichaId:   ficha.id,
          pipefyId:    ficha.pipefyCardId || null,
          title:       titulo,
          nomeContato: ficha.nomeContato||'',
          telefone:    ficha.telefone||'',
          phaseId:     'cliente_loja',
        })
      }).then(r=>r.json()).catch(e=>({ok:false,error:e.message}));
      resultados.push({ id: ficha.id, nome: ficha.nomeContato, telefone: ficha.telefone, phase: ficha.phase, board: r });
    }
    return res.status(200).json({ ok:true, encontradas: fichas.length, resultados });
  }

  // ── POST retentar-board — registra no board fichas que ficaram sem card ──
  if (req.method === 'POST' && action === 'retentar-board') {
    const { id } = req.body || {};
    if (!id) return res.status(400).json({ ok:false, error:'id obrigatorio' });
    const db = await dbGet(FL_KEY) || defaultDB();
    const ficha = db.fichas.find(f => f.id === id);
    if (!ficha) return res.status(404).json({ ok:false, error:'Ficha nao encontrada' });
    const titulo = (ficha.nomeContato + ' (Loja) - ' + ficha.equipamento +
      ' | ' + (ficha.descricao||'') +
      ' | Diag: ' + (ficha.descricaoTecnica||'') +
      ' | R$' + String(parseFloat(ficha.orcamento?.valor||0).toFixed(2)) +
      ' ' + (ficha.orcamento?.formaPagamento||'') +
      ' OS:' + ficha.id
    ).replace(/"/g,"'").slice(0,255);
    const boardBase = process.env.FL_BASE_URL || 'https://reparoeletroadm.com';
    const r = await fetch(boardBase+'/api/board?action=add-loja-card&k=tfk-re2026-Bx7mQp9zKw4Y', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({
        flFichaId:   ficha.id,
        pipefyId:    ficha.pipefyCardId || null,
        title:       titulo,
        nomeContato: ficha.nomeContato||'',
        telefone:    ficha.telefone||'',
        phaseId:     'cliente_loja',
      })
    }).then(r=>r.json()).catch(e=>({ok:false,error:e.message}));
    return res.status(200).json({ ok: r.ok, msg: r.msg || null, error: r.error || null });
  }


  // ── GET audit-criar — últimos registros de auditoria de criação de fichas ──
  if (action === 'audit-criar') {
    const aud = (await dbGet('fl_criar_audit')) || { regs: [] };
    return res.status(200).json({ ok: true, total: aud.regs.length, registros: aud.regs.slice(0, 40) });
  }

  // ── GET conferencia-fl2: TODAS as fichas ATIVAS do histórico inteiro (texto puro) ──
  if (req.method === 'GET' && action === 'conferencia-fl2') {
    const flDb = (await dbGet(FL_KEY)) || { fichas: [] };
    const t4 = t => String(t || '').replace(/\D/g, '').slice(-4);
    const ATIVAS = ['analise', 'orcamento_cadastrado', 'producao', 'conserto_realizado'];
    const ativas = (flDb.fichas || []).filter(f => ATIVAS.includes(f.phase));
    const linhas = ativas.slice(0, 300).map(f =>
      [f.id, (f.nomeContato || '').slice(0, 18), t4(f.telefone), f.phase || '', (f.createdAt || '').slice(5, 16)].join('|'));
    res.setHeader('Content-Type', 'text/plain; charset=utf-8');
    return res.status(200).send('TOTAL_FICHAS:' + (flDb.fichas || []).length + ' ATIVAS:' + ativas.length + '\n' + linhas.join('\n'));
  }

  // ── GET conferencia-fl: dump COMPACTO só do FL (texto puro, colável) ──
  if (req.method === 'GET' && action === 'conferencia-fl') {
    const flDb = (await dbGet(FL_KEY)) || { fichas: [] };
    const t4 = t => String(t || '').replace(/\D/g, '').slice(-4);
    const linhas = (flDb.fichas || []).slice(0, 120).map(f =>
      [f.id, (f.nomeContato || '').slice(0, 18), t4(f.telefone), f.phase || '', (f.createdAt || '').slice(5, 16)].join('|'));
    res.setHeader('Content-Type', 'text/plain; charset=utf-8');
    return res.status(200).send('TOTAL:' + (flDb.fichas || []).length + '\n' + linhas.join('\n'));
  }

  // ── GET conferencia-dump: lista enxuta p/ conferência física (FL + pipe: nome, 4 últimos do tel, fase) ──
  if (req.method === 'GET' && action === 'conferencia-dump') {
    const [flDb, pipeDb] = await Promise.all([
      dbGet(FL_KEY).then(v => v || { fichas: [] }),
      dbGet('reparoeletro_pipe').then(v => v || { cards: [] }),
    ]);
    const t4 = t => String(t || '').replace(/\D/g, '').slice(-4);
    const fl = (flDb.fichas || []).map(f => ({ id: f.id, n: f.nomeContato || f.nome || '', t: t4(f.telefone || f.tel), f: f.phase || f.status || '', em: (f.createdAt || '').slice(0, 16), mv: (f.movedAt || '').slice(0, 16) }));
    const pp = (pipeDb.cards || []).map(c => ({ id: c.id, n: c.nomeContato || '', t: t4(c.telefone), f: c.phase || '' }));
    return res.status(200).json({ ok: true, fl, pipe: pp });
  }

  // ── GET remover-fichas-analise?ids=FL-0843,FL-0844 — remove ficha do FL + card da Análise Loja (com backup) ──
  if (action === 'remover-fichas-analise') {
    const ids = String(req.query.ids || '').split(',').map(s => s.trim()).filter(Boolean);
    if (!ids.length) return res.status(400).json({ ok: false, error: 'informe ?ids=FL-0001,FL-0002' });
    try {
      const db = (await dbGet(FL_KEY)) || { fichas: [] };
      const board = (await dbGet('reparoeletro_board')) || { cards: [] };
      const setIds = new Set(ids);
      const fichasRemovidas = (db.fichas || []).filter(f => setIds.has(String(f.id)));
      const cardsRemovidos = (board.cards || []).filter(c =>
        setIds.has(String(c.flFichaId)) && (c.phaseId || c.phase) === 'analise_loja');
      // Backup antes de remover (recuperável)
      const bak = (await dbGet('reparoeletro_fl_removidas_bak')) || { itens: [] };
      bak.itens.unshift({ em: new Date().toISOString(), fichas: fichasRemovidas, cards: cardsRemovidos });
      bak.itens = bak.itens.slice(0, 20);
      await dbSet('reparoeletro_fl_removidas_bak', bak);
      // Remoção
      db.fichas = (db.fichas || []).filter(f => !setIds.has(String(f.id)));
      board.cards = (board.cards || []).filter(c =>
        !(setIds.has(String(c.flFichaId)) && (c.phaseId || c.phase) === 'analise_loja'));
      await dbSet(FL_KEY, db);
      await dbSet('reparoeletro_board', board);
      return res.status(200).json({ ok: true,
        fichasRemovidas: fichasRemovidas.map(f => ({ id: f.id, nome: f.nomeContato })),
        cardsRemovidos: cardsRemovidos.length,
        backup: 'reparoeletro_fl_removidas_bak (últimos 20 lotes)' });
    } catch (e) { return res.status(500).json({ ok: false, error: e.message }); }
  }

  // ── GET diag-ficha?q=3174 — estado da ficha no FL e no board (diagnóstico) ──
  if (action === 'diag-ficha') {
    const q = String(req.query.q || '').toLowerCase();
    if (!q) return res.status(400).json({ ok:false, error:'informe ?q=' });
    const flDb = await dbGet(FL_KEY) || defaultDB();
    const boardDb = await dbGet('reparoeletro_board') || { cards: [] };
    const fichasFL = (flDb.fichas || [])
      .filter(f => (f.nomeContato||'').toLowerCase().includes(q) || (f.telefone||'').replace(/\D/g,'').endsWith(q))
      .map(f => ({ id:f.id, nome:f.nomeContato, tel:f.telefone, phase:f.phase,
                   movedAt:f.movedAt, pipefyCardId:f.pipefyCardId||null, liberadoHoje:!!f.liberadoHoje }));
    const cardsBoard = (boardDb.cards || [])
      .filter(c => (c.title||'').toLowerCase().includes(q))
      .map(c => ({ pipefyId:c.pipefyId||null, title:c.title, phaseId:c.phaseId,
                   movedAt:c.movedAt, flFichaId:c.flFichaId||null }));
    return res.status(200).json({ ok:true, q, fichasFL, cardsBoard });
  }

  // ── GET forcar-conserto?id=X — move ficha específica para conserto_realizado ──
  if (action === 'forcar-conserto') {
    const idF = String(req.query.id || '');
    if (!idF) return res.status(400).json({ ok:false, error:'informe ?id=' });
    const flDb = await dbGet(FL_KEY) || defaultDB();
    const ficha = (flDb.fichas || []).find(f => String(f.id) === idF);
    if (!ficha) return res.status(404).json({ ok:false, error:'ficha não encontrada', id:idF });
    const faseAnt = ficha.phase;
    const now = new Date().toISOString();
    ficha.phase = 'conserto_realizado';
    ficha._avisarPronto = true;
    ficha.liberadoHoje = true;
    ficha.movedAt = now;
    ficha.history = (ficha.history||[]).concat([{ phase:'conserto_realizado', ts:now, via:'forcar-conserto' }]);
    await processarAvisoPendente(ficha);
    await dbSet(FL_KEY, flDb);
    return res.status(200).json({ ok:true, id:ficha.id, nome:ficha.nomeContato, de:faseAnt, para:'conserto_realizado' });
  }

  // ── GET sync-fl — corrige fichas presas (loja_feito board ≠ conserto_realizado FL) ──
  if (action === 'sync-fl') {
    const BOARD_KEY2 = 'reparoeletro_board';
    const boardDb = await dbGet(BOARD_KEY2) || { cards: [] };
    const flDb = await dbGet(FL_KEY) || defaultDB();
    const lojaFeitoCards = (boardDb.cards || []).filter(c => c.phaseId === 'loja_feito');
    let corrigidos = 0;
    for (const card of lojaFeitoCards) {
      // 3 formas: flFichaId > OS: no título > pipefyId × pipefyCardId
      const osMatch = (card.title || '').match(/OS:([a-zA-Z0-9-]+)/);
      let ficha = null;
      if (card.flFichaId)
        ficha = flDb.fichas.find(f => f.id === String(card.flFichaId));
      if (!ficha && osMatch)
        ficha = flDb.fichas.find(f => f.id === String(osMatch[1]));
      if (!ficha && card.pipefyId)
        ficha = flDb.fichas.find(f => f.pipefyCardId && String(f.pipefyCardId) === String(card.pipefyId));
      if (!ficha || ficha.phase !== 'producao') continue;
      // Ficha presa: está em loja_feito no board mas producao no FL → corrigir
      const now = new Date().toISOString();
      ficha.phase = 'conserto_realizado';
      ficha._avisarPronto = true;
      ficha.liberadoHoje = true;
      ficha.movedAt = now;
      ficha.history = (ficha.history || []).concat([{ phase: 'conserto_realizado', ts: now, origem: 'sync-fl' }]);
      await processarAvisoPendente(ficha);
      card.flFichaId = ficha.id; // guardar para notificações futuras
      corrigidos++;
    }
    if (corrigidos > 0) {
      await dbSet(FL_KEY, flDb);
      await dbSet(BOARD_KEY2, boardDb);
    }
    // Corrigir cards de loja criados em producao que deveriam estar em cliente_loja
    const BOARD_KEY3 = 'reparoeletro_board';
    const boardDb2 = await dbGet(BOARD_KEY3) || { cards: [] };
    let corrigidosLoja = 0;
    for(const card of boardDb2.cards){
      if(card.phaseId === 'producao' && card.flFichaId && String(card.id).includes('-loja')){
        card.phaseId = 'cliente_loja';
        corrigidosLoja++;
      }
    }
    if(corrigidosLoja > 0) await dbSet(BOARD_KEY3, boardDb2);
    // Sync retroativo: fichas em analise sem card → analise_loja
    const boardDb4 = await dbGet(BOARD_KEY2) || { cards: [] };
    const boardFlIds = new Set(boardDb4.cards.map(c => c.flFichaId).filter(Boolean));
    const semCard = (flDb.fichas||[]).filter(f => f.phase === 'analise' && !boardFlIds.has(f.id));
    let syncAnalise = 0;
    const _base = process.env.FL_BASE_URL || 'https://reparoeletroadm.com';
    for (const f of semCard) {
      try {
        const t = (f.nomeContato+' (Loja) - '+(f.equipamento||'')+' | '+(f.descricao||'')+' OS:'+f.id).replace(/"/g,"'").slice(0,255);
        await fetch(_base+'/api/board?action=add-loja-card&k=tfk-re2026-Bx7mQp9zKw4Y',{
          method:'POST', headers:{'Content-Type':'application/json'},
          body: JSON.stringify({ flFichaId:f.id, pipefyId:null, title:t, nomeContato:f.nomeContato||'', telefone:f.telefone||'', phaseId:'analise_loja' })
        });
        syncAnalise++;
      } catch(e){ console.error('[sync-fl] analise→board:', e.message); }
    }
    return res.status(200).json({ ok: true, corrigidos, corrigidosLoja, syncAnalise, total: lojaFeitoCards.length });
  }


  // ── GET limpar-erp — remove fichas FL que já foram para ERP/Finalizado no Pipefy ──
  if (action === 'limpar-erp') {
    const db = await dbGet(FL_KEY) || defaultDB();

    // 1. Buscar todas as fases do pipe com seus cards
    let erpIds = new Set();
    let fasesErp = [];
    try {
      const data = await pipefyQ(`query {
        pipe(id: "${PIPE_ID}") {
          phases {
            id name
            cards(first: 100) {
              edges { node { id } }
              pageInfo { hasNextPage endCursor }
            }
          }
        }
      }`);
      const phases = data?.pipe?.phases || [];
      for (const ph of phases) {
        const l = ph.name.toLowerCase();
        if (l.includes('erp') || l.includes('finaliz') || l.includes('conclu') ||
            l.includes('descar') || l.includes('reprov')) {
          fasesErp.push(ph.name);
          ph.cards.edges.forEach(e => erpIds.add(String(e.node.id)));
          // Paginação
          let cursor = ph.cards.pageInfo?.hasNextPage ? ph.cards.pageInfo.endCursor : null;
          while (cursor) {
            const data2 = await pipefyQ(`query {
              pipe(id: "${PIPE_ID}") {
                phases {
                  name
                  cards(first: 100, after: "${cursor}") {
                    edges { node { id } }
                    pageInfo { hasNextPage endCursor }
                  }
                }
              }
            }`);
            const ph2 = (data2?.pipe?.phases||[]).find(p=>p.name===ph.name);
            if (!ph2) break;
            ph2.cards.edges.forEach(e => erpIds.add(String(e.node.id)));
            cursor = ph2.cards.pageInfo?.hasNextPage ? ph2.cards.pageInfo.endCursor : null;
          }
        }
      }
    } catch(e) {
      // Pipefy best-effort: falha no sync não bloqueia
      console.warn('[fl] sync-fl Pipefy falhou:', e.message);
      // erpIds fica vazio — nenhuma ficha removida, sistema continua
    }

    // 2. Encontrar fichas FL cujo pipefyCardId está em ERP/Finalizado
    const antes = db.fichas.length;
    const removidas = db.fichas.filter(f => f.pipefyCardId && erpIds.has(String(f.pipefyCardId)));
    const idsRemovidos = removidas.map(f => ({ id:f.id, nome:f.nomeContato, phase:f.phase, pipefyId:f.pipefyCardId }));

    // 3. Remover do array
    db.fichas = db.fichas.filter(f => !f.pipefyCardId || !erpIds.has(String(f.pipefyCardId)));

    if (removidas.length > 0) await dbSet(FL_KEY, db);

    return res.status(200).json({
      ok: true,
      fasesErp,
      totalErpIds: erpIds.size,
      removidas: idsRemovidos,
      antes,
      depois: db.fichas.length
    });
  }


  // ── GET retry-pipefy-pending — cria cards Pipefy que falharam ──────
  if (action === 'retry-pipefy-pending') {
    const db = await dbGet(FL_KEY) || defaultDB();
    // Fichas pendentes: pipefyPending=true OU producao sem pipefyCardId há mais de 2 min
    const cutoff = new Date(Date.now() - 2 * 60 * 1000).toISOString();
    const pendentes = db.fichas.filter(f =>
      f.phase === 'producao' &&
      !f.pipefyCardId &&
      (f.pipefyPending || (f.movedAt && f.movedAt < cutoff))
    );

    if (pendentes.length === 0) return res.status(200).json({ ok:true, pendentes:0 });

    let criados = 0, erros = 0;
    const aprovadoPhaseId = await getPipefyPhaseId('aprovad').catch(()=>null);
    if (!aprovadoPhaseId) return res.status(200).json({ ok:false, error:'Fase Aprovado nao encontrada', pendentes:pendentes.length });

    for (const ficha of pendentes) {
      try {
        const titleCompleto=(ficha.nomeContato+' (Loja) - '+(ficha.equipamento||'')+
          ' | '+(ficha.descricao||'')+' | Diag: '+(ficha.descricaoTecnica||'')+
          ' | R$'+String(parseFloat(ficha.orcamento?.valor||0).toFixed(2))+
          ' '+(ficha.orcamento?.formaPagamento||'pix')+' OS:'+ficha.id
        ).replace(/"/g,"'").slice(0,255);
        const nomeCard = (ficha.nomeContato+' (Loja)').replace(/"/g,"'").slice(0,255);
        const telCard  = (ficha.telefone||'').replace(/"/g,"'").slice(0,100);
        const data = await pipefyQ(
          'mutation{createCard(input:{pipe_id:"'+PIPE_ID+'" phase_id:"'+aprovadoPhaseId+'" title:"'+titleCompleto+'" fields_attributes:[{field_id:"nome_do_contato" field_value:"'+nomeCard+'"},{field_id:"telefone" field_value:"'+telCard+'"}]}){card{id}}}'
        );
        const pipefyId = data?.createCard?.card?.id || null;
        if (pipefyId) {
          ficha.pipefyCardId = pipefyId;
          ficha.pipefyPending = false;
          // Atualizar no board
          const boardDb = await dbGet('reparoeletro_board') || {};
          const card = (boardDb.cards||[]).find(c => c.flFichaId === ficha.id);
          if (card && (!card.pipefyId || card.pipefyId === ficha.id)) {
            card.pipefyId = String(pipefyId);
            await dbSet('reparoeletro_board', boardDb);
          }
          // Atualizar valor no Pipefy
          const vn = String(parseFloat(ficha.orcamento?.valor||0).toFixed(2));
          pipefyQ('mutation{updateCardField(input:{card_id:"'+pipefyId+'" field_id:"valor_de_contrato" new_value:"'+vn+'"}){success}}').catch(()=>{});
          criados++;
          console.log('[FL] retry-pipefy: criado', pipefyId, 'para', ficha.id);
        }
      } catch(e) {
        erros++;
        console.error('[FL] retry-pipefy:', ficha.id, e.message);
      }
    }

    await dbSet(FL_KEY, db);
    return res.status(200).json({ ok:true, pendentes:pendentes.length, criados, erros });
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
    await dbSet(FL_KEY,db);
    logAction({ modulo:'Frente de Loja', fichaId:ficha.id||'', ficha:ficha.nomeContato||'', acao:'Mover fase', para:phase, gatilho:'', status:'ok' }).catch(()=>{});
    return res.status(200).json({ok:true,ficha});
  }

  // Limpar fichas reprovadas do banco
  if(action==='limpar-reprovados'){
    const db=await dbGet(FL_KEY)||defaultDB();
    const antes=db.fichas.length;
    db.fichas=db.fichas.filter(f=>f.phase!=='reprovado');
    const removidos=antes-db.fichas.length;
    await dbSet(FL_KEY,db);
    return res.status(200).json({ok:true,removidos});
  }

    // ── load-producao: fichas em fase producao ─────────────────────────────
  if (action === 'load-producao') {
    try {
      const flDb = (await dbGet(FL_KEY)) || defaultDB();
      const fichas = (flDb.fichas || []).filter(f => f.phase === 'producao');
      return res.status(200).json({ ok:true, fichas, total:fichas.length });
    } catch(e) { return res.status(500).json({ ok:false, error:e.message }); }
  }

  // ── load-board: todos os cards do board técnico ──────────────────────────
  if (action === 'load-board') {
    try {
      const boardDb = (await dbGet('reparoeletro_board')) || { cards:[] };
      return res.status(200).json({ ok:true, cards: boardDb.cards || [] });
    } catch(e) { return res.status(500).json({ ok:false, error:e.message }); }
  }

  return res.status(404).json({ok:false,error:'Ação não encontrada'});
}
