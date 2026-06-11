
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


// ── Helper: mover card no Pipe ADM pelo pipefyId (sem depender do Pipefy) ──
async function moverNoPipe(pipefyId, novaFase, dados) {
  // pipefyId pode ser null quando Pipefy falhou — usar localId como fallback
  const _refId = pipefyId || (dados && dados.localId) || null;
  if (!_refId) return;
  try {
    const PIPE_KEY_H = 'reparoeletro_pipe';
    const U = (process.env.UPSTASH_URL   || '').replace(/['"]/g,'').trim();
    const T = (process.env.UPSTASH_TOKEN || '').replace(/['"]/g,'').trim();
    async function _pg(k) {
      const r = await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});
      const j = await r.json(); const v = j[0]?.result; if(!v) return null;
      let val=JSON.parse(v); if(typeof val==='string'){try{val=JSON.parse(val);}catch(e){}} return(val&&typeof val==='object')?val:null;
    }
    async function _ps(k,v){await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});}
    const db=(await _pg(PIPE_KEY_H))||{cards:[],syncedPipefyIds:[],lastSync:null};
    const refIdStr = String(_refId);
    const card=(db.cards||[]).find(c=>
      c.pipefyId===refIdStr || c.id===refIdStr ||
      (dados?.localId && (c.id===String(dados.localId) || c.pipefyId===String(dados.localId)))
    );
    const now=new Date().toISOString();
    if(!card){
      if(dados&&dados.nomeContato){
        db.cards.unshift({
          id: 'PIPE-' + Date.now().toString(36).toUpperCase() + '-' + Math.random().toString(36).slice(2,5).toUpperCase(),
          pipefyId: pipefyId ? String(pipefyId) : null,
          localId:  dados.localId ? String(dados.localId) : null,
          phase: novaFase,
          nomeContato: dados.nomeContato||'',
          telefone: dados.telefone||'',
          equipamento: dados.equipamento||'',
          modelo: dados.modelo||'',
          descricao: dados.descricao||'',
          endereco: dados.endereco||'',
          valor: parseFloat(dados.valor||0)||0,
          origem: dados.origem||'sistema',
          criadoEm: now, movedAt: now,
          aguardandoDesde: novaFase==='aguardando_aprovacao' ? now : null,
          history:[], analiseCompra:false
        });
        await _ps(PIPE_KEY_H,db);
      }
      return;
    }
    card.history=(card.history||[]).concat([{phase:card.phase,ts:now}]);
    card.phase=novaFase; card.movedAt=now;
    if(novaFase==='aguardando_aprovacao') card.aguardandoDesde=now;
    if(dados){if(dados.valor!==undefined)card.valor=parseFloat(dados.valor)||0;if(dados.nomeContato)card.nomeContato=dados.nomeContato;if(dados.modelo!==undefined&&dados.modelo)card.modelo=dados.modelo;}
    await _ps(PIPE_KEY_H,db);
  } catch(e){console.error('[pipe-mover]',novaFase,e.message);}
}

// api/logistica.js — Sistema de Logística de Coleta
const U = process.env.UPSTASH_URL;
const T = process.env.UPSTASH_TOKEN;
const LOG_KEY = 'reparoeletro_logistica';

async function dbGet(key) {
  try {
    const r = await fetch(`${U}/get/${key}`, { headers: { Authorization: `Bearer ${T}` } });
    const j = await r.json();
    return j.result ? JSON.parse(j.result) : null;
  } catch(e) { return null; }
}
async function dbSet(key, val) {
  try {
    await fetch(`${U}/set/${key}`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${T}`, 'Content-Type': 'application/json' },
      body: JSON.stringify(val)
    });
    return true;
  } catch(e) { return false; }
}

function defaultDB() { return { fichas: [], nextId: 1 }; }



const ATEND_LOG_KEY = 'reparoeletro_atend_logistica';
async function registrarFichaAtendimento(ficha) {
  try {
    const db = (await dbGet(ATEND_LOG_KEY)) || { fichas: [] };
    if (!Array.isArray(db.fichas)) db.fichas = [];
    if (db.fichas.find(function(f){ return f.id === ficha.id; })) return;
    db.fichas.unshift({
      id: ficha.id, nome: ficha.nome || ficha.nomeContato || '',
      telefone: ficha.telefone || '', equipamento: ficha.equipamento || '',
      defeito: ficha.defeito || '', registradoEm: new Date().toISOString(),
      origem: ficha.origem || 'logistica'
    });
    var cutoff = new Date(Date.now() - 90*24*60*60*1000).toISOString();
    db.fichas = db.fichas.filter(function(f){ return (f.registradoEm||'') > cutoff; });
    await dbSet(ATEND_LOG_KEY, db);
  } catch(e) { console.error('[atend-log]', e.message); }
}

async function registrarPassagem(phase, ficha) {
  try {
    const hoje = new Date().toLocaleDateString('pt-BR', {timeZone:'America/Sao_Paulo'}).split('/').reverse().join('-');
    const db   = (await dbGet('reparoeletro_log_metricas')) || {};
    if (!db[hoje]) db[hoje] = {};
    if (!db[hoje][phase]) db[hoje][phase] = { total: 0, fichas: [] };
    // Compatibilidade: se era número velho, converter
    if (typeof db[hoje][phase] === 'number') db[hoje][phase] = { total: db[hoje][phase], fichas: [] };
    db[hoje][phase].total++;
    if (ficha) {
      db[hoje][phase].fichas.push({
        id: ficha.id, nome: ficha.nome||ficha.nomeContato||'',
        equipamento: ficha.equipamento||'', ts: new Date().toISOString()
      });
      // Limitar lista a 50 por fase por dia
      db[hoje][phase].fichas = db[hoje][phase].fichas.slice(0, 50);
    }
    const cutoff = new Date(); cutoff.setDate(cutoff.getDate() - 90);
    Object.keys(db).forEach(d => { if (new Date(d) < cutoff) delete db[d]; });
    await dbSet('reparoeletro_log_metricas', db);
  } catch(e) { console.error('registrarPassagem:', e.message); }
}


// ── PIPEFY: criar card direto em Aguardando Aprovação ────────
// Padrão idêntico ao api/orcamento.js → createPipefyCard
const PIPE_ID             = '305832912';
const AGUARDANDO_PHASE_ID = '334875152';

async function pipefyQuery() {
  // Pipefy desconectado em 01/06/2026 — ADM opera 100% local (Redis)
  return null;
}

// Busca APENAS start_form_fields — mesma lógica do orcamento.js
let _pipeStructure = null;
async function fetchPipeStructure() {
  if (_pipeStructure) return _pipeStructure;
  const data = await pipefyQuery(`query {
    pipe(id: "${PIPE_ID}") {
      phases { id name }
      start_form_fields { id label type }
    }
  }`).catch(()=>{});
  _pipeStructure = {
    phases: data?.pipe?.phases || [],
    fields: data?.pipe?.start_form_fields || [],
  };
  return _pipeStructure;
}

async function criarCardPipefy() { return null; }


// ── Pipefy é ESPELHO — nunca bloqueia o fluxo local ─────────────────────
async function pipefyBestEffort(fn) {
  try { return await fn(); } catch(e) { console.warn('[Pipefy]', e.message); return null; }
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const action = req.query.action;

  // ── GET load ──────────────────────────────────────────────
  if (action === 'load') {
    const db = await dbGet(LOG_KEY) || defaultDB();
    return res.status(200).json({ ok: true, fichas: db.fichas || [] });
  }



  // ── GET buscar-ficha: encontra ficha por nome ou id ──────────
  if (action === 'buscar-ficha') {
    const q = (req.query.q || '').toLowerCase().trim();
    if (!q) return res.status(400).json({ ok: false, error: 'q obrigatorio' });
    const db = await dbGet(LOG_KEY) || defaultDB();
    const encontradas = db.fichas.filter(f =>
      (f.nome    || '').toLowerCase().includes(q) ||
      (f.id      || '').toLowerCase().includes(q) ||
      (f.telefone|| '').includes(q)
    );
    // Incluir resultado do último fix para diagnóstico
    const fixResult = await dbGet('fix_clarice_result').catch(()=>null);
    return res.status(200).json({ ok: true, total: encontradas.length, fichas: encontradas, fixResult });
  }


  // ── GET listar-sem-pipefy — fichas em orc_registrado sem card no Pipefy ──
  if (action === 'listar-sem-pipefy') {
    const db = await dbGet(LOG_KEY) || defaultDB();
    const sem = db.fichas.filter(f =>
      !f.pipefyCardId && (f.phase === 'orc_registrado' || f.pipefyErro)
    ).map(f => ({
      id:         f.id,
      nome:       f.nome,
      equipamento:f.equipamento || '',
      fase:       f.phase,
      pipefyErro: f.pipefyErro || null,
      criadoEm:   f.criadoEm || ''
    }));
    return res.status(200).json({ ok:true, total: sem.length, fichas: sem });
  }

  // ── GET forcar-pipefy-todos — cria card no Pipefy para todas fichas pendentes ──
  if (action === 'forcar-pipefy-todos') {
    const db     = await dbGet(LOG_KEY) || defaultDB();
    const pendentes = db.fichas.filter(f => !f.pipefyCardId && f.diagnostico);
    const resultado = [];
    for (const ficha of pendentes) {
      try {
        const card = await criarCardPipefy({
          nome:        ficha.nome,
          telefone:    ficha.telefone || '',
          equipamento: ficha.equipamento || '',
          defeito:     ficha.defeito || '',
          endereco:    ficha.endereco || ''
        });
        if (card?.id) {
          ficha.pipefyCardId = String(card.id);
          ficha.pipefyErro   = null;
          const precoFinal = ficha.diagnostico?.preco;
          if (precoFinal) {
            await pipefyQuery(
              `mutation { updateCardField(input: { card_id: "${card.id}", field_id: "valor_de_contrato", new_value: "${precoFinal}" }) { success } }`
            ).catch(() => {});
          }
          await pipefyQuery(
            `mutation { moveCardToPhase(input: { card_id: "${card.id}", destination_phase_id: "${AGUARDANDO_PHASE_ID}" }) { card { id } } }`
          ).catch(() => {});
          // Atualizar reparoeletro_orcamentos para evitar duplicata pelo orc-sync
          try {
            const ORC_KEY2 = 'reparoeletro_orcamentos';
            const orcDb2 = (await dbGet(ORC_KEY2)) || { fichas:[], syncedIds:[], initialized:true };
            const orcIdx = orcDb2.fichas.findIndex(f => f.id === ficha.id || f.pipefyId === ficha.id);
            if (orcIdx >= 0) {
              orcDb2.fichas[orcIdx].id       = String(card.id);
              orcDb2.fichas[orcIdx].pipefyId = String(card.id);
            } else {
              // Ainda não está em orcamentos — adicionar agora
              orcDb2.fichas.unshift({
                id: String(card.id), pipefyId: String(card.id),
                nome: ficha.nome, tel: ficha.telefone||'',
                desc: (ficha.equipamento||'') + ' — ' + (ficha.defeito||''),
                end: ficha.endereco||'', textoOrc: ficha.diagnostico?.textoOrc||'',
                precoSugerido: precoFinal||null, status:'pendente', preco:null,
                createdAt: new Date().toISOString(),
              });
            }
            if (!orcDb2.syncedIds.includes(String(card.id))) {
              orcDb2.syncedIds.push(String(card.id));
            }
            await dbSet(ORC_KEY2, orcDb2);
          } catch(oe) { console.error('[Log] forcar sync orc-key:', oe.message); }
          resultado.push({ id: ficha.id, nome: ficha.nome, pipefyCardId: card.id, ok: true });
        } else {
          resultado.push({ id: ficha.id, nome: ficha.nome, ok: false, erro: 'card sem id' });
        }
      } catch(e) {
        ficha.pipefyErro   = e.message;
        ficha.pipefyErroTs = new Date().toISOString();
        resultado.push({ id: ficha.id, nome: ficha.nome, ok: false, erro: e.message });
      }
    }
    await dbSet(LOG_KEY, db);
    const ok  = resultado.filter(r => r.ok).length;
    const err = resultado.filter(r => !r.ok).length;
    return res.status(200).json({ ok: true, total: pendentes.length, criados: ok, erros: err, resultado });
  }

  // ── GET retry-pipefy: tenta criar card no Pipefy para ficha sem pipefyCardId ──
  if (action === 'retry-pipefy') {
    const id = req.query.id;
    if (!id) return res.status(400).json({ ok: false, error: 'id obrigatorio' });
    const db = await dbGet(LOG_KEY) || defaultDB();
    const ficha = db.fichas.find(f => f.id === id);
    if (!ficha) return res.status(404).json({ ok: false, error: 'ficha nao encontrada' });
    if (ficha.pipefyCardId) {
      return res.status(200).json({ ok: true, info: 'ja tem pipefyCardId', pipefyCardId: ficha.pipefyCardId });
    }
    try {
      const card = await criarCardPipefy({
        nome:        ficha.nome,
        telefone:    ficha.telefone || '',
        equipamento: ficha.equipamento || '',
        defeito:     ficha.defeito || '',
        endereco:    ficha.endereco || ''
      });
      // Pipefy best-effort: continua mesmo sem card
      if (!card?.id) {
        console.warn('[log] Pipefy nao retornou id — salvando ficha sem pipefyCardId');
      } else {
        ficha.pipefyCardId = String(card.id);
      }
      await dbSet(LOG_KEY, db);
      // Atualizar valor se tiver diagnóstico
      const precoFinal = ficha.diagnostico?.preco;
      if (precoFinal) {
        await pipefyQuery(`mutation { updateCardField(input: { card_id: "${card.id}", field_id: "valor_de_contrato", new_value: "${precoFinal}" }) { success } }`).catch(()=>{});
      }
      // Mover para Aguardando Aprovação
      await pipefyQuery(`mutation { moveCardToPhase(input: { card_id: "${card.id}", destination_phase_id: "${AGUARDANDO_PHASE_ID}" }) { card { id } } }`).catch(()=>{});
      return res.status(200).json({ ok: true, pipefyCardId: card.id, url: card.url, nome: ficha.nome });
    } catch(e) {
      return res.status(500).json({ ok: false, error: e.message });
    }
  }

  // ── GET metricas ─────────────────────────────────────────────
  if (action === 'metricas') {
    const MET_KEY = 'reparoeletro_log_metricas';
    const met = (await dbGet(MET_KEY)) || {};
    return res.status(200).json({ ok: true, metricas: met });
  }

  // ── POST criar ────────────────────────────────────────────
  if (req.method === 'POST' && action === 'criar') {
    const { nome, telefone, endereco, equipamento, defeito, pipefyCardId, texto } = req.body || {};
    if (!nome) return res.status(400).json({ ok: false, error: 'nome obrigatorio' });

    const db = await dbGet(LOG_KEY) || defaultDB();
    const id = 'LOG-' + String(db.nextId || 1).padStart(4, '0');
    const ficha = {
      id, nome, telefone: telefone || '', endereco: endereco || '',
      equipamento: equipamento || '', defeito: defeito || '',
      pipefyCardId: pipefyCardId || null, texto: texto || '',
      phase: 'liberado_coleta',
      criadoEm: new Date().toISOString(),
      movedAt: new Date().toISOString(),
      diagnostico: null,
    };
    db.fichas.unshift(ficha);
    db.nextId = (db.nextId || 1) + 1;
    await dbSet(LOG_KEY, db);
    registrarPassagem('liberado_coleta', ficha).catch(() => {});
    registrarFichaAtendimento(ficha).catch(() => {});
    return res.status(201).json({ ok: true, ficha });
  }

  // ── POST mover ────────────────────────────────────────────
  if (req.method === 'POST' && action === 'mover') {
    const { id, phase } = req.body || {};
    const PHASES = ['liberado_coleta','horario_marcado','em_rota','motorista_parceiro','remarcar','coleta_efetuada','orc_registrado'];
    if (!id || !PHASES.includes(phase)) return res.status(400).json({ ok: false, error: 'invalido' });

    const db = await dbGet(LOG_KEY) || defaultDB();
    const ficha = db.fichas.find(f => f.id === id);
    if (!ficha) return res.status(404).json({ ok: false, error: 'nao encontrada' });
    ficha.phase = phase;
    ficha.movedAt = new Date().toISOString();
    await dbSet(LOG_KEY, db);
    registrarPassagem(phase, ficha).catch(() => {});
    if (phase === 'liberado_coleta') registrarFichaAtendimento(ficha).catch(() => {});
    return res.status(200).json({ ok: true, ficha });
  }


  // ── POST marcar-checado: marca ficha como checada (verde), reseta alerta ──
  if (req.method === 'POST' && action === 'marcar-checado') {
    const { id } = req.body || {};
    if (!id) return res.status(400).json({ ok: false, error: 'id obrigatorio' });
    const db = await dbGet(LOG_KEY) || defaultDB();
    const ficha = (db.fichas || []).find(f => f.id === id);
    if (!ficha) return res.status(404).json({ ok: false, error: 'ficha nao encontrada' });
    ficha.checado   = true;
    ficha.checadoEm = new Date().toISOString();
    await dbSet(LOG_KEY, db);
    return res.status(200).json({ ok: true, id, checado: true });
  }

  // ── POST mover-motorista: move para Motorista Parceiro salvando o nome ──
  if (req.method === 'POST' && action === 'mover-motorista') {
    const { id, motoristaNome } = req.body || {};
    if (!id || !motoristaNome) return res.status(400).json({ ok: false, error: 'id e motoristaNome obrigatorios' });
    const db = await dbGet(LOG_KEY) || defaultDB();
    const ficha = db.fichas.find(f => f.id === id);
    if (!ficha) return res.status(404).json({ ok: false, error: 'nao encontrada' });
    ficha.phase        = 'motorista_parceiro';
    ficha.motoristaNome = motoristaNome.trim();
    ficha.movedAt      = new Date().toISOString();
    await dbSet(LOG_KEY, db);
    registrarPassagem('motorista_parceiro', ficha).catch(() => {});
    return res.status(200).json({ ok: true, ficha });
  }


  // ── POST marcar-horario ───────────────────────────────────────
  if (req.method === 'POST' && action === 'marcar-horario') {
    const { id, horario } = req.body || {};
    if (!id || !horario) return res.status(400).json({ ok: false, error: 'id e horario obrigatorios' });
    const db = await dbGet(LOG_KEY) || defaultDB();
    const ficha = db.fichas.find(f => f.id === id);
    if (!ficha) return res.status(404).json({ ok: false, error: 'nao encontrada' });
    ficha.phase          = 'horario_marcado';
    ficha.horarioColeta  = horario; // ISO datetime string
    ficha.movedAt        = new Date().toISOString();
    await dbSet(LOG_KEY, db);
    registrarPassagem('horario_marcado', ficha).catch(() => {});
    return res.status(200).json({ ok: true, ficha });
  }

  // ── POST atualizar-dados ──────────────────────────────────
  if (req.method === 'POST' && action === 'atualizar-dados') {
    const { id, nome, telefone, endereco, equipamento, defeito } = req.body || {};
    const db = await dbGet(LOG_KEY) || defaultDB();
    const ficha = db.fichas.find(f => f.id === id);
    if (!ficha) return res.status(404).json({ ok: false, error: 'nao encontrada' });
    if (nome)       ficha.nome = nome;
    if (telefone)   ficha.telefone = telefone;
    if (endereco)   ficha.endereco = endereco;
    if (equipamento) ficha.equipamento = equipamento;
    if (defeito)    ficha.defeito = defeito;
    await dbSet(LOG_KEY, db);
    return res.status(200).json({ ok: true, ficha });
  }

  // ── POST salvar-diagnostico ───────────────────────────────
  if (req.method === 'POST' && action === 'salvar-diagnostico') {
    const { id, diagnostico } = req.body || {};
    const db = await dbGet(LOG_KEY) || defaultDB();
    const ficha = db.fichas.find(f => f.id === id);
    if (!ficha) return res.status(404).json({ ok: false, error: 'nao encontrada' });
    ficha.diagnostico = diagnostico;
    // Não mover para orc_registrado aqui — a fase muda em gerar-orcamento
    // (só quando o Pipefy for criado/movido com sucesso)
    await dbSet(LOG_KEY, db);
    return res.status(200).json({ ok: true, ficha });
  }


  // ── POST gerar-orcamento — gera texto, salva no orc e move Pipefy ──
  if (req.method === 'POST' && action === 'gerar-orcamento') {
    const { id } = req.body || {};
    const db = await dbGet(LOG_KEY) || defaultDB();
    const ficha = db.fichas.find(f => f.id === id);
    if (!ficha) return res.status(404).json({ ok:false, error:'ficha nao encontrada' });
    if (!ficha.diagnostico) return res.status(400).json({ ok:false, error:'sem diagnostico' });

    const ORC_KEY = 'reparoeletro_orcamentos';

    // Gerar textos para cada equipamento do diagnóstico
    const equips = ficha.diagnostico.equips || [ficha.diagnostico];
    const nome   = ficha.nome || '';

    function priNome(n) { return n ? n.trim().split(/\s+/)[0] : 'cliente'; }

    function gerarTexto(tipo, subtipo, servicos, precoInput, templates) {
      const pn = priNome(nome);
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
        if (tem(['Haste'])) { const tpl = T.microondas_haste?.texto || `Ola, [NOME] bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orçamento:\n\nPara fazer a desmontagem, instalação da haste, montagem e regulagem consigo fazer para você por [VALOR] reais apenas. Aprovando ja iniciamos o conserto.`; return { texto: applyTpl(tpl, 'haste', '350'), preco:'350' }; }
        if (tem(['Pintura'])) { const tpl = T.microondas_pintura?.texto || `Ola, [NOME] bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orçamento:\n\nPara fazer a desmontagem, pintura, montagem, regulagem e revisão consigo fazer para você por [VALOR] reais apenas. Aprovando ja iniciamos o conserto.`; return { texto: applyTpl(tpl, 'pintura', '350'), preco:'350' }; }
        if (tem(['Magnetron'])) {
          const tpl = T.microondas_magnetron?.texto || `Ola, [NOME] bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario fazer a troca do Magnetron, peca responsavel pelo aquecimento do aparelho. Este conserto completo fica em [VALOR] reais apenas. Aprovando ja iniciamos o conserto.`;
          return { texto: applyTpl(tpl, 'Magnetron', '390'), preco:'390' };
        }
        const p = s.join(', ');
        const tpl = T.microondas_eletrico?.texto || `Ola, [NOME] bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario refazer a parte eletrica que causou danos no conjunto do [peças], as pecas serao trocadas tambem. Este conserto completo fica em [VALOR] reais apenas. Aprovando ja iniciamos o conserto.`;
        return { texto: applyTpl(tpl, p, '350'), preco:'350' };
      }
      if (tipo === 'bblend') {
        // Bblend — preço fixo R$ 1.490 independente das peças selecionadas
        const p = s.join(', ') || 'conjunto do motor';
        return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario fazer a troca do ${p}. Este conserto completo fica em 1490 reais apenas. Aprovando ja iniciamos o conserto.`, preco:'1490' };
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
          if (tem(['Recuperação de Placa'])) return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario refazer a parte eletrica que causou danos no conjunto da placa principal, será feito a reoperação da placa tambem. Este conserto completo fica em 490 reais apenas. Aprovando ja iniciamos o conserto.`, preco:'490' };
          if (tem(['Troca de Placa'])) return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario fazer a troca do conjunto conjunto da Placa Principal, será feito a reoperação eletrica tambem. Este conserto completo fica em ${x2(precoInput)} reais apenas. Aprovando ja iniciamos o conserto.`, preco:x2(precoInput) };
        }
        if (subtipo === 'Eletrônico') {
          if (tem(['Kit Termo Elétrico'])) return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario refazer a parte eletrica que causou danos no conjunto do cooler, placa de resfriamento e pasta termica, as pecas serao trocadas tambem. Este conserto completo fica em 350 reais apenas. Aprovando ja iniciamos o conserto.`, preco:'350' };
          if (tem(['Recuperação de Placa'])) return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario refazer a parte eletrica que causou danos no conjunto da placa principal, será feito a reoperação da placa tambem. Este conserto completo fica em 350 reais apenas. Aprovando ja iniciamos o conserto.`, preco:'350' };
          if (tem(['Troca de Placa'])) return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario fazer a troca do conjunto conjunto da Placa Principal, será feito a reoperação eletrica tambem. Este conserto completo fica em ${x2(precoInput)} reais apenas. Aprovando ja iniciamos o conserto.`, preco:x2(precoInput) };
        }
      }
      if (tipo === 'forno') {
        const pb = subtipo === 'Grande' ? '790' : '490';
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

    // Carregar templates customizados do Redis
    let customTemplates = {};
    try {
      const tplDb = await dbGet('reparoeletro_orc_templates');
      if (tplDb) customTemplates = tplDb;
    } catch(e) { console.error('[Log] templates:', e.message); }

    // Gerar texto para cada equipamento
    const resultados = equips.map(eq =>
      gerarTexto(eq.tipo, eq.subtipo, eq.servicos, eq.preco, customTemplates)
    );

    // Texto final
    let textoFinal, precoFinal;
    if (resultados.length === 1) {
      textoFinal = resultados[0].texto;
      precoFinal = resultados[0].preco;
    } else {
      const qtd      = resultados.length;
      const soma     = resultados.reduce((acc,r)=>acc+parseInt(r.preco||0),0);
      const descPerc = qtd === 2 ? 0.10 : qtd === 3 ? 0.15 : 0.20; // max 20%
      const comDesc  = Math.round(soma * (1 - descPerc));
      precoFinal     = String(comDesc);

      // Remover "Aprovando ja iniciamos o conserto" de cada texto individual
      const removeAprovando = (txt) =>
        (txt||'').replace(/\s*Aprovando ja iniciamos o conserto\.?/gi, '').trimEnd();

      // Montar textos individuais sem a frase final
      const partes = resultados.map((r,i) =>
        `Equipamento ${i+1}:\n${removeAprovando(r.texto||'')}`
      ).join('\n\n');

      // Frase de desconto no final
      const fraseFinal = `Consertando os ${qtd} juntos eu consigo um desconto para voce de ${soma} para ${comDesc} reais. Aprovando ja iniciamos o conserto.`;
      textoFinal = partes + '\n\n' + fraseFinal;
    }

    // Salvar na Logística
    ficha.diagnostico.textoOrc = textoFinal;
    ficha.diagnostico.preco    = precoFinal;
    ficha.phase   = 'orc_registrado';
    registrarPassagem('orc_registrado', ficha).catch(() => {});
    registrarPassagem('orc_registrado', ficha).catch(() => {});
    ficha.movedAt = new Date().toISOString();
    await dbSet(LOG_KEY, db);

    // Salvar no Redis de orçamentos (ORC_KEY) — formato compatível com orc-sync
    try {
      const orcDb = (await dbGet(ORC_KEY)) || { fichas:[], syncedIds:[], initialized:true };
      const orcFicha = {
        id:            ficha.pipefyCardId || ficha.id,
        pipefyId:      ficha.pipefyCardId || ficha.id,
        nome:          ficha.nome,
        tel:           ficha.telefone || '',
        desc:          ficha.equipamento + ' — ' + ficha.defeito,
        end:           ficha.endereco || '',
        age:           null,
        comentarios:   [],
        textoOrc:      textoFinal,
        precoSugerido: precoFinal,
        status:        'pendente',
        preco:         null,
        createdAt:     new Date().toISOString(),
      };
      // Evitar duplicata
      if (!orcDb.fichas.find(f => f.id === orcFicha.id)) {
        orcDb.fichas.unshift(orcFicha);
        if (ficha.pipefyCardId && !orcDb.syncedIds.includes(ficha.pipefyCardId)) {
          orcDb.syncedIds.push(ficha.pipefyCardId);
        }
        await dbSet(ORC_KEY, orcDb);
      }
    } catch(e) { console.error('[Log] orc-key:', e.message); }

    // ── Pipe ADM: criar/atualizar card em aguardando_aprovacao (SEM Pipefy) ─────
    const _pipId  = ficha.pipefyCardId || null;
    const _nome   = fmt4dig(ficha.nome || ficha.nomeContato || '', ficha.telefone||'');
    const _tel    = ficha.telefone || '';
    const _equip  = ficha.equipamento || '';
    const _desc   = ficha.defeito || '';
    const _valor  = parseFloat(precoFinal) || 0;
    // Extrair modelo do diagnóstico (primeiro equipamento)
    const _modelo = (ficha.diagnostico?.equips?.[0]?.modelo || '').trim();
    await moverNoPipe(_pipId, 'aguardando_aprovacao', {
      nomeContato: _nome, telefone: _tel,
      equipamento: _equip, descricao: _desc,
      valor: _valor, origem: 'logistica',
      endereco: ficha.endereco || '',
      modelo: _modelo,
      localId: ficha.id  // fallback quando não há pipefyCardId
    }).catch(e => console.error('[Log→Pipe]', e.message));

    // Mover ou CRIAR card no Pipefy em Aguardando Aprovação
    try {
      if (ficha.pipefyCardId) {
        // Card já existe — só mover e atualizar valor
        await pipefyQuery(`mutation { moveCardToPhase(input: { card_id: "${ficha.pipefyCardId}", destination_phase_id: "${AGUARDANDO_PHASE_ID}" }) { card { id } } }`).catch(()=>{});
        if (precoFinal) {
          await pipefyQuery(`mutation { updateCardField(input: { card_id: "${ficha.pipefyCardId}", field_id: "valor_de_contrato", new_value: "${precoFinal}" }) { success } }`).catch(()=>{});
        }
        console.log('[Log] Pipefy movido para Aguardando:', ficha.pipefyCardId);
      } else {
        // Ficha manual — criar card novo direto em Aguardando Aprovação
        const card = await criarCardPipefy({
          nome:        ficha.nome,
          telefone:    ficha.telefone || '',
          equipamento: ficha.equipamento || '',
          defeito:     ficha.defeito || '',
          endereco:    ficha.endereco || ''
        });
        if (card?.id) {
          // Salvar o pipefyCardId na ficha para uso futuro
          ficha.pipefyCardId = String(card.id);
          await dbSet(LOG_KEY, db);
          // Atualizar valor de contrato
          if (precoFinal) {
            await pipefyQuery(`mutation { updateCardField(input: { card_id: "${card.id}", field_id: "valor_de_contrato", new_value: "${precoFinal}" }) { success } }`).catch(()=>{});
          }
          // Atualizar reparoeletro_orcamentos: trocar ID local pelo ID real do Pipefy
          // e adicionar ao syncedIds para orc-sync não duplicar
          try {
            const ORC_KEY2 = 'reparoeletro_orcamentos';
            const orcDb2 = (await dbGet(ORC_KEY2)) || { fichas:[], syncedIds:[], initialized:true };
            // Trocar entrada com id=ficha.id pelo id/pipefyId real
            const orcIdx = orcDb2.fichas.findIndex(f => f.id === ficha.id || f.pipefyId === ficha.id);
            if (orcIdx >= 0) {
              orcDb2.fichas[orcIdx].id       = String(card.id);
              orcDb2.fichas[orcIdx].pipefyId = String(card.id);
            }
            // Garantir que o ID real está em syncedIds
            if (!orcDb2.syncedIds.includes(String(card.id))) {
              orcDb2.syncedIds.push(String(card.id));
            }
            await dbSet(ORC_KEY2, orcDb2);
          } catch(oe) { console.error('[Log] sync orc-key:', oe.message); }
          console.log('[Log] Pipefy card CRIADO:', card.id, card.url);
        }
      }
    } catch(e) {
      console.error('[Log] Pipefy:', e.message);
      // Salvar erro para diagnóstico — não é silencioso
      ficha.pipefyErro = e.message;
      ficha.pipefyErroTs = new Date().toISOString();
      await dbSet(LOG_KEY, db);
    }

    return res.status(200).json({
      ok:true, textoFinal, precoFinal, ficha,
      pipefyOk: !!ficha.pipefyCardId,
      pipefyErro: ficha.pipefyErro || null
    });
  }


  // ── GET limpar-orc-registrado — cron noturno, limpa coluna Orçamento Registrado ──
  if (action === 'limpar-orc-registrado') {
    const db = await dbGet(LOG_KEY) || defaultDB();
    const antes = db.fichas.length;
    // Só deletar fichas em orc_registrado que já têm pipefyCardId confirmado
    // Fichas sem pipefyCardId ficam para retry (não perder dados)
    db.fichas = db.fichas.filter(f =>
      f.phase !== 'orc_registrado' || !f.pipefyCardId
    );
    const removidas = antes - db.fichas.length;
    if (removidas > 0) await dbSet(LOG_KEY, db);
    return res.status(200).json({ ok: true, removidas, restantes: db.fichas.length });
  }

  // ── POST cancelar ────────────────────────────────────────

  // ── POST finalizar-rs: finaliza ficha de garantia sem orçamento ──────────
  if (req.method === 'POST' && action === 'finalizar-rs') {
    const { id } = req.body || {};
    if (!id) return res.status(400).json({ ok: false, error: 'id obrigatorio' });
    const db = await dbGet(LOG_KEY) || defaultDB();
    const ficha = db.fichas.find(f => f.id === id);
    if (!ficha) return res.status(404).json({ ok: false, error: 'nao encontrada' });
    ficha.phase       = 'finalizado_rs';
    ficha.finalizado  = true;
    ficha.finalizadoEm = new Date().toISOString();
    ficha.movedAt     = ficha.finalizadoEm;
    await dbSet(LOG_KEY, db);
    return res.status(200).json({ ok: true, ficha });
  }

  if (req.method === 'POST' && action === 'cancelar') {
    const { id } = req.body || {};
    const db = await dbGet(LOG_KEY) || defaultDB();
    db.fichas = db.fichas.filter(f => f.id !== id);
    await dbSet(LOG_KEY, db);
    return res.status(200).json({ ok: true });
  }

  return res.status(404).json({ ok: false, error: 'ação não encontrada' });
};
