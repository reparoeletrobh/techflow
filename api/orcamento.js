
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



const PIPE_ID    = "305832912";

async function pipefyQuery() {
  // Pipefy desconectado em 01/06/2026 — ADM opera 100% local (Redis)
  return null;
}

async function fetchPipeStructure() {
  const data = await pipefyQuery(`query {
    pipe(id: "${PIPE_ID}") {
      phases { id name }
      start_form_fields { id label type }
    }
  }`);
  return {
    phases: data?.pipe?.phases || [],
    fields: data?.pipe?.start_form_fields || [],
  };
}

// Atualiza o campo Valor total no card do Pipefy
async function updateCardValue(pipefyId, valor) {
  // Campo currency no Pipefy espera número puro ex: "350.00"
  const numerico = String(parseFloat(String(valor).replace(",", ".")) || 0);
  const mutation = `mutation {
    updateCardField(input: {
      card_id: "${pipefyId}"
      field_id: "valor_de_contrato"
      new_value: "${numerico}"
    }) { success }
  }`;
  return await pipefyQuery(mutation);
}

// ── PARSER DE FICHA ─────────────────────────────────────────
// Suporta o formato:
//   Nome: Marcelo
//   Aparelho: Microondas
//   Defeito: Liga mas não esquenta
//   Cep/Endereço: Rua Geórgia 155, Condomínio Pinheiro, ap 504
//   Telefone: +5511979960998
//   Bairro: Estrela Dalva, Belo Horizonte
// A linha Bairro é sempre anexada ao endereço
function parseFichaTexto(txt) {
  const result = { nome:"", telefone:"", aparelho:"", defeito:"", endereco:"" };
  if (!txt) return result;

  const linhas = txt.split(/\r?\n/).map(l => l.trim()).filter(Boolean);
  let bairro = "";

  for (const linha of linhas) {
    const sep = linha.indexOf(":");
    if (sep < 0) continue;
    const chave = linha.slice(0, sep).trim().toLowerCase()
      .normalize("NFD").replace(/[\u0300-\u036f]/g, ""); // remove acentos
    const valor = linha.slice(sep + 1).trim();
    if (!valor) continue;

    if (chave.includes("nome"))                                result.nome     = valor;
    else if (chave.includes("aparelho") || chave.includes("equip")) result.aparelho = valor;
    else if (chave.includes("defeito") || chave.includes("problema")) result.defeito  = valor;
    else if (chave.includes("telefone") || chave.includes("fone") || chave.includes("cel") || chave.includes("whatsapp")) result.telefone = valor;
    else if (chave.includes("cep") || chave.includes("endere") || chave.includes("rua") || chave.includes("av") || chave.includes("logra")) result.endereco = valor;
    else if (chave.includes("bairro") || chave.includes("cidade") || chave.includes("local")) bairro = valor;
  }

  // Anexa bairro ao endereço
  if (bairro && result.endereco) result.endereco += ", " + bairro;
  else if (bairro)               result.endereco = bairro;

  return result;
}

async function createPipefyCard() {
  return { ok: false, error: 'Pipefy desconectado' };
}

module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(200).end();

  const { action } = req.query;

  if (action === "estrutura") {
    try {
      const estrutura = await fetchPipeStructure();
      return res.status(200).json({ ok: true, ...estrutura });
    } catch(e) {
      return res.status(200).json({ ok: false, error: e.message });
    }
  }

  if (req.method === "POST" && action === "criar-card") {
    let { nome, telefone, aparelho, defeito, endereco, phaseId, texto, preco } = req.body || {};

    // Se veio texto bruto, faz o parse
    if (texto && !nome) {
      const parsed = parseFichaTexto(texto);
      nome     = parsed.nome     || nome;
      telefone = parsed.telefone || telefone;
      aparelho = parsed.aparelho || aparelho;
      defeito  = parsed.defeito  || defeito;
      endereco = parsed.endereco || endereco;
    }

    if (!nome || !telefone || !aparelho || !defeito)
      return res.status(400).json({ ok: false, error: "nome, telefone, aparelho e defeito são obrigatórios" });
    // horarioColeta vem quando o cliente escolheu "Agendar Coleta"
    const horarioColeta  = req.body?.horarioColeta  || null;
    const coletaAgendada = req.body?.coletaAgendada || false;
    const coletaDataTexto = req.body?.coletaDataTexto || null;

    try {
      // Pipefy best-effort — não bloqueia se falhar
      let card = null;
      let pipefyErro = null;
      try {
        card = await createPipefyCard({ phaseId, nome, telefone, aparelho, defeito, endereco: endereco || "" });
      } catch(ep) {
        pipefyErro = ep.message;
        console.error('[orcamento] Pipefy falhou (best-effort):', ep.message);
      }

      // Registrar na Logística — SÍNCRONO antes de retornar
      try {
        const U2 = process.env.UPSTASH_URL;
        const T2 = process.env.UPSTASH_TOKEN;
        const LOG_KEY = 'reparoeletro_logistica';
        const logDb = await fetch(`${U2}/get/${LOG_KEY}`, { headers: { Authorization: `Bearer ${T2}` } })
          .then(r=>r.json()).then(j=>j.result ? JSON.parse(j.result) : { fichas:[], nextId:1 });
        const logId = 'LOG-' + String(logDb.nextId || 1).padStart(4,'0');

        // Se agendado → horario_marcado (mesmo se o parse do horário falhou)
        const phaseLogistica  = (horarioColeta || coletaAgendada) ? 'horario_marcado' : 'liberado_coleta';

        logDb.fichas.unshift({
          id: logId, nome, telefone: telefone||'', endereco: endereco||'',
          equipamento: aparelho||'', defeito: defeito||'',
          pipefyCardId: card?.id || null, texto: texto||'',
          phase: phaseLogistica,
          horarioColeta: horarioColeta || null,
          horarioColetaTexto: coletaDataTexto || null, // texto original digitado pelo operador
          criadoEm: new Date().toISOString(),
          movedAt: new Date().toISOString(),
          diagnostico: null,
        });
        logDb.nextId = (logDb.nextId || 1) + 1;
        await fetch(`${U2}/set/${LOG_KEY}`, {
          method: 'POST',
          headers: { Authorization: `Bearer ${T2}`, 'Content-Type': 'application/json' },
          body: JSON.stringify(logDb)
        });
        console.log('[Log] ficha criada:', logId);
      } catch(e) { console.error('[Log] criar:', e.message); }

      logAction({ modulo:'Orçamento', fichaId:'', ficha:nome||'', acao:'Novo orçamento criado', para:'aguardando_aprovacao', gatilho:'→ Pipe aguardando_aprovacao', status:'ok', detalhe:'Valor: R$'+(preco||0) }).catch(()=>{});
      return res.status(200).json({
        ok:       true,
        cardId:   card?.id || null,
        pipefyErro,
        fichaInfo: {
          nome:       nome,
          equipamento: aparelho,
          destinos:   ['Logística (diagnóstico pendente)']
        }
      });
    } catch(e) {
      return res.status(200).json({ ok: false, error: e.message });
    }
  }



  // ── GET fix-clarice-pipefy: cria card Pipefy para ficha Clarice sem pipefyCardId ──
  if (action === "fix-clarice-pipefy") {
    const LOG_KEY  = "reparoeletro_logistica";
    const ORC_KEY2 = "reparoeletro_orcamentos";
    const AGUARDANDO = "334875152";
    try {
      // Buscar ficha Clarice na logística
      const logDb = await dbGet(LOG_KEY);
      if (!logDb) return res.status(500).json({ ok:false, erro:"LOG_KEY vazio" });

      const ficha = logDb.fichas.find(f =>
        (f.nome || "").toLowerCase().includes("clarice") ||
        (f.id   || "").includes("0273")
      );
      if (!ficha) return res.status(404).json({ ok:false, erro:"Ficha Clarice nao encontrada", total: logDb.fichas.length });

      if (ficha.pipefyCardId) {
        // Já tem card — só mover para Aguardando
        await pipefyQuery(`mutation { moveCardToPhase(input: { card_id: "${ficha.pipefyCardId}", destination_phase_id: "${AGUARDANDO}" }) { card { id } } }`);
        return res.status(200).json({ ok:true, info:"ja tinha card, movido", pipefyCardId: ficha.pipefyCardId });
      }

      // Criar card usando createPipefyCard provado
      const data = await createPipefyCard({
        phaseId:  AGUARDANDO,
        nome:     ficha.nome     || "",
        telefone: ficha.telefone || "",
        aparelho: ficha.equipamento || "",
        defeito:  ficha.defeito  || "",
        endereco: ficha.endereco || ""
      });
      const card = data?.createCard?.card;
      const cardId = card?.id || null;
      // Pipefy best-effort: continua mesmo sem card
      if (!cardId) {
        console.warn('[orc] Pipefy nao retornou card — salvando ficha sem pipefyCardId');
      }

      // Mover para Aguardando se não foi criado lá diretamente
      if (cardId && card.current_phase?.name && !card.current_phase.name.toLowerCase().includes("aguardando")) {
        await pipefyQuery(`mutation { moveCardToPhase(input: { card_id: "${card.id}", destination_phase_id: "${AGUARDANDO}" }) { card { id } } }`).catch(()=>{});
      }

      // Atualizar valor de contrato
      const preco = ficha.diagnostico?.preco;
      if (preco && cardId) {
        const numPreco = parseFloat(String(preco).replace(",",".")) || 0;
        if (numPreco > 0) {
          await updateCardValue(card.id, numPreco).catch(()=>{});
        }
      }

      // Salvar pipefyCardId na ficha da logística
      const fichaIdx = logDb.fichas.findIndex(f => f.id === ficha.id);
      if (fichaIdx >= 0) {
        logDb.fichas[fichaIdx].pipefyCardId = String(card.id);
        delete logDb.fichas[fichaIdx].pipefyErro;
        await dbSet(LOG_KEY, logDb);
      }

      // Atualizar orçamento
      const orcDb = await dbGet(ORC_KEY2);
      if (orcDb?.fichas) {
        const orcIdx = orcDb.fichas.findIndex(f => f.id === ficha.id);
        if (orcIdx >= 0) { orcDb.fichas[orcIdx].id = String(card.id); orcDb.fichas[orcIdx].pipefyId = String(card.id); await dbSet(ORC_KEY2, orcDb); }
      }

      return res.status(200).json({ ok:true, pipefyCardId: card.id, url: card.url, nome: ficha.nome, preco, fase: card.current_phase?.name });
    } catch(e) {
      return res.status(500).json({ ok:false, erro: e.message });
    }
  }

  // ── GET templates-load ────────────────────────────────────────
  if (action === 'templates-load') {
    const saved = await dbGet('reparoeletro_orc_templates');
    return res.status(200).json({ ok: true, templates: saved || {} });
  }

  // ── POST templates-save ───────────────────────────────────────
  if (req.method === 'POST' && action === 'templates-save') {
    const { templates } = req.body || {};
    if (!templates) return res.status(400).json({ ok: false, error: 'templates ausente' });
    await dbSet('reparoeletro_orc_templates', templates);
    return res.status(200).json({ ok: true });
  }

  // ── GET orc-load ──────────────────────────────────────────
  // ── GET gmb-load — fichas em ERP para solicitação de avaliação GMB ──────────
  // ── POST gmb-marcar-enviado — salva ficha como contatada GMB ──────────────
  if (req.method === 'POST' && action === 'gmb-marcar-enviado') {
    const { id, nome, tel, desc } = req.body || {};
    const GMB_ENV_KEY  = 'gmb_enviados';
    const GMB_PEND_KEY = 'gmb_pendentes';
    // Remover do pool pendente
    try {
      const db_pend = (await dbGet(GMB_PEND_KEY)) || { ids: [] };
      db_pend.ids = (db_pend.ids || []).filter(i => String(i) !== String(id));
      await dbSet(GMB_PEND_KEY, db_pend);
    } catch(ep) { console.warn('[gmb] remover pendente:', ep.message); }
    const db_g = (await dbGet(GMB_ENV_KEY)) || { fichas: [] };
    // Evitar duplicatas
    if (!db_g.fichas.find(f => f.id === id)) {
      db_g.fichas.unshift({
        id, nome, tel, desc,
        enviadoEm: new Date().toISOString()
      });
      db_g.fichas = db_g.fichas.slice(0, 5000); // limite alto p/ não derrubar dedup (bug corrigido em 30/06)
      await dbSet(GMB_ENV_KEY, db_g);
    }
    return res.status(200).json({ ok: true });
  }

  // ── GET gmb-enviados — lista fichas já contatadas ──────────────────────────
  if (action === 'gmb-enviados') {
    const GMB_ENV_KEY = 'gmb_enviados';
    const db_g = (await dbGet(GMB_ENV_KEY)) || { fichas: [] };
    return res.status(200).json({ ok: true, fichas: db_g.fichas || [] });
  }

  // ── POST gmb-filtrar-data — redefine gmb_pendentes apenas com fichas de uma data ──
  if (action === 'gmb-filtrar-data') {
    const data = req.query.data || (req.body && req.body.data) || '';  // ex: 2026-06-13
    if (!data) return res.status(400).json({ ok:false, error:'data obrigatória ?data=YYYY-MM-DD' });
    try {
      const BALCAO_KEY   = 'reparoeletro_balcao';
      const PIPE_KEY_GMB = 'reparoeletro_pipe';
      const GMB_ENV_KEY  = 'gmb_enviados';
      const GMB_PEND_KEY = 'gmb_pendentes';

      const db_balcao = (await dbGet(BALCAO_KEY)) || [];
      const db_gmb    = await dbGet(PIPE_KEY_GMB);
      const db_env    = (await dbGet(GMB_ENV_KEY)) || { fichas: [] };

      const jaEnviadosIds = new Set((db_env.fichas||[]).map(f=>String(f.id)));

      // 1. IDs dos cards que entraram em ERP na data — busca em múltiplas fontes

      const idsNaData = new Set();
      const fontes = [];

      // 1a. Balcão: pagoEm ou entradaEm
      (Array.isArray(db_balcao) ? db_balcao : []).forEach(function(b) {
        const dt = b.pagoEm || b.entradaEm || '';
        if (dt.startsWith(data) && b.pipefyId) {
          idsNaData.add(String(b.pipefyId));
          if (b.flFichaId) idsNaData.add(String(b.flFichaId));
          fontes.push({ id: String(b.pipefyId), fonte: 'balcao' });
        }
      });

      // 1b. Logística: fichas com entregaRealizadaEm ou movedAt na data (fase entrega_realizada ou finalizado)
      try {
        const db_log = (await dbGet('reparoeletro_logistica')) || { fichas: [] };
        (db_log.fichas || []).forEach(function(f) {
          const dt = f.entregaRealizadaEm || f.movedAt || f.pagoEm || f.finalizadoEm || '';
          const fase = f.phase || f.fase || '';
          if (dt.startsWith(data) && (fase === 'entrega_realizada' || fase === 'finalizado' || fase === 'erp')) {
            const id = f.pipefyCardId || f.pipefyId || f.id || '';
            if (id) { idsNaData.add(String(id)); fontes.push({ id: String(id), fonte: 'logistica' }); }
          }
        });
      } catch(el) { console.warn('log search:', el.message); }

      // 1c. Financeiro: fichas pagas na data
      try {
        const db_fin = (await dbGet('reparoeletro_financeiro')) || { fichas: [] };
        (db_fin.fichas || []).forEach(function(f) {
          const dt = f.pagoEm || f.dataConfirmacao || f.movedAt || '';
          if (dt.startsWith(data) && f.status === 'pago') {
            const id = f.pipeCardId || f.pipefyId || f.pipefyCardId || '';
            if (id) { idsNaData.add(String(id)); fontes.push({ id: String(id), fonte: 'financeiro' }); }
          }
        });
      } catch(ef) { console.warn('fin search:', ef.message); }

      // 1d. Pipe: cards com movedAt na data E phase=erp ou history com erp nessa data
      const cards_all = (db_gmb && Array.isArray(db_gmb.cards)) ? db_gmb.cards : [];
      cards_all.forEach(function(card) {
        const mt = card.movedAt || '';
        // Fichas que foram movidas PARA ERP nessa data (antes do cron sobrescrever)
        // Detectar pelo history: se há entrada anterior com phase!=erp e o próximo é erp nessa data
        const hist = card.history || [];
        hist.forEach(function(h) {
          // O cron registra {phase:'erp', ts: quando saiu do erp}
          // Não é confiável para data de entrada — ignorar
        });
        // Mas se o card tem pipefyId numérico pequeno e estava em ERP, pode ter entrado via balcão
        // já coberto acima
      });

      // 2. Enriquecer fichas FL- com dados da Frente de Loja
      const FL_KEY = 'reparoeletro_frenteloja';
      const db_fl  = (await dbGet(FL_KEY)) || { fichas: [] };
      const flFichas = db_fl.fichas || [];

      // 3. Cruzar com cards do pipe — busca por pipefyId, flFichaId, localId ou id
      const cards   = (db_gmb && Array.isArray(db_gmb.cards)) ? db_gmb.cards : [];
      const novasIds = [];
      const fichasEncontradas = [];

      for (const id of idsNaData) {
        if (jaEnviadosIds.has(id)) continue;
        const sid = String(id);
        // Buscar no pipe por qualquer campo de ID
        const card = cards.find(function(c) {
          return String(c.id||'')===sid ||
                 String(c.pipefyId||'')===sid ||
                 String(c.localId||'')===sid ||
                 String(c.flFichaId||'')===sid;
        });
        // Para fichas FL-, buscar dados na frenteloja
        let nome = card ? (card.nomeContato||card.title||'') : '';
        let tel  = card ? (card.telefone||'') : '';
        let valor = card ? (card.valor||null) : null;
        if (sid.startsWith('FL-') && (!nome || !tel)) {
          const fl = flFichas.find(function(f) { return f.id === sid; });
          if (fl) {
            nome  = nome  || fl.nome  || fl.nomeContato || '';
            tel   = tel   || fl.telefone || '';
            valor = valor || fl.valor || null;
          }
        }
        // Usar também dados diretos do balcão
        const balEntry = (Array.isArray(db_balcao) ? db_balcao : []).find(function(b) {
          return String(b.pipefyId||'')===sid || String(b.flFichaId||'')===sid;
        });
        if (balEntry) {
          nome  = nome  || balEntry.nomeContato || '';
          tel   = tel   || balEntry.telefone    || '';
          valor = valor || balEntry.valor       || null;
        }
        novasIds.push(sid);
        fichasEncontradas.push({
          id:    sid,
          nome:  nome  || '—',
          tel:   tel   || '',
          valor: valor || null,
          phase: card ? card.phase : '?',
        });
      }

      // 3. Salvar novo gmb_pendentes com APENAS essas fichas
      await dbSet(GMB_PEND_KEY, { ids: novasIds });

      return res.status(200).json({
        ok: true,
        data,
        total: novasIds.length,
        fichas: fichasEncontradas,
        msg: novasIds.length + ' fichas de ' + data + ' definidas no pool GMB'
      });
    } catch(e) {
      return res.status(200).json({ ok:false, error: e.message });
    }
  }

  // ── GET gmb-inspecionar — mostra todos cards que o cron moveu para identificar os de 13/06 ──
  if (action === 'gmb-inspecionar') {
    try {
      const db_gmb   = await dbGet('reparoeletro_pipe');
      const db_fl    = (await dbGet('reparoeletro_frenteloja')) || { fichas: [] };
      const db_bal   = (await dbGet('reparoeletro_balcao')) || [];
      const GMB_PEND = (await dbGet('gmb_pendentes')) || { ids: [] };
      const GMB_ENV  = (await dbGet('gmb_enviados'))  || { fichas: [] };
      const pendIds  = new Set((GMB_PEND.ids||[]).map(String));
      const envIds   = new Set((GMB_ENV.fichas||[]).map(f=>String(f.id)));

      const cards = (db_gmb && Array.isArray(db_gmb.cards)) ? db_gmb.cards : [];
      // Cards movidos pelo cron de 15/06 02:59 UTC
      const cronTs = '2026-06-15T02:59:46';
      const movidos = cards
        .filter(c => (c.movedAt||'').startsWith(cronTs) && c.phase==='finalizado')
        .map(function(c) {
          const cid = String(c.id||c.pipefyId||'');
          // Tentar enriquecer com dados do balcão
          const bal = (Array.isArray(db_bal)?db_bal:[]).find(b=>
            String(b.pipefyId||'')===cid || String(b.flFichaId||'')===cid
          );
          // Enriquecer com frenteloja
          const fl = (db_fl.fichas||[]).find(f=> f.id===cid || f.pipefyId===cid);
          const nome = c.nomeContato||c.title||(fl&&(fl.nome||fl.nomeContato))||'—';
          const tel  = c.telefone||(bal&&bal.telefone)||(fl&&fl.telefone)||'';
          const pagoEm = (bal&&(bal.pagoEm||bal.entradaEm))||'';
          return {
            id: cid,
            nome: nome.slice(0,30),
            tel,
            valor: c.valor||null,
            pagoEm: pagoEm.slice(0,10)||'—',
            noGMB: pendIds.has(cid) ? 'SIM' : envIds.has(cid) ? 'ENVIADO' : 'NÃO',
          };
        })
        .sort((a,b) => a.pagoEm.localeCompare(b.pagoEm));

      return res.status(200).json({ ok:true, total: movidos.length, cards: movidos });
    } catch(e) { return res.status(200).json({ ok:false, error:e.message }); }
  }

  // ── GET gmb-adicionar — adiciona IDs específicos ao pool GMB via query string ──
  if (action === 'gmb-adicionar') {
    const idsParam = req.query.ids || '';
    const novos = idsParam.split(',').map(s=>s.trim()).filter(Boolean);
    if (!novos.length) return res.status(400).json({ ok:false, error:'?ids=ID1,ID2,...' });
    try {
      const GMB_PEND_KEY = 'gmb_pendentes';
      const GMB_ENV_KEY  = 'gmb_enviados';
      const db_pend = (await dbGet(GMB_PEND_KEY)) || { ids: [] };
      const db_env  = (await dbGet(GMB_ENV_KEY))  || { fichas: [] };
      const jaEnviados = new Set((db_env.fichas||[]).map(f=>String(f.id)));
      const pendSet = new Set((db_pend.ids||[]).map(String));
      const adicionados = [];
      const jaExistiam  = [];
      novos.forEach(function(id) {
        if (jaEnviados.has(id)) return;
        if (pendSet.has(id)) { jaExistiam.push(id); return; }
        pendSet.add(id);
        db_pend.ids.push(id);
        adicionados.push(id);
      });
      if (adicionados.length) await dbSet(GMB_PEND_KEY, db_pend);
      return res.status(200).json({
        ok: true,
        adicionados, jaExistiam,
        totalPool: db_pend.ids.length,
        msg: adicionados.length + ' IDs adicionados ao pool GMB'
      });
    } catch(e) { return res.status(200).json({ ok:false, error:e.message }); }
  }

  // ── GET gmb-restaurar — recupera fichas movidas para finalizado pelo cron ──
  if (action === 'gmb-restaurar') {
    try {
      const PIPE_KEY_GMB  = 'reparoeletro_pipe';
      const GMB_ENV_KEY   = 'gmb_enviados';
      const GMB_PEND_KEY  = 'gmb_pendentes';
      const db_gmb  = await dbGet(PIPE_KEY_GMB);
      const db_env  = (await dbGet(GMB_ENV_KEY))  || { fichas: [] };
      const db_pend = (await dbGet(GMB_PEND_KEY)) || { ids: [] };

      if (!db_gmb || !Array.isArray(db_gmb.cards))
        return res.status(200).json({ ok:true, restauradas:[], total:0 });

      const jaEnviadosIds = new Set((db_env.fichas||[]).map(f=>String(f.id)));
      const pendIds = new Set((db_pend.ids||[]).map(String));
      const horaLimite = req.query.desde || '2026-06-09T00:00:00Z'; // segunda 02:59 UTC
      const horaCutoff = req.query.ate   || new Date().toISOString();

      // Fichas em 'finalizado' que foram movidas dentro da janela do cron
      const restauradas = [];
      let pendUpdated = false;

      for (const card of db_gmb.cards) {
        const cardId = String(card.id || card.pipefyId || '');
        if (card.phase !== 'finalizado') continue;
        if (jaEnviadosIds.has(cardId)) continue; // já foi enviado para GMB
        if (pendIds.has(cardId)) continue; // já está no pool

        // Verificar se foi de ERP para finalizado pelo cron (history)
        const hist = (card.history || []);
        const tevERP = hist.some(h => h.phase === 'erp') || (card.movedAt >= horaLimite && card.movedAt <= horaCutoff);
        if (!tevERP) continue;

        // Adicionar ao pool pendente para aparecer no GMB
        db_pend.ids.push(cardId);
        pendIds.add(cardId);
        pendUpdated = true;

        restauradas.push({
          id: cardId, nome: card.nomeContato||'—',
          tel: card.telefone||'', valor: card.valor||null,
          movedAt: card.movedAt,
        });
      }

      if (pendUpdated) await dbSet(GMB_PEND_KEY, db_pend);
      return res.status(200).json({ ok:true, restauradas, total: restauradas.length, msg: restauradas.length + ' fichas restauradas no pool GMB' });
    } catch(e) {
      return res.status(200).json({ ok:false, error: e.message });
    }
  }

    if (action === "gmb-load") {
    const PIPE_KEY_GMB  = 'reparoeletro_pipe';
    const GMB_ENV_KEY   = 'gmb_enviados';
    const GMB_PEND_KEY  = 'gmb_pendentes';
    const db_gmb  = await dbGet(PIPE_KEY_GMB);
    const db_env  = (await dbGet(GMB_ENV_KEY))  || { fichas: [] };
    const db_pend = (await dbGet(GMB_PEND_KEY)) || { ids: [] };

    const jaEnviadosIds   = new Set((db_env.fichas || []).map(f => String(f.id)));
    const jaEnviadosNomes = new Set((db_env.fichas || []).map(f => (f.nome||'').toLowerCase().trim()));

    if (!db_gmb || !Array.isArray(db_gmb.cards)) {
      return res.status(200).json({ ok: true, cards: [], total: 0 });
    }

    // Coletar IDs pendentes (pool separado que não é afetado pelo cron ERP→Finalizado)
    const pendIds = new Set((db_pend.ids || []).map(String));

    // Também adicionar ao pool fichas em phase==='erp' que ainda não estão no pool
    let pendUpdated = false;
    const nomesVistos = new Set();
    const erp = [];

    for (const card of db_gmb.cards) {
      const cardId = String(card.id || card.pipefyId || '');
      const nome   = (card.nomeContato || card.title || '').toLowerCase().trim();
      const eErp   = card.phase === 'erp';
      const ePend  = pendIds.has(cardId);

      // Adicionar ao pool se está em erp e não estava ainda
      if (eErp && cardId && !pendIds.has(cardId) && !jaEnviadosIds.has(cardId)) {
        // Só re-adiciona ao pool pendente se NÃO foi enviado antes
        pendIds.add(cardId);
        db_pend.ids = db_pend.ids || [];
        db_pend.ids.push(cardId);
        pendUpdated = true;
      }

      // Mostrar se está no pool pendente (independente da phase atual)
      if (!ePend && !eErp) continue;
      if (jaEnviadosIds.has(cardId)) continue;
      if (nome && jaEnviadosNomes.has(nome)) continue;
      if (nome && nomesVistos.has(nome)) continue;
      if (nome) nomesVistos.add(nome);

      erp.push({
        id:      cardId,
        pipefyId: card.pipefyId || card.id,
        nome:    card.nomeContato || card.title || '—',
        tel:     card.telefone || card.tel || '',
        desc:    card.equipamento || card.desc || '',
        valor:   card.valor || null,
        movedAt: card.movedAt || null,
        phase:   card.phase,
      });
    }

    if (pendUpdated) await dbSet(GMB_PEND_KEY, db_pend);
    return res.status(200).json({ ok: true, total: erp.length, cards: erp });
  }

    if (action === "orc-load") {
    const db = await dbGet(ORC_KEY) || { fichas: [], syncedIds: [] };
    // Deduplica por id
    const seen = new Set();
    db.fichas = (db.fichas || []).filter(f => { if (seen.has(f.id)) return false; seen.add(f.id); return true; });
    return res.status(200).json({ ok: true, fichas: db.fichas || [] });
  }

  // ── GET orc-sync ───────────────────────────────────────────
  if (action === "orc-sync") {
    const db = await dbGet(ORC_KEY) || { fichas: [], syncedIds: [], initialized: false, maxIdSeen: 0 };
    if (!Array.isArray(db.fichas))    db.fichas    = [];
    if (!Array.isArray(db.syncedIds)) db.syncedIds = [];
    let newCount = 0, pipefyError = null;
    try {
      const cards = await fetchAguardandoAprovacao();
      // DEBUG temporário
      if (req.query.debug === "1") {
        return res.status(200).json({ ok: true, debug: true, cards_found: cards.length, card_ids: cards.map(c => c.pipefyId), maxIdSeen: db.maxIdSeen });
      }
      // Primeira vez: guarda o maior ID atual como referência — não importa nada
      if (!db.initialized) {
        const maxId = cards.reduce((max, c) => Math.max(max, parseInt(c.pipefyId)||0), 0);
        db.maxIdSeen  = maxId;
        db.initialized = true;
        // Também marca todos como vistos para não importar se alguém chamar orc-forcar
        cards.forEach(card => {
          if (!db.syncedIds.includes(card.pipefyId)) db.syncedIds.push(card.pipefyId);
        });
        await dbSet(ORC_KEY, db);
        return res.status(200).json({ ok: true, newCount: 0, initialized: true, maxIdSeen: maxId, pipefyError: null });
      }
      // Remove do syncedIds cards que saíram da fase (permite reimportar se voltarem)
      const idsNaFase = new Set(cards.map(card => card.pipefyId));
      db.syncedIds = (db.syncedIds || []).filter(id => idsNaFase.has(id));

      // Importa apenas cards nunca vistos (não estão no syncedIds)
      for (const card of cards) {
        if (db.syncedIds.includes(card.pipefyId)) continue;

        // Verifica se as notas têm múltiplos equipamentos (ex: "purificador: ... bebedouro: ...")
        // Usa o campo "Notas do treinamento" diretamente (preserva quebras de parágrafo)
        const notasField = card.notas || (card.comentarios ? card.comentarios.join("\n\n") : "");
        const multiEquip = splitEquipamentos(notasField);

        // Função auxiliar para gerar texto de orçamento para 1 equipamento
        function gerarFicha(descEquip, comentariosEquip, sufixoId) {
          let textoOrc = "", precoSugerido = null;
          try {
            let regra = detectarRegra(descEquip, comentariosEquip);
            if (regra) {
              let precoRegra = parseFloat(regra.preco || "0");
              const equipImp = detectarEquipamento(descEquip, "");
              if (isGrande(descEquip, "") && (equipImp === "forno" || equipImp === "adega") && precoRegra > 0) {
                precoRegra += 300;
                regra = Object.assign({}, regra, { preco: String(precoRegra) });
              }
              let texto = substituirNome(regra.texto, card.nome);
              precoSugerido = regra.preco || null;
              if (precoSugerido) texto = texto.replace("[VALOR]", precoSugerido + " reais");
              textoOrc = texto;
            } else {
              const tp = templatePadrao(descEquip, card.nome);
              textoOrc = typeof tp === "object" ? (tp.texto || "") : String(tp || "");
            }
          } catch(e) {
            textoOrc = "Ola, bom dia, sou o Pedro da Reparo Eletro, vou te enviar agora o orcamento:\n\nEste conserto completo fica em [VALOR] apenas. Aprovando ja iniciamos o conserto.";
          }
          return {
            id:          card.pipefyId + (sufixoId || ""),
            pipefyId:    card.pipefyId,
            nome:        card.nome,
            tel:         card.tel,
            desc:        descEquip,
            end:         card.end,
            age:         card.age,
            comentarios: comentariosEquip,
            textoOrc,
            precoSugerido,
            status:      "pendente",
            preco:       null,
            createdAt:   new Date().toISOString(),
          };
        }

        if (multiEquip && multiEquip.length >= 2) {
          // 1 ficha combinando todos os equipamentos no padrão correto
          var qtd = multiEquip.length;
          var primeiroNome = card.nome ? card.nome.split(" ")[0] : "cliente";
          var totalPreco = 0;
          var partesTexto = [];

          for (var ei = 0; ei < multiEquip.length; ei++) {
            var equip = multiEquip[ei];
            var descEquip = equip.nomeEquip + ": " + equip.descProblema;
            var fichaTemp = gerarFicha(descEquip, [equip.descProblema], "-tmp");
            // Extrai só o corpo diagnóstico (remove "Foram feitos...identificamos que" mas mantém o resto)
            var corpo = fichaTemp.textoOrc || "";
            // Remove o cabecalho "Ola, ... orcamento:\n\n"
            var dblN = corpo.indexOf("\n\n");
            if (dblN > 0) corpo = corpo.slice(dblN + 2).trim();
            // Remove "Aprovando ja iniciamos o conserto." do final — vai na linha final combinada
            corpo = corpo.replace(/\.? Aprovando ja iniciamos o conserto\.?$/, "").trim();
            var nomeCapital = equip.nomeEquip.charAt(0).toUpperCase() + equip.nomeEquip.slice(1);
            partesTexto.push("Em relacao ao " + nomeCapital + ":\n" + corpo);
            totalPreco += parseFloat(fichaTemp.precoSugerido || "0");
          }

          // Calcula desconto: 2 equip=10%, 3=15%, 4+=20%
          var descPct = qtd >= 4 ? 20 : qtd === 3 ? 15 : 10;
          var precoComDesconto = Math.round(totalPreco * (1 - descPct / 100));
          var linhaFinal = "Consertando os " + qtd + " juntos eu consigo um desconto para voce de " + totalPreco + " reais por " + precoComDesconto + " apenas. Aprovando ja iniciamos o conserto.";

          var cabecalho = "Ola, " + primeiroNome + " bom dia, sou o Pedro da Reparo Eletro, vou te enviar agora o orcamento:\n\n";
          var textoFinal = cabecalho + partesTexto.join("\n\n") + "\n\n" + linhaFinal;

          var fichaCombinada = gerarFicha(card.desc, card.comentarios, "");
          fichaCombinada.textoOrc = textoFinal;
          fichaCombinada.precoSugerido = String(precoComDesconto);
          fichaCombinada.multiEquip = true;
          db.fichas.unshift(fichaCombinada);
        } else {
          // 1 equipamento normal
          const ficha = gerarFicha(card.desc, card.comentarios, "");
          db.fichas.unshift(ficha);
        }

        db.syncedIds.push(card.pipefyId);
        newCount++;
      }
      if (newCount > 0) await dbSet(ORC_KEY, db);
    } catch(e) { pipefyError = e.message; }
    return res.status(200).json({ ok: true, newCount, pipefyError, maxIdSeen: db.maxIdSeen });
  }

  // ── POST orc-update-texto ──────────────────────────────────
  // Regenera ou edita o texto de orçamento de uma ficha
  // ── POST orc-update-preco — atualiza preco sem mudar status ─
  if (req.method === "POST" && action === "orc-update-preco") {
    const { id, preco, precoSugerido } = req.body || {};
    const db = await dbGet(ORC_KEY) || { fichas: [], syncedIds: [] };
    const ficha = db.fichas.find(f => f.id === id);
    if (!ficha) return res.status(404).json({ ok: false, error: "Ficha não encontrada" });
    if (preco !== undefined) ficha.preco = preco;
    if (precoSugerido !== undefined) ficha.precoSugerido = precoSugerido;
    await dbSet(ORC_KEY, db);
    return res.status(200).json({ ok: true, ficha });
  }

  if (req.method === "POST" && action === "orc-update-texto") {
    const { id, textoOrc } = req.body || {};
    const db = await dbGet(ORC_KEY) || { fichas: [], syncedIds: [] };
    const ficha = db.fichas.find(f => f.id === id);
    if (!ficha) return res.status(404).json({ ok: false, error: "Ficha não encontrada" });
    ficha.textoOrc = textoOrc;
    await dbSet(ORC_KEY, db);
    return res.status(200).json({ ok: true, ficha });
  }

  // ── POST orc-regenerar-todos — busca dados frescos do Pipefy por card e regenera
  if (action === "orc-regenerar-todos") {
    const db = await dbGet(ORC_KEY) || { fichas: [], syncedIds: [] };
    const pendentes = (db.fichas || []).filter(f => f.status === "pendente");
    let count = 0;
    for (const ficha of pendentes) {
      try {
        // Busca dados frescos direto do card no Pipefy
        const fresh = await fetchCardData(ficha.pipefyId);
        if (fresh) {
          ficha.comentarios = fresh.comentarios.length ? fresh.comentarios : (ficha.comentarios || []);
          ficha.desc        = fresh.desc  || ficha.desc;
          ficha.nome        = fresh.nome  || ficha.nome;
        }
        try {
          const orcResult = await gerarTextoOrcamento(ficha.desc, ficha.comentarios, ficha.nome);
          if (orcResult && typeof orcResult === "object") {
            let texto = orcResult.texto || "";
            const preco = orcResult.preco || null;
            if (preco) { texto = texto.replace("[VALOR]", preco + " reais"); ficha.precoSugerido = preco; }
            ficha.textoOrc = texto;
          } else {
            ficha.textoOrc = String(orcResult || "");
          }
        } catch(e) {
          const tp = templatePadrao(ficha.desc, ficha.nome);
          ficha.textoOrc = typeof tp === "object" ? (tp.texto || "") : String(tp || "");
        }
        count++;
      } catch(e) { console.error("regenerar", ficha.pipefyId, e.message); }
    }
    await dbSet(ORC_KEY, db);
    return res.status(200).json({ ok: true, regenerados: count });
  }

  // ── POST orc-regenerar ─────────────────────────────────────
  if (req.method === "POST" && action === "orc-regenerar") {
    const { id } = req.body || {};
    const db = await dbGet(ORC_KEY) || { fichas: [], syncedIds: [] };
    const ficha = db.fichas.find(f => f.id === id);
    if (!ficha) return res.status(404).json({ ok: false, error: "Ficha não encontrada" });
    try {
      // Busca dados frescos do Pipefy antes de regenerar
      try {
        const fresh = await fetchCardData(ficha.pipefyId);
        if (fresh && fresh.comentarios.length) {
          ficha.comentarios = fresh.comentarios;
          ficha.desc = fresh.desc || ficha.desc;
        }
      } catch(e) {}
      var orcResult = await gerarTextoOrcamento(ficha.desc, ficha.comentarios, ficha.nome);
      if (orcResult && typeof orcResult === "object") {
        ficha.textoOrc = orcResult.texto;
        if (orcResult.preco) ficha.precoSugerido = orcResult.preco;
      } else {
        ficha.textoOrc = orcResult || "";
      }
      await dbSet(ORC_KEY, db);
      return res.status(200).json({ ok: true, textoOrc: ficha.textoOrc, precoSugerido: ficha.precoSugerido });
    } catch(e) {
      return res.status(200).json({ ok: false, error: e.message });
    }
  }

  // ── POST orc-enviar ───────────────────────────────────────
  if (req.method === "POST" && action === "orc-enviar") {
    const { id, preco } = req.body || {};
    if (!id) return res.status(400).json({ ok: false, error: "id obrigatório" });
    const db = await dbGet(ORC_KEY) || { fichas: [], syncedIds: [] };
    const ficha = db.fichas.find(f => f.id === id);
    if (!ficha) return res.status(404).json({ ok: false, error: "Ficha não encontrada" });
    ficha.status    = "enviado";
    ficha.preco     = preco || null;
    ficha.enviadoAt = new Date().toISOString();
    await dbSet(ORC_KEY, db);
    // Atualiza valor no Pipefy — aguarda e retorna erro se falhar
    let pipefyUpdateOk = true, pipefyUpdateError = null;
    if (preco && ficha.pipefyId) {
      try {
        await updateCardValue(ficha.pipefyId, preco);
      } catch(e) {
        pipefyUpdateOk = false;
        pipefyUpdateError = e.message;
        console.error("updateCardValue:", e.message);
      }
    }
    return res.status(200).json({ ok: true, ficha, pipefyUpdateOk, pipefyUpdateError });
  }

  // ── POST orc-status ────────────────────────────────────────
  if (req.method === "POST" && action === "orc-status") {
    const { id, status } = req.body || {};
    const db = await dbGet(ORC_KEY) || { fichas: [], syncedIds: [] };
    const ficha = db.fichas.find(f => f.id === id);
    if (!ficha) return res.status(404).json({ ok: false, error: "Ficha não encontrada" });
    ficha.status = status;
    await dbSet(ORC_KEY, db);
    // Se aprovado: tira da Ultima Chamada no Pipefy e volta para Aguardando Aprovacao
    if (status === "aprovado" && ficha.pipefyId) {
      try {
        const cd = await pipefyQuery(`query { card(id: "${ficha.pipefyId}") { current_phase { id } } }`);
        if (cd?.card?.current_phase?.id === "338413470") {
          await pipefyQuery(`mutation { moveCardToPhase(input: { card_id: "${ficha.pipefyId}", destination_phase_id: "334875152" }) { card { id } } }`);
        }
      } catch(e) { /* silencioso */ }
    }
    return res.status(200).json({ ok: true });
  }

  // ── POST orc-forcar ───────────────────────────────────────
  // Remove um pipefyId do syncedIds para forçar reimportação
  if (req.method === "POST" && action === "orc-forcar") {
    const { pipefyId } = req.body || {};
    if (!pipefyId) return res.status(400).json({ ok: false, error: "pipefyId obrigatório" });
    const db = await dbGet(ORC_KEY) || { fichas: [], syncedIds: [] };
    db.syncedIds = (db.syncedIds || []).filter(id => id !== String(pipefyId));
    // Remove todas as fichas existentes com esse pipefyId (inclui variantes -eq2, -eq3, etc.)
    const before = (db.fichas || []).length;
    db.fichas = (db.fichas || []).filter(f => f.pipefyId !== String(pipefyId));
    const removed = before - db.fichas.length;
    await dbSet(ORC_KEY, db);
    return res.status(200).json({ ok: true, msg: "ID e fichas removidos. Próximo sync vai reimportar.", fichasRemovidas: removed });
  }

  // ── GET orc-card-debug — mostra todos os campos de um card específico
  if (action === "orc-card-debug") {
    const { id } = req.query;
    if (!id) return res.status(400).json({ ok: false, error: "id obrigatório" });
    try {
      const data = await pipefyQuery(`query {
        card(id: "${id}") {
          id title
          fields { name value }
          comments { text author { name } }
        }
      }`);
      return res.status(200).json({ ok: true, card: data?.card });
    } catch(e) {
      return res.status(200).json({ ok: false, error: e.message });
    }
  }

  // ── GET phase-ids — retorna IDs das fases do Pipefy
  if (action === "phase-ids") {
    const data = await pipefyQuery(`query {
      pipe(id: "${PIPE_ID}") {
        phases {
          id name
          fields { id label type }
        }
      }
    }`);
    const phases = data?.pipe?.phases || [];
    return res.status(200).json({
      ok: true,
      phases: phases.map(p => ({
        id: p.id,
        name: p.name,
        fields: p.fields?.map(f => ({id:f.id, label:f.label, type:f.type}))
      }))
    });
  }

  // ── GET orc-debug ─────────────────────────────────────────
  if (action === "orc-debug") {
    const result = {};
    try {
      const data = await pipefyQuery(`query {
        pipe(id: "${PIPE_ID}") {
          phases {
            name
            cards(first: 50) {
              edges { node { id title } }
            }
          }
        }
      }`);
      const phases = data?.pipe?.phases || [];
      // Mostra todas as fases mas destaca Aguardando Aprovação com IDs
      const aguPhase = phases.find(p => {
        const n = p.name.toLowerCase().replace(/[^a-z0-9 ]/g,"");
        return n.includes("aguardando aprova");
      });
      result.aguardando_aprovacao = aguPhase ? {
        count: aguPhase.cards.edges.length,
        cards: aguPhase.cards.edges.map(e => ({ id: e.node.id, title: e.node.title })),
      } : null;
      result.all_phases_count = phases.map(p => ({ name: p.name, count: p.cards.edges.length }));
    } catch(e) { result.pipefy_error = e.message; }
    const db = await dbGet(ORC_KEY) || {};
    result.initialized    = db.initialized;
    result.syncedIds      = db.syncedIds || [];
    result.fichas_count   = (db.fichas || []).length;
    return res.status(200).json(result);
  }

  // ── GET orc-sync-forcar-todos ─────────────────────────────
  // Remove do syncedIds todos os cards que estão AGORA em Aguardando Aprovação
  // Permite reimportar fichas que já estiveram na fase antes
  if (action === "orc-sync-forcar-todos") {
    const db = await dbGet(ORC_KEY) || { fichas: [], syncedIds: [] };
    let cards = [];
    try { cards = await fetchAguardandoAprovacao(); } catch(e) {
      return res.status(200).json({ ok: false, error: e.message });
    }
    // Remove do syncedIds apenas os que ainda estão na fase (não os que já foram processados e saíram)
    const idsNaFase = new Set(cards.map(c => c.pipefyId));
    const antes = db.syncedIds.length;
    db.syncedIds = (db.syncedIds || []).filter(id => !idsNaFase.has(id));
    // Também remove fichas já existentes desses IDs para não duplicar
    db.fichas = (db.fichas || []).filter(f => !idsNaFase.has(f.pipefyId));
    await dbSet(ORC_KEY, db);
    return res.status(200).json({ ok: true, removidos: antes - db.syncedIds.length, total_na_fase: cards.length, msg: "Chame orc-sync agora para importar." });
  }

  // ── GET orc-sync-fichas — sincroniza syncedIds com fichas existentes
  if (action === "orc-sync-fichas") {
    const db = await dbGet(ORC_KEY) || { fichas: [], syncedIds: [] };
    if (!Array.isArray(db.syncedIds)) db.syncedIds = [];
    let added = 0;
    (db.fichas || []).forEach(f => {
      if (!db.syncedIds.includes(f.pipefyId)) {
        db.syncedIds.push(f.pipefyId);
        added++;
      }
    });
    await dbSet(ORC_KEY, db);
    return res.status(200).json({ ok: true, added, total: db.syncedIds.length });
  }

  // ── GET orc-reset-init ────────────────────────────────────
  if (action === "orc-reset-init") {
    const db = await dbGet(ORC_KEY) || { fichas: [], syncedIds: [] };
    db.initialized = false;
    db.syncedIds   = [];
    db.fichas      = [];
    db.maxIdSeen   = 0;
    await dbSet(ORC_KEY, db);
    return res.status(200).json({ ok: true, msg: "Reset completo. Chame orc-sync para inicializar." });
  }

  // ── GET orc-limpar-enviados ───────────────────────────────
  // Remove fichas com status "enviado" — chamado automaticamente no fim do dia
  if (action === "orc-limpar-enviados") {
    const db = await dbGet(ORC_KEY) || { fichas: [], syncedIds: [] };
    const before = db.fichas.length;
    db.fichas = db.fichas.filter(f => f.status !== "enviado");
    const removed = before - db.fichas.length;
    if (removed > 0) await dbSet(ORC_KEY, db);
    return res.status(200).json({ ok: true, removed });
  }

  // ── POST orc-excluir ───────────────────────────────────────
  if (req.method === "POST" && action === "orc-excluir") {
    const { id } = req.body || {};
    const db = await dbGet(ORC_KEY) || { fichas: [], syncedIds: [] };
    db.fichas    = db.fichas.filter(f => f.id !== id);
    db.syncedIds = db.syncedIds.filter(s => s !== id);
    await dbSet(ORC_KEY, db);
    return res.status(200).json({ ok: true });
  }

  // ── GET sem-resposta — cards em aguardando_aprovacao +48h (Redis local)
  if (action === "sem-resposta") {
    try {
      const _uo2=(process.env.UPSTASH_URL||'').replace(/['"]/g,'').trim();
      const _to2=(process.env.UPSTASH_TOKEN||'').replace(/['"]/g,'').trim();
      async function _pga2(k){const r=await fetch(_uo2+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+_to2,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});const j=await r.json();const v=j[0]?.result;if(!v)return null;try{let x=JSON.parse(v);if(typeof x==='string')x=JSON.parse(x);return x;}catch(e){return null;}}
      const pipeS = await _pga2('reparoeletro_pipe');
      const agora2 = Date.now();
      const MS48 = 48 * 60 * 60 * 1000;
      const cards = ((pipeS&&pipeS.cards)||[])
        .filter(function(c){ return c.phase === 'aguardando_aprovacao'; })
        .map(function(c){
          var desde = c.aguardandoDesde || c.movedAt || c.criadoEm || null;
          var desdeMs = desde ? new Date(desde).getTime() : 0;
          var diffMs = desdeMs ? (agora2 - desdeMs) : 0;
          var ageDias = Math.floor(diffMs / (1000*60*60*24));
          return {
            pipefyId: c.pipefyId || c.id,
            localId:  c.id,
            title: c.nomeContato || '',
            nome:  c.nomeContato || '',
            tel:   c.telefone || '',
            desc:  c.descricao || c.equipamento || '',
            age:   ageDias
          };
        })
        .filter(function(c){ return c.age >= 2; })
        .sort(function(a,b){ return b.age - a.age; });
      return res.status(200).json({ ok: true, cards, fonte: 'redis_local' });
    } catch(e) {
      return res.status(200).json({ ok: false, error: e.message });
    }
  }

  // ── GET ultima-chamada — espelho da fase Ultima Chamada do Pipefy
  if (action === "ultima-chamada") {
    try {
      const phaseId = "338413470"; // Ultima Chamada
      const data = await pipefyQuery(`query {
        phase(id: "${phaseId}") {
          cards(first: 50) {
            edges {
              node {
                id title age
                fields { name value }
              }
            }
          }
        }
      }`);
      const cards = (data?.phase?.cards?.edges || []).map(({node}) => {
        const fields = node.fields || [];
        const nome = fields.find(f=>f.name.toLowerCase().includes("nome"))?.value || node.title;
        const tel  = fields.find(f=>f.name.toLowerCase().includes("telefone")||f.name.toLowerCase().includes("fone"))?.value || "";
        const desc = fields.find(f=>f.name.toLowerCase().includes("descri"))?.value || "";
        const dataEnc = fields.find(f=>f.name.toLowerCase().includes("encerr")||f.name.toLowerCase().includes("prazo")||f.name.toLowerCase().includes("esperada"))?.value || "";
        return { pipefyId: String(node.id), title: node.title, nome, tel, desc, age: node.age || 0, dataEncerramento: dataEnc };
      });
      return res.status(200).json({ ok: true, cards });
    } catch(e) {
      return res.status(200).json({ ok: false, error: e.message });
    }
  }

  // ── POST alertar — move card para Ultima Chamada + preenche data +7 dias úteis
  if (req.method === "POST" && action === "alertar") {
    const { pipefyId } = req.body || {};
    if (!pipefyId) return res.status(400).json({ ok: false, error: "pipefyId obrigatorio" });
    try {
      const phaseId    = "338413470"; // Ultima Chamada
      const dataLimite = addDiasUteis(7);

      // Move para Ultima Chamada (ignora erro se já estiver lá)
      try {
        await pipefyQuery(`mutation {
          moveCardToPhase(input: { card_id: "${pipefyId}", destination_phase_id: "${phaseId}" }) {
            card { id }
          }
        }`);
      } catch(moveErr) { /* ignora "already in phase" */ }

      // Seta due_date nativa do card (+7 dias úteis)
      await pipefyQuery(`mutation {
        updateCard(input: { id: "${pipefyId}", due_date: "${dataLimite}T23:59:00-03:00" }) {
          card { id due_date }
        }
      }`);

      // Mover também no Pipe ADM local (reparoeletro_pipe)
      try {
        const _uo=(process.env.UPSTASH_URL||'').replace(/['"]/g,'').trim();
        const _to=(process.env.UPSTASH_TOKEN||'').replace(/['"]/g,'').trim();
        async function _pga(k){const r=await fetch(_uo+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+_to,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});const j=await r.json();const v=j[0]?.result;if(!v)return null;try{let x=JSON.parse(v);if(typeof x==='string')x=JSON.parse(x);return x;}catch(e){return null;}}
        async function _psa(k,v){await fetch(_uo+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+_to,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});}
        const pipeA = await _pga('reparoeletro_pipe');
        if (pipeA && Array.isArray(pipeA.cards)) {
          const nowA = new Date().toISOString();
          const cardA = pipeA.cards.find(function(c){
            return c.pipefyId === String(pipefyId) || c.id === String(pipefyId);
          });
          if (cardA) {
            cardA.history = (cardA.history||[]).concat([{phase:cardA.phase, ts:nowA}]);
            cardA.phase   = 'ultima_chamada';
            cardA.movedAt = nowA;
            pipeA.lastSync = nowA;
            await _psa('reparoeletro_pipe', pipeA);
          }
        }
      } catch(ea){ console.error('[alertar→pipe]', ea.message); }

      return res.status(200).json({ ok: true, pipefyId, phaseId, dataLimite });
    } catch(e) {
      return res.status(200).json({ ok: false, error: e.message });
    }
  }

  // ── GET debug-card-time — retorna campos de tempo brutos de um card
  if (action === "debug-card-time") {
    const cardId = req.query.id || "1322520742";
    const data = await pipefyQuery(`query {
      card(id: "${cardId}") {
        id title age created_at updated_at
        current_phase { id name }
        phases_history { phase { id name } firstTimeIn lastTimeOut }
      }
    }`);
    return res.status(200).json({ ok: true, card: data?.card });
  }

  return res.status(404).json({ ok: false, error: "Ação não encontrada" });
};

// ── ORÇAMENTOS ────────────────────────────────────────────────

const UPSTASH_URL   = (process.env.UPSTASH_URL   || "").replace(/['"]/g, "").trim();
const UPSTASH_TOKEN = (process.env.UPSTASH_TOKEN || "").replace(/['"]/g, "").trim();
const ORC_KEY = "reparoeletro_orcamentos";

async function dbGet(key) {
  try {
    const r = await fetch(`${UPSTASH_URL}/pipeline`, {
      method: "POST",
      headers: { Authorization: `Bearer ${UPSTASH_TOKEN}`, "Content-Type": "application/json" },
      body: JSON.stringify([["GET", key]]),
    });
    const j = await r.json();
    return j[0]?.result ? JSON.parse(j[0].result) : null;
  } catch(e) { return null; }
}

async function dbSet(key, value) {
  try {
    const r = await fetch(`${UPSTASH_URL}/pipeline`, {
      method: "POST",
      headers: { Authorization: `Bearer ${UPSTASH_TOKEN}`, "Content-Type": "application/json" },
      body: JSON.stringify([["SET", key, JSON.stringify(value)]]),
    });
    const j = await r.json();
    return j[0]?.result === "OK";
  } catch(e) { return false; }
}

// Busca campos e atividades de um card específico pelo ID
async function fetchCardData(pipefyId) {
  const data = await pipefyQuery(`query {
    card(id: "${pipefyId}") {
      id title
      fields { name value }
      comments { text }
    }
  }`);
  const node   = data?.card;
  if (!node) return null;
  const fields = node.fields || [];
  const nome  = fields.find(f => f.name.toLowerCase().includes("nome"))?.value || node.title;
  const tel   = fields.find(f => f.name.toLowerCase().includes("telefone") || f.name.toLowerCase().includes("fone"))?.value || "";
  const desc  = fields.find(f => f.name.toLowerCase().includes("descri"))?.value || "";
  const end   = fields.find(f => f.name.toLowerCase().includes("endere"))?.value || "";
  const extras = fields
    .filter(f => !["telefone","fone","nome","endere","valor"].some(k => f.name.toLowerCase().includes(k)))
    .map(f => f.value).filter(Boolean);
  const comentarios = [
    ...(node.comments || []).map(c => c.text).filter(Boolean),
    ...extras,
  ];
  return { pipefyId: String(node.id), title: node.title, nome, tel, desc, end, comentarios };
}

// Busca cards em Aguardando Aprovação direto pelo ID da fase (mais rápido e completo)
const AGUARDANDO_APROVACAO_PHASE_ID = "334875152";

// Busca ID de uma fase pelo nome
async function getPhaseIdByName(name) {
  const data = await pipefyQuery(`query { pipe(id: "${PIPE_ID}") { phases { id name } } }`);
  const phase = (data?.pipe?.phases || []).find(p => p.name.toLowerCase().includes(name.toLowerCase()));
  return phase?.id || null;
}

// Busca ID do campo de data de encerramento
async function getDateFieldId() {
  const data = await pipefyQuery(`query { pipe(id: "${PIPE_ID}") { start_form_fields { id label type } phases { name fields { id label type } } } }`);
  const allFields = [
    ...(data?.pipe?.start_form_fields || []),
    ...(data?.pipe?.phases || []).flatMap(ph => ph.fields || [])
  ];
  // Procura campo de data por prioridade: vencimento > encerr > prazo > data
  const keywords = ["vencimento", "encerr", "prazo", "data", "date"];
  for (const kw of keywords) {
    const f = allFields.find(f => f.type === "date" && f.label.toLowerCase().includes(kw));
    if (f) return f.id;
  }
  return null;
}

// Calcula data + N dias úteis (pula sábado e domingo)
function addDiasUteis(dias) {
  const d = new Date();
  let count = 0;
  while (count < dias) {
    d.setDate(d.getDate() + 1);
    const dow = d.getDay();
    if (dow !== 0 && dow !== 6) count++;
  }
  // Formato ISO YYYY-MM-DD
  return d.toISOString().split("T")[0];
}

async function fetchAguardandoAprovacao() {
  const all = [];
  let cursor = null, hasNext = true;
  while (hasNext) {
    const after = cursor ? `, after: "${cursor}"` : "";
    const data = await pipefyQuery(`query {
      phase(id: "${AGUARDANDO_APROVACAO_PHASE_ID}") {
        cards(first: 50${after}) {
          pageInfo { hasNextPage endCursor }
          edges {
            node {
              id title age
              fields { name value }
              comments { text author { name } created_at }
            }
          }
        }
      }
    }`);
    const phase = data?.phase;
    if (!phase) break;
    for (const { node } of phase.cards.edges) {
      const fields = node.fields || [];
      const nome     = fields.find(f => f.name.toLowerCase().includes("nome"))?.value || node.title;
      const tel      = fields.find(f => f.name.toLowerCase().includes("telefone") || f.name.toLowerCase().includes("fone"))?.value || "";
      const desc     = fields.find(f => f.name.toLowerCase().includes("descri"))?.value || "";
      const end      = fields.find(f => f.name.toLowerCase().includes("endere"))?.value || "";
      const notas    = fields.find(f => f.name.toLowerCase().includes("nota") || f.name.toLowerCase().includes("treina"))?.value || "";
      // Agrega TODOS os campos de texto como fonte de keywords para detecção
      const extras = fields
        .filter(f => !["telefone","fone","nome","endere","valor"].some(k => f.name.toLowerCase().includes(k)))
        .map(f => f.value).filter(Boolean);
      const comentarios = [
        ...(node.comments || []).map(c => c.text).filter(Boolean),
        ...extras,
      ];
      all.push({ pipefyId: String(node.id), title: node.title, nome, tel, desc, end, age: node.age, comentarios, notas });
    }
    hasNext = phase.cards.pageInfo?.hasNextPage ?? false;
    cursor  = phase.cards.pageInfo?.endCursor ?? null;
  }
  return all;
}

// Gera texto de orçamento com Claude
// ── NORMALIZA TEXTO (remove acentos, minúsculo) ──────────────
function norm(s) {
  return String(s || "").toLowerCase()
    .normalize("NFD").replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9 ]/g, " ");
}

function hasAny(texto, words) {
  return words.some(function(w) { return texto.indexOf(norm(w)) >= 0; });
}

const ORCAMENTO_REGRAS = [
  // 1. Termoelétrico + vela → R$ 390 (verificar ANTES do termoelétrico puro)
  {
    keywords: ["vela","velas"],
    extraKeys: ["termoeletrico","termeletrico","termo eletrico","termo-eletrico","thermoeletrico",
                "cooler","culer","coler","colder","peltier","pasta termica","kit frio","kit termoeletrico"],
    templateBase: "Ola, [NOME] bom dia, sou o Pedro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario refazer a parte eletrica que causou danos no conjunto do cooler, placa de resfriamento e pasta termica, alem da troca da vela, as pecas serao trocadas tambem. Este conserto completo fica em [PRECO] reais apenas. Aprovando ja iniciamos o conserto.",
    precoBase:  "390",
    precoExtra: "390",
  },
  // 2. Termoelétrico puro → R$ 350
  {
    keywords: ["termoeletrico","termeletrico","termo eletrico","termo-eletrico","thermoeletrico","termoeltrico",
               "termoelectric","kit termoeletrico","kit termo eletrico","kit termo-eletrico",
               "cooler","culer","coler","colder",
               "placa de resfriamento","placa resfriamento","placa fria",
               "peltier","peltyer","peltir",
               "pasta termica","pasta terminca","pasta termika","pasta termca",
               "kit frio","kit termico","conjunto termoeletrico"],
    template: "Ola, [NOME] bom dia, sou o Pedro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario refazer a parte eletrica que causou danos no conjunto do cooler, placa de resfriamento e pasta termica, as pecas serao trocadas tambem. Este conserto completo fica em [VALOR] reais apenas. Aprovando ja iniciamos o conserto.",
  },
  // 3. Magnetron → R$ 370
  {
    keywords: ["magnetron","magnetrao","magneton","magentron","magnetrom","magnetron","magnetico","magnet"],
    template: "Ola, [NOME] bom dia, sou o Pedro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario refazer a parte eletrica que causou danos no conjunto do magnetron, as pecas serao trocadas tambem. Este conserto completo fica em [VALOR] reais apenas. Aprovando ja iniciamos o conserto.",
  },
  // 4. Fusível/capacitor → R$ 320
  {
    keywords: ["fusivel","fusível","fusirel","fuzivel","fusiveil","queimou fusivel","fusivel de alta",
               "capacitor e fusivel","troca do fusivel","troca de fusivel"],
    template: "Ola, [NOME] bom dia, sou o Pedro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario refazer a parte eletrica que causou danos no conjunto do capacitor e fusivel de alta, as pecas serao trocadas tambem. Este conserto completo fica em [VALOR] reais apenas. Aprovando ja iniciamos o conserto.",
  },
  // 4b. Sensor de temperatura + capacitor → R$ 320
  {
    keywords: ["sensor de temperatura","sensor temperatura","sensor termico","sensor termico","sensore temperatura",
               "troca do sensor","troca sensor","sensor e capacitor","capacitor e sensor"],
    template: "Ola, [NOME] bom dia, sou o Pedro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario refazer a parte eletrica que causou danos no conjunto do sensor de temperatura e capacitor, as pecas serao trocadas tambem. Este conserto completo fica em [VALOR] reais apenas. Aprovando ja iniciamos o conserto.",
    valor: "320",
  },
  // 5. Microchave → R$ 320
  {
    keywords: ["microchave","micro chave","micro-chave","chave micro"],
    template: "Ola, [NOME] bom dia, sou o Pedro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario refazer a parte eletrica que causou danos no conjunto do capacitor e microchave de acionamento, as pecas serao trocadas tambem. Este conserto completo fica em [VALOR] reais apenas. Aprovando ja iniciamos o conserto.",
  },
  // 6. Membrana → R$ 320
  {
    keywords: ["membrana","membrane","menbrana"],
    template: "Ola, [NOME] bom dia, sou o Pedro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario refazer a parte eletrica que causou danos no conjunto da membrana, as pecas serao trocadas tambem. Este conserto completo fica em [VALOR] reais apenas. Aprovando ja iniciamos o conserto.",
  },
  // 7. Placa micra → R$ 320
  {
    keywords: ["placa micra","placa microondas","placa do microondas","placa micro"],
    template: "Ola, [NOME] bom dia, sou o Pedro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario refazer a parte eletrica que causou danos no conjunto do capacitor e placa micra, as pecas serao trocadas tambem. Este conserto completo fica em [VALOR] reais apenas. Aprovando ja iniciamos o conserto.",
  },
  // 8. Válvula de água / acionamento → R$ 370
  {
    keywords: ["valvula","válvula","valvula de agua","valvula de acionamento","troca da valvula","troca de valvula","valvula solenoide","solenoide"],
    excludeKeys: ["gas","gás","recarga","refrigerante"],
    template: "Ola, [NOME] bom dia, sou o Pedro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario fazer a troca do conjunto da valvula de acionamento de agua. Este conserto completo fica em [VALOR] reais apenas. Aprovando ja iniciamos o conserto.",
    preco: "370",
  },

  // 9. Gás → R$ 450
  {
    keywords: ["valvula de gas","valvula gas","recarga de gas","recarga gas","gas refrigerante","carga de gas"],
    template: "Ola, [NOME] bom dia, sou o Pedro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario fazer a troca da valvula de gas, solda e recarga de gas refrigerante. Este conserto completo fica em [VALOR] reais apenas. Aprovando ja iniciamos o conserto.",
    preco: "450",
  },
  // 9. Hidráulica → R$ 350 ou R$ 450 se tiver motor/gas
  {
    keywords: ["mangueira","conexao","conexoes","duto","dutos","hidraulica","hidraulico","vazando","vazamento"],
    extraKeys: ["motor","gas","compressor"],
    templateBase: "Ola, [NOME] bom dia, sou o Pedro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario fazer a troca dos dutos e conexoes hidraulicas. Este conserto completo fica em [PRECO] reais apenas. Aprovando ja iniciamos o conserto.",
    precoBase:  "350",
    precoExtra: "450",
  },
  // 10. Forno — parte elétrica genérica → R$ 450
  {
    keywords: ["parte eletrica","parte elétrica","reoperacao eletrica","reoperação eletrica"],
    // Só aplica se NÃO tiver peça específica (timer, resistencia etc.)
    excludeKeys: ["timer","resistencia","resistência","termostato","termóstato"],
    template: "Ola, [NOME] bom dia, sou o Pedro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario refazer a parte eletrica que esta sobrecarregando o equipamento, sera feito a reoperacao eletrica. Este conserto completo fica em [VALOR] reais apenas. Aprovando ja iniciamos o conserto.",
  },
  // 11. Forno — timer → R$ 450
  {
    keywords: ["timer","timmer","tmer"],
    templateBase: "Ola, [NOME] bom dia, sou o Pedro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario refazer a parte eletrica que causou danos no conjunto do timer que esta sobrecarregando o equipamento, as pecas serao trocadas tambem. Este conserto completo fica em [PRECO] reais apenas. Aprovando ja iniciamos o conserto.",
    precoBase: "450", precoExtra: "450",
  },
  // 12. Forno — resistência → R$ 450
  {
    keywords: ["resistencia","resistência","rezistencia","rezistência"],
    templateBase: "Ola, [NOME] bom dia, sou o Pedro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario refazer a parte eletrica que causou danos no conjunto da resistencia que esta sobrecarregando o equipamento, as pecas serao trocadas tambem. Este conserto completo fica em [PRECO] reais apenas. Aprovando ja iniciamos o conserto.",
    precoBase: "450", precoExtra: "450",
  },
  // 13. Forno — termostato → R$ 450
  {
    keywords: ["termostato","termóstato","termostat","termostast"],
    templateBase: "Ola, [NOME] bom dia, sou o Pedro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario refazer a parte eletrica que causou danos no conjunto do termostato que esta sobrecarregando o equipamento, as pecas serao trocadas tambem. Este conserto completo fica em [PRECO] reais apenas. Aprovando ja iniciamos o conserto.",
    precoBase: "450", precoExtra: "450",
  },
  // 14. Placa principal / recuperação de placa → R$ 350
  {
    keywords: ["placa principal","placa de potencia","placa potencia","placa de controle","placa controle",
               "recuperacao da placa","recuperação da placa","recupera da placa","recuperar placa","reoperacao","reoperação"],
    template: "Ola, [NOME] bom dia, sou o Pedro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario refazer a parte eletrica que causou danos no conjunto da placa principal, sera feito a reoperacao da placa tambem. Este conserto completo fica em [VALOR] reais apenas. Aprovando ja iniciamos o conserto.",
  },

  // 15. Display → R$ 370
  {
    keywords: ["display","teclado display","painel display","troca do display","troca de display","display microondas"],
    template: "Ola, [NOME] bom dia, sou o Pedro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario refazer a parte eletrica que causou danos no conjunto do display, as pecas serao trocadas tambem. Este conserto individual fica em [VALOR] reais apenas. Aprovando ja iniciamos o conserto.",
    preco: "370",
  },
];


// Preços sugeridos por índice de regra (mesma ordem de ORCAMENTO_REGRAS)
var PRECOS_REGRAS = ["390","350","370","320","320","320","320","320","370","450","350","450","450","450","450","370","350"];

// ── DETECTA TIPO DE EQUIPAMENTO ──────────────────────────────────
// ── DETECTA MÚLTIPLOS EQUIPAMENTOS NAS NOTAS ────────────────────
// Formato: "equipamento: descricao do problema // equipamento2: descricao"
function splitEquipamentos(notas) {
  if (!notas) return null;
  // Detecta padrão "palavra: texto" separados por linha em branco ou nova linha
  // Ex: "purificador: troca do kit // bebedouro: troca das conexoes"
  const partes = notas.split(/\n\s*\n/).map(p => p.trim()).filter(Boolean);
  if (partes.length < 2) return null;
  
  const equipamentos = [];
  for (const parte of partes) {
    const match = parte.match(/^([^:]+):\s*(.+)/s);
    if (match) {
      equipamentos.push({ nomeEquip: match[1].trim(), descProblema: match[2].trim() });
    }
  }
  return equipamentos.length >= 2 ? equipamentos : null;
}

function detectarEquipamento(desc, titulo) {
  var texto = norm([desc||"", titulo||""].join(" "));
  if (texto.includes("microondas") || texto.includes("micro ondas") || texto.includes("forno micro")) return "microondas";
  if (texto.includes("purificador") || texto.includes("purif")) return "purificador";
  if (texto.includes("adega"))  return "adega";
  if (texto.includes("forno"))  return "forno";
  if (texto.includes("geladeira") || texto.includes("refrigerador")) return "geladeira";
  if (texto.includes("lavadora") || texto.includes("maquina de lavar") || texto.includes("lava")) return "lavadora";
  if (texto.includes("secadora") || texto.includes("centrifuga")) return "secadora";
  if (texto.includes("lava loucas") || texto.includes("lava-loucas") || texto.includes("lava louça")) return "lava-loucas";
  return null;
}

// Verifica se o equipamento é "grande" (ex: forno grande, adega grande)
function isGrande(desc, titulo) {
  var texto = norm([desc||"", titulo||""].join(" "));
  return texto.includes("grande");
}

// Gera linha de equipamento para orçamento multi
function linhaEquipamento(equip, descDiagnostico) {
  if (!equip) return descDiagnostico;
  var nomes = {
    "microondas": "Microondas", "purificador": "Purificador",
    "adega": "Adega", "forno": "Forno", "geladeira": "Geladeira",
    "lavadora": "Lavadora", "secadora": "Secadora", "lava-loucas": "Lava-Louças"
  };
  return (nomes[equip] || equip) + ":\n" + descDiagnostico;
}

function detectarRegra(desc, comentarios) {
  var textoNorm = norm([desc || ""].concat(comentarios || []).join(" "));
  for (var i = 0; i < ORCAMENTO_REGRAS.length; i++) {
    var regra = ORCAMENTO_REGRAS[i];
    if (!hasAny(textoNorm, regra.keywords)) continue;
    if (regra.excludeKeys && hasAny(textoNorm, regra.excludeKeys)) continue;
    if (regra.templateBase) {
      var comExtra = regra.extraKeys && hasAny(textoNorm, regra.extraKeys);
      var preco = comExtra ? regra.precoExtra : regra.precoBase;
      return { texto: regra.templateBase, preco: preco };
    }
    return { texto: regra.template, preco: PRECOS_REGRAS[i] || null };
  }
  return null;
}


function primeiroNome(nome) {
  return nome ? nome.trim().split(/\s+/)[0] : "";
}

function substituirNome(template, nome) {
  var p = primeiroNome(nome);
  return template.replace(/\[NOME\]/g, p);
}

function templatePadrao(desc, nome) {
  var p = primeiroNome(nome);
  var saud = p ? "Ola, " + p + " bom dia" : "Ola, bom dia";
  return saud + ", sou o Pedro da Reparo Eletro, vou te enviar agora o orcamento:\n\nRealizamos todos os testes e identificamos o problema. Faremos o reparo completo com substituicao das pecas necessarias.\n\nEste conserto completo fica em [VALOR] apenas. Aprovando ja iniciamos o conserto.";
}

// ── DETECTA MÚLTIPLOS EQUIPAMENTOS ────────────────────────────
// Formato do Pipefy: "m:troca do fusível e capacitor peca 40 mtroca do magnétron peca 110"
// Cada equipamento começa com "m:" ou "m" no meio do texto
function detectarMultiplosEquipamentos(comentarios) {
  const texto = (comentarios || []).join(" ");
  const raw = texto.trim();

  // Divide onde termina "peca NUMERO" seguido de novo equipamento com "m"
  // Ex: "m:troca do fusível e capacitor peca 40 mtroca do magnétron peca 110"
  const blocos = raw.split(/(?<=pe[cç]a\s+\d+)\s+m/i).filter(s => s.trim());
  if (blocos.length >= 2) return parseBlocos(blocos);

  // Fallback: split por "m:" no início de cada bloco
  const blocos2 = raw.split(/(?:^|\s+)m:/i).filter(s => s.trim());
  if (blocos2.length >= 2) return parseBlocos(blocos2);

  return null;
}

function parseBlocos(blocos) {
  const partes = [];
  for (const bloco of blocos) {
    // Remove prefixos m: ou m do início
    const trimmed = bloco.trim().replace(/^m[:.\s]*/i, "").trim();
    if (trimmed.length < 3) continue;
    // Remove trecho "peca NUMERO" — é custo de peça, não preço do orçamento
    const descBloco = trimmed.replace(/\s*pe[cç]a\s+\d+\s*/gi, " ").trim();
    if (descBloco) partes.push({ desc: descBloco });
  }
  return partes.length >= 2 ? partes : null;
}

function gerarTextoMultiplos(partes, nome) {
  const primeiro = nome ? nome.trim().split(/\s+/)[0] : "";
  const saud = primeiro || "cliente";
  let total = 0;
  let linhas = [];

  for (let i = 0; i < partes.length; i++) {
    const p = partes[i];
    const n = i + 1;
    // Detecta preço pela regra de cada equipamento individual
    const regra = detectarRegra(p.desc, []);
    const preco = regra ? parseInt(regra.preco || "0") : 0;
    if (preco) total += preco;

    const textoEquip = gerarDescricaoEquip(p.desc);
    linhas.push("Em relacao ao microondas " + n + " " + textoEquip + " Este conserto individual fica em " + (preco ? preco + " reais" : "[VALOR]") + ".");
  }

  // Desconto combo: ~10% arredondado para dezena
  const desconto = total > 0 ? Math.round(total * 0.9 / 10) * 10 : null;

  let msg = "Ola, " + saud + ", foram feitos todos os testes:\n\n";
  msg += linhas.join("\n\n");

  if (desconto && total > desconto) {
    msg += "\n\nConsertando os " + partes.length + " juntos eu consigo um desconto para voce de " + total + " reais por " + desconto + " apenas. Aprovando ja iniciamos o conserto.";
  } else {
    msg += "\n\nAprovando ja iniciamos o conserto.";
  }

  return { texto: msg, preco: desconto ? String(desconto) : String(total || "") };
}

function gerarDescricaoEquip(desc) {
  const n = norm(desc);
  if (hasAny(n, ["magnetron","magnetrao","magneton","magentron"])) {
    return "sera necessario fazer a troca do conjunto do magnetron, sera feito a reoperacao eletrica tambem.";
  }
  if (hasAny(n, ["fusivel","fusível","fusirel","capacitor"])) {
    return "sera necessario refazer a parte eletrica que causou danos no conjunto do capacitor e fusivel de alta que estao sobrecarregando o sistema, as pecas serao trocadas tambem.";
  }
  if (hasAny(n, ["microchave","micro chave"])) {
    return "sera necessario refazer a parte eletrica que causou danos no conjunto do capacitor e microchave de acionamento, as pecas serao trocadas tambem.";
  }
  if (hasAny(n, ["membrana"])) {
    return "sera necessario refazer a parte eletrica que causou danos no conjunto da membrana, as pecas serao trocadas tambem.";
  }
  if (hasAny(n, ["placa micra","placa micro"])) {
    return "sera necessario refazer a parte eletrica que causou danos no conjunto do capacitor e placa micra, as pecas serao trocadas tambem.";
  }
  if (hasAny(n, ["termoeletrico","cooler","peltier","pasta termica"])) {
    return "sera necessario refazer a parte eletrica que causou danos no conjunto do cooler, placa de resfriamento e pasta termica, as pecas serao trocadas tambem.";
  }
  if (hasAny(n, ["placa principal","reoperacao","recuperacao"])) {
    return "sera necessario refazer a parte eletrica que causou danos no conjunto da placa principal, sera feito a reoperacao da placa tambem.";
  }
  return "sera necessario realizar o reparo identificado nos testes, as pecas necessarias serao trocadas.";
}

async function gerarTextoOrcamento(desc, comentarios, nome) {
  // Verifica múltiplos equipamentos primeiro
  const multiplos = detectarMultiplosEquipamentos(comentarios);
  if (multiplos) return gerarTextoMultiplos(multiplos, nome);

  var regra = detectarRegra(desc, comentarios);
  if (regra) {
    var precoBase = parseFloat(regra.preco || "0");
    // Regra "grande": adiciona R$300 se o equipamento for grande (forno grande, adega grande)
    var equip = detectarEquipamento(desc, "");
    if (isGrande(desc, "") && (equip === "forno" || equip === "adega") && precoBase > 0) {
      precoBase += 300;
      regra = Object.assign({}, regra, { preco: String(precoBase) });
    }
    return { texto: substituirNome(regra.texto, nome), preco: regra.preco };
  }

  var primeiro = primeiroNome(nome) || "cliente";
  var comStr   = (comentarios || []).join("; ");
  var userMsg  = "Nome: " + primeiro + "\r\nDefeito: " + (desc || "nao informado") + (comStr ? "\r\nAtividades: " + comStr : "");
  var sysMsg   = "Voce e Pedro da Reparo Eletro. Gere orcamento: Ola, NOME bom dia, sou o Pedro da Reparo Eletro, vou te enviar agora o orcamento: [diagnostico]. Este conserto completo fica em [VALOR] apenas. Aprovando ja iniciamos o conserto. Use o primeiro nome real, deixe [VALOR] literal.";

  try {
    var controller = typeof AbortController !== "undefined" ? new AbortController() : null;
    var timer = controller ? setTimeout(function() { controller.abort(); }, 8000) : null;
    var fetchOpts = {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ model: "claude-sonnet-4-20250514", max_tokens: 400, system: sysMsg, messages: [{ role: "user", content: userMsg }] }),
    };
    if (controller) fetchOpts.signal = controller.signal;
    var res  = await fetch("https://api.anthropic.com/v1/messages", fetchOpts);
    if (timer) clearTimeout(timer);
    var data = await res.json();
    var texto = (data.content && data.content[0] && data.content[0].text) ? data.content[0].text : "";
    if (texto && texto.indexOf("[VALOR]") >= 0) return { texto: texto, preco: null };
    if (texto && texto.length > 20) return { texto: texto + "\r\n\r\nEste conserto completo fica em [VALOR] apenas. Aprovando ja iniciamos o conserto.", preco: null };
  } catch(e) {
    console.error("gerarTextoOrcamento:", String(e.message || e));
  }

  return { texto: templatePadrao(desc, nome), preco: null };
}


// Busca campos e atividades de um card específico pelo ID
async function fetchCardData(pipefyId) {
  const data = await pipefyQuery(`query {
    card(id: "${pipefyId}") {
      id title
      fields { name value }
      comments { text }
    }
  }`);
  const node   = data?.card;
  if (!node) return null;
  const fields = node.fields || [];
  const nome  = fields.find(f => f.name.toLowerCase().includes("nome"))?.value || node.title;
  const tel   = fields.find(f => f.name.toLowerCase().includes("telefone") || f.name.toLowerCase().includes("fone"))?.value || "";
  const desc  = fields.find(f => f.name.toLowerCase().includes("descri"))?.value || "";
  const end   = fields.find(f => f.name.toLowerCase().includes("endere"))?.value || "";
  const extras = fields
    .filter(f => !["telefone","fone","nome","endere","valor"].some(k => f.name.toLowerCase().includes(k)))
    .map(f => f.value).filter(Boolean);
  const comentarios = [
    ...(node.comments || []).map(c => c.text).filter(Boolean),
    ...extras,
  ];
  return { pipefyId: String(node.id), title: node.title, nome, tel, desc, end, comentarios };
}

// Busca cards em Aguardando Aprovação direto pelo ID da fase (mais rápido e completo)

// Gera texto de orçamento com Claude
// ── NORMALIZA TEXTO (remove acentos, minúsculo) ──────────────
// Busca campos e atividades de um card específico pelo ID
async function fetchCardData(pipefyId) {
  const data = await pipefyQuery(`query {
    card(id: "${pipefyId}") {
      id title
      fields { name value }
      comments { text }
    }
  }`);
  const node   = data?.card;
  if (!node) return null;
  const fields = node.fields || [];
  const nome  = fields.find(f => f.name.toLowerCase().includes("nome"))?.value || node.title;
  const tel   = fields.find(f => f.name.toLowerCase().includes("telefone") || f.name.toLowerCase().includes("fone"))?.value || "";
  const desc  = fields.find(f => f.name.toLowerCase().includes("descri"))?.value || "";
  const end   = fields.find(f => f.name.toLowerCase().includes("endere"))?.value || "";
  const extras = fields
    .filter(f => !["telefone","fone","nome","endere","valor"].some(k => f.name.toLowerCase().includes(k)))
    .map(f => f.value).filter(Boolean);
  const comentarios = [
    ...(node.comments || []).map(c => c.text).filter(Boolean),
    ...extras,
  ];
  return { pipefyId: String(node.id), title: node.title, nome, tel, desc, end, comentarios };
}

// Busca cards em Aguardando Aprovação direto pelo ID da fase (mais rápido e completo)

// Gera texto de orçamento com Claude
// ── REGRAS DE ORÇAMENTO ──────────────────────────────────────
// Cada regra: { keywords, template }
// keywords: palavras-chave buscadas em QUALQUER campo do card (desc + comentarios)
// template: texto final com [NOME] como placeholder do nome do cliente
