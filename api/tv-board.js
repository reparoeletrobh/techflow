// api/tv-board.js — Board do painel TV (independente do Reparo Eletro)
const PIPE_ID   = "306904889";
const BOARD_KEY = "tv_board";
const LOGS_KEY  = "tv_logs";
const LIBERADO_ROTA_PHASE_ID = "341638193"; // "Liberado para Rota"
const UPSTASH_URL   = (process.env.UPSTASH_URL   || "").replace(/['"]/g,"").trim();
const UPSTASH_TOKEN = (process.env.UPSTASH_TOKEN || "").replace(/['"]/g,"").trim();

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

async function dbSet(key, val) {
  try {
    await fetch(`${UPSTASH_URL}/pipeline`, {
      method: "POST",
      headers: { Authorization: `Bearer ${UPSTASH_TOKEN}`, "Content-Type": "application/json" },
      body: JSON.stringify([["SET", key, JSON.stringify(val)]]),
    });
    return true;
  } catch(e) { return false; }
}

async function pipefyQuery() {
  // Pipefy desconectado — TV opera 100% local (Redis)
  return null;
}

function defaultBoard() {
  return {
    phases: [
      { id: "aprovado",        name: "Aprovado"           },
      { id: "barramento",      name: "Barramento"         },
      { id: "producao",        name: "Produção"           },
      { id: "urgencia",        name: "Urgência"           },
      { id: "condenado",       name: "Condenado"          },
      { id: "comprar_peca",    name: "Comprar Peça"       },
      { id: "aguardando_peca", name: "Aguardando Peça"    },
      { id: "peca_disponivel", name: "Peça Disponível"    },
      { id: "loja_feito",      name: "Loja Feito"         },
      { id: "delivery_feito",  name: "Delivery Feito"     },
      { id: "aguardando_ret",  name: "Aguardando Retirada"},
      { id: "liberado_rota",   name: "Liberado para Rota" },
    ],
    cards:     [],
    syncedIds: [],
    movesLog:  [],
    metaLog:   [],
  };
}

function sanitizeBoard(b) {
  if (!b) return defaultBoard();
  if (!Array.isArray(b.phases))    b.phases    = defaultBoard().phases;
  // Garantir fase barramento existe (adicionada em 18/06/2026)
  if (!b.phases.find(function(p){ return p.id === 'barramento'; })) {
    var idxAprov = b.phases.findIndex(function(p){ return p.id === 'aprovado'; });
    var insAt = idxAprov >= 0 ? idxAprov + 1 : 1;
    b.phases.splice(insAt, 0, { id: 'barramento', name: 'Barramento' });
  }
  if (!b.phases.find(function(p){ return p.id === 'condenado'; })) {
    var idxUrg = b.phases.findIndex(function(p){ return p.id === 'urgencia'; });
    var insAt2 = idxUrg >= 0 ? idxUrg + 1 : 3;
    b.phases.splice(insAt2, 0, { id: 'condenado', name: 'Condenado' });
  }
  if (!Array.isArray(b.cards))     b.cards     = [];
  if (!Array.isArray(b.syncedIds)) b.syncedIds = [];
  if (!Array.isArray(b.movesLog))  b.movesLog  = [];
  if (!Array.isArray(b.metaLog))   b.metaLog   = [];
  return b;
}

function trimLog(arr, max = 500) {
  return arr.length > max ? arr.slice(-max) : arr;
}

async function saveLogs(board) {
  await dbSet(LOGS_KEY, { movesLog: board.movesLog || [], metaLog: board.metaLog || [] });
}

// Busca cards aprovados do Pipefy
async function fetchApprovedCards() {
  const data = await pipefyQuery(`query {
    pipe(id: "${PIPE_ID}") {
      phases {
        name
        cards(first: 50) {
          edges {
            node {
              id title
              fields { name value }
            }
          }
        }
      }
    }
  }`);
  const phases = data?.pipe?.phases || [];
  const cards = [];
  for (const ph of phases) {
    const l = ph.name.toLowerCase();
    if (l.includes("aprovado") || l.includes("recebido") || l.includes("produção") || l.includes("producao")) {
      for (const { node } of (ph.cards?.edges || [])) {
        const fields  = node.fields || [];
        const nomeF   = fields.find(f => f.name.toLowerCase().includes("nome"));
        const telF    = fields.find(f => f.name.toLowerCase().includes("telefone") || f.name.toLowerCase().includes("fone"));
        const endF    = fields.find(f => f.name.toLowerCase().includes("endere"));
        const descF   = fields.find(f => f.name.toLowerCase().includes("descri"));
        const title   = node.title || "";
        const m       = title.match(/^(.*?)\s+(\d{3,6})$/);
        cards.push({
          pipefyId:    String(node.id),
          osCode:      m ? m[2] : null,
          nomeContato: nomeF?.value?.trim() || (m ? m[1].trim() : title),
          telefone:    telF?.value || null,
          endereco:    endF?.value || null,
          descricao:   descF?.value || null,
          title,
        });
      }
    }
  }
  return cards;
}

// Busca IDs de cards em fases de conclusão
async function fetchDoneIds() {
  try {
    const data = await pipefyQuery(`query {
      pipe(id: "${PIPE_ID}") {
        phases {
          name
          cards(first: 50) { edges { node { id } } }
        }
      }
    }`);
    const ids = [];
    for (const ph of (data?.pipe?.phases || [])) {
      const l = ph.name.toLowerCase();
      if (l.includes("erp") || l.includes("finaliz") || l.includes("conclu") ||
          l.includes("descar") || l.includes("reprov")) {
        ph.cards.edges.forEach(e => ids.push(String(e.node.id)));
      }
    }
    return ids;
  } catch(e) { return []; }
}

// Busca fase atual de um card
async function fetchCardPhase(pipefyId) {
  try {
    const data = await pipefyQuery(`query { card(id: "${pipefyId}") { current_phase { name } } }`);
    if (!data?.card) return "NOT_FOUND";
    return data.card.current_phase?.name || "NOT_FOUND";
  } catch(e) { return "NOT_FOUND"; }
}

// Move card no Pipefy
async function moveCardPipefy() { return { ok: false }; }

// ── GEOCODIFICAÇÃO — cobre RMBH (Região Metropolitana de BH) ──
// Cidades: BH, Contagem, Betim, Ribeirão das Neves, Santa Luzia,
//          Nova Lima, Vespasiano, Lagoa Santa, Ibirité, Sabará, etc.
const RMBH_CIDADES = [
  "belo horizonte","contagem","betim","ribeirão das neves","ribeiro das neves",
  "santa luzia","nova lima","vespasiano","lagoa santa","ibirité","ibirite",
  "sabará","sabara","pedro leopoldo","esmeraldas","brumadinho","sarzedo",
  "mario campos","mario campos","mateus leme","confins","taquaraçu de minas",
  "jaboticatubas","capim branco","matozinhos","funilândia","funilândia",
  "paraopeba","baldim","jequitibá","prudente de morais","caetanópolis"
];

async function geocodificar(endereco) {
  const GMAPS_KEY    = (process.env.GOOGLE_MAPS_KEY || "").trim();
  const OPENCAGE_KEY = (process.env.OPENCAGE_KEY    || "").trim();

  // Normaliza: remove complementos, expande abreviações, substitui "BH"
  const endNorm = endereco
    .replace(/,?\s*\bBH\b/gi, ", Belo Horizonte")
    .replace(/\bR\.\s+/g, "Rua ")
    .replace(/\bAv\.\s+/g, "Avenida ")
    .replace(/\bAl\.\s+/g, "Alameda ")
    .replace(/,?\s*[-]?\s*(ap(to)?\.?|apartamento|bloco|bl\.?|sala|lote)\s*[\w\d]+/gi, "")
    .replace(/\s+/g, " ").trim();

  // Verifica se já menciona alguma cidade da RMBH
  const endLow = endNorm.toLowerCase();
  const temCidade = RMBH_CIDADES.some(function(ct) { return endLow.includes(ct); });

  // Se não mencionar cidade, adiciona "Região Metropolitana de Belo Horizonte, MG, Brasil"
  const endFull = temCidade
    ? endNorm + ", MG, Brasil"
    : endNorm + ", Região Metropolitana de Belo Horizonte, MG, Brasil";

  // Bbox RMBH (maior que só BH): lat -20.8 a -18.8, lng -45.2 a -43.0
  const dentoRMBH = function(lat, lng) {
    return lat > -20.8 && lat < -18.8 && lng > -45.2 && lng < -43.0;
  };

  const nomQuery = async function(q) {
    // viewbox cobre toda a RMBH, bounded=0 para não limitar demais
    const url = "https://nominatim.openstreetmap.org/search?q=" + encodeURIComponent(q)
      + "&format=json&limit=5&countrycodes=br"
      + "&viewbox=-45.2,-20.8,-43.0,-18.8&bounded=0";
    const r = await fetch(url, { headers: { "User-Agent": "TVAssistencia/1.0 (reparoeletroadm.com)" } });
    const j = await r.json();
    return (j || []).find(function(x) { return dentoRMBH(parseFloat(x.lat), parseFloat(x.lon)); }) || null;
  };

  try {
    // Tentativa 1: endereço completo
    let best = await nomQuery(endFull);
    // Tentativa 2: sem número
    if (!best) {
      const semNum = endFull.replace(/,?\s*\d+[-\w]*/g, "").replace(/\s+/g, " ").trim();
      if (semNum !== endFull) best = await nomQuery(semNum);
    }
    // Tentativa 3: só bairro/cidade + RMBH
    if (!best) {
      const partes = endNorm.split(",").map(function(p){ return p.trim(); }).filter(Boolean);
      for (let i = partes.length - 1; i >= 1; i--) {
        best = await nomQuery(partes[i] + ", Região Metropolitana de Belo Horizonte, MG, Brasil");
        if (best) break;
      }
    }
    if (best) return { lat: parseFloat(best.lat), lng: parseFloat(best.lon) };
  } catch(e) { console.error("Nominatim:", e.message); }

  // Fallback Google Maps
  if (GMAPS_KEY) {
    try {
      const r = await fetch("https://maps.googleapis.com/maps/api/geocode/json?address="
        + encodeURIComponent(endFull) + "&region=br&key=" + GMAPS_KEY);
      const j = await r.json();
      if (j.status === "OK" && j.results && j.results[0]) {
        const loc = j.results[0].geometry.location;
        if (dentoRMBH(loc.lat, loc.lng)) return { lat: loc.lat, lng: loc.lng };
      }
    } catch(e) { console.error("GMaps:", e.message); }
  }

  // Fallback OpenCage
  if (OPENCAGE_KEY) {
    try {
      const r = await fetch("https://api.opencagedata.com/geocode/v1/json?q="
        + encodeURIComponent(endFull)
        + "&key=" + OPENCAGE_KEY
        + "&countrycode=br&limit=5&no_annotations=1&proximity=-19.9245,-43.9352");
      const j = await r.json();
      const ok = (j.results || []).filter(function(x) {
        return (x.confidence || 0) >= 5 && dentoRMBH(x.geometry.lat, x.geometry.lng);
      });
      if (ok.length) return { lat: ok[0].geometry.lat, lng: ok[0].geometry.lng };
    } catch(e) { console.error("OpenCage:", e.message); }
  }

  return null;
}

// Distância euclidiana (graus) entre dois pontos
function distGraus(a, b) {
  const dlat = a.lat - b.lat;
  const dlng = (a.lng - b.lng) * Math.cos(a.lat * Math.PI / 180);
  return Math.sqrt(dlat*dlat + dlng*dlng);
}

module.exports = async function handler(req, res) {
  // 🔐 TF-AUTH (Fase 1): chave obrigatória em toda chamada
  const _tfk = (req.query && req.query.k) || req.headers['x-tf-key'] || '';
  if (_tfk !== ((process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim())) {
    return res.status(401).json({ ok: false, error: 'não autorizado' });
  }

  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(200).end();

  const action = req.query.action || "";

  try {

    // ── GET load ────────────────────────────────────────────────
    if (action === "load") {
      const board = sanitizeBoard(await dbGet(BOARD_KEY));
      return res.status(200).json({ ok: true, board });
    }

    // ── GET listar-fases ─────────────────────────────────────────
    if (action === "listar-fases") {
      const data = await pipefyQuery(`query { pipe(id: "${PIPE_ID}") { phases { id name } } }`);
      const phases = data?.pipe?.phases || [];
      return res.status(200).json({ ok: true, pipe: PIPE_ID, phases });
    }

    // ── GET sync — sincroniza board com Pipefy ───────────────────
    if (action === "sync") {
      const board = sanitizeBoard(await dbGet(BOARD_KEY));
      let novos = 0, pipefyError = null;
      try {
        const aprovados = await fetchApprovedCards();
        const doneIds   = await fetchDoneIds();
        // Remove cards finalizados
        board.cards = board.cards.filter(function(c) { return !doneIds.includes(c.pipefyId); });
        // Carrega coleta (modelo + peca do diagnostico) e compras de uma vez
        let _cpDb=null,_cpChanged=false,_coletaCards=[];
        try{_cpDb=(await dbGet("tv_compras_pecas"))||{pecas:[]};if(!Array.isArray(_cpDb.pecas))_cpDb.pecas=[];}catch(_e){}
        try{const _cr=await dbGet("tv_coleta_cards");_coletaCards=(_cr&&_cr.cards)?_cr.cards:[];}catch(_e){}
        for (const c of aprovados) {
          // Cria compra independente de syncedIds — recria se foi limpo
          if(_cpDb&&!_cpDb.pecas.some(function(p){return p.pipefyId===String(c.pipefyId);})){
            const _cc=_coletaCards.find(function(cc){return String(cc.pipefyId)===String(c.pipefyId);});
            _cpDb.pecas.unshift({
              id:Date.now().toString(36)+Math.random().toString(36).slice(2,5),
              origem:"tv_aprovado",pipefyId:String(c.pipefyId),
              os:c.osCode||String(c.pipefyId).slice(-4),
              nomeContato:c.nomeContato||c.title||"—",
              descricao:c.descricao||c.title||"TV aprovada",
              modelo:_cc?(_cc.modelo||null):null,
              peca:_cc?(_cc.diagnostico||null):null,
              diagnosticoResumo:c.diagnosticoResumo||'',
              modeloTv:c.modeloTv||(_cc?(_cc.modelo||''):''),
              status:"pendente",createdAt:new Date().toISOString(),
              urgente:false,obs:"",quantidade:1
            });
            _cpChanged=true;
          }
          // Adiciona ao board somente se for novo
          if(!board.syncedIds.includes(c.pipefyId)){
            board.cards.push(Object.assign({phaseId:"aprovado"},c));
            board.syncedIds.push(c.pipefyId);
            novos++;
          }
        }
        if(_cpDb&&_cpChanged){try{await dbSet("tv_compras_pecas",_cpDb);}catch(_e){console.error("[TVCompraAuto]",_e.message);}}
        board.syncedIds = trimLog(board.syncedIds, 2000);
        if (novos > 0) await dbSet(BOARD_KEY, board);
      } catch(e) { pipefyError = e.message; }
      return res.status(200).json({ ok: true, novos, pipefyError });
    }

    // ── POST update-card ─────────────────────────────────────────
    if (req.method === "POST" && action === "update-card") {
      const { pipefyId, endereco, nomeContato, telefone, descricao, urgente, lat, lng, geocFonte } = req.body || {};
      if (!pipefyId) return res.status(400).json({ ok: false, error: "pipefyId obrigatório" });
      const board = sanitizeBoard(await dbGet(BOARD_KEY));
      const card  = board.cards.find(function(c) { return c.pipefyId === String(pipefyId); });
      if (!card) return res.status(404).json({ ok: false, error: "Card não encontrado" });
      if (endereco    !== undefined) card.endereco    = endereco;
      if (nomeContato !== undefined) card.nomeContato = nomeContato;
      if (telefone    !== undefined) card.telefone    = telefone;
      if (descricao   !== undefined) card.descricao   = descricao;
      if (urgente     !== undefined) card.urgente     = urgente;
      if (lat         !== undefined) card.lat         = lat;
      if (lng         !== undefined) card.lng         = lng;
      if (geocFonte   !== undefined) card.geocFonte   = geocFonte;
      await dbSet(BOARD_KEY, board);
      return res.status(200).json({ ok: true, card });
    }

    // ── POST move — move card para fase (local + Pipefy) ─────────
    if (req.method === "POST" && action === "move") {
      const bd = req.body || {};
      const { pipefyId, phaseId, pipefyPhaseId, techName } = bd;
      if (!phaseId) return res.status(400).json({ ok: false, error: "phaseId obrigatório" });
      if (!pipefyId) return res.status(400).json({ ok: false, error: "pipefyId obrigatório" });
      const board = sanitizeBoard(await dbGet(BOARD_KEY));
      // Busca por pipefyId OU por id (cards pós-Pipefy)
      const card  = board.cards.find(function(c) {
        return c.pipefyId === String(pipefyId) || c.id === String(pipefyId);
      });
      if (!card) return res.status(404).json({ ok: false, error: "Card não encontrado" });
      card.phaseId = phaseId;
      if (techName) card.techName = techName;
      // Salvar campos extras do barramento
      if (bd.polegadas    !== undefined) card.polegadas    = bd.polegadas;
      if (bd.pecaDiag     !== undefined) card.pecaDiag     = bd.pecaDiag;
      if (bd.descricaoDiag!== undefined) card.descricaoDiag= bd.descricaoDiag;
      if (bd.isBarramento !== undefined) card.isBarramento = bd.isBarramento;
      card.movidoEm = new Date().toISOString();
      board.movesLog = trimLog([...(board.movesLog || []),
        { id: pipefyId, phase: phaseId, tech: techName, ts: Date.now() }]);
      await dbSet(BOARD_KEY, board);

      // ── Gatilho: loja_feito → tv_frenteloja conserto-realizado ──────────────
      if (phaseId === 'loja_feito') {
        try {
          const _U=(process.env.UPSTASH_URL||'').replace(/['"]/g,'').trim();
          const _T=(process.env.UPSTASH_TOKEN||'').replace(/['"]/g,'').trim();
          async function _flGet(k){const r=await fetch(_U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+_T,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});const j=await r.json();let v=j[0]?.result;if(!v)return null;try{let x=JSON.parse(v);if(typeof x==='string')x=JSON.parse(x);return x;}catch(e){return null;}}
          async function _flSet(k,v){await fetch(_U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+_T,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});}
          const FL_KEY = 'tv_frenteloja';
          const flDb   = await _flGet(FL_KEY);
          if (flDb && Array.isArray(flDb.fichas)) {
            const now2 = new Date().toISOString();
            // Buscar por fichaId (campo no card do board) ou por nome+tel
            let ficha = card.fichaId
              ? flDb.fichas.find(function(f){ return f.id === String(card.fichaId); })
              : flDb.fichas.find(function(f){
                  return f.phase === 'producao' &&
                    (card.nomeContato||'').toLowerCase().includes((f.nomeContato||'').toLowerCase().split(' ')[0]) ||
                    (f.telefone||'').replace(/\D/g,'').slice(-4) === (card.telefone||'').replace(/\D/g,'').slice(-4);
                });
            if (ficha && ficha.phase === 'producao') {
              ficha.phase      = 'conserto_realizado';
              ficha.consertoEm = now2;
              ficha.updatedAt  = now2;
              await _flSet(FL_KEY, flDb);
              console.log('[tv-board loja_feito→frenteloja] conserto-realizado:', ficha.id, ficha.nomeContato);
            }
          }
        } catch(eFL) { console.error('[tv-board loja_feito→frenteloja]', eFL.message); }
      }

      // Move no Pipefy se phase ID real foi fornecido
      let pipefyResult = null;
      if (pipefyPhaseId) {
        try { pipefyResult = await moveCardPipefy(pipefyId, pipefyPhaseId); }
        catch(e) { pipefyResult = { error: e.message }; }
      }
      return res.status(200).json({ ok: true, card, pipefy: pipefyResult });
    }

    // ── POST move-rs — move para RS ──────────────────────────────
    if (req.method === "POST" && action === "move-rs") {
      const { pipefyId } = req.body || {};
      if (!pipefyId) return res.status(400).json({ ok: false, error: "pipefyId obrigatório" });
      const board = sanitizeBoard(await dbGet(BOARD_KEY));
      const card  = board.cards.find(function(c) { return c.pipefyId === String(pipefyId); });
      if (!card) return res.status(404).json({ ok: false, error: "Card não encontrado" });
      card.phaseId  = "rs";
      card.movidoEm = new Date().toISOString();
      board.movesLog = trimLog([...(board.movesLog || []),
        { id: pipefyId, phase: "rs", ts: Date.now() }]);
      await dbSet(BOARD_KEY, board);
      return res.status(200).json({ ok: true, card });
    }

    // ── POST move-batch — move vários cards de uma vez ───────────
    if (req.method === "POST" && action === "move-batch") {
      const { cardIds, phaseId, pipefyPhaseId } = req.body || {};
      if (!Array.isArray(cardIds) || !phaseId)
        return res.status(400).json({ ok: false, error: "cardIds e phaseId obrigatórios" });
      const board = sanitizeBoard(await dbGet(BOARD_KEY));
      const movidos = [];
      for (const id of cardIds) {
        const card = board.cards.find(function(c) { return c.pipefyId === String(id); });
        if (card) {
          card.phaseId  = phaseId;
          card.movidoEm = new Date().toISOString();
          movidos.push(card);
        }
      }
      await dbSet(BOARD_KEY, board);
      // Move no Pipefy em paralelo se fornecido
      if (pipefyPhaseId) {
        await Promise.allSettled(movidos.map(function(c) {
          return moveCardPipefy(c.pipefyId, pipefyPhaseId).catch(function(){});
        }));
      }
      return res.status(200).json({ ok: true, movidos: movidos.length });
    }

    // ── POST cleanup-ret — remove cards em fases finais ──────────
    if (req.method === "POST" && action === "cleanup-ret") {
      const board = sanitizeBoard(await dbGet(BOARD_KEY));
      const antes  = board.cards.length;
      try {
        const doneIds = await fetchDoneIds();
        board.cards = board.cards.filter(function(c) { return !doneIds.includes(c.pipefyId); });
      } catch(e) {
        const fasesFinais = ["finalizado","descarte","pronto_venda","erp","rs"];
        board.cards = board.cards.filter(function(c) { return !fasesFinais.includes(c.phaseId); });
      }
      const removidos = antes - board.cards.length;
      await dbSet(BOARD_KEY, board);
      return res.status(200).json({ ok: true, removidos });
    }

    // ── POST otimizar-rota — geocodifica + otimiza com OSRM ──────
    if (req.method === "POST" && action === "otimizar-rota") {
      const { cardIds } = req.body || {};
      if (!Array.isArray(cardIds) || !cardIds.length)
        return res.status(400).json({ ok: false, error: "cardIds obrigatório" });
      const board = sanitizeBoard(await dbGet(BOARD_KEY));
      const cards = cardIds.map(function(id) {
        return board.cards.find(function(c) { return c.pipefyId === String(id); })
          || { pipefyId: String(id), endereco: "" };
      });
      // Geocodifica com delay para não saturar Nominatim
      const coords = [];
      for (let i = 0; i < cards.length; i++) {
        const end = cards[i].endereco || "";
        if (!end) { coords.push(null); continue; }
        try { coords.push(await geocodificar(end)); }
        catch(e) { coords.push(null); }
        if (i < cards.length - 1) await new Promise(function(r) { setTimeout(r, 350); });
      }
      const comCoord  = cards.filter(function(_, i) { return coords[i] !== null; });
      const semCoord  = cards.filter(function(_, i) { return coords[i] === null; });
      const coordsVal = coords.filter(function(c) { return c !== null; });
      const oficina   = { lat: -19.9679, lng: -44.0078 }; // TV Assistência
      const { ordem, fonte, melhoria } = await otimizarPontos(coordsVal, oficina);
      const ordenados = ordem.map(function(i) { return comCoord[i]; }).concat(semCoord);
      const resultado = ordenados.map(function(card) {
        const idx = comCoord.indexOf(card);
        return {
          pipefyId:    card.pipefyId,
          nomeContato: card.nomeContato || "",
          endereco:    card.endereco || "",
          coords:      idx >= 0 ? coordsVal[idx] : null,
          geocoded:    idx >= 0,
        };
      });
      return res.status(200).json({
        ok: true, ordenados: resultado,
        semCoord: semCoord.length,
        fonte,                      // "osrm" | "local"
        melhoria2opt: melhoria,
      });
    }

// ══════════════════════════════════════════════════════════════
// OTIMIZAÇÃO DE ROTA — Pipeline:
//   1. OSRM Trip API (grafo real de estradas BH/RMBH)
//      - <10 paradas: brute force (ótimo exato)
//      - ≥10 paradas: farthest-insertion heuristic
//   2. Fallback: NN + 2-opt + Or-opt (distância euclidiana)
// ══════════════════════════════════════════════════════════════

// ── 1. OSRM Trip API ─────────────────────────────────────────
// Chama o endpoint /trip do OSRM com pontos de coleta + oficina como início fixo.
// Retorna array de índices ordenados (excluindo o 0 = oficina) ou null se falhar.
async function osrmTrip(pontos, oficina) {
  // coords: lng,lat separados por ; — oficina PRIMEIRO (source=first)
  const coords = [oficina, ...pontos]
    .map(function(p) { return p.lng.toFixed(6) + ',' + p.lat.toFixed(6); })
    .join(';');
  const url = 'https://router.project-osrm.org/trip/v1/driving/' + coords
    + '?source=first&roundtrip=false&destination=any&overview=false&annotations=false';
  try {
    const controller = new AbortController();
    const tID = setTimeout(function() { controller.abort(); }, 8000); // 8s timeout
    const r   = await fetch(url, { signal: controller.signal, headers: { 'User-Agent': 'TVAssistencia/2.0' } });
    clearTimeout(tID);
    const j = await r.json();
    if (!j || j.code !== 'Ok' || !j.waypoints) return null;
    // waypoints[i].waypoint_index = posição na rota otimizada do input[i]
    // input[0] = oficina (posição 0 garantida por source=first)
    // input[1..n] = paradas de coleta
    // Monta: ordemOSRM[posição] = índice original da parada (0-based nos pontos[])
    const n = pontos.length;
    const ordem = new Array(n);
    for (let inputIdx = 1; inputIdx <= n; inputIdx++) { // ignora oficina (input[0])
      const wp  = j.waypoints[inputIdx];
      const pos = wp.waypoint_index - 1; // -1 porque posição 0 = oficina
      if (pos >= 0 && pos < n) ordem[pos] = inputIdx - 1; // índice em pontos[]
    }
    // Verifica se todos os índices foram preenchidos
    if (ordem.some(function(x) { return x === undefined; })) return null;
    return ordem;
  } catch(e) {
    console.log('OSRM Trip falhou:', e.message);
    return null;
  }
}

// ── 2a. Nearest Neighbor ─────────────────────────────────────
function nearestNeighbor(pontos, inicio) {
  const vis = new Array(pontos.length).fill(false);
  const ord = [];
  let atual = inicio;
  for (let s = 0; s < pontos.length; s++) {
    let bIdx = -1, bDist = Infinity;
    for (let i = 0; i < pontos.length; i++) {
      if (vis[i]) continue;
      const d = distGraus(atual, pontos[i]);
      if (d < bDist) { bDist = d; bIdx = i; }
    }
    if (bIdx < 0) break;
    vis[bIdx] = true; ord.push(bIdx); atual = pontos[bIdx];
  }
  return ord;
}

// ── 2b. 2-opt ────────────────────────────────────────────────
// Remove cruzamentos de arestas
function doisOpt(pontos, ordemNN, pontoInicio) {
  let rota = ordemNN.slice();
  const n = rota.length;

  function distRota(r) {
    let d = distGraus(pontoInicio, pontos[r[0]]);
    for (let i = 0; i < r.length-1; i++) d += distGraus(pontos[r[i]], pontos[r[i+1]]);
    d += distGraus(pontos[r[r.length-1]], pontoInicio);
    return d;
  }

  let melhorou = true, iter = 0;
  while (melhorou && iter < 100) {
    melhorou = false; iter++;
    for (let i = 0; i < n-1; i++) {
      for (let j = i+1; j < n; j++) {
        const A = i === 0 ? pontoInicio : pontos[rota[i-1]];
        const B = pontos[rota[i]];
        const C = pontos[rota[j]];
        const D = j === n-1 ? pontoInicio : pontos[rota[j+1]];
        if (distGraus(A,C) + distGraus(B,D) < distGraus(A,B) + distGraus(C,D) - 1e-10) {
          let l = i, r2 = j;
          while (l < r2) { const t = rota[l]; rota[l] = rota[r2]; rota[r2] = t; l++; r2--; }
          melhorou = true;
        }
      }
    }
  }
  return rota;
}

// ── 2c. Or-opt ───────────────────────────────────────────────
// Complemento ao 2-opt: tenta *realocar* cada ponto para a melhor posição.
// Encontra melhorias que 2-opt não descobre (pontos isolados fora de lugar).
// O(n²) por passagem, converge em 2-3 iterações para rotas de 5-15 paradas.
function orOpt(pontos, rota, pontoInicio) {
  const n = rota.length;
  if (n < 3) return rota;

  // Distância total da rota
  function dist(r) {
    let d = distGraus(pontoInicio, pontos[r[0]]);
    for (let i = 0; i < r.length-1; i++) d += distGraus(pontos[r[i]], pontos[r[i+1]]);
    d += distGraus(pontos[r[r.length-1]], pontoInicio);
    return d;
  }

  let atual = rota.slice();
  let melhorou = true;
  let passes  = 0;

  while (melhorou && passes < 50) {
    melhorou = false; passes++;

    for (let i = 0; i < n; i++) {
      // Remove o ponto na posição i
      const removido = atual[i];
      const semI = atual.filter(function(_, idx) { return idx !== i; });

      // Custo do ponto removido na posição original
      const antA = i === 0 ? pontoInicio : pontos[atual[i-1]];
      const antB = pontos[removido];
      const antC = i === n-1 ? pontoInicio : pontos[atual[i+1]];
      const custoRemocao = distGraus(antA, antB) + distGraus(antB, antC) - distGraus(antA, antC);

      // Tenta inserir o ponto removido em cada outra posição
      let melhorGanho = 1e-10; // só aceita se realmente melhorar
      let melhorPos   = -1;

      for (let j = 0; j <= semI.length; j++) {
        const prevP = j === 0 ? pontoInicio : pontos[semI[j-1]];
        const nextP = j === semI.length ? pontoInicio : pontos[semI[j]];
        // Custo de inserir removido entre prevP e nextP
        const custoInsercao = distGraus(prevP, pontos[removido])
                            + distGraus(pontos[removido], nextP)
                            - distGraus(prevP, nextP);
        const ganho = custoRemocao - custoInsercao;
        if (ganho > melhorGanho) {
          melhorGanho = ganho;
          melhorPos   = j;
        }
      }

      if (melhorPos >= 0) {
        // Aplica a relocação
        const nova = semI.slice(0, melhorPos)
          .concat([removido])
          .concat(semI.slice(melhorPos));
        atual   = nova;
        melhorou = true;
        break; // reinicia do início após qualquer melhoria
      }
    }
  }
  return atual;
}

// ── ORQUESTRADOR ─────────────────────────────────────────────
// Combina OSRM + fallback local search
// Retorna { ordem: [...indices], fonte: "osrm"|"local", melhoria }
async function otimizarPontos(pontos, pontoInicio) {
  const n = pontos.length;
  if (n === 0) return { ordem: [], fonte: 'trivial', melhoria: 0 };
  if (n === 1) return { ordem: [0], fonte: 'trivial', melhoria: 0 };

  // Distância total de uma ordem
  function distOrd(ord) {
    let d = distGraus(pontoInicio, pontos[ord[0]]);
    for (let i = 0; i < ord.length-1; i++) d += distGraus(pontos[ord[i]], pontos[ord[i+1]]);
    d += distGraus(pontos[ord[ord.length-1]], pontoInicio);
    return d;
  }

  // ── Tenta OSRM Trip (grafo real de estradas) ──────────────
  const osrmOrdem = await osrmTrip(pontos, pontoInicio);

  if (osrmOrdem) {
    // OSRM retornou uma rota válida — aplica Or-opt adicional na métrica euclidiana
    // para eventuais refinamentos locais (OSRM otimiza tempo de viagem, nós distância)
    const osrmMelhorado = orOpt(pontos, osrmOrdem, pontoInicio);
    console.log('Rota via OSRM Trip API (grafo real de estradas BH)');
    return { ordem: osrmMelhorado, fonte: 'osrm', melhoria: 0 };
  }

  // ── Fallback: NN + 2-opt + Or-opt ─────────────────────────
  console.log('OSRM indisponível — usando NN+2opt+Or-opt (distância euclidiana)');
  const ordemNN = nearestNeighbor(pontos, pontoInicio);
  const distNN  = distOrd(ordemNN);

  const ordemOpt = doisOpt(pontos, ordemNN, pontoInicio);
  const ordemFinal = orOpt(pontos, ordemOpt, pontoInicio);
  const distFinal = distOrd(ordemFinal);

  const melhoria = Math.round((1 - distFinal / distNN) * 100);
  return { ordem: ordemFinal, fonte: 'local', melhoria };
}

// ── GET sync-coleta — busca cards na fase 341638193 (Liberado para Rota) ──
    if (req.method === "GET" && action === "sync-coleta") {
      const board = sanitizeBoard(await dbGet(BOARD_KEY));
      let edges = [];
      try {
        const data = await pipefyQuery(
          "query { phase(id: \"" + LIBERADO_ROTA_PHASE_ID + "\") { cards(first: 50) { edges { node { id title fields { name value } } } } } }"
        );
        edges = data && data.phase && data.phase.cards ? data.phase.cards.edges : [];
      } catch(e) {
        return res.status(200).json({ ok: false, error: "Pipefy: " + e.message });
      }
      let moved = 0;
      for (const edge of edges) {
        const node = edge.node;
        const id   = String(node.id);
        const existing = board.cards.find(function(c) { return c.pipefyId === id; });
        if (existing) {
          if (existing.phaseId !== "liberado_rota") {
            existing.phaseId = "liberado_rota";
            existing.movedAt = new Date().toISOString();
            moved++;
          }
        } else {
          const fields = node.fields || [];
          const nomeF  = fields.find(function(f) { return f.name.toLowerCase().includes("nome"); });
          const telF   = fields.find(function(f) { return f.name.toLowerCase().includes("telefone") || f.name.toLowerCase().includes("fone"); });
          const endF   = fields.find(function(f) { return f.name.toLowerCase().includes("endere"); });
          const descF  = fields.find(function(f) { return f.name.toLowerCase().includes("descri"); });
          const tel    = (telF && telF.value) ? telF.value : "";
          const digits = tel.replace(/[^0-9]/g, "");
          const ultimos4 = digits.slice(-4);
          const nome   = (nomeF && nomeF.value) ? nomeF.value : node.title;
          board.cards.unshift({
            pipefyId:    id,
            title:       node.title,
            nomeContato: nome + (ultimos4 ? " " + ultimos4 : ""),
            telefone:    tel,
            endereco:    (endF  && endF.value)  ? endF.value  : "",
            descricao:   (descF && descF.value) ? descF.value : "",
            phaseId:     "liberado_rota",
            movedAt:     new Date().toISOString(),
            movedBy:     "Pipefy",
            addedAt:     new Date().toISOString(),
          });
          if (board.syncedIds.indexOf(id) === -1) board.syncedIds.push(id);
          moved++;
        }
      }
      if (moved > 0) {
        try { await dbSet(BOARD_KEY, board); } catch(e) { /* ignore */ }
      }
      const filaAtual = board.cards.filter(function(c) { return c.phaseId === "liberado_rota"; });

      // Remove do Redis cards que já não estão mais em "Liberado para Rota" no Pipefy
      // Isso corrige cards fantasma que ficaram presos na fila após virar rota
      const idsNoPipefy = new Set(edges.map(function(e) { return String(e.node.id); }));
      const fantasmas = filaAtual.filter(function(c) { return !idsNoPipefy.has(c.pipefyId); });
      if (fantasmas.length > 0) {
        fantasmas.forEach(function(c) {
          // Muda phaseId para "rota_andamento" em vez de deletar — preserva histórico
          c.phaseId = "rota_andamento";
          c.movedAt = new Date().toISOString();
        });
        try { await dbSet(BOARD_KEY, board); } catch(e) { /* ignora */ }
      }

      const filaLimpa = board.cards.filter(function(c) { return c.phaseId === "liberado_rota"; });
      return res.status(200).json({ ok: true, found: edges.length, moved: moved, filaCount: filaLimpa.length, fantasmasRemovidos: fantasmas.length });
    }

    // ── GET remarcar-fase — busca cards na fase Remarcar ao vivo no Pipefy ──
    if (action === "remarcar-fase") {
      try {
        const query = "query { phase(id: \"" + "341638217" + "\") { cards(first: 50) { edges { node { id title fields { name value } } } } } }";
        const r = await fetch(PIPEFY_API, {
          method: "POST",
          headers: { "Content-Type": "application/json", Authorization: "Bearer " + (process.env.PIPEFY_TOKEN || "").trim() },
          body: JSON.stringify({ query }),
        });
        const j = await r.json();
        if (j.errors) return res.status(200).json({ ok: false, error: j.errors[0].message });
        const edges = j.data?.phase?.cards?.edges || [];
        const cards = edges.map(function(e) {
          const node   = e.node;
          const fields = node.fields || [];
          const nomeF  = fields.find(function(f){ return f.name.toLowerCase().includes("nome"); });
          const telF   = fields.find(function(f){ return f.name.toLowerCase().includes("telefone") || f.name.toLowerCase().includes("fone"); });
          const endF   = fields.find(function(f){ return f.name.toLowerCase().includes("endere"); });
          const descF  = fields.find(function(f){ return f.name.toLowerCase().includes("descri"); });
          const title  = node.title || "";
          const m      = title.match(/^(.*?)\s+(\d{3,6})$/);
          return {
            pipefyId:    String(node.id),
            osCode:      m ? m[2] : null,
            nomeContato: (nomeF?.value?.trim()) || (m ? m[1].trim() : title),
            telefone:    telF?.value || null,
            endereco:    endF?.value || null,
            descricao:   descF?.value || null,
            title,
          };
        });
        return res.status(200).json({ ok: true, cards, count: cards.length });
      } catch(e) {
        return res.status(200).json({ ok: false, error: "remarcar-fase: " + e.message });
      }
    }

        // ── backfill-compras — recria entradas para todos os cards do board ──
    if (action === "backfill-compras") {
      const board = sanitizeBoard(await dbGet(BOARD_KEY));
      const cards = (board.cards || []).filter(c => c.pipefyId);
      let cpDb = (await dbGet("tv_compras_pecas")) || { pecas: [] };
      if (!Array.isArray(cpDb.pecas)) cpDb.pecas = [];
      let coletaCards = [];
      try { const cr = await dbGet("tv_coleta_cards"); coletaCards = (cr && cr.cards) ? cr.cards : []; } catch(_e) {}
      let adicionados = 0;
      const now = new Date().toISOString();
      for (const c of cards) {
        if (!cpDb.pecas.some(p => p.pipefyId === String(c.pipefyId))) {
          const cc = coletaCards.find(x => String(x.pipefyId) === String(c.pipefyId));
          cpDb.pecas.unshift({
            id: Date.now().toString(36) + Math.random().toString(36).slice(2,5),
            origem: "tv_aprovado", pipefyId: String(c.pipefyId),
            os: c.osCode || String(c.pipefyId).slice(-4),
            nomeContato: c.nomeContato || c.title || "—",
            descricao: c.descricao || c.title || "TV aprovada",
            modelo: cc ? (cc.modelo || null) : null,
            peca:   cc ? (cc.diagnostico || null) : null,
            diagnosticoResumo: c.diagnosticoResumo || '',
            modeloTv: c.modeloTv || (cc ? (cc.modelo || '') : ''),
            status: "pendente", createdAt: now,
            urgente: false, obs: "", quantidade: 1
          });
          adicionados++;
        }
      }
      if (adicionados > 0) await dbSet("tv_compras_pecas", cpDb);
      return res.status(200).json({ ok: true, adicionados, total: cpDb.pecas.length });
    }

    // ── comparar-tecnico-pipe — compara aprovados do board vs pipe TV ──────────
    if (action === 'comparar-tecnico-pipe') {
      const boardDB = await dbGet(BOARD_KEY);
      const pipeDB  = await dbGet('tv_pipe');

      // TV Técnico: cards com phaseId === 'aprovado'
      const boardAprov = (boardDB?.cards || []).filter(function(c) {
        return c.phaseId === 'aprovado';
      }).map(function(c) {
        return {
          id:       c.id       || c.pipefyId || '',
          pipefyId: c.pipefyId || c.id       || '',
          nome:     c.nomeContato || c.title  || '—',
          osCode:   c.osCode   || '',
          phaseId:  c.phaseId,
        };
      });

      // TV Pipe: cards com phase === 'aprovados' (tv_pipe usa 'phase', não 'phaseId')
      const pipeAprov = (pipeDB?.cards || []).filter(function(c) {
        return c.phase === 'aprovados';
      }).map(function(c) {
        return {
          id:       c.id       || c.pipefyId || '',
          pipefyId: c.pipefyId || c.id       || '',
          nome:     c.nomeContato || c.title  || '—',
        };
      });

      // Construir set de IDs do pipe para busca rápida
      const pipeIds = new Set();
      pipeAprov.forEach(function(c) {
        if (c.id)       pipeIds.add(String(c.id));
        if (c.pipefyId) pipeIds.add(String(c.pipefyId));
      });

      // Comparar: ficha do técnico está no pipe?
      const permanecem = [], saem = [];
      boardAprov.forEach(function(c) {
        const noP = pipeIds.has(String(c.id)) || pipeIds.has(String(c.pipefyId));
        if (noP) permanecem.push(c);
        else     saem.push(c);
      });

      // Debug: mostrar todos os IDs para análise
      return res.status(200).json({
        ok: true,
        totalTecnico: boardAprov.length,
        totalPipe:    pipeAprov.length,
        permanecem,
        saem,
        pipeIds: Array.from(pipeIds).slice(0, 50),
        resumo: saem.length + ' saem · ' + permanecem.length + ' permanecem de ' + boardAprov.length + ' no técnico',
      });
    }

    // ── limpar-aprovado-board — remove fichas de aprovado que não estão no pipe ──
    if (action === 'limpar-aprovado-board') {
      const boardDB = await dbGet(BOARD_KEY);
      const pipeDB  = await dbGet('tv_pipe');
      if (!boardDB || !Array.isArray(boardDB.cards))
        return res.status(200).json({ ok:false, error:'board vazio' });

      // IDs aprovados no pipe TV (usa c.phase)
      const pipeIds = new Set();
      (pipeDB?.cards || []).forEach(function(c) {
        if (c.phase === 'aprovados') {
          if (c.id)       pipeIds.add(String(c.id));
          if (c.pipefyId) pipeIds.add(String(c.pipefyId));
        }
      });

      // Remover prefixo LOCAL- para comparar com pipe
      function normalizeId(id) {
        return String(id).replace(/^LOCAL-/, '');
      }

      const antes = boardDB.cards.filter(function(c){ return c.phaseId==='aprovado'; }).length;
      const removidos = [], mantidos = [];

      // Separar: manter só os que têm correspondência no pipe
      boardDB.cards = boardDB.cards.filter(function(card) {
        if (card.phaseId !== 'aprovado') return true; // outras fases: manter sempre
        const idNorm   = normalizeId(card.id || card.pipefyId || '');
        const pipefyN  = normalizeId(card.pipefyId || card.id || '');
        const osNorm   = normalizeId(card.osCode || '').replace(/^PIPE-/, '');
        const noP = pipeIds.has(String(card.id)) ||
                    pipeIds.has(String(card.pipefyId)) ||
                    pipeIds.has(idNorm) || pipeIds.has(pipefyN);
        if (noP) { mantidos.push({ nome: card.nomeContato||'—', id: card.id }); return true; }
        removidos.push({ nome: card.nomeContato||'—', id: card.id, osCode: card.osCode||'' });
        return false;
      });

      const depois = boardDB.cards.filter(function(c){ return c.phaseId==='aprovado'; }).length;
      boardDB.lastClean = new Date().toISOString();
      await dbSet(BOARD_KEY, boardDB);

      return res.status(200).json({
        ok: true,
        antes, depois,
        removidos: removidos.length, mantidos: mantidos.length,
        fichasRemovidas: removidos,
        fichasMantidas:  mantidos,
        resumo: removidos.length + ' fichas removidas de aprovado, ' + mantidos.length + ' mantidas'
      });
    }

    // ── POST set-barramento-compra ───────────────────────────────────────────────
    if (req.method === 'POST' && action === 'set-barramento-compra') {
      const bd = req.body || {};
      const db = await dbGet(BOARD_KEY) || defaultBoard();
      const card = (db.cards || []).find(function(c){
        return c.pipefyId === bd.pipefyId || c.id === bd.pipefyId;
      });
      if (!card) return res.status(404).json({ ok: false, error: 'Card não encontrado' });
      // Marcar compra — card FICA em barramento até o barramento chegar fisicamente
      card.barramentoTipo    = bd.tipo || 'local';     // 'local' | 'internet'
      card.barramentoPrazo   = bd.prazo || null;       // data estimada se internet
      card.barramentoStatus  = 'aguardando';           // esperando chegar
      card.barramentoCompraEm = new Date().toISOString();
      // NÃO move para producao — fica em barramento com status 'aguardando'
      // Só vai para producao quando 'barramento-chegou' for acionado
      card.movedAt           = new Date().toISOString();
      await dbSet(BOARD_KEY, db);
      return res.status(200).json({ ok: true, card });
    }

    // ── POST barramento-chegou ────────────────────────────────────────────────────
    if (req.method === 'POST' && action === 'barramento-chegou') {
      const bd = req.body || {};
      const db = await dbGet(BOARD_KEY) || defaultBoard();
      const idx = (db.cards || []).findIndex(function(c){
        return c.pipefyId === bd.pipefyId || c.id === bd.pipefyId;
      });
      if (idx < 0) return res.status(404).json({ ok: false, error: 'Card não encontrado' });
      const card = db.cards[idx];
      card.barramentoStatus  = 'disponivel';
      card.barramentoChegouEm = new Date().toISOString();
      card.phaseId            = 'producao';
      card.movedAt            = new Date().toISOString();
      // Mover para o TOPO da producao (remover e reinserir no início do array)
      db.cards.splice(idx, 1);
      db.cards.unshift(card);
      await dbSet(BOARD_KEY, db);
      return res.status(200).json({ ok: true, card });
    }

        return res.status(404).json({ ok: false, error: "Ação não encontrada" });

  } catch(e) {
    console.error("tv-board handler:", e.message);
    return res.status(200).json({ ok: false, error: "Erro interno: " + e.message });
  }
};
