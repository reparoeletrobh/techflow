// api/tv-board.js — Board do painel TV (independente do Reparo Eletro)
const PIPE_ID   = "306904889";
const BOARD_KEY = "tv_board";
const LOGS_KEY  = "tv_logs";
const LIBERADO_ROTA_PHASE_ID = "341638193"; // "Liberado para Rota"
const UPSTASH_URL   = (process.env.UPSTASH_URL   || "").replace(/['"]/g,"").trim();
const UPSTASH_TOKEN = (process.env.UPSTASH_TOKEN || "").replace(/['"]/g,"").trim();
const PIPEFY_API    = "https://api.pipefy.com/graphql";

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

async function pipefyQuery(query) {
  const r = await fetch(PIPEFY_API, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${(process.env.PIPEFY_TOKEN || "").trim()}`,
    },
    body: JSON.stringify({ query }),
  });
  const j = await r.json();
  if (j.errors) {
    const msg = Array.isArray(j.errors) ? j.errors.map(e => e.message).join("; ") : String(j.errors);
    throw new Error(msg);
  }
  return j.data;
}

function defaultBoard() {
  return {
    phases: [
      { id: "aprovado",        name: "Aprovado"           },
      { id: "producao",        name: "Produção"           },
      { id: "urgencia",        name: "Urgência"           },
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
async function moveCardPipefy(cardId, phaseId) {
  const data = await pipefyQuery(`mutation {
    moveCardToPhase(input: { card_id: ${cardId}, destination_phase_id: ${phaseId} }) {
      card { id current_phase { id name } }
    }
  }`);
  return data?.moveCardToPhase?.card;
}

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
        // Adiciona novos cards
        for (const c of aprovados) {
          if (!board.syncedIds.includes(c.pipefyId)) {
            board.cards.push(Object.assign({ phaseId: "aprovado" }, c));
            board.syncedIds.push(c.pipefyId);
            novos++;
          }
        }
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
      const { pipefyId, phaseId, pipefyPhaseId, techName } = req.body || {};
      if (!pipefyId || !phaseId) return res.status(400).json({ ok: false, error: "pipefyId e phaseId obrigatórios" });
      const board = sanitizeBoard(await dbGet(BOARD_KEY));
      const card  = board.cards.find(function(c) { return c.pipefyId === String(pipefyId); });
      if (!card) return res.status(404).json({ ok: false, error: "Card não encontrado" });
      card.phaseId = phaseId;
      if (techName) card.techName = techName;
      card.movidoEm = new Date().toISOString();
      board.movesLog = trimLog([...(board.movesLog || []),
        { id: pipefyId, phase: phaseId, tech: techName, ts: Date.now() }]);
      await dbSet(BOARD_KEY, board);
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

    return res.status(404).json({ ok: false, error: "Ação não encontrada" });

  } catch(e) {
    console.error("tv-board handler:", e.message);
    return res.status(200).json({ ok: false, error: "Erro interno: " + e.message });
  }
};
