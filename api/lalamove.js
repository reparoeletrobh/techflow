// lalamove.js — Integração Lalamove v3 API (Brazil)
// Fluxo: Adicionar fichas → Cotar → Confirmar Pedido
const crypto = require("crypto");

const UPSTASH_URL   = (process.env.UPSTASH_URL   || "").replace(/['"]/g,"").trim();
const UPSTASH_TOKEN = (process.env.UPSTASH_TOKEN || "").replace(/['"]/g,"").trim();
const LALA_KEY      = "reparoeletro_lalamove";
const PIPE_ID       = "305832912";
const PIPEFY_API    = "https://api.pipefy.com/graphql";

const LALA_HOST_PROD    = "rest.lalamove.com";
const LALA_HOST_SANDBOX = "rest.sandbox.lalamove.com";

// Endereço fixo da loja
const LOJA = {
  nome:     "Reparo Eletro",
  endereco: "Rua Ouro Preto, 663, Barro Preto, Belo Horizonte, MG, Brasil",
  telefone: "+5531997856023",
  lat:      "-19.9245",
  lng:      "-43.9352",
};

// ── Redis ─────────────────────────────────────────────────────
async function dbGet(key) {
  try {
    const r = await fetch(UPSTASH_URL + "/pipeline", {
      method: "POST",
      headers: { Authorization: "Bearer " + UPSTASH_TOKEN, "Content-Type": "application/json" },
      body: JSON.stringify([["GET", key]]),
    });
    const j = await r.json();
    return j[0]?.result ? JSON.parse(j[0].result) : null;
  } catch(e) { return null; }
}

async function dbSet(key, value) {
  try {
    const r = await fetch(UPSTASH_URL + "/pipeline", {
      method: "POST",
      headers: { Authorization: "Bearer " + UPSTASH_TOKEN, "Content-Type": "application/json" },
      body: JSON.stringify([["SET", key, JSON.stringify(value)]]),
    });
    const j = await r.json();
    return j[0]?.result === "OK";
  } catch(e) { return false; }
}

// ── Pipefy — busca endereço e telefone do card ────────────────
async function fetchCardDados(pipefyId) {
  try {
    const res = await fetch(PIPEFY_API, {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: "Bearer " + (process.env.PIPEFY_TOKEN || "").trim() },
      body: JSON.stringify({ query: `query { card(id:"${pipefyId}") { id title fields { name value } } }` }),
    });
    const j = await res.json();
    const fields = j?.data?.card?.fields || [];
    const endField = fields.find(f => f.name.toLowerCase().includes("endere"));
    const telField = fields.find(f => f.name.toLowerCase().includes("telefone") || f.name.toLowerCase().includes("fone"));
    return {
      endereco: endField?.value || null,
      telefone: telField?.value || null,
    };
  } catch(e) { return {}; }
}

// ── Geocoding via Nominatim (OpenStreetMap) ───────────────────
async function geocodificar(endereco) {
  const GMAPS_KEY    = (process.env.GOOGLE_MAPS_KEY || "").trim();
  const OPENCAGE_KEY = (process.env.OPENCAGE_KEY    || "").trim();

  // ── BBOX RMBH: cobre BH + toda região metropolitana ──────────
  const dentroRMBH = (lat, lng) => lat > -20.8 && lat < -18.8 && lng > -45.2 && lng < -43.0;

  // ── NORMALIZAÇÃO (sem regex destrutiva) ───────────────────────
  const normalizar = (end) => end
    .replace(/,?\s*BH/gi, "")           // BH → remove (vai ser adicionado depois)
    .replace(/R\.\s+/g, "Rua ")
    .replace(/Av\.\s+/g, "Avenida ")
    .replace(/Al\.\s+/g, "Alameda ")
    .replace(/Trav\.\s+/g, "Travessa ")
    .replace(/Pç\.\s+/g, "Praça ")
    .replace(/,?\s*-?\s*(ap(to)?\.?|apart(amento)?|bloco|bl\.?|sala|lote|cj\.?|conj\.?)\s*[\w\d\/\-]+/gi, "")
    .replace(/\s+/g, " ").trim();

  // ── PARSE: extrai partes do endereço ─────────────────────────
  const parsearEndereco = (end) => {
    // Formato típico: "Rua Fulano, 123, Bairro, Cidade" ou "Rua Fulano 123, Bairro"
    const partes = end.split(",").map(p => p.trim()).filter(Boolean);
    let rua = "", numero = "", bairro = "", cidade = "";

    // Primeira parte: rua + possível número colado
    if (partes[0]) {
      const mNum = partes[0].match(/^(.+?)\s+(\d{1,5}[A-Za-z]?)$/);
      if (mNum) { rua = mNum[1]; numero = mNum[2]; }
      else { rua = partes[0]; }
    }
    // Segunda parte: pode ser número ou bairro
    if (partes[1]) {
      if (/^\d{1,5}[A-Za-z]?$/.test(partes[1])) {
        numero = partes[1];
        bairro = partes[2] || "";
        cidade = partes[3] || "";
      } else {
        bairro = partes[1];
        cidade = partes[2] || "";
      }
    }
    // Detecta cidade na RMBH
    const RMBH = ["belo horizonte","contagem","betim","ribeirão das neves","santa luzia","nova lima",
      "vespasiano","lagoa santa","ibirité","sabará","pedro leopoldo","esmeraldas","brumadinho",
      "sarzedo","matozinhos","confins","taquaraçu de minas","jaboticatubas","capim branco"];
    const endLow = end.toLowerCase();
    const cidadeDetectada = RMBH.find(ct => endLow.includes(ct)) || "";
    if (cidadeDetectada && !cidade) cidade = cidadeDetectada;
    if (!cidade) cidade = "Belo Horizonte";

    return { rua, numero, bairro, cidade };
  };

  const endNorm = normalizar(endereco);
  const { rua, numero, bairro, cidade } = parsearEndereco(endNorm);
  const cidadeFull = cidade.toLowerCase().includes("belo horizonte") ? "Belo Horizonte, MG, Brasil"
    : cidade + ", MG, Brasil";

  // ── 1. GOOGLE MAPS (prioridade quando disponível) ─────────────
  // Google é o mais preciso para endereços brasileiros
  if (GMAPS_KEY) {
    try {
      // Monta query limpa sem complementos
      const partsGmaps = [rua, numero, bairro, cidadeFull].filter(Boolean).join(", ");
      const url = "https://maps.googleapis.com/maps/api/geocode/json?address="
        + encodeURIComponent(partsGmaps)
        + "&region=br&language=pt-BR&key=" + GMAPS_KEY;
      const r = await fetch(url);
      const j = await r.json();
      if (j.status === "OK" && j.results && j.results[0]) {
        const res  = j.results[0];
        const loc  = res.geometry.location;
        // Aceita apenas resultados com precisão de rua ou melhor
        const tipo = res.geometry.location_type;
        if (dentroRMBH(loc.lat, loc.lng) && (tipo === "ROOFTOP" || tipo === "RANGE_INTERPOLATED" || tipo === "GEOMETRIC_CENTER")) {
          console.log("Geocode Google OK:", tipo, partsGmaps);
          return { lat: String(loc.lat), lng: String(loc.lng), fonte: "google", tipo };
        }
        // Se não achou com número, tenta sem
        if (numero) {
          const semNum = [rua, bairro, cidadeFull].filter(Boolean).join(", ");
          const r2 = await fetch("https://maps.googleapis.com/maps/api/geocode/json?address="
            + encodeURIComponent(semNum) + "&region=br&language=pt-BR&key=" + GMAPS_KEY);
          const j2 = await r2.json();
          if (j2.status === "OK" && j2.results && j2.results[0]) {
            const loc2 = j2.results[0].geometry.location;
            if (dentroRMBH(loc2.lat, loc2.lng)) {
              console.log("Geocode Google (sem num) OK:", semNum);
              return { lat: String(loc2.lat), lng: String(loc2.lng), fonte: "google_sem_num" };
            }
          }
        }
      }
    } catch(e) { console.error("Google Maps:", e.message); }
  }

  // ── 2. NOMINATIM — busca estruturada (mais precisa) ───────────
  const nomEstruturada = async (street, city, countrycode) => {
    const params = new URLSearchParams({
      format: "jsonv2",
      limit: "5",
      countrycodes: countrycode || "br",
      addressdetails: "1",
      // bounded=1 com viewbox garante resultados DENTRO da RMBH
      viewbox: "-45.2,-20.8,-43.0,-18.8",
      bounded: "1",
    });
    if (street) params.set("street", street);
    if (city)   params.set("city",   city);
    params.set("country", "Brasil");
    const url = "https://nominatim.openstreetmap.org/search?" + params.toString();
    const r = await fetch(url, { headers: { "User-Agent": "ReparoEletro/1.0 (reparoeletroadm.com)" } });
    const j = await r.json();
    if (!Array.isArray(j) || !j.length) return null;
    // Prioriza: house > building > road > neighbourhood
    const tipos = ["house","building","amenity","road","residential","neighbourhood"];
    for (const tipo of tipos) {
      const match = j.find(x => x.type === tipo && dentroRMBH(parseFloat(x.lat), parseFloat(x.lon)));
      if (match) return match;
    }
    // Fallback: qualquer resultado dentro da RMBH
    return j.find(x => dentroRMBH(parseFloat(x.lat), parseFloat(x.lon))) || null;
  };

  const nomLivre = async (q) => {
    const params = new URLSearchParams({
      q, format: "jsonv2", limit: "5", countrycodes: "br",
      addressdetails: "1", viewbox: "-45.2,-20.8,-43.0,-18.8", bounded: "1",
    });
    const url = "https://nominatim.openstreetmap.org/search?" + params.toString();
    const r = await fetch(url, { headers: { "User-Agent": "ReparoEletro/1.0 (reparoeletroadm.com)" } });
    const j = await r.json();
    if (!Array.isArray(j) || !j.length) return null;
    const tipos = ["house","building","amenity","road","residential","neighbourhood"];
    for (const tipo of tipos) {
      const match = j.find(x => x.type === tipo && dentroRMBH(parseFloat(x.lat), parseFloat(x.lon)));
      if (match) return match;
    }
    return j.find(x => dentroRMBH(parseFloat(x.lat), parseFloat(x.lon))) || null;
  };

  try {
    // T1: busca estruturada com número (mais precisa)
    if (rua && numero) {
      const best = await nomEstruturada(rua + " " + numero, cidade || "Belo Horizonte");
      if (best) { console.log("Nom estruturada+num OK:", rua, numero); return { lat: String(best.lat), lng: String(best.lon), fonte: "nominatim_struct" }; }
    }
    // T2: busca estruturada sem número
    if (rua) {
      const best = await nomEstruturada(rua, cidade || "Belo Horizonte");
      if (best) { console.log("Nom estruturada sem num OK:", rua); return { lat: String(best.lat), lng: String(best.lon), fonte: "nominatim_struct_semnum" }; }
    }
    // T3: busca livre com cidade detectada
    const queryLivre = [rua, numero, bairro, cidade, "MG Brasil"].filter(Boolean).join(", ");
    const best3 = await nomLivre(queryLivre);
    if (best3) { console.log("Nom livre OK:", queryLivre); return { lat: String(best3.lat), lng: String(best3.lon), fonte: "nominatim_livre" }; }
    // T4: só rua + cidade (sem bairro que pode confundir)
    if (rua && rua !== queryLivre) {
      const q4 = [rua, cidade, "MG Brasil"].filter(Boolean).join(", ");
      const best4 = await nomLivre(q4);
      if (best4) { console.log("Nom rua+cidade OK:", q4); return { lat: String(best4.lat), lng: String(best4.lon), fonte: "nominatim_rua_cidade" }; }
    }
  } catch(e) { console.error("Nominatim:", e.message); }

  // ── 3. OPENCAGE ───────────────────────────────────────────────
  if (OPENCAGE_KEY) {
    try {
      const partsOC = [rua, numero, bairro, cidade, "MG, Brasil"].filter(Boolean).join(", ");
      const url = "https://api.opencagedata.com/geocode/v1/json?q="
        + encodeURIComponent(partsOC)
        + "&key=" + OPENCAGE_KEY
        + "&countrycode=br&limit=5&language=pt&no_annotations=1"
        + "&proximity=-19.9245,-43.9352"
        + "&bounds=-45.2,-20.8,-43.0,-18.8";
      const r = await fetch(url);
      const j = await r.json();
      const validos = (j.results || []).filter(x =>
        (x.confidence || 0) >= 6 && dentroRMBH(x.geometry.lat, x.geometry.lng)
      );
      if (validos.length) {
        console.log("OpenCage OK:", partsOC, "conf:", validos[0].confidence);
        return { lat: String(validos[0].geometry.lat), lng: String(validos[0].geometry.lng), fonte: "opencage" };
      }
    } catch(e) { console.error("OpenCage:", e.message); }
  }

  console.error("Geocode falhou para:", endereco);
  return null;
}

// ── HMAC signature para Lalamove ──────────────────────────────
function lalamoveSign(secret, timestamp, method, path, body) {
  const raw = `${timestamp}\r\n${method.toUpperCase()}\r\n${path}\r\n\r\n${body}`;
  return crypto.createHmac("sha256", secret).update(raw).digest("hex");
}

function lalamoveHeaders(key, secret, method, path, body) {
  const ts  = Date.now().toString();
  const sig = lalamoveSign(secret, ts, method, path, body);
  const token = key + ":" + ts + ":" + sig;
  return {
    "Content-Type":  "application/json",
    "Authorization": "hmac " + token,
    "Market":        "BR",
    "Request-ID":    ts + "-" + Math.random().toString(36).slice(2,8),
    "Accept":        "application/json",
  };
}

// ── Chamada para Lalamove via fetch nativo ────────────────────
async function lalaFetch(host, path, method, headers, body) {
  const url = "https://" + host + path;
  const opts = { method, headers };
  if (body) opts.body = body;
  const r = await fetch(url, opts);
  const text = await r.text();
  return { status: r.status, body: text };
}

// Formata telefone para padrão internacional +55
// Extrai complemento do endereço (ap, apto, bloco, sala, etc.)
function extractComplemento(endereco) {
  if (!endereco) return "";
  const m = endereco.match(/,?\s*[-–]?\s*((ap(to)?\.?\s*\d+[\w]*|apartamento\s*\d+[\w]*|bloco\s*[\w]+|bl\.?\s*[\w]+|sala\s*[\w]+|loja\s*[\w]+|andar\s*[\w]+|conjunto\s*[\w]+|cj\.?\s*[\w]+))/i);
  return m ? m[1].trim() : "";
}

// Encontra o stop da loja por coordenadas (não por índice — a otimização pode mover ela)
function findLojaStop(lalaStops) {
  const lojaLat = parseFloat(LOJA.lat);
  const lojaLng = parseFloat(LOJA.lng);
  let best = null, bestDist = Infinity;
  for (const s of lalaStops) {
    const lat = parseFloat(s.coordinates?.lat);
    const lng = parseFloat(s.coordinates?.lng);
    if (isNaN(lat) || isNaN(lng)) continue;
    const dist = Math.pow(lat - lojaLat, 2) + Math.pow(lng - lojaLng, 2);
    if (dist < bestDist) { bestDist = dist; best = s; }
  }
  return best;
}

// Encontra a ficha que melhor corresponde a um stop pelo par de coordenadas
// usedIds: Set de pipefyIds já usados para evitar duplicatas
function matchFichaByCoords(fichas, coords, usedIds) {
  const candidates = usedIds ? fichas.filter(f => !usedIds.has(f.pipefyId)) : fichas;
  if (!candidates.length) return fichas[0]; // fallback
  if (!coords) { const f = candidates[0]; if (usedIds) usedIds.add(f.pipefyId); return f; }
  const lat = parseFloat(coords.lat);
  const lng = parseFloat(coords.lng);
  let best = candidates[0], bestDist = Infinity;
  for (const f of candidates) {
    const fLat = parseFloat(f.lat);
    const fLng = parseFloat(f.lng);
    if (isNaN(fLat) || isNaN(fLng)) continue;
    const dist = Math.pow(fLat - lat, 2) + Math.pow(fLng - lng, 2);
    if (dist < bestDist) { bestDist = dist; best = f; }
  }
  if (usedIds) usedIds.add(best.pipefyId);
  return best;
}

// Formata nome para Lalamove: "Nome Telefone"
function formatNomeLala(nomeContato, telefone) {
  var tel = (telefone || "").replace(/\D/g, "").slice(-11); // últimos 11 dígitos
  if (!tel) return nomeContato || "Cliente";
  var telFmt = tel.length === 11
    ? "(" + tel.slice(0,2) + ") " + tel.slice(2,7) + "-" + tel.slice(7)
    : tel;
  return (nomeContato || "Cliente") + " " + telFmt;
}

function formatTelIntl(tel) {
  if (!tel) return "+5531997856023"; // fallback loja
  const digits = tel.replace(/\D/g, "");
  if (digits.startsWith("55") && digits.length >= 12) return "+" + digits;
  if (digits.length === 11) return "+55" + digits;
  if (digits.length === 10) return "+55" + digits;
  return "+55" + digits;
}

// ── HANDLER ───────────────────────────────────────────────────
module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(200).end();

  const { action } = req.query;
  const LALA_KEY_ENV    = (process.env.LALAMOVE_API_KEY    || "").trim();
  const LALA_SECRET_ENV = (process.env.LALAMOVE_API_SECRET || "").trim();
  const SANDBOX         = (process.env.LALAMOVE_SANDBOX    || "false") === "true";
  const LALA_HOST       = SANDBOX ? LALA_HOST_SANDBOX : LALA_HOST_PROD;

  // ── GET load ─────────────────────────────────────────────────
  if (action === "load") {
    const db = await dbGet(LALA_KEY) || { fichas: [] };
    return res.status(200).json({
      ok: true,
      fichas:    db.fichas || [],
      temApiKey: !!(LALA_KEY_ENV),
      sandbox:   SANDBOX,
      loja:      { nome: LOJA.nome, endereco: LOJA.endereco },
    });
  }

  // ── POST adicionar ────────────────────────────────────────────
  if (req.method === "POST" && action === "adicionar") {
    const { pipefyId, tipo, nomeContato, osCode, descricao, enderecoManual, telefoneManual } = req.body || {};
    if (!pipefyId || !tipo) return res.status(400).json({ ok: false, error: "pipefyId e tipo obrigatórios" });

    const db = await dbGet(LALA_KEY) || { fichas: [] };
    if (!Array.isArray(db.fichas)) db.fichas = [];

    // Evita duplicata
    if (db.fichas.find(f => f.pipefyId === pipefyId && f.tipo === tipo))
      return res.status(200).json({ ok: true, msg: "Já adicionado", duplicata: true });

    // Busca endereço e telefone no Pipefy
    let endereco = enderecoManual || null;
    let telefone = telefoneManual || null;
    if (!endereco || !telefone) {
      const dados = await fetchCardDados(pipefyId);
      if (!endereco) endereco = dados.endereco || null;
      if (!telefone) telefone = dados.telefone || null;
    }

    // Geocodifica
    let lat = null, lng = null;
    if (endereco) {
      const coords = await geocodificar(endereco);
      if (coords) { lat = coords.lat; lng = coords.lng; }
    }

    db.fichas.push({
      pipefyId, tipo,
      osCode:      osCode      || null,
      nomeContato: nomeContato || null,
      descricao:   descricao   || null,
      endereco:    endereco    || null,
      telefone:    telefone    || null,
      lat, lng,
      addedAt: new Date().toISOString(),
      status:  "pendente",
    });

    await dbSet(LALA_KEY, db);
    return res.status(200).json({ ok: true, lat, lng, enderecoResolvido: endereco });
  }

  // ── POST update-endereco ──────────────────────────────────────
  if (req.method === "POST" && action === "update-endereco") {
    const { pipefyId, tipo, endereco, telefone } = req.body || {};
    const db = await dbGet(LALA_KEY) || { fichas: [] };
    const f  = db.fichas.find(x => x.pipefyId === pipefyId && x.tipo === tipo);
    if (!f) return res.status(404).json({ ok: false, error: "Ficha não encontrada" });
    if (endereco) {
      f.endereco = endereco;
      const coords = await geocodificar(endereco);
      if (coords) { f.lat = coords.lat; f.lng = coords.lng; }
    }
    if (telefone) f.telefone = telefone;
    await dbSet(LALA_KEY, db);
    return res.status(200).json({ ok: true, lat: f.lat, lng: f.lng });
  }

  // ── POST remover ──────────────────────────────────────────────
  if (req.method === "POST" && action === "remover") {
    const { pipefyId, tipo } = req.body || {};
    const db = await dbGet(LALA_KEY) || { fichas: [], removedIds: [] };
    if (!Array.isArray(db.removedIds)) db.removedIds = [];
    db.fichas = db.fichas.filter(f => !(f.pipefyId === pipefyId && f.tipo === tipo));
    // Guarda ID removido para impedir reimportação
    const key = pipefyId + ":" + tipo;
    if (!db.removedIds.includes(key)) db.removedIds.push(key);
    await dbSet(LALA_KEY, db);
    return res.status(200).json({ ok: true });
  }

  // ── Testa cada ficha individualmente contra a API Lalamove para achar coordenadas ruins
async function testarFichasIndividualmente(pendentes, tipo, key, secret) {
  const erros = [];
  const fmtCoord = v => parseFloat(v).toFixed(6);
  for (const f of pendentes) {
    const clientStop = { coordinates: { lat: fmtCoord(f.lat), lng: fmtCoord(f.lng) }, address: f.endereco || "Belo Horizonte, MG" };
    const lojaStop   = { coordinates: { lat: fmtCoord(LOJA.lat), lng: fmtCoord(LOJA.lng) }, address: LOJA.endereco };
    const stops = tipo === "coleta" ? [clientStop, lojaStop] : [lojaStop, clientStop];
    const body  = JSON.stringify({ data: { serviceType: "CAR", language: "pt_BR", stops, isRouteOptimized: false } });
    const hdrs  = lalamoveHeaders(key, secret, "POST", "/v3/quotations", body);
    try {
      const { status, body: rb } = await lalaFetch(LALA_HOST, "/v3/quotations", "POST", hdrs, body);
      const rj = JSON.parse(rb);
      if (status !== 201 || !rj.data?.quotationId) {
        const errCode = (rj.errors || [])[0]?.id || "ERRO_DESCONHECIDO";
        const errMsg  = (rj.errors || [])[0]?.message || JSON.stringify(rj).slice(0, 150);
        erros.push({ pipefyId: f.pipefyId, nomeContato: f.nomeContato, endereco: f.endereco, erroMsg: errCode + ": " + errMsg });
      }
    } catch(e) {
      erros.push({ pipefyId: f.pipefyId, nomeContato: f.nomeContato, endereco: f.endereco, erroMsg: "Falha na requisição: " + e.message });
    }
  }
  return erros;
}

// ── POST reset-erro — reseta ficha com erro de volta para pendente
  if (req.method === "POST" && action === "reset-erro") {
    const { pipefyId, tipo } = req.body || {};
    if (!pipefyId) return res.status(400).json({ ok:false, error:"pipefyId obrigatório" });
    const db = await dbGet(LALA_KEY) || { fichas: [] };
    const f  = db.fichas.find(x => x.pipefyId === pipefyId && (!tipo || x.tipo === tipo));
    if (!f) return res.status(404).json({ ok:false, error:"Ficha não encontrada" });
    f.status  = "pendente";
    f.erroMsg = null;
    f.erroAt  = null;
    await dbSet(LALA_KEY, db);
    return res.status(200).json({ ok:true });
  }

  // ── POST enviar-lote — envia TODAS as fichas em uma única corrida ─────────
  if (req.method === "POST" && action === "enviar-lote") {
    if (!LALA_KEY_ENV || !LALA_SECRET_ENV)
      return res.status(400).json({ ok: false, error: "API keys não configuradas" });

    const { tipo, loteIds } = req.body || {};
    if (!tipo)
      return res.status(400).json({ ok: false, error: "tipo obrigatorio" });

    const db = await dbGet(LALA_KEY) || { fichas: [] };
    // Se loteIds enviado, usa apenas esses — senão usa TODAS as pendentes do tipo
    const pendentes = (db.fichas || []).filter(f =>
      f.tipo === tipo &&
      f.status === "pendente" &&
      f.lat && f.lng &&
      (Array.isArray(loteIds) ? loteIds.includes(f.pipefyId) : true)
    );

    if (!pendentes.length)
      return res.status(400).json({ ok: false, error: "Nenhuma ficha válida com coordenadas" });

    const fmtCoord = v => parseFloat(v).toFixed(6);

    // Monta stops com rota otimizada
    let stops;
    if (tipo === "coleta") {
      stops = [
        ...pendentes.map(f => ({ coordinates: { lat: fmtCoord(f.lat), lng: fmtCoord(f.lng) }, address: f.endereco || "Belo Horizonte, MG" })),
        { coordinates: { lat: fmtCoord(LOJA.lat), lng: fmtCoord(LOJA.lng) }, address: LOJA.endereco },
      ];
    } else {
      stops = [
        { coordinates: { lat: fmtCoord(LOJA.lat), lng: fmtCoord(LOJA.lng) }, address: LOJA.endereco },
        ...pendentes.map(f => ({ coordinates: { lat: fmtCoord(f.lat), lng: fmtCoord(f.lng) }, address: f.endereco || "Belo Horizonte, MG" })),
      ];
    }

    // ETAPA 1: Cotação com rota otimizada
    const quotePath = "/v3/quotations";
    const quoteBody = JSON.stringify({ data: { serviceType: "CAR", language: "pt_BR", stops, isRouteOptimized: true } });
    const quoteHdrs = lalamoveHeaders(LALA_KEY_ENV, LALA_SECRET_ENV, "POST", quotePath, quoteBody);

    let quotationId, lalaStops;
    try {
      const { status: qs, body: qb } = await lalaFetch(LALA_HOST, quotePath, "POST", quoteHdrs, quoteBody);
      const qj = JSON.parse(qb);
      if (qs !== 201 || !qj.data?.quotationId) {
        // Tenta identificar fichas com problema testando individualmente
        const erros = await testarFichasIndividualmente(pendentes, tipo, LALA_KEY_ENV, LALA_SECRET_ENV);
        if (erros.length) {
          // Marca fichas com erro no DB
          erros.forEach(({ pipefyId, erroMsg }) => {
            const dbF = db.fichas.find(x => x.pipefyId === pipefyId);
            if (dbF) { dbF.status = "erro"; dbF.erroMsg = erroMsg; dbF.erroAt = new Date().toISOString(); }
          });
          await dbSet(LALA_KEY, db);
          return res.status(200).json({
            ok: false,
            erros,
            error: `${erros.length} ficha(s) com endereço inválido marcadas — corrija e tente novamente`,
          });
        }
        return res.status(200).json({ ok: false, error: "Erro na cotação: " + JSON.stringify(qj.errors || qj).slice(0,300) });
      }
      quotationId = qj.data.quotationId;
      lalaStops   = qj.data.stops || [];
    } catch(e) {
      return res.status(200).json({ ok: false, error: "Cotação falhou: " + e.message });
    }

    // ETAPA 2: Pedido
    let senderStopId, recipientStops;
    // Identifica o stop da loja pelas coordenadas (ignora posição — a otimização pode mover)
    const lojaStop = findLojaStop(lalaStops);
    const clientStops = lalaStops.filter(s => s.stopId !== lojaStop?.stopId);
    if (tipo === "coleta") {
      senderStopId   = lojaStop?.stopId;
      { const _used = new Set();
      recipientStops = clientStops.map((s) => {
        const f = matchFichaByCoords(pendentes, s.coordinates, _used);
        return { stopId: s.stopId, name: formatNomeLala(f?.nomeContato, f?.telefone), phone: formatTelIntl(f?.telefone), remarks: extractComplemento(f?.endereco) };
      }); }
    } else {
      senderStopId   = lojaStop?.stopId;
      { const _used = new Set();
      recipientStops = clientStops.map((s) => {
        const f = matchFichaByCoords(pendentes, s.coordinates, _used);
        return { stopId: s.stopId, name: formatNomeLala(f?.nomeContato, f?.telefone), phone: formatTelIntl(f?.telefone), remarks: extractComplemento(f?.endereco) };
      }); }
    }

    const orderPath = "/v3/orders";
    const orderBody = JSON.stringify({ data: {
      quotationId,
      sender:     { stopId: senderStopId, name: LOJA.nome, phone: LOJA.telefone },
      recipients: recipientStops,
    }});
    const orderHdrs = lalamoveHeaders(LALA_KEY_ENV, LALA_SECRET_ENV, "POST", orderPath, orderBody);

    try {
      const { status: os, body: ob } = await lalaFetch(LALA_HOST, orderPath, "POST", orderHdrs, orderBody);
      const oj = JSON.parse(ob);
      if (os !== 201 || !oj.data?.orderId)
        return res.status(200).json({ ok: false, error: "Erro no pedido: " + JSON.stringify(oj.errors || oj).slice(0,300) });

      // Marca fichas do lote como enviadas
      const agora = new Date().toISOString();
      pendentes.forEach(f => {
        const dbF = db.fichas.find(x => x.pipefyId === f.pipefyId);
        if (dbF) { dbF.status = "enviado"; dbF.orderId = oj.data.orderId; dbF.enviadoAt = agora; }
      });
      await dbSet(LALA_KEY, db);
      return res.status(200).json({ ok: true, orderId: oj.data.orderId, count: pendentes.length });
    } catch(e) {
      return res.status(200).json({ ok: false, error: "Pedido falhou: " + e.message });
    }
  }

  if (req.method === "POST" && action === "enviar-lalamove") {
    if (!LALA_KEY_ENV || !LALA_SECRET_ENV)
      return res.status(400).json({ ok: false, error: "LALAMOVE_API_KEY e LALAMOVE_API_SECRET não configurados no Vercel" });

    const { tipo } = req.body || {};
    const db = await dbGet(LALA_KEY) || { fichas: [] };
    const pendentes = (db.fichas || []).filter(f => f.tipo === tipo && f.status === "pendente");

    if (!pendentes.length)
      return res.status(400).json({ ok: false, error: "Nenhuma ficha pendente" });

    // Geocodifica fichas sem coordenadas
    for (const f of pendentes) {
      if (!f.lat || !f.lng) {
        if (f.endereco) {
          const coords = await geocodificar(f.endereco);
          if (coords) { f.lat = coords.lat; f.lng = coords.lng; }
        }
      }
    }

    const semCoords = pendentes.filter(f => !f.lat || !f.lng);
    if (semCoords.length)
      return res.status(400).json({ ok: false, error: semCoords.length + " ficha(s) sem endereço/coordenadas. Corrija no painel.", fichasSemCoords: semCoords.map(f => f.osCode || f.nomeContato) });

    // Monta stops
    let stops;
    if (tipo === "coleta") {
      const fmtCoord = v => parseFloat(v).toFixed(6);
      stops = [
        ...pendentes.map(f => ({ coordinates: { lat: fmtCoord(f.lat), lng: fmtCoord(f.lng) }, address: f.endereco || "Belo Horizonte, MG" })),
        { coordinates: { lat: fmtCoord(LOJA.lat), lng: fmtCoord(LOJA.lng) }, address: LOJA.endereco },
      ];
    } else {
      stops = [
        { coordinates: { lat: fmtCoord(LOJA.lat), lng: fmtCoord(LOJA.lng) }, address: LOJA.endereco },
        ...pendentes.map(f => ({ coordinates: { lat: fmtCoord(f.lat), lng: fmtCoord(f.lng) }, address: f.endereco || "Belo Horizonte, MG" })),
      ];
    }

    // ETAPA 1: Cotação
    const quotePath = "/v3/quotations";
    const quoteBody = JSON.stringify({ data: { serviceType: "CAR", language: "pt_BR", stops } });
    const quoteHdrs = lalamoveHeaders(LALA_KEY_ENV, LALA_SECRET_ENV, "POST", quotePath, quoteBody);

    let quotationId, lalaStops;
    try {
      const { status: qs, body: qb } = await lalaFetch(LALA_HOST, quotePath, "POST", quoteHdrs, quoteBody);
      const qj = JSON.parse(qb);
      if (qs !== 201 || !qj.data?.quotationId)
        return res.status(200).json({ ok: false, error: "Erro na cotação: " + JSON.stringify(qj.errors || qj).slice(0,300), httpStatus: qs });
      quotationId = qj.data.quotationId;
      lalaStops   = qj.data.stops || [];
    } catch(e) {
      return res.status(200).json({ ok: false, error: "Cotação falhou: " + e.message });
    }

    // ETAPA 2: Pedido
    let senderStopId, recipientStops;
    const lojaStop2 = findLojaStop(lalaStops);
    const clientStops2 = lalaStops.filter(s => s.stopId !== lojaStop2?.stopId);
    if (tipo === "coleta") {
      senderStopId   = lojaStop2?.stopId;
      { const _u=new Set(); const _cs=findLojaStop(lalaStops); const _cl=lalaStops.filter(s=>s.stopId!==_cs?.stopId); recipientStops = _cl.map((s) => { const f = matchFichaByCoords(pendentes, s.coordinates, _u); return { stopId: s.stopId, name: formatNomeLala(f?.nomeContato, f?.telefone), phone: formatTelIntl(f?.telefone), remarks: extractComplemento(f?.endereco) }; }); }
    } else {
      senderStopId   = lojaStop?.stopId;
      { const _u=new Set(); const _cs2=findLojaStop(lalaStops); const _cl2=lalaStops.filter(s=>s.stopId!==_cs2?.stopId); recipientStops = _cl2.map((s) => { const f = matchFichaByCoords(pendentes, s.coordinates, _u); return { stopId: s.stopId, name: formatNomeLala(f?.nomeContato, f?.telefone), phone: formatTelIntl(f?.telefone), remarks: extractComplemento(f?.endereco) }; }); }
    }

    const orderPath = "/v3/orders";
    const orderBody = JSON.stringify({ data: { quotationId, sender: { stopId: senderStopId, name: LOJA.nome, phone: LOJA.telefone }, recipients: recipientStops } });
    const orderHdrs = lalamoveHeaders(LALA_KEY_ENV, LALA_SECRET_ENV, "POST", orderPath, orderBody);

    try {
      const { status: os, body: ob } = await lalaFetch(LALA_HOST, orderPath, "POST", orderHdrs, orderBody);
      const oj = JSON.parse(ob);
      if (os !== 201 || !oj.data?.orderId)
        return res.status(200).json({ ok: false, error: "Erro ao criar pedido: " + JSON.stringify(oj.errors || oj).slice(0,300), httpStatus: os });

      // Marca fichas como enviadas e salva geocoords atualizadas
      pendentes.forEach(f => { f.status = "enviado"; f.orderId = oj.data.orderId; f.enviadoAt = new Date().toISOString(); });
      await dbSet(LALA_KEY, db);
      return res.status(200).json({ ok: true, orderId: oj.data.orderId, shareLink: oj.data.shareLink, count: pendentes.length, quotationId });
    } catch(e) {
      return res.status(200).json({ ok: false, error: "Pedido falhou: " + e.message });
    }
  }


  // ── GET status-pedido ─────────────────────────────────────────
  if (action === "status-pedido") {
    if (!LALA_KEY_ENV || !LALA_SECRET_ENV)
      return res.status(400).json({ ok: false, error: "API key não configurada" });

    const { orderId } = req.query;
    if (!orderId) return res.status(400).json({ ok: false, error: "orderId obrigatório" });

    const path = "/v3/orders/" + orderId;
    const hdrs = lalamoveHeaders(LALA_KEY_ENV, LALA_SECRET_ENV, "GET", path, "");
    try {
      const { status, body } = await lalaFetch(LALA_HOST, path, "GET", hdrs, null);
      const j = JSON.parse(body);
      return res.status(200).json({ ok: status === 200, data: j.data, httpStatus: status });
    } catch(e) {
      return res.status(200).json({ ok: false, error: e.message });
    }
  }

  // ── POST ou GET limpar-tudo — limpa fila, move fichas atuais para removedIds ──
  if (action === "limpar-tudo") {
    const db = await dbGet(LALA_KEY) || {};
    const fichasAtuais = db.fichas || [];
    // Adiciona todas as fichas atuais ao removedIds para não reimportá-las
    const removedIds = db.removedIds || [];
    fichasAtuais.forEach(f => {
      const key = f.pipefyId + ":" + f.tipo;
      if (!removedIds.includes(key)) removedIds.push(key);
    });
    await dbSet(LALA_KEY, {
      fichas:     [],
      removedIds: removedIds,
    });
    return res.status(200).json({ ok: true, msg: "Fila limpa. " + fichasAtuais.length + " ficha(s) arquivadas." });
  }

  // ── POST limpar-enviados ──────────────────────────────────────
  if (req.method === "POST" && action === "limpar-enviados") {
    const db = await dbGet(LALA_KEY) || { fichas: [] };
    db.fichas = db.fichas.filter(f => f.status !== "enviado");
    delete db.cotacaoAtual;
    await dbSet(LALA_KEY, db);
    return res.status(200).json({ ok: true });
  }

  // ── POST reset-fila — apaga todas as fichas
  if (req.method === "POST" && action === "reset-fila") {
    await dbSet(LALA_KEY, { fichas: [] });
    return res.status(200).json({ ok: true });
  }

  // ── POST set-coords — salva lat/lng de uma ficha (geocoding feito no frontend)
  if (req.method === "POST" && action === "set-coords") {
    const { id, lat, lng } = req.body || {};
    if (!id || !lat || !lng) return res.status(400).json({ ok: false, error: "id, lat e lng obrigatorios" });
    const db = await dbGet(LALA_KEY) || { fichas: [] };
    const f = (db.fichas || []).find(x => x.pipefyId === id || x.id === id);
    if (!f) return res.status(404).json({ ok: false, error: "Ficha nao encontrada" });
    f.lat = lat; f.lng = lng;
    await dbSet(LALA_KEY, db);
    return res.status(200).json({ ok: true });
  }

  // ── POST geocodificar-tudo — geocodifica todas as fichas sem coords ──
  if (req.method === "POST" && action === "geocodificar-tudo") {
    const db = await dbGet(LALA_KEY) || { fichas: [] };
    const semCoords = (db.fichas || []).filter(f => f.status === "pendente" && f.endereco && (!f.lat || !f.lng));
    let ok_count = 0, fail_count = 0;

    for (const f of semCoords) {
      const coords = await geocodificar(f.endereco);
      if (coords) {
        f.lat = coords.lat;
        f.lng = coords.lng;
        ok_count++;
      } else {
        fail_count++;
      }
      // Respeita rate limit do Nominatim: 1 req/segundo
      await new Promise(r => setTimeout(r, 1100));
    }

    if (ok_count > 0) await dbSet(LALA_KEY, db);
    return res.status(200).json({ ok: true, ok_count, fail_count, total: semCoords.length });
  }

  // ── POST geocodificar-ficha — geocodifica uma ficha específica ──
  if (req.method === "POST" && action === "geocodificar-ficha") {
    const { id } = req.body || {};
    const db = await dbGet(LALA_KEY) || { fichas: [] };
    const f = (db.fichas || []).find(x => x.pipefyId === id || x.id === id);
    if (!f) return res.status(404).json({ ok: false, error: "Ficha não encontrada" });
    if (!f.endereco) return res.status(400).json({ ok: false, error: "Ficha sem endereço" });
    const coords = await geocodificar(f.endereco);
    if (!coords) return res.status(200).json({ ok: false, error: "Endereço não encontrado no mapa" });
    f.lat = coords.lat;
    f.lng = coords.lng;
    await dbSet(LALA_KEY, db);
    return res.status(200).json({ ok: true, lat: f.lat, lng: f.lng });
  }

  // ── POST test-cotacao-raw — testa cotação com serviceType variável
  if (req.method === "POST" && action === "test-cotacao-raw") {
    if (!LALA_KEY_ENV || !LALA_SECRET_ENV)
      return res.status(200).json({ ok: false, error: "API key não configurada" });

    const { serviceType = "MOTORCYCLE", useSandbox = false } = req.body || {};
    const host = useSandbox ? LALA_HOST_SANDBOX : LALA_HOST_PROD;

    // Stop mínimo válido (BH)
    const stops = [
      { coordinates: { lat: "-19.924500", lng: "-43.935200" }, address: "Rua Ouro Preto 663, Barro Preto, BH" },
      { coordinates: { lat: "-19.920000", lng: "-43.940000" }, address: "Rua Sapucai, Floresta, BH" },
    ];

    const quotePath = "/v3/quotations";
    const quoteBody = JSON.stringify({ data: { serviceType, language: "pt_BR", stops, requesterContact: { name: "Reparo Eletro", phone: "+5531997856023" } } });
    const quoteHdrs = lalamoveHeaders(LALA_KEY_ENV, LALA_SECRET_ENV, "POST", quotePath, quoteBody);

    try {
      const { status, body } = await lalaFetch(host, quotePath, "POST", quoteHdrs, quoteBody);
      let parsed = null; try { parsed = JSON.parse(body); } catch(e) {}
      return res.status(200).json({ ok: status === 201, httpStatus: status, response: parsed || body, serviceType, host });
    } catch(e) {
      return res.status(200).json({ ok: false, error: e.message });
    }
  }

  // ── GET check-services — verifica market info e serviços disponíveis
  if (action === "check-services") {
    if (!LALA_KEY_ENV || !LALA_SECRET_ENV)
      return res.status(200).json({ ok: false, error: "API key não configurada" });
    try {
      // Tenta GET /v3/cities para ver se a chave é válida
      const path = "/v3/cities?country=BR";
      const hdrs = lalamoveHeaders(LALA_KEY_ENV, LALA_SECRET_ENV, "GET", path, "");
      const { status, body } = await lalaFetch(LALA_HOST, path, "GET", hdrs, null);
      let parsed = null; try { parsed = JSON.parse(body); } catch(e) {}
      return res.status(200).json({
        ok: status === 200,
        httpStatus: status,
        response: parsed || body,
        host: LALA_HOST,
        keyPrefix: LALA_KEY_ENV.slice(0,8) + "...",
        sandbox: SANDBOX,
      });
    } catch(e) {
      return res.status(200).json({ ok: false, error: e.message });
    }
  }

  // ── POST debug-cotacao — testa cotação com 1 ficha e retorna detalhes completos
  if (req.method === "POST" && action === "debug-cotacao") {
    if (!LALA_KEY_ENV || !LALA_SECRET_ENV)
      return res.status(200).json({ ok: false, error: "API key não configurada" });

    const db = await dbGet(LALA_KEY) || { fichas: [] };
    const pendente = (db.fichas || []).find(f => f.status === "pendente" && f.lat && f.lng);
    if (!pendente) return res.status(200).json({ ok: false, error: "Nenhuma ficha com coordenadas" });

    const fmtCoord = v => parseFloat(v).toFixed(6);
    const stops = [
      { location: { lat: fmtCoord(pendente.lat), lng: fmtCoord(pendente.lng) }, addresses: { pt_BR: { displayString: pendente.endereco || "BH", country: "BR" } } },
      { location: { lat: fmtCoord(LOJA.lat), lng: fmtCoord(LOJA.lng) }, addresses: { pt_BR: { displayString: LOJA.endereco, country: "BR" } } },
    ];

    const quotePath = "/v3/quotations";
    const quoteBody = JSON.stringify({ data: { serviceType: "CAR", language: "pt_BR", stops } });
    const quoteHdrs = lalamoveHeaders(LALA_KEY_ENV, LALA_SECRET_ENV, "POST", quotePath, quoteBody);

    try {
      const { status, body } = await lalaFetch(LALA_HOST, quotePath, "POST", quoteHdrs, quoteBody);
      let parsed = null;
      try { parsed = JSON.parse(body); } catch(e) {}
      return res.status(200).json({
        ok: status === 201,
        httpStatus: status,
        response: parsed || body,
        sentBody: JSON.parse(quoteBody),
        headers: { Authorization: quoteHdrs.Authorization?.slice(0,50) + "...", Market: quoteHdrs.Market },
        host: LALA_HOST,
        keyPrefix: LALA_KEY_ENV.slice(0,8) + "...",
      });
    } catch(e) {
      return res.status(200).json({ ok: false, error: e.message });
    }
  }

  // ── GET debug-geocode — testa o geocodificador para um endereço ─────────
  if (action === "debug-geocode") {
    const endereco = req.query.endereco || "Rua Ouro Preto, 663, Barro Preto, Belo Horizonte, MG";
    const GMAPS_KEY    = (process.env.GOOGLE_MAPS_KEY || "").trim();
    const OPENCAGE_KEY = (process.env.OPENCAGE_KEY    || "").trim();
    try {
      const inicio = Date.now();
      const coords = await geocodificar(endereco);
      const ms = Date.now() - inicio;
      return res.status(200).json({
        ok: !!coords, endereco, coords,
        ms, googleKey: !!GMAPS_KEY, opencageKey: !!OPENCAGE_KEY,
        googleMapsUrl: coords ? "https://maps.google.com/?q=" + coords.lat + "," + coords.lng : null,
      });
    } catch(e) {
      return res.status(200).json({ ok: false, endereco, error: e.message });
    }
  }


  // ── GET atualizar-nomes — busca nome correto do Pipefy para fichas existentes ──
  if (action === "atualizar-nomes") {
    const db = await dbGet(LALA_KEY) || { fichas: [] };
    const pendentes = (db.fichas || []).filter(f => f.status === "pendente" && f.pipefyId);
    if (!pendentes.length) return res.status(200).json({ ok: true, updated: 0 });

    const PIPEFY_TOKEN = (process.env.PIPEFY_TOKEN || "").trim();
    let updated = 0;

    // Busca em lotes de 5 para não sobrecarregar
    for (let i = 0; i < pendentes.length; i += 5) {
      const lote = pendentes.slice(i, i + 5);
      const aliases = lote.map((f, j) =>
        `c${j}: card(id: "${f.pipefyId}") { id fields { name value } }`
      ).join("\n");
      try {
        const r = await fetch(PIPEFY_API, {
          method: "POST",
          headers: { "Content-Type": "application/json", Authorization: `Bearer ${PIPEFY_TOKEN}` },
          body: JSON.stringify({ query: `query { ${aliases} }` }),
        });
        const data = (await r.json()).data || {};
        lote.forEach((f, j) => {
          const card   = data[`c${j}`];
          if (!card) return;
          const fields    = card.fields || [];
          const nomeField = fields.find(fl => fl.name.toLowerCase().includes("nome"));
          const telField  = fields.find(fl => fl.name.toLowerCase().includes("telefone") || fl.name.toLowerCase().includes("fone"));
          const endField  = fields.find(fl => fl.name.toLowerCase().includes("endere"));
          const nome = nomeField?.value?.trim();
          if (nome) {
            const idx = db.fichas.findIndex(x => x.pipefyId === f.pipefyId);
            if (idx >= 0) {
              db.fichas[idx].nomeContato = nome;
              if (telField?.value) db.fichas[idx].telefone = telField.value;
              if (endField?.value && !db.fichas[idx].endereco) db.fichas[idx].endereco = endField.value;
              updated++;
            }
          }
        });
      } catch(e) { console.error("atualizar-nomes lote:", e.message); }
    }

    if (updated > 0) await dbSet(LALA_KEY, db);
    return res.status(200).json({ ok: true, updated, total: pendentes.length });
  }

  return res.status(404).json({ ok: false, error: "Ação não encontrada" });
};
