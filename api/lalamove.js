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

  // Normaliza o endereço: expande abreviações, remove complementos, garante BH
  const endNorm = endereco
    .replace(/,?\s*BH/gi, "")
    .replace(/R\.\s+/g, "Rua ")
    .replace(/Av\.\s+/g, "Avenida ")
    .replace(/Al\.\s+/g, "Alameda ")
    .replace(/,?\s*[-]?\s*(ap(to)?\.?|apartamento|bloco|bl\.?|sala|lote)\s*[\w\d]+/gi, "")
    .replace(/\s+/g, " ").trim();

  const endBH = (endNorm.toLowerCase().includes("belo horizonte") ? endNorm : endNorm + ", Belo Horizonte, MG, Brasil");

  // Validação: coords devem ser de MG
  const dentroMG = (lat, lng) => lat > -23 && lat < -14 && lng > -52 && lng < -39;
  // Validação mais estrita: bbox de BH e região metropolitana
  const dentoBH  = (lat, lng) => lat > -20.3 && lat < -19.4 && lng > -44.5 && lng < -43.3;

  // ── 1. Nominatim (OSM) — melhor cobertura do Brasil ──────────
  const nomQuery = async (q) => {
    const url = "https://nominatim.openstreetmap.org/search?q="
      + encodeURIComponent(q)
      + "&format=json&limit=5&countrycodes=br&viewbox=-44.5,-20.3,-43.3,-19.4&bounded=0";
    const r = await fetch(url, { headers: { "User-Agent": "ReparoEletro/1.0 (reparoeletroadm.com)" } });
    const j = await r.json();
    const emBH = j?.find(x => dentoBH(parseFloat(x.lat), parseFloat(x.lon)));
    const emMG = j?.find(x => dentroMG(parseFloat(x.lat), parseFloat(x.lon)));
    return emBH || emMG || null;
  };

  try {
    // Tentativa 1: endereço completo
    let best = await nomQuery(endBH);

    // Tentativa 2: sem número (só rua + bairro + cidade)
    if (!best) {
      const semNum = endBH.replace(/,?\s*\d+[-\w]*/g, "").replace(/\s+/g, " ").trim();
      if (semNum !== endBH) best = await nomQuery(semNum);
    }

    // Tentativa 3: só bairro + cidade (fallback de proximidade)
    if (!best) {
      const partes = endNorm.split(",").map(p => p.trim());
      // Tenta pegar o bairro (geralmente 2º ou 3º elemento)
      for (let i = partes.length - 1; i >= 1; i--) {
        const bairro = partes[i] + ", Belo Horizonte, MG, Brasil";
        best = await nomQuery(bairro);
        if (best) break;
      }
    }

    if (best) return { lat: String(best.lat), lng: String(best.lon) };
  } catch(e) { console.error("Nominatim:", e.message); }

  // ── 2. Google Maps (se configurado) ──────────────────────────
  if (GMAPS_KEY) {
    try {
      const url = "https://maps.googleapis.com/maps/api/geocode/json?address="
        + encodeURIComponent(endBH) + "&region=br&key=" + GMAPS_KEY;
      const r = await fetch(url);
      const j = await r.json();
      if (j.status === "OK" && j.results?.[0]) {
        const { lat, lng } = j.results[0].geometry.location;
        if (dentroMG(lat, lng)) return { lat: String(lat), lng: String(lng) };
      }
    } catch(e) { console.error("Google Maps:", e.message); }
  }

  // ── 3. OpenCage (se configurado) ─────────────────────────────
  if (OPENCAGE_KEY) {
    try {
      const url = "https://api.opencagedata.com/geocode/v1/json?q="
        + encodeURIComponent(endBH)
        + "&key=" + OPENCAGE_KEY
        + "&countrycode=br&limit=5&language=pt&no_annotations=1"
        + "&proximity=-19.9245,-43.9352";
      const r = await fetch(url);
      const j = await r.json();
      const precisos = (j.results || []).filter(x => (x.confidence || 0) >= 7 && dentroMG(x.geometry.lat, x.geometry.lng));
      if (precisos.length) return { lat: String(precisos[0].geometry.lat), lng: String(precisos[0].geometry.lng) };
    } catch(e) { console.error("OpenCage:", e.message); }
  }

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
      if (qs !== 201 || !qj.data?.quotationId)
        return res.status(200).json({ ok: false, error: "Erro na cotação: " + JSON.stringify(qj.errors || qj).slice(0,300) });
      quotationId = qj.data.quotationId;
      lalaStops   = qj.data.stops || [];
    } catch(e) {
      return res.status(200).json({ ok: false, error: "Cotação falhou: " + e.message });
    }

    // ETAPA 2: Pedido
    let senderStopId, recipientStops;
    if (tipo === "coleta") {
      senderStopId   = lalaStops[lalaStops.length - 1]?.stopId;
      recipientStops = lalaStops.slice(0, -1).map((s, i) => ({
        stopId: s.stopId,
        name:   pendentes[i]?.nomeContato || "Cliente",
        phone:  formatTelIntl(pendentes[i]?.telefone),
      }));
    } else {
      senderStopId   = lalaStops[0]?.stopId;
      recipientStops = lalaStops.slice(1).map((s, i) => ({
        stopId: s.stopId,
        name:   pendentes[i]?.nomeContato || "Cliente",
        phone:  formatTelIntl(pendentes[i]?.telefone),
      }));
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
    if (tipo === "coleta") {
      senderStopId   = lalaStops[lalaStops.length - 1]?.stopId;
      recipientStops = lalaStops.slice(0, -1).map((s, i) => ({ stopId: s.stopId, name: pendentes[i]?.nomeContato || "Cliente", phone: formatTelIntl(pendentes[i]?.telefone) }));
    } else {
      senderStopId   = lalaStops[0]?.stopId;
      recipientStops = lalaStops.slice(1).map((s, i) => ({ stopId: s.stopId, name: pendentes[i]?.nomeContato || "Cliente", phone: formatTelIntl(pendentes[i]?.telefone) }));
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

  // ── POST ou GET limpar-tudo — limpa fila e salva timestamp de limpeza ───────
  if (action === "limpar-tudo") {
    const db = await dbGet(LALA_KEY) || {};
    // Preserva syncedIds para não reimportar fichas antigas
    // Salva clearTimestamp: só fichas que entraram em Coleta Solicitada APÓS este momento serão importadas
    await dbSet(LALA_KEY, {
      fichas:         [],
      removedIds:     [],
      syncedIds:      db.syncedIds || [],
      clearTimestamp: new Date().toISOString(),
    });
    return res.status(200).json({ ok: true, msg: "Fila limpa", clearTimestamp: new Date().toISOString() });
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

  // ── GET debug-geocode — testa os 3 geocodificadores em sequência ───────
  if (action === "debug-geocode") {
    const GMAPS_KEY    = (process.env.GOOGLE_MAPS_KEY || "").trim();
    const OPENCAGE_KEY = (process.env.OPENCAGE_KEY    || "").trim();
    const endereco     = req.query.endereco || "Rua Ouro Preto, 663, Barro Preto, Belo Horizonte, MG";

    const endBH = endereco.toLowerCase().includes("belo horizonte") ? endereco : endereco + ", Belo Horizonte, MG, Brasil";
    const dentroMG = (lat, lng) => lat > -23 && lat < -14 && lng > -52 && lng < -39;
    const dentoBH  = (lat, lng) => lat > -20.3 && lat < -19.4 && lng > -44.5 && lng < -43.3;

    const resultado = {
      endereco, endBH,
      googleKeyPresente: !!GMAPS_KEY,
      opencageKeyPresente: !!OPENCAGE_KEY,
      testes: {}
    };

    // Teste 1: Nominatim — tenta 3 variações
    try {
      const nomQ = async (q) => {
        const u = "https://nominatim.openstreetmap.org/search?q=" + encodeURIComponent(q)
          + "&format=json&limit=5&countrycodes=br&viewbox=-44.5,-20.3,-43.3,-19.4&bounded=0";
        const r = await fetch(u, { headers: { "User-Agent": "ReparoEletro/1.0" } });
        return r.json();
      };
      const semNum = endBH.replace(/,?\s*\d+[-\w]*/g, "").replace(/\s+/g, " ").trim();
      const partes = endBH.split(",").map(p => p.trim());
      const bairro = partes.length > 1 ? partes[partes.length-2] + ", Belo Horizonte, MG" : null;

      const t1 = await nomQ(endBH);
      const t2 = semNum !== endBH ? await nomQ(semNum) : [];
      const t3 = bairro ? await nomQ(bairro) : [];

      const findBest = (j) => {
        const emBH = j?.find(x => dentoBH(parseFloat(x.lat), parseFloat(x.lon)));
        const emMG = j?.find(x => dentroMG(parseFloat(x.lat), parseFloat(x.lon)));
        return emBH || emMG || null;
      };
      const best = findBest(t1) || findBest(t2) || findBest(t3);

      resultado.testes.nominatim = {
        status: best ? "OK" : "sem resultado",
        coords: best ? { lat: parseFloat(best.lat), lng: parseFloat(best.lon) } : null,
        display_name: best?.display_name || null,
        tentativas: {
          completo:    { query: endBH, resultados: t1?.length || 0, achou: !!findBest(t1) },
          semNumero:   { query: semNum, resultados: t2?.length || 0, achou: !!findBest(t2) },
          bairro:      { query: bairro, resultados: t3?.length || 0, achou: !!findBest(t3) },
        }
      };
    } catch(e) { resultado.testes.nominatim = { status: "erro: " + e.message }; }

    // Teste 2: Google Maps
    if (GMAPS_KEY) {
      try {
        const url = "https://maps.googleapis.com/maps/api/geocode/json?address="
          + encodeURIComponent(endBH) + "&region=br&key=" + GMAPS_KEY;
        const r = await fetch(url);
        const j = await r.json();
        const ok = j.status === "OK" && j.results?.[0];
        resultado.testes.google = {
          status: j.status,
          coords: ok ? { lat: j.results[0].geometry.location.lat, lng: j.results[0].geometry.location.lng } : null,
          formatted: ok ? j.results[0].formatted_address : null,
          erro: j.error_message || null,
        };
      } catch(e) { resultado.testes.google = { status: "erro: " + e.message }; }
    } else {
      resultado.testes.google = { status: "key não configurada" };
    }

    // Teste 3: OpenCage
    if (OPENCAGE_KEY) {
      try {
        const url = "https://api.opencagedata.com/geocode/v1/json?q="
          + encodeURIComponent(endBH)
          + "&key=" + OPENCAGE_KEY
          + "&countrycode=br&limit=5&language=pt&no_annotations=1&proximity=-19.9245,-43.9352";
        const r = await fetch(url);
        const j = await r.json();
        resultado.testes.opencage = {
          totalResultados: j.results?.length || 0,
          resultados: (j.results || []).map(x => ({
            formatted: x.formatted,
            confidence: x.confidence,
            lat: x.geometry.lat,
            lng: x.geometry.lng,
          })),
          melhorConfidence: j.results?.[0]?.confidence || 0,
        };
      } catch(e) { resultado.testes.opencage = { status: "erro: " + e.message }; }
    } else {
      resultado.testes.opencage = { status: "key não configurada" };
    }

    // Resultado final (qual seria usado)
    const nom = resultado.testes.nominatim;
    const goo = resultado.testes.google;
    const oc  = resultado.testes.opencage;
    resultado.apiUsada = nom?.coords ? "Nominatim" : goo?.coords ? "Google Maps" : (oc?.melhorConfidence >= 7 ? "OpenCage" : "nenhuma");
    resultado.coordsFinal = nom?.coords || goo?.coords || null;

    return res.status(200).json(resultado);
  }

  return res.status(404).json({ ok: false, error: "Ação não encontrada" });
};
