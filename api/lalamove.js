// lalamove.js — Integração Lalamove v3 API (Brazil)
// Fluxo: Adicionar fichas → Cotar → Confirmar Pedido
const https  = require("https");
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
  try {
    const q = encodeURIComponent(endereco + ", Belo Horizonte, MG, Brasil");
    const url = "https://nominatim.openstreetmap.org/search?q=" + q + "&format=json&limit=1&countrycodes=br";
    const r = await fetch(url, { headers: { "User-Agent": "ReparoEletro/1.0 (reparoeletroadm.com)" } });
    const j = await r.json();
    if (j && j[0]) return { lat: j[0].lat, lng: j[0].lon };
    return null;
  } catch(e) { return null; }
}

// ── HMAC signature para Lalamove ──────────────────────────────
function lalamoveSign(secret, timestamp, method, path, body) {
  const raw = `${timestamp}\r\n${method.toUpperCase()}\r\n${path}\r\n\r\n${body}`;
  return crypto.createHmac("sha256", secret).update(raw).digest("hex");
}

function lalamoveHeaders(key, secret, method, path, body) {
  const ts  = Date.now().toString();
  const sig = lalamoveSign(secret, ts, method, path, body);
  return {
    "Content-Type":  "application/json; charset=utf8",
    "Authorization": `hmac id="${key}", nonce="${ts}", signature="${sig}"`,
    "Market":        "BR",
    "Accept":        "application/json",
  };
}

// ── Chamada HTTPS para Lalamove ───────────────────────────────
function lalaFetch(host, path, method, headers, body) {
  return new Promise((resolve, reject) => {
    const buf = body ? Buffer.from(body, "utf8") : null;
    const opts = {
      hostname: host, port: 443, path, method,
      headers: { ...headers, ...(buf ? { "Content-Length": buf.length } : {}) },
    };
    const req = https.request(opts, r => {
      const chunks = [];
      r.on("data", c => chunks.push(c));
      r.on("end", () => resolve({ status: r.statusCode, body: Buffer.concat(chunks).toString("utf8") }));
    });
    req.on("error", reject);
    if (buf) req.write(buf);
    req.end();
  });
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
    const db = await dbGet(LALA_KEY) || { fichas: [] };
    db.fichas = db.fichas.filter(f => !(f.pipefyId === pipefyId && f.tipo === tipo));
    await dbSet(LALA_KEY, db);
    return res.status(200).json({ ok: true });
  }

  // ── POST enviar-lalamove — cotar + pedir em sequência automática
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
      stops = [
        ...pendentes.map(f => ({ location: { lat: String(f.lat), lng: String(f.lng) }, addresses: { pt_BR: { displayString: f.endereco, country: "BR" } } })),
        { location: { lat: LOJA.lat, lng: LOJA.lng }, addresses: { pt_BR: { displayString: LOJA.endereco, country: "BR" } } },
      ];
    } else {
      stops = [
        { location: { lat: LOJA.lat, lng: LOJA.lng }, addresses: { pt_BR: { displayString: LOJA.endereco, country: "BR" } } },
        ...pendentes.map(f => ({ location: { lat: String(f.lat), lng: String(f.lng) }, addresses: { pt_BR: { displayString: f.endereco, country: "BR" } } })),
      ];
    }

    // ETAPA 1: Cotação
    const quotePath = "/v3/quotations";
    const quoteBody = JSON.stringify({ data: { serviceType: "MOTORCYCLE", language: "pt_BR", stops, requesterContact: { name: LOJA.nome, phone: LOJA.telefone } } });
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

  // ── POST ou GET limpar-tudo — limpa TODA a fila ─────────────
  if (action === "limpar-tudo") {
    await dbSet(LALA_KEY, { fichas: [] });
    return res.status(200).json({ ok: true, msg: "Fila limpa" });
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

  return res.status(404).json({ ok: false, error: "Ação não encontrada" });
};
