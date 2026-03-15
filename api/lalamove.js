const UPSTASH_URL   = (process.env.UPSTASH_URL   || "").replace(/['"]/g,"").trim();
const UPSTASH_TOKEN = (process.env.UPSTASH_TOKEN || "").replace(/['"]/g,"").trim();
const BOARD_KEY     = "reparoeletro_board";
const LALA_KEY      = "reparoeletro_lalamove";
const PIPE_ID       = "305832912";
const PIPEFY_API    = "https://api.pipefy.com/graphql";

// Endereço fixo da loja
const LOJA = {
  nome:     "Reparo Eletro",
  endereco: "Rua Ouro Preto, 663, Barro Preto, Belo Horizonte, MG",
};

async function dbGet(key) {
  try {
    const r = await fetch(`${UPSTASH_URL}/pipeline`, {
      method:"POST",
      headers:{ Authorization:`Bearer ${UPSTASH_TOKEN}`, "Content-Type":"application/json" },
      body: JSON.stringify([["GET", key]]),
    });
    const j = await r.json();
    return j[0]?.result ? JSON.parse(j[0].result) : null;
  } catch(e) { return null; }
}

async function dbSet(key, value) {
  try {
    const r = await fetch(`${UPSTASH_URL}/pipeline`, {
      method:"POST",
      headers:{ Authorization:`Bearer ${UPSTASH_TOKEN}`, "Content-Type":"application/json" },
      body: JSON.stringify([["SET", key, JSON.stringify(value)]]),
    });
    const j = await r.json();
    return j[0]?.result === "OK";
  } catch(e) { return false; }
}

// Busca endereço de um card no Pipefy
async function fetchCardEndereco(pipefyId) {
  try {
    const res = await fetch(PIPEFY_API, {
      method:"POST",
      headers:{ "Content-Type":"application/json", Authorization:`Bearer ${(process.env.PIPEFY_TOKEN||"").trim()}` },
      body: JSON.stringify({ query:`query { card(id:"${pipefyId}") { id title fields { name value } } }` }),
    });
    const j = await res.json();
    const fields = j?.data?.card?.fields || [];
    const endField = fields.find(f => f.name.toLowerCase().includes("endere"));
    return endField?.value || null;
  } catch(e) { return null; }
}

// Cria pedido no Lalamove (real quando tiver API key)
async function criarPedidoLalamove(paradas) {
  const key    = (process.env.LALAMOVE_API_KEY    || "").trim();
  const secret = (process.env.LALAMOVE_API_SECRET || "").trim();
  if (!key || !secret) throw new Error("Lalamove API Key não configurada. Adicione LALAMOVE_API_KEY e LALAMOVE_API_SECRET nas variáveis de ambiente do Vercel.");

  const timestamp = Date.now().toString();
  const body = JSON.stringify({
    data: {
      serviceType: "CAR",
      language:    "pt_BR",
      stops: paradas.map(p => ({
        location: { address: p.endereco },
        POI:      p.nome || "",
      })),
      requesterContact: { name: "Reparo Eletro", phone: "+553199785-6023" },
    },
  });

  // HMAC-SHA256 signature
  const crypto = await import("crypto");
  const rawSignature = `${timestamp}\r\nPOST\r\n/v3/quotations\r\n\r\n${body}`;
  const sig = crypto.createHmac("sha256", secret).update(rawSignature).digest("hex");

  const r = await fetch("https://rest.lalamove.com/v3/quotations", {
    method:"POST",
    headers:{
      "Content-Type":"application/json; charset=utf8",
      Authorization:`hmac id="${key}", nonce="${timestamp}", signature="${sig}"`,
      Market: "BR",
    },
    body,
  });
  return await r.json();
}

module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin","*");
  res.setHeader("Access-Control-Allow-Methods","GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers","Content-Type");
  if (req.method === "OPTIONS") return res.status(200).end();

  const { action } = req.query;

  // ── GET load ──────────────────────────────────────────────
  if (action === "load") {
    const db = await dbGet(LALA_KEY) || { fichas: [] };
    const temApiKey = !!(process.env.LALAMOVE_API_KEY);
    return res.status(200).json({ ok:true, fichas: db.fichas||[], temApiKey, loja: LOJA });
  }

  // ── POST adicionar — adiciona ficha à lista de coletas/entregas
  if (req.method === "POST" && action === "adicionar") {
    const { pipefyId, tipo, nomeContato, osCode, descricao, enderecoManual } = req.body || {};
    if (!pipefyId || !tipo) return res.status(400).json({ ok:false, error:"pipefyId e tipo obrigatórios" });

    const db = await dbGet(LALA_KEY) || { fichas: [] };
    if (!Array.isArray(db.fichas)) db.fichas = [];

    // Evita duplicata
    if (db.fichas.find(f => f.pipefyId === pipefyId && f.tipo === tipo)) {
      return res.status(200).json({ ok:true, msg:"Já adicionado", duplicata:true });
    }

    // Busca endereço no Pipefy se for coleta
    let endereco = enderecoManual || null;
    if (!endereco && tipo === "coleta") {
      endereco = await fetchCardEndereco(pipefyId);
    }
    if (!endereco && tipo === "entrega") {
      endereco = await fetchCardEndereco(pipefyId);
    }

    db.fichas.push({
      pipefyId,
      tipo,         // "coleta" | "entrega"
      osCode:       osCode       || null,
      nomeContato:  nomeContato  || null,
      descricao:    descricao    || null,
      endereco:     endereco     || null,
      addedAt:      new Date().toISOString(),
      status:       "pendente",  // pendente | enviado
    });

    await dbSet(LALA_KEY, db);
    return res.status(200).json({ ok:true });
  }

  // ── POST update-endereco — corrige endereço de uma ficha
  if (req.method === "POST" && action === "update-endereco") {
    const { pipefyId, tipo, endereco } = req.body || {};
    const db = await dbGet(LALA_KEY) || { fichas: [] };
    const f  = db.fichas.find(x => x.pipefyId === pipefyId && x.tipo === tipo);
    if (!f) return res.status(404).json({ ok:false, error:"Ficha não encontrada" });
    f.endereco = endereco;
    await dbSet(LALA_KEY, db);
    return res.status(200).json({ ok:true });
  }

  // ── POST remover
  if (req.method === "POST" && action === "remover") {
    const { pipefyId, tipo } = req.body || {};
    const db = await dbGet(LALA_KEY) || { fichas: [] };
    db.fichas = db.fichas.filter(f => !(f.pipefyId === pipefyId && f.tipo === tipo));
    await dbSet(LALA_KEY, db);
    return res.status(200).json({ ok:true });
  }

  // ── POST enviar-lalamove — envia todas as fichas pendentes de um tipo
  if (req.method === "POST" && action === "enviar-lalamove") {
    const { tipo } = req.body || {};
    const db = await dbGet(LALA_KEY) || { fichas: [] };
    const pendentes = db.fichas.filter(f => f.tipo === tipo && f.status === "pendente" && f.endereco);

    if (!pendentes.length) return res.status(400).json({ ok:false, error:"Nenhuma ficha com endereço preenchido" });

    // Monta paradas: coleta = clientes → loja; entrega = loja → clientes
    let paradas;
    if (tipo === "coleta") {
      paradas = [
        ...pendentes.map(f => ({ nome: f.nomeContato || f.osCode, endereco: f.endereco })),
        { nome: LOJA.nome, endereco: LOJA.endereco },
      ];
    } else {
      paradas = [
        { nome: LOJA.nome, endereco: LOJA.endereco },
        ...pendentes.map(f => ({ nome: f.nomeContato || f.osCode, endereco: f.endereco })),
      ];
    }

    try {
      const result = await criarPedidoLalamove(paradas);
      // Marca como enviado
      pendentes.forEach(f => { f.status = "enviado"; f.enviadoAt = new Date().toISOString(); });
      await dbSet(LALA_KEY, db);
      return res.status(200).json({ ok:true, lalamove: result, count: pendentes.length });
    } catch(e) {
      return res.status(200).json({ ok:false, error: e.message });
    }
  }

  // ── POST limpar-enviados
  if (req.method === "POST" && action === "limpar-enviados") {
    const db = await dbGet(LALA_KEY) || { fichas: [] };
    db.fichas = db.fichas.filter(f => f.status !== "enviado");
    await dbSet(LALA_KEY, db);
    return res.status(200).json({ ok:true });
  }

  return res.status(404).json({ ok:false, error:"Ação não encontrada" });
};
