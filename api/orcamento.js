const PIPEFY_API = "https://api.pipefy.com/graphql";
const PIPE_ID    = "305832912";

async function pipefyQuery(query) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 15000);
  try {
    const res = await fetch(PIPEFY_API, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${(process.env.PIPEFY_TOKEN || "").trim()}`,
      },
      body: JSON.stringify({ query }),
      signal: controller.signal,
    });
    const text = await res.text();
    let json;
    try { json = JSON.parse(text); } catch(e) { throw new Error("Pipefy retornou resposta inválida"); }
    if (json.errors) throw new Error(json.errors.map(e => e.message).join("; "));
    return json.data;
  } finally { clearTimeout(timer); }
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
  const mutation = `mutation {
    updateCardField(input: {
      card_id: "${pipefyId}"
      field_id: "valor_de_contrato"
      new_value: "${String(valor).replace(/"/g, "")}"
    }) { success }
  }`;
  return await pipefyQuery(mutation);
}

async function createPipefyCard({ phaseId, nome, telefone, aparelho, defeito, endereco }) {
  const descricao   = `${aparelho} — ${defeito}`;
  const ultimos4    = telefone.replace(/\D/g, "").slice(-4);
  const nomeContato = `${nome} ${ultimos4}`;

  const { fields } = await fetchPipeStructure();

  function findField(keywords) {
    return fields.find(f =>
      keywords.some(kw => f.label.toLowerCase().includes(kw))
    );
  }

  const nomeField = findField(["nome"]);
  const telField  = findField(["telefone", "fone", "celular"]);
  const descField = findField(["descrição", "descricao", "empresa", "descri"]);
  const endField  = findField(["endereço", "endereco", "endere"]);

  const fieldsAttr = [];
  if (nomeField) fieldsAttr.push(`{ field_id: "${nomeField.id}", field_value: ${JSON.stringify(nomeContato)} }`);
  if (telField)  fieldsAttr.push(`{ field_id: "${telField.id}",  field_value: ${JSON.stringify(telefone)} }`);
  if (descField) fieldsAttr.push(`{ field_id: "${descField.id}", field_value: ${JSON.stringify(descricao)} }`);
  if (endField && endereco) fieldsAttr.push(`{ field_id: "${endField.id}", field_value: ${JSON.stringify(endereco)} }`);

  // Tenta criar com phase_id primeiro, se falhar cria sem (vai para fase inicial)
  const tryCreate = async (usePhaseId) => {
    const phaseArg = usePhaseId && phaseId ? `\n      phase_id: "${phaseId}"` : "";
    const mutation = `mutation {
      createCard(input: {
        pipe_id: "${PIPE_ID}"${phaseArg}
        fields_attributes: [${fieldsAttr.join(", ")}]
      }) {
        card { id title url current_phase { name } }
      }
    }`;
    return await pipefyQuery(mutation);
  };

  let data;
  try {
    data = await tryCreate(true);
  } catch(e) {
    // Se falhar com phase_id, tenta sem
    data = await tryCreate(false);
  }

  return data?.createCard?.card;
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
    const { nome, telefone, aparelho, defeito, endereco, phaseId } = req.body || {};
    if (!nome || !telefone || !aparelho || !defeito)
      return res.status(400).json({ ok: false, error: "nome, telefone, aparelho e defeito são obrigatórios" });
    try {
      const card = await createPipefyCard({ phaseId, nome, telefone, aparelho, defeito, endereco: endereco || "" });
      return res.status(200).json({ ok: true, card });
    } catch(e) {
      return res.status(200).json({ ok: false, error: e.message });
    }
  }

  // ── GET orc-load ──────────────────────────────────────────
  if (action === "orc-load") {
    const db = await dbGet(ORC_KEY) || { fichas: [], syncedIds: [] };
    return res.status(200).json({ ok: true, fichas: db.fichas || [] });
  }

  // ── GET orc-sync ───────────────────────────────────────────
  if (action === "orc-sync") {
    const db = await dbGet(ORC_KEY) || { fichas: [], syncedIds: [], initialized: false };
    if (!Array.isArray(db.fichas))    db.fichas    = [];
    if (!Array.isArray(db.syncedIds)) db.syncedIds = [];
    let newCount = 0, pipefyError = null;
    try {
      const cards = await fetchAguardandoAprovacao();
      // Primeira vez: apenas registra os IDs existentes sem criar fichas
      if (!db.initialized) {
        cards.forEach(card => {
          if (!db.syncedIds.includes(card.pipefyId)) db.syncedIds.push(card.pipefyId);
        });
        db.initialized = true;
        await dbSet(ORC_KEY, db);
        return res.status(200).json({ ok: true, newCount: 0, initialized: true, pipefyError: null });
      }
      // Próximas vezes: importa apenas cards novos
      for (const card of cards) {
        if (db.syncedIds.includes(card.pipefyId)) continue;
        let textoOrc = "";
        try { textoOrc = await gerarTextoOrcamento(card.desc, card.comentarios); } catch(e) {}
        db.fichas.unshift({
          id:          card.pipefyId,
          pipefyId:    card.pipefyId,
          nome:        card.nome,
          tel:         card.tel,
          desc:        card.desc,
          end:         card.end,
          age:         card.age,
          comentarios: card.comentarios,
          textoOrc,
          status:      "pendente",
          preco:       null,
          createdAt:   new Date().toISOString(),
        });
        db.syncedIds.push(card.pipefyId);
        newCount++;
      }
      if (newCount > 0) await dbSet(ORC_KEY, db);
    } catch(e) { pipefyError = e.message; }
    return res.status(200).json({ ok: true, newCount, pipefyError });
  }

  // ── POST orc-update-texto ──────────────────────────────────
  // Regenera ou edita o texto de orçamento de uma ficha
  if (req.method === "POST" && action === "orc-update-texto") {
    const { id, textoOrc } = req.body || {};
    const db = await dbGet(ORC_KEY) || { fichas: [], syncedIds: [] };
    const ficha = db.fichas.find(f => f.id === id);
    if (!ficha) return res.status(404).json({ ok: false, error: "Ficha não encontrada" });
    ficha.textoOrc = textoOrc;
    await dbSet(ORC_KEY, db);
    return res.status(200).json({ ok: true, ficha });
  }

  // ── POST orc-regenerar ─────────────────────────────────────
  if (req.method === "POST" && action === "orc-regenerar") {
    const { id } = req.body || {};
    const db = await dbGet(ORC_KEY) || { fichas: [], syncedIds: [] };
    const ficha = db.fichas.find(f => f.id === id);
    if (!ficha) return res.status(404).json({ ok: false, error: "Ficha não encontrada" });
    try {
      ficha.textoOrc = await gerarTextoOrcamento(ficha.desc, ficha.comentarios);
      await dbSet(ORC_KEY, db);
      return res.status(200).json({ ok: true, textoOrc: ficha.textoOrc });
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
    // Atualiza valor no Pipefy em background (não bloqueia)
    if (preco && ficha.pipefyId && !ficha.pipefyId.startsWith("local")) {
      updateCardValue(ficha.pipefyId, preco).catch(e => console.error("updateCardValue:", e.message));
    }
    return res.status(200).json({ ok: true, ficha });
  }

  // ── POST orc-status ────────────────────────────────────────
  if (req.method === "POST" && action === "orc-status") {
    const { id, status } = req.body || {};
    const db = await dbGet(ORC_KEY) || { fichas: [], syncedIds: [] };
    const ficha = db.fichas.find(f => f.id === id);
    if (!ficha) return res.status(404).json({ ok: false, error: "Ficha não encontrada" });
    ficha.status = status;
    await dbSet(ORC_KEY, db);
    return res.status(200).json({ ok: true });
  }

  // ── GET orc-reset-init ────────────────────────────────────
  // Reseta o flag de inicialização para reimportar tudo
  if (action === "orc-reset-init") {
    const db = await dbGet(ORC_KEY) || { fichas: [], syncedIds: [] };
    db.initialized = false;
    db.syncedIds   = [];
    await dbSet(ORC_KEY, db);
    return res.status(200).json({ ok: true, msg: "Reset feito. Próximo sync vai marcar cards atuais como vistos sem importar." });
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

// Busca cards em Aguardando Aprovação com campos e comentários
async function fetchAguardandoAprovacao() {
  const data = await pipefyQuery(`query {
    pipe(id: "${PIPE_ID}") {
      phases {
        name
        cards(first: 50) {
          edges {
            node {
              id title age
              fields { name value }
              comments { text author { name } created_at }
            }
          }
        }
      }
    }
  }`);
  const phases = data?.pipe?.phases || [];
  const phase  = phases.find(p => p.name.toLowerCase().includes("aguardando aprovação") || p.name.toLowerCase().includes("aguardando aprovacao"));
  if (!phase) return [];
  return phase.cards.edges.map(e => {
    const node = e.node;
    const fields = node.fields || [];
    const nome    = fields.find(f => f.name.toLowerCase().includes("nome"))?.value || node.title;
    const tel     = fields.find(f => f.name.toLowerCase().includes("telefone") || f.name.toLowerCase().includes("fone"))?.value || "";
    const desc    = fields.find(f => f.name.toLowerCase().includes("descri") || f.name.toLowerCase().includes("empresa"))?.value || "";
    const end     = fields.find(f => f.name.toLowerCase().includes("endere"))?.value || "";
    const comentarios = (node.comments || []).map(c => c.text).filter(Boolean);
    return { pipefyId: String(node.id), title: node.title, nome, tel, desc, end, age: node.age, comentarios };
  });
}

// Gera texto de orçamento com Claude
async function gerarTextoOrcamento(desc, comentarios) {
  const comStr = comentarios && comentarios.length ? comentarios.join("; ") : "";
  const userMsg = `Defeito/Descrição: ${desc}${comStr ? "\nAtividades registradas no card: " + comStr : ""}`;
  const res = await fetch("https://api.anthropic.com/v1/messages", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      model: "claude-sonnet-4-20250514",
      max_tokens: 400,
      system: `Você é Pedro, técnico da Reparo Eletro em BH. Gera textos de orçamento para enviar no WhatsApp.

REGRAS ABSOLUTAS:
- Siga EXATAMENTE esta estrutura, sem desviar:

Olá, bom dia, sou o Pedro da Reparo Eletro, vou te enviar agora o orçamento:

[1 a 2 frases descrevendo o diagnóstico e o que será feito/trocado, baseado no defeito e nas atividades]

Este conserto completo fica em [VALOR] apenas. Aprovando já iniciamos o conserto.

- Deixe [VALOR] literalmente assim — será substituído pelo atendente
- Use os termos técnicos das atividades registradas (kit termoelétrico, cooler, peltier, placa, etc.)
- Se não houver atividades, descreva com base no defeito de forma genérica
- Sem emojis
- Máximo 3 parágrafos curtos no total`,
      messages: [{ role: "user", content: userMsg }],
    }),
  });
  const data = await res.json();
  return data.content?.[0]?.text || "";
}
