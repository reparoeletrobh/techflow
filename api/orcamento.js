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
      // Importa apenas cards com ID maior que o máximo visto na inicialização
      for (const card of cards) {
        const cardId = parseInt(card.pipefyId) || 0;
        if (cardId <= db.maxIdSeen) continue;        // card antigo
        if (db.syncedIds.includes(card.pipefyId)) continue; // já processado
        let textoOrc = "";
        try { textoOrc = await gerarTextoOrcamento(card.desc, card.comentarios, card.nome); } catch(e) {}
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
        if (cardId > db.maxIdSeen) db.maxIdSeen = cardId;
        newCount++;
      }
      if (newCount > 0) await dbSet(ORC_KEY, db);
    } catch(e) { pipefyError = e.message; }
    return res.status(200).json({ ok: true, newCount, pipefyError, maxIdSeen: db.maxIdSeen });
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
      ficha.textoOrc = await gerarTextoOrcamento(ficha.desc, ficha.comentarios, ficha.nome);
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

  // ── POST orc-forcar ───────────────────────────────────────
  // Remove um pipefyId do syncedIds para forçar reimportação
  if (req.method === "POST" && action === "orc-forcar") {
    const { pipefyId } = req.body || {};
    if (!pipefyId) return res.status(400).json({ ok: false, error: "pipefyId obrigatório" });
    const db = await dbGet(ORC_KEY) || { fichas: [], syncedIds: [] };
    db.syncedIds = (db.syncedIds || []).filter(id => id !== String(pipefyId));
    await dbSet(ORC_KEY, db);
    return res.status(200).json({ ok: true, msg: "ID removido. Próximo sync vai importar este card." });
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

// Busca cards em Aguardando Aprovação direto pelo ID da fase (mais rápido e completo)
const AGUARDANDO_APROVACAO_PHASE_ID = "334875152";

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
      const nome   = fields.find(f => f.name.toLowerCase().includes("nome"))?.value || node.title;
      const tel    = fields.find(f => f.name.toLowerCase().includes("telefone") || f.name.toLowerCase().includes("fone"))?.value || "";
      const desc   = fields.find(f => f.name.toLowerCase().includes("descri") || f.name.toLowerCase().includes("empresa"))?.value || "";
      const end    = fields.find(f => f.name.toLowerCase().includes("endere"))?.value || "";
      const comentarios = (node.comments || []).map(c => c.text).filter(Boolean);
      all.push({ pipefyId: String(node.id), title: node.title, nome, tel, desc, end, age: node.age, comentarios });
    }
    hasNext = phase.cards.pageInfo?.hasNextPage ?? false;
    cursor  = phase.cards.pageInfo?.endCursor ?? null;
  }
  return all;
}

// Gera texto de orçamento com Claude
// ── REGRAS DE ORÇAMENTO ──────────────────────────────────────
// Cada regra: { keywords, template }
// keywords: palavras-chave buscadas em QUALQUER campo do card (desc + comentarios)
// template: texto final com [NOME] como placeholder do nome do cliente
const ORCAMENTO_REGRAS = [
  {
    keywords: ["termoeletrico", "termeletrico", "termoelétrico", "cooler", "placa de resfriamento", "peltier", "pasta termica", "pasta térmica", "kit frio", "kit termoeletrico"],
    template: `Olá, [NOME] bom dia, sou o Pedro da Reparo Eletro, vou te enviar agora o orçamento:
Foram feitos todos os testes e identificamos que será necessário refazer a parte elétrica que causou danos no conjunto do cooler, placa de resfriamento e pasta térmica, as peças serão trocadas também. Este conserto completo fica em 350 reais apenas. Aprovando já iniciamos o conserto.`,
  },
  // ── Adicionar novos casos aqui ──
  // { keywords: ["fusível", "fusivel", "queimou"], template: `Olá, [NOME]...` },
];

function detectarRegra(desc, comentarios) {
  const texto = [desc, ...(comentarios || [])].join(" ").toLowerCase()
    .normalize("NFD").replace(/[̀-ͯ]/g, ""); // remove acentos
  for (const regra of ORCAMENTO_REGRAS) {
    const match = regra.keywords.some(kw =>
      texto.includes(kw.toLowerCase().normalize("NFD").replace(/[̀-ͯ]/g, ""))
    );
    if (match) return regra.template;
  }
  return null;
}

function templatePadrao(desc) {
  return `Olá, [NOME] bom dia, sou o Pedro da Reparo Eletro, vou te enviar agora o orçamento:

Realizamos todos os testes e identificamos o problema: ${desc || "defeito identificado após análise"}. Faremos o reparo completo com substituição das peças necessárias.

Este conserto completo fica em [VALOR] apenas. Aprovando já iniciamos o conserto.`;
}

async function gerarTextoOrcamento(desc, comentarios, nome) {
  // 1. Verifica se bate com alguma regra conhecida
  const regra = detectarRegra(desc, comentarios);
  if (regra) {
    return regra.replace(/\[NOME\]/g, nome ? nome.split(" ")[0] : "");
  }

  // 2. Sem regra — usa IA para gerar com base no defeito e comentários
  if (!desc && (!comentarios || !comentarios.length)) return templatePadrao(desc);

  const nomeFirst = nome ? nome.split(" ")[0] : "";
  const comStr = comentarios && comentarios.length ? comentarios.join("; ") : "";
  const userMsg = `Nome do cliente: ${nomeFirst || "cliente"}
Defeito/Descrição: ${desc || "não informado"}${comStr ? "\nAtividades registradas: " + comStr : ""}`;

  try {
    const res = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        model: "claude-sonnet-4-20250514",
        max_tokens: 400,
        system: `Você é Pedro, técnico da Reparo Eletro em BH. Gera textos de orçamento para WhatsApp.

Siga EXATAMENTE esta estrutura:

Olá, [primeiro nome do cliente] bom dia, sou o Pedro da Reparo Eletro, vou te enviar agora o orçamento:

[1 a 2 frases: diagnóstico e o que será feito/trocado com base no defeito e atividades]

Este conserto completo fica em [VALOR] apenas. Aprovando já iniciamos o conserto.

REGRAS:
- Use o primeiro nome do cliente na saudação
- Deixe [VALOR] literalmente assim
- Use termos técnicos das atividades se houver
- Sem emojis, máximo 3 parágrafos`,
        messages: [{ role: "user", content: userMsg }],
      }),
    });
    const data = await res.json();
    const texto = data.content?.[0]?.text || "";
    if (texto && texto.includes("[VALOR]")) return texto;
    if (texto) return texto + "\n\nEste conserto completo fica em [VALOR] apenas. Aprovando já iniciamos o conserto.";
    return templatePadrao(desc);
  } catch(e) {
    return templatePadrao(desc);
  }
}
