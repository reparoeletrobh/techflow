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

  return res.status(404).json({ ok: false, error: "Ação não encontrada" });
};
