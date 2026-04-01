const PIPEFY_API = "https://api.pipefy.com/graphql";
const PIPE_ID    = "306904889";
const ORC_KEY    = "tv_orcamentos";

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
  // Formata telefone para campo phone do Pipefy: (xx)9 xxxx-xxxx
  // Campo type=phone do Pipefy aceita formato livre mas precisa ser consistente
  function formatarTelefone(tel) {
    const digits = tel.replace(/\D/g, "");
    // Remove prefixo 55 (Brasil) se presente com 12 ou 13 dígitos
    var d = (digits.length === 13 && digits.startsWith("55")) ? digits.slice(2)
          : (digits.length === 12 && digits.startsWith("55")) ? digits.slice(2)
          : digits;
    // Celular com 10 dígitos (sem o 9): adiciona o 9 após o DDD
    if (d.length === 10) {
      d = d.slice(0,2) + "9" + d.slice(2);
    }
    // Celular com 9 dígitos (sem DDD): adiciona o 9 na frente
    if (d.length === 9 && d[0] !== "9") {
      d = "9" + d;
    }
    // Agora formata: 11 dígitos = (DD)9 XXXX-XXXX
    if (d.length === 11) {
      return "(" + d.slice(0,2) + ")" + d[2] + " " + d.slice(3,7) + "-" + d.slice(7);
    }
    return tel;
  }
  const telefoneFmt = formatarTelefone(telefone);
  console.log("telefone original:", telefone, "formatado:", telefoneFmt);

  if (nomeField) fieldsAttr.push(`{ field_id: "${nomeField.id}", field_value: ${JSON.stringify(nomeContato)} }`);
  if (telField)  fieldsAttr.push(`{ field_id: "${telField.id}",  field_value: ${JSON.stringify(telefoneFmt)} }`);
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
      // Marca como inicializado (sem pular cards)
      if (!db.initialized) {
        db.initialized = true;
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
          // TV: detecta polegadas e peça para gerar orçamento com tabela de precos TV
          let textoOrc = "", precoSugerido = null;
          try {
            const pol   = detectarPolegadas(descEquip, comentariosEquip);
            const peca  = detectarPecaTV(comentariosEquip) || "conjunto eletronico";
            const preco = pol ? getPrecoPorPolegadas(pol) : null;
            const primeiro = primeiroNome(card.nome) || "cliente";
            const saud = "Ola, " + primeiro + " bom dia, aqui e o Pedro da TV Assistencia.";
            const diag = "\n\nForam feitos todos os testes e identificamos que sera necessario refazer a parte eletrica que causou danos no conjunto da " + peca + ". As pecas serao trocadas tambem e sera feito a reoperacao eletrica.";
            const valor = preco
              ? " Este conserto completo fica em " + preco + " reais apenas. Aprovando ja iniciamos o servico."
              : " Este conserto completo fica em [VALOR] apenas. Aprovando ja iniciamos o servico.";
            textoOrc = saud + diag + valor;
            if (preco) precoSugerido = preco.replace(".","").replace(",",".");
          } catch(e) {
            textoOrc = templatePadrao(descEquip, card.nome);
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

  // ── GET sem-resposta — cards em Aguardando Aprovação há mais de 48h
  if (action === "sem-resposta") {
    try {
      const data = await pipefyQuery(`query {
        phase(id: "${AGUARDANDO_APROVACAO_PHASE_ID}") {
          cards(first: 50) {
            edges {
              node {
                id title
                fields { name value }
                phases_history { phase { id } firstTimeIn }
              }
            }
          }
        }
      }`);
      const agora = Date.now();
      const QUARENTA_OITO_H_MS = 48 * 60 * 60 * 1000;
      const cards = (data?.phase?.cards?.edges || []).map(({node}) => {
        const fields = node.fields || [];
        const nome = fields.find(f=>f.name.toLowerCase().includes("nome"))?.value || node.title;
        const tel  = fields.find(f=>f.name.toLowerCase().includes("telefone")||f.name.toLowerCase().includes("fone"))?.value || "";
        const desc = fields.find(f=>f.name.toLowerCase().includes("descri"))?.value || "";
        // Usa phases_history para pegar exatamente quando entrou em Aguardando Aprovação
        const hist = (node.phases_history || []).find(h => h.phase?.id === String(AGUARDANDO_APROVACAO_PHASE_ID));
        const entradaFaseMs = hist?.firstTimeIn ? new Date(hist.firstTimeIn).getTime() : 0;
        const diffMs = entradaFaseMs ? (agora - entradaFaseMs) : 0;
        const ageDias = Math.floor(diffMs / (1000 * 60 * 60 * 24));
        return { pipefyId: String(node.id), title: node.title, nome, tel, desc, age: ageDias };
      }).filter(c => c.age >= 2); // 2+ dias na fase Aguardando Aprovação
      return res.status(200).json({ ok: true, cards });
    } catch(e) {
      return res.status(200).json({ ok: false, error: e.message });
    }
  }

  // ── GET ultima-chamada — espelho da fase Ultima Chamada do Pipefy
  if (action === "ultima-chamada") {
    try {
      const phaseId = "341638208"; // TV: Ultima Chamada
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
const AGUARDANDO_APROVACAO_PHASE_ID = "341638194"; // TV: Aguardando Aprovacao (Enviados)
const AGUARDANDO_ORCAMENTO_PHASE_ID  = "341638197"; // TV: Aguardando Orcamento (pendentes/novos)

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
  // Busca das 2 fases: Aguardando Orçamento E Aguardando Aprovação
  const all = [];
  for (const phaseId of [AGUARDANDO_ORCAMENTO_PHASE_ID, AGUARDANDO_APROVACAO_PHASE_ID]) {
  let cursor = null, hasNext = true;
  while (hasNext) {
    const after = cursor ? `, after: "${cursor}"` : "";
    const data = await pipefyQuery(`query {
      phase(id: "${phaseId}") {
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
  } // end for phaseId
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
  return saud + ", aqui e o Pedro da TV Assistencia.\n\nForam feitos todos os testes e identificamos que sera necessario refazer a parte eletrica que causou danos no conjunto da [peca]. As pecas serao trocadas tambem e sera feito a reoperacao eletrica. Este conserto completo fica em [VALOR] apenas. Aprovando ja iniciamos o servico.";
}

// Tabela de precos TV por polegadas
var PRECOS_TV = [
  { min: 30, max: 39, preco: "490,00" },
  { min: 40, max: 49, preco: "690,00" },
  { min: 50, max: 59, preco: "890,00" },
  { min: 60, max: 69, preco: "1.490,00" },
  { min: 70, max: 79, preco: "1.990,00" },
];

function detectarPolegadas(desc, comentarios) {
  var texto = (desc || "") + " " + (comentarios || []).join(" ");
  // Busca padroes como "55 pol", "55\"", "55 polegadas", "55pol"
  var m = texto.match(/(\d{2})\s*(?:pol(?:egadas?)?|\"|'')/i);
  if (m) return parseInt(m[1]);
  // Busca numeros isolados entre 30 e 79
  var nums = texto.match(/\b([3-7]\d)\b/g);
  if (nums) {
    for (var n of nums) {
      var v = parseInt(n);
      if (v >= 30 && v <= 79) return v;
    }
  }
  return null;
}

function detectarPecaTV(comentarios) {
  var texto = (comentarios || []).join(" ").toLowerCase();
  if (texto.includes("barramento"))     return "barramento";
  if (texto.includes("placa t-con") || texto.includes("t-com") || texto.includes("tcon")) return "placa T-CON";
  if (texto.includes("placa main") || texto.includes("placa principal")) return "placa principal";
  if (texto.includes("placa"))          return "placa";
  if (texto.includes("flat"))           return "flat cable";
  if (texto.includes("memória") || texto.includes("memoria")) return "memoria";
  if (texto.includes("solda"))          return "solda";
  if (texto.includes("fonte"))          return "fonte";
  if (texto.includes("backlight") || texto.includes("led")) return "backlight LED";
  return null;
}

function getPrecoPorPolegadas(pol) {
  if (!pol) return null;
  for (var faixa of PRECOS_TV) {
    if (pol >= faixa.min && pol <= faixa.max) return faixa.preco;
  }
  return null;
}

async function gerarTextoOrcamento(desc, comentarios, nome) {
  var primeiro = primeiroNome(nome) || "cliente";
  var pol      = detectarPolegadas(desc, comentarios);
  var peca     = detectarPecaTV(comentarios) || "conjunto eletronico";
  var preco    = pol ? getPrecoPorPolegadas(pol) : null;

  // Monta texto com o modelo padrao TV
  var saud  = "Ola, " + primeiro + " bom dia, aqui e o Pedro da TV Assistencia.";
  var diag  = "\n\nForam feitos todos os testes e identificamos que sera necessario refazer a parte eletrica que causou danos no conjunto da " + peca + ". As pecas serao trocadas tambem e sera feito a reoperacao eletrica.";
  var valor = preco
    ? " Este conserto completo fica em " + preco + " reais apenas. Aprovando ja iniciamos o servico."
    : " Este conserto completo fica em [VALOR] apenas. Aprovando ja iniciamos o servico.";

  var texto = saud + diag + valor;

  // Se nao detectou polegadas, tenta com Claude para extrair
  if (!pol) {
    try {
      var comStr  = (comentarios || []).join("; ");
      var userMsg = "Ficha TV:\nDefeito: " + (desc||"nao informado") + (comStr ? "\nInfos: "+comStr : "");
      var sysMsg  = "Voce e Pedro da TV Assistencia. Gere orcamento de TV seguindo EXATAMENTE este modelo:\n\nOla, NOME bom dia, aqui e o Pedro da TV Assistencia.\n\nForam feitos todos os testes e identificamos que sera necessario refazer a parte eletrica que causou danos no conjunto da [PECA]. As pecas serao trocadas tambem e sera feito a reoperacao eletrica. Este conserto completo fica em [VALOR] apenas. Aprovando ja iniciamos o servico.\n\nSubstitua NOME pelo primeiro nome real. Mantenha [VALOR] literal. [PECA] = peca mais provavel (barramento, placa, placa T-CON, flat, memoria, solda, fonte). Responda so com o texto do orcamento.";
      var res = await fetch("https://api.anthropic.com/v1/messages", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ model: "claude-sonnet-4-20250514", max_tokens: 300, system: sysMsg, messages: [{ role: "user", content: userMsg }] }),
      });
      var data  = await res.json();
      var textoAI = (data.content && data.content[0] && data.content[0].text) || "";
      if (textoAI && textoAI.length > 30) return { texto: textoAI, preco: null };
    } catch(e) {
      console.error("gerarTextoOrcamento TV:", e.message);
    }
  }

  return { texto, preco: preco ? preco.replace(".","").replace(",",".") : null };
}

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
