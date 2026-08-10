// api/garantia.js — Sistema de Garantia v2
const GARANTIA_KEY  = "reparoeletro_garantia_v2";
const PIPE_ID       = "305832912";
// Fases Pipefy (Reparo Eletro)
const PIPEFY_FASE_SOLICITAR_COLETA  = "334875150"; // fase inicial para delivery
const PIPEFY_FASE_SOLICITAR_ENTREGA = "334875186"; // Solicitar Entrega
const PIPEFY_FASE_FINALIZADO        = "334875153"; // Finalizado

const UPSTASH_URL   = (process.env.UPSTASH_URL   || "").replace(/['"]/g,"").trim();
const UPSTASH_TOKEN = (process.env.UPSTASH_TOKEN || "").replace(/['"]/g,"").trim();

async function dbGet(key) {
  try {
    const r = await fetch(UPSTASH_URL + "/pipeline", {
      method: "POST",
      headers: { Authorization: "Bearer " + UPSTASH_TOKEN, "Content-Type": "application/json" },
      body: JSON.stringify([["GET", key]]),
    });
    const j = await r.json();
    return j[0] && j[0].result ? JSON.parse(j[0].result) : null;
  } catch(e) { return null; }
}
async function dbSet(key, val) {
  try {
    await fetch(UPSTASH_URL + "/pipeline", {
      method: "POST",
      headers: { Authorization: "Bearer " + UPSTASH_TOKEN, "Content-Type": "application/json" },
      body: JSON.stringify([["SET", key, JSON.stringify(val)]]),
    });
    return true;
  } catch(e) { return false; }
}

function uid() { return Date.now().toString(36) + Math.random().toString(36).slice(2,7); }
function pipefyToken() { return (process.env.PIPEFY_TOKEN || "").trim(); }

// ── PIPEFY HELPERS ────────────────────────────────────────────
async function pipefyQuery() {
  // Pipefy desconectado em 01/06/2026 — ADM opera 100% local (Redis)
  return null;
}

// Cria card no Pipefy para delivery
async function criarCardPipefy() { return { ok: false, error: 'Pipefy desconectado' }; }

// Move card Pipefy para uma fase
async function moverCardPipefy(pipefyId, phaseId) {
  const query = `mutation { moveCardToPhase(input: { card_id: "${pipefyId}", destination_phase_id: "${phaseId}" }) { card { id current_phase { name } } } }`;
  return { ok: false, error: 'Pipefy desconectado' };
}

// Fases por tipo
const FASES = {
  loja_imediata: [
    { id: "producao",            label: "Produção" },
    { id: "conserto_concluido",  label: "Conserto Concluído" },
    { id: "equip_retirado",      label: "Equipamento Retirado" },
  ],
  loja_acompanhamento: [
    { id: "producao",            label: "Produção" },
    { id: "conserto_concluido",  label: "Conserto Concluído" },
    { id: "teste_realizado",     label: "Teste Realizado" },
    { id: "equip_retirado",      label: "Equipamento Retirado" },
  ],
  delivery: [
    { id: "coleta_solicitada",   label: "Coleta Solicitada" },
    { id: "producao",            label: "Produção" },
    { id: "conserto_concluido",  label: "Conserto Concluído" },
    { id: "teste_realizado",     label: "Teste Realizado" },
    { id: "solicitar_entrega",   label: "Solicitar Entrega" },
    { id: "entrega_realizada",   label: "Entrega Realizada" },
  ],
  rua: [
    { id: "garantia_solicitada", label: "Garantia Solicitada" },
    { id: "equip_recolhido",     label: "Equipamento Recolhido" },
    { id: "conserto_realizado",  label: "Conserto Realizado" },
  ],
};

function primeiraFase(tipo) { return (FASES[tipo] || [])[0]?.id || "producao"; }
function defaultDB()        { return { fichas: [] }; }
function isConcluida(ficha) {
  const ultimas = { loja_imediata: "equip_retirado", loja_acompanhamento: "equip_retirado", delivery: "entrega_realizada", rua: "conserto_realizado" };
  return ficha.faseId === ultimas[ficha.tipo];
}

module.exports = async function handler(req, res) {
  // 🔐 TF-AUTH (Fase 1): chave obrigatória em toda chamada
  const _tfk = (req.query && req.query.k) || req.headers['x-tf-key'] || '';
  if (_tfk !== ((process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim())) {
    return res.status(401).json({ ok: false, error: 'não autorizado' });
  }

  res.setHeader("Access-Control-Allow-Origin", "https://reparoeletroadm.com");
  res.setHeader("X-Content-Type-Options","nosniff");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(200).end();

  const { action } = req.query;

  try {

    // ── GET load ──────────────────────────────────────────────
      // ═══ 🛡️ FILA DE GARANTIA (visão Conflitos) ═══
  const FILA_KEY = "reparoeletro_garantia_fila";

  // ── ➕ GARANTIR-14: coloca na garantia os equipamentos conferidos na loja ──
  if (action === 'garantir-lista') {
    const LISTA = [
      ['Alexandre','1991'],['Augusto','2582'],['Vera','9757'],['Marcio','2908'],
      ['Vilmar','1427'],['Isabel','0942'],['Elton','4404'],['Lucas','3292'],
      ['Emerson','5978'],['Emília','0611'],['Gilda','7270'],['Daianne','3878'],
      ['Davidson','8937'],['Paulo','8011'],
    ];
    const d4 = t => String(t || '').replace(/\D/g, '').slice(-4);
    const db = (await dbGet(GARANTIA_KEY)) || { fichas: [] };
    const campo = db.fichas ? 'fichas' : (db.cards ? 'cards' : 'fichas');
    db[campo] = db[campo] || [];
    const jaTem = new Set(db[campo].map(f => d4(f.telefone)));

    // procura os dados de cada um nos outros bancos
    const FONTES = ['reparoeletro_pipe', 'reparoeletro_logistica', 'fichas_adm', 'reparoeletro_arquivo'];
    const dados = {};
    for (const k of FONTES) {
      try {
        const b = await dbGet(k);
        for (const L of ['cards', 'fichas']) {
          for (const x of ((b || {})[L] || [])) {
            const c = d4(x.telefone);
            if (!LISTA.some(([, cod]) => cod === c)) continue;
            if (dados[c] && dados[c].equipamento) continue;   // já tem um bom
            dados[c] = { nome: x.nomeContato || x.nome, telefone: x.telefone,
              equipamento: x.equipamento || x.descricao || '', valor: x.valor || 0,
              endereco: x.endereco || '', origemBanco: k };
          }
        }
      } catch (e) {}
    }
    const criados = [], jaEstavam = [], semDados = [];
    for (const [nome, cod] of LISTA) {
      if (jaTem.has(cod)) { jaEstavam.push(nome + ' ' + cod); continue; }
      const d = dados[cod];
      criados.push({
        id: 'gar_' + cod + '_' + Date.now().toString(36),
        nome: d ? (d.nome || nome) : nome,
        cliente: d ? (d.nome || nome) : nome,
        telefone: d ? d.telefone : cod,
        equipamento: d ? d.equipamento : '',
        endereco: d ? d.endereco : '',
        valor: 0,
        status: 'garantia_solicitada',
        phase: 'garantia_solicitada',
        origem: 'conferência física na loja 10/08',
        obs: d ? ('dados recuperados de ' + d.origemBanco) : 'CADASTRAR EQUIPAMENTO — não encontrado no sistema',
        criadoEm: new Date().toISOString(),
      });
      if (!d) semDados.push(nome + ' ' + cod);
    }
    if (String(req.query.aplicar || '') !== '1') {
      return res.status(200).json({ ok: true, modo: 'prévia',
        jaEstavamNaGarantia: jaEstavam.length, jaEstavam,
        vaoSerCriados: criados.length,
        semDadosNoSistema: semDados.length, semDados,
        DETALHE: criados.map(c => c.nome + ' ' + String(c.telefone).slice(-4) +
          ' | ' + (c.equipamento || '⚠️ sem equipamento') + ' | ' + c.obs),
        dica: 'para criar: &aplicar=1' });
    }
    db[campo] = criados.concat(db[campo]);
    await dbSet(GARANTIA_KEY, db);
    return res.status(200).json({ ok: true,
      criados: criados.length, jaEstavam: jaEstavam.length,
      semDadosNoSistema: semDados,
      totalAgora: db[campo].length });
  }

  // ── 🧹 MANTER-APENAS: deixa na garantia só os equipamentos conferidos na loja ──
  if (action === 'manter-apenas') {
    const LISTA = [
      ['Alexandre','1991'],['Augusto','2582'],['Vera','9757'],['Marcio','2908'],
      ['Vilmar','1427'],['Isabel','0942'],['Elton','4404'],['Lucas','3292'],
      ['Emerson','5978'],['Emília','0611'],['Gilda','7270'],['Daianne','3878'],
      ['Davidson','8937'],['Paulo','8011'],
    ];
    const cods = new Set(LISTA.map(x => x[1]));
    const d4 = t => String(t || '').replace(/\D/g, '').slice(-4);
    const db = (await dbGet(GARANTIA_KEY)) || { fichas: [] };
    const lista = db.fichas || db.cards || [];
    const campo = db.fichas ? 'fichas' : 'cards';
    const ficam = [], saem = [];
    // 🎯 casa APENAS pelos 4 dígitos do telefone — casar por nome puxava homônimos
    // (Emerson 5705, Davidson 8927, Marcio 2804 não são os da lista)
    for (const f of lista) {
      (cods.has(d4(f.telefone)) ? ficam : saem).push(f);
    }
    const naLista = LISTA.filter(([, c]) => !ficam.some(f => d4(f.telefone) === c));
    // 🔎 os que não estão na garantia — onde eles estão?
    const ondeEstao = {};
    if (naLista.length) {
      for (const chave of ['reparoeletro_pipe', 'reparoeletro_arquivo', 'reparoeletro_logistica',
        'fichas_adm', 'reparoeletro_garantia_fila']) {
        try {
          const b = await dbGet(chave);
          for (const L of ['cards', 'fichas']) {
            for (const x of ((b || {})[L] || [])) {
              const c4 = d4(x.telefone);
              if (!naLista.some(([, c]) => c === c4)) continue;
              ondeEstao[c4] = ondeEstao[c4] || [];
              ondeEstao[c4].push(chave + ' · ' + String(x.phaseId || x.phase || x.status || '?'));
            }
          }
        } catch (e) {}
      }
    }
    if (String(req.query.aplicar || '') !== '1') {
      return res.status(200).json({ ok: true, modo: 'prévia',
        ONDE_ESTAO_OS_QUE_FALTAM: Object.entries(ondeEstao).map(([c, v]) =>
          c + ': ' + [...new Set(v)].slice(0, 3).join(' | ')),
        totalHoje: lista.length,
        vaoFicar: ficam.length, vaoSair: saem.length,
        naListaMasNaoEncontrados: naLista.map(x => x[0] + ' ' + x[1]),
        FICAM: ficam.map(f => String(f.nome || f.cliente || '?').slice(0, 20) + ' ' + d4(f.telefone) +
          ' | ' + String(f.equipamento || f.descricao || '').slice(0, 20)),
        SAEM: saem.slice(0, 50).map(f => String(f.nome || f.cliente || '?').slice(0, 20) + ' ' + d4(f.telefone)),
        dica: 'para aplicar: &aplicar=1' });
    }
    try { await dbSet('reparoeletro_garantia_lixeira', { em: new Date().toISOString(), fichas: saem }); } catch (e) {}
    db[campo] = ficam;
    await dbSet(GARANTIA_KEY, db);
    return res.status(200).json({ ok: true,
      ficaram: ficam.length, removidos: saem.length,
      naListaMasNaoEncontrados: naLista.map(x => x[0] + ' ' + x[1]),
      backup: 'cópia em reparoeletro_garantia_lixeira' });
  }
  if (action === "fila-load") {
    const fdb = (await dbGet(FILA_KEY)) || { itens: [] };
    const itens = [...(fdb.itens || [])].sort((a, b) => {
      const sa = a.status === "resolvido" ? 1 : 0, sb = b.status === "resolvido" ? 1 : 0;
      return sa - sb || new Date(b.criadoEm) - new Date(a.criadoEm);
    });
    const abertos = itens.filter(i => i.status !== "resolvido").length;
    const hoje = new Date(Date.now() - 3 * 3600 * 1000).toISOString().slice(0, 10);
    const resolvidosHoje = itens.filter(i => i.status === "resolvido" && String(i.resolvidoEm || "").slice(0, 10) === hoje).length;
    return res.status(200).json({ ok: true, itens: itens.slice(0, 200), abertos, resolvidosHoje, totalResolvidos: itens.filter(i => i.status === "resolvido").length });
  }
  if (action === "fila-badge") {
    const fdb = (await dbGet(FILA_KEY)) || { itens: [] };
    return res.status(200).json({ ok: true, abertos: (fdb.itens || []).filter(i => i.status !== "resolvido").length });
  }
  if (req.method === "POST" && action === "fila-criar") {
    const b = req.body || {};
    const fdb = (await dbGet(FILA_KEY)) || { itens: [] };
    const d8n = String(b.telefone || "").replace(/\D/g, "").slice(-8);
    // dedupe: mesmo telefone com item aberto não duplica
    if (d8n && (fdb.itens || []).some(i => i.status !== "resolvido" && String(i.telefone || "").replace(/\D/g, "").slice(-8) === d8n)) {
      return res.status(200).json({ ok: true, dedupe: true });
    }
    fdb.itens.unshift({
      id: "gar_" + Date.now().toString(36) + Math.random().toString(36).slice(2, 5),
      nome: String(b.nome || "Cliente").slice(0, 80),
      telefone: String(b.telefone || "").slice(0, 20),
      equipamento: String(b.equipamento || "").slice(0, 80),
      relato: String(b.relato || b.motivo || "").slice(0, 400),
      origem: String(b.origem || "manual").slice(0, 30),
      status: "aberto", criadoEm: new Date().toISOString(),
    });
    await dbSet(FILA_KEY, fdb);
    return res.status(200).json({ ok: true });
  }
  if (req.method === "POST" && action === "fila-resolver") {
    const { id, destino } = req.body || {};
    if (!["video", "qc"].includes(destino)) return res.status(400).json({ ok: false, error: "destino: video|qc" });
    const fdb = (await dbGet(FILA_KEY)) || { itens: [] };
    const it = (fdb.itens || []).find(x => x.id === id);
    if (!it) return res.status(404).json({ ok: false });
    it.status = "resolvido";
    it.destino = destino;
    it.resolvidoEm = new Date().toISOString();
    await dbSet(FILA_KEY, fdb);
    return res.status(200).json({ ok: true });
  }
  if (req.method === "POST" && action === "fila-reabrir") {
    const fdb = (await dbGet(FILA_KEY)) || { itens: [] };
    const it = (fdb.itens || []).find(x => x.id === (req.body || {}).id);
    if (!it) return res.status(404).json({ ok: false });
    it.status = "aberto"; delete it.destino; delete it.resolvidoEm;
    await dbSet(FILA_KEY, fdb);
    return res.status(200).json({ ok: true });
  }

  // ── 📊 RELATORIO: garantias por técnico, tempo de resolução e volume por período ──
  if (action === "relatorio") {
    const dias = Math.min(365, Math.max(1, parseInt(req.query.dias || "30", 10)));
    const corte = Date.now() - dias * 86400000;
    const db = await dbGet(GARANTIA_KEY) || defaultDB();
    const todas = (db.garantias || []).concat(db.lojaImediata || []);
    const noPeriodo = todas.filter(g => new Date(g.criadaEm || 0).getTime() >= corte);

    const horas = g => {
      if (!g.concluida) return null;
      const ini = new Date(g.criadaEm || 0).getTime();
      const fim = new Date(g.concluidaEm || g.movidaEm || 0).getTime();
      return (ini && fim && fim > ini) ? Number(((fim - ini) / 3600000).toFixed(1)) : null;
    };
    const media = a => a.length ? Number((a.reduce((s, x) => s + x, 0) / a.length).toFixed(1)) : null;

    // por TÉCNICO DE ORIGEM (quem fez o serviço que voltou em garantia)
    const porTec = {};
    for (const g of noPeriodo) {
      const t = (g.tecnicoOrigem || g.tecnico || "(não informado)").trim();
      if (!porTec[t]) porTec[t] = { tecnico: t, total: 0, abertas: 0, concluidas: 0, tempos: [], equipamentos: {} };
      const p = porTec[t];
      p.total++;
      if (g.concluida) { p.concluidas++; const hh = horas(g); if (hh != null) p.tempos.push(hh); }
      else p.abertas++;
      const eq = String(g.equipamento || g.defeito || "").toLowerCase();
      const cat = /micro-?\s?ondas/.test(eq) ? "micro-ondas"
        : (/purificador|bebedouro/.test(eq) ? "purificador"
        : (/adega|cervejeir/.test(eq) ? "adega"
        : (/\btvs?\b|televis/.test(eq) ? "tv"
        : (/forno/.test(eq) ? "forno" : "outros"))));
      p.equipamentos[cat] = (p.equipamentos[cat] || 0) + 1;
    }
    const ranking = Object.values(porTec).map(p => ({
      tecnico: p.tecnico, garantias: p.total, abertas: p.abertas, concluidas: p.concluidas,
      horasMedias: media(p.tempos),
      equipamentos: p.equipamentos,
    })).sort((a, b) => b.garantias - a.garantias);

    // por DIA
    const porDia = {};
    for (const g of noPeriodo) {
      const d = new Date(new Date(g.criadaEm).getTime() - 3 * 3600000).toISOString().slice(0, 10);
      porDia[d] = (porDia[d] || 0) + 1;
    }
    // por TIPO de garantia
    const porTipo = noPeriodo.reduce((o, g) => { const t = g.tipo || "?"; o[t] = (o[t] || 0) + 1; return o; }, {});
    const temposGerais = noPeriodo.map(horas).filter(x => x != null);

    if (String(req.query.mini || "") === "1") {
      return res.status(200).json({ dias,
        entraram: noPeriodo.length,
        abertas: noPeriodo.filter(g => !g.concluida).length,
        concluidas: noPeriodo.filter(g => g.concluida).length,
        horasMedias: media(temposGerais),
        porTipo,
        porTecnico: ranking.reduce((o, r) => { o[r.tecnico] = r.garantias; return o; }, {}) });
    }
    return res.status(200).json({ ok: true, periodoDias: dias,
      RESUMO: {
        entraram: noPeriodo.length,
        abertasAgora: todas.filter(g => !g.concluida).length,
        concluidasNoPeriodo: noPeriodo.filter(g => g.concluida).length,
        horasMediasParaResolver: media(temposGerais),
        porTipo,
      },
      POR_TECNICO: ranking.map(r => r.tecnico + " | " + r.garantias + " garantia(s) | " +
        r.concluidas + " resolvida(s)" + (r.horasMedias != null ? " em " + r.horasMedias + "h em média" : "") +
        (r.abertas ? " | " + r.abertas + " aberta(s)" : "")),
      porDia,
      detalhePorTecnico: ranking,
      abertas: todas.filter(g => !g.concluida).map(g => ({
        nome: g.nome, telefone: String(g.telefone || "").slice(-4),
        equipamento: g.equipamento || g.defeito, tipo: g.tipo,
        tecnicoOrigem: g.tecnicoOrigem || g.tecnico || null,
        diasAberta: Number(((Date.now() - new Date(g.criadaEm || 0).getTime()) / 86400000).toFixed(1)),
      })).sort((a, b) => b.diasAberta - a.diasAberta) });
  }

  if (action === "load") {
      const db = await dbGet(GARANTIA_KEY) || defaultDB();
      return res.status(200).json({ ok: true, fichas: db.fichas || [], fases: FASES });
    }

    // ── POST cadastrar ─────────────────────────────────────────
    if (req.method === "POST" && action === "cadastrar") {
      const { nome, telefone, defeito, endereco, tipo } = req.body || {};
      if (!nome || !telefone || !defeito || !tipo)
        return res.status(400).json({ ok: false, error: "nome, telefone, defeito e tipo são obrigatórios" });
      if (!FASES[tipo])
        return res.status(400).json({ ok: false, error: "tipo inválido: " + tipo });

      const db = await dbGet(GARANTIA_KEY) || defaultDB();
            const tecnico = (req.body.tecnico || "").trim();
      ficha = {
        id:         uid(),
        nome:       nome.trim(),
        telefone:   telefone.trim(),
        defeito:    defeito.trim(),
        endereco:   (endereco || "").trim(),
        tipo,
        tecnico:    tecnico || null,
        faseId:     primeiraFase(tipo),
        criadaEm:   new Date().toISOString(),
        movidaEm:   new Date().toISOString(),
        concluida:  false,
        pipefyId:   null,
        pipefyErro: null,
      };

      // Delivery → cria card no Pipefy imediatamente
      if (tipo === "delivery") {
        // Criar card na coluna Garantia do Pipe ADM
        try {
          const _gU=(process.env.UPSTASH_URL||'').replace(/['"]/g,'').trim();
          const _gT=(process.env.UPSTASH_TOKEN||'').replace(/['"]/g,'').trim();
          async function _gg(k){const r=await fetch(_gU+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+_gT,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});const j=await r.json();const v=j[0]?.result;if(!v)return null;try{let x=JSON.parse(v);if(typeof x==='string')x=JSON.parse(x);return x;}catch(e){return null;}}
          async function _gs(k,v){await fetch(_gU+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+_gT,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});}
          const pdbG=(await _gg('reparoeletro_pipe'))||{cards:[],syncedPipefyIds:[],lastSync:null};
          if(!Array.isArray(pdbG.cards))pdbG.cards=[];
          const nowG=new Date().toISOString();
          pdbG.cards.unshift({
            id:'GARANTIA-'+String(Date.now()),
            phase:'garantia',
            nomeContato:ficha.nome||'',
            telefone:ficha.telefone||'',
            equipamento:ficha.equipamento||'',
            descricao:ficha.defeito||'',
            valor:parseFloat(ficha.valorServico)||0,
            origem:'garantia_delivery',
            garantiaId:ficha.id,
            criadoEm:nowG,movedAt:nowG,
            aguardandoDesde:null,history:[],analiseCompra:false
          });
          pdbG.lastSync=nowG;
          await _gs('reparoeletro_pipe',pdbG);
        } catch(eg){ console.error('[garantia→pipe]',eg.message); }

        const pip = await criarCardPipefy(ficha);
        if (pip.ok) {
          ficha.pipefyId    = pip.pipefyId;
          ficha.pipefyTitle = pip.pipefyTitle;
        } else {
          ficha.pipefyErro = pip.error;
        }

        // Delivery → registrar também na Logística em "Liberado para Coleta"
        try {
          const U2 = process.env.UPSTASH_URL;
          const T2 = process.env.UPSTASH_TOKEN;
          const LOG_KEY = "reparoeletro_logistica";
          const logDb = await fetch(`${U2}/get/${LOG_KEY}`, {
            headers: { Authorization: `Bearer ${T2}` }
          }).then(r=>r.json()).then(j => j.result ? JSON.parse(j.result) : { fichas:[], nextId:1 });
          const logId = "LOG-" + String(logDb.nextId || 1).padStart(4, "0");
          logDb.fichas.unshift({
            id:          logId,
            nome:        ficha.nome,
            telefone:    ficha.telefone || "",
            endereco:    ficha.endereco || "",
            equipamento: "",
            defeito:     ficha.defeito || "",
            pipefyCardId: ficha.pipefyId ? String(ficha.pipefyId) : null,
            texto:       "[Garantia Delivery]",
            phase:       "liberado_coleta",
            origem:      "garantia",
            garantiaId:  ficha.id,
            criadoEm:    new Date().toISOString(),
            movedAt:     new Date().toISOString(),
            diagnostico: null,
          });
          logDb.nextId = (logDb.nextId || 1) + 1;
          await fetch(`${U2}/set/${LOG_KEY}`, {
            method: "POST",
            headers: { Authorization: `Bearer ${T2}`, "Content-Type": "application/json" },
            body: JSON.stringify(logDb)
          });
          console.log("[Garantia] ficha logística criada:", logId);
        } catch(e) { console.error("[Garantia] logística:", e.message); }
      }

      db.fichas.unshift(ficha);
      await dbSet(GARANTIA_KEY, db);
      return res.status(200).json({ ok: true, ficha });
    }

    // ── POST mover ─────────────────────────────────────────────
    if (req.method === "POST" && action === "mover") {
      const { id, faseId } = req.body || {};
      if (!id || !faseId) return res.status(400).json({ ok: false, error: "id e faseId obrigatórios" });

      const db = await dbGet(GARANTIA_KEY) || defaultDB();
      const ficha = db.fichas.find(function(f) { return f.id === id; });
      if (!ficha) return res.status(404).json({ ok: false, error: "Ficha não encontrada" });

      const fases = FASES[ficha.tipo] || [];
      if (!fases.find(function(f) { return f.id === faseId; }))
        return res.status(400).json({ ok: false, error: "Fase inválida para este tipo" });

      ficha.faseId   = faseId;
      ficha.movidaEm = new Date().toISOString();
      // Ao mover via Técnico, auto-conclui (sai da coluna)
      ficha.concluida = false;

      // Delivery + solicitar_entrega → move card Pipefy para Solicitar Entrega
      let pipefyResult = null;
      if (ficha.tipo === "delivery" && faseId === "solicitar_entrega" && ficha.pipefyId) {
        pipefyResult = await moverCardPipefy(ficha.pipefyId, PIPEFY_FASE_SOLICITAR_ENTREGA);
        if (!pipefyResult.ok) ficha.pipefyErro = pipefyResult.error;
      }

      await dbSet(GARANTIA_KEY, db);
      return res.status(200).json({ ok: true, ficha, pipefy: pipefyResult });
    }

    // ── POST concluir ──────────────────────────────────────────
    if (req.method === "POST" && action === "concluir") {
      const { id } = req.body || {};
      const db = await dbGet(GARANTIA_KEY) || defaultDB();
      const ficha = db.fichas.find(function(f) { return f.id === id; });
      if (!ficha) return res.status(404).json({ ok: false, error: "Ficha não encontrada" });
      const ultimas = { loja_imediata: "equip_retirado", loja_acompanhamento: "equip_retirado", delivery: "entrega_realizada", rua: "conserto_realizado" };
      ficha.faseId      = ultimas[ficha.tipo] || ficha.faseId;
      ficha.concluida   = true;
      ficha.concluidaEm = new Date().toISOString();
      ficha.concluida = true;
      ficha.concluidaEm = new Date().toISOString();
      ficha.concluidaMotivo = "movida_tecnico";
      await dbSet(GARANTIA_KEY, db);
      return res.status(200).json({ ok: true, ficha });
    }

    // ── POST reabrir ───────────────────────────────────────────
    if (req.method === "POST" && action === "reabrir") {
      const { id } = req.body || {};
      const db = await dbGet(GARANTIA_KEY) || defaultDB();
      const ficha = db.fichas.find(function(f) { return f.id === id; });
      if (!ficha) return res.status(404).json({ ok: false, error: "Ficha não encontrada" });
      ficha.concluida   = false;
      ficha.concluidaEm = null;
      ficha.faseId      = primeiraFase(ficha.tipo);
      ficha.movidaEm    = new Date().toISOString();
      await dbSet(GARANTIA_KEY, db);
      return res.status(200).json({ ok: true, ficha });
    }

    // ── POST marcar-wpp: cliente comunicado via WhatsApp ─────────
    if (action === 'marcar-wpp') {
      const { id } = req.body || {};
      if (!id) return res.status(400).json({ ok:false, error:'id obrigatório' });
      const db = (await dbGet(GARANTIA_KEY)) || defaultDB();
      const ficha = (db.fichas||[]).find(f => f.id === id);
      if (!ficha) return res.status(404).json({ ok:false, error:'Ficha não encontrada' });
      ficha.wppComunicado   = true;
      ficha.wppComunicadoEm = new Date().toISOString();
      await dbSet(GARANTIA_KEY, db);
      return res.status(200).json({ ok:true });
    }

    // ── POST excluir ───────────────────────────────────────────
    if (req.method === "POST" && action === "excluir") {
      const { id } = req.body || {};
      const db = await dbGet(GARANTIA_KEY) || defaultDB();
      db.fichas = db.fichas.filter(function(f) { return f.id !== id; });
      await dbSet(GARANTIA_KEY, db);
      return res.status(200).json({ ok: true });
    }

    // ── GET pipefy-sync ────────────────────────────────────────
    // Verifica fichas delivery que estão no Pipefy como Finalizado
    // e move para entrega_realizada no nosso sistema
    if (action === "pipefy-sync") {
      const db = await dbGet(GARANTIA_KEY) || defaultDB();
      // Busca fichas delivery com pipefyId que ainda não foram concluídas
      const pendentes = db.fichas.filter(function(f) {
        return f.tipo === "delivery" && f.pipefyId && !f.concluida;
      });
      if (!pendentes.length) return res.status(200).json({ ok: true, sincronizados: 0 });

      // Busca o card de cada uma no Pipefy para ver a fase atual
      const ids = pendentes.map(function(f) { return f.pipefyId; });
      const cardQueries = ids.map(function(cid) {
        return '  c' + cid + ': card(id: "' + cid + '") { id current_phase { id name } }';
      }).join("\n");
      const query = "query {\n" + cardQueries + "\n}";
      const r = await pipefyQuery(query);

      let sincronizados = 0;
      if (r.data) {
        pendentes.forEach(function(ficha) {
          const cardData = r.data["c" + ficha.pipefyId];
          if (cardData && cardData.current_phase && cardData.current_phase.id === PIPEFY_FASE_FINALIZADO) {
            ficha.faseId      = "entrega_realizada";
            ficha.concluida   = true;
            ficha.concluidaEm = new Date().toISOString();
            sincronizados++;
          }
        });
        if (sincronizados > 0) await dbSet(GARANTIA_KEY, db);
      }

      return res.status(200).json({ ok: true, sincronizados, pipefyErro: r.error || null });
    }

    if (action === "tecnico-load") {
    const db = await dbGet(GARANTIA_KEY) || defaultDB();
    const all = db.fichas || [];
    return res.status(200).json({ ok: true,
      garantias:    all.filter(f => (f.tipo === "loja_acompanhamento" || f.tipo === "delivery") && !f.concluida),
      lojaImediata: all.filter(f => f.tipo === "loja_imediata" && !f.concluida)
    });
  }
  if (action === "relatorio-tecnico") {
    const db = await dbGet(GARANTIA_KEY) || defaultDB();
    const all = db.fichas || [];
    const agora = new Date();
    const hist = [];
    for (let m = 0; m < 6; m++) {
      const d   = new Date(agora.getFullYear(), agora.getMonth() - m, 1);
      const ym  = d.getFullYear() + "-" + String(d.getMonth()+1).padStart(2,"0");
      const lbl = d.toLocaleDateString("pt-BR", { month:"short", year:"2-digit" });
      const fichasM = all.filter(f => f.criadaEm && f.criadaEm.slice(0,7) === ym);
      const porTec  = {};
      fichasM.forEach(f => { const t = f.tecnico || "N/D"; porTec[t] = (porTec[t]||0)+1; });
      hist.push({ ym, label: lbl, total: fichasM.length, porTecnico: porTec });
    }
    return res.status(200).json({ ok: true, mesAtual: hist[0]||{label:"",total:0,porTecnico:{}}, historico: hist });
  }
      // ── compare-pipefy — compara fichas ativas com fase atual no Pipefy ──
  if (action === "compare-pipefy") {
    const db   = await dbGet(GARANTIA_KEY) || defaultDB();
    const all  = db.fichas || [];
    const ativas = all.filter(f => !f.concluida);
    const comPipefy  = ativas.filter(f => f.pipefyId);
    const semPipefy  = ativas.filter(f => !f.pipefyId);

    // Query em lote — todos os pipefyIds de uma vez
    let pipefyFases = {};
    if (comPipefy.length > 0) {
      try {
        const parts = comPipefy.map(f =>
          'c' + f.pipefyId + ': card(id: "' + f.pipefyId + '") { id title current_phase { id name } }'
        ).join("\n");
        // Fetch direto — cards do pipe TV não são acessíveis via pipefyQuery do pipe garantia
        const _tok = pipefyToken();
        const _resp = await fetch(PIPEFY_API, {
          method: "POST",
          headers: { "Content-Type": "application/json", Authorization: "Bearer " + _tok },
          body: JSON.stringify({ query: "query {\n" + parts + "\n}" })
        });
        const _json = await _resp.json();
        const data = _json.data || {};
        for (const ficha of comPipefy) {
          const key  = "c" + ficha.pipefyId;
          const card = data && data[key];
          pipefyFases[ficha.pipefyId] = card
            ? { fase: card.current_phase ? card.current_phase.name : "?", faseId: card.current_phase ? card.current_phase.id : "?" }
            : { fase: "Não encontrado no Pipefy", faseId: null };
        }
      } catch(e) {
        for (const f of comPipefy) pipefyFases[f.pipefyId] = { fase: "Erro: " + e.message, faseId: null };
      }
    }

    const result = ativas.map(f => ({
      nome:       f.nome,
      tipo:       f.tipo,
      faseLocal:  f.faseId,
      pipefyId:   f.pipefyId || null,
      pipefyFase: f.pipefyId ? (pipefyFases[f.pipefyId] || {}).fase : "Sem ID Pipefy",
      pipefyFaseId: f.pipefyId ? (pipefyFases[f.pipefyId] || {}).faseId : null,
      dias:       Math.floor((Date.now() - new Date(f.movidaEm)) / 86400000),
    }));

    return res.status(200).json({ ok: true, total: result.length, fichas: result });
  }

    // ── force-remove-finalizados ─────────────────────────────────
  if (action === "force-remove-finalizados") {
    const db = await dbGet(GARANTIA_KEY) || defaultDB();
    // pipefyIds confirmados como Finalizado no Pipefy
    const PIPEFY_FINALIZADOS = new Set([
      "1341050397","1340515029","1339647098","1339437751",
      "1338487077","1338477821","1338141543","1337253696",
      "1336559831","1336463675","1336246266","1335982509",
      "1335887472","1335874019","1335392718","1335154250"
    ]);
    // Fases terminais locais
    const FASES_T = ["entrega_realizada","equip_retirado","equip_recolhido","conserto_realizado","servico_finalizado"];
    const removidas = [];
    for (const f of db.fichas || []) {
      if (f.concluida) continue;
      const byPipefy = f.pipefyId && PIPEFY_FINALIZADOS.has(f.pipefyId);
      const byFase   = FASES_T.includes(f.faseId);
      if (byPipefy || byFase) {
        f.concluida = true;
        f.concluidaEm = new Date().toISOString();
        f.concluidaMotivo = byPipefy ? "pipefy_finalizado_force" : "fase_terminal";
        removidas.push({ nome: f.nome, pipefyId: f.pipefyId||null, motivo: f.concluidaMotivo });
      }
    }
    if (removidas.length > 0) await dbSet(GARANTIA_KEY, db);
    return res.status(200).json({ ok: true, total: removidas.length, removidas });
  }

    if (action === "limpar-coluna") {
    const db = await dbGet(GARANTIA_KEY) || defaultDB();
    let count = 0;
    for (const f of db.fichas || []) {
      if (!f.concluida) { f.concluida=true; f.concluidaEm=new Date().toISOString(); f.concluidaMotivo="reset_manual"; count++; }
    }
    if (count > 0) await dbSet(GARANTIA_KEY, db);
    return res.status(200).json({ ok: true, removidas: count });
  }

  return res.status(404).json({ ok: false, error: "Ação não encontrada" });

  } catch(e) {
    return res.status(200).json({ ok: false, error: "Erro interno: " + e.message });
  }
};
