
// ── Helper: gravar no log central ────────────────────────────────────────
async function logAction(entry) {
  try {
    const _U=(process.env.UPSTASH_URL||'').replace(/['"]/g,'').trim();
    const _T=(process.env.UPSTASH_TOKEN||'').replace(/['"]/g,'').trim();
    const _K='reparoeletro_log';
    const _r=await fetch(_U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+_T,'Content-Type':'application/json'},body:JSON.stringify([['GET',_K]])});
    const _j=await _r.json();const _v=_j[0]?.result;
    let _log=[];if(_v){try{_log=JSON.parse(_v);if(typeof _log==='string')_log=JSON.parse(_log);}catch(e){}}if(!Array.isArray(_log))_log=[];
    _log.unshift({ts:new Date().toISOString(),modulo:entry.modulo||'—',fichaId:entry.fichaId||'',ficha:entry.ficha||'',acao:entry.acao||'',de:entry.de||'',para:entry.para||'',gatilho:entry.gatilho||'',status:entry.status||'ok',detalhe:entry.detalhe||''});
    if(_log.length>500)_log.splice(500);
    await fetch(_U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+_T,'Content-Type':'application/json'},body:JSON.stringify([['SET',_K,JSON.stringify(_log)]])});
  }catch(e){}
}

const UPSTASH_URL   = (process.env.UPSTASH_URL   || "").replace(/['"]/g, "").trim();
const UPSTASH_TOKEN = (process.env.UPSTASH_TOKEN || "").replace(/['"]/g, "").trim();
const VENDAS_KEY    = "reparoeletro_vendas";
const FIN_KEY       = "reparoeletro_financeiro";
const PIPE_ID       = "305832912";

async function pipefyQuery() {
  // Pipefy desconectado em 01/06/2026 — ADM opera 100% local (Redis)
  return null;
}

async function getProntoParaVendaPhaseId() {
  const data = await pipefyQuery(
    "query { pipe(id: \"" + PIPE_ID + "\") { phases { id name } } }"
  );
  const phases = data?.pipe?.phases || [];
  const phase = phases.find(p => p.name.toLowerCase().includes("pronto para venda"));
  return phase?.id || null;
}

async function getReceberPhaseId() {
  const data = await pipefyQuery(
    "query { pipe(id: \"" + PIPE_ID + "\") { phases { id name } } }"
  );
  const phases = data?.pipe?.phases || [];
  const phase = phases.find(p => p.name.toLowerCase().includes("receber"));
  return phase?.id || null;
}

async function criarCardPipefy() { return null; }

async function dbGet(key) {
  try {
    const r = await fetch(`${UPSTASH_URL}/pipeline`, {
      method: "POST",
      headers: { Authorization: `Bearer ${UPSTASH_TOKEN}`, "Content-Type": "application/json" },
      body: JSON.stringify([["GET", key]]),
    });
    const j = await r.json();
    return j[0]?.result ? JSON.parse(j[0].result) : null;
  } catch (e) { return null; }
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
  } catch (e) { return false; }
}

function defaultDB()  { return { produtos: [] }; }
function defaultFin() { return { records: [], syncedIds: [] }; }


// ── Pipefy é ESPELHO — nunca bloqueia o fluxo local ─────────────────────
async function pipefyBestEffort(fn) {
  try { return await fn(); } catch(e) { console.warn('[Pipefy]', e.message); return null; }
}

module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(200).end();

  const { action } = req.query;

  // ── GET load ───────────────────────────────────────────────
  if (action === "load") {
    const db = await dbGet(VENDAS_KEY) || defaultDB();
    // Para produtos vendidos, não retorna fotos (economiza payload)
    const produtos = (db.produtos || []).map(p =>
      p.vendido ? { ...p, fotos: [] } : p
    );
    res.setHeader('Cache-Control', 'no-store');
    return res.status(200).json({ ok: true, produtos });
  }

  // ── GET stats (para metas) ─────────────────────────────────
  if (action === "stats") {
    const db = await dbGet(VENDAS_KEY) || defaultDB();
    const produtos = db.produtos || [];

    function toBRT(d) { return new Date(new Date(d).toLocaleString("en-US", { timeZone: "America/Sao_Paulo" })); }
    const nowBRT = toBRT(new Date()); nowBRT.setHours(0,0,0,0);
    const todayUTC = new Date(nowBRT.getTime() + 3*60*60*1000);
    const weekBRT = toBRT(new Date()); const wd = weekBRT.getDay();
    weekBRT.setDate(weekBRT.getDate() + (wd===0?-6:1-wd)); weekBRT.setHours(0,0,0,0);
    const weekUTC = new Date(weekBRT.getTime() + 3*60*60*1000);

    const cadastradosHoje   = produtos.filter(p => p.createdAt && new Date(p.createdAt) >= todayUTC).length;
    const cadastradosSemana = produtos.filter(p => p.createdAt && new Date(p.createdAt) >= weekUTC).length;

    const vendidosHoje      = produtos.filter(p => p.soldAt && new Date(p.soldAt) >= todayUTC).length;
    const vendidosSemana    = produtos.filter(p => p.soldAt && new Date(p.soldAt) >= weekUTC).length;

    const vendaLojaHoje     = produtos.filter(p => p.soldAt && new Date(p.soldAt) >= todayUTC   && p.vendedor === "Loja").length;
    const vendaLojaSemana   = produtos.filter(p => p.soldAt && new Date(p.soldAt) >= weekUTC    && p.vendedor === "Loja").length;
    const vendaOnlineHoje   = produtos.filter(p => p.soldAt && new Date(p.soldAt) >= todayUTC   && p.vendedor === "Online").length;
    const vendaOnlineSemana = produtos.filter(p => p.soldAt && new Date(p.soldAt) >= weekUTC    && p.vendedor === "Online").length;

    return res.status(200).json({
      ok: true,
      cadastradosHoje, cadastradosSemana,
      vendidosHoje, vendidosSemana,
      vendaLojaHoje, vendaLojaSemana,
      vendaOnlineHoje, vendaOnlineSemana,
    });
  }

  // ── POST salvar (criar ou editar) ──────────────────────────
  if (req.method === "POST" && action === "salvar") {
    const { id, codigo, tipo, descricao, capacidade, preco, fotos } = req.body || {};
    if (!codigo || !descricao || preco === undefined)
      return res.status(400).json({ ok: false, error: "codigo, descricao e preco são obrigatórios" });

    const db = await dbGet(VENDAS_KEY) || defaultDB();
    // Limita a 3 fotos por produto e verifica tamanho total
    const fotosRaw = Array.isArray(fotos) ? fotos.filter(Boolean).slice(0, 3) : [];
    const fotosArr = fotosRaw;

    if (id) {
      const idx = db.produtos.findIndex(p => p.id === id);
      if (idx < 0) return res.status(404).json({ ok: false, error: "Produto não encontrado" });
      db.produtos[idx] = { ...db.produtos[idx], codigo, tipo: tipo||null, descricao, capacidade: capacidade||null, preco, fotos: fotosArr, updatedAt: new Date().toISOString() };
      await dbSet(VENDAS_KEY, db);
      return res.status(200).json({ ok: true, produto: db.produtos[idx] });
    } else {
      const produto = {
        id: Date.now().toString(), codigo, tipo: tipo||null, descricao,
        capacidade: capacidade||null, preco, fotos: fotosArr,
        vendido: false, soldAt: null,
        createdAt: new Date().toISOString(), updatedAt: new Date().toISOString(),
      };
      db.produtos.unshift(produto);
      // Verifica tamanho antes de salvar
      const payload = JSON.stringify(db);
      if(payload.length > 900000) {
        // Remove fotos de produtos vendidos para liberar espaço
        db.produtos = db.produtos.map(p => p.vendido ? {...p, fotos:[]} : p);
      }
      try {
        await dbSet(VENDAS_KEY, db);
      } catch(e) {
        return res.status(500).json({ ok: false, error: "Erro ao salvar: banco de dados cheio. Contate o suporte." });
      }
      return res.status(200).json({ ok: true, produto });
    }
  }

  // ── POST vender ────────────────────────────────────────────
  // Cria ficha no financeiro em "emitir_nf" e marca produto como vendido
  if (req.method === "POST" && action === "vender") {
    const { produtoId, nomeCliente, telefone, cpfCnpj, vendedor, modalidade } = req.body || {};
    if (!produtoId || !nomeCliente)
      return res.status(400).json({ ok: false, error: "produtoId e nomeCliente obrigatórios" });

    const [db, fin] = await Promise.all([
      dbGet(VENDAS_KEY) || defaultDB(),
      dbGet(FIN_KEY)    || defaultFin(),
    ]);

    const idx = db.produtos.findIndex(p => p.id === produtoId);
    if (idx < 0) return res.status(404).json({ ok: false, error: "Produto não encontrado" });
    const p = db.produtos[idx];

    const now = new Date().toISOString();
    const precoFmt = parseFloat(p.preco).toLocaleString("pt-BR",{minimumFractionDigits:2,style:"currency",currency:"BRL"});
    const dataVenda = new Date().toLocaleDateString("pt-BR",{timeZone:"America/Sao_Paulo"});

    // ── Ficha no Financeiro (emitir NF) ──────────────────────────────────
    const fichaId = `venda-${Date.now()}`;
    const ficha = {
      id:          fichaId,
      pipefyId:    fichaId,
      osCode:      p.codigo,
      nomeContato: nomeCliente,
      telefone:    telefone || null,
      cpfCnpj:     cpfCnpj  || null,
      title:       `${p.tipo ? p.tipo + " — " : ""}${p.descricao.substring(0,60)}`,
      descricao:   `${p.descricao}${p.capacidade ? " — " + p.capacidade : ""} | Valor: ${precoFmt}`,
      equipamento: p.tipo || null,
      preco:       p.preco,
      origem:      "venda_equipamento",
      phaseId:     "emitir_nf",
      createdAt:   now, movedAt: now,
      history:     [{ phaseId: "emitir_nf", ts: now }],
    };
    if (!Array.isArray(fin.records))   fin.records   = [];
    if (!Array.isArray(fin.syncedIds)) fin.syncedIds = [];
    fin.records.unshift(ficha);

    // ── Marca produto como vendido ────────────────────────────────────────
    db.produtos[idx] = { ...p, vendido: true, soldAt: now, compradorNome: nomeCliente,
      compradorTel: telefone||null,
      nomeVendedor: vendedor||null, modalidade: modalidade||null,
      vendedor: modalidade||vendedor||null,
      updatedAt: now };

    await Promise.all([dbSet(VENDAS_KEY, db), dbSet(FIN_KEY, fin)]);

    // ── Texto WhatsApp para almoxarifado ──────────────────────────────────
    const textoAlmox = [
      "📦 *SEPARAÇÃO — ALMOXARIFADO*",
      "",
      `📅 Data: ${dataVenda}`,
      `🏷️ Código: ${p.codigo || "—"}`,
      `📦 Tipo: ${p.tipo || "—"}`,
      `📝 Descrição: ${p.descricao}`,
      `💰 Valor: ${precoFmt}`,
      "",
      `👤 Comprador: ${nomeCliente}`,
      telefone ? `📱 Telefone: ${telefone}` : null,
      "",
      `🛒 Vendedor: ${vendedor || "—"}`,
      `🔖 Modalidade: ${modalidade || "—"}`,
    ].filter(l => l !== null).join("\n");

    // ── Pipefy: card em Receber (almoxarifado) ────────────────────────────
    let pipefyReceberCardId = null;
    try {
      const phaseReceber = await getReceberPhaseId();
      if (phaseReceber) {
        const tituloReceber = `VENDA — ${p.codigo || p.tipo || "Equipamento"} | ${nomeCliente}`;
        const descReceber   = [
          p.descricao,
          `Valor: ${precoFmt}`,
          `Vendedor: ${vendedor||"—"}`,
          `Modalidade: ${modalidade||"—"}`,
          "",
          textoAlmox
        ].join("\n");
        const safeTitle = tituloReceber.replace(/"/g,"'").slice(0,255);
        const safeName  = nomeCliente.replace(/"/g,"'").slice(0,255);
        const safeTel   = (telefone||"").replace(/"/g,"'").slice(0,100);
        const safeDesc  = descReceber.replace(/"/g,"'").slice(0,3000);
        let dataAlmox = null;
        try {
          dataAlmox = await pipefyQuery(
            "mutation { createCard(input: { pipe_id: \"" + PIPE_ID + "\" phase_id: \"" + phaseReceber + "\" title: \"" + safeTitle + "\" fields_attributes: [ { field_id: \"nome_do_contato\" field_value: \"" + safeName + "\" }, { field_id: \"telefone\" field_value: \"" + safeTel + "\" }, { field_id: \"descri_o\" field_value: \"" + safeDesc + "\" } ] }) { card { id } } }"
          );
        } catch(eF) {
          dataAlmox = await pipefyQuery(
            "mutation { createCard(input: { pipe_id: \"" + PIPE_ID + "\" phase_id: \"" + phaseReceber + "\" title: \"" + safeTitle + "\" }) { card { id } } }"
          );
        }
        pipefyReceberCardId = dataAlmox?.createCard?.card?.id || null;
      }
    } catch(e) { console.error("Pipefy Receber card:", e.message); }

    // ── Pipe ADM: criar card em Receber ────────────────────────────────────
    try {
      const UV2=(process.env.UPSTASH_URL||'').replace(/['"]/g,'').trim();
      const TV2=(process.env.UPSTASH_TOKEN||'').replace(/['"]/g,'').trim();
      async function _vg2(k){const r=await fetch(UV2+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+TV2,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});const j=await r.json();const v=j[0]?.result;if(!v)return null;try{let x=JSON.parse(v);if(typeof x==='string')x=JSON.parse(x);return x;}catch(e){return null;}}
      async function _vs2(k,v){await fetch(UV2+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+TV2,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});}
      const pdb2=(await _vg2('reparoeletro_pipe'))||{cards:[],syncedPipefyIds:[],lastSync:null};
      if(!Array.isArray(pdb2.cards))pdb2.cards=[];
      const nowV=new Date().toISOString();
      pdb2.cards.unshift({
        id:'PIPE-'+Date.now().toString(36).toUpperCase() + '-' + Math.random().toString(36).slice(2,5).toUpperCase(),
        pipefyId: pipefyReceberCardId||null,
        phase:'receber',
        nomeContato: nomeCliente||'',
        telefone: (req.body.telefone||''),
        equipamento: p.descricao||'',
        descricao: 'VENDA — '+(p.codigo||''),
        valor: parseFloat(p.preco)||0,
        origem:'venda', criadoEm:nowV, movedAt:nowV,
        aguardandoDesde:null, history:[], analiseCompra:false
      });
      pdb2.lastSync=nowV;
      await _vs2('reparoeletro_pipe',pdb2);
    } catch(ev2){ console.error('[venda→pipe]',ev2.message); }

    return res.status(200).json({
      ok: true, ficha,
      produto: db.produtos[idx],
      pipefyReceberCardId,
      textoAlmox,
    });
  }

  // ── POST retentar-pipefy — recria card no Pipefy para venda já feita ──────
  if (req.method === "POST" && action === "retentar-pipefy") {
    const { produtoId } = req.body || {};
    if (!produtoId) return res.status(400).json({ ok:false, error:"produtoId obrigatório" });
    const db = await dbGet(VENDAS_KEY) || defaultDB();
    const p  = db.produtos.find(x => x.id === produtoId);
    if (!p)  return res.status(404).json({ ok:false, error:"Produto não encontrado" });
    if (!p.vendido) return res.status(400).json({ ok:false, error:"Produto não foi vendido" });

    const precoFmt = parseFloat(p.preco).toLocaleString("pt-BR",{minimumFractionDigits:2,style:"currency",currency:"BRL"});
    const dataVenda = p.soldAt ? new Date(p.soldAt).toLocaleDateString("pt-BR",{timeZone:"America/Sao_Paulo"}) : new Date().toLocaleDateString("pt-BR",{timeZone:"America/Sao_Paulo"});
    const nomeCliente = p.compradorNome || "—";
    const telefone    = p.compradorTel  || "";
    const vendedor    = p.nomeVendedor  || p.vendedor || "—";
    const modalidade  = p.modalidade    || "—";

    const textoAlmox = [
      "📦 *SEPARAÇÃO — ALMOXARIFADO*","",
      `📅 Data: ${dataVenda}`,
      `🏷️ Código: ${p.codigo || "—"}`,
      `📦 Tipo: ${p.tipo || "—"}`,
      `📝 Descrição: ${p.descricao}`,
      `💰 Valor: ${precoFmt}`,"",
      `👤 Comprador: ${nomeCliente}`,
      telefone ? `📱 Telefone: ${telefone}` : null,"",
      `🛒 Vendedor: ${vendedor}`,
      `🔖 Modalidade: ${modalidade}`,
    ].filter(l => l !== null).join("\n");

    // ── Salvar no Pipe ADM (independente do Pipefy) ────────────────────────
    let pipefyCardId = null;
    const U_V = (process.env.UPSTASH_URL   || '').replace(/['"]/g,'').trim();
    const T_V = (process.env.UPSTASH_TOKEN || '').replace(/['"]/g,'').trim();
    try {
      async function _vpg(k) {
        const r = await fetch(U_V+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T_V,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});
        const j = await r.json(); const v = j[0]?.result; if(!v) return null;
        let val=JSON.parse(v); if(typeof val==='string'){try{val=JSON.parse(val);}catch(e){}} return(val&&typeof val==='object')?val:null;
      }
      async function _vps(k,v){await fetch(U_V+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T_V,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});}
      const pipeDb=(await _vpg('reparoeletro_pipe'))||{cards:[],syncedPipefyIds:[],lastSync:null};
      if(!Array.isArray(pipeDb.cards)) pipeDb.cards=[];
      const now_v=new Date().toISOString();
      pipeDb.cards.unshift({
        id:'PIPE-'+Date.now().toString(36).toUpperCase() + '-' + Math.random().toString(36).slice(2,5).toUpperCase(),
        pipefyId:null, phase:'receber',
        nomeContato:nomeCliente||'', telefone:telefone||'',
        equipamento:(p.descricao||''), descricao:`VENDA — ${p.codigo||''}`,
        valor:parseFloat(preco)||0, origem:'venda',
        criadoEm:now_v, movedAt:now_v, aguardandoDesde:null, history:[], analiseCompra:false
      });
      pipeDb.lastSync=now_v;
      await _vps('reparoeletro_pipe',pipeDb);
    } catch(epipe){ console.error('[vendas→pipe]', epipe.message); }

    // ── Pipefy (best-effort) ─────────────────────────────────────────────
    try {
      const phaseReceber = await getReceberPhaseId();
      if (phaseReceber) {
        const tituloCompleto = `VENDA — ${p.codigo || "—"} | ${p.tipo || "—"} ${p.descricao} | ${nomeCliente} | ${precoFmt} | ${vendedor} | ${modalidade}`;
        const titulo = tituloCompleto.replace(/"/g,"'").slice(0, 255);
        const mutation = `mutation { createCard(input: { pipe_id: "${PIPE_ID}" phase_id: "${phaseReceber}" title: "${titulo}" }) { card { id } } }`;
        const data = await pipefyQuery(mutation);
        pipefyCardId = data?.createCard?.card?.id || null;
      }
    } catch(e) { console.warn('[vendas] Pipefy best-effort falhou:', e.message); }

    logAction({ modulo:'Vendas', fichaId:'', ficha:nomeCliente||'', acao:'Registrar venda', para:'receber', gatilho:'→ Pipe receber', status:'ok', detalhe:(p.descricao||'')+' R$'+preco }).catch(()=>{});
    return res.status(200).json({ ok:true, pipefyCardId, textoAlmox });
  }

  // ── POST excluir ───────────────────────────────────────────
  // ── GET diagnostico — relatório completo do banco de vendas ─────────────────
  // ── GET cruzar-os — cruzamento completo de OSs entre catálogo e vendas ──────
  if (action === "cruzar-os") {
    const codigos = (req.query.codigos || "").split(",").map(s => s.trim().toLowerCase()).filter(Boolean);
    if (!codigos.length) return res.status(400).json({ ok:false, error:"Informe ?codigos=2277,3376,..." });

    // Carregar todas as fontes em paralelo
    const [dbVendas, dbCheckout, dbFin, dbPipe, dbLog] = await Promise.all([
      dbGet("reparoeletro_vendas"),
      dbGet("reparoeletro_checkout_vendas"),
      dbGet("reparoeletro_financeiro"),
      dbGet("reparoeletro_pipe"),
      dbGet("reparoeletro_log"),
    ]);

    const produtos   = (dbVendas?.produtos    || []);
    const vendas     = (dbCheckout?.vendas    || []);
    const records    = (dbFin?.records        || []);
    const cards      = (dbPipe?.cards         || []);
    const logs       = Array.isArray(dbLog)   ? dbLog : (dbLog?.entries || []);

    const resultado = codigos.map(function(cod) {
      // Buscar no catálogo por código ou ID
      const produto = produtos.find(p =>
        (p.codigo||"").toLowerCase().includes(cod) ||
        (p.id||"").toLowerCase().includes(cod) ||
        (p.descricao||"").toLowerCase().includes(cod)
      );
      // Buscar no checkout (vendas concluídas)
      const vendaCheckout = vendas.filter(v =>
        JSON.stringify(v).toLowerCase().includes(cod)
      );
      // Buscar no financeiro
      const recFin = records.filter(r =>
        JSON.stringify(r).toLowerCase().includes(cod)
      );
      // Buscar no pipe
      const cardPipe = cards.filter(c =>
        JSON.stringify(c).toLowerCase().includes(cod)
      );
      // Buscar no log
      const logEntries = logs.filter(l =>
        JSON.stringify(l).toLowerCase().includes(cod)
      ).slice(0, 5);

      return {
        codigo: cod,
        catalogo: produto ? {
          id:          produto.id,
          codigo:      produto.codigo,
          descricao:   produto.descricao,
          preco:       produto.preco,
          vendido:     produto.vendido,
          soldAt:      produto.soldAt || null,
          updatedAt:   produto.updatedAt || null,
          status:      produto.vendido ? "VENDIDO" : "DISPONÍVEL",
        } : null,
        naLoja:        !!produto && !produto.vendido,
        foiVendido:    !!produto?.vendido,
        checkoutVenda: vendaCheckout.map(v => ({
          id:        v.id,
          comprador: v.comprador?.nome || v.compradorNome || "—",
          valor:     v.valor,
          data:      v.criadoEm || v.data,
          status:    v.status,
          produto:   v.produto?.codigo || v.produtoCodigo || "—",
        })),
        financeiro: recFin.map(r => ({
          id:      r.id,
          nome:    r.nome || r.nomeContato,
          fase:    r.phaseId,
          valor:   r.valor,
          paidAt:  r.paidAt,
          osCode:  r.osCode,
        })),
        pipe: cardPipe.map(c2 => ({
          id:     c2.id,
          nome:   c2.nomeContato || c2.title,
          fase:   c2.phase,
          valor:  c2.valor,
          history:(c2.history||[]).slice(-3),
        })),
        log: logEntries,
        diagnostico: (() => {
          if (!produto) return "❓ Não encontrado no catálogo";
          if (!produto.vendido && vendaCheckout.length > 0) return "⚠️ VENDA NO CHECKOUT MAS FLAG vendido=false NO CATÁLOGO";
          if (!produto.vendido && recFin.some(r => r.paidAt)) return "⚠️ PAGO NO FINANCEIRO MAS FLAG vendido=false NO CATÁLOGO";
          if (produto.vendido) return "✅ Vendido corretamente (vendido=true)";
          return "📋 Disponível no catálogo, sem movimentação";
        })(),
      };
    });

    return res.status(200).json({
      ok:       true,
      total:    codigos.length,
      geradoEm: new Date().toISOString(),
      resultados: resultado,
      resumo: resultado.map(r => ({
        codigo:      r.codigo,
        naLoja:      r.naLoja,
        foiVendido:  r.foiVendido,
        diagnostico: r.diagnostico,
      })),
    });
  }

    if (action === "diagnostico") {
    const db  = await dbGet(VENDAS_KEY) || defaultDB();
    const ck  = await dbGet("reparoeletro_checkout_vendas") || { vendas: [] };
    const produtos = db.produtos || [];

    // 1. Contagens básicas
    const total       = produtos.length;
    const disponiveis = produtos.filter(p => !p.vendido && !p.excluido).length;
    const vendidos    = produtos.filter(p =>  p.vendido).length;
    const semFlag     = produtos.filter(p =>  p.vendido === undefined).length;

    // 2. Inconsistências: constam no checkout como vendidos mas flag vendido=false
    const idsVendidosCK = new Set((ck.vendas || []).map(v => v.produto?.id).filter(Boolean));
    const inconsistentes = produtos.filter(p => idsVendidosCK.has(p.id) && !p.vendido);

    // 3. Duplicatas
    const ids = produtos.map(p => p.id);
    const dupes = ids.filter((id, i) => ids.indexOf(id) !== i);

    // 4. Produtos sem ID
    const semId = produtos.filter(p => !p.id).length;

    // 5. Produtos "vendido=false" que têm soldAt (inconsistência grave)
    const vendidoFalseComSoldAt = produtos.filter(p => !p.vendido && p.soldAt);

    // 6. Últimas 10 operações (mais recentes criados)
    const recentes = [...produtos]
      .sort((a,b) => new Date(b.updatedAt||b.createdAt||0) - new Date(a.updatedAt||a.createdAt||0))
      .slice(0, 10)
      .map(p => ({ id: p.id, codigo: p.codigo, descricao: p.descricao?.slice(0,40), vendido: p.vendido, soldAt: p.soldAt, updatedAt: p.updatedAt }));

    // 7. Verificar Cache-Control ativo
    const cacheHeader = "s-maxage=60, stale-while-revalidate=300";

    return res.status(200).json({
      ok: true,
      geradoEm: new Date().toISOString(),
      contagens: { total, disponiveis, vendidos, semFlagVendido: semFlag },
      inconsistencias: {
        totalInconsistentes: inconsistentes.length,
        detalhes: inconsistentes.map(p => ({
          id: p.id, codigo: p.codigo,
          descricao: p.descricao?.slice(0, 40),
          vendido: p.vendido,
          soldAt: p.soldAt,
          apareceuNoCheckout: true
        })),
      },
      duplicatas: { total: dupes.length, ids: [...new Set(dupes)].slice(0, 10) },
      semId,
      vendidoFalseComSoldAt: vendidoFalseComSoldAt.map(p => ({
        id: p.id, codigo: p.codigo,
        descricao: p.descricao?.slice(0, 40),
        soldAt: p.soldAt
      })),
      recentes,
      checkoutVendas: (ck.vendas || []).length,
      alerta: inconsistentes.length > 0
        ? "⚠️ " + inconsistentes.length + " produto(s) no checkout como vendido mas flag=false na loja"
        : "✅ Sem inconsistências de flag encontradas",
      causaProvavel: [
        inconsistentes.length > 0 ? "Produtos vendidos via MP mas flag vendido não atualizada no Redis (race condition webhook)" : null,
        vendidoFalseComSoldAt.length > 0 ? "Produtos com soldAt mas vendido=false — cancelar-venda acidentalmente chamado" : null,
        dupes.length > 0 ? "IDs duplicados no banco" : null,
        "Cache Vercel Edge ativo (s-maxage=60, stale=300s) — pode servir dados desatualizados por até 5 min",
      ].filter(Boolean),
    });
  }

    if (req.method === "POST" && action === "excluir") {
    const { id } = req.body || {};
    if (!id) return res.status(400).json({ ok: false, error: "id obrigatório" });
    const db = await dbGet(VENDAS_KEY) || defaultDB();
    db.produtos = db.produtos.filter(p => p.id !== id);
    await dbSet(VENDAS_KEY, db);
    return res.status(200).json({ ok: true });
  }

  // ── POST cancelar-venda — devolve produto ao estoque ───────────────────
  if (req.method === "POST" && action === "cancelar-venda") {
    const { id } = req.body || {};
    if (!id) return res.status(400).json({ ok: false, error: "id obrigatório" });
    const db = await dbGet(VENDAS_KEY) || defaultDB();
    const idx = db.produtos.findIndex(p => p.id === id);
    if (idx < 0) return res.status(404).json({ ok: false, error: "Produto não encontrado" });
    // Limpa todos os dados de venda, volta ao estado de estoque
    const p = db.produtos[idx];
    db.produtos[idx] = {
      ...p,
      vendido:      false,
      soldAt:       null,
      compradorNome: null,
      nomeVendedor: null,
      modalidade:   null,
      vendedor:     null,
      updatedAt:    new Date().toISOString(),
    };
    await dbSet(VENDAS_KEY, db);
    return res.status(200).json({ ok: true, produto: db.produtos[idx] });
  }


  // ── POST update-pipefy-card ────────────────────────────────────────────────
  if (req.method === "POST" && action === "update-pipefy-card") {
    const { cardId, nome, telefone } = req.body || {};
    if (!cardId) return res.status(400).json({ ok:false, error:"cardId obrigatorio" });
    try {
      const parts = [];
      if (nome)     parts.push(`m1: updateCardField(input: { card_id: "${cardId}" field_id: "nome_do_contato" new_value: "${nome.replace(/"/g,"'")}" }) { card { id } }`);
      if (telefone) parts.push(`m2: updateCardField(input: { card_id: "${cardId}" field_id: "telefone" new_value: "${telefone.replace(/"/g,"'")}" }) { card { id } }`);
      if (!parts.length) return res.status(400).json({ ok:false, error:"nome ou telefone obrigatorio" });
      await pipefyQuery("mutation { " + parts.join(" ") + " }");
      return res.status(200).json({ ok:true, cardId });
    } catch(e) {
      return res.status(500).json({ ok:false, error:e.message });
    }
  }

  return res.status(404).json({ ok: false, error: "Ação não encontrada" });
};
