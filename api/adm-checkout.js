// api/adm-checkout.js — Checkout ADM (Microondas / Bebedouro)
// Chaves separadas das de TV para manter relatórios independentes

const U = process.env.UPSTASH_URL;
const T = process.env.UPSTASH_TOKEN;
const CFG_KEY    = 'reparoeletro_checkout_config';
const VENDAS_KEY = 'reparoeletro_checkout_vendas';

async function dbGet(k) {
  const r = await fetch(`${U}/pipeline`, {
    method:'POST',
    headers:{ Authorization:`Bearer ${T}`, 'Content-Type':'application/json' },
    body: JSON.stringify([['GET', k]])
  });
  const j = await r.json();
  const v = j[0]?.result;
  if (!v) return null;
  const p = JSON.parse(v);
  return typeof p === 'string' ? JSON.parse(p) : p;
}

async function dbSet(k, val) {
  await fetch(`${U}/pipeline`, {
    method:'POST',
    headers:{ Authorization:`Bearer ${T}`, 'Content-Type':'application/json' },
    body: JSON.stringify([['SET', k, JSON.stringify(val)]])
  });
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Cache-Control', 'no-store');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const action = req.query.action || '';

  // ── GET load-config ──────────────────────────────────────────
  if (action === 'load-config') {
    const cfg = (await dbGet(CFG_KEY)) || {};
    return res.status(200).json({ ok:true, ...cfg });
  }

  // ── POST save-config ─────────────────────────────────────────
  if (req.method === 'POST' && action === 'save-config') {
    const { provedor, config } = req.body || {};
    const cfg = (await dbGet(CFG_KEY)) || {};
    if (provedor !== undefined) cfg.provedor = provedor;
    if (config   !== undefined) cfg.config   = config;
    await dbSet(CFG_KEY, cfg);
    return res.status(200).json({ ok:true });
  }

  // ── POST set-destaque ────────────────────────────────────────
  if (req.method === 'POST' && action === 'set-destaque') {
    const { produtoId, destaque } = req.body || {};
    const cfg = (await dbGet(CFG_KEY)) || {};
    cfg.destaques = cfg.destaques || {};
    if (destaque) cfg.destaques[produtoId] = destaque;
    else delete cfg.destaques[produtoId];
    await dbSet(CFG_KEY, cfg);
    return res.status(200).json({ ok:true });
  }

  // ── GET load-equipamentos — carrega Microondas/Bebedouro ─────
  if (action === 'load-equipamentos') {
    const proto = req.headers['x-forwarded-proto'] || 'https';
    const host  = req.headers.host;
    const d     = await fetch(`${proto}://${host}/api/vendas?action=load`).then(r => r.json());
    const prods = (d.produtos || []).filter(p => !p.vendido);
    const cfg   = (await dbGet(CFG_KEY)) || {};
    return res.status(200).json({
      ok: true,
      produtos: prods.map(p => ({ ...p, _destaque: cfg.destaques?.[p.id] || null }))
    });
  }

  // ── POST registrar-venda ─────────────────────────────────────
  if (req.method === 'POST' && action === 'registrar-venda') {
    const { produto, comprador, valor, provedor, paymentId, paymentMethod, installments } = req.body || {};
    if (!produto?.id) return res.status(400).json({ ok:false, error:'produto obrigatorio' });
    const db = (await dbGet(VENDAS_KEY)) || { vendas: [] };
    db.vendas.unshift({
      id:            Date.now().toString(36),
      produto, comprador, valor,
      provedor:      provedor || 'mercado_pago',
      paymentId:     paymentId     || null,
      paymentMethod: paymentMethod || null,
      installments:  installments  || null,
      criadoEm:      new Date().toISOString()
    });
    await dbSet(VENDAS_KEY, db);

    // ── Pipe ADM: criar card em Receber para separação ─────────────────────
    try {
      async function _cg(k){const r=await fetch(`${U}/pipeline`,{method:'POST',headers:{Authorization:`Bearer ${T}`,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});const j=await r.json();const v=j[0]?.result;if(!v)return null;try{let x=JSON.parse(v);if(typeof x==='string')x=JSON.parse(x);return x;}catch(e){return null;}}
      async function _cs(k,v){await fetch(`${U}/pipeline`,{method:'POST',headers:{Authorization:`Bearer ${T}`,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});}
      const pdb=(await _cg('reparoeletro_pipe'))||{cards:[],lastSync:null};
      if(!Array.isArray(pdb.cards))pdb.cards=[];
      const nowC=new Date().toISOString();
      const prod=produto||{};
      const compNome=(comprador?.nome||comprador?.name||'Cliente');
      const titulo=`VENDA — ${prod.codigo||prod.id||'Equipamento'} | ${compNome}`;
      const descricao=[prod.descricao||prod.nome||'',`Valor: R$${parseFloat(valor||0).toFixed(2)}`,`MP: ${paymentId||'—'}`,`Método: ${paymentMethod||'pix'}`].filter(Boolean).join(' | ');
      pdb.cards.unshift({
        id: 'PIPE-'+Date.now().toString(36).toUpperCase()+'-'+Math.random().toString(36).slice(2,4).toUpperCase(),
        pipefyId: null, phase:'receber',
        nomeContato: compNome,
        telefone: comprador?.telefone||comprador?.phone||'',
        equipamento: prod.tipo||prod.nome||'',
        descricao: descricao,
        title: titulo,
        valor: parseFloat(valor||0),
        origem: 'venda_checkout',
        vendaProdutoId: prod.id||null,
        codEquip: prod.codigo||null,
        criadoEm: nowC, movedAt: nowC,
        history:[{phase:'receber',ts:nowC,obs:'venda_checkout_mp'}],
        aguardandoDesde: null, analiseCompra: false
      });
      pdb.lastSync=nowC;
      await _cs('reparoeletro_pipe', pdb);
      console.log('[checkout] Card criado em receber para:', compNome, prod.codigo||prod.id);
    } catch(ep){
      console.error('[checkout] pipe receber:', ep.message);
      // Registrar falha no log central para rastreabilidade
      try {
        const _LK='reparoeletro_log';
        const _lr=await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['GET',_LK]])});
        const _lj=await _lr.json(); const _lv=_lj[0]?.result;
        let _log=[]; if(_lv){try{_log=JSON.parse(_lv);if(typeof _log==='string')_log=JSON.parse(_log);}catch(e){}}
        if(!Array.isArray(_log))_log=[];
        _log.unshift({ts:new Date().toISOString(),modulo:'adm-checkout',fichaId:paymentId||'',ficha:(req.body?.comprador?.nome||''),acao:'criar-card-receber',status:'erro',detalhe:ep.message});
        if(_log.length>2000)_log.splice(2000);
        await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['SET',_LK,JSON.stringify(_log)]])});
      } catch(el){}
    }

    return res.status(200).json({ ok:true });
  }


  // ── GET debug-vendas — mostra raw do Redis para diagnóstico ──
  // ── GET fix-receber — cria card receber para venda sem card no pipe ─────────
  if (action === 'fix-receber') {
    try {
      const VENDAS_K = 'reparoeletro_checkout_vendas';
      const PIPE_K   = 'reparoeletro_pipe';
      const ck  = (await dbGet(VENDAS_K)) || { vendas: [] };
      const pdb = (await dbGet(PIPE_K))   || { cards: [] };
      if (!Array.isArray(pdb.cards)) pdb.cards = [];

      // Pegar a venda mais recente
      const n = parseInt(req.query.n || '1');
      const vendas = (ck.vendas || []).slice(0, n);
      if (!vendas.length) return res.status(404).json({ ok:false, error:'Nenhuma venda encontrada' });

      const criados = [];
      for (const venda of vendas) {
        // Verificar se já tem card no pipe para este paymentId
        const jaExiste = pdb.cards.some(c =>
          c.origem === 'venda_checkout' &&
          (c.vendaProdutoId === venda.produto?.id || c.descricao?.includes(venda.paymentId||''))
        );
        if (jaExiste) { criados.push({ venda: venda.produto?.descricao, status: 'ja_existe' }); continue; }

        const nowC = new Date().toISOString();
        const prod = venda.produto || {};
        const compNome = venda.comprador?.nome || 'Cliente';
        const titulo = 'VENDA — ' + (prod.codigo || prod.id || 'Equipamento') + ' | ' + compNome;
        const descricao = [
          prod.descricao || prod.nome || '',
          'Valor: R$' + parseFloat(venda.valor || 0).toFixed(2),
          'MP: ' + (venda.paymentId || '—'),
          'Método: ' + (venda.paymentMethod || 'pix'),
        ].filter(Boolean).join(' | ');

        pdb.cards.unshift({
          id: 'PIPE-' + Date.now().toString(36).toUpperCase() + '-FIX',
          pipefyId: null, phase: 'receber',
          nomeContato: compNome,
          telefone: venda.comprador?.telefone || '',
          equipamento: prod.tipo || prod.nome || '',
          descricao: descricao,
          title: titulo,
          valor: parseFloat(venda.valor || 0),
          origem: 'venda_checkout',
          vendaProdutoId: prod.id || null,
          codEquip: prod.codigo || null,
          criadoEm: nowC, movedAt: nowC,
          history: [{ phase: 'receber', ts: nowC, obs: 'fix-receber-manual' }],
          aguardandoDesde: null, analiseCompra: false,
        });
        criados.push({ venda: titulo, status: 'criado', produto: prod.descricao || prod.id });
      }

      pdb.lastSync = new Date().toISOString();
      await dbSet(PIPE_K, pdb);

      return res.status(200).json({
        ok: true,
        totalVendas: vendas.length,
        criados,
        msg: '✅ Cards criados em receber no pipe ADM',
      });
    } catch(e) {
      return res.status(500).json({ ok: false, error: e.message });
    }
  }

    if (action === 'debug-vendas') {
    try {
      const r = await fetch(`${U}/pipeline`, {
        method:'POST',
        headers:{ Authorization:`Bearer ${T}`, 'Content-Type':'application/json' },
        body: JSON.stringify([['GET', VENDAS_KEY]])
      });
      const j = await r.json();
      const raw = j[0]?.result;
      const parsed = raw ? JSON.parse(raw) : null;
      const tipo = Array.isArray(parsed) ? 'array' : typeof parsed;
      const qtd  = parsed?.vendas?.length ?? (Array.isArray(parsed) ? parsed.length : 0);
      return res.status(200).json({
        ok: true, key: VENDAS_KEY, rawLength: raw?.length || 0,
        tipo, qtd, primeiros2: parsed?.vendas?.slice(0,2) || (Array.isArray(parsed) ? parsed.slice(0,2) : null)
      });
    } catch(e) {
      return res.status(200).json({ ok:false, erro: e.message });
    }
  }

  // ── GET load-vendas ──────────────────────────────────────────
  if (action === 'load-vendas') {
    try {
      const raw = await dbGet(VENDAS_KEY);
      // Aceitar tanto { vendas: [] } quanto array direto (compatibilidade)
      let vendas = [];
      if (raw) {
        if (Array.isArray(raw))        vendas = raw;
        else if (Array.isArray(raw.vendas)) vendas = raw.vendas;
      }
      const total = vendas.reduce((s, v) => s + parseFloat(v.valor || 0), 0);
      return res.status(200).json({ ok:true, vendas, total: parseFloat(total.toFixed(2)), count: vendas.length });
    } catch(e) {
      return res.status(200).json({ ok:true, vendas:[], total:0, count:0, erro: e.message });
    }
  }

  return res.status(404).json({ ok:false, error:'ação não encontrada' });
};
