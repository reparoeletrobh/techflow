// api/relatorio-logistica-pipefy.js
// Compara fichas da Logística com cards no Pipefy
module.exports = async function handler(req, res) {
  // 🔐 TF-AUTH (Fase 1): chave obrigatória em toda chamada
  const _tfk = (req.query && req.query.k) || req.headers['x-tf-key'] || '';
  if (_tfk !== ((process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim())) {
    return res.status(401).json({ ok: false, error: 'não autorizado' });
  }

  res.setHeader('Cache-Control', 'no-store');
  res.setHeader('Access-Control-Allow-Origin', '*');

  const U  = (process.env.UPSTASH_URL   || '').replace(/['"]/g,'').trim();
  const T  = (process.env.UPSTASH_TOKEN || '').replace(/['"]/g,'').trim();
  const PT = (process.env.PIPEFY_TOKEN  || '').replace(/['"]/g,'').trim();

  async function rGet(key) {
    try {
      const r = await fetch(`${U}/pipeline`, {
        method:'POST',
        headers:{ Authorization:`Bearer ${T}`, 'Content-Type':'application/json' },
        body: JSON.stringify([['GET', key]])
      });
      const j = await r.json();
      const v = j[0]?.result;
      if (!v) return null;
      const p = JSON.parse(v);
      return typeof p === 'string' ? JSON.parse(p) : p;
    } catch(e) { return null; }
  }

  async function pipefyQ(query) {
    const r = await fetch('https://api.pipefy.com/graphql', {
      method:'POST',
      headers:{ 'Content-Type':'application/json', Authorization:`Bearer ${PT}` },
      body: JSON.stringify({ query })
    });
    const j = await r.json();
    if (j.errors) throw new Error(j.errors[0].message);
    return j.data;
  }

  const hoje = req.query.data || new Date().toISOString().slice(0,10); // YYYY-MM-DD

  try {
    // 1. Ler logística
    const logDb = await rGet('reparoeletro_logistica');
    const fichas = (logDb?.fichas || []);

    // Filtrar fichas de hoje (por criadoEm ou movedAt)
    const fichasHoje = fichas.filter(f => {
      const data = (f.diagnostico?.preco ? f.movedAt : f.criadoEm) || f.criadoEm || '';
      return data.startsWith(hoje);
    });

    // Todas as fichas com orc_registrado OU com diagnóstico
    const fichasOrc = fichas.filter(f =>
      f.phase === 'orc_registrado' ||
      (f.diagnostico?.preco && f.movedAt?.startsWith(hoje))
    );

    // 2. Para cada ficha com pipefyCardId, consultar o Pipefy
    const comPipefy    = fichasOrc.filter(f => f.pipefyCardId);
    const semPipefy    = fichasOrc.filter(f => !f.pipefyCardId);

    const pipefyResults = {};
    for (const f of comPipefy) {
      try {
        const d = await pipefyQ(`query {
          card(id: "${f.pipefyCardId}") {
            id title
            current_phase { name }
            fields { name value }
          }
        }`);
        pipefyResults[f.pipefyCardId] = d?.card || null;
      } catch(e) {
        pipefyResults[f.pipefyCardId] = { erro: e.message };
      }
    }

    // 3. Montar tabela de comparação
    const tabela = fichasOrc.map(f => {
      const pipefy = f.pipefyCardId ? pipefyResults[f.pipefyCardId] : null;
      const nomeMatch   = pipefy?.title ? pipefy.title.includes(f.nome?.split(' ')[0] || '') : null;
      const faseAtual   = pipefy?.current_phase?.name || null;
      const valorPipefy = pipefy?.fields?.find(x => x.name?.toLowerCase().includes('valor'))?.value || null;

      return {
        id:          f.id,
        nome:        f.nome,
        telefone:    f.telefone || '',
        equipamento: f.equipamento || '',
        phase:       f.phase,
        movedAt:     f.movedAt?.slice(0,16)?.replace('T',' ') || '',
        preco:       f.diagnostico?.preco || null,
        pipefyCardId:f.pipefyCardId || null,
        pipefyStatus: !f.pipefyCardId ? '❌ Sem card'
          : pipefy?.erro        ? `⚠️ Erro: ${pipefy.erro}`
          : pipefy              ? '✅ Card existe'
          : '❓ Não encontrado',
        fasePipefy:  faseAtual || '-',
        valorPipefy: valorPipefy || '-',
        nomeMatch:   nomeMatch === null ? '-' : (nomeMatch ? '✅' : '⚠️'),
        pipefyErro:  f.pipefyErro || null,
      };
    });

    const resumo = {
      data:              hoje,
      totalOrcamentos:   fichasOrc.length,
      comPipefyCardId:   comPipefy.length,
      semPipefyCardId:   semPipefy.length,
      pipefyConfirmados: tabela.filter(r => r.pipefyStatus.startsWith('✅')).length,
      pipefyPendentes:   tabela.filter(r => r.pipefyStatus.startsWith('❌')).length,
      pipefyErros:       tabela.filter(r => r.pipefyStatus.startsWith('⚠️')).length,
    };

    return res.status(200).json({ ok:true, resumo, tabela, todasFichasHoje: fichasHoje.length });
  } catch(e) {
    return res.status(500).json({ ok:false, erro: e.message });
  }
};
