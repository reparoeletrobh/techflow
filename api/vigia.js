// ═══════════════════════════════════════════════════════════════════
// VIGIA — nenhuma ficha pode sair de uma coluna sem chegar em outra.
// Existe porque em 11/08 a Maria 1499 e o Kaio 8225 saíram do Remarcar
// e não apareceram no atendimento: a coluna foi liberada, a gravação da
// ficha nova não persistiu, e ninguém soube até o cliente ser esquecido.
// ═══════════════════════════════════════════════════════════════════
const U = (process.env.UPSTASH_URL || '').replace(/['"]/g, '').trim();
const T = (process.env.UPSTASH_TOKEN || '').replace(/[\n\r'"]/g, '').trim();
async function dbGet(k) {
  try {
    const r = await fetch(`${U}/get/${k}`, { headers: { Authorization: `Bearer ${T}` } }).then(x => x.json());
    return r && r.result ? JSON.parse(r.result) : null;
  } catch (e) { return null; }
}
async function dbSet(k, v) {
  try {
    await fetch(`${U}/set/${k}`, { method: 'POST',
      headers: { Authorization: `Bearer ${T}`, 'Content-Type': 'application/json' },
      body: JSON.stringify(v) });
    return true;
  } catch (e) { return false; }
}
const d8 = t => String(t || '').replace(/\D/g, '').slice(-8);
const hh = d => d ? new Date(new Date(d).getTime() - 3 * 3600000).toISOString().slice(5, 16).replace('T', ' ') : '?';

// onde uma ficha pode legitimamente estar
const BANCOS = [
  ['fichas_adm', 'fichas'], ['fichas_tv', 'fichas'],
  ['reparoeletro_logistica', 'fichas'], ['tv_logistica', 'fichas'],
  ['prospeccao_adm', 'fichas'], ['prospeccao_tv', 'fichas'],
  ['reparoeletro_pipe', 'cards'], ['tv_pipe', 'cards'],
  ['reparoeletro_arquivo', 'fichas'], ['tv_arquivo', 'fichas'],
  ['reparoeletro_garantia_v2', 'fichas'], ['reparoeletro_board', 'cards'],
];

async function ondeEstaCadaTelefone() {
  const mapa = {};
  for (const [k, L] of BANCOS) {
    try {
      const b = await dbGet(k);
      for (const x of ((b || {})[L] || [])) {
        const t = d8(x.telefone);
        if (t.length < 8) continue;
        (mapa[t] = mapa[t] || []).push({
          banco: k, fase: String(x.status || x.phase || x.phaseId || x.faseId || '?'),
          origem: x.origem || null, id: x.id,
        });
      }
    } catch (e) {}
  }
  return mapa;
}

module.exports = async function handler(req, res) {
  const _k = (req.query && req.query.k) || req.headers['x-tf-key'] || '';
  if (_k !== ((process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim())) {
    return res.status(401).json({ ok: false, error: 'não autorizado' });
  }
  res.setHeader('Access-Control-Allow-Origin', 'https://reparoeletroadm.com');
  const { action } = req.query;

  // ── 🚨 VARRER: procura fichas em estado inconsistente ──
  if (action === 'varrer' || !action) {
    const horas = Math.min(168, Math.max(1, parseInt(req.query.horas || '48', 10)));
    const corte = Date.now() - horas * 3600000;
    const mapa = await ondeEstaCadaTelefone();
    const alertas = [];

    // 1) marcada como devolvida do remarcar, mas sem ficha no atendimento
    for (const [k, L] of [['reparoeletro_logistica', 'adm'], ['tv_logistica', 'tv']]) {
      const b = await dbGet(k);
      for (const f of ((b || {}).fichas || [])) {
        const q = new Date(f.enviadoProspeccaoEm || f.remarcadoEm || 0).getTime();
        if (!q || q < corte) continue;
        const onde = mapa[d8(f.telefone)] || [];
        const chegou = onde.some(o => (o.banco === 'fichas_adm' || o.banco === 'fichas_tv') &&
          (o.origem === 'remarcar' || String(o.id || '').startsWith('rem_') ||
           String(o.id || '').startsWith('fic_reag_') || String(o.id || '').startsWith('rec_')));
        if (!chegou) alertas.push({ tipo: 'REMARCAR_SEM_DESTINO', sis: L,
          nome: f.nome, tel: d8(f.telefone).slice(-4), quando: hh(f.enviadoProspeccaoEm || f.remarcadoEm),
          detalhe: 'saiu da coluna Remarcar e não há ficha no atendimento',
          // 🔍 mostra onde o telefone aparece, para saber se é perda real ou falha de leitura
          ondeAparece: onde.map(o => o.banco + ':' + o.fase +
            (o.origem ? '(' + o.origem + ')' : '') + '[' + String(o.id || '').slice(0, 10) + ']'),
          telefoneNaLogistica: String(f.telefone || ''),
          motivo: f.motivoRemarcar || '(sem motivo)' });
      }
    }

    // 2) ficha em fase de saída sem nenhum registro em outro banco
    for (const [k, L] of [['reparoeletro_logistica', 'fichas'], ['tv_logistica', 'fichas']]) {
      const b = await dbGet(k);
      for (const f of ((b || {})[L] || [])) {
        if (String(f.phase || '') !== 'prospeccao') continue;
        const q = new Date(f.enviadoProspeccaoEm || f.movedAt || 0).getTime();
        if (!q || q < corte) continue;
        const onde = (mapa[d8(f.telefone)] || []).filter(o => o.banco !== k);
        if (!onde.length) alertas.push({ tipo: 'SEM_DESTINO', sis: k,
          nome: f.nome, tel: d8(f.telefone).slice(-4), quando: hh(f.enviadoProspeccaoEm || f.movedAt),
          detalhe: 'marcada como enviada mas não existe em nenhum outro banco' });
      }
    }

    // 3) ficha criada hoje sem telefone válido — nunca poderá ser contatada
    for (const [k, L] of [['fichas_adm', 'fichas'], ['fichas_tv', 'fichas']]) {
      const b = await dbGet(k);
      for (const f of ((b || {})[L] || [])) {
        const q = new Date(f.criadoEm || f.registradoEm || 0).getTime();
        if (!q || q < corte) continue;
        if (d8(f.telefone).length >= 8) continue;
        alertas.push({ tipo: 'SEM_TELEFONE', sis: k, nome: f.nome,
          tel: String(f.telefone || '(vazio)'), quando: hh(f.criadoEm),
          detalhe: 'ficha sem telefone válido — impossível contatar' });
      }
    }

    // 4) ficha de garantia sem tipo — não renderiza em coluna nenhuma
    try {
      const g = await dbGet('reparoeletro_garantia_v2');
      for (const f of ((g || {}).fichas || [])) {
        if (f.tipo) continue;
        alertas.push({ tipo: 'GARANTIA_SEM_TIPO', sis: 'garantia', nome: f.nome,
          tel: d8(f.telefone).slice(-4), quando: hh(f.criadaEm || f.criadoEm),
          detalhe: 'garantia sem tipo — invisível em todas as colunas' });
      }
    } catch (e) {}

    // 5) 🚨 banco perto do limite de 1 MB — gravações começam a falhar em silêncio
    const tamanhos = {};
    for (const [k] of BANCOS) {
      try {
        const b = await dbGet(k);
        if (!b) continue;
        const t = JSON.stringify(b).length;
        tamanhos[k] = (t / 1048576).toFixed(2) + ' MB';
        if (t > 900000) alertas.push({ tipo: 'BANCO_NO_LIMITE', sis: k, nome: k,
          tel: '', quando: 'agora',
          detalhe: 'banco com ' + (t / 1048576).toFixed(2) + ' MB — perto do limite de 1 MB, ' +
            'gravações podem falhar em silêncio e fichas se perdem' });
      } catch (e) {}
    }
    const porTipo = alertas.reduce((o, a) => { o[a.tipo] = (o[a.tipo] || 0) + 1; return o; }, {});
    // guarda o histórico para acompanhar a evolução
    try {
      const dia = new Date(Date.now() - 3 * 3600000).toISOString().slice(0, 10);
      const hist = (await dbGet('vigia_historico')) || { dias: {} };
      hist.dias[dia] = { em: new Date().toISOString(), total: alertas.length, porTipo };
      await dbSet('vigia_historico', hist);
    } catch (e) {}

    return res.status(200).json({ ok: alertas.length === 0,
      janelaHoras: horas,
      totalDeProblemas: alertas.length,
      POR_TIPO: porTipo,
      TAMANHO_DOS_BANCOS: tamanhos,
      SITUACAO: alertas.length === 0
        ? '✅ nenhuma ficha perdida — todas as saídas têm destino'
        : '🚨 ' + alertas.length + ' ficha(s) em estado inconsistente',
      ALERTAS: alertas.map(a => a.tipo + ' | ' + a.sis + ' | ' +
        String(a.nome || '?').slice(0, 20) + ' ' + a.tel + ' | ' + a.quando +
        ' | ' + a.detalhe +
        (a.ondeAparece ? ' → ONDE: ' + (a.ondeAparece.join(' | ') || 'em lugar nenhum') : '') +
        (a.telefoneNaLogistica ? ' | tel: ' + a.telefoneNaLogistica : '')),
      comoCorrigir: {
        REMARCAR_SEM_DESTINO: '/api/logistica?action=recriar-perdidas&dia=HOJE&aplicar=1',
        GARANTIA_SEM_TIPO: 'atribuir o tipo na tela de garantia',
        SEM_TELEFONE: 'corrigir o telefone na ficha',
      } });
  }

  // ── 🧹 ENXUGAR: remove campos redundantes que incham o banco ──
  // textoCopiar ocupa ~241 bytes por ficha (26% de fichas_adm) e é apenas texto
  // pronto para colar — pode ser montado na hora a partir dos próprios campos.
  if (action === 'enxugar') {
    const banco = String(req.query.banco || 'fichas_adm');
    const CAMPOS = String(req.query.campos || 'textoCopiar').split(',').map(x => x.trim()).filter(Boolean);
    const db = await dbGet(banco);
    if (!db || !Array.isArray(db.fichas)) return res.status(200).json({ ok: false, error: 'banco sem lista fichas' });
    const antes = JSON.stringify(db).length;
    let afetadas = 0;
    const copia = JSON.parse(JSON.stringify(db));
    for (const f of copia.fichas) {
      let mexeu = false;
      for (const c of CAMPOS) if (f[c] !== undefined) { delete f[c]; mexeu = true; }
      if (mexeu) afetadas++;
    }
    const depois = JSON.stringify(copia).length;
    if (String(req.query.aplicar || '') !== '1') {
      return res.status(200).json({ ok: true, modo: 'prévia',
        banco, campos: CAMPOS, fichasAfetadas: afetadas,
        tamanhoAtualMB: (antes / 1048576).toFixed(2),
        tamanhoDepoisMB: (depois / 1048576).toFixed(2),
        economiaMB: ((antes - depois) / 1048576).toFixed(2),
        economiaPct: Math.round((antes - depois) / antes * 100) + '%',
        dica: 'para aplicar: &aplicar=1' });
    }
    await dbSet(banco, copia);
    const conf = await dbGet(banco);
    const persistiu = ((conf || {}).fichas || []).length === copia.fichas.length;
    return res.status(200).json({ ok: persistiu,
      fichasAfetadas: afetadas,
      tamanhoAntesMB: (antes / 1048576).toFixed(2),
      tamanhoDepoisMB: (depois / 1048576).toFixed(2),
      persistiu,
      alerta: persistiu ? undefined : '🚨 a gravação não persistiu — banco ainda no limite' });
  }

  // ── 📦 ARQUIVAR-ANTIGAS: tira do banco quente o que já foi resolvido ──
  // fichas_adm chegou a 0,97 MB com 1089 registros. O limite do Upstash é 1 MB por
  // chave, e perto dele as gravações concorrentes falham em silêncio — foi assim que
  // fichas devolvidas do remarcar sumiram sem deixar rastro.
  if (action === 'arquivar-antigas') {
    const banco = String(req.query.banco || 'fichas_adm');
    const arquivo = banco + '_arquivo';
    const diasManter = Math.min(365, Math.max(7, parseInt(req.query.dias || '30', 10)));
    const manterN = parseInt(req.query.manter || '0', 10);   // manter as N mais recentes
    const corte = Date.now() - diasManter * 86400000;
    // fases em que a ficha já cumpriu seu papel no banco quente
    const FINAIS = ['logistica', 'finalizado', 'descarte', 'cliente_loja', 'concluido',
      'prospeccao', 'duplicada', 'arquivada', 'entregue'];
    const ATIVAS = ['criada', 'contato_feito', 'entrar_contato'];
    const db = await dbGet(banco);
    const LISTA = Array.isArray((db || {}).fichas) ? 'fichas'
      : Array.isArray((db || {}).cards) ? 'cards' : null;
    if (!db || !LISTA) return res.status(200).json({ ok: false, error: 'banco sem lista reconhecida' });
    const dtDe = f => new Date(f.criadoEm || f.registradoEm || f.movedAt || 0).getTime();
    const ficam = [], vao = [];
    if (manterN > 0) {
      // 📌 critério simples e seguro: guarda as N mais recentes e tudo que ainda está
      // ativo; o resto, já encerrado, vai para o arquivo
      const ordenadas = [...db[LISTA]].sort((a, b) => dtDe(b) - dtDe(a));
      ordenadas.forEach((f, i) => {
        const ativa = ATIVAS.includes(String(f.status || ''));
        const encerrada = FINAIS.includes(String(f.status || ''));
        if (i < manterN || ativa || !encerrada) ficam.push(f); else vao.push(f);
      });
    } else {
      for (const f of db[LISTA]) {
        const q = dtDe(f);
        const antiga = q && q < corte;
        const encerrada = FINAIS.includes(String(f.status || ''));
        (antiga && encerrada ? vao : ficam).push(f);
      }
    }
    db.fichas = db[LISTA];
    const tamAntes = JSON.stringify(db).length;
    const tamDepois = JSON.stringify({ ...db, fichas: ficam }).length;
    if (String(req.query.aplicar || '') !== '1') {
      // 🔍 distribuição real: por status, por mês e quantas sem data
      const porStatus = db.fichas.reduce((o, f) => {
        const s = String(f.status || '(sem status)'); o[s] = (o[s] || 0) + 1; return o; }, {});
      const porMes = db.fichas.reduce((o, f) => {
        const d = String(f.criadoEm || f.registradoEm || '');
        const k = d ? d.slice(0, 7) : '(SEM DATA)'; o[k] = (o[k] || 0) + 1; return o; }, {});
      // qual campo ocupa mais espaço?
      const amostra = db.fichas.slice(0, 50);
      const porCampo = {};
      for (const f of amostra) {
        for (const [k, v] of Object.entries(f)) {
          porCampo[k] = (porCampo[k] || 0) + JSON.stringify(v || '').length;
        }
      }
      const pesados = Object.entries(porCampo).sort((a, b) => b[1] - a[1]).slice(0, 8)
        .map(([k, v]) => k + ': ~' + Math.round(v / amostra.length) + ' bytes/ficha');
      return res.status(200).json({ ok: true, modo: 'prévia',
        banco, registrosHoje: db.fichas.length,
        POR_STATUS: porStatus,
        POR_MES: Object.fromEntries(Object.entries(porMes).sort()),
        CAMPOS_MAIS_PESADOS: pesados,
        bytesPorFicha: Math.round(JSON.stringify(db).length / db.fichas.length),
        vaoParaOArquivo: vao.length, permanecem: ficam.length,
        tamanhoAtualMB: (tamAntes / 1048576).toFixed(2),
        tamanhoDepoisMB: (tamDepois / 1048576).toFixed(2),
        criterio: 'criada há mais de ' + diasManter + ' dias E já encerrada (' + FINAIS.join(', ') + ')',
        amostra: vao.slice(0, 10).map(f => String(f.nome || '?').slice(0, 20) + ' | ' +
          f.status + ' | ' + String(f.criadoEm || '').slice(0, 10)),
        dica: 'para arquivar: &aplicar=1' });
    }
    // guarda no arquivo, somando ao que já houver
    const arq = (await dbGet(arquivo)) || { fichas: [] };
    arq.fichas = (arq.fichas || []).concat(vao);
    await dbSet(arquivo, arq);
    // confere que o arquivo persistiu ANTES de tirar do banco quente
    const conf = await dbGet(arquivo);
    const salvou = ((conf || {}).fichas || []).length >= arq.fichas.length - 2;
    if (!salvou) return res.status(200).json({ ok: false,
      error: '🚨 o arquivo não persistiu — NADA foi removido do banco quente' });
    db[LISTA] = ficam;
    await dbSet(banco, db);
    // confere que o banco quente realmente encolheu
    const conf2 = await dbGet(banco);
    const agora = ((conf2 || {})[LISTA] || []).length;
    if (agora !== ficam.length) return res.status(200).json({ ok: false,
      error: '🚨 a gravação não persistiu — o arquivo foi salvo, mas o banco quente continua com ' + agora,
      arquivadas: vao.length });
    return res.status(200).json({ ok: true,
      arquivadas: vao.length, permanecem: ficam.length,
      tamanhoAntesMB: (tamAntes / 1048576).toFixed(2),
      tamanhoDepoisMB: (tamDepois / 1048576).toFixed(2),
      arquivo });
  }

  // ── 🧪 TESTE-GRAVACAO: o banco aceita escrita? qual o tamanho? ──
  if (action === 'teste-gravacao') {
    const alvo = String(req.query.banco || 'fichas_adm');
    const antes = await dbGet(alvo);
    const tamanho = JSON.stringify(antes || {}).length;
    const quantas = ((antes || {}).fichas || (antes || {}).cards || []).length;
    if (String(req.query.aplicar || '') !== '1') {
      return res.status(200).json({ ok: true, modo: 'só leitura',
        banco: alvo,
        registros: quantas,
        tamanhoBytes: tamanho,
        tamanhoMB: (tamanho / 1048576).toFixed(2),
        limiteUpstash: '1 MB por chave no plano gratuito · 100 MB no pago',
        alerta: tamanho > 900000 ? '🚨 PERTO DO LIMITE — gravações podem falhar em silêncio' :
                tamanho > 500000 ? '⚠️ banco grande, vale acompanhar' : '✅ tamanho tranquilo',
        dica: 'para testar uma gravação real: &aplicar=1' });
    }
    // grava um marcador e confere se persistiu
    const db = antes || { fichas: [] };
    const lista = db.fichas ? 'fichas' : (db.cards ? 'cards' : 'fichas');
    db[lista] = db[lista] || [];
    const marca = 'teste_vigia_' + Date.now().toString(36);
    db[lista].unshift({ id: marca, nome: 'TESTE DO VIGIA', telefone: '00000000000',
      status: 'teste', criadoEm: new Date().toISOString() });
    const gravou = await dbSet(alvo, db);
    await new Promise(s => setTimeout(s, 400));
    const depois = await dbGet(alvo);
    const achou = ((depois || {})[lista] || []).some(x => x.id === marca);
    // limpa o marcador
    if (achou) {
      const limpo = await dbGet(alvo);
      limpo[lista] = (limpo[lista] || []).filter(x => x.id !== marca);
      await dbSet(alvo, limpo);
    }
    return res.status(200).json({ ok: achou,
      banco: alvo, registros: quantas,
      tamanhoMB: (tamanho / 1048576).toFixed(2),
      dbSetRetornou: gravou,
      persistiu: achou,
      VEREDITO: achou
        ? '✅ o banco aceita gravação normalmente'
        : '🚨 A GRAVAÇÃO NÃO PERSISTIU — é aqui que as fichas se perdem' });
  }

  // ── 🩺 EXAME-COMPLETO: tudo que pode estar quebrado, num só lugar ──
  if (action === 'exame-completo') {
    const horas = Math.min(168, Math.max(1, parseInt(req.query.horas || '48', 10)));
    const corte = Date.now() - horas * 3600000;
    const hoje = new Date(Date.now() - 3 * 3600000).toISOString().slice(0, 10);
    const P = [];   // problemas
    const add = (grav, area, o, quem, oque, comoResolver) =>
      P.push({ gravidade: grav, area, objeto: o, quem, problema: oque, resolver: comoResolver });

    const [fA, fT, lgA, lgT, ppA, ppT, gar, fila, qual, abordados] = await Promise.all([
      dbGet('fichas_adm'), dbGet('fichas_tv'),
      dbGet('reparoeletro_logistica'), dbGet('tv_logistica'),
      dbGet('reparoeletro_pipe'), dbGet('tv_pipe'),
      dbGet('reparoeletro_garantia_v2'), dbGet('reparoeletro_garantia_fila'),
      dbGet('reparoeletro_qualidade'), dbGet('wa_abordados').then(v => v || { tels: {} }),
    ]);

    // 1️⃣ ficha saiu do remarcar e não chegou ao atendimento
    const chegou = new Set();
    for (const b of [fA, fT]) for (const f of (((b || {}).fichas) || [])) {
      const id = String(f.id || '');
      if (String(f.origem || '') === 'remarcar' || f.reagendarColeta === true ||
          id.startsWith('rem_') || id.startsWith('fic_reag_') || id.startsWith('rec_')) {
        chegou.add(d8(f.telefone));
      }
    }
    for (const [b, sis] of [[lgA, 'ADM'], [lgT, 'TV']]) {
      for (const f of (((b || {}).fichas) || [])) {
        const q = new Date(f.enviadoProspeccaoEm || f.remarcadoEm || 0).getTime();
        if (!q || q < corte) continue;
        if (chegou.has(d8(f.telefone))) continue;
        add('🔴 GRAVE', 'Remarcar', sis, (f.nome || '?') + ' ' + d8(f.telefone).slice(-4),
          'saiu da coluna e não há ficha no atendimento',
          '/api/logistica?action=recriar-perdidas&dia=' + hoje + '&aplicar=1');
      }
    }

    // 2️⃣ ficha abordada travada em "criada"
    for (const [b, sis] of [[fA, 'ADM'], [fT, 'TV']]) {
      for (const f of (((b || {}).fichas) || [])) {
        if (String(f.status || '') !== 'criada') continue;
        if (!abordados.tels[d8(f.telefone)]) continue;
        add('🟠 MÉDIO', 'Fichas', sis, (f.nome || '?') + ' ' + d8(f.telefone).slice(-4),
          'foi abordada pelo bot mas continua em Ficha Criada',
          '/api/wa-bot?action=destravar-criadas&aplicar=1');
      }
    }

    // 3️⃣ garantia sem tipo — invisível
    for (const f of (((gar || {}).fichas) || [])) {
      if (f.tipo) continue;
      add('🟠 MÉDIO', 'Garantia', '', (f.nome || '?') + ' ' + d8(f.telefone).slice(-4),
        'sem tipo — não aparece em nenhuma coluna', 'atribuir o tipo na tela de garantia');
    }

    // 4️⃣ garantia de loja que não está na fila
    const naFila = new Set((((fila || {}).itens) || [])
      .filter(i => i.status !== 'resolvido').map(i => d8(i.telefone)));
    for (const f of (((gar || {}).fichas) || [])) {
      if (f.concluida) continue;
      const t = String(f.tipo || '');
      const deveEstar = (t === 'loja_imediata' || t === 'loja_acompanhamento') ||
        (t === 'rua' && f.faseId === 'equip_recolhido');
      if (!deveEstar || naFila.has(d8(f.telefone))) continue;
      add('🟡 LEVE', 'Garantia', t, (f.nome || '?') + ' ' + d8(f.telefone).slice(-4),
        'deveria estar na fila de tratamento e não está',
        '/api/garantia?action=sincronizar-fila&aplicar=1');
    }

    // 5️⃣ inspeção de qualidade sem técnico
    for (const i of (((qual || {}).inspecoes) || [])) {
      if (i.tecnico || i.status === 'aprovado') continue;
      const q = new Date(i.criadoEm || 0).getTime();
      if (!q || q < corte) continue;
      add('🟡 LEVE', 'Qualidade', i.os || '', i.cliente || '?',
        'inspeção sem técnico responsável', 'informar o técnico na tela de qualidade');
    }

    // 6️⃣ card no pipe sem valor em fase que exige
    const EXIGE_VALOR = ['aprovados', 'producao', 'solicitar_entrega', 'erp'];
    for (const [b, sis] of [[ppA, 'ADM'], [ppT, 'TV']]) {
      for (const c of (((b || {}).cards) || [])) {
        const fase = String(c.phaseId || c.phase || '');
        if (!EXIGE_VALOR.includes(fase)) continue;
        if (Number(c.valor || 0) > 0) continue;
        add('🟠 MÉDIO', 'Pipe', sis, (c.nomeContato || c.nome || '?') + ' ' + d8(c.telefone).slice(-4),
          'card em ' + fase + ' sem valor definido', 'informar o valor no card');
      }
    }

    // 7️⃣ mensagens presas esperando a janela reabrir
    try {
      const pend = await dbGet('wa_pendentes_janela');
      for (const it of (((pend || {}).itens) || [])) {
        const dias = (Date.now() - new Date(it.criadoEm || 0).getTime()) / 86400000;
        if (dias < 2) continue;
        add('🟡 LEVE', 'WhatsApp', '', String(it.tel || '').slice(-4),
          'mensagem aguarda há ' + dias.toFixed(0) + ' dias o cliente responder ao template',
          'ligar para o cliente — ele não respondeu ao WhatsApp');
      }
    } catch (e) {}

    // 8️⃣ devoluções do remarcar que falharam
    try {
      const lg = await dbGet('log_remarcar_' + hoje);
      for (const x of (((lg || {}).itens) || [])) {
        if (x.resultado !== 'erro') continue;
        add('🔴 GRAVE', 'Remarcar', x.sistema, (x.nome || '?') + ' ' + String(x.telefone).slice(-4),
          'a devolução falhou: ' + (x.detalhe || ''),
          '/api/logistica?action=processar-pendentes&aplicar=1');
      }
    } catch (e) {}

    // 9️⃣ tamanho dos bancos
    const tam = {};
    for (const [k] of BANCOS) {
      try {
        const b = await dbGet(k);
        if (!b) continue;
        const t = JSON.stringify(b).length;
        tam[k] = (t / 1048576).toFixed(2) + ' MB';
        if (t > 900000) add('🟠 MÉDIO', 'Banco', k, k,
          'banco grande (' + (t / 1048576).toFixed(2) + ' MB) — quanto maior, maior a chance de duas gravações se sobreporem e uma ficha se perder',
          '/api/vigia?action=arquivar-antigas&banco=' + k + '&manter=400&aplicar=1');
      } catch (e) {}
    }

    const porGravidade = P.reduce((o, x) => { o[x.gravidade] = (o[x.gravidade] || 0) + 1; return o; }, {});
    const porArea = P.reduce((o, x) => { o[x.area] = (o[x.area] || 0) + 1; return o; }, {});
    // histórico
    try {
      const hst = (await dbGet('vigia_exame')) || { dias: {} };
      hst.dias[hoje] = { em: new Date().toISOString(), total: P.length, porArea };
      await dbSet('vigia_exame', hst);
    } catch (e) {}

    return res.status(200).json({ ok: P.length === 0,
      janelaHoras: horas,
      VEREDITO: P.length === 0 ? '✅ nenhum problema encontrado'
        : '🚨 ' + P.length + ' problema(s): ' + Object.entries(porGravidade)
            .map(([g, n]) => n + ' ' + g).join(' · '),
      POR_GRAVIDADE: porGravidade,
      POR_AREA: porArea,
      TAMANHO_DOS_BANCOS: tam,
      // 📦 agrupado por tipo de problema, para a tela mostrar resumido e expandir
      GRUPOS: Object.values(P.reduce((o, x) => {
        const k = x.gravidade + '|' + x.area + '|' + x.problema.split(' — ')[0].split(':')[0].slice(0, 60);
        o[k] = o[k] || { gravidade: x.gravidade, area: x.area, problema: x.problema.split(':')[0],
          quantos: 0, quem: [], resolver: x.resolver };
        o[k].quantos++;
        if (o[k].quem.length < 60) o[k].quem.push(x.quem);
        return o; }, {})).sort((a, b) => {
          const peso = g => g.includes('GRAVE') ? 0 : g.includes('MÉDIO') ? 1 : 2;
          return peso(a.gravidade) - peso(b.gravidade) || b.quantos - a.quantos;
        }),
      GRAVES: P.filter(x => x.gravidade.includes('GRAVE'))
        .map(x => x.area + ' | ' + x.quem + ' | ' + x.problema + ' → ' + x.resolver),
      MEDIOS: P.filter(x => x.gravidade.includes('MÉDIO'))
        .map(x => x.area + ' | ' + x.quem + ' | ' + x.problema),
      LEVES: P.filter(x => x.gravidade.includes('LEVE'))
        .map(x => x.area + ' | ' + x.quem + ' | ' + x.problema),
      RESOLVIVEIS_AUTOMATICAMENTE:
        (P.some(x => x.area === 'Remarcar') ? ['remarcar'] : [])
        .concat(P.some(x => /Ficha Criada/.test(x.problema)) ? ['criadas'] : [])
        .concat(P.some(x => /fila de tratamento/.test(x.problema)) ? ['garantia'] : []),
      COMO_RESOLVER: [...new Set(P.map(x => x.resolver).filter(r => r.startsWith('/api')))] });
  }

  // ── 📋 PLANO: as etapas que o Resolver vai executar, uma a uma ──
  if (action === 'plano') {
    const hoje = new Date(Date.now() - 3 * 3600000).toISOString().slice(0, 10);
    const etapas = [];
    // 1) devoluções do remarcar, dia a dia
    for (let i = 0; i < 7; i++) {
      const d = new Date(Date.now() - 3 * 3600000 - i * 86400000).toISOString().slice(0, 10);
      etapas.push({ id: 'remarcar-' + d,
        titulo: 'Devolver ao atendimento as fichas do Remarcar de ' + d.slice(8) + '/' + d.slice(5, 7),
        oQueFaz: 'procura fichas que saíram da coluna Remarcar naquele dia e não chegaram em Entrar em Contato, e as recria',
        url: 'logistica?action=recriar-perdidas&dia=' + d + '&aplicar=1' });
    }
    etapas.push({ id: 'pendentes',
      titulo: 'Reprocessar a fila de pendências do Remarcar',
      oQueFaz: 'tenta de novo as devoluções cuja gravação não persistiu',
      url: 'logistica?action=processar-pendentes&aplicar=1' });
    etapas.push({ id: 'criadas',
      titulo: 'Destravar fichas abordadas presas em Ficha Criada',
      oQueFaz: 'move para Contato Feito quem o bot já abordou, usando o horário real da abordagem',
      url: 'wa-bot?action=destravar-criadas&aplicar=1' });
    etapas.push({ id: 'garantia',
      titulo: 'Enviar à fila de tratamento as garantias de loja',
      oQueFaz: 'garantia de loja entra na fila ao ser cadastrada; esta etapa recupera as que ficaram fora',
      url: 'garantia?action=sincronizar-fila&aplicar=1' });
    etapas.push({ id: 'despachar',
      titulo: 'Enviar mensagens que aguardavam a janela do WhatsApp reabrir',
      oQueFaz: 'quem respondeu ao template recebe agora a mensagem que ficou guardada',
      url: 'wa-bot?action=despachar-pendentes&aplicar=1' });
    return res.status(200).json({ ok: true, etapas,
      naoAutomatico: [
        'garantia sem tipo — precisa da sua decisão sobre qual tipo atribuir',
        'card sem valor — o valor tem de ser informado por uma pessoa',
        'banco grande — o arquivamento tem etapa própria, com prévia',
      ] });
  }

  // ── ▶️ EXECUTAR-ETAPA: roda uma etapa e devolve o que aconteceu ──
  if (action === 'executar-etapa') {
    const url = String(req.query.url || '');
    if (!url || !/^[a-z-]+\?action=/.test(url)) {
      return res.status(400).json({ ok: false, error: 'etapa inválida' });
    }
    const K = (process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim();
    const inicio = Date.now();
    try {
      const resp = await fetch('https://reparoeletroadm.com/api/' + url + '&k=' + K);
      const txt = await resp.text();
      let r = null;
      try { r = JSON.parse(txt); } catch (e) {
        return res.status(200).json({ ok: false, httpStatus: resp.status,
          erro: 'a resposta não veio em formato JSON',
          respostaCrua: txt.slice(0, 400), duracaoMs: Date.now() - inicio });
      }
      // resume o que mudou, em linguagem de operação
      const n = r.recriadas || r.destravadas || r.enviadas || r.resolvidas || r.restauradas ||
        r.arquivadas || r.criadas || r.movidas || 0;
      const nada = (r.pendentes === 0) || (r.msg && /nada/i.test(r.msg));
      return res.status(200).json({ ok: r.ok !== false,
        httpStatus: resp.status,
        quantidade: n,
        semNadaAFazer: !!nada,
        resumo: nada ? 'nada a fazer nesta etapa'
          : (n ? n + ' registro(s) tratado(s)' : 'executada'),
        detalhe: r.feitos || r.L || r.nomes || r.lista || null,
        erro: r.ok === false ? (r.error || 'falhou') : null,
        retornoCompleto: r,
        duracaoMs: Date.now() - inicio });
    } catch (e) {
      return res.status(200).json({ ok: false, erro: e.message, tipo: 'exceção',
        duracaoMs: Date.now() - inicio });
    }
  }

  // ── 🛠️ RESOLVER: aplica as correções conhecidas, uma por tipo de problema ──
  if (action === 'resolver') {
    const quais = String(req.query.o || 'tudo').split(',').map(x => x.trim()).filter(Boolean);
    const quer = t => quais.includes('tudo') || quais.includes(t);
    const K = (process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim();
    const base = 'https://reparoeletroadm.com/api/';
    const hoje = new Date(Date.now() - 3 * 3600000).toISOString().slice(0, 10);
    const feitos = [], erros = [];
    const detalhesErro = [];
    const chamar = async (rotulo, url) => {
      const inicio = Date.now();
      try {
        const resp = await fetch(base + url + '&k=' + K);
        const txt = await resp.text();
        let r = null;
        try { r = JSON.parse(txt); } catch (e) {
          detalhesErro.push({ etapa: rotulo, url, httpStatus: resp.status,
            erro: 'resposta não é JSON', respostaCrua: txt.slice(0, 300) });
          erros.push(rotulo + ': resposta inválida (HTTP ' + resp.status + ')');
          return;
        }
        if (r && r.ok !== false) { feitos.push(rotulo + ': ' + JSON.stringify(r).slice(0, 110)); return; }
        // 🔍 guarda tudo que o endpoint devolveu, para diagnóstico
        detalhesErro.push({ etapa: rotulo, url, httpStatus: resp.status,
          erro: r.error || 'sem mensagem', retornoCompleto: r,
          duracaoMs: Date.now() - inicio });
        erros.push(rotulo + ': ' + (r.error || 'falhou'));
      } catch (e) {
        detalhesErro.push({ etapa: rotulo, url, erro: e.message, tipo: 'exceção' });
        erros.push(rotulo + ': ' + e.message);
      }
    };
    if (String(req.query.aplicar || '') !== '1') {
      return res.status(200).json({ ok: true, modo: 'prévia',
        oQueSeraFeito: [
          quer('remarcar') ? '🔴 devolver ao atendimento as fichas que saíram do Remarcar sem destino (últimos 7 dias)' : null,
          quer('criadas') ? '🟠 mover para Contato Feito as fichas já abordadas presas em Ficha Criada' : null,
          quer('garantia') ? '🟡 enviar à fila de tratamento as garantias de loja que estão fora' : null,
          quer('pendentes') ? '🔴 reprocessar as devoluções que ficaram na fila de pendências' : null,
        ].filter(Boolean),
        naoSeraFeito: [
          'garantia sem tipo — precisa da sua decisão sobre qual tipo atribuir',
          'card sem valor — o valor tem de ser informado por uma pessoa',
          'banco perto do limite — arquivar exige sua autorização',
        ],
        dica: 'para executar: &aplicar=1' });
    }
    if (quer('remarcar')) {
      for (let i = 0; i < 7; i++) {
        const d = new Date(Date.now() - 3 * 3600000 - i * 86400000).toISOString().slice(0, 10);
        await chamar('remarcar ' + d, 'logistica?action=recriar-perdidas&dia=' + d + '&aplicar=1');
      }
    }
    if (quer('pendentes')) await chamar('pendências', 'logistica?action=processar-pendentes&aplicar=1');
    if (quer('criadas')) await chamar('fichas travadas', 'wa-bot?action=destravar-criadas&aplicar=1');
    if (quer('garantia')) await chamar('fila de garantia', 'garantia?action=sincronizar-fila&aplicar=1');
    return res.status(200).json({ ok: erros.length === 0, feitos, erros,
      // 📋 bloco pronto para copiar e enviar ao suporte quando algo falha
      DIAGNOSTICO: detalhesErro.length ? {
        quando: new Date().toISOString(),
        quantasFalharam: detalhesErro.length,
        detalhes: detalhesErro,
        textoParaCopiar: '=== FALHA AO RESOLVER — ' +
          new Date(Date.now() - 3 * 3600000).toISOString().slice(0, 16).replace('T', ' ') + ' BRT ===\n' +
          detalhesErro.map(d => '• ' + d.etapa + '\n  url: ' + (d.url || '?') +
            '\n  http: ' + (d.httpStatus || '?') + '\n  erro: ' + d.erro +
            (d.respostaCrua ? '\n  resposta: ' + d.respostaCrua : '') +
            (d.retornoCompleto ? '\n  retorno: ' + JSON.stringify(d.retornoCompleto).slice(0, 400) : '')
          ).join('\n\n'),
      } : null,
      proximoPasso: erros.length
        ? 'copie o campo textoParaCopiar e envie para o suporte'
        : 'rode o exame-completo de novo para confirmar' });
  }

  // ── 🔎 AUDITAR-RESOLVER: o que as correções realmente fizeram ──
  if (action === 'auditar-resolver') {
    const min = Math.min(720, Math.max(5, parseInt(req.query.minutos || '60', 10)));
    const desde = Date.now() - min * 60000;
    const hh = d => d ? new Date(new Date(d).getTime() - 3 * 3600000).toISOString().slice(5, 16).replace('T', ' ') : '?';
    const achados = [], alertas = [];

    // 1️⃣ fichas recriadas pelo recriar-perdidas
    for (const k of ['fichas_adm', 'fichas_tv']) {
      const b = await dbGet(k);
      for (const f of (((b || {}).fichas) || [])) {
        const q = new Date(f.criadoEm || 0).getTime();
        if (!q || q < desde) continue;
        const id = String(f.id || '');
        if (!id.startsWith('fic_reag_') && !id.startsWith('rem_') && String(f.origem||'') !== 'remarcar') continue;
        achados.push({ etapa: 'remarcar', banco: k, quem: (f.nome || '?') + ' ' + d8(f.telefone).slice(-4),
          oQue: 'ficha devolvida ao atendimento', status: f.status, quando: hh(f.criadoEm) });
      }
    }
    // 2️⃣ fichas destravadas
    for (const k of ['fichas_adm', 'fichas_tv']) {
      const b = await dbGet(k);
      for (const f of (((b || {}).fichas) || [])) {
        const q = new Date(f.destravadaEm || 0).getTime();
        if (!q || q < desde) continue;
        achados.push({ etapa: 'destravar', banco: k, quem: (f.nome || '?') + ' ' + d8(f.telefone).slice(-4),
          oQue: 'movida de Ficha Criada para ' + f.status, quando: hh(f.destravadaEm) });
      }
    }
    // 3️⃣ garantias que entraram na fila
    try {
      const fl = await dbGet('reparoeletro_garantia_fila');
      for (const i of (((fl || {}).itens) || [])) {
        const q = new Date(i.criadoEm || 0).getTime();
        if (!q || q < desde) continue;
        achados.push({ etapa: 'garantia', banco: 'fila', quem: (i.nome || '?') + ' ' + d8(i.telefone).slice(-4),
          oQue: 'entrou na fila de tratamento (' + (i.origem || '?') + ')', quando: hh(i.criadoEm) });
      }
    } catch (e) {}
    // 4️⃣ 📲 MENSAGENS ENVIADAS — a etapa de maior risco
    let enviadas = [];
    try {
      const ev = await dbGet('wa_eventos');
      const lista = Array.isArray(ev) ? ev : ((ev || {}).itens || (ev || {}).eventos || []);
      enviadas = lista.filter(e => {
        const q = new Date(e.ts || 0).getTime();
        return q >= desde && e.dir === 'out' &&
          ['pendente-despachada', 'template'].includes(String(e.tipo || ''));
      }).map(e => ({ tel: String(e.tel || '').slice(-4), tipo: e.tipo,
        texto: String(e.texto || '').slice(0, 90), quando: hh(e.ts) }));
    } catch (e) {}
    for (const m of enviadas) {
      achados.push({ etapa: 'whatsapp', banco: '', quem: m.tel,
        oQue: '📲 MENSAGEM ENVIADA (' + m.tipo + '): ' + m.texto, quando: m.quando });
    }

    // ⚠️ verificações de sanidade
    const porTel = {};
    for (const a of achados) { const t = String(a.quem).slice(-4); (porTel[t] = porTel[t] || []).push(a.etapa); }
    for (const [t, ets] of Object.entries(porTel)) {
      if (ets.length > 2) alertas.push('cliente ' + t + ' foi tocado por ' + ets.length +
        ' etapas (' + [...new Set(ets)].join(', ') + ') — conferir se não duplicou');
    }
    if (enviadas.length > 20) alertas.push('🚨 ' + enviadas.length +
      ' mensagens enviadas de uma vez — volume alto, conferir se foi intencional');
    // ⚠️ contato duplicado de verdade: o cliente tem COLETA ATIVA em paralelo.
    // A ficha de origem continua na logística em fase prospeccao/remarcar — é dela
    // que veio a devolução, então contá-la gerava alarme falso em todos os casos.
    const FASES_ATIVAS = ['em_rota', 'horario_marcado', 'motorista_parceiro',
      'liberado_para_rota', 'coleta_solicitada', 'aguardando_coleta'];
    const comColetaAtiva = new Set();
    for (const k of ['reparoeletro_logistica', 'tv_logistica']) {
      const b = await dbGet(k);
      for (const x of (((b || {}).fichas) || [])) {
        if (!FASES_ATIVAS.includes(String(x.phase || ''))) continue;
        comColetaAtiva.add(d8(x.telefone));
      }
    }
    for (const a of achados.filter(x => x.etapa === 'remarcar')) {
      const t = String(a.quem).replace(/\D/g, '').slice(-4);
      if ([...comColetaAtiva].some(e => e.endsWith(t))) {
        alertas.push('⚠️ ' + a.quem + ' foi devolvido ao atendimento mas já tem COLETA ATIVA marcada — conferir para não ligar em duplicidade');
      }
    }

    const porEtapa = achados.reduce((o, a) => { o[a.etapa] = (o[a.etapa] || 0) + 1; return o; }, {});
    return res.status(200).json({ ok: alertas.length === 0,
      janelaMinutos: min,
      VEREDITO: alertas.length === 0
        ? '✅ tudo dentro do esperado — nenhuma ação fora do previsto'
        : '⚠️ ' + alertas.length + ' ponto(s) merecem conferência',
      totalDeAcoes: achados.length,
      POR_ETAPA: porEtapa,
      MENSAGENS_ENVIADAS: enviadas.length,
      ALERTAS: alertas,
      DETALHE: achados.sort((a, b) => String(b.quando).localeCompare(String(a.quando)))
        .map(a => a.quando + ' | ' + a.etapa.padEnd(10) + ' | ' +
          String(a.quem).slice(0, 24).padEnd(24) + ' | ' + a.oQue) });
  }

  // ── 📈 HISTORICO: evolução dos problemas ao longo dos dias ──
  if (action === 'historico') {
    const h = (await dbGet('vigia_historico')) || { dias: {} };
    return res.status(200).json({ ok: true,
      L: Object.entries(h.dias).sort((a, b) => b[0].localeCompare(a[0])).slice(0, 30)
        .map(([d, v]) => d + ' | ' + v.total + ' problema(s) | ' + JSON.stringify(v.porTipo)) });
  }

  return res.status(400).json({ ok: false, acoes: ['varrer', 'historico'] });
};
