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
    const corte = Date.now() - diasManter * 86400000;
    const FINAIS = ['logistica', 'finalizado', 'descarte', 'cliente_loja', 'concluido'];
    const db = await dbGet(banco);
    if (!db || !Array.isArray(db.fichas)) return res.status(200).json({ ok: false, error: 'banco sem lista fichas' });
    const ficam = [], vao = [];
    for (const f of db.fichas) {
      const q = new Date(f.criadoEm || f.registradoEm || 0).getTime();
      const antiga = q && q < corte;
      const encerrada = FINAIS.includes(String(f.status || ''));
      (antiga && encerrada ? vao : ficam).push(f);
    }
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
    db.fichas = ficam;
    await dbSet(banco, db);
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
        if (t > 900000) add('🟠 MÉDIO', 'Banco', k, '',
          'com ' + (t / 1048576).toFixed(2) + ' MB — perto do limite, gravações podem falhar',
          'arquivar registros antigos');
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
      GRAVES: P.filter(x => x.gravidade.includes('GRAVE'))
        .map(x => x.area + ' | ' + x.quem + ' | ' + x.problema + ' → ' + x.resolver),
      MEDIOS: P.filter(x => x.gravidade.includes('MÉDIO'))
        .map(x => x.area + ' | ' + x.quem + ' | ' + x.problema),
      LEVES: P.filter(x => x.gravidade.includes('LEVE'))
        .map(x => x.area + ' | ' + x.quem + ' | ' + x.problema),
      COMO_RESOLVER: [...new Set(P.map(x => x.resolver).filter(r => r.startsWith('/api')))] });
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
