const U = (process.env.UPSTASH_URL   ||'').replace(/['"]/g,'').trim();
const T = (process.env.UPSTASH_TOKEN ||'').replace(/[\n\r'"]/g,'').trim();

const SHEET_ID   = '1ovSEGZ7if5-wdNZpd1cbLlyg0PZpsrT9fQwOIzfG_mw';
const SHEET_CSV  = `https://docs.google.com/spreadsheets/d/${SHEET_ID}/export?format=csv`;

const KEY_EXCLUIDAS = 'fichas_linhas_excluidas';
const KEY_ADM    = 'fichas_adm';
const KEY_TV     = 'fichas_tv';
const KEY_CURSOR = 'fichas_sheet_cursor';

// ── Redis helpers ────────────────────────────────────────────────────────────
async function dbGet(key) {
  try {
    const r = await fetch(`${U}/get/${key}`, { headers:{ Authorization:`Bearer ${T}` } });
    const j = await r.json();
    return j.result ? JSON.parse(j.result) : null;
  } catch { return null; }
}
async function dbSet(key, val) {
  try {
    await fetch(`${U}/set/${key}`, {
      method:'POST',
      headers:{ Authorization:`Bearer ${T}`, 'Content-Type':'application/json' },
      body: JSON.stringify(val)
    });
    return true;
  } catch { return false; }
}

// ── CSV parser robusto (suporta campos com quebras de linha dentro de aspas) ──
function parseCSV(text) {
  const rows = [];
  const t = text.replace(/\r\n/g,'\n').replace(/\r/g,'\n');
  let i = 0, cols = [], cur = '', inQ = false;

  while (i < t.length) {
    const c = t[i];
    if (inQ) {
      if (c === '"') {
        if (t[i+1] === '"') { cur += '"'; i += 2; } // aspas escapadas
        else { inQ = false; i++; }                    // fecha aspas
      } else {
        cur += c; i++;                                 // conteúdo dentro de aspas (inclui \n)
      }
    } else {
      if (c === '"') { inQ = true; i++; }
      else if (c === ',') { cols.push(cur); cur = ''; i++; }
      else if (c === '\n') {
        cols.push(cur);
        if (cols.some(x => x.trim())) rows.push(cols); // só salva se tem conteúdo
        cols = []; cur = ''; i++;
      } else { cur += c; i++; }
    }
  }
  // última linha
  cols.push(cur);
  if (cols.some(x => x.trim())) rows.push(cols);
  return rows;
}

// ── Detectar sistema pelo equipamento ────────────────────────────────────────
function detectSistema(equip) {
  const e = (equip||'').toLowerCase();
  if (e.includes('tv') || e.includes('televi') || e.includes('monitor') || e.includes('smart')) return 'tv';
  return 'adm';
}

// ── Formatar número para wa.me ────────────────────────────────────────────────
function waNum(tel) {
  const d = String(tel||'').replace(/\D/g,'');
  if (d.startsWith('55') && d.length >= 12) return d;
  if (d.length === 11) return '55' + d;
  return '55' + d;
}

// ── Textos de contato ─────────────────────────────────────────────────────────
const TEXTO_ADM = `Olá, tudo bem? Alessandro aqui, responsável pela logística da Reparo Eletro.\n\nTEMOS 2 OPÇÕES: COLETA E ENTREGA / ATENDIMENTO NO BALCÃO\n\n*ATENÇÃO: Você trazendo aqui na loja seu equipamento o orçamento é gratuito e consertamos em 15 minutos! Estamos na Rua Ouro Preto 663 - Barro Preto*\n\nCaso você prefira usar a nossa coleta e entrega, podemos buscar hoje mesmo na sua casa! Aguardo sua resposta.\n\nJá estamos prontos para te atender! Me fala qual opção escolheu por favor.`;
const TEXTO_TV  = `Olá, tudo bem? Sou o Alessandro, responsável pela Logística da Reparo Eletro - TVs.\n\nPodemos prosseguir com o atendimento?`;

// ── Handler principal ─────────────────────────────────────────────────────────
export default async function handler(req, res) {
  // 🔐 TF-AUTH (Fase 1): chave obrigatória em toda chamada
  const _tfk = (req.query && req.query.k) || req.headers['x-tf-key'] || '';
  if (_tfk !== ((process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim())) {
    return res.status(401).json({ ok: false, error: 'não autorizado' });
  }

  res.setHeader('Access-Control-Allow-Origin', 'https://reparoeletroadm.com');
  res.setHeader('Cache-Control', 'no-cache');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const action = req.query.action || (req.body && req.body.action) || '';

  // ── SYNC: busca novas linhas via CSV público ───────────────────────────────
  // ── 🔎 ENTRADAS-HOJE: por que o contador não bate com a planilha (SOMENTE LEITURA) ──
  if (action === 'entradas-hoje') {
    const ini = new Date(); ini.setHours(0, 0, 0, 0);
    const iniBR = new Date(ini.getTime());
    const [aA, aT] = await Promise.all([dbGet(KEY_ADM), dbGet(KEY_TV)]);
    const todas = [...(((aA || {}).fichas) || []).map(f => ({ ...f, _sis: 'adm' })),
                   ...(((aT || {}).fichas) || []).map(f => ({ ...f, _sis: 'tv' }))];
    const hoje = todas.filter(f => f.criadoEm && new Date(f.criadoEm) >= iniBR);
    const resg = hoje.filter(f => f.resgatada);
    const novas = hoje.filter(f => !f.resgatada);
    // telefone repetido: mesma pessoa entrando mais de uma vez
    const porTel = {};
    for (const f of todas) {
      const k = String(f.telefone || '').replace(/\D/g, '').slice(-8);
      if (k) (porTel[k] = porTel[k] || []).push(f);
    }
    const dupHoje = hoje.filter(f => {
      const k = String(f.telefone || '').replace(/\D/g, '').slice(-8);
      return k && (porTel[k] || []).length > 1;
    });
    const linha = f => [f._sis, 'linha=' + (f.sheetRow != null ? f.sheetRow : '?'),
      (f.resgatada ? 'RESGATADA' : 'nova'), f.nome || '', f.equipamento || '',
      String(f.telefone || '').slice(-8),
      new Date(f.criadoEm).toISOString().slice(11, 16)].join(';');
    if (String(req.query.curto || '') === '1') {
      return res.status(200).send(
        'ENTRADAS HOJE=' + hoje.length + ' (novas=' + novas.length + ' resgatadas=' + resg.length +
        ' telRepetido=' + dupHoje.length + ')\n' + hoje.map(linha).join('\n'));
    }
    return res.status(200).json({ ok: true, hoje: hoje.length,
      novasDeVerdade: novas.length, resgatadas: resg.length, telefoneRepetido: dupHoje.length,
      detalhe: hoje.map(f => ({ sistema: f._sis, sheetRow: f.sheetRow, resgatada: !!f.resgatada,
        nome: f.nome, equipamento: f.equipamento, tel: String(f.telefone || '').slice(-8),
        criadoEm: f.criadoEm })) });
  }

  if (action === 'sync') {
    try {
      const resp = await fetch(SHEET_CSV, { redirect:'follow' });
      if (!resp.ok) return res.status(200).json({ ok:false, error:`HTTP ${resp.status} ao buscar planilha`, novas:0 });

      const text = await resp.text();
      const rows = parseCSV(text);

      // Linha 0 é o header — dados começam na linha 1
      const total = rows.length; // inclui header

      // ── Primeira execução: salva cursor na ÚLTIMA LINHA COM DADOS (não a última da planilha) ──
      const cursor = await dbGet(KEY_CURSOR);
      if (!cursor || cursor.row == null) {
        // Encontrar índice da última linha com conteúdo real
        let ultimaComDado = 0;
        for (let i = rows.length - 1; i >= 1; i--) {
          if (String(rows[i][0]||'').trim() || String(rows[i][1]||'').trim()) { // A=tel ou B=nome
            ultimaComDado = i + 1; // próxima posição após a última linha com dado
            break;
          }
        }
        await dbSet(KEY_CURSOR, { row: ultimaComDado, iniciadoEm: new Date().toISOString() });
        return res.status(200).json({ ok:true, novas:0, totalPlanilha: total, ultimaLinhaComDado: ultimaComDado, msg:`Cursor iniciado na linha ${ultimaComDado} (última com dado). Somente fichas novas serão importadas.` });
      }

      // ── Processar apenas linhas após o cursor ──────────────────────────────
      // Pegar só linhas após cursor E com conteúdo real (ignora linhas vazias pré-alocadas)
      // Apenas linhas onde A (telefone) ou B (nome) têm conteúdo
      const dbAdm = (await dbGet(KEY_ADM)) || { fichas:[] };
      const dbTv  = (await dbGet(KEY_TV))  || { fichas:[] };
      let novas = 0, resgatadas = 0;

      const d8De = t => String(t || '').replace(/\D/g, '').slice(-8);
      // Linhas da planilha que JÁ viraram ficha (o dedupe verdadeiro é por LINHA — telefone repete p/ cliente recorrente)
      const rowsExistentes = new Set(
        [...dbAdm.fichas, ...dbTv.fichas].map(f => f.sheetRow).filter(x => x != null));
      // Linhas excluídas manualmente NUNCA voltam (nem pelo cursor, nem pela rede de resgate)
      try {
        const tombI = (await dbGet(KEY_EXCLUIDAS)) || { linhas: {} };
        for (const k of Object.keys(tombI.linhas || {})) rowsExistentes.add(Number(k));
      } catch (e) {}
      // ASSINATURA da linha (tel+nome+equip): protege contra reimportacao em massa quando os
      // sheetRow antigos foram gravados com formula diferente (linhas vazias desalinhavam)
      const assinar = (tel, nome, equip) =>
        String(tel || '').replace(/\D/g, '').slice(-8) + '|' +
        String(nome || '').trim().toLowerCase().slice(0, 20) + '|' +
        String(equip || '').trim().toLowerCase().slice(0, 20);
      const CORTE_SIG = Date.now() - 60 * 86400000;
      const assinaturas = new Set(
        [...dbAdm.fichas, ...dbTv.fichas]
          .filter(f => new Date(f.criadoEm || 0).getTime() > CORTE_SIG)
          .map(f => f.sheetSig || assinar(f.telefone, f.nome, f.equipamento)));

      // Linhas candidatas: (a) todas após o cursor, com ÍNDICE REAL da planilha;
      // (b) RESGATE — últimas 40 linhas com dado ANTES do cursor cujo telefone nunca virou ficha
      const candidatas = [];
      for (let ri = cursor.row; ri < rows.length; ri++) {
        if (String(rows[ri][0]||'').trim() || String(rows[ri][1]||'').trim()) candidatas.push({ ri, resgate: false });
      }
      let vistasAtras = 0;
      for (let ri = Math.min(cursor.row, rows.length) - 1; ri >= 1 && vistasAtras < 40; ri--) {
        const telB = String(rows[ri][0]||'').replace(/\D/g,'').trim();
        const nomeB = String(rows[ri][1]||'').trim();
        if (!telB && !nomeB) continue;
        vistasAtras++;
        if (!rowsExistentes.has(ri + 1) &&
            !assinaturas.has(assinar(rows[ri][0], rows[ri][1], rows[ri][3] || rows[ri][2]))) {
          candidatas.push({ ri, resgate: true });
        }
      }
      if (!candidatas.length) {
        return res.status(200).json({ ok:true, novas:0, total });
      }

      for (const { ri, resgate } of candidatas) {
        const row    = rows[ri];
        const rowNum = ri + 1; // linha REAL da planilha

        // Estrutura: A=tel, B=nome, C=equip, D=defeito, E=endereço, F=msg, G=horário
        const tel   = String(row[0]||'').replace(/\D/g,'').trim();
        const nome  = String(row[1]||'').trim();
        const equip = String(row[2]||'').trim();
        const def   = String(row[3]||'').trim();
        const end   = String(row[4]||'').trim();
        const hora  = String(row[6]||'').trim();

        if (!nome && !tel) continue;

        // Deduplicação: pular se sheetRow já existe
        const jaExisteSync = dbAdm.fichas.some(f => f.sheetRow === rowNum) ||
                             dbTv.fichas.some(f => f.sheetRow === rowNum);
        if (jaExisteSync) continue;

        const sistema = detectSistema(equip);
        const id = `fsh_${rowNum}_${tel.slice(-4)}_${Date.now().toString(36)}`;

        const ficha = {
          id, sheetRow: rowNum, sheetSig: assinar(tel, nome, equip),
          nome, telefone: tel, endereco: end,
          equipamento: equip, defeito: def, horario: hora,
          sistema, waNum: waNum(tel),
          textoCopiar: sistema === 'tv' ? TEXTO_TV : TEXTO_ADM,
          status: 'criada',
          criadoEm: new Date().toISOString(),
          contatoFeitoEm: null,
          logisticaEm: null,
        };

        if (resgate) ficha.resgatada = true;
        if (sistema === 'tv') dbTv.fichas.unshift(ficha);
        else                  dbAdm.fichas.unshift(ficha);
        rowsExistentes.add(rowNum);
        assinaturas.add(assinar(tel, nome, equip));
        novas++;
        if (resgate) resgatadas++;
      }

      if (novas > 0) {
        await dbSet(KEY_ADM, dbAdm);
        await dbSet(KEY_TV,  dbTv);
      }
      if (resgatadas > 0) console.log(`[fichas sync] ${resgatadas} ficha(s) resgatada(s) de linhas atrás do cursor`);
      // Atualizar cursor para última linha com dado (não total pré-alocado)
      let novoUltimo = cursor.row;
      for (let i = rows.length - 1; i >= 1; i--) {
        if (String(rows[i][0]||'').trim() || String(rows[i][1]||'').trim()) { // A=tel ou B=nome
          novoUltimo = i + 1;
          break;
        }
      }
      await dbSet(KEY_CURSOR, { row: novoUltimo, atualizadoEm: new Date().toISOString() });

      return res.status(200).json({ ok:true, novas, total });
    } catch(e) {
      return res.status(200).json({ ok:false, error: e.message, novas:0 });
    }
  }

  // ── LOAD: retorna fichas com auto-move +24h ────────────────────────────────
  if (action === 'load') {
    const sistema = req.query.sistema || (req.body && req.body.sistema) || 'adm';
    const key = sistema === 'tv' ? KEY_TV : KEY_ADM;
    const db  = (await dbGet(key)) || { fichas:[] };
    const agora = Date.now();
    let mudou = false;
    const novosEntrar = [];
    for (const f of db.fichas) {
      if (f.status === 'contato_feito' && f.contatoFeitoEm) {
        // Bot abordou → 1 hora para cadastrar na logística; contato manual → 24 horas
        const limiteMs = f.abordadoPorBot ? 60*60*1000 : 24*60*60*1000;
        if (agora - new Date(f.contatoFeitoEm).getTime() > limiteMs) {
          f.status = 'entrar_contato';
          mudou = true;
          novosEntrar.push(f);
        }
      }
    }
    if (mudou) await dbSet(key, db);
    // Evento para o relatório da prospecção (espelho Entrar em Contato)
    if (novosEntrar.length > 0) {
      try {
        const sisEvt = key === 'fichas_tv' ? 'tv' : 'adm';
        const tsEvt = new Date().toISOString();
        for (const fEvt of novosEntrar) {
          const evt = JSON.stringify({ ts: tsEvt, tipo: 'entrar_contato', de: null, sis: sisEvt, id: fEvt.id, nome: fEvt.nome });
          await fetch(`${U}/rpush/prospeccao_evt_list/${encodeURIComponent(evt)}`, { headers: { Authorization: `Bearer ${T}` } });
        }
      } catch(_) {}
    }
    return res.status(200).json({ ok:true, fichas: db.fichas });
  }

  // ── MOVER-CONTATO ─────────────────────────────────────────────────────────
  if (req.method === 'POST' && action === 'mover-contato') {
    const { id, sistema } = req.body || {};
    const key = sistema === 'tv' ? KEY_TV : KEY_ADM;
    const db  = (await dbGet(key)) || { fichas:[] };
    const f   = db.fichas.find(x => x.id === id);
    if (!f) return res.status(404).json({ ok:false, error:'Não encontrado' });
    f.status = 'contato_feito';
    f.contatoFeitoEm = new Date().toISOString();
    await dbSet(key, db);
    return res.status(200).json({ ok:true });
  }

  // ── CADASTRAR-LOGISTICA ───────────────────────────────────────────────────
  if (req.method === 'POST' && action === 'cadastrar-logistica') {
    const { id, sistema } = req.body || {};
    const key   = sistema === 'tv' ? KEY_TV : KEY_ADM;
    const db    = (await dbGet(key)) || { fichas:[] };
    const ficha = db.fichas.find(x => x.id === id);
    if (!ficha) return res.status(404).json({ ok:false, error:'Não encontrado' });

    // TV → tv_logistica | ADM → reparoeletro_logistica
    const LOG_KEY = sistema === 'tv' ? 'tv_logistica' : 'reparoeletro_logistica';
    const logDb   = (await dbGet(LOG_KEY)) || { fichas:[] };
    const tipoColeta   = req.body.tipoColeta  || 'imediato';
    const dataAgendada = req.body.dataAgendada || null;
    const faixaHorario = req.body.faixaHorario || null;
    const origemTipo   = req.body.origemTipo === 'ativa' ? 'ativa' : 'passiva'; // fichas: default passiva
    // Dados conferidos/corrigidos no modal (principalmente endereço)
    const dados = req.body.dados;
    if(dados&&typeof dados==='object'){
      if(dados.nome)ficha.nome=dados.nome;
      if(dados.telefone)ficha.telefone=String(dados.telefone).replace(/\D/g,'');
      if(dados.equipamento)ficha.equipamento=dados.equipamento;
      if(dados.defeito)ficha.defeito=dados.defeito;
      if(dados.endereco)ficha.endereco=dados.endereco;
    }
    const obsLog = (dados && dados.observacao) ? String(dados.observacao).trim() : '';
    // imediato → liberado_coleta | agendado → horario_marcado
    const phase = tipoColeta === 'agendado' ? 'horario_marcado' : 'liberado_coleta';
    // Montar horarioColeta no formato datetime-local que a logística usa
    let horarioColeta = null;
    if (tipoColeta === 'agendado' && dataAgendada && faixaHorario) {
      const horaInicio = faixaHorario.split(' - ')[0] || '08:00';
      horarioColeta = `${dataAgendada}T${horaInicio}`;
    }
    logDb.fichas.unshift({
      id:           'log_' + Date.now().toString(36),
      observacao:   obsLog,
      nome:         ficha.nome,
      telefone:     ficha.telefone,
      endereco:     ficha.endereco,
      equipamento:  ficha.equipamento,
      defeito:      ficha.defeito,
      phase,
      dataAgendada: dataAgendada || null,
      faixaHorario: faixaHorario || null,
      horarioColeta,
      origem:       'ficha_planilha',
      origemTipo,
      criadoEm:     new Date().toISOString(),
      movedAt:      new Date().toISOString(),
    });
    await dbSet(LOG_KEY, logDb);

    const stAntesLog    = ficha.status;
    ficha.status        = 'logistica';
    ficha.logisticaEm   = new Date().toISOString();
    ficha.logisticaTipo = origemTipo;
    // Evento p/ árvore da prospecção: só quando vem da coluna espelhada
    if (stAntesLog === 'entrar_contato') {
      try {
        const evt2 = JSON.stringify({ ts: new Date().toISOString(), tipo: 'logistica', de: 'entrar_contato',
          sis: sistema === 'tv' ? 'tv' : 'adm', id: ficha.id, nome: ficha.nome });
        await fetch(`${U}/rpush/prospeccao_evt_list/${encodeURIComponent(evt2)}`, { headers: { Authorization: `Bearer ${T}` } });
      } catch(_) {}
    }
    await dbSet(key, db);
    return res.status(200).json({ ok:true });
  }

  // ── EXCLUIR ───────────────────────────────────────────────────────────────
  // ── 🔎 CONFERIR-DUPLICATA: o cliente existe em outro lugar? (chamado antes de excluir) ──
  if (action === 'conferir-duplicata') {
    const tel = String(req.query.tel || '').replace(/\D/g, '').slice(-8);
    const idAtual = String(req.query.id || '');
    if (tel.length < 8) return res.status(400).json({ ok: false, error: 'informe ?tel=' });
    const [fA, fT, lgA, lgT, pros, ppA, ppT] = await Promise.all([
      dbGet(KEY_ADM), dbGet(KEY_TV), dbGet('reparoeletro_logistica'), dbGet('tv_logistica'),
      dbGet('prospeccao_adm'), dbGet('reparoeletro_pipe'), dbGet('tv_pipe'),
    ]);
    const bate = t => String(t || '').replace(/\D/g, '').slice(-8) === tel;
    const achados = [];
    const varre = (arr, rot, campo) => { for (const x of (arr || [])) {
      if (!bate(x.telefone) || x.id === idAtual) continue;
      achados.push(rot + ': ' + (x[campo] || x.status || x.phase || '')); } };
    varre(((fA || {}).fichas), 'ficha ADM', 'status');
    varre(((fT || {}).fichas), 'ficha TV', 'status');
    varre(((lgA || {}).fichas), 'logística ADM', 'phase');
    varre(((lgT || {}).fichas), 'logística TV', 'phase');
    varre(((pros || {}).fichas), 'prospecção', 'status');
    for (const c of (((ppA || {}).cards) || [])) if (bate(c.telefone)) achados.push('pipe ADM: ' + (c.phaseId || c.phase || ''));
    for (const c of (((ppT || {}).cards) || [])) if (bate(c.telefone)) achados.push('pipe TV: ' + (c.phaseId || c.phase || ''));
    return res.status(200).json({ ok: true, ehDuplicata: achados.length > 0,
      ondeMais: achados.slice(0, 8),
      aviso: achados.length ? null : 'este cliente NÃO aparece em nenhum outro lugar do sistema — não é duplicata' });
  }

  if (req.method === 'POST' && action === 'excluir') {
    const { id, sistema } = req.body || {};
    const key = sistema === 'tv' ? KEY_TV : KEY_ADM;
    const db  = (await dbGet(key)) || { fichas:[] };
    const alvo = (db.fichas || []).find(x => x.id === id);
    // TOMBSTONE DA LINHA: sem isto a rede de resgate reimportava a linha no ciclo seguinte
    if (alvo && alvo.sheetRow != null) {
      try {
        const tomb = (await dbGet(KEY_EXCLUIDAS)) || { linhas: {} };
        if (!tomb.linhas) tomb.linhas = {};
        // LIXEIRA: guarda a ficha INTEIRA para poder restaurar, com autor e motivo
        tomb.linhas[String(alvo.sheetRow)] = { em: new Date().toISOString(),
          nome: alvo.nome || '', tel: alvo.telefone || '',
          equipamento: alvo.equipamento || '', status: alvo.status || 'criada',
          por: String((req.body || {}).por || 'não informado').slice(0, 40),
          motivo: String((req.body || {}).motivo || 'não informado').slice(0, 120),
          origem: String((req.body || {}).origem || 'tela de fichas').slice(0, 30),
          ficha: alvo };
        const corte = Date.now() - 180 * 86400000;
        for (const k of Object.keys(tomb.linhas)) {
          if (new Date(tomb.linhas[k].em || 0).getTime() < corte) delete tomb.linhas[k];
        }
        await dbSet(KEY_EXCLUIDAS, tomb);
      } catch (e) {}
    }
    const dbF = (await dbGet(key)) || db;              // relê (o sync pode ter gravado no meio)
    dbF.fichas = (dbF.fichas || []).filter(x => x.id !== id);
    await dbSet(key, dbF);
    return res.status(200).json({ ok:true, linhaBloqueada: alvo ? alvo.sheetRow : null });
  }

  // ── 🔎 CRUZAR-PLANILHA: compara linha a linha a planilha com as fichas do sistema ──
  if (action === 'cruzar-planilha') {
    const dias = Math.min(30, Math.max(0, parseInt(req.query.dias || '0', 10)));   // 0 = só hoje
    const resp = await fetch(SHEET_CSV);
    const csv = await resp.text();
    // MESMO parser do importador — endereço e defeito têm quebra de linha dentro das aspas,
    // e dividir por linha física quebra o alinhamento das colunas
    const rows = parseCSV(csv);
    const [dbAdm, dbTv, cursor, tomb] = await Promise.all([
      dbGet(KEY_ADM), dbGet(KEY_TV), dbGet(KEY_CURSOR), dbGet(KEY_EXCLUIDAS),
    ]);
    const todas = [...(((dbAdm || {}).fichas) || []), ...(((dbTv || {}).fichas) || [])];
    const porRow = new Map(); const porTel = new Map();
    for (const f of todas) {
      if (f.sheetRow != null) porRow.set(Number(f.sheetRow), f);
      const d = String(f.telefone || '').replace(/\D/g, '').slice(-8);
      if (d.length >= 8) { if (!porTel.has(d)) porTel.set(d, []); porTel.get(d).push(f); }
    }
    const bloqueadas = new Set(Object.keys(((tomb || {}).linhas) || {}).map(Number));
    // hoje em Brasília
    const hojeBrt = new Date(Date.now() - 3 * 3600 * 1000);
    const alvoDia = hojeBrt.toISOString().slice(0, 10);
    const limiteDias = dias;
    const ehDoPeriodo = (txtHora) => {
      const s = String(txtHora || '');
      const m = s.match(/(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})/);
      if (!m) return null;
      let [_, d1, m1, a1] = m;
      if (a1.length === 2) a1 = '20' + a1;
      const dt = new Date(Date.UTC(Number(a1), Number(m1) - 1, Number(d1)));
      const iso = dt.toISOString().slice(0, 10);
      if (limiteDias === 0) return iso === alvoDia;
      return dt.getTime() >= Date.now() - 3 * 3600 * 1000 - limiteDias * 86400000;
    };
    const naPlanilha = [], faltando = [], semData = [];
    for (let ri = 1; ri < rows.length; ri++) {
      const row = rows[ri];
      const tel = String(row[0] || '').replace(/\D/g, '').trim();
      const nome = String(row[1] || '').trim();
      const equip = String(row[2] || '').trim();
      const hora = String(row[6] || row[5] || row[7] || '').trim();
      if (!tel && !nome) continue;
      const noPeriodo = ehDoPeriodo(hora);
      if (noPeriodo === null) { if (ri > rows.length - 40) semData.push({ linha: ri + 1, nome, tel, hora }); continue; }
      if (!noPeriodo) continue;
      const rowNum = ri + 1;
      const ficha = porRow.get(rowNum);
      const d8 = tel.slice(-8);
      const porTelefone = porTel.get(d8) || [];
      const item = { linha: rowNum, nome, telefone: tel, equipamento: equip, horario: hora,
        temFicha: !!ficha, statusFicha: ficha ? (ficha.status || 'criada') : null,
        fichasMesmoTelefone: porTelefone.length };
      naPlanilha.push(item);
      if (!ficha) {
        let motivo = 'motivo não identificado';
        if (bloqueadas.has(rowNum)) motivo = 'linha foi EXCLUÍDA manualmente (bloqueada para não voltar)';
        else if (porTelefone.length) motivo = 'existe ficha do mesmo telefone em outra linha (' +
          porTelefone.map(f => f.sheetRow).join(', ') + ') — possível duplicata do cliente';
        else if (cursor && rowNum > (cursor.row || 0) + 1) motivo = 'linha ainda não alcançada pelo cursor (sync pendente)';
        else if (!tel || tel.length < 10) motivo = 'telefone inválido ou ausente na planilha';
        faltando.push(Object.assign({ motivo }, item));
      }
    }
    return res.status(200).json({ ok: true,
      periodo: dias === 0 ? 'hoje (' + alvoDia + ')' : 'últimos ' + dias + ' dias',
      cursorAtual: (cursor || {}).row || null, totalLinhasPlanilha: rows.length - 1,
      avisoAlinhamento: 'o parser descarta linhas totalmente vazias — se alguém preencher uma linha vazia no meio da planilha, os números de linha deslocam e o pareamento com as fichas antigas se perde',
      naPlanilha: naPlanilha.length, comFicha: naPlanilha.filter(x => x.temFicha).length,
      semFicha: faltando.length,
      faltando,
      linhasSemDataReconhecida: semData.slice(0, 10),
      detalhe: naPlanilha });
  }

  // ── 🔁 RECUPERAR-PERDIDAS: acha e devolve as linhas da planilha que não viraram ficha ──
  // Prévia por padrão; &aplicar=1 executa. Só mexe no período pedido (padrão 7 dias).
  if (action === 'recuperar-perdidas') {
    const dias = Math.min(30, Math.max(1, parseInt(req.query.dias || '7', 10)));
    const resp = await fetch(SHEET_CSV, { redirect: 'follow' });
    const rows = parseCSV(await resp.text());
    // A ficha SAI de fichas_adm/fichas_tv quando avança (logística, prospecção, pipe, arquivo).
    // Procurar só ali fazia parecer perdida quem na verdade foi atendida.
    const [dbAdm, dbTv, cursor, tomb, lgA, lgT, pros, ppA, ppT, arqA, arqT] = await Promise.all([
      dbGet(KEY_ADM), dbGet(KEY_TV), dbGet(KEY_CURSOR), dbGet(KEY_EXCLUIDAS),
      dbGet('reparoeletro_logistica'), dbGet('tv_logistica'), dbGet('prospeccao_adm'),
      dbGet('reparoeletro_pipe'), dbGet('tv_pipe'),
      dbGet('reparoeletro_arquivo'), dbGet('tv_arquivo'),
    ]);
    const todas = [...(((dbAdm || {}).fichas) || []), ...(((dbTv || {}).fichas) || [])];
    const porRow = new Set(todas.map(f => Number(f.sheetRow)).filter(x => !isNaN(x)));
    // ONDE o cliente aparece em toda a operação (por telefone)
    const ondeEsta = new Map();
    const marcar = (tel, lugar) => {
      const d = String(tel || '').replace(/\D/g, '').slice(-8);
      if (d.length < 8) return;
      if (!ondeEsta.has(d)) ondeEsta.set(d, new Set());
      ondeEsta.get(d).add(lugar);
    };
    for (const f of (((dbAdm || {}).fichas) || [])) marcar(f.telefone, 'ficha ADM (' + (f.status || 'criada') + ')');
    for (const f of (((dbTv || {}).fichas) || [])) marcar(f.telefone, 'ficha TV (' + (f.status || 'criada') + ')');
    for (const f of (((lgA || {}).fichas) || [])) marcar(f.telefone, 'logística ADM');
    for (const f of (((lgT || {}).fichas) || [])) marcar(f.telefone, 'logística TV');
    for (const f of (((pros || {}).fichas) || [])) marcar(f.telefone, 'prospecção (' + (f.status || '') + ')');
    for (const c of (((ppA || {}).cards) || [])) marcar(c.telefone, 'pipe ADM (' + (c.phaseId || c.phase || '') + ')');
    for (const c of (((ppT || {}).cards) || [])) marcar(c.telefone, 'pipe TV (' + (c.phaseId || c.phase || '') + ')');
    for (const c of (((arqA || {}).cards) || [])) marcar(c.telefone, 'arquivo ADM');
    for (const c of (((arqT || {}).cards) || [])) marcar(c.telefone, 'arquivo TV');
    const porTel = new Set([...ondeEsta.keys()]);
    const bloqueadas = ((tomb || {}).linhas) || {};
    const corte = Date.now() - 3 * 3600 * 1000 - dias * 86400000;
    const parseData = (s) => {
      const m = String(s || '').match(/(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})/);
      if (!m) return null;
      let a = m[3]; if (a.length === 2) a = '20' + a;
      return new Date(Date.UTC(Number(a), Number(m[2]) - 1, Number(m[1]))).getTime();
    };
    const perdidas = [];
    for (let ri = 1; ri < rows.length; ri++) {
      const row = rows[ri];
      const tel = String(row[0] || '').replace(/\D/g, '').trim();
      const nome = String(row[1] || '').trim();
      const equip = String(row[2] || '').trim();
      const hora = String(row[6] || row[5] || row[7] || '').trim();
      if (!tel && !nome) continue;
      const dt = parseData(hora);
      if (dt == null || dt < corte) continue;                 // fora do período
      const rowNum = ri + 1;
      if (porRow.has(rowNum)) continue;                       // já virou ficha
      const d8 = tel.slice(-8);
      const lugares = d8.length >= 8 && ondeEsta.has(d8) ? [...ondeEsta.get(d8)] : [];
      const estaNaOperacao = lugares.length > 0;
      const bloq = !!bloqueadas[String(rowNum)];
      let acao;
      if (estaNaOperacao) acao = 'JÁ FOI ATENDIDA — está em: ' + lugares.join(' | ');
      else if (bloq) acao = 'desbloquear e reimportar';
      else acao = 'forçar reimportação';
      perdidas.push({ linha: rowNum, nome, telefone: tel, equipamento: equip, horario: hora,
        bloqueada: bloq,
        bloqueadaEm: bloq ? bloqueadas[String(rowNum)].em : null,
        ondeEstaOCliente: lugares, acao });
    }
    const recuperaveis = perdidas.filter(p => !String(p.acao).startsWith('JÁ FOI ATENDIDA'));
    if (String(req.query.aplicar || '') === '1' && recuperaveis.length) {
      // 1) desbloqueia as linhas
      const t2 = (await dbGet(KEY_EXCLUIDAS)) || { linhas: {} };
      for (const p of recuperaveis) delete (t2.linhas || {})[String(p.linha)];
      await dbSet(KEY_EXCLUIDAS, t2);
      // 2) volta o cursor para antes da mais antiga, para o sync alcançá-las
      const menor = Math.min.apply(null, recuperaveis.map(p => p.linha));
      const cur = (await dbGet(KEY_CURSOR)) || {};
      if ((cur.row || 0) >= menor) {
        await dbSet(KEY_CURSOR, { row: menor - 1, ajustadoEm: new Date().toISOString(), motivo: 'recuperação de fichas perdidas' });
      }
      // 3) limpa o registro de "já abordado" desses telefones — a ficha volta para ser atendida do zero
      try {
        const ab = (await dbGet('wa_abordados')) || { tels: {} };
        let limpos = 0;
        for (const p of recuperaveis) {
          const d = String(p.telefone || '').replace(/\D/g, '').slice(-8);
          if (d.length >= 8 && ab.tels && ab.tels[d]) { delete ab.tels[d]; limpos++; }
        }
        if (limpos) await dbSet('wa_abordados', ab);
      } catch (e) {}
      // 4) marca as fichas recuperadas para a autocura não reclassificá-las nas primeiras 24h
      try {
        const agoraR = new Date().toISOString();
        const linhasRec = new Set(recuperaveis.map(p => Number(p.linha)));
        for (const key of [KEY_ADM, KEY_TV]) {
          const bd = (await dbGet(key)) || { fichas: [] };
          let mexeu = false;
          for (const f of (bd.fichas || [])) {
            if (linhasRec.has(Number(f.sheetRow)) && !f.recuperadaEm) { f.recuperadaEm = agoraR; mexeu = true; }
          }
          if (mexeu) await dbSet(key, bd);
        }
      } catch (e) {}
      return res.status(200).json({ ok: true, modo: 'APLICADO',
        desbloqueadas: recuperaveis.length, cursorVoltouPara: menor - 1,
        proximoPasso: 'o sync do próximo ciclo (5 min) reimporta — ou rode /api/fichas?action=sync agora',
        recuperadas: recuperaveis, ignoradas: perdidas.filter(p => !recuperaveis.includes(p)) });
    }
    // ?curto=1 → uma linha curta por ficha, para caber no chat
    if (String(req.query.curto || '') === '1') {
      const linha = p => (p.nome || '?').slice(0, 18) + ' ' + String(p.telefone || '').slice(-4) +
        ' | ' + String(p.equipamento || '').slice(0, 18) +
        ' | ' + String(p.horario || '').slice(0, 14) +
        ' | ' + (p.bloqueada ? 'excluída' : 'não importada');
      return res.status(200).json({ ok: true,
        dias, perdidas: recuperaveis.length, jaAtendidas: perdidas.length - recuperaveis.length,
        lista: recuperaveis.slice(0, 30).map(linha),
        dica: 'para recuperar: trocar &curto=1 por &aplicar=1' });
    }
    return res.status(200).json({ ok: true, modo: 'prévia (nada foi alterado)',
      periodoDias: dias, cursorAtual: (cursor || {}).row || null,
      linhasSemFichaNoBancoDeCriadas: perdidas.length,
      jaAtendidas: perdidas.length - recuperaveis.length,
      realmentePerdidas: recuperaveis.length,
      lista: recuperaveis,
      jaAtendidasDetalhe: perdidas.filter(p => String(p.acao).startsWith('JÁ FOI ATENDIDA')).slice(0, 20),
      dica: 'para recuperar: mesmo link com &aplicar=1' });
  }

  // ── 🔬 RAIO-X DO DIA: planilha → ficha → status → mensagem do bot → resposta ──
  if (action === 'raio-x-dia') {
    const dias = Math.min(7, Math.max(0, parseInt(req.query.dias || '0', 10)));
    const rows = parseCSV(await (await fetch(SHEET_CSV, { redirect: 'follow' })).text());
    const [dbAdm, dbTv, lgA, lgT, pros, ppA, ppT, tombDb] = await Promise.all([
      dbGet(KEY_ADM), dbGet(KEY_TV), dbGet('reparoeletro_logistica'), dbGet('tv_logistica'),
      dbGet('prospeccao_adm'), dbGet('reparoeletro_pipe'), dbGet('tv_pipe'), dbGet(KEY_EXCLUIDAS),
    ]);
    const tombRX = ((tombDb || {}).linhas) || {};
    // eventos do WhatsApp
    let evts = [];
    try {
      const r = await fetch(`${U}/lrange/wa_evt_list/-5000/-1`, { headers: { Authorization: `Bearer ${T}` } });
      const j = await r.json();
      evts = (j.result || []).map(x => { try { return JSON.parse(x); } catch (e) { return null; } }).filter(Boolean);
    } catch (e) {}
    const d8 = t => String(t || '').replace(/\D/g, '').slice(-8);
    const enviadas = {}, recebidas = {};
    for (const e of evts) {
      const d = d8(e.tel); if (d.length < 8) continue;
      const t = new Date(e.ts || 0).getTime();
      if (e.dir === 'out') { if (!enviadas[d] || t > enviadas[d]) enviadas[d] = t; }
      if (e.dir === 'in') { if (!recebidas[d] || t > recebidas[d]) recebidas[d] = t; }
    }
    // onde a ficha está
    const onde = {};
    const marca = (tel, lugar) => { const d = d8(tel); if (d.length < 8) return;
      if (!onde[d]) onde[d] = []; onde[d].push(lugar); };
    for (const f of (((dbAdm || {}).fichas) || [])) marca(f.telefone, 'ficha ADM: ' + (f.status || 'criada'));
    for (const f of (((dbTv || {}).fichas) || [])) marca(f.telefone, 'ficha TV: ' + (f.status || 'criada'));
    for (const f of (((lgA || {}).fichas) || [])) marca(f.telefone, 'logística ADM: ' + (f.phase || ''));
    for (const f of (((lgT || {}).fichas) || [])) marca(f.telefone, 'logística TV: ' + (f.phase || ''));
    for (const f of (((pros || {}).fichas) || [])) marca(f.telefone, 'prospecção: ' + (f.status || ''));
    for (const c of (((ppA || {}).cards) || [])) marca(c.telefone, 'pipe ADM: ' + (c.phaseId || c.phase || ''));
    for (const c of (((ppT || {}).cards) || [])) marca(c.telefone, 'pipe TV: ' + (c.phaseId || c.phase || ''));

    const hojeBrt = new Date(Date.now() - 3 * 3600 * 1000).toISOString().slice(0, 10);
    const corte = Date.now() - 3 * 3600 * 1000 - dias * 86400000;
    const parseData = s => { const m = String(s || '').match(/(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})/);
      if (!m) return null; let a = m[3]; if (a.length === 2) a = '20' + a;
      return new Date(Date.UTC(Number(a), Number(m[2]) - 1, Number(m[1]))).getTime(); };

    const linhas = [];
    for (let ri = 1; ri < rows.length; ri++) {
      const row = rows[ri];
      const tel = String(row[0] || '').replace(/\D/g, '').trim();
      const nome = String(row[1] || '').trim();
      const equip = String(row[2] || '').trim();
      const hora = String(row[6] || row[5] || row[7] || '').trim();
      if (!tel && !nome) continue;
      const dt = parseData(hora);
      if (dt == null) continue;
      const iso = new Date(dt).toISOString().slice(0, 10);
      if (dias === 0 ? iso !== hojeBrt : dt < corte) continue;
      const d = tel.slice(-8);
      const lugares = onde[d] || [];
      const temMsg = !!enviadas[d];
      const respondeu = !!recebidas[d];
      let diagnostico;
      if (!lugares.length) {
        const bloq = tombRX[String(ri + 1)];
        diagnostico = bloq
          ? '🔴 EXCLUÍDA (' + (bloq.motivo || 'motivo não informado') + ')'
          : '🔴 NÃO ENTROU NO SISTEMA';
      }
      else if (temMsg && respondeu) diagnostico = '🟢 abordado e respondeu';
      else if (temMsg) diagnostico = '🟡 abordado, sem resposta';
      else if (lugares.some(l => /log[íi]stica|pipe|cliente_loja|prospeccao|prospecção/.test(l))) diagnostico = '🔵 avançou sem o bot falar (cadastro manual)';
      else diagnostico = '🔴 NO SISTEMA MAS SEM MENSAGEM DO BOT';
      linhas.push({ linha: ri + 1, nome, telefone: tel, equipamento: equip, horario: hora,
        diagnostico, ondeEsta: lugares,
        botEnviou: temMsg ? new Date(enviadas[d]).toISOString() : null,
        clienteRespondeu: respondeu ? new Date(recebidas[d]).toISOString() : null });
    }
    const conta = p => linhas.filter(l => l.diagnostico.startsWith(p)).length;
    const resumo = {
      totalPlanilha: linhas.length,
      abordadoEResponde: conta('🟢'),
      abordadoSemResposta: conta('🟡'),
      avancouSemBotFalar: conta('🔵'),
      semMensagemDoBot: linhas.filter(l => l.diagnostico.includes('SEM MENSAGEM')).length,
      naoEntrouNoSistema: linhas.filter(l => l.diagnostico.includes('NÃO ENTROU')).length,
      excluidasNaLixeira: linhas.filter(l => l.diagnostico.includes('EXCLUÍDA')).length,
    };
    // ?curto=1 → só o essencial, uma linha por caso (cabe no chat)
    if (String(req.query.curto || '') === '1') {
      const compacta = l => l.nome + ' ' + l.telefone.slice(-4) + ' | ' + l.equipamento.slice(0, 22) +
        ' | ' + l.horario.slice(-5) + ' | ' + l.diagnostico +
        (l.ondeEsta.length ? ' | ' + l.ondeEsta[0] : '');
      return res.status(200).json({ ok: true, dia: dias === 0 ? hojeBrt : 'últimos ' + dias + ' dias',
        resumo,
        problemas: linhas.filter(l => l.diagnostico.startsWith('🔴')).map(compacta),
        avancouSemBot: linhas.filter(l => l.diagnostico.startsWith('🔵')).map(compacta) });
    }
    return res.status(200).json({ ok: true, dia: dias === 0 ? hojeBrt : 'últimos ' + dias + ' dias',
      resumo,
      problemas: linhas.filter(l => l.diagnostico.startsWith('🔴')),
      todas: linhas });
  }

  // ── 🗑 LIXEIRA: fichas excluídas, com restauração ──
  // ── 📊 CENSO-FASES: quantos registros existem em CADA fase, em TODOS os bancos ──
  if (action === 'censo-fases') {
    const bancos = [
      ['fichas_adm','Fichas ADM'],['fichas_tv','Fichas TV'],
      ['reparoeletro_logistica','Logística ADM'],['tv_logistica','Logística TV'],
      ['reparoeletro_pipe','Pipe ADM'],['tv_pipe','Pipe TV'],
      ['reparoeletro_arquivo','Arquivo ADM'],['tv_arquivo','Arquivo TV'],
      ['reparoeletro_logistica_arquivo','Arq. Log. ADM'],['tv_logistica_arquivo','Arq. Log. TV'],
      ['prospeccao_adm','Prospecção'],['reparoeletro_frenteloja','Frente de Loja'],
      ['reparoeletro_balcao','Balcão'],['wa_arquivadas','Arq. WhatsApp'],
    ];
    const filtro = String(req.query.fase || '').toLowerCase();
    const out = {}; const amostras = {};
    for (const [chave, rot] of bancos) {
      const b = await dbGet(chave);
      if (!b) continue;
      const itens = (b.fichas || []).concat(b.cards || []).concat(Array.isArray(b) ? b : []);
      for (const x of itens) {
        const f = String(x.phaseId || x.phase || x.status || x.faseId || '(sem fase)');
        if (filtro && !f.toLowerCase().includes(filtro)) continue;
        const k = rot + ' · ' + f;
        out[k] = (out[k] || 0) + 1;
        if (filtro) {
          amostras[k] = amostras[k] || [];
          if (amostras[k].length < 40) {
            const d = new Date(x.movedAt || x.movidaEm || x.criadoEm || 0).getTime();
            amostras[k].push((x.nome || x.nomeContato || '?').slice(0,20) + ' ' +
              String(x.telefone || '').slice(-4) +
              (d ? ' · ' + ((Date.now()-d)/86400000).toFixed(1) + 'd' : ''));
          }
        }
      }
    }
    const ord = Object.keys(out).sort((a,b) => out[b]-out[a]);
    return res.status(200).json({ ok: true, filtro: filtro || '(todas)',
      totalRegistros: Object.values(out).reduce((s,x)=>s+x,0),
      CONTAGEM: ord.map(k => k + ' → ' + out[k]),
      amostras: filtro ? amostras : undefined });
  }

  // ── 🔎 LOTE-FASES: recebe uma lista de 4 dígitos e devolve fase e tempo de cada um ──
  if (action === 'lote-fases') {
    const bruto = String(req.query.d || req.query.digitos || '');
    const alvos = [...new Set(bruto.split(/[,;\s]+/).map(x => x.replace(/\D/g, '')).filter(x => x.length >= 4))];
    if (!alvos.length) return res.status(400).json({ ok: false, error: 'informe ?d=4115,3188,0432...' });

    const [fA, fT, lgA, lgT, ppA, ppT, arqA, arqT, pros, gar, fl, balc, orcA, orcT] = await Promise.all([
      dbGet('fichas_adm'), dbGet('fichas_tv'),
      dbGet('reparoeletro_logistica'), dbGet('tv_logistica'),
      dbGet('reparoeletro_pipe'), dbGet('tv_pipe'),
      dbGet('reparoeletro_arquivo'), dbGet('tv_arquivo'),
      dbGet('prospeccao_adm'), dbGet('reparoeletro_garantia_v2'),
      dbGet('reparoeletro_frenteloja'), dbGet('reparoeletro_balcao'),
      dbGet('reparoeletro_orcamentos'), dbGet('tv_orcamentos'),
    ]);
    // 📦 ARQUIVOS DA LOGÍSTICA — é onde ficam última chamada, descarte e afins.
    // Sem eles a busca não achava quase nada dessas fases.
    const [lgArqA, lgArqT, waArq, finArq] = await Promise.all([
      dbGet('reparoeletro_logistica_arquivo'), dbGet('tv_logistica_arquivo'),
      dbGet('wa_arquivadas'), dbGet('reparoeletro_financeiro_arquivo'),
    ]);
    const dias = ts => { const t = new Date(ts || 0).getTime(); return t ? Number(((Date.now() - t) / 86400000).toFixed(1)) : null; };
    const so = t => String(t || '').replace(/\D/g, '');
    const achados = {};
    const guarda = (tel, onde, fase, nome, equip, quando, extra) => {
      const d = so(tel); if (d.length < 4) return;
      for (const a of alvos) {
        if (!d.endsWith(a)) continue;
        achados[a] = achados[a] || [];
        achados[a].push({ onde, fase: fase || '(sem fase)', nome: nome || '', 
          equipamento: String(equip || '').slice(0, 24),
          diasNaFase: dias(quando), ...(extra || {}) });
      }
    };
    for (const f of (((fA || {}).fichas) || [])) guarda(f.telefone, 'Fichas ADM', f.status, f.nome, f.equipamento, f.movedAt || f.criadoEm);
    for (const f of (((fT || {}).fichas) || [])) guarda(f.telefone, 'Fichas TV', f.status, f.nome, f.equipamento, f.movedAt || f.criadoEm);
    for (const f of (((lgA || {}).fichas) || [])) guarda(f.telefone, 'Logística ADM', f.phase, f.nome, f.equipamento, f.movedAt || f.criadoEm);
    for (const f of (((lgT || {}).fichas) || [])) guarda(f.telefone, 'Logística TV', f.phase, f.nome, f.equipamento, f.movedAt || f.criadoEm);
    for (const c of (((ppA || {}).cards) || [])) guarda(c.telefone, 'Pipe ADM', c.phaseId || c.phase, c.nomeContato, c.equipamento, c.movedAt || c.criadoEm);
    for (const c of (((ppT || {}).cards) || [])) guarda(c.telefone, 'Pipe TV', c.phaseId || c.phase, c.nomeContato, c.equipamento, c.movedAt || c.criadoEm);
    for (const c of (((arqA || {}).cards) || [])) guarda(c.telefone, 'Arquivo ADM', c.phaseId || c.phase, c.nomeContato, c.equipamento, c.arquivadoEm || c.movedAt);
    for (const c of (((arqT || {}).cards) || [])) guarda(c.telefone, 'Arquivo TV', c.phaseId || c.phase, c.nomeContato, c.equipamento, c.arquivadoEm || c.movedAt);
    for (const f of (((pros || {}).fichas) || [])) guarda(f.telefone, 'Prospecção', f.status, f.nome, f.equipamento, f.movedAt || f.criadoEm);
    for (const g of ((((gar || {}).garantias) || []).concat(((gar || {}).lojaImediata) || []))) {
      guarda(g.telefone, 'Garantia', g.faseId || (g.concluida ? 'concluída' : 'aberta'), g.nome, g.defeito, g.movidaEm || g.criadaEm);
    }
    for (const f of (((fl || {}).fichas) || [])) guarda(f.telefone, 'Frente de Loja', f.status || f.phase, f.nome || f.nomeContato, f.equipamento, f.movedAt || f.criadoEm);
    for (const b of (((balc || {}).fichas) || (((balc || {}).cards) || []))) guarda(b.telefone, 'Balcão', b.status || b.phase, b.nome || b.nomeContato, b.equipamento, b.movedAt || b.criadoEm);
    for (const o of (((orcA || {}).fichas) || [])) guarda(o.telefone, 'Orçamentos ADM', o.status || o.phase, o.nome, o.equipamento, o.movedAt || o.criadoEm);
    for (const o of (((orcT || {}).fichas) || [])) guarda(o.telefone, 'Orçamentos TV', o.status || o.phase, o.nome, o.equipamento, o.movedAt || o.criadoEm);
    const varreArq = (banco, rot) => {
      const b = banco || {};
      for (const x of ((b.fichas || []).concat(b.cards || []).concat(b.itens || []).concat(Array.isArray(b) ? b : []))) {
        guarda(x.telefone || x.tel, rot, x.phase || x.phaseId || x.status || x.faseId,
          x.nome || x.nomeContato, x.equipamento || x.descricao,
          x.arquivadoEm || x.movedAt || x.movidaEm || x.criadoEm);
      }
    };
    varreArq(lgArqA, 'Arq. Logística ADM');
    varreArq(lgArqT, 'Arq. Logística TV');
    varreArq(waArq, 'Arq. WhatsApp');
    varreArq(finArq, 'Arq. Financeiro');

    const semNada = alvos.filter(a => !achados[a]);
    const linhas = [];
    for (const a of alvos) {
      const lista = achados[a];
      if (!lista) { linhas.push(a + ' | ❌ NÃO ENCONTRADO'); continue; }
      // prioriza o registro vivo mais recente
      const ordem = ['Pipe ADM', 'Pipe TV', 'Logística ADM', 'Logística TV', 'Garantia', 'Fichas ADM', 'Fichas TV', 'Prospecção', 'Arquivo ADM', 'Arquivo TV'];
      lista.sort((x, y) => ordem.indexOf(x.onde) - ordem.indexOf(y.onde));
      const p = lista[0];
      linhas.push(a + ' | ' + String(p.nome).slice(0, 16) + ' | ' + p.onde + ' | ' + p.fase +
        ' | ' + (p.diasNaFase != null ? p.diasNaFase + 'd' : '?') +
        (lista.length > 1 ? ' | +' + (lista.length - 1) : ''));
    }
    // ?fases=finalizado,erp,descarte → só as ocorrências dessas fases
    const filtro = String(req.query.fases || '').split(',').map(x => x.trim().toLowerCase()).filter(Boolean);
    if (filtro.length) {
      const casa = f => filtro.some(x => String(f || '').toLowerCase().includes(x));
      const porFase = {};
      let n = 0;
      for (const a of alvos) {
        for (const p of (achados[a] || [])) {
          if (!casa(p.fase)) continue;
          const k = p.fase;
          (porFase[k] = porFase[k] || []).push(
            a + ' | ' + String(p.nome || '?').slice(0, 22) + ' | ' + p.onde +
            ' | ' + (p.diasNaFase != null ? p.diasNaFase + 'd' : '?') +
            (p.equipamento ? ' | ' + p.equipamento : ''));
          n++;
        }
      }
      for (const k of Object.keys(porFase)) {
        porFase[k].sort((x, y) => {
          const dx = parseFloat((x.split('|')[3] || '').replace('d', '')) || 0;
          const dy = parseFloat((y.split('|')[3] || '').replace('d', '')) || 0;
          return dy - dx;
        });
      }
      return res.status(200).json({ ok: true,
        filtro, codigosConsultados: alvos.length, ocorrencias: n,
        porFase: Object.keys(porFase).reduce((o, k) => { o[k] = porFase[k].length; return o; }, {}),
        RESULTADO: porFase });
    }
    // ?todos=1 → uma linha por OCORRÊNCIA, não só a principal
    if (String(req.query.todos || '') === '1') {
      const todas = [];
      for (const a of alvos) {
        const lista = achados[a];
        if (!lista) { todas.push(a + ' | ❌ sem registro'); continue; }
        const ordem = ['Pipe ADM', 'Pipe TV', 'Logística ADM', 'Logística TV', 'Garantia', 'Frente de Loja',
          'Balcão', 'Orçamentos ADM', 'Orçamentos TV', 'Fichas ADM', 'Fichas TV', 'Prospecção', 'Arquivo ADM', 'Arquivo TV'];
        lista.sort((x, y) => ordem.indexOf(x.onde) - ordem.indexOf(y.onde));
        for (const p of lista) {
          todas.push(a + ' | ' + String(p.nome || '?').slice(0, 18) + ' | ' + p.onde + ' | ' + p.fase +
            ' | ' + (p.diasNaFase != null ? p.diasNaFase + 'd' : '?') +
            (p.equipamento ? ' | ' + p.equipamento : ''));
        }
      }
      return res.status(200).json({ ok: true,
        pedidos: alvos.length,
        totalOcorrencias: todas.length,
        semRegistro: semNada.length,
        TODAS: todas });
    }
    return res.status(200).json({ ok: true,
      pedidos: alvos.length, encontrados: alvos.length - semNada.length,
      naoEncontrados: semNada.length,
      LISTA: linhas,
      semRegistro: semNada,
      detalhe: String(req.query.full || '') === '1' ? achados : undefined });
  }

  if (action === 'lixeira') {
    const tomb = (await dbGet(KEY_EXCLUIDAS)) || { linhas: {} };
    const dias = Math.min(180, Math.max(1, parseInt(req.query.dias || '30', 10)));
    const corte = Date.now() - dias * 86400000;
    const itens = Object.keys(tomb.linhas || {})
      .map(k => Object.assign({ linha: Number(k) }, tomb.linhas[k]))
      .filter(x => new Date(x.em || 0).getTime() >= corte)
      .sort((a, b) => String(b.em).localeCompare(String(a.em)));

    // restaurar: ?restaurar=LINHA  ou  ?restaurarTodas=1
    const alvo = String(req.query.restaurar || '');
    const todas = String(req.query.restaurarTodas || '') === '1';
    if (alvo || todas) {
      const paraRestaurar = todas ? itens : itens.filter(x => String(x.linha) === alvo);
      if (!paraRestaurar.length) return res.status(404).json({ ok: false, error: 'nada a restaurar' });
      const dbA = (await dbGet(KEY_ADM)) || { fichas: [] };
      const dbT = (await dbGet(KEY_TV)) || { fichas: [] };
      let voltaram = 0;
      for (const it of paraRestaurar) {
        if (!it.ficha) continue;                       // exclusões antigas não guardaram a ficha
        const ehTv = /\btv\b|televis/i.test(String(it.equipamento || ''));
        const banco = ehTv ? dbT : dbA;
        if (!(banco.fichas || []).some(f => f.id === it.ficha.id)) {
          banco.fichas.unshift(Object.assign({}, it.ficha, { restauradaEm: new Date().toISOString() }));
          voltaram++;
        }
        delete tomb.linhas[String(it.linha)];
      }
      await Promise.all([dbSet(KEY_ADM, dbA), dbSet(KEY_TV, dbT), dbSet(KEY_EXCLUIDAS, tomb)]);
      const semDados = paraRestaurar.filter(x => !x.ficha).length;
      return res.status(200).json({ ok: true, restauradas: voltaram,
        semDadosParaRestaurar: semDados,
        obs: semDados ? 'exclusões anteriores a hoje não guardaram os dados — use recuperar-perdidas para reimportar da planilha' : undefined });
    }
    return res.status(200).json({ ok: true, periodoDias: dias, total: itens.length,
      podemSerRestauradas: itens.filter(x => !!x.ficha).length,
      itens: itens.map(x => ({ linha: x.linha, nome: x.nome, telefone: x.tel,
        equipamento: x.equipamento, statusQuandoExcluida: x.status,
        excluidaEm: x.em, por: x.por || 'não informado', motivo: x.motivo || 'não informado',
        origem: x.origem, temDadosParaRestaurar: !!x.ficha })),
      comoRestaurar: '?restaurar=NUMERO_DA_LINHA ou ?restaurarTodas=1' });
  }

  // ── 🛡 AUDITORIA DIÁRIA: guarda o resultado do cruzamento e alerta se sumiu ficha ──
  if (action === 'auditoria-diaria') {
    const r = await fetch(`https://reparoeletroadm.com/api/fichas?action=recuperar-perdidas&dias=2&k=${(process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim()}`)
      .then(x => x.json()).catch(() => null);
    const perdidas = (r && r.realmentePerdidas) || 0;
    const reg = (await dbGet('fichas_auditoria')) || { dias: {} };
    const hoje = new Date(Date.now() - 3 * 3600 * 1000).toISOString().slice(0, 10);
    reg.dias[hoje] = { em: new Date().toISOString(), perdidas,
      lista: ((r && r.lista) || []).slice(0, 30).map(x => ({ linha: x.linha, nome: x.nome, tel: x.telefone })) };
    for (const d of Object.keys(reg.dias)) {
      if (new Date(d).getTime() < Date.now() - 30 * 86400000) delete reg.dias[d];
    }
    await dbSet('fichas_auditoria', reg);
    return res.status(200).json({ ok: true, dia: hoje, perdidas,
      alerta: perdidas > 0 ? '⚠️ ' + perdidas + ' linha(s) da planilha não viraram ficha nas últimas 48h' : '✅ nenhuma ficha perdida',
      lista: reg.dias[hoje].lista });
  }

  // ── 📊 AUDITORIA-BADGE: número para a tela mostrar ──
  if (action === 'auditoria-badge') {
    const reg = (await dbGet('fichas_auditoria')) || { dias: {} };
    const hoje = new Date(Date.now() - 3 * 3600 * 1000).toISOString().slice(0, 10);
    const d = reg.dias[hoje] || { perdidas: 0 };
    return res.status(200).json({ ok: true, perdidas: d.perdidas || 0, verificadoEm: d.em || null });
  }

  // ── LINHAS EXCLUÍDAS: quais linhas da planilha estão bloqueadas ──
  if (action === 'linhas-excluidas') {
    const tomb = (await dbGet(KEY_EXCLUIDAS)) || { linhas: {} };
    const lista = Object.keys(tomb.linhas || {}).map(k => Object.assign({ linha: Number(k) }, tomb.linhas[k]))
      .sort((a, b) => b.linha - a.linha);
    if (req.query.liberar) {                            // libera uma linha específica
      const l = String(req.query.liberar);
      if (tomb.linhas && tomb.linhas[l]) { delete tomb.linhas[l]; await dbSet(KEY_EXCLUIDAS, tomb); }
      return res.status(200).json({ ok: true, liberada: l });
    }
    return res.status(200).json({ ok: true, total: lista.length, linhas: lista.slice(0, 100) });
  }

  // ── RESET-CURSOR: zera cursor para ser recalculado no próximo sync ────────
  if (action === 'reset-cursor') {
    await dbSet(KEY_CURSOR, null);
    return res.status(200).json({ ok:true, msg:'Cursor zerado. Acesse /api/fichas?action=sync para reinicializar.' });
  }

  // ── BADGE: faz sync da planilha + retorna contagem de fichas novas ─────────
  if (action === 'badge') {
    const sistema = req.query.sistema || 'adm';
    // Sync com THROTTLE de 3min: evita N usuários x polling 30s
    // gerarem centenas de downloads da planilha por hora
    try {
      const cursorPre = await dbGet(KEY_CURSOR);
      const lastSync  = cursorPre?.atualizadoEm || cursorPre?.iniciadoEm || null;
      const throttleOk = !lastSync || (Date.now() - new Date(lastSync).getTime()) > 3*60*1000;
      if (!throttleOk) {
        const dbT = (await dbGet(sistema === 'tv' ? KEY_TV : KEY_ADM)) || { fichas:[] };
        const novasT = (dbT.fichas||[]).filter(f => f.status === 'criada').length;
        return res.status(200).json({ ok:true, novas: novasT, throttled:true });
      }
      const resp = await fetch(SHEET_CSV, { redirect:'follow' });
      if (resp.ok) {
        const text = await resp.text();
        const rows = parseCSV(text);
        const total = rows.length;
        const cursor = await dbGet(KEY_CURSOR);
        // Atualiza timestamp SEMPRE (necessário p/ throttle), mesmo sem linhas novas
        if (cursor && cursor.row != null && total <= cursor.row) {
          await dbSet(KEY_CURSOR, { ...cursor, atualizadoEm: new Date().toISOString() });
        }
        if (cursor && cursor.row != null && total > cursor.row) {
          const novasRows = rows.slice(cursor.row).filter(r =>
            String(r[0]||'').trim() || String(r[1]||'').trim()
          );
          if (novasRows.length > 0) {
            const dbAdm = (await dbGet(KEY_ADM)) || { fichas:[] };
            const dbTv  = (await dbGet(KEY_TV))  || { fichas:[] };
            let importadas = 0;
            for (let i = 0; i < novasRows.length; i++) {
              const row    = novasRows[i];
              const rowNum = cursor.row + i + 1;
              const tel    = String(row[0]||'').replace(/\D/g,'').trim();
              const nome   = String(row[1]||'').trim();
              const equip  = String(row[2]||'').trim();
              const def    = String(row[3]||'').trim();
              const end    = String(row[4]||'').trim();
              const hora   = String(row[6]||'').trim();
              if (!nome && !tel) continue;

              // Deduplicação: pular se sheetRow já existe
              const jaExisteBadge = dbAdm.fichas.some(f => f.sheetRow === rowNum) ||
                                    dbTv.fichas.some(f => f.sheetRow === rowNum);
              if (jaExisteBadge) continue;

              const sis = detectSistema(equip);
              const ficha = {
                id: `fsh_${rowNum}_${tel.slice(-4)}_${Date.now().toString(36)}`,
                sheetRow: rowNum, nome, telefone: tel, endereco: end,
                equipamento: equip, defeito: def, horario: hora, sistema: sis,
                waNum: waNum(tel),
                textoCopiar: sis === 'tv' ? TEXTO_TV : TEXTO_ADM,
                status: 'criada', criadoEm: new Date().toISOString(),
                contatoFeitoEm: null, logisticaEm: null,
              };
              if (sis === 'tv') dbTv.fichas.unshift(ficha);
              else              dbAdm.fichas.unshift(ficha);
              importadas++;
            }
            if (importadas > 0) {
              await dbSet(KEY_ADM, dbAdm);
              await dbSet(KEY_TV,  dbTv);
            }
            // Atualizar cursor para última linha com dado
            let novoUltimo = cursor.row;
            for (let i = rows.length - 1; i >= 1; i--) {
              if (String(rows[i][0]||'').trim() || String(rows[i][1]||'').trim()) {
                novoUltimo = i + 1; break;
              }
            }
            await dbSet(KEY_CURSOR, { row: novoUltimo, atualizadoEm: new Date().toISOString() });
          }
        }
      }
    } catch(_) {}
    // Retornar contagem atual
    const key = sistema === 'tv' ? KEY_TV : KEY_ADM;
    const db  = (await dbGet(key)) || { fichas:[] };
    const novas = (db.fichas||[]).filter(f => f.status === 'criada').length;
    return res.status(200).json({ ok:true, novas });
  }

  // ── DUPLICADAS-RELATORIO: varre "criada" duplicadas (GET lista; &limpar=1 remove) ──
  if (action === 'duplicadas-relatorio') {
    const assinar2 = (tel, nome, equip) =>
      String(tel || '').replace(/\D/g, '').slice(-8) + '|' +
      String(nome || '').trim().toLowerCase().slice(0, 20) + '|' +
      String(equip || '').trim().toLowerCase().slice(0, 20);
    const PESO = { criada: 0, ficha_criada: 0, contato_feito: 2, entrar_contato: 3, cliente_loja: 4, prospeccao: 4, logistica: 5 };
    const relat = [];
    const remover = { [KEY_ADM]: [], [KEY_TV]: [] };
    const mover = {};
    for (const key of [KEY_ADM, KEY_TV]) {
      const db = (await dbGet(key)) || { fichas: [] };
      const grupos = {};
      for (const f of (db.fichas || [])) {
        const sig = f.sheetSig || assinar2(f.telefone, f.nome, f.equipamento);
        (grupos[sig] = grupos[sig] || []).push(f);
      }
      for (const sig of Object.keys(grupos)) {
        const g = grupos[sig];
        if (g.length < 2) continue;
        // mantem a de estagio mais avancado; empate -> a mais antiga
        const ordenado = [...g].sort((a, b) =>
          (PESO[b.status] || 0) - (PESO[a.status] || 0) ||
          new Date(a.criadoEm || 0) - new Date(b.criadoEm || 0));
        const fica = ordenado[0];
        const EM_OPERACAO = ['logistica', 'orcamento', 'aprovado'];
        for (const d of ordenado.slice(1)) {
          const ehCriada = !d.status || ['criada', 'ficha_criada'].includes(d.status);
          let acao = 'manter (histórico — fases diferentes)';
          if (ehCriada) {
            acao = EM_OPERACAO.includes(fica.status)
              ? 'entrar_contato (cliente REFEZ a ficha — equipamento já conosco, ligar)'
              : 'eliminar (linha duplicada no mesmo cadastro)';
          }
          relat.push({ sistema: key === KEY_ADM ? 'adm' : 'tv', nome: d.nome, telefone: d.telefone,
            equipamento: d.equipamento, statusDuplicada: d.status || 'criada', sheetRowDuplicada: d.sheetRow,
            mantida: { id: fica.id, status: fica.status, sheetRow: fica.sheetRow }, acao,
            removivel: acao.startsWith('eliminar') });
          if (acao.startsWith('eliminar')) remover[key].push(d.id);
          if (acao.startsWith('entrar_contato')) {
            d.status = 'entrar_contato';
            d.entrarContatoMotivo = 'cliente refez a ficha — equipamento já em atendimento; ligar para tirar dúvidas';
            d.fichaRefeita = true;
            (mover[key] = mover[key] || []).push(d.id);
          }
        }
      }
    }
    if (String(req.query.limpar || '') === '1') {
      let total = 0, movidas = 0;
      for (const key of [KEY_ADM, KEY_TV]) {
        const paraMover = mover[key] || [];
        if (!remover[key].length && !paraMover.length) continue;
        const db = (await dbGet(key)) || { fichas: [] };
        const antes = db.fichas.length;
        db.fichas = db.fichas.filter(f => !remover[key].includes(f.id));
        total += antes - db.fichas.length;
        for (const f of db.fichas) if (paraMover.includes(f.id)) {
          f.status = 'entrar_contato';
          f.entrarContatoMotivo = 'cliente refez a ficha — equipamento já em atendimento; ligar para tirar dúvidas';
          f.fichaRefeita = true; movidas++;
        }
        await dbSet(key, db);
      }
      return res.status(200).json({ ok: true, eliminadas: total, movidasParaEntrarContato: movidas, relatorio: relat });
    }
    return res.status(200).json({ ok: true, duplicadas: relat.length,
      aEliminar: relat.filter(r => r.removivel).length,
      aLigar: relat.filter(r => String(r.acao || '').startsWith('entrar_contato')).length,
      historicoIntacto: relat.filter(r => String(r.acao || '').startsWith('manter')).length,
      relatorio: relat.slice(0, 60),
      dica: 'com &limpar=1: elimina só as duplicações técnicas e manda as fichas refeitas para Entrar em Contato (o histórico não é tocado)' });
  }

  // ── LIMPAR-DUPLICATAS: remove fichas com sheetRow repetido ─────────────
  if (action === 'limpar-duplicatas') {
    let total = 0;
    for (const key of [KEY_ADM, KEY_TV]) {
      const db = (await dbGet(key)) || { fichas:[] };
      const vistas = new Set();
      const antes  = db.fichas.length;
      db.fichas = db.fichas.filter(f => {
        if (!f.sheetRow) return true; // sem sheetRow, mantém
        if (vistas.has(f.sheetRow)) return false; // duplicata, remove
        vistas.add(f.sheetRow);
        return true;
      });
      const removidas = antes - db.fichas.length;
      if (removidas > 0) { await dbSet(key, db); total += removidas; }
    }
    return res.status(200).json({ ok:true, removidas: total });
  }

  return res.status(404).json({ ok:false, error:'Ação não encontrada' });
}
