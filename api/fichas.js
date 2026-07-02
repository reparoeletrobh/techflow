const U = (process.env.UPSTASH_URL   ||'').replace(/['"]/g,'').trim();
const T = (process.env.UPSTASH_TOKEN ||'').replace(/[\n\r'"]/g,'').trim();

const SHEET_ID   = '1ovSEGZ7if5-wdNZpd1cbLlyg0PZpsrT9fQwOIzfG_mw';
const SHEET_CSV  = `https://docs.google.com/spreadsheets/d/${SHEET_ID}/export?format=csv`;

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
  res.setHeader('Access-Control-Allow-Origin', 'https://reparoeletroadm.com');
  res.setHeader('Cache-Control', 'no-cache');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const action = req.query.action || (req.body && req.body.action) || '';

  // ── SYNC: busca novas linhas via CSV público ───────────────────────────────
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
      const novasRows = rows.slice(cursor.row).filter(r => String(r[0]||'').trim() || String(r[1]||'').trim());
      if (!novasRows.length) {
        return res.status(200).json({ ok:true, novas:0, total });
      }

      const dbAdm = (await dbGet(KEY_ADM)) || { fichas:[] };
      const dbTv  = (await dbGet(KEY_TV))  || { fichas:[] };
      let novas = 0;

      for (let i = 0; i < novasRows.length; i++) {
        const row    = novasRows[i];
        const rowNum = cursor.row + i + 1;

        // Estrutura: A=tel, B=nome, C=equip, D=defeito, E=endereço, F=msg, G=horário
        const tel   = String(row[0]||'').replace(/\D/g,'').trim();
        const nome  = String(row[1]||'').trim();
        const equip = String(row[2]||'').trim();
        const def   = String(row[3]||'').trim();
        const end   = String(row[4]||'').trim();
        const hora  = String(row[6]||'').trim();

        if (!nome && !tel) continue;

        const sistema = detectSistema(equip);
        const id = `fsh_${rowNum}_${tel.slice(-4)}_${Date.now().toString(36)}`;

        const ficha = {
          id, sheetRow: rowNum,
          nome, telefone: tel, endereco: end,
          equipamento: equip, defeito: def, horario: hora,
          sistema, waNum: waNum(tel),
          textoCopiar: sistema === 'tv' ? TEXTO_TV : TEXTO_ADM,
          status: 'criada',
          criadoEm: new Date().toISOString(),
          contatoFeitoEm: null,
          logisticaEm: null,
        };

        if (sistema === 'tv') dbTv.fichas.unshift(ficha);
        else                  dbAdm.fichas.unshift(ficha);
        novas++;
      }

      if (novas > 0) {
        await dbSet(KEY_ADM, dbAdm);
        await dbSet(KEY_TV,  dbTv);
      }
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
    for (const f of db.fichas) {
      if (f.status === 'contato_feito' && f.contatoFeitoEm) {
        if (agora - new Date(f.contatoFeitoEm).getTime() > 24*60*60*1000) {
          f.status = 'entrar_contato';
          mudou = true;
        }
      }
    }
    if (mudou) await dbSet(key, db);
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

    const LOG_KEY = 'reparoeletro_logistica';
    const logDb   = (await dbGet(LOG_KEY)) || { fichas:[] };
    logDb.fichas.unshift({
      id:          'log_' + Date.now().toString(36),
      nome:        ficha.nome,
      telefone:    ficha.telefone,
      endereco:    ficha.endereco,
      equipamento: ficha.equipamento,
      defeito:     ficha.defeito,
      phase:       'liberado_coleta',
      origem:      'ficha_planilha',
      criadoEm:    new Date().toISOString(),
      movedAt:     new Date().toISOString(),
    });
    await dbSet(LOG_KEY, logDb);

    ficha.status      = 'logistica';
    ficha.logisticaEm = new Date().toISOString();
    await dbSet(key, db);
    return res.status(200).json({ ok:true });
  }

  // ── EXCLUIR ───────────────────────────────────────────────────────────────
  if (req.method === 'POST' && action === 'excluir') {
    const { id, sistema } = req.body || {};
    const key = sistema === 'tv' ? KEY_TV : KEY_ADM;
    const db  = (await dbGet(key)) || { fichas:[] };
    db.fichas = db.fichas.filter(x => x.id !== id);
    await dbSet(key, db);
    return res.status(200).json({ ok:true });
  }

  // ── RESET-CURSOR: zera cursor para ser recalculado no próximo sync ────────
  if (action === 'reset-cursor') {
    await dbSet(KEY_CURSOR, null);
    return res.status(200).json({ ok:true, msg:'Cursor zerado. Acesse /api/fichas?action=sync para reinicializar.' });
  }

  // ── BADGE: retorna contagem de fichas novas ─────────────────────────────
  if (action === 'badge') {
    const sistema = req.query.sistema || 'adm';
    const key = sistema === 'tv' ? KEY_TV : KEY_ADM;
    const db  = (await dbGet(key)) || { fichas:[] };
    const novas = (db.fichas||[]).filter(f => f.status === 'criada').length;
    return res.status(200).json({ ok:true, novas });
  }

  return res.status(404).json({ ok:false, error:'Ação não encontrada' });
}
