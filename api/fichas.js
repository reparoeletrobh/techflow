const U = (process.env.UPSTASH_URL||'').replace(/['"]/g,'').trim();
const T = (process.env.UPSTASH_TOKEN||'').replace(/[\n\r'"]/g,'').trim();

const SHEET_ID = process.env.FICHAS_SHEET_ID || '1ovSEGZ7if5-wdNZpd1cbLlyg0PZpsrT9fQwOIzfG_mw';
const GAPI_KEY = process.env.GOOGLE_API_KEY  || '';

const KEY_ADM    = 'fichas_adm';
const KEY_TV     = 'fichas_tv';
const KEY_CURSOR = 'fichas_sheet_cursor';

async function dbGet(key) {
  try {
    const r = await fetch(`${U}/get/${key}`, { headers: { Authorization: `Bearer ${T}` } });
    const j = await r.json();
    return j.result ? JSON.parse(j.result) : null;
  } catch { return null; }
}
async function dbSet(key, val) {
  try {
    await fetch(`${U}/set/${key}`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${T}`, 'Content-Type': 'application/json' },
      body: JSON.stringify(val)
    });
    return true;
  } catch { return false; }
}

function detectSistema(equip) {
  const e = (equip||'').toLowerCase();
  if (e.includes('tv') || e.includes('televi') || e.includes('monitor') || e.includes('smart')) return 'tv';
  return 'adm';
}

function gerarId(row, tel) {
  return `fsh_${row}_${String(tel||'').replace(/\D/g,'').slice(-4)}_${Date.now().toString(36)}`;
}

function waNum(tel) {
  const d = String(tel||'').replace(/\D/g,'');
  if (d.startsWith('55') && d.length >= 12) return d;
  if (d.length === 11) return '55' + d;
  if (d.length === 10) return '55' + d;
  return d.length >= 12 ? d : '55' + d;
}

const TEXTO_ADM = `Olá, tudo bem? Alessandro aqui, responsável pela logística da Reparo Eletro.\n\nTEMOS 2 OPÇÕES: COLETA E ENTREGA / ATENDIMENTO NO BALCÃO\n\n*ATENÇÃO: Você trazendo aqui na loja seu equipamento o orçamento é gratuito e consertamos em 15 minutos! Estamos na Rua Ouro Preto 663 - Barro Preto*\n\nCaso você prefira usar a nossa coleta e entrega, podemos buscar hoje mesmo na sua casa! Aguardo sua resposta.\n\nJá estamos prontos para te atender! Me fala qual opção escolheu por favor.`;
const TEXTO_TV  = `Olá, tudo bem? Sou o Alessandro, responsável pela Logística da Reparo Eletro - TVs.\n\nPodemos prosseguir com o atendimento?`;

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', 'https://reparoeletroadm.com');
  res.setHeader('Cache-Control', 'no-cache');

  const action = req.query.action || (req.body && req.body.action) || '';

  // ── SYNC ────────────────────────────────────────────────────────────────
  if (action === 'sync') {
    if (!SHEET_ID || !GAPI_KEY)
      return res.status(200).json({ ok:false, error:'FICHAS_SHEET_ID ou GOOGLE_API_KEY não configurado', novas:0 });
    try {
      const url  = `https://sheets.googleapis.com/v4/spreadsheets/${SHEET_ID}/values/A2:H?key=${GAPI_KEY}`;
      const resp = await fetch(url);
      const data = await resp.json();
      if (!resp.ok) return res.status(200).json({ ok:false, error: data.error?.message||'Sheets API erro', novas:0 });

      const rows  = data.values || [];
      const total = rows.length;

      // Primeira execução: salva cursor sem importar nada
      const cursor = await dbGet(KEY_CURSOR);
      if (!cursor || cursor.row == null) {
        await dbSet(KEY_CURSOR, { row: total, iniciadoEm: new Date().toISOString() });
        return res.status(200).json({ ok:true, novas:0, msg:`Cursor iniciado na linha ${total} — apenas novas fichas serão importadas.` });
      }

      const novasRows = rows.slice(cursor.row);
      if (!novasRows.length) {
        return res.status(200).json({ ok:true, novas:0 });
      }

      const dbAdm = (await dbGet(KEY_ADM)) || { fichas:[] };
      const dbTv  = (await dbGet(KEY_TV))  || { fichas:[] };
      let novas = 0;

      for (let i = 0; i < novasRows.length; i++) {
        const row    = novasRows[i];
        const rowNum = cursor.row + i + 1;
        const tel    = (row[0]||'').replace(/\D/g,'').trim(); // coluna A: telefone
        const nome   = (row[1]||'').trim();                    // coluna B: nome
        const equip  = (row[2]||'').trim();                    // coluna C: equipamento
        const def    = (row[3]||'').trim();                    // coluna D: defeito
        const end    = (row[4]||'').trim();                    // coluna E: endereço
        if (!nome && !tel) continue;

        const sistema = detectSistema(equip);
        const ficha = {
          id: gerarId(rowNum, tel), sheetRow: rowNum,
          nome, telefone: tel, endereco: end, equipamento: equip, defeito: def,
          sistema, waNum: waNum(tel),
          textoCopiar: sistema === 'tv' ? TEXTO_TV : TEXTO_ADM,
          status: 'criada', criadoEm: new Date().toISOString(),
          contatoFeitoEm: null, logisticaEm: null,
        };
        if (sistema === 'tv') dbTv.fichas.unshift(ficha);
        else                  dbAdm.fichas.unshift(ficha);
        novas++;
      }

      if (novas > 0) { await dbSet(KEY_ADM, dbAdm); await dbSet(KEY_TV, dbTv); }
      await dbSet(KEY_CURSOR, { row: total, atualizadoEm: new Date().toISOString() });
      return res.status(200).json({ ok:true, novas, total });
    } catch(e) { return res.status(200).json({ ok:false, error: e.message, novas:0 }); }
  }

  // ── LOAD ────────────────────────────────────────────────────────────────
  if (action === 'load') {
    const sistema = req.query.sistema || req.body?.sistema || 'adm';
    const key = sistema === 'tv' ? KEY_TV : KEY_ADM;
    const db  = (await dbGet(key)) || { fichas:[] };
    const agora = Date.now();
    let mudou = false;
    for (const f of db.fichas) {
      if (f.status === 'contato_feito' && f.contatoFeitoEm) {
        if (agora - new Date(f.contatoFeitoEm).getTime() > 24*60*60*1000) {
          f.status = 'entrar_contato'; mudou = true;
        }
      }
    }
    if (mudou) await dbSet(key, db);
    return res.status(200).json({ ok:true, fichas: db.fichas });
  }

  // ── MOVER-CONTATO ───────────────────────────────────────────────────────
  if (req.method === 'POST' && action === 'mover-contato') {
    const { id, sistema } = req.body || {};
    const key = sistema === 'tv' ? KEY_TV : KEY_ADM;
    const db  = (await dbGet(key)) || { fichas:[] };
    const f   = db.fichas.find(x => x.id === id);
    if (!f) return res.status(404).json({ ok:false, error:'Não encontrado' });
    f.status = 'contato_feito'; f.contatoFeitoEm = new Date().toISOString();
    await dbSet(key, db);
    return res.status(200).json({ ok:true });
  }

  // ── CADASTRAR-LOGISTICA ─────────────────────────────────────────────────
  if (req.method === 'POST' && action === 'cadastrar-logistica') {
    const { id, sistema } = req.body || {};
    const key   = sistema === 'tv' ? KEY_TV : KEY_ADM;
    const db    = (await dbGet(key)) || { fichas:[] };
    const ficha = db.fichas.find(x => x.id === id);
    if (!ficha) return res.status(404).json({ ok:false, error:'Não encontrado' });

    const LOG_KEY = 'reparoeletro_logistica';
    const logDb   = (await dbGet(LOG_KEY)) || { fichas:[] };
    logDb.fichas.unshift({
      id: 'log_' + Date.now().toString(36),
      nome: ficha.nome, telefone: ficha.telefone,
      endereco: ficha.endereco, equipamento: ficha.equipamento, defeito: ficha.defeito,
      phase: 'liberado_coleta', origem: 'ficha_planilha',
      criadoEm: new Date().toISOString(), movedAt: new Date().toISOString(),
    });
    await dbSet(LOG_KEY, logDb);
    ficha.status = 'logistica'; ficha.logisticaEm = new Date().toISOString();
    await dbSet(key, db);
    return res.status(200).json({ ok:true });
  }

  // ── EXCLUIR ─────────────────────────────────────────────────────────────
  if (req.method === 'POST' && action === 'excluir') {
    const { id, sistema } = req.body || {};
    const key = sistema === 'tv' ? KEY_TV : KEY_ADM;
    const db  = (await dbGet(key)) || { fichas:[] };
    db.fichas = db.fichas.filter(x => x.id !== id);
    await dbSet(key, db);
    return res.status(200).json({ ok:true });
  }

  return res.status(404).json({ ok:false, error:'Ação não encontrada' });
}
