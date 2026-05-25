// api/logistica.js — Sistema de Logística de Coleta
const U = process.env.UPSTASH_URL;
const T = process.env.UPSTASH_TOKEN;
const LOG_KEY = 'reparoeletro_logistica';

async function dbGet(key) {
  try {
    const r = await fetch(`${U}/get/${key}`, { headers: { Authorization: `Bearer ${T}` } });
    const j = await r.json();
    return j.result ? JSON.parse(j.result) : null;
  } catch(e) { return null; }
}
async function dbSet(key, val) {
  try {
    await fetch(`${U}/set/${key}`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${T}`, 'Content-Type': 'application/json' },
      body: JSON.stringify(val)
    });
    return true;
  } catch(e) { return false; }
}

function defaultDB() { return { fichas: [], nextId: 1 }; }


async function registrarPassagem(phase) {
  try {
    const hoje = new Date().toLocaleDateString('pt-BR', {timeZone:'America/Sao_Paulo'}).split('/').reverse().join('-');
    const db   = (await dbGet('reparoeletro_log_metricas')) || {};
    if (!db[hoje]) db[hoje] = {};
    db[hoje][phase] = (db[hoje][phase] || 0) + 1;
    const cutoff = new Date(); cutoff.setDate(cutoff.getDate() - 30);
    Object.keys(db).forEach(d => { if (new Date(d) < cutoff) delete db[d]; });
    await dbSet('reparoeletro_log_metricas', db);
  } catch(e) { console.error('registrarPassagem:', e.message); }
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const action = req.query.action;

  // ── GET load ──────────────────────────────────────────────
  if (action === 'load') {
    const db = await dbGet(LOG_KEY) || defaultDB();
    return res.status(200).json({ ok: true, fichas: db.fichas || [] });
  }


  // ── GET metricas ─────────────────────────────────────────────
  if (action === 'metricas') {
    const MET_KEY = 'reparoeletro_log_metricas';
    const met = (await dbGet(MET_KEY)) || {};
    const hoje = new Date().toLocaleDateString('pt-BR', {timeZone:'America/Sao_Paulo'}).split('/').reverse().join('-');

    // Backfill: se hoje nao tem dados, semear com fichas em cada coluna agora
    // (baseline do dia — a partir daqui cada movimentacao acumula por cima)
    if (!met[hoje] || !Object.keys(met[hoje]).length) {
      const fichasDb = await dbGet(LOG_KEY) || defaultDB();
      met[hoje] = {};
      for (const f of fichasDb.fichas || []) {
        if (f.phase) {
          met[hoje][f.phase] = (met[hoje][f.phase] || 0) + 1;
        }
      }
      await dbSet(MET_KEY, met);
    }

    return res.status(200).json({ ok: true, metricas: met });
  }

  // ── POST criar ────────────────────────────────────────────
  if (req.method === 'POST' && action === 'criar') {
    const { nome, telefone, endereco, equipamento, defeito, pipefyCardId, texto } = req.body || {};
    if (!nome) return res.status(400).json({ ok: false, error: 'nome obrigatorio' });

    const db = await dbGet(LOG_KEY) || defaultDB();
    const id = 'LOG-' + String(db.nextId || 1).padStart(4, '0');
    const ficha = {
      id, nome, telefone: telefone || '', endereco: endereco || '',
      equipamento: equipamento || '', defeito: defeito || '',
      pipefyCardId: pipefyCardId || null, texto: texto || '',
      phase: 'liberado_coleta',
      criadoEm: new Date().toISOString(),
      movedAt: new Date().toISOString(),
      diagnostico: null,
    };
    db.fichas.unshift(ficha);
    db.nextId = (db.nextId || 1) + 1;
    await dbSet(LOG_KEY, db);
    registrarPassagem('liberado_coleta').catch(() => {});
    return res.status(201).json({ ok: true, ficha });
  }

  // ── POST mover ────────────────────────────────────────────
  if (req.method === 'POST' && action === 'mover') {
    const { id, phase } = req.body || {};
    const PHASES = ['liberado_coleta','em_rota','remarcar','coleta_efetuada','orc_registrado'];
    if (!id || !PHASES.includes(phase)) return res.status(400).json({ ok: false, error: 'invalido' });

    const db = await dbGet(LOG_KEY) || defaultDB();
    const ficha = db.fichas.find(f => f.id === id);
    if (!ficha) return res.status(404).json({ ok: false, error: 'nao encontrada' });
    ficha.phase = phase;
    ficha.movedAt = new Date().toISOString();
    await dbSet(LOG_KEY, db);
    registrarPassagem(phase).catch(() => {});
    return res.status(200).json({ ok: true, ficha });
  }

  // ── POST atualizar-dados ──────────────────────────────────
  if (req.method === 'POST' && action === 'atualizar-dados') {
    const { id, nome, telefone, endereco, equipamento, defeito } = req.body || {};
    const db = await dbGet(LOG_KEY) || defaultDB();
    const ficha = db.fichas.find(f => f.id === id);
    if (!ficha) return res.status(404).json({ ok: false, error: 'nao encontrada' });
    if (nome)       ficha.nome = nome;
    if (telefone)   ficha.telefone = telefone;
    if (endereco)   ficha.endereco = endereco;
    if (equipamento) ficha.equipamento = equipamento;
    if (defeito)    ficha.defeito = defeito;
    await dbSet(LOG_KEY, db);
    return res.status(200).json({ ok: true, ficha });
  }

  // ── POST salvar-diagnostico ───────────────────────────────
  if (req.method === 'POST' && action === 'salvar-diagnostico') {
    const { id, diagnostico } = req.body || {};
    const db = await dbGet(LOG_KEY) || defaultDB();
    const ficha = db.fichas.find(f => f.id === id);
    if (!ficha) return res.status(404).json({ ok: false, error: 'nao encontrada' });
    ficha.diagnostico = diagnostico;
    ficha.phase = 'orc_registrado';
    ficha.movedAt = new Date().toISOString();
    await dbSet(LOG_KEY, db);
    return res.status(200).json({ ok: true, ficha });
  }


  // ── POST gerar-orcamento — gera texto, salva no orc e move Pipefy ──
  if (req.method === 'POST' && action === 'gerar-orcamento') {
    const { id } = req.body || {};
    const db = await dbGet(LOG_KEY) || defaultDB();
    const ficha = db.fichas.find(f => f.id === id);
    if (!ficha) return res.status(404).json({ ok:false, error:'ficha nao encontrada' });
    if (!ficha.diagnostico) return res.status(400).json({ ok:false, error:'sem diagnostico' });

    const PIPEFY_API = 'https://api.pipefy.com/graphql';
    const PIPE_TOKEN = process.env.PIPEFY_TOKEN || '';
    const ORC_KEY    = 'reparoeletro_orcamentos';
    const AGUARDANDO_PHASE_ID = '334875152';

    async function pipefyQ(query) {
      const r = await fetch(PIPEFY_API, {
        method:'POST',
        headers:{'Content-Type':'application/json','Authorization':'Bearer '+PIPE_TOKEN.trim()},
        body: JSON.stringify({query})
      });
      const j = await r.json();
      if (j.errors) throw new Error(j.errors[0].message);
      return j.data;
    }

    // Gerar textos para cada equipamento do diagnóstico
    const equips = ficha.diagnostico.equips || [ficha.diagnostico];
    const nome   = ficha.nome || '';

    function priNome(n) { return n ? n.trim().split(/\s+/)[0] : 'cliente'; }

    function gerarTexto(tipo, subtipo, servicos, precoInput, templates) {
      const pn = priNome(nome);
      const s  = servicos || [];
      const tem = (lista) => s.some(x => lista.includes(x));
      const pecas = (lista) => s.filter(x => lista.includes(x)).join(', ') || s.join(', ');
      const x2 = (v) => String(Math.round(parseFloat(v||0)*2));
      const T = templates || {};
      // Substituir placeholders num template
      function applyTpl(tpl, pecasStr, preco) {
        return tpl
          .replace(/\[NOME\]/g, pn)
          .replace(/\[peças\]/g, pecasStr || s.join(', '))
          .replace(/\[VALOR\]/g, preco || '');
      }

      if (tipo === 'microondas') {
        if (tem(['Troca de Placa','Display'])) {
          const p = pecas(['Troca de Placa','Display']);
          const tpl = T.microondas_placa?.texto || `Ola, [NOME] bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario fazer a troca do conjunto conjunto da [peças], será feito a reoperação eletrica tambem. Este conserto completo fica em [VALOR] reais apenas. Aprovando ja iniciamos o conserto.`;
          return { texto: applyTpl(tpl, p, x2(precoInput)), preco:x2(precoInput) };
        }
        if (tem(['Vidro','Porta'])) {
          const p = pecas(['Vidro','Porta']);
          const tpl = T.microondas_vidro?.texto || `Ola, [NOME] bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orçamento:\n\nPara fazer a desmontagem, instalação da [peças], montagem e regulagem consigo fazer para você por [VALOR] reais apenas. Aprovando ja iniciamos o conserto.`;
          return { texto: applyTpl(tpl, p, x2(precoInput)), preco:x2(precoInput) };
        }
        if (tem(['Haste'])) { const tpl = T.microondas_haste?.texto || `Ola, [NOME] bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orçamento:\n\nPara fazer a desmontagem, instalação da haste, montagem e regulagem consigo fazer para você por [VALOR] reais apenas. Aprovando ja iniciamos o conserto.`; return { texto: applyTpl(tpl, 'haste', '350'), preco:'350' }; }
        if (tem(['Pintura'])) { const tpl = T.microondas_pintura?.texto || `Ola, [NOME] bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orçamento:\n\nPara fazer a desmontagem, pintura, montagem, regulagem e revisão consigo fazer para você por [VALOR] reais apenas. Aprovando ja iniciamos o conserto.`; return { texto: applyTpl(tpl, 'pintura', '350'), preco:'350' }; }
        const p = s.join(', ');
        const tpl = T.microondas_eletrico?.texto || `Ola, [NOME] bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario refazer a parte eletrica que causou danos no conjunto do [peças], as pecas serao trocadas tambem. Este conserto completo fica em [VALOR] reais apenas. Aprovando ja iniciamos o conserto.`;
        return { texto: applyTpl(tpl, p, '350'), preco:'350' };
      }
      if (tipo === 'purificador') {
        if (subtipo === 'Motor') {
          if (tem(['Gás'])) return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario fazer a troca da valvula de gas, solda e recarga de gas refrigerante. Este conserto completo fica em 490 reais apenas. Aprovando ja iniciamos o conserto.`, preco:'490' };
          const p = s.join(', ');
          return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario fazer a troca do conjunto da ${p}. Este conserto completo fica em 490 reais apenas. Aprovando ja iniciamos o conserto.`, preco:'490' };
        }
        if (subtipo === 'Eletrônico') {
          if (tem(['Kit Termo Elétrico'])) return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario refazer a parte eletrica que causou danos no conjunto do cooler, placa de resfriamento e pasta termica, as pecas serao trocadas tambem. Este conserto completo fica em 350 reais apenas. Aprovando ja iniciamos o conserto.`, preco:'350' };
          if (tem(['Recuperação de Placa'])) return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario refazer a parte eletrica que causou danos no conjunto da placa principal, será feito a reoperação da placa tambem. Este conserto completo fica em 350 reais apenas. Aprovando ja iniciamos o conserto.`, preco:'350' };
          const p = s.join(', ');
          return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario fazer a troca do conjunto da ${p}. Este conserto completo fica em 350 reais apenas. Aprovando ja iniciamos o conserto.`, preco:'350' };
        }
      }
      if (tipo === 'adega') {
        if (subtipo === 'Motor') {
          if (tem(['Gás'])) return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario fazer a troca da valvula de gas, solda e recarga de gas refrigerante. Este conserto completo fica em 490 reais apenas. Aprovando ja iniciamos o conserto.`, preco:'490' };
          if (tem(['Recuperação de Placa'])) return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario refazer a parte eletrica que causou danos no conjunto da placa principal, será feito a reoperação da placa tambem. Este conserto completo fica em 490 reais apenas. Aprovando ja iniciamos o conserto.`, preco:'490' };
          if (tem(['Troca de Placa'])) return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario fazer a troca do conjunto conjunto da Placa Principal, será feito a reoperação eletrica tambem. Este conserto completo fica em ${x2(precoInput)} reais apenas. Aprovando ja iniciamos o conserto.`, preco:x2(precoInput) };
        }
        if (subtipo === 'Eletrônico') {
          if (tem(['Kit Termo Elétrico'])) return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario refazer a parte eletrica que causou danos no conjunto do cooler, placa de resfriamento e pasta termica, as pecas serao trocadas tambem. Este conserto completo fica em 350 reais apenas. Aprovando ja iniciamos o conserto.`, preco:'350' };
          if (tem(['Recuperação de Placa'])) return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario refazer a parte eletrica que causou danos no conjunto da placa principal, será feito a reoperação da placa tambem. Este conserto completo fica em 350 reais apenas. Aprovando ja iniciamos o conserto.`, preco:'350' };
          if (tem(['Troca de Placa'])) return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario fazer a troca do conjunto conjunto da Placa Principal, será feito a reoperação eletrica tambem. Este conserto completo fica em ${x2(precoInput)} reais apenas. Aprovando ja iniciamos o conserto.`, preco:x2(precoInput) };
        }
      }
      if (tipo === 'forno') {
        const pb = subtipo === 'Grande' ? '790' : '490';
        if (tem(['Troca de Placa','Display'])) {
          const p = pecas(['Troca de Placa','Display']);
          return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario fazer a troca do conjunto conjunto do ${p}: será feito a reoperação eletrica tambem. Este conserto completo fica em ${x2(precoInput)} reais apenas. Aprovando ja iniciamos o conserto.`, preco:x2(precoInput) };
        }
        if (tem(['Porta','Vidro','Mola'])) {
          const p = pecas(['Porta','Vidro','Mola']);
          return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orçamento:\n\nPara fazer a desmontagem, instalação da ${p}, montagem e regulagem consigo fazer para você por ${x2(precoInput)} reais apenas. Aprovando ja iniciamos o conserto.`, preco:x2(precoInput) };
        }
        const p = s.join(', ');
        return { texto:`Ola, ${pn} bom dia, sou o Alessandro da Reparo Eletro, vou te enviar agora o orcamento:\n\nForam feitos todos os testes e identificamos que sera necessario refazer a parte eletrica que causou danos no conjunto da ${p}, será feito a reoperação da placa tambem. Este conserto completo fica em ${pb} reais apenas. Aprovando ja iniciamos o conserto.`, preco:pb };
      }
      return { texto: null, preco: null };
    }

    // Carregar templates customizados do Redis
    let customTemplates = {};
    try {
      const tplDb = await dbGet('reparoeletro_orc_templates');
      if (tplDb) customTemplates = tplDb;
    } catch(e) { console.error('[Log] templates:', e.message); }

    // Gerar texto para cada equipamento
    const resultados = equips.map(eq =>
      gerarTexto(eq.tipo, eq.subtipo, eq.servicos, eq.preco, customTemplates)
    );

    // Texto final
    let textoFinal, precoFinal;
    if (resultados.length === 1) {
      textoFinal = resultados[0].texto;
      precoFinal = resultados[0].preco;
    } else {
      const qtd      = resultados.length;
      const soma     = resultados.reduce((acc,r)=>acc+parseInt(r.preco||0),0);
      const descPerc = qtd === 2 ? 0.10 : qtd === 3 ? 0.15 : 0.20; // max 20%
      const comDesc  = Math.round(soma * (1 - descPerc));
      precoFinal     = String(comDesc);

      // Remover "Aprovando ja iniciamos o conserto" de cada texto individual
      const removeAprovando = (txt) =>
        (txt||'').replace(/\s*Aprovando ja iniciamos o conserto\.?/gi, '').trimEnd();

      // Montar textos individuais sem a frase final
      const partes = resultados.map((r,i) =>
        `Equipamento ${i+1}:\n${removeAprovando(r.texto||'')}`
      ).join('\n\n');

      // Frase de desconto no final
      const fraseFinal = `Consertando os ${qtd} juntos eu consigo um desconto para voce de ${soma} para ${comDesc} reais. Aprovando ja iniciamos o conserto.`;
      textoFinal = partes + '\n\n' + fraseFinal;
    }

    // Salvar na Logística
    ficha.diagnostico.textoOrc = textoFinal;
    ficha.diagnostico.preco    = precoFinal;
    ficha.phase   = 'orc_registrado';
    ficha.movedAt = new Date().toISOString();
    await dbSet(LOG_KEY, db);

    // Salvar no Redis de orçamentos (ORC_KEY) — formato compatível com orc-sync
    try {
      const orcDb = (await dbGet(ORC_KEY)) || { fichas:[], syncedIds:[], initialized:true };
      const orcFicha = {
        id:            ficha.pipefyCardId || ficha.id,
        pipefyId:      ficha.pipefyCardId || ficha.id,
        nome:          ficha.nome,
        tel:           ficha.telefone || '',
        desc:          ficha.equipamento + ' — ' + ficha.defeito,
        end:           ficha.endereco || '',
        age:           null,
        comentarios:   [],
        textoOrc:      textoFinal,
        precoSugerido: precoFinal,
        status:        'pendente',
        preco:         null,
        createdAt:     new Date().toISOString(),
      };
      // Evitar duplicata
      if (!orcDb.fichas.find(f => f.id === orcFicha.id)) {
        orcDb.fichas.unshift(orcFicha);
        if (ficha.pipefyCardId && !orcDb.syncedIds.includes(ficha.pipefyCardId)) {
          orcDb.syncedIds.push(ficha.pipefyCardId);
        }
        await dbSet(ORC_KEY, orcDb);
      }
    } catch(e) { console.error('[Log] orc-key:', e.message); }

    // Mover card Pipefy para Aguardando Aprovação + atualizar valor
    if (ficha.pipefyCardId) {
      try {
        await pipefyQ(`mutation { moveCardToPhase(input: { card_id: "${ficha.pipefyCardId}", destination_phase_id: "${AGUARDANDO_PHASE_ID}" }) { card { id } } }`);
        if (precoFinal) {
          await pipefyQ(`mutation { updateCardField(input: { card_id: "${ficha.pipefyCardId}", field_id: "valor_de_contrato", new_value: "${precoFinal}" }) { success } }`);
        }
        console.log('[Log] Pipefy movido para Aguardando:', ficha.pipefyCardId);
      } catch(e) { console.error('[Log] Pipefy move:', e.message); }
    }

    return res.status(200).json({ ok:true, textoFinal, precoFinal, ficha });
  }


  // ── GET limpar-orc-registrado — cron noturno, limpa coluna Orçamento Registrado ──
  if (action === 'limpar-orc-registrado') {
    const db = await dbGet(LOG_KEY) || defaultDB();
    const antes = db.fichas.length;
    db.fichas = db.fichas.filter(f => f.phase !== 'orc_registrado');
    const removidas = antes - db.fichas.length;
    if (removidas > 0) await dbSet(LOG_KEY, db);
    return res.status(200).json({ ok: true, removidas, restantes: db.fichas.length });
  }

  // ── POST cancelar ────────────────────────────────────────
  if (req.method === 'POST' && action === 'cancelar') {
    const { id } = req.body || {};
    const db = await dbGet(LOG_KEY) || defaultDB();
    db.fichas = db.fichas.filter(f => f.id !== id);
    await dbSet(LOG_KEY, db);
    return res.status(200).json({ ok: true });
  }

  return res.status(404).json({ ok: false, error: 'ação não encontrada' });
};
