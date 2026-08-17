// api/fila-ligacao.js — Fila de ligação ativa (Fase 0)
// Ordena as fichas da prospecção por prioridade de conversão e registra o
// desfecho de cada ligação. NÃO disca — o operador liga do telefone dele.
//
//   GET  ?action=fila&k=CHAVE[&curto=1][&limite=50]
//   POST ?action=desfecho&k=CHAVE   { id, sistema, atendeu, resultado, obs, duracaoSeg }
//   GET  ?action=relatorio&k=CHAVE[&dias=7][&curto=1]

const U = (process.env.UPSTASH_URL || '').replace(/['"]/g, '').trim();
const T = (process.env.UPSTASH_TOKEN || '').replace(/['"]/g, '').trim();
const LOG_KEY = 'fila_ligacao_log';

async function db(cmds) {
  const r = await fetch(U + '/pipeline', {
    method: 'POST',
    headers: { Authorization: 'Bearer ' + T, 'Content-Type': 'application/json' },
    body: JSON.stringify(cmds),
  });
  return r.json();
}
async function dbGet(k) {
  try { const j = await db([['GET', k]]); return j[0]?.result ? JSON.parse(j[0].result) : null; }
  catch (e) { return null; }
}
async function dbSet(k, v) {
  try { await db([['SET', k, JSON.stringify(v)]]); return true; } catch (e) { return false; }
}

// ── PRIORIDADE (ordem definida pelo Pedro) ──────────────────────────
// 1 Entrar em Contato · 2 Conflito de REPROVAÇÃO · 3 Retornar
// 4 Cliente Loja VERMELHO · 5 Lead
// Ordem: quem está esperando contato vem primeiro; o lead, que é o mais frio,
// por último. Retornar vem antes de Cliente Loja porque é compromisso marcado.
const PRIO = { entrar_contato: 1, conflito_reprovacao: 2, retornar: 3, cliente_loja_vermelho: 4, lead: 5 };
const ROTULO = {
  entrar_contato: 'Entrar em Contato', conflito_reprovacao: 'Conflito — reprovou orçamento',
  cliente_loja_vermelho: 'Cliente Loja — prometeu e não veio', retornar: 'Retornar', lead: 'Lead',
};

// Só entram na fila os conflitos de REPROVAÇÃO de orçamento.
// Os demais conflitos são resolvidos por mensagem, não por ligação.
function ehConflitoDeReprovacao(f) {
  const m = String(f.motivoConflito || '').toLowerCase();
  if (/an[áa]lise de compra|promessa n[ãa]o cumprida/.test(m)) return false;
  return /reprov|recus|n[ãa]o aprov|devolu[çc][ãa]o|5 fases/.test(m);
}

// Cliente Loja vermelho = passou de 48h sem definir, ou confirmou e não veio em 7 dias
function lojaVermelho(f) {
  const agora = Date.now();
  if (f.lojaConfirmouEm) {
    const dias = (agora - new Date(f.lojaConfirmouEm).getTime()) / 86400000;
    return dias > 7 ? { sim: true, txt: 'confirmou há ' + Math.floor(dias) + ' dias e não veio' } : { sim: false };
  }
  const h = (agora - new Date(f.movidoEm || f.criadoEm || 0).getTime()) / 3600000;
  return h > 48 ? { sim: true, txt: 'há ' + Math.floor(h) + 'h sem definição' } : { sim: false };
}

const d8 = t => String(t || '').replace(/\D/g, '').slice(-8);

export default async function handler(req, res) {
  const q = req.query || {};
  if (String(q.k || '').trim() !== ((process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim())) {
    return res.status(401).json({ ok: false, error: 'chave invalida' });
  }
  const action = q.action || 'fila';
  const curto = q.curto === '1';

  // ── POST desfecho ───────────────────────────────────────────────
  if (req.method === 'POST' && action === 'desfecho') {
    const { id, sistema, atendeu, resultado, obs, duracaoSeg, telefone, nome } = req.body || {};
    if (!id) return res.status(400).json({ ok: false, error: 'id obrigatorio' });
    if (!atendeu) return res.status(400).json({ ok: false, error: 'informe se atendeu' });
    const log = (await dbGet(LOG_KEY)) || { regs: [] };
    if (!Array.isArray(log.regs)) log.regs = [];
    log.regs.unshift({
      ts: new Date().toISOString(), id, sistema: sistema || 'adm',
      nome: String(nome || '').slice(0, 60), telefone: String(telefone || '').replace(/\D/g, ''),
      atendeu: String(atendeu), resultado: String(resultado || ''),
      obs: String(obs || '').slice(0, 200), duracaoSeg: parseInt(duracaoSeg || 0, 10) || 0,
    });
    if (log.regs.length > 3000) log.regs.splice(3000);
    const ok = await dbSet(LOG_KEY, log);
    if (!ok) return res.status(500).json({ ok: false, error: 'não consegui gravar o desfecho' });
    // confere o próprio resultado
    const chk = (await dbGet(LOG_KEY)) || { regs: [] };
    const gravou = (chk.regs || []).some(r => r.id === id && r.ts === log.regs[0].ts);
    if (!gravou) return res.status(500).json({ ok: false, error: 'desfecho NÃO confirmado na releitura' });
    return res.status(200).json({ ok: true, registrado: log.regs[0] });
  }

  // ── GET fila ────────────────────────────────────────────────────
  if (action === 'fila') {
    const limite = Math.min(parseInt(q.limite || '50', 10) || 50, 200);
    const [prosp, fAdm, fTv, log] = await Promise.all([
      dbGet('prospeccao_adm'), dbGet('fichas_adm'), dbGet('fichas_tv'), dbGet(LOG_KEY),
    ]);
    const regs = (log || {}).regs || [];
    // já ligado hoje? evita repetir o mesmo cliente no mesmo dia
    const hoje = new Date().toISOString().slice(0, 10);
    const ligadosHoje = new Set(regs.filter(r => String(r.ts).slice(0, 10) === hoje).map(r => d8(r.telefone)));
    const tentativasPor = {};
    for (const r of regs) { const k = d8(r.telefone); tentativasPor[k] = (tentativasPor[k] || 0) + 1; }

    const itens = [];
    const push = (f, tipo, nota, sis) => {
      const tel = String(f.telefone || '').replace(/\D/g, '');
      if (tel.length < 10) return;
      if (ligadosHoje.has(d8(tel))) return;
      itens.push({
        id: f.id, sistema: sis || f.origemSistema || 'adm', tipo, prio: PRIO[tipo],
        rotulo: ROTULO[tipo], nome: f.nome || f.nomeContato || 'Cliente', telefone: tel,
        equipamento: f.equipamento || '', defeito: f.defeito || '',
        nota: nota || '', motivo: String(f.motivoConflito || '').slice(0, 200),
        desde: f.movidoEm || f.criadoEm || null,
        tentativas: tentativasPor[d8(tel)] || 0,
      });
    };

    for (const f of ((prosp || {}).fichas || [])) {
      const st = f.status || '';
      if (st === 'entrar_contato') push(f, 'entrar_contato', '');
      else if (st === 'conflitos_bot') { if (ehConflitoDeReprovacao(f)) push(f, 'conflito_reprovacao', 'tentativa humana antes da devolução'); }
      else if (st === 'cliente_loja') { const v = lojaVermelho(f); if (v.sim) push(f, 'cliente_loja_vermelho', v.txt + ' — oferecer coleta'); }
      else if (st === 'retornar') push(f, 'retornar', '');
      else if (st === 'lead') push(f, 'lead', '');
    }
    // espelho das fichas em entrar_contato dos dois sistemas
    for (const [banco, sis] of [[fAdm, 'adm'], [fTv, 'tv']]) {
      for (const f of ((banco || {}).fichas || [])) {
        if (f.status === 'entrar_contato') push(f, 'entrar_contato', '', sis);
      }
    }
    // dedupe por telefone mantendo a maior prioridade
    const porTel = {};
    for (const it of itens) {
      const k = d8(it.telefone);
      if (!porTel[k] || it.prio < porTel[k].prio) porTel[k] = it;
    }
    // ordena: prioridade, depois menos tentativas, depois mais antigo
    const fila = Object.values(porTel).sort((a, b) =>
      a.prio - b.prio || a.tentativas - b.tentativas ||
      String(a.desde || '').localeCompare(String(b.desde || ''))).slice(0, limite);

    const resumo = {};
    for (const it of fila) resumo[it.rotulo] = (resumo[it.rotulo] || 0) + 1;
    if (curto) {
      return res.status(200).send('FILA total=' + fila.length + '\n' +
        Object.entries(resumo).map(([k, v]) => k + '=' + v).join(' · ') + '\n' +
        fila.slice(0, 30).map((i, n) => (n + 1) + ';' + i.rotulo + ';' + i.nome + ';' + i.telefone +
          ';' + (i.equipamento || '-') + ';tent=' + i.tentativas).join('\n'));
    }
    return res.status(200).json({ ok: true, total: fila.length, resumo, fila });
  }

  // ── GET relatorio ───────────────────────────────────────────────
  if (action === 'relatorio') {
    const dias = Math.min(parseInt(q.dias || '7', 10) || 7, 90);
    const corte = Date.now() - dias * 86400000;
    const log = (await dbGet(LOG_KEY)) || { regs: [] };
    const regs = ((log.regs) || []).filter(r => new Date(r.ts).getTime() >= corte);
    const conta = (arr, campo) => arr.reduce((m, r) => { const k = r[campo] || '—'; m[k] = (m[k] || 0) + 1; return m; }, {});
    const atenderam = regs.filter(r => r.atendeu === 'sim');
    const seg = atenderam.reduce((s, r) => s + (r.duracaoSeg || 0), 0);
    const porHora = {};
    for (const r of regs) {
      const h = new Date(new Date(r.ts).getTime() - 3 * 3600000).getUTCHours();
      porHora[h] = porHora[h] || { total: 0, at: 0 };
      porHora[h].total++; if (r.atendeu === 'sim') porHora[h].at++;
    }
    const horas = Object.entries(porHora).sort((a, b) => a[0] - b[0])
      .map(([h, v]) => h + 'h:' + Math.round((v.at / v.total) * 100) + '%(' + v.total + ')');
    const out = {
      ok: true, dias, ligacoes: regs.length, atendidas: atenderam.length,
      taxaAtendimento: regs.length ? Math.round((atenderam.length / regs.length) * 100) : 0,
      tempoTotalMin: Math.round(seg / 60),
      tempoMedioSeg: atenderam.length ? Math.round(seg / atenderam.length) : 0,
      porDesfecho: conta(regs, 'atendeu'), porResultado: conta(atenderam, 'resultado'),
      atendimentoPorHora: horas,
    };
    if (curto) {
      return res.status(200).send('RELATORIO ' + dias + 'd\n' +
        'ligacoes=' + out.ligacoes + ' atendidas=' + out.atendidas + ' taxa=' + out.taxaAtendimento + '%' +
        ' tempo=' + out.tempoTotalMin + 'min medio=' + out.tempoMedioSeg + 's\n' +
        'desfecho: ' + Object.entries(out.porDesfecho).map(([k, v]) => k + '=' + v).join(' ') + '\n' +
        'resultado: ' + Object.entries(out.porResultado).map(([k, v]) => k + '=' + v).join(' ') + '\n' +
        'atendimento por hora: ' + horas.join(' '));
    }
    return res.status(200).json(out);
  }

  return res.status(400).json({ ok: false, error: 'action invalida (fila|desfecho|relatorio)' });
}
