// ═══════════════════════════════════════════════════════════════════
// ROTINA — agrupa tarefas periódicas num só cron.
// Existe porque a Vercel limita a 40 cron jobs e o projeto passou disso:
// os três últimos registrados nunca chegaram a rodar, e fichas da planilha
// deixaram de entrar sem que nada acusasse.
// ═══════════════════════════════════════════════════════════════════
const K = () => (process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim();
const BASE = 'https://reparoeletroadm.com/api/';

async function chamar(rotulo, url) {
  const t0 = Date.now();
  try {
    const r = await fetch(BASE + url + '&k=' + K()).then(x => x.json());
    return { rotulo, ok: r && r.ok !== false,
      resumo: r.criadas ? JSON.stringify(r.criadas)
        : (r.recriadas ?? r.enviadas ?? r.resolvidas ?? r.destravadas ??
           r.totalDeProblemas ?? r.pendentes ?? 'ok'),
      erro: (r && r.ok === false) ? (r.error || 'falhou') : null,
      ms: Date.now() - t0 };
  } catch (e) { return { rotulo, ok: false, erro: e.message, ms: Date.now() - t0 }; }
}

module.exports = async function handler(req, res) {
  const _k = (req.query && req.query.k) || req.headers['x-tf-key'] || '';
  if (_k !== K()) return res.status(401).json({ ok: false, error: 'não autorizado' });
  const { action } = req.query;
  const feitos = [];

  // ── ⏱️ a cada 10 minutos: o que não pode esperar ──
  if (action === 'frequente') {
    feitos.push(await chamar('devoluções pendentes do remarcar',
      'logistica?action=processar-pendentes&aplicar=1'));
    feitos.push(await chamar('mensagens aguardando a janela reabrir',
      'wa-bot?action=despachar-pendentes&aplicar=1'));
  }

  // ── 🕐 de hora em hora: rede de segurança ──
  else if (action === 'de-hora-em-hora') {
    feitos.push(await chamar('trazer fichas da planilha',
      'fichas?action=sync-completo&dias=3&aplicar=1'));
    feitos.push(await chamar('devoluções pendentes do remarcar',
      'logistica?action=processar-pendentes&aplicar=1'));
    feitos.push(await chamar('exame do sistema', 'vigia?action=exame-completo&horas=48'));
  }
  else return res.status(400).json({ ok: false, acoes: ['frequente', 'de-hora-em-hora'] });

  // registro para conferência
  try {
    const U = (process.env.UPSTASH_URL || '').replace(/['"]/g, '').trim();
    const T = (process.env.UPSTASH_TOKEN || '').replace(/[\n\r'"]/g, '').trim();
    const dia = new Date(Date.now() - 3 * 3600000).toISOString().slice(0, 10);
    const chave = 'rotina_log_' + dia;
    const atual = await fetch(`${U}/get/${chave}`, { headers: { Authorization: `Bearer ${T}` } })
      .then(x => x.json()).then(r => (r && r.result) ? JSON.parse(r.result) : { itens: [] })
      .catch(() => ({ itens: [] }));
    atual.itens = [{ ts: new Date().toISOString(), acao: action, feitos }]
      .concat(atual.itens || []).slice(0, 200);
    await fetch(`${U}/set/${chave}`, { method: 'POST',
      headers: { Authorization: `Bearer ${T}`, 'Content-Type': 'application/json' },
      body: JSON.stringify(atual) });
  } catch (e) {}

  const falhas = feitos.filter(f => !f.ok);
  return res.status(200).json({ ok: falhas.length === 0, acao: action,
    executadas: feitos.length, falhas: falhas.length, feitos });
};
