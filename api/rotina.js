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
    // rede de segurança do remarcar: passou de 20 para 10 minutos ao entrar aqui
    feitos.push(await chamar('rede de segurança do remarcar',
      'tv-logistica?action=reprocessar-remarcar&aplicar=1'));
    // ⏱️ a régua que move de Contato Feito para Entrar em Contato só rodava quando
    // alguém abria a tela de fichas — agora corre sozinha
    feitos.push(await chamar('régua de contato', 'fichas?action=load&sistema=adm'));
    feitos.push(await chamar('régua de contato TV', 'fichas?action=load&sistema=tv'));
    feitos.push(await chamar('destravar fichas abordadas', 'wa-bot?action=destravar-criadas&aplicar=1'));
    // 📺 TV condenada: avisa quem foi para retirada e recolhe a escolha do cliente
    feitos.push(await chamar('avisar TV condenada', 'wa-bot?action=tv-condenada-avisar&aplicar=1'));
    feitos.push(await chamar('escolhas da TV condenada', 'wa-bot?action=tv-condenada-respostas'));
    // 📣 avisos de conflito ao cliente. O aviso de abertura sai quando o
    // conflito é registrado; o de desfecho quando ele chega em Relatar Cliente,
    // que é onde a equipe dá o caso por encerrado — o campo de solução guarda
    // o andamento da produção e não servia como critério.
    const AVISOS_CONFLITO_LIGADOS = true;
    if (AVISOS_CONFLITO_LIGADOS) {
      feitos.push(await chamar('avisos de conflito ao cliente',
        'conflitos?action=avisar-clientes&aplicar=1'));
    }
  }

  // ── 🕐 de hora em hora: rede de segurança ──
  else if (action === 'de-hora-em-hora') {
    // 🕐 hora e dia de Brasília, usados pelas rotinas com horário próprio.
    // Declarados no início do bloco: usá-los antes derruba a rotina inteira.
    const agoraBR2 = new Date(Date.now() - 3 * 3600000);
    const horaBR = agoraBR2.getUTCHours(), diaSem = agoraBR2.getUTCDay();

    feitos.push(await chamar('trazer fichas da planilha',
      'fichas?action=sync-completo&dias=3&aplicar=1'));
    feitos.push(await chamar('devoluções pendentes do remarcar',
      'logistica?action=processar-pendentes&aplicar=1'));
    feitos.push(await chamar('exame do sistema', 'vigia?action=exame-completo&horas=48'));
    // 📇 disparo registrado sem mensagem correspondente deixa o cliente invisível:
    // a régua o dá por atendido e ninguém o cobra. Confere sem aplicar nada.
    feitos.push(await chamar('disparos sem prova de envio',
      'wa-bot?action=conferir-disparos-fantasma'));
    // 📈 fotografia do ritmo do controle de qualidade: três leituras por dia
    // bastam para medir entrada, vazão e tempo de atendimento sem inflar o log
    if ([9, 14, 19].includes(horaBR)) {
      feitos.push(await chamar('ritmo da qualidade', 'qualidade?action=registrar-ritmo'));
    }
    // 🌙 fechamento do dia: última chance de trazer o que ficou para trás, e
    // somente do próprio dia. A rotina roda aos 58 minutos justamente para que
    // esta passagem caia às 23:58 — se fosse mais cedo, a linha registrada
    // depois disso se perderia na virada.
    if (horaBR === 23) {
      feitos.push(await chamar('fechamento do dia — fichas de hoje',
        'fichas?action=sync-completo&aplicar=1'));
    }
    // 🏪 lembrete de retirada: só às 10h, uma vez por dia (o limite de crons
    // da Vercel está esgotado, então a rotina de hora em hora faz o papel)

    // segunda a sábado, às 10h: no domingo a loja não atende
    if (horaBR === 10 && diaSem >= 1 && diaSem <= 6) {
      feitos.push(await chamar('lembrete de retirada na loja',
        'wa-bot?action=retirada-loja&aplicar=1'));
    }
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
