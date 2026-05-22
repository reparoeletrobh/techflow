const BOARD_KEY = 'reparoeletro_board';
const FL_KEY    = 'reparoeletro_frenteloja';

async function dbGet(key) {
  const U = process.env.UPSTASH_URL;
  const T = process.env.UPSTASH_TOKEN;
  const r = await fetch(`${U}/get/${key}`, {
    headers: { Authorization: `Bearer ${T}` }
  });
  const j = await r.json();
  return j.result ? JSON.parse(j.result) : null;
}

const MAPA = {
  cliente_loja:'producao', producao:'producao', comprar_peca:'producao',
  aguardando_peca:'producao', urgencia:'producao', peca_disponivel:'producao',
  aguardando_ret:'producao', loja_feito:'conserto_realizado',
  delivery_feito:'pago', finalizado:'pago'
};

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin','*');
  res.setHeader('Cache-Control','no-cache');

  const [flDb, boardDb] = await Promise.all([dbGet(FL_KEY), dbGet(BOARD_KEY)]);
  const fichas = flDb?.fichas || [];
  const cards  = boardDb?.cards || [];

  const rows = fichas
    .filter(f => ['producao','conserto_realizado'].includes(f.phase))
    .map(f => {
      const card = cards.find(c =>
        c.flFichaId === f.id ||
        (f.pipefyCardId && String(c.pipefyId) === String(f.pipefyCardId))
      );
      const expectedFL = card ? (MAPA[card.phaseId] || '?') : 'sem_card';
      const correto = expectedFL === f.phase || expectedFL === 'sem_card';
      const status = correto ? 'OK' : 'ERRO';
      return [f.id, f.nomeContato, f.phase, card?.phaseId||'sem_card', expectedFL, status].join(',');
    });

  const csv = ['ID,Nome,FL_Phase,Board_Phase,Esperado_FL,Status', ...rows].join('\n');
  res.setHeader('Content-Type','text/plain');
  res.status(200).send(csv);
}
