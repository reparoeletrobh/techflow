import { dbGet } from './frenteloja.js';

const FL_KEY    = 'reparoeletro_frenteloja';
const BOARD_KEY = 'reparoeletro_board';

const U = process.env.UPSTASH_URL;
const T = process.env.UPSTASH_TOKEN;

async function dbGetLocal(k) {
  try {
    const r = await fetch(U+'/pipeline', {
      method:'POST',
      headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},
      body: JSON.stringify([['GET',k]])
    });
    const j = await r.json();
    return j[0]?.result ? JSON.parse(j[0].result) : null;
  } catch(e) { return null; }
}

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin','*');
  
  const [flDb, boardDb] = await Promise.all([
    dbGetLocal(FL_KEY),
    dbGetLocal(BOARD_KEY)
  ]);
  
  const fichas  = flDb?.fichas  || [];
  const cards   = boardDb?.cards || [];
  
  const mapa = {
    cliente_loja:'producao', producao:'producao', comprar_peca:'producao',
    aguardando_peca:'producao', urgencia:'producao', peca_disponivel:'producao',
    aguardando_ret:'producao', loja_feito:'conserto_realizado',
    delivery_feito:'pago', finalizado:'pago'
  };
  
  const resultado = fichas
    .filter(f => ['producao','conserto_realizado'].includes(f.phase))
    .map(f => {
      const card = cards.find(c =>
        c.flFichaId === f.id ||
        (f.pipefyCardId && String(c.pipefyId) === String(f.pipefyCardId))
      );
      const expectedFL = card ? (mapa[card.phaseId] || '?') : null;
      const correto = !card || expectedFL === f.phase;
      return {
        id: f.id, nome: f.nomeContato,
        flPhase: f.phase, boardPhase: card?.phaseId || 'sem_card',
        correto
      };
    });
  
  return res.status(200).json({ ok:true, total: resultado.length, fichas: resultado });
}
