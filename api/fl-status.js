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

const MAPA_FL = {
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

  const resultado = fichas
    .filter(f => ['producao','conserto_realizado'].includes(f.phase))
    .map(f => {
      const card = cards.find(c =>
        c.flFichaId === f.id ||
        (f.pipefyCardId && String(c.pipefyId) === String(f.pipefyCardId))
      );
      const boardPhase = card?.phaseId || 'sem_card';
      const expectedFL = MAPA_FL[boardPhase] || null;
      
      // Determinar status
      let status;
      if (boardPhase === 'sem_card') {
        status = 'SEM_CARD'; // aprovado mas não aparece no Técnico
      } else if (expectedFL === f.phase) {
        status = 'OK';
      } else {
        status = 'ERRO';
      }

      const movedAt = f.history?.filter(h=>h.phase==='producao').slice(-1)[0]?.ts 
                   || f.movedAt || f.criadoEm || '';

      return [
        f.id,
        f.nomeContato?.replace(/,/g,''),
        f.equipamento?.replace(/,/g,'') || '',
        f.phase,
        boardPhase,
        expectedFL || '?',
        status,
        movedAt ? movedAt.slice(0,10) : ''
      ].join(',');
    })
    .sort((a,b) => {
      // ERRO e SEM_CARD primeiro
      const sa = a.split(',')[6], sb = b.split(',')[6];
      const order = {ERRO:0, SEM_CARD:1, OK:2};
      return (order[sa]||3) - (order[sb]||3);
    });

  const csv = ['ID,Nome,Equipamento,FL_Phase,Board_Phase,Esperado_FL,Status,Data'].concat(resultado).join('\n');
  res.setHeader('Content-Type','text/plain; charset=utf-8');
  res.status(200).send(csv);
}
