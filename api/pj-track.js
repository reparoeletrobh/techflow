// api/pj-track.js — Tracking de engajamento dos leads SDR PJ
// GET /api/pj-track?e=open&id=sdr-xxx        → pixel abertura email → +15 pts
// GET /api/pj-track?e=apresentacao&id=sdr-xxx → clique apresentação → +20 pts → redireciona
// GET /api/pj-track?e=proposta&id=sdr-xxx     → solicitar proposta  → +40 pts → redireciona

const U = (process.env.UPSTASH_URL   || '').replace(/['"]/g,'').trim();
const T = (process.env.UPSTASH_TOKEN || '').replace(/['"]/g,'').trim();
const KEY = 'pj_sdr';

async function dbGet(k){
  try{
    const r=await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});
    const j=await r.json(); const v=j[0]?.result; if(!v)return null;
    let x=JSON.parse(v); if(typeof x==='string')x=JSON.parse(x); return x;
  }catch(e){return null;}
}
async function dbSet(k,v){
  await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});
}

// GIF 1×1 pixel transparente
const PIXEL_GIF = Buffer.from('R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7','base64');

export default async function handler(req, res) {
  const event = req.query.e || '';
  const leadId = req.query.id || '';
  const now = new Date().toISOString();

  // Sempre responder rápido — tracking não bloqueia UX
  const db = leadId ? await dbGet(KEY) : null;
  const lead = db?.leads?.find(l => l.id === leadId);

  if (lead) {
    let atualizado = false;

    if (event === 'open' && !lead.emailAberto) {
      lead.emailAberto = true;
      lead.emailAbertoEm = now;
      lead.historico = lead.historico || [];
      lead.historico.unshift({ ts: now, texto: '📧 Email de apresentação aberto', tipo: 'tracking', pontos: '+15' });
      atualizado = true;
      console.log('[track] email aberto:', leadId);
    }

    if (event === 'apresentacao') {
      if (!lead.apresentacaoVista) {
        lead.apresentacaoVista = true;
        lead.historico = lead.historico || [];
        lead.historico.unshift({ ts: now, texto: '🖥️ Clicou em Ver Apresentação Completa', tipo: 'tracking', pontos: '+20' });
        atualizado = true;
      }
      // Registrar cada visita
      lead.apresentacaoVisitas = (lead.apresentacaoVisitas || 0) + 1;
      lead.apresentacaoUltimaEm = now;
      atualizado = true;
      console.log('[track] apresentacao clicada:', leadId);
    }

    if (event === 'proposta' && !lead.propostaSolicitada) {
      lead.propostaSolicitada = true;
      lead.propostaSolicitadaEm = now;
      lead.historico = lead.historico || [];
      lead.historico.unshift({ ts: now, texto: '🔥 Clicou em Solicitar Proposta', tipo: 'tracking', pontos: '+40' });
      // Atualizar fase para 'proposta' se ainda for inicial
      if (['novo','contatado','qualificado'].includes(lead.fase)) {
        lead.fase = 'proposta';
        lead.historico.unshift({ ts: now, texto: '📋 Fase atualizada para Proposta (via tracking)', tipo: 'fase_auto' });
      }
      atualizado = true;
      console.log('[track] proposta solicitada:', leadId);
    }

    if (atualizado) {
      db.leads = db.leads.map(l => l.id === leadId ? lead : l);
      await dbSet(KEY, db);
    }
  }

  // Responder conforme o tipo de evento
  if (event === 'open') {
    // Retornar pixel 1x1 transparente
    res.setHeader('Content-Type', 'image/gif');
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, max-age=0');
    res.setHeader('Pragma', 'no-cache');
    return res.status(200).send(PIXEL_GIF);
  }

  if (event === 'apresentacao') {
    return res.redirect(302, 'https://reparoeletroadm.com/institucional');
  }

  if (event === 'proposta') {
    // Redirecionar para WhatsApp com mensagem pré-preenchida
    const nome = lead?.responsavel || '';
    const empresa = lead?.empresa || '';
    const msg = encodeURIComponent(`Olá Pedro! Sou ${nome} da ${empresa} e gostaria de solicitar uma proposta de manutenção.`);
    return res.redirect(302, `https://wa.me/5531997862505?text=${msg}`);
  }

  return res.status(200).json({ ok: true });
}
