// api/pj-webhook.js — Resend Inbound webhook para emails PJ
const UPSTASH_URL   = (process.env.UPSTASH_URL   || '').replace(/['"]/g,'').trim();
const UPSTASH_TOKEN = (process.env.UPSTASH_TOKEN || '').replace(/['"]/g,'').trim();
const INBOX_KEY     = 'pj_inbox';

async function dbGet(key) {
  try {
    const r = await fetch(UPSTASH_URL+'/pipeline',{method:'POST',
      headers:{Authorization:'Bearer '+UPSTASH_TOKEN,'Content-Type':'application/json'},
      body:JSON.stringify([['GET',key]])});
    const j = await r.json();
    const v = j[0]?.result;
    if (!v) return null;
    let x = JSON.parse(v); if (typeof x==='string') x=JSON.parse(x); return x;
  } catch(e){ return null; }
}

async function dbSet(key,val) {
  await fetch(UPSTASH_URL+'/pipeline',{method:'POST',
    headers:{Authorization:'Bearer '+UPSTASH_TOKEN,'Content-Type':'application/json'},
    body:JSON.stringify([['SET',key,JSON.stringify(val)]])});
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin','*');
  if (req.method === 'OPTIONS') return res.status(200).end();

  // Resend envia POST com o email recebido
  if (req.method === 'POST') {
    try {
      const event = req.body || {};

      // Aceitar qualquer evento (verificação ou email real)
      if (event.type !== 'email.received') {
        return res.status(200).json({ ok: true, ignored: true });
      }

      // Salvar email recebido no Redis
      const inbox = (await dbGet(INBOX_KEY)) || { emails: [] };
      inbox.emails.unshift({
        id:          event.data?.email_id || Date.now().toString(36),
        de:          event.data?.from || '',
        para:        event.data?.to   || '',
        assunto:     event.data?.subject || '',
        texto:       event.data?.text || '',
        html:        event.data?.html || '',
        recebidoEm:  event.data?.created_at || new Date().toISOString(),
        lido:        false,
      });
      inbox.emails = inbox.emails.slice(0, 500);
      await dbSet(INBOX_KEY, inbox);

      return res.status(200).json({ ok: true });
    } catch(e) {
      console.error('pj-webhook:', e.message);
      return res.status(200).json({ ok: true }); // sempre 200 pro Resend
    }
  }

  // GET — health check / verificação manual
  if (req.method === 'GET') {
    const inbox = (await dbGet(INBOX_KEY)) || { emails: [] };
    return res.status(200).json({
      ok: true,
      status: 'Webhook PJ ativo',
      emails: inbox.emails.length,
      ultimoRecebido: inbox.emails[0]?.recebidoEm || null,
    });
  }

  return res.status(200).json({ ok: true });
};
