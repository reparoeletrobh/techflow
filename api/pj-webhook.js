// api/pj-webhook.js — Resend Inbound webhook para emails PJ
const UPSTASH_URL   = (process.env.UPSTASH_URL   || '').replace(/['"]/g,'').trim();
const UPSTASH_TOKEN = (process.env.UPSTASH_TOKEN || '').replace(/['"]/g,'').trim();
const INBOX_KEY     = 'pj_inbox';
const RESEND_KEY    = (process.env.RESEND_API_KEY||'').trim();

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
      const LOG_KEY = 'pj_webhook_log';

      // Logar TUDO que chega (para diagnóstico)
      const logDb = (await dbGet(LOG_KEY)) || { eventos: [] };
      logDb.eventos.unshift({
        ts: new Date().toISOString(),
        type: event.type || 'sem_type',
        keys: Object.keys(event),
        dataKeys: event.data ? Object.keys(event.data) : [],
        raw: JSON.stringify(event).slice(0, 800),
        // Campos específicos para diagnóstico
        emailId: event.data?.email_id || null,
        fromField: event.data?.from || null,
        subjectField: event.data?.subject || null,
        hasBody: !!(event.data?.html || event.data?.plain_text || event.data?.text)
      });
      logDb.eventos = logDb.eventos.slice(0, 50);
      await dbSet(LOG_KEY, logDb);

      // Aceitar email.received OU qualquer estrutura com 'from'/'subject'
      const data = event.data || event;
      const isEmail = event.type === 'email.received' ||
                      data.from || data.subject || data.email_id;

      if (!isEmail) {
        return res.status(200).json({ ok: true, ignored: true, type: event.type });
      }

      // Webhook só tem metadados — buscar corpo via Receiving API imediatamente
      const emailId = data.email_id || data.id;
      let textoFinal = data.text || data.plain_text || '';
      let htmlFinal  = data.html || '';
      if (emailId && RESEND_KEY) {
        try {
          // Tentar endpoint da Receiving API
          let bodyRes = await fetch('https://api.resend.com/emails/receiving/' + emailId, {
            headers: { Authorization: 'Bearer ' + RESEND_KEY }
          });
          // Fallback para endpoint genérico se receiving não existir
          if (!bodyRes.ok) {
            bodyRes = await fetch('https://api.resend.com/emails/' + emailId, {
              headers: { Authorization: 'Bearer ' + RESEND_KEY }
            });
          }
          if (bodyRes.ok) {
            const bd = await bodyRes.json();
            textoFinal = bd.text || bd.plain_text || bd.body_plain || textoFinal;
            htmlFinal  = bd.html || bd.body_html || htmlFinal;
            console.log('[webhook] corpo buscado, chars:', textoFinal.length + htmlFinal.length);
          } else {
            console.log('[webhook] API retornou:', bodyRes.status, 'para', emailId);
          }
        } catch(ef) { console.error('[webhook] fetch body error:', ef.message); }
      }

      // Salvar email recebido no Redis
      const inbox = (await dbGet(INBOX_KEY)) || { emails: [] };
      const to = Array.isArray(data.to) ? data.to.join(', ') : (data.to || '');
      inbox.emails.unshift({
        id:         emailId || Date.now().toString(36),
        de:         data.from || '',
        para:       to,
        assunto:    data.subject || '',
        texto:      textoFinal,
        html:       htmlFinal,
        recebidoEm: data.created_at || new Date().toISOString(),
        lido:       false,
      });
      inbox.emails = inbox.emails.slice(0, 500);
      await dbSet(INBOX_KEY, inbox);

      return res.status(200).json({ ok: true, saved: true });
    } catch(e) {
      console.error('pj-webhook:', e.message);
      return res.status(200).json({ ok: true });
    }
  }

  // GET — health check + diagnóstico
  if (req.method === 'GET') {
    const inbox  = (await dbGet(INBOX_KEY))    || { emails: [] };
    const logDb  = (await dbGet('pj_webhook_log')) || { eventos: [] };
    return res.status(200).json({
      ok: true,
      status: 'Webhook PJ ativo',
      emails: inbox.emails.length,
      ultimoEmail: inbox.emails[0] ? {
        de: inbox.emails[0].de,
        assunto: inbox.emails[0].assunto,
        recebidoEm: inbox.emails[0].recebidoEm
      } : null,
      ultimosEventos: logDb.eventos.slice(0, 5),
      diagnostico: logDb.eventos.length === 0
        ? '⚠️ Nenhum evento recebido — Resend não está disparando o webhook'
        : '✅ Webhook ativo — '+logDb.eventos.length+' evento(s) recebido(s)'
    });
  }

  return res.status(200).json({ ok: true });
};
