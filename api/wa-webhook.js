// api/wa-webhook.js — Webhook oficial Meta WhatsApp Cloud API
// GET: verificação do webhook (hub.challenge) | POST: recepção de mensagens
const U = (process.env.UPSTASH_URL || '').replace(/['"]/g, '').trim();
const T = (process.env.UPSTASH_TOKEN || '').replace(/[\n\r'"]/g, '').trim();
const VERIFY_TOKEN = (process.env.WA_VERIFY_TOKEN || 'reparo-eletro-bot-2026').trim();

const EVT_LIST = 'wa_evt_list'; // lista atômica: {ts,tel,nome,dir,texto,msgId,tipo}

async function rpushEvt(evt) {
  try {
    await fetch(`${U}/rpush/${EVT_LIST}/${encodeURIComponent(JSON.stringify(evt))}`,
      { headers: { Authorization: `Bearer ${T}` } });
    // Poda: manter últimos 8000 eventos
    await fetch(`${U}/ltrim/${EVT_LIST}/-8000/-1`,
      { headers: { Authorization: `Bearer ${T}` } });
  } catch (_) {}
}

export default async function handler(req, res) {
  // ── Verificação do webhook (configuração inicial na Meta) ──
  if (req.method === 'GET') {
    const mode = req.query['hub.mode'];
    const token = req.query['hub.verify_token'];
    const challenge = req.query['hub.challenge'];
    if (mode === 'subscribe' && token === VERIFY_TOKEN) {
      return res.status(200).send(challenge);
    }
    return res.status(403).json({ ok: false, error: 'verify_token inválido' });
  }

  // ── Recepção de mensagens ──
  if (req.method === 'POST') {
    try {
      const body = req.body || {};
      const entries = body.entry || [];
      let recebidas = 0;
      for (const entry of entries) {
        for (const change of (entry.changes || [])) {
          const value = change.value || {};
          const contatos = {};
          for (const c of (value.contacts || [])) {
            contatos[c.wa_id] = (c.profile && c.profile.name) || '';
          }
          for (const msg of (value.messages || [])) {
            const tel = String(msg.from || '');
            let texto = '';
            if (msg.type === 'text') texto = (msg.text && msg.text.body) || '';
            else if (msg.type === 'button') texto = (msg.button && msg.button.text) || '[botão]';
            else if (msg.type === 'interactive') {
              const i = msg.interactive || {};
              texto = (i.button_reply && i.button_reply.title) || (i.list_reply && i.list_reply.title) || '[interativo]';
            }
            else texto = '[' + (msg.type || 'mídia') + ']';
            await rpushEvt({
              ts: new Date(parseInt(msg.timestamp || '0', 10) * 1000 || Date.now()).toISOString(),
              tel, nome: contatos[tel] || '', dir: 'in',
              texto: texto.slice(0, 2000), msgId: msg.id || null, tipo: msg.type || 'text',
            });
            recebidas++;
          }
          // Status de entrega (sent/delivered/read) — registrar leve
          for (const st of (value.statuses || [])) {
            await rpushEvt({
              ts: new Date().toISOString(), tel: String(st.recipient_id || ''),
              dir: 'status', texto: st.status + (st.errors ? ' | ' + JSON.stringify(st.errors).slice(0,300) : ''),
              msgId: st.id || null, tipo: 'status',
            });
          }
        }
      }
      // Meta exige 200 rápido
      return res.status(200).json({ ok: true, recebidas });
    } catch (e) {
      // Nunca falhar para a Meta (evita retries em loop)
      return res.status(200).json({ ok: false, error: e.message });
    }
  }

  return res.status(405).json({ ok: false, error: 'método não suportado' });
}
