// api/wa-webhook.js — Webhook oficial Meta WhatsApp Cloud API
// GET: verificação do webhook (hub.challenge) | POST: recepção de mensagens
const U = (process.env.UPSTASH_URL || '').replace(/['"]/g, '').trim();
const T = (process.env.UPSTASH_TOKEN || '').replace(/[\n\r'"]/g, '').trim();
const VERIFY_TOKEN = (process.env.WA_VERIFY_TOKEN || 'reparo-eletro-bot-2026').trim();

const EVT_LIST = 'wa_evt_list'; // lista atômica: {ts,tel,nome,dir,texto,msgId,tipo}

async function dbGet(k) {
  try {
    const r = await fetch(`${U}/get/${k}`, { headers: { Authorization: `Bearer ${T}` } });
    const j = await r.json();
    return j.result ? JSON.parse(j.result) : null;
  } catch (e) { return null; }
}

async function dbSet(k, v) {
  try {
    await fetch(`${U}/set/${k}`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${T}`, 'Content-Type': 'application/json' },
      body: JSON.stringify(v),
    });
    return true;
  } catch (e) { return false; }
}

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
  // 🔐 Fase 2: segredo na URL do webhook (ativar exigência com env WEBHOOK_STRICT=1)
  if (req.method === 'POST') {
    const _vt = String((req.query && req.query.vt) || '');
    const _vtOk = _vt === ((process.env.WA_WEBHOOK_SECRET || 'wh-re2026-Kp8xQm2z').trim());
    if (String(process.env.WEBHOOK_STRICT || '') === '1' && !_vtOk) {
      return res.status(401).json({ ok: false, error: 'assinatura ausente' });
    }
  }

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
            else if (msg.type === 'image') texto = '📷 [foto]' + ((msg.image && msg.image.caption) ? ' ' + msg.image.caption : '');
            else if (msg.type === 'video') texto = '🎬 [vídeo]' + ((msg.video && msg.video.caption) ? ' ' + msg.video.caption : '');
            else if (msg.type === 'audio') texto = '🎤 [áudio]';
            else texto = '[' + (msg.type || 'mídia') + ']';
            // 🎯 ORIGEM DO ANÚNCIO (Click-to-WhatsApp): a Meta manda o referral SÓ na primeira
            // mensagem da conversa. Se não gravar agora, a atribuição se perde para sempre.
            let refAd = null;
            try {
              const rf = msg.referral || {};
              if (rf.source_id || rf.ctwa_clid) {
                const d8r = String(tel).replace(/\D/g, '').slice(-8);
                refAd = {
                  adId: rf.source_id || null,
                  ctwaClid: rf.ctwa_clid || null,
                  tipo: rf.source_type || null,
                  titulo: String(rf.headline || '').slice(0, 120),
                  url: String(rf.source_url || '').slice(0, 300),
                  em: new Date().toISOString(),
                };
                const org = (await dbGet('wa_origem_anuncio')) || { por: {} };
                // primeira origem manda (o lead é daquele criativo); registra as demais no histórico
                if (!org.por[d8r]) org.por[d8r] = refAd;
                else {
                  org.por[d8r].recorrencias = (org.por[d8r].recorrencias || 0) + 1;
                  org.por[d8r].ultimaEm = refAd.em;
                }
                await dbSet('wa_origem_anuncio', org);
              }
            } catch (e) {}
            await rpushEvt({
              ts: new Date(parseInt(msg.timestamp || '0', 10) * 1000 || Date.now()).toISOString(),
              tel, nome: contatos[tel] || '', dir: 'in',
              texto: texto.slice(0, 2000), msgId: msg.id || null, tipo: msg.type || 'text',
              mediaId: (msg.type === 'image' && msg.image && msg.image.id) || (msg.type === 'audio' && msg.audio && msg.audio.id) || null,
              origemAd: refAd ? { adId: refAd.adId, titulo: refAd.titulo } : undefined,
            });
            recebidas++;
            // 🤖 AUTO-RESPOSTA (trava de teste): telefones em wa_bot_config.execTels recebem resposta automática do cérebro
            try {
              const cfgAuto = await dbGet('wa_bot_config');
              let telsAuto = (cfgAuto && Array.isArray(cfgAuto.execTels)) ? cfgAuto.execTels : [];
              if (cfgAuto && cfgAuto.modoAberto === true) telsAuto = [tel]; // 🔓 MODO ABERTO: todos os clientes
              const d8w = String(tel).replace(/\D/g, '').slice(-8);
              if (telsAuto.some(t => String(t).replace(/\D/g, '').slice(-8) === d8w)) {
                const KW = (process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim();
                // cliente arquivado voltou a falar → desarquiva sozinho
                try {
                  const arqW = (await dbGet('wa_arquivadas')) || { tels: {} };
                  const d8aw = String(tel).replace(/\D/g, '').slice(-8);
                  if (arqW.tels[d8aw]) { delete arqW.tels[d8aw]; await dbSet('wa_arquivadas', arqW); }
                } catch (e) {}
                await fetch(`https://reparoeletroadm.com/api/wa-bot?action=auto-responder&tel=${d8w ? String(tel).replace(/\D/g, '') : ''}&k=${KW}`);
              }
            } catch (eAuto) {}
          }
          // Status de entrega (sent/delivered/read) — registrar leve
          for (const st of (value.statuses || [])) {
            await rpushEvt({
              ts: new Date().toISOString(), tel: String(st.recipient_id || ''),
              dir: 'status', texto: st.status + (st.errors ? ' | ' + JSON.stringify(st.errors).slice(0,300) : ''),
              msgId: st.id || null, tipo: 'status',
            });
            // ↩️ FALHA DE PAGAMENTO: desfaz a marcação de "abordado", senão o cliente
            // fica marcado como contactado sem nunca ter recebido nada (abordagem fantasma).
            try {
              const codD = Number((st.errors && st.errors[0] && st.errors[0].code) || 0);
              if (st.status === 'failed' && [131042, 131047, 131026].includes(codD)) {
                const telD = String(st.recipient_id || '').replace(/\D/g, '');
                const d8d = telD.slice(-8);
                // marca o bloqueio para o bot parar de insistir por 2h
                try { const cfgB = (await dbGet('wa_bot_config')) || {};
                  cfgB.bloqueioPagamentoEm = new Date().toISOString();
                  await dbSet('wa_bot_config', cfgB); } catch (e) {}
                const ab = (await dbGet('wa_abordados')) || { tels: {} };
                let mexeu = false;
                for (const k of Object.keys(ab.tels || {})) {
                  if (String(k).replace(/\D/g, '').endsWith(d8d)) { delete ab.tels[k]; mexeu = true; }
                }
                if (mexeu) await dbSet('wa_abordados', ab);
              }
            } catch (e) {}

            // 📒 FILA DE RECUPERAÇÃO: toda falha de entrega vai para um registro permanente,
            // com o template tentado e a origem — para reenviar quando a conta for liberada.
            try {
              if (st.status === 'failed') {
                const codF = (st.errors && st.errors[0] && st.errors[0].code) || 0;
                const dia = new Date(Date.now() - 3 * 3600000).toISOString().slice(0, 10);
                const chave = 'wa_falhas_' + dia;
                const reg = (await dbGet(chave)) || { itens: [] };
                // procura o envio correspondente para saber QUAL template e de onde veio
                // o envio grava o par msgId → template num índice leve; ler de lá é seguro
                let tplNome = null, viaNome = null, txtEnv = null;
                try {
                  const idx = (await dbGet('wa_envio_idx')) || {};
                  const info = idx[st.id];
                  if (info) { tplNome = info.template; viaNome = info.via; txtEnv = info.texto; }
                } catch (e) {}
                const jaTem = (reg.itens || []).some(x => x.msgId === st.id);
                if (!jaTem) {
                  reg.itens.push({
                    ts: new Date().toISOString(),
                    telefone: String(st.recipient_id || ''),
                    msgId: st.id || null,
                    template: tplNome || '(não identificado)',
                    origem: viaNome || '(não informada)',
                    textoTentado: String(txtEnv || '').slice(0, 160),
                    codigo: codF,
                    // 📱 de qual número saiu — permite separar falhas do número antigo
                    phoneId: String((value && value.metadata && value.metadata.phone_number_id) || ''),
                    motivo: (st.errors && st.errors[0] && (st.errors[0].title || st.errors[0].message)) || 'falha',
                    recuperado: false,
                  });
                  reg.atualizadoEm = new Date().toISOString();
                  await dbSet(chave, reg);
                }
              }
            } catch (e) {}

            // 🚨 FALHA DE CONTA (pagamento pendente, número inelegível): nenhuma mensagem
            // será entregue até alguém resolver. Abre conflito UMA vez por hora, não a cada
            // mensagem — senão inunda o painel. Sem isso, a operação para em silêncio.
            try {
              const cod = (st.errors && st.errors[0] && st.errors[0].code) || 0;
              if (st.status === 'failed' && [131042, 131047, 131026, 133010].includes(Number(cod))) {
                const marca = (await dbGet('wa_alerta_conta')) || {};
                const agora = Date.now();
                if (!marca.em || agora - new Date(marca.em).getTime() > 3600000) {
                  await dbSet('wa_alerta_conta', { em: new Date().toISOString(), codigo: cod });
                  const K = (process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim();
                  const det = (st.errors[0].error_data && st.errors[0].error_data.details) || st.errors[0].title || '';
                  await fetch('https://reparoeletroadm.com/api/prospeccao?action=criar-conflito&k=' + K, {
                    method: 'POST', headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ nome: '⚠️ CONTA DO WHATSAPP', telefone: '',
                      equipamento: 'SISTEMA',
                      motivo: '🚨 A META ESTÁ RECUSANDO AS MENSAGENS (erro ' + cod + '). NENHUM cliente está recebendo. ' +
                        String(det).slice(0, 180) + ' — verificar cobrança/status da conta no Business Manager AGORA.' }),
                  }).catch(() => null);
                }
              }
            } catch (eAl) {}
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
