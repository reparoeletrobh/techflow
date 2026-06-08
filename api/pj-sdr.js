// api/pj-sdr.js — Módulo SDR CRM | Reparo Eletro BH
const U = (process.env.UPSTASH_URL   ||'').replace(/['"]/g,'').trim();
const T = (process.env.UPSTASH_TOKEN ||'').replace(/['"]/g,'').trim();
const SDR_KEY   = 'pj_sdr';
const INBOX_KEY = 'pj_inbox';

async function dbGet(k){
  try{
    const r=await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});
    const j=await r.json();
    const v=j[0]?.result;
    if(!v)return null;
    let x=JSON.parse(v);
    if(typeof x==='string')x=JSON.parse(x);
    return x;
  }catch(e){return null;}
}
async function dbSet(k,v){
  try{
    await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});
  }catch(e){}
}

// ── Cadência oficial (9 toques, 21 dias) ─────────────────────────────────────
const CADENCIA_STEPS = [
  { passo:1, dia:0,  canal:'email',    label:'Email D0 — Apresentação + Dor',     templateId:'e0' },
  { passo:2, dia:2,  canal:'whatsapp', label:'WhatsApp D2 — Pergunta curta',       templateId:'w2' },
  { passo:3, dia:4,  canal:'email',    label:'Email D4 — Diferencial (Coleta+NF)', templateId:'e4' },
  { passo:4, dia:6,  canal:'linkedin', label:'LinkedIn D6 — Conexão',              templateId:'l6' },
  { passo:5, dia:9,  canal:'email',    label:'Email D9 — Case / Prova Social',     templateId:'e9' },
  { passo:6, dia:12, canal:'whatsapp', label:'WhatsApp D12 — Urgência leve',       templateId:'w12'},
  { passo:7, dia:15, canal:'email',    label:'Email D15 — Conteúdo de Valor',      templateId:'e15'},
  { passo:8, dia:18, canal:'ligacao',  label:'Ligação D18 — Chamada direta',       templateId:'lg18'},
  { passo:9, dia:21, canal:'email',    label:'Email D21 — Breakup',                templateId:'e21'},
];

// ── Templates de mensagem ─────────────────────────────────────────────────────
const TEMPLATES = {
  e0_plain: `Oi {{responsavel}}, aqui é o Pedro Teixeira da Reparo Eletro BH.\n\nA gente faz manutenção especializada de microondas e bebedouros para empresas como a {{empresa}} — com coleta e entrega na sede, nota fiscal e boleto com prazo.\n\nVale 5 minutos pra eu te mostrar como funciona?\n\nAbraço,\nPedro`,
  w2: `Oi {{responsavel}}, Pedro da Reparo Eletro aqui 👋\nVocês têm microondas ou bebedouro no escritório que eventualmente precisa de manutenção?\nPergunto pois trabalho com empresas em BH e tenho coleta e NF. 🙂`,
  e4: `{{responsavel}}, passando pra reforçar:\n\nA gente busca o equipamento na {{empresa}}, conserta e devolve funcionando — sem que ninguém precise sair do escritório. Emitimos NF e aceitamos boleto com prazo de 15 a 30 dias.\n\nQuer receber nossa tabela de preços?`,
  l6: `Oi {{responsavel}}, mandei um email sobre manutenção de equipamentos para a {{empresa}}. Trabalho com coleta, NF e boleto. Posso te enviar nossa proposta por aqui?`,
  e9: `{{responsavel}}, uma empresa de porte similar à {{empresa}} economizou mais de 40% substituindo a troca de equipamentos por manutenção preventiva conosco.\n\nPosso te mostrar como funcionaria para vocês? Levo 10 minutos.`,
  w12:`{{responsavel}}, você teria uns 10 minutos essa semana pra eu te mostrar como funciona nossa manutenção corporativa? Atendo toda BH e região 🔧`,
  e15:`{{responsavel}}, preparei um checklist rápido: "5 sinais que seu microondas corporativo precisa de manutenção".\n\nMando pra você? É gratuito e pode ajudar a evitar quebras no pior momento.`,
  lg18:`Script D18 — Ligação direta:\n"Oi {{responsavel}}, aqui é o Pedro da Reparo Eletro BH. Mandei alguns emails sobre manutenção de equipamentos para a {{empresa}}. Você tem 2 minutinhos?"`,
  e0_html: `<!DOCTYPE html><html lang="pt-BR"><head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#f4f4f4;font-family:'Segoe UI',Arial,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f4f4f4;padding:24px 0;"><tr><td align="center">
<table width="580" cellpadding="0" cellspacing="0" style="background:#fff;border-radius:8px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,.08);">
<tr><td style="background:#1a1a2e;padding:24px 36px;text-align:center;">
  <div style="font-size:20px;font-weight:800;color:#fff;letter-spacing:1px;">⚡ REPARO ELETRO BH</div>
  <div style="font-size:10px;color:#8899aa;margin-top:3px;letter-spacing:2px;">MICROONDAS · BEBEDOUROS · CORPORATIVO</div>
</td></tr>
<tr><td style="padding:28px 36px 0;">
  <p style="margin:0;font-size:15px;color:#222;">Olá, <strong>{{responsavel}}</strong>!</p>
  <p style="margin:10px 0 0;font-size:14px;color:#444;line-height:1.7;">Meu nome é <strong>Pedro Teixeira</strong> da <strong>Reparo Eletro BH</strong>. Somos especialistas em manutenção de <strong>microondas e bebedouros corporativos</strong> em BH e região.</p>
  <p style="margin:10px 0 0;font-size:14px;color:#444;line-height:1.7;">Acredito que podemos ajudar a <strong>{{empresa}}</strong> a reduzir custos e evitar paradas por equipamentos com defeito.</p>
</td></tr>
<tr><td style="padding:20px 36px 0;">
  <table width="100%" cellpadding="0" cellspacing="0">
    <tr>
      <td width="50%" style="padding:0 6px 10px 0;vertical-align:top;">
        <table cellpadding="12" cellspacing="0" style="background:#f8f9ff;border-left:3px solid #3b82f6;border-radius:0 6px 6px 0;width:100%;"><tr><td>
          <div style="font-size:18px">🚚</div><div style="font-size:12px;font-weight:700;color:#1a1a2e;">Coleta e Entrega</div>
          <div style="font-size:11px;color:#666;margin-top:2px;">Buscamos e devolvemos na sua empresa.</div>
        </td></tr></table>
      </td>
      <td width="50%" style="padding:0 0 10px 6px;vertical-align:top;">
        <table cellpadding="12" cellspacing="0" style="background:#f8f9ff;border-left:3px solid #10d982;border-radius:0 6px 6px 0;width:100%;"><tr><td>
          <div style="font-size:18px">📄</div><div style="font-size:12px;font-weight:700;color:#1a1a2e;">Nota Fiscal</div>
          <div style="font-size:11px;color:#666;margin-top:2px;">NF para todas as manutenções.</div>
        </td></tr></table>
      </td>
    </tr>
    <tr>
      <td width="50%" style="padding:0 6px 0 0;vertical-align:top;">
        <table cellpadding="12" cellspacing="0" style="background:#f8f9ff;border-left:3px solid #f5c800;border-radius:0 6px 6px 0;width:100%;"><tr><td>
          <div style="font-size:18px">💳</div><div style="font-size:12px;font-weight:700;color:#1a1a2e;">Boleto 15-30 dias</div>
          <div style="font-size:11px;color:#666;margin-top:2px;">Facilita o fluxo de caixa.</div>
        </td></tr></table>
      </td>
      <td width="50%" style="padding:0 0 0 6px;vertical-align:top;">
        <table cellpadding="12" cellspacing="0" style="background:#f8f9ff;border-left:3px solid #f97316;border-radius:0 6px 6px 0;width:100%;"><tr><td>
          <div style="font-size:18px">⚡</div><div style="font-size:12px;font-weight:700;color:#1a1a2e;">Diagnóstico 24h</div>
          <div style="font-size:11px;color:#666;margin-top:2px;">Prazo médio: 5 a 7 dias úteis.</div>
        </td></tr></table>
      </td>
    </tr>
  </table>
</td></tr>
<tr><td style="padding:20px 36px;">
  <p style="margin:0 0 16px;font-size:14px;color:#444;line-height:1.7;">Posso enviar uma <strong>tabela de preços</strong> para a {{empresa}}? Leva menos de 5 minutos.</p>
  <table cellpadding="0" cellspacing="0"><tr>
    <td style="background:#3b82f6;border-radius:6px;padding:11px 24px;">
      <a href="https://reparoeletroadm.com/institucional" style="color:#fff;text-decoration:none;font-size:13px;font-weight:700;">Ver Apresentação Completa →</a>
    </td>
  </tr></table>
</td></tr>
<tr><td style="padding:0 36px 28px;">
  <table cellpadding="0" cellspacing="0" style="border-top:1px solid #e8ecf0;padding-top:18px;width:100%;">
    <tr>
      <td style="vertical-align:middle;padding-right:12px;">
        <div style="width:40px;height:40px;background:#1a1a2e;border-radius:50%;text-align:center;line-height:40px;font-size:18px;">⚡</div>
      </td>
      <td style="vertical-align:middle;">
        <div style="font-size:13px;font-weight:700;color:#1a1a2e;">Pedro Teixeira</div>
        <div style="font-size:11px;color:#3b82f6;font-weight:600;">Reparo Eletro, Microondas e Bebedouros</div>
        <div style="font-size:10px;color:#888;margin-top:2px;">📍 BH &nbsp;|&nbsp; 🌐 reparoeletroadm.com/institucional &nbsp;|&nbsp; 📧 pedro@comercial.reparoeletroadm.com</div>
      </td>
    </tr>
  </table>
</td></tr>
</table></td></tr></table></body></html>`,
  e21:`{{responsavel}}, entendo que talvez não seja o momento certo para a {{empresa}}.\n\nVou deixar nossa linha aberta — se precisar de manutenção de microondas ou bebedouros, é só chamar.\n\nAbraço,\nPedro Teixeira | Reparo Eletro BH`,
};

// ── Lead Score ────────────────────────────────────────────────────────────────
function calcScore(lead) {
  let s = 0;
  // Comportamental
  if (lead.emailRespondeu)      s += 40;
  if (lead.waRespondeu)         s += 30;
  if (lead.reuniaoAgendada)     s += 50;
  if (lead.propostaSolicitada)  s += 40;
  if (lead.emailAberto)         s += 15;
  if (lead.linkedinRespondeu)   s += 20;
  // Perfil
  if (lead.funcionarios === '51-200') s += 20;
  if (lead.funcionarios === '200+')   s += 30;
  if (lead.equipamentos === '16-50')  s += 15;
  if (lead.equipamentos === '50+')    s += 25;
  const cargosAltos = ['sócio','ceo','diretor','proprietário','dono','c-level'];
  if (cargosAltos.some(c => (lead.cargo||'').toLowerCase().includes(c))) s += 15;
  const segsAltos = ['Restaurante','Hotel','Escola','Clínica','Hospital'];
  if (segsAltos.includes(lead.segmento)) s += 10;
  return Math.min(100, s);
}

function scoreBadge(s) {
  if (s >= 81) return '💎';
  if (s >= 61) return '🔥';
  if (s >= 31) return '🌡️';
  return '🧊';
}

// ── Próxima ação da cadência ──────────────────────────────────────────────────
function proximaAcao(lead) {
  const toques = lead.toques || [];
  const feitos = new Set(toques.filter(t => t.realizado).map(t => t.passo));
  return CADENCIA_STEPS.find(s => !feitos.has(s.passo)) || null;
}

// ── Ações vencidas (atrasadas) ────────────────────────────────────────────────
function acaoAtrasada(lead) {
  const prox = proximaAcao(lead);
  if (!prox || !lead.inicioEm) return false;
  const inicio = new Date(lead.inicioEm).getTime();
  const diasPassados = (Date.now() - inicio) / 86400000;
  return diasPassados > prox.dia + 1;
}

function defaultDB() { return { leads: [] }; }

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin','*');
  const action = req.query.action || '';
  const db = await dbGet(SDR_KEY) || defaultDB();
  if (!db.leads) db.leads = [];

  // ── GET listar ──────────────────────────────────────────────────────────────
  if (action === 'listar') {
    const inbox = (await dbGet(INBOX_KEY)) || { emails: [] };
    const emailsRecebidos = (inbox.emails||[]).map(e => (e.de||'').toLowerCase());
    const leads = db.leads.map(l => ({
      ...l,
      score: calcScore(l),
      scoreBadge: scoreBadge(calcScore(l)),
      proximaAcao: proximaAcao(l),
      atrasado: acaoAtrasada(l),
      // Cruzar com inbox — tem email desse lead no inbox?
      emailNoInbox: emailsRecebidos.some(e => e.includes((l.email||'').toLowerCase().split('@')[0]) || (l.email||'') && e === l.email.toLowerCase()),
      emailInboxId: (inbox.emails||[]).find(e => (e.de||'').toLowerCase().includes((l.email||'').split('@')[0]))?.id || null,
    }));
    // Painel de inteligência
    const painel = {
      respondeuEmail:   leads.filter(l => l.emailRespondeu || l.emailNoInbox).length,
      acoesHoje:        leads.filter(l => l.atrasado || (proximaAcao(l) && !l.fase.match(/cliente|perdido/))).length,
      quentes:          leads.filter(l => l.score >= 61).length,
      reunioes:         leads.filter(l => l.reuniaoAgendada).length,
      emCadencia:       leads.filter(l => !['cliente','perdido','novo'].includes(l.fase)).length,
      pipelineEstimado: leads.filter(l => ['qualificado','proposta','cliente'].includes(l.fase)).reduce((s,l) => s + (l.potencial||0), 0),
    };
    return res.status(200).json({ ok:true, leads, painel, total: leads.length });
  }

  // ── POST criar ──────────────────────────────────────────────────────────────
  if (req.method === 'POST' && action === 'criar') {
    const { empresa,responsavel,cargo,email,telefone,cidade,segmento,
            funcionarios,equipamentos,potencial,obs } = req.body || {};
    if (!empresa || !responsavel) return res.status(400).json({ ok:false, error:'empresa e responsavel obrigatórios' });
    const novo = {
      id: 'sdr-'+Date.now().toString(36),
      empresa, responsavel, cargo:cargo||'', email:email||'', telefone:(telefone||'').replace(/\D/g,''),
      cidade:cidade||'BH', segmento:segmento||'Escritório',
      funcionarios:funcionarios||'1-10', equipamentos:equipamentos||'1-5',
      potencial: parseInt(potencial)||0, obs:obs||'',
      fase: 'novo', toques: [], inicioEm: null,
      emailRespondeu:false, waRespondeu:false, linkedinRespondeu:false,
      reuniaoAgendada:false, propostaSolicitada:false, emailAberto:false,
      criadoEm: new Date().toISOString(),
    };
    db.leads.unshift(novo);
    await dbSet(SDR_KEY, db);
    // Enviar email D0 automaticamente se tiver email
    let emailEnviado = false;
    if (novo.email && process.env.RESEND_API_KEY) {
      try {
        const htmlBody = TEMPLATES.e0_html
          .replace(/\{\{responsavel\}\}/g, novo.responsavel)
          .replace(/\{\{empresa\}\}/g, novo.empresa);
        const plainBody = TEMPLATES.e0_plain
          .replace(/\{\{responsavel\}\}/g, novo.responsavel)
          .replace(/\{\{empresa\}\}/g, novo.empresa);
        const er = await fetch('https://api.resend.com/emails', {
          method: 'POST',
          headers: { 'Authorization': 'Bearer ' + process.env.RESEND_API_KEY, 'Content-Type': 'application/json' },
          body: JSON.stringify({
            from: 'Pedro Teixeira | Reparo Eletro, Microondas e Bebedouros <pedro@comercial.reparoeletroadm.com>',
            reply_to: ['pedro@ciacaluuir.resend.app'],
            to: [novo.email],
            subject: 'Manutenção de Microondas e Bebedouros para ' + novo.empresa,
            html: htmlBody,
            text: plainBody,
          })
        });
        const ed = await er.json();
        if (ed.id) {
          emailEnviado = true;
          // Registrar toque D0 automaticamente
          novo.toques.push({ passo:1, canal:'email', label:'Email D0 — Apresentação + Dor', realizado:true, data:new Date().toISOString(), respondeu:false });
          novo.fase = 'primeiro_contato';
          novo.inicioEm = novo.criadoEm;
          await dbSet(SDR_KEY, db);
        }
      } catch(ef) { console.error('Email D0 error:', ef.message); }
    }
    return res.status(200).json({ ok:true, lead: novo, emailD0Enviado: emailEnviado });
  }

  // ── POST atualizar ──────────────────────────────────────────────────────────
  if (req.method === 'POST' && action === 'atualizar') {
    const { id, ...campos } = req.body || {};
    const lead = db.leads.find(l => l.id === id);
    if (!lead) return res.status(404).json({ ok:false, error:'lead não encontrado' });
    Object.assign(lead, campos);
    lead.score = calcScore(lead);
    await dbSet(SDR_KEY, db);
    return res.status(200).json({ ok:true, lead });
  }

  // ── POST mover-fase ─────────────────────────────────────────────────────────
  if (req.method === 'POST' && action === 'mover-fase') {
    const { id, fase } = req.body || {};
    const lead = db.leads.find(l => l.id === id);
    if (!lead) return res.status(404).json({ ok:false, error:'lead não encontrado' });
    lead.fase = fase;
    if (fase === 'primeiro_contato' && !lead.inicioEm) lead.inicioEm = new Date().toISOString();
    if (fase === 'reuniaoAgendada') lead.reuniaoAgendada = true;
    lead.score = calcScore(lead);
    await dbSet(SDR_KEY, db);
    return res.status(200).json({ ok:true, lead });
  }

  // ── POST registrar-toque ────────────────────────────────────────────────────
  if (req.method === 'POST' && action === 'registrar-toque') {
    const { id, passo, respondeu } = req.body || {};
    const lead = db.leads.find(l => l.id === id);
    if (!lead) return res.status(404).json({ ok:false, error:'lead não encontrado' });
    if (!lead.toques) lead.toques = [];
    if (!lead.inicioEm) lead.inicioEm = new Date().toISOString();
    const step = CADENCIA_STEPS.find(s => s.passo === passo);
    const existing = lead.toques.find(t => t.passo === passo);
    if (existing) {
      existing.realizado = true;
      existing.data = new Date().toISOString();
      existing.respondeu = !!respondeu;
    } else {
      lead.toques.push({ passo, canal: step?.canal||'', label: step?.label||'', realizado:true, data: new Date().toISOString(), respondeu: !!respondeu });
    }
    // Atualizar flags de resposta
    if (respondeu && step) {
      if (step.canal === 'email')    { lead.emailRespondeu = true; lead.fase = 'respondeu'; }
      if (step.canal === 'whatsapp') { lead.waRespondeu = true; lead.fase = 'respondeu'; }
      if (step.canal === 'linkedin') { lead.linkedinRespondeu = true; }
    }
    // Avançar fase automaticamente
    if (lead.fase === 'novo' || lead.fase === 'primeiro_contato') lead.fase = 'em_cadencia';
    lead.ultimoToque = new Date().toISOString();
    lead.score = calcScore(lead);
    await dbSet(SDR_KEY, db);
    return res.status(200).json({ ok:true, lead, proximaAcao: proximaAcao(lead) });
  }

  // ── POST excluir ────────────────────────────────────────────────────────────
  if (req.method === 'POST' && action === 'excluir') {
    const { id } = req.body || {};
    db.leads = db.leads.filter(l => l.id !== id);
    await dbSet(SDR_KEY, db);
    return res.status(200).json({ ok:true });
  }

  // ── GET templates ───────────────────────────────────────────────────────────
  if (action === 'templates') {
    return res.status(200).json({ ok:true, templates: TEMPLATES, steps: CADENCIA_STEPS });
  }

  // ── GET acoes-hoje ──────────────────────────────────────────────────────────
  if (action === 'acoes-hoje') {
    const acoes = db.leads
      .filter(l => !['cliente','perdido'].includes(l.fase))
      .map(l => ({ lead:l, prox: proximaAcao(l), atrasado: acaoAtrasada(l) }))
      .filter(a => a.prox && a.atrasado)
      .sort((a,b) => calcScore(b.lead) - calcScore(a.lead))
      .slice(0, 20);
    return res.status(200).json({ ok:true, acoes });
  }

  return res.status(404).json({ ok:false, error:'ação não encontrada: '+action });
};
