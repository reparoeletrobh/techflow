// api/pj-email.js — Disparos de email via Resend + leitura da inbox
const RESEND_KEY = (process.env.RESEND_API_KEY||'').trim();
const U = (process.env.UPSTASH_URL   ||'').replace(/['"]/g,'').trim();
const T = (process.env.UPSTASH_TOKEN ||'').replace(/['"]/g,'').trim();
const INBOX_KEY = 'pj_inbox';
const CAMP_KEY  = 'pj_campanhas';

async function dbGet(k){try{const r=await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});const j=await r.json();const v=j[0]?.result;if(!v)return null;let x=JSON.parse(v);if(typeof x==='string')x=JSON.parse(x);return x;}catch(e){return null;}}
async function dbSet(k,v){await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});}

module.exports = async function handler(req,res){
  res.setHeader('Access-Control-Allow-Origin','*');
  if(req.method==='OPTIONS')return res.status(200).end();
  const action=req.query.action||'';

  // GET inbox
  if(action==='inbox'){
    const db=await dbGet(INBOX_KEY)||{emails:[]};
    const naoLidos=(db.emails||[]).filter(e=>!e.lido).length;
    return res.status(200).json({ok:true,emails:db.emails||[],naoLidos});
  }

  // POST marcar-lido
  if(req.method==='POST'&&action==='marcar-lido'){
    const{id}=req.body||{};
    const db=await dbGet(INBOX_KEY)||{emails:[]};
    const e=db.emails.find(x=>x.id===id);
    if(e) e.lido=true;
    await dbSet(INBOX_KEY,db);
    return res.status(200).json({ok:true});
  }

  // POST enviar — envia email para um ou vários destinatários
  if(req.method==='POST'&&action==='enviar'){
    if(!RESEND_KEY)return res.status(400).json({ok:false,error:'RESEND_API_KEY não configurado'});
    const{de,para,assunto,html,texto,remetente}=req.body||{};
    if(!para||!assunto)return res.status(400).json({ok:false,error:'para e assunto obrigatórios'});
    const destinatarios=Array.isArray(para)?para:[para];
    const resultados=[];
    for(const dest of destinatarios){
      try{
        const r=await fetch('https://api.resend.com/emails',{
          method:'POST',
          headers:{'Content-Type':'application/json','Authorization':'Bearer '+RESEND_KEY},
          body:JSON.stringify({
            from: remetente||de||'Pedro <pedro@comercial.reparoeletroadm.com>',
            to:[dest.email||dest],
            subject:assunto.replace(/\{\{empresa\}\}/g,dest.empresa||'').replace(/\{\{responsavel\}\}/g,dest.responsavel||''),
            html:(html||'').replace(/\{\{empresa\}\}/g,dest.empresa||'').replace(/\{\{responsavel\}\}/g,dest.responsavel||'').replace(/\{\{cidade\}\}/g,dest.cidade||''),
            text:(texto||'').replace(/\{\{empresa\}\}/g,dest.empresa||'').replace(/\{\{responsavel\}\}/g,dest.responsavel||''),
          })
        });
        const j=await r.json();
        resultados.push({email:dest.email||dest,ok:!j.statusCode,id:j.id,erro:j.message});
      }catch(e){resultados.push({email:dest.email||dest,ok:false,erro:e.message});}
    }
    // Salvar campanha no log
    const cDb=await dbGet(CAMP_KEY)||{campanhas:[]};
    const falhasDetalhe=resultados.filter(r=>!r.ok).map(r=>({email:r.email,erro:r.erro||'erro desconhecido'}));
    cDb.campanhas.unshift({id:Date.now().toString(36),assunto,total:destinatarios.length,
      enviados:resultados.filter(r=>r.ok).length,falhas:resultados.filter(r=>!r.ok).length,
      falhasDetalhe,criadoEm:new Date().toISOString()});
    cDb.campanhas=cDb.campanhas.slice(0,100);
    await dbSet(CAMP_KEY,cDb);
    return res.status(200).json({ok:true,resultados,total:destinatarios.length,enviados:resultados.filter(r=>r.ok).length,falhas:resultados.filter(r=>!r.ok).length,erros:resultados.filter(r=>!r.ok).map(r=>({email:r.email,erro:r.erro}))});
  }

  // GET campanhas
  if(action==='campanhas'){
    const db=await dbGet(CAMP_KEY)||{campanhas:[]};
    return res.status(200).json({ok:true,campanhas:db.campanhas||[]});
  }

  // ── GET testar-resend — diagnóstico completo da integração Resend ─────────────
  if (action === 'testar-resend') {
    const chave = RESEND_KEY ? RESEND_KEY.slice(0,8)+'...' : 'NÃO CONFIGURADA';
    if (!RESEND_KEY) {
      return res.status(200).json({ ok:false, erro:'RESEND_API_KEY não configurada no Vercel', chave });
    }
    try {
      const r = await fetch('https://api.resend.com/domains', {
        headers:{ Authorization:'Bearer '+RESEND_KEY }
      });
      const j = await r.json();
      // Acionar verificação para domínio não verificado
      const dominio = (j.data||[]).find(d => d.name && d.name.includes('reparoeletroadm'));
      if (dominio && dominio.status !== 'verified') {
        await fetch('https://api.resend.com/domains/'+dominio.id+'/verify', {
          method:'POST', headers:{ Authorization:'Bearer '+RESEND_KEY }
        }).catch(()=>{});
        await new Promise(r=>setTimeout(r,3000));
        // Buscar status atualizado
        const r2 = await fetch('https://api.resend.com/domains/'+dominio.id, {
          headers:{ Authorization:'Bearer '+RESEND_KEY }
        });
        const j2 = await r2.json();
        if (j2.status) dominio.status = j2.status;
      }
      const dominios = (j.data||[]).map(d=>({ nome:d.name, status:d.status, regiao:d.region }));
      // Testar envio com endereço de teste
      const rEnvio = await fetch('https://api.resend.com/emails', {
        method:'POST',
        headers:{'Content-Type':'application/json','Authorization':'Bearer '+RESEND_KEY},
        body: JSON.stringify({
          from: 'Pedro <pedro@comercial.reparoeletroadm.com>',
          to: ['delivered@resend.dev'], // endereço de teste oficial do Resend
          subject: 'Teste Reparo Eletro BH',
          text: 'Teste de envio — diagnóstico do sistema PJ.'
        })
      });
      const jEnvio = await rEnvio.json();
      return res.status(200).json({
        ok: !jEnvio.statusCode,
        chave,
        dominiosVerificados: dominios,
        testeEnvio: {
          sucesso: !jEnvio.statusCode,
          id: jEnvio.id || null,
          erro: jEnvio.message || jEnvio.error || null,
          statusCode: jEnvio.statusCode || null,
          nomeErro: jEnvio.name || null,
        },
        diagnostico: !jEnvio.statusCode
          ? '✅ Resend configurado corretamente'
          : '❌ Falha: ' + (jEnvio.message || jEnvio.error || JSON.stringify(jEnvio))
      });
    } catch(e) {
      return res.status(200).json({ ok:false, chave, erro: e.message });
    }
  }

    // ── GET status-completo — verifica records DNS + status detalhado ───────────
  if (action === 'status-completo') {
    if (!RESEND_KEY) return res.status(200).json({ ok:false, erro:'RESEND_API_KEY não configurada' });
    try {
      // 1. Listar TODOS os domínios (sem acionar verify — não reseta status)
      const rDom = await fetch('https://api.resend.com/domains', {
        headers:{ Authorization:'Bearer '+RESEND_KEY }
      });
      const jDom = await rDom.json();
      const todosOsDominios = (jDom.data||[]).map(d=>({nome:d.name,status:d.status,id:d.id}));
      const dominio = (jDom.data||[]).find(d => d.name && d.name.includes('reparoeletroadm'));
      
      if (!dominio) return res.status(200).json({ 
        ok:false, erro:'Domínio reparoeletroadm não encontrado no Resend',
        todosOsDominios
      });

      // 2. Buscar detalhes SEM acionar verify
      const rDet = await fetch('https://api.resend.com/domains/'+dominio.id, {
        headers:{ Authorization:'Bearer '+RESEND_KEY }
      });
      const jDet = await rDet.json();
      
      // 4. Analisar cada record DNS
      const records = (jDet.records||[]).map(rec => ({
        tipo: rec.type,
        nome: rec.name,
        valor: (rec.value||'').slice(0,60),
        status: rec.status,
        ok: rec.status === 'verified',
      }));
      
      const totalVerificados = records.filter(r=>r.ok).length;
      const totalPendentes   = records.filter(r=>!r.ok).length;
      
      return res.status(200).json({
        ok: jDet.status === 'verified',
        dominio: jDet.name,
        status: jDet.status,
        totalRecords: records.length,
        verificados: totalVerificados,
        pendentes: totalPendentes,
        records,
        podeEnviar: jDet.status === 'verified',
        mensagem: jDet.status === 'verified'
          ? '✅ Domínio verificado — pode disparar!'
          : '⏳ '+totalPendentes+' record(s) ainda pendentes. DNS pode demorar até 24h para propagar.',
      });
    } catch(e) {
      return res.status(200).json({ ok:false, erro: e.message });
    }
  }

    return res.status(404).json({ok:false,error:'action não encontrada'});
};
