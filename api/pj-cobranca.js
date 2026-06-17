// api/pj-cobranca.js — Cobrança PJ: NF-e + Boleto + Email | Reparo Eletro BH
// NÃO altera financeiro.js — lógica própria para clientes PJ

const U=(process.env.UPSTASH_URL||'').replace(/['"]/g,'').trim();
const T=(process.env.UPSTASH_TOKEN||'').replace(/['"]/g,'').trim();
const FOR_KEY='pj_fornecedores';
const PIPE_KEY='reparoeletro_pipe';

async function dbGet(k){try{const r=await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});const j=await r.json();const v=j[0]?.result;if(!v)return null;let x=JSON.parse(v);if(typeof x==='string')x=JSON.parse(x);return x;}catch(e){return null;}}
async function dbSet(k,v){try{await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});}catch(e){}}

function normData(d){
  if(!d) return d;
  d=String(d).trim();
  if(d.includes('/')){const p=d.split('/');if(p.length===3&&p[0].length<=2)return p[2]+'-'+p[1].padStart(2,'0')+'-'+p[0].padStart(2,'0');}
  return d;
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin','https://reparoeletroadm.com');
  res.setHeader('Access-Control-Allow-Methods','GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers','Content-Type');
  if(req.method==='OPTIONS') return res.status(200).end();

  const action = req.query.action||'';

  // ── GET buscar-fichas — busca fichas do pipe por nome/OS ─────────────────────
  if(action==='buscar-fichas'){
    const q=(req.query.q||'').toLowerCase().trim();
    if(!q) return res.status(200).json({ok:true,fichas:[]});
    const pipe = await dbGet(PIPE_KEY) || {cards:[]};
    const fichas = (pipe.cards||[])
      .filter(c=>{
        const nome=(c.nomeContato||'').toLowerCase();
        const equip=(c.equipamento||'').toLowerCase();
        const id=String(c.id||c.numero||'').toLowerCase();
        return nome.includes(q)||equip.includes(q)||id.includes(q);
      })
      .slice(0,10)
      .map(c=>({
        id:c.id, numero:c.numero||c.id,
        nome:c.nomeContato, equipamento:c.equipamento||'',
        valor:parseFloat(c.valor||0), fase:c.phase||c.fase||'',
        descricao:c.descricao||c.obs||''
      }));
    return res.status(200).json({ok:true,fichas});
  }

  // ── POST faturar — emite NF + cria boleto ────────────────────────────────────
  if(req.method==='POST' && action==='faturar'){
    const {clienteId,fichaId,valor,vencimento,descricao,retencaoISS} = req.body||{};
    if(!clienteId||!valor||!vencimento) return res.status(400).json({ok:false,error:'clienteId, valor e vencimento obrigatórios'});

    const fornDb = await dbGet(FOR_KEY)||{fornecedores:[]};
    const cliente = fornDb.fornecedores.find(f=>f.id===clienteId);
    if(!cliente) return res.status(404).json({ok:false,error:'cliente não encontrado'});

    const vlr = parseFloat(valor);
    const resultado = {ok:true,nf:null,boleto:null,errors:[]};

    // ── 1. EMITIR NF-e ──────────────────────────────────────────────────────────
    const NFSE_CERT = process.env.NFSE_CERT_PFX;
    if(!NFSE_CERT){
      resultado.errors.push('NF: NFSE_CERT_PFX não configurado');
    } else {
      try {
        // Montar discriminação
        const discr = descricao||('Manutenção de eletrodomésticos – '+cliente.razaoSocial+
          (fichaId?' – OS '+fichaId:''));
        // Chamar internamente o endpoint nfse
        const nfBody = {
          tomadorCpfCnpj: (cliente.cnpj||'').replace(/\D/g,''),
          tomadorNome:    cliente.razaoSocial,
          discriminacao:  discr,
          valor:          vlr,
          retencaoISS:    retencaoISS||cliente.retencaoISS||false,
        };
        const nfRes = await fetch(
          'https://reparoeletroadm.com/api/nfse?action=emitir',
          {method:'POST',headers:{'Content-Type':'application/json',
            'x-internal-call':'1'},
          body:JSON.stringify(nfBody)}
        );
        const nfData = await nfRes.json();
        if(nfData.ok && nfData.chaveAcesso){
          resultado.nf = {
            chave:   nfData.chaveAcesso,
            idDps:   nfData.idDps,
            danfeUrl:'/api/nfse?action=danfe&chave='+nfData.chaveAcesso,
            alertas: nfData.alertas||[],
          };
        } else {
          resultado.errors.push('NF: '+(nfData.error||'falha na emissão'));
        }
      } catch(nfe){
        resultado.errors.push('NF: '+nfe.message);
      }
    }

    // ── 2. CRIAR BOLETO via Asaas ─────────────────────────────────────────────
    const ASAAS_KEY = (process.env.ASAAS_API_KEY||'').trim();
    const ASAAS_URL = ASAAS_KEY.includes('sandbox')
      ? 'https://api-sandbox.asaas.com/v3'
      : 'https://api.asaas.com/v3';

    if (!ASAAS_KEY) {
      resultado.errors.push('Boleto: ASAAS_API_KEY não configurada no Vercel');
    } else {
      try {
        const asaasHdr = {
          'Content-Type': 'application/json',
          'access_token':  ASAAS_KEY,
        };

        // 2a. Buscar/criar cliente no Asaas pelo CNPJ
        const cnpjLimpo = (cliente.cnpj||'').replace(/\D/g,'');
        let asaasCustomerId = cliente.asaasCustomerId || null;

        if (!asaasCustomerId && cnpjLimpo) {
          const busca = await fetch(
            `${ASAAS_URL}/customers?cpfCnpj=${cnpjLimpo}&limit=1`,
            { headers: asaasHdr }
          ).then(r=>r.json()).catch(()=>null);
          if (busca?.data?.length > 0) asaasCustomerId = busca.data[0].id;
        }

        if (!asaasCustomerId) {
          const custRes = await fetch(`${ASAAS_URL}/customers`, {
            method: 'POST', headers: asaasHdr,
            body: JSON.stringify({
              name:    cliente.razaoSocial || cliente.responsavel || 'Cliente PJ',
              email:   cliente.email || '',
              phone:   (cliente.telefone||'').replace(/\D/g,''),
              ...(cnpjLimpo ? { cpfCnpj: cnpjLimpo } : {}),
              externalReference: clienteId,
            }),
          }).then(r=>r.json());

          if (custRes.id) {
            asaasCustomerId = custRes.id;
            cliente.asaasCustomerId = asaasCustomerId;
            await dbSet(FOR_KEY, fornDb);
            // Configurar notificações: apenas WhatsApp
            try {
              const notifRes = await fetch(`${ASAAS_URL}/customers/${asaasCustomerId}/notifications`,
                { headers: asaasHdr }).then(r=>r.json()).catch(()=>null);
              if (notifRes?.data?.length > 0) {
                for (const notif of notifRes.data) {
                  await fetch(`${ASAAS_URL}/customers/${asaasCustomerId}/notifications/${notif.id}`,{
                    method:'PUT', headers: asaasHdr,
                    body: JSON.stringify({ enabled:true, emailEnabledForCustomer:false, smsEnabledForCustomer:false, whatsappEnabledForCustomer:true, phoneCallEnabledForCustomer:false }),
                  }).catch(()=>{});
                }
              }
            } catch(wErr) { console.warn('[pj-cobranca] notif WhatsApp:', wErr.message); }
          } else {
            throw new Error('Erro ao criar cliente Asaas: '+(custRes.errors?.[0]?.description||JSON.stringify(custRes).slice(0,100)));
          }
        }

        // 2b. Emitir boleto
        const payRes = await fetch(`${ASAAS_URL}/payments`, {
          method: 'POST', headers: asaasHdr,
          body: JSON.stringify({
            customer:        asaasCustomerId,
            billingType:     'BOLETO',
            value:           vlr,
            dueDate:         normData(vencimento),
            description:     descricao || `Manutenção — ${cliente.razaoSocial||'Cliente PJ'}`,
            externalReference: `${clienteId}-${cobId||Date.now()}`,
            fine:    { value: 2 },
            interest:{ value: 1 },
          }),
        }).then(r=>r.json());

        if (payRes.id && payRes.bankSlipUrl) {
          resultado.boleto = {
            asaasPaymentId: payRes.id,
            url:            payRes.bankSlipUrl,
            invoiceUrl:     payRes.invoiceUrl||'',
            nossoNumero:    payRes.nossoNumero||'',
            vencimento:     normData(vencimento),
            valor:          vlr,
            status:         payRes.status,
          };

          // 2c. Anexar PDF da NF na fatura Asaas (se NF foi emitida)
          if (resultado.nf?.chave) {
            try {
              const nfPdfRes = await fetch(`https://reparoeletroadm.com/api/nfse?action=danfe&chave=${resultado.nf.chave}`);
              if (nfPdfRes.ok) {
                const pdfBlob = new Blob([await nfPdfRes.arrayBuffer()], { type:'application/pdf' });
                const fd = new FormData();
                fd.append('availableAfterPayment','false');
                fd.append('type','INVOICE');
                fd.append('file', pdfBlob, `NF-${resultado.nf.chave.slice(-8)}.pdf`);
                const docRes = await fetch(`${ASAAS_URL}/payments/${payRes.id}/documents`,{
                  method:'POST', headers:{ 'access_token': ASAAS_KEY }, body: fd,
                }).then(r=>r.json()).catch(()=>null);
                if (docRes?.id) resultado.boleto.nfAnexada = true;
              }
            } catch(nfErr) { console.warn('[pj-cobranca] NF upload:', nfErr.message); }
          }
        } else {
          const err = payRes.errors?.[0]?.description || payRes.description || JSON.stringify(payRes).slice(0,200);
          resultado.errors.push('Boleto Asaas: '+err);
          console.error('[pj-cobranca] Asaas erro:', err);
        }
      } catch(be) {
        resultado.errors.push('Boleto: '+be.message);
        console.error('[pj-cobranca] Boleto catch:', be.message);
      }
    }
    // ── 3. Salvar cobrança no cliente ───────────────────────────────────────────
    if(resultado.nf||resultado.boleto){
      const cli = fornDb.fornecedores.find(f=>f.id===clienteId);
      if(cli){
        if(!cli.cobrancas) cli.cobrancas=[];
        const cobId='COB-'+String((fornDb.nextCobId||1)).padStart(5,'0');
        fornDb.nextCobId = (fornDb.nextCobId||1)+1;
        cli.cobrancas.push({
          id:cobId, valor:vlr, vencimento, descricao:descricao||'',
          status:'pendente', criadoEm:new Date().toISOString(),
          nfChave:resultado.nf?.chave||null,
          boletoId:resultado.boleto?.asaasPaymentId||null,
          boletoUrl:resultado.boleto?.url||null,
          fichaId:fichaId||null,
          retencaoISS:!!(retencaoISS||cliente.retencaoISS),
        });
        await dbSet(FOR_KEY,fornDb);
        resultado.cobId = cobId;
      }
    }

    resultado.ok = resultado.errors.length === 0 ||
                   !!(resultado.nf||resultado.boleto);
    return res.status(200).json(resultado);
  }

  // ── POST reemitir-nf — re-emite NF quando falhou na emissão original ──────────
  if(req.method==='POST' && action==='reemitir-nf'){
    const {clienteId, cobId} = req.body||{};
    if(!clienteId||!cobId) return res.status(400).json({ok:false,error:'clienteId e cobId obrigatórios'});

    const fornDb = await dbGet(FOR_KEY)||{fornecedores:[]};
    const cliente = fornDb.fornecedores.find(f=>f.id===clienteId);
    if(!cliente) return res.status(404).json({ok:false,error:'cliente não encontrado'});

    const cob = (cliente.cobrancas||[]).find(c=>c.id===cobId);
    if(!cob) return res.status(404).json({ok:false,error:'cobrança não encontrada'});
    if(cob.nfChave) return res.status(400).json({ok:false,error:'NF já emitida para esta cobrança: '+cob.nfChave});

    const cnpj = (cliente.cnpj||'').replace(/\D/g,'');
    if(!cnpj) return res.status(400).json({ok:false,error:'Cliente sem CNPJ cadastrado — cadastre o CNPJ antes de re-emitir'});

    const NFSE_CERT = process.env.NFSE_CERT_PFX;
    if(!NFSE_CERT) return res.status(400).json({ok:false,error:'NFSE_CERT_PFX não configurado'});

    try {
      const discr = cob.descricao || ('Manutenção de eletrodomésticos – '+cliente.razaoSocial+(cob.fichaId?' – OS '+cob.fichaId:''));
      const nfBody = {
        tomadorCpfCnpj: cnpj,
        tomadorNome:    cliente.razaoSocial,
        discriminacao:  discr,
        valor:          parseFloat(cob.valor),
        retencaoISS:    !!(cob.retencaoISS||cliente.retencaoISS),
      };
      const nfRes  = await fetch('https://reparoeletroadm.com/api/nfse?action=emitir',
        {method:'POST',headers:{'Content-Type':'application/json','x-internal-call':'1'},body:JSON.stringify(nfBody)});
      const nfData = await nfRes.json();

      if(!nfData.ok||!nfData.chaveAcesso)
        return res.status(200).json({ok:false,error:'NF: '+(nfData.error||'falha na emissão')});

      // Atualizar cobrança com a chave da NF
      cob.nfChave    = nfData.chaveAcesso;
      cob.nfEmitidaEm = new Date().toISOString();
      await dbSet(FOR_KEY, fornDb);

      // Tentar anexar PDF no boleto Asaas (se boletoId disponível)
      let nfAnexada = false;
      const boletoAsaasId = cob.boletoId||cob.asaasPaymentId||null;
      if(boletoAsaasId){
        try {
          const ASAAS_KEY = (process.env.ASAAS_API_KEY||'').trim();
          const ASAAS_URL = ASAAS_KEY.includes('sandbox')
            ? 'https://api-sandbox.asaas.com/v3' : 'https://api.asaas.com/v3';
          const pdfRes = await fetch('https://reparoeletroadm.com/api/nfse?action=danfe&chave='+nfData.chaveAcesso);
          if(pdfRes.ok){
            const buf = await pdfRes.arrayBuffer();
            const fd = new FormData();
            fd.append('availableAfterPayment','false');
            fd.append('type','INVOICE');
            fd.append('file', new Blob([buf],{type:'application/pdf'}), 'NF-'+nfData.chaveAcesso.slice(-8)+'.pdf');
            const docRes = await fetch(`${ASAAS_URL}/payments/${boletoAsaasId}/documents`,
              {method:'POST',headers:{'access_token':ASAAS_KEY},body:fd}).then(r=>r.json()).catch(()=>null);
            if(docRes?.id) nfAnexada = true;
          }
        } catch(ae){ console.warn('[reemitir-nf] anexar Asaas:',ae.message); }
      }

      return res.status(200).json({
        ok:true,
        chaveAcesso: nfData.chaveAcesso,
        danfeUrl:    '/api/nfse?action=danfe&chave='+nfData.chaveAcesso,
        nfAnexada,
        cobId,
        msg: 'NF emitida com sucesso'+(nfAnexada?' e anexada ao boleto Asaas':''),
      });
    } catch(e){
      return res.status(200).json({ok:false,error:'Erro ao re-emitir NF: '+e.message});
    }
  }

  // ── POST enviar-email — envia NF + boleto por email ──────────────────────────
  if(req.method==='POST' && action==='enviar-email'){
    const {para,destinos,clienteNome,valor,vencimento,nfChave,boletoUrl,barcodeContent,cobId,assunto,msgExtra} = req.body||{};
    const RESEND_KEY = process.env.RESEND_API_KEY;
    if(!RESEND_KEY) return res.status(400).json({ok:false,error:'RESEND_API_KEY não configurado'});
    if(!para) return res.status(400).json({ok:false,error:'destinatário obrigatório'});

    // Buscar PDF da NF para anexar
    const attachments = [];
    const anexosList = [];
    if(nfChave){
      try{
        const pdfRes = await fetch('https://reparoeletroadm.com/api/nfse?action=danfe&chave='+nfChave);
        if(pdfRes.ok){
          const buf = await pdfRes.arrayBuffer();
          const b64 = Buffer.from(buf).toString('base64');
          attachments.push({filename:'NotaFiscal-'+nfChave.slice(-8)+'.pdf',content:b64});
          anexosList.push('Nota Fiscal PDF');
        }
      }catch(pe){console.warn('NF pdf erro:',pe.message);}
    }

    const vlrFmt = 'R$ '+parseFloat(valor||0).toFixed(2).replace('.',',');
    if(boletoUrl) anexosList.push('Boleto (link no email)');

    const emailBody = {
      from:'Pedro Teixeira | Reparo Eletro, Microondas e Bebedouros <pedro@comercial.reparoeletroadm.com>',
      reply_to:['pedro@ciacaluuir.resend.app'],
      to: destinos && destinos.length ? destinos : [para],
      subject: assunto || ('Cobrança e Nota Fiscal – '+clienteNome),
      attachments,
      html:`<!DOCTYPE html><html><body style="font-family:'Segoe UI',sans-serif;background:#f4f4f4;padding:32px 0">
<table width="580" cellpadding="0" cellspacing="0" style="background:#fff;border-radius:8px;margin:0 auto;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,.08)">
<tr><td style="background:#1a1a2e;padding:22px 32px;text-align:center">
  <div style="font-size:18px;font-weight:800;color:#fff">⚡ REPARO ELETRO BH</div>
  <div style="font-size:10px;color:#8899aa;margin-top:3px">MICROONDAS · BEBEDOUROS · CORPORATIVO</div>
</td></tr>
<tr><td style="padding:28px 32px">
  <p style="font-size:15px;color:#222">Prezado(a) <strong>${clienteNome}</strong>,</p>
  <p style="font-size:14px;color:#444;margin-top:10px">Segue sua cobrança referente aos serviços prestados:</p>
  <table width="100%" cellpadding="12" cellspacing="0" style="background:#f8f9ff;border:1px solid #e8ecf0;border-radius:8px;margin:16px 0">
    <tr><td style="font-size:13px;color:#444"><strong>Valor:</strong></td><td style="font-size:15px;font-weight:700;color:#1a1a2e;text-align:right">${vlrFmt}</td></tr>
    <tr style="border-top:1px solid #e8ecf0"><td style="font-size:13px;color:#444"><strong>Vencimento:</strong></td><td style="font-family:monospace;color:#444;text-align:right">${vencimento||''}</td></tr>
    ${cobId?`<tr style="border-top:1px solid #e8ecf0"><td style="font-size:13px;color:#444"><strong>Referência:</strong></td><td style="font-family:monospace;color:#444;text-align:right">${cobId}</td></tr>`:''}
  </table>
  ${boletoUrl?`<p style="margin:16px 0"><a href="${boletoUrl}" style="display:inline-block;background:#3b82f6;color:#fff;font-weight:700;padding:12px 24px;border-radius:6px;text-decoration:none;font-size:13px">🔵 Pagar com Boleto →</a></p>`:''}
  ${barcodeContent?`<p style="font-family:monospace;font-size:11px;color:#666;background:#f8f9ff;padding:10px;border-radius:6px;border:1px solid #e8ecf0;word-break:break-all">${barcodeContent}</p>`:''}
  ${nfChave?'<p style="font-size:12px;color:#666;margin-top:12px">📎 A Nota Fiscal está anexada a este email.</p>':''}
</td></tr>
<tr><td style="padding:0 32px 24px">
  <table cellpadding="0" cellspacing="0" style="border-top:1px solid #e8ecf0;padding-top:16px;width:100%">
    <tr>
      <td style="width:40px;vertical-align:middle">
        <div style="width:36px;height:36px;background:#1a1a2e;border-radius:50%;text-align:center;line-height:36px;font-size:16px">⚡</div>
      </td>
      <td style="padding-left:12px;vertical-align:middle">
        <div style="font-size:13px;font-weight:700;color:#1a1a2e">Pedro Teixeira</div>
        <div style="font-size:11px;color:#3b82f6">Reparo Eletro, Microondas e Bebedouros</div>
        <div style="font-size:10px;color:#888">📧 pedro@comercial.reparoeletroadm.com</div>
      </td>
    </tr>
  </table>
</td></tr>
</table></body></html>`,
    };

    try{
      const er = await fetch('https://api.resend.com/emails',{
        method:'POST',headers:{'Authorization':'Bearer '+RESEND_KEY,'Content-Type':'application/json'},
        body:JSON.stringify(emailBody),
      });
      const ed = await er.json();
      if(ed.id){
        // Salvar em pj_enviados para aparecer no inbox
        try{
          const eDb=(await dbGet('pj_enviados'))||{emails:[]};
          eDb.emails.unshift({
            id:'ENV-'+Date.now(),
            para: destinos||[para],
            assunto: assunto||('Cobrança e Nota Fiscal – '+clienteNome),
            clienteNome, cobId, clienteId,
            anexos: anexosList,
            enviadoEm: new Date().toISOString(),
            emailId: ed.id,
          });
          if(eDb.emails.length>200) eDb.emails=eDb.emails.slice(0,200);
          await dbSet('pj_enviados',eDb);
        }catch(eSv){console.warn('[pj-cobranca] salvar enviado:',eSv.message);}

        return res.status(200).json({ok:true, emailId:ed.id, anexos:anexosList});
      }
      return res.status(200).json({ok:false,error:ed.message||'falha Resend'});
    }catch(ee){
      return res.status(200).json({ok:false,error:ee.message});
    }
  }

  // ── POST reemitir-boleto — cancela anterior e gera novo boleto Asaas ──────
  if(req.method==='POST' && action==='reemitir-boleto'){
    const{clienteId,cobId,vencimento:vencReq,valor:valorReq}=req.body||{};
    const fornDb=await dbGet(FOR_KEY)||{fornecedores:[]};
    const cli=fornDb.fornecedores.find(f=>f.id===clienteId);
    if(!cli) return res.status(404).json({ok:false,error:'cliente não encontrado'});
    const cob=(cli.cobrancas||[]).find(c=>c.id===cobId);
    if(!cob) return res.status(404).json({ok:false,error:'cobrança não encontrada'});

    const ASAAS_KEY=(process.env.ASAAS_API_KEY||'').trim();
    const ASAAS_URL=ASAAS_KEY.includes('sandbox')
      ?'https://api-sandbox.asaas.com/v3':'https://api.asaas.com/v3';
    if(!ASAAS_KEY) return res.status(200).json({ok:false,error:'ASAAS_API_KEY não configurada'});

    try {
      const asaasHdr={'Content-Type':'application/json','access_token':ASAAS_KEY};

      // Cancelar boleto anterior se existir
      if(cob.asaasPaymentId){
        await fetch(`${ASAAS_URL}/payments/${cob.asaasPaymentId}`,{
          method:'DELETE', headers:asaasHdr
        }).catch(()=>{});
      }

      // Garantir cliente Asaas
      let asaasCustomerId=cli.asaasCustomerId||null;
      if(!asaasCustomerId){
        const cnpjLimpo=(cli.cnpj||'').replace(/\D/g,'');
        const custRes=await fetch(`${ASAAS_URL}/customers`,{
          method:'POST', headers:asaasHdr,
          body:JSON.stringify({
            name:cli.razaoSocial||cli.responsavel||'Cliente PJ',
            email:cli.email||'',
            phone:(cli.telefone||'').replace(/\D/g,''),
            ...(cnpjLimpo?{cpfCnpj:cnpjLimpo}:{}),
            externalReference:clienteId,
          }),
        }).then(r=>r.json());
        if(custRes.id){ asaasCustomerId=custRes.id; cli.asaasCustomerId=custRes.id; }
      }

      // Emitir novo boleto com data/valor do input (ou do Redis como fallback)
      const dueDate = normData(vencReq||cob.vencimento);
      const valor   = parseFloat(valorReq||cob.valor||0);
      const payRes  = await fetch(`${ASAAS_URL}/payments`,{
        method:'POST', headers:asaasHdr,
        body:JSON.stringify({
          customer:asaasCustomerId,
          billingType:'BOLETO',
          value:valor,
          dueDate,
          description:cob.descricao||`Manutenção — ${cli.razaoSocial||'Cliente PJ'}`,
          externalReference:`${clienteId}-${cobId}-reemissao`,
          fine:{value:2}, interest:{value:1},
        }),
      }).then(r=>r.json());

      if(payRes.id && payRes.bankSlipUrl){
        cob.boletoUrl      = payRes.bankSlipUrl;
        cob.invoiceUrl     = payRes.invoiceUrl||'';
        cob.asaasPaymentId = payRes.id;
        cob.nossoNumero    = payRes.nossoNumero||'';
        await dbSet(FOR_KEY,fornDb);
        return res.status(200).json({ok:true,boletoUrl:payRes.bankSlipUrl,invoiceUrl:payRes.invoiceUrl||''});
      }
      const err=payRes.errors?.[0]?.description||JSON.stringify(payRes).slice(0,150);
      return res.status(200).json({ok:false,error:'Asaas: '+err});
    }catch(e){return res.status(200).json({ok:false,error:e.message});}
  }
  // ── POST cancelar-nf — cancela NF via SEFAZ ──────────────────────────────────
  if(req.method==='POST' && action==='cancelar-nf'){
    const{clienteId,cobId}=req.body||{};
    const fornDb=await dbGet(FOR_KEY)||{fornecedores:[]};
    const cli=fornDb.fornecedores.find(f=>f.id===clienteId);
    const cob=cli&&(cli.cobrancas||[]).find(c=>c.id===cobId);
    if(!cob||!cob.nfChave) return res.status(400).json({ok:false,error:'Cobrança ou NF não encontrada'});
    // Solicitar cancelamento via nfse endpoint
    try{
      const r=await fetch('https://reparoeletroadm.com/api/nfse?action=cancelar',{
        method:'POST',headers:{'Content-Type':'application/json'},
        body:JSON.stringify({chave:cob.nfChave})
      });
      const d=await r.json();
      if(d.ok){
        cob.nfCancelada=true; cob.nfChave=null;
        await dbSet(FOR_KEY,fornDb);
        return res.status(200).json({ok:true,msg:'NF cancelada com sucesso'});
      }
      return res.status(200).json({ok:false,error:d.error||'Falha no cancelamento'});
    }catch(e){return res.status(200).json({ok:false,error:e.message});}
  }

  // ── POST cancelar-boleto — cancela no Asaas (DELETE /v3/payments/{id}) ───────
  if(req.method==='POST' && action==='cancelar-boleto'){
    const{clienteId,cobId}=req.body||{};
    const fornDb=await dbGet(FOR_KEY)||{fornecedores:[]};
    const cli=fornDb.fornecedores.find(f=>f.id===clienteId);
    const cob=cli&&(cli.cobrancas||[]).find(c=>c.id===cobId);
    if(!cob) return res.status(404).json({ok:false,error:'Cobrança não encontrada'});
    try{
      const ASAAS_KEY=(process.env.ASAAS_API_KEY||'').trim();
      const ASAAS_URL=ASAAS_KEY.includes('sandbox')
        ?'https://api-sandbox.asaas.com/v3':'https://api.asaas.com/v3';
      if(cob.asaasPaymentId && ASAAS_KEY){
        const r=await fetch(`${ASAAS_URL}/payments/${cob.asaasPaymentId}`,{
          method:'DELETE',headers:{'access_token':ASAAS_KEY}
        }).then(res=>res.json()).catch(()=>({deleted:false}));
        if(r.deleted===true){
          cob.boletoUrl=''; cob.asaasPaymentId='';
          await dbSet(FOR_KEY,fornDb);
          return res.status(200).json({ok:true});
        }
        return res.status(200).json({ok:false,error:r?.errors?.[0]?.description||JSON.stringify(r).slice(0,100)});
      }
      cob.boletoUrl=''; cob.preferenceId=null;
      await dbSet(FOR_KEY,fornDb);
      return res.status(200).json({ok:true});
    }catch(e){return res.status(200).json({ok:false,error:e.message});}
  }
  // ── POST atualizar-cobranca — altera valor/vencimento antes de emitir ─────────
  if(req.method==='POST' && action==='atualizar-cobranca'){
    const{clienteId,cobId,valor,vencimento,descricao}=req.body||{};
    const fornDb=await dbGet(FOR_KEY)||{fornecedores:[]};
    const cli=fornDb.fornecedores.find(f=>f.id===clienteId);
    const cob=cli&&(cli.cobrancas||[]).find(c=>c.id===cobId);
    if(!cob) return res.status(404).json({ok:false,error:'não encontrada'});
    if(valor)      cob.valor=parseFloat(valor);
    if(vencimento) cob.vencimento=normData(vencimento);
    if(descricao)  cob.descricao=descricao;
    await dbSet(FOR_KEY,fornDb);
    return res.status(200).json({ok:true});
  }

  return res.status(404).json({ok:false,error:'action não encontrada: '+action});
};
