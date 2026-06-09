// api/pj-cobranca.js — Cobrança PJ: NF-e + Boleto + Email | Reparo Eletro BH
// NÃO altera financeiro.js — lógica própria para clientes PJ

const U=(process.env.UPSTASH_URL||'').replace(/['"]/g,'').trim();
const T=(process.env.UPSTASH_TOKEN||'').replace(/['"]/g,'').trim();
const FOR_KEY='pj_fornecedores';
const PIPE_KEY='reparoeletro_pipe';

async function dbGet(k){try{const r=await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});const j=await r.json();const v=j[0]?.result;if(!v)return null;let x=JSON.parse(v);if(typeof x==='string')x=JSON.parse(x);return x;}catch(e){return null;}}
async function dbSet(k,v){try{await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});}catch(e){}}

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

    // ── 2. CRIAR BOLETO via Mercado Pago ────────────────────────────────────────
    const MP_TOKEN = process.env.MP_ACCESS_TOKEN;
    if(!MP_TOKEN){
      resultado.errors.push('Boleto: MP_ACCESS_TOKEN não configurado');
    } else {
      try {
        const nomeArr = (cliente.responsavel||cliente.razaoSocial||'Cliente').split(' ');
        const cnpjLimpo = (cliente.cnpj||'').replace(/\D/g,'');
        const vencISO   = vencimento+'T23:59:59.000-03:00';
        const boletoBody = {
          transaction_amount: vlr,
          description: 'Manutenção de eletrodomésticos – '+(cliente.razaoSocial||'Cliente'),
          payment_method_id: 'boleto',
          date_of_expiration: vencISO,
          payer: {
            email:      cliente.email||'financeiro@reparoeletroadm.com',
            first_name: nomeArr[0]||'Cliente',
            last_name:  nomeArr.slice(1).join(' ')||'PJ',
            identification: cnpjLimpo.length===14
              ? {type:'CNPJ', number:cnpjLimpo}
              : {type:'CPF',  number:cnpjLimpo.slice(0,11)},
          },
          metadata: {
            origem:'pj_cobranca', clienteId, fichaId:fichaId||'', cobId:Date.now()
          },
          notification_url:'https://reparoeletroadm.com/api/webhook-mp',
        };
        const bRes = await fetch('https://api.mercadopago.com/v1/payments',{
          method:'POST',
          headers:{
            'Content-Type':'application/json',
            'Authorization':'Bearer '+MP_TOKEN,
            'X-Idempotency-Key':clienteId+'-'+Date.now(),
          },
          body:JSON.stringify(boletoBody),
        });
        const bData = await bRes.json();
        if(bData.id){
          resultado.boleto = {
            id:          bData.id,
            status:      bData.status,
            url:         bData.transaction_details?.external_resource_url||'',
            barcode:     bData.barcode?.content||'',
            vencimento:  vencimento,
            valor:       vlr,
          };
        } else {
          resultado.errors.push('Boleto: '+(bData.message||bData.error||'falha MP'));
        }
      } catch(be){
        resultado.errors.push('Boleto: '+be.message);
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
          boletoId:resultado.boleto?.id||null,
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

  // ── POST enviar-email — envia NF + boleto por email ──────────────────────────
  if(req.method==='POST' && action==='enviar-email'){
    const {para,clienteNome,valor,vencimento,nfChave,boletoUrl,barcodeContent,cobId} = req.body||{};
    const RESEND_KEY = process.env.RESEND_API_KEY;
    if(!RESEND_KEY) return res.status(400).json({ok:false,error:'RESEND_API_KEY não configurado'});
    if(!para) return res.status(400).json({ok:false,error:'destinatário obrigatório'});

    // Buscar PDF da NF para anexar
    const attachments = [];
    if(nfChave){
      try{
        const pdfRes = await fetch('https://reparoeletroadm.com/api/nfse?action=danfe&chave='+nfChave);
        if(pdfRes.ok){
          const buf = await pdfRes.arrayBuffer();
          const b64 = Buffer.from(buf).toString('base64');
          attachments.push({filename:'NotaFiscal-'+nfChave.slice(-8)+'.pdf',content:b64});
        }
      }catch(pe){console.warn('NF pdf erro:',pe.message);}
    }

    const vlrFmt = 'R$ '+parseFloat(valor||0).toFixed(2).replace('.',',');

    const emailBody = {
      from:'Pedro Teixeira | Reparo Eletro, Microondas e Bebedouros <pedro@comercial.reparoeletroadm.com>',
      reply_to:['pedro@ciacaluuir.resend.app'],
      to:[para],
      subject:'Cobrança e Nota Fiscal – '+clienteNome,
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
      if(ed.id) return res.status(200).json({ok:true,emailId:ed.id});
      return res.status(200).json({ok:false,error:ed.message||'falha Resend'});
    }catch(ee){
      return res.status(200).json({ok:false,error:ee.message});
    }
  }

  return res.status(404).json({ok:false,error:'action não encontrada: '+action});
};
