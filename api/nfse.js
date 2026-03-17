// v2026-03-16-fix-xpath
const https     = require("https");
const forge     = require("node-forge");
const xmlCrypto = require("xml-crypto");
const crypto = require("crypto");
const zlib   = require("zlib");

const UPSTASH_URL   = (process.env.UPSTASH_URL   || "").replace(/[\'"]/g,"").trim();
const UPSTASH_TOKEN = (process.env.UPSTASH_TOKEN || "").replace(/[\'"]/g,"").trim();

async function dbGet(key) {
  try {
    const r = await fetch(UPSTASH_URL + "/pipeline", {
      method: "POST",
      headers: { Authorization: "Bearer " + UPSTASH_TOKEN, "Content-Type": "application/json" },
      body: JSON.stringify([["GET", key]]),
    });
    const j = await r.json();
    const v = j[0]?.result;
    return v ? JSON.parse(v) : null;
  } catch(e) { return null; }
}

async function dbSet(key, value) {
  try {
    const r = await fetch(UPSTASH_URL + "/pipeline", {
      method: "POST",
      headers: { Authorization: "Bearer " + UPSTASH_TOKEN, "Content-Type": "application/json" },
      body: JSON.stringify([["SET", key, JSON.stringify(value)]]),
    });
    const j = await r.json();
    return j[0]?.result === "OK";
  } catch(e) { return false; }
}

// Homologação: sefin.producaorestrita.nfse.gov.br
// Produção:    sefin.nfse.gov.br
const NFSE_HOMOLOG   = (process.env.NFSE_HOMOLOG || "false") === "true"; // default: PRODUÇÃO
const NFSE_HOST      = NFSE_HOMOLOG ? "sefin.producaorestrita.nfse.gov.br" : "sefin.nfse.gov.br";
const NFSE_PATH      = "/SefinNacional/nfse";
const CNPJ_EMPRESA   = (process.env.NFSE_CNPJ || "59485378000175").replace(/\D/g,"");
const IM_EMPRESA     =  process.env.NFSE_IM   || "16391680010";
const COD_MUN_BH     = "3106200";

function loadCert() {
  const b64  = (process.env.NFSE_CERT_PFX   || "").trim();
  const pass = (process.env.NFSE_CERT_SENHA  || "").trim();
  if (!b64)  throw new Error("NFSE_CERT_PFX não configurado no Vercel");
  if (!pass) throw new Error("NFSE_CERT_SENHA não configurado no Vercel");
  return { pfx: Buffer.from(b64, "base64"), passphrase: pass };
}

function escXml(s) {
  return String(s||"").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;");
}

function hoje() {
  // Data no fuso BRT (UTC-3) para evitar dCompet > dhEmi
  const d = new Date(new Date().toLocaleString("en-US", { timeZone: "America/Sao_Paulo" }));
  const pad = n => String(n).padStart(2,"0");
  return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}`;
}
function agora() {
  const d = new Date(new Date().toLocaleString("en-US",{timeZone:"America/Sao_Paulo"}));
  const pad = n => String(n).padStart(2,"0");
  return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}-03:00`;
}

function genId(seq) {
  // TSIdDPS = DPS(3) + cMunEmissor(7) + tpInsc(1) + CNPJ(14) + Serie(5) + Numero(15) = 45 chars
  // tpInsc: 1=CPF, 2=CNPJ
  var num   = String(seq || 1).padStart(15, "0");
  var serie = "00001"; // 5 dígitos numéricos
  var id    = "DPS" + COD_MUN_BH + "2" + CNPJ_EMPRESA + serie + num;
  return id; // 3+7+1+14+5+15 = 45 chars
}

// Monta XML da DPS conforme schema do governo
function montarDPS({ cpfcnpj, nome, discriminacao, valor, numDPS }) {
  const cpfLimpo = cpfcnpj.replace(/\D/g,"");
  const isCnpj   = cpfLimpo.length === 14;
  const toma     = isCnpj ? `<CNPJ>${cpfLimpo}</CNPJ>` : `<CPF>${cpfLimpo}</CPF>`;
  const vlr      = parseFloat(valor).toFixed(2);
  const id       = numDPS; // DPS(3)+cMun(7)+tpInsc(1)+CNPJ(14)+serie(5)+nDPS(15) = 45
  const nDPS     = String(numDPS).slice(-15).replace(/^0+/,"") || "1";

  return `<?xml version="1.0" encoding="UTF-8"?>\n` +
`<DPS versao="1.01" xmlns="http://www.sped.fazenda.gov.br/nfse">\n` +
`  <infDPS Id="${id}">\n` +
`    <tpAmb>${NFSE_HOMOLOG ? 2 : 1}</tpAmb>\n` +
`    <dhEmi>${agora()}</dhEmi>\n` +
`    <verAplic>reparoeletro-1.0</verAplic>\n` +
`    <serie>00001</serie>\n` +
`    <nDPS>${nDPS}</nDPS>\n` +
`    <dCompet>${hoje()}</dCompet>\n` +
`    <tpEmit>1</tpEmit>\n` +
`    <cLocEmi>${COD_MUN_BH}</cLocEmi>\n` +
`    <prest>\n` +
`      <CNPJ>${CNPJ_EMPRESA}</CNPJ>\n` +
`      <IM>${IM_EMPRESA}</IM>\n` +
`      <regTrib>\n` +
`        <opSimpNac>3</opSimpNac>\n` +
`        <regApTribSN>1</regApTribSN>\n` +
`        <regEspTrib>0</regEspTrib>\n` +
`      </regTrib>\n` +
`    </prest>\n` +
`    <toma>\n` +
`      ${toma}\n` +
`      <xNome>${escXml(nome || "Consumidor Final")}</xNome>\n` +
`    </toma>\n` +
`    <serv>\n` +
`      <locPrest>\n` +
`        <cLocPrestacao>${COD_MUN_BH}</cLocPrestacao>\n` +
`      </locPrest>\n` +
`      <cServ>\n` +
`        <cTribNac>140201</cTribNac>\n` +
`        <cTribMun>001</cTribMun>\n` +
`        <xDescServ>${escXml(discriminacao)}</xDescServ>\n` +
`      </cServ>\n` +
`    </serv>\n` +
`    <valores>\n` +
`      <vServPrest>\n` +
`        <vServ>${vlr}</vServ>\n` +
`      </vServPrest>\n` +
`      <trib>\n` +
`        <tribMun>\n` +
`          <tribISSQN>1</tribISSQN>\n` +
`          <tpRetISSQN>1</tpRetISSQN>\n` +
`        </tribMun>\n` +
`        <totTrib>\n` +
`          <pTotTribSN>6.00</pTotTribSN>\n` +
`        </totTrib>\n` +
`      </trib>\n` +
`    </valores>\n` +
`  </infDPS>\n` +
`</DPS>`;
}


// Assina DPS com XMLDSig usando node-forge
async function assinarXML(xml, pfxBuf, passphrase) {
  try {
    // 1. Extrai chave e cert do PFX
    const p12Asn1 = forge.asn1.fromDer(forge.util.createBuffer(pfxBuf));
    const p12     = forge.pkcs12.pkcs12FromAsn1(p12Asn1, passphrase);
    const shrouded = (p12.getBags({bagType:forge.pki.oids.pkcs8ShroudedKeyBag})[forge.pki.oids.pkcs8ShroudedKeyBag]||[]);
    const plain    = (p12.getBags({bagType:forge.pki.oids.keyBag})[forge.pki.oids.keyBag]||[]);
    const keyBag   = [...shrouded,...plain][0];
    if (!keyBag) throw new Error("Chave privada não encontrada");
    const privateKey = keyBag.key;
    const certBags   = (p12.getBags({bagType:forge.pki.oids.certBag})[forge.pki.oids.certBag]||[]);
    const certBase64 = certBags[0]
      ? forge.pki.certificateToPem(certBags[0].cert)
          .replace(/-----BEGIN CERTIFICATE-----/,"").replace(/-----END CERTIFICATE-----/,"").replace(/\s/g,"")
      : "";

    // 2. Id do infDPS
    const idMatch = xml.match(/infDPS Id="([^"]+)"/);
    if (!idMatch) throw new Error("Id do infDPS não encontrado");
    const refId = idMatch[1];

    // 3. C14N do infDPS — injeta xmlns herdado e expande self-closing tags
    const infDpsRaw = xml.match(/<infDPS[\s\S]*?<\/infDPS>/)?.[0];
    if (!infDpsRaw) throw new Error("infDPS não encontrado");
    let infDpsC14n = infDpsRaw.replace(/^<infDPS /, '<infDPS xmlns="http://www.sped.fazenda.gov.br/nfse" ');
    // C14N: expande self-closing tags (/<tag attr/> → <tag attr></tag>)
    infDpsC14n = infDpsC14n.replace(/<([a-zA-Z][^>]*?)\/>/g, (m, inner) => `<${inner.trimEnd()}></${inner.trim().split(/[\s>]/)[0]}>`);

    // 4. DigestValue = SHA256(infDpsC14n)
    const md = forge.md.sha256.create();
    md.update(infDpsC14n, "utf8");
    const digest = forge.util.encode64(md.digest().bytes());

    // 5. SignedInfo em C14N (sem self-closing, com xmlns standalone para assinar)
    // Elementos filhos: sem self-closing (C14N expande tudo)
    const signedInfoC14n =
      `<SignedInfo xmlns="http://www.w3.org/2000/09/xmldsig#">` +
      `<CanonicalizationMethod Algorithm="http://www.w3.org/2001/10/xml-exc-c14n#WithComments"></CanonicalizationMethod>` +
      `<SignatureMethod Algorithm="http://www.w3.org/2001/04/xmldsig-more#rsa-sha256"></SignatureMethod>` +
      `<Reference URI="#${refId}">` +
        `<Transforms>` +
          `<Transform Algorithm="http://www.w3.org/2000/09/xmldsig#enveloped-signature"></Transform>` +
          `<Transform Algorithm="http://www.w3.org/2001/10/xml-exc-c14n#WithComments"></Transform>` +
        `</Transforms>` +
        `<DigestMethod Algorithm="http://www.w3.org/2001/04/xmlenc#sha256"></DigestMethod>` +
        `<DigestValue>${digest}</DigestValue>` +
      `</Reference>` +
      `</SignedInfo>`;

    // 6. Assina SignedInfo C14N com RSA-SHA256
    const mdSig = forge.md.sha256.create();
    mdSig.update(signedInfoC14n, "utf8");
    const sigValue = forge.util.encode64(privateKey.sign(mdSig));

    // 7. Bloco Signature no XML (SignedInfo sem xmlns — herdado de Signature)
    const signedInfoInXml = signedInfoC14n.replace('<SignedInfo xmlns="http://www.w3.org/2000/09/xmldsig#">', "<SignedInfo>");
    const signature =
      `<Signature xmlns="http://www.w3.org/2000/09/xmldsig#">` +
        signedInfoInXml +
        `<SignatureValue>${sigValue}</SignatureValue>` +
        (certBase64 ? `<KeyInfo><X509Data><X509Certificate>${certBase64}</X509Certificate></X509Data></KeyInfo>` : "") +
      `</Signature>`;

    return xml.replace("</infDPS>", "</infDPS>" + signature);

  } catch(e) {
    console.error("assinarXML erro:", e.message);
    return xml;
  }
}


// Extrai campos do XML da NFS-e
function extrairDadosXml(xml) {
  const get = (tag) => {
    const m = xml.match(new RegExp("<" + tag + "[^>]*>([\\s\\S]*?)<\\/" + tag + ">"));
    return m ? m[1].trim() : "";
  };
  return {
    nNFSe:        get("nNFSe"),
    dhProc:       get("dhProc"),
    dCompet:      get("dCompet"),
    dhEmi:        get("dhEmi"),
    xDescServ:    get("xDescServ"),
    vServ:        get("vServ"),
    cpfTomador:   get("CPF") || get("CNPJ"),
    xNomeTomador: get("xNome"),
  };
}

function gerarDanfeHtml(dados, chave) {
  const fmtDT = (v) => { try { return new Date(v).toLocaleString("pt-BR",{timeZone:"America/Sao_Paulo"}); } catch(e){ return v||""; }};
  const fmtD  = (v) => v ? v.split("-").reverse().join("/") : "";
  const fmtV  = (v) => v ? Number(v).toLocaleString("pt-BR",{minimumFractionDigits:2}) : "0,00";
  const doc   = dados.cpfTomador||"";
  const docFmt= doc.length===11 ? doc.replace(/(\d{3})(\d{3})(\d{3})(\d{2})/,"$1.$2.$3-$4")
                                 : doc.replace(/(\d{2})(\d{3})(\d{3})(\d{4})(\d{2})/,"$1.$2.$3/$4-$5");
  const S = (s) => String(s||"").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");
  return "<!DOCTYPE html><html lang='pt-BR'><head><meta charset='UTF-8'>" +
    "<title>NFS-e " + S(dados.nNFSe) + "</title>" +
    "<style>*{margin:0;padding:0;box-sizing:border-box}body{font-family:Arial,sans-serif;font-size:11px;padding:16px;max-width:800px;margin:0 auto}" +
    ".hdr{border:2px solid #000;padding:10px;margin-bottom:6px;overflow:hidden}" +
    ".nfnum{float:right;text-align:right}.nfnum .n{font-size:24px;font-weight:bold}.nfnum .l{font-size:9px;text-transform:uppercase;color:#666}" +
    ".sec{border:1px solid #999;margin-bottom:5px}.sec-t{background:#ddd;font-weight:bold;padding:3px 8px;font-size:9px;text-transform:uppercase;border-bottom:1px solid #999}" +
    ".row{display:flex;flex-wrap:wrap}.f{padding:4px 8px;border-right:1px solid #ddd;border-bottom:1px solid #ddd;flex:1;min-width:100px}" +
    ".f:last-child{border-right:none}.f label{display:block;font-size:8px;color:#666;text-transform:uppercase}.f span{font-size:11px;font-weight:600}" +
    ".f.w2{flex:2}.f.w3{flex:3}.desc{padding:6px 8px;white-space:pre-wrap;line-height:1.5}" +
    ".chave{font-size:8px;word-break:break-all;padding:5px 8px;background:#f9f9f9;border-top:1px solid #ddd}" +
    ".vv{font-size:16px;font-weight:bold;color:#1a6e1a}" +
    ".footer{text-align:center;font-size:9px;color:#666;margin-top:10px;padding-top:8px;border-top:1px dashed #ccc}" +
    ".pbtn{display:block;margin:16px auto;padding:10px 32px;font-size:14px;background:#2563eb;color:#fff;border:none;border-radius:8px;cursor:pointer;font-weight:bold}" +
    "@media print{.pbtn{display:none}body{padding:0}}</style></head><body>" +
    "<div class='hdr'>" +
      "<div class='nfnum'><div class='l'>NFS-e N\xba</div><div class='n'>" + S(dados.nNFSe||"\u2014") + "</div>" +
        "<div class='l' style='margin-top:4px'>Emiss\xe3o</div><div style='font-size:10px;font-weight:600'>" + fmtDT(dados.dhEmi||dados.dhProc) + "</div></div>" +
      "<h1 style='font-size:13px;font-weight:bold;text-transform:uppercase'>Nota Fiscal de Servi\xe7o Eletr\xf4nica \u2014 NFS-e</h1>" +
      "<h2 style='font-size:11px;margin-top:2px'>REPARO ELETRO - CONSERTO DE ELETRODOMESTICOS LTDA</h2>" +
      "<p style='font-size:9px;color:#444;margin-top:2px'>CNPJ: 59.485.378/0001-75 &nbsp;|&nbsp; IM: 16391680010 &nbsp;|&nbsp; Belo Horizonte - MG</p>" +
      "<p style='font-size:9px;color:#444'>Rua Ouro Preto, 663 \u2014 Barro Preto \u2014 CEP 30170-044 &nbsp;|&nbsp; (31) 9785-6023</p>" +
    "</div>" +
    "<div class='sec'><div class='sec-t'>Tomador do Servi\xe7o</div><div class='row'>" +
      "<div class='f w3'><label>Nome / Raz\xe3o Social</label><span>" + S(dados.xNomeTomador||"Consumidor Final") + "</span></div>" +
      "<div class='f'><label>CPF / CNPJ</label><span>" + S(docFmt||"\u2014") + "</span></div>" +
    "</div></div>" +
    "<div class='sec'><div class='sec-t'>Dados do Servi\xe7o</div>" +
      "<div class='row'>" +
        "<div class='f'><label>Compet\xeancia</label><span>" + fmtD(dados.dCompet) + "</span></div>" +
        "<div class='f w2'><label>C\xf3digo do Servi\xe7o</label><span>14.02 \u2014 Manuten\xe7\xe3o de eletrodom\xe9sticos</span></div>" +
        "<div class='f'><label>Munic\xedpio</label><span>Belo Horizonte / MG</span></div>" +
      "</div>" +
      "<div class='desc'><strong>Discrimina\xe7\xe3o:</strong><br>" + S(dados.xDescServ||"") + "</div>" +
    "</div>" +
    "<div class='sec'><div class='sec-t'>Valores</div><div class='row'>" +
      "<div class='f'><label>Valor do Servi\xe7o</label><span class='vv'>R$ " + fmtV(dados.vServ) + "</span></div>" +
      "<div class='f'><label>Tributa\xe7\xe3o ISSQN</label><span>Simples Nacional \u2014 Tribut\xe1vel</span></div>" +
      "<div class='f'><label>Valor L\xedquido</label><span class='vv'>R$ " + fmtV(dados.vServ) + "</span></div>" +
    "</div></div>" +
    "<div class='sec'><div class='sec-t'>Chave de Acesso</div><div class='chave'>" + chave + "</div></div>" +
    "<div class='footer'>" +
      "<p>Documento emitido eletronicamente conforme LC 116/2003 \u2014 NFS-e Padr\xe3o Nacional</p>" +
      "<p style='margin-top:3px'>Consulte: <strong>https://www.nfse.gov.br/ConsultaNacional</strong></p>" +
    "</div>" +
    "<button class='pbtn' onclick='window.print()'>Imprimir / Salvar como PDF</button>" +
    "</body></html>";
}

// Baixa DANFE via API REST NFS-e (aceita certificado mTLS) e salva no Redis
async function buscarESalvarDanfe(chaveAcesso, certOpts) {
  try {
    const host = NFSE_HOMOLOG ? "sefin.producaorestrita.nfse.gov.br" : "sefin.nfse.gov.br";

    // Busca JSON da NFS-e via API REST (aceita mTLS)
    const resp = await new Promise((resolve, reject) => {
      const opts = {
        hostname: host, port: 443,
        path:     `/SefinNacional/nfse/${chaveAcesso}`,
        method:   "GET",
        pfx: certOpts.pfx, passphrase: certOpts.passphrase,
        rejectUnauthorized: true,
        headers: { "Accept": "application/json" },
      };
      const req = https.request(opts, r => {
        const chunks = [];
        r.on("data", c => chunks.push(c));
        r.on("end", () => resolve({ status: r.statusCode, buf: Buffer.concat(chunks) }));
      });
      req.on("error", reject);
      req.end();
    });

    if (resp.status !== 200) {
      console.log("NFS-e API status:", resp.status);
      return false;
    }

    // Descomprime XML GZip
    const json   = JSON.parse(resp.buf.toString("utf8"));
    const xmlBuf = zlib.gunzipSync(Buffer.from(json.nfseXmlGZipB64, "base64"));
    const xml    = xmlBuf.toString("utf8");

    // Extrai dados e gera DANFE HTML
    const dados   = extrairDadosXml(xml);
    dados.chaveAcesso = chaveAcesso;
    const danfeHtml = gerarDanfeHtml(dados, chaveAcesso);

    // Salva no Redis (como HTML string)
    await fetch(UPSTASH_URL + "/pipeline", {
      method: "POST",
      headers: { Authorization: "Bearer " + UPSTASH_TOKEN, "Content-Type": "application/json" },
      body: JSON.stringify([
        ["SET",    "danfe:" + chaveAcesso, Buffer.from(danfeHtml).toString("base64")],
        ["EXPIRE", "danfe:" + chaveAcesso, 31536000],
      ]),
    });
    console.log("DANFE HTML salvo no Redis:", chaveAcesso.slice(-10));
    return true;

  } catch(e) {
    console.error("buscarESalvarDanfe erro:", e.message);
    return false;
  }
}

// Comprime XML em GZip e converte para base64
function gzipBase64(xml) {
  return new Promise((res, rej) => {
    zlib.gzip(Buffer.from(xml, "utf8"), (err, buf) => {
      if (err) return rej(err);
      res(buf.toString("base64"));
    });
  });
}

// Chama a API do governo com mTLS
function chamarAPI(dpsXmlGZipB64, certOpts) {
  return new Promise((res, rej) => {
    const body = Buffer.from(JSON.stringify({ dpsXmlGZipB64 }), "utf8");
    const opts = {
      hostname:           NFSE_HOST,
      port:               443,
      path:               NFSE_PATH,
      method:             "POST",
      headers: {
        "Content-Type":   "application/json",
        "Content-Length": body.length,
        "Accept":         "application/json",
      },
      pfx:                certOpts.pfx,
      passphrase:         certOpts.passphrase,
      rejectUnauthorized: true,
    };
    const req = https.request(opts, r => {
      const chunks = [];
      r.on("data", c => chunks.push(c));
      r.on("end", () => res({ status: r.statusCode, body: Buffer.concat(chunks).toString("utf8") }));
    });
    req.on("error", rej);
    req.write(body);
    req.end();
  });
}

function parseResp(body) {
  try {
    const j = JSON.parse(body);
    if (j.chaveAcesso) return { ok: true, chaveAcesso: j.chaveAcesso, idDps: j.idDps, nfseXml: j.nfseXmlGZipB64, alertas: j.alertas };
    // Erro
    const msgs = (j.mensagens||j.erros||[]).map(m => m.mensagem||m.descricao||JSON.stringify(m)).join("; ");
    return { ok: false, erro: msgs || j.mensagem || body.slice(0,400) };
  } catch(e) {
    return { ok: false, erro: body.slice(0,400) };
  }
}

module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin","*");
  res.setHeader("Access-Control-Allow-Methods","GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers","Content-Type");
  if (req.method === "OPTIONS") return res.status(200).end();

  const { action } = req.query;

  // ── GET debug-xml — retorna XML gerado sem enviar ao governo
  if (action === "debug-xml") {
    try {
      let certOpts;
      try { certOpts = loadCert(); } catch(e) { certOpts = null; }
      const xml = montarDPS({
        cpfcnpj: "12345678901",
        nome:    "Cliente Teste",
        discriminacao: "Servico de manutencao. Garantia 90 dias.",
        valor:   "350.00",
        numDPS:  genId(1),
      });
      const signed = certOpts ? await assinarXML(xml, certOpts.pfx, certOpts.passphrase) : xml;
      const hasSignature = signed.includes("<Signature");
      res.setHeader("Content-Type","text/xml");
      res.setHeader("X-Has-Signature", String(hasSignature));
      return res.status(200).send(signed);
    } catch(e) {
      return res.status(200).json({ ok: false, error: e.message });
    }
  }

  // ── GET test-sign — diagnose signing
  if (action === "test-sign") {
    const result = { steps: [] };
    try {
      const certOpts = loadCert();
      result.steps.push("cert loaded: " + certOpts.pfx.length + " bytes");
      try {
        // Parse PFX com node-forge
        const p12Asn1 = forge.asn1.fromDer(forge.util.createBuffer(certOpts.pfx));
        result.steps.push("pfx parsed");
        const p12 = forge.pkcs12.pkcs12FromAsn1(p12Asn1, certOpts.passphrase);
        result.steps.push("pkcs12 loaded");

        // Extrai chave privada
        let privateKeyPem = "";
        const shroudedBags = p12.getBags({ bagType: forge.pki.oids.pkcs8ShroudedKeyBag })[forge.pki.oids.pkcs8ShroudedKeyBag] || [];
        const keyBags      = p12.getBags({ bagType: forge.pki.oids.keyBag })[forge.pki.oids.keyBag] || [];
        const allKeyBags   = [...shroudedBags, ...keyBags];
        if (allKeyBags.length > 0) {
          privateKeyPem = forge.pki.privateKeyToPem(allKeyBags[0].key);
          result.steps.push("privateKey extracted: " + privateKeyPem.slice(0,40));
        } else {
          result.steps.push("no key bags found");
        }

        if (privateKeyPem) {
          const pk = crypto.createPrivateKey({ key: privateKeyPem, format: "pem" });
          const signer = crypto.createSign("sha256WithRSAEncryption");
          signer.update("test");
          const sig = signer.sign(pk, "base64");
          result.steps.push("signature OK: " + sig.slice(0,30) + "...");
          result.ok = true;
        } else {
          result.ok = false;
        }
      } catch(e) {
        result.steps.push("sign error: " + e.message);
        result.ok = false;
      }
    } catch(e) {
      result.steps.push("cert error: " + e.message);
      result.ok = false;
    }
    return res.status(200).json(result);
  }

  if (action === "diag-sign") {
    // Retorna exatamente o que está sendo assinado para debug
    try {
      const certOpts = loadCert();
      const p12Asn1  = forge.asn1.fromDer(forge.util.createBuffer(certOpts.pfx));
      const p12      = forge.pkcs12.pkcs12FromAsn1(p12Asn1, certOpts.passphrase);
      const bags     = [...(p12.getBags({bagType:forge.pki.oids.pkcs8ShroudedKeyBag})[forge.pki.oids.pkcs8ShroudedKeyBag]||[]),
                        ...(p12.getBags({bagType:forge.pki.oids.keyBag})[forge.pki.oids.keyBag]||[])];
      const privKey  = bags[0].key;
      const privPem  = forge.pki.privateKeyToPem(privKey);

      // Gera XML de teste e assina
      const seq = 9999;
      const xml  = montarDPS({ cpfcnpj:"12345678901", nome:"Teste", discriminacao:"Teste diag", valor:"100.00", numDPS: genId(seq) });
      
      // Extrai o que vai ser assinado
      const idMatch = xml.match(/infDPS Id="([^"]+)"/);
      const refId   = idMatch[1];
      const infDpsRaw = xml.match(/<infDPS[\s\S]*?<\/infDPS>/)[0];
      const infDpsC14n = infDpsRaw.replace(/^<infDPS /, '<infDPS xmlns="http://www.sped.fazenda.gov.br/nfse" ');
      const digestBuf  = crypto.createHash("sha256").update(infDpsC14n,"utf8").digest();
      const digest     = digestBuf.toString("base64");

      const signedInfoStr =
        `<SignedInfo xmlns="http://www.w3.org/2000/09/xmldsig#">` +
        `<CanonicalizationMethod Algorithm="http://www.w3.org/2001/10/xml-exc-c14n#WithComments"/>` +
        `<SignatureMethod Algorithm="http://www.w3.org/2001/04/xmldsig-more#rsa-sha256"/>` +
        `<Reference URI="#${refId}">` +
        `<Transforms><Transform Algorithm="http://www.w3.org/2000/09/xmldsig#enveloped-signature"/>` +
        `<Transform Algorithm="http://www.w3.org/2001/10/xml-exc-c14n#WithComments"/></Transforms>` +
        `<DigestMethod Algorithm="http://www.w3.org/2001/04/xmlenc#sha256"/>` +
        `<DigestValue>${digest}</DigestValue></Reference></SignedInfo>`;

      // Assina com Node.js crypto usando PEM extraído pelo forge
      const nodeKey = crypto.createPrivateKey({ key: privPem, format: "pem" });
      const signer  = crypto.createSign("RSA-SHA256");
      signer.update(signedInfoStr, "utf8");
      const sigNode = signer.sign(nodeKey, "base64");

      // Assina com forge directamente
      const md = forge.md.sha256.create();
      md.update(signedInfoStr, "utf8");
      const sigForge = Buffer.from(privKey.sign(md), "binary").toString("base64");

      return res.status(200).json({
        ok: true,
        refId,
        digest: digest.slice(0,20)+"...",
        signedInfoLen: signedInfoStr.length,
        signedInfoPreview: signedInfoStr.slice(0,100),
        sigNodeLen: sigNode.length,
        sigForgeLen: sigForge.length,
        sigsMatch: sigNode === sigForge,
      });
    } catch(e) {
      return res.status(200).json({ ok: false, error: e.message, stack: (e.stack||"").slice(0,300) });
    }
  }

  if (action === "xtest2") {
    const steps = [];
    try {
      // Check xmldom availability
      let xmldom;
      try { xmldom = require("xmldom"); steps.push("xmldom: OK"); }
      catch(e) { steps.push("xmldom: " + e.message); }

      // Try xpath module
      let xpathMod;
      try { xpathMod = require("xpath"); steps.push("xpath module: OK"); }
      catch(e) { steps.push("xpath module: " + e.message); }

      if (xmldom && xpathMod) {
        const doc = new xmldom.DOMParser().parseFromString(
          `<DPS xmlns="http://www.sped.fazenda.gov.br/nfse"><infDPS Id="X1"><v>1</v></infDPS></DPS>`
        );
        const nodes1 = xpathMod.select("//*[local-name()='infDPS']", doc);
        steps.push("xpath select infDPS: " + nodes1.length + " nodes");
        const nodes2 = xpathMod.select("/*", doc);
        steps.push("xpath select /*: " + nodes2.length + " nodes");
      }

      return res.status(200).json({ ok: true, steps });
    } catch(e) {
      return res.status(200).json({ ok: false, error: e.message, steps });
    }
  }

  if (action === "xtest") {
    const steps = [];
    try {
      const certOpts = loadCert();
      const p12Asn1 = forge.asn1.fromDer(forge.util.createBuffer(certOpts.pfx));
      const p12 = forge.pkcs12.pkcs12FromAsn1(p12Asn1, certOpts.passphrase);
      const allBags = [
        ...(p12.getBags({ bagType: forge.pki.oids.pkcs8ShroudedKeyBag })[forge.pki.oids.pkcs8ShroudedKeyBag] || []),
        ...(p12.getBags({ bagType: forge.pki.oids.keyBag })[forge.pki.oids.keyBag] || []),
      ];
      const pem = forge.pki.privateKeyToPem(allBags[0].key);
      steps.push("key: OK");

      const { SignedXml } = xmlCrypto;
      const sig = new SignedXml({ privateKey: pem });

      // Try simplest possible xpath
      const xpaths = [
        "//*[local-name(.)='infDPS']",
        "/*",
        "/DPS",
        "//infDPS",
      ];

      const xml = `<DPS xmlns="http://www.sped.fazenda.gov.br/nfse"><infDPS Id="X1"><v>1</v></infDPS></DPS>`;

      for (const xp of xpaths) {
        try {
          const s2 = new SignedXml({ privateKey: pem });
          s2.addReference({
            xpath: xp,
            transforms: ["http://www.w3.org/2000/09/xmldsig#enveloped-signature"],
            digestAlgorithm: "http://www.w3.org/2001/04/xmlenc#sha256",
          });
          await s2.computeSignature(xml);
          const out = s2.getSignedXml();
          steps.push("xpath '" + xp + "': " + (out.includes("<Signature") ? "OK" : "no sig"));
          if (out.includes("<Signature")) break;
        } catch(e) {
          steps.push("xpath '" + xp + "': ERR " + e.message.slice(0,50));
        }
      }
      return res.status(200).json({ ok: true, steps });
    } catch(e) {
      return res.status(200).json({ ok: false, error: e.message, steps });
    }
  }

  if (action === "test-xmlcrypto-v") {
    try {
      const ver = require("/var/task/node_modules/xml-crypto/package.json").version;
      const SignedXml = require("xml-crypto").SignedXml;
      return res.status(200).json({ ok: true, version: ver, SignedXmlType: typeof SignedXml });
    } catch(e) {
      return res.status(200).json({ ok: false, error: e.message });
    }
  }

  if (action === "test-xmlcrypto") {
    const steps = [];
    try {
      const certOpts = loadCert();
      steps.push("cert OK");
      const p12Asn1 = forge.asn1.fromDer(forge.util.createBuffer(certOpts.pfx));
      const p12     = forge.pkcs12.pkcs12FromAsn1(p12Asn1, certOpts.passphrase);
      const shrouded = (p12.getBags({ bagType: forge.pki.oids.pkcs8ShroudedKeyBag })[forge.pki.oids.pkcs8ShroudedKeyBag] || []);
      const plain    = (p12.getBags({ bagType: forge.pki.oids.keyBag })[forge.pki.oids.keyBag] || []);
      const keyBag   = [...shrouded, ...plain][0];
      if (!keyBag) throw new Error("No key bag");
      const privateKeyPem = forge.pki.privateKeyToPem(keyBag.key);
      steps.push("key extracted");

      const { SignedXml } = xmlCrypto;
      steps.push("SignedXml type: " + typeof SignedXml);

      const sig = new SignedXml({
        privateKey: privateKeyPem,
        canonicalizationAlgorithm: "http://www.w3.org/2001/10/xml-exc-c14n#WithComments",
        signatureAlgorithm: "http://www.w3.org/2001/04/xmldsig-more#rsa-sha256",
      });

      sig.addReference({
        xpath: "//*[local-name(.)='infDPS']",
        transforms: ["http://www.w3.org/2000/09/xmldsig#enveloped-signature","http://www.w3.org/2001/10/xml-exc-c14n#WithComments"],
        digestAlgorithm: "http://www.w3.org/2001/04/xmlenc#sha256",
      });
      steps.push("reference added");

      const testXml = `<DPS xmlns="http://www.sped.fazenda.gov.br/nfse"><infDPS Id="DPS123"><test>x</test></infDPS></DPS>`;

      try {
        await sig.computeSignature(testXml, { location: { reference: "//*[local-name(.)='DPS']", action: "append" } });
        steps.push("computeSignature OK");
      } catch(e2) {
        steps.push("computeSignature ERROR: " + e2.message);
        return res.status(200).json({ ok: false, steps });
      }

      const signed = sig.getSignedXml();
      steps.push("getSignedXml len: " + signed.length);
      steps.push("hasSig: " + signed.includes("<Signature"));

      return res.status(200).json({ ok: true, steps, preview: signed.slice(0,300) });
    } catch(e) {
      return res.status(200).json({ ok: false, error: e.message, steps });
    }
  }

  // ── GET danfe?chave=XXX&nome=YYY — serve DANFE do Redis (ou tenta baixar do gov)
  if (action === "danfe") {
    const chave = (req.query.chave || "").trim();
    const nome  = (req.query.nome  || "danfe-" + chave.slice(-10)).trim();
    if (!chave) return res.status(400).json({ ok: false, error: "chave obrigatória" });

    // 1. Tenta servir do Redis
    const getRedisDanfe = async () => {
      const r = await fetch(UPSTASH_URL + "/get/danfe:" + chave, {
        headers: { Authorization: "Bearer " + UPSTASH_TOKEN },
      });
      const j = await r.json();
      return j.result ? Buffer.from(j.result, "base64").toString("utf8") : null;
    };

    let certOpts;
    try { certOpts = loadCert(); } catch(e) { return res.status(400).json({ ok: false, error: e.message }); }

    let html = null;
    try { html = await getRedisDanfe(); } catch(e) {}

    // 2. Não tem no Redis — busca da API do governo
    if (!html) {
      try { await buscarESalvarDanfe(chave, certOpts); } catch(e) {}
      try { html = await getRedisDanfe(); } catch(e) {}
    }

    if (html && html.startsWith("<!DOCTYPE")) {
      // Serve HTML diretamente
      res.setHeader("Content-Type", "text/html; charset=utf-8");
      return res.status(200).send(html);
    }

    return res.status(503).json({ ok: false, error: "DANFE não disponível. Tente em alguns instantes." });
  }

  // ── GET danfe-html?chave=XXX — gera DANFE em HTML para impressão/download como PDF
  if (action === "danfe-html") {
    const chave = (req.query.chave || "").trim();
    const nome  = (req.query.nome  || "Cliente").trim();
    if (!chave) return res.status(400).json({ ok: false, error: "chave obrigatória" });

    // Busca dados salvos no Redis junto com o DANFE (salvos na emissão)
    let dadosNF = null;
    try {
      const r = await fetch(UPSTASH_URL + "/get/nfdata:" + chave, {
        headers: { Authorization: "Bearer " + UPSTASH_TOKEN },
      });
      const j = await r.json();
      if (j.result) dadosNF = JSON.parse(j.result);
    } catch(e) {}

    const d = dadosNF || {};
    const html = `<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<title>NFS-e - ${nome}</title>
<style>
  * { margin:0; padding:0; box-sizing:border-box; }
  body { font-family: Arial, sans-serif; font-size: 11px; color: #000; background: #fff; padding: 20px; }
  .header { text-align: center; border: 2px solid #000; padding: 10px; margin-bottom: 8px; }
  .header h1 { font-size: 16px; font-weight: bold; }
  .header h2 { font-size: 13px; }
  .header p  { font-size: 10px; margin-top: 4px; }
  .section { border: 1px solid #000; margin-bottom: 6px; }
  .section-title { background: #e0e0e0; font-weight: bold; padding: 4px 8px; font-size: 10px; text-transform: uppercase; border-bottom: 1px solid #000; }
  .row { display: flex; border-bottom: 1px solid #ccc; }
  .row:last-child { border-bottom: none; }
  .field { padding: 4px 8px; flex: 1; }
  .field label { display: block; font-size: 9px; color: #555; text-transform: uppercase; }
  .field span  { font-size: 11px; font-weight: bold; }
  .field.w2 { flex: 2; }
  .field.w3 { flex: 3; }
  .chave { font-size: 9px; word-break: break-all; padding: 6px 8px; background: #f5f5f5; border-top: 1px solid #ccc; }
  .valor-box { text-align: center; padding: 10px; }
  .valor-box .v { font-size: 22px; font-weight: bold; color: #000; }
  .footer { text-align: center; font-size: 9px; color: #666; margin-top: 10px; }
  @media print { body { padding: 0; } button { display: none; } }
</style>
</head>
<body>

<div class="header">
  <h1>NOTA FISCAL DE SERVIÇO ELETRÔNICA — NFS-e</h1>
  <h2>REPARO ELETRO - CONSERTO DE ELETRODOMESTICOS LTDA</h2>
  <p>CNPJ: 59.485.378/0001-75 &nbsp;|&nbsp; IM: 16391680010 &nbsp;|&nbsp; Belo Horizonte - MG</p>
  <p>Rua Ouro Preto, 663 - Barro Preto - CEP 30170-044</p>
</div>

<div class="section">
  <div class="section-title">Dados da Nota</div>
  <div class="row">
    <div class="field"><label>Chave de Acesso</label><span style="font-size:9px;font-weight:normal">${chave}</span></div>
  </div>
  <div class="row">
    <div class="field"><label>Data de Emissão</label><span>${d.dhEmi || new Date().toLocaleDateString("pt-BR")}</span></div>
    <div class="field"><label>Competência</label><span>${d.dCompet || new Date().toLocaleDateString("pt-BR")}</span></div>
    <div class="field"><label>Código do Serviço</label><span>14.02.01 — Manutenção e reparação de eletrodomésticos</span></div>
  </div>
</div>

<div class="section">
  <div class="section-title">Tomador do Serviço</div>
  <div class="row">
    <div class="field w3"><label>Nome / Razão Social</label><span>${d.tomadorNome || nome.replace(/_/g,' ')}</span></div>
    <div class="field"><label>CPF / CNPJ</label><span>${d.tomadorDoc || ""}</span></div>
  </div>
</div>

<div class="section">
  <div class="section-title">Discriminação do Serviço</div>
  <div class="row">
    <div class="field" style="white-space:pre-wrap"><label>Descrição</label><span>${d.discriminacao || ""}</span></div>
  </div>
</div>

<div class="section">
  <div class="section-title">Valores</div>
  <div class="row">
    <div class="field"><label>Valor do Serviço</label><span class="v">R$ ${Number(d.valor||0).toLocaleString("pt-BR",{minimumFractionDigits:2})}</span></div>
    <div class="field"><label>Tributação ISSQN</label><span>Operação Tributável — Simples Nacional</span></div>
    <div class="field"><label>Município de Incidência</label><span>Belo Horizonte / MG</span></div>
  </div>
</div>

<div class="footer">
  <p>Documento emitido eletronicamente conforme LC 116/2003 — NFS-e Padrão Nacional</p>
  <p style="margin-top:4px">Consulte sua nota em: <strong>https://www.nfse.gov.br/ConsultaNacional</strong></p>
</div>

<br>
<div style="text-align:center">
  <button onclick="window.print()" style="padding:10px 30px;font-size:14px;background:#3b9eff;color:#fff;border:none;border-radius:8px;cursor:pointer;">🖨️ Imprimir / Salvar como PDF</button>
</div>

</body>
</html>`;

    res.setHeader("Content-Type", "text/html; charset=utf-8");
    return res.status(200).send(html);
  }

  if (action === "danfe-force") {
    // Força busca da NFS-e via API e gera DANFE — sem cache Redis
    const chave = (req.query.chave || "").trim();
    if (!chave) return res.status(400).json({ ok: false, error: "chave obrigatória" });
    let certOpts;
    try { certOpts = loadCert(); } catch(e) { return res.status(200).json({ step: "cert", error: e.message }); }
    try {
      const host = NFSE_HOMOLOG ? "sefin.producaorestrita.nfse.gov.br" : "sefin.nfse.gov.br";
      const resp = await new Promise((resolve, reject) => {
        const opts = {
          hostname: host, port: 443,
          path: `/SefinNacional/nfse/${chave}`,
          method: "GET",
          pfx: certOpts.pfx, passphrase: certOpts.passphrase,
          rejectUnauthorized: false,
          headers: { "Accept": "application/json" },
        };
        const req2 = https.request(opts, r => {
          const chunks = [];
          r.on("data", c => chunks.push(c));
          r.on("end", () => resolve({ status: r.statusCode, buf: Buffer.concat(chunks) }));
        });
        req2.on("error", reject);
        req2.end();
      });

      if (resp.status !== 200) {
        return res.status(200).json({ step: "api", status: resp.status, body: resp.buf.slice(0,200).toString() });
      }

      const json   = JSON.parse(resp.buf.toString("utf8"));
      const xmlBuf = zlib.gunzipSync(Buffer.from(json.nfseXmlGZipB64, "base64"));
      const xml    = xmlBuf.toString("utf8");
      const dados  = extrairDadosXml(xml);
      dados.chaveAcesso = chave;
      const danfeHtml = gerarDanfeHtml(dados, chave);

      // Salva no Redis
      await fetch(UPSTASH_URL + "/pipeline", {
        method: "POST",
        headers: { Authorization: "Bearer " + UPSTASH_TOKEN, "Content-Type": "application/json" },
        body: JSON.stringify([
          ["SET",    "danfe:" + chave, Buffer.from(danfeHtml).toString("base64")],
          ["EXPIRE", "danfe:" + chave, 31536000],
        ]),
      });

      res.setHeader("Content-Type", "text/html; charset=utf-8");
      return res.status(200).send(danfeHtml);

    } catch(e) {
      return res.status(200).json({ step: "error", error: e.message, stack: (e.stack||"").slice(0,300) });
    }
  }

  if (action === "danfe-nfse-json") {
    // Retorna o JSON completo da NFS-e da API do governo
    const chave = (req.query.chave || "31062002259485378000175000000000016126034193872720").trim();
    let certOpts;
    try { certOpts = loadCert(); } catch(e) { return res.status(200).json({ error: e.message }); }
    const host = NFSE_HOMOLOG ? "sefin.producaorestrita.nfse.gov.br" : "sefin.nfse.gov.br";
    const r = await new Promise((resolve) => {
      const opts = { hostname: host, port: 443, path: `/SefinNacional/nfse/${chave}`, method: "GET",
        pfx: certOpts.pfx, passphrase: certOpts.passphrase, rejectUnauthorized: false,
        headers: { "Accept": "application/json" },
      };
      const req2 = https.request(opts, r => {
        const chunks = [];
        r.on("data", c => chunks.push(c));
        r.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
      });
      req2.on("error", e => resolve(JSON.stringify({ error: e.message })));
      req2.end();
    });
    try {
      const j = JSON.parse(r);
      // Mostra só as chaves de primeiro nível e tamanhos
      const summary = {};
      for (const [k,v] of Object.entries(j)) {
        summary[k] = typeof v === "string" ? v.slice(0,80) + (v.length>80?"...":"") : v;
      }
      return res.status(200).json({ ok: true, keys: Object.keys(j), summary });
    } catch(e) {
      return res.status(200).json({ ok: false, raw: r.slice(0,500) });
    }
  }

  if (action === "danfe-api-test") {
    const chave = (req.query.chave || "31062002259485378000175000000000016126034193872720").trim();
    let certOpts;
    try { certOpts = loadCert(); } catch(e) { return res.status(200).json({ error: e.message }); }
    const host = NFSE_HOMOLOG ? "sefin.producaorestrita.nfse.gov.br" : "sefin.nfse.gov.br";
    const paths = [
      `/SefinNacional/nfse/${chave}/danfe`,
      `/SefinNacional/nfse/${chave}`,
    ];
    const results = [];
    for (const path of paths) {
      const r = await new Promise((resolve) => {
        const opts = { hostname: host, port: 443, path, method: "GET",
          pfx: certOpts.pfx, passphrase: certOpts.passphrase, rejectUnauthorized: false,
          headers: { "Accept": "application/pdf,application/json,*/*" },
        };
        const req2 = https.request(opts, r => {
          const chunks = [];
          r.on("data", c => chunks.push(c));
          r.on("end", () => resolve({ status: r.statusCode, ct: r.headers["content-type"], len: Buffer.concat(chunks).length, preview: Buffer.concat(chunks).slice(0,150).toString() }));
        });
        req2.on("error", e => resolve({ error: e.message }));
        req2.end();
      });
      results.push({ path, ...r });
    }
    return res.status(200).json({ ok: true, host, results });
  }

  if (action === "danfe-debug") {
    const chave = (req.query.chave || "").trim() || "31062002259485378000175000000000016126034193872720";
    let certOpts;
    try { certOpts = loadCert(); } catch(e) { return res.status(200).json({ step: "cert", error: e.message }); }
    try {
      const result = await new Promise((resolve) => {
        const opts = {
          hostname: "www.nfse.gov.br",
          port: 443,
          path: `/EmissorNacional/Notas/Download/DANFSe/${chave}`,
          method: "GET",
          pfx:        certOpts.pfx,
          passphrase: certOpts.passphrase,
          rejectUnauthorized: false,
          headers: { "Accept": "application/pdf,*/*", "User-Agent": "Mozilla/5.0" },
        };
        const req2 = https.request(opts, r => {
          const chunks = [];
          r.on("data", c => chunks.push(c));
          r.on("end", () => {
            const body = Buffer.concat(chunks);
            resolve({
              status: r.statusCode,
              ct: r.headers["content-type"],
              location: r.headers["location"],
              bodyPreview: body.slice(0,200).toString("utf8"),
              bodyLen: body.length,
            });
          });
        });
        req2.on("error", e => resolve({ error: e.message }));
        req2.end();
      });
      return res.status(200).json({ ok: true, result });
    } catch(e) {
      return res.status(200).json({ ok: false, error: e.message });
    }
  }

  if (action === "status") {
    return res.status(200).json({
      ok: !!(process.env.NFSE_CERT_PFX && process.env.NFSE_CERT_SENHA),
      temCert:  !!(process.env.NFSE_CERT_PFX),
      temSenha: !!(process.env.NFSE_CERT_SENHA),
      temIM:    !!(IM_EMPRESA),
      cnpj:     CNPJ_EMPRESA,
      im:       IM_EMPRESA,
    });
  }

  if (req.method === "POST" && action === "emitir") {
    const { tomadorCpfCnpj, tomadorNome, discriminacao, valor } = req.body || {};
    if (!tomadorCpfCnpj || !valor)
      return res.status(400).json({ ok: false, error: "tomadorCpfCnpj e valor obrigatórios" });

    let certOpts;
    try { certOpts = loadCert(); } catch(e) { return res.status(400).json({ ok: false, error: e.message }); }

    try {
      // Busca próximo número sequencial do Redis para garantir unicidade
      let seq = 1;
      try {
        const seqRes = await dbGet("reparoeletro_nfse_seq");
        seq = (seqRes || 0) + 1;
        await dbSet("reparoeletro_nfse_seq", seq);
      } catch(e) { seq = Date.now() % 999999999999999 || 1; }
      const numDPS = genId(seq);
      console.log("ID gerado:", numDPS, "len:", numDPS.length);
      const xml    = montarDPS({ cpfcnpj: tomadorCpfCnpj, nome: tomadorNome, discriminacao, valor, numDPS });
      const xmlAss = await assinarXML(xml, certOpts.pfx, certOpts.passphrase);
      const b64gz  = await gzipBase64(xmlAss);
      const { status, body } = await chamarAPI(b64gz, certOpts);
      const parsed = parseResp(body);

      if (parsed.ok) {
        // Salva dados da NF no Redis para geração do DANFE offline
        const nfData = {
          dhEmi:         agora(),
          dCompet:       hoje(),
          tomadorNome:   tomadorNome || "",
          tomadorDoc:    tomadorCpfCnpj || "",
          discriminacao: discriminacao  || "",
          valor:         parseFloat(valor).toFixed(2),
          chaveAcesso:   parsed.chaveAcesso,
        };
        fetch(UPSTASH_URL + "/pipeline", {
          method: "POST",
          headers: { Authorization: "Bearer " + UPSTASH_TOKEN, "Content-Type": "application/json" },
          body: JSON.stringify([
            ["SET", "nfdata:" + parsed.chaveAcesso, JSON.stringify(nfData)],
            ["EXPIRE", "nfdata:" + parsed.chaveAcesso, 31536000],
          ]),
        }).catch(()=>{});
        // Tenta salvar DANFE do portal no Redis (pode falhar sem sessão)
        setTimeout(() => buscarESalvarDanfe(parsed.chaveAcesso, certOpts).catch(()=>{}), 2000);
        return res.status(200).json({ ok: true, chaveAcesso: parsed.chaveAcesso, idDps: parsed.idDps, alertas: parsed.alertas });
      } else {
        return res.status(200).json({ ok: false, error: parsed.erro, httpStatus: status, idDPS: numDPS, idLen: numDPS.length });
      }
    } catch(e) {
      return res.status(200).json({ ok: false, error: e.message });
    }
  }

  return res.status(404).json({ ok: false, error: "Ação não encontrada" });
};
