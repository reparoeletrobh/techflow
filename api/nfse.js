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
  const getIn = (parent, tag) => {
    const pMatch = xml.match(new RegExp("<" + parent + "[^>]*>([\\s\\S]*?)<\\/" + parent + ">"));
    if (!pMatch) return "";
    const m = pMatch[1].match(new RegExp("<" + tag + "[^>]*>([\\s\\S]*?)<\\/" + tag + ">"));
    return m ? m[1].trim() : "";
  };
  return {
    nNFSe:        get("nNFSe"),
    dhProc:       get("dhProc"),
    dCompet:      get("dCompet"),
    dhEmi:        get("dhEmi"),
    xDescServ:    get("xDescServ"),
    // vServ dentro de vServPrest
    vServ:        getIn("vServPrest", "vServ") || get("vServ"),
    // Tomador: CPF/CNPJ e xNome dentro de <toma>
    cpfTomador:   getIn("toma", "CPF") || getIn("toma", "CNPJ"),
    xNomeTomador: getIn("toma", "xNome"),
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
