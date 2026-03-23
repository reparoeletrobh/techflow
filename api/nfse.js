// nfse.js — NFS-e Nacional com Certificado A1
// Dependências: node-forge (package.json)
const https  = require("https");
const crypto = require("crypto");
const zlib   = require("zlib");
const forge  = require("node-forge");

const NFSE_HOMOLOG  = (process.env.NFSE_HOMOLOG || "false") === "true";
const NFSE_HOST     = NFSE_HOMOLOG ? "sefin.producaorestrita.nfse.gov.br" : "sefin.nfse.gov.br";
const NFSE_PATH     = "/SefinNacional/nfse";
const CNPJ_EMPRESA  = (process.env.NFSE_CNPJ || "59485378000175").replace(/\D/g,"");
const IM_EMPRESA    =  process.env.NFSE_IM   || "16391680010";
const COD_MUN_BH    = "3106200";
const UPSTASH_URL   = (process.env.UPSTASH_URL   || "").replace(/['"]/g,"").trim();
const UPSTASH_TOKEN = (process.env.UPSTASH_TOKEN || "").replace(/['"]/g,"").trim();

// ── Cert ─────────────────────────────────────────────────────
function loadCert() {
  const b64  = (process.env.NFSE_CERT_PFX   || "").trim();
  const pass = (process.env.NFSE_CERT_SENHA  || "").trim();
  if (!b64)  throw new Error("NFSE_CERT_PFX nao configurado no Vercel");
  if (!pass) throw new Error("NFSE_CERT_SENHA nao configurado no Vercel");
  return { pfx: Buffer.from(b64, "base64"), passphrase: pass };
}

// ── Redis ────────────────────────────────────────────────────
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

// ── Helpers ──────────────────────────────────────────────────
function escXml(s) {
  return String(s||"").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;");
}

function agora() {
  const d = new Date(new Date().toLocaleString("en-US",{timeZone:"America/Sao_Paulo"}));
  const p = n => String(n).padStart(2,"0");
  return `${d.getFullYear()}-${p(d.getMonth()+1)}-${p(d.getDate())}T${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}-03:00`;
}

function hoje() {
  const d = new Date(new Date().toLocaleString("en-US",{timeZone:"America/Sao_Paulo"}));
  const p = n => String(n).padStart(2,"0");
  return `${d.getFullYear()}-${p(d.getMonth()+1)}-${p(d.getDate())}`;
}

function genId(seq) {
  // TSIdDPS = DPS(3)+cMun(7)+tpInsc(1)+CNPJ(14)+Serie(5)+Numero(15) = 45 chars
  const num   = String(seq || 1).padStart(15,"0");
  const serie = "00001";
  return "DPS" + COD_MUN_BH + "2" + CNPJ_EMPRESA + serie + num;
}

// ── Monta DPS ────────────────────────────────────────────────
function montarDPS({ cpfcnpj, nome, discriminacao, valor, numDPS }) {
  const cpfLimpo = cpfcnpj.replace(/\D/g,"");
  const isCnpj   = cpfLimpo.length === 14;
  const toma     = isCnpj ? "<CNPJ>" + cpfLimpo + "</CNPJ>" : "<CPF>" + cpfLimpo + "</CPF>";
  const vlr      = parseFloat(valor).toFixed(2);
  const id       = numDPS;
  const nDPS     = String(seq_num_only(numDPS));

  return '<?xml version="1.0" encoding="UTF-8"?>\n' +
'<DPS versao="1.01" xmlns="http://www.sped.fazenda.gov.br/nfse">\n' +
'  <infDPS Id="' + id + '">\n' +
'    <tpAmb>' + (NFSE_HOMOLOG ? 2 : 1) + '</tpAmb>\n' +
'    <dhEmi>' + agora() + '</dhEmi>\n' +
'    <verAplic>reparoeletro-1.0</verAplic>\n' +
'    <serie>00001</serie>\n' +
'    <nDPS>' + nDPS + '</nDPS>\n' +
'    <dCompet>' + hoje() + '</dCompet>\n' +
'    <tpEmit>1</tpEmit>\n' +
'    <cLocEmi>' + COD_MUN_BH + '</cLocEmi>\n' +
'    <prest>\n' +
'      <CNPJ>' + CNPJ_EMPRESA + '</CNPJ>\n' +
'      <IM>' + IM_EMPRESA + '</IM>\n' +
'      <regTrib>\n' +
'        <opSimpNac>3</opSimpNac>\n' +
'        <regApTribSN>1</regApTribSN>\n' +
'        <regEspTrib>0</regEspTrib>\n' +
'      </regTrib>\n' +
'    </prest>\n' +
'    <toma>\n' +
'      ' + toma + '\n' +
'      <xNome>' + escXml(nome || "Consumidor Final") + '</xNome>\n' +
'    </toma>\n' +
'    <serv>\n' +
'      <locPrest>\n' +
'        <cLocPrestacao>' + COD_MUN_BH + '</cLocPrestacao>\n' +
'      </locPrest>\n' +
'      <cServ>\n' +
'        <cTribNac>140201</cTribNac>\n' +
'        <cTribMun>001</cTribMun>\n' +
'        <xDescServ>' + escXml(discriminacao) + '</xDescServ>\n' +
'      </cServ>\n' +
'    </serv>\n' +
'    <valores>\n' +
'      <vServPrest>\n' +
'        <vServ>' + vlr + '</vServ>\n' +
'      </vServPrest>\n' +
'      <trib>\n' +
'        <tribMun>\n' +
'          <tribISSQN>1</tribISSQN>\n' +
'          <tpRetISSQN>1</tpRetISSQN>\n' +
'        </tribMun>\n' +
'        <totTrib>\n' +
'          <pTotTribSN>6.00</pTotTribSN>\n' +
'        </totTrib>\n' +
'      </trib>\n' +
'    </valores>\n' +
'  </infDPS>\n' +
'</DPS>';
}

// Extrai apenas o numero sequencial do TSIdDPS
function seq_num_only(id) {
  // id = DPS(3)+cMun(7)+tpInsc(1)+CNPJ(14)+Serie(5)+Numero(15)
  const n = String(id).slice(-15).replace(/^0+/,"");
  return n || "1";
}

// ── Assinatura XMLDSig com node-forge ────────────────────────
async function assinarXML(xml, pfxBuf, passphrase) {
  try {
    const p12Asn1 = forge.asn1.fromDer(forge.util.createBuffer(pfxBuf));
    const p12     = forge.pkcs12.pkcs12FromAsn1(p12Asn1, passphrase);
    const shrouded = (p12.getBags({bagType:forge.pki.oids.pkcs8ShroudedKeyBag})[forge.pki.oids.pkcs8ShroudedKeyBag]||[]);
    const plain    = (p12.getBags({bagType:forge.pki.oids.keyBag})[forge.pki.oids.keyBag]||[]);
    const keyBag   = [...shrouded,...plain][0];
    if (!keyBag) throw new Error("Chave privada nao encontrada no PFX");
    const privateKey = keyBag.key;

    const certBags   = (p12.getBags({bagType:forge.pki.oids.certBag})[forge.pki.oids.certBag]||[]);
    const certBase64 = certBags[0]
      ? forge.pki.certificateToPem(certBags[0].cert)
          .replace(/-----BEGIN CERTIFICATE-----/,"").replace(/-----END CERTIFICATE-----/,"").replace(/\s/g,"")
      : "";

    const idMatch = xml.match(/infDPS Id="([^"]+)"/);
    if (!idMatch) throw new Error("Id do infDPS nao encontrado");
    const refId = idMatch[1];

    // C14N do infDPS — injeta xmlns e expande self-closing tags
    const infDpsRaw = xml.match(/<infDPS[\s\S]*?<\/infDPS>/)?.[0];
    if (!infDpsRaw) throw new Error("infDPS nao encontrado");
    let infDpsC14n = infDpsRaw.replace(/^<infDPS /, '<infDPS xmlns="http://www.sped.fazenda.gov.br/nfse" ');
    infDpsC14n = infDpsC14n.replace(/<([a-zA-Z][^>]*?)\/>/g, (m, inner) => "<" + inner.trimEnd() + "></" + inner.trim().split(/[\s>]/)[0] + ">");

    const md = forge.md.sha256.create();
    md.update(infDpsC14n, "utf8");
    const digest = forge.util.encode64(md.digest().bytes());

    const signedInfoC14n =
      '<SignedInfo xmlns="http://www.w3.org/2000/09/xmldsig#">' +
      '<CanonicalizationMethod Algorithm="http://www.w3.org/2001/10/xml-exc-c14n#WithComments"></CanonicalizationMethod>' +
      '<SignatureMethod Algorithm="http://www.w3.org/2001/04/xmldsig-more#rsa-sha256"></SignatureMethod>' +
      '<Reference URI="#' + refId + '">' +
        '<Transforms>' +
          '<Transform Algorithm="http://www.w3.org/2000/09/xmldsig#enveloped-signature"></Transform>' +
          '<Transform Algorithm="http://www.w3.org/2001/10/xml-exc-c14n#WithComments"></Transform>' +
        '</Transforms>' +
        '<DigestMethod Algorithm="http://www.w3.org/2001/04/xmlenc#sha256"></DigestMethod>' +
        '<DigestValue>' + digest + '</DigestValue>' +
      '</Reference>' +
      '</SignedInfo>';

    const mdSig = forge.md.sha256.create();
    mdSig.update(signedInfoC14n, "utf8");
    const sigValue = forge.util.encode64(privateKey.sign(mdSig));

    const signedInfoInXml = signedInfoC14n.replace('<SignedInfo xmlns="http://www.w3.org/2000/09/xmldsig#">', "<SignedInfo>");
    const signature =
      '<Signature xmlns="http://www.w3.org/2000/09/xmldsig#">' +
        signedInfoInXml +
        '<SignatureValue>' + sigValue + '</SignatureValue>' +
        (certBase64 ? '<KeyInfo><X509Data><X509Certificate>' + certBase64 + '</X509Certificate></X509Data></KeyInfo>' : "") +
      '</Signature>';

    return xml.replace("</infDPS>", "</infDPS>" + signature);
  } catch(e) {
    console.error("assinarXML erro:", e.message);
    return xml;
  }
}

// ── GZip + Base64 ────────────────────────────────────────────
function gzipBase64(xml) {
  return new Promise((res, rej) => {
    zlib.gzip(Buffer.from(xml, "utf8"), (err, buf) => {
      if (err) return rej(err);
      res(buf.toString("base64"));
    });
  });
}

// ── Chama API do governo ─────────────────────────────────────
function chamarAPI(dpsXmlGZipB64, certOpts) {
  return new Promise((res, rej) => {
    const body = Buffer.from(JSON.stringify({ dpsXmlGZipB64 }), "utf8");
    const opts = {
      hostname: NFSE_HOST, port: 443, path: NFSE_PATH, method: "POST",
      headers: { "Content-Type": "application/json", "Content-Length": body.length, "Accept": "application/json" },
      pfx: certOpts.pfx, passphrase: certOpts.passphrase, rejectUnauthorized: true,
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
    if (j.chaveAcesso) return { ok: true, chaveAcesso: j.chaveAcesso, idDps: j.idDps, alertas: j.alertas };
    const msgs = (j.mensagens||j.erros||[]).map(m => m.mensagem||m.descricao||JSON.stringify(m)).join("; ");
    return { ok: false, erro: msgs || j.mensagem || body.slice(0,400) };
  } catch(e) {
    return { ok: false, erro: body.slice(0,400) };
  }
}

// ── DANFE ────────────────────────────────────────────────────
function extrairDadosXml(xml) {
  function get(tag) {
    var m = xml.match(new RegExp("<" + tag + "[^>]*>([\\s\\S]*?)<\\/" + tag + ">"));
    return m ? m[1].trim() : "";
  }
  function getIn(parent, tag) {
    var pm = xml.match(new RegExp("<" + parent + "[^>]*>([\\s\\S]*?)<\\/" + parent + ">"));
    if (!pm) return "";
    var m = pm[1].match(new RegExp("<" + tag + "[^>]*>([\\s\\S]*?)<\\/" + tag + ">"));
    return m ? m[1].trim() : "";
  }
  return {
    nNFSe:        get("nNFSe"),
    dhProc:       get("dhProc"),
    dCompet:      get("dCompet"),
    dhEmi:        get("dhEmi"),
    xDescServ:    get("xDescServ"),
    vServ:        getIn("vServPrest","vServ") || get("vServ"),
    cpfTomador:   getIn("toma","CPF") || getIn("toma","CNPJ"),
    xNomeTomador: getIn("toma","xNome"),
  };
}

function gerarDanfeHtml(dados, chave) {
  var fmtDT = function(v) { try { return new Date(v).toLocaleString("pt-BR",{timeZone:"America/Sao_Paulo"}); } catch(e){ return v||""; }};
  var fmtD  = function(v) { return v ? v.split("-").reverse().join("/") : ""; };
  var fmtV  = function(v) { return v ? Number(v).toLocaleString("pt-BR",{minimumFractionDigits:2}) : "0,00"; };
  var doc   = dados.cpfTomador||"";
  var docFmt= doc.length===11 ? doc.replace(/(\d{3})(\d{3})(\d{3})(\d{2})/,"$1.$2.$3-$4")
                               : doc.replace(/(\d{2})(\d{3})(\d{3})(\d{4})(\d{2})/,"$1.$2.$3/$4-$5");
  var S = function(s) { return String(s||"").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;"); };
  var css =
    "*{margin:0;padding:0;box-sizing:border-box}" +
    "body{font-family:Arial,Helvetica,sans-serif;font-size:11px;background:#fff;color:#222;padding:18px;max-width:820px;margin:0 auto}" +
    ".hdr{display:flex;border:2px solid #1a7a3c;border-radius:4px;overflow:hidden;margin-bottom:8px}" +
    ".hdr-left{background:#1a7a3c;color:#fff;padding:12px 14px;min-width:195px;display:flex;flex-direction:column;justify-content:center}" +
    ".hdr-left h1{font-size:12px;font-weight:bold;text-transform:uppercase;line-height:1.4}" +
    ".hdr-left p{font-size:8px;margin-top:2px;opacity:.9;line-height:1.5}" +
    ".hdr-mid{flex:1;padding:10px 14px;display:flex;flex-direction:column;justify-content:center;border-left:1px solid #ccc;border-right:1px solid #ccc}" +
    ".hdr-mid .t{font-size:12px;font-weight:bold;color:#1a7a3c;text-transform:uppercase}" +
    ".hdr-mid .s{font-size:8px;color:#666;margin-top:3px}" +
    ".hdr-right{background:#1a4fa0;color:#fff;padding:10px 14px;min-width:110px;display:flex;flex-direction:column;justify-content:center;align-items:center;text-align:center}" +
    ".hdr-right .lbl{font-size:8px;text-transform:uppercase;opacity:.8}" +
    ".hdr-right .num{font-size:26px;font-weight:bold;line-height:1.1}" +
    ".hdr-right .tag{font-size:8px;margin-top:3px;background:rgba(255,255,255,.2);padding:2px 6px;border-radius:8px}" +
    ".sec{border:1px solid #ddd;border-radius:3px;margin-bottom:6px;overflow:hidden}" +
    ".sec-t{background:#1a7a3c;color:#fff;font-size:8px;font-weight:bold;text-transform:uppercase;padding:4px 10px;letter-spacing:.4px}" +
    ".row{display:flex;flex-wrap:wrap;background:#f9f9f9}" +
    ".f{padding:5px 10px;flex:1;min-width:100px;border-right:1px solid #eee;border-bottom:1px solid #eee}" +
    ".f:last-child{border-right:none}" +
    ".f label{display:block;font-size:7.5px;color:#888;text-transform:uppercase;margin-bottom:1px}" +
    ".f span{font-size:10.5px;font-weight:bold;color:#222}" +
    ".f.w2{flex:2}.f.w3{flex:3}" +
    ".desc{padding:7px 10px;white-space:pre-wrap;line-height:1.6;font-size:10.5px;background:#f9f9f9;border-top:1px solid #eee}" +
    ".val-box{display:flex;align-items:stretch;background:#f9f9f9}" +
    ".val-left{flex:1;padding:8px 10px}" +
    ".val-right{background:#e8f5ee;border-left:2px solid #1a7a3c;padding:10px 18px;text-align:center;min-width:170px;display:flex;flex-direction:column;justify-content:center}" +
    ".val-right .vl{font-size:8px;color:#555;text-transform:uppercase}" +
    ".val-right .vm{font-size:22px;font-weight:bold;color:#1a7a3c}" +
    ".gar{background:#fff8e8;border:1px solid #e0a000;border-radius:3px;padding:9px 12px;margin-bottom:6px;font-size:9px;line-height:1.6;color:#5a3e00}" +
    ".gar strong{color:#7a5000}" +
    ".chv{background:#f5f5f5;border:1px solid #ddd;border-radius:3px;padding:6px 10px;font-family:Courier,monospace;font-size:7.5px;word-break:break-all;color:#444;text-align:center;margin-bottom:6px}" +
    ".foot{background:#1a7a3c;color:#fff;text-align:center;padding:5px;font-size:7.5px;border-radius:3px}" +
    ".pbtn{display:block;margin:14px auto 0;padding:10px 36px;font-size:13px;background:#1a4fa0;color:#fff;border:none;border-radius:6px;cursor:pointer;font-weight:bold}" +
    "@media print{.pbtn{display:none}body{padding:0}}";
  return "<!DOCTYPE html><html lang='pt-BR'><head><meta charset='UTF-8'>" +
    "<title>NFS-e " + S(dados.nNFSe||"") + "</title>" +
    "<style>" + css + "</style></head><body>" +

    "<div class='hdr'>" +
      "<div class='hdr-left'>" +
        "<h1>Reparo Eletro</h1>" +
        "<h1>Conserto de Eletrodom\xe9sticos</h1>" +
        "<p>CNPJ: 59.485.378/0001-75</p>" +
        "<p>IM: 16391680010</p>" +
        "<p>Rua Ouro Preto, 663 \u2014 Barro Preto</p>" +
        "<p>Belo Horizonte/MG \u2014 (31) 9785-6023</p>" +
      "</div>" +
      "<div class='hdr-mid'>" +
        "<div class='t'>Nota Fiscal de Servi\xe7o Eletr\xf4nica</div>" +
        "<div class='s'>Emiss\xe3o: " + fmtDT(dados.dhEmi||dados.dhProc) + "</div>" +
        "<div class='s'>Compet\xeancia: " + fmtD(dados.dCompet) + "</div>" +
      "</div>" +
      "<div class='hdr-right'>" +
        "<div class='lbl'>NFS-e N\xba</div>" +
        "<div class='num'>" + S(dados.nNFSe||"\u2014") + "</div>" +
        "<div class='tag'>NFS-e Nacional</div>" +
      "</div>" +
    "</div>" +

    "<div class='sec'>" +
      "<div class='sec-t'>Tomador do Servi\xe7o</div>" +
      "<div class='row'>" +
        "<div class='f w3'><label>Nome / Raz\xe3o Social</label><span>" + S(dados.xNomeTomador||"Consumidor Final") + "</span></div>" +
        "<div class='f'><label>CPF / CNPJ</label><span>" + S(docFmt||"\u2014") + "</span></div>" +
      "</div>" +
    "</div>" +

    "<div class='sec'>" +
      "<div class='sec-t'>Dados do Servi\xe7o</div>" +
      "<div class='row'>" +
        "<div class='f'><label>Compet\xeancia</label><span>" + fmtD(dados.dCompet) + "</span></div>" +
        "<div class='f w2'><label>C\xf3digo do Servi\xe7o (LC 116/2003)</label><span>14.02 \u2014 Manuten\xe7\xe3o de eletrodom\xe9sticos</span></div>" +
        "<div class='f'><label>Munic\xedpio de Incid\xeancia</label><span>Belo Horizonte / MG</span></div>" +
      "</div>" +
      "<div class='desc'><strong>Discrimina\xe7\xe3o:</strong><br>" + S(dados.xDescServ||"") + "</div>" +
    "</div>" +

    "<div class='sec'>" +
      "<div class='sec-t'>Valores</div>" +
      "<div class='val-box'>" +
        "<div class='val-left'>" +
          "<div class='row' style='background:transparent'>" +
            "<div class='f'><label>Tributa\xe7\xe3o ISSQN</label><span>Simples Nacional \u2014 Tribut\xe1vel</span></div>" +
            "<div class='f'><label>Al\xedquota ISS</label><span>2,00%</span></div>" +
            "<div class='f'><label>Regime Tribut\xe1rio</label><span>Simples Nacional</span></div>" +
          "</div>" +
        "</div>" +
        "<div class='val-right'>" +
          "<div class='vl'>Valor Total do Servi\xe7o</div>" +
          "<div class='vm'>R$ " + fmtV(dados.vServ) + "</div>" +
        "</div>" +
      "</div>" +
    "</div>" +

    "<div class='gar'>" +
      "<strong>Termo de Garantia \u2014 CDC Art. 26 e 27 (Lei 8.078/90)</strong><br>" +
      "O servi\xe7o prestado possui garantia contratual de <strong>90 (noventa) dias</strong> a partir da data de emiss\xe3o desta nota fiscal, " +
      "conforme disposto no Art. 26, II do C\xf3digo de Defesa do Consumidor. Aplica-se tamb\xe9m a garantia legal de 90 dias para servi\xe7os dur\xe1veis (Art. 26, I do CDC).<br>" +
      "A garantia cobre exclusivamente o defeito objeto do reparo realizado, n\xe3o abrangendo novos defeitos decorrentes de mau uso, quedas, " +
      "infiltra\xe7\xe3o de l\xedquidos, tens\xe3o el\xe9trica inadequada ou interven\xe7\xe3o de terceiros. " +
      "Para acionar a garantia, apresente esta nota fiscal e entre em contato: <strong>(31) 9785-6023</strong>." +
    "</div>" +

    "<div class='chv'><strong>Chave de Acesso:</strong> " + chave + "</div>" +

    "<div class='foot'>" +
      "Documento emitido eletronicamente conforme LC 116/2003 \u2014 NFS-e Padr\xe3o Nacional &nbsp;|&nbsp; " +
      "Consulte: <strong>nfse.gov.br/ConsultaNacional</strong>" +
    "</div>" +

    "<button class='pbtn' onclick='window.print()'>Imprimir / Salvar como PDF</button>" +
    "</body></html>";
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

async function buscarESalvarDanfe(chaveAcesso, certOpts) {
  try {
    const resp = await new Promise((resolve, reject) => {
      const opts = {
        hostname: NFSE_HOST, port: 443,
        path: "/SefinNacional/nfse/" + chaveAcesso, method: "GET",
        pfx: certOpts.pfx, passphrase: certOpts.passphrase, rejectUnauthorized: true,
        headers: { "Accept": "application/json" },
      };
      const req = https.request(opts, r => {
        const chunks = []; r.on("data",c=>chunks.push(c));
        r.on("end",()=>resolve({ status: r.statusCode, buf: Buffer.concat(chunks) }));
      });
      req.on("error", reject); req.end();
    });
    if (resp.status !== 200) return false;
    const json   = JSON.parse(resp.buf.toString("utf8"));
    const xmlBuf = zlib.gunzipSync(Buffer.from(json.nfseXmlGZipB64, "base64"));
    const xml    = xmlBuf.toString("utf8");
    const dados  = extrairDadosXml(xml);
    const danfeHtml = gerarDanfeHtml(dados, chaveAcesso);
    await fetch(UPSTASH_URL + "/pipeline", {
      method: "POST",
      headers: { Authorization: "Bearer " + UPSTASH_TOKEN, "Content-Type": "application/json" },
      body: JSON.stringify([
        ["SET",    "danfe:" + chaveAcesso, Buffer.from(danfeHtml).toString("base64")],
        ["EXPIRE", "danfe:" + chaveAcesso, 31536000],
      ]),
    });
    return true;
  } catch(e) {
    console.error("buscarESalvarDanfe:", e.message);
    return false;
  }
}

// ── HANDLER ──────────────────────────────────────────────────
module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin","*");
  res.setHeader("Access-Control-Allow-Methods","GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers","Content-Type");
  if (req.method === "OPTIONS") return res.status(200).end();
  const { action } = req.query;

  // GET status
  if (action === "status") {
    return res.status(200).json({
      ok: !!(process.env.NFSE_CERT_PFX && process.env.NFSE_CERT_SENHA),
      temCert: !!(process.env.NFSE_CERT_PFX),
      temSenha: !!(process.env.NFSE_CERT_SENHA),
      temIM: !!(IM_EMPRESA),
      cnpj: CNPJ_EMPRESA, im: IM_EMPRESA, homolog: NFSE_HOMOLOG,
    });
  }

  // GET danfe — serve DANFE do Redis
  if (action === "danfe") {
    const chave = (req.query.chave || "").trim();
    const nome  = (req.query.nome  || "danfe-" + chave.slice(-10)).trim();
    if (!chave) return res.status(400).json({ ok: false, error: "chave obrigatoria" });
    let certOpts;
    try { certOpts = loadCert(); } catch(e) { return res.status(400).json({ ok: false, error: e.message }); }
    async function getRedisDanfe() {
      const r = await fetch(UPSTASH_URL + "/get/danfe:" + chave, { headers: { Authorization: "Bearer " + UPSTASH_TOKEN } });
      const j = await r.json();
      return j.result ? Buffer.from(j.result, "base64") : null;
    }
    let buf = null;
    try { buf = await getRedisDanfe(); } catch(e) {}
    if (!buf) {
      try { await buscarESalvarDanfe(chave, certOpts); } catch(e) {}
      try { buf = await getRedisDanfe(); } catch(e) {}
    }
    if (buf) {
      const str = buf.toString("utf8");
      if (str.startsWith("<!DOCTYPE")) { res.setHeader("Content-Type","text/html; charset=utf-8"); return res.status(200).send(str); }
      if (buf[0] === 0x25 && buf[1] === 0x50) {
        res.setHeader("Content-Type","application/pdf");
        res.setHeader("Content-Disposition","inline; filename=\"" + nome + ".pdf\"");
        return res.status(200).send(buf);
      }
      res.setHeader("Content-Type","text/html; charset=utf-8");
      return res.status(200).send(str);
    }
    return res.status(503).json({ ok: false, error: "DANFE nao disponivel." });
  }

  // GET danfe-force — regenera DANFE do XML do governo
  if (action === "danfe-force") {
    const chave = (req.query.chave || "").trim();
    if (!chave) return res.status(400).json({ ok: false, error: "chave obrigatoria" });
    let certOpts;
    try { certOpts = loadCert(); } catch(e) { return res.status(200).json({ step:"cert", error:e.message }); }
    try {
      const resp = await new Promise((resolve, reject) => {
        const opts = { hostname: NFSE_HOST, port: 443, path: "/SefinNacional/nfse/" + chave, method: "GET",
          pfx: certOpts.pfx, passphrase: certOpts.passphrase, rejectUnauthorized: false,
          headers: { "Accept": "application/json" },
        };
        const req2 = https.request(opts, r => { const chunks = []; r.on("data",c=>chunks.push(c)); r.on("end",()=>resolve({status:r.statusCode,buf:Buffer.concat(chunks)})); });
        req2.on("error", reject); req2.end();
      });
      if (resp.status !== 200) return res.status(200).json({ step:"api", status:resp.status, body:resp.buf.slice(0,200).toString() });
      const json   = JSON.parse(resp.buf.toString("utf8"));
      const xmlBuf = zlib.gunzipSync(Buffer.from(json.nfseXmlGZipB64, "base64"));
      const xml    = xmlBuf.toString("utf8");
      const dados  = extrairDadosXml(xml);
      const danfeHtml = gerarDanfeHtml(dados, chave);
      await fetch(UPSTASH_URL + "/pipeline", {
        method:"POST", headers:{Authorization:"Bearer "+UPSTASH_TOKEN,"Content-Type":"application/json"},
        body: JSON.stringify([["SET","danfe:"+chave,Buffer.from(danfeHtml).toString("base64")],["EXPIRE","danfe:"+chave,31536000]]),
      });
      res.setHeader("Content-Type","text/html; charset=utf-8");
      return res.status(200).send(danfeHtml);
    } catch(e) { return res.status(200).json({ step:"error", error:e.message }); }
  }

  // POST danfe-store
  if (req.method === "POST" && action === "danfe-store") {
    const { chave, pdfBase64 } = req.body || {};
    if (!chave || !pdfBase64) return res.status(400).json({ ok: false, error: "chave e pdfBase64 obrigatorios" });
    try {
      await fetch(UPSTASH_URL + "/pipeline", {
        method:"POST", headers:{Authorization:"Bearer "+UPSTASH_TOKEN,"Content-Type":"application/json"},
        body: JSON.stringify([["SET","danfe:"+chave,pdfBase64],["EXPIRE","danfe:"+chave,31536000]]),
      });
      return res.status(200).json({ ok: true });
    } catch(e) { return res.status(500).json({ ok:false, error:e.message }); }
  }

  // GET test-sign
  if (action === "test-sign") {
    const result = { steps: [] };
    try {
      const certOpts = loadCert();
      result.steps.push("cert loaded: " + certOpts.pfx.length + " bytes");
      const p12Asn1 = forge.asn1.fromDer(forge.util.createBuffer(certOpts.pfx));
      const p12     = forge.pkcs12.pkcs12FromAsn1(p12Asn1, certOpts.passphrase);
      result.steps.push("pkcs12 loaded");
      const allBags = [...(p12.getBags({bagType:forge.pki.oids.pkcs8ShroudedKeyBag})[forge.pki.oids.pkcs8ShroudedKeyBag]||[]),
                       ...(p12.getBags({bagType:forge.pki.oids.keyBag})[forge.pki.oids.keyBag]||[])];
      if (!allBags.length) { result.steps.push("no key bags"); result.ok = false; return res.status(200).json(result); }
      const pem = forge.pki.privateKeyToPem(allBags[0].key);
      result.steps.push("privateKey: " + pem.slice(0,40));
      result.ok = true;
    } catch(e) { result.steps.push("error: " + e.message); result.ok = false; }
    return res.status(200).json(result);
  }

  // GET debug-xml
  if (action === "debug-xml") {
    try {
      let certOpts; try { certOpts = loadCert(); } catch(e) { certOpts = null; }
      const xml = montarDPS({ cpfcnpj:"12345678901", nome:"Cliente Teste", discriminacao:"Manutencao. Garantia 90 dias.", valor:"350.00", numDPS: genId(1) });
      const signed = certOpts ? await assinarXML(xml, certOpts.pfx, certOpts.passphrase) : xml;
      res.setHeader("Content-Type","text/xml");
      return res.status(200).send(signed);
    } catch(e) { return res.status(200).json({ ok:false, error:e.message }); }
  }

  // POST emitir
  if (req.method === "POST" && action === "emitir") {
    const { tomadorCpfCnpj, tomadorNome, discriminacao, valor } = req.body || {};
    if (!tomadorCpfCnpj || !valor) return res.status(400).json({ ok:false, error:"tomadorCpfCnpj e valor obrigatorios" });
    let certOpts;
    try { certOpts = loadCert(); } catch(e) { return res.status(400).json({ ok:false, error:e.message }); }
    try {
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
        await buscarESalvarDanfe(parsed.chaveAcesso, certOpts).catch(e => console.error("DANFE:", e.message));
        return res.status(200).json({ ok:true, chaveAcesso:parsed.chaveAcesso, idDps:parsed.idDps, alertas:parsed.alertas });
      } else {
        return res.status(200).json({ ok:false, error:parsed.erro, httpStatus:status, idDPS:numDPS, idLen:numDPS.length });
      }
    } catch(e) { return res.status(200).json({ ok:false, error:e.message }); }
  }

  return res.status(404).json({ ok:false, error:"Acao nao encontrada" });
};
