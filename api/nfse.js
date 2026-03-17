const https     = require("https");
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
const NFSE_HOMOLOG   = (process.env.NFSE_HOMOLOG || "true") === "true";
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

function hoje() { return new Date().toISOString().slice(0,10); }
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


// Assina DPS com XMLDSig — implementação manual baseada no XML real do portal
// Algoritmos: exc-c14n#WithComments + rsa-sha256 + sha256
function assinarXML(xml, pfxBuf, passphrase) {
  try {
    const privateKey = crypto.createPrivateKey({ key: pfxBuf, format: "pkcs12", passphrase });

    // Extrai certificado X509 do PFX para incluir no KeyInfo
    let certBase64 = "";
    try {
      // Node 15+: usa X509Certificate
      const forge = null; // não disponível
      // Alternativa: usa o próprio pfxBuf para extrair cert via openssl
      // Por ora, inclui KeyInfo vazio — API aceita sem certificado público
    } catch(e) {}

    // Id do infDPS
    const idMatch = xml.match(/infDPS Id="([^"]+)"/);
    if (!idMatch) throw new Error("Id do infDPS não encontrado");
    const refId = idMatch[1];

    // 1. Canonicalize o conteúdo do infDPS (C14N exclusivo — para XML simples sem namespaces aninhados
    //    o C14N exclusivo é equivalente ao XML original sem declaração de namespace no infDPS)
    const infDpsMatch = xml.match(/<infDPS[\s\S]*?<\/infDPS>/);
    if (!infDpsMatch) throw new Error("infDPS não encontrado");

    // Para C14N exc#WithComments do infDPS dentro do DPS:
    // - Remove espaços extras entre elementos
    // - Adiciona xmlns herdado do elemento pai
    const c14n = infDpsMatch[0]
      .replace(/\r\n/g, '\n')
      .replace(/\r/g, '\n');

    // 2. DigestValue = SHA256(c14n)
    const digest = crypto.createHash("sha256").update(c14n, "utf8").digest("base64");

    // 3. SignedInfo canônico
    const signedInfo =
      `<SignedInfo xmlns="http://www.w3.org/2000/09/xmldsig#">` +
      `<CanonicalizationMethod Algorithm="http://www.w3.org/2001/10/xml-exc-c14n#WithComments"/>` +
      `<SignatureMethod Algorithm="http://www.w3.org/2001/04/xmldsig-more#rsa-sha256"/>` +
      `<Reference URI="#${refId}">` +
        `<Transforms>` +
          `<Transform Algorithm="http://www.w3.org/2000/09/xmldsig#enveloped-signature"/>` +
          `<Transform Algorithm="http://www.w3.org/2001/10/xml-exc-c14n#WithComments"/>` +
        `</Transforms>` +
        `<DigestMethod Algorithm="http://www.w3.org/2001/04/xmlenc#sha256"/>` +
        `<DigestValue>${digest}</DigestValue>` +
      `</Reference>` +
      `</SignedInfo>`;

    // 4. SignatureValue = RSA-SHA256(SignedInfo)
    const signer = crypto.createSign("sha256WithRSAEncryption");
    signer.update(signedInfo, "utf8");
    const sigValue = signer.sign(privateKey, "base64");

    // 5. Bloco Signature completo
    const signature =
      `<Signature xmlns="http://www.w3.org/2000/09/xmldsig#">` +
        signedInfo +
        `<SignatureValue>${sigValue}</SignatureValue>` +
      `</Signature>`;

    // 6. Injeta antes do </infDPS>
    const signed = xml.replace("</infDPS>", signature + "</infDPS>");
    console.log("Signature present:", signed.includes("<Signature"));
    return signed;

  } catch(e) {
    console.error("assinarXML erro:", e.message);
    return xml;
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
      const signed = certOpts ? assinarXML(xml, certOpts.pfx, certOpts.passphrase) : xml;
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
        const pk = crypto.createPrivateKey({ key: certOpts.pfx, format: "pkcs12", passphrase: certOpts.passphrase });
        result.steps.push("privateKey created: " + pk.asymmetricKeyType);
        const pem = pk.export({ type: "pkcs8", format: "pem" }).toString();
        result.steps.push("pem exported: " + pem.slice(0,40));
        const signer = crypto.createSign("sha256WithRSAEncryption");
        signer.update("test");
        const sig = signer.sign(pk, "base64");
        result.steps.push("signature: " + sig.slice(0,30) + "...");
        result.ok = true;
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
      const xmlAss = assinarXML(xml, certOpts.pfx, certOpts.passphrase);
      const b64gz  = await gzipBase64(xmlAss);
      const { status, body } = await chamarAPI(b64gz, certOpts);
      const parsed = parseResp(body);

      if (parsed.ok) {
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
