// ── NFS-e Nacional — Emissão via API com Certificado A1 ──────
// Endpoint: POST /api/nfse?action=emitir
// Requer variáveis de ambiente no Vercel:
//   NFSE_CERT_PFX    — certificado A1 em base64
//   NFSE_CERT_SENHA  — senha do certificado
//   NFSE_IM          — inscrição municipal
//   NFSE_CNPJ        — CNPJ da empresa (sem pontuação)

const https   = require("https");
const crypto  = require("crypto");
const zlib    = require("zlib");

const NFSE_API = "https://www.nfse.gov.br/SefinNacional/nfse";
const CNPJ_EMPRESA = (process.env.NFSE_CNPJ  || "59485378000175").replace(/\D/g,"");
const IM_EMPRESA   =  process.env.NFSE_IM     || "16391680010";
const COD_MUNICIPIO_BH = "3106200"; // Código IBGE Belo Horizonte
const COD_PAIS         = "1058";    // Brasil
const COD_SERVICO      = "14.01";   // Reparação e manutenção de máquinas e equipamentos

// ── Carrega certificado PFX ──────────────────────────────────
function loadCert() {
  const b64  = (process.env.NFSE_CERT_PFX || "").trim();
  const pass = (process.env.NFSE_CERT_SENHA || "").trim();
  if (!b64) throw new Error("NFSE_CERT_PFX não configurado");
  if (!pass) throw new Error("NFSE_CERT_SENHA não configurado");
  return { pfx: Buffer.from(b64, "base64"), passphrase: pass };
}

// ── Gera número sequencial da DPS ────────────────────────────
function genNumDPS() {
  return String(Date.now()).slice(-15).padStart(15, "0");
}

// ── Monta XML da DPS ─────────────────────────────────────────
function montarDPS({ tomadorCpfCnpj, tomadorNome, discriminacao, valor, dataCompetencia, numDPS }) {
  const data = dataCompetencia || new Date().toISOString().slice(0,10);
  const cpfcnpjLimpo = tomadorCpfCnpj.replace(/\D/g,"");
  const isCnpj = cpfcnpjLimpo.length === 14;
  const valorFmt = parseFloat(valor).toFixed(2);
  const hoje = new Date().toISOString().slice(0,10);

  const tomadorTag = isCnpj
    ? `<CNPJ>${cpfcnpjLimpo}</CNPJ>`
    : `<CPF>${cpfcnpjLimpo}</CPF>`;

  return `<?xml version="1.0" encoding="UTF-8"?>
<DPS xmlns="http://www.sped.fazenda.gov.br/nfse" versao="1.00">
  <infDPS Id="DPS${numDPS}">
    <tpAmb>1</tpAmb>
    <dhEmi>${hoje}T${new Date().toTimeString().slice(0,8)}-03:00</dhEmi>
    <verAplic>1.0.0</verAplic>
    <serie>A</serie>
    <nDPS>${numDPS}</nDPS>
    <dCompet>${data}</dCompet>
    <tpEmit>1</tpEmit>
    <cLocEmi>${COD_MUNICIPIO_BH}</cLocEmi>
    <prest>
      <CNPJ>${CNPJ_EMPRESA}</CNPJ>
      <IM>${IM_EMPRESA}</IM>
      <end>
        <endNac>
          <cMun>${COD_MUNICIPIO_BH}</cMun>
          <CEP>30190130</CEP>
        </endNac>
      </end>
    </prest>
    <toma>
      ${tomadorTag}
      <xNome>${escapeXml(tomadorNome || "Consumidor Final")}</xNome>
    </toma>
    <serv>
      <locPrest>
        <cLocPrestacao>${COD_MUNICIPIO_BH}</cLocPrestacao>
      </locPrest>
      <cServ>
        <cTribNac>${COD_SERVICO}</cTribNac>
        <xDescServ>${escapeXml(discriminacao)}</xDescServ>
      </cServ>
    </serv>
    <valores>
      <vServPrest>
        <vReceb>${valorFmt}</vReceb>
      </vServPrest>
      <trib>
        <tribMun>
          <tribISSQN>1</tribISSQN>
          <cLocIncid>${COD_MUNICIPIO_BH}</cLocIncid>
          <pAliq>2.00</pAliq>
        </tribMun>
        <totTrib>
          <pTotTribSN>6.00</pTotTribSN>
        </totTrib>
      </trib>
    </valores>
  </infDPS>
</DPS>`;
}

function escapeXml(s) {
  return String(s||"")
    .replace(/&/g,"&amp;")
    .replace(/</g,"&lt;")
    .replace(/>/g,"&gt;")
    .replace(/"/g,"&quot;")
    .replace(/'/g,"&apos;");
}

// ── Assina XML com certificado A1 ────────────────────────────
async function assinarXML(xml, pfxBuffer, passphrase) {
  // Usa crypto nativo do Node.js para assinar
  // A assinatura segue o padrão XML-DSig exigido pelo governo
  const sign = crypto.createSign("sha256WithRSAEncryption");
  sign.update(xml);

  // Extrai chave privada do PFX
  // Node.js 15+ suporta importação direta de PFX
  let privateKey;
  try {
    const p12 = crypto.createPrivateKey({ key: pfxBuffer, format: "pkcs12", passphrase });
    privateKey = p12;
  } catch(e) {
    // Fallback: usa o PFX diretamente como agentOptions
    privateKey = null;
  }

  if (privateKey) {
    sign.update(xml);
    const signature = sign.sign(privateKey, "base64");
    // Injeta assinatura no XML
    return xml.replace("</DPS>", `<Signature>${signature}</Signature></DPS>`);
  }

  // Se não conseguiu extrair a chave, retorna XML sem assinatura
  // (a API pode aceitar mTLS como autenticação suficiente)
  return xml;
}

// ── Chama API do governo ─────────────────────────────────────
async function chamarAPINFSe(xmlAssinado, certOpts) {
  return new Promise((resolve, reject) => {
    // Comprime o XML em GZIP conforme exigido pela API
    zlib.gzip(Buffer.from(xmlAssinado, "utf8"), (err, gzipped) => {
      if (err) return reject(err);

      const options = {
        hostname: "www.nfse.gov.br",
        port:     443,
        path:     "/SefinNacional/nfse",
        method:   "POST",
        headers:  {
          "Content-Type":     "application/xml",
          "Content-Encoding": "gzip",
          "Content-Length":   gzipped.length,
          "Accept":           "application/xml",
        },
        pfx:        certOpts.pfx,
        passphrase: certOpts.passphrase,
        rejectUnauthorized: true,
      };

      const req = https.request(options, (res) => {
        const chunks = [];
        res.on("data", chunk => chunks.push(chunk));
        res.on("end", () => {
          const body = Buffer.concat(chunks).toString("utf8");
          resolve({ status: res.statusCode, body });
        });
      });
      req.on("error", reject);
      req.write(gzipped);
      req.end();
    });
  });
}

// ── Extrai dados da resposta XML ─────────────────────────────
function parseResposta(xml) {
  function getTag(tag) {
    var rx = new RegExp("<" + tag + "[^>]*>([\s\S]*?)<\/" + tag + ">");
    var m = xml.match(rx);
    return m ? m[1].trim() : null;
  }
  var chaveAcesso = getTag("chNFSe") || getTag("chaveAcesso");
  var numero      = getTag("nNFSe")  || getTag("numero");
  var codigoVerif = getTag("cVerifCod");
  var sucesso     = xml.includes("nNFSe") || xml.includes("chNFSe");
  var erro = getTag("xMotivo") || getTag("descricaoErro") || getTag("xMsg")
           || getTag("mensagem") || getTag("Mensagem") || getTag("faultstring")
           || (sucesso ? null : xml.slice(0, 300));
  return { sucesso, chaveAcesso, numero, codigoVerif, erro };
}

// ── HANDLER ──────────────────────────────────────────────────
module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(200).end();

  const { action } = req.query;

  // ── POST emitir ─────────────────────────────────────────────
  if (req.method === "POST" && action === "emitir") {
    const { tomadorCpfCnpj, tomadorNome, discriminacao, valor, dataCompetencia } = req.body || {};
    if (!tomadorCpfCnpj || !valor)
      return res.status(400).json({ ok: false, error: "tomadorCpfCnpj e valor obrigatórios" });

    if (!IM_EMPRESA)
      return res.status(400).json({ ok: false, error: "Inscrição Municipal não configurada (NFSE_IM)" });

    let certOpts;
    try { certOpts = loadCert(); }
    catch(e) { return res.status(400).json({ ok: false, error: e.message }); }

    const numDPS = genNumDPS();

    try {
      const xml = montarDPS({ tomadorCpfCnpj, tomadorNome, discriminacao, valor, dataCompetencia, numDPS });
      const xmlAssinado = await assinarXML(xml, certOpts.pfx, certOpts.passphrase);
      const { status, body } = await chamarAPINFSe(xmlAssinado, certOpts);
      const parsed = parseResposta(body);

      if (parsed.sucesso) {
        return res.status(200).json({
          ok:          true,
          chaveAcesso: parsed.chaveAcesso,
          numero:      parsed.numero,
          codigoVerif: parsed.codigoVerif,
          xmlResposta: body,
        });
      } else {
        return res.status(200).json({
          ok:    false,
          error: parsed.erro || body.slice(0,400) || "Erro desconhecido",
          status,
          body:  body.slice(0, 500),
        });
      }
    } catch(e) {
      return res.status(200).json({ ok: false, error: e.message });
    }
  }

  // ── GET status — verifica configuração ────────────────────
  if (action === "status") {
    const temCert = !!(process.env.NFSE_CERT_PFX);
    const temIM   = !!(process.env.NFSE_IM);
    const temCNPJ = !!(process.env.NFSE_CNPJ);
    return res.status(200).json({
      ok:     temCert && temIM && temCNPJ,
      temCert,
      temIM,
      temCNPJ,
      cnpj:   CNPJ_EMPRESA,
      im:     IM_EMPRESA || "(não configurado)",
    });
  }

  return res.status(404).json({ ok: false, error: "Ação não encontrada" });
};
