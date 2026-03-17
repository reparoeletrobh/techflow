// Endpoint temporário para baixar e expor o XSD do governo
const https = require("https");
const zlib  = require("zlib");

module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin","*");
  
  // Download zip do governo
  const zipBuf = await new Promise((resolve, reject) => {
    https.get("https://www.gov.br/nfse/pt-br/biblioteca/documentacao-tecnica/documentacao-atual/nfse-esquemas_xsd-v1-01-20260209.zip", r => {
      const chunks = [];
      r.on("data", c => chunks.push(c));
      r.on("end", () => resolve(Buffer.concat(chunks)));
    }).on("error", reject);
  });

  // Parse ZIP e extrai tiposComplexos_v1.01.xsd
  const bytes = zipBuf;
  let content = "";
  for(let i = 0; i < bytes.length - 4; i++) {
    if(bytes[i]===0x50&&bytes[i+1]===0x4B&&bytes[i+2]===0x03&&bytes[i+3]===0x04) {
      const fnLen = bytes[i+26] | (bytes[i+27]<<8);
      const exLen = bytes[i+28] | (bytes[i+29]<<8);
      const fn    = bytes.slice(i+30, i+30+fnLen).toString("utf8");
      if(fn.includes("tiposComplexos_v1.01")) {
        const dataStart  = i+30+fnLen+exLen;
        const compSize   = bytes[i+18]|(bytes[i+19]<<8)|(bytes[i+20]<<16)|(bytes[i+21]<<24);
        const compressed = bytes.slice(dataStart, dataStart+compSize);
        content = zlib.inflateRawSync(compressed).toString("utf8");
        break;
      }
    }
  }
  if(!content) return res.status(404).json({ error: "XSD not found" });
  
  // Retorna trecho relevante do tribMun
  const idx = content.indexOf("tribMun");
  const section = content.slice(Math.max(0,idx-200), idx+2000);
  res.setHeader("Content-Type","text/plain");
  return res.status(200).send(section);
};
