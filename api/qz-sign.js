// api/qz-sign.js — Assina desafios do QZ Tray com chave privada RSA
import { createSign } from 'crypto';

function normalizarChave(raw) {
  // Remove espaços extras no início/fim
  raw = raw.trim();

  // Substituir \n literais (dois chars) por quebra real
  raw = raw.replace(/\\n/g, '\n');

  // Extrair apenas o conteúdo base64 entre os headers PEM
  const match = raw.match(/-----BEGIN PRIVATE KEY-----([\s\S]*?)-----END PRIVATE KEY-----/);
  if (!match) throw new Error('Formato de chave inválido — verifique QZ_PRIVATE_KEY no Vercel');

  // Limpar o conteúdo: remover tudo que não é base64
  const b64 = match[1].replace(/[^A-Za-z0-9+/=]/g, '');

  // Quebrar em linhas de 64 chars (formato PEM padrão)
  const linhas = b64.match(/.{1,64}/g).join('\n');

  return `-----BEGIN PRIVATE KEY-----\n${linhas}\n-----END PRIVATE KEY-----`;
}

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const request = req.query.request || (req.body && req.body.request) || '';
  if (!request) return res.status(400).json({ error: 'request obrigatorio' });

  const rawKey = process.env.QZ_PRIVATE_KEY || '';
  if (!rawKey) return res.status(500).json({ error: 'QZ_PRIVATE_KEY não configurada no Vercel' });

  try {
    const chave = normalizarChave(rawKey);
    const sign = createSign('SHA512');
    sign.update(request);
    const assinatura = sign.sign(chave, 'base64');
    res.setHeader('Content-Type', 'text/plain');
    return res.status(200).send(assinatura);
  } catch (e) {
    return res.status(500).json({ error: e.message, dica: 'Cole a chave privada completa no Vercel em QZ_PRIVATE_KEY' });
  }
}
