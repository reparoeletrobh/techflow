// api/qz-sign.js — Assina desafios do QZ Tray com chave privada RSA
import { createSign, createPrivateKey } from 'crypto';

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const request = req.query.request || (req.body && req.body.request) || '';
  if (!request) return res.status(400).json({ error: 'request obrigatorio' });

  let rawKey = process.env.QZ_PRIVATE_KEY || '';
  if (!rawKey) return res.status(500).json({ error: 'QZ_PRIVATE_KEY nao configurada' });

  try {
    // Normalizar chave: repor quebras de linha se foram perdidas no Vercel
    rawKey = rawKey.replace(/\\n/g, '\n');
    if (!rawKey.includes('\n')) {
      // Chave colada sem quebras — reconstruir formato PEM
      rawKey = rawKey
        .replace('-----BEGIN PRIVATE KEY-----', '-----BEGIN PRIVATE KEY-----\n')
        .replace('-----END PRIVATE KEY-----', '\n-----END PRIVATE KEY-----')
        .replace(/(.{64})/g, '$1\n');
    }

    const privateKey = createPrivateKey({ key: rawKey, format: 'pem' });
    const sign = createSign('SHA512');
    sign.update(request);
    const signature = sign.sign(privateKey, 'base64');

    res.setHeader('Content-Type', 'text/plain');
    return res.status(200).send(signature);
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }
}
