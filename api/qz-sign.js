// api/qz-sign.js — Assina desafios do QZ Tray com chave privada RSA
import { createSign } from 'crypto';

const PRIVATE_KEY = process.env.QZ_PRIVATE_KEY || '';

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const request = req.query.request || (req.body && req.body.request) || '';
  if (!request) return res.status(400).json({ error: 'request obrigatorio' });
  if (!PRIVATE_KEY) return res.status(500).json({ error: 'QZ_PRIVATE_KEY nao configurada' });

  try {
    const sign = createSign('SHA512');
    sign.update(request);
    const signature = sign.sign(PRIVATE_KEY, 'base64');
    res.setHeader('Content-Type', 'text/plain');
    return res.status(200).send(signature);
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }
}
