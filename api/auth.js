const crypto = require('crypto');

const ADM_USER   = process.env.ADM_USER   || 'admin';
const ADM_PASS   = process.env.ADM_PASS   || '123456';
const TOKEN_SECRET = process.env.TOKEN_SECRET || 'reparoeletro-adm-secret-2026';
const TOKEN_TTL  = 24 * 60 * 60 * 1000; // 24h em ms

function makeToken(username) {
  const expiry  = Date.now() + TOKEN_TTL;
  const payload = `${username}:${expiry}`;
  const sig     = crypto.createHmac('sha256', TOKEN_SECRET).update(payload).digest('hex');
  return { token: `${payload}.${sig}`, expiry };
}

function verifyToken(token) {
  if (!token) return null;
  const lastDot = token.lastIndexOf('.');
  if (lastDot < 0) return null;
  const payload = token.substring(0, lastDot);
  const sig     = token.substring(lastDot + 1);
  const expected = crypto.createHmac('sha256', TOKEN_SECRET).update(payload).digest('hex');
  if (sig !== expected) return null;
  const [username, expiry] = payload.split(':');
  if (Date.now() > parseInt(expiry)) return null;
  return { username, expiry: parseInt(expiry) };
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();

  // POST /api/auth — login
  if (req.method === 'POST') {
    const { username, password } = req.body || {};
    if (username === ADM_USER && password === ADM_PASS) {
      const { token, expiry } = makeToken(username);
      return res.status(200).json({ ok: true, token, expiry });
    }
    return res.status(401).json({ ok: false, error: 'Usuário ou senha inválidos' });
  }

  // GET /api/auth?action=verify
  if (req.method === 'GET' && req.query.action === 'verify') {
    const token = req.headers['x-auth-token'] || req.query.token;
    const data  = verifyToken(token);
    if (data) return res.status(200).json({ ok: true, ...data });
    return res.status(401).json({ ok: false, error: 'Token inválido ou expirado' });
  }

  return res.status(405).json({ ok: false, error: 'Method not allowed' });
};
