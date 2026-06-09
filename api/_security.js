// api/_security.js — Middleware de segurança compartilhado
// Prefixo _ = não é rota Vercel, apenas importado por outros arquivos

const ALLOWED_ORIGINS = [
  'https://reparoeletroadm.com',
  'https://www.reparoeletroadm.com',
];

const MAX_BODY_BYTES = 512 * 1024; // 512KB

// ── Verificar origem (CORS restrito) ──────────────────────────────────────────
function getAllowedOrigin(req) {
  const origin = req.headers['origin'] || '';
  const referer = req.headers['referer'] || '';
  // Em dev/Vercel preview aceita tudo; em prod, verifica
  if (!origin) return 'https://reparoeletroadm.com'; // server-to-server
  if (ALLOWED_ORIGINS.some(o => origin.startsWith(o))) return origin;
  if (ALLOWED_ORIGINS.some(o => referer.startsWith(o))) return referer.split('/').slice(0,3).join('/');
  return null; // origem não permitida
}

// ── Headers de segurança ──────────────────────────────────────────────────────
function setSecurityHeaders(req, res) {
  const allowedOrigin = getAllowedOrigin(req);
  res.setHeader('Access-Control-Allow-Origin', allowedOrigin || 'https://reparoeletroadm.com');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type,x-app-key');
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('X-Frame-Options', 'SAMEORIGIN');
  res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
}

// ── Verificar app key (proteção básica de endpoints internos) ─────────────────
function verificarAppKey(req) {
  const APP_KEY = process.env.APP_INTERNAL_KEY || '';
  if (!APP_KEY) return true; // se não configurada, não bloqueia (graceful degradation)
  const keyHeader = req.headers['x-app-key'] || req.query._k || '';
  return keyHeader === APP_KEY;
}

// ── Limitar tamanho do payload ────────────────────────────────────────────────
function verificarPayload(req) {
  const contentLength = parseInt(req.headers['content-length'] || '0');
  return contentLength <= MAX_BODY_BYTES;
}

// ── Validar assinatura HMAC do Mercado Pago ───────────────────────────────────
function verificarAssinaturaMP(req) {
  // Mercado Pago envia: x-signature: ts=xxx,v1=hash
  const xSig = req.headers['x-signature'] || '';
  const xReqId = req.headers['x-request-id'] || '';
  if (!xSig) return false;
  const MP_WEBHOOK_SECRET = process.env.MP_WEBHOOK_SECRET || '';
  if (!MP_WEBHOOK_SECRET) return true; // sem segredo configurado, aceita (não bloqueia)
  try {
    const crypto = require('crypto');
    const parts = xSig.split(',');
    const ts = (parts.find(p=>p.startsWith('ts='))||'').replace('ts=','');
    const v1 = (parts.find(p=>p.startsWith('v1='))||'').replace('v1=','');
    const manifest = `id:${req.query.id||''};request-id:${xReqId};ts:${ts};`;
    const expected = crypto.createHmac('sha256', MP_WEBHOOK_SECRET).update(manifest).digest('hex');
    return crypto.timingSafeEqual(Buffer.from(v1,'hex'), Buffer.from(expected,'hex'));
  } catch(e) { return false; }
}

// ── Middleware completo para APIs internas ────────────────────────────────────
function middlewareInterno(req, res) {
  setSecurityHeaders(req, res);
  if (req.method === 'OPTIONS') { res.status(200).end(); return false; }
  if (!verificarPayload(req)) {
    res.status(413).json({ok:false,error:'Payload muito grande (máx 512KB)'});
    return false;
  }
  return true; // prosseguir
}

module.exports = { setSecurityHeaders, verificarAppKey, verificarPayload, verificarAssinaturaMP, middlewareInterno, getAllowedOrigin };
