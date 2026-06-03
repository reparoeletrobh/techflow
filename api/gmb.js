// api/gmb.js — Google Business Profile API integration
const UPSTASH_URL   = (process.env.UPSTASH_URL   || '').replace(/['"]/g,'').trim();
const UPSTASH_TOKEN = (process.env.UPSTASH_TOKEN || '').replace(/['"]/g,'').trim();
const CLIENT_ID     = (process.env.GMB_CLIENT_ID     || '').trim();
const CLIENT_SECRET = (process.env.GMB_CLIENT_SECRET || '').trim();
const ACCOUNT_ID    = (process.env.GMB_ACCOUNT_ID    || '').trim();
const REDIRECT_URI  = 'https://reparoeletroadm.com/api/gmb?action=oauth-callback';
const TOKEN_KEY     = 'gmb_oauth_token';
const CACHE_KEY     = 'gmb_reviews_cache';
const SCOPE         = 'https://www.googleapis.com/auth/business.manage';

async function dbGet(key) {
  try {
    const r = await fetch(UPSTASH_URL+'/pipeline',{method:'POST',
      headers:{Authorization:'Bearer '+UPSTASH_TOKEN,'Content-Type':'application/json'},
      body:JSON.stringify([['GET',key]])});
    const j = await r.json();
    const v = j[0]?.result;
    if (!v) return null;
    let x = JSON.parse(v);
    if (typeof x === 'string') x = JSON.parse(x);
    return x;
  } catch(e) { return null; }
}

async function dbSet(key, val) {
  await fetch(UPSTASH_URL+'/pipeline',{method:'POST',
    headers:{Authorization:'Bearer '+UPSTASH_TOKEN,'Content-Type':'application/json'},
    body:JSON.stringify([['SET',key,JSON.stringify(val)]])});
}

// Refresh access token usando o refresh_token
async function refreshToken(tokenData) {
  const r = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: {'Content-Type':'application/x-www-form-urlencoded'},
    body: new URLSearchParams({
      client_id:     CLIENT_ID,
      client_secret: CLIENT_SECRET,
      refresh_token: tokenData.refresh_token,
      grant_type:    'refresh_token',
    }),
  });
  const j = await r.json();
  if (j.error) throw new Error('Refresh token falhou: ' + j.error_description);
  const updated = { ...tokenData, access_token: j.access_token, expires_at: Date.now() + (j.expires_in * 1000) };
  await dbSet(TOKEN_KEY, updated);
  return updated;
}

// Obter access token válido (atualiza se expirado)
async function getAccessToken() {
  let token = await dbGet(TOKEN_KEY);
  if (!token) throw new Error('Não autorizado — acesse /api/gmb?action=auth-url para autorizar');
  if (Date.now() > (token.expires_at - 60000)) token = await refreshToken(token);
  return token.access_token;
}

// Chamada autenticada à API do GMB
async function gmbFetch(path, method, body) {
  const accessToken = await getAccessToken();
  const base = 'https://mybusiness.googleapis.com/v4';
  const r = await fetch(base + path, {
    method: method || 'GET',
    headers: {
      'Authorization': 'Bearer ' + accessToken,
      'Content-Type': 'application/json',
    },
    body: body ? JSON.stringify(body) : undefined,
  });
  const j = await r.json();
  if (j.error) throw new Error(j.error.message || JSON.stringify(j.error));
  return j;
}

// Detectar location ID automaticamente
async function getLocationName() {
  const accId = ACCOUNT_ID || (await gmbFetch('/accounts')).accounts?.[0]?.name;
  const locs = await gmbFetch('/'+accId+'/locations?readMask=name,title');
  return locs.locations?.[0]?.name; // ex: accounts/XXX/locations/YYY
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin','*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const action = req.query.action || '';

  // ── GET auth-url — gera link OAuth para autorizar ──────────────────────────
  if (action === 'auth-url') {
    if (!CLIENT_ID) return res.status(400).json({ ok:false, error:'GMB_CLIENT_ID não configurado no Vercel' });
    const url = 'https://accounts.google.com/o/oauth2/v2/auth?' + new URLSearchParams({
      client_id:     CLIENT_ID,
      redirect_uri:  REDIRECT_URI,
      response_type: 'code',
      scope:         SCOPE,
      access_type:   'offline',
      prompt:        'consent',
    });
    return res.status(200).json({ ok:true, authUrl: url, instrucao: 'Abra o authUrl no navegador, autorize e aguarde o redirect' });
  }

  // ── GET oauth-callback — recebe o código e troca por tokens ────────────────
  if (action === 'oauth-callback') {
    const code = req.query.code;
    if (!code) return res.status(400).send('Código não recebido');
    try {
      const r = await fetch('https://oauth2.googleapis.com/token', {
        method: 'POST',
        headers: {'Content-Type':'application/x-www-form-urlencoded'},
        body: new URLSearchParams({ code, client_id:CLIENT_ID, client_secret:CLIENT_SECRET, redirect_uri:REDIRECT_URI, grant_type:'authorization_code' }),
      });
      const j = await r.json();
      if (j.error) return res.status(400).send('Erro OAuth: ' + j.error_description);
      await dbSet(TOKEN_KEY, { access_token:j.access_token, refresh_token:j.refresh_token, expires_at: Date.now()+(j.expires_in*1000) });
      return res.redirect('/gmb.html?authorized=1');
    } catch(e) { return res.status(500).send('Erro: ' + e.message); }
  }

  // ── GET status — verifica se está autorizado ───────────────────────────────
  if (action === 'status') {
    const token = await dbGet(TOKEN_KEY);
    if (!token) return res.status(200).json({ ok:true, autorizado:false });
    const expira = new Date(token.expires_at).toLocaleString('pt-BR',{timeZone:'America/Sao_Paulo'});
    return res.status(200).json({ ok:true, autorizado:true, tokenExpira:expira, temRefresh:!!token.refresh_token });
  }

  // ── GET reviews — lista avaliações ────────────────────────────────────────
  if (action === 'reviews') {
    try {
      const cache = await dbGet(CACHE_KEY);
      const forceRefresh = req.query.refresh === '1';
      if (cache && !forceRefresh && (Date.now() - cache.cachedAt) < 5*60*1000) {
        return res.status(200).json({ ok:true, ...cache, fromCache:true });
      }
      const locName = await getLocationName();
      const data = await gmbFetch('/'+locName+'/reviews?pageSize=50&orderBy=updateTime+desc');
      const reviews = (data.reviews || []).map(function(r) {
        return {
          name:         r.name,
          reviewId:     r.reviewId,
          reviewer:     r.reviewer?.displayName || 'Anônimo',
          rating:       r.starRating, // ONE TWO THREE FOUR FIVE
          stars:        {'ONE':1,'TWO':2,'THREE':3,'FOUR':4,'FIVE':5}[r.starRating] || 0,
          comment:      r.comment || '',
          publishedAt:  r.createTime,
          updatedAt:    r.updateTime,
          reply:        r.reviewReply ? { comment:r.reviewReply.comment, updatedAt:r.reviewReply.updateTime } : null,
        };
      });
      const result = { reviews, total:reviews.length, semResposta:reviews.filter(function(r){return!r.reply;}).length, cachedAt:Date.now() };
      await dbSet(CACHE_KEY, result);
      return res.status(200).json({ ok:true, ...result, fromCache:false });
    } catch(e) { return res.status(200).json({ ok:false, error:e.message }); }
  }

  // ── POST reply — responder avaliação ──────────────────────────────────────
  if (req.method === 'POST' && action === 'reply') {
    const { reviewName, comment } = req.body || {};
    if (!reviewName || !comment) return res.status(400).json({ ok:false, error:'reviewName e comment obrigatórios' });
    try {
      await gmbFetch('/'+reviewName+'/reply', 'PUT', { comment });
      await dbSet(CACHE_KEY, null); // invalidar cache
      return res.status(200).json({ ok:true, msg:'Resposta publicada' });
    } catch(e) { return res.status(200).json({ ok:false, error:e.message }); }
  }

  // ── DELETE reply — deletar resposta ───────────────────────────────────────
  if (req.method === 'DELETE' && action === 'delete-reply') {
    const { reviewName } = req.body || {};
    if (!reviewName) return res.status(400).json({ ok:false, error:'reviewName obrigatório' });
    try {
      await gmbFetch('/'+reviewName+'/reply', 'DELETE');
      await dbSet(CACHE_KEY, null);
      return res.status(200).json({ ok:true, msg:'Resposta removida' });
    } catch(e) { return res.status(200).json({ ok:false, error:e.message }); }
  }

  return res.status(404).json({ ok:false, error:'action não encontrada' });
};
