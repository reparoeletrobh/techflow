// api/security-status.js — Status das configurações de segurança
// Acesso restrito: só com APP_INTERNAL_KEY
module.exports = async function(req,res){
  const key = req.headers['x-app-key'] || req.query._k || '';
  const APP_KEY = process.env.APP_INTERNAL_KEY || '';
  if (!APP_KEY || key !== APP_KEY) {
    return res.status(401).json({ok:false,error:'Não autorizado'});
  }
  const checks = [
    { item:'ADM_USER configurado',      ok: !!process.env.ADM_USER,            critico:true  },
    { item:'ADM_PASS configurado',      ok: !!process.env.ADM_PASS,            critico:true  },
    { item:'TOKEN_SECRET configurado',  ok: !!process.env.TOKEN_SECRET,        critico:false },
    { item:'MP_WEBHOOK_SECRET config.', ok: !!process.env.MP_WEBHOOK_SECRET,   critico:true  },
    { item:'RESEND_WEBHOOK_SECRET',     ok: !!process.env.RESEND_WEBHOOK_SECRET,critico:false },
    { item:'APP_INTERNAL_KEY config.',  ok: !!process.env.APP_INTERNAL_KEY,    critico:false },
    { item:'UPSTASH_URL configurado',   ok: !!process.env.UPSTASH_URL,         critico:true  },
    { item:'UPSTASH_TOKEN configurado', ok: !!process.env.UPSTASH_TOKEN,       critico:true  },
    { item:'RESEND_API_KEY configurado',ok: !!process.env.RESEND_API_KEY,      critico:true  },
    { item:'MP_ACCESS_TOKEN config.',   ok: !!process.env.MP_ACCESS_TOKEN,     critico:true  },
  ];
  const falhos   = checks.filter(c=>!c.ok);
  const criticos = falhos.filter(c=>c.critico);
  return res.status(200).json({
    ok: criticos.length === 0,
    score: Math.round((checks.filter(c=>c.ok).length/checks.length)*100),
    criticos: criticos.length,
    checks,
    resumo: criticos.length===0 ? '✅ Todas as configs críticas OK' : `⚠️ ${criticos.length} config(s) crítica(s) faltando`
  });
};
