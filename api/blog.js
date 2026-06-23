// api/blog.js — Sistema de Blog Reparo Eletro BH
// Redis key: blog_posts, blog_analytics, blog_padrao
export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin','*');
  res.setHeader('Access-Control-Allow-Methods','GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers','Content-Type');
  if (req.method==='OPTIONS') return res.status(200).end();

  const U=(process.env.UPSTASH_URL||'').replace(/['"]/g,'').trim();
  const T=(process.env.UPSTASH_TOKEN||'').replace(/['"]/g,'').trim();
  const AKEY=(process.env.ANTHROPIC_API_KEY||'').trim();

  async function dbGet(k){try{const r=await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});const j=await r.json();let v=j[0]?.result;if(!v)return null;try{let x=JSON.parse(v);if(typeof x==='string')x=JSON.parse(x);return x;}catch(e){return null;}}catch(e){return null;}}
  async function dbSet(k,v){await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});}

  const action=req.query.action||'';

  // ── Slugify helper ──────────────────────────────────────────────────────
  function slugify(t){return t.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g,'').replace(/[^a-z0-9]+/g,'-').replace(/^-|-$/g,'').slice(0,80);}

  // ── GERAR: pesquisa + geração dos 5 posts ──────────────────────────────
  if (req.method==='POST' && action==='gerar') {
    const { regenerar, padroes } = req.body || {};
    const padrao = await dbGet('blog_padrao') || { voz:'', exemplos:[] };
    const dataHoje = new Date().toLocaleDateString('pt-BR', {weekday:'long',year:'numeric',month:'long',day:'numeric',timeZone:'America/Sao_Paulo'});

    const categorias = [
      { id:'tv',          nome:'Televisão',       emoji:'📺', kw:'conserto TV BH' },
      { id:'microondas',  nome:'Microondas',       emoji:'📡', kw:'conserto microondas BH' },
      { id:'purificador', nome:'Purificador/Bebedouro', emoji:'💧', kw:'conserto purificador BH' },
      { id:'geladeira',   nome:'Geladeira',        emoji:'🧊', kw:'conserto geladeira BH' },
      { id:'lavaseca',    nome:'Lava e Seca',      emoji:'👕', kw:'conserto lava e seca BH' },
    ];

    const systemPrompt = `Você é um redator especialista em SEO local para a Reparo Eletro BH, empresa de conserto de eletrodomésticos em Belo Horizonte e região metropolitana (Contagem, Betim, Santa Luzia, Ribeirão das Neves, Ibirité, Nova Lima).

OBJETIVO PRINCIPAL: converter leitores em leads — cada post deve terminar com CTA forte para WhatsApp (31) 9 9785-6023.

EMPRESA: Reparo Eletro BH | 15 anos de mercado | 39.000+ consertos | Atendemos BH e toda RMBH | Segunda a sábado até 21h | Técnico vai até você

PADRÃO DOS POSTS:
- Tom: especialista mas acessível, como um amigo que entende do assunto
- Estrutura: Gancho (dor do leitor) → Contexto (por que acontece) → Solução (o que fazer) → CTA
- SEO: incluir cidade/bairros da RMBH naturalmente no texto
- Tamanho: 800-1200 palavras
- Incluir: H2 com palavras-chave, lista de sintomas, dicas práticas, urgência na CTA
- NUNCA dizer "procure um profissional qualificado" — dizer "chame a Reparo Eletro"

${padrao.voz ? 'VOZ/TOM APROVADO:\n'+padrao.voz : ''}
${padrao.exemplos?.length ? 'TRECHOS APROVADOS ANTERIORMENTE:\n'+padrao.exemplos.slice(-3).join('\n---\n') : ''}`;

    const posts = [];
    for (const cat of categorias) {
      try {
        const resp = await fetch('https://api.anthropic.com/v1/messages', {
          method:'POST',
          headers:{'Content-Type':'application/json','x-api-key':AKEY,'anthropic-version':'2023-06-01'},
          body: JSON.stringify({
            model:'claude-sonnet-4-6',
            max_tokens:2000,
            system: systemPrompt,
            messages:[{role:'user',content:`Gere um post completo de blog para a categoria: ${cat.nome} ${cat.emoji}

Data de hoje: ${dataHoje}
Palavra-chave principal: "${cat.kw}"

Responda APENAS com JSON válido, sem markdown, no formato:
{
  "titulo": "título SEO otimizado para BH (60-70 chars, mencionar cidade)",
  "subtitulo": "subtítulo cativante com benefício claro",
  "slug": "url-amigavel-com-bh-no-final",
  "meta_descricao": "descrição para Google (150-160 chars, incluir BH e CTA direto)",
  "keywords": ["conserto em bh","tecnico belo horizonte","assistencia tecnica rmbh","reparo eletrodomestico bh","conserto domicilio bh"],
  "imagem_alt": "conserto de eletrodoméstico em Belo Horizonte — Reparo Eletro BH",
  "imagem_query": "appliance repair technician home service",
  "corpo_html": "HTML completo do post com H2, parágrafos, listas e CTA final. Use: <h2 class=\"post-h2\">, <p class=\"post-p\">, <ul class=\"post-ul\">. CTA obrigatória: <div class=\"post-cta\"><h3>Seu aparelho com problema?</h3><p>Técnico vai até você em BH e toda RMBH. Seg–Sáb até 21h.</p><a href=\"#\">📱 Chamar Técnico Agora</a></div>. Mencione naturalmente: Belo Horizonte, Contagem, Betim, RMBH ao longo do texto.",
  "resumo": "resumo de 2 frases para o card no blog (incluir BH)",
  "tempo_leitura": "6 min",
  "tags": ["conserto bh","eletrodoméstico","belo horizonte","assistência técnica","rmbh"],
  "faqs": [
    {"q": "Quanto custa o conserto em BH?", "a": "valor estimado com contexto"},
    {"q": "Qual o prazo para atendimento em Belo Horizonte?", "a": "prazo real e diferencial"},
    {"q": "A Reparo Eletro atende em toda a RMBH?", "a": "Sim! Atendemos BH, Contagem, Betim, Santa Luzia, Ribeirão das Neves, Ibirité e Nova Lima."},
    {"q": "Como chamar um técnico?", "a": "resposta com CTA e número (31) 9 9785-6023"}
  ]
}`]