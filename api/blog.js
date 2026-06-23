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
  "titulo": "título SEO otimizado (60-70 chars)",
  "subtitulo": "subtítulo cativante",
  "slug": "url-amigavel-do-post",
  "meta_descricao": "descrição para Google (150-160 chars)",
  "keywords": ["palavra1","palavra2","palavra3","palavra4","palavra5"],
  "imagem_alt": "texto alternativo da imagem",
  "imagem_query": "query em inglês para buscar imagem (ex: microwave appliance repair)",
  "corpo_html": "HTML completo do post com H2, parágrafos, listas e CTA final. Use classes: post-h2, post-p, post-ul, post-cta",
  "resumo": "resumo de 2 frases para o card no blog",
  "tempo_leitura": "5 min",
  "tags": ["tag1","tag2","tag3"]
}`}]
          })
        });
        const data = await resp.json();
        const texto = data.content?.[0]?.text || '{}';
        let post;
        try { post = JSON.parse(texto.replace(/```json|```/g,'').trim()); }
        catch(e) { post = { titulo: cat.nome+' — '+dataHoje, corpo_html:'<p>Erro ao gerar conteúdo</p>' }; }
        post.categoria = cat.id;
        post.categoriaNome = cat.nome;
        post.emoji = cat.emoji;
        post.geradoEm = new Date().toISOString();
        post.status = 'rascunho';
        post.slug = post.slug || slugify(post.titulo||cat.id+'-'+Date.now());
        posts.push(post);
      } catch(e) {
        posts.push({ categoria:cat.id, categoriaNome:cat.nome, emoji:cat.emoji, titulo:'Erro ao gerar: '+e.message, status:'erro' });
      }
    }
    return res.status(200).json({ ok:true, posts, geradoEm: new Date().toISOString() });
  }

  // ── PUBLICAR ────────────────────────────────────────────────────────────
  if (req.method==='POST' && action==='publicar') {
    const post = req.body || {};
    if (!post.slug) return res.status(400).json({ ok:false, error:'slug obrigatório' });
    const db = (await dbGet('blog_posts')) || { posts:[] };
    post.publicadoEm = new Date().toISOString();
    post.status = 'publicado';
    // Remover rascunho anterior do mesmo slug
    db.posts = (db.posts||[]).filter(p => p.slug !== post.slug);
    db.posts.unshift(post);
    await dbSet('blog_posts', db);
    // Inicializar analytics
    const an = (await dbGet('blog_analytics')) || {};
    if (!an[post.slug]) an[post.slug] = { views:0, cta_clicks:0, scroll_50:0, scroll_100:0, origem:{} };
    await dbSet('blog_analytics', an);
    return res.status(200).json({ ok:true, slug:post.slug });
  }

  // ── LISTAR posts publicados ──────────────────────────────────────────────
  if (req.method==='GET' && action==='listar') {
    const db = (await dbGet('blog_posts')) || { posts:[] };
    const an = (await dbGet('blog_analytics')) || {};
    const posts = (db.posts||[]).filter(p=>p.status==='publicado').map(p=>({
      slug:p.slug, titulo:p.titulo, subtitulo:p.subtitulo, resumo:p.resumo,
      categoria:p.categoria, categoriaNome:p.categoriaNome, emoji:p.emoji,
      publicadoEm:p.publicadoEm, tempo_leitura:p.tempo_leitura, tags:p.tags,
      imagem_query:p.imagem_query, imagem_alt:p.imagem_alt,
      views: an[p.slug]?.views||0, cta_clicks: an[p.slug]?.cta_clicks||0
    }));
    return res.status(200).json({ ok:true, posts });
  }

  // ── POST INDIVIDUAL ──────────────────────────────────────────────────────
  if (req.method==='GET' && action==='post') {
    const slug = req.query.slug;
    if (!slug) return res.status(400).json({ ok:false, error:'slug obrigatório' });
    const db = (await dbGet('blog_posts')) || { posts:[] };
    const post = (db.posts||[]).find(p=>p.slug===slug && p.status==='publicado');
    if (!post) return res.status(404).json({ ok:false, error:'Post não encontrado' });
    return res.status(200).json({ ok:true, post });
  }

  // ── VIEW: registrar visualização ─────────────────────────────────────────
  if (req.method==='POST' && action==='view') {
    const { slug, referrer } = req.body||{};
    if (!slug) return res.status(200).json({ ok:true });
    const an = (await dbGet('blog_analytics')) || {};
    if (!an[slug]) an[slug] = { views:0, cta_clicks:0, scroll_50:0, scroll_100:0, origem:{} };
    an[slug].views = (an[slug].views||0)+1;
    if (referrer) {
      try { const h=new URL(referrer).hostname; an[slug].origem[h]=(an[slug].origem[h]||0)+1; } catch(e){}
    }
    await dbSet('blog_analytics', an);
    return res.status(200).json({ ok:true });
  }

  // ── CTA CLICK ────────────────────────────────────────────────────────────
  if (req.method==='POST' && action==='cta') {
    const { slug } = req.body||{};
    if (!slug) return res.status(200).json({ ok:true });
    const an = (await dbGet('blog_analytics')) || {};
    if (!an[slug]) an[slug] = { views:0, cta_clicks:0, scroll_50:0, scroll_100:0, origem:{} };
    an[slug].cta_clicks = (an[slug].cta_clicks||0)+1;
    await dbSet('blog_analytics', an);
    return res.status(200).json({ ok:true });
  }

  // ── METRICAS ─────────────────────────────────────────────────────────────
  if (req.method==='GET' && action==='metricas') {
    const an = (await dbGet('blog_analytics')) || {};
    const db = (await dbGet('blog_posts')) || { posts:[] };
    const posts = (db.posts||[]).filter(p=>p.status==='publicado');
    const total_views = Object.values(an).reduce((s,v)=>s+(v.views||0),0);
    const total_cta   = Object.values(an).reduce((s,v)=>s+(v.cta_clicks||0),0);
    const por_post = posts.map(p=>({
      slug:p.slug, titulo:p.titulo, categoria:p.categoria, emoji:p.emoji,
      publicadoEm:p.publicadoEm,
      views:an[p.slug]?.views||0, cta_clicks:an[p.slug]?.cta_clicks||0,
      taxa_cta: an[p.slug]?.views ? Math.round((an[p.slug].cta_clicks||0)*100/(an[p.slug].views||1))+'%' : '0%',
      origem:an[p.slug]?.origem||{}
    })).sort((a,b)=>(b.views-a.views));
    return res.status(200).json({ ok:true, total_views, total_cta, por_post });
  }

  // ── CHAT: sugestões de melhoria por IA ───────────────────────────────────
  if (req.method==='POST' && action==='chat') {
    const { post_titulo, post_corpo, mensagem, historico } = req.body||{};
    const msgs = (historico||[]).map(m=>({role:m.role,content:m.content}));
    msgs.push({role:'user',content:mensagem});
    const resp = await fetch('https://api.anthropic.com/v1/messages',{
      method:'POST',
      headers:{'Content-Type':'application/json','x-api-key':AKEY,'anthropic-version':'2023-06-01'},
      body:JSON.stringify({
        model:'claude-sonnet-4-6', max_tokens:1000,
        system:`Você é um assistente especialista em copywriting e SEO para a Reparo Eletro BH.
Está analisando e sugerindo melhorias para o blog post: "${post_titulo}".
Seja objetivo e direto. Sugira melhorias específicas. Quando o usuário pedir para regenerar algo, forneça o HTML ou texto pronto.
Foco: conversão (leads para WhatsApp), SEO local BH, tom especialista.`,
        messages:msgs
      })
    });
    const data = await resp.json();
    const resposta = data.content?.[0]?.text||'Erro ao processar';
    return res.status(200).json({ ok:true, resposta });
  }

  // ── SALVAR PADRÃO: aprendizado ────────────────────────────────────────────
  if (req.method==='POST' && action==='salvar-padrao') {
    const { voz, trecho } = req.body||{};
    const padrao = (await dbGet('blog_padrao')) || { voz:'', exemplos:[] };
    if (voz) padrao.voz = voz;
    if (trecho) { padrao.exemplos = padrao.exemplos||[]; padrao.exemplos.push(trecho); if(padrao.exemplos.length>10) padrao.exemplos=padrao.exemplos.slice(-10); }
    await dbSet('blog_padrao', padrao);
    return res.status(200).json({ ok:true });
  }

  // ── RASCUNHOS (admin) ─────────────────────────────────────────────────────
  if (req.method==='GET' && action==='rascunhos') {
    const db = (await dbGet('blog_posts')) || { posts:[] };
    const posts = (db.posts||[]).filter(p=>p.status==='rascunho');
    return res.status(200).json({ ok:true, posts });
  }

  return res.status(404).json({ ok:false, error:'Ação não encontrada' });
}
