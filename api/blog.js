// api/blog.js — Sistema de Blog Reparo Eletro BH
export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin','*');
  res.setHeader('Access-Control-Allow-Methods','GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers','Content-Type');
  if (req.method==='OPTIONS') return res.status(200).end();

  const U=(process.env.UPSTASH_URL||'').replace(/['"]/g,'').trim();
  const T=(process.env.UPSTASH_TOKEN||'').replace(/['"]/g,'').trim();

  async function dbGet(k){
    try{
      const r=await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['GET',k]])});
      const j=await r.json();let v=j[0]?.result;
      if(!v)return null;
      try{let x=JSON.parse(v);if(typeof x==='string')x=JSON.parse(x);return x;}catch(e){return null;}
    }catch(e){return null;}
  }
  async function dbSet(k,v){
    await fetch(U+'/pipeline',{method:'POST',headers:{Authorization:'Bearer '+T,'Content-Type':'application/json'},body:JSON.stringify([['SET',k,JSON.stringify(v)]])});
  }

  const action=req.query.action||'';

  // Gemini API key: env var primeiro, depois Redis
  let AKEY=(process.env.GEMINI_API_KEY||'').trim();
  if (!AKEY) {
    const cfg = await dbGet('blog_config');
    AKEY = (cfg?.gemini_key||'').trim();
  }

  function slugify(t){
    return t.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g,'')
      .replace(/[^a-z0-9]+/g,'-').replace(/^-|-$/g,'').slice(0,80);
  }

  // ── LISTAR MODELOS DISPONÍVEIS ───────────────────────────────────────────
  if (action==='listar-modelos') {
    if (!AKEY) return res.status(200).json({ok:false,erro:'Chave não configurada'});
    try {
      const resp = await fetch(`https://generativelanguage.googleapis.com/v1beta/models?key=${AKEY}`);
      const data = await resp.json();
      if (data.error) return res.status(200).json({ok:false,erro:data.error.message,status:resp.status});
      const modelos = (data.models||[]).filter(m=>m.supportedGenerationMethods?.includes('generateContent')).map(m=>m.name);
      return res.status(200).json({ok:true,modelos});
    } catch(e) {
      return res.status(200).json({ok:false,erro:e.message});
    }
  }

  // ── TESTAR API GEMINI ────────────────────────────────────────────────────
  if (action==='testar-api') {
    if (!AKEY) return res.status(200).json({ok:false,erro:'Chave não configurada'});
    try {
      const resp = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/gemini-flash-lite-latest:generateContent?key=${AKEY}`,{
        method:'POST',
        headers:{'Content-Type':'application/json'},
        body:JSON.stringify({
          contents:[{parts:[{text:'Diga apenas: OK'}]}],
          generationConfig:{maxOutputTokens:10}
        })
      });
      const data = await resp.json();
      if (data.error) return res.status(200).json({ok:false,erro:data.error.message,detalhe:data.error,status:resp.status});
      const txt = data.candidates?.[0]?.content?.parts?.[0]?.text||'';
      return res.status(200).json({ok:true,resposta:txt,modelo:'gemini-1.5-flash'});
    } catch(e) {
      return res.status(200).json({ok:false,erro:e.message});
    }
  }

  // ── CHECK CONFIG ─────────────────────────────────────────────────────────
  if (action==='check-config') {
    const cfg = await dbGet('blog_config');
    const envKey = (process.env.GEMINI_API_KEY||'').trim();
    return res.status(200).json({
      ok:true,
      configurado:!!(envKey||cfg?.gemini_key),
      fonte: envKey?'env':(cfg?.gemini_key?'redis':'nenhuma')
    });
  }

  // ── SALVAR CONFIG ─────────────────────────────────────────────────────────
  if (req.method==='POST' && action==='salvar-config') {
    const {anthropic_key}=req.body||{};  // campo reutilizado para gemini_key
    if (!anthropic_key) return res.status(400).json({ok:false,error:'Chave obrigatória'});
    const cfg=(await dbGet('blog_config'))||{};
    cfg.gemini_key=anthropic_key.trim();
    await dbSet('blog_config',cfg);
    return res.status(200).json({ok:true});
  }

  // ── GERAR ─────────────────────────────────────────────────────────────────
  if (req.method==='POST' && action==='gerar') {
    if (!AKEY) {
      return res.status(200).json({ok:false,error:'Chave Anthropic não configurada. Clique em ⚙️ para configurar.'});
    }

    const padrao = await dbGet('blog_padrao')||{voz:'',exemplos:[]};
    const dataHoje = new Date().toLocaleDateString('pt-BR',{weekday:'long',year:'numeric',month:'long',day:'numeric',timeZone:'America/Sao_Paulo'});

    const categorias = [
      {id:'tv',          nome:'Televisão',           emoji:'📺', kw:'conserto TV BH'},
      {id:'microondas',  nome:'Microondas',           emoji:'📡', kw:'conserto microondas BH'},
      {id:'purificador', nome:'Purificador/Bebedouro',emoji:'💧', kw:'conserto purificador água BH'},
      {id:'geladeira',   nome:'Geladeira',            emoji:'🧊', kw:'conserto geladeira BH'},
      {id:'lavaseca',    nome:'Lava e Seca',          emoji:'👕', kw:'conserto lava e seca BH'},
    ];

    const sys = `Você é redator especialista em SEO local e brand awareness para a Reparo Eletro BH, empresa de conserto de eletrodomésticos em Belo Horizonte e RMBH.

════ EMPRESA ════
Nome: Reparo Eletro BH
Diferencial: +15 anos de mercado | +39.000 consertos realizados | Técnico vai até você
Área: BH, Contagem, Betim, Santa Luzia, Ribeirão das Neves, Ibirité, Nova Lima
Contato: WhatsApp (31) 9 9785-6023 | Seg–Sáb até 21h
Serviços: TV, Geladeira, Microondas, Lava e Seca, Purificador/Bebedouro

════ TIPO DO ARTIGO ════
Brand Awareness / Reconhecimento de marca.
O leitor aprende a resolver um problema → a assistência técnica qualificada é apresentada como solução natural.
NÃO é publicidade direta. É conteúdo educativo onde a Reparo Eletro BH surge como referência confiável.

════ TAMANHO E LEITURA ════
→ 1.500 a 2.000 palavras (sweet spot para SEO Google + AI Overviews)
→ Indicar "8 min de leitura" no post
→ Parágrafos curtos (3–4 linhas máximo)
→ TL;DR de 3 frases no início do post

════ ESTRUTURA OBRIGATÓRIA ════
1. GANCHO (150–200 palavras)
   Dor do leitor. Ex: "Sua TV apagou sozinha? Antes de comprar outra, leia isso."
   Criar identificação emocional com o problema.

2. CONTEXTO — Por que isso acontece? (200–300 palavras)
   Explicar tecnicamente de forma acessível.
   Listar 3–5 causas mais comuns com detalhes reais.

3. O QUE TENTAR EM CASA (200–250 palavras)
   2–3 dicas SEGURAS que o leitor pode fazer sozinho.
   Não ensinar a desmontar o aparelho.
   Empoderar o leitor → construir confiança.

4. QUANDO CHAMAR UM TÉCNICO (200–250 palavras)
   Listar sintomas que exigem profissional.
   Criar urgência: "Tentar sem experiência pode piorar e custar mais."

5. COMO ESCOLHER UMA BOA ASSISTÊNCIA TÉCNICA (200–250 palavras)
   Critérios objetivos: tempo de mercado, garantia, peça original, avaliações.
   Construir autoridade sem mencionar a Reparo Eletro ainda.

6. POR QUE A REPARO ELETRO BH (150–200 palavras)
   CTA SUAVE. Diferencial, não venda agressiva.
   "+15 anos", "+39.000 consertos", "técnico vai até você", "BH e toda RMBH"

7. FAQ (4 PERGUNTAS OBRIGATÓRIAS — crítico para AI Overviews)
   Respostas diretas e específicas com valores/prazos reais.

8. CTA FINAL
   Urgência + benefício + WhatsApp (31) 9 9785-6023.

════ SEO GOOGLE ════
→ H1 com keyword principal + cidade (ex: "Geladeira não gela em BH: o que fazer")
→ H2s com keywords secundárias (Belo Horizonte, RMBH, técnico, conserto)
→ Mencionar bairros e cidades da RMBH naturalmente no texto
→ 2–3 links internos para outros posts do blog e páginas /geladeira, /lava-e-seca
→ E-E-A-T: citar "15 anos de experiência" e "+39.000 consertos" no corpo do texto

════ SEO PARA IA (ChatGPT, Perplexity, AI Overview) ════
→ ISLAND TEST: cada parágrafo deve fazer sentido sozinho (sem "Isso...", "Eles..." sem contexto)
→ Dados específicos e factuals: valores estimados, prazos concretos
→ FAQ Schema com 4 perguntas reais que o cliente faz
→ Bloco de resposta direta nas primeiras 2 linhas do post
→ Formatos que IA adora: listas, passo a passo, Q&A

════ IMAGENS (incluir no HTML como comentários/placeholders) ════
→ [FEATURED IMAGE 1200×628px] — antes do primeiro parágrafo
→ [IMAGEM SEÇÃO] — após cada H2 principal (ilustrar o problema/solução)
→ [INFOGRÁFICO] — no meio do post (ex: "5 sinais que precisam de técnico")
→ [BANNER CTA] — antes do CTA final
→ Alt text de todas: "conserto [equipamento] Belo Horizonte Reparo Eletro BH"

════ TOM E VOZ ════
→ Especialista mas acessível. "Como um amigo técnico que sabe do assunto."
→ Direto ao ponto. Sem enrolação.
→ Urgência real, não fake: baseada em fatos (aparelho pode estragar mais se ignorado)
→ Empático: o leitor está frustrado/preocupado com o aparelho parado

${padrao.voz?'════ VOZ APROVADA PELO CLIENTE ════\n'+padrao.voz:''}
${padrao.exemplos?.length?'════ EXEMPLOS APROVADOS ════\n'+padrao.exemplos.slice(-2).join('\n---\n'):''}`;

    const posts=[];
    for (const cat of categorias) {
      try {
        const resp = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/gemini-flash-lite-latest:generateContent?key=${AKEY}`,{
          method:'POST',
          headers:{'Content-Type':'application/json'},
          body:JSON.stringify({
            contents:[{parts:[{text: sys + '\n\n---\n\n' + `Gere um post de blog para: ${cat.nome} ${cat.emoji}
Data: ${dataHoje}
Keyword principal: "${cat.kw}"

Responda APENAS JSON válido (sem markdown), neste formato exato:
{
  "titulo": "título SEO 60-70 chars com BH ou Belo Horizonte",
  "subtitulo": "subtítulo cativante",
  "slug": "url-amigavel-com-bh",
  "meta_descricao": "meta description 150-160 chars com BH e CTA",
  "keywords": ["${cat.kw}","${cat.nome.toLowerCase()} belo horizonte","assistencia tecnica bh","conserto domicilio bh","reparo ${cat.nome.toLowerCase()} rmbh"],
  "imagem_alt": "conserto ${cat.nome.toLowerCase()} Belo Horizonte Reparo Eletro BH",
  "imagem_query": "${cat.id} appliance repair technician",
  "corpo_html": "<h2 class='post-h2'>...</h2><p class='post-p'>...</p><ul class='post-ul'><li>...</li></ul><div class='post-cta'><h3>Seu ${cat.nome} com problema?</h3><p>Técnico vai até você em BH e RMBH. Seg–Sáb até 21h!</p><a href='#'>📱 Chamar Técnico Agora</a></div>",
  "resumo": "2 frases resumindo o post com menção a BH",
  "tempo_leitura": "6 min",
  "tags": ["${cat.nome.toLowerCase()}","conserto bh","eletrodoméstico","belo horizonte","rmbh"],
  "faqs": [
    {"q": "Quanto custa conserto de ${cat.nome.toLowerCase()} em BH?", "a": "resposta com valor estimado"},
    {"q": "Qual prazo para conserto em Belo Horizonte?", "a": "resposta com prazo real"},
    {"q": "Atendem toda a RMBH?", "a": "Sim! Atendemos BH, Contagem, Betim, Santa Luzia, Ribeirão das Neves, Ibirité e Nova Lima."},
    {"q": "Como chamar técnico?", "a": "WhatsApp (31) 9 9785-6023 ou clique no botão abaixo."}
  ]
}`}]}],
            generationConfig:{temperature:0.8,maxOutputTokens:2500}
          })
        });
        const data=await resp.json();
        if (data.error) throw new Error(data.error.message||JSON.stringify(data.error));
        const texto=data.candidates?.[0]?.content?.parts?.[0]?.text||'{}';
        let post;
        try{post=JSON.parse(texto.replace(/```json|```/g,'').trim());}
        catch(e){post={titulo:`${cat.nome} — ${dataHoje}`,corpo_html:'<p>Erro ao gerar conteúdo.</p>',status:'erro'};}
        post.categoria=cat.id;
        post.categoriaNome=cat.nome;
        post.emoji=cat.emoji;
        post.geradoEm=new Date().toISOString();
        post.status=post.status||'rascunho';
        post.slug=post.slug||slugify(post.titulo||cat.id+'-'+Date.now());
        posts.push(post);
      } catch(e) {
        posts.push({categoria:cat.id,categoriaNome:cat.nome,emoji:cat.emoji,
          titulo:'Erro: '+e.message,status:'erro',corpo_html:'<p>'+e.message+'</p>'});
      }
    }
    return res.status(200).json({ok:true,posts,geradoEm:new Date().toISOString()});
  }

  // ── PUBLICAR ──────────────────────────────────────────────────────────────
  if (req.method==='POST' && action==='publicar') {
    const post=req.body||{};
    if (!post.slug) return res.status(400).json({ok:false,error:'slug obrigatório'});
    const db=(await dbGet('blog_posts'))||{posts:[]};
    post.publicadoEm=new Date().toISOString();
    post.status='publicado';
    db.posts=(db.posts||[]).filter(p=>p.slug!==post.slug);
    db.posts.unshift(post);
    await dbSet('blog_posts',db);
    const an=(await dbGet('blog_analytics'))||{};
    if (!an[post.slug]) an[post.slug]={views:0,cta_clicks:0,origem:{}};
    await dbSet('blog_analytics',an);
    return res.status(200).json({ok:true,slug:post.slug});
  }

  // ── LISTAR ────────────────────────────────────────────────────────────────
  if (action==='listar') {
    const db=(await dbGet('blog_posts'))||{posts:[]};
    const an=(await dbGet('blog_analytics'))||{};
    const posts=(db.posts||[]).filter(p=>p.status==='publicado').map(p=>({
      slug:p.slug,titulo:p.titulo,subtitulo:p.subtitulo,resumo:p.resumo,
      categoria:p.categoria,categoriaNome:p.categoriaNome,emoji:p.emoji,
      publicadoEm:p.publicadoEm,tempo_leitura:p.tempo_leitura,tags:p.tags,
      imagem_query:p.imagem_query,imagem_alt:p.imagem_alt,
      views:an[p.slug]?.views||0,cta_clicks:an[p.slug]?.cta_clicks||0
    }));
    return res.status(200).json({ok:true,posts});
  }

  // ── POST INDIVIDUAL ───────────────────────────────────────────────────────
  if (action==='post') {
    const slug=req.query.slug;
    if (!slug) return res.status(400).json({ok:false,error:'slug obrigatório'});
    const db=(await dbGet('blog_posts'))||{posts:[]};
    const post=(db.posts||[]).find(p=>p.slug===slug&&p.status==='publicado');
    if (!post) return res.status(404).json({ok:false,error:'Post não encontrado'});
    return res.status(200).json({ok:true,post});
  }

  // ── VIEW ──────────────────────────────────────────────────────────────────
  if (req.method==='POST' && action==='view') {
    const {slug,referrer}=req.body||{};
    if (!slug) return res.status(200).json({ok:true});
    const an=(await dbGet('blog_analytics'))||{};
    if (!an[slug]) an[slug]={views:0,cta_clicks:0,origem:{}};
    an[slug].views=(an[slug].views||0)+1;
    if (referrer){try{const h=new URL(referrer).hostname;an[slug].origem[h]=(an[slug].origem[h]||0)+1;}catch(e){}}
    await dbSet('blog_analytics',an);
    return res.status(200).json({ok:true});
  }

  // ── CTA CLICK ─────────────────────────────────────────────────────────────
  if (req.method==='POST' && action==='cta') {
    const {slug}=req.body||{};
    if (!slug) return res.status(200).json({ok:true});
    const an=(await dbGet('blog_analytics'))||{};
    if (!an[slug]) an[slug]={views:0,cta_clicks:0,origem:{}};
    an[slug].cta_clicks=(an[slug].cta_clicks||0)+1;
    await dbSet('blog_analytics',an);
    return res.status(200).json({ok:true});
  }

  // ── MÉTRICAS ──────────────────────────────────────────────────────────────
  if (action==='metricas') {
    const an=(await dbGet('blog_analytics'))||{};
    const db=(await dbGet('blog_posts'))||{posts:[]};
    const posts=(db.posts||[]).filter(p=>p.status==='publicado');
    const total_views=Object.values(an).reduce((s,v)=>s+(v.views||0),0);
    const total_cta=Object.values(an).reduce((s,v)=>s+(v.cta_clicks||0),0);
    const por_post=posts.map(p=>({
      slug:p.slug,titulo:p.titulo,categoria:p.categoria,emoji:p.emoji,
      publicadoEm:p.publicadoEm,
      views:an[p.slug]?.views||0,cta_clicks:an[p.slug]?.cta_clicks||0,
      taxa_cta:an[p.slug]?.views?Math.round((an[p.slug].cta_clicks||0)*100/(an[p.slug].views||1))+'%':'0%',
      origem:an[p.slug]?.origem||{}
    })).sort((a,b)=>b.views-a.views);
    return res.status(200).json({ok:true,total_views,total_cta,por_post});
  }

  // ── CHAT ──────────────────────────────────────────────────────────────────
  if (req.method==='POST' && action==='chat') {
    if (!AKEY) return res.status(200).json({ok:false,error:'Chave Anthropic não configurada'});
    const {post_titulo,post_corpo,mensagem,historico}=req.body||{};
    const msgs=(historico||[]).map(m=>({role:m.role,content:m.content}));
    msgs.push({role:'user',content:mensagem});
    const chatPrompt = `Assistente de copywriting para o blog da Reparo Eletro BH. Post atual: "${post_titulo}". Sugira melhorias específicas, foco em SEO local BH e conversão.\n\n`+msgs.map(m=>m.role+': '+m.content).join('\n');
    const resp=await fetch(`https://generativelanguage.googleapis.com/v1beta/models/gemini-flash-lite-latest:generateContent?key=${AKEY}`,{
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({contents:[{parts:[{text:chatPrompt}]}],generationConfig:{maxOutputTokens:1000}})
    });
    const data=await resp.json();
    if (data.error) return res.status(200).json({ok:false,error:data.error.message});
    return res.status(200).json({ok:true,resposta:data.candidates?.[0]?.content?.parts?.[0]?.text||'Sem resposta'});
  }

  // ── SALVAR PADRÃO ─────────────────────────────────────────────────────────
  if (req.method==='POST' && action==='salvar-padrao') {
    const {voz,trecho}=req.body||{};
    const padrao=(await dbGet('blog_padrao'))||{voz:'',exemplos:[]};
    if (voz) padrao.voz=voz;
    if (trecho){padrao.exemplos=padrao.exemplos||[];padrao.exemplos.push(trecho);if(padrao.exemplos.length>10)padrao.exemplos=padrao.exemplos.slice(-10);}
    await dbSet('blog_padrao',padrao);
    return res.status(200).json({ok:true});
  }

  // ── RASCUNHOS ─────────────────────────────────────────────────────────────
  if (action==='rascunhos') {
    const db=(await dbGet('blog_posts'))||{posts:[]};
    return res.status(200).json({ok:true,posts:(db.posts||[]).filter(p=>p.status==='rascunho')});
  }

  return res.status(404).json({ok:false,error:'Ação não encontrada: '+action});
}
