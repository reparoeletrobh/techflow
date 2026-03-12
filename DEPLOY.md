# 🚀 Como publicar o TechFlow na Vercel

## Passo 1 — Subir no GitHub

1. Acesse https://github.com/new
2. Crie um repositório chamado `techflow` (deixe **privado**)
3. Clique em **"uploading an existing file"**
4. Arraste os 3 arquivos/pasta:
   - `index.html`
   - `vercel.json`
   - pasta `api/` com o arquivo `pipefy.js`
5. Clique em **"Commit changes"**

## Passo 2 — Conectar ao Vercel

1. Acesse https://vercel.com e faça login
2. Clique em **"Add New Project"**
3. Selecione o repositório `techflow`
4. Clique em **"Deploy"**

## Passo 3 — Adicionar a API Key do Pipefy (OBRIGATÓRIO)

1. No painel do projeto na Vercel, clique em **"Settings"**
2. Vá em **"Environment Variables"**
3. Adicione a variável:
   - **Name:** `PIPEFY_TOKEN`
   - **Value:** `eyJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJQaXBlZnkiLCJpYXQiOjE3NzMyNDI2MzcsImp0aSI6ImJmMDNmNWEyLWJkNmMtNGM0MS1iNzdkLWUyYzBjYTllNWE1MCIsInN1YiI6MzA2NTg1NjU0LCJ1c2VyIjp7ImlkIjozMDY1ODU2NTQsImVtYWlsIjoicmVwYXJvZWxldHJvYmhAZ21haWwuY29tIn0sInVzZXJfdHlwZSI6ImF1dGhlbnRpY2F0ZWQifQ.tfviNEwov6CZx8V8xAN8U7OogUKmElj_KxqQTxU6FEcfQQdHGcjovNJOcezxxCNs-WwD3I9d5Ivu5fbF7FWAEA`
4. Clique em **"Save"**
5. Vá em **"Deployments"** e clique em **"Redeploy"**

## Pronto! 🎉

Seu dashboard estará disponível em:
`https://techflow-XXXX.vercel.app`

---

## O que o dashboard faz

- **Dashboard** — Carrega todas as OS diretamente do seu Pipe do Pipefy, organizadas por fase, com auto-refresh a cada 60 segundos
- **Mover OS** — Busca a OS pelo título ou ID e move para outra etapa
- **Nova OS** — Cria uma nova ordem de serviço direto no Pipefy

