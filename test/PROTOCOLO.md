# 🛡️ Protocolo de Alterações TechFlow (pós-episódio das 997)

## Regra da casa
NENHUMA alteração sobe direto para produção. O fluxo:

1. **Desenvolver na branch `dev`** (não em `main`)
2. **Testar no harness**: `node test/harness.js` — precisa dar 🟢 VERDE
   - O harness simula o Redis em memória e roda os handlers REAIS
   - Cobre: duplicação (anti-997), corrida de escrita, perda de ações humanas,
     fantasmas, fusível anti-enxurrada, transições do bot
3. **Novos recursos = novos cenários**: toda feature nova ganha um teste no harness
4. **Janela de deploy**: uma vez ao dia (combinada com Pedro), merge dev→main
5. **Smoke test pós-deploy**: conferir as telas afetadas em produção

## As duas camadas de verificação

**1. Harness (`node test/harness.js`)** — comportamento. Redis simulado, handlers reais.
Cobre: almoxarifado, wa-bot e **precificação (cenário 7)** — tabela ADM completa,
multi-equipamento com desconto, e paridade logística × frente de loja (−10%).
O placar informa se rodou DENTRO ou FORA da janela comercial: fora dela, o
dedupe do bot (cenário 6) NÃO é testado — o verde é mais fraco.

**2. Auditoria estática (`node test/auditoria.js`)** — o repositório inteiro.
Cobre o que o harness não vê: sintaxe das 86 APIs (CJS e ESM), blocos JS das
90 telas, função chamada em on* que não existe no arquivo, phase×phaseId em
quem lê o pipe, divergência de preço logística×loja, cron sem rota.

**Regra da casa atualizada: os DOIS precisam estar verdes antes de qualquer push.**

Ao alterar a tabela de preço: mude o cenário 7 PRIMEIRO com os valores novos,
rode (tem que dar VERMELHO), depois altere o código nos DOIS arquivos
(logistica.js e frenteloja.js) até dar verde. Teste que nunca falhou não protege.

## Rodar os testes
    cd repo && node test/harness.js

## Histórico de bugs que o harness teria evitado/pegou
- 24/07: enxurrada 997 (dedupe com campo errado `criadaEm` vs `criadoEm`) — cenário 1
- 24/07: corrida sync × ações do usuário (fichas "voltando") — cenário 2
- 24/07: PEGO ANTES DE PRODUÇÃO na 1ª rodada do harness: mescla desfazia auto-conclusões — cenário 4
