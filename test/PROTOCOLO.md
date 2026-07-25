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

## Rodar os testes
    cd repo && node test/harness.js

## Histórico de bugs que o harness teria evitado/pegou
- 24/07: enxurrada 997 (dedupe com campo errado `criadaEm` vs `criadoEm`) — cenário 1
- 24/07: corrida sync × ações do usuário (fichas "voltando") — cenário 2
- 24/07: PEGO ANTES DE PRODUÇÃO na 1ª rodada do harness: mescla desfazia auto-conclusões — cenário 4
