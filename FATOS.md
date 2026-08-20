# FATOS DO SISTEMA

Este arquivo existe porque erros de memória custaram tempo real: prazos ditos
errado, travas de horário procuradas no lugar errado, correções dadas como
concluídas sem confirmação. **Antes de afirmar qualquer número, data ou regra
deste documento, consulte-o.** Não é documentação — é a fonte da verdade sobre
o que é fato e o que precisa ser medido.

Se algo aqui divergir do sistema, o sistema está certo e este arquivo precisa
ser atualizado. Se algo não estiver aqui, **meça antes de afirmar.**

---

## CICLO DE CAMPANHAS

**Começa sábado 13h · termina sábado 11h da semana seguinte.**
Não é sexta. Não é domingo. O selo do ciclo é a data de INÍCIO, no formato
DDMMAAAA — o ciclo que começou em 15/08/2026 tem selo `15082026` e termina em
22/08/2026 às 11h.

**Verbas base por campanha, no momento da criação:**
- TV: R$ 108,75
- ADM (micro-ondas, purificador, adega, institucional): R$ 75,75

Essas são as verbas de criação. O Copiloto pode redistribuí-las quando
autorizado, e a partir daí a verba real é a que está na Meta — não a base.

**Teto de gasto do ciclo:** R$ 2.500 ADM + R$ 870 TV.

**A conta do ciclo fecha assim:**
`orçamento das ATIVAS + gasto realizado das PAUSADAS ≤ teto`
Não se soma o orçamento das pausadas: verba de campanha pausada não é gasto
nem está disponível. É número de configuração.

---

## JANELAS DE HORÁRIO

Todas em horário de Brasília (UTC−3).

| O quê | Dias úteis | Sábado | Domingo |
|---|---|---|---|
| Envio de orçamento e conserto finalizado (cron 3min) | 7h–18h | 7h–13h | não roda |
| Disparo de orçamentos pendentes (ação manual) | 8h–18h | 8h–12h | não roda |
| Pesquisa de satisfação | 8h–19h | 8h–19h | não roda |
| Régua de retirada | 10h | 10h | não roda |
| Régua de recuperação 7d | 10h, 13h, 14h, 17h | idem | não roda |
| Avisos de conflito | 8h–19h | 8h–13h | não roda |

**A loja fecha às 18h nos dias úteis e 13h no sábado.** Qualquer janela que
termine antes disso está cortando expediente — foi o que aconteceu com a trava
das 16h, que deixou dois dias de orçamentos sem aviso.

**Limpeza semanal:** domingo 23h59, move os finalizados em massa para o ERP.
Movimentação dessa janela NÃO é entrada real e não deve ser usada como data.

---

## ONDE CADA COISA É GRAVADA

**Orçamento** — o carimbo `orcamentoEm` é gravado quando o valor é lançado pela
primeira vez, nos QUATRO caminhos do botão Diagnóstico:
`frenteloja.js` · `logistica.js` · `tv-logistica.js` · `board.js`
Não é sobrescrito em reajuste. Cards antigos podem não ter o carimbo — nesses,
a data vem do histórico da fase.

**`movedAt` NÃO é data de nada.** Muda a cada movimentação. Usá-lo como data de
orçamento ou de entrada no ERP produz números errados.

**Entrada no ERP** — livro próprio `erp_entradas`, gravado de hora em hora.
O histórico do cartão não serve: a limpeza de domingo apaga a distinção.

**Fila da pesquisa de satisfação** — a fonte é a aba Google Meu Negócio
(`gmb_pendentes`), não o livro de entradas. Quem está parado lá é quem
recebeu o serviço e ainda não foi abordado.

**Exclusão da fila de ligação** — o botão da tela chama `fichas?action=excluir`,
não `prospeccao?action=excluir`. A proteção contra retorno depende do telefone
estar em `prospeccao_excluidos`.

---

## CRIAÇÃO DE CAMPANHA

**Nenhum criativo pode nascer sem título e sem corpo.** Anúncio mudo é vídeo
sem chamada e praticamente não converte — foi o que zerou "Tela lavada 508" e
"Led queimado 508", as duas melhores de TV, no meio do ciclo de 15/08.

O texto vem em três camadas, nesta ordem:
1. dicionário do defeito, pelo nome do arquivo do vídeo
2. texto do próprio modelo que está sendo duplicado
3. texto genérico da categoria

O retorno do `subir-agora` traz `SEM_TEXTO_NO_DICIONARIO` — vídeos cujo nome não
foi reconhecido, que valem entrar no dicionário — e `MUDOS`, conferência feita
na própria Meta logo após a criação. **`MUDOS` tem de vir vazio.**

---

## REGRAS DE NEGÓCIO QUE JÁ CUSTARAM ERRO

**Verba de campanha pausada não é liberada.** Não construir redistribuição
automática de "verba órfã" — isso já inflou o orçamento de ADM uma vez.

**Cada número de WhatsApp responde pelo próprio canal.** Cliente que escreveu
para o número antigo abriu janela naquele número; a resposta sai por ele.

**As 5 fases do funil não se pulam:** F1 orçamento → F2 desconto Pix →
F3 retirada no balcão → F4 troca → F5 compra do equipamento.

**Prazo de reforma e pintura: 3 a 7 dias.** Os 15 minutos do anúncio valem
para balcão com troca de peça simples.

**A descrição da ficha é o defeito de origem**, registrado antes de o
equipamento chegar à loja. Não descreve o estado atual.

**Rotinas automáticas não desfazem ação manual.** Ficha importada à mão,
exclusão feita pela equipe, decisão registrada — nenhuma limpeza pode reverter.

---

## COMO EU DEVO TRABALHAR AQUI

**Quando o Pedro diz que algo está errado, está errado.** O histórico é de
acerto integral. Investigar antes de explicar; nunca racionalizar.

**Não afirmar número sem medir.** Ritmo de gasto, quantidade de fichas, prazo
restante — se não veio de uma consulta feita agora, é palpite e deve ser dito
como tal.

**Uma explicação plausível não é a causa.** Já corrigi três caminhos certos
sem resolver o problema, porque parei de investigar ao encontrar algo
consertável. Confirmar contra o caso concreto relatado.

**Harness verde ANTES do push.** Não encadear teste e deploy no mesmo comando:
já subi vermelho três vezes por ler o resultado depois de publicar.

**Endereço de API não é entrega.** Funcionalidade que não aparece na tela onde
a pessoa trabalha não foi entregue.

---

## PROTOCOLO DE DEPLOY

```
branch dev → node test/harness.js → 🟢 VERDE → merge main → push → sleep 66s
```

Arquivo de função na Vercel tem limite de tamanho. `wa-bot.js` passou de 470 mil
caracteres e deixou de ser publicado — o sintoma é traiçoeiro: nenhum erro,
a versão antiga continua servindo e as ações novas respondem "não encontrada".
**Funcionalidade nova vai em arquivo próprio.**
