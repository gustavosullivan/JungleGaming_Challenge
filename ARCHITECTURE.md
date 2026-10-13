# Como ficou organizado

## Dinheiro

`Money` é `int64` em centavos mais o código da moeda. Não usei float em lugar nenhum do valor: parse, conta, JSON e coluna. No banco o valor é `BIGINT`.

A escala é duas casas. `"25"`, `"25.0"` e `"25.00"` viram `"25.00"` antes do hash. Se a entrada vem vazia, negativa, em notação científica, `NaN`, `Infinity`, com mais de duas casas ou com zero à esquerda (`01.00`), eu recuso. Não arredondo.

O máximo é `math.MaxInt64` centavos, `92233720368547758.07`. Soma, subtração e negação estouram com erro, inclusive negar `MinInt64`. Saldo de carteira não fica negativo. A diferença da reconciliação pode, porque é conta interna.

O fluxo do desafio é BRL. O tipo carrega a moeda mesmo assim, e BRL com USD não soma.

## Onde a transação SQL começa e termina

O caso de uso fala com uma interface. Por baixo é `pgx`, SQL escrito na mão, isolamento `READ COMMITTED`.

Quando a operação não depende de outra, eu faço um commit só: trava a carteira, grava a transação já no estado final, o lançamento, a outbox e, se veio do SQS, a inbox. Não deixo um `PENDING` gravado no meio. Se o processo morre antes do commit, o banco desfaz. Se morre depois, o replay acha o resultado.

`PENDING_REFERENCE` é o único estado intermediário que eu confirmo. A inbox dessa mensagem fecha no mesmo commit. O worker de referência continua depois, de novo com lock da carteira.

Abertura com saldo positivo grava a carteira na versão 1, um `OPENING` já `PROCESSED`, o crédito no ledger e os dois eventos. Saldo zero grava só a carteira. A versão não sobe nessa abertura: o saldo inicial é a criação, não um movimento depois. A versão só anda em `Debit` e `Credit`.

Reconciliação lê com `FOR SHARE` e soma o ledger. Não dá `UPDATE` no saldo.

## Idempotência

SHA-256 em hex de um JSON com as chaves em ordem alfabética:

`externalTransactionId`, `gameId`, `kind`, `money.amount`, `money.currency`, `playerId`, `providerId`, `referenceExternalTransactionId` (só se não estiver vazio), `roundId`, `walletId`.

A `Idempotency-Key`, o `messageId` e o resto do transporte ficam de fora. HTTP e SQS chamam a mesma função, então `"25"` e `"25.00"` batem.

Tem índice único em `idempotency_key` e em `(provider_id, external_transaction_id)`. Mesma chave e mesmo hash devolvem o que já foi gravado, com o saldo daquela decisão. Chave reaproveitada com outro hash, ou a mesma operação externa com outra chave, é 409. Isso fica no Postgres, não em memória.

As duas buscas são consultas separadas em `READ COMMITTED`, então outra instância pode confirmar a mesma operação entre uma e outra. Por isso a linha achada pelo id externo também é comparada pela chave: mesma chave é replay, só chave diferente é 409. Se duas instâncias passarem juntas pelas buscas, o índice único barra a segunda no `INSERT` e ela relê o que a primeira gravou.

## Concorrência

O lock é `SELECT ... FROM wallets WHERE id = $1 FOR UPDATE`. Carteira diferente não espera. Não tem lock global nem fila dentro do processo.

O `UPDATE` do saldo ainda exige `version = $anterior`, e o `CHECK` não deixa `balance_minor` negativo. Se duas escritas passassem do lock, a segunda não acharia a versão e a transação inteira desfaz.

O caso das duas apostas de 80 em saldo de 100 fica com uma `PROCESSED`, uma `REJECTED` (`INSUFFICIENT_BALANCE`), saldo 20.00 e um débito só. Tem teste com três pools e outro com três processos separados.

No worker de referência a ordem é carteira, depois a transação. Na submissão também trava a carteira antes de inserir. Os dois lados pegam a carteira primeiro.

## Referência que ainda não chegou

REFUND e ROLLBACK precisam de `referenceExternalTransactionId`. WIN pode apontar pra uma BET da mesma rodada. Se a referência não existe, ou existe mas ainda não terminou, a operação fica `PENDING_REFERENCE`, com `next_attempt_at` e `deadline_at`.

O worker pega a linha com `FOR UPDATE SKIP LOCKED`. Backoff começa em 200ms, dobra até 30s, no máximo 8 tentativas, TTL de 2 minutos. Dá pra mudar pelos envs. Estourou tentativa ou TTL: `REJECTED` com `REFERENCE_NOT_FOUND` e o evento de rejeição.

Se a referência terminou `REJECTED` ou `FAILED`, a reversão sai na hora com `REFERENCE_NOT_SUCCESSFUL`. Player, carteira, moeda ou rodada diferentes: `REFERENCE_MISMATCH`. Kind que não pode ser revertido: `INVALID_REFERENCE_KIND`. Valor diferente: `REFERENCE_AMOUNT_MISMATCH`. Não tem reversão parcial.

## REFUND e ROLLBACK juntos

Os dois devolveriam o mesmo débito de uma BET, então eu não deixo os dois passarem. O índice único parcial em `(provider_id, reference_external_transaction_id)` pega `REFUND` e `ROLLBACK` quando o status é `PENDING`, `PENDING_REFERENCE` ou `PROCESSED`. A segunda reversão bem-sucedida não entra. Se a primeira já processou, a resposta é `ALREADY_REVERSED`. Se ainda está pendente, `REVERSAL_IN_PROGRESS`. Rejeitada sai do índice, então uma tentativa nova, com outro id externo, pode ser feita.

ROLLBACK de WIN ou de REFUND debita. Se o saldo não cobre, o código é `REVERSAL_INSUFFICIENT_BALANCE`, não o `INSUFFICIENT_BALANCE` da aposta. ROLLBACK de BET e REFUND de BET creditam.

LOSS exige `"0.00"`. Não mexe na versão, não gera ledger e não gera `WalletBalanceChanged`. Gera `WagerTransactionProcessed`. Se vier com outro valor, eu gravo `REJECTED` / `INVALID_AMOUNT` com o valor que chegou. O `CHECK` só exige zero quando o status é `PROCESSED`, senão essa rejeição não caberia na tabela.

## Estados

`PENDING` só existe na memória, antes do commit. `PENDING_REFERENCE` espera o worker. `PROCESSED`, `REJECTED` e `FAILED` são terminais: o trigger recusa `UPDATE`.

Regra de negócio vira `REJECTED` com `failureCode` fixo. Erro de conexão, deadlock e timeout desfazem a transação e a mensagem do SQS ganha visibility com backoff. Corpo inválido, hash diferente ou chave repetida é permanente: visibility 0 até o redrive mandar pra DLQ.

`FAILED` existe no domínio e no schema pra uma falha permanente que já tenha linha. O worker de hoje não grava essa linha. Mensagem inválida nem vira transação, e falha de infra não chega a commitar.

## Inbox e outbox

Inbox: `(consumer_name, message_id)`, consumer `wager-transactions`, hash SHA-256 do corpo cru. Reentrega com o mesmo hash e inbox concluída só apaga a mensagem. Corpo diferente no mesmo `messageId` é permanente.

A mensagem sai da fila depois do commit. Rejeição de negócio confirmada também pode ser apagada. No SIGTERM o consumer para de chamar `ReceiveMessage`, termina o que já pegou dentro de `SHUTDOWN_TIMEOUT` e, se o prazo estourar, zera a visibility pra outra instância.

O `eventId` nasce dentro da transação de domínio. O publisher faz `FOR UPDATE SKIP LOCKED`, publica em `wager-events.fifo` com `MessageDeduplicationId = eventId` e `MessageGroupId` igual ao id da carteira, e só então marca `published_at`. Dois publishers não levam a mesma linha. Se o `locked_until` venceu, outra instância republica o mesmo `eventId`.

O envelope tem `eventId`, `eventType`, `aggregateId`, `correlationId`, `causationId`, `occurredAt` em UTC com milissegundos, `version` (1, definido no construtor) e `data`. O JSON é montado na hora da construção e gravado como está. `WalletBalanceChanged` leva carteira, transação, direção, valor, saldo anterior, saldo posterior e versão.

## Auth

Keycloak, realm `jungle`, client credentials. Escolhi o Keycloak porque é o IdP que o desafio recomenda, roda no Compose sem conta externa e importa o realm de um JSON versionado (`deploy/keycloak/realm-jungle.json`), então clients, secrets, papéis e o claim `provider_id` sobem iguais em qualquer máquina. Client credentials é o fluxo OAuth 2.0 de serviço pra serviço: o provedor de jogo é um sistema, não uma pessoa.

A API confere RS256, issuer, expiração e audience `wagering-api` com as chaves do JWKS. Não emite token e não guarda senha. O Keycloak está com `KC_HOSTNAME` fixo em `http://localhost:8081`, então o `iss` é o mesmo pra quem pede token de fora e pra API dentro da rede.

`internal` abre carteira, lê ledger e reconcilia. `provider` manda operação e lê transação. O claim `provider_id` tem que ser o do corpo e o do path. Transação de outro provedor, inclusive replay e `OPENING`, responde 404 pra não dizer se existe. Pedir o path de outro provedor responde 403. Nenhum desses caminhos grava saldo.

SQS não leva JWT. Quem publica na fila já passou pela credencial do broker (`test`/`test` no LocalStack). O consumer ainda recusa `OPENING`, valida o payload e usa a mesma idempotência. A política da fila é que limita quem publica; o domínio não confia no corpo pra furar invariante.

## Fx e desligar

Config, pool, store, SQS, verifier, casos de uso, HTTP, consumer, outbox e o worker de referência entram por construtor. O `fx.Lifecycle` sobe o pool (com ping), o HTTP e os workers. No stop a ordem inverte: para de aceitar HTTP, cancela os loops, espera o que está em andamento e só então fecha o pool.

O domínio não importa Fx, `net/http`, SQS nem `pgx`.

## Log e métrica

Log em JSON com `correlationId`, `messageId`, `transactionId`, `walletId`, `providerId`, status e `failureCode`. Sem token, sem corpo e sem o valor da aposta. Se a reconciliação divergir, eu registro a diferença, porque é isso que a gente quer ver.

`/metrics` tem resultado por kind e status, replay, retry, DLQ, conflito de versão, idade da outbox mais antiga e divergência de reconciliação.

## Interpretações

Pontos em que o enunciado deixa margem e a leitura que eu adotei:

- Operação sem dependência termina no mesmo commit, sem `PENDING` gravado. O enunciado deixa, e um `PENDING` gravado à toa só aumentaria a janela de crash.
- A abertura com saldo positivo fica na versão 1. O crédito inicial é a criação da carteira, não um movimento depois dela.
- WIN pode vir sem referência. Se vier com uma, ela tem que ser uma BET da mesma rodada, e a resolução segue as mesmas regras de pendência das reversões.
- LOSS com valor diferente de `"0.00"` é gravado como `REJECTED` / `INVALID_AMOUNT`, não como 400. Assim a rejeição fica auditável e o replay devolve a mesma resposta.
- REFUND e ROLLBACK sobre a mesma BET são mutuamente exclusivos: os dois devolveriam o mesmo débito.
- Transação de outro provedor responde 404, e não 403, pra não revelar que ela existe.
- O SQS não leva JWT. A autorização ali é a credencial do broker; o consumidor ainda valida o payload e recusa `OPENING`.

## Limitações e o que não fiz

- Partida dobrada com conta da casa, tracing e teste de carga.
- `FAILED` não é gravado pelos workers, pelo motivo da seção de estados.
- O LocalStack roda sem persistência: se o container reiniciar, as filas somem até o `sqs-init` rodar de novo.
- O Keycloak roda em `start-dev`, com HTTP e `KC_HOSTNAME` fixo em `localhost:8081`. Serve pro ambiente local, não pra produção.
- Os testes de integração usam o mesmo banco e as mesmas filas do Compose e limpam as tabelas. Precisam das instâncias da API paradas e não podem rodar contra dados que se queira guardar.
- O `-race` precisa de cgo. No Windows sem gcc, roda num container Go, como está no README.
