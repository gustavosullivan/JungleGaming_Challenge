# Apostas

API em Go que abre carteira e processa BET, WIN, LOSS, REFUND e ROLLBACK. A operação entra por HTTP ou por SQS e cai no mesmo caso de uso. Saldo, ledger, inbox e outbox vão no mesmo commit do Postgres. O evento só sai depois, num worker separado.

Go 1.27. O `go.mod` e o Dockerfile declaram essa versão. As decisões técnicas estão no [`ARCHITECTURE.md`](ARCHITECTURE.md).

## Pré-requisitos

- Docker com Compose v2. Só isso já sobe tudo e roda a aplicação.
- Go 1.27, pra rodar os testes ou o binário fora do container.
- Um gcc no PATH pro `go test -race`. No Linux o toolchain já resolve. No Windows, ou instala um gcc, ou roda o `-race` num container Go (está em [Testes](#testes)).
- Portas livres: `5435` (Postgres), `4566` (LocalStack), `8081` (Keycloak), `8090` (API) e, no modo de três instâncias, `8082` e `8083`.

O Compose publica o banco em `5435` e a API em `8090`, e não nas portas padrão, porque é comum a máquina já ter um Postgres na 5432 e alguma coisa na 8080. Dentro da rede do Compose o banco continua `postgres:5432` e a API escuta 8080.

## Pra rodar

```bash
docker compose up --build
```

Sobe o Postgres, o LocalStack, o Keycloak e a API. Antes da API, três serviços rodam uma vez e saem: o `migrate` aplica o schema, o `sqs-init` cria as filas e o `keycloak-setup` coloca os papéis nos clients. A API só sobe se os três terminarem com sucesso.

Pra conferir:

```bash
curl -s localhost:8090/health/ready
# {"postgres":"ok","sqs":"ok","status":"ready"}
```

Três processos, cada um com o próprio pool e a própria memória:

```bash
docker compose --profile multi up --build
```

As outras duas ficam em `localhost:8082` e `localhost:8083`. As três leem a mesma fila, a mesma outbox e as mesmas pendências de referência.

Pra derrubar: `docker compose --profile multi down`. Com `-v` apaga também o banco.

### Binário na máquina, infra no Compose

A aplicação lê só variáveis de ambiente, não lê arquivo `.env`. Então o `.env.example` precisa ser carregado no shell antes. Pare o container da API antes, porque os dois usam a porta 8090.

```bash
docker compose up -d postgres localstack sqs-init keycloak keycloak-setup migrate
set -a; . ./.env.example; set +a
go run ./cmd/wagering
```

No PowerShell:

```powershell
docker compose up -d postgres localstack sqs-init keycloak keycloak-setup migrate
Get-Content .env.example | Where-Object { $_ -match '=' } | ForEach-Object { $k, $v = $_ -split '=', 2; Set-Item "env:$k" $v }
go run ./cmd/wagering
```

## Variáveis de ambiente

O `.env.example` tem os valores pra rodar na máquina. O `deploy/app.env` tem os mesmos valores com os nomes de dentro da rede do Compose (`postgres`, `localstack`, `keycloak`). Nenhum dos dois tem segredo de verdade: as credenciais da AWS são o `test`/`test` do LocalStack.

| Variável | Obrigatória | Padrão | Pra quê |
| --- | --- | --- | --- |
| `DATABASE_URL` | sim | | conexão do Postgres |
| `SQS_ENDPOINT` | sim | | endpoint do SQS (LocalStack) |
| `SQS_QUEUE_URL` | sim | | fila de entrada `wager-transactions.fifo` |
| `SQS_DLQ_URL` | sim | | DLQ `wager-transactions-dlq.fifo` |
| `SQS_EVENTS_QUEUE_URL` | sim | | fila de saída da outbox `wager-events.fifo` |
| `OIDC_ISSUER` | sim | | `iss` esperado no token |
| `OIDC_JWKS_URL` | sim | | de onde vêm as chaves públicas do Keycloak |
| `OIDC_AUDIENCE` | não | `wagering-api` | `aud` esperado no token |
| `HTTP_ADDR` | não | `:8080` | endereço do servidor HTTP |
| `SQS_REGION` | não | `us-east-1` | região do SQS |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | não | `test` / `test` | credenciais do broker |
| `SQS_WAIT_TIME_SECONDS` | não | `5` | long polling da fila de entrada |
| `SQS_VISIBILITY_TIMEOUT` | não | `30` | visibility de cada mensagem recebida, em segundos |
| `SQS_MAX_MESSAGES` | não | `5` | mensagens por `ReceiveMessage` |
| `REFERENCE_MAX_ATTEMPTS` | não | `8` | tentativas de achar a referência de uma reversão |
| `REFERENCE_TTL` | não | `2m` | prazo máximo de uma pendência de referência |
| `REFERENCE_BACKOFF` | não | `200ms` | primeiro intervalo do backoff exponencial (teto de 30s) |
| `REFERENCE_POLL_INTERVAL` | não | `500ms` | intervalo do worker de referências |
| `OUTBOX_POLL_INTERVAL` | não | `500ms` | intervalo do publisher da outbox |
| `OUTBOX_LEASE` | não | `30s` | quanto tempo um publisher segura um evento antes de outro poder assumir |
| `SHUTDOWN_TIMEOUT` | não | `20s` | prazo pra terminar o trabalho em andamento no SIGTERM |

O `OIDC_ISSUER` é sempre `http://localhost:8081/realms/jungle`, mesmo dentro do Compose, porque o Keycloak está com `KC_HOSTNAME` fixo e emite todo token com esse `iss`. Só o `OIDC_JWKS_URL` muda pra `keycloak:8080` dentro da rede.

A aplicação valida a configuração na subida: faltou variável obrigatória, ela não sobe.

## Migrations

```bash
DATABASE_URL=postgres://wagering:wagering@localhost:5435/wagering?sslmode=disable go run ./cmd/migrate up
DATABASE_URL=postgres://wagering:wagering@localhost:5435/wagering?sslmode=disable go run ./cmd/migrate down
```

`up` aplica o que falta. `down` desfaz a última versão. No Compose: `docker compose run --rm migrate` e `docker compose run --rm --entrypoint /app/migrate migrate down`.

## Filas

O `sqs-init` (`deploy/sqs/init.sh`) cria `wager-transactions.fifo` (entrada), `wager-transactions-dlq.fifo` (redrive depois de 5 recebimentos) e `wager-events.fifo` (saída da outbox). Pode rodar de novo sem problema:

```bash
docker compose run --rm sqs-init
```

O LocalStack roda sem persistência. Se o container dele reiniciar, as filas somem e o `/health/ready` fica com `"sqs":"down"` até rodar o `sqs-init` de novo.

Visibility da entrada é 30s. `MessageGroupId` é o `walletId`. `MessageDeduplicationId` na entrada é o id da mensagem; na saída é o `eventId`. Isso não substitui a idempotência da aplicação: a mesma operação pode voltar depois da janela do FIFO, ou chegar por HTTP.

Corpo da mensagem de entrada:

```json
{
  "messageId": "msg-123",
  "type": "WagerTransactionRequested",
  "occurredAt": "2026-09-08T12:00:00.000Z",
  "data": {
    "providerId": "provider-a",
    "externalTransactionId": "transaction-123",
    "idempotencyKey": "provider-a:transaction-123",
    "playerId": "...",
    "walletId": "...",
    "roundId": "round-987",
    "gameId": "fortune-chimp",
    "kind": "BET",
    "money": { "amount": "25.00", "currency": "BRL" }
  }
}
```

Pra mandar uma na mão, com o corpo salvo em `msg.json`:

```bash
docker compose cp msg.json localstack:/tmp/msg.json
docker compose exec localstack awslocal sqs send-message \
  --queue-url http://localhost:4566/000000000000/wager-transactions.fifo \
  --message-group-id <walletId> --message-deduplication-id msg-123 \
  --message-body file:///tmp/msg.json
```

## Login

O Keycloak sobe com o realm `jungle` importado de `deploy/keycloak/realm-jungle.json`, e o `keycloak-setup` (`deploy/keycloak/setup.sh`) completa os papéis. Não tem passo manual. A API usa client credentials, e o token tem que ter audience `wagering-api`.

| Client | Secret | Uso |
| --- | --- | --- |
| `provider-a` | `provider-a-secret` | apostar e ler as próprias transações |
| `provider-b` | `provider-b-secret` | igual, isolado do A |
| `provider-short` | `provider-short-secret` | token de 3 segundos, pra ver expiração |
| `wallet-internal` | `wallet-internal-secret` | abrir carteira, ledger, reconciliação, ler qualquer transação |

O `provider_id` do token tem que bater com o `providerId` do corpo. Se não bater, 403 e o saldo não mexe.

O console do Keycloak é http://localhost:8081. Usuário `admin`, senha `admin`. Não é e-mail. A conta do realm `jungle` é `gustavo@jungle.local` / `Jungle123`, em http://localhost:8081/realms/jungle/account.

```bash
TOKEN=$(curl -s -X POST http://localhost:8081/realms/jungle/protocol/openid-connect/token \
  -d grant_type=client_credentials \
  -d client_id=wallet-internal \
  -d client_secret=wallet-internal-secret | jq -r .access_token)
```

Pra aposta, troca o client pra `provider-a`.

No PowerShell:

```powershell
$TOKEN = (Invoke-RestMethod -Method Post http://localhost:8081/realms/jungle/protocol/openid-connect/token -Body @{grant_type="client_credentials"; client_id="wallet-internal"; client_secret="wallet-internal-secret"}).access_token
```

## Exemplos

Abrir carteira, com o token interno:

```bash
curl -s -X POST localhost:8090/wallets \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","initialBalance":{"amount":"1000.00","currency":"BRL"}}'
```

Aposta, com o token do `provider-a`:

```bash
curl -s -X POST localhost:8090/wagering/transactions \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: provider-a:transaction-123" \
  -d '{"providerId":"provider-a","externalTransactionId":"transaction-123","playerId":"...","walletId":"...","roundId":"round-987","gameId":"fortune-chimp","kind":"BET","money":{"amount":"25.00","currency":"BRL"}}'
```

REFUND e ROLLBACK mandam `referenceExternalTransactionId`.

Leitura e reconciliação:

```bash
curl -s localhost:8090/wallets/<walletId> -H "Authorization: Bearer $TOKEN"
curl -s "localhost:8090/wallets/<walletId>/ledger?limit=50" -H "Authorization: Bearer $TOKEN"
curl -s localhost:8090/wagering/transactions/<transactionId> -H "Authorization: Bearer $TOKEN"
curl -s localhost:8090/providers/provider-a/wagering/transactions/transaction-123 -H "Authorization: Bearer $TOKEN"
curl -s -X POST localhost:8090/wallets/<walletId>/reconciliation -H "Authorization: Bearer $TOKEN"
```

O ledger vem em ordem estável. Quando há mais páginas, a resposta traz `nextCursor`, um valor opaco que vai de volta em `?cursor=`.

Health é público: `GET /health/live` e `GET /health/ready`. Métricas em `GET /metrics`.

| O que aconteceu | HTTP |
| --- | --- |
| processou, ou devolveu o resultado já gravado | 200, `idempotentReplay` true no replay |
| ficou esperando a referência | 202 |
| JSON, dinheiro ou kind inválido | 400 |
| sem token, token ruim ou expirado | 401 |
| papel errado, ou provedor tentando operar como outro | 403 |
| carteira ou transação que esse caller não pode ver | 404 |
| mesma chave com outro corpo, outra chave pra mesma operação, ou carteira duplicada | 409 |
| regra de negócio, com `failureCode` | 422 |
| Postgres ou SQS fora | 503 |

Replay de uma rejeição volta 422 de novo, com o saldo que foi visto na hora da decisão, não o saldo de agora.

Os 200, 202 e 422 do envio de operação têm o mesmo corpo. `balance` vem quando a decisão foi tomada, e `failureCode` só na rejeição:

```json
{ "transactionId": "0192f298-...", "status": "REJECTED", "failureCode": "INSUFFICIENT_BALANCE",
  "balance": { "amount": "20.00", "currency": "BRL" }, "idempotentReplay": false }
```

Os outros erros têm um código fixo em `error` e uma mensagem curta:

```json
{ "error": "conflict", "message": "idempotency key reused with a different payload" }
```

| HTTP | `error` |
| --- | --- |
| 400 | `invalid_request` |
| 401 | `unauthorized` |
| 403 | `forbidden` |
| 404 | `not_found` |
| 409 | `conflict` |
| 503 | `unavailable` |
| 500 | `internal` |

400 é entrada corrigível: nada é gravado, e o mesmo pedido pode voltar corrigido com a mesma chave. 422 é resultado definitivo: a rejeição fica gravada, a mesma chave sempre devolve a mesma rejeição, e uma nova tentativa precisa de outro `externalTransactionId`. 503 é transitório: pode reenviar igual.

| `failureCode` | Quando |
| --- | --- |
| `INSUFFICIENT_BALANCE` | BET maior que o saldo |
| `REVERSAL_INSUFFICIENT_BALANCE` | ROLLBACK de WIN ou REFUND que deixaria o saldo negativo |
| `INVALID_AMOUNT` | BET, WIN, REFUND ou ROLLBACK com zero, ou LOSS diferente de `"0.00"` |
| `CURRENCY_MISMATCH` | moeda diferente da carteira |
| `PLAYER_WALLET_MISMATCH` | a carteira não é desse jogador |
| `REFERENCE_NOT_FOUND` | a referência não chegou dentro das tentativas ou do TTL |
| `REFERENCE_NOT_SUCCESSFUL` | a referência terminou `REJECTED` ou `FAILED` |
| `REFERENCE_MISMATCH` | referência de outro jogador, carteira, moeda ou rodada |
| `REFERENCE_AMOUNT_MISMATCH` | valor diferente do referenciado |
| `INVALID_REFERENCE_KIND` | kind que não pode ser revertido por essa operação |
| `ALREADY_REVERSED` | a referência já tem uma reversão processada |
| `REVERSAL_IN_PROGRESS` | a referência já tem uma reversão pendente |

## Testes

Os quatro comandos de verificação:

```bash
docker compose up --build
go test ./...
go test -race ./...
go vet ./...
```

`go test ./...` roda só os unitários. Os de integração ficam atrás da build tag `integration` e precisam da infraestrutura no ar (abaixo).

### Unitários

Não precisam de Docker nem de rede:

| Pacote | O que cobre |
| --- | --- |
| `internal/domain/money` | parse, escala de duas casas, limites de `int64`, entrada inválida, moeda diferente |
| `internal/domain/wallet`, `ledger`, `wager`, `event`, `idempotency` | invariantes da carteira, transições de estado, lançamento, envelope do evento, hash canônico |
| `internal/app/wagering` | regras dos cinco kinds, política de zero por kind, conflito de payload na mesma chave, referência pendente (resolução, TTL, tentativas), abertura com transação interna e eventos, falha no meio da transação sem rastro, concorrência, isolamento por provedor. Roda contra um store em memória (`internal/app/port/memstore`) que imita os índices únicos e `CHECK`s do schema, e cada teste termina conferindo saldo contra ledger |
| `internal/platform/auth` | token válido, expirado, `alg: none`, chave de fora, `kid` desconhecido, issuer e audience errados, sem papel, sem `provider_id`, assinatura adulterada. As chaves vêm de um JWKS local (`authtest`) |
| `internal/transport/httpapi` | 401 em toda rota de negócio sem token válido, 403 nas operações internas pra provedor, isolamento entre provedores, sem efeito financeiro e sem vazar dado |
| `internal/transport/sqsconsumer` | apaga só depois do commit, reentrega sem segundo débito, rejeição de negócio confirmada, mensagem venenosa deixada pra DLQ, backoff de visibility em falha transitória, parada no shutdown |
| `internal/transport/outbox` | cada evento marcado uma vez, falha do broker reagendada, lote interrompido no shutdown devolvido na hora pra outro publisher |
| `internal/fxapp` | o grafo do Fx resolve |

O store em memória não substitui o Postgres: os mesmos fluxos rodam de novo na integração contra o banco de verdade.

O `-race` precisa de cgo. No Linux o toolchain já resolve. No Windows sem gcc, roda num container Go:

```bash
docker run --rm -v "${PWD}:/src" -w /src golang:1.27 go test -race ./...
```

### Preparar as dependências da integração

Os testes usam Postgres, Keycloak e LocalStack de verdade, nas portas publicadas pelo Compose. Nada disso é mock. Os padrões batem com o Compose (`localhost:5435`, `http://localhost:8081/realms/jungle`, `http://127.0.0.1:4566`) e dá pra trocar por `DATABASE_URL`, `OIDC_ISSUER`, `OIDC_JWKS_URL` e `SQS_ENDPOINT`.

```bash
docker compose up -d postgres localstack sqs-init keycloak keycloak-setup migrate
docker compose --profile multi stop app app-2 app-3
```

As APIs precisam estar paradas. Os testes limpam as tabelas, e os workers de uma API no ar pegariam a outbox e as pendências de referência antes do teste. Não rode contra um banco com dados que você queira guardar.

Cada teste que mexe com SQS cria as próprias filas FIFO (`it-<id>-in.fifo` com redrive pra `it-<id>-dlq.fifo`, e `it-<id>-events.fifo`) e apaga no fim. Assim um teste não lê mensagem de outro, nem da fila da API.

Os três serviços são obrigatórios. Se o Keycloak ou o LocalStack não responderem, o teste falha, não pula. Pra uma rodada rápida só com Postgres, `INTEGRATION_ALLOW_SKIP=1` transforma essas falhas em skip.

### Integração

```bash
go test -tags integration ./...
go test -tags integration -race ./...
go vet -tags integration ./...
```

Sem gcc no Windows, o `-race` roda num container Go com a rede do host, que enxerga as mesmas portas e o mesmo emissor `localhost:8081` dos tokens:

```bash
docker run --rm --network host -v "${PWD}:/src" -w /src golang:1.27 go test -tags integration -race ./...
```

Ao fim de cada teste, um `Cleanup` confere todas as carteiras do banco: saldo gravado igual a créditos menos débitos do ledger, saldo nunca negativo, último `balance_after` igual ao saldo, nenhuma transação não processada com lançamento e nenhuma processada com movimento sem lançamento.

### Múltiplas instâncias

O `TestThreeApplicationInstances` sobe três aplicações Fx completas no mesmo processo de teste, cada uma com o próprio pool, servidor HTTP, consumer, publisher e worker de referência, apontando pro mesmo banco e as mesmas filas. Ele repete ali as 50 apostas iguais (uma nova e 49 replays com o mesmo `transactionId`), as duas de 80 em cima de 100, seis carteiras em paralelo, uma pendência resolvida por qualquer instância e mensagens consumidas por uma delas.

O `TestTwoLargeBetsInSeparateProcesses` dispara três processos de verdade, cada um com a própria memória e as próprias conexões. Os testes de processo morto também usam subprocessos: o binário de teste se chama de novo com `WAGER_PROC` definido.

Na mão, com `docker compose --profile multi up --build`, dá pra mandar a mesma operação pelas portas 8090, 8082 e 8083. Todas devolvem o mesmo `transactionId` e o ledger fica com um lançamento só.

### Simulações de falha

O que os testes de integração cobrem:

| Falha | Teste |
| --- | --- |
| mesma operação recebida 50 vezes em paralelo: um débito, 49 replays | `TestFiftyDuplicatesAndTwoLargeBets`, `TestThreeApplicationInstances` |
| duas apostas de 80.00 em saldo de 100.00 | `TestFiftyDuplicatesAndTwoLargeBets`, `TestTwoLargeBetsInSeparateProcesses`, `TestThreeApplicationInstances` |
| carteiras diferentes em paralelo | `TestWalletsDoNotBlockEachOther`, `TestThreeApplicationInstances` |
| a mesma mensagem recebida três vezes do SQS (dedup ids diferentes, `ReceiveCount` provado), um débito só | `TestRepeatedDeliveriesAreDeduplicatedByTheApplication` |
| consumidor morto (processo encerrado) depois do commit e antes do `DeleteMessage`: a reentrega chega com `ReceiveCount` 2 e não debita de novo | `TestConsumerKilledAfterCommitBeforeDelete` |
| falha transitória no banco: a mensagem volta pela fila e processa na segunda | `TestTransientFailureIsRetriedThroughTheQueue` |
| mensagem venenosa vai pra DLQ pelo redrive | `TestPoisonMessageReachesTheDLQ` |
| mesma operação por HTTP e por SQS, nas duas ordens, e cópia conflitante pela fila | `TestHTTPAndSQSForTheSameOperation`, `TestInboxRedeliveryAndCrossChannel` |
| dois publishers na mesma outbox, publisher que morre com eventos travados | `TestOutboxClaimIsExclusiveAndRecoverable` |
| três publishers com falha do broker na primeira tentativa, retry até o SQS real | `TestConcurrentPublishersRetryOnRealSQS` |
| reversão antes da referência, resolvida depois; REFUND e ROLLBACK exclusivos | `TestReversalArrivesFirstAndExclusiveRefund` |
| referência que nunca chega: rejeição por expiração | `TestReferenceExpires` |
| processo morto depois de assumir uma pendência: outra instância retoma quando o lease vence | `TestPendingReferenceResumedAfterProcessIsKilled` |
| reinício preservando idempotência, pendências e saldo | `TestRestartPreservesIdempotencyPendingAndBalances` |
| subida e desligamento do Fx, com os workers parados, o pool fechado e a porta liberada | `TestFxAppStartsServesAndReleasesResources` |
| ledger editado ou apagado direto no banco, saldo negativo forçado, transação terminal alterada | `TestLedgerIsImmutableAndBalanceCannotGoNegative` |
| migrations aplicadas, revertidas e aplicadas de novo | `TestMigrationsDownAndUp` |
| token ausente, inválido, adulterado e expirado no Keycloak real; isolamento de provedor em escrita, leitura e replay; operação interna restrita; nada disso mexe em saldo, ledger ou outbox | `TestAuthenticationAndProviderIsolation` |

Pra repetir um teste sensível a tempo várias vezes:

```bash
go test -tags integration -count=5 -run TestRestartPreservesIdempotencyPendingAndBalances ./internal/integration/...
```

Roteiros manuais, com `docker compose --profile multi up --build` no ar:

1. **Instância morta com pendência.** Mande um REFUND com `referenceExternalTransactionId` de uma BET que ainda não existe: volta 202 com `PENDING_REFERENCE`. Mate a primeira instância com `docker compose kill app`. Mande a BET pela 8082. Depois do backoff (alguns segundos) o REFUND aparece `PROCESSED` em `GET /providers/provider-a/wagering/transactions/<id>`, resolvido por outra instância.
2. **Postgres fora.** `docker compose stop postgres`. O `/health/ready` volta 503 com `"postgres":"down"` e as escritas voltam 503 `unavailable`. `docker compose start postgres` e a API volta sozinha.
3. **SQS fora.** `docker compose stop localstack`. O `/health/ready` volta 503 com `"sqs":"down"`. Depois de `docker compose start localstack`, rode `docker compose run --rm sqs-init`, porque as filas não sobrevivem ao restart.
4. **Reinício geral.** `docker compose --profile multi restart app app-2 app-3`. Reenviar uma operação já processada devolve o mesmo resultado, com `idempotentReplay: true`, e a reconciliação da carteira continua `consistent: true`.

`gofmt` em tudo. Dependências no `go.mod` e no `go.sum`.
