# Apostas

API em Go que abre carteira e processa BET, WIN, LOSS, REFUND e ROLLBACK. A operação entra por HTTP ou por SQS e cai no mesmo caso de uso. Saldo, ledger, inbox e outbox vão no mesmo commit do Postgres. O evento só sai depois, num worker separado.

Go 1.27. O `go.mod` e o Dockerfile declaram essa versão.

## Pra rodar

Precisa de Go 1.27 e Docker Compose v2. O `.env.example` tem os valores locais, nada de segredo de verdade.

```bash
docker compose up --build
```

Sobe o Postgres, o LocalStack (4566), o Keycloak (8081) e a API. O `sqs-init` cria as filas, o `keycloak-setup` coloca os papéis nos clients e o `migrate` aplica o schema antes da API.

Nesta máquina a 5432 já é o Postgres do Windows e a 8080 é o Apache, então o Compose publica o banco em `localhost:5435` e a API em `localhost:8090`. Dentro da rede o banco continua `postgres:5432` e a API escuta 8080.

Três processos, cada um com o próprio pool:

```bash
docker compose --profile multi up --build
```

As outras duas ficam em `localhost:8082` e `localhost:8083`. As três leem a mesma fila e a mesma outbox.

Se eu quiser o binário na máquina e só a infra no Compose:

```bash
docker compose up postgres localstack sqs-init keycloak keycloak-setup migrate
cp .env.example .env
go run ./cmd/wagering
```

## Migrations

```bash
DATABASE_URL=postgres://wagering:wagering@localhost:5435/wagering?sslmode=disable go run ./cmd/migrate up
DATABASE_URL=postgres://wagering:wagering@localhost:5435/wagering?sslmode=disable go run ./cmd/migrate down
```

`up` aplica o que falta. `down` desfaz a última versão. No Compose: `docker compose run --rm migrate` e `docker compose run --rm --entrypoint /app/migrate migrate down`.

## Filas

O `sqs-init` cria `wager-transactions.fifo` (entrada), `wager-transactions-dlq.fifo` (redrive depois de 5 recebimentos) e `wager-events.fifo` (saída da outbox).

Visibility da entrada é 30s. `MessageGroupId` é o `walletId`. `MessageDeduplicationId` na entrada é o id da mensagem; na saída é o `eventId`. Isso não substitui a idempotência da aplicação: a mesma operação pode voltar depois da janela do FIFO, ou chegar por HTTP.

## Login

Realm `jungle`, client credentials. O token tem que ter audience `wagering-api`.

| Client | Secret | Uso |
| --- | --- | --- |
| `provider-a` | `provider-a-secret` | apostar e ler as próprias transações |
| `provider-b` | `provider-b-secret` | igual, isolado do A |
| `provider-short` | `provider-short-secret` | token de 3 segundos, pra ver expiração |
| `wallet-internal` | `wallet-internal-secret` | abrir carteira, ledger, reconciliação, ler qualquer transação |

O `provider_id` do token tem que bater com o `providerId` do corpo. Se não bater, 403 e o saldo não mexe.

O console do Keycloak é http://localhost:8081. Usuário `admin`, senha `admin`. Não é e-mail. A conta do realm `jungle` é `gustavo@jungle.local` / `Jungle123`, em http://localhost:8081/realms/jungle/account.

```bash
curl -s -X POST http://localhost:8081/realms/jungle/protocol/openid-connect/token \
  -d grant_type=client_credentials \
  -d client_id=wallet-internal \
  -d client_secret=wallet-internal-secret
```

Pra aposta, troca o client pra `provider-a`.

## Exemplos

Abrir carteira, com o token interno:

```bash
curl -s -X POST localhost:8090/wallets \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","initialBalance":{"amount":"1000.00","currency":"BRL"}}'
```

Aposta:

```bash
curl -s -X POST localhost:8090/wagering/transactions \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: provider-a:transaction-123" \
  -d '{"providerId":"provider-a","externalTransactionId":"transaction-123","playerId":"...","walletId":"...","roundId":"round-987","gameId":"fortune-chimp","kind":"BET","money":{"amount":"25.00","currency":"BRL"}}'
```

REFUND e ROLLBACK mandam `referenceExternalTransactionId`. Health é público: `GET /health/live` e `GET /health/ready`. Métricas em `GET /metrics`.

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

## Testes

Sem Docker:

```bash
go test ./...
go test -race ./...
go vet ./...
```

`-race` usa cgo. No Linux o toolchain já resolve. No Windows precisa de um gcc no PATH.

Integração, com o Compose no ar. Postgres é obrigatório. Os testes de Keycloak e de fila pulam se esses dois não responderem.

```bash
go test -tags integration ./...
go test -tags integration -race ./...
```

As 50 apostas iguais, as duas de 80 em cima de 100 e as carteiras em paralelo usam três pools. Tem também um teste que dispara três processos de verdade, cada um com a própria memória. O teste do Fx sobe a aplicação e desliga. A reentrega da fila é o segundo `AcceptDelivery` com o mesmo `messageId`: a inbox já está concluída e não nasce outro débito.

`gofmt` em tudo. Dependências no `go.mod` e no `go.sum`.
