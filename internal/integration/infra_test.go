//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jungle-gaming/wagering/internal/app/port"
	"github.com/jungle-gaming/wagering/internal/fxapp"
	"github.com/jungle-gaming/wagering/internal/platform/config"
	"github.com/jungle-gaming/wagering/internal/platform/sqs"
	"github.com/jungle-gaming/wagering/internal/transport/httpapi"
	"go.uber.org/fx"
)

// Keycloak and LocalStack are mandatory: a missing container fails the run
// instead of silently skipping. INTEGRATION_ALLOW_SKIP=1 relaxes that for a
// quick local run against Postgres only.
func allowSkip() bool { return os.Getenv("INTEGRATION_ALLOW_SKIP") == "1" }

func issuerURL() string { return env("OIDC_ISSUER", "http://localhost:8081/realms/jungle") }

func jwksURL() string {
	return env("OIDC_JWKS_URL", issuerURL()+"/protocol/openid-connect/certs")
}

func sqsEndpoint() string { return env("SQS_ENDPOINT", "http://127.0.0.1:4566") }

func requireKeycloak(t *testing.T) string {
	t.Helper()
	issuer := issuerURL()
	if keycloakUp(issuer) {
		return issuer
	}
	if allowSkip() {
		t.Skip("keycloak is not reachable at " + issuer)
	}
	t.Fatalf("keycloak is not reachable at %s; run: docker compose up -d keycloak keycloak-setup", issuer)
	return ""
}

func rawSQS(t *testing.T) *awssqs.Client {
	t.Helper()
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatal(err)
	}
	return awssqs.NewFromConfig(cfg, func(o *awssqs.Options) { o.BaseEndpoint = aws.String(sqsEndpoint()) })
}

func requireLocalStack(t *testing.T) *awssqs.Client {
	t.Helper()
	api := rawSQS(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := api.ListQueues(ctx, &awssqs.ListQueuesInput{}); err != nil {
		if allowSkip() {
			t.Skip("localstack is not reachable: " + err.Error())
		}
		t.Fatalf("localstack is not reachable at %s: %v; run: docker compose up -d localstack sqs-init", sqsEndpoint(), err)
	}
	return api
}

// testQueues is a private set of FIFO queues (inbound, DLQ with redrive and
// events) created on the real LocalStack for one test, so a running API or
// another test cannot consume its messages.
type testQueues struct {
	cfg    config.Config
	client *sqs.Client
	api    *awssqs.Client
}

func newQueues(t *testing.T, maxReceive int) testQueues {
	t.Helper()
	api := requireLocalStack(t)
	ctx := context.Background()
	prefix := "it-" + strings.ReplaceAll(uuid.NewString()[:13], "-", "")
	urlOf := func(name string) string { return sqsEndpoint() + "/000000000000/" + name }
	create := func(name string, attrs map[string]string) string {
		attrs["FifoQueue"] = "true"
		if _, err := api.CreateQueue(ctx, &awssqs.CreateQueueInput{QueueName: aws.String(name), Attributes: attrs}); err != nil {
			t.Fatal(err)
		}
		u := urlOf(name)
		t.Cleanup(func() {
			_, _ = api.DeleteQueue(context.Background(), &awssqs.DeleteQueueInput{QueueUrl: aws.String(u)})
		})
		return u
	}
	dlq := create(prefix+"-dlq.fifo", map[string]string{})
	attrs, err := api.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl: aws.String(dlq), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	if err != nil {
		t.Fatal(err)
	}
	redrive, _ := json.Marshal(map[string]string{
		"deadLetterTargetArn": attrs.Attributes["QueueArn"], "maxReceiveCount": fmt.Sprint(maxReceive),
	})
	in := create(prefix+"-in.fifo", map[string]string{"VisibilityTimeout": "30", "RedrivePolicy": string(redrive)})
	events := create(prefix+"-events.fifo", map[string]string{})

	cfg := config.Config{
		SQSEndpoint: sqsEndpoint(), SQSRegion: "us-east-1", AWSAccessKey: "test", AWSSecretKey: "test",
		QueueURL: in, DLQURL: dlq, EventsQueueURL: events,
		SQSMaxMessages: 10, SQSWaitSeconds: 1, SQSVisibilitySeconds: 5, ShutdownTimeout: 5 * time.Second,
		OutboxPoll: 100 * time.Millisecond, OutboxLease: 30 * time.Second,
	}
	client, err := sqs.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return testQueues{cfg: cfg, client: client, api: api}
}

func (q testQueues) send(t *testing.T, groupID, dedupID string, body []byte) {
	t.Helper()
	if err := q.client.SendInbound(context.Background(), groupID, dedupID, body); err != nil {
		t.Fatal(err)
	}
}

// drainDLQ reads everything currently in the DLQ.
func (q testQueues) drainDLQ(t *testing.T) []string {
	t.Helper()
	out, err := q.api.ReceiveMessage(context.Background(), &awssqs.ReceiveMessageInput{
		QueueUrl: aws.String(q.cfg.DLQURL), MaxNumberOfMessages: 10, WaitTimeSeconds: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	var bodies []string
	for _, m := range out.Messages {
		bodies = append(bodies, aws.ToString(m.Body))
	}
	return bodies
}

// inboundDepth counts visible plus in-flight messages on the inbound queue.
func (q testQueues) inboundDepth(t *testing.T) int {
	t.Helper()
	out, err := q.api.GetQueueAttributes(context.Background(), &awssqs.GetQueueAttributesInput{
		QueueUrl: aws.String(q.cfg.QueueURL),
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameApproximateNumberOfMessages,
			types.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	var visible, inflight int
	fmt.Sscan(out.Attributes["ApproximateNumberOfMessages"], &visible)
	fmt.Sscan(out.Attributes["ApproximateNumberOfMessagesNotVisible"], &inflight)
	return visible + inflight
}

// recordingQueues wraps the real SQS client and remembers what each receive
// returned, so tests can prove a message was delivered more than once.
type recordingQueues struct {
	port.Queues
	mu       sync.Mutex
	receives []int
	deleted  int
}

func (r *recordingQueues) Receive(ctx context.Context, max, wait, visibility int32) ([]port.Incoming, error) {
	msgs, err := r.Queues.Receive(ctx, max, wait, visibility)
	r.mu.Lock()
	for _, m := range msgs {
		r.receives = append(r.receives, m.ReceiveCount)
	}
	r.mu.Unlock()
	return msgs, err
}

func (r *recordingQueues) Delete(ctx context.Context, receipt string) error {
	err := r.Queues.Delete(ctx, receipt)
	if err == nil {
		r.mu.Lock()
		r.deleted++
		r.mu.Unlock()
	}
	return err
}

func (r *recordingQueues) snapshot() ([]int, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int(nil), r.receives...), r.deleted
}

// assertFinancialConsistency runs after every test that truncates: each
// stored balance must equal credits minus debits, the last ledger line must
// match the balance, and only PROCESSED transactions may have ledger lines.
func assertFinancialConsistency(t *testing.T) {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), dsn())
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	ctx := context.Background()
	rows, err := pool.Query(ctx, `
		SELECT w.id::text, w.balance_minor,
		       COALESCE(SUM(CASE WHEN l.direction = 'CREDIT' THEN l.amount_minor ELSE -l.amount_minor END), 0)
		FROM wallets w
		LEFT JOIN wallet_ledger_entries l ON l.wallet_id = w.id
		GROUP BY w.id, w.balance_minor`)
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for rows.Next() {
		var id string
		var stored, ledger int64
		if err := rows.Scan(&id, &stored, &ledger); err != nil {
			t.Fatal(err)
		}
		checked++
		if stored != ledger || stored < 0 {
			t.Errorf("wallet %s stored=%d ledger=%d", id, stored, ledger)
		}
	}
	rows.Close()
	if n := count(t, pool, `
		SELECT count(*) FROM wallets w
		JOIN LATERAL (
			SELECT balance_after_minor FROM wallet_ledger_entries
			WHERE wallet_id = w.id ORDER BY created_at DESC, id DESC LIMIT 1
		) last ON true
		WHERE last.balance_after_minor <> w.balance_minor`); n != 0 {
		t.Errorf("%d wallets whose last ledger line differs from the balance", n)
	}
	if n := count(t, pool, `
		SELECT count(*) FROM wallet_ledger_entries l
		JOIN wager_transactions t ON t.id = l.transaction_id
		WHERE t.status <> 'PROCESSED'`); n != 0 {
		t.Errorf("%d ledger lines for transactions that are not PROCESSED", n)
	}
	if n := count(t, pool, `
		SELECT count(*) FROM wager_transactions t
		WHERE t.status = 'PROCESSED' AND t.amount_minor > 0
		  AND NOT EXISTS (SELECT 1 FROM wallet_ledger_entries l WHERE l.transaction_id = t.id)`); n != 0 {
		t.Errorf("%d processed movements without a ledger line", n)
	}
	t.Logf("financial consistency checked on %d wallets", checked)
}

// runningApp is a full Fx application: config, pool, HTTP server, SQS
// consumer, outbox publisher and reference worker.
type runningApp struct {
	app  *fx.App
	srv  *httpapi.Server
	pool *pgxpool.Pool
	base string
}

// configureApps points every application started in this test at the given
// private queues, the compose Postgres and the real Keycloak.
func configureApps(t *testing.T, q testQueues) {
	t.Helper()
	for k, v := range map[string]string{
		"HTTP_ADDR": "127.0.0.1:0", "DATABASE_URL": dsn(),
		"SQS_ENDPOINT": q.cfg.SQSEndpoint, "SQS_QUEUE_URL": q.cfg.QueueURL,
		"SQS_DLQ_URL": q.cfg.DLQURL, "SQS_EVENTS_QUEUE_URL": q.cfg.EventsQueueURL,
		"OIDC_ISSUER": issuerURL(), "OIDC_JWKS_URL": jwksURL(),
		"SQS_WAIT_TIME_SECONDS": "1", "SQS_VISIBILITY_TIMEOUT": "5",
		"REFERENCE_BACKOFF": "100ms", "REFERENCE_POLL_INTERVAL": "100ms", "REFERENCE_TTL": "1m",
		"OUTBOX_POLL_INTERVAL": "100ms", "SHUTDOWN_TIMEOUT": "5s",
	} {
		t.Setenv(k, v)
	}
}

func startApp(t *testing.T) *runningApp {
	t.Helper()
	a := &runningApp{}
	a.app = fx.New(
		fxapp.Options(),
		fx.Decorate(func(*slog.Logger) *slog.Logger { return discardLog() }),
		fx.Populate(&a.srv, &a.pool),
		fx.NopLogger,
	)
	if err := a.app.Err(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := a.app.Start(ctx); err != nil {
		t.Fatal(err)
	}
	a.base = "http://" + a.srv.Addr()
	return a
}

func (a *runningApp) stop(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := a.app.Stop(ctx); err != nil {
		t.Fatalf("stop: %v", err)
	}
}

// workerMarkers identify goroutines owned by the application: HTTP server,
// queue consumer, outbox publisher, reference worker and pgx pool.
var workerMarkers = []string{
	"wagering/internal/transport/sqsconsumer.(*Consumer).loop",
	"wagering/internal/transport/outbox.(*Publisher).loop",
	"wagering/internal/app/wagering.(*Service).RunReferenceLoop",
	"wagering/internal/transport/httpapi.NewServer",
	"pgxpool.(*Pool).backgroundHealthCheck",
}

func workerGoroutines() int {
	buf := make([]byte, 8<<20)
	buf = buf[:runtime.Stack(buf, true)]
	n := 0
	for _, g := range strings.Split(string(buf), "\n\n") {
		for _, marker := range workerMarkers {
			if strings.Contains(g, marker) {
				n++
				break
			}
		}
	}
	return n
}

func waitFor(t *testing.T, what string, timeout time.Duration, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

var apiClient = &http.Client{Timeout: 15 * time.Second}

type apiResult struct {
	Status int
	Body   map[string]any
	Raw    string
}

func (r apiResult) str(key string) string {
	v, _ := r.Body[key].(string)
	return v
}

func (r apiResult) balance() string {
	m, _ := r.Body["balance"].(map[string]any)
	v, _ := m["amount"].(string)
	return v
}

func apiCall(t *testing.T, base, method, path, bearer, key string, body any) apiResult {
	t.Helper()
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, base+path, reader)
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	res, err := apiClient.Do(req)
	if err != nil {
		t.Errorf("%s %s: %v", method, path, err)
		return apiResult{}
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	out := apiResult{Status: res.StatusCode, Raw: string(raw)}
	_ = json.Unmarshal(raw, &out.Body)
	return out
}

func openViaAPI(t *testing.T, base, internal string, player uuid.UUID, amount string) string {
	t.Helper()
	res := apiCall(t, base, http.MethodPost, "/wallets", internal, "", map[string]any{
		"playerId": player.String(), "initialBalance": map[string]string{"amount": amount, "currency": "BRL"},
	})
	if res.Status != http.StatusCreated {
		t.Fatalf("open wallet %d %s", res.Status, res.Raw)
	}
	return res.str("id")
}

func wagerBody(provider string, player uuid.UUID, walletID, external, kind, amount, ref string) map[string]any {
	body := map[string]any{
		"providerId": provider, "externalTransactionId": external,
		"playerId": player.String(), "walletId": walletID,
		"roundId": "round-1", "gameId": "fortune-chimp", "kind": kind,
		"money": map[string]string{"amount": amount, "currency": "BRL"},
	}
	if ref != "" {
		body["referenceExternalTransactionId"] = ref
	}
	return body
}

func queueMessage(messageID string, wager map[string]any) []byte {
	data := map[string]any{}
	for k, v := range wager {
		data[k] = v
	}
	data["idempotencyKey"] = fmt.Sprintf("%s:%s", wager["providerId"], wager["externalTransactionId"])
	raw, err := json.Marshal(map[string]any{
		"messageId": messageID, "type": "WagerTransactionRequested",
		"occurredAt": "2026-09-08T12:00:00.000Z", "data": data,
	})
	if err != nil {
		panic(err)
	}
	return raw
}

func inboxCompleted(t *testing.T, pool *pgxpool.Pool, messageID string) bool {
	t.Helper()
	return count(t, pool, `SELECT count(*) FROM inbox WHERE message_id = $1 AND completed_at IS NOT NULL`, messageID) == 1
}

func statusByExternal(t *testing.T, pool *pgxpool.Pool, external string) string {
	t.Helper()
	var status string
	err := pool.QueryRow(context.Background(), `SELECT status FROM wager_transactions WHERE external_transaction_id = $1`, external).Scan(&status)
	if err != nil {
		return ""
	}
	return status
}

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), dsn())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}
