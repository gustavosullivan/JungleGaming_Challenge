//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jungle-gaming/wagering/internal/app/port"
	"github.com/jungle-gaming/wagering/internal/app/wagering"
	"github.com/jungle-gaming/wagering/internal/platform/postgres"
	"github.com/jungle-gaming/wagering/internal/transport/outbox"
	"github.com/jungle-gaming/wagering/internal/transport/sqsconsumer"
)

// Three deliveries of the same operation: the same messageId twice and a
// different messageId once. Each uses its own MessageDeduplicationId, so the
// FIFO queue delivers all of them and only the application can deduplicate.
func TestRepeatedDeliveriesAreDeduplicatedByTheApplication(t *testing.T) {
	q := newQueues(t, 5)
	wallets, wagers, pool := newStack(t, time.Minute, 4)
	defer pool.Close()
	truncate(t, pool)
	opened := open(t, wallets, "80.00")
	body := messageBody(opened, "dup-bet", "BET", "30.00", "")
	other := queueMessage("msg-dup-bet-2", map[string]any{
		"providerId": "provider-a", "externalTransactionId": "dup-bet",
		"playerId": opened.Player.String(), "walletId": opened.Wallet.ID.String(),
		"roundId": "round-1", "gameId": "fortune-chimp", "kind": "BET",
		"money": map[string]string{"amount": "30.00", "currency": "BRL"},
	})
	q.send(t, opened.Wallet.ID.String(), "d1", body)
	q.send(t, opened.Wallet.ID.String(), "d2", body)
	q.send(t, opened.Wallet.ID.String(), "d3", other)

	rec := &recordingQueues{Queues: q.client}
	consumer := sqsconsumer.NewWorker(rec, wagers, discardLog(), nil, q.cfg)
	waitFor(t, "three receipts", 15*time.Second, func() bool {
		_, _ = consumer.ProcessAvailable(context.Background())
		_, deleted := rec.snapshot()
		return deleted == 3
	})
	receives, _ := rec.snapshot()
	if len(receives) != 3 {
		t.Fatalf("receives = %v", receives)
	}
	if n := count(t, pool, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, opened.Wallet.ID); n != 1 {
		t.Fatalf("debits = %d", n)
	}
	if n := count(t, pool, `SELECT count(*) FROM wager_transactions WHERE external_transaction_id = 'dup-bet'`); n != 1 {
		t.Fatalf("transactions = %d", n)
	}
	for _, id := range []string{"msg-dup-bet", "msg-dup-bet-2"} {
		if !inboxCompleted(t, pool, id) {
			t.Fatalf("inbox %s not completed", id)
		}
	}
	if balanceOf(t, pool, opened.Wallet.ID) != "50.00" || q.inboundDepth(t) != 0 {
		t.Fatalf("balance=%s depth=%d", balanceOf(t, pool, opened.Wallet.ID), q.inboundDepth(t))
	}
}

func TestPoisonMessageReachesTheDLQ(t *testing.T) {
	q := newQueues(t, 2)
	wallets, wagers, pool := newStack(t, time.Minute, 4)
	defer pool.Close()
	truncate(t, pool)
	opened := open(t, wallets, "10.00")
	poison := []byte(`{"messageId":"poison-1","type":"WagerTransactionRequested","data":{"kind":"BET","money":{"amount":1.5}}}`)
	q.send(t, opened.Wallet.ID.String(), "poison-1", poison)

	rec := &recordingQueues{Queues: q.client}
	consumer := sqsconsumer.NewWorker(rec, wagers, discardLog(), nil, q.cfg)
	var dlq []string
	waitFor(t, "redrive to the DLQ", 30*time.Second, func() bool {
		_, _ = consumer.ProcessAvailable(context.Background())
		dlq = append(dlq, q.drainDLQ(t)...)
		return len(dlq) > 0
	})
	if dlq[0] != string(poison) {
		t.Fatalf("dlq body = %s", dlq[0])
	}
	receives, deleted := rec.snapshot()
	if len(receives) < 2 || deleted != 0 {
		t.Fatalf("receives=%v deleted=%d", receives, deleted)
	}
	if q.inboundDepth(t) != 0 || balanceOf(t, pool, opened.Wallet.ID) != "10.00" {
		t.Fatal("poison message left the queue dirty or moved money")
	}
}

type flakyStore struct {
	*postgres.Store
	failures atomic.Int32
}

func (f *flakyStore) WithinTx(ctx context.Context, fn func(context.Context, port.Tx) error) error {
	if f.failures.Add(-1) >= 0 {
		return errors.New("read tcp 127.0.0.1:5435: connection reset by peer")
	}
	return f.Store.WithinTx(ctx, fn)
}

func TestTransientFailureIsRetriedThroughTheQueue(t *testing.T) {
	q := newQueues(t, 5)
	wallets, _, pool := newStack(t, time.Minute, 4)
	defer pool.Close()
	truncate(t, pool)
	opened := open(t, wallets, "60.00")
	store := &flakyStore{Store: postgres.NewStore(pool)}
	store.failures.Store(1)
	wagers := wagering.New(wagering.Options{Store: store, Log: discardLog()})
	q.send(t, opened.Wallet.ID.String(), "retry-1", messageBody(opened, "retry-bet", "BET", "15.00", ""))

	rec := &recordingQueues{Queues: q.client}
	consumer := sqsconsumer.NewWorker(rec, wagers, discardLog(), nil, q.cfg)
	waitFor(t, "retry after the visibility backoff", 20*time.Second, func() bool {
		_, _ = consumer.ProcessAvailable(context.Background())
		_, deleted := rec.snapshot()
		return deleted == 1
	})
	receives, _ := rec.snapshot()
	if len(receives) != 2 || receives[1] < 2 {
		t.Fatalf("receive counts = %v", receives)
	}
	if balanceOf(t, pool, opened.Wallet.ID) != "45.00" {
		t.Fatal(balanceOf(t, pool, opened.Wallet.ID))
	}
}

type flakyPublisher struct {
	port.Publisher
	mu        sync.Mutex
	failFirst int
	calls     map[string]int
	ok        map[string]int
}

func (f *flakyPublisher) Publish(ctx context.Context, groupID, dedupID string, body []byte) error {
	f.mu.Lock()
	f.calls[dedupID]++
	fail := f.calls[dedupID] <= f.failFirst
	f.mu.Unlock()
	if fail {
		return errors.New("broker unavailable")
	}
	if err := f.Publisher.Publish(ctx, groupID, dedupID, body); err != nil {
		return err
	}
	f.mu.Lock()
	f.ok[dedupID]++
	f.mu.Unlock()
	return nil
}

// Three publishers poll the same outbox while the broker rejects the first
// attempt of every event. Each event must be confirmed exactly once and reach
// the real events queue with its eventId.
func TestConcurrentPublishersRetryOnRealSQS(t *testing.T) {
	q := newQueues(t, 5)
	wallets, wagers, pool := newStack(t, time.Minute, 4)
	defer pool.Close()
	truncate(t, pool)
	for i := 0; i < 3; i++ {
		w := open(t, wallets, "20.00")
		submit(t, wagers, w.Wallet, w.Player, "pub-"+w.Wallet.ID.String(), "BET", "5.00", "")
	}
	total := count(t, pool, `SELECT count(*) FROM outbox`)

	bus := &flakyPublisher{Publisher: q.client, failFirst: 1, calls: map[string]int{}, ok: map[string]int{}}
	store := postgres.NewStore(pool)
	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pending := func() int {
		n := -1
		_ = pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE published_at IS NULL`).Scan(&n)
		return n
	}
	for i := 0; i < 3; i++ {
		p := outbox.NewWorker(store, bus, discardLog(), nil, q.cfg)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil && pending() != 0 {
				p.PublishOnce(ctx)
				time.Sleep(50 * time.Millisecond)
			}
		}()
	}
	wg.Wait()
	if n := count(t, pool, `SELECT count(*) FROM outbox WHERE published_at IS NULL`); n != 0 {
		t.Fatalf("unpublished = %d", n)
	}
	if n := count(t, pool, `SELECT count(*) FROM outbox WHERE attempts <> 2`); n != 0 {
		t.Fatalf("%d events were not retried exactly once", n)
	}
	for id, n := range bus.ok {
		if n != 1 {
			t.Fatalf("event %s confirmed %d times", id, n)
		}
	}
	if len(bus.ok) != total {
		t.Fatalf("confirmed %d of %d events", len(bus.ok), total)
	}
	received := map[string]bool{}
	waitFor(t, "events on the queue", 15*time.Second, func() bool {
		bodies, err := q.client.ReceiveEvents(context.Background(), 10)
		if err != nil {
			t.Fatal(err)
		}
		for _, raw := range bodies {
			var ev struct{ EventID string }
			_ = json.Unmarshal(raw, &ev)
			received[ev.EventID] = true
		}
		return len(received) == total
	})
	for id := range received {
		if bus.ok[id] != 1 {
			t.Fatalf("unexpected event %s on the queue", id)
		}
	}
}

// The same operation crosses both channels on a running application: HTTP
// first then SQS, and SQS first then HTTP. Either way there is one debit and
// the second channel gets the stored result.
func TestHTTPAndSQSForTheSameOperation(t *testing.T) {
	issuer := requireKeycloak(t)
	q := newQueues(t, 3)
	pool := testPool(t)
	truncate(t, pool)
	configureApps(t, q)
	app := startApp(t)
	defer app.stop(t)
	internal := token(t, issuer, "wallet-internal", "wallet-internal-secret")
	providerA := token(t, issuer, "provider-a", "provider-a-secret")

	player := uuid.Must(uuid.NewV7())
	walletID := openViaAPI(t, app.base, internal, player, "100.00")

	httpFirst := wagerBody("provider-a", player, walletID, "cross-1", "BET", "20.00", "")
	res := apiCall(t, app.base, http.MethodPost, "/wagering/transactions", providerA, "provider-a:cross-1", httpFirst)
	if res.Status != http.StatusOK || res.Body["idempotentReplay"] != false {
		t.Fatalf("http first %d %s", res.Status, res.Raw)
	}
	q.send(t, walletID, "cross-1-msg", queueMessage("cross-1-msg", httpFirst))
	waitFor(t, "sqs copy of cross-1", 15*time.Second, func() bool { return inboxCompleted(t, pool, "cross-1-msg") })

	sqsFirst := wagerBody("provider-a", player, walletID, "cross-2", "BET", "30.00", "")
	q.send(t, walletID, "cross-2-msg", queueMessage("cross-2-msg", sqsFirst))
	waitFor(t, "sqs cross-2", 15*time.Second, func() bool { return inboxCompleted(t, pool, "cross-2-msg") })
	var sqsTx string
	if err := pool.QueryRow(context.Background(), `SELECT id::text FROM wager_transactions WHERE external_transaction_id = 'cross-2'`).Scan(&sqsTx); err != nil {
		t.Fatal(err)
	}
	res = apiCall(t, app.base, http.MethodPost, "/wagering/transactions", providerA, "provider-a:cross-2", sqsFirst)
	if res.Status != http.StatusOK || res.Body["idempotentReplay"] != true || res.str("transactionId") != sqsTx || res.balance() != "50.00" {
		t.Fatalf("http after sqs %d %s", res.Status, res.Raw)
	}

	changed := wagerBody("provider-a", player, walletID, "cross-1", "BET", "99.00", "")
	q.send(t, walletID, "cross-1-changed", queueMessage("cross-1-changed", changed))
	waitFor(t, "conflicting copy in the DLQ", 30*time.Second, func() bool { return len(q.drainDLQ(t)) > 0 })

	id := uuid.MustParse(walletID)
	if n := count(t, pool, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, id); n != 2 {
		t.Fatalf("debits = %d", n)
	}
	if balanceOf(t, pool, id) != "50.00" {
		t.Fatal(balanceOf(t, pool, id))
	}
	rec := apiCall(t, app.base, http.MethodPost, "/wallets/"+walletID+"/reconciliation", internal, "", nil)
	if rec.Status != http.StatusOK || rec.Body["consistent"] != true {
		t.Fatalf("reconciliation %d %s", rec.Status, rec.Raw)
	}
}
