package sqsconsumer_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jungle-gaming/wagering/internal/app/port"
	"github.com/jungle-gaming/wagering/internal/app/port/memstore"
	"github.com/jungle-gaming/wagering/internal/app/wagering"
	walletsvc "github.com/jungle-gaming/wagering/internal/app/wallet"
	"github.com/jungle-gaming/wagering/internal/platform/config"
	"github.com/jungle-gaming/wagering/internal/transport/sqsconsumer"
	"go.uber.org/fx/fxtest"
)

type fakeQueue struct {
	mu         sync.Mutex
	pending    []port.Incoming
	deleted    []string
	visibility map[string]int32
	block      bool
	arrived    chan struct{}
	cancelled  atomic.Bool
}

func (q *fakeQueue) push(receipt string, count int, body []byte) {
	q.mu.Lock()
	q.pending = append(q.pending, port.Incoming{Receipt: receipt, Body: body, ReceiveCount: count})
	arrived := q.arrived
	q.mu.Unlock()
	if arrived != nil {
		select {
		case arrived <- struct{}{}:
		default:
		}
	}
}

// Receive behaves like a long poll when block is set: it returns as soon as
// a message arrives or the context is cancelled.
func (q *fakeQueue) Receive(ctx context.Context, max, _, _ int32) ([]port.Incoming, error) {
	for {
		q.mu.Lock()
		if len(q.pending) > 0 {
			n := int(max)
			if n > len(q.pending) {
				n = len(q.pending)
			}
			out := q.pending[:n]
			q.pending = q.pending[n:]
			q.mu.Unlock()
			return out, nil
		}
		block, arrived := q.block, q.arrived
		q.mu.Unlock()
		if !block {
			return nil, nil
		}
		select {
		case <-ctx.Done():
			q.cancelled.Store(true)
			return nil, ctx.Err()
		case <-arrived:
		}
	}
}

func (q *fakeQueue) Delete(_ context.Context, receipt string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.deleted = append(q.deleted, receipt)
	return nil
}

func (q *fakeQueue) ChangeVisibility(_ context.Context, receipt string, timeout int32) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.visibility == nil {
		q.visibility = map[string]int32{}
	}
	q.visibility[receipt] = timeout
	return nil
}

func (q *fakeQueue) Ready(context.Context) error { return nil }

func (q *fakeQueue) wasDeleted(receipt string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, r := range q.deleted {
		if r == receipt {
			return true
		}
	}
	return false
}

func (q *fakeQueue) visibilityOf(receipt string) (int32, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	v, ok := q.visibility[receipt]
	return v, ok
}

type counter struct {
	mu      sync.Mutex
	dlq     int
	retries map[string]int
}

func (c *counter) Submit(string, string, time.Duration) {}
func (c *counter) Replay()                              {}
func (c *counter) Conflict()                            {}
func (c *counter) Divergence()                          {}
func (c *counter) OutboxLag(float64)                    {}
func (c *counter) DLQ()                                 { c.mu.Lock(); c.dlq++; c.mu.Unlock() }
func (c *counter) Retry(component string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.retries == nil {
		c.retries = map[string]int{}
	}
	c.retries[component]++
}

type rig struct {
	t        *testing.T
	store    *memstore.Store
	wallets  *walletsvc.Service
	queue    *fakeQueue
	metrics  *counter
	consumer *sqsconsumer.Consumer
	cfg      config.Config
	wallet   walletsvc.View
	player   uuid.UUID
}

func newRig(t *testing.T) *rig {
	t.Helper()
	store := memstore.New()
	r := &rig{
		t: t, store: store, wallets: walletsvc.New(store, nil, nil), queue: &fakeQueue{}, metrics: &counter{},
		cfg:    config.Config{SQSMaxMessages: 10, SQSWaitSeconds: 1, SQSVisibilitySeconds: 30, ShutdownTimeout: 5 * time.Second},
		player: uuid.Must(uuid.NewV7()),
	}
	wagers := wagering.New(wagering.Options{Store: store})
	r.consumer = sqsconsumer.NewWorker(r.queue, wagers, nil, r.metrics, r.cfg)
	view, err := r.wallets.Open(context.Background(), r.player, "100.00", "BRL", "")
	if err != nil {
		t.Fatal(err)
	}
	r.wallet = view
	return r
}

func (r *rig) body(messageID, external, kind, amount string) []byte {
	raw, err := json.Marshal(map[string]any{
		"messageId": messageID, "type": "WagerTransactionRequested", "occurredAt": "2026-09-08T12:00:00.000Z",
		"data": map[string]any{
			"providerId": "provider-a", "externalTransactionId": external, "idempotencyKey": "provider-a:" + external,
			"playerId": r.player.String(), "walletId": r.wallet.ID.String(),
			"roundId": "round-1", "gameId": "fortune-chimp", "kind": kind,
			"money": map[string]string{"amount": amount, "currency": "BRL"},
		},
	})
	if err != nil {
		r.t.Fatal(err)
	}
	return raw
}

func (r *rig) drain() {
	r.t.Helper()
	if _, err := r.consumer.ProcessAvailable(context.Background()); err != nil {
		r.t.Fatal(err)
	}
}

func (r *rig) balance() string {
	r.t.Helper()
	view, err := r.wallets.Get(context.Background(), r.wallet.ID)
	if err != nil {
		r.t.Fatal(err)
	}
	return view.Balance.String()
}

func TestMessageIsDeletedOnlyAfterTheCommit(t *testing.T) {
	r := newRig(t)
	r.queue.push("r1", 1, r.body("msg-1", "bet-1", "BET", "30.00"))
	r.drain()
	if !r.queue.wasDeleted("r1") || r.balance() != "70.00" {
		t.Fatalf("deleted=%v balance=%s", r.queue.wasDeleted("r1"), r.balance())
	}
	if row, ok := r.store.Inbox("wager-transactions", "msg-1"); !ok || row.CompletedAt == nil {
		t.Fatalf("inbox %+v", row)
	}
}

func TestRedeliveryIsDeletedWithoutASecondDebit(t *testing.T) {
	r := newRig(t)
	body := r.body("msg-1", "bet-1", "BET", "30.00")
	r.queue.push("r1", 1, body)
	r.queue.push("r2", 2, body)
	r.queue.push("r3", 1, r.body("msg-other", "bet-1", "BET", "30.00"))
	r.drain()
	for _, receipt := range []string{"r1", "r2", "r3"} {
		if !r.queue.wasDeleted(receipt) {
			t.Fatalf("%s not deleted", receipt)
		}
	}
	if r.balance() != "70.00" || len(r.store.Ledger(r.wallet.ID)) != 2 {
		t.Fatalf("balance=%s entries=%d", r.balance(), len(r.store.Ledger(r.wallet.ID)))
	}
}

func TestBusinessRejectionIsAcknowledged(t *testing.T) {
	r := newRig(t)
	r.queue.push("r1", 1, r.body("msg-1", "bet-1", "BET", "500.00"))
	r.drain()
	if !r.queue.wasDeleted("r1") || r.balance() != "100.00" {
		t.Fatal("a recorded rejection must leave the queue")
	}
}

func TestPoisonMessagesAreLeftForTheDLQ(t *testing.T) {
	r := newRig(t)
	valid := r.body("msg-x", "bet-x", "BET", "1.00")
	var unknown map[string]any
	_ = json.Unmarshal(valid, &unknown)
	unknown["extra"] = true
	unknownRaw, _ := json.Marshal(unknown)

	poison := map[string][]byte{
		"p-json":    []byte(`{not json`),
		"p-unknown": unknownRaw,
		"p-type":    []byte(`{"messageId":"m","type":"Other","data":{}}`),
		"p-noid":    []byte(`{"type":"WagerTransactionRequested","data":{}}`),
		"p-player":  []byte(`{"messageId":"m","type":"WagerTransactionRequested","data":{"playerId":"x","walletId":"y"}}`),
		"p-opening": r.body("msg-open", "open-1", "OPENING", "1.00"),
		"p-float":   r.body("msg-float", "bet-f", "BET", "1e2"),
	}
	for receipt, body := range poison {
		r.queue.push(receipt, 1, body)
	}
	r.drain()
	for receipt := range poison {
		if r.queue.wasDeleted(receipt) {
			t.Fatalf("%s was deleted", receipt)
		}
		if v, ok := r.queue.visibilityOf(receipt); !ok || v != 0 {
			t.Fatalf("%s visibility %d %v", receipt, v, ok)
		}
	}
	if r.metrics.dlq != len(poison) || r.balance() != "100.00" {
		t.Fatalf("dlq=%d balance=%s", r.metrics.dlq, r.balance())
	}

	r.queue.push("changed", 2, r.body("msg-1", "bet-1", "BET", "1.00"))
	r.drain()
	r.queue.push("changed-2", 3, r.body("msg-1", "bet-1", "BET", "2.00"))
	r.drain()
	if v, _ := r.queue.visibilityOf("changed-2"); v != 0 || r.queue.wasDeleted("changed-2") {
		t.Fatal("changed body for the same messageId must not be acknowledged")
	}
}

func TestTransientFailureBacksOffAndRetries(t *testing.T) {
	r := newRig(t)
	body := r.body("msg-1", "bet-1", "BET", "30.00")
	for i, want := range map[int]int32{1: 2, 2: 4, 3: 8, 4: 16, 5: 30, 9: 30} {
		receipt := "r" + string(rune('0'+i))
		r.store.FailNext(errors.New("dial tcp: connection refused"))
		r.queue.push(receipt, i, body)
		r.drain()
		if v, ok := r.queue.visibilityOf(receipt); !ok || v != want {
			t.Fatalf("receive %d visibility = %d, want %d", i, v, want)
		}
		if r.queue.wasDeleted(receipt) {
			t.Fatal("failed delivery was deleted")
		}
	}
	if r.metrics.retries["sqs"] != 6 || r.balance() != "100.00" {
		t.Fatalf("retries=%v balance=%s", r.metrics.retries, r.balance())
	}
	r.queue.push("ok", 7, body)
	r.drain()
	if !r.queue.wasDeleted("ok") || r.balance() != "70.00" {
		t.Fatal("retry did not succeed")
	}
}

func TestConsumerStopsOnShutdown(t *testing.T) {
	r := newRig(t)
	r.queue.block = true
	r.queue.arrived = make(chan struct{}, 1)
	lc := fxtest.NewLifecycle(t)
	wagers := wagering.New(wagering.Options{Store: r.store})
	sqsconsumer.New(lc, r.queue, wagers, nil, r.metrics, r.cfg)
	lc.RequireStart()
	r.queue.push("r1", 1, r.body("msg-1", "bet-1", "BET", "10.00"))
	deadline := time.Now().Add(3 * time.Second)
	for !r.queue.wasDeleted("r1") && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !r.queue.wasDeleted("r1") {
		t.Fatal("background loop did not process the message")
	}
	stopped := make(chan struct{})
	go func() {
		lc.RequireStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("consumer did not stop")
	}
	if !r.queue.cancelled.Load() {
		t.Fatal("long poll was not cancelled")
	}
}
