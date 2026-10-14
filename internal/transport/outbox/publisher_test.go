package outbox_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jungle-gaming/wagering/internal/app/port/memstore"
	walletsvc "github.com/jungle-gaming/wagering/internal/app/wallet"
	"github.com/jungle-gaming/wagering/internal/platform/config"
	"github.com/jungle-gaming/wagering/internal/transport/outbox"
	"go.uber.org/fx/fxtest"
)

type bus struct {
	mu    sync.Mutex
	fail  int
	seen  map[string]int
	group map[string]string
}

func (b *bus) Publish(_ context.Context, groupID, dedupID string, _ []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.fail > 0 {
		b.fail--
		return errors.New("broker unavailable")
	}
	if b.seen == nil {
		b.seen = map[string]int{}
		b.group = map[string]string{}
	}
	b.seen[dedupID]++
	b.group[dedupID] = groupID
	return nil
}

func (b *bus) count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.seen)
}

func seed(t *testing.T) (*memstore.Store, walletsvc.View) {
	t.Helper()
	store := memstore.New()
	view, err := walletsvc.New(store, nil, nil).Open(context.Background(), uuid.Must(uuid.NewV7()), "10.00", "BRL", "")
	if err != nil {
		t.Fatal(err)
	}
	return store, view
}

func TestPublishMarksEveryEventOnce(t *testing.T) {
	store, view := seed(t)
	b := &bus{}
	p := outbox.NewWorker(store, b, nil, nil, config.Config{OutboxPoll: time.Second, OutboxLease: 30 * time.Second})
	p.PublishOnce(context.Background())
	p.PublishOnce(context.Background())
	if store.Unpublished() != 0 || b.count() != 2 {
		t.Fatalf("unpublished=%d published=%d", store.Unpublished(), b.count())
	}
	for id, n := range b.seen {
		if n != 1 || b.group[id] != view.ID.String() {
			t.Fatalf("event %s published %d times in group %s", id, n, b.group[id])
		}
	}
}

func TestBrokerFailureIsRescheduled(t *testing.T) {
	store, _ := seed(t)
	b := &bus{fail: 2}
	p := outbox.NewWorker(store, b, nil, nil, config.Config{OutboxPoll: time.Second, OutboxLease: 30 * time.Second})
	p.PublishOnce(context.Background())
	if store.Unpublished() != 2 || b.count() != 0 {
		t.Fatal("failed publish was confirmed")
	}
	p.PublishOnce(context.Background())
	if b.count() != 0 {
		t.Fatal("rescheduled event was retried before its backoff")
	}
	time.Sleep(300 * time.Millisecond)
	p.PublishOnce(context.Background())
	if store.Unpublished() != 0 || b.count() != 2 {
		t.Fatalf("after backoff unpublished=%d published=%d", store.Unpublished(), b.count())
	}
}

func TestInterruptedBatchIsReleasedForAnotherPublisher(t *testing.T) {
	store, _ := seed(t)
	cfg := config.Config{OutboxPoll: time.Second, OutboxLease: 30 * time.Second}
	stopping, cancel := context.WithCancel(context.Background())
	cancel()
	outbox.NewWorker(store, &bus{}, nil, nil, cfg).PublishOnce(stopping)

	other := &bus{}
	outbox.NewWorker(store, other, nil, nil, cfg).PublishOnce(context.Background())
	if store.Unpublished() != 0 || other.count() != 2 {
		t.Fatalf("claimed rows stayed locked: unpublished=%d published=%d", store.Unpublished(), other.count())
	}
}

func TestPublisherLifecycle(t *testing.T) {
	store, _ := seed(t)
	b := &bus{}
	lc := fxtest.NewLifecycle(t)
	outbox.New(lc, store, b, nil, nil, config.Config{OutboxPoll: 20 * time.Millisecond, OutboxLease: 30 * time.Second})
	lc.RequireStart()
	deadline := time.Now().Add(3 * time.Second)
	for store.Unpublished() > 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if store.Unpublished() != 0 {
		t.Fatal("background publisher did not drain the outbox")
	}
	done := make(chan struct{})
	go func() {
		lc.RequireStop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("publisher did not stop")
	}
}
