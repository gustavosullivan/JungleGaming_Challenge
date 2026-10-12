package outbox

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jungle-gaming/wagering/internal/app/observe"
	"github.com/jungle-gaming/wagering/internal/app/port"
	"github.com/jungle-gaming/wagering/internal/platform/config"
	"go.uber.org/fx"
)

type Publisher struct {
	db      port.Store
	bus     port.Publisher
	log     *slog.Logger
	metrics observe.Recorder
	poll    time.Duration
	lease   time.Duration
	worker  string
	cancel  context.CancelFunc
	stopped chan struct{}
}

func NewWorker(db port.Store, bus port.Publisher, log *slog.Logger, metrics observe.Recorder, cfg config.Config) *Publisher {
	if metrics == nil {
		metrics = observe.Nop{}
	}
	host, _ := os.Hostname()
	id, _ := uuid.NewV7()
	return &Publisher{
		db: db, bus: bus, log: log, metrics: metrics,
		poll: cfg.OutboxPoll, lease: cfg.OutboxLease,
		worker:  host + "-" + id.String()[:8],
		stopped: make(chan struct{}),
	}
}

func New(lc fx.Lifecycle, db port.Store, bus port.Publisher, log *slog.Logger, metrics observe.Recorder, cfg config.Config) *Publisher {
	p := NewWorker(db, bus, log, metrics, cfg)
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			ctx, cancel := context.WithCancel(context.Background())
			p.cancel = cancel
			go p.loop(ctx)
			return nil
		},
		OnStop: func(ctx context.Context) error {
			if p.cancel != nil {
				p.cancel()
			}
			select {
			case <-p.stopped:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	})
	return p
}

func (p *Publisher) loop(ctx context.Context) {
	defer close(p.stopped)
	ticker := time.NewTicker(p.poll)
	defer ticker.Stop()
	p.PublishOnce(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.PublishOnce(ctx)
		}
	}
}

func (p *Publisher) PublishOnce(ctx context.Context) {
	if lag, err := p.db.OutboxLag(ctx); err == nil {
		p.metrics.OutboxLag(lag.Seconds())
	}
	msgs, err := p.db.ClaimOutbox(ctx, p.worker, 20, p.lease)
	if err != nil {
		if ctx.Err() == nil && p.log != nil {
			p.log.Error("claim outbox", "err", err.Error())
		}
		return
	}
	for _, msg := range msgs {
		if ctx.Err() != nil {
			return
		}
		err := p.bus.Publish(ctx, msg.AggregateID, msg.EventID.String(), msg.Payload)
		if err != nil {
			p.metrics.Retry("outbox")
			next := time.Now().UTC().Add(retryDelay(msg.Attempts))
			if rerr := p.db.RescheduleOutbox(ctx, msg.EventID, next); rerr != nil && p.log != nil {
				p.log.Error("reschedule outbox", "eventId", msg.EventID.String(), "err", rerr.Error())
			}
			continue
		}
		if err := p.db.MarkOutboxPublished(ctx, msg.EventID, time.Now().UTC()); err != nil && p.log != nil {
			p.log.Error("confirm outbox", "eventId", msg.EventID.String(), "err", err.Error())
		}
	}
}

func retryDelay(attempts int) time.Duration {
	delay := 200 * time.Millisecond
	for i := 1; i < attempts; i++ {
		delay *= 2
		if delay >= time.Minute {
			return time.Minute
		}
	}
	return delay
}
