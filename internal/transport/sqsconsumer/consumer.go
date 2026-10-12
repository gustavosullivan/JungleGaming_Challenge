package sqsconsumer

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jungle-gaming/wagering/internal/app/errs"
	"github.com/jungle-gaming/wagering/internal/app/observe"
	"github.com/jungle-gaming/wagering/internal/app/port"
	"github.com/jungle-gaming/wagering/internal/app/wagering"
	"github.com/jungle-gaming/wagering/internal/domain/wager"
	"github.com/jungle-gaming/wagering/internal/platform/config"
	"github.com/jungle-gaming/wagering/internal/platform/postgres"
	"go.uber.org/fx"
)

type Consumer struct {
	queues  port.Queues
	svc     *wagering.Service
	log     *slog.Logger
	metrics observe.Recorder
	cfg     config.Config

	mu      sync.Mutex
	active  string
	cancel  context.CancelFunc
	stopped chan struct{}
}

func NewWorker(queues port.Queues, svc *wagering.Service, log *slog.Logger, metrics observe.Recorder, cfg config.Config) *Consumer {
	if metrics == nil {
		metrics = observe.Nop{}
	}
	return &Consumer{queues: queues, svc: svc, log: log, metrics: metrics, cfg: cfg, stopped: make(chan struct{})}
}

func New(lc fx.Lifecycle, queues port.Queues, svc *wagering.Service, log *slog.Logger, metrics observe.Recorder, cfg config.Config) *Consumer {
	c := NewWorker(queues, svc, log, metrics, cfg)
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			ctx, cancel := context.WithCancel(context.Background())
			c.cancel = cancel
			go c.loop(ctx)
			return nil
		},
		OnStop: func(ctx context.Context) error {
			if c.cancel != nil {
				c.cancel()
			}
			select {
			case <-c.stopped:
				return nil
			case <-ctx.Done():
				c.releaseActive()
				return ctx.Err()
			}
		},
	})
	return c
}

// ProcessAvailable receives a single batch and handles it. Shutdown stops
// the long poll; callers use this from tests and one-shot drains.
func (c *Consumer) ProcessAvailable(ctx context.Context) (int, error) {
	msgs, err := c.queues.Receive(ctx, c.cfg.SQSMaxMessages, c.cfg.SQSWaitSeconds, c.cfg.SQSVisibilitySeconds)
	if err != nil {
		return 0, err
	}
	for _, msg := range msgs {
		c.handle(ctx, msg)
	}
	return len(msgs), nil
}

func (c *Consumer) loop(ctx context.Context) {
	defer close(c.stopped)
	for ctx.Err() == nil {
		msgs, err := c.queues.Receive(ctx, c.cfg.SQSMaxMessages, c.cfg.SQSWaitSeconds, c.cfg.SQSVisibilitySeconds)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if c.log != nil {
				c.log.Error("sqs receive", "err", err.Error())
			}
			c.metrics.Retry("sqs")
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		for _, msg := range msgs {
			if ctx.Err() != nil {
				return
			}
			c.handle(ctx, msg)
		}
	}
}

func (c *Consumer) handle(ctx context.Context, msg port.Incoming) {
	c.mu.Lock()
	c.active = msg.Receipt
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.active = ""
		c.mu.Unlock()
	}()

	work, cancel := context.WithTimeout(context.Background(), c.cfg.ShutdownTimeout)
	defer cancel()
	if ctx.Err() != nil {
		return
	}
	cmd, messageID, err := parse(msg.Body)
	if err != nil {
		c.permanent(work, msg, err)
		return
	}
	err = c.svc.AcceptDelivery(work, messageID, msg.Body, cmd)
	if err == nil {
		if delErr := c.queues.Delete(work, msg.Receipt); delErr != nil && c.log != nil {
			c.log.Error("sqs delete", "messageId", messageID, "err", delErr.Error())
		}
		return
	}
	if ctx.Err() != nil || work.Err() != nil {
		_ = c.queues.ChangeVisibility(context.Background(), msg.Receipt, 0)
		return
	}
	if errs.IsPermanent(err) {
		c.permanent(work, msg, err)
		return
	}
	if postgres.IsTransient(err) || errors.Is(err, context.DeadlineExceeded) {
		c.metrics.Retry("sqs")
	}
	delay := visibilityBackoff(msg.ReceiveCount)
	_ = c.queues.ChangeVisibility(work, msg.Receipt, delay)
	if c.log != nil {
		c.log.Warn("sqs retry", "messageId", messageID, "err", err.Error(), "visibilitySeconds", delay)
	}
}

func (c *Consumer) permanent(ctx context.Context, msg port.Incoming, err error) {
	c.metrics.DLQ()
	if c.log != nil {
		c.log.Error("sqs permanent failure", "err", err.Error(), "receiveCount", msg.ReceiveCount)
	}
	_ = c.queues.ChangeVisibility(ctx, msg.Receipt, 0)
}

func (c *Consumer) releaseActive() {
	c.mu.Lock()
	receipt := c.active
	c.mu.Unlock()
	if receipt == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = c.queues.ChangeVisibility(ctx, receipt, 0)
}

func visibilityBackoff(receiveCount int) int32 {
	delay := int32(2)
	for i := 1; i < receiveCount; i++ {
		delay *= 2
		if delay >= 30 {
			return 30
		}
	}
	return delay
}

type envelope struct {
	MessageID     string `json:"messageId"`
	Type          string `json:"type"`
	OccurredAt    string `json:"occurredAt"`
	CorrelationID string `json:"correlationId"`
	Data          struct {
		ProviderID            string `json:"providerId"`
		ExternalTransactionID string `json:"externalTransactionId"`
		IdempotencyKey        string `json:"idempotencyKey"`
		PlayerID              string `json:"playerId"`
		WalletID              string `json:"walletId"`
		RoundID               string `json:"roundId"`
		GameID                string `json:"gameId"`
		Kind                  string `json:"kind"`
		Money                 struct {
			Amount   string `json:"amount"`
			Currency string `json:"currency"`
		} `json:"money"`
		ReferenceExternalID string `json:"referenceExternalTransactionId"`
	} `json:"data"`
}

func parse(body []byte) (wagering.Command, string, error) {
	dec := json.NewDecoder(bytesReader(body))
	dec.DisallowUnknownFields()
	var env envelope
	if err := dec.Decode(&env); err != nil {
		return wagering.Command{}, "", &errs.Permanent{Err: err}
	}
	if env.Type != "WagerTransactionRequested" || env.MessageID == "" {
		return wagering.Command{}, "", &errs.Permanent{Err: errors.New("invalid wager envelope")}
	}
	playerID, err := uuid.Parse(env.Data.PlayerID)
	if err != nil {
		return wagering.Command{}, "", &errs.Permanent{Err: errors.New("invalid playerId")}
	}
	walletID, err := uuid.Parse(env.Data.WalletID)
	if err != nil {
		return wagering.Command{}, "", &errs.Permanent{Err: errors.New("invalid walletId")}
	}
	cmd := wagering.Command{
		ProviderID: env.Data.ProviderID, ExternalTransactionID: env.Data.ExternalTransactionID,
		IdempotencyKey: env.Data.IdempotencyKey, PlayerID: playerID, WalletID: walletID,
		RoundID: env.Data.RoundID, GameID: env.Data.GameID, Kind: wager.Kind(env.Data.Kind),
		Amount: env.Data.Money.Amount, Currency: env.Data.Money.Currency,
		ReferenceExternalID: env.Data.ReferenceExternalID, CorrelationID: env.CorrelationID,
	}
	if cmd.CorrelationID == "" {
		cmd.CorrelationID = env.MessageID
	}
	return cmd, env.MessageID, nil
}
