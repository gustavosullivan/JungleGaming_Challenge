package fxapp

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jungle-gaming/wagering/internal/app/observe"
	"github.com/jungle-gaming/wagering/internal/app/port"
	"github.com/jungle-gaming/wagering/internal/app/wagering"
	walletsvc "github.com/jungle-gaming/wagering/internal/app/wallet"
	"github.com/jungle-gaming/wagering/internal/platform/auth"
	"github.com/jungle-gaming/wagering/internal/platform/config"
	"github.com/jungle-gaming/wagering/internal/platform/metrics"
	"github.com/jungle-gaming/wagering/internal/platform/postgres"
	"github.com/jungle-gaming/wagering/internal/platform/sqs"
	"github.com/jungle-gaming/wagering/internal/transport/httpapi"
	"github.com/jungle-gaming/wagering/internal/transport/outbox"
	"github.com/jungle-gaming/wagering/internal/transport/sqsconsumer"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/fx"
)

func New() *fx.App {
	return fx.New(
		fx.StopTimeout(20*time.Second),
		Options(),
	)
}

func Options() fx.Option {
	return fx.Options(
		fx.Provide(config.Load),
		fx.Provide(newLogger),
		fx.Provide(metrics.New),
		fx.Provide(func(rec *metrics.Recorder) observe.Recorder { return rec }),
		fx.Provide(newRegistry),
		fx.Provide(newPool),
		fx.Provide(postgres.NewStore),
		fx.Provide(func(s *postgres.Store) port.Store { return s }),
		fx.Provide(sqs.New),
		fx.Provide(func(c *sqs.Client) port.Queues { return c }),
		fx.Provide(func(c *sqs.Client) port.Publisher { return c }),
		fx.Provide(newVerifier),
		fx.Provide(newWagering),
		fx.Provide(newWallets),
		fx.Provide(httpapi.NewHandler),
		fx.Provide(httpapi.NewServer),
		fx.Provide(sqsconsumer.New),
		fx.Provide(outbox.New),
		fx.Provide(newReferenceLoop),
		fx.Invoke(func(*httpapi.Server, *sqsconsumer.Consumer, *outbox.Publisher, *referenceLoop) {}),
	)
}

func newLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
}

func newRegistry(rec *metrics.Recorder) (*prometheus.Registry, error) {
	reg := prometheus.NewRegistry()
	if err := rec.Register(reg); err != nil {
		return nil, err
	}
	return reg, nil
}

func newPool(lc fx.Lifecycle, cfg config.Config) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(context.Background(), cfg.DatabaseURL)
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error { return pool.Ping(ctx) },
		OnStop: func(context.Context) error {
			pool.Close()
			return nil
		},
	})
	return pool, nil
}

func newVerifier(cfg config.Config) *auth.Verifier {
	return auth.NewVerifier(cfg.OIDCIssuer, cfg.OIDCJWKSURL, cfg.OIDCAudience)
}

func newWagering(store port.Store, log *slog.Logger, rec observe.Recorder, cfg config.Config) *wagering.Service {
	return wagering.New(wagering.Options{
		Store: store, Log: log, Metrics: rec,
		MaxAttempts: cfg.ReferenceMaxAttempts, TTL: cfg.ReferenceTTL,
		Backoff: cfg.ReferenceBackoff, PollInterval: cfg.ReferencePoll,
	})
}

func newWallets(store port.Store, log *slog.Logger, rec observe.Recorder) *walletsvc.Service {
	return walletsvc.New(store, log, rec)
}

type referenceLoop struct {
	cancel  context.CancelFunc
	stopped chan struct{}
}

func newReferenceLoop(lc fx.Lifecycle, svc *wagering.Service) *referenceLoop {
	loop := &referenceLoop{stopped: make(chan struct{})}
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			ctx, cancel := context.WithCancel(context.Background())
			loop.cancel = cancel
			go func() {
				defer close(loop.stopped)
				svc.RunReferenceLoop(ctx)
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			if loop.cancel != nil {
				loop.cancel()
			}
			select {
			case <-loop.stopped:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	})
	return loop
}
