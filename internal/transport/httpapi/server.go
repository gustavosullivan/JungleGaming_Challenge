package httpapi

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jungle-gaming/wagering/internal/platform/config"
	"go.uber.org/fx"
)

type Server struct {
	http *http.Server
	log  *slog.Logger

	mu   sync.RWMutex
	addr string
}

func NewServer(lc fx.Lifecycle, cfg config.Config, handler http.Handler, log *slog.Logger) *Server {
	srv := &Server{
		log: log,
		http: &http.Server{
			Addr:              cfg.HTTPAddr,
			Handler:           handler,
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       15 * time.Second,
			WriteTimeout:      30 * time.Second,
			IdleTimeout:       60 * time.Second,
		},
	}
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			ln, err := net.Listen("tcp", srv.http.Addr)
			if err != nil {
				return err
			}
			srv.mu.Lock()
			srv.addr = ln.Addr().String()
			srv.mu.Unlock()
			go func() {
				err := srv.http.Serve(ln)
				if err != nil && !errors.Is(err, http.ErrServerClosed) && log != nil {
					log.Error("http server", "err", err.Error())
				}
			}()
			if log != nil {
				log.Info("http listening", "addr", ln.Addr().String())
			}
			return nil
		},
		OnStop: func(ctx context.Context) error {
			if log != nil {
				log.Info("http shutting down")
			}
			return srv.http.Shutdown(ctx)
		},
	})
	return srv
}

func (s *Server) Addr() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.addr
}

type correlationKey struct{}

func CorrelationFrom(ctx context.Context) string {
	v, _ := ctx.Value(correlationKey{}).(string)
	return v
}

func withCorrelation(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationKey{}, id)
}

func correlationMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Correlation-Id")
		if id == "" {
			generated, err := uuid.NewV7()
			if err != nil {
				http.Error(w, "internal error", http.StatusInternalServerError)
				return
			}
			id = generated.String()
		}
		w.Header().Set("X-Correlation-Id", id)
		next.ServeHTTP(w, r.WithContext(withCorrelation(r.Context(), id)))
	})
}
