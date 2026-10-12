package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jungle-gaming/wagering/internal/app/errs"
	"github.com/jungle-gaming/wagering/internal/app/wagering"
	walletsvc "github.com/jungle-gaming/wagering/internal/app/wallet"
	"github.com/jungle-gaming/wagering/internal/domain/money"
	"github.com/jungle-gaming/wagering/internal/domain/wager"
	"github.com/jungle-gaming/wagering/internal/platform/auth"
	"github.com/jungle-gaming/wagering/internal/platform/postgres"
	"github.com/jungle-gaming/wagering/internal/platform/sqs"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Handler struct {
	wallets *walletsvc.Service
	wagers  *wagering.Service
	authn   *auth.Verifier
	pool    *pgxpool.Pool
	queues  *sqs.Client
	metrics http.Handler
	log     *slog.Logger
}

func NewHandler(wallets *walletsvc.Service, wagers *wagering.Service, verifier *auth.Verifier, pool *pgxpool.Pool, queues *sqs.Client, reg *prometheus.Registry, log *slog.Logger) http.Handler {
	h := &Handler{
		wallets: wallets, wagers: wagers, authn: verifier, pool: pool, queues: queues, log: log,
		metrics: promhttp.HandlerFor(reg, promhttp.HandlerOpts{}),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", h.live)
	mux.HandleFunc("GET /health/ready", h.ready)
	mux.Handle("GET /metrics", h.metrics)
	mux.Handle("POST /wallets", h.require(h.openWallet, internalOnly))
	mux.Handle("GET /wallets/{walletId}", h.require(h.getWallet, internalOnly))
	mux.Handle("GET /wallets/{walletId}/ledger", h.require(h.ledger, internalOnly))
	mux.Handle("POST /wallets/{walletId}/reconciliation", h.require(h.reconcile, internalOnly))
	mux.Handle("POST /wagering/transactions", h.require(h.submit, providerOnly))
	mux.Handle("GET /wagering/transactions/{transactionId}", h.require(h.getTransaction, anyRole))
	mux.Handle("GET /providers/{providerId}/wagering/transactions/{externalTransactionId}", h.require(h.getByExternal, anyRole))
	return correlationMiddleware(h.accessLog(mux))
}

func (h *Handler) live(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "live"})
}

func (h *Handler) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := contextTimeout(r, 2*time.Second)
	defer cancel()
	body := map[string]string{"postgres": "ok", "sqs": "ok"}
	status := http.StatusOK
	if err := h.pool.Ping(ctx); err != nil {
		body["postgres"] = "down"
		status = http.StatusServiceUnavailable
	}
	if err := h.queues.Ready(ctx); err != nil {
		body["sqs"] = "down"
		status = http.StatusServiceUnavailable
	}
	if status == http.StatusOK {
		body["status"] = "ready"
	} else {
		body["status"] = "unavailable"
	}
	writeJSON(w, status, body)
}

func (h *Handler) openWallet(w http.ResponseWriter, r *http.Request) {
	var body struct {
		PlayerID       string `json:"playerId"`
		InitialBalance struct {
			Amount   string `json:"amount"`
			Currency string `json:"currency"`
		} `json:"initialBalance"`
	}
	if err := decode(w, r, &body); err != nil {
		writeErr(w, r, h.log, err)
		return
	}
	playerID, err := uuid.Parse(body.PlayerID)
	if err != nil {
		writeErr(w, r, h.log, &errs.Invalid{Reason: "invalid playerId"})
		return
	}
	view, err := h.wallets.Open(r.Context(), playerID, body.InitialBalance.Amount, body.InitialBalance.Currency, CorrelationFrom(r.Context()))
	if err != nil {
		writeErr(w, r, h.log, err)
		return
	}
	writeJSON(w, http.StatusCreated, walletJSON(view))
}

func (h *Handler) getWallet(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("walletId"))
	if err != nil {
		writeErr(w, r, h.log, &errs.Invalid{Reason: "invalid walletId"})
		return
	}
	view, err := h.wallets.Get(r.Context(), id)
	if err != nil {
		writeErr(w, r, h.log, err)
		return
	}
	writeJSON(w, http.StatusOK, walletJSON(view))
}

func (h *Handler) ledger(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("walletId"))
	if err != nil {
		writeErr(w, r, h.log, &errs.Invalid{Reason: "invalid walletId"})
		return
	}
	limit := 50
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			writeErr(w, r, h.log, &errs.Invalid{Reason: "invalid limit"})
			return
		}
		limit = n
	}
	page, err := h.wallets.Ledger(r.Context(), id, r.URL.Query().Get("cursor"), limit)
	if err != nil {
		writeErr(w, r, h.log, err)
		return
	}
	entries := make([]map[string]any, 0, len(page.Entries))
	for _, entry := range page.Entries {
		entries = append(entries, map[string]any{
			"id":            entry.ID().String(),
			"transactionId": entry.TransactionID().String(),
			"direction":     entry.Direction(),
			"money":         moneyJSON(entry.Amount()),
			"balanceBefore": moneyJSON(entry.BalanceBefore()),
			"balanceAfter":  moneyJSON(entry.BalanceAfter()),
			"createdAt":     entry.CreatedAt().UTC().Format(time.RFC3339Nano),
		})
	}
	resp := map[string]any{"entries": entries}
	if page.NextCursor != "" {
		resp["nextCursor"] = page.NextCursor
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) reconcile(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("walletId"))
	if err != nil {
		writeErr(w, r, h.log, &errs.Invalid{Reason: "invalid walletId"})
		return
	}
	result, err := h.wallets.Reconcile(r.Context(), id)
	if err != nil {
		writeErr(w, r, h.log, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"walletId":          result.WalletID.String(),
		"storedBalance":     moneyJSON(result.StoredBalance),
		"calculatedBalance": moneyJSON(result.CalculatedBalance),
		"difference":        moneyJSON(result.Difference),
		"consistent":        result.Consistent,
		"checkedEntries":    result.CheckedEntries,
	})
}

func (h *Handler) submit(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Idempotency-Key")
	var body struct {
		ProviderID            string `json:"providerId"`
		ExternalTransactionID string `json:"externalTransactionId"`
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
	}
	if err := decode(w, r, &body); err != nil {
		writeErr(w, r, h.log, err)
		return
	}
	playerID, err := uuid.Parse(body.PlayerID)
	if err != nil {
		writeErr(w, r, h.log, &errs.Invalid{Reason: "invalid playerId"})
		return
	}
	walletID, err := uuid.Parse(body.WalletID)
	if err != nil {
		writeErr(w, r, h.log, &errs.Invalid{Reason: "invalid walletId"})
		return
	}
	id, _ := auth.FromContext(r.Context())
	result, err := h.wagers.Submit(r.Context(), wagering.Command{
		ProviderID: body.ProviderID, ExternalTransactionID: body.ExternalTransactionID,
		IdempotencyKey: key, PlayerID: playerID, WalletID: walletID,
		RoundID: body.RoundID, GameID: body.GameID, Kind: wager.Kind(body.Kind),
		Amount: body.Money.Amount, Currency: body.Money.Currency,
		ReferenceExternalID: body.ReferenceExternalID,
		CorrelationID:       CorrelationFrom(r.Context()),
		RestrictProvider:    id.ProviderID,
	})
	if err != nil {
		writeErr(w, r, h.log, err)
		return
	}
	writeJSON(w, statusOf(result), resultJSON(result))
}

func (h *Handler) getTransaction(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("transactionId"))
	if err != nil {
		writeErr(w, r, h.log, &errs.Invalid{Reason: "invalid transactionId"})
		return
	}
	tx, err := h.wallets.GetTransaction(r.Context(), id)
	if err != nil {
		writeErr(w, r, h.log, err)
		return
	}
	caller, _ := auth.FromContext(r.Context())
	if !canSee(caller, tx) {
		writeErr(w, r, h.log, &errs.NotFound{Reason: "transaction not found"})
		return
	}
	writeJSON(w, http.StatusOK, transactionJSON(tx))
}

func (h *Handler) getByExternal(w http.ResponseWriter, r *http.Request) {
	providerID := r.PathValue("providerId")
	externalID := r.PathValue("externalTransactionId")
	caller, _ := auth.FromContext(r.Context())
	if caller.Provider && !caller.Internal && caller.ProviderID != providerID {
		writeErr(w, r, h.log, &errs.Forbidden{Reason: "provider cannot read another provider"})
		return
	}
	tx, err := h.wallets.GetByExternal(r.Context(), providerID, externalID)
	if err != nil {
		writeErr(w, r, h.log, err)
		return
	}
	if !canSee(caller, tx) {
		writeErr(w, r, h.log, &errs.NotFound{Reason: "transaction not found"})
		return
	}
	writeJSON(w, http.StatusOK, transactionJSON(tx))
}

func canSee(caller auth.Identity, tx *wager.Transaction) bool {
	if caller.Internal {
		return true
	}
	if tx.Origin() == wager.OriginInternal || tx.ProviderID() == "" {
		return false
	}
	return caller.Provider && caller.ProviderID == tx.ProviderID()
}

func internalOnly(id auth.Identity) bool { return id.Internal }
func providerOnly(id auth.Identity) bool { return id.Provider }
func anyRole(id auth.Identity) bool      { return id.Internal || id.Provider }

func (h *Handler) require(next http.HandlerFunc, allow func(auth.Identity) bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, ok := auth.Bearer(r)
		if !ok {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized", "message": "missing bearer token"})
			return
		}
		id, err := h.authn.Verify(r.Context(), raw)
		if err != nil {
			if h.log != nil {
				h.log.Info("authentication failed", "correlationId", CorrelationFrom(r.Context()), "err", err.Error())
			}
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized", "message": "invalid token"})
			return
		}
		if !allow(id) {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "forbidden", "message": "insufficient permission"})
			return
		}
		next(w, r.WithContext(auth.WithIdentity(r.Context(), id)))
	})
}

func (h *Handler) accessLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		if h.log == nil || r.URL.Path == "/metrics" {
			return
		}
		h.log.Info("request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", rec.status,
			"durationMs", time.Since(start).Milliseconds(),
			"correlationId", CorrelationFrom(r.Context()),
		)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

func decode(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<16)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return &errs.Invalid{Reason: "invalid json body"}
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return &errs.Invalid{Reason: "invalid json body"}
	}
	return nil
}

func writeErr(w http.ResponseWriter, r *http.Request, log *slog.Logger, err error) {
	var invalid *errs.Invalid
	var forbidden *errs.Forbidden
	var conflict *errs.Conflict
	var missing *errs.NotFound
	var unavailable *errs.Unavailable
	switch {
	case errors.As(err, &invalid):
		writeJSON(w, http.StatusBadRequest, errBody("invalid_request", invalid.Reason))
	case errors.As(err, &forbidden):
		writeJSON(w, http.StatusForbidden, errBody("forbidden", forbidden.Reason))
	case errors.As(err, &conflict):
		writeJSON(w, http.StatusConflict, errBody("conflict", conflict.Reason))
	case errors.As(err, &missing):
		writeJSON(w, http.StatusNotFound, errBody("not_found", missing.Reason))
	case errors.As(err, &unavailable), postgres.IsTransient(err):
		writeJSON(w, http.StatusServiceUnavailable, errBody("unavailable", "temporary infrastructure failure"))
	default:
		if log != nil {
			log.Error("request failed", "correlationId", CorrelationFrom(r.Context()), "err", err.Error())
		}
		writeJSON(w, http.StatusInternalServerError, errBody("internal", "internal error"))
	}
}

func errBody(code, message string) map[string]string {
	return map[string]string{"error": code, "message": message}
}

func statusOf(res wagering.Result) int {
	switch res.Status {
	case wager.StatusProcessed:
		return http.StatusOK
	case wager.StatusPending, wager.StatusPendingReference:
		return http.StatusAccepted
	case wager.StatusRejected:
		return http.StatusUnprocessableEntity
	default:
		return http.StatusInternalServerError
	}
}

func resultJSON(res wagering.Result) map[string]any {
	body := map[string]any{
		"transactionId":    res.TransactionID.String(),
		"status":           res.Status,
		"idempotentReplay": res.IdempotentReplay,
	}
	if res.FailureCode != "" {
		body["failureCode"] = res.FailureCode
	}
	if res.Balance != nil {
		body["balance"] = moneyJSON(*res.Balance)
	}
	return body
}

func walletJSON(view walletsvc.View) map[string]any {
	return map[string]any{
		"id":        view.ID.String(),
		"playerId":  view.PlayerID.String(),
		"balance":   moneyJSON(view.Balance),
		"version":   view.Version,
		"createdAt": view.CreatedAt.UTC().Format(time.RFC3339Nano),
		"updatedAt": view.UpdatedAt.UTC().Format(time.RFC3339Nano),
	}
}

func transactionJSON(tx *wager.Transaction) map[string]any {
	body := map[string]any{
		"transactionId": tx.ID().String(),
		"origin":        tx.Origin(),
		"kind":          tx.Kind(),
		"status":        tx.Status(),
		"walletId":      tx.WalletID().String(),
		"playerId":      tx.PlayerID().String(),
		"money":         moneyJSON(tx.Amount()),
		"createdAt":     tx.CreatedAt().UTC().Format(time.RFC3339Nano),
		"updatedAt":     tx.UpdatedAt().UTC().Format(time.RFC3339Nano),
	}
	if tx.ProviderID() != "" {
		body["providerId"] = tx.ProviderID()
	}
	if tx.ExternalTransactionID() != "" {
		body["externalTransactionId"] = tx.ExternalTransactionID()
	}
	if tx.RoundID() != "" {
		body["roundId"] = tx.RoundID()
	}
	if tx.GameID() != "" {
		body["gameId"] = tx.GameID()
	}
	if tx.ReferenceExternalID() != "" {
		body["referenceExternalTransactionId"] = tx.ReferenceExternalID()
	}
	if ref := tx.ReferenceTransactionID(); ref != nil {
		body["referenceTransactionId"] = ref.String()
	}
	if tx.FailureCode() != "" {
		body["failureCode"] = tx.FailureCode()
	}
	if bal := tx.ResultBalance(); bal != nil {
		body["balance"] = moneyJSON(*bal)
	}
	return body
}

type moneyDTO struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

func moneyJSON(m money.Money) moneyDTO {
	return moneyDTO{Amount: m.String(), Currency: m.Currency()}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func contextTimeout(r *http.Request, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(r.Context(), d)
}
