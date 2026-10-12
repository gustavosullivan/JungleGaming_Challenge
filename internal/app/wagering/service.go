package wagering

import (
	"context"
	"errors"
	"log/slog"
	"regexp"
	"time"

	"github.com/google/uuid"
	"github.com/jungle-gaming/wagering/internal/app/errs"
	"github.com/jungle-gaming/wagering/internal/app/observe"
	"github.com/jungle-gaming/wagering/internal/app/port"
	"github.com/jungle-gaming/wagering/internal/domain/idempotency"
	"github.com/jungle-gaming/wagering/internal/domain/money"
	"github.com/jungle-gaming/wagering/internal/domain/wager"
)

const consumerName = "wager-transactions"

var (
	providerPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)
	tokenPattern    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
)

type Service struct {
	db          port.Store
	log         *slog.Logger
	metrics     observe.Recorder
	now         func() time.Time
	maxAttempts int
	ttl         time.Duration
	backoff     time.Duration
	poll        time.Duration
}

type Options struct {
	Store        port.Store
	Log          *slog.Logger
	Metrics      observe.Recorder
	Now          func() time.Time
	MaxAttempts  int
	TTL          time.Duration
	Backoff      time.Duration
	PollInterval time.Duration
}

func New(opts Options) *Service {
	metrics := opts.Metrics
	if metrics == nil {
		metrics = observe.Nop{}
	}
	if opts.MaxAttempts < 1 {
		opts.MaxAttempts = 8
	}
	if opts.TTL <= 0 {
		opts.TTL = 2 * time.Minute
	}
	if opts.Backoff <= 0 {
		opts.Backoff = 200 * time.Millisecond
	}
	if opts.PollInterval <= 0 {
		opts.PollInterval = 500 * time.Millisecond
	}
	return &Service{
		db: opts.Store, log: opts.Log, metrics: metrics, now: opts.Now,
		maxAttempts: opts.MaxAttempts, ttl: opts.TTL, backoff: opts.Backoff, poll: opts.PollInterval,
	}
}

type Command struct {
	ProviderID            string
	ExternalTransactionID string
	IdempotencyKey        string
	PlayerID              uuid.UUID
	WalletID              uuid.UUID
	RoundID               string
	GameID                string
	Kind                  wager.Kind
	Amount                string
	Currency              string
	ReferenceExternalID   string
	CorrelationID         string
	RestrictProvider      string

	money money.Money
	hash  string
}

type Result struct {
	TransactionID    uuid.UUID
	Status           wager.Status
	FailureCode      string
	Balance          *money.Money
	IdempotentReplay bool
	Kind             wager.Kind
}

func (s *Service) Submit(ctx context.Context, cmd Command) (Result, error) {
	if err := prepare(&cmd); err != nil {
		return Result{}, err
	}
	started := time.Now()
	var last error
	for attempt := 0; attempt < 3; attempt++ {
		res, err := s.submitOnce(ctx, cmd)
		if err == nil {
			s.observe(res, time.Since(started))
			s.logResult(cmd, res)
			return res, nil
		}
		var conflict *port.Conflict
		if errors.As(err, &conflict) && (conflict.Constraint == "ux_wager_idempotency" || conflict.Constraint == "ux_wager_external") {
			res, rerr := s.readExisting(ctx, cmd)
			if rerr != nil {
				return Result{}, rerr
			}
			s.observe(res, time.Since(started))
			s.logResult(cmd, res)
			return res, nil
		}
		if errors.As(err, &conflict) && conflict.Constraint == "ux_wager_one_reversal" {
			last = err
			continue
		}
		if errors.Is(err, port.ErrConcurrentUpdate) {
			s.metrics.Conflict()
			last = err
			continue
		}
		return Result{}, err
	}
	if last == nil {
		last = &errs.Unavailable{Reason: "could not apply the operation"}
	}
	return Result{}, last
}

func (s *Service) submitOnce(ctx context.Context, cmd Command) (Result, error) {
	var result Result
	err := s.db.WithinTx(ctx, func(ctx context.Context, tx port.Tx) error {
		res, err := s.submitInTx(ctx, tx, cmd)
		if err != nil {
			return err
		}
		result = res
		return nil
	})
	return result, err
}

func (s *Service) submitInTx(ctx context.Context, tx port.Tx, cmd Command) (Result, error) {
	existing, err := tx.FindWagerByIdempotency(ctx, cmd.IdempotencyKey)
	if err != nil && !errors.Is(err, port.ErrNotFound) {
		return Result{}, err
	}
	if existing != nil {
		return replay(existing, cmd)
	}
	byExternal, err := tx.FindWagerByExternal(ctx, cmd.ProviderID, cmd.ExternalTransactionID)
	if err != nil && !errors.Is(err, port.ErrNotFound) {
		return Result{}, err
	}
	if byExternal != nil {
		return Result{}, &errs.Conflict{Reason: "external transaction already recorded with another idempotency key"}
	}

	w, err := tx.LockWallet(ctx, cmd.WalletID, port.LockUpdate)
	if errors.Is(err, port.ErrNotFound) {
		return Result{}, &errs.NotFound{Reason: "wallet not found"}
	}
	if err != nil {
		return Result{}, err
	}
	existing, err = tx.FindWagerByIdempotency(ctx, cmd.IdempotencyKey)
	if err != nil && !errors.Is(err, port.ErrNotFound) {
		return Result{}, err
	}
	if existing != nil {
		return replay(existing, cmd)
	}

	id, err := uuid.NewV7()
	if err != nil {
		return Result{}, err
	}
	op, err := wager.NewExternal(wager.ExternalInput{
		ID: id, Kind: cmd.Kind, WalletID: cmd.WalletID, PlayerID: cmd.PlayerID, Amount: cmd.money,
		ProviderID: cmd.ProviderID, ExternalTransactionID: cmd.ExternalTransactionID,
		IdempotencyKey: cmd.IdempotencyKey, PayloadHash: cmd.hash, RoundID: cmd.RoundID, GameID: cmd.GameID,
		ReferenceExternalID: cmd.ReferenceExternalID, Now: s.clock(),
	})
	if err != nil {
		return Result{}, &errs.Invalid{Reason: err.Error()}
	}
	return s.apply(ctx, tx, w, op, cmd.CorrelationID, false)
}

func replay(existing *wager.Transaction, cmd Command) (Result, error) {
	if cmd.RestrictProvider != "" && existing.ProviderID() != "" && existing.ProviderID() != cmd.RestrictProvider {
		return Result{}, &errs.Forbidden{Reason: "transaction belongs to another provider"}
	}
	if existing.PayloadHash() != cmd.hash {
		return Result{}, &errs.Conflict{Reason: "idempotency key reused with a different payload"}
	}
	return toResult(existing, true), nil
}

func (s *Service) readExisting(ctx context.Context, cmd Command) (Result, error) {
	existing, err := s.db.GetWagerByIdempotency(ctx, cmd.IdempotencyKey)
	if errors.Is(err, port.ErrNotFound) {
		existing, err = s.db.GetWagerByExternal(ctx, cmd.ProviderID, cmd.ExternalTransactionID)
	}
	if errors.Is(err, port.ErrNotFound) {
		return Result{}, &errs.Unavailable{Reason: "conflicting write rolled back; retry"}
	}
	if err != nil {
		return Result{}, err
	}
	if existing.IdempotencyKey() != cmd.IdempotencyKey {
		return Result{}, &errs.Conflict{Reason: "external transaction already recorded with another idempotency key"}
	}
	return replay(existing, cmd)
}

func toResult(tx *wager.Transaction, replayed bool) Result {
	return Result{
		TransactionID:    tx.ID(),
		Status:           tx.Status(),
		FailureCode:      tx.FailureCode(),
		Balance:          tx.ResultBalance(),
		IdempotentReplay: replayed,
		Kind:             tx.Kind(),
	}
}

func prepare(cmd *Command) error {
	if cmd.Kind == wager.KindOpening || !validKind(cmd.Kind) {
		return &errs.Invalid{Reason: "kind is not accepted"}
	}
	if cmd.PlayerID == uuid.Nil || cmd.WalletID == uuid.Nil {
		return &errs.Invalid{Reason: "playerId and walletId are required"}
	}
	if !providerPattern.MatchString(cmd.ProviderID) {
		return &errs.Invalid{Reason: "invalid providerId"}
	}
	if !tokenPattern.MatchString(cmd.ExternalTransactionID) || !tokenPattern.MatchString(cmd.RoundID) || !tokenPattern.MatchString(cmd.GameID) {
		return &errs.Invalid{Reason: "invalid externalTransactionId, roundId or gameId"}
	}
	if cmd.IdempotencyKey == "" || len(cmd.IdempotencyKey) > 255 || containsSpace(cmd.IdempotencyKey) {
		return &errs.Invalid{Reason: "invalid Idempotency-Key"}
	}
	if cmd.RestrictProvider != "" && cmd.RestrictProvider != cmd.ProviderID {
		return &errs.Forbidden{Reason: "provider is not allowed to submit for this providerId"}
	}
	switch cmd.Kind {
	case wager.KindRefund, wager.KindRollback:
		if cmd.ReferenceExternalID == "" {
			return &errs.Invalid{Reason: "referenceExternalTransactionId is required"}
		}
		if !tokenPattern.MatchString(cmd.ReferenceExternalID) {
			return &errs.Invalid{Reason: "invalid referenceExternalTransactionId"}
		}
	case wager.KindBet, wager.KindLoss:
		if cmd.ReferenceExternalID != "" {
			return &errs.Invalid{Reason: "reference is not allowed for this kind"}
		}
	default:
		if cmd.ReferenceExternalID != "" && !tokenPattern.MatchString(cmd.ReferenceExternalID) {
			return &errs.Invalid{Reason: "invalid referenceExternalTransactionId"}
		}
	}
	parsed, err := money.Parse(cmd.Amount, cmd.Currency)
	if err != nil {
		return &errs.Invalid{Reason: "invalid money"}
	}
	sum, err := idempotency.Hash(idempotency.Fields{
		ProviderID: cmd.ProviderID, ExternalTransactionID: cmd.ExternalTransactionID,
		PlayerID: cmd.PlayerID.String(), WalletID: cmd.WalletID.String(),
		RoundID: cmd.RoundID, GameID: cmd.GameID, Kind: string(cmd.Kind),
		Amount: parsed.String(), Currency: parsed.Currency(),
		ReferenceExternalID: cmd.ReferenceExternalID,
	})
	if err != nil {
		return &errs.Invalid{Reason: "invalid money"}
	}
	cmd.money = parsed
	cmd.hash = sum
	if cmd.CorrelationID == "" {
		id, err := uuid.NewV7()
		if err != nil {
			return err
		}
		cmd.CorrelationID = id.String()
	}
	return nil
}

func validKind(k wager.Kind) bool {
	switch k {
	case wager.KindBet, wager.KindWin, wager.KindLoss, wager.KindRefund, wager.KindRollback:
		return true
	default:
		return false
	}
}

func containsSpace(s string) bool {
	for _, r := range s {
		if r == ' ' || r == '\n' || r == '\t' || r == '\r' {
			return true
		}
	}
	return false
}

func (s *Service) clock() time.Time {
	now := time.Now()
	if s.now != nil {
		now = s.now()
	}
	return now.UTC().Truncate(time.Millisecond)
}

func (s *Service) observe(res Result, elapsed time.Duration) {
	if res.IdempotentReplay {
		s.metrics.Replay()
	}
	kind := string(res.Kind)
	if kind == "" {
		kind = "unknown"
	}
	s.metrics.Submit(kind, string(res.Status), elapsed)
}

func (s *Service) logResult(cmd Command, res Result) {
	if s.log == nil {
		return
	}
	s.log.Info("wager result",
		"correlationId", cmd.CorrelationID,
		"transactionId", res.TransactionID.String(),
		"walletId", cmd.WalletID.String(),
		"providerId", cmd.ProviderID,
		"kind", res.Kind,
		"status", res.Status,
		"failureCode", res.FailureCode,
		"replay", res.IdempotentReplay,
	)
}
