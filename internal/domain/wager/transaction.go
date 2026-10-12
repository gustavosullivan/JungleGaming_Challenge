package wager

import (
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jungle-gaming/wagering/internal/domain/money"
)

type Origin string
type Kind string
type Status string

const (
	OriginInternal Origin = "INTERNAL"
	OriginExternal Origin = "EXTERNAL"

	KindOpening  Kind = "OPENING"
	KindBet      Kind = "BET"
	KindWin      Kind = "WIN"
	KindLoss     Kind = "LOSS"
	KindRefund   Kind = "REFUND"
	KindRollback Kind = "ROLLBACK"

	StatusPending          Status = "PENDING"
	StatusPendingReference Status = "PENDING_REFERENCE"
	StatusProcessed        Status = "PROCESSED"
	StatusRejected         Status = "REJECTED"
	StatusFailed           Status = "FAILED"
)

const (
	CodeInsufficientBalance         = "INSUFFICIENT_BALANCE"
	CodeReversalInsufficientBalance = "REVERSAL_INSUFFICIENT_BALANCE"
	CodeInvalidAmount               = "INVALID_AMOUNT"
	CodeCurrencyMismatch            = "CURRENCY_MISMATCH"
	CodePlayerWalletMismatch        = "PLAYER_WALLET_MISMATCH"
	CodeReferenceNotFound           = "REFERENCE_NOT_FOUND"
	CodeReferenceNotSuccessful      = "REFERENCE_NOT_SUCCESSFUL"
	CodeReferenceMismatch           = "REFERENCE_MISMATCH"
	CodeReferenceAmountMismatch     = "REFERENCE_AMOUNT_MISMATCH"
	CodeInvalidReferenceKind        = "INVALID_REFERENCE_KIND"
	CodeAlreadyReversed             = "ALREADY_REVERSED"
	CodeReversalInProgress          = "REVERSAL_IN_PROGRESS"
)

var (
	ErrInvalid  = errors.New("invalid wager transaction")
	ErrTerminal = errors.New("transaction is terminal")
	ErrKind     = errors.New("kind is not allowed on this channel")
)

// Transaction is the recorded wager operation. New* starts at PENDING.
// Rehydrate restores a row and does not replay the movement.
type Transaction struct {
	id                     uuid.UUID
	origin                 Origin
	kind                   Kind
	status                 Status
	walletID               uuid.UUID
	playerID               uuid.UUID
	amount                 money.Money
	providerID             string
	externalTransactionID  string
	idempotencyKey         string
	payloadHash            string
	roundID                string
	gameID                 string
	referenceExternalID    string
	referenceTransactionID *uuid.UUID
	failureCode            string
	resultBalance          *money.Money
	createdAt              time.Time
	updatedAt              time.Time
}

type ExternalInput struct {
	ID                    uuid.UUID
	Kind                  Kind
	WalletID              uuid.UUID
	PlayerID              uuid.UUID
	Amount                money.Money
	ProviderID            string
	ExternalTransactionID string
	IdempotencyKey        string
	PayloadHash           string
	RoundID               string
	GameID                string
	ReferenceExternalID   string
	Now                   time.Time
}

func NewExternal(in ExternalInput) (*Transaction, error) {
	if in.Kind == KindOpening {
		return nil, ErrKind
	}
	if !validExternalKind(in.Kind) {
		return nil, ErrInvalid
	}
	if in.ID == uuid.Nil || in.WalletID == uuid.Nil || in.PlayerID == uuid.Nil || in.Now.IsZero() {
		return nil, ErrInvalid
	}
	if in.Amount.Currency() == "" || in.Amount.IsNegative() {
		return nil, ErrInvalid
	}
	if in.ProviderID == "" || in.ExternalTransactionID == "" || in.IdempotencyKey == "" || in.PayloadHash == "" || in.RoundID == "" || in.GameID == "" {
		return nil, ErrInvalid
	}
	switch in.Kind {
	case KindRefund, KindRollback:
		if in.ReferenceExternalID == "" {
			return nil, ErrInvalid
		}
	case KindBet, KindLoss:
		if in.ReferenceExternalID != "" {
			return nil, ErrInvalid
		}
	}
	now := in.Now.UTC()
	return &Transaction{
		id:                    in.ID,
		origin:                OriginExternal,
		kind:                  in.Kind,
		status:                StatusPending,
		walletID:              in.WalletID,
		playerID:              in.PlayerID,
		amount:                in.Amount,
		providerID:            in.ProviderID,
		externalTransactionID: in.ExternalTransactionID,
		idempotencyKey:        in.IdempotencyKey,
		payloadHash:           in.PayloadHash,
		roundID:               in.RoundID,
		gameID:                in.GameID,
		referenceExternalID:   in.ReferenceExternalID,
		createdAt:             now,
		updatedAt:             now,
	}, nil
}

type OpeningInput struct {
	ID       uuid.UUID
	WalletID uuid.UUID
	PlayerID uuid.UUID
	Amount   money.Money
	Now      time.Time
}

func NewOpening(in OpeningInput) (*Transaction, error) {
	if in.ID == uuid.Nil || in.WalletID == uuid.Nil || in.PlayerID == uuid.Nil || in.Now.IsZero() {
		return nil, ErrInvalid
	}
	if in.Amount.Currency() == "" || in.Amount.IsNegative() {
		return nil, ErrInvalid
	}
	now := in.Now.UTC()
	return &Transaction{
		id:        in.ID,
		origin:    OriginInternal,
		kind:      KindOpening,
		status:    StatusPending,
		walletID:  in.WalletID,
		playerID:  in.PlayerID,
		amount:    in.Amount,
		createdAt: now,
		updatedAt: now,
	}, nil
}

func Rehydrate(s Snapshot) (*Transaction, error) {
	if s.ID == uuid.Nil || s.WalletID == uuid.Nil || s.PlayerID == uuid.Nil || s.CreatedAt.IsZero() || s.UpdatedAt.IsZero() {
		return nil, ErrInvalid
	}
	if s.Amount.Currency() == "" || s.Amount.IsNegative() {
		return nil, ErrInvalid
	}
	if !validStatus(s.Status) {
		return nil, ErrInvalid
	}
	switch s.Origin {
	case OriginInternal:
		if s.Kind != KindOpening || s.ProviderID != "" || s.ExternalTransactionID != "" || s.IdempotencyKey != "" || s.PayloadHash != "" || s.RoundID != "" || s.GameID != "" || s.ReferenceExternalID != "" {
			return nil, ErrInvalid
		}
	case OriginExternal:
		if s.Kind == KindOpening || !validExternalKind(s.Kind) {
			return nil, ErrInvalid
		}
		if s.ProviderID == "" || s.ExternalTransactionID == "" || s.IdempotencyKey == "" || s.PayloadHash == "" || s.RoundID == "" || s.GameID == "" {
			return nil, ErrInvalid
		}
	default:
		return nil, ErrInvalid
	}
	terminal := s.Status == StatusProcessed || s.Status == StatusRejected || s.Status == StatusFailed
	if terminal && s.Status != StatusFailed && s.ResultBalance == nil {
		return nil, ErrInvalid
	}
	if (s.Status == StatusRejected || s.Status == StatusFailed) && s.FailureCode == "" {
		return nil, ErrInvalid
	}
	if s.Status != StatusRejected && s.Status != StatusFailed && s.FailureCode != "" {
		return nil, ErrInvalid
	}
	t := &Transaction{
		id:                     s.ID,
		origin:                 s.Origin,
		kind:                   s.Kind,
		status:                 s.Status,
		walletID:               s.WalletID,
		playerID:               s.PlayerID,
		amount:                 s.Amount,
		providerID:             s.ProviderID,
		externalTransactionID:  s.ExternalTransactionID,
		idempotencyKey:         s.IdempotencyKey,
		payloadHash:            s.PayloadHash,
		roundID:                s.RoundID,
		gameID:                 s.GameID,
		referenceExternalID:    s.ReferenceExternalID,
		referenceTransactionID: s.ReferenceTransactionID,
		failureCode:            s.FailureCode,
		createdAt:              s.CreatedAt.UTC(),
		updatedAt:              s.UpdatedAt.UTC(),
	}
	if s.ResultBalance != nil {
		cp := *s.ResultBalance
		t.resultBalance = &cp
	}
	return t, nil
}

func (t *Transaction) MarkPendingReference(now time.Time) error {
	if t.IsTerminal() {
		return ErrTerminal
	}
	if t.status != StatusPending {
		return ErrInvalid
	}
	t.status = StatusPendingReference
	t.updatedAt = now.UTC()
	return nil
}

func (t *Transaction) MarkProcessed(result money.Money, referenceID *uuid.UUID, now time.Time) error {
	if !t.canAdvance() {
		return ErrTerminal
	}
	if result.Currency() == "" || result.IsNegative() || result.Currency() != t.amount.Currency() {
		return ErrInvalid
	}
	cp := result
	t.status = StatusProcessed
	t.resultBalance = &cp
	t.failureCode = ""
	t.referenceTransactionID = cloneID(referenceID)
	t.updatedAt = now.UTC()
	return nil
}

func (t *Transaction) MarkRejected(code string, balance money.Money, now time.Time) error {
	if !t.canAdvance() {
		return ErrTerminal
	}
	if code == "" || balance.Currency() == "" || balance.IsNegative() {
		return ErrInvalid
	}
	cp := balance
	t.status = StatusRejected
	t.failureCode = code
	t.resultBalance = &cp
	t.updatedAt = now.UTC()
	return nil
}

func (t *Transaction) MarkFailed(code string, now time.Time) error {
	if !t.canAdvance() {
		return ErrTerminal
	}
	if code == "" {
		return ErrInvalid
	}
	t.status = StatusFailed
	t.failureCode = code
	t.updatedAt = now.UTC()
	return nil
}

func (t *Transaction) canAdvance() bool {
	return t.status == StatusPending || t.status == StatusPendingReference
}

func (t *Transaction) IsTerminal() bool {
	return t.status == StatusProcessed || t.status == StatusRejected || t.status == StatusFailed
}

func validExternalKind(k Kind) bool {
	switch k {
	case KindBet, KindWin, KindLoss, KindRefund, KindRollback:
		return true
	default:
		return false
	}
}

func validStatus(s Status) bool {
	switch s {
	case StatusPending, StatusPendingReference, StatusProcessed, StatusRejected, StatusFailed:
		return true
	default:
		return false
	}
}

func cloneID(id *uuid.UUID) *uuid.UUID {
	if id == nil {
		return nil
	}
	cp := *id
	return &cp
}

func (t *Transaction) ID() uuid.UUID                 { return t.id }
func (t *Transaction) Origin() Origin                { return t.origin }
func (t *Transaction) Kind() Kind                    { return t.kind }
func (t *Transaction) Status() Status                { return t.status }
func (t *Transaction) WalletID() uuid.UUID           { return t.walletID }
func (t *Transaction) PlayerID() uuid.UUID           { return t.playerID }
func (t *Transaction) Amount() money.Money           { return t.amount }
func (t *Transaction) ProviderID() string            { return t.providerID }
func (t *Transaction) ExternalTransactionID() string { return t.externalTransactionID }
func (t *Transaction) IdempotencyKey() string        { return t.idempotencyKey }
func (t *Transaction) PayloadHash() string           { return t.payloadHash }
func (t *Transaction) RoundID() string               { return t.roundID }
func (t *Transaction) GameID() string                { return t.gameID }
func (t *Transaction) ReferenceExternalID() string   { return t.referenceExternalID }
func (t *Transaction) ReferenceTransactionID() *uuid.UUID {
	return cloneID(t.referenceTransactionID)
}
func (t *Transaction) FailureCode() string { return t.failureCode }
func (t *Transaction) ResultBalance() *money.Money {
	if t.resultBalance == nil {
		return nil
	}
	cp := *t.resultBalance
	return &cp
}
func (t *Transaction) CreatedAt() time.Time { return t.createdAt }
func (t *Transaction) UpdatedAt() time.Time { return t.updatedAt }

// Snapshot is the persistence view. Rehydrate does not treat it as a command.
type Snapshot struct {
	ID                     uuid.UUID
	Origin                 Origin
	Kind                   Kind
	Status                 Status
	WalletID               uuid.UUID
	PlayerID               uuid.UUID
	Amount                 money.Money
	ProviderID             string
	ExternalTransactionID  string
	IdempotencyKey         string
	PayloadHash            string
	RoundID                string
	GameID                 string
	ReferenceExternalID    string
	ReferenceTransactionID *uuid.UUID
	FailureCode            string
	ResultBalance          *money.Money
	CreatedAt              time.Time
	UpdatedAt              time.Time
}

func (t *Transaction) Snapshot() Snapshot {
	return Snapshot{
		ID:                     t.id,
		Origin:                 t.origin,
		Kind:                   t.kind,
		Status:                 t.status,
		WalletID:               t.walletID,
		PlayerID:               t.playerID,
		Amount:                 t.amount,
		ProviderID:             t.providerID,
		ExternalTransactionID:  t.externalTransactionID,
		IdempotencyKey:         t.idempotencyKey,
		PayloadHash:            t.payloadHash,
		RoundID:                t.roundID,
		GameID:                 t.gameID,
		ReferenceExternalID:    t.referenceExternalID,
		ReferenceTransactionID: cloneID(t.referenceTransactionID),
		FailureCode:            t.failureCode,
		ResultBalance:          t.ResultBalance(),
		CreatedAt:              t.createdAt,
		UpdatedAt:              t.updatedAt,
	}
}
