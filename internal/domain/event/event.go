package event

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jungle-gaming/wagering/internal/domain/money"
)

const SchemaVersion = 1

const (
	TypeProcessed        = "WagerTransactionProcessed"
	TypeRejected         = "WagerTransactionRejected"
	TypeBalanceChanged   = "WalletBalanceChanged"
	TypePendingReference = "WagerTransactionPendingReference"
)

var ErrInvalid = errors.New("invalid event")

// Envelope is an immutable integration event. The constructor marshals the
// payload immediately; callers persist that snapshot.
type Envelope struct {
	eventID       uuid.UUID
	eventType     string
	aggregateID   string
	correlationID string
	causationID   string
	occurredAt    time.Time
	version       int
	payload       []byte
}

type MoneyView struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

func moneyView(m money.Money) MoneyView {
	return MoneyView{Amount: m.String(), Currency: m.Currency()}
}

type wire struct {
	EventID       string `json:"eventId"`
	EventType     string `json:"eventType"`
	AggregateID   string `json:"aggregateId"`
	CorrelationID string `json:"correlationId"`
	CausationID   string `json:"causationId,omitempty"`
	OccurredAt    string `json:"occurredAt"`
	Version       int    `json:"version"`
	Data          any    `json:"data"`
}

func stamp(id uuid.UUID, eventType, aggregateID, correlationID, causationID string, at time.Time, data any) (Envelope, error) {
	if id == uuid.Nil || eventType == "" || aggregateID == "" || correlationID == "" || at.IsZero() {
		return Envelope{}, ErrInvalid
	}
	body, err := json.Marshal(wire{
		EventID:       id.String(),
		EventType:     eventType,
		AggregateID:   aggregateID,
		CorrelationID: correlationID,
		CausationID:   causationID,
		OccurredAt:    at.UTC().Format("2006-01-02T15:04:05.000Z"),
		Version:       SchemaVersion,
		Data:          data,
	})
	if err != nil {
		return Envelope{}, err
	}
	return Envelope{
		eventID:       id,
		eventType:     eventType,
		aggregateID:   aggregateID,
		correlationID: correlationID,
		causationID:   causationID,
		occurredAt:    at.UTC(),
		version:       SchemaVersion,
		payload:       body,
	}, nil
}

type Processed struct {
	TransactionID         uuid.UUID
	WalletID              uuid.UUID
	PlayerID              uuid.UUID
	ProviderID            string
	ExternalTransactionID string
	RoundID               string
	GameID                string
	Kind                  string
	Amount                money.Money
}

func NewProcessed(id uuid.UUID, correlationID, causationID string, at time.Time, p Processed) (Envelope, error) {
	data := map[string]any{
		"transactionId": p.TransactionID.String(),
		"walletId":      p.WalletID.String(),
		"playerId":      p.PlayerID.String(),
		"kind":          p.Kind,
		"money":         moneyView(p.Amount),
		"status":        "PROCESSED",
	}
	if p.ProviderID != "" {
		data["providerId"] = p.ProviderID
	}
	if p.ExternalTransactionID != "" {
		data["externalTransactionId"] = p.ExternalTransactionID
	}
	if p.RoundID != "" {
		data["roundId"] = p.RoundID
	}
	if p.GameID != "" {
		data["gameId"] = p.GameID
	}
	return stamp(id, TypeProcessed, p.WalletID.String(), correlationID, causationID, at, data)
}

type Rejected struct {
	TransactionID         uuid.UUID
	WalletID              uuid.UUID
	PlayerID              uuid.UUID
	ProviderID            string
	ExternalTransactionID string
	Kind                  string
	FailureCode           string
	Amount                money.Money
}

func NewRejected(id uuid.UUID, correlationID, causationID string, at time.Time, p Rejected) (Envelope, error) {
	if p.FailureCode == "" {
		return Envelope{}, ErrInvalid
	}
	data := map[string]any{
		"transactionId": p.TransactionID.String(),
		"walletId":      p.WalletID.String(),
		"playerId":      p.PlayerID.String(),
		"kind":          p.Kind,
		"failureCode":   p.FailureCode,
		"money":         moneyView(p.Amount),
		"status":        "REJECTED",
	}
	if p.ProviderID != "" {
		data["providerId"] = p.ProviderID
	}
	if p.ExternalTransactionID != "" {
		data["externalTransactionId"] = p.ExternalTransactionID
	}
	return stamp(id, TypeRejected, p.WalletID.String(), correlationID, causationID, at, data)
}

type BalanceChanged struct {
	WalletID      uuid.UUID
	TransactionID uuid.UUID
	Direction     string
	Amount        money.Money
	BalanceBefore money.Money
	BalanceAfter  money.Money
	WalletVersion int64
}

func NewBalanceChanged(id uuid.UUID, correlationID, causationID string, at time.Time, p BalanceChanged) (Envelope, error) {
	if p.Direction != "DEBIT" && p.Direction != "CREDIT" {
		return Envelope{}, ErrInvalid
	}
	if p.WalletVersion < 1 {
		return Envelope{}, ErrInvalid
	}
	data := map[string]any{
		"walletId":      p.WalletID.String(),
		"transactionId": p.TransactionID.String(),
		"direction":     p.Direction,
		"money":         moneyView(p.Amount),
		"balanceBefore": moneyView(p.BalanceBefore),
		"balanceAfter":  moneyView(p.BalanceAfter),
		"walletVersion": p.WalletVersion,
	}
	return stamp(id, TypeBalanceChanged, p.WalletID.String(), correlationID, causationID, at, data)
}

type PendingReference struct {
	TransactionID         uuid.UUID
	WalletID              uuid.UUID
	ProviderID            string
	ExternalTransactionID string
	ReferenceExternalID   string
	Kind                  string
}

func NewPendingReference(id uuid.UUID, correlationID, causationID string, at time.Time, p PendingReference) (Envelope, error) {
	if p.ReferenceExternalID == "" {
		return Envelope{}, ErrInvalid
	}
	data := map[string]any{
		"transactionId":                  p.TransactionID.String(),
		"walletId":                       p.WalletID.String(),
		"providerId":                     p.ProviderID,
		"externalTransactionId":          p.ExternalTransactionID,
		"referenceExternalTransactionId": p.ReferenceExternalID,
		"kind":                           p.Kind,
		"status":                         "PENDING_REFERENCE",
	}
	return stamp(id, TypePendingReference, p.WalletID.String(), correlationID, causationID, at, data)
}

func (e Envelope) EventID() uuid.UUID    { return e.eventID }
func (e Envelope) EventType() string     { return e.eventType }
func (e Envelope) AggregateID() string   { return e.aggregateID }
func (e Envelope) CorrelationID() string { return e.correlationID }
func (e Envelope) CausationID() string   { return e.causationID }
func (e Envelope) OccurredAt() time.Time { return e.occurredAt }
func (e Envelope) Version() int          { return e.version }
func (e Envelope) Payload() []byte       { return append([]byte(nil), e.payload...) }
