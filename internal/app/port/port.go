package port

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jungle-gaming/wagering/internal/domain/event"
	"github.com/jungle-gaming/wagering/internal/domain/ledger"
	"github.com/jungle-gaming/wagering/internal/domain/wager"
	"github.com/jungle-gaming/wagering/internal/domain/wallet"
)

var (
	ErrNotFound         = errors.New("not found")
	ErrConcurrentUpdate = errors.New("concurrent update")
)

type LockMode int

const (
	LockUpdate LockMode = iota
	LockShare
)

type Conflict struct{ Constraint string }

func (e *Conflict) Error() string { return "unique violation: " + e.Constraint }

type Inbox struct {
	Consumer    string
	MessageID   string
	PayloadHash string
	ReceivedAt  time.Time
	CompletedAt *time.Time
}

type Wait struct {
	TransactionID uuid.UUID
	Attempts      int
	NextAttemptAt time.Time
	DeadlineAt    time.Time
}

type OutboxMessage struct {
	EventID     uuid.UUID
	AggregateID string
	EventType   string
	Payload     []byte
	Attempts    int
}

type LedgerPage struct {
	Entries []ledger.Entry
	HasMore bool
}

type Tx interface {
	LockWallet(ctx context.Context, id uuid.UUID, mode LockMode) (*wallet.Wallet, error)
	InsertWallet(ctx context.Context, w *wallet.Wallet) error
	UpdateWallet(ctx context.Context, w *wallet.Wallet, previousVersion int64) error

	FindWagerByID(ctx context.Context, id uuid.UUID) (*wager.Transaction, error)
	LockWager(ctx context.Context, id uuid.UUID) (*wager.Transaction, error)
	FindWagerByIdempotency(ctx context.Context, key string) (*wager.Transaction, error)
	FindWagerByExternal(ctx context.Context, providerID, externalID string) (*wager.Transaction, error)
	FindBlockingReversal(ctx context.Context, providerID, referenceExternalID string, except uuid.UUID) (*wager.Transaction, error)
	InsertWager(ctx context.Context, tx *wager.Transaction) error
	UpdateWager(ctx context.Context, tx *wager.Transaction) error

	InsertLedger(ctx context.Context, entry ledger.Entry) error
	ListLedger(ctx context.Context, walletID uuid.UUID, after *LedgerCursor, limit int) (LedgerPage, error)
	ListAllLedger(ctx context.Context, walletID uuid.UUID) ([]ledger.Entry, error)

	InsertInbox(ctx context.Context, row Inbox) error
	FindInbox(ctx context.Context, consumer, messageID string) (Inbox, error)
	CompleteInbox(ctx context.Context, consumer, messageID string, at time.Time) error

	InsertOutbox(ctx context.Context, ev event.Envelope) error

	InsertWait(ctx context.Context, row Wait) error
	FindWait(ctx context.Context, transactionID uuid.UUID) (Wait, error)
	UpdateWait(ctx context.Context, row Wait) error
	DeleteWait(ctx context.Context, transactionID uuid.UUID) error
}

type LedgerCursor struct {
	CreatedAt time.Time
	ID        uuid.UUID
}

type Store interface {
	WithinTx(ctx context.Context, fn func(context.Context, Tx) error) error

	GetWallet(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error)
	GetWager(ctx context.Context, id uuid.UUID) (*wager.Transaction, error)
	GetWagerByExternal(ctx context.Context, providerID, externalID string) (*wager.Transaction, error)
	GetWagerByIdempotency(ctx context.Context, key string) (*wager.Transaction, error)
	ListLedger(ctx context.Context, walletID uuid.UUID, after *LedgerCursor, limit int) (LedgerPage, error)

	ClaimWaits(ctx context.Context, limit int, lease time.Duration) ([]uuid.UUID, error)
	ClaimOutbox(ctx context.Context, worker string, limit int, lease time.Duration) ([]OutboxMessage, error)
	MarkOutboxPublished(ctx context.Context, eventID uuid.UUID, at time.Time) error
	RescheduleOutbox(ctx context.Context, eventID uuid.UUID, next time.Time) error
	OutboxLag(ctx context.Context) (time.Duration, error)
}

type Publisher interface {
	Publish(ctx context.Context, groupID, dedupID string, body []byte) error
}

type Queues interface {
	Receive(ctx context.Context, max int32, wait, visibility int32) ([]Incoming, error)
	Delete(ctx context.Context, receipt string) error
	ChangeVisibility(ctx context.Context, receipt string, timeout int32) error
	Ready(ctx context.Context) error
}

type Incoming struct {
	Receipt      string
	Body         []byte
	ReceiveCount int
}
