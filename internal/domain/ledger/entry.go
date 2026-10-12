package ledger

import (
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jungle-gaming/wagering/internal/domain/money"
)

type Direction string

const (
	Debit  Direction = "DEBIT"
	Credit Direction = "CREDIT"
)

var (
	ErrInvalid   = errors.New("invalid ledger entry")
	ErrEquation  = errors.New("ledger equation does not hold")
	ErrDirection = errors.New("invalid ledger direction")
)

// Entry is an immutable ledger line. Construction checks
// balanceAfter = balanceBefore ± amount. Rehydrate repeats that check
// and does not post anything to a wallet.
type Entry struct {
	id            uuid.UUID
	walletID      uuid.UUID
	transactionID uuid.UUID
	direction     Direction
	amount        money.Money
	balanceBefore money.Money
	balanceAfter  money.Money
	createdAt     time.Time
}

type Snapshot struct {
	ID            uuid.UUID
	WalletID      uuid.UUID
	TransactionID uuid.UUID
	Direction     Direction
	Amount        money.Money
	BalanceBefore money.Money
	BalanceAfter  money.Money
	CreatedAt     time.Time
}

func New(id, walletID, transactionID uuid.UUID, direction Direction, amount, before money.Money, at time.Time) (Entry, error) {
	after, err := apply(direction, before, amount)
	if err != nil {
		return Entry{}, err
	}
	if id == uuid.Nil || walletID == uuid.Nil || transactionID == uuid.Nil || at.IsZero() {
		return Entry{}, ErrInvalid
	}
	if after.Minor() < 0 || before.Minor() < 0 {
		return Entry{}, ErrEquation
	}
	return Entry{
		id:            id,
		walletID:      walletID,
		transactionID: transactionID,
		direction:     direction,
		amount:        amount,
		balanceBefore: before,
		balanceAfter:  after,
		createdAt:     at.UTC(),
	}, nil
}

func Rehydrate(s Snapshot) (Entry, error) {
	if s.ID == uuid.Nil || s.WalletID == uuid.Nil || s.TransactionID == uuid.Nil || s.CreatedAt.IsZero() {
		return Entry{}, ErrInvalid
	}
	expected, err := apply(s.Direction, s.BalanceBefore, s.Amount)
	if err != nil {
		return Entry{}, err
	}
	if !expected.Equal(s.BalanceAfter) || s.BalanceAfter.Minor() < 0 || s.BalanceBefore.Minor() < 0 {
		return Entry{}, ErrEquation
	}
	return Entry{
		id:            s.ID,
		walletID:      s.WalletID,
		transactionID: s.TransactionID,
		direction:     s.Direction,
		amount:        s.Amount,
		balanceBefore: s.BalanceBefore,
		balanceAfter:  s.BalanceAfter,
		createdAt:     s.CreatedAt.UTC(),
	}, nil
}

func apply(direction Direction, before, amount money.Money) (money.Money, error) {
	if amount.Minor() <= 0 || amount.Currency() == "" || before.Currency() == "" {
		return money.Money{}, ErrInvalid
	}
	switch direction {
	case Credit:
		return before.Add(amount)
	case Debit:
		return before.Sub(amount)
	default:
		return money.Money{}, ErrDirection
	}
}

func (e Entry) ID() uuid.UUID              { return e.id }
func (e Entry) WalletID() uuid.UUID        { return e.walletID }
func (e Entry) TransactionID() uuid.UUID   { return e.transactionID }
func (e Entry) Direction() Direction       { return e.direction }
func (e Entry) Amount() money.Money        { return e.amount }
func (e Entry) BalanceBefore() money.Money { return e.balanceBefore }
func (e Entry) BalanceAfter() money.Money  { return e.balanceAfter }
func (e Entry) CreatedAt() time.Time       { return e.createdAt }
