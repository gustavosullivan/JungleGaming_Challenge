package wallet

import (
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jungle-gaming/wagering/internal/domain/money"
)

var (
	ErrInvalid             = errors.New("invalid wallet")
	ErrInsufficientBalance = errors.New("insufficient balance")
	ErrCurrencyMismatch    = errors.New("wallet currency mismatch")
	ErrAmount              = errors.New("amount must be positive")
)

// Wallet is the financial aggregate. Balance changes only through Debit
// and Credit, which also bump the version. Opening sets version 1 and the
// initial balance together; that first credit is not a later mutation.
type Wallet struct {
	id        uuid.UUID
	playerID  uuid.UUID
	balance   money.Money
	version   int64
	createdAt time.Time
	updatedAt time.Time
}

func Open(id, playerID uuid.UUID, initial money.Money, now time.Time) (*Wallet, error) {
	if id == uuid.Nil || playerID == uuid.Nil || now.IsZero() || initial.Currency() == "" {
		return nil, ErrInvalid
	}
	if initial.IsNegative() {
		return nil, ErrInvalid
	}
	now = now.UTC()
	return &Wallet{
		id:        id,
		playerID:  playerID,
		balance:   initial,
		version:   1,
		createdAt: now,
		updatedAt: now,
	}, nil
}

func Rehydrate(id, playerID uuid.UUID, balance money.Money, version int64, createdAt, updatedAt time.Time) (*Wallet, error) {
	if id == uuid.Nil || playerID == uuid.Nil || balance.Currency() == "" || version < 1 || createdAt.IsZero() || updatedAt.IsZero() {
		return nil, ErrInvalid
	}
	if balance.IsNegative() {
		return nil, ErrInsufficientBalance
	}
	return &Wallet{
		id:        id,
		playerID:  playerID,
		balance:   balance,
		version:   version,
		createdAt: createdAt.UTC(),
		updatedAt: updatedAt.UTC(),
	}, nil
}

// Debit removes a positive amount. On failure the wallet is unchanged.
func (w *Wallet) Debit(m money.Money, now time.Time) (before money.Money, err error) {
	return w.move(m, now, true)
}

func (w *Wallet) Credit(m money.Money, now time.Time) (before money.Money, err error) {
	return w.move(m, now, false)
}

func (w *Wallet) move(m money.Money, now time.Time, debit bool) (money.Money, error) {
	if m.Currency() != w.balance.Currency() {
		return money.Money{}, ErrCurrencyMismatch
	}
	if !m.IsPositive() {
		return money.Money{}, ErrAmount
	}
	before := w.balance
	var next money.Money
	var err error
	if debit {
		if w.balance.Minor() < m.Minor() {
			return money.Money{}, ErrInsufficientBalance
		}
		next, err = w.balance.Sub(m)
	} else {
		next, err = w.balance.Add(m)
	}
	if err != nil {
		return money.Money{}, err
	}
	if next.IsNegative() {
		return money.Money{}, ErrInsufficientBalance
	}
	w.balance = next
	w.version++
	w.updatedAt = now.UTC()
	return before, nil
}

func (w *Wallet) ID() uuid.UUID        { return w.id }
func (w *Wallet) PlayerID() uuid.UUID  { return w.playerID }
func (w *Wallet) Balance() money.Money { return w.balance }
func (w *Wallet) Currency() string     { return w.balance.Currency() }
func (w *Wallet) Version() int64       { return w.version }
func (w *Wallet) CreatedAt() time.Time { return w.createdAt }
func (w *Wallet) UpdatedAt() time.Time { return w.updatedAt }
