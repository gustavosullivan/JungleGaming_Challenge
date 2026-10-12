package wallet

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jungle-gaming/wagering/internal/domain/money"
)

func TestOpenAndMovements(t *testing.T) {
	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	id, player := uuid.Must(uuid.NewV7()), uuid.Must(uuid.NewV7())
	initial, err := money.Parse("100.00", "BRL")
	if err != nil {
		t.Fatal(err)
	}
	w, err := Open(id, player, initial, now)
	if err != nil {
		t.Fatal(err)
	}
	if w.Version() != 1 || w.Balance().String() != "100.00" {
		t.Fatalf("opened version=%d balance=%s", w.Version(), w.Balance())
	}

	bet, _ := money.Parse("80.00", "BRL")
	if _, err := w.Debit(bet, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if w.Version() != 2 || w.Balance().String() != "20.00" {
		t.Fatalf("after debit version=%d balance=%s", w.Version(), w.Balance())
	}
	version := w.Version()
	balance := w.Balance()
	if _, err := w.Debit(bet, now.Add(2*time.Second)); err != ErrInsufficientBalance {
		t.Fatalf("second debit err = %v", err)
	}
	if w.Version() != version || w.Balance().String() != balance.String() {
		t.Fatal("failed debit mutated the wallet")
	}

	usd, _ := money.Parse("1.00", "USD")
	if _, err := w.Credit(usd, now); err != ErrCurrencyMismatch {
		t.Fatalf("currency err = %v", err)
	}
	zero, _ := money.Zero("BRL")
	if _, err := w.Credit(zero, now); err != ErrAmount {
		t.Fatalf("zero credit err = %v", err)
	}
}

func TestZeroOpening(t *testing.T) {
	zero, err := money.Zero("BRL")
	if err != nil {
		t.Fatal(err)
	}
	w, err := Open(uuid.Must(uuid.NewV7()), uuid.Must(uuid.NewV7()), zero, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if w.Version() != 1 || !w.Balance().IsZero() {
		t.Fatalf("wallet = %s v%d", w.Balance(), w.Version())
	}
}

func TestRehydrateDoesNotApply(t *testing.T) {
	bal, _ := money.Parse("20.00", "BRL")
	created := time.Now().UTC().Add(-time.Hour)
	w, err := Rehydrate(uuid.Must(uuid.NewV7()), uuid.Must(uuid.NewV7()), bal, 4, created, created)
	if err != nil {
		t.Fatal(err)
	}
	if w.Version() != 4 || w.Balance().String() != "20.00" {
		t.Fatalf("rehydrated version=%d balance=%s", w.Version(), w.Balance())
	}
	neg, _ := money.FromMinor(-1, "BRL")
	if _, err := Rehydrate(w.ID(), w.PlayerID(), neg, 1, created, created); err == nil {
		t.Fatal("accepted negative balance")
	}
}

func TestRejectsNegativeOpening(t *testing.T) {
	neg, err := money.FromMinor(-100, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(uuid.Must(uuid.NewV7()), uuid.Must(uuid.NewV7()), neg, time.Now().UTC()); err == nil {
		t.Fatal("accepted negative opening")
	}
}
