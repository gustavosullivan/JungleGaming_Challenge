package ledger

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jungle-gaming/wagering/internal/domain/money"
)

func TestEntryEquation(t *testing.T) {
	now := time.Now().UTC()
	before, _ := money.Parse("100.00", "BRL")
	amount, _ := money.Parse("80.00", "BRL")
	entry, err := New(uuid.Must(uuid.NewV7()), uuid.Must(uuid.NewV7()), uuid.Must(uuid.NewV7()), Debit, amount, before, now)
	if err != nil {
		t.Fatal(err)
	}
	if entry.BalanceAfter().String() != "20.00" || entry.Direction() != Debit {
		t.Fatalf("entry after=%s dir=%s", entry.BalanceAfter(), entry.Direction())
	}

	tooMuch, _ := money.Parse("80.00", "BRL")
	if _, err := New(entry.ID(), entry.WalletID(), entry.TransactionID(), Debit, tooMuch, entry.BalanceAfter(), now); err == nil {
		t.Fatal("accepted debit below zero")
	}

	usd, _ := money.Parse("1.00", "USD")
	if _, err := New(entry.ID(), entry.WalletID(), entry.TransactionID(), Credit, usd, before, now); err == nil {
		t.Fatal("accepted mixed currency")
	}
}

func TestRehydrateChecksEquation(t *testing.T) {
	now := time.Now().UTC()
	amount, _ := money.Parse("10.00", "BRL")
	before, _ := money.Zero("BRL")
	after, _ := money.Parse("10.00", "BRL")
	entry, err := Rehydrate(Snapshot{
		ID: uuid.Must(uuid.NewV7()), WalletID: uuid.Must(uuid.NewV7()), TransactionID: uuid.Must(uuid.NewV7()),
		Direction: Credit, Amount: amount, BalanceBefore: before, BalanceAfter: after, CreatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if entry.BalanceAfter().String() != "10.00" {
		t.Fatal(entry.BalanceAfter())
	}
	badAfter, _ := money.Parse("9.00", "BRL")
	if _, err := Rehydrate(Snapshot{
		ID: entry.ID(), WalletID: entry.WalletID(), TransactionID: entry.TransactionID(),
		Direction: Credit, Amount: amount, BalanceBefore: before, BalanceAfter: badAfter, CreatedAt: now,
	}); err != ErrEquation {
		t.Fatalf("err = %v", err)
	}
}
