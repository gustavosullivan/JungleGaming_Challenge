package wager

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jungle-gaming/wagering/internal/domain/money"
)

func TestExternalTransitions(t *testing.T) {
	now := time.Now().UTC()
	tx := mustExternal(t, KindBet, "25.00", "", now)

	if err := tx.MarkProcessed(mustMoney(t, "75.00"), nil, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if tx.Status() != StatusProcessed || tx.ResultBalance().String() != "75.00" {
		t.Fatalf("status=%s balance=%v", tx.Status(), tx.ResultBalance())
	}
	failed := mustExternal(t, KindWin, "1.00", "", now)
	if err := failed.MarkFailed("STORAGE_CORRUPT", now); err != nil {
		t.Fatal(err)
	}
	if failed.Status() != StatusFailed || !failed.IsTerminal() {
		t.Fatal(failed.Status())
	}
	if err := failed.MarkProcessed(mustMoney(t, "1.00"), nil, now); !errors.Is(err, ErrTerminal) {
		t.Fatalf("failed transition err = %v", err)
	}

	if err := tx.MarkRejected(CodeInsufficientBalance, mustMoney(t, "75.00"), now); !errors.Is(err, ErrTerminal) {
		t.Fatalf("processed transition err = %v", err)
	}
	if err := tx.MarkPendingReference(now); !errors.Is(err, ErrTerminal) {
		t.Fatalf("pending transition err = %v", err)
	}
	again, err := Rehydrate(tx.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	if again.Status() != StatusProcessed || again.ResultBalance().String() != "75.00" {
		t.Fatal("rehydrate changed the outcome")
	}
}

func TestReferenceRulesAndZeroPolicy(t *testing.T) {
	now := time.Now().UTC()
	if _, err := NewExternal(baseInput(KindBet, "10.00", "bet-1", now)); err == nil {
		t.Fatal("bet accepted a reference")
	}
	if _, err := NewExternal(baseInput(KindRefund, "10.00", "", now)); err == nil {
		t.Fatal("refund without reference")
	}
	if _, err := NewExternal(baseInput(KindOpening, "10.00", "", now)); !errors.Is(err, ErrKind) {
		t.Fatalf("opening err = %v", err)
	}

	loss := mustExternal(t, KindLoss, "0.00", "", now)
	if err := loss.MarkRejected(CodeInvalidAmount, mustMoney(t, "10.00"), now); err != nil {
		t.Fatal(err)
	}
	if loss.FailureCode() != CodeInvalidAmount || !loss.IsTerminal() {
		t.Fatalf("loss status=%s code=%s", loss.Status(), loss.FailureCode())
	}

	refund := mustExternal(t, KindRefund, "10.00", "bet-1", now)
	if err := refund.MarkPendingReference(now); err != nil {
		t.Fatal(err)
	}
	if refund.Status() != StatusPendingReference {
		t.Fatal(refund.Status())
	}
	if err := refund.MarkPendingReference(now); !errors.Is(err, ErrInvalid) {
		t.Fatalf("second wait err = %v", err)
	}
	ref := uuid.Must(uuid.NewV7())
	if err := refund.MarkProcessed(mustMoney(t, "20.00"), &ref, now); err != nil {
		t.Fatal(err)
	}
	if refund.ReferenceTransactionID() == nil || *refund.ReferenceTransactionID() != ref {
		t.Fatal("missing internal reference")
	}
}

func TestOpeningHasNoExternalMetadata(t *testing.T) {
	now := time.Now().UTC()
	tx, err := NewOpening(OpeningInput{
		ID: uuid.Must(uuid.NewV7()), WalletID: uuid.Must(uuid.NewV7()), PlayerID: uuid.Must(uuid.NewV7()),
		Amount: mustMoney(t, "1000.00"), Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.MarkProcessed(mustMoney(t, "1000.00"), nil, now); err != nil {
		t.Fatal(err)
	}
	snap := tx.Snapshot()
	if snap.Origin != OriginInternal || snap.ProviderID != "" || snap.IdempotencyKey != "" || snap.RoundID != "" {
		t.Fatalf("opening leaked external fields: %+v", snap)
	}
	if _, err := Rehydrate(snap); err != nil {
		t.Fatal(err)
	}
}

func mustExternal(t *testing.T, kind Kind, amount, ref string, now time.Time) *Transaction {
	t.Helper()
	tx, err := NewExternal(baseInput(kind, amount, ref, now))
	if err != nil {
		t.Fatal(err)
	}
	return tx
}

func baseInput(kind Kind, amount, ref string, now time.Time) ExternalInput {
	return ExternalInput{
		ID: uuid.Must(uuid.NewV7()), Kind: kind,
		WalletID: uuid.Must(uuid.NewV7()), PlayerID: uuid.Must(uuid.NewV7()),
		Amount: moneyOf(amount), ProviderID: "provider-a",
		ExternalTransactionID: "tx-1", IdempotencyKey: "provider-a:tx-1",
		PayloadHash: "abc", RoundID: "round-1", GameID: "fortune-chimp",
		ReferenceExternalID: ref, Now: now,
	}
}

func mustMoney(t *testing.T, amount string) money.Money {
	t.Helper()
	return moneyOf(amount)
}

func moneyOf(amount string) money.Money {
	m, err := money.Parse(amount, "BRL")
	if err != nil {
		panic(err)
	}
	return m
}
