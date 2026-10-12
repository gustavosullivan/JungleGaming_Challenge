package event

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jungle-gaming/wagering/internal/domain/money"
)

func TestBalanceChangedSnapshot(t *testing.T) {
	at := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	amount, _ := money.Parse("25.00", "BRL")
	before, _ := money.Parse("100.00", "BRL")
	after, _ := money.Parse("75.00", "BRL")
	walletID := uuid.Must(uuid.NewV7())
	txID := uuid.Must(uuid.NewV7())
	ev, err := NewBalanceChanged(uuid.Must(uuid.NewV7()), "corr-1", txID.String(), at, BalanceChanged{
		WalletID: walletID, TransactionID: txID, Direction: "DEBIT",
		Amount: amount, BalanceBefore: before, BalanceAfter: after, WalletVersion: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if ev.EventType() != TypeBalanceChanged || ev.Version() != SchemaVersion {
		t.Fatalf("type=%s version=%d", ev.EventType(), ev.Version())
	}
	raw := string(ev.Payload())
	if strings.Contains(raw, "25.00") == false || strings.Contains(raw, "75.00") == false {
		t.Fatalf("payload = %s", raw)
	}
	var decoded map[string]any
	if err := json.Unmarshal(ev.Payload(), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded["version"].(float64) != 1 {
		t.Fatalf("version = %v", decoded["version"])
	}
	data := decoded["data"].(map[string]any)
	moneyObj := data["money"].(map[string]any)
	if _, ok := moneyObj["amount"].(string); !ok {
		t.Fatalf("amount was not a string: %#v", moneyObj["amount"])
	}
	if decoded["occurredAt"] != "2026-09-08T12:00:00.000Z" {
		t.Fatalf("occurredAt = %v", decoded["occurredAt"])
	}
}

func TestProcessedOmitsExternalFieldsForOpening(t *testing.T) {
	at := time.Now().UTC()
	amount, _ := money.Parse("1000.00", "BRL")
	ev, err := NewProcessed(uuid.Must(uuid.NewV7()), "corr", "cause", at, Processed{
		TransactionID: uuid.Must(uuid.NewV7()),
		WalletID:      uuid.Must(uuid.NewV7()),
		PlayerID:      uuid.Must(uuid.NewV7()),
		Kind:          "OPENING",
		Amount:        amount,
	})
	if err != nil {
		t.Fatal(err)
	}
	raw := string(ev.Payload())
	if strings.Contains(raw, "providerId") || strings.Contains(raw, "roundId") {
		t.Fatalf("opening event has external metadata: %s", raw)
	}
}

func TestRejectedRequiresCode(t *testing.T) {
	amount, _ := money.Parse("80.00", "BRL")
	_, err := NewRejected(uuid.Must(uuid.NewV7()), "corr", "cause", time.Now().UTC(), Rejected{
		TransactionID: uuid.Must(uuid.NewV7()),
		WalletID:      uuid.Must(uuid.NewV7()),
		PlayerID:      uuid.Must(uuid.NewV7()),
		Kind:          "BET",
		Amount:        amount,
	})
	if err == nil {
		t.Fatal("accepted rejection without a code")
	}
}
