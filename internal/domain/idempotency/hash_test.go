package idempotency

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func TestHashNormalizesAmountAndIgnoresKey(t *testing.T) {
	base := Fields{
		ProviderID: "provider-a", ExternalTransactionID: "transaction-123",
		PlayerID: "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
		WalletID: "0192f291-27dd-7d3f-8071-5f8685deef37",
		RoundID:  "round-987", GameID: "fortune-chimp", Kind: "BET",
		Amount: "25.00", Currency: "BRL",
	}
	a, err := Hash(base)
	if err != nil {
		t.Fatal(err)
	}
	base.Amount = "25"
	b, err := Hash(base)
	if err != nil {
		t.Fatal(err)
	}
	base.Amount = "25.0"
	c, err := Hash(base)
	if err != nil {
		t.Fatal(err)
	}
	if a != b || b != c {
		t.Fatalf("normalized hashes differ: %s %s %s", a, b, c)
	}

	base.Amount = "25.00"
	withRef := base
	withRef.ReferenceExternalID = "transaction-1"
	refHash, err := Hash(withRef)
	if err != nil {
		t.Fatal(err)
	}
	if refHash == a {
		t.Fatal("reference did not change the hash")
	}

	base.Amount = "25.000"
	if _, err := Hash(base); err == nil {
		t.Fatal("accepted excess scale")
	}
	base.Amount = "1e2"
	if _, err := Hash(base); err == nil {
		t.Fatal("accepted scientific notation")
	}

	canonical := `{"externalTransactionId":"transaction-123","gameId":"fortune-chimp","kind":"BET","money":{"amount":"25.00","currency":"BRL"},"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","providerId":"provider-a","roundId":"round-987","walletId":"0192f291-27dd-7d3f-8071-5f8685deef37"}`
	sum := sha256.Sum256([]byte(canonical))
	if a != hex.EncodeToString(sum[:]) {
		t.Fatalf("hash = %s", a)
	}
}

func TestHashChangesWhenPayloadChanges(t *testing.T) {
	left := Fields{
		ProviderID: "provider-a", ExternalTransactionID: "transaction-123",
		PlayerID: "p", WalletID: "w", RoundID: "r", GameID: "g", Kind: "BET",
		Amount: "80.00", Currency: "BRL",
	}
	right := left
	right.Amount = "80.01"
	h1, err := Hash(left)
	if err != nil {
		t.Fatal(err)
	}
	h2, err := Hash(right)
	if err != nil {
		t.Fatal(err)
	}
	if h1 == h2 {
		t.Fatal("different payloads hashed equal")
	}
}
