package idempotency

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"

	"github.com/jungle-gaming/wagering/internal/domain/money"
)

// Fields are the business inputs that identify a wager payload.
// The idempotency key and transport metadata are intentionally absent:
// HTTP and SQS both hash this struct, so the two entrances compare equal.
type Fields struct {
	ProviderID            string
	ExternalTransactionID string
	PlayerID              string
	WalletID              string
	RoundID               string
	GameID                string
	Kind                  string
	Amount                string
	Currency              string
	ReferenceExternalID   string
}

// Hash returns the hex SHA-256 of a canonical JSON object.
// Object keys are written in sorted order. Money is normalized with
// money.String before hashing, so "25", "25.0" and "25.00" collide and
// "25.000" is rejected. An empty reference is omitted, not stored as "".
func Hash(f Fields) (string, error) {
	m, err := money.Parse(f.Amount, f.Currency)
	if err != nil {
		return "", err
	}
	buf := make([]byte, 0, 256)
	buf = append(buf, '{')
	buf = writeString(buf, "externalTransactionId", f.ExternalTransactionID, true)
	buf = writeString(buf, "gameId", f.GameID, false)
	buf = writeString(buf, "kind", f.Kind, false)
	buf = append(buf, `,"money":{"amount":`...)
	buf = strconv.AppendQuote(buf, m.String())
	buf = append(buf, `,"currency":`...)
	buf = strconv.AppendQuote(buf, m.Currency())
	buf = append(buf, '}')
	buf = writeString(buf, "playerId", f.PlayerID, false)
	buf = writeString(buf, "providerId", f.ProviderID, false)
	if f.ReferenceExternalID != "" {
		buf = writeString(buf, "referenceExternalTransactionId", f.ReferenceExternalID, false)
	}
	buf = writeString(buf, "roundId", f.RoundID, false)
	buf = writeString(buf, "walletId", f.WalletID, false)
	buf = append(buf, '}')
	sum := sha256.Sum256(buf)
	return hex.EncodeToString(sum[:]), nil
}

func writeString(buf []byte, key, value string, first bool) []byte {
	if !first {
		buf = append(buf, ',')
	}
	buf = strconv.AppendQuote(buf, key)
	buf = append(buf, ':')
	buf = strconv.AppendQuote(buf, value)
	return buf
}
