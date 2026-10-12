package money

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"regexp"
	"strings"
)

// Scale is the number of minor units in one major unit. The external
// contract uses two decimal places, so 1 BRL is 100 minor units.
const Scale int64 = 100

var (
	ErrInvalid  = errors.New("invalid money")
	ErrCurrency = errors.New("currency mismatch")
	ErrOverflow = errors.New("money overflow")
	ErrNegative = errors.New("negative amount")
)

// MaxMinor is the largest magnitude that fits in int64 minor units.
const MaxMinor int64 = math.MaxInt64

// amountPattern accepts a non-negative decimal with at most two fractional
// digits. Leading zeros are rejected so "00.50" cannot be confused with a
// different lexical form of the same value. "25", "25.0" and "25.00" are
// accepted and later normalized to two places.
var amountPattern = regexp.MustCompile(`^(0|[1-9][0-9]*)(\.[0-9]{1,2})?$`)

var currencyPattern = regexp.MustCompile(`^[A-Z]{3}$`)

// Money is an immutable amount in minor units plus an ISO 4217 currency.
// The zero value is not a valid amount: currency is empty and callers must
// use Parse, Zero or FromMinor.
type Money struct {
	minor    int64
	currency string
}

func (m Money) Minor() int64 { return m.minor }

func (m Money) Currency() string { return m.currency }

func (m Money) IsZero() bool { return m.minor == 0 }

func (m Money) IsPositive() bool { return m.minor > 0 }

func (m Money) IsNegative() bool { return m.minor < 0 }

// Parse builds a non-negative amount from a decimal string. Scientific
// notation, signs, blank input and more than two fractional digits are
// rejected instead of being rounded.
func Parse(amount, currency string) (Money, error) {
	cur, err := checkCurrency(currency)
	if err != nil {
		return Money{}, err
	}
	if strings.ContainsAny(amount, "eE+-_ ,") || strings.TrimSpace(amount) != amount {
		return Money{}, ErrInvalid
	}
	minor, err := parseMinor(amount)
	if err != nil {
		return Money{}, err
	}
	return Money{minor: minor, currency: cur}, nil
}

// Zero returns a zero amount in the given currency.
func Zero(currency string) (Money, error) {
	cur, err := checkCurrency(currency)
	if err != nil {
		return Money{}, err
	}
	return Money{minor: 0, currency: cur}, nil
}

// FromMinor rebuilds a stored amount, including negative differences.
// It does not reapply any movement.
func FromMinor(minor int64, currency string) (Money, error) {
	cur, err := checkCurrency(currency)
	if err != nil {
		return Money{}, err
	}
	return Money{minor: minor, currency: cur}, nil
}

func checkCurrency(currency string) (string, error) {
	if !currencyPattern.MatchString(currency) {
		return "", ErrInvalid
	}
	return currency, nil
}

func parseMinor(amount string) (int64, error) {
	if !amountPattern.MatchString(amount) {
		return 0, ErrInvalid
	}
	whole, frac, _ := strings.Cut(amount, ".")
	switch len(frac) {
	case 0:
		frac = "00"
	case 1:
		frac += "0"
	case 2:
	default:
		return 0, ErrInvalid
	}
	w, ok := new(big.Int).SetString(whole, 10)
	if !ok {
		return 0, ErrInvalid
	}
	f, ok := new(big.Int).SetString(frac, 10)
	if !ok {
		return 0, ErrInvalid
	}
	w.Mul(w, big.NewInt(Scale))
	w.Add(w, f)
	if !w.IsInt64() {
		return 0, ErrOverflow
	}
	return w.Int64(), nil
}

func (m Money) sameCurrency(o Money) error {
	if m.currency == "" || o.currency == "" || m.currency != o.currency {
		return ErrCurrency
	}
	return nil
}

func (m Money) Add(o Money) (Money, error) {
	if err := m.sameCurrency(o); err != nil {
		return Money{}, err
	}
	sum, ok := add(m.minor, o.minor)
	if !ok {
		return Money{}, ErrOverflow
	}
	return Money{minor: sum, currency: m.currency}, nil
}

func (m Money) Sub(o Money) (Money, error) {
	if err := m.sameCurrency(o); err != nil {
		return Money{}, err
	}
	diff, ok := sub(m.minor, o.minor)
	if !ok {
		return Money{}, ErrOverflow
	}
	return Money{minor: diff, currency: m.currency}, nil
}

func (m Money) Neg() (Money, error) {
	if m.currency == "" {
		return Money{}, ErrInvalid
	}
	if m.minor == math.MinInt64 {
		return Money{}, ErrOverflow
	}
	return Money{minor: -m.minor, currency: m.currency}, nil
}

func (m Money) Cmp(o Money) (int, error) {
	if err := m.sameCurrency(o); err != nil {
		return 0, err
	}
	switch {
	case m.minor < o.minor:
		return -1, nil
	case m.minor > o.minor:
		return 1, nil
	default:
		return 0, nil
	}
}

func (m Money) Equal(o Money) bool {
	return m.currency == o.currency && m.minor == o.minor
}

// String renders the canonical two-decimal form used in the API, in the
// idempotency hash and in event payloads. "25" and "25.0" both become "25.00".
func (m Money) String() string {
	return formatMinor(m.minor)
}

func formatMinor(v int64) string {
	neg := v < 0
	var u uint64
	if neg {
		if v == math.MinInt64 {
			u = uint64(math.MaxInt64) + 1
		} else {
			u = uint64(-v)
		}
	} else {
		u = uint64(v)
	}
	sign := ""
	if neg {
		sign = "-"
	}
	return fmt.Sprintf("%s%d.%02d", sign, u/uint64(Scale), u%uint64(Scale))
}

func (m Money) MarshalJSON() ([]byte, error) {
	if m.currency == "" {
		return nil, ErrInvalid
	}
	return json.Marshal(struct {
		Amount   string `json:"amount"`
		Currency string `json:"currency"`
	}{Amount: m.String(), Currency: m.currency})
}

func (m *Money) UnmarshalJSON(b []byte) error {
	var raw struct {
		Amount   json.RawMessage `json:"amount"`
		Currency string          `json:"currency"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return ErrInvalid
	}
	if len(raw.Amount) == 0 || raw.Amount[0] != '"' {
		return ErrInvalid
	}
	var amount string
	if err := json.Unmarshal(raw.Amount, &amount); err != nil {
		return ErrInvalid
	}
	parsed, err := Parse(amount, raw.Currency)
	if err != nil {
		return err
	}
	*m = parsed
	return nil
}

func add(a, b int64) (int64, bool) {
	c := a + b
	if (b > 0 && c < a) || (b < 0 && c > a) {
		return 0, false
	}
	return c, true
}

func sub(a, b int64) (int64, bool) {
	c := a - b
	if (b > 0 && c > a) || (b < 0 && c < a) {
		return 0, false
	}
	return c, true
}
