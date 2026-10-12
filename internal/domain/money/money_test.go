package money

import (
	"encoding/json"
	"math"
	"testing"
)

func TestParseNormalizesScale(t *testing.T) {
	samples := []string{"25", "25.0", "25.00", "25.10", "0", "0.5", "0.50"}
	want := []string{"25.00", "25.00", "25.00", "25.10", "0.00", "0.50", "0.50"}
	for i, sample := range samples {
		got, err := Parse(sample, "BRL")
		if err != nil {
			t.Fatalf("parse %s: %v", sample, err)
		}
		if got.String() != want[i] {
			t.Fatalf("parse %s = %s, want %s", sample, got.String(), want[i])
		}
		if got.Currency() != "BRL" {
			t.Fatalf("currency = %s", got.Currency())
		}
	}
}

func TestParseRejectsBadInput(t *testing.T) {
	bad := []string{
		"", " ", "25 ", " 25.00", "+25.00", "-1.00", "25.000", "25.", ".50",
		"01.00", "00.50", "1e2", "1E2", "NaN", "Infinity", "inf", "1,00", "BRL",
	}
	for _, sample := range bad {
		if _, err := Parse(sample, "BRL"); err == nil {
			t.Fatalf("accepted %q", sample)
		}
	}
	if _, err := Parse("10.00", "brl"); err == nil {
		t.Fatal("accepted lowercase currency")
	}
	if _, err := Parse("10.00", "BR"); err == nil {
		t.Fatal("accepted short currency")
	}
	if _, err := Parse("10.00", ""); err == nil {
		t.Fatal("accepted empty currency")
	}
}

func TestParseOverflow(t *testing.T) {
	ok, err := Parse("92233720368547758.07", "BRL")
	if err != nil {
		t.Fatal(err)
	}
	if ok.Minor() != math.MaxInt64 {
		t.Fatalf("minor = %d", ok.Minor())
	}
	if _, err := Parse("92233720368547758.08", "BRL"); err != ErrOverflow {
		t.Fatalf("overflow err = %v", err)
	}
}

func TestArithmeticAndCurrency(t *testing.T) {
	a := mustParse(t, "10.50", "BRL")
	b := mustParse(t, "0.50", "BRL")
	usd := mustParse(t, "0.50", "USD")

	sum, err := a.Add(b)
	if err != nil || sum.String() != "11.00" {
		t.Fatalf("add = %s %v", sum, err)
	}
	diff, err := a.Sub(b)
	if err != nil || diff.String() != "10.00" {
		t.Fatalf("sub = %s %v", diff, err)
	}
	neg, err := b.Neg()
	if err != nil || neg.String() != "-0.50" || !neg.IsNegative() {
		t.Fatalf("neg = %s %v", neg, err)
	}
	cmp, err := a.Cmp(b)
	if err != nil || cmp != 1 {
		t.Fatalf("cmp = %d %v", cmp, err)
	}
	if _, err := a.Add(usd); err != ErrCurrency {
		t.Fatalf("add currency err = %v", err)
	}
	if _, err := a.Sub(usd); err != ErrCurrency {
		t.Fatalf("sub currency err = %v", err)
	}
	if _, err := a.Cmp(usd); err != ErrCurrency {
		t.Fatalf("cmp currency err = %v", err)
	}

	zero, err := Zero("BRL")
	if err != nil || !zero.IsZero() || zero.String() != "0.00" {
		t.Fatalf("zero = %+v %v", zero, err)
	}
	below, err := zero.Sub(b)
	if err != nil || below.String() != "-0.50" {
		t.Fatalf("internal negative = %s %v", below, err)
	}
}

func TestNegOverflow(t *testing.T) {
	m, err := FromMinor(math.MinInt64, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Neg(); err != ErrOverflow {
		t.Fatalf("err = %v", err)
	}
	max, err := FromMinor(math.MaxInt64, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := max.Add(mustParse(t, "0.01", "BRL")); err != ErrOverflow {
		t.Fatalf("add err = %v", err)
	}
}

func TestJSONStringAmount(t *testing.T) {
	m := mustParse(t, "25", "BRL")
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != `{"amount":"25.00","currency":"BRL"}` {
		t.Fatalf("json = %s", raw)
	}
	var back Money
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	if !back.Equal(m) {
		t.Fatalf("roundtrip = %s", back)
	}
	for _, bad := range []string{
		`{"amount":25.00,"currency":"BRL"}`,
		`{"amount":"25.000","currency":"BRL"}`,
		`{"amount":"NaN","currency":"BRL"}`,
		`{"amount":"-1.00","currency":"BRL"}`,
		`{"amount":"1e2","currency":"BRL"}`,
	} {
		var parsed Money
		if err := json.Unmarshal([]byte(bad), &parsed); err == nil {
			t.Fatalf("accepted %s", bad)
		}
	}
}

func mustParse(t *testing.T, amount, currency string) Money {
	t.Helper()
	m, err := Parse(amount, currency)
	if err != nil {
		t.Fatal(err)
	}
	return m
}
