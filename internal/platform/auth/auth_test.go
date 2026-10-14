package auth_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"net/http"
	"testing"
	"time"

	"github.com/jungle-gaming/wagering/internal/platform/auth"
	"github.com/jungle-gaming/wagering/internal/platform/auth/authtest"
)

func TestVerifierAcceptsValidTokens(t *testing.T) {
	iss := authtest.NewIssuer(t)
	v := auth.NewVerifier(iss.URL, iss.JWKSURL, authtest.Audience)

	id, err := v.Verify(context.Background(), iss.Provider("provider-a"))
	if err != nil {
		t.Fatal(err)
	}
	if !id.Provider || id.Internal || id.ProviderID != "provider-a" {
		t.Fatalf("provider identity %+v", id)
	}

	id, err = v.Verify(context.Background(), iss.Internal())
	if err != nil {
		t.Fatal(err)
	}
	if !id.Internal || id.Provider {
		t.Fatalf("internal identity %+v", id)
	}

	claims := iss.Claims("provider-a", "provider")
	claims["aud"] = []string{"account", authtest.Audience}
	if _, err := v.Verify(context.Background(), iss.Sign(claims)); err != nil {
		t.Fatalf("audience list: %v", err)
	}
}

func TestVerifierRejectsBadTokens(t *testing.T) {
	iss := authtest.NewIssuer(t)
	v := auth.NewVerifier(iss.URL, iss.JWKSURL, authtest.Audience)
	other, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}

	cases := map[string]string{
		"garbage":        "not-a-token",
		"empty":          "",
		"alg none":       authtest.Unsigned(iss.Claims("provider-a", "provider")),
		"foreign key":    authtest.SignWith(other, "test-key", iss.Claims("provider-a", "provider")),
		"unknown kid":    authtest.SignWith(other, "rotated", iss.Claims("provider-a", "provider")),
		"expired":        iss.Sign(with(iss.Claims("provider-a", "provider"), "exp", time.Now().Add(-time.Minute).Unix())),
		"wrong issuer":   iss.Sign(with(iss.Claims("provider-a", "provider"), "iss", "http://evil.local/realms/test")),
		"wrong audience": iss.Sign(with(iss.Claims("provider-a", "provider"), "aud", "account")),
		"no audience":    iss.Sign(without(iss.Claims("provider-a", "provider"), "aud")),
		"no role":        iss.Sign(iss.Claims("provider-a")),
		"unknown role":   iss.Sign(iss.Claims("provider-a", "admin")),
		"no provider_id": iss.Sign(iss.Claims("", "provider")),
	}
	for name, raw := range cases {
		if id, err := v.Verify(context.Background(), raw); err == nil {
			t.Fatalf("%s accepted: %+v", name, id)
		}
	}

	tampered := iss.Provider("provider-a")
	tampered = tampered[:len(tampered)-4] + "AAAA"
	if _, err := v.Verify(context.Background(), tampered); err == nil {
		t.Fatal("tampered signature accepted")
	}
}

func TestBearer(t *testing.T) {
	for header, want := range map[string]bool{
		"Bearer abc": true, "bearer abc": true, "": false, "Bearer ": false, "Basic abc": false, "abc": false,
	} {
		r, _ := http.NewRequest(http.MethodGet, "/", nil)
		if header != "" {
			r.Header.Set("Authorization", header)
		}
		if _, ok := auth.Bearer(r); ok != want {
			t.Fatalf("%q ok=%v", header, ok)
		}
	}
}

func with(c map[string]any, key string, value any) map[string]any {
	c[key] = value
	return c
}

func without(c map[string]any, key string) map[string]any {
	delete(c, key)
	return c
}
