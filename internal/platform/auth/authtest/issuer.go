// Package authtest signs RS256 tokens and serves the matching JWKS so unit
// tests can exercise auth.Verifier without Keycloak. Integration tests use
// the real Keycloak instead.
package authtest

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

const Audience = "wagering-api"

type Issuer struct {
	URL     string
	JWKSURL string
	key     *rsa.PrivateKey
	kid     string
}

func NewIssuer(t testing.TB) *Issuer {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	iss := &Issuer{key: key, kid: "test-key"}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]string{{
			"kty": "RSA", "kid": iss.kid, "use": "sig", "alg": "RS256",
			"n": b64(key.PublicKey.N.Bytes()),
			"e": b64(big.NewInt(int64(key.PublicKey.E)).Bytes()),
		}}})
	}))
	t.Cleanup(srv.Close)
	iss.URL = srv.URL + "/realms/test"
	iss.JWKSURL = srv.URL + "/certs"
	return iss
}

// Claims returns a valid claim set for the given roles and provider.
func (i *Issuer) Claims(providerID string, roles ...string) map[string]any {
	now := time.Now()
	c := map[string]any{
		"iss": i.URL, "sub": "service-account-" + providerID, "aud": Audience,
		"iat": now.Unix(), "exp": now.Add(5 * time.Minute).Unix(),
		"realm_access": map[string]any{"roles": roles},
	}
	if providerID != "" {
		c["provider_id"] = providerID
	}
	return c
}

func (i *Issuer) Provider(providerID string) string {
	return i.Sign(i.Claims(providerID, "provider"))
}

func (i *Issuer) Internal() string {
	return i.Sign(i.Claims("", "internal"))
}

func (i *Issuer) Sign(claims map[string]any) string {
	return SignWith(i.key, i.kid, claims)
}

func SignWith(key *rsa.PrivateKey, kid string, claims map[string]any) string {
	header := b64(mustJSON(map[string]string{"alg": "RS256", "typ": "JWT", "kid": kid}))
	payload := b64(mustJSON(claims))
	digest := sha256.Sum256([]byte(header + "." + payload))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		panic(err)
	}
	return header + "." + payload + "." + b64(sig)
}

// Unsigned builds a token with alg "none".
func Unsigned(claims map[string]any) string {
	return b64(mustJSON(map[string]string{"alg": "none", "typ": "JWT"})) + "." + b64(mustJSON(claims)) + "."
}

func b64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

func mustJSON(v any) []byte {
	raw, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return raw
}
