package auth

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/coreos/go-oidc/v3/oidc"
)

type Identity struct {
	Subject    string
	ProviderID string
	Internal   bool
	Provider   bool
}

type Verifier struct {
	verifier *oidc.IDTokenVerifier
	audience string
}

func NewVerifier(issuer, jwksURL, audience string) *Verifier {
	keySet := oidc.NewRemoteKeySet(context.Background(), jwksURL)
	verifier := oidc.NewVerifier(issuer, keySet, &oidc.Config{
		SkipClientIDCheck:    true,
		SupportedSigningAlgs: []string{oidc.RS256},
	})
	return &Verifier{verifier: verifier, audience: audience}
}

type audience []string

func (a *audience) UnmarshalJSON(b []byte) error {
	if len(b) == 0 || string(b) == "null" {
		return nil
	}
	if b[0] == '"' {
		var one string
		if err := json.Unmarshal(b, &one); err != nil {
			return err
		}
		*a = []string{one}
		return nil
	}
	var many []string
	if err := json.Unmarshal(b, &many); err != nil {
		return err
	}
	*a = many
	return nil
}

type claims struct {
	Subject     string   `json:"sub"`
	Audience    audience `json:"aud"`
	ProviderID  string   `json:"provider_id"`
	RealmAccess struct {
		Roles []string `json:"roles"`
	} `json:"realm_access"`
}

func (v *Verifier) Verify(ctx context.Context, raw string) (Identity, error) {
	token, err := v.verifier.Verify(ctx, raw)
	if err != nil {
		return Identity{}, err
	}
	var c claims
	if err := token.Claims(&c); err != nil {
		return Identity{}, err
	}
	if !contains(c.Audience, v.audience) {
		return Identity{}, errors.New("unexpected audience")
	}
	id := Identity{Subject: c.Subject, ProviderID: c.ProviderID}
	for _, role := range c.RealmAccess.Roles {
		switch role {
		case "internal":
			id.Internal = true
		case "provider":
			id.Provider = true
		}
	}
	if !id.Internal && !id.Provider {
		return Identity{}, errors.New("missing role")
	}
	if id.Provider && !id.Internal && id.ProviderID == "" {
		return Identity{}, errors.New("missing provider_id")
	}
	return id, nil
}

func contains(values []string, want string) bool {
	for _, v := range values {
		if v == want {
			return true
		}
	}
	return false
}

type ctxKey struct{}

func WithIdentity(ctx context.Context, id Identity) context.Context {
	return context.WithValue(ctx, ctxKey{}, id)
}

func FromContext(ctx context.Context) (Identity, bool) {
	id, ok := ctx.Value(ctxKey{}).(Identity)
	return id, ok
}

func Bearer(r *http.Request) (string, bool) {
	h := r.Header.Get("Authorization")
	scheme, token, ok := strings.Cut(h, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || token == "" {
		return "", false
	}
	return token, true
}
