package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jungle-gaming/wagering/internal/app/port/memstore"
	"github.com/jungle-gaming/wagering/internal/app/wagering"
	walletsvc "github.com/jungle-gaming/wagering/internal/app/wallet"
	"github.com/jungle-gaming/wagering/internal/platform/auth"
	"github.com/jungle-gaming/wagering/internal/platform/auth/authtest"
	"github.com/jungle-gaming/wagering/internal/transport/httpapi"
	"github.com/prometheus/client_golang/prometheus"
)

type env struct {
	t      *testing.T
	srv    *httptest.Server
	store  *memstore.Store
	iss    *authtest.Issuer
	wallet uuid.UUID
	player uuid.UUID
}

func newEnv(t *testing.T) *env {
	t.Helper()
	iss := authtest.NewIssuer(t)
	store := memstore.New()
	wallets := walletsvc.New(store, nil, nil)
	wagers := wagering.New(wagering.Options{Store: store})
	handler := httpapi.NewHandler(wallets, wagers, auth.NewVerifier(iss.URL, iss.JWKSURL, authtest.Audience), nil, nil, prometheus.NewRegistry(), nil)
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	e := &env{t: t, srv: srv, store: store, iss: iss, player: uuid.Must(uuid.NewV7())}

	status, body := e.do(http.MethodPost, "/wallets", iss.Internal(), "", map[string]any{
		"playerId": e.player.String(), "initialBalance": map[string]string{"amount": "100.00", "currency": "BRL"},
	})
	if status != http.StatusCreated {
		t.Fatalf("open %d %s", status, body)
	}
	var opened struct{ ID string }
	if err := json.Unmarshal(body, &opened); err != nil {
		t.Fatal(err)
	}
	e.wallet = uuid.MustParse(opened.ID)
	return e
}

func (e *env) do(method, path, token, key string, body any) (int, []byte) {
	e.t.Helper()
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			e.t.Fatal(err)
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, e.srv.URL+path, reader)
	if err != nil {
		e.t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	return res.StatusCode, raw
}

func (e *env) bet(provider, external, amount string) map[string]any {
	return map[string]any{
		"providerId": provider, "externalTransactionId": external,
		"playerId": e.player.String(), "walletId": e.wallet.String(),
		"roundId": "round-1", "gameId": "fortune-chimp", "kind": "BET",
		"money": map[string]string{"amount": amount, "currency": "BRL"},
	}
}

type snapshot struct {
	balance string
	entries int
	events  int
}

func (e *env) snapshot() snapshot {
	w, err := e.store.GetWallet(context.Background(), e.wallet)
	if err != nil {
		e.t.Fatal(err)
	}
	return snapshot{balance: w.Balance().String(), entries: len(e.store.Ledger(e.wallet)), events: len(e.store.Events())}
}

func assertNoLeak(t *testing.T, name string, body []byte, secrets ...string) {
	t.Helper()
	for _, s := range secrets {
		if s != "" && strings.Contains(string(body), s) {
			t.Fatalf("%s leaked %q: %s", name, s, body)
		}
	}
}

func TestBusinessEndpointsRequireAValidToken(t *testing.T) {
	e := newEnv(t)
	before := e.snapshot()
	expired := e.iss.Claims("provider-a", "provider")
	expired["exp"] = time.Now().Add(-time.Minute).Unix()
	tokens := map[string]string{
		"missing":  "",
		"garbage":  "garbage",
		"expired":  e.iss.Sign(expired),
		"alg none": authtest.Unsigned(e.iss.Claims("provider-a", "provider")),
	}
	routes := []struct{ method, path string }{
		{http.MethodPost, "/wallets"},
		{http.MethodGet, "/wallets/" + e.wallet.String()},
		{http.MethodGet, "/wallets/" + e.wallet.String() + "/ledger"},
		{http.MethodPost, "/wallets/" + e.wallet.String() + "/reconciliation"},
		{http.MethodPost, "/wagering/transactions"},
		{http.MethodGet, "/wagering/transactions/" + uuid.NewString()},
		{http.MethodGet, "/providers/provider-a/wagering/transactions/bet-1"},
	}
	for name, token := range tokens {
		for _, r := range routes {
			var body any
			if r.method == http.MethodPost {
				body = e.bet("provider-a", "bet-1", "10.00")
			}
			status, raw := e.do(r.method, r.path, token, "provider-a:bet-1", body)
			if status != http.StatusUnauthorized {
				t.Fatalf("%s %s %s = %d", name, r.method, r.path, status)
			}
			assertNoLeak(t, name, raw, "100.00", e.player.String())
		}
	}
	if got := e.snapshot(); got != before {
		t.Fatalf("unauthenticated calls changed state: %+v -> %+v", before, got)
	}
	if status, _ := e.do(http.MethodGet, "/health/live", "", "", nil); status != http.StatusOK {
		t.Fatalf("live = %d", status)
	}
}

func TestInternalOperationsAreRestricted(t *testing.T) {
	e := newEnv(t)
	provider := e.iss.Provider("provider-a")
	before := e.snapshot()
	for _, r := range []struct{ method, path string }{
		{http.MethodGet, "/wallets/" + e.wallet.String()},
		{http.MethodGet, "/wallets/" + e.wallet.String() + "/ledger"},
		{http.MethodPost, "/wallets/" + e.wallet.String() + "/reconciliation"},
	} {
		status, raw := e.do(r.method, r.path, provider, "", nil)
		if status != http.StatusForbidden {
			t.Fatalf("provider %s %s = %d", r.method, r.path, status)
		}
		assertNoLeak(t, r.path, raw, "100.00", e.player.String())
	}
	status, _ := e.do(http.MethodPost, "/wallets", provider, "", map[string]any{
		"playerId": uuid.NewString(), "initialBalance": map[string]string{"amount": "999.00", "currency": "BRL"},
	})
	if status != http.StatusForbidden {
		t.Fatalf("provider open = %d", status)
	}
	if status, _ := e.do(http.MethodPost, "/wagering/transactions", e.iss.Internal(), "provider-a:bet-1", e.bet("provider-a", "bet-1", "10.00")); status != http.StatusForbidden {
		t.Fatalf("internal submit = %d", status)
	}
	if got := e.snapshot(); got != before {
		t.Fatalf("forbidden calls changed state: %+v -> %+v", before, got)
	}
}

func TestProvidersAreIsolated(t *testing.T) {
	e := newEnv(t)
	a, b := e.iss.Provider("provider-a"), e.iss.Provider("provider-b")

	before := e.snapshot()
	if status, _ := e.do(http.MethodPost, "/wagering/transactions", b, "provider-a:bet-1", e.bet("provider-a", "bet-1", "10.00")); status != http.StatusForbidden {
		t.Fatalf("b submitting as a = %d", status)
	}
	if got := e.snapshot(); got != before {
		t.Fatal("foreign submit changed state")
	}

	status, raw := e.do(http.MethodPost, "/wagering/transactions", a, "provider-a:bet-1", e.bet("provider-a", "bet-1", "10.00"))
	if status != http.StatusOK {
		t.Fatalf("a submit = %d %s", status, raw)
	}
	var res struct{ TransactionID string }
	if err := json.Unmarshal(raw, &res); err != nil {
		t.Fatal(err)
	}
	after := e.snapshot()

	status, raw = e.do(http.MethodPost, "/wagering/transactions", b, "provider-a:bet-1", e.bet("provider-b", "bet-1", "10.00"))
	if status != http.StatusForbidden {
		t.Fatalf("b replaying a's key = %d %s", status, raw)
	}
	assertNoLeak(t, "replay", raw, res.TransactionID, "90.00")

	if status, raw := e.do(http.MethodGet, "/wagering/transactions/"+res.TransactionID, b, "", nil); status != http.StatusNotFound {
		t.Fatalf("b reading a's transaction = %d %s", status, raw)
	} else {
		assertNoLeak(t, "read", raw, res.TransactionID, "90.00", e.player.String())
	}
	if status, _ := e.do(http.MethodGet, "/providers/provider-a/wagering/transactions/bet-1", b, "", nil); status != http.StatusForbidden {
		t.Fatalf("b reading a's external path = %d", status)
	}
	if status, _ := e.do(http.MethodGet, "/providers/provider-b/wagering/transactions/bet-1", b, "", nil); status != http.StatusNotFound {
		t.Fatalf("b reading its own missing id = %d", status)
	}
	if status, _ := e.do(http.MethodGet, "/wagering/transactions/"+res.TransactionID, a, "", nil); status != http.StatusOK {
		t.Fatalf("a reading its own transaction = %d", status)
	}
	if status, _ := e.do(http.MethodGet, "/wagering/transactions/"+res.TransactionID, e.iss.Internal(), "", nil); status != http.StatusOK {
		t.Fatalf("internal read = %d", status)
	}

	opening := e.store.Wagers(e.wallet)[0]
	if status, _ := e.do(http.MethodGet, "/wagering/transactions/"+opening.ID.String(), a, "", nil); status != http.StatusNotFound {
		t.Fatalf("provider reading the opening = %d", status)
	}
	if got := e.snapshot(); got != after {
		t.Fatalf("isolation checks changed state: %+v -> %+v", after, got)
	}
}
