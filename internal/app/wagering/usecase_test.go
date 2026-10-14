package wagering_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jungle-gaming/wagering/internal/app/errs"
	"github.com/jungle-gaming/wagering/internal/app/port/memstore"
	"github.com/jungle-gaming/wagering/internal/app/wagering"
	walletsvc "github.com/jungle-gaming/wagering/internal/app/wallet"
	"github.com/jungle-gaming/wagering/internal/domain/event"
	"github.com/jungle-gaming/wagering/internal/domain/ledger"
	"github.com/jungle-gaming/wagering/internal/domain/wager"
)

type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

type fixture struct {
	t       *testing.T
	store   *memstore.Store
	clock   *clock
	wallets *walletsvc.Service
	wagers  *wagering.Service
	wallet  walletsvc.View
	player  uuid.UUID
}

func newFixture(t *testing.T, initial string) *fixture {
	t.Helper()
	// The wallet service stamps the opening with the real clock and the ledger
	// is ordered by created_at, so the wager clock starts just after it.
	c := &clock{now: time.Now().UTC().Add(time.Second)}
	store := memstore.New()
	store.Now = c.Now
	f := &fixture{
		t: t, store: store, clock: c,
		wallets: walletsvc.New(store, nil, nil),
		wagers: wagering.New(wagering.Options{
			Store: store, Now: c.Now, MaxAttempts: 3, TTL: time.Minute,
			Backoff: time.Second, PollInterval: time.Second,
		}),
		player: uuid.Must(uuid.NewV7()),
	}
	view, err := f.wallets.Open(context.Background(), f.player, initial, "BRL", "corr-open")
	if err != nil {
		t.Fatal(err)
	}
	f.wallet = view
	t.Cleanup(f.assertConsistent)
	return f
}

func (f *fixture) cmd(external, kind, amount, ref string) wagering.Command {
	return wagering.Command{
		ProviderID: "provider-a", ExternalTransactionID: external, IdempotencyKey: "provider-a:" + external,
		PlayerID: f.player, WalletID: f.wallet.ID, RoundID: "round-1", GameID: "fortune-chimp",
		Kind: wager.Kind(kind), Amount: amount, Currency: "BRL", ReferenceExternalID: ref,
		CorrelationID: "corr-" + external,
	}
}

func (f *fixture) submit(external, kind, amount, ref string) wagering.Result {
	f.t.Helper()
	res, err := f.wagers.Submit(context.Background(), f.cmd(external, kind, amount, ref))
	if err != nil {
		f.t.Fatalf("%s %s %s: %v", kind, external, amount, err)
	}
	return res
}

func (f *fixture) expect(res wagering.Result, status wager.Status, code, balance string) {
	f.t.Helper()
	if res.Status != status || res.FailureCode != code {
		f.t.Fatalf("status=%s code=%s, want %s %s", res.Status, res.FailureCode, status, code)
	}
	if balance != "" && (res.Balance == nil || res.Balance.String() != balance) {
		f.t.Fatalf("result balance = %v, want %s", res.Balance, balance)
	}
}

func (f *fixture) balance() string {
	f.t.Helper()
	view, err := f.wallets.Get(context.Background(), f.wallet.ID)
	if err != nil {
		f.t.Fatal(err)
	}
	return view.Balance.String()
}

func (f *fixture) version() int64 {
	f.t.Helper()
	view, err := f.wallets.Get(context.Background(), f.wallet.ID)
	if err != nil {
		f.t.Fatal(err)
	}
	return view.Version
}

func (f *fixture) entries() []ledger.Entry { return f.store.Ledger(f.wallet.ID) }

func (f *fixture) eventsOf(txID uuid.UUID) []string {
	var out []string
	for _, ev := range f.store.Events() {
		if ev.CausationID() == txID.String() {
			out = append(out, ev.EventType())
		}
	}
	return out
}

// assertConsistent checks the wallet invariants after every test: the stored
// balance equals credits minus debits, every ledger line chains from the
// previous one and the version moved exactly once per posting.
func (f *fixture) assertConsistent() {
	rec, err := f.wallets.Reconcile(context.Background(), f.wallet.ID)
	if err != nil {
		f.t.Fatal(err)
	}
	if !rec.Consistent || !rec.Difference.IsZero() {
		f.t.Fatalf("reconciliation %+v", rec)
	}
	movements := int64(len(f.entries()))
	if f.wallet.Balance.IsPositive() {
		movements--
	}
	if got := f.version(); got != 1+movements {
		f.t.Fatalf("version %d after %d movements", got, movements)
	}
}

func TestBetDebitsAndReplayReturnsTheOriginalResult(t *testing.T) {
	f := newFixture(t, "100.00")
	bet := f.submit("bet-1", "BET", "25", "")
	f.expect(bet, wager.StatusProcessed, "", "75.00")
	if bet.IdempotentReplay {
		t.Fatal("first submission flagged as replay")
	}
	entries := f.entries()
	last := entries[len(entries)-1]
	if last.Direction() != ledger.Debit || last.Amount().String() != "25.00" ||
		last.BalanceBefore().String() != "100.00" || last.BalanceAfter().String() != "75.00" || last.TransactionID() != bet.TransactionID {
		t.Fatalf("ledger %+v", last)
	}
	if got := f.eventsOf(bet.TransactionID); len(got) != 2 || got[0] != event.TypeProcessed || got[1] != event.TypeBalanceChanged {
		t.Fatalf("events %v", got)
	}

	f.submit("bet-2", "BET", "5.00", "")
	replay := f.submit("bet-1", "BET", "25.00", "")
	if !replay.IdempotentReplay || replay.TransactionID != bet.TransactionID {
		t.Fatalf("replay %+v", replay)
	}
	f.expect(replay, wager.StatusProcessed, "", "75.00")
	if f.balance() != "70.00" || len(f.entries()) != 3 {
		t.Fatalf("replay moved money: balance=%s entries=%d", f.balance(), len(f.entries()))
	}
	if got := f.eventsOf(bet.TransactionID); len(got) != 2 {
		t.Fatalf("replay emitted events: %v", got)
	}
}

func TestSameKeyWithAnotherPayloadIsAConflict(t *testing.T) {
	f := newFixture(t, "100.00")
	f.submit("bet-1", "BET", "10.00", "")
	events := len(f.store.Events())

	mutations := map[string]func(*wagering.Command){
		"amount":   func(c *wagering.Command) { c.Amount = "10.01" },
		"round":    func(c *wagering.Command) { c.RoundID = "round-2" },
		"game":     func(c *wagering.Command) { c.GameID = "other-game" },
		"kind":     func(c *wagering.Command) { c.Kind = wager.KindWin },
		"currency": func(c *wagering.Command) { c.Currency = "USD" },
		"external": func(c *wagering.Command) { c.ExternalTransactionID = "bet-other" },
	}
	for name, mutate := range mutations {
		cmd := f.cmd("bet-1", "BET", "10.00", "")
		mutate(&cmd)
		if _, err := f.wagers.Submit(context.Background(), cmd); !errors.As(err, new(*errs.Conflict)) {
			t.Fatalf("%s: err = %v", name, err)
		}
	}
	other := f.cmd("bet-1", "BET", "10.00", "")
	other.IdempotencyKey = "provider-a:another-key"
	if _, err := f.wagers.Submit(context.Background(), other); !errors.As(err, new(*errs.Conflict)) {
		t.Fatalf("same external with another key: err = %v", err)
	}
	if f.balance() != "90.00" || len(f.entries()) != 2 || len(f.store.Events()) != events {
		t.Fatal("a conflict changed state")
	}
}

func TestZeroAmountPolicyPerKind(t *testing.T) {
	f := newFixture(t, "50.00")
	f.submit("bet-ref", "BET", "10.00", "")
	version, entries := f.version(), len(f.entries())

	for _, c := range []struct{ kind, ref string }{
		{"BET", ""}, {"WIN", ""}, {"WIN", "bet-ref"}, {"REFUND", "bet-ref"}, {"ROLLBACK", "bet-ref"},
	} {
		res := f.submit("zero-"+c.kind+c.ref, c.kind, "0.00", c.ref)
		f.expect(res, wager.StatusRejected, wager.CodeInvalidAmount, "40.00")
		if got := f.eventsOf(res.TransactionID); len(got) != 1 || got[0] != event.TypeRejected {
			t.Fatalf("%s zero events %v", c.kind, got)
		}
	}

	loss := f.submit("loss-zero", "LOSS", "0", "")
	f.expect(loss, wager.StatusProcessed, "", "40.00")
	if got := f.eventsOf(loss.TransactionID); len(got) != 1 || got[0] != event.TypeProcessed {
		t.Fatalf("loss events %v", got)
	}
	badLoss := f.submit("loss-positive", "LOSS", "1.00", "")
	f.expect(badLoss, wager.StatusRejected, wager.CodeInvalidAmount, "40.00")

	if f.version() != version || len(f.entries()) != entries || f.balance() != "40.00" {
		t.Fatalf("zero policy moved the wallet: v%d entries=%d balance=%s", f.version(), len(f.entries()), f.balance())
	}
	if _, err := f.wagers.Submit(context.Background(), f.cmd("loss-ref", "LOSS", "0.00", "bet-ref")); !errors.As(err, new(*errs.Invalid)) {
		t.Fatalf("loss with reference err = %v", err)
	}
}

func TestBetRules(t *testing.T) {
	f := newFixture(t, "30.00")
	f.expect(f.submit("bet-all", "BET", "30.00", ""), wager.StatusProcessed, "", "0.00")
	f.expect(f.submit("bet-empty", "BET", "0.01", ""), wager.StatusRejected, wager.CodeInsufficientBalance, "0.00")
	if _, err := f.wagers.Submit(context.Background(), f.cmd("bet-ref", "BET", "1.00", "bet-all")); !errors.As(err, new(*errs.Invalid)) {
		t.Fatalf("bet with reference err = %v", err)
	}
}

func TestWinRules(t *testing.T) {
	f := newFixture(t, "100.00")
	f.expect(f.submit("win-free", "WIN", "15.00", ""), wager.StatusProcessed, "", "115.00")
	f.submit("bet-1", "BET", "10.00", "")
	win := f.submit("win-1", "WIN", "40.00", "bet-1")
	f.expect(win, wager.StatusProcessed, "", "145.00")
	got, err := f.wallets.GetTransaction(context.Background(), win.TransactionID)
	if err != nil || got.ReferenceTransactionID() == nil {
		t.Fatalf("win reference %v %v", got, err)
	}

	f.submit("loss-1", "LOSS", "0.00", "")
	f.expect(f.submit("win-on-loss", "WIN", "1.00", "loss-1"), wager.StatusRejected, wager.CodeInvalidReferenceKind, "145.00")

	other := f.cmd("win-other-round", "WIN", "1.00", "bet-1")
	other.RoundID = "round-2"
	res, err := f.wagers.Submit(context.Background(), other)
	if err != nil {
		t.Fatal(err)
	}
	f.expect(res, wager.StatusRejected, wager.CodeReferenceMismatch, "145.00")
}

func TestRefundRules(t *testing.T) {
	f := newFixture(t, "100.00")
	f.submit("bet-1", "BET", "30.00", "")
	f.expect(f.submit("refund-wrong-amount", "REFUND", "29.99", "bet-1"), wager.StatusRejected, wager.CodeReferenceAmountMismatch, "70.00")

	refund := f.submit("refund-1", "REFUND", "30.00", "bet-1")
	f.expect(refund, wager.StatusProcessed, "", "100.00")
	entries := f.entries()
	if last := entries[len(entries)-1]; last.Direction() != ledger.Credit || last.TransactionID() != refund.TransactionID {
		t.Fatalf("refund ledger %+v", last)
	}

	f.expect(f.submit("refund-2", "REFUND", "30.00", "bet-1"), wager.StatusRejected, wager.CodeAlreadyReversed, "100.00")
	f.expect(f.submit("rollback-after-refund", "ROLLBACK", "30.00", "bet-1"), wager.StatusRejected, wager.CodeAlreadyReversed, "100.00")

	f.submit("win-1", "WIN", "5.00", "")
	f.expect(f.submit("refund-win", "REFUND", "5.00", "win-1"), wager.StatusRejected, wager.CodeInvalidReferenceKind, "105.00")

	f.expect(f.submit("bet-big", "BET", "500.00", ""), wager.StatusRejected, wager.CodeInsufficientBalance, "105.00")
	f.expect(f.submit("refund-rejected", "REFUND", "500.00", "bet-big"), wager.StatusRejected, wager.CodeReferenceNotSuccessful, "105.00")

	if _, err := f.wagers.Submit(context.Background(), f.cmd("refund-noref", "REFUND", "1.00", "")); !errors.As(err, new(*errs.Invalid)) {
		t.Fatalf("refund without reference err = %v", err)
	}
}

func TestRollbackRules(t *testing.T) {
	f := newFixture(t, "100.00")
	f.submit("bet-1", "BET", "20.00", "")
	f.expect(f.submit("rollback-bet", "ROLLBACK", "20.00", "bet-1"), wager.StatusProcessed, "", "100.00")

	f.submit("win-1", "WIN", "50.00", "")
	f.expect(f.submit("rollback-win", "ROLLBACK", "50.00", "win-1"), wager.StatusProcessed, "", "100.00")
	entries := f.entries()
	if last := entries[len(entries)-1]; last.Direction() != ledger.Debit {
		t.Fatalf("rollback of win must debit, got %s", last.Direction())
	}

	f.submit("bet-2", "BET", "10.00", "")
	f.submit("refund-2", "REFUND", "10.00", "bet-2")
	f.expect(f.submit("rollback-refund", "ROLLBACK", "10.00", "refund-2"), wager.StatusProcessed, "", "90.00")

	f.submit("loss-1", "LOSS", "0.00", "")
	f.expect(f.submit("rollback-loss", "ROLLBACK", "1.00", "loss-1"), wager.StatusRejected, wager.CodeInvalidReferenceKind, "90.00")

	f.submit("win-2", "WIN", "60.00", "")
	f.submit("bet-drain", "BET", "150.00", "")
	f.expect(f.submit("rollback-win-2", "ROLLBACK", "60.00", "win-2"), wager.StatusRejected, wager.CodeReversalInsufficientBalance, "0.00")
	if f.balance() != "0.00" {
		t.Fatalf("balance = %s", f.balance())
	}
}

func TestWalletAndPlayerChecks(t *testing.T) {
	f := newFixture(t, "10.00")
	stranger := f.cmd("bet-stranger", "BET", "1.00", "")
	stranger.PlayerID = uuid.Must(uuid.NewV7())
	res, err := f.wagers.Submit(context.Background(), stranger)
	if err != nil {
		t.Fatal(err)
	}
	f.expect(res, wager.StatusRejected, wager.CodePlayerWalletMismatch, "10.00")

	usd := f.cmd("bet-usd", "BET", "1.00", "")
	usd.Currency = "USD"
	res, err = f.wagers.Submit(context.Background(), usd)
	if err != nil {
		t.Fatal(err)
	}
	f.expect(res, wager.StatusRejected, wager.CodeCurrencyMismatch, "10.00")

	missing := f.cmd("bet-missing", "BET", "1.00", "")
	missing.WalletID = uuid.Must(uuid.NewV7())
	if _, err := f.wagers.Submit(context.Background(), missing); !errors.As(err, new(*errs.NotFound)) {
		t.Fatalf("missing wallet err = %v", err)
	}
}

func TestInvalidCommandsNeverTouchTheStore(t *testing.T) {
	f := newFixture(t, "10.00")
	events := len(f.store.Events())
	cases := map[string]func(*wagering.Command){
		"opening kind":    func(c *wagering.Command) { c.Kind = wager.KindOpening },
		"unknown kind":    func(c *wagering.Command) { c.Kind = "JACKPOT" },
		"float amount":    func(c *wagering.Command) { c.Amount = "1e2" },
		"negative amount": func(c *wagering.Command) { c.Amount = "-1.00" },
		"excess scale":    func(c *wagering.Command) { c.Amount = "1.001" },
		"empty amount":    func(c *wagering.Command) { c.Amount = "" },
		"NaN":             func(c *wagering.Command) { c.Amount = "NaN" },
		"overflow":        func(c *wagering.Command) { c.Amount = "92233720368547758.08" },
		"bad currency":    func(c *wagering.Command) { c.Currency = "brl" },
		"empty key":       func(c *wagering.Command) { c.IdempotencyKey = "" },
		"key with space":  func(c *wagering.Command) { c.IdempotencyKey = "provider-a: x" },
		"bad provider":    func(c *wagering.Command) { c.ProviderID = "provider a" },
		"empty external":  func(c *wagering.Command) { c.ExternalTransactionID = "" },
		"empty round":     func(c *wagering.Command) { c.RoundID = "" },
		"nil player":      func(c *wagering.Command) { c.PlayerID = uuid.Nil },
		"nil wallet":      func(c *wagering.Command) { c.WalletID = uuid.Nil },
	}
	for name, mutate := range cases {
		cmd := f.cmd("bad", "BET", "1.00", "")
		mutate(&cmd)
		if _, err := f.wagers.Submit(context.Background(), cmd); !errors.As(err, new(*errs.Invalid)) {
			t.Fatalf("%s: err = %v", name, err)
		}
	}
	if len(f.store.Events()) != events || f.balance() != "10.00" {
		t.Fatal("invalid command changed state")
	}
}

func TestProviderRestriction(t *testing.T) {
	f := newFixture(t, "10.00")
	foreign := f.cmd("bet-1", "BET", "1.00", "")
	foreign.RestrictProvider = "provider-b"
	if _, err := f.wagers.Submit(context.Background(), foreign); !errors.As(err, new(*errs.Forbidden)) {
		t.Fatalf("foreign submit err = %v", err)
	}
	f.submit("bet-1", "BET", "1.00", "")

	stolen := f.cmd("bet-1", "BET", "1.00", "")
	stolen.ProviderID = "provider-b"
	stolen.RestrictProvider = "provider-b"
	if _, err := f.wagers.Submit(context.Background(), stolen); !errors.As(err, new(*errs.Forbidden)) {
		t.Fatalf("replay of another provider's key err = %v", err)
	}
	if f.balance() != "9.00" {
		t.Fatal(f.balance())
	}
}

func TestReversalWaitsForItsReference(t *testing.T) {
	f := newFixture(t, "100.00")
	refund := f.submit("refund-1", "REFUND", "25.00", "bet-1")
	f.expect(refund, wager.StatusPendingReference, "", "")
	if refund.Balance != nil {
		t.Fatal("pending result carries a balance")
	}
	if _, ok := f.store.Wait(refund.TransactionID); !ok {
		t.Fatal("no wait row")
	}
	if got := f.eventsOf(refund.TransactionID); len(got) != 1 || got[0] != event.TypePendingReference {
		t.Fatalf("pending events %v", got)
	}
	again := f.submit("refund-1", "REFUND", "25.00", "bet-1")
	if !again.IdempotentReplay || again.Status != wager.StatusPendingReference {
		t.Fatalf("pending replay %+v", again)
	}

	f.submit("bet-1", "BET", "25.00", "")
	f.clock.Advance(2 * time.Second)
	f.wagers.SweepOnce(context.Background())

	got, err := f.wallets.GetTransaction(context.Background(), refund.TransactionID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status() != wager.StatusProcessed || got.ReferenceTransactionID() == nil {
		t.Fatalf("resolved status=%s code=%s", got.Status(), got.FailureCode())
	}
	if _, ok := f.store.Wait(refund.TransactionID); ok {
		t.Fatal("wait row left behind")
	}
	if f.balance() != "100.00" {
		t.Fatal(f.balance())
	}
	replay := f.submit("refund-1", "REFUND", "25.00", "bet-1")
	if !replay.IdempotentReplay || replay.Status != wager.StatusProcessed {
		t.Fatalf("replay after resolution %+v", replay)
	}
}

func TestPendingReferenceExpiresByTTL(t *testing.T) {
	f := newFixture(t, "40.00")
	res := f.submit("rollback-1", "ROLLBACK", "10.00", "never")
	f.expect(res, wager.StatusPendingReference, "", "")
	f.clock.Advance(2 * time.Minute)
	f.wagers.SweepOnce(context.Background())
	got, err := f.wallets.GetTransaction(context.Background(), res.TransactionID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status() != wager.StatusRejected || got.FailureCode() != wager.CodeReferenceNotFound {
		t.Fatalf("status=%s code=%s", got.Status(), got.FailureCode())
	}
	if types := f.eventsOf(res.TransactionID); types[len(types)-1] != event.TypeRejected {
		t.Fatalf("events %v", types)
	}
	late := f.submit("never", "BET", "10.00", "")
	f.expect(late, wager.StatusProcessed, "", "30.00")
	if f.balance() != "30.00" {
		t.Fatal(f.balance())
	}
}

func TestPendingReferenceExpiresByAttempts(t *testing.T) {
	f := newFixture(t, "40.00")
	res := f.submit("refund-1", "REFUND", "10.00", "never")
	for i := 0; i < 5; i++ {
		f.clock.Advance(5 * time.Second)
		f.wagers.SweepOnce(context.Background())
	}
	got, err := f.wallets.GetTransaction(context.Background(), res.TransactionID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status() != wager.StatusRejected || got.FailureCode() != wager.CodeReferenceNotFound {
		t.Fatalf("status=%s code=%s", got.Status(), got.FailureCode())
	}
}

func TestPendingReversalBlocksASecondOne(t *testing.T) {
	f := newFixture(t, "100.00")
	f.submit("refund-1", "REFUND", "10.00", "bet-1")
	f.submit("bet-1", "BET", "10.00", "")
	f.expect(f.submit("rollback-1", "ROLLBACK", "10.00", "bet-1"), wager.StatusRejected, wager.CodeReversalInProgress, "90.00")
	f.clock.Advance(2 * time.Second)
	f.wagers.SweepOnce(context.Background())
	if f.balance() != "100.00" {
		t.Fatal(f.balance())
	}
}

func TestDeliveryDeduplicatesByMessageAndOperation(t *testing.T) {
	f := newFixture(t, "80.00")
	ctx := context.Background()
	cmd := f.cmd("sqs-bet", "BET", "30.00", "")
	body := []byte(`{"messageId":"msg-1"}`)
	for i := 0; i < 3; i++ {
		if err := f.wagers.AcceptDelivery(ctx, "msg-1", body, cmd); err != nil {
			t.Fatalf("delivery %d: %v", i, err)
		}
	}
	if err := f.wagers.AcceptDelivery(ctx, "msg-2", []byte(`{"messageId":"msg-2"}`), cmd); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"msg-1", "msg-2"} {
		row, ok := f.store.Inbox("wager-transactions", id)
		if !ok || row.CompletedAt == nil {
			t.Fatalf("inbox %s = %+v", id, row)
		}
	}
	if err := f.wagers.AcceptDelivery(ctx, "msg-1", []byte(`{"messageId":"msg-1","x":1}`), cmd); !errs.IsPermanent(err) {
		t.Fatalf("changed body err = %v", err)
	}
	if err := f.wagers.AcceptDelivery(ctx, "", body, cmd); !errs.IsPermanent(err) {
		t.Fatalf("missing id err = %v", err)
	}
	opening := cmd
	opening.Kind = wager.KindOpening
	if err := f.wagers.AcceptDelivery(ctx, "msg-3", body, opening); !errs.IsPermanent(err) {
		t.Fatalf("opening over the queue err = %v", err)
	}
	if f.balance() != "50.00" || len(f.entries()) != 2 {
		t.Fatalf("balance=%s entries=%d", f.balance(), len(f.entries()))
	}
	http := f.submit("sqs-bet", "BET", "30.00", "")
	if !http.IdempotentReplay {
		t.Fatalf("http after sqs %+v", http)
	}
}

func TestFailedTransactionLeavesNoTrace(t *testing.T) {
	f := newFixture(t, "20.00")
	f.store.FailNext(errors.New("connection reset by peer"))
	if _, err := f.wagers.Submit(context.Background(), f.cmd("bet-1", "BET", "5.00", "")); err == nil {
		t.Fatal("expected the infrastructure error")
	}
	if f.balance() != "20.00" || len(f.store.Wagers(f.wallet.ID)) != 1 {
		t.Fatal("rolled back attempt left data")
	}
	f.expect(f.submit("bet-1", "BET", "5.00", ""), wager.StatusProcessed, "", "15.00")
}

func TestReconciliationFlagsATamperedBalance(t *testing.T) {
	f := newFixture(t, "20.00")
	f.submit("bet-1", "BET", "5.00", "")
	f.store.TamperBalance(f.wallet.ID, 1600)
	rec, err := f.wallets.Reconcile(context.Background(), f.wallet.ID)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Consistent || rec.Difference.String() != "1.00" || rec.CalculatedBalance.String() != "15.00" {
		t.Fatalf("reconciliation %+v", rec)
	}
	f.store.TamperBalance(f.wallet.ID, 1500)
}

func TestOpeningRecordsAnInternalTransaction(t *testing.T) {
	f := newFixture(t, "1000.00")
	if f.wallet.Version != 1 || f.wallet.Balance.String() != "1000.00" {
		t.Fatalf("view %+v", f.wallet)
	}
	rows := f.store.Wagers(f.wallet.ID)
	if len(rows) != 1 {
		t.Fatalf("transactions = %d", len(rows))
	}
	op := rows[0]
	if op.Kind != wager.KindOpening || op.Origin != wager.OriginInternal || op.Status != wager.StatusProcessed {
		t.Fatalf("opening %+v", op)
	}
	if op.ProviderID != "" || op.ExternalTransactionID != "" || op.IdempotencyKey != "" || op.PayloadHash != "" ||
		op.RoundID != "" || op.GameID != "" || op.ReferenceExternalID != "" {
		t.Fatalf("opening carries external metadata: %+v", op)
	}
	if op.ResultBalance == nil || op.ResultBalance.String() != "1000.00" || op.Amount.String() != "1000.00" {
		t.Fatalf("opening amounts %+v", op)
	}

	entries := f.entries()
	if len(entries) != 1 || entries[0].Direction() != ledger.Credit || entries[0].BalanceBefore().String() != "0.00" ||
		entries[0].BalanceAfter().String() != "1000.00" || entries[0].TransactionID() != op.ID {
		t.Fatalf("opening ledger %+v", entries)
	}

	events := f.store.Events()
	if len(events) != 2 || events[0].EventType() != event.TypeProcessed || events[1].EventType() != event.TypeBalanceChanged {
		t.Fatalf("opening events %d", len(events))
	}
	for _, ev := range events {
		if ev.CorrelationID() != "corr-open" || ev.CausationID() != op.ID.String() || ev.AggregateID() != f.wallet.ID.String() {
			t.Fatalf("envelope %s corr=%s cause=%s agg=%s", ev.EventType(), ev.CorrelationID(), ev.CausationID(), ev.AggregateID())
		}
	}
	var processed struct {
		Data map[string]any `json:"data"`
	}
	if err := json.Unmarshal(events[0].Payload(), &processed); err != nil {
		t.Fatal(err)
	}
	if processed.Data["kind"] != "OPENING" {
		t.Fatalf("processed data %v", processed.Data)
	}
	for _, field := range []string{"providerId", "externalTransactionId", "roundId", "gameId"} {
		if _, ok := processed.Data[field]; ok {
			t.Fatalf("opening event has %s", field)
		}
	}
	var changed struct {
		Data struct {
			Direction     string          `json:"direction"`
			BalanceBefore event.MoneyView `json:"balanceBefore"`
			BalanceAfter  event.MoneyView `json:"balanceAfter"`
			WalletVersion int64           `json:"walletVersion"`
		} `json:"data"`
	}
	if err := json.Unmarshal(events[1].Payload(), &changed); err != nil {
		t.Fatal(err)
	}
	if changed.Data.Direction != "CREDIT" || changed.Data.BalanceBefore.Amount != "0.00" ||
		changed.Data.BalanceAfter.Amount != "1000.00" || changed.Data.WalletVersion != 1 {
		t.Fatalf("balance changed %+v", changed.Data)
	}
}

func TestZeroOpeningRecordsOnlyTheWallet(t *testing.T) {
	f := newFixture(t, "0.00")
	if f.wallet.Version != 1 || !f.wallet.Balance.IsZero() {
		t.Fatalf("view %+v", f.wallet)
	}
	if n := len(f.store.Wagers(f.wallet.ID)); n != 0 {
		t.Fatalf("transactions = %d", n)
	}
	if len(f.entries()) != 0 || len(f.store.Events()) != 0 {
		t.Fatal("zero opening wrote ledger or events")
	}
	f.expect(f.submit("bet-1", "BET", "0.01", ""), wager.StatusRejected, wager.CodeInsufficientBalance, "0.00")
}

func TestOpeningValidation(t *testing.T) {
	f := newFixture(t, "10.00")
	ctx := context.Background()
	if _, err := f.wallets.Open(ctx, f.player, "5.00", "BRL", ""); !errors.As(err, new(*errs.Conflict)) {
		t.Fatalf("duplicate wallet err = %v", err)
	}
	if _, err := f.wallets.Open(ctx, f.player, "5.00", "USD", ""); err != nil {
		t.Fatalf("second currency for the same player: %v", err)
	}
	for _, amount := range []string{"-1.00", "1.001", "1e3", "", "NaN"} {
		if _, err := f.wallets.Open(ctx, uuid.Must(uuid.NewV7()), amount, "BRL", ""); !errors.As(err, new(*errs.Invalid)) {
			t.Fatalf("amount %q err = %v", amount, err)
		}
	}
	if _, err := f.wallets.Open(ctx, uuid.Nil, "1.00", "BRL", ""); !errors.As(err, new(*errs.Invalid)) {
		t.Fatalf("nil player err = %v", err)
	}
}

func TestConcurrentSubmissionsKeepTheInvariant(t *testing.T) {
	f := newFixture(t, "100.00")
	var wg sync.WaitGroup
	results := make([]wagering.Result, 50)
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := f.wagers.Submit(context.Background(), f.cmd("same-bet", "BET", "10.00", ""))
			if err != nil {
				t.Error(err)
			}
			results[i] = res
		}(i)
	}
	wg.Wait()
	fresh := 0
	for _, res := range results {
		if !res.IdempotentReplay {
			fresh++
		}
	}
	if fresh != 1 || f.balance() != "90.00" {
		t.Fatalf("fresh=%d balance=%s", fresh, f.balance())
	}
}
