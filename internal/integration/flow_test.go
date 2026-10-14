//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jungle-gaming/wagering/internal/app/errs"
	"github.com/jungle-gaming/wagering/internal/app/wagering"
	walletsvc "github.com/jungle-gaming/wagering/internal/app/wallet"
	"github.com/jungle-gaming/wagering/internal/domain/wager"
	"github.com/jungle-gaming/wagering/internal/platform/auth"
	"github.com/jungle-gaming/wagering/internal/platform/config"
	"github.com/jungle-gaming/wagering/internal/platform/metrics"
	"github.com/jungle-gaming/wagering/internal/platform/migrate"
	"github.com/jungle-gaming/wagering/internal/platform/postgres"
	"github.com/jungle-gaming/wagering/internal/transport/httpapi"
	"github.com/jungle-gaming/wagering/internal/transport/outbox"
	"github.com/jungle-gaming/wagering/internal/transport/sqsconsumer"
	"github.com/prometheus/client_golang/prometheus"
)

func TestMain(m *testing.M) {
	switch os.Getenv("WAGER_PROC") {
	case "submit":
		os.Exit(submitFromEnv())
	case "pending-and-hang":
		os.Exit(pendingAndHang())
	case "consume-and-hang":
		os.Exit(consumeAndHang())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn())
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := pool.Ping(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "postgres is not reachable; run docker compose up --build first")
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := migrate.Up(ctx, pool); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	pool.Close()
	os.Exit(m.Run())
}

func TestOpeningLedgerReplayAndConflict(t *testing.T) {
	wallets, wagers, pool := newStack(t, time.Second, 4)
	defer pool.Close()
	truncate(t, pool)

	player := uuid.Must(uuid.NewV7())
	opened, err := wallets.Open(context.Background(), player, "1000.00", "BRL", "corr-open")
	if err != nil {
		t.Fatal(err)
	}
	if opened.Version != 1 || opened.Balance.String() != "1000.00" {
		t.Fatalf("opened %+v", opened)
	}
	if _, err := wallets.Open(context.Background(), player, "1.00", "BRL", "corr-open-2"); !errors.As(err, new(*errs.Conflict)) {
		t.Fatalf("duplicate wallet err = %v", err)
	}

	zeroPlayer := uuid.Must(uuid.NewV7())
	zero, err := wallets.Open(context.Background(), zeroPlayer, "0.00", "BRL", "corr-zero")
	if err != nil {
		t.Fatal(err)
	}
	if n := count(t, pool, `SELECT count(*) FROM wager_transactions WHERE wallet_id = $1`, zero.ID); n != 0 {
		t.Fatalf("zero opening created %d transactions", n)
	}

	res := submit(t, wagers, opened, player, "transaction-123", "BET", "25.00", "")
	if res.Status != wager.StatusProcessed || res.IdempotentReplay || res.Balance.String() != "975.00" {
		t.Fatalf("bet %+v", res)
	}
	replay := submit(t, wagers, opened, player, "transaction-123", "BET", "25.00", "")
	if !replay.IdempotentReplay || replay.TransactionID != res.TransactionID || replay.Balance.String() != "975.00" {
		t.Fatalf("replay %+v", replay)
	}
	_, err = wagers.Submit(context.Background(), command(opened, player, "transaction-123", "provider-a:transaction-123", "BET", "26.00", ""))
	if !errors.As(err, new(*errs.Conflict)) {
		t.Fatalf("payload conflict err = %v", err)
	}
	_, err = wagers.Submit(context.Background(), command(opened, player, "transaction-123", "provider-a:other-key", "BET", "25.00", ""))
	if !errors.As(err, new(*errs.Conflict)) {
		t.Fatalf("key conflict err = %v", err)
	}
	if got := balanceOf(t, pool, opened.ID); got != "975.00" {
		t.Fatalf("balance = %s", got)
	}
	if n := count(t, pool, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, opened.ID); n != 1 {
		t.Fatalf("debits = %d", n)
	}
	types := eventTypes(t, pool)
	if !containsAll(types, "WagerTransactionProcessed", "WalletBalanceChanged") {
		t.Fatalf("events = %v", types)
	}
	rec, err := wallets.Reconcile(context.Background(), opened.ID)
	if err != nil || !rec.Consistent || rec.CheckedEntries != 2 || rec.Difference.String() != "0.00" {
		t.Fatalf("reconcile %+v %v", rec, err)
	}
}

func TestFiftyDuplicatesAndTwoLargeBets(t *testing.T) {
	stacks := make([]stack, 3)
	for i := range stacks {
		wallets, wagers, p := newStack(t, time.Second, 4)
		stacks[i] = stack{wallets: wallets, wagers: wagers, pool: p}
		t.Cleanup(p.Close)
	}
	pool := stacks[0].pool
	truncate(t, pool)
	player := uuid.Must(uuid.NewV7())
	opened, err := stacks[0].wallets.Open(context.Background(), player, "100.00", "BRL", "corr")
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	results := make([]wagering.Result, 50)
	errList := make([]error, 50)
	start := make(chan struct{})
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i], errList[i] = stacks[i%3].wagers.Submit(context.Background(), command(opened, player, "same-bet", "provider-a:same-bet", "BET", "10.00", ""))
		}(i)
	}
	close(start)
	wg.Wait()
	fresh, replays := 0, 0
	for i, err := range errList {
		if err != nil {
			t.Fatal(err)
		}
		if results[i].TransactionID != results[0].TransactionID || results[i].Status != wager.StatusProcessed {
			t.Fatalf("result %d = %+v", i, results[i])
		}
		if results[i].IdempotentReplay {
			replays++
		} else {
			fresh++
		}
	}
	if fresh != 1 || replays != 49 {
		t.Fatalf("fresh=%d replays=%d, want 1 and 49", fresh, replays)
	}
	if n := count(t, pool, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, opened.ID); n != 1 {
		t.Fatalf("duplicate debits = %d", n)
	}
	if got := balanceOf(t, pool, opened.ID); got != "90.00" {
		t.Fatalf("balance after duplicates = %s", got)
	}

	player2 := uuid.Must(uuid.NewV7())
	race, err := stacks[0].wallets.Open(context.Background(), player2, "100.00", "BRL", "corr-race")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var processed, rejected int
	var wg2 sync.WaitGroup
	for i, external := range []string{"bet-a", "bet-b"} {
		wg2.Add(1)
		go func(i int, external string) {
			defer wg2.Done()
			res, err := stacks[i].wagers.Submit(context.Background(), command(race, player2, external, "provider-a:"+external, "BET", "80.00", ""))
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			switch res.Status {
			case wager.StatusProcessed:
				processed++
			case wager.StatusRejected:
				rejected++
				if res.FailureCode != wager.CodeInsufficientBalance {
					t.Errorf("failure code %s", res.FailureCode)
				}
			default:
				t.Errorf("status %s", res.Status)
			}
		}(i, external)
	}
	wg2.Wait()
	if processed != 1 || rejected != 1 {
		t.Fatalf("processed=%d rejected=%d", processed, rejected)
	}
	if got := balanceOf(t, pool, race.ID); got != "20.00" {
		t.Fatalf("race balance = %s", got)
	}
	if n := count(t, pool, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, race.ID); n != 1 {
		t.Fatalf("race debits = %d", n)
	}
	for _, external := range []string{"bet-a", "bet-b"} {
		res, err := stacks[2].wagers.Submit(context.Background(), command(race, player2, external, "provider-a:"+external, "BET", "80.00", ""))
		if err != nil || !res.IdempotentReplay {
			t.Fatalf("replay %s %+v %v", external, res, err)
		}
	}
	if got := balanceOf(t, pool, race.ID); got != "20.00" {
		t.Fatalf("balance after replay = %s", got)
	}
	rec, err := stacks[1].wallets.Reconcile(context.Background(), race.ID)
	if err != nil || !rec.Consistent || rec.StoredBalance.String() != "20.00" {
		t.Fatalf("reconcile %+v %v", rec, err)
	}
}

func TestTwoLargeBetsInSeparateProcesses(t *testing.T) {
	wallets, _, pool := newStack(t, time.Second, 4)
	t.Cleanup(pool.Close)
	truncate(t, pool)

	player := uuid.Must(uuid.NewV7())
	opened, err := wallets.Open(context.Background(), player, "100.00", "BRL", "corr-proc")
	if err != nil {
		t.Fatal(err)
	}
	otherPlayer := uuid.Must(uuid.NewV7())
	other, err := wallets.Open(context.Background(), otherPlayer, "40.00", "BRL", "corr-other")
	if err != nil {
		t.Fatal(err)
	}

	type job struct {
		player   uuid.UUID
		wallet   walletsvc.View
		external string
		amount   string
	}
	jobs := []job{
		{player, opened, "proc-a", "80.00"},
		{player, opened, "proc-b", "80.00"},
		{otherPlayer, other, "proc-c", "10.00"},
	}
	out := make([]string, len(jobs))
	errs := make([]error, len(jobs))
	var wg sync.WaitGroup
	for i, item := range jobs {
		wg.Add(1)
		go func(i int, item job) {
			defer wg.Done()
			out[i], errs[i] = runSubmitProcess(item.player, item.wallet, item.external, item.amount)
		}(i, item)
	}
	wg.Wait()

	var processed, rejected int
	for i := range jobs[:2] {
		if errs[i] != nil {
			t.Fatalf("process %d: %v\n%s", i, errs[i], out[i])
		}
		status, code, ok := parseProcessStatus(out[i])
		if !ok {
			t.Fatalf("process %d output %q", i, out[i])
		}
		switch status {
		case string(wager.StatusProcessed):
			processed++
		case string(wager.StatusRejected):
			rejected++
			if code != wager.CodeInsufficientBalance {
				t.Fatalf("failure code %s", code)
			}
		default:
			t.Fatalf("status %s", status)
		}
	}
	if processed != 1 || rejected != 1 {
		t.Fatalf("processed=%d rejected=%d", processed, rejected)
	}
	if errs[2] != nil {
		t.Fatalf("other wallet process: %v\n%s", errs[2], out[2])
	}
	if status, _, ok := parseProcessStatus(out[2]); !ok || status != string(wager.StatusProcessed) {
		t.Fatalf("other wallet output %q", out[2])
	}
	if got := balanceOf(t, pool, opened.ID); got != "20.00" {
		t.Fatalf("race balance = %s", got)
	}
	if n := count(t, pool, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, opened.ID); n != 1 {
		t.Fatalf("race debits = %d", n)
	}
	if got := balanceOf(t, pool, other.ID); got != "30.00" {
		t.Fatalf("other balance = %s", got)
	}
}

func runSubmitProcess(player uuid.UUID, wallet walletsvc.View, external, amount string) (string, error) {
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(),
		"WAGER_PROC=submit",
		"DATABASE_URL="+dsn(),
		"WALLET_ID="+wallet.ID.String(),
		"PLAYER_ID="+player.String(),
		"EXTERNAL_ID="+external,
		"IDEMPOTENCY_KEY=provider-a:"+external,
		"KIND=BET",
		"AMOUNT="+amount,
	)
	raw, err := cmd.CombinedOutput()
	return string(raw), err
}

func parseProcessStatus(out string) (string, string, bool) {
	for _, line := range strings.Split(out, "\n") {
		fields := strings.Fields(strings.TrimSpace(line))
		if len(fields) == 0 {
			continue
		}
		if fields[0] != string(wager.StatusProcessed) && fields[0] != string(wager.StatusRejected) {
			continue
		}
		code := ""
		if len(fields) > 1 {
			code = fields[1]
		}
		return fields[0], code, true
	}
	return "", "", false
}

func submitFromEnv() int {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn())
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer pool.Close()
	wagers := wagering.New(wagering.Options{
		Store: postgres.NewStore(pool), MaxAttempts: 4, TTL: time.Second,
		Backoff: 20 * time.Millisecond, PollInterval: 20 * time.Millisecond,
	})
	walletID, err := uuid.Parse(os.Getenv("WALLET_ID"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	playerID, err := uuid.Parse(os.Getenv("PLAYER_ID"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	res, err := wagers.Submit(ctx, command(walletsvc.View{ID: walletID}, playerID, os.Getenv("EXTERNAL_ID"), os.Getenv("IDEMPOTENCY_KEY"), os.Getenv("KIND"), os.Getenv("AMOUNT"), ""))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	fmt.Printf("%s %s\n", res.Status, res.FailureCode)
	return 0
}

func TestWalletsDoNotBlockEachOther(t *testing.T) {
	wallets, wagers, pool := newStack(t, time.Second, 4)
	defer pool.Close()
	truncate(t, pool)
	a := open(t, wallets, "50.00")
	b := open(t, wallets, "50.00")
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT id FROM wallets WHERE id = $1 FOR UPDATE`, a.Wallet.ID); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		cctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, err := wagers.Submit(cctx, command(b.Wallet, b.Player, "bet-b", "provider-a:bet-b", "BET", "10.00", ""))
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("bet on a different wallet waited on the locked wallet")
	}
}

func TestReversalArrivesFirstAndExclusiveRefund(t *testing.T) {
	wallets, wagers, pool := newStack(t, 300*time.Millisecond, 2)
	defer pool.Close()
	truncate(t, pool)
	opened := open(t, wallets, "100.00")

	refund := submit(t, wagers, opened.Wallet, opened.Player, "refund-1", "REFUND", "25.00", "bet-1")
	if refund.Status != wager.StatusPendingReference {
		t.Fatalf("early refund %+v", refund)
	}
	bet := submit(t, wagers, opened.Wallet, opened.Player, "bet-1", "BET", "25.00", "")
	if bet.Status != wager.StatusProcessed || bet.Balance.String() != "75.00" {
		t.Fatalf("bet %+v", bet)
	}
	if err := wagers.Resume(context.Background(), refund.TransactionID); err != nil {
		t.Fatal(err)
	}
	got, err := wallets.GetTransaction(context.Background(), refund.TransactionID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status() != wager.StatusProcessed {
		t.Fatalf("refund status %s %s", got.Status(), got.FailureCode())
	}
	if balanceOf(t, pool, opened.Wallet.ID) != "100.00" {
		t.Fatalf("balance = %s", balanceOf(t, pool, opened.Wallet.ID))
	}

	rollback := submit(t, wagers, opened.Wallet, opened.Player, "rollback-1", "ROLLBACK", "25.00", "bet-1")
	if rollback.Status != wager.StatusRejected || rollback.FailureCode != wager.CodeAlreadyReversed {
		t.Fatalf("second reversal %+v", rollback)
	}
	if balanceOf(t, pool, opened.Wallet.ID) != "100.00" {
		t.Fatal(balanceOf(t, pool, opened.Wallet.ID))
	}
}

func TestReferenceExpires(t *testing.T) {
	wallets, wagers, pool := newStack(t, 200*time.Millisecond, 4)
	defer pool.Close()
	truncate(t, pool)
	opened := open(t, wallets, "40.00")
	res := submit(t, wagers, opened.Wallet, opened.Player, "refund-missing", "REFUND", "10.00", "never-bet")
	if res.Status != wager.StatusPendingReference {
		t.Fatalf("%+v", res)
	}
	time.Sleep(350 * time.Millisecond)
	wagers.SweepOnce(context.Background())
	got, err := wallets.GetTransaction(context.Background(), res.TransactionID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status() != wager.StatusRejected || got.FailureCode() != wager.CodeReferenceNotFound {
		t.Fatalf("status=%s code=%s", got.Status(), got.FailureCode())
	}
	if n := count(t, pool, `SELECT count(*) FROM outbox WHERE event_type = 'WagerTransactionRejected'`); n == 0 {
		t.Fatal("missing rejection event")
	}
}

func TestRollbackInsufficientBalanceUsesItsOwnCode(t *testing.T) {
	wallets, wagers, pool := newStack(t, time.Second, 4)
	defer pool.Close()
	truncate(t, pool)
	opened := open(t, wallets, "100.00")
	win := submit(t, wagers, opened.Wallet, opened.Player, "win-1", "WIN", "20.00", "")
	if win.Status != wager.StatusProcessed || win.Balance.String() != "120.00" {
		t.Fatalf("win %+v", win)
	}
	bet := submit(t, wagers, opened.Wallet, opened.Player, "bet-big", "BET", "110.00", "")
	if bet.Status != wager.StatusProcessed || bet.Balance.String() != "10.00" {
		t.Fatalf("bet %+v", bet)
	}
	short := submit(t, wagers, opened.Wallet, opened.Player, "bet-short", "BET", "50.00", "")
	if short.Status != wager.StatusRejected || short.FailureCode != wager.CodeInsufficientBalance {
		t.Fatalf("short bet %+v", short)
	}
	rollback := submit(t, wagers, opened.Wallet, opened.Player, "rollback-win", "ROLLBACK", "20.00", "win-1")
	if rollback.Status != wager.StatusRejected || rollback.FailureCode != wager.CodeReversalInsufficientBalance {
		t.Fatalf("rollback %+v", rollback)
	}
	if balanceOf(t, pool, opened.Wallet.ID) != "10.00" {
		t.Fatal(balanceOf(t, pool, opened.Wallet.ID))
	}
}

func TestLossDoesNotMoveTheWallet(t *testing.T) {
	wallets, wagers, pool := newStack(t, time.Second, 4)
	defer pool.Close()
	truncate(t, pool)
	opened := open(t, wallets, "15.00")
	before := count(t, pool, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1`, opened.Wallet.ID)
	res := submit(t, wagers, opened.Wallet, opened.Player, "loss-1", "LOSS", "0.00", "")
	if res.Status != wager.StatusProcessed || res.Balance.String() != "15.00" {
		t.Fatalf("%+v", res)
	}
	view, err := wallets.Get(context.Background(), opened.Wallet.ID)
	if err != nil {
		t.Fatal(err)
	}
	if view.Version != 1 {
		t.Fatalf("version = %d", view.Version)
	}
	if count(t, pool, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1`, opened.Wallet.ID) != before {
		t.Fatal("loss wrote a ledger entry")
	}
	bad := submit(t, wagers, opened.Wallet, opened.Player, "loss-2", "LOSS", "1.00", "")
	if bad.Status != wager.StatusRejected || bad.FailureCode != wager.CodeInvalidAmount {
		t.Fatalf("%+v", bad)
	}
}

func TestInboxRedeliveryAndCrossChannel(t *testing.T) {
	wallets, wagers, pool := newStack(t, time.Second, 4)
	defer pool.Close()
	truncate(t, pool)
	opened := open(t, wallets, "80.00")
	body := messageBody(opened, "sqs-bet", "BET", "30.00", "")
	cmd := command(opened.Wallet, opened.Player, "sqs-bet", "provider-a:sqs-bet", "BET", "30.00", "")
	if err := wagers.AcceptDelivery(context.Background(), "msg-1", body, cmd); err != nil {
		t.Fatal(err)
	}
	if err := wagers.AcceptDelivery(context.Background(), "msg-1", body, cmd); err != nil {
		t.Fatal(err)
	}
	if err := wagers.AcceptDelivery(context.Background(), "msg-2", body, cmd); err != nil {
		t.Fatal(err)
	}
	httpRes := submit(t, wagers, opened.Wallet, opened.Player, "sqs-bet", "BET", "30.00", "")
	if !httpRes.IdempotentReplay {
		t.Fatalf("http replay %+v", httpRes)
	}
	if n := count(t, pool, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, opened.Wallet.ID); n != 1 {
		t.Fatalf("debits = %d", n)
	}
	changed := append([]byte{}, body...)
	changed = bytes.Replace(changed, []byte("30.00"), []byte("31.00"), 1)
	err := wagers.AcceptDelivery(context.Background(), "msg-1", changed, cmd)
	if !errs.IsPermanent(err) {
		t.Fatalf("hash mismatch err = %v", err)
	}
}

func TestLedgerIsImmutableAndBalanceCannotGoNegative(t *testing.T) {
	wallets, _, pool := newStack(t, time.Second, 4)
	defer pool.Close()
	truncate(t, pool)
	opened := open(t, wallets, "5.00")
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `DELETE FROM wallet_ledger_entries WHERE wallet_id = $1`, opened.Wallet.ID); err == nil {
		t.Fatal("deleted a ledger entry")
	}
	if _, err := pool.Exec(ctx, `UPDATE wallet_ledger_entries SET amount_minor = 1 WHERE wallet_id = $1`, opened.Wallet.ID); err == nil {
		t.Fatal("updated a ledger entry")
	}
	if _, err := pool.Exec(ctx, `UPDATE wallets SET balance_minor = -1 WHERE id = $1`, opened.Wallet.ID); err == nil {
		t.Fatal("stored a negative balance")
	}
	txRow := count(t, pool, `SELECT count(*) FROM wager_transactions WHERE wallet_id = $1 AND status = 'PROCESSED'`, opened.Wallet.ID)
	if txRow != 1 {
		t.Fatalf("opening rows = %d", txRow)
	}
	if _, err := pool.Exec(ctx, `UPDATE wager_transactions SET status = 'REJECTED', failure_code = 'NOPE' WHERE wallet_id = $1`, opened.Wallet.ID); err == nil {
		t.Fatal("updated a terminal transaction")
	}
}

func TestOutboxClaimIsExclusiveAndRecoverable(t *testing.T) {
	wallets, _, pool := newStack(t, time.Second, 4)
	defer pool.Close()
	truncate(t, pool)
	bus := &memBus{}
	cfg := config.Config{OutboxPoll: time.Second, OutboxLease: 30 * time.Second}
	store := postgres.NewStore(pool)
	_ = open(t, wallets, "12.00")
	publishers := []*outbox.Publisher{
		outbox.NewWorker(store, bus, discardLog(), nil, cfg),
		outbox.NewWorker(store, bus, discardLog(), nil, cfg),
	}
	var wg sync.WaitGroup
	for _, p := range publishers {
		wg.Add(1)
		go func(p *outbox.Publisher) {
			defer wg.Done()
			p.PublishOnce(context.Background())
		}(p)
	}
	wg.Wait()
	if n := count(t, pool, `SELECT count(*) FROM outbox WHERE published_at IS NULL`); n != 0 {
		t.Fatalf("unpublished = %d", n)
	}
	if n := count(t, pool, `SELECT count(*) FROM outbox WHERE attempts <> 1`); n != 0 {
		t.Fatalf("rows claimed more than once = %d", n)
	}
	if _, err := pool.Exec(context.Background(), `
		UPDATE outbox
		SET published_at = NULL, locked_by = 'dead', locked_until = now() - interval '1 minute', attempts = 1`); err != nil {
		t.Fatal(err)
	}
	publishers[0].PublishOnce(context.Background())
	if n := count(t, pool, `SELECT count(*) FROM outbox WHERE published_at IS NULL`); n != 0 {
		t.Fatalf("abandoned rows left unpublished = %d", n)
	}
	if got, want := len(bus.ids()), count(t, pool, `SELECT count(*) FROM outbox`); got < want {
		t.Fatalf("published messages = %d, rows = %d", got, want)
	}
}

func TestMigrationsDownAndUp(t *testing.T) {
	pool, err := pgxpool.New(context.Background(), dsn())
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	ctx := context.Background()
	defer func() { _ = migrate.Up(ctx, pool) }()
	if err := migrate.Down(ctx, pool); err != nil {
		t.Fatal(err)
	}
	var exists bool
	if err := pool.QueryRow(ctx, `SELECT to_regclass('public.wallets') IS NOT NULL`).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("wallets still exists after down")
	}
	if err := migrate.Up(ctx, pool); err != nil {
		t.Fatal(err)
	}
}

func TestAuthenticationAndProviderIsolation(t *testing.T) {
	issuer := requireKeycloak(t)
	wallets, wagers, pool := newStack(t, time.Second, 4)
	defer pool.Close()
	truncate(t, pool)
	reg := prometheus.NewRegistry()
	rec := metrics.New()
	if err := rec.Register(reg); err != nil {
		t.Fatal(err)
	}
	queues := newQueues(t, 5).client
	handler := httpapi.NewHandler(wallets, wagers, auth.NewVerifier(issuer, jwksURL(), "wagering-api"), pool, queues, reg, discardLog())
	srv := httptest.NewServer(handler)
	defer srv.Close()

	internal := token(t, issuer, "wallet-internal", "wallet-internal-secret")
	providerA := token(t, issuer, "provider-a", "provider-a-secret")
	providerB := token(t, issuer, "provider-b", "provider-b-secret")

	if status := call(t, srv, http.MethodPost, "/wallets", internal, nil); status == 0 {
		t.Fatal("no response")
	}
	if status := call(t, srv, http.MethodGet, "/wallets/"+uuid.Must(uuid.NewV7()).String(), "", nil); status != http.StatusUnauthorized {
		t.Fatalf("missing token status %d", status)
	}
	if status := call(t, srv, http.MethodGet, "/health/live", "", nil); status != http.StatusOK {
		t.Fatalf("live %d", status)
	}
	req := openBody(uuid.Must(uuid.NewV7()), "40.00")
	if status := call(t, srv, http.MethodPost, "/wallets", providerA, req); status != http.StatusForbidden {
		t.Fatalf("provider open status %d", status)
	}
	player := uuid.Must(uuid.NewV7())
	status, resp := callJSON(t, srv, http.MethodPost, "/wallets", internal, openBody(player, "40.00"))
	if status != http.StatusCreated {
		t.Fatalf("open status %d %s", status, resp)
	}
	var opened struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(resp, &opened); err != nil {
		t.Fatal(err)
	}
	bet := betBody(player, opened.ID, "provider-a", "auth-bet", "BET", "10.00", "")
	if status := call(t, srv, http.MethodPost, "/wagering/transactions", providerB, withKey(bet, "provider-a:auth-bet")); status != http.StatusForbidden {
		t.Fatalf("foreign submit status %d", status)
	}
	status, resp = callJSON(t, srv, http.MethodPost, "/wagering/transactions", providerA, withKey(bet, "provider-a:auth-bet"))
	if status != http.StatusOK {
		t.Fatalf("submit status %d %s", status, resp)
	}
	var result struct {
		TransactionID string `json:"transactionId"`
	}
	if err := json.Unmarshal(resp, &result); err != nil {
		t.Fatal(err)
	}
	if status := call(t, srv, http.MethodGet, "/wagering/transactions/"+result.TransactionID, providerB, nil); status != http.StatusNotFound {
		t.Fatalf("foreign read status %d", status)
	}
	if status := call(t, srv, http.MethodGet, "/providers/provider-a/wagering/transactions/auth-bet", providerB, nil); status != http.StatusForbidden {
		t.Fatalf("foreign external read status %d", status)
	}
	if status := call(t, srv, http.MethodGet, "/wagering/transactions/"+result.TransactionID, providerA, nil); status != http.StatusOK {
		t.Fatalf("owner read status %d", status)
	}
	if got := balanceOf(t, pool, uuid.MustParse(opened.ID)); got != "30.00" {
		t.Fatalf("balance = %s", got)
	}
	if status := call(t, srv, http.MethodGet, "/wallets/"+opened.ID, "not-a-token", nil); status != http.StatusUnauthorized {
		t.Fatalf("garbage token status %d", status)
	}

	walletID := uuid.MustParse(opened.ID)
	ledgerLines := count(t, pool, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1`, walletID)
	events := count(t, pool, `SELECT count(*) FROM outbox`)
	noEffect := func(step string) {
		t.Helper()
		if got := balanceOf(t, pool, walletID); got != "30.00" {
			t.Fatalf("%s: balance = %s", step, got)
		}
		if n := count(t, pool, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1`, walletID); n != ledgerLines {
			t.Fatalf("%s: ledger lines %d -> %d", step, ledgerLines, n)
		}
		if n := count(t, pool, `SELECT count(*) FROM outbox`); n != events {
			t.Fatalf("%s: outbox rows %d -> %d", step, events, n)
		}
	}

	for _, path := range []string{"/wallets/" + opened.ID, "/wallets/" + opened.ID + "/ledger"} {
		status, body := callJSON(t, srv, http.MethodGet, path, providerA, nil)
		if status != http.StatusForbidden || strings.Contains(string(body), "30.00") {
			t.Fatalf("provider on internal %s: %d %s", path, status, body)
		}
	}
	if status := call(t, srv, http.MethodPost, "/wallets/"+opened.ID+"/reconciliation", providerB, nil); status != http.StatusForbidden {
		t.Fatalf("provider reconciliation status %d", status)
	}
	if status := call(t, srv, http.MethodPost, "/wagering/transactions", internal, withKey(bet, "provider-a:internal-bet")); status != http.StatusForbidden {
		t.Fatalf("internal submit status %d", status)
	}

	stolen := betBody(player, opened.ID, "provider-b", "auth-bet", "BET", "10.00", "")
	status, resp = callJSON(t, srv, http.MethodPost, "/wagering/transactions", providerB, withKey(stolen, "provider-a:auth-bet"))
	if status != http.StatusForbidden || strings.Contains(string(resp), result.TransactionID) {
		t.Fatalf("provider b replaying a's key: %d %s", status, resp)
	}
	status, resp = callJSON(t, srv, http.MethodGet, "/wagering/transactions/"+result.TransactionID, providerB, nil)
	if strings.Contains(string(resp), opened.ID) || strings.Contains(string(resp), player.String()) {
		t.Fatalf("foreign read leaked data: %d %s", status, resp)
	}
	var openingID string
	if err := pool.QueryRow(context.Background(), `SELECT id::text FROM wager_transactions WHERE wallet_id = $1 AND kind = 'OPENING'`, walletID).Scan(&openingID); err != nil {
		t.Fatal(err)
	}
	if status := call(t, srv, http.MethodGet, "/wagering/transactions/"+openingID, providerA, nil); status != http.StatusNotFound {
		t.Fatalf("provider reading the internal opening %d", status)
	}
	if status := call(t, srv, http.MethodGet, "/wagering/transactions/"+result.TransactionID, internal, nil); status != http.StatusOK {
		t.Fatalf("internal read %d", status)
	}

	tampered := providerA[:len(providerA)-6] + "AAAAAA"
	if status := call(t, srv, http.MethodPost, "/wagering/transactions", tampered, withKey(betBody(player, opened.ID, "provider-a", "tampered", "BET", "1.00", ""), "provider-a:tampered")); status != http.StatusUnauthorized {
		t.Fatalf("tampered signature status %d", status)
	}
	if status := call(t, srv, http.MethodPost, "/wagering/transactions", "", withKey(betBody(player, opened.ID, "provider-a", "anon", "BET", "1.00", ""), "provider-a:anon")); status != http.StatusUnauthorized {
		t.Fatalf("missing token on submit %d", status)
	}
	noEffect("rejected calls")

	short := token(t, issuer, "provider-short", "provider-short-secret")
	time.Sleep(4 * time.Second)
	if status := call(t, srv, http.MethodPost, "/wagering/transactions", short, withKey(betBody(player, opened.ID, "provider-a", "expired-bet", "BET", "1.00", ""), "provider-a:expired-bet")); status != http.StatusUnauthorized {
		t.Fatalf("expired token status %d", status)
	}
	noEffect("expired token")
}

func TestQueueRoundTrip(t *testing.T) {
	q := newQueues(t, 5)
	cfg, client := q.cfg, q.client
	wallets, wagers, pool := newStack(t, time.Second, 4)
	defer pool.Close()
	truncate(t, pool)
	opened := open(t, wallets, "70.00")
	body := messageBody(opened, "queue-bet", "BET", "20.00", "")
	if err := client.SendInbound(context.Background(), opened.Wallet.ID.String(), uuid.NewString(), body); err != nil {
		t.Fatal(err)
	}
	consumer := sqsconsumer.NewWorker(client, wagers, discardLog(), nil, cfg)
	n, err := consumer.ProcessAvailable(context.Background())
	if err != nil || n == 0 {
		t.Fatalf("processed %d %v", n, err)
	}
	if balanceOf(t, pool, opened.Wallet.ID) != "50.00" {
		t.Fatal(balanceOf(t, pool, opened.Wallet.ID))
	}
	pub := outbox.NewWorker(postgres.NewStore(pool), client, discardLog(), nil, cfg)
	pub.PublishOnce(context.Background())
	events, err := client.ReceiveEvents(context.Background(), 10)
	if err != nil || len(events) == 0 {
		t.Fatalf("events %d %v", len(events), err)
	}
	ids := map[string]bool{}
	for _, raw := range events {
		var envelope struct {
			EventID   string `json:"eventId"`
			EventType string `json:"eventType"`
		}
		if err := json.Unmarshal(raw, &envelope); err != nil || envelope.EventID == "" {
			t.Fatalf("event %s: %v", raw, err)
		}
		ids[envelope.EventID] = true
	}
	for id := range ids {
		if count(t, pool, `SELECT count(*) FROM outbox WHERE event_id = $1 AND published_at IS NOT NULL`, uuid.MustParse(id)) != 1 {
			t.Fatalf("event %s was received but is not marked as published", id)
		}
	}
}

type openedWallet struct {
	Player uuid.UUID
	Wallet walletsvc.View
}

type stack struct {
	wallets *walletsvc.Service
	wagers  *wagering.Service
	pool    *pgxpool.Pool
}

type memBus struct {
	mu   sync.Mutex
	seen []string
}

func (m *memBus) Publish(_ context.Context, _, dedupID string, _ []byte) error {
	m.mu.Lock()
	m.seen = append(m.seen, dedupID)
	m.mu.Unlock()
	return nil
}

func (m *memBus) ids() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.seen...)
}

func newStack(t *testing.T, ttl time.Duration, attempts int) (*walletsvc.Service, *wagering.Service, *pgxpool.Pool) {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), dsn())
	if err != nil {
		t.Fatal(err)
	}
	store := postgres.NewStore(pool)
	log := discardLog()
	wallets := walletsvc.New(store, log, nil)
	wagers := wagering.New(wagering.Options{
		Store: store, Log: log, MaxAttempts: attempts, TTL: ttl,
		Backoff: 20 * time.Millisecond, PollInterval: 20 * time.Millisecond,
	})
	return wallets, wagers, pool
}

func open(t *testing.T, wallets *walletsvc.Service, amount string) openedWallet {
	t.Helper()
	player := uuid.Must(uuid.NewV7())
	view, err := wallets.Open(context.Background(), player, amount, "BRL", "corr-"+player.String())
	if err != nil {
		t.Fatal(err)
	}
	return openedWallet{Player: player, Wallet: view}
}

func submit(t *testing.T, svc *wagering.Service, wallet walletsvc.View, player uuid.UUID, external, kind, amount, ref string) wagering.Result {
	t.Helper()
	res, err := svc.Submit(context.Background(), command(wallet, player, external, "provider-a:"+external, kind, amount, ref))
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func command(wallet walletsvc.View, player uuid.UUID, external, key, kind, amount, ref string) wagering.Command {
	return wagering.Command{
		ProviderID: "provider-a", ExternalTransactionID: external, IdempotencyKey: key,
		PlayerID: player, WalletID: wallet.ID, RoundID: "round-1", GameID: "fortune-chimp",
		Kind: wager.Kind(kind), Amount: amount, Currency: "BRL", ReferenceExternalID: ref,
		CorrelationID: "corr-" + external,
	}
}

func truncate(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(context.Background(), `TRUNCATE reference_waits, outbox, inbox, wallet_ledger_entries, wager_transactions, wallets CASCADE`)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { assertFinancialConsistency(t) })
}

func count(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func balanceOf(t *testing.T, pool *pgxpool.Pool, id uuid.UUID) string {
	t.Helper()
	var minor int64
	if err := pool.QueryRow(context.Background(), `SELECT balance_minor FROM wallets WHERE id = $1`, id).Scan(&minor); err != nil {
		t.Fatal(err)
	}
	sign := ""
	if minor < 0 {
		sign = "-"
		minor = -minor
	}
	return fmt.Sprintf("%s%d.%02d", sign, minor/100, minor%100)
}

func eventTypes(t *testing.T, pool *pgxpool.Pool) []string {
	t.Helper()
	rows, err := pool.Query(context.Background(), `SELECT event_type FROM outbox`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var kind string
		if err := rows.Scan(&kind); err != nil {
			t.Fatal(err)
		}
		out = append(out, kind)
	}
	return out
}

func containsAll(got []string, want ...string) bool {
	set := map[string]bool{}
	for _, g := range got {
		set[g] = true
	}
	for _, w := range want {
		if !set[w] {
			return false
		}
	}
	return true
}

func messageBody(opened openedWallet, external, kind, amount, ref string) []byte {
	payload := map[string]any{
		"messageId":  "msg-" + external,
		"type":       "WagerTransactionRequested",
		"occurredAt": "2026-09-08T12:00:00.000Z",
		"data": map[string]any{
			"providerId": "provider-a", "externalTransactionId": external,
			"idempotencyKey": "provider-a:" + external,
			"playerId":       opened.Player.String(), "walletId": opened.Wallet.ID.String(),
			"roundId": "round-1", "gameId": "fortune-chimp", "kind": kind,
			"money": map[string]string{"amount": amount, "currency": "BRL"},
		},
	}
	if ref != "" {
		payload["data"].(map[string]any)["referenceExternalTransactionId"] = ref
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	return raw
}

func discardLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func dsn() string {
	return env("DATABASE_URL", "postgres://wagering:wagering@localhost:5435/wagering?sslmode=disable")
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func keycloakUp(issuer string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, issuer, nil)
	if err != nil {
		return false
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	res.Body.Close()
	return res.StatusCode < 500
}

func token(t *testing.T, issuer, clientID, secret string) string {
	t.Helper()
	res, err := http.PostForm(issuer+"/protocol/openid-connect/token", url.Values{
		"grant_type": {"client_credentials"}, "client_id": {clientID}, "client_secret": {secret},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var body struct {
		AccessToken string `json:"access_token"`
		Error       string `json:"error"`
	}
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.AccessToken == "" {
		t.Fatalf("token for %s: %s", clientID, body.Error)
	}
	return body.AccessToken
}

func openBody(player uuid.UUID, amount string) []byte {
	raw, _ := json.Marshal(map[string]any{
		"playerId":       player.String(),
		"initialBalance": map[string]string{"amount": amount, "currency": "BRL"},
	})
	return raw
}

func betBody(player uuid.UUID, walletID, provider, external, kind, amount, ref string) []byte {
	body := map[string]any{
		"providerId": provider, "externalTransactionId": external,
		"playerId": player.String(), "walletId": walletID,
		"roundId": "round-1", "gameId": "fortune-chimp", "kind": kind,
		"money": map[string]string{"amount": amount, "currency": "BRL"},
	}
	if ref != "" {
		body["referenceExternalTransactionId"] = ref
	}
	raw, _ := json.Marshal(body)
	return raw
}

func withKey(body []byte, key string) *keyedBody {
	return &keyedBody{body: body, key: key}
}

type keyedBody struct {
	body []byte
	key  string
}

func call(t *testing.T, srv *httptest.Server, method, path, bearer string, body any) int {
	t.Helper()
	status, _ := callJSON(t, srv, method, path, bearer, body)
	return status
}

func callJSON(t *testing.T, srv *httptest.Server, method, path, bearer string, body any) (int, []byte) {
	t.Helper()
	var reader io.Reader
	var key string
	switch v := body.(type) {
	case nil:
	case []byte:
		reader = bytes.NewReader(v)
	case *keyedBody:
		reader = bytes.NewReader(v.body)
		key = v.key
	default:
		t.Fatalf("bad body %T", body)
	}
	req, err := http.NewRequest(method, srv.URL+path, reader)
	if err != nil {
		t.Fatal(err)
	}
	if reader != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	return res.StatusCode, raw
}
