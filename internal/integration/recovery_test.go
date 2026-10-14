//go:build integration

package integration

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jungle-gaming/wagering/internal/app/port"
	"github.com/jungle-gaming/wagering/internal/app/wagering"
	walletsvc "github.com/jungle-gaming/wagering/internal/app/wallet"
	"github.com/jungle-gaming/wagering/internal/domain/wager"
	"github.com/jungle-gaming/wagering/internal/platform/config"
	"github.com/jungle-gaming/wagering/internal/platform/postgres"
	"github.com/jungle-gaming/wagering/internal/platform/sqs"
	"github.com/jungle-gaming/wagering/internal/transport/sqsconsumer"
)

// The full Fx graph starts with real dependencies, serves HTTP, consumes the
// queue and publishes the outbox; Stop must end every worker goroutine,
// close the HTTP listener and close the pool.
func TestFxAppStartsServesAndReleasesResources(t *testing.T) {
	issuer := requireKeycloak(t)
	q := newQueues(t, 5)
	pool := testPool(t)
	truncate(t, pool)
	configureApps(t, q)

	baseline := workerGoroutines()
	app := startApp(t)
	if running := workerGoroutines(); running <= baseline {
		t.Fatalf("worker goroutines %d, baseline %d", running, baseline)
	}
	if res := apiCall(t, app.base, http.MethodGet, "/health/ready", "", "", nil); res.Status != http.StatusOK || res.str("status") != "ready" {
		t.Fatalf("ready %d %s", res.Status, res.Raw)
	}
	if res := apiCall(t, app.base, http.MethodGet, "/metrics", "", "", nil); res.Status != http.StatusOK || !strings.Contains(res.Raw, "outbox_oldest_unpublished_seconds") {
		t.Fatalf("metrics %d", res.Status)
	}

	internal := token(t, issuer, "wallet-internal", "wallet-internal-secret")
	providerA := token(t, issuer, "provider-a", "provider-a-secret")
	player := uuid.Must(uuid.NewV7())
	walletID := openViaAPI(t, app.base, internal, player, "50.00")
	res := apiCall(t, app.base, http.MethodPost, "/wagering/transactions", providerA, "provider-a:fx-bet",
		wagerBody("provider-a", player, walletID, "fx-bet", "BET", "5.00", ""))
	if res.Status != http.StatusOK {
		t.Fatalf("bet %d %s", res.Status, res.Raw)
	}
	q.send(t, walletID, "fx-msg", queueMessage("fx-msg", wagerBody("provider-a", player, walletID, "fx-sqs", "BET", "7.00", "")))
	waitFor(t, "app consumer", 15*time.Second, func() bool { return inboxCompleted(t, pool, "fx-msg") })
	waitFor(t, "app publisher", 15*time.Second, func() bool {
		return count(t, pool, `SELECT count(*) FROM outbox WHERE published_at IS NULL`) == 0
	})

	addr := app.srv.Addr()
	app.stop(t)
	waitFor(t, "worker goroutines to exit", 5*time.Second, func() bool { return workerGoroutines() <= baseline })
	if err := app.pool.Ping(context.Background()); err == nil {
		t.Fatal("pool still usable after stop")
	}
	if n := app.pool.Stat().TotalConns(); n != 0 {
		t.Fatalf("pool still holds %d connections", n)
	}
	if conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond); err == nil {
		conn.Close()
		t.Fatal("HTTP listener still accepts connections")
	}
	if balanceOf(t, pool, uuid.MustParse(walletID)) != "38.00" {
		t.Fatal(balanceOf(t, pool, uuid.MustParse(walletID)))
	}
}

// A first application records a bet, a pending refund and a queued bet, then
// stops. A second one, with fresh memory and pool, must replay the stored
// results, resume the pending refund and keep the books balanced.
func TestRestartPreservesIdempotencyPendingAndBalances(t *testing.T) {
	issuer := requireKeycloak(t)
	q := newQueues(t, 5)
	pool := testPool(t)
	truncate(t, pool)
	configureApps(t, q)
	internal := token(t, issuer, "wallet-internal", "wallet-internal-secret")
	providerA := token(t, issuer, "provider-a", "provider-a-secret")

	first := startApp(t)
	player := uuid.Must(uuid.NewV7())
	walletID := openViaAPI(t, first.base, internal, player, "100.00")
	bet := wagerBody("provider-a", player, walletID, "restart-bet", "BET", "25.00", "")
	res := apiCall(t, first.base, http.MethodPost, "/wagering/transactions", providerA, "provider-a:restart-bet", bet)
	if res.Status != http.StatusOK || res.balance() != "75.00" {
		t.Fatalf("bet %d %s", res.Status, res.Raw)
	}
	betID := res.str("transactionId")
	refund := wagerBody("provider-a", player, walletID, "restart-refund", "REFUND", "10.00", "restart-late")
	res = apiCall(t, first.base, http.MethodPost, "/wagering/transactions", providerA, "provider-a:restart-refund", refund)
	if res.Status != http.StatusAccepted || res.str("status") != string(wager.StatusPendingReference) {
		t.Fatalf("refund %d %s", res.Status, res.Raw)
	}
	refundID := res.str("transactionId")
	queued := wagerBody("provider-a", player, walletID, "restart-sqs", "BET", "5.00", "")
	q.send(t, walletID, "restart-msg", queueMessage("restart-msg", queued))
	waitFor(t, "first app consumer", 15*time.Second, func() bool { return inboxCompleted(t, pool, "restart-msg") })
	first.stop(t)

	if statusByExternal(t, pool, "restart-refund") != string(wager.StatusPendingReference) {
		t.Fatal("pending refund did not survive the stop")
	}
	if count(t, pool, `SELECT count(*) FROM reference_waits WHERE transaction_id = $1`, uuid.MustParse(refundID)) != 1 {
		t.Fatal("reference wait did not survive the stop")
	}

	second := startApp(t)
	defer second.stop(t)
	res = apiCall(t, second.base, http.MethodPost, "/wagering/transactions", providerA, "provider-a:restart-bet", bet)
	if res.Status != http.StatusOK || res.Body["idempotentReplay"] != true || res.str("transactionId") != betID || res.balance() != "75.00" {
		t.Fatalf("bet replay after restart %d %s", res.Status, res.Raw)
	}
	res = apiCall(t, second.base, http.MethodPost, "/wagering/transactions", providerA, "provider-a:restart-refund", refund)
	if res.Status != http.StatusAccepted || res.Body["idempotentReplay"] != true || res.str("transactionId") != refundID {
		t.Fatalf("refund replay after restart %d %s", res.Status, res.Raw)
	}
	q.send(t, walletID, "restart-msg-again", queueMessage("restart-msg-again", queued))
	waitFor(t, "second app consumer", 15*time.Second, func() bool { return inboxCompleted(t, pool, "restart-msg-again") })

	late := wagerBody("provider-a", player, walletID, "restart-late", "BET", "10.00", "")
	if res := apiCall(t, second.base, http.MethodPost, "/wagering/transactions", providerA, "provider-a:restart-late", late); res.Status != http.StatusOK {
		t.Fatalf("late bet %d %s", res.Status, res.Raw)
	}
	waitFor(t, "refund resolved by the second app", 15*time.Second, func() bool {
		return statusByExternal(t, pool, "restart-refund") == string(wager.StatusProcessed)
	})
	read := apiCall(t, second.base, http.MethodGet, "/providers/provider-a/wagering/transactions/restart-refund", providerA, "", nil)
	if read.Status != http.StatusOK || read.str("status") != string(wager.StatusProcessed) || read.str("transactionId") != refundID {
		t.Fatalf("refund after restart %d %s", read.Status, read.Raw)
	}
	rec := apiCall(t, second.base, http.MethodPost, "/wallets/"+walletID+"/reconciliation", internal, "", nil)
	stored, _ := rec.Body["storedBalance"].(map[string]any)
	if rec.Status != http.StatusOK || rec.Body["consistent"] != true || stored["amount"] != "70.00" {
		t.Fatalf("reconciliation %d %s", rec.Status, rec.Raw)
	}
	if n := count(t, pool, `SELECT count(*) FROM wager_transactions WHERE wallet_id = $1 AND origin = 'EXTERNAL'`, uuid.MustParse(walletID)); n != 4 {
		t.Fatalf("external transactions = %d", n)
	}
	waitFor(t, "outbox drained after restart", 15*time.Second, func() bool {
		return count(t, pool, `SELECT count(*) FROM outbox WHERE published_at IS NULL`) == 0
	})
}

// A separate process confirms a refund as PENDING_REFERENCE, claims its wait
// row as its reference worker would, and is killed before applying it.
// Another instance must take the work over once the claim lease expires.
func TestPendingReferenceResumedAfterProcessIsKilled(t *testing.T) {
	wallets, wagers, pool := newStack(t, time.Minute, 8)
	defer pool.Close()
	truncate(t, pool)
	opened := open(t, wallets, "100.00")

	cmd := exec.Command(os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(),
		"WAGER_PROC=pending-and-hang", "DATABASE_URL="+dsn(),
		"WALLET_ID="+opened.Wallet.ID.String(), "PLAYER_ID="+opened.Player.String(),
		"EXTERNAL_ID=refund-killed", "REF=bet-killed", "AMOUNT=20.00", "LEASE=3s",
	)
	line := runUntilKilled(t, cmd, "CLAIMED")
	txID := uuid.MustParse(strings.TrimSpace(strings.TrimPrefix(line, "CLAIMED")))

	if statusByExternal(t, pool, "refund-killed") != string(wager.StatusPendingReference) {
		t.Fatal("killed process did not leave the refund pending")
	}
	if count(t, pool, `SELECT count(*) FROM reference_waits WHERE transaction_id = $1 AND locked_until > now()`, txID) != 1 {
		t.Fatal("wait row is not held by the killed process")
	}

	bet := submit(t, wagers, opened.Wallet, opened.Player, "bet-killed", "BET", "20.00", "")
	if bet.Status != wager.StatusProcessed {
		t.Fatalf("bet %+v", bet)
	}
	wagers.SweepOnce(context.Background())
	if statusByExternal(t, pool, "refund-killed") != string(wager.StatusPendingReference) {
		t.Fatal("work was taken before the dead claim expired")
	}
	waitFor(t, "takeover after the lease", 10*time.Second, func() bool {
		wagers.SweepOnce(context.Background())
		return statusByExternal(t, pool, "refund-killed") == string(wager.StatusProcessed)
	})
	if balanceOf(t, pool, opened.Wallet.ID) != "100.00" {
		t.Fatal(balanceOf(t, pool, opened.Wallet.ID))
	}
	if count(t, pool, `SELECT count(*) FROM reference_waits`) != 0 {
		t.Fatal("wait row left behind")
	}
}

// A separate consumer process commits the debit and is killed inside Delete,
// before the message leaves the queue. After the visibility timeout another
// consumer receives it again; the inbox makes it a no-op and it is removed.
func TestConsumerKilledAfterCommitBeforeDelete(t *testing.T) {
	q := newQueues(t, 5)
	wallets, wagers, pool := newStack(t, time.Minute, 4)
	defer pool.Close()
	truncate(t, pool)
	opened := open(t, wallets, "80.00")
	q.send(t, opened.Wallet.ID.String(), "killed-1", messageBody(opened, "killed-bet", "BET", "30.00", ""))

	cmd := exec.Command(os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(),
		"WAGER_PROC=consume-and-hang", "DATABASE_URL="+dsn(),
		"SQS_ENDPOINT="+q.cfg.SQSEndpoint, "SQS_QUEUE_URL="+q.cfg.QueueURL,
		"SQS_DLQ_URL="+q.cfg.DLQURL, "SQS_EVENTS_QUEUE_URL="+q.cfg.EventsQueueURL,
	)
	runUntilKilled(t, cmd, "COMMITTED")

	if n := count(t, pool, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, opened.Wallet.ID); n != 1 {
		t.Fatalf("debits after the crash = %d", n)
	}
	if !inboxCompleted(t, pool, "msg-killed-bet") {
		t.Fatal("inbox was not committed with the debit")
	}

	rec := &recordingQueues{Queues: q.client}
	consumer := sqsconsumer.NewWorker(rec, wagers, discardLog(), nil, q.cfg)
	waitFor(t, "redelivery after the visibility timeout", 20*time.Second, func() bool {
		_, _ = consumer.ProcessAvailable(context.Background())
		_, deleted := rec.snapshot()
		return deleted == 1
	})
	receives, _ := rec.snapshot()
	if len(receives) != 1 || receives[0] < 2 {
		t.Fatalf("redelivery receive counts = %v", receives)
	}
	if n := count(t, pool, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, opened.Wallet.ID); n != 1 {
		t.Fatalf("debits after redelivery = %d", n)
	}
	if balanceOf(t, pool, opened.Wallet.ID) != "50.00" || q.inboundDepth(t) != 0 {
		t.Fatalf("balance=%s depth=%d", balanceOf(t, pool, opened.Wallet.ID), q.inboundDepth(t))
	}
}

// Three complete applications (own pool, own memory, own workers) share the
// database and the queues. The concurrency scenarios go through HTTP with
// real Keycloak tokens and are spread across the three instances.
func TestThreeApplicationInstances(t *testing.T) {
	issuer := requireKeycloak(t)
	q := newQueues(t, 5)
	pool := testPool(t)
	truncate(t, pool)
	configureApps(t, q)
	apps := []*runningApp{startApp(t), startApp(t), startApp(t)}
	defer func() {
		for _, a := range apps {
			a.stop(t)
		}
	}()
	internal := token(t, issuer, "wallet-internal", "wallet-internal-secret")
	providerA := token(t, issuer, "provider-a", "provider-a-secret")
	post := func(app int, key string, body map[string]any) apiResult {
		return apiCall(t, apps[app%3].base, http.MethodPost, "/wagering/transactions", providerA, key, body)
	}

	t.Run("fifty identical bets", func(t *testing.T) {
		player := uuid.Must(uuid.NewV7())
		walletID := openViaAPI(t, apps[0].base, internal, player, "100.00")
		body := wagerBody("provider-a", player, walletID, "three-dup", "BET", "10.00", "")
		results := make([]apiResult, 50)
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := range results {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				results[i] = post(i, "provider-a:three-dup", body)
			}(i)
		}
		close(start)
		wg.Wait()
		fresh, replays := 0, 0
		for i, res := range results {
			if res.Status != http.StatusOK || res.str("transactionId") != results[0].str("transactionId") {
				t.Fatalf("request %d: %d %s", i, res.Status, res.Raw)
			}
			if res.Body["idempotentReplay"] == true {
				replays++
			} else {
				fresh++
			}
		}
		if fresh != 1 || replays != 49 {
			t.Fatalf("fresh=%d replays=%d", fresh, replays)
		}
		id := uuid.MustParse(walletID)
		if n := count(t, pool, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, id); n != 1 {
			t.Fatalf("debits = %d", n)
		}
		if balanceOf(t, pool, id) != "90.00" {
			t.Fatal(balanceOf(t, pool, id))
		}
	})

	t.Run("two bets of 80 on 100", func(t *testing.T) {
		player := uuid.Must(uuid.NewV7())
		walletID := openViaAPI(t, apps[0].base, internal, player, "100.00")
		results := make([]apiResult, 2)
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i, external := range []string{"three-80-a", "three-80-b"} {
			wg.Add(1)
			go func(i int, external string) {
				defer wg.Done()
				<-start
				results[i] = post(i+1, "provider-a:"+external, wagerBody("provider-a", player, walletID, external, "BET", "80.00", ""))
			}(i, external)
		}
		close(start)
		wg.Wait()
		statuses := []int{results[0].Status, results[1].Status}
		sort.Ints(statuses)
		if statuses[0] != http.StatusOK || statuses[1] != http.StatusUnprocessableEntity {
			t.Fatalf("statuses %v: %s | %s", statuses, results[0].Raw, results[1].Raw)
		}
		for _, res := range results {
			if res.Status == http.StatusUnprocessableEntity && res.str("failureCode") != wager.CodeInsufficientBalance {
				t.Fatalf("rejection %s", res.Raw)
			}
		}
		if balanceOf(t, pool, uuid.MustParse(walletID)) != "20.00" {
			t.Fatal(balanceOf(t, pool, uuid.MustParse(walletID)))
		}
	})

	t.Run("distinct wallets in parallel", func(t *testing.T) {
		type target struct {
			player uuid.UUID
			wallet string
		}
		targets := make([]target, 6)
		for i := range targets {
			targets[i].player = uuid.Must(uuid.NewV7())
			targets[i].wallet = openViaAPI(t, apps[i%3].base, internal, targets[i].player, "50.00")
		}
		var wg sync.WaitGroup
		var mu sync.Mutex
		failures := 0
		start := make(chan struct{})
		for i, tg := range targets {
			for j := 0; j < 10; j++ {
				wg.Add(1)
				go func(i, j int, tg target) {
					defer wg.Done()
					<-start
					external := fmt.Sprintf("par-%d-%d", i, j)
					res := post(i+j, "provider-a:"+external, wagerBody("provider-a", tg.player, tg.wallet, external, "BET", "1.00", ""))
					if res.Status != http.StatusOK {
						mu.Lock()
						failures++
						mu.Unlock()
					}
				}(i, j, tg)
			}
		}
		close(start)
		wg.Wait()
		if failures != 0 {
			t.Fatalf("%d parallel bets failed", failures)
		}
		for _, tg := range targets {
			if got := balanceOf(t, pool, uuid.MustParse(tg.wallet)); got != "40.00" {
				t.Fatalf("wallet %s balance %s", tg.wallet, got)
			}
		}
	})

	t.Run("pending reference resolved by any instance", func(t *testing.T) {
		player := uuid.Must(uuid.NewV7())
		walletID := openViaAPI(t, apps[1].base, internal, player, "30.00")
		res := post(0, "provider-a:three-refund", wagerBody("provider-a", player, walletID, "three-refund", "REFUND", "5.00", "three-ref-bet"))
		if res.Status != http.StatusAccepted {
			t.Fatalf("refund %d %s", res.Status, res.Raw)
		}
		if res := post(2, "provider-a:three-ref-bet", wagerBody("provider-a", player, walletID, "three-ref-bet", "BET", "5.00", "")); res.Status != http.StatusOK {
			t.Fatalf("bet %d %s", res.Status, res.Raw)
		}
		waitFor(t, "refund resolution", 15*time.Second, func() bool {
			return statusByExternal(t, pool, "three-refund") == string(wager.StatusProcessed)
		})
		if balanceOf(t, pool, uuid.MustParse(walletID)) != "30.00" {
			t.Fatal(balanceOf(t, pool, uuid.MustParse(walletID)))
		}
	})

	t.Run("queue consumed by one of the instances", func(t *testing.T) {
		player := uuid.Must(uuid.NewV7())
		walletID := openViaAPI(t, apps[2].base, internal, player, "30.00")
		body := wagerBody("provider-a", player, walletID, "three-sqs", "BET", "3.00", "")
		for i := 0; i < 3; i++ {
			q.send(t, walletID, fmt.Sprintf("three-sqs-%d", i), queueMessage(fmt.Sprintf("three-sqs-%d", i), body))
		}
		waitFor(t, "three deliveries", 20*time.Second, func() bool {
			return count(t, pool, `SELECT count(*) FROM inbox WHERE message_id LIKE 'three-sqs-%' AND completed_at IS NOT NULL`) == 3
		})
		if res := post(1, "provider-a:three-sqs", body); res.Status != http.StatusOK || res.Body["idempotentReplay"] != true {
			t.Fatalf("http replay %d %s", res.Status, res.Raw)
		}
		if balanceOf(t, pool, uuid.MustParse(walletID)) != "27.00" {
			t.Fatal(balanceOf(t, pool, uuid.MustParse(walletID)))
		}
	})

	waitFor(t, "outbox drained by the instances", 15*time.Second, func() bool {
		return count(t, pool, `SELECT count(*) FROM outbox WHERE published_at IS NULL`) == 0
	})
	if n := count(t, pool, `SELECT count(DISTINCT locked_by) FROM outbox WHERE locked_by IS NOT NULL`); n > 3 {
		t.Fatalf("unexpected publishers %d", n)
	}
}

// runUntilKilled starts cmd, waits for a stdout line starting with marker,
// kills the process and returns that line.
func runUntilKilled(t *testing.T, cmd *exec.Cmd, marker string) string {
	t.Helper()
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	found := make(chan string, 1)
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			if strings.HasPrefix(sc.Text(), marker) {
				found <- sc.Text()
				return
			}
		}
		close(found)
	}()
	var line string
	select {
	case l, ok := <-found:
		if !ok {
			_ = cmd.Wait()
			t.Fatalf("child exited before %s: %s", marker, stderr.String())
		}
		line = l
	case <-time.After(30 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatalf("child never printed %s: %s", marker, stderr.String())
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	return line
}

func childPool() (*pgxpool.Pool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn())
	if err != nil {
		return nil, err
	}
	return pool, pool.Ping(ctx)
}

func pendingAndHang() int {
	pool, err := childPool()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	store := postgres.NewStore(pool)
	wagers := wagering.New(wagering.Options{Store: store, MaxAttempts: 8, TTL: time.Minute, Backoff: 10 * time.Millisecond})
	walletID := uuid.MustParse(os.Getenv("WALLET_ID"))
	playerID := uuid.MustParse(os.Getenv("PLAYER_ID"))
	external := os.Getenv("EXTERNAL_ID")
	res, err := wagers.Submit(context.Background(), command(walletsvc.View{ID: walletID}, playerID, external, "provider-a:"+external, "REFUND", os.Getenv("AMOUNT"), os.Getenv("REF")))
	if err != nil || res.Status != wager.StatusPendingReference {
		fmt.Fprintln(os.Stderr, "submit", res.Status, err)
		return 1
	}
	lease, _ := time.ParseDuration(os.Getenv("LEASE"))
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		ids, err := store.ClaimWaits(context.Background(), 20, lease)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		for _, id := range ids {
			if id == res.TransactionID {
				fmt.Println("CLAIMED", id)
				time.Sleep(time.Hour)
				return 0
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	fmt.Fprintln(os.Stderr, "never claimed the wait row")
	return 1
}

// hangOnDelete stands for a consumer that dies after the commit and before
// the message is removed: Delete reports and then never returns.
type hangOnDelete struct{ port.Queues }

func (hangOnDelete) Delete(context.Context, string) error {
	fmt.Println("COMMITTED")
	time.Sleep(time.Hour)
	return nil
}

func consumeAndHang() int {
	pool, err := childPool()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	cfg := config.Config{
		SQSEndpoint: os.Getenv("SQS_ENDPOINT"), SQSRegion: "us-east-1", AWSAccessKey: "test", AWSSecretKey: "test",
		QueueURL: os.Getenv("SQS_QUEUE_URL"), DLQURL: os.Getenv("SQS_DLQ_URL"), EventsQueueURL: os.Getenv("SQS_EVENTS_QUEUE_URL"),
		SQSMaxMessages: 1, SQSWaitSeconds: 2, SQSVisibilitySeconds: 3, ShutdownTimeout: time.Minute,
	}
	client, err := sqs.New(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	wagers := wagering.New(wagering.Options{Store: postgres.NewStore(pool)})
	consumer := sqsconsumer.NewWorker(hangOnDelete{client}, wagers, nil, nil, cfg)
	for i := 0; i < 10; i++ {
		if _, err := consumer.ProcessAvailable(context.Background()); err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
	}
	fmt.Fprintln(os.Stderr, "no message received")
	return 1
}
