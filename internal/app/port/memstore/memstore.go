// Package memstore is an in-memory port.Store for unit tests. Each WithinTx
// runs serialized on a copy of the state and swaps it in only on success, so
// a failed callback leaves nothing behind. Unique indexes and CHECK rules
// mirror migrations/0001_init.up.sql and report the same constraint names.
package memstore

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jungle-gaming/wagering/internal/app/port"
	"github.com/jungle-gaming/wagering/internal/domain/event"
	"github.com/jungle-gaming/wagering/internal/domain/ledger"
	"github.com/jungle-gaming/wagering/internal/domain/money"
	"github.com/jungle-gaming/wagering/internal/domain/wager"
	"github.com/jungle-gaming/wagering/internal/domain/wallet"
)

var ErrCheck = errors.New("check constraint violated")

type walletRow struct {
	id        uuid.UUID
	playerID  uuid.UUID
	balance   money.Money
	version   int64
	createdAt time.Time
	updatedAt time.Time
}

type outboxRow struct {
	env         event.Envelope
	attempts    int
	nextAt      time.Time
	publishedAt *time.Time
	lockedUntil time.Time
}

type waitRow struct {
	wait        port.Wait
	lockedUntil time.Time
}

type state struct {
	wallets map[uuid.UUID]walletRow
	wagers  map[uuid.UUID]wager.Snapshot
	ledger  []ledger.Entry
	inbox   map[[2]string]port.Inbox
	outbox  []outboxRow
	waits   map[uuid.UUID]waitRow
}

func (s *state) clone() *state {
	out := &state{
		wallets: make(map[uuid.UUID]walletRow, len(s.wallets)),
		wagers:  make(map[uuid.UUID]wager.Snapshot, len(s.wagers)),
		ledger:  append([]ledger.Entry(nil), s.ledger...),
		inbox:   make(map[[2]string]port.Inbox, len(s.inbox)),
		outbox:  append([]outboxRow(nil), s.outbox...),
		waits:   make(map[uuid.UUID]waitRow, len(s.waits)),
	}
	for k, v := range s.wallets {
		out.wallets[k] = v
	}
	for k, v := range s.wagers {
		out.wagers[k] = v
	}
	for k, v := range s.inbox {
		out.inbox[k] = v
	}
	for k, v := range s.waits {
		out.waits[k] = v
	}
	return out
}

type Store struct {
	mu       sync.Mutex
	st       *state
	failNext []error
	Now      func() time.Time
}

func New() *Store {
	return &Store{
		st: &state{
			wallets: map[uuid.UUID]walletRow{},
			wagers:  map[uuid.UUID]wager.Snapshot{},
			inbox:   map[[2]string]port.Inbox{},
			waits:   map[uuid.UUID]waitRow{},
		},
		Now: time.Now,
	}
}

// FailNext makes the next WithinTx calls return the given errors, in order,
// without running the callback.
func (s *Store) FailNext(errs ...error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failNext = append(s.failNext, errs...)
}

func (s *Store) WithinTx(ctx context.Context, fn func(context.Context, port.Tx) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.failNext) > 0 {
		err := s.failNext[0]
		s.failNext = s.failNext[1:]
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	work := s.st.clone()
	if err := fn(ctx, &tx{st: work}); err != nil {
		return err
	}
	s.st = work
	return nil
}

func (s *Store) GetWallet(_ context.Context, id uuid.UUID) (*wallet.Wallet, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return (&tx{st: s.st}).wallet(id)
}

func (s *Store) GetWager(_ context.Context, id uuid.UUID) (*wager.Transaction, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return (&tx{st: s.st}).FindWagerByID(context.Background(), id)
}

func (s *Store) GetWagerByExternal(_ context.Context, providerID, externalID string) (*wager.Transaction, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return (&tx{st: s.st}).FindWagerByExternal(context.Background(), providerID, externalID)
}

func (s *Store) GetWagerByIdempotency(_ context.Context, key string) (*wager.Transaction, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return (&tx{st: s.st}).FindWagerByIdempotency(context.Background(), key)
}

func (s *Store) ListLedger(_ context.Context, walletID uuid.UUID, after *port.LedgerCursor, limit int) (port.LedgerPage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return (&tx{st: s.st}).ListLedger(context.Background(), walletID, after, limit)
}

func (s *Store) ClaimWaits(_ context.Context, limit int, lease time.Duration) ([]uuid.UUID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.Now()
	rows := make([]waitRow, 0, len(s.st.waits))
	for _, row := range s.st.waits {
		if !row.wait.NextAttemptAt.After(now) && row.lockedUntil.Before(now) {
			rows = append(rows, row)
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].wait.NextAttemptAt.Before(rows[j].wait.NextAttemptAt) })
	var ids []uuid.UUID
	for _, row := range rows {
		if len(ids) == limit {
			break
		}
		row.lockedUntil = now.Add(lease)
		s.st.waits[row.wait.TransactionID] = row
		ids = append(ids, row.wait.TransactionID)
	}
	return ids, nil
}

func (s *Store) ClaimOutbox(_ context.Context, _ string, limit int, lease time.Duration) ([]port.OutboxMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.Now()
	var out []port.OutboxMessage
	for i := range s.st.outbox {
		row := &s.st.outbox[i]
		if len(out) == limit {
			break
		}
		if row.publishedAt != nil || row.nextAt.After(now) || !row.lockedUntil.Before(now) {
			continue
		}
		row.attempts++
		row.lockedUntil = now.Add(lease)
		out = append(out, port.OutboxMessage{
			EventID: row.env.EventID(), AggregateID: row.env.AggregateID(), EventType: row.env.EventType(),
			Payload: row.env.Payload(), Attempts: row.attempts,
		})
	}
	return out, nil
}

func (s *Store) MarkOutboxPublished(_ context.Context, eventID uuid.UUID, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.st.outbox {
		row := &s.st.outbox[i]
		if row.env.EventID() == eventID && row.publishedAt == nil {
			stamp := at
			row.publishedAt = &stamp
			row.lockedUntil = time.Time{}
		}
	}
	return nil
}

func (s *Store) RescheduleOutbox(_ context.Context, eventID uuid.UUID, next time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.st.outbox {
		row := &s.st.outbox[i]
		if row.env.EventID() == eventID && row.publishedAt == nil {
			row.nextAt = next
			row.lockedUntil = time.Time{}
		}
	}
	return nil
}

func (s *Store) OutboxLag(context.Context) (time.Duration, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var oldest time.Time
	for _, row := range s.st.outbox {
		if row.publishedAt == nil && (oldest.IsZero() || row.env.OccurredAt().Before(oldest)) {
			oldest = row.env.OccurredAt()
		}
	}
	if oldest.IsZero() {
		return 0, nil
	}
	return s.Now().Sub(oldest), nil
}

// Events returns every outbox envelope in insertion order.
func (s *Store) Events() []event.Envelope {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]event.Envelope, 0, len(s.st.outbox))
	for _, row := range s.st.outbox {
		out = append(out, row.env)
	}
	return out
}

// Unpublished counts outbox rows still waiting for the publisher.
func (s *Store) Unpublished() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, row := range s.st.outbox {
		if row.publishedAt == nil {
			n++
		}
	}
	return n
}

// Ledger returns the wallet entries in posting order.
func (s *Store) Ledger(walletID uuid.UUID) []ledger.Entry {
	s.mu.Lock()
	defer s.mu.Unlock()
	return (&tx{st: s.st}).entries(walletID)
}

// Wagers returns every transaction recorded for the wallet.
func (s *Store) Wagers(walletID uuid.UUID) []wager.Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []wager.Snapshot
	for _, snap := range s.st.wagers {
		if snap.WalletID == walletID {
			out = append(out, snap)
		}
	}
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i].ID[:], out[j].ID[:]) < 0 })
	return out
}

func (s *Store) Wait(id uuid.UUID) (port.Wait, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.st.waits[id]
	return row.wait, ok
}

func (s *Store) Inbox(consumer, messageID string) (port.Inbox, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.st.inbox[[2]string{consumer, messageID}]
	return row, ok
}

// TamperBalance overwrites the stored balance without a ledger entry, the
// way a manual UPDATE would. Reconciliation must notice it.
func (s *Store) TamperBalance(id uuid.UUID, minor int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	row := s.st.wallets[id]
	bal, err := money.FromMinor(minor, row.balance.Currency())
	if err != nil {
		panic(err)
	}
	row.balance = bal
	s.st.wallets[id] = row
}

type tx struct{ st *state }

func (t *tx) wallet(id uuid.UUID) (*wallet.Wallet, error) {
	row, ok := t.st.wallets[id]
	if !ok {
		return nil, port.ErrNotFound
	}
	return wallet.Rehydrate(row.id, row.playerID, row.balance, row.version, row.createdAt, row.updatedAt)
}

func (t *tx) LockWallet(_ context.Context, id uuid.UUID, _ port.LockMode) (*wallet.Wallet, error) {
	return t.wallet(id)
}

func (t *tx) InsertWallet(_ context.Context, w *wallet.Wallet) error {
	if _, ok := t.st.wallets[w.ID()]; ok {
		return &port.Conflict{Constraint: "wallets_pkey"}
	}
	for _, row := range t.st.wallets {
		if row.playerID == w.PlayerID() && row.balance.Currency() == w.Currency() {
			return &port.Conflict{Constraint: "wallets_player_currency"}
		}
	}
	if w.Balance().IsNegative() || w.Version() < 1 {
		return ErrCheck
	}
	t.st.wallets[w.ID()] = walletRow{
		id: w.ID(), playerID: w.PlayerID(), balance: w.Balance(), version: w.Version(),
		createdAt: w.CreatedAt(), updatedAt: w.UpdatedAt(),
	}
	return nil
}

func (t *tx) UpdateWallet(_ context.Context, w *wallet.Wallet, previousVersion int64) error {
	row, ok := t.st.wallets[w.ID()]
	if !ok || row.version != previousVersion {
		return port.ErrConcurrentUpdate
	}
	if w.Balance().IsNegative() {
		return ErrCheck
	}
	row.balance = w.Balance()
	row.version = w.Version()
	row.updatedAt = w.UpdatedAt()
	t.st.wallets[w.ID()] = row
	return nil
}

func (t *tx) FindWagerByID(_ context.Context, id uuid.UUID) (*wager.Transaction, error) {
	snap, ok := t.st.wagers[id]
	if !ok {
		return nil, port.ErrNotFound
	}
	return wager.Rehydrate(snap)
}

func (t *tx) LockWager(ctx context.Context, id uuid.UUID) (*wager.Transaction, error) {
	return t.FindWagerByID(ctx, id)
}

func (t *tx) FindWagerByIdempotency(_ context.Context, key string) (*wager.Transaction, error) {
	for _, snap := range t.st.wagers {
		if snap.IdempotencyKey != "" && snap.IdempotencyKey == key {
			return wager.Rehydrate(snap)
		}
	}
	return nil, port.ErrNotFound
}

func (t *tx) FindWagerByExternal(_ context.Context, providerID, externalID string) (*wager.Transaction, error) {
	for _, snap := range t.st.wagers {
		if snap.ProviderID == providerID && snap.ExternalTransactionID == externalID && snap.Origin == wager.OriginExternal {
			return wager.Rehydrate(snap)
		}
	}
	return nil, port.ErrNotFound
}

func blocksReversal(s wager.Snapshot) bool {
	if s.Kind != wager.KindRefund && s.Kind != wager.KindRollback {
		return false
	}
	switch s.Status {
	case wager.StatusPending, wager.StatusPendingReference, wager.StatusProcessed:
		return true
	}
	return false
}

func (t *tx) FindBlockingReversal(_ context.Context, providerID, referenceExternalID string, except uuid.UUID) (*wager.Transaction, error) {
	for _, snap := range t.st.wagers {
		if snap.ID != except && snap.ProviderID == providerID && snap.ReferenceExternalID == referenceExternalID && blocksReversal(snap) {
			return wager.Rehydrate(snap)
		}
	}
	return nil, port.ErrNotFound
}

func checkWager(s wager.Snapshot) error {
	if s.Status == wager.StatusProcessed {
		switch s.Kind {
		case wager.KindLoss:
			if !s.Amount.IsZero() {
				return ErrCheck
			}
		case wager.KindBet, wager.KindWin, wager.KindRefund, wager.KindRollback:
			if !s.Amount.IsPositive() {
				return ErrCheck
			}
		}
	}
	terminalResult := s.Status == wager.StatusProcessed || s.Status == wager.StatusRejected
	if terminalResult != (s.ResultBalance != nil) {
		return ErrCheck
	}
	if s.ResultBalance != nil && s.ResultBalance.IsNegative() {
		return ErrCheck
	}
	return nil
}

func (t *tx) uniqueWager(s wager.Snapshot) error {
	for id, other := range t.st.wagers {
		if id == s.ID {
			continue
		}
		if s.IdempotencyKey != "" && other.IdempotencyKey == s.IdempotencyKey {
			return &port.Conflict{Constraint: "ux_wager_idempotency"}
		}
		if s.Origin == wager.OriginExternal && other.Origin == wager.OriginExternal &&
			other.ProviderID == s.ProviderID && other.ExternalTransactionID == s.ExternalTransactionID {
			return &port.Conflict{Constraint: "ux_wager_external"}
		}
		if s.Kind == wager.KindOpening && other.Kind == wager.KindOpening && other.WalletID == s.WalletID {
			return &port.Conflict{Constraint: "ux_wager_opening"}
		}
		if blocksReversal(s) && blocksReversal(other) &&
			other.ProviderID == s.ProviderID && other.ReferenceExternalID == s.ReferenceExternalID {
			return &port.Conflict{Constraint: "ux_wager_one_reversal"}
		}
	}
	return nil
}

func (t *tx) InsertWager(_ context.Context, w *wager.Transaction) error {
	s := w.Snapshot()
	if _, ok := t.st.wagers[s.ID]; ok {
		return &port.Conflict{Constraint: "wager_transactions_pkey"}
	}
	if _, ok := t.st.wallets[s.WalletID]; !ok {
		return ErrCheck
	}
	if err := t.uniqueWager(s); err != nil {
		return err
	}
	if err := checkWager(s); err != nil {
		return err
	}
	t.st.wagers[s.ID] = s
	return nil
}

func (t *tx) UpdateWager(_ context.Context, w *wager.Transaction) error {
	s := w.Snapshot()
	current, ok := t.st.wagers[s.ID]
	if !ok || (current.Status != wager.StatusPending && current.Status != wager.StatusPendingReference) {
		return port.ErrConcurrentUpdate
	}
	if err := t.uniqueWager(s); err != nil {
		return err
	}
	if err := checkWager(s); err != nil {
		return err
	}
	current.Status = s.Status
	current.ReferenceTransactionID = s.ReferenceTransactionID
	current.FailureCode = s.FailureCode
	current.ResultBalance = s.ResultBalance
	current.UpdatedAt = s.UpdatedAt
	t.st.wagers[s.ID] = current
	return nil
}

func (t *tx) InsertLedger(_ context.Context, entry ledger.Entry) error {
	for _, e := range t.st.ledger {
		if e.ID() == entry.ID() {
			return &port.Conflict{Constraint: "wallet_ledger_entries_pkey"}
		}
		if e.WalletID() == entry.WalletID() && e.TransactionID() == entry.TransactionID() {
			return &port.Conflict{Constraint: "ux_ledger_wallet_tx"}
		}
	}
	if _, ok := t.st.wagers[entry.TransactionID()]; !ok {
		return ErrCheck
	}
	t.st.ledger = append(t.st.ledger, entry)
	return nil
}

func (t *tx) entries(walletID uuid.UUID) []ledger.Entry {
	var out []ledger.Entry
	for _, e := range t.st.ledger {
		if e.WalletID() == walletID {
			out = append(out, e)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if !out[i].CreatedAt().Equal(out[j].CreatedAt()) {
			return out[i].CreatedAt().Before(out[j].CreatedAt())
		}
		a, b := out[i].ID(), out[j].ID()
		return bytes.Compare(a[:], b[:]) < 0
	})
	return out
}

func (t *tx) ListLedger(_ context.Context, walletID uuid.UUID, after *port.LedgerCursor, limit int) (port.LedgerPage, error) {
	all := t.entries(walletID)
	var out []ledger.Entry
	for _, e := range all {
		if after != nil {
			id := e.ID()
			if e.CreatedAt().Before(after.CreatedAt) {
				continue
			}
			if e.CreatedAt().Equal(after.CreatedAt) && bytes.Compare(id[:], after.ID[:]) <= 0 {
				continue
			}
		}
		out = append(out, e)
	}
	page := port.LedgerPage{Entries: out}
	if limit > 0 && len(out) > limit {
		page.Entries = out[:limit]
		page.HasMore = true
	}
	return page, nil
}

func (t *tx) ListAllLedger(_ context.Context, walletID uuid.UUID) ([]ledger.Entry, error) {
	return t.entries(walletID), nil
}

func (t *tx) InsertInbox(_ context.Context, row port.Inbox) error {
	key := [2]string{row.Consumer, row.MessageID}
	if _, ok := t.st.inbox[key]; ok {
		return &port.Conflict{Constraint: "inbox_pkey"}
	}
	row.CompletedAt = nil
	t.st.inbox[key] = row
	return nil
}

func (t *tx) FindInbox(_ context.Context, consumer, messageID string) (port.Inbox, error) {
	row, ok := t.st.inbox[[2]string{consumer, messageID}]
	if !ok {
		return port.Inbox{}, port.ErrNotFound
	}
	return row, nil
}

func (t *tx) CompleteInbox(_ context.Context, consumer, messageID string, at time.Time) error {
	key := [2]string{consumer, messageID}
	row, ok := t.st.inbox[key]
	if !ok || row.CompletedAt != nil {
		return port.ErrConcurrentUpdate
	}
	stamp := at
	row.CompletedAt = &stamp
	t.st.inbox[key] = row
	return nil
}

func (t *tx) InsertOutbox(_ context.Context, ev event.Envelope) error {
	for _, row := range t.st.outbox {
		if row.env.EventID() == ev.EventID() {
			return &port.Conflict{Constraint: "outbox_pkey"}
		}
	}
	t.st.outbox = append(t.st.outbox, outboxRow{env: ev, nextAt: ev.OccurredAt()})
	return nil
}

func (t *tx) InsertWait(_ context.Context, row port.Wait) error {
	if _, ok := t.st.waits[row.TransactionID]; ok {
		return &port.Conflict{Constraint: "reference_waits_pkey"}
	}
	if _, ok := t.st.wagers[row.TransactionID]; !ok {
		return ErrCheck
	}
	t.st.waits[row.TransactionID] = waitRow{wait: row}
	return nil
}

func (t *tx) FindWait(_ context.Context, transactionID uuid.UUID) (port.Wait, error) {
	row, ok := t.st.waits[transactionID]
	if !ok {
		return port.Wait{}, port.ErrNotFound
	}
	return row.wait, nil
}

func (t *tx) UpdateWait(_ context.Context, row port.Wait) error {
	if _, ok := t.st.waits[row.TransactionID]; !ok {
		return port.ErrNotFound
	}
	t.st.waits[row.TransactionID] = waitRow{wait: row}
	return nil
}

func (t *tx) DeleteWait(_ context.Context, transactionID uuid.UUID) error {
	delete(t.st.waits, transactionID)
	return nil
}
