package wallet

import (
	"context"
	"encoding/base64"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jungle-gaming/wagering/internal/app/errs"
	"github.com/jungle-gaming/wagering/internal/app/observe"
	"github.com/jungle-gaming/wagering/internal/app/port"
	"github.com/jungle-gaming/wagering/internal/domain/event"
	"github.com/jungle-gaming/wagering/internal/domain/ledger"
	"github.com/jungle-gaming/wagering/internal/domain/money"
	"github.com/jungle-gaming/wagering/internal/domain/wager"
	domainwallet "github.com/jungle-gaming/wagering/internal/domain/wallet"
)

type Service struct {
	db      port.Store
	log     *slog.Logger
	metrics observe.Recorder
	now     func() time.Time
}

func New(db port.Store, log *slog.Logger, metrics observe.Recorder) *Service {
	if metrics == nil {
		metrics = observe.Nop{}
	}
	return &Service{db: db, log: log, metrics: metrics, now: time.Now}
}

type View struct {
	ID        uuid.UUID
	PlayerID  uuid.UUID
	Balance   money.Money
	Version   int64
	CreatedAt time.Time
	UpdatedAt time.Time
}

type Reconciliation struct {
	WalletID          uuid.UUID
	StoredBalance     money.Money
	CalculatedBalance money.Money
	Difference        money.Money
	Consistent        bool
	CheckedEntries    int
}

func (s *Service) Open(ctx context.Context, playerID uuid.UUID, amount, currency, correlationID string) (View, error) {
	parsed, err := money.Parse(amount, currency)
	if err != nil {
		return View{}, &errs.Invalid{Reason: "invalid money"}
	}
	if playerID == uuid.Nil {
		return View{}, &errs.Invalid{Reason: "playerId is required"}
	}
	if correlationID == "" {
		id, err := uuid.NewV7()
		if err != nil {
			return View{}, err
		}
		correlationID = id.String()
	}
	id, err := uuid.NewV7()
	if err != nil {
		return View{}, err
	}
	now := s.clock()
	w, err := domainwallet.Open(id, playerID, parsed, now)
	if err != nil {
		return View{}, &errs.Invalid{Reason: err.Error()}
	}
	err = s.db.WithinTx(ctx, func(ctx context.Context, tx port.Tx) error {
		if err := tx.InsertWallet(ctx, w); err != nil {
			var conflict *port.Conflict
			if errors.As(err, &conflict) && conflict.Constraint == "wallets_player_currency" {
				return &errs.Conflict{Reason: "wallet already exists for this player and currency"}
			}
			return err
		}
		if parsed.IsZero() {
			return nil
		}
		return s.insertOpening(ctx, tx, w, parsed, correlationID, now)
	})
	if err != nil {
		return View{}, err
	}
	return viewOf(w), nil
}

func (s *Service) insertOpening(ctx context.Context, tx port.Tx, w *domainwallet.Wallet, initial money.Money, correlationID string, now time.Time) error {
	txID, err := uuid.NewV7()
	if err != nil {
		return err
	}
	op, err := wager.NewOpening(wager.OpeningInput{
		ID: txID, WalletID: w.ID(), PlayerID: w.PlayerID(), Amount: initial, Now: now,
	})
	if err != nil {
		return err
	}
	if err := op.MarkProcessed(initial, nil, now); err != nil {
		return err
	}
	if err := tx.InsertWager(ctx, op); err != nil {
		return err
	}
	zero, err := money.Zero(initial.Currency())
	if err != nil {
		return err
	}
	entryID, err := uuid.NewV7()
	if err != nil {
		return err
	}
	entry, err := ledger.New(entryID, w.ID(), op.ID(), ledger.Credit, initial, zero, now)
	if err != nil {
		return err
	}
	if err := tx.InsertLedger(ctx, entry); err != nil {
		return err
	}
	if err := emit(ctx, tx, func(id uuid.UUID) (event.Envelope, error) {
		return event.NewProcessed(id, correlationID, op.ID().String(), now, event.Processed{
			TransactionID: op.ID(), WalletID: w.ID(), PlayerID: w.PlayerID(),
			Kind: string(wager.KindOpening), Amount: initial,
		})
	}); err != nil {
		return err
	}
	return emit(ctx, tx, func(id uuid.UUID) (event.Envelope, error) {
		return event.NewBalanceChanged(id, correlationID, op.ID().String(), now, event.BalanceChanged{
			WalletID: w.ID(), TransactionID: op.ID(), Direction: string(ledger.Credit),
			Amount: initial, BalanceBefore: zero, BalanceAfter: initial, WalletVersion: w.Version(),
		})
	})
}

func (s *Service) Get(ctx context.Context, id uuid.UUID) (View, error) {
	w, err := s.db.GetWallet(ctx, id)
	if errors.Is(err, port.ErrNotFound) {
		return View{}, &errs.NotFound{Reason: "wallet not found"}
	}
	if err != nil {
		return View{}, err
	}
	return viewOf(w), nil
}

type LedgerPage struct {
	Entries    []ledger.Entry
	NextCursor string
}

func (s *Service) Ledger(ctx context.Context, walletID uuid.UUID, cursor string, limit int) (LedgerPage, error) {
	if _, err := s.db.GetWallet(ctx, walletID); err != nil {
		if errors.Is(err, port.ErrNotFound) {
			return LedgerPage{}, &errs.NotFound{Reason: "wallet not found"}
		}
		return LedgerPage{}, err
	}
	if limit <= 0 {
		limit = 50
	}
	if limit > 100 {
		limit = 100
	}
	var after *port.LedgerCursor
	if cursor != "" {
		decoded, err := DecodeCursor(cursor)
		if err != nil {
			return LedgerPage{}, &errs.Invalid{Reason: "invalid cursor"}
		}
		after = &decoded
	}
	page, err := s.db.ListLedger(ctx, walletID, after, limit)
	if err != nil {
		return LedgerPage{}, err
	}
	out := LedgerPage{Entries: page.Entries}
	if page.HasMore && len(page.Entries) > 0 {
		last := page.Entries[len(page.Entries)-1]
		out.NextCursor = EncodeCursor(last.CreatedAt(), last.ID())
	}
	return out, nil
}

func (s *Service) Reconcile(ctx context.Context, walletID uuid.UUID) (Reconciliation, error) {
	var out Reconciliation
	err := s.db.WithinTx(ctx, func(ctx context.Context, tx port.Tx) error {
		w, err := tx.LockWallet(ctx, walletID, port.LockShare)
		if errors.Is(err, port.ErrNotFound) {
			return &errs.NotFound{Reason: "wallet not found"}
		}
		if err != nil {
			return err
		}
		entries, err := tx.ListAllLedger(ctx, walletID)
		if err != nil {
			return err
		}
		calculated, err := money.Zero(w.Currency())
		if err != nil {
			return err
		}
		chain := calculated
		chainOK := true
		for _, entry := range entries {
			if entry.Amount().Currency() != w.Currency() || !entry.BalanceBefore().Equal(chain) {
				chainOK = false
			}
			if entry.Direction() == ledger.Credit {
				calculated, err = calculated.Add(entry.Amount())
			} else {
				calculated, err = calculated.Sub(entry.Amount())
			}
			if err != nil {
				return err
			}
			if !entry.BalanceAfter().Equal(calculated) {
				chainOK = false
			}
			chain = entry.BalanceAfter()
		}
		difference, err := w.Balance().Sub(calculated)
		if err != nil {
			return err
		}
		out = Reconciliation{
			WalletID:          w.ID(),
			StoredBalance:     w.Balance(),
			CalculatedBalance: calculated,
			Difference:        difference,
			Consistent:        chainOK && difference.IsZero(),
			CheckedEntries:    len(entries),
		}
		return nil
	})
	if err != nil {
		return Reconciliation{}, err
	}
	if !out.Consistent {
		s.metrics.Divergence()
		if s.log != nil {
			s.log.Warn("wallet reconciliation diverged",
				"walletId", out.WalletID.String(),
				"difference", out.Difference.String(),
				"checkedEntries", out.CheckedEntries,
			)
		}
	}
	return out, nil
}

func (s *Service) GetTransaction(ctx context.Context, id uuid.UUID) (*wager.Transaction, error) {
	tx, err := s.db.GetWager(ctx, id)
	if errors.Is(err, port.ErrNotFound) {
		return nil, &errs.NotFound{Reason: "transaction not found"}
	}
	return tx, err
}

func (s *Service) GetByExternal(ctx context.Context, providerID, externalID string) (*wager.Transaction, error) {
	tx, err := s.db.GetWagerByExternal(ctx, providerID, externalID)
	if errors.Is(err, port.ErrNotFound) {
		return nil, &errs.NotFound{Reason: "transaction not found"}
	}
	return tx, err
}

func viewOf(w *domainwallet.Wallet) View {
	return View{
		ID: w.ID(), PlayerID: w.PlayerID(), Balance: w.Balance(), Version: w.Version(),
		CreatedAt: w.CreatedAt(), UpdatedAt: w.UpdatedAt(),
	}
}

func (s *Service) clock() time.Time {
	now := time.Now()
	if s.now != nil {
		now = s.now()
	}
	return now.UTC().Truncate(time.Millisecond)
}

func emit(ctx context.Context, tx port.Tx, build func(uuid.UUID) (event.Envelope, error)) error {
	id, err := uuid.NewV7()
	if err != nil {
		return err
	}
	ev, err := build(id)
	if err != nil {
		return err
	}
	return tx.InsertOutbox(ctx, ev)
}

func EncodeCursor(at time.Time, id uuid.UUID) string {
	raw := at.UTC().Format(time.RFC3339Nano) + "|" + id.String()
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func DecodeCursor(s string) (port.LedgerCursor, error) {
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return port.LedgerCursor{}, err
	}
	stamp, idRaw, ok := strings.Cut(string(raw), "|")
	if !ok {
		return port.LedgerCursor{}, errors.New("cursor")
	}
	at, err := time.Parse(time.RFC3339Nano, stamp)
	if err != nil {
		return port.LedgerCursor{}, err
	}
	id, err := uuid.Parse(idRaw)
	if err != nil {
		return port.LedgerCursor{}, err
	}
	return port.LedgerCursor{CreatedAt: at, ID: id}, nil
}
