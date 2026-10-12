package wagering

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jungle-gaming/wagering/internal/app/port"
	"github.com/jungle-gaming/wagering/internal/domain/event"
	"github.com/jungle-gaming/wagering/internal/domain/ledger"
	"github.com/jungle-gaming/wagering/internal/domain/money"
	"github.com/jungle-gaming/wagering/internal/domain/wager"
	"github.com/jungle-gaming/wagering/internal/domain/wallet"
)

func (s *Service) apply(ctx context.Context, tx port.Tx, w *wallet.Wallet, op *wager.Transaction, correlation string, stored bool) (Result, error) {
	if w.PlayerID() != op.PlayerID() {
		return s.finishReject(ctx, tx, w, op, wager.CodePlayerWalletMismatch, correlation, stored)
	}
	if w.Currency() != op.Amount().Currency() {
		return s.finishReject(ctx, tx, w, op, wager.CodeCurrencyMismatch, correlation, stored)
	}
	if op.Kind() == wager.KindLoss {
		if !op.Amount().IsZero() {
			return s.finishReject(ctx, tx, w, op, wager.CodeInvalidAmount, correlation, stored)
		}
		return s.finishLoss(ctx, tx, w, op, correlation, stored)
	}
	if !op.Amount().IsPositive() {
		return s.finishReject(ctx, tx, w, op, wager.CodeInvalidAmount, correlation, stored)
	}

	var ref *wager.Transaction
	if op.ReferenceExternalID() != "" {
		found, code, wait, err := s.resolveReference(ctx, tx, op)
		if err != nil {
			return Result{}, err
		}
		if wait {
			return s.keepWaiting(ctx, tx, w, op, correlation, stored)
		}
		if code != "" {
			return s.finishReject(ctx, tx, w, op, code, correlation, stored)
		}
		ref = found
	}

	direction, reversal, err := movement(op, ref)
	if err != nil {
		return s.finishReject(ctx, tx, w, op, wager.CodeInvalidReferenceKind, correlation, stored)
	}
	return s.finishMovement(ctx, tx, w, op, ref, direction, reversal, correlation, stored)
}

func (s *Service) resolveReference(ctx context.Context, tx port.Tx, op *wager.Transaction) (*wager.Transaction, string, bool, error) {
	found, err := tx.FindWagerByExternal(ctx, op.ProviderID(), op.ReferenceExternalID())
	if errors.Is(err, port.ErrNotFound) {
		return nil, "", true, nil
	}
	if err != nil {
		return nil, "", false, err
	}
	switch found.Status() {
	case wager.StatusPending, wager.StatusPendingReference:
		return found, "", true, nil
	case wager.StatusRejected, wager.StatusFailed:
		return found, wager.CodeReferenceNotSuccessful, false, nil
	case wager.StatusProcessed:
		if code := matchReference(op, found); code != "" {
			return found, code, false, nil
		}
		if op.Kind() == wager.KindRefund || op.Kind() == wager.KindRollback {
			blocker, err := tx.FindBlockingReversal(ctx, op.ProviderID(), op.ReferenceExternalID(), op.ID())
			if err != nil && !errors.Is(err, port.ErrNotFound) {
				return nil, "", false, err
			}
			if blocker != nil {
				if blocker.Status() == wager.StatusProcessed {
					return found, wager.CodeAlreadyReversed, false, nil
				}
				return found, wager.CodeReversalInProgress, false, nil
			}
		}
		return found, "", false, nil
	default:
		return found, wager.CodeReferenceMismatch, false, nil
	}
}

func matchReference(op, ref *wager.Transaction) string {
	if ref.PlayerID() != op.PlayerID() || ref.WalletID() != op.WalletID() || ref.RoundID() != op.RoundID() || ref.Amount().Currency() != op.Amount().Currency() {
		return wager.CodeReferenceMismatch
	}
	switch op.Kind() {
	case wager.KindWin:
		if ref.Kind() != wager.KindBet {
			return wager.CodeInvalidReferenceKind
		}
	case wager.KindRefund:
		if ref.Kind() != wager.KindBet {
			return wager.CodeInvalidReferenceKind
		}
		if !ref.Amount().Equal(op.Amount()) {
			return wager.CodeReferenceAmountMismatch
		}
	case wager.KindRollback:
		switch ref.Kind() {
		case wager.KindBet, wager.KindWin, wager.KindRefund:
		default:
			return wager.CodeInvalidReferenceKind
		}
		if !ref.Amount().Equal(op.Amount()) {
			return wager.CodeReferenceAmountMismatch
		}
	default:
		return wager.CodeReferenceMismatch
	}
	return ""
}

func movement(op, ref *wager.Transaction) (ledger.Direction, bool, error) {
	switch op.Kind() {
	case wager.KindBet:
		return ledger.Debit, false, nil
	case wager.KindWin:
		return ledger.Credit, false, nil
	case wager.KindRefund:
		return ledger.Credit, true, nil
	case wager.KindRollback:
		if ref == nil {
			return "", false, errors.New("rollback without reference")
		}
		switch ref.Kind() {
		case wager.KindBet:
			return ledger.Credit, true, nil
		case wager.KindWin, wager.KindRefund:
			return ledger.Debit, true, nil
		default:
			return "", false, errors.New("rollback target")
		}
	default:
		return "", false, errors.New("no movement")
	}
}

func (s *Service) finishMovement(ctx context.Context, tx port.Tx, w *wallet.Wallet, op *wager.Transaction, ref *wager.Transaction, direction ledger.Direction, reversal bool, correlation string, stored bool) (Result, error) {
	now := s.clock()
	previous := w.Version()
	var before = w.Balance()
	var err error
	if direction == ledger.Debit {
		before, err = w.Debit(op.Amount(), now)
	} else {
		before, err = w.Credit(op.Amount(), now)
	}
	if errors.Is(err, wallet.ErrInsufficientBalance) {
		code := wager.CodeInsufficientBalance
		if reversal {
			code = wager.CodeReversalInsufficientBalance
		}
		return s.finishReject(ctx, tx, w, op, code, correlation, stored)
	}
	if err != nil {
		return Result{}, err
	}
	if err := tx.UpdateWallet(ctx, w, previous); err != nil {
		return Result{}, err
	}
	var refID *uuid.UUID
	if ref != nil {
		id := ref.ID()
		refID = &id
	}
	if err := op.MarkProcessed(w.Balance(), refID, now); err != nil {
		return Result{}, err
	}
	if err := s.persist(ctx, tx, op, stored); err != nil {
		return Result{}, err
	}
	entryID, err := uuid.NewV7()
	if err != nil {
		return Result{}, err
	}
	entry, err := ledger.New(entryID, w.ID(), op.ID(), direction, op.Amount(), before, now)
	if err != nil {
		return Result{}, err
	}
	if err := tx.InsertLedger(ctx, entry); err != nil {
		return Result{}, err
	}
	if err := s.emitProcessed(ctx, tx, op, correlation, now); err != nil {
		return Result{}, err
	}
	if err := s.emitBalance(ctx, tx, op, direction, before, w, correlation, now); err != nil {
		return Result{}, err
	}
	if stored {
		if err := tx.DeleteWait(ctx, op.ID()); err != nil {
			return Result{}, err
		}
	}
	return toResult(op, false), nil
}

func (s *Service) finishLoss(ctx context.Context, tx port.Tx, w *wallet.Wallet, op *wager.Transaction, correlation string, stored bool) (Result, error) {
	now := s.clock()
	if err := op.MarkProcessed(w.Balance(), nil, now); err != nil {
		return Result{}, err
	}
	if err := s.persist(ctx, tx, op, stored); err != nil {
		return Result{}, err
	}
	if err := s.emitProcessed(ctx, tx, op, correlation, now); err != nil {
		return Result{}, err
	}
	if stored {
		if err := tx.DeleteWait(ctx, op.ID()); err != nil {
			return Result{}, err
		}
	}
	return toResult(op, false), nil
}

func (s *Service) finishReject(ctx context.Context, tx port.Tx, w *wallet.Wallet, op *wager.Transaction, code, correlation string, stored bool) (Result, error) {
	now := s.clock()
	if err := op.MarkRejected(code, w.Balance(), now); err != nil {
		return Result{}, err
	}
	if err := s.persist(ctx, tx, op, stored); err != nil {
		return Result{}, err
	}
	if err := s.emit(ctx, tx, func(id uuid.UUID) (event.Envelope, error) {
		return event.NewRejected(id, correlation, op.ID().String(), now, event.Rejected{
			TransactionID: op.ID(), WalletID: op.WalletID(), PlayerID: op.PlayerID(),
			ProviderID: op.ProviderID(), ExternalTransactionID: op.ExternalTransactionID(),
			Kind: string(op.Kind()), FailureCode: code, Amount: op.Amount(),
		})
	}); err != nil {
		return Result{}, err
	}
	if stored {
		if err := tx.DeleteWait(ctx, op.ID()); err != nil {
			return Result{}, err
		}
	}
	return toResult(op, false), nil
}

func (s *Service) keepWaiting(ctx context.Context, tx port.Tx, w *wallet.Wallet, op *wager.Transaction, correlation string, stored bool) (Result, error) {
	now := s.clock()
	if !stored {
		if err := op.MarkPendingReference(now); err != nil {
			return Result{}, err
		}
		if err := s.persist(ctx, tx, op, false); err != nil {
			return Result{}, err
		}
		deadline := now.Add(s.ttl)
		next := now.Add(s.backoff)
		if !next.Before(deadline) {
			next = deadline
		}
		if err := tx.InsertWait(ctx, port.Wait{
			TransactionID: op.ID(), Attempts: 0, NextAttemptAt: next, DeadlineAt: deadline,
		}); err != nil {
			return Result{}, err
		}
		if err := s.emit(ctx, tx, func(id uuid.UUID) (event.Envelope, error) {
			return event.NewPendingReference(id, correlation, op.ID().String(), now, event.PendingReference{
				TransactionID: op.ID(), WalletID: op.WalletID(), ProviderID: op.ProviderID(),
				ExternalTransactionID: op.ExternalTransactionID(), ReferenceExternalID: op.ReferenceExternalID(),
				Kind: string(op.Kind()),
			})
		}); err != nil {
			return Result{}, err
		}
		return toResult(op, false), nil
	}

	row, err := tx.FindWait(ctx, op.ID())
	if err != nil {
		return Result{}, err
	}
	if !now.Before(row.DeadlineAt) || row.Attempts+1 > s.maxAttempts {
		return s.finishReject(ctx, tx, w, op, wager.CodeReferenceNotFound, correlation, true)
	}
	row.Attempts++
	next := now.Add(backoff(row.Attempts, s.backoff, 30*time.Second))
	if !next.Before(row.DeadlineAt) {
		next = row.DeadlineAt
	}
	row.NextAttemptAt = next
	if err := tx.UpdateWait(ctx, row); err != nil {
		return Result{}, err
	}
	return toResult(op, false), nil
}

func (s *Service) persist(ctx context.Context, tx port.Tx, op *wager.Transaction, stored bool) error {
	if stored {
		return tx.UpdateWager(ctx, op)
	}
	return tx.InsertWager(ctx, op)
}

func (s *Service) emitProcessed(ctx context.Context, tx port.Tx, op *wager.Transaction, correlation string, now time.Time) error {
	return s.emit(ctx, tx, func(id uuid.UUID) (event.Envelope, error) {
		return event.NewProcessed(id, correlation, op.ID().String(), now, event.Processed{
			TransactionID: op.ID(), WalletID: op.WalletID(), PlayerID: op.PlayerID(),
			ProviderID: op.ProviderID(), ExternalTransactionID: op.ExternalTransactionID(),
			RoundID: op.RoundID(), GameID: op.GameID(), Kind: string(op.Kind()), Amount: op.Amount(),
		})
	})
}

func (s *Service) emitBalance(ctx context.Context, tx port.Tx, op *wager.Transaction, direction ledger.Direction, before money.Money, w *wallet.Wallet, correlation string, now time.Time) error {
	return s.emit(ctx, tx, func(id uuid.UUID) (event.Envelope, error) {
		return event.NewBalanceChanged(id, correlation, op.ID().String(), now, event.BalanceChanged{
			WalletID: w.ID(), TransactionID: op.ID(), Direction: string(direction),
			Amount: op.Amount(), BalanceBefore: before, BalanceAfter: w.Balance(), WalletVersion: w.Version(),
		})
	})
}

func (s *Service) emit(ctx context.Context, tx port.Tx, build func(uuid.UUID) (event.Envelope, error)) error {
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
