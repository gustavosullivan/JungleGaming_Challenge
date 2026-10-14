package wagering

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jungle-gaming/wagering/internal/app/port"
	"github.com/jungle-gaming/wagering/internal/domain/wager"
)

// drainTimeout bounds how long a claimed batch may keep running after
// shutdown starts.
const drainTimeout = 10 * time.Second

func (s *Service) RunReferenceLoop(ctx context.Context) {
	ticker := time.NewTicker(s.poll)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Cancelling a claimed batch would leave its rows locked for the
			// whole lease, so the batch finishes even if shutdown begins.
			work, cancel := context.WithTimeout(context.WithoutCancel(ctx), drainTimeout)
			s.SweepOnce(work)
			cancel()
		}
	}
}

func (s *Service) SweepOnce(ctx context.Context) {
	ids, err := s.db.ClaimWaits(ctx, 20, 30*time.Second)
	if err != nil {
		if ctx.Err() != nil || s.log == nil {
			return
		}
		s.log.Error("claim reference waits", "err", err.Error())
		return
	}
	for _, id := range ids {
		if ctx.Err() != nil {
			return
		}
		if err := s.Resume(ctx, id); err != nil {
			s.metrics.Retry("reference")
			if s.log != nil {
				s.log.Error("resume reference", "transactionId", id.String(), "err", err.Error())
			}
		}
	}
}

func (s *Service) Resume(ctx context.Context, id uuid.UUID) error {
	return s.db.WithinTx(ctx, func(ctx context.Context, tx port.Tx) error {
		peeked, err := tx.FindWagerByID(ctx, id)
		if errors.Is(err, port.ErrNotFound) {
			return tx.DeleteWait(ctx, id)
		}
		if err != nil {
			return err
		}
		if peeked.IsTerminal() {
			return tx.DeleteWait(ctx, id)
		}
		w, err := tx.LockWallet(ctx, peeked.WalletID(), port.LockUpdate)
		if err != nil {
			return err
		}
		op, err := tx.LockWager(ctx, id)
		if err != nil {
			return err
		}
		if op.Status() != wager.StatusPendingReference {
			if op.IsTerminal() {
				return tx.DeleteWait(ctx, id)
			}
			return nil
		}
		_, err = s.apply(ctx, tx, w, op, op.ID().String(), true)
		return err
	})
}
