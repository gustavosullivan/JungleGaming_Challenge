package wagering

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"

	"github.com/jungle-gaming/wagering/internal/app/errs"
	"github.com/jungle-gaming/wagering/internal/app/port"
)

// AcceptDelivery runs the wager use case and the inbox insert in one
// database transaction. The caller deletes the queue message only after
// this returns nil.
func (s *Service) AcceptDelivery(ctx context.Context, messageID string, body []byte, cmd Command) error {
	if messageID == "" {
		return &errs.Permanent{Err: errors.New("missing message id")}
	}
	if err := prepare(&cmd); err != nil {
		return err
	}
	sum := sha256.Sum256(body)
	hash := hex.EncodeToString(sum[:])
	var last error
	for attempt := 0; attempt < 3; attempt++ {
		err := s.acceptOnce(ctx, messageID, hash, cmd)
		if err == nil {
			return nil
		}
		var conflict *port.Conflict
		if errors.As(err, &conflict) {
			switch conflict.Constraint {
			case "inbox_pkey":
				return s.redelivery(ctx, messageID, hash)
			case "ux_wager_idempotency", "ux_wager_external":
				if _, rerr := s.readExisting(ctx, cmd); rerr != nil {
					return rerr
				}
				return s.rememberDelivery(ctx, messageID, hash)
			case "ux_wager_one_reversal":
				last = err
				continue
			}
		}
		if errors.Is(err, port.ErrConcurrentUpdate) {
			s.metrics.Conflict()
			last = err
			continue
		}
		return err
	}
	if last == nil {
		last = &errs.Unavailable{Reason: "could not accept the message"}
	}
	return last
}

func (s *Service) acceptOnce(ctx context.Context, messageID, hash string, cmd Command) error {
	return s.db.WithinTx(ctx, func(ctx context.Context, tx port.Tx) error {
		now := s.clock()
		if err := tx.InsertInbox(ctx, port.Inbox{
			Consumer: consumerName, MessageID: messageID, PayloadHash: hash, ReceivedAt: now,
		}); err != nil {
			return err
		}
		if _, err := s.submitInTx(ctx, tx, cmd); err != nil {
			return err
		}
		return tx.CompleteInbox(ctx, consumerName, messageID, now)
	})
}

func (s *Service) redelivery(ctx context.Context, messageID, hash string) error {
	var row port.Inbox
	err := s.db.WithinTx(ctx, func(ctx context.Context, tx port.Tx) error {
		found, err := tx.FindInbox(ctx, consumerName, messageID)
		if err != nil {
			return err
		}
		row = found
		return nil
	})
	if err != nil {
		return err
	}
	if row.PayloadHash != hash {
		return &errs.Permanent{Err: errors.New("message body changed for the same message id")}
	}
	if row.CompletedAt == nil {
		return &errs.Unavailable{Reason: "previous delivery did not finish"}
	}
	return nil
}

func (s *Service) rememberDelivery(ctx context.Context, messageID, hash string) error {
	now := s.clock()
	err := s.db.WithinTx(ctx, func(ctx context.Context, tx port.Tx) error {
		if err := tx.InsertInbox(ctx, port.Inbox{
			Consumer: consumerName, MessageID: messageID, PayloadHash: hash, ReceivedAt: now,
		}); err != nil {
			return err
		}
		return tx.CompleteInbox(ctx, consumerName, messageID, now)
	})
	var conflict *port.Conflict
	if errors.As(err, &conflict) && conflict.Constraint == "inbox_pkey" {
		return s.redelivery(ctx, messageID, hash)
	}
	return err
}
