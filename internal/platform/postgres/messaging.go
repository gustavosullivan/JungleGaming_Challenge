package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jungle-gaming/wagering/internal/app/port"
	"github.com/jungle-gaming/wagering/internal/domain/event"
)

func (t *pgTx) InsertInbox(ctx context.Context, row port.Inbox) error {
	_, err := t.tx.Exec(ctx, `
		INSERT INTO inbox (consumer_name, message_id, payload_hash, received_at)
		VALUES ($1, $2, $3, $4)`,
		row.Consumer, row.MessageID, row.PayloadHash, row.ReceivedAt)
	return mapWrite(err)
}

func (t *pgTx) FindInbox(ctx context.Context, consumer, messageID string) (port.Inbox, error) {
	var row port.Inbox
	var completed *time.Time
	err := t.tx.QueryRow(ctx, `
		SELECT consumer_name, message_id, payload_hash, received_at, completed_at
		FROM inbox WHERE consumer_name = $1 AND message_id = $2`, consumer, messageID).
		Scan(&row.Consumer, &row.MessageID, &row.PayloadHash, &row.ReceivedAt, &completed)
	if errors.Is(err, pgx.ErrNoRows) {
		return port.Inbox{}, port.ErrNotFound
	}
	if err != nil {
		return port.Inbox{}, err
	}
	row.PayloadHash = trimHash(row.PayloadHash)
	row.CompletedAt = completed
	return row, nil
}

func (t *pgTx) CompleteInbox(ctx context.Context, consumer, messageID string, at time.Time) error {
	tag, err := t.tx.Exec(ctx, `
		UPDATE inbox SET completed_at = $3
		WHERE consumer_name = $1 AND message_id = $2 AND completed_at IS NULL`,
		consumer, messageID, at)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return port.ErrConcurrentUpdate
	}
	return nil
}

func (t *pgTx) InsertOutbox(ctx context.Context, ev event.Envelope) error {
	_, err := t.tx.Exec(ctx, `
		INSERT INTO outbox (event_id, aggregate_id, event_type, payload, occurred_at, attempts, next_attempt_at)
		VALUES ($1, $2, $3, $4::jsonb, $5, 0, $5)`,
		uuidArg(ev.EventID()), ev.AggregateID(), ev.EventType(), string(ev.Payload()), ev.OccurredAt())
	return err
}

func (t *pgTx) InsertWait(ctx context.Context, row port.Wait) error {
	_, err := t.tx.Exec(ctx, `
		INSERT INTO reference_waits (transaction_id, attempts, next_attempt_at, deadline_at)
		VALUES ($1, $2, $3, $4)`,
		uuidArg(row.TransactionID), row.Attempts, row.NextAttemptAt, row.DeadlineAt)
	return mapWrite(err)
}

func (t *pgTx) FindWait(ctx context.Context, transactionID uuid.UUID) (port.Wait, error) {
	var row port.Wait
	var id pgtype.UUID
	err := t.tx.QueryRow(ctx, `
		SELECT transaction_id, attempts, next_attempt_at, deadline_at
		FROM reference_waits WHERE transaction_id = $1`, uuidArg(transactionID)).
		Scan(&id, &row.Attempts, &row.NextAttemptAt, &row.DeadlineAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return port.Wait{}, port.ErrNotFound
	}
	if err != nil {
		return port.Wait{}, err
	}
	row.TransactionID = takeUUID(id)
	return row, nil
}

func (t *pgTx) UpdateWait(ctx context.Context, row port.Wait) error {
	tag, err := t.tx.Exec(ctx, `
		UPDATE reference_waits
		SET attempts = $2, next_attempt_at = $3, deadline_at = $4, locked_until = NULL
		WHERE transaction_id = $1`,
		uuidArg(row.TransactionID), row.Attempts, row.NextAttemptAt, row.DeadlineAt)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return port.ErrNotFound
	}
	return nil
}

func (t *pgTx) DeleteWait(ctx context.Context, transactionID uuid.UUID) error {
	_, err := t.tx.Exec(ctx, `DELETE FROM reference_waits WHERE transaction_id = $1`, uuidArg(transactionID))
	return err
}

func (s *Store) FindInbox(ctx context.Context, consumer, messageID string) (port.Inbox, error) {
	var row port.Inbox
	var completed *time.Time
	err := s.pool.QueryRow(ctx, `
		SELECT consumer_name, message_id, payload_hash, received_at, completed_at
		FROM inbox WHERE consumer_name = $1 AND message_id = $2`, consumer, messageID).
		Scan(&row.Consumer, &row.MessageID, &row.PayloadHash, &row.ReceivedAt, &completed)
	if errors.Is(err, pgx.ErrNoRows) {
		return port.Inbox{}, port.ErrNotFound
	}
	if err != nil {
		return port.Inbox{}, err
	}
	row.PayloadHash = trimHash(row.PayloadHash)
	row.CompletedAt = completed
	return row, nil
}

func (s *Store) MarkInboxCompleted(ctx context.Context, consumer, messageID, hash string, at time.Time) error {
	return s.WithinTx(ctx, func(ctx context.Context, tx port.Tx) error {
		existing, err := tx.FindInbox(ctx, consumer, messageID)
		if errors.Is(err, port.ErrNotFound) {
			if err := tx.InsertInbox(ctx, port.Inbox{
				Consumer: consumer, MessageID: messageID, PayloadHash: hash, ReceivedAt: at,
			}); err != nil {
				return err
			}
			return tx.CompleteInbox(ctx, consumer, messageID, at)
		}
		if err != nil {
			return err
		}
		if existing.CompletedAt != nil {
			return nil
		}
		return tx.CompleteInbox(ctx, consumer, messageID, at)
	})
}

func (s *Store) ClaimWaits(ctx context.Context, limit int, lease time.Duration) ([]uuid.UUID, error) {
	rows, err := s.pool.Query(ctx, `
		WITH picked AS (
			SELECT transaction_id
			FROM reference_waits
			WHERE next_attempt_at <= now()
			  AND (locked_until IS NULL OR locked_until < now())
			ORDER BY next_attempt_at
			FOR UPDATE SKIP LOCKED
			LIMIT $1
		)
		UPDATE reference_waits w
		SET locked_until = now() + make_interval(secs => $2::int)
		FROM picked
		WHERE w.transaction_id = picked.transaction_id
		RETURNING w.transaction_id`, limit, int(lease.Seconds()))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []uuid.UUID
	for rows.Next() {
		var id pgtype.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, takeUUID(id))
	}
	return ids, rows.Err()
}

func (s *Store) ClaimOutbox(ctx context.Context, worker string, limit int, lease time.Duration) ([]port.OutboxMessage, error) {
	rows, err := s.pool.Query(ctx, `
		WITH picked AS (
			SELECT event_id
			FROM outbox
			WHERE published_at IS NULL
			  AND next_attempt_at <= now()
			  AND (locked_until IS NULL OR locked_until < now())
			ORDER BY next_attempt_at
			FOR UPDATE SKIP LOCKED
			LIMIT $3
		)
		UPDATE outbox o
		SET locked_by = $1,
			locked_until = now() + make_interval(secs => $2::int),
			attempts = o.attempts + 1
		FROM picked
		WHERE o.event_id = picked.event_id
		RETURNING o.event_id, o.aggregate_id, o.event_type, o.payload, o.attempts`,
		worker, int(lease.Seconds()), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []port.OutboxMessage
	for rows.Next() {
		var (
			msg port.OutboxMessage
			id  pgtype.UUID
		)
		if err := rows.Scan(&id, &msg.AggregateID, &msg.EventType, &msg.Payload, &msg.Attempts); err != nil {
			return nil, err
		}
		msg.EventID = takeUUID(id)
		out = append(out, msg)
	}
	return out, rows.Err()
}

func (s *Store) MarkOutboxPublished(ctx context.Context, eventID uuid.UUID, at time.Time) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE outbox
		SET published_at = $2, locked_by = NULL, locked_until = NULL
		WHERE event_id = $1 AND published_at IS NULL`, uuidArg(eventID), at)
	return err
}

func (s *Store) RescheduleOutbox(ctx context.Context, eventID uuid.UUID, next time.Time) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE outbox
		SET next_attempt_at = $2, locked_by = NULL, locked_until = NULL
		WHERE event_id = $1 AND published_at IS NULL`, uuidArg(eventID), next)
	return err
}

func (s *Store) OutboxLag(ctx context.Context) (time.Duration, error) {
	var seconds float64
	err := s.pool.QueryRow(ctx, `
		SELECT COALESCE(EXTRACT(EPOCH FROM (now() - min(occurred_at))), 0)
		FROM outbox
		WHERE published_at IS NULL`).Scan(&seconds)
	if err != nil {
		return 0, err
	}
	return time.Duration(seconds * float64(time.Second)), nil
}

func trimHash(s string) string {
	return stringsTrim(s)
}

func stringsTrim(s string) string {
	for len(s) > 0 && (s[len(s)-1] == ' ' || s[0] == ' ') {
		if s[len(s)-1] == ' ' {
			s = s[:len(s)-1]
			continue
		}
		s = s[1:]
	}
	return s
}
