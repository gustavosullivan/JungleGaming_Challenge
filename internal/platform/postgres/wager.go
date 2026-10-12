package postgres

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jungle-gaming/wagering/internal/app/port"
	"github.com/jungle-gaming/wagering/internal/domain/money"
	"github.com/jungle-gaming/wagering/internal/domain/wager"
)

const wagerSelect = `
	SELECT id, origin, kind, status, wallet_id, player_id, currency, amount_minor,
		provider_id, external_transaction_id, idempotency_key, payload_hash,
		round_id, game_id, reference_external_transaction_id, reference_transaction_id,
		failure_code, result_balance_minor, created_at, updated_at
	FROM wager_transactions`

func (t *pgTx) FindWagerByID(ctx context.Context, id uuid.UUID) (*wager.Transaction, error) {
	return scanWager(t.tx.QueryRow(ctx, wagerSelect+` WHERE id = $1`, uuidArg(id)))
}

func (t *pgTx) LockWager(ctx context.Context, id uuid.UUID) (*wager.Transaction, error) {
	return scanWager(t.tx.QueryRow(ctx, wagerSelect+` WHERE id = $1 FOR UPDATE`, uuidArg(id)))
}

func (t *pgTx) FindWagerByIdempotency(ctx context.Context, key string) (*wager.Transaction, error) {
	return scanWager(t.tx.QueryRow(ctx, wagerSelect+` WHERE idempotency_key = $1`, key))
}

func (t *pgTx) FindWagerByExternal(ctx context.Context, providerID, externalID string) (*wager.Transaction, error) {
	return scanWager(t.tx.QueryRow(ctx, wagerSelect+` WHERE provider_id = $1 AND external_transaction_id = $2`, providerID, externalID))
}

func (t *pgTx) FindBlockingReversal(ctx context.Context, providerID, referenceExternalID string, except uuid.UUID) (*wager.Transaction, error) {
	return scanWager(t.tx.QueryRow(ctx, wagerSelect+`
		WHERE provider_id = $1
		  AND reference_external_transaction_id = $2
		  AND kind IN ('REFUND', 'ROLLBACK')
		  AND status IN ('PENDING', 'PENDING_REFERENCE', 'PROCESSED')
		  AND id <> $3
		LIMIT 1`, providerID, referenceExternalID, uuidArg(except)))
}

func (t *pgTx) InsertWager(ctx context.Context, tx *wager.Transaction) error {
	s := tx.Snapshot()
	var result any
	if s.ResultBalance != nil {
		result = s.ResultBalance.Minor()
	}
	_, err := t.tx.Exec(ctx, `
		INSERT INTO wager_transactions (
			id, origin, kind, status, wallet_id, player_id, currency, amount_minor,
			provider_id, external_transaction_id, idempotency_key, payload_hash,
			round_id, game_id, reference_external_transaction_id, reference_transaction_id,
			failure_code, result_balance_minor, created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8,
			$9, $10, $11, $12,
			$13, $14, $15, $16,
			$17, $18, $19, $20
		)`,
		uuidArg(s.ID), string(s.Origin), string(s.Kind), string(s.Status), uuidArg(s.WalletID), uuidArg(s.PlayerID), s.Amount.Currency(), s.Amount.Minor(),
		nullString(s.ProviderID), nullString(s.ExternalTransactionID), nullString(s.IdempotencyKey), nullString(s.PayloadHash),
		nullString(s.RoundID), nullString(s.GameID), nullString(s.ReferenceExternalID), uuidPtrArg(s.ReferenceTransactionID),
		nullString(s.FailureCode), result, s.CreatedAt, s.UpdatedAt,
	)
	return mapWrite(err)
}

func (t *pgTx) UpdateWager(ctx context.Context, tx *wager.Transaction) error {
	s := tx.Snapshot()
	var result any
	if s.ResultBalance != nil {
		result = s.ResultBalance.Minor()
	}
	tag, err := t.tx.Exec(ctx, `
		UPDATE wager_transactions
		SET status = $2,
			reference_transaction_id = $3,
			failure_code = $4,
			result_balance_minor = $5,
			updated_at = $6
		WHERE id = $1
		  AND status IN ('PENDING', 'PENDING_REFERENCE')`,
		uuidArg(s.ID), string(s.Status), uuidPtrArg(s.ReferenceTransactionID), nullString(s.FailureCode), result, s.UpdatedAt)
	if err != nil {
		return mapWrite(err)
	}
	if tag.RowsAffected() == 0 {
		return port.ErrConcurrentUpdate
	}
	return nil
}

func (s *Store) GetWager(ctx context.Context, id uuid.UUID) (*wager.Transaction, error) {
	return scanWager(s.pool.QueryRow(ctx, wagerSelect+` WHERE id = $1`, uuidArg(id)))
}

func (s *Store) GetWagerByExternal(ctx context.Context, providerID, externalID string) (*wager.Transaction, error) {
	return scanWager(s.pool.QueryRow(ctx, wagerSelect+` WHERE provider_id = $1 AND external_transaction_id = $2`, providerID, externalID))
}

func (s *Store) GetWagerByIdempotency(ctx context.Context, key string) (*wager.Transaction, error) {
	return scanWager(s.pool.QueryRow(ctx, wagerSelect+` WHERE idempotency_key = $1`, key))
}

func scanWager(row pgx.Row) (*wager.Transaction, error) {
	var (
		id, walletID, playerID pgtype.UUID
		origin, kind, status   string
		currency               string
		amountMinor            int64
		provider, external     *string
		idem, hash             *string
		roundID, gameID        *string
		refExt, failure        *string
		refID                  pgtype.UUID
		resultMinor            *int64
		created, updated       time.Time
	)
	err := row.Scan(
		&id, &origin, &kind, &status, &walletID, &playerID, &currency, &amountMinor,
		&provider, &external, &idem, &hash,
		&roundID, &gameID, &refExt, &refID,
		&failure, &resultMinor, &created, &updated,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, port.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	cur := strings.TrimSpace(currency)
	amt, err := money.FromMinor(amountMinor, cur)
	if err != nil {
		return nil, err
	}
	snap := wager.Snapshot{
		ID:                    takeUUID(id),
		Origin:                wager.Origin(origin),
		Kind:                  wager.Kind(kind),
		Status:                wager.Status(status),
		WalletID:              takeUUID(walletID),
		PlayerID:              takeUUID(playerID),
		Amount:                amt,
		ProviderID:            deref(provider),
		ExternalTransactionID: deref(external),
		IdempotencyKey:        deref(idem),
		PayloadHash:           strings.TrimSpace(deref(hash)),
		RoundID:               deref(roundID),
		GameID:                deref(gameID),
		ReferenceExternalID:   deref(refExt),
		FailureCode:           deref(failure),
		CreatedAt:             created,
		UpdatedAt:             updated,
	}
	if refID.Valid {
		u := uuid.UUID(refID.Bytes)
		snap.ReferenceTransactionID = &u
	}
	if resultMinor != nil {
		m, err := money.FromMinor(*resultMinor, cur)
		if err != nil {
			return nil, err
		}
		snap.ResultBalance = &m
	}
	return wager.Rehydrate(snap)
}

func deref(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}
