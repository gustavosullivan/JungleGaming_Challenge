package postgres

import (
	"context"
	"errors"
	"net"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jungle-gaming/wagering/internal/app/port"
	"github.com/jungle-gaming/wagering/internal/domain/money"
	"github.com/jungle-gaming/wagering/internal/domain/wallet"
)

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

func (s *Store) WithinTx(ctx context.Context, fn func(context.Context, port.Tx) error) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := fn(ctx, &pgTx{tx: tx}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

type pgTx struct{ tx pgx.Tx }

func uuidArg(id uuid.UUID) pgtype.UUID {
	return pgtype.UUID{Bytes: [16]byte(id), Valid: true}
}

func uuidPtrArg(id *uuid.UUID) pgtype.UUID {
	if id == nil || *id == uuid.Nil {
		return pgtype.UUID{}
	}
	return uuidArg(*id)
}

func takeUUID(v pgtype.UUID) uuid.UUID {
	if !v.Valid {
		return uuid.Nil
	}
	return uuid.UUID(v.Bytes)
}

func nullString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func mapWrite(err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return &port.Conflict{Constraint: pgErr.ConstraintName}
	}
	return err
}

func IsTransient(err error) bool {
	if err == nil {
		return false
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		code := pgErr.Code
		switch {
		case strings.HasPrefix(code, "08"),
			strings.HasPrefix(code, "40"),
			strings.HasPrefix(code, "53"),
			strings.HasPrefix(code, "57"),
			code == "55P03":
			return true
		}
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "timeout")
}

func (t *pgTx) LockWallet(ctx context.Context, id uuid.UUID, mode port.LockMode) (*wallet.Wallet, error) {
	query := walletSelect + ` WHERE id = $1 FOR SHARE`
	if mode == port.LockUpdate {
		query = walletSelect + ` WHERE id = $1 FOR UPDATE`
	}
	return scanWallet(t.tx.QueryRow(ctx, query, uuidArg(id)))
}

func (t *pgTx) InsertWallet(ctx context.Context, w *wallet.Wallet) error {
	_, err := t.tx.Exec(ctx, `
		INSERT INTO wallets (id, player_id, currency, balance_minor, version, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		uuidArg(w.ID()), uuidArg(w.PlayerID()), w.Currency(), w.Balance().Minor(), w.Version(), w.CreatedAt(), w.UpdatedAt())
	return mapWrite(err)
}

func (t *pgTx) UpdateWallet(ctx context.Context, w *wallet.Wallet, previousVersion int64) error {
	tag, err := t.tx.Exec(ctx, `
		UPDATE wallets
		SET balance_minor = $1, version = $2, updated_at = $3
		WHERE id = $4 AND version = $5`,
		w.Balance().Minor(), w.Version(), w.UpdatedAt(), uuidArg(w.ID()), previousVersion)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return port.ErrConcurrentUpdate
	}
	return nil
}

const walletSelect = `
	SELECT id, player_id, currency, balance_minor, version, created_at, updated_at
	FROM wallets`

func scanWallet(row pgx.Row) (*wallet.Wallet, error) {
	var (
		id, player pgtype.UUID
		currency   string
		minor      int64
		version    int64
		created    time.Time
		updated    time.Time
	)
	if err := row.Scan(&id, &player, &currency, &minor, &version, &created, &updated); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, port.ErrNotFound
		}
		return nil, err
	}
	bal, err := money.FromMinor(minor, strings.TrimSpace(currency))
	if err != nil {
		return nil, err
	}
	return wallet.Rehydrate(takeUUID(id), takeUUID(player), bal, version, created, updated)
}

func (s *Store) GetWallet(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error) {
	return scanWallet(s.pool.QueryRow(ctx, walletSelect+` WHERE id = $1`, uuidArg(id)))
}
