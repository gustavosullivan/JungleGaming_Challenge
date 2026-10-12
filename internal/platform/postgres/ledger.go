package postgres

import (
	"context"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jungle-gaming/wagering/internal/app/port"
	"github.com/jungle-gaming/wagering/internal/domain/ledger"
	"github.com/jungle-gaming/wagering/internal/domain/money"
)

type queryer interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func (t *pgTx) InsertLedger(ctx context.Context, entry ledger.Entry) error {
	_, err := t.tx.Exec(ctx, `
		INSERT INTO wallet_ledger_entries (
			id, wallet_id, transaction_id, direction, amount_minor, currency,
			balance_before_minor, balance_after_minor, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		uuidArg(entry.ID()), uuidArg(entry.WalletID()), uuidArg(entry.TransactionID()),
		string(entry.Direction()), entry.Amount().Minor(), entry.Amount().Currency(),
		entry.BalanceBefore().Minor(), entry.BalanceAfter().Minor(), entry.CreatedAt())
	return mapWrite(err)
}

func (t *pgTx) ListLedger(ctx context.Context, walletID uuid.UUID, after *port.LedgerCursor, limit int) (port.LedgerPage, error) {
	return listLedger(ctx, t.tx, walletID, after, limit)
}

func (t *pgTx) ListAllLedger(ctx context.Context, walletID uuid.UUID) ([]ledger.Entry, error) {
	page, err := listLedger(ctx, t.tx, walletID, nil, 0)
	if err != nil {
		return nil, err
	}
	return page.Entries, nil
}

func (s *Store) ListLedger(ctx context.Context, walletID uuid.UUID, after *port.LedgerCursor, limit int) (port.LedgerPage, error) {
	return listLedger(ctx, s.pool, walletID, after, limit)
}

func listLedger(ctx context.Context, q queryer, walletID uuid.UUID, after *port.LedgerCursor, limit int) (port.LedgerPage, error) {
	args := []any{uuidArg(walletID)}
	query := ledgerSelect + ` WHERE wallet_id = $1`
	if after != nil {
		query += ` AND (created_at, id) > ($2, $3)`
		args = append(args, after.CreatedAt, uuidArg(after.ID))
	}
	query += ` ORDER BY created_at ASC, id ASC`
	if limit > 0 {
		args = append(args, limit+1)
		query += ` LIMIT $` + itoa(len(args))
	}
	rows, err := q.Query(ctx, query, args...)
	if err != nil {
		return port.LedgerPage{}, err
	}
	defer rows.Close()
	var entries []ledger.Entry
	for rows.Next() {
		entry, err := scanLedger(rows)
		if err != nil {
			return port.LedgerPage{}, err
		}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return port.LedgerPage{}, err
	}
	page := port.LedgerPage{Entries: entries}
	if limit > 0 && len(entries) > limit {
		page.HasMore = true
		page.Entries = entries[:limit]
	}
	return page, nil
}

const ledgerSelect = `
	SELECT id, wallet_id, transaction_id, direction, amount_minor, currency,
		balance_before_minor, balance_after_minor, created_at
	FROM wallet_ledger_entries`

func scanLedger(row pgx.Row) (ledger.Entry, error) {
	var (
		id, walletID, txID    pgtype.UUID
		direction, currency   string
		amount, before, after int64
		created               time.Time
	)
	if err := row.Scan(&id, &walletID, &txID, &direction, &amount, &currency, &before, &after, &created); err != nil {
		return ledger.Entry{}, err
	}
	cur := strings.TrimSpace(currency)
	amt, err := money.FromMinor(amount, cur)
	if err != nil {
		return ledger.Entry{}, err
	}
	beforeM, err := money.FromMinor(before, cur)
	if err != nil {
		return ledger.Entry{}, err
	}
	afterM, err := money.FromMinor(after, cur)
	if err != nil {
		return ledger.Entry{}, err
	}
	return ledger.Rehydrate(ledger.Snapshot{
		ID: takeUUID(id), WalletID: takeUUID(walletID), TransactionID: takeUUID(txID),
		Direction: ledger.Direction(direction), Amount: amt,
		BalanceBefore: beforeM, BalanceAfter: afterM, CreatedAt: created,
	})
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [16]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
