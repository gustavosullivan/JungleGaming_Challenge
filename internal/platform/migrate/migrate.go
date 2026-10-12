package migrate

import (
	"context"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jungle-gaming/wagering/migrations"
)

type step struct {
	version int
	name    string
	up      string
	down    string
}

func Up(ctx context.Context, pool *pgxpool.Pool) error {
	if err := ensureTable(ctx, pool); err != nil {
		return err
	}
	steps, err := load()
	if err != nil {
		return err
	}
	applied, err := appliedVersions(ctx, pool)
	if err != nil {
		return err
	}
	for _, s := range steps {
		if applied[s.version] {
			continue
		}
		if err := execVersion(ctx, pool, s.up, s.version, s.name, true); err != nil {
			return fmt.Errorf("migrate up %d: %w", s.version, err)
		}
	}
	return nil
}

func Down(ctx context.Context, pool *pgxpool.Pool) error {
	if err := ensureTable(ctx, pool); err != nil {
		return err
	}
	steps, err := load()
	if err != nil {
		return err
	}
	applied, err := appliedVersions(ctx, pool)
	if err != nil {
		return err
	}
	var last *step
	for i := range steps {
		if applied[steps[i].version] {
			last = &steps[i]
		}
	}
	if last == nil {
		return nil
	}
	if err := execVersion(ctx, pool, last.down, last.version, last.name, false); err != nil {
		return fmt.Errorf("migrate down %d: %w", last.version, err)
	}
	return nil
}

func ensureTable(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version integer PRIMARY KEY,
			name text NOT NULL,
			applied_at timestamptz NOT NULL
		)`)
	return err
}

func appliedVersions(ctx context.Context, pool *pgxpool.Pool) (map[int]bool, error) {
	rows, err := pool.Query(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int]bool{}
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out[v] = true
	}
	return out, rows.Err()
}

func execVersion(ctx context.Context, pool *pgxpool.Pool, sql string, version int, name string, up bool) error {
	stmts, err := statements(sql)
	if err != nil {
		return err
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for _, stmt := range stmts {
		if _, err := tx.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("%s: %w", firstLine(stmt), err)
		}
	}
	if up {
		_, err = tx.Exec(ctx, `INSERT INTO schema_migrations (version, name, applied_at) VALUES ($1, $2, now())`, version, name)
	} else {
		_, err = tx.Exec(ctx, `DELETE FROM schema_migrations WHERE version = $1`, version)
	}
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func load() ([]step, error) {
	entries, err := fs.ReadDir(migrations.FS, ".")
	if err != nil {
		return nil, err
	}
	byVersion := map[int]*step{}
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".sql") {
			continue
		}
		version, kind, stem, err := parseName(name)
		if err != nil {
			return nil, err
		}
		body, err := fs.ReadFile(migrations.FS, name)
		if err != nil {
			return nil, err
		}
		s := byVersion[version]
		if s == nil {
			s = &step{version: version, name: stem}
			byVersion[version] = s
		}
		if kind == "up" {
			s.up = string(body)
		} else {
			s.down = string(body)
		}
	}
	out := make([]step, 0, len(byVersion))
	for _, s := range byVersion {
		if s.up == "" || s.down == "" {
			return nil, fmt.Errorf("migration %d is missing up or down", s.version)
		}
		out = append(out, *s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].version < out[j].version })
	return out, nil
}

func parseName(name string) (int, string, string, error) {
	parts := strings.SplitN(name, "_", 2)
	if len(parts) != 2 {
		return 0, "", "", fmt.Errorf("bad migration name %s", name)
	}
	version, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, "", "", fmt.Errorf("bad migration version %s", name)
	}
	rest := parts[1]
	switch {
	case strings.HasSuffix(rest, ".up.sql"):
		return version, "up", strings.TrimSuffix(rest, ".up.sql"), nil
	case strings.HasSuffix(rest, ".down.sql"):
		return version, "down", strings.TrimSuffix(rest, ".down.sql"), nil
	default:
		return 0, "", "", fmt.Errorf("bad migration name %s", name)
	}
}

func statements(sql string) ([]string, error) {
	var out []string
	var b strings.Builder
	dollar := false
	for i := 0; i < len(sql); i++ {
		if !dollar && strings.HasPrefix(sql[i:], "--") {
			nl := strings.IndexByte(sql[i:], '\n')
			if nl < 0 {
				break
			}
			i += nl
			continue
		}
		if strings.HasPrefix(sql[i:], "$$") {
			dollar = !dollar
			b.WriteString("$$")
			i++
			continue
		}
		if sql[i] == ';' && !dollar {
			if s := strings.TrimSpace(b.String()); s != "" {
				out = append(out, s)
			}
			b.Reset()
			continue
		}
		b.WriteByte(sql[i])
	}
	if dollar {
		return nil, fmt.Errorf("unclosed dollar quote")
	}
	if s := strings.TrimSpace(b.String()); s != "" {
		out = append(out, s)
	}
	return out, nil
}

func firstLine(stmt string) string {
	line, _, _ := strings.Cut(stmt, "\n")
	if len(line) > 80 {
		return line[:80]
	}
	return line
}
