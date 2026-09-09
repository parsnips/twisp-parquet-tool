package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/duckdb/duckdb-go/v2"
)

type store struct {
	db             *sql.DB
	entities       sync.Map // entity -> *entityState, guarded by its gate
	sourceSchemas  sync.Map // physical Parquet schema signature -> []column
	lookupFiles    *sql.Stmt
	recordFiles    *sql.Stmt
	identity       sync.RWMutex
	tenantID       string // protected by identity; first successful nonempty import binds it
	migratedTables int
}

func ident(s string) string   { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }
func literal(s string) string { return `'` + strings.ReplaceAll(s, `'`, `''`) + `'` }

func openStore(ctx context.Context, path, endpoint, tenant, memoryLimit string) (*store, error) {
	// duckdb-go splits DSNs at '?' and does not URL-decode filesystem paths.
	if strings.ContainsAny(path, "?%") {
		return nil, errors.New("database path must not contain '?' or '%' (DuckDB Go DSN delimiters)")
	}
	connector, err := duckdb.NewConnector(path, func(conn driver.ExecerContext) error {
		_, err := conn.ExecContext(context.Background(), "SET TimeZone = 'UTC'", nil)
		return err
	})
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(5)
	s := &store{db: db}
	ok := false
	defer func() {
		if !ok {
			db.Close()
		}
	}()
	if _, err := db.ExecContext(ctx, "SET TimeZone = 'UTC'"); err != nil {
		return nil, err
	}
	if _, err := db.ExecContext(ctx, "SET memory_limit = "+literal(memoryLimit)); err != nil {
		return nil, err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	for _, statement := range []string{
		`CREATE SCHEMA IF NOT EXISTS root`,
		`CREATE SCHEMA IF NOT EXISTS public`,
		`CREATE SCHEMA IF NOT EXISTS _twisp`,
		`CREATE TABLE IF NOT EXISTS _twisp.settings (name VARCHAR PRIMARY KEY, value VARCHAR NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS _twisp.imported_files (
            key VARCHAR PRIMARY KEY, entity VARCHAR NOT NULL, row_count BIGINT NOT NULL,
            byte_count BIGINT NOT NULL, imported_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
        )`,
	} {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return nil, err
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO _twisp.settings VALUES ('format_version', '1') ON CONFLICT DO NOTHING`); err != nil {
		return nil, err
	}
	for _, setting := range [][2]string{{"endpoint", endpoint}, {"tenant", tenant}} {
		if err := bindSetting(ctx, tx, setting[0], setting[1]); err != nil {
			return nil, err
		}
	}
	for _, schema := range []string{"main", "public"} {
		if _, err := tx.ExecContext(ctx, `CREATE OR REPLACE MACRO `+schema+`.f_sql_status_int(status) AS
          CASE status WHEN 'ALIVE' THEN 0 WHEN 'EOL' THEN 1 WHEN 'DELETE' THEN 2 ELSE NULL END`); err != nil {
			return nil, err
		}
	}
	s.migratedTables, err = migrateUUIDEncoding(ctx, tx)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	err = db.QueryRowContext(ctx, `SELECT value FROM _twisp.settings WHERE name = 'record_tenantid'`).Scan(&s.tenantID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	s.lookupFiles, err = db.PrepareContext(ctx, `SELECT key FROM _twisp.imported_files WHERE key IN (SELECT unnest(?))`)
	if err != nil {
		return nil, err
	}
	s.recordFiles, err = db.PrepareContext(ctx, `INSERT INTO _twisp.imported_files(key,entity,row_count,byte_count)
        SELECT unnest(?), ?, unnest(?), unnest(?)`)
	if err != nil {
		s.lookupFiles.Close()
		return nil, err
	}
	ok = true
	return s, nil
}

func bindSetting(ctx context.Context, tx *sql.Tx, name, value string) error {
	var existing string
	err := tx.QueryRowContext(ctx, `SELECT value FROM _twisp.settings WHERE name = ?`, name).Scan(&existing)
	if errors.Is(err, sql.ErrNoRows) {
		_, err = tx.ExecContext(ctx, `INSERT INTO _twisp.settings VALUES (?, ?)`, name, value)
		return err
	}
	if err != nil {
		return err
	}
	if existing != value {
		return fmt.Errorf("database %s is %q, requested %q; use a separate -db file", name, existing, value)
	}
	return nil
}

func (s *store) Close() error {
	return errors.Join(s.lookupFiles.Close(), s.recordFiles.Close(), s.db.Close())
}
func (s *store) checkpoint(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, "CHECKPOINT")
	return err
}
func (s *store) hasFile(ctx context.Context, key string) (bool, error) {
	keys, err := s.hasFiles(ctx, []string{key})
	return keys[key], err
}

func (s *store) hasFiles(ctx context.Context, keys []string) (map[string]bool, error) {
	return lookupKeys(ctx, s.lookupFiles, keys)
}

func lookupKeys(ctx context.Context, stmt *sql.Stmt, keys []string) (map[string]bool, error) {
	found := make(map[string]bool)
	if len(keys) == 0 {
		return found, nil
	}
	rows, err := stmt.QueryContext(ctx, keys)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, err
		}
		found[key] = true
	}
	return found, rows.Err()
}

type column struct{ Name, Type string }

func columns(ctx context.Context, tx *sql.Tx, source string) ([]column, error) {
	rows, err := tx.QueryContext(ctx, "DESCRIBE SELECT * FROM "+source)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []column
	for rows.Next() {
		var c column
		var nullable, key, def, extra sql.NullString
		if err := rows.Scan(&c.Name, &c.Type, &nullable, &key, &def, &extra); err != nil {
			return nil, err
		}
		result = append(result, c)
	}
	return result, rows.Err()
}

func columnMap(cols []column) map[string]string {
	out := make(map[string]string, len(cols))
	for _, c := range cols {
		out[c.Name] = c.Type
	}
	return out
}

// Single-file entry point also uses the batched implementation.
func (s *store) importFile(ctx context.Context, task fileTask, path string, size int64) (int64, bool, error) {
	result, err := s.importBatch(ctx, []downloadedFile{{Task: task, Path: path, Size: size}})
	return result.Rows, result.Imported != 0, err
}

func createViews(ctx context.Context, tx *sql.Tx, entity string) error {
	raw := "root." + ident("parquet_"+entity)
	history := "public." + ident(entity+"_history")
	latest := "public." + ident(entity)
	// Match tools/protoutil/redshift_schema_gen.go and tenant datashare views:
	// history resolves status transitions at the SAME version before latest ranks
	// versions. Filtering ALIVE before ranking would resurrect deleted records.
	for _, statement := range []string{
		`CREATE VIEW ` + history + ` AS SELECT * FROM ` + raw + `
          QUALIFY row_number() OVER (
            PARTITION BY record_tenantid, record_rowid, record_version
            ORDER BY public.f_sql_status_int(record_status) DESC, record_begin DESC
          ) = 1`,
		`CREATE VIEW ` + latest + ` AS SELECT * FROM ` + history + `
          QUALIFY row_number() OVER (
            PARTITION BY record_tenantid, record_rowid ORDER BY record_version DESC
          ) = 1`,
		`CREATE VIEW main.` + ident(entity+"_history") + ` AS SELECT * FROM ` + history,
		`CREATE VIEW main.` + ident(entity) + ` AS SELECT * FROM ` + latest,
	} {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}
