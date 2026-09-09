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
	entities       sync.Map // entity -> context-aware gate; one transaction per entity
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

func (s *store) Close() error { return s.db.Close() }
func (s *store) checkpoint(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, "CHECKPOINT")
	return err
}
func (s *store) hasFile(ctx context.Context, key string) (bool, error) {
	var found bool
	err := s.db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM _twisp.imported_files WHERE key = ?)`, key).Scan(&found)
	return found, err
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

// Each file, its schema/view updates, and its checkpoint commit atomically. A failed
// or interrupted import can be rerun without skipping data or inserting it twice.
func (s *store) importFile(ctx context.Context, task fileTask, path string, size int64) (int64, bool, error) {
	if !entityName.MatchString(task.Entity) {
		return 0, false, errors.New("invalid entity name")
	}
	gate, _ := s.entities.LoadOrStore(task.Entity, make(chan struct{}, 1))
	select {
	case gate.(chan struct{}) <- struct{}{}:
	case <-ctx.Done():
		return 0, false, ctx.Err()
	}
	defer func() { <-gate.(chan struct{}) }()
	// Serialize only startup until one successful import establishes tenant identity.
	// Every later entity transaction can hold the read lock concurrently.
	s.identity.RLock()
	if s.tenantID == "" {
		s.identity.RUnlock()
		s.identity.Lock()
		defer s.identity.Unlock()
	} else {
		defer s.identity.RUnlock()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, false, err
	}
	defer tx.Rollback()
	var loaded bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM _twisp.imported_files WHERE key = ?)`, task.Key).Scan(&loaded); err != nil {
		return 0, false, err
	}
	if loaded {
		return 0, false, nil
	}
	source := `read_parquet(` + literal(path) + `, hive_partitioning = false)`
	sourceColumns, err := columns(ctx, tx, source)
	if err != nil {
		return 0, false, fmt.Errorf("read Parquet schema: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `CREATE TEMP TABLE _twisp_incoming AS SELECT `+normalizedProjection(sourceColumns)+` FROM `+source); err != nil {
		return 0, false, fmt.Errorf("read Parquet: %w", err)
	}
	incoming, err := columns(ctx, tx, "_twisp_incoming")
	if err != nil {
		return 0, false, err
	}
	types := columnMap(incoming)
	for _, name := range []string{"record_begin", "record_rowid", "record_status", "record_tenantid", "record_version"} {
		if _, ok := types[name]; !ok {
			return 0, false, fmt.Errorf("Parquet is missing CDC column %s", name)
		}
	}
	switch types["record_version"] {
	case "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT", "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "UHUGEINT":
	default:
		return 0, false, errors.New("record_version must have an integer Parquet type")
	}
	var rowCount, tenantCount, invalid int64
	var tenantUUID sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT count(*), count(DISTINCT record_tenantid), min(CAST(record_tenantid AS VARCHAR)),
      count(*) FILTER (WHERE record_begin IS NULL OR record_rowid IS NULL OR record_rowid = '' OR
        record_version IS NULL OR record_version < 0 OR record_tenantid IS NULL OR record_tenantid = '' OR
        record_status IS NULL OR record_status NOT IN ('ALIVE', 'EOL', 'DELETE'))
      FROM _twisp_incoming`).Scan(&rowCount, &tenantCount, &tenantUUID, &invalid)
	if err != nil {
		return 0, false, err
	}
	if invalid != 0 {
		return 0, false, fmt.Errorf("Parquet contains %d rows with invalid CDC metadata", invalid)
	}
	if tenantCount > 1 {
		return 0, false, errors.New("Parquet contains more than one record_tenantid")
	}
	if tenantUUID.Valid {
		if err := bindSetting(ctx, tx, "record_tenantid", tenantUUID.String); err != nil {
			return 0, false, err
		}
	}
	rawName := "parquet_" + task.Entity
	raw := "root." + ident(rawName)
	var exists bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'root' AND table_name = ?)`, rawName).Scan(&exists); err != nil {
		return 0, false, err
	}
	var added []column
	if exists {
		current, err := columns(ctx, tx, raw)
		if err != nil {
			return 0, false, err
		}
		types := columnMap(current)
		for _, col := range incoming {
			old, found := types[col.Name]
			if !found {
				added = append(added, col)
				continue
			}
			if old != col.Type {
				return 0, false, fmt.Errorf("schema type changed for %s.%s: %s -> %s; refusing a potentially lossy cast", task.Entity, col.Name, old, col.Type)
			}
		}
	}
	// Existing views see appended rows automatically. Only rebuild them when the
	// schema changes, avoiding unnecessary catalog writes on every imported file.
	rebuildViews := !exists || len(added) > 0
	if rebuildViews {
		if err := dropEntityViews(ctx, tx, task.Entity); err != nil {
			return 0, false, err
		}
	}
	if !exists {
		if _, err := tx.ExecContext(ctx, "CREATE TABLE "+raw+" AS SELECT * FROM _twisp_incoming WHERE false"); err != nil {
			return 0, false, err
		}
	} else {
		for _, col := range added {
			if _, err := tx.ExecContext(ctx, "ALTER TABLE "+raw+" ADD COLUMN "+ident(col.Name)+" "+col.Type); err != nil {
				return 0, false, err
			}
		}
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO "+raw+" BY NAME SELECT * FROM _twisp_incoming"); err != nil {
		return 0, false, err
	}
	if rebuildViews {
		if err := createViews(ctx, tx, task.Entity); err != nil {
			return 0, false, err
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO _twisp.imported_files(key, entity, row_count, byte_count) VALUES (?, ?, ?, ?)`, task.Key, task.Entity, rowCount, size); err != nil {
		return 0, false, err
	}
	if _, err := tx.ExecContext(ctx, "DROP TABLE _twisp_incoming"); err != nil {
		return 0, false, err
	}
	if err := tx.Commit(); err != nil {
		return 0, false, err
	}
	if s.tenantID == "" && tenantUUID.Valid {
		s.tenantID = tenantUUID.String
	}
	return rowCount, true, nil
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
