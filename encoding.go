package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// Older warehouse files encoded UUIDs as unannotated binary bytes. Newer files
// encode them as UTF-8 strings. Do not apply this to arbitrary binary fields such
// as balance.dimension, or to user-defined external/correlation identifiers.
func uuidColumn(name string) bool {
	return name == "record_rowid" || name == "record_tenantid" || name == "void_of" || name == "voided_by" ||
		(strings.HasSuffix(name, "_id") && name != "external_id" && name != "correlation_id")
}

func binaryUUID(expr string) string {
	return `CASE WHEN ` + expr + ` IS NULL THEN NULL
      WHEN octet_length(` + expr + `) = 0 THEN ''
      WHEN octet_length(` + expr + `) = 16 THEN lower(regexp_replace(hex(` + expr + `),
        '(.{8})(.{4})(.{4})(.{4})(.{12})', '\1-\2-\3-\4-\5'))
      WHEN try_cast(CAST(` + expr + ` AS VARCHAR) AS UUID) IS NOT NULL
        THEN CAST(CAST(CAST(` + expr + ` AS VARCHAR) AS UUID) AS VARCHAR)
      ELSE error('Invalid binary UUID: expected 16 bytes or UUID text') END`
}

func normalizedColumn(c column) string {
	expr := ident(c.Name)
	if !uuidColumn(c.Name) {
		return expr
	}
	switch c.Type {
	case "BLOB":
		return binaryUUID(expr)
	case "UUID":
		return "CAST(" + expr + " AS VARCHAR)"
	case "VARCHAR":
		return "coalesce(CAST(try_cast(" + expr + " AS UUID) AS VARCHAR), " + expr + ")"
	default:
		return expr
	}
}

func normalizedProjection(cols []column) string {
	items := make([]string, len(cols))
	for i, c := range cols {
		items[i] = normalizedColumn(c) + " AS " + ident(c.Name)
	}
	return strings.Join(items, ", ")
}

func dropEntityViews(ctx context.Context, tx *sql.Tx, entity string) error {
	for _, schema := range []string{"main", "public"} {
		for _, name := range []string{entity, entity + "_history"} {
			if _, err := tx.ExecContext(ctx, "DROP VIEW IF EXISTS "+schema+"."+ident(name)); err != nil {
				return err
			}
		}
	}
	return nil
}

// Upgrade existing databases in the same initialization transaction. Checkpoints
// and row counts are preserved, so a restart continues without downloading old files.
func migrateUUIDEncoding(ctx context.Context, tx *sql.Tx) (int, error) {
	var version string
	if err := tx.QueryRowContext(ctx, `SELECT value FROM _twisp.settings WHERE name = 'format_version'`).Scan(&version); err != nil {
		return 0, err
	}
	if version == "2" {
		return 0, nil
	}
	if version != "1" {
		return 0, fmt.Errorf("unsupported database format version %q", version)
	}
	rows, err := tx.QueryContext(ctx, `SELECT table_name FROM information_schema.tables WHERE table_schema = 'root' AND starts_with(table_name, 'parquet_') ORDER BY table_name`)
	if err != nil {
		return 0, err
	}
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return 0, err
		}
		tables = append(tables, name)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, err
	}
	for _, name := range tables {
		entity := strings.TrimPrefix(name, "parquet_")
		if !entityName.MatchString(entity) {
			return 0, fmt.Errorf("invalid warehouse table %q", name)
		}
		table := "root." + ident(name)
		cols, err := columns(ctx, tx, table)
		if err != nil {
			return 0, err
		}
		if err := dropEntityViews(ctx, tx, entity); err != nil {
			return 0, err
		}
		for _, c := range cols {
			if !uuidColumn(c.Name) {
				continue
			}
			switch c.Type {
			case "BLOB", "UUID":
				_, err = tx.ExecContext(ctx, "ALTER TABLE "+table+" ALTER COLUMN "+ident(c.Name)+" TYPE VARCHAR USING "+normalizedColumn(c))
			case "VARCHAR":
				_, err = tx.ExecContext(ctx, "UPDATE "+table+" SET "+ident(c.Name)+" = "+normalizedColumn(c)+" WHERE "+ident(c.Name)+" <> "+normalizedColumn(c))
			}
			if err != nil {
				return 0, fmt.Errorf("normalize existing %s.%s: %w", entity, c.Name, err)
			}
		}
		if err := createViews(ctx, tx, entity); err != nil {
			return 0, err
		}
	}
	// Version 1 stored CAST(BLOB AS VARCHAR), i.e. DuckDB's escaped binary spelling.
	// CAST back to BLOB recovers those bytes before formatting as canonical UUID text.
	_, err = tx.ExecContext(ctx, `UPDATE _twisp.settings SET value =
      CASE WHEN try_cast(value AS UUID) IS NOT NULL THEN CAST(CAST(value AS UUID) AS VARCHAR)
      ELSE `+binaryUUID("CAST(value AS BLOB)")+` END WHERE name = 'record_tenantid'`)
	if err != nil {
		return 0, fmt.Errorf("normalize saved tenant identity: %w", err)
	}
	_, err = tx.ExecContext(ctx, `UPDATE _twisp.settings SET value = '2' WHERE name = 'format_version'`)
	return len(tables), err
}
