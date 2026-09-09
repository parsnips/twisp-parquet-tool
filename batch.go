package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
)

const sourceFileColumn = "__twisp_source_file"

type entityState struct {
	gate    chan struct{}
	columns []column // immutable between successful commits; no speculative cache updates
}

type schemaGroup struct {
	signature string
	files     []downloadedFile
}

func filePaths(files []downloadedFile) []string {
	paths := make([]string, len(files))
	for i, f := range files {
		paths[i] = f.Path
	}
	return paths
}

func pathList(files []downloadedFile) string {
	paths := filePaths(files)
	for i := range paths {
		paths[i] = literal(paths[i])
	}
	return "[" + strings.Join(paths, ",") + "]"
}

// Read all footers with one query, grouping by the full physical schema. Grouping
// BEFORE read_parquet prevents DuckDB from implicitly casting legacy BLOB UUIDs
// to escaped strings, or silently casting an incompatible column across files.
func schemaGroups(ctx context.Context, tx *sql.Tx, files []downloadedFile) ([]schemaGroup, error) {
	rows, err := tx.QueryContext(ctx, `SELECT file_name, to_json(list(struct_pack(
        name := name, physical_type := type, type_length := type_length,
        repetition_type := repetition_type, num_children := num_children,
        converted_type := converted_type, scale := scale, decimal_precision := "precision",
        field_id := field_id, logical_type := logical_type, duckdb_type := duckdb_type
      ) ORDER BY column_id))::VARCHAR FROM parquet_schema(?) GROUP BY file_name`, filePaths(files))
	if err != nil {
		return nil, fmt.Errorf("read batch Parquet schemas: %w", err)
	}
	signatures := make(map[string]string, len(files))
	for rows.Next() {
		var path, signature string
		if err := rows.Scan(&path, &signature); err != nil {
			rows.Close()
			return nil, err
		}
		signatures[path] = signature
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	var groups []schemaGroup
	indexes := map[string]int{}
	for _, f := range files {
		signature, ok := signatures[f.Path]
		if !ok {
			return nil, fmt.Errorf("missing Parquet schema for %s", f.Task.Key)
		}
		i, ok := indexes[signature]
		if !ok {
			i = len(groups)
			indexes[signature] = i
			groups = append(groups, schemaGroup{signature: signature})
		}
		groups[i].files = append(groups[i].files, f)
	}
	return groups, nil
}

func normalizedColumns(cols []column) []column {
	result := append([]column(nil), cols...)
	for i, c := range result {
		if uuidColumn(c.Name) && (c.Type == "BLOB" || c.Type == "UUID") {
			result[i].Type = "VARCHAR"
		}
	}
	return result
}

func validateColumns(cols []column) error {
	types := columnMap(cols)
	for _, c := range cols {
		if strings.EqualFold(c.Name, sourceFileColumn) {
			return fmt.Errorf("Parquet uses reserved column %s", c.Name)
		}
	}
	for _, name := range []string{"record_begin", "record_rowid", "record_status", "record_tenantid", "record_version"} {
		if _, ok := types[name]; !ok {
			return fmt.Errorf("Parquet is missing CDC column %s", name)
		}
	}
	switch types["record_version"] {
	case "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT", "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "UHUGEINT":
		return nil
	default:
		return errors.New("record_version must have an integer Parquet type")
	}
}

// Every file and its checkpoint in a batch commits atomically. Different physical
// schemas get separate vectorized reads within that same entity transaction.
func (s *store) importBatch(ctx context.Context, input []downloadedFile) (totals, error) {
	var result totals
	if len(input) == 0 {
		return result, nil
	}
	entity := input[0].Task.Entity
	if !entityName.MatchString(entity) {
		return result, errors.New("invalid entity name")
	}
	keys := make([]string, len(input))
	for i, f := range input {
		if f.Task.Entity != entity {
			return result, errors.New("a batch must contain only one entity")
		}
		keys[i] = f.Task.Key
	}
	value, _ := s.entities.LoadOrStore(entity, &entityState{gate: make(chan struct{}, 1)})
	state := value.(*entityState)
	select {
	case state.gate <- struct{}{}:
	case <-ctx.Done():
		return result, ctx.Err()
	}
	defer func() { <-state.gate }()
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
		return result, err
	}
	defer tx.Rollback()
	lookup := tx.StmtContext(ctx, s.lookupFiles)
	defer lookup.Close()
	loaded, err := lookupKeys(ctx, lookup, keys)
	if err != nil {
		return result, err
	}
	var files []downloadedFile
	seen := make(map[string]bool, len(input))
	paths := make(map[string]string, len(input))
	for _, f := range input {
		if loaded[f.Task.Key] || seen[f.Task.Key] {
			result.Skipped++
			continue
		}
		seen[f.Task.Key] = true
		f.Path, err = filepath.Abs(f.Path)
		if err != nil {
			return totals{}, err
		}
		if _, ok := paths[f.Path]; ok {
			return totals{}, errors.New("different object keys must have distinct downloaded paths in a batch")
		}
		paths[f.Path] = f.Task.Key
		files = append(files, f)
	}
	if len(files) == 0 {
		return result, nil
	}
	groups, err := schemaGroups(ctx, tx, files)
	if err != nil {
		return totals{}, err
	}
	rawName := "parquet_" + entity
	raw := "root." + ident(rawName)
	current := append([]column(nil), state.columns...)
	exists := len(current) > 0
	if !exists {
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='root' AND table_name=?)`, rawName).Scan(&exists); err != nil {
			return totals{}, err
		}
		if exists {
			current, err = columns(ctx, tx, raw)
			if err != nil {
				return totals{}, err
			}
		}
	}
	viewChanges := false
	tenantID := s.tenantID
	counts := make(map[string]int64, len(files))
	for _, g := range groups {
		var sourceColumns []column
		if cached, ok := s.sourceSchemas.Load(g.signature); ok {
			sourceColumns = cached.([]column)
		} else {
			sourceColumns, err = columns(ctx, tx, `read_parquet(`+literal(g.files[0].Path)+`, hive_partitioning=false)`)
			if err != nil {
				return totals{}, err
			}
			if err := validateColumns(sourceColumns); err != nil {
				return totals{}, err
			}
			s.sourceSchemas.Store(g.signature, sourceColumns)
		}
		incoming := normalizedColumns(sourceColumns)
		if _, err := tx.ExecContext(ctx, `CREATE TEMP TABLE _twisp_incoming AS SELECT `+normalizedProjection(sourceColumns)+`, `+ident(sourceFileColumn)+
			` FROM read_parquet(`+pathList(g.files)+`, hive_partitioning=false, filename=`+literal(sourceFileColumn)+`)`); err != nil {
			return totals{}, fmt.Errorf("read Parquet batch: %w", err)
		}
		groupTenant, groupCounts, err := validateIncoming(ctx, tx, paths, tenantID)
		if err != nil {
			return totals{}, err
		}
		if groupTenant != "" {
			tenantID = groupTenant
		}
		for path, n := range groupCounts {
			counts[path] = n
		}
		known := columnMap(current)
		var added []column
		for _, c := range incoming {
			old, found := known[c.Name]
			if !found {
				added = append(added, c)
			} else if old != c.Type {
				return totals{}, fmt.Errorf("schema type changed for %s.%s: %s -> %s; refusing a potentially lossy cast", entity, c.Name, old, c.Type)
			}
		}
		if !exists || len(added) > 0 {
			if !viewChanges {
				if err := dropEntityViews(ctx, tx, entity); err != nil {
					return totals{}, err
				}
				viewChanges = true
			}
			if !exists {
				if _, err := tx.ExecContext(ctx, "CREATE TABLE "+raw+" AS SELECT * EXCLUDE ("+ident(sourceFileColumn)+") FROM _twisp_incoming WHERE false"); err != nil {
					return totals{}, err
				}
				exists = true
				current = append([]column(nil), incoming...)
			} else {
				for _, c := range added {
					if _, err := tx.ExecContext(ctx, "ALTER TABLE "+raw+" ADD COLUMN "+ident(c.Name)+" "+c.Type); err != nil {
						return totals{}, err
					}
					current = append(current, c)
				}
			}
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO "+raw+" BY NAME SELECT * EXCLUDE ("+ident(sourceFileColumn)+") FROM _twisp_incoming"); err != nil {
			return totals{}, err
		}
		if _, err := tx.ExecContext(ctx, "DROP TABLE _twisp_incoming"); err != nil {
			return totals{}, err
		}
	}
	if s.tenantID == "" && tenantID != "" {
		if err := bindSetting(ctx, tx, "record_tenantid", tenantID); err != nil {
			return totals{}, err
		}
	}
	if viewChanges {
		if err := createViews(ctx, tx, entity); err != nil {
			return totals{}, err
		}
	}
	keys = make([]string, len(files))
	rowCounts := make([]int64, len(files))
	sizes := make([]int64, len(files))
	for i, f := range files {
		keys[i] = f.Task.Key
		rowCounts[i] = counts[f.Path]
		sizes[i] = f.Size
		result.Rows += rowCounts[i]
		result.Bytes += f.Size
	}
	manifest := tx.StmtContext(ctx, s.recordFiles)
	defer manifest.Close()
	if _, err := manifest.ExecContext(ctx, keys, entity, rowCounts, sizes); err != nil {
		return totals{}, err
	}
	if err := tx.Commit(); err != nil {
		return totals{}, err
	}
	state.columns = current
	if s.tenantID == "" {
		s.tenantID = tenantID
	}
	result.Imported = int64(len(files))
	return result, nil
}

func validateIncoming(ctx context.Context, tx *sql.Tx, keys map[string]string, tenantID string) (string, map[string]int64, error) {
	rows, err := tx.QueryContext(ctx, `SELECT `+ident(sourceFileColumn)+`,count(*),count(DISTINCT record_tenantid),min(CAST(record_tenantid AS VARCHAR)),
        count(*) FILTER (WHERE record_begin IS NULL OR record_rowid IS NULL OR record_rowid='' OR
        record_version IS NULL OR record_version<0 OR record_tenantid IS NULL OR record_tenantid='' OR
        record_status IS NULL OR record_status NOT IN ('ALIVE','EOL','DELETE'))
        FROM _twisp_incoming GROUP BY `+ident(sourceFileColumn))
	if err != nil {
		return "", nil, err
	}
	defer rows.Close()
	counts := map[string]int64{}
	for rows.Next() {
		var path string
		var n, tenants, invalid int64
		var tenant sql.NullString
		if err := rows.Scan(&path, &n, &tenants, &tenant, &invalid); err != nil {
			return "", nil, err
		}
		key, ok := keys[path]
		if !ok {
			return "", nil, fmt.Errorf("unexpected source file in batch: %s", path)
		}
		if invalid > 0 {
			return "", nil, fmt.Errorf("%s: %d rows with invalid CDC metadata", key, invalid)
		}
		if tenants > 1 {
			return "", nil, fmt.Errorf("%s: Parquet contains more than one record_tenantid", key)
		}
		if tenant.Valid {
			if tenantID != "" && tenant.String != tenantID {
				return "", nil, fmt.Errorf("%s: database record_tenantid is %q, requested %q; use a separate -db file", key, tenantID, tenant.String)
			}
			tenantID = tenant.String
		}
		counts[path] = n
	}
	return tenantID, counts, rows.Err()
}
