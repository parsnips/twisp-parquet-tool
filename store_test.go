package main

import (
	"context"
	"database/sql"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const fixtureTenant = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"

func fixtureParquet(t testing.TB, query string) string {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	path := filepath.Join(t.TempDir(), "fixture.parquet")
	if _, err := db.Exec("COPY (" + query + ") TO " + literal(path) + " (FORMAT PARQUET)"); err != nil {
		t.Fatal(err)
	}
	return path
}

func fixtureQuery(values, extra string) string {
	return `SELECT TIMESTAMP '2026-09-01 00:00:00' AS record_begin,
    rowid AS record_rowid, status AS record_status, '` + fixtureTenant + `' AS record_tenantid,
    version::UBIGINT AS record_version, rowid AS account_id, name` + extra + `
    FROM (VALUES ` + values + `) AS rows(rowid, version, status, name)`
}

func testStore(t *testing.T) *store {
	t.Helper()
	s, err := openStore(context.Background(), filepath.Join(t.TempDir(), "offline.duckdb"), "https://example.test/graphql", "Sandbox", "256MB")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func importFixture(t *testing.T, s *store, key, path string) {
	t.Helper()
	entity, err := entityForKey(key)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, imported, err := s.importFile(context.Background(), fileTask{Key: key, Entity: entity}, path, info.Size()); err != nil || !imported {
		t.Fatalf("import: imported=%v err=%v", imported, err)
	}
}

func count(t *testing.T, s *store, query string) int64 {
	t.Helper()
	var n int64
	if err := s.db.QueryRow(query).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestRedshiftViewsAndSchemaEvolution(t *testing.T) {
	s := testStore(t)
	first := fixtureParquet(t, fixtureQuery(`('a', 1, 'ALIVE', 'old'), ('b', 1, 'ALIVE', 'deleted later'), ('c', 1, 'ALIVE', 'ended later')`, ""))
	// Seed/live overlap; later delivery can contain earlier record versions.
	second := fixtureParquet(t, fixtureQuery(`('a', 2, 'ALIVE', 'new'), ('a', 1, 'EOL', 'old'), ('a', 1, 'ALIVE', 'old'),
      ('b', 1, 'EOL', 'deleted later'), ('b', 1, 'DELETE', 'deleted later'), ('c', 1, 'EOL', 'ended later')`, ", 'ADDED' AS external_id"))
	third := fixtureParquet(t, fixtureQuery(`('a', 1, 'ALIVE', 'old')`, ""))
	key := "warehouse/parquet/2026/09/01/00/account/one.parquet"
	importFixture(t, s, key, first)
	importFixture(t, s, "warehouse/parquet/2026/09/01/00/account/two.parquet", second)
	importFixture(t, s, "warehouse/parquet/2026/09/01/00/account/three.parquet", third)
	for query, want := range map[string]int64{
		"SELECT count(*) FROM account_history":                            4,
		"SELECT count(*) FROM public.account_history":                     4,
		"SELECT count(*) FROM account":                                    3,
		"SELECT count(*) FROM public.account WHERE record_status='ALIVE'": 1,
		"SELECT count(*) FROM account WHERE account_id='a' AND name='new' AND record_version=2 AND external_id='ADDED'": 1,
		"SELECT count(*) FROM account WHERE account_id='b' AND record_status='DELETE'":                                  1,
		"SELECT count(*) FROM account WHERE account_id='c' AND record_status='EOL'":                                     1,
		"SELECT count(*) FROM account_history WHERE account_id='a' AND record_version=1 AND record_status='EOL'":        1,
		"SELECT count(*) FROM root.parquet_account":                                                                     10,
		"SELECT count(*) FROM root.parquet_account WHERE external_id IS NULL":                                           4,
		"SELECT count(*) FROM _twisp.imported_files":                                                                    3,
	} {
		if got := count(t, s, query); got != want {
			t.Errorf("%s: got %d, want %d", query, got, want)
		}
	}
	// Checkpoint is consulted before opening the already-imported file.
	if _, imported, err := s.importFile(context.Background(), fileTask{Key: key, Entity: "account"}, "does-not-exist", 0); err != nil || imported {
		t.Fatalf("duplicate import: imported=%v err=%v", imported, err)
	}
}

func TestFailedImportRollsBackAndCanResume(t *testing.T) {
	s := testStore(t)
	first := fixtureParquet(t, fixtureQuery(`('a', 1, 'ALIVE', 'old')`, ""))
	importFixture(t, s, "warehouse/parquet/2026/09/01/00/account/good.parquet", first)
	tests := []struct{ name, query, match string }{
		{"type change", strings.Replace(fixtureQuery(`('a', 2, 'ALIVE', 'new')`, ""), "rowid AS account_id", "42::BIGINT AS account_id", 1), "schema type changed"},
		{"other tenant", strings.ReplaceAll(fixtureQuery(`('a', 2, 'ALIVE', 'new')`, ""), fixtureTenant, "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"), "record_tenantid"},
		{"unknown status", fixtureQuery(`('a', 2, 'UNKNOWN', 'new')`, ""), "invalid CDC"},
		{"missing metadata", "SELECT 1 AS account_id", "missing CDC"},
		{"string version", strings.Replace(fixtureQuery(`('a', 2, 'ALIVE', 'new')`, ""), "version::UBIGINT", "version::VARCHAR", 1), "integer Parquet type"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := fixtureParquet(t, tc.query)
			key := "warehouse/parquet/2026/09/01/00/account/retry.parquet"
			_, _, err := s.importFile(context.Background(), fileTask{Key: key, Entity: "account"}, path, 0)
			if err == nil || !strings.Contains(err.Error(), tc.match) {
				t.Fatalf("got %v, want %s", err, tc.match)
			}
			if count(t, s, "SELECT count(*) FROM _twisp.imported_files") != 1 || count(t, s, "SELECT count(*) FROM account") != 1 {
				t.Fatal("failed import changed committed data")
			}
		})
	}
	// A failure must not leave a temp table or partial schema update behind.
	importFixture(t, s, "warehouse/parquet/2026/09/01/00/account/retry.parquet", first)
}

func TestDatabaseIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a space's #file.duckdb")
	s, err := openStore(context.Background(), path, "https://one.test", "Tenant1", "256MB")
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
	for _, identity := range [][2]string{{"https://one.test", "Tenant2"}, {"https://two.test", "Tenant1"}} {
		s, err := openStore(context.Background(), path, identity[0], identity[1], "256MB")
		if err == nil {
			s.Close()
			t.Fatal("accepted another tenant/endpoint")
		}
	}
}

func TestEmptyFileAndMultipleTenants(t *testing.T) {
	s := testStore(t)
	empty := fixtureParquet(t, fixtureQuery(`('a', 1, 'ALIVE', 'old')`, "")+" WHERE false")
	importFixture(t, s, "warehouse/parquet/2026/09/01/00/account/empty.parquet", empty)
	if count(t, s, "SELECT count(*) FROM account") != 0 {
		t.Fatal("empty file produced rows")
	}
	query := fixtureQuery(`('a', 1, 'ALIVE', 'old')`, "")
	mixed := fixtureParquet(t, query+" UNION ALL "+strings.ReplaceAll(query, fixtureTenant, "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"))
	_, _, err := s.importFile(context.Background(), fileTask{Key: "mixed", Entity: "account"}, mixed, 0)
	if err == nil || !strings.Contains(err.Error(), "more than one") {
		t.Fatalf("unexpected result: %v", err)
	}
}

func TestNativeParquetTypesAndDuckDBCLI(t *testing.T) {
	s := testStore(t)
	path := fixtureParquet(t, fixtureQuery(`('a', 1, 'ALIVE', 'name')`,
		`, '{"category":"cash"}'::JSON AS metadata, DATE '2026-09-01' AS effective,
		'123.000000000000000001 USD' AS amount, '\x00\xFF'::BLOB AS dimension`))
	importFixture(t, s, "warehouse/parquet/2026/09/01/00/account/types.parquet", path)
	if count(t, s, `SELECT count(*) FROM account WHERE metadata->>'category' = 'cash'
      AND effective = DATE '2026-09-01' AND amount = '123.000000000000000001 USD'
      AND hex(dimension) = '00FF'`) != 1 {
		t.Fatal("Parquet values changed")
	}
	cli, err := exec.LookPath("duckdb")
	if err != nil {
		t.Log("duckdb CLI not installed; skipping CLI interoperability check")
		return
	}
	var dbPath string
	if err := s.db.QueryRow("SELECT path FROM duckdb_databases() WHERE NOT internal").Scan(&dbPath); err != nil {
		t.Fatal(err)
	}
	if err := s.checkpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	os.Remove(path)
	out, err := exec.Command(cli, "-readonly", dbPath, "-csv", "-noheader", "-c", "SELECT name, amount FROM public.account").CombinedOutput()
	if err != nil {
		t.Fatalf("DuckDB CLI: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "123.000000000000000001 USD") {
		t.Fatalf("unexpected CLI output: %s", out)
	}
}
