package main

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

const fixtureAccount = "01234567-89ab-cdef-0123-456789abcdef"

func legacyQuery(query string) string {
	query = strings.ReplaceAll(query, "rowid AS record_rowid", "from_hex(replace(rowid, '-', '')) AS record_rowid")
	query = strings.ReplaceAll(query, "rowid AS account_id", "from_hex(replace(rowid, '-', '')) AS account_id")
	return strings.ReplaceAll(query, literal(fixtureTenant)+" AS record_tenantid", "from_hex("+literal(strings.ReplaceAll(fixtureTenant, "-", ""))+") AS record_tenantid")
}

func TestInlineUUIDNormalization(t *testing.T) {
	s := testStore(t)
	old := fixtureParquet(t, legacyQuery(fixtureQuery(`('`+fixtureAccount+`',1,'ALIVE','old')`, ", from_hex('00112233445566778899aabbccddeeff') AS dimension, ''::BLOB AS void_of")))
	modern := fixtureParquet(t, fixtureQuery(`('`+fixtureAccount+`',1,'EOL','old'),('`+fixtureAccount+`',2,'ALIVE','new')`, ", from_hex('00112233445566778899aabbccddeeff') AS dimension"))
	importFixture(t, s, "warehouse/parquet/2024/02/02/18/account/old.parquet", old)
	importFixture(t, s, "warehouse/parquet/2024/02/02/20/account/new.parquet", modern)
	if count(t, s, "SELECT count(*) FROM account_history") != 2 || count(t, s, "SELECT count(*) FROM account WHERE name='new'") != 1 {
		t.Fatal("binary/text UUIDs split the same record")
	}
	if count(t, s, "SELECT count(*) FROM root.parquet_account WHERE void_of = ''") != 1 {
		t.Fatal("empty optional UUID changed")
	}
	if count(t, s, `SELECT count(*) FROM root.parquet_account WHERE account_id = '`+fixtureAccount+`' AND record_tenantid = '`+fixtureTenant+`' AND typeof(dimension) = 'BLOB' AND hex(dimension) = '00112233445566778899AABBCCDDEEFF'`) != 3 {
		t.Fatal("UUID normalization changed identifiers or non-UUID binary data")
	}
	// A real tenant mismatch must still be rejected after normalization.
	other := fixtureParquet(t, strings.ReplaceAll(legacyQuery(fixtureQuery(`('`+fixtureAccount+`',3,'ALIVE','wrong tenant')`, "")), strings.ReplaceAll(fixtureTenant, "-", ""), "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"))
	if _, _, err := s.importFile(context.Background(), fileTask{Key: "wrong-tenant", Entity: "account"}, other, 0); err == nil {
		t.Fatal("accepted another tenant")
	}
}

func TestExistingDatabaseUUIDMigrationPreservesResume(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "legacy.duckdb")
	s, err := openStore(ctx, path, "https://example.test/graphql", "Sandbox", "256MB")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	old := fixtureParquet(t, legacyQuery(fixtureQuery(`('`+fixtureAccount+`',1,'ALIVE','old')`, "")))
	key := "warehouse/parquet/2024/02/02/18/account/old.parquet"
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{
		"CREATE TABLE root.parquet_account AS SELECT * FROM read_parquet(" + literal(old) + ")",
		"UPDATE _twisp.settings SET value='1' WHERE name='format_version'",
		"INSERT INTO _twisp.settings SELECT 'record_tenantid',CAST(record_tenantid AS VARCHAR) FROM root.parquet_account LIMIT 1",
		"INSERT INTO _twisp.imported_files(key,entity,row_count,byte_count) VALUES (" + literal(key) + ",'account',1,100)",
	} {
		if _, err := tx.Exec(sql); err != nil {
			tx.Rollback()
			t.Fatal(err)
		}
	}
	if err := createViews(ctx, tx, "account"); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err = openStore(ctx, path, "https://example.test/graphql", "Sandbox", "256MB")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.tenantID != fixtureTenant || s.migratedTables != 1 {
		t.Fatalf("migration identity=%s tables=%d", s.tenantID, s.migratedTables)
	}
	if loaded, err := s.hasFile(ctx, key); err != nil || !loaded {
		t.Fatal("lost import checkpoint")
	}
	if count(t, s, "SELECT count(*) FROM account WHERE account_id='"+fixtureAccount+"'") != 1 {
		t.Fatal("lost existing record")
	}
	newFile := fixtureParquet(t, fixtureQuery(`('`+fixtureAccount+`',2,'ALIVE','new')`, ""))
	importFixture(t, s, "warehouse/parquet/2024/02/02/20/account/new.parquet", newFile)
	if count(t, s, "SELECT count(*) FROM account_history") != 2 || count(t, s, "SELECT count(*) FROM _twisp.imported_files") != 2 {
		t.Fatal("resume after migration failed")
	}
}
