package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// Local ingestion only: 256 distinct five-row files, including durable checkpoints.
// Downloads and listing are deliberately excluded so this isolates database overhead.
func BenchmarkSmallFiles(b *testing.B)        { benchmarkImport(b, 1) }
func BenchmarkBatchedSmallFiles(b *testing.B) { benchmarkImport(b, 128) }

func benchmarkImport(b *testing.B, batchSize int) {
	files := benchmarkFiles(b)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		s, err := openStore(context.Background(), filepath.Join(b.TempDir(), "benchmark.duckdb"), "https://example.test", "Sandbox", "256MB")
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		for i := 0; i < len(files); i += batchSize {
			if _, err := s.importBatch(context.Background(), files[i:min(i+batchSize, len(files))]); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		s.Close()
		b.StartTimer()
	}
	b.ReportMetric(float64(len(files)*b.N)/b.Elapsed().Seconds(), "files/s")
}

func benchmarkFiles(b *testing.B) []downloadedFile {
	b.Helper()
	path := fixtureParquet(b, fixtureQuery(`('a',1,'ALIVE','one'),('b',1,'ALIVE','two'),('c',1,'ALIVE','three'),('d',1,'ALIVE','four'),('e',1,'ALIVE','five')`,
		`, 'ACTIVE' AS status, '1000' AS code, 'DEBIT' AS normal_balance_type,
        'Account description' AS description, '{"category":"cash"}'::JSON AS metadata,
        TIMESTAMP '2026-09-01' AS created, TIMESTAMP '2026-09-01' AS modified,
        'reference' AS external_id, false AS config_is_account_set`))
	data, err := os.ReadFile(path)
	if err != nil {
		b.Fatal(err)
	}
	dir := b.TempDir()
	files := make([]downloadedFile, 256)
	for i := range files {
		path := filepath.Join(dir, fmt.Sprintf("%04d.parquet", i))
		if err := os.WriteFile(path, data, 0600); err != nil {
			b.Fatal(err)
		}
		files[i] = downloadedFile{Task: fileTask{Key: fmt.Sprintf("warehouse/parquet/2026/09/01/00/account/%04d.parquet", i), Entity: "account"}, Path: path, Size: int64(len(data))}
	}
	return files
}
