package main

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

func batchFixture(t *testing.T, key, query string) downloadedFile {
	t.Helper()
	path := fixtureParquet(t, query)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return downloadedFile{Task: fileTask{Key: key, Entity: "account"}, Path: path, Size: info.Size()}
}

func TestBatchMixedEncodingsSchemasEmptyAndResume(t *testing.T) {
	s := testStore(t)
	base := fixtureQuery(`('`+fixtureAccount+`',1,'ALIVE','old')`, "")
	files := []downloadedFile{
		batchFixture(t, "old", legacyQuery(base)),
		batchFixture(t, "transition", fixtureQuery(`('`+fixtureAccount+`',1,'EOL','old')`, "")),
		batchFixture(t, "new", fixtureQuery(`('`+fixtureAccount+`',2,'ALIVE','new')`, ", 'extra' AS external_id")),
		batchFixture(t, "empty", base+" WHERE false"),
	}
	result, err := s.importBatch(context.Background(), files)
	if err != nil || result.Imported != 4 || result.Rows != 3 {
		t.Fatalf("batch: %+v %v", result, err)
	}
	if count(t, s, "SELECT count(*) FROM account_history") != 2 || count(t, s, "SELECT count(*) FROM account WHERE name='new' AND external_id='extra' AND account_id='"+fixtureAccount+"'") != 1 {
		t.Fatal("incorrect mixed-schema views")
	}
	if count(t, s, "SELECT count(*) FROM _twisp.imported_files WHERE key='empty' AND row_count=0") != 1 {
		t.Fatal("empty file missing checkpoint")
	}
	if count(t, s, "SELECT sum(row_count) FROM _twisp.imported_files") != 3 {
		t.Fatal("incorrect per-file counts")
	}
	for i := range files {
		os.Remove(files[i].Path)
	}
	next := batchFixture(t, "next", fixtureQuery(`('`+fixtureAccount+`',3,'ALIVE','latest')`, ""))
	files = append(files, next, next)
	result, err = s.importBatch(context.Background(), files)
	if err != nil || result.Imported != 1 || result.Skipped != 5 || result.Rows != 1 {
		t.Fatalf("resume: %+v %v", result, err)
	}
	if count(t, s, "SELECT count(*) FROM root.parquet_account") != 4 {
		t.Fatal("duplicate rows after resume")
	}
}

func TestBatchRollbackPreservesCommittedSchemaAndCheckpoints(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(map[bool]string{false: "new entity", true: "existing entity"}[existing], func(t *testing.T) {
			s := testStore(t)
			base := fixtureQuery(`('a',1,'ALIVE','old')`, "")
			want := int64(0)
			if existing {
				f := batchFixture(t, "seed", base)
				if _, err := s.importBatch(context.Background(), []downloadedFile{f}); err != nil {
					t.Fatal(err)
				}
				want = 1
			}
			first := batchFixture(t, "one", fixtureQuery(`('b',1,'ALIVE','one')`, ", 'added' AS extra"))
			bad := batchFixture(t, "two", strings.ReplaceAll(fixtureQuery(`('c',1,'ALIVE','two')`, ""), fixtureTenant, "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"))
			if result, err := s.importBatch(context.Background(), []downloadedFile{first, bad}); err == nil || result.Imported != 0 {
				t.Fatalf("accepted invalid batch: %+v %v", result, err)
			}
			if count(t, s, "SELECT count(*) FROM _twisp.imported_files") != want {
				t.Fatal("partial checkpoint committed")
			}
			if count(t, s, "SELECT count(*) FROM information_schema.columns WHERE table_schema='root' AND table_name='parquet_account' AND column_name='extra'") != 0 {
				t.Fatal("failed batch leaked schema change")
			}
			if existing && count(t, s, "SELECT count(*) FROM account") != 1 {
				t.Fatal("failed batch changed existing views")
			}
			if !existing && s.tenantID != "" {
				t.Fatal("failed batch changed identity cache")
			}
			good := batchFixture(t, "two", base)
			if result, err := s.importBatch(context.Background(), []downloadedFile{first, good}); err != nil || result.Imported != 2 {
				t.Fatalf("retry: %+v %v", result, err)
			}
			if count(t, s, "SELECT count(*) FROM root.parquet_account") != want+2 || count(t, s, "SELECT count(*) FROM account WHERE extra='added'") != 1 {
				t.Fatal("retry lost rows/schema")
			}
		})
	}
}

func TestBatchRejectsImplicitTypeCoercion(t *testing.T) {
	s := testStore(t)
	first := batchFixture(t, "one", fixtureQuery(`('a',1,'ALIVE','a')`, ", 42::BIGINT AS value"))
	second := batchFixture(t, "two", fixtureQuery(`('b',1,'ALIVE','b')`, ", '42' AS value"))
	if _, err := s.importBatch(context.Background(), []downloadedFile{first, second}); err == nil || !strings.Contains(err.Error(), "schema type changed") {
		t.Fatalf("expected incompatible schema: %v", err)
	}
	if count(t, s, "SELECT count(*) FROM _twisp.imported_files") != 0 {
		t.Fatal("committed incompatible batch")
	}
}

func TestBatchSchedulerBoundsFlushAndSerialization(t *testing.T) {
	for _, tc := range []struct {
		name     string
		opts     batchOptions
		sizes    []int64
		maxFiles int
		maxBytes int64
	}{
		{"file limit", batchOptions{Size: 3, Wait: time.Hour}, []int64{1, 1, 1, 1, 1, 1, 1}, 3, 3},
		{"byte limit", batchOptions{Size: 10, Bytes: 5, Wait: time.Hour}, []int64{2, 2, 2, 8, 2}, 2, 5},
		{"buffer pressure", batchOptions{Size: 10, BufferFiles: 2, Wait: time.Hour}, []int64{1, 1, 1, 1, 1}, 2, 2},
		{"byte pressure", batchOptions{Size: 10, BufferBytes: 3, Wait: time.Hour}, []int64{2, 2, 2, 2, 2}, 2, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			ready := make(chan downloadedFile)
			work := make(chan []downloadedFile)
			done := make(chan string)
			stopped := make(chan struct{})
			go func() {
				defer close(stopped)
				defer close(work)
				scheduleBatches(ctx, ready, work, done, 2, tc.opts.defaults())
			}()
			go func() {
				defer close(ready)
				for i, size := range tc.sizes {
					select {
					case ready <- downloadedFile{Task: fileTask{Key: string(rune('a' + i)), Entity: "account"}, Size: size, QueuedAt: time.Now()}:
					case <-ctx.Done():
						return
					}
				}
			}()
			seen := map[string]bool{}
			for batch := range work {
				if len(batch) > tc.maxFiles {
					t.Errorf("batch length %d exceeds %d", len(batch), tc.maxFiles)
				}
				var size int64
				for _, f := range batch {
					size += f.Size
					if seen[f.Task.Key] {
						t.Error("duplicate dispatch")
					}
					seen[f.Task.Key] = true
				}
				if size > tc.maxBytes && len(batch) != 1 {
					t.Errorf("batch bytes %d exceeds %d", size, tc.maxBytes)
				}
				// An entity remains occupied until its transaction reports completion.
				select {
				case other := <-work:
					t.Fatalf("concurrent batch for same entity: %+v", other)
				case <-time.After(time.Millisecond):
				}
				select {
				case done <- "account":
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			<-stopped
			if len(seen) != len(tc.sizes) || ctx.Err() != nil {
				t.Fatalf("lost files or hung: %d %v", len(seen), ctx.Err())
			}
		})
	}
}

func TestBatchSchedulerFlushesPartialWhileListingOpen(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ready := make(chan downloadedFile, 1)
	ready <- downloadedFile{Task: fileTask{Entity: "account"}, QueuedAt: time.Now()}
	work := make(chan []downloadedFile)
	done := make(chan string)
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		scheduleBatches(ctx, ready, work, done, 1, batchOptions{Wait: 20 * time.Millisecond}.defaults())
	}()
	select {
	case batch := <-work:
		if len(batch) != 1 {
			t.Fatal("wrong partial batch")
		}
	case <-ctx.Done():
		t.Fatal("partial batch never flushed")
	}
	cancel()
	<-stopped
}
