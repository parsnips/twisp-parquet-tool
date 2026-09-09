package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"testing"
	"time"
)

type pipelineFixture struct {
	mu               sync.Mutex
	active           map[string]int
	max              int
	overlappedEntity bool
	started          chan string
	release          chan struct{}
	failKey          string
}

func (f *pipelineFixture) download(ctx context.Context, key string, link *downloadLink, dir string) (string, int64, error) {
	file, err := os.CreateTemp(dir, "*.parquet")
	if err != nil {
		return "", 0, err
	}
	file.Close()
	return file.Name(), 1, nil
}
func (f *pipelineFixture) importFile(ctx context.Context, task fileTask, path string, size int64) (int64, bool, error) {
	f.mu.Lock()
	f.active[task.Entity]++
	if f.active[task.Entity] > 1 {
		f.overlappedEntity = true
	}
	active := 0
	for _, n := range f.active {
		active += n
	}
	f.max = max(f.max, active)
	f.mu.Unlock()
	defer func() { f.mu.Lock(); f.active[task.Entity]--; f.mu.Unlock() }()
	f.started <- task.Entity
	if task.Key == f.failKey {
		return 0, false, errors.New("fixture import failed")
	}
	select {
	case <-ctx.Done():
		return 0, false, ctx.Err()
	case <-f.release:
		return 1, true, nil
	}
}

func (f *pipelineFixture) importBatch(ctx context.Context, files []downloadedFile) (totals, error) {
	var result totals
	for _, file := range files {
		rows, imported, err := f.importFile(ctx, file.Task, file.Path, file.Size)
		if err != nil {
			return totals{}, err
		}
		if imported {
			result.Imported++
			result.Rows += rows
			result.Bytes += file.Size
		} else {
			result.Skipped++
		}
	}
	return result, nil
}

func TestPipelineImportsDifferentEntitiesConcurrently(t *testing.T) {
	dir := t.TempDir()
	f := &pipelineFixture{active: map[string]int{}, started: make(chan string, 4), release: make(chan struct{})}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	p := newImportPipeline(ctx, f, f, dir, 1, 2, log.New(io.Discard, "", 0))
	for _, task := range []fileTask{{Key: "a1", Entity: "account"}, {Key: "a2", Entity: "account"}, {Key: "e1", Entity: "entry"}} {
		if err := p.enqueue(task); err != nil {
			t.Fatal(err)
		}
	}
	seen := map[string]bool{}
	for len(seen) < 2 {
		select {
		case entity := <-f.started:
			seen[entity] = true
		case <-ctx.Done():
			p.finish(ctx.Err())
			t.Fatal("different entity imports did not overlap")
		}
	}
	close(f.release)
	total, err := p.finish(nil)
	if err != nil || total.Imported != 3 {
		t.Fatalf("pipeline: %+v %v", total, err)
	}
	if f.max != 2 || f.overlappedEntity {
		t.Fatalf("max=%d same-entity overlap=%v", f.max, f.overlappedEntity)
	}
	files, err := os.ReadDir(dir)
	if err != nil || len(files) != 0 {
		t.Fatalf("download cleanup: %v %v", files, err)
	}
}

func TestPipelineFailureCancelsOtherStages(t *testing.T) {
	dir := t.TempDir()
	f := &pipelineFixture{active: map[string]int{}, started: make(chan string, 4), release: make(chan struct{}), failKey: "bad"}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	p := newImportPipeline(ctx, f, f, dir, 2, 2, log.New(io.Discard, "", 0))
	for _, task := range []fileTask{{Key: "blocked", Entity: "account"}, {Key: "bad", Entity: "entry"}} {
		if err := p.enqueue(task); err != nil {
			t.Fatal(err)
		}
	}
	_, err := p.finish(nil)
	if err == nil || ctx.Err() != nil {
		t.Fatalf("pipeline did not promptly cancel: %v", err)
	}
	files, err := os.ReadDir(dir)
	if err != nil || len(files) != 0 {
		t.Fatalf("failed pipeline download cleanup: %v %v", files, err)
	}
}

func TestConcurrentStoreImportsPreserveCheckpointsAndViews(t *testing.T) {
	s := testStore(t)
	path := fixtureParquet(t, fixtureQuery(`('a',1,'ALIVE','name')`, ""))
	entities := []string{"account", "entry", "balance", "transaction"}
	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for _, entity := range entities {
		for i := 0; i < 8; i++ {
			wg.Go(func() {
				key := fmt.Sprintf("warehouse/parquet/2026/09/01/00/%s/%d.parquet", entity, i)
				_, _, err := s.importFile(context.Background(), fileTask{Key: key, Entity: entity}, path, 1)
				errs <- err
			})
		}
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if count(t, s, "SELECT count(*) FROM _twisp.imported_files") != 32 {
		t.Fatal("concurrent checkpoints were lost")
	}
	for _, entity := range entities {
		if count(t, s, "SELECT count(*) FROM "+ident(entity)) != 1 || count(t, s, "SELECT count(*) FROM root."+ident("parquet_"+entity)) != 8 {
			t.Fatalf("incorrect %s views/data", entity)
		}
	}
}
