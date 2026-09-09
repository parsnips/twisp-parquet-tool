package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
)

type fileDownloader interface {
	download(context.Context, string, *downloadLink, string) (string, int64, error)
}
type fileImporter interface {
	importFile(context.Context, fileTask, string, int64) (int64, bool, error)
}
type downloadedFile struct {
	Task fileTask
	Path string
	Size int64
}

// Listing, downloads, and entity imports overlap across page boundaries. Bounded
// channels apply backpressure so a fast network cannot fill disk without limit.
type importPipeline struct {
	ctx    context.Context
	cancel context.CancelFunc
	jobs   chan fileTask
	done   chan struct{}
	mu     sync.Mutex
	total  totals
	err    error
}

func newImportPipeline(ctx context.Context, api fileDownloader, store fileImporter, dir string, downloads, imports int, logger *log.Logger) *importPipeline {
	ctx, cancel := context.WithCancel(ctx)
	p := &importPipeline{ctx: ctx, cancel: cancel, jobs: make(chan fileTask, downloads), done: make(chan struct{})}
	ready := make(chan downloadedFile, downloads)
	work := make(chan downloadedFile)
	completed := make(chan string, imports)
	var downloading, importing sync.WaitGroup
	for i := 0; i < downloads; i++ {
		downloading.Go(func() {
			for {
				select {
				case <-ctx.Done():
					return
				case task, ok := <-p.jobs:
					if !ok {
						return
					}
					path, size, err := api.download(ctx, task.Key, task.Link, dir)
					if err != nil {
						p.fail(fmt.Errorf("download %s: %w", task.Key, err))
						return
					}
					select {
					case ready <- downloadedFile{Task: task, Path: path, Size: size}:
					case <-ctx.Done():
						os.Remove(path)
						return
					}
				}
			}
		})
	}
	for i := 0; i < imports; i++ {
		importing.Go(func() {
			for f := range work {
				rows, imported, err := store.importFile(ctx, f.Task, f.Path, f.Size)
				os.Remove(f.Path)
				if err != nil {
					p.fail(fmt.Errorf("import %s: %w", f.Task.Key, err))
					return
				}
				p.mu.Lock()
				if imported {
					p.total.Imported++
					p.total.Rows += rows
					p.total.Bytes += f.Size
					logger.Printf("imported %s (%d rows, %.1f MiB)", f.Task.Key, rows, float64(f.Size)/(1024*1024))
				} else {
					p.total.Skipped++
				}
				p.mu.Unlock()
				select {
				case completed <- f.Task.Entity:
				case <-ctx.Done():
					return
				}
			}
		})
	}
	scheduled := make(chan struct{})
	go func() {
		defer close(scheduled)
		defer close(work)
		pending := []downloadedFile{}
		defer func() {
			for _, f := range pending {
				os.Remove(f.Path)
			}
		}()
		busy := map[string]bool{}
		input := (<-chan downloadedFile)(ready)
		for input != nil || len(pending) > 0 || len(busy) > 0 {
			// Pick a ready entity instead of parking every importer behind one busy table.
			index := -1
			if len(busy) < imports {
				for i, f := range pending {
					if !busy[f.Task.Entity] {
						index = i
						break
					}
				}
			}
			var dispatch chan downloadedFile
			var next downloadedFile
			if index >= 0 {
				dispatch = work
				next = pending[index]
			}
			receive := input
			if len(pending) >= 2*imports {
				receive = nil
			}
			select {
			case <-ctx.Done():
				return
			case f, ok := <-receive:
				if !ok {
					input = nil
				} else {
					pending = append(pending, f)
				}
			case dispatch <- next:
				busy[next.Task.Entity] = true
				pending = append(pending[:index], pending[index+1:]...)
			case entity := <-completed:
				delete(busy, entity)
			}
		}
	}()
	go func() {
		downloading.Wait()
		close(ready)
		<-scheduled
		// On cancellation the scheduler may leave downloaded files in this channel.
		for f := range ready {
			os.Remove(f.Path)
		}
		importing.Wait()
		close(p.done)
	}()
	return p
}

func (p *importPipeline) fail(err error) {
	if err == nil {
		return
	}
	p.mu.Lock()
	if p.err == nil {
		p.err = err
		p.cancel()
	}
	p.mu.Unlock()
}

func (p *importPipeline) enqueue(task fileTask) error {
	select {
	case <-p.ctx.Done():
		return p.ctx.Err()
	case p.jobs <- task:
		return nil
	}
}

// The producer calls finish once, after it stops enqueueing. Errors cancel all
// stages; success drains outstanding work before closing the database.
func (p *importPipeline) finish(err error) (totals, error) {
	p.fail(err)
	close(p.jobs)
	<-p.done
	defer p.cancel()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err == nil {
		p.err = p.ctx.Err()
	}
	return p.total, p.err
}
