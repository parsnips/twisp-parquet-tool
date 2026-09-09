package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

type fileDownloader interface {
	download(context.Context, string, *downloadLink, string) (string, int64, error)
}
type fileImporter interface {
	importBatch(context.Context, []downloadedFile) (totals, error)
}
type downloadedFile struct {
	Task     fileTask
	Path     string
	Size     int64
	QueuedAt time.Time
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

type batchOptions struct {
	Size, BufferFiles  int
	Bytes, BufferBytes int64
	Wait               time.Duration
}

func (o batchOptions) defaults() batchOptions {
	if o.Size == 0 {
		o.Size = 128
	}
	if o.BufferFiles == 0 {
		o.BufferFiles = 1024
	}
	if o.Bytes == 0 {
		o.Bytes = 64 << 20
	}
	if o.BufferBytes == 0 {
		o.BufferBytes = 256 << 20
	}
	if o.Wait == 0 {
		o.Wait = time.Second
	}
	return o
}

func newImportPipeline(ctx context.Context, api fileDownloader, store fileImporter, dir string, downloads, imports int, logger *log.Logger, options ...batchOptions) *importPipeline {
	opts := batchOptions{}.defaults()
	if len(options) > 0 {
		opts = options[0].defaults()
	}
	ctx, cancel := context.WithCancel(ctx)
	p := &importPipeline{ctx: ctx, cancel: cancel, jobs: make(chan fileTask, downloads), done: make(chan struct{})}
	ready := make(chan downloadedFile, downloads)
	work := make(chan []downloadedFile)
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
					case ready <- downloadedFile{Task: task, Path: path, Size: size, QueuedAt: time.Now()}:
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
			for files := range work {
				start := time.Now()
				result, err := store.importBatch(ctx, files)
				for _, f := range files {
					os.Remove(f.Path)
				}
				if err != nil {
					message := err.Error()
					for _, f := range files {
						message = strings.ReplaceAll(message, f.Path, f.Task.Key)
					}
					p.fail(fmt.Errorf("import %s batch: %s", files[0].Task.Entity, message))
					return
				}
				p.mu.Lock()
				p.total.Imported += result.Imported
				p.total.Skipped += result.Skipped
				p.total.Rows += result.Rows
				p.total.Bytes += result.Bytes
				if result.Imported > 0 {
					logger.Printf("imported %d %s files (%d rows, %.1f MiB) in %s; last key %s", result.Imported, files[0].Task.Entity, result.Rows, float64(result.Bytes)/(1024*1024), time.Since(start).Round(time.Millisecond), files[len(files)-1].Task.Key)
				}
				p.mu.Unlock()
				select {
				case completed <- files[0].Task.Entity:
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
		scheduleBatches(ctx, ready, work, completed, imports, opts)
	}()
	go func() {
		downloading.Wait()
		close(ready)
		<-scheduled
		for f := range ready {
			os.Remove(f.Path)
		}
		importing.Wait()
		close(p.done)
	}()
	return p
}

func scheduleBatches(ctx context.Context, ready <-chan downloadedFile, work chan<- []downloadedFile, completed <-chan string, imports int, opts batchOptions) {
	queues := map[string][]downloadedFile{}
	var order []string
	defer func() {
		for _, queue := range queues {
			for _, f := range queue {
				os.Remove(f.Path)
			}
		}
	}()
	busy := map[string]bool{}
	pending := 0
	var pendingBytes int64
	tick := time.NewTicker(min(opts.Wait, 100*time.Millisecond))
	defer tick.Stop()
	for ready != nil || pending > 0 || len(busy) > 0 {
		pressure := pending >= opts.BufferFiles || pendingBytes >= opts.BufferBytes
		var dispatch chan<- []downloadedFile
		var batch []downloadedFile
		selected := -1
		if len(busy) < imports {
			for i, entity := range order {
				queue := queues[entity]
				if busy[entity] || len(queue) == 0 {
					continue
				}
				n := 0
				var size int64
				for n < len(queue) && n < opts.Size {
					if n > 0 && size+queue[n].Size > opts.Bytes {
						break
					}
					size += queue[n].Size
					n++
					if size >= opts.Bytes {
						break
					}
				}
				full := n == opts.Size || size >= opts.Bytes || n < len(queue)
				if full || pressure || ready == nil || time.Since(queue[0].QueuedAt) >= opts.Wait {
					dispatch = work
					batch = queue[:n]
					selected = i
					break
				}
			}
		}
		receive := ready
		if pressure {
			receive = nil
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		case f, ok := <-receive:
			if !ok {
				ready = nil
				continue
			}
			if _, ok := queues[f.Task.Entity]; !ok {
				order = append(order, f.Task.Entity)
			}
			queues[f.Task.Entity] = append(queues[f.Task.Entity], f)
			pending++
			pendingBytes += f.Size
		case dispatch <- batch:
			entity := batch[0].Task.Entity
			busy[entity] = true
			// Copy the tail so later appends cannot overwrite a dispatched batch.
			queues[entity] = append([]downloadedFile(nil), queues[entity][len(batch):]...)
			pending -= len(batch)
			for _, f := range batch {
				pendingBytes -= f.Size
			}
			order = append(append(order[:selected], order[selected+1:]...), entity)
		case entity := <-completed:
			delete(busy, entity)
		}
	}
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
