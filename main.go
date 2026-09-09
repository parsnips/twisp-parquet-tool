// twisp-parquet-tool downloads the tenant's warehouse CDC files into an offline DuckDB database.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"
)

const defaultEndpoint = "https://api.us-east-1.cloud.twisp.com/financial/v1/graphql"

type config struct {
	Token, Tenant, Endpoint, Database, Prefix, TempDir, MemoryLimit string
	PageSize, Workers, ImportWorkers, Retries                       int
	Timeout                                                         time.Duration
}

func parseConfig(args []string) (config, error) {
	var c config
	fs := flag.NewFlagSet("twisp-parquet-tool", flag.ContinueOnError)
	// Read the environment after parsing so -help never prints a token as a default.
	fs.StringVar(&c.Token, "token", "", "Bearer token (or TWISP_TOKEN)")
	fs.StringVar(&c.Tenant, "tenant", os.Getenv("TWISP_TENANT"), "Twisp account ID sent as x-twisp-account-id (or TWISP_TENANT)")
	endpoint := os.Getenv("TWISP_ENDPOINT")
	if endpoint == "" {
		endpoint = defaultEndpoint
	}
	fs.StringVar(&c.Endpoint, "endpoint", endpoint, "Twisp GraphQL endpoint (or TWISP_ENDPOINT)")
	fs.StringVar(&c.Database, "db", "twisp.duckdb", "Persistent DuckDB output file")
	fs.StringVar(&c.Prefix, "prefix", "warehouse", "Warehouse key prefix; default lists every available partition")
	fs.StringVar(&c.TempDir, "temp-dir", "", "Parent directory for temporary downloads (default OS temp directory)")
	fs.StringVar(&c.MemoryLimit, "memory-limit", "1GB", "DuckDB memory limit; larger queries can spill to disk")
	fs.IntVar(&c.PageSize, "page-size", 250, "Files per listing page (1–1000)")
	fs.IntVar(&c.Workers, "workers", 4, "Concurrent Parquet downloads")
	fs.IntVar(&c.ImportWorkers, "import-workers", 4, "Concurrent DuckDB imports across entity types (one per entity)")
	fs.IntVar(&c.Retries, "retries", 4, "Retries for transient API/download failures")
	fs.DurationVar(&c.Timeout, "timeout", 10*time.Minute, "Timeout for each HTTP request, including a download")
	if err := fs.Parse(args); err != nil {
		return c, err
	}
	if fs.NArg() != 0 {
		return c, errors.New("unexpected positional arguments; use -help")
	}
	if c.Token == "" {
		c.Token = os.Getenv("TWISP_TOKEN")
	}
	c.Token = strings.TrimSpace(c.Token)
	if len(c.Token) >= 7 && strings.EqualFold(c.Token[:7], "Bearer ") {
		c.Token = strings.TrimSpace(c.Token[7:])
	}
	c.Tenant = strings.TrimSpace(c.Tenant)
	if c.Token == "" || c.Tenant == "" {
		return c, errors.New("provide -token and -tenant, or TWISP_TOKEN and TWISP_TENANT")
	}
	u, err := url.Parse(c.Endpoint)
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return c, errors.New("endpoint must be an HTTP(S) URL without credentials, query parameters, or fragment")
	}
	c.Endpoint = u.String()
	if c.Database == "" || c.Database == ":memory:" {
		return c, errors.New("-db must be a persistent file path")
	}
	if (c.Prefix != "warehouse" && c.Prefix != "warehouse/" && c.Prefix != "warehouse/parquet" && !strings.HasPrefix(c.Prefix, "warehouse/parquet/")) || strings.Contains(c.Prefix, "..") || strings.Contains(c.Prefix, "\\") {
		return c, errors.New("-prefix must be warehouse or a warehouse/parquet/ prefix without parent paths")
	}
	if c.PageSize < 1 || c.PageSize > 1000 || c.Workers < 1 || c.Workers > 64 || c.ImportWorkers < 1 || c.ImportWorkers > 64 || c.Retries < 0 || c.Retries > 10 || c.Timeout <= 0 {
		return c, errors.New("require page-size 1–1000, workers and import-workers 1–64, retries 0–10, and a positive timeout")
	}
	return c, nil
}

var entityName = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

func entityForKey(key string) (string, error) {
	p := strings.Split(key, "/")
	if len(p) != 8 || p[0] != "warehouse" || p[1] != "parquet" || !entityName.MatchString(p[6]) ||
		!strings.HasSuffix(p[7], ".parquet") || strings.Contains(key, "..") || strings.Contains(key, "\\") {
		return "", fmt.Errorf("unexpected warehouse Parquet key %q", key)
	}
	if _, err := time.Parse("2006/01/02/15", strings.Join(p[2:6], "/")); err != nil {
		return "", fmt.Errorf("invalid UTC partition in key %q", key)
	}
	return p[6], nil
}

type totals struct{ Imported, Skipped, Rows, Bytes int64 }

func run(ctx context.Context, c config, logger *log.Logger) (totals, error) {
	var total totals
	dbPath, err := filepath.Abs(c.Database)
	if err != nil {
		return total, err
	}
	if err := os.MkdirAll(filepath.Dir(dbPath), 0700); err != nil {
		return total, err
	}
	s, err := openStore(ctx, dbPath, c.Endpoint, c.Tenant, c.MemoryLimit)
	if err != nil {
		return total, err
	}
	defer s.Close()
	if s.migratedTables > 0 {
		logger.Printf("normalized legacy UUID encodings in %d existing tables; import checkpoints preserved", s.migratedTables)
	}
	imports := c.ImportWorkers
	if imports == 0 {
		imports = 4
	}
	// Reserve one connection for listing/checkpoint lookups while imports run.
	s.db.SetMaxOpenConns(imports + 1)
	s.db.SetMaxIdleConns(imports + 1)
	dir, err := os.MkdirTemp(c.TempDir, "twisp-parquet-")
	if err != nil {
		return total, err
	}
	defer os.RemoveAll(dir)
	api := newAPI(c)
	defer api.http.CloseIdleConnections()
	pipeline := newImportPipeline(ctx, api, s, dir, c.Workers, imports, logger)
	logger.Printf("pipeline: %d downloads, up to %d concurrent entity imports", c.Workers, imports)
	var skipped int64
	listErr := func() error {
		var pageToken *string
		seen := map[string]bool{}
		for page := 1; ; page++ {
			listing, err := api.list(pipeline.ctx, c.Prefix, c.PageSize, pageToken)
			if err != nil {
				return fmt.Errorf("list page %d: %w", page, err)
			}
			logger.Printf("page %d: %d keys", page, len(listing.Keys))
			for _, entry := range listing.Keys {
				if !strings.HasPrefix(entry.Key, c.Prefix) {
					return fmt.Errorf("API returned key outside requested prefix: %q", entry.Key)
				}
				entity, err := entityForKey(entry.Key)
				if err != nil {
					return err
				}
				loaded, err := s.hasFile(pipeline.ctx, entry.Key)
				if err != nil {
					return err
				}
				if loaded {
					skipped++
					continue
				}
				if err := pipeline.enqueue(fileTask{Key: entry.Key, Entity: entity, Link: entry.Download}); err != nil {
					return err
				}
			}
			pageToken = listing.NextPageToken
			if pageToken == nil || *pageToken == "" {
				break
			}
			if seen[*pageToken] {
				return errors.New("API repeated a page token; stopping to avoid an infinite listing")
			}
			seen[*pageToken] = true
		}
		return nil
	}()
	total, err = pipeline.finish(listErr)
	total.Skipped += skipped
	if err != nil {
		return total, err
	}
	if err := s.checkpoint(ctx); err != nil {
		return total, err
	}
	if total.Imported+total.Skipped == 0 {
		logger.Print("No Parquet files found. Check the endpoint, tenant, prefix, and whether the warehouse pipeline is enabled and seeded.")
	}
	logger.Printf("complete: imported %d files / %d rows / %.1f MiB; skipped %d previously imported files; database %s",
		total.Imported, total.Rows, float64(total.Bytes)/(1024*1024), total.Skipped, dbPath)
	return total, nil
}

type fileTask struct {
	Key, Entity string
	Link        *downloadLink
}

func main() {
	logger := log.New(os.Stderr, "", log.LstdFlags)
	c, err := parseConfig(os.Args[1:])
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err == nil {
		ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer cancel()
		_, err = run(ctx, c, logger)
	}
	if err != nil {
		// Never emit a token even if an upstream error happens to echo it.
		message := err.Error()
		if c.Token != "" {
			message = strings.ReplaceAll(message, c.Token, "[REDACTED]")
		}
		fmt.Fprintln(os.Stderr, "error:", message)
		os.Exit(1)
	}
}
