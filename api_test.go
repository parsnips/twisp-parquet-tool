package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func respond(t *testing.T, w http.ResponseWriter, data any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{"data": data}); err != nil {
		t.Error(err)
	}
}

func TestEndToEndPaginationDownloadRefreshAndOfflineResume(t *testing.T) {
	first := fixtureParquet(t, fixtureQuery(`('a', 1, 'ALIVE', 'old')`, ""))
	second := fixtureParquet(t, fixtureQuery(`('a', 1, 'EOL', 'old'), ('a', 2, 'ALIVE', 'new')`, ", 'extra' AS external_id"))
	files := map[string][]byte{}
	for key, path := range map[string]string{"one": first, "two": second, "three": first} {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		files[key] = b
	}
	var mu sync.Mutex
	var pages []string
	var downloads, refreshes int
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/graphql" {
			if r.Header.Get("Authorization") != "" || r.Header.Get("x-twisp-account-id") != "" {
				t.Error("Twisp credentials leaked to download request")
			}
			if r.Header.Get("x-signed-test") != "required" {
				t.Error("download omitted signed header")
			}
			mu.Lock()
			downloads++
			mu.Unlock()
			if r.URL.Path == "/expired" {
				w.WriteHeader(403)
				return
			}
			b, ok := files[strings.TrimPrefix(r.URL.Path, "/")]
			if !ok {
				http.NotFound(w, r)
				return
			}
			w.Write(b)
			return
		}
		if r.Method != "POST" || r.Header.Get("Authorization") != "Bearer secret" || r.Header.Get("x-twisp-account-id") != "Sandbox" {
			t.Error("wrong GraphQL authentication")
		}
		var request struct {
			Query     string
			Variables map[string]any
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		link := func(path string) any {
			return map[string]any{
				"downloadURL": server.URL + path + "?signature=private", "downloadURLExpiration": time.Now().Add(5 * time.Minute).UTC().Format(time.RFC3339),
				"downloadHeaders": map[string]any{"x-signed-test": "required"},
			}
		}
		if strings.Contains(request.Query, "createDownload") {
			mu.Lock()
			refreshes++
			mu.Unlock()
			if !strings.HasSuffix(request.Variables["key"].(string), "two.parquet") {
				t.Error("unnecessary refresh")
			}
			respond(t, w, map[string]any{"files": map[string]any{"createDownload": link("/two")}})
			return
		}
		if request.Variables["prefix"] != "warehouse" {
			t.Error("did not list all warehouse files")
		}
		token, _ := request.Variables["token"].(string)
		mu.Lock()
		pages = append(pages, token)
		mu.Unlock()
		base := "warehouse/parquet/2026/09/01/00/"
		keys := []any{}
		var next any
		switch token {
		case "":
			keys = append(keys, map[string]any{"key": base + "account/one.parquet", "download": link("/one")})
			next = "empty-page"
		case "empty-page":
			next = "page-two" // Empty pages can still have continuation tokens.
		case "page-two":
			keys = append(keys, map[string]any{"key": base + "account/two.parquet", "download": link("/expired")})
			keys = append(keys, map[string]any{"key": base + "entry/three.parquet", "download": link("/three")})
		default:
			t.Errorf("unexpected token %s", token)
		}
		respond(t, w, map[string]any{"files": map[string]any{"listPage": map[string]any{"keys": keys, "nextPageToken": next}}})
	}))
	defer server.Close()
	dir := t.TempDir()
	c := config{Token: "secret", Tenant: "Sandbox", Endpoint: server.URL + "/graphql", Database: filepath.Join(dir, "offline.duckdb"), Prefix: "warehouse", TempDir: dir, MemoryLimit: "256MB", PageSize: 1000, Workers: 2, Retries: 1, Timeout: 5 * time.Second}
	logger := log.New(io.Discard, "", 0)
	total, err := run(context.Background(), c, logger)
	if err != nil {
		t.Fatal(err)
	}
	if total.Imported != 3 || total.Rows != 4 {
		t.Fatalf("totals: %+v", total)
	}
	mu.Lock()
	if strings.Join(pages, ",") != ",empty-page,page-two" || downloads != 4 || refreshes != 1 {
		t.Errorf("pages=%v downloads=%d refreshes=%d", pages, downloads, refreshes)
	}
	mu.Unlock()
	total, err = run(context.Background(), c, logger)
	if err != nil {
		t.Fatal(err)
	}
	if total.Imported != 0 || total.Skipped != 3 {
		t.Fatalf("resume: %+v", total)
	}
	mu.Lock()
	if downloads != 4 {
		t.Error("resume downloaded existing files")
	}
	mu.Unlock()
	// All data and view definitions survive without either API or Parquet files.
	server.Close()
	os.Remove(first)
	os.Remove(second)
	s, err := openStore(context.Background(), c.Database, c.Endpoint, c.Tenant, c.MemoryLimit)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if count(t, s, "SELECT count(*) FROM account WHERE name='new' AND external_id='extra'") != 1 || count(t, s, "SELECT count(*) FROM account_history") != 2 || count(t, s, "SELECT count(*) FROM entry") != 1 {
		t.Fatal("offline views are incorrect")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "twisp-parquet-") {
			t.Errorf("temporary download directory was not removed: %s", entry.Name())
		}
	}
}

func TestAPIErrorHandling(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
		want       string
	}{
		{"unauthorized", `denied`, 401, "HTTP 401"},
		{"graphql", `{"data":{"files":null},"errors":[{"message":"denied secret"}]}`, 200, "denied [REDACTED]"},
		{"null data", `{"data":null}`, 200, "no data"},
		{"null listing", `{"data":{"files":{"listPage":null}}}`, 200, "no file listing"},
		{"invalid json", `<html>error</html>`, 200, "invalid JSON"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(tc.status); fmt.Fprint(w, tc.body) }))
			defer server.Close()
			a := newAPI(config{Endpoint: server.URL, Token: "secret", Tenant: "Sandbox", Timeout: time.Second})
			_, err := a.list(context.Background(), "warehouse", 1000, nil)
			if err == nil || !strings.Contains(err.Error(), tc.want) || strings.Contains(err.Error(), "secret") {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestTransientRetryAndCancellation(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			w.WriteHeader(429)
			return
		}
		respond(t, w, map[string]any{"files": map[string]any{"listPage": map[string]any{"keys": []any{}, "nextPageToken": nil}}})
	}))
	defer server.Close()
	a := newAPI(config{Endpoint: server.URL, Token: "secret", Tenant: "Sandbox", Timeout: time.Second, Retries: 2})
	a.backoff = time.Millisecond
	if _, err := a.list(context.Background(), "warehouse", 1000, nil); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("calls: %d", calls)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := a.pause(ctx, 0, "60"); err != context.Canceled {
		t.Fatalf("expected cancellation: %v", err)
	}
}

func TestFailureResumeAndRepeatedPageToken(t *testing.T) {
	path := fixtureParquet(t, fixtureQuery(`('a',1,'ALIVE','name')`, ""))
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	fail, repeat := true, false
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/parquet" {
			w.Write(b)
			return
		}
		var request struct{ Variables map[string]any }
		json.NewDecoder(r.Body).Decode(&request)
		token, _ := request.Variables["token"].(string)
		if token == "next" && fail {
			w.WriteHeader(503)
			return
		}
		keys := []any{}
		var next any
		if token == "" {
			keys = append(keys, map[string]any{"key": "warehouse/parquet/2026/09/01/00/account/one.parquet", "download": map[string]any{"downloadURL": server.URL + "/parquet", "downloadHeaders": map[string]string{}}})
			next = "next"
		} else if repeat {
			next = "next"
		}
		respond(t, w, map[string]any{"files": map[string]any{"listPage": map[string]any{"keys": keys, "nextPageToken": next}}})
	}))
	defer server.Close()
	c := config{Token: "secret", Tenant: "Sandbox", Endpoint: server.URL, Database: filepath.Join(t.TempDir(), "offline.duckdb"), Prefix: "warehouse", MemoryLimit: "256MB", PageSize: 1000, Workers: 1, Timeout: time.Second}
	logger := log.New(io.Discard, "", 0)
	total, err := run(context.Background(), c, logger)
	if err == nil || total.Imported > 1 {
		t.Fatalf("expected partial import and error: %+v %v", total, err)
	}
	committed := total.Imported // Listing can now fail while the previous page is still importing.
	fail = false
	total, err = run(context.Background(), c, logger)
	if err != nil || total.Skipped != committed || total.Imported != 1-committed {
		t.Fatalf("resume failed: %+v %v", total, err)
	}
	repeat = true
	_, err = run(context.Background(), c, logger)
	if err == nil || !strings.Contains(err.Error(), "repeated a page token") {
		t.Fatalf("expected repeat detection: %v", err)
	}
}
