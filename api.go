package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type apiClient struct {
	endpoint, token, tenant string
	http                    *http.Client
	retries                 int
	backoff                 time.Duration
}

func newAPI(c config) *apiClient {
	return &apiClient{
		endpoint: c.Endpoint, token: c.Token, tenant: c.Tenant, retries: c.Retries, backoff: time.Second,
		http: &http.Client{Timeout: c.Timeout, CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse // Presigned URLs and API endpoints should be final URLs.
		}},
	}
}

func (a *apiClient) pause(ctx context.Context, attempt int, retryAfter string) error {
	delay := a.backoff * time.Duration(1<<min(attempt, 5))
	if n, err := strconv.Atoi(retryAfter); err == nil && n > 0 {
		delay = time.Duration(n) * time.Second
	} else if t, err := http.ParseTime(retryAfter); err == nil && time.Until(t) > 0 {
		delay = time.Until(t)
	}
	delay = min(delay, time.Minute)
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func transient(status int) bool { return status == 408 || status == 429 || status >= 500 }

// Strip the URL from transport errors: presigned query parameters are credentials.
func transportError(err error) error {
	var u *url.Error
	if errors.As(err, &u) {
		return u.Err
	}
	return err
}

func (a *apiClient) graphql(ctx context.Context, query string, variables any, out any) error {
	body, err := json.Marshal(map[string]any{"query": query, "variables": variables})
	if err != nil {
		return err
	}
	for attempt := 0; ; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.endpoint, bytes.NewReader(body))
		if err != nil {
			return transportError(err)
		}
		req.Header.Set("Authorization", "Bearer "+a.token)
		req.Header.Set("x-twisp-account-id", a.tenant)
		req.Header.Set("Content-Type", "application/json")
		resp, err := a.http.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if attempt >= a.retries {
				return fmt.Errorf("API request: %w", transportError(err))
			}
			if err := a.pause(ctx, attempt, ""); err != nil {
				return err
			}
			continue
		}
		data, readErr := io.ReadAll(io.LimitReader(resp.Body, (8<<20)+1))
		resp.Body.Close()
		if (transient(resp.StatusCode) || readErr != nil) && attempt < a.retries {
			if err := a.pause(ctx, attempt, resp.Header.Get("Retry-After")); err != nil {
				return err
			}
			continue
		}
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("API HTTP %d (check token, tenant, endpoint, and Files API permissions)", resp.StatusCode)
		}
		if readErr != nil {
			return fmt.Errorf("read API response: %w", transportError(readErr))
		}
		if len(data) > 8<<20 {
			return errors.New("API response exceeds 8 MiB")
		}
		var envelope struct {
			Data   json.RawMessage `json:"data"`
			Errors []struct {
				Message string `json:"message"`
			} `json:"errors"`
		}
		if err := json.Unmarshal(data, &envelope); err != nil {
			return errors.New("API returned invalid JSON")
		}
		if len(envelope.Errors) != 0 {
			messages := make([]string, len(envelope.Errors))
			for i, e := range envelope.Errors {
				messages[i] = strings.ReplaceAll(e.Message, a.token, "[REDACTED]")
			}
			return fmt.Errorf("GraphQL: %s", strings.Join(messages, "; "))
		}
		if len(envelope.Data) == 0 || string(envelope.Data) == "null" {
			return errors.New("GraphQL returned no data")
		}
		return json.Unmarshal(envelope.Data, out)
	}
}

type filePage struct {
	Keys []struct {
		Key      string        `json:"key"`
		Download *downloadLink `json:"download"`
	} `json:"keys"`
	NextPageToken *string `json:"nextPageToken"`
}

func (a *apiClient) list(ctx context.Context, prefix string, size int, token *string) (*filePage, error) {
	var data struct {
		Files *struct {
			Page *filePage `json:"listPage"`
		} `json:"files"`
	}
	err := a.graphql(ctx, `query ListParquetFiles($prefix: String!, $size: Int!, $token: String) {
  files { listPage(keyPrefix: $prefix, pageSize: $size, pageToken: $token) {
    keys { key download { downloadURL downloadURLExpiration downloadHeaders } } nextPageToken
  } }
}`, map[string]any{"prefix": prefix, "size": size, "token": token}, &data)
	if err != nil {
		return nil, err
	}
	if data.Files == nil || data.Files.Page == nil || data.Files.Page.Keys == nil {
		return nil, errors.New("GraphQL returned no file listing")
	}
	for _, entry := range data.Files.Page.Keys {
		if entry.Download != nil {
			entry.Download.ReceivedAt = time.Now()
		}
	}
	return data.Files.Page, nil
}

type downloadLink struct {
	URL        string                     `json:"downloadURL"`
	Headers    map[string]json.RawMessage `json:"downloadHeaders"`
	Expiration time.Time                  `json:"downloadURLExpiration"`
	ReceivedAt time.Time                  `json:"-"`
}

func (a *apiClient) link(ctx context.Context, key string) (*downloadLink, error) {
	var data struct {
		Files *struct {
			Download *downloadLink `json:"createDownload"`
		} `json:"files"`
	}
	err := a.graphql(ctx, `mutation DownloadParquetFile($key: String!) {
  files { createDownload(key: $key) { downloadURL downloadHeaders } }
}`, map[string]any{"key": key}, &data)
	if err != nil {
		return nil, err
	}
	if data.Files == nil || data.Files.Download == nil || data.Files.Download.URL == "" {
		return nil, errors.New("GraphQL returned no download URL")
	}
	return data.Files.Download, nil
}

func (a *apiClient) download(ctx context.Context, key string, initial *downloadLink, dir string) (string, int64, error) {
	for attempt := 0; ; attempt++ {
		link := initial
		if attempt > 0 || link == nil || link.URL == "" || time.Since(link.ReceivedAt) > 4*time.Minute ||
			(!link.Expiration.IsZero() && time.Until(link.Expiration) < 15*time.Second) {
			var err error
			link, err = a.link(ctx, key)
			if err != nil {
				return "", 0, err
			}
		}
		path, size, retry, after, err := a.fetch(ctx, link, dir)
		if err == nil {
			return path, size, nil
		}
		if ctx.Err() != nil {
			return "", 0, ctx.Err()
		}
		if !retry || attempt >= a.retries {
			return "", 0, err
		}
		if err := a.pause(ctx, attempt, after); err != nil {
			return "", 0, err
		}
	}
}

func (a *apiClient) fetch(ctx context.Context, link *downloadLink, dir string) (path string, size int64, retry bool, after string, err error) {
	u, parseErr := url.Parse(link.URL)
	if parseErr != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil {
		return "", 0, false, "", errors.New("invalid download URL")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, link.URL, nil)
	if err != nil {
		return "", 0, false, "", errors.New("invalid download request")
	}
	for k, raw := range link.Headers {
		var value string
		var values []string
		if json.Unmarshal(raw, &value) == nil {
			values = []string{value}
		} else if json.Unmarshal(raw, &values) != nil {
			return "", 0, false, "", fmt.Errorf("invalid download header %q", k)
		}
		for _, v := range values {
			if strings.EqualFold(k, "Host") {
				req.Host = v
			} else {
				req.Header.Add(k, v)
			}
		}
	}
	// Only the signed headers above go to object storage, never the Twisp bearer token.
	resp, err := a.http.Do(req)
	if err != nil {
		return "", 0, true, "", fmt.Errorf("download request: %w", transportError(err))
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", 0, transient(resp.StatusCode) || resp.StatusCode == 403, resp.Header.Get("Retry-After"), fmt.Errorf("download HTTP %d", resp.StatusCode)
	}
	f, err := os.CreateTemp(dir, "*.parquet")
	if err != nil {
		return "", 0, false, "", err
	}
	path = f.Name()
	defer func() {
		if err != nil {
			os.Remove(path)
		}
	}()
	size, err = io.Copy(f, resp.Body)
	closeErr := f.Close()
	if err != nil {
		return path, 0, true, "", fmt.Errorf("save download: %w", transportError(err))
	}
	if closeErr != nil {
		return path, 0, false, "", closeErr
	}
	if resp.ContentLength >= 0 && resp.ContentLength != size {
		return path, 0, true, "", errors.New("incomplete download")
	}
	return path, size, false, "", nil
}
