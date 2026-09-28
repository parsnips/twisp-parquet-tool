package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testJWT(expires time.Time) string {
	return "eyJhbGciOiJSUzI1NiJ9." + base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf(`{"exp":%d}`, expires.Unix()))) + ".signature"
}

func TestIAMCacheConcurrentRefreshAndProof(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != http.MethodPost || r.URL.Path != "/token/iam" || r.Header.Get("Content-Type") != "application/json" {
			t.Error("incorrect token exchange request")
		}
		var body struct {
			Token      string
			Expiration time.Time
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
			return
		}
		proof, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(body.Token, "twisp-aws-v1."))
		if err != nil || string(proof) != "https://sts.example.test/?signed=proof" || !strings.HasPrefix(body.Token, "twisp-aws-v1.") {
			t.Error("incorrect signed identity proof")
		}
		if !body.Expiration.Equal(now.Add(14 * time.Minute)) {
			t.Error("incorrect proof expiration")
		}
		fmt.Fprint(w, testJWT(now.Add(5*time.Minute)))
	}))
	defer server.Close()
	a := &iamAuth{authURL: server.URL + "/", http: server.Client(), clock: func() time.Time { return now }, presign: func(context.Context) (string, error) { return "https://sts.example.test/?signed=proof", nil }}
	first, err := a.bearer(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		var wg sync.WaitGroup
		for range 16 {
			wg.Go(func() {
				if _, err := a.bearer(context.Background()); err != nil {
					t.Error(err)
				}
			})
		}
		wg.Wait()
		if calls.Load() != 1 {
			t.Fatalf("fresh token exchanged %d times", calls.Load())
		}
	}
	now = now.Add(4 * time.Minute)
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			token, err := a.bearer(context.Background())
			if err != nil || token == first {
				t.Errorf("refresh failed: %v", err)
			}
		})
	}
	wg.Wait()
	if calls.Load() != 2 {
		t.Fatalf("concurrent refresh made %d exchanges", calls.Load())
	}
}

func TestIAMRejectsBadResponsesWithoutLeakingTokens(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
	}{
		{"denied", "private-signed-proof", http.StatusForbidden},
		{"invalid", "private-token", http.StatusOK},
		{"missing expiry", "e30.e30.private-signature", http.StatusOK},
		{"expired", testJWT(time.Now().Add(-time.Hour)), http.StatusOK},
		{"oversized", strings.Repeat("s", 65537), http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(tc.status); fmt.Fprint(w, tc.body) }))
			defer server.Close()
			a := &iamAuth{authURL: server.URL + "/", http: server.Client(), clock: time.Now, presign: func(context.Context) (string, error) { return "private-signed-proof", nil }}
			_, err := a.bearer(context.Background())
			if err == nil || strings.Contains(err.Error(), tc.body) || strings.Contains(err.Error(), "private-") {
				t.Fatalf("unsafe or missing error: %v", err)
			}
			if a.token != "" {
				t.Fatal("invalid token was cached")
			}
		})
	}
}

func TestIAMCancellationAndRedirect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	a := &iamAuth{}
	if _, err := a.bearer(ctx); err != context.Canceled {
		t.Fatalf("cancellation: %v", err)
	}
	var leaked atomic.Bool
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { leaked.Store(true) }))
	defer target.Close()
	issuer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer issuer.Close()
	api := newAPI(config{Timeout: time.Second})
	defer api.http.CloseIdleConnections()
	a = &iamAuth{authURL: issuer.URL + "/", http: api.http, clock: time.Now, presign: func(context.Context) (string, error) { return "private-signed-proof", nil }}
	if _, err := a.bearer(context.Background()); err == nil {
		t.Fatal("accepted redirect")
	}
	if leaked.Load() {
		t.Fatal("signed identity proof forwarded to redirect")
	}
}

func TestIAMSignsSelectedProfileAndRegion(t *testing.T) {
	dir := t.TempDir()
	credentials := filepath.Join(dir, "credentials")
	if err := os.WriteFile(credentials, []byte("[test-profile]\naws_access_key_id = TEST_PROFILE_KEY\naws_secret_access_key = test-secret\naws_session_token = test-session\n"), 0600); err != nil {
		t.Fatal(err)
	}
	for k, v := range map[string]string{"AWS_CONFIG_FILE": filepath.Join(dir, "config"), "AWS_SHARED_CREDENTIALS_FILE": credentials, "AWS_ACCESS_KEY_ID": "", "AWS_SECRET_ACCESS_KEY": "", "AWS_SESSION_TOKEN": "", "AWS_PROFILE": "", "AWS_ENDPOINT_URL": "", "AWS_ENDPOINT_URL_STS": "", "AWS_EC2_METADATA_DISABLED": "true"} {
		t.Setenv(k, v)
	}
	a, err := newIAMAuth(context.Background(), config{AWSProfile: "test-profile", AWSRegion: "us-west-2"}, http.DefaultClient)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := a.presign(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(proof)
	if err != nil {
		t.Fatal(err)
	}
	if u.Host != "sts.us-west-2.amazonaws.com" || !strings.HasPrefix(u.Query().Get("X-Amz-Credential"), "TEST_PROFILE_KEY/") || u.Query().Get("X-Amz-Security-Token") != "test-session" {
		t.Fatal("wrong profile or STS region")
	}
	if !strings.Contains(u.Query().Get("X-Amz-SignedHeaders"), "x-twisp-aws-id") || u.Query().Get("X-Amz-Expires") != "0" {
		t.Error("missing Twisp identity binding or expiration in signed request")
	}
	if a.authURL != "https://auth.us-west-2.cloud.twisp.com/" {
		t.Fatal("incorrect Twisp auth region")
	}
}

func TestAWSIAMConfig(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		args                  []string
		envToken, envEndpoint string
		valid                 bool
		endpoint              string
	}{
		{name: "token", args: []string{"-token", "secret", "-tenant", "Sandbox"}, valid: true, endpoint: defaultEndpoint},
		{name: "iam", args: []string{"-auth", "aws-iam", "-aws-profile", "sandbox", "-aws-region", "us-west-2", "-tenant", "Sandbox"}, valid: true, endpoint: "https://api.us-west-2.cloud.twisp.com/financial/v1/graphql"},
		{name: "missing region", args: []string{"-auth", "aws-iam", "-tenant", "Sandbox"}},
		{name: "invalid region", args: []string{"-auth", "aws-iam", "-aws-region", "us-east-1.evil.test/", "-tenant", "Sandbox"}},
		{name: "token conflict", args: []string{"-auth", "aws-iam", "-aws-region", "us-west-2", "-tenant", "Sandbox"}, envToken: "secret"},
		{name: "region mismatch", args: []string{"-auth", "aws-iam", "-aws-region", "us-west-2", "-tenant", "Sandbox"}, envEndpoint: defaultEndpoint},
		{name: "explicit mismatch", args: []string{"-auth", "aws-iam", "-aws-region", "us-west-2", "-tenant", "Sandbox", "-endpoint", defaultEndpoint}},
		{name: "unknown auth", args: []string{"-auth", "invalid", "-tenant", "Sandbox"}},
		{name: "unused aws settings", args: []string{"-token", "secret", "-tenant", "Sandbox", "-aws-profile", "sandbox"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("TWISP_TOKEN", tc.envToken)
			t.Setenv("TWISP_ENDPOINT", tc.envEndpoint)
			t.Setenv("TWISP_TENANT", "")
			c, err := parseConfig(tc.args)
			if (err == nil) != tc.valid {
				t.Fatalf("config error: %v", err)
			}
			if tc.valid && c.Endpoint != tc.endpoint {
				t.Fatalf("endpoint %s", c.Endpoint)
			}
		})
	}
}

func TestAPIUsesRefreshedTokenAndRedactsIt(t *testing.T) {
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		want := fmt.Sprintf("temporary-token-%d", calls)
		if r.Header.Get("Authorization") != "Bearer "+want {
			t.Error("stale API token")
		}
		fmt.Fprintf(w, `{"errors":[{"message":"denied %s"}]}`, want)
	}))
	defer server.Close()
	a := newAPI(config{Endpoint: server.URL, Timeout: time.Second})
	a.tokenFunc = func(context.Context) (string, error) { return fmt.Sprintf("temporary-token-%d", calls+1), nil }
	for range 2 {
		_, err := a.list(context.Background(), "warehouse", 1, nil)
		if err == nil || strings.Contains(err.Error(), "temporary-token") || !strings.Contains(err.Error(), "[REDACTED]") {
			t.Fatalf("unsafe or missing error: %v", err)
		}
	}
}
