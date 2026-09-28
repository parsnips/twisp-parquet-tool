package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

type iamAuth struct {
	mu      sync.Mutex
	token   string
	expires time.Time
	authURL string
	http    *http.Client
	presign func(context.Context) (string, error)
	clock   func() time.Time
}

func newIAMAuth(ctx context.Context, c config, client *http.Client) (*iamAuth, error) {
	options := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(c.AWSRegion)}
	if c.AWSProfile != "" {
		options = append(options, awsconfig.WithSharedConfigProfile(c.AWSProfile))
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, options...)
	if err != nil {
		return nil, errors.New("load AWS configuration: check -aws-profile and your AWS configuration")
	}
	authURL := "https://auth." + c.AWSRegion + ".cloud.twisp.com/"
	stsClient := sts.NewFromConfig(cfg, func(o *sts.Options) {
		o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
			return stack.Build.Add(middleware.BuildMiddlewareFunc("TwispIAMHeaders",
				func(ctx context.Context, in middleware.BuildInput, next middleware.BuildHandler) (middleware.BuildOutput, middleware.Metadata, error) {
					req, ok := in.Request.(*smithyhttp.Request)
					if !ok {
						return middleware.BuildOutput{}, middleware.Metadata{}, errors.New("unexpected STS request type")
					}
					req.Header.Set("x-twisp-aws-id", authURL)
					// Twisp requires this header even though the STS presigner omits it.
					req.Header.Set("X-Amz-Expires", "0")
					return next.HandleBuild(ctx, in)
				}), middleware.After)
		})
	})
	signer := sts.NewPresignClient(stsClient)
	return &iamAuth{
		authURL: authURL, http: client, clock: time.Now,
		presign: func(ctx context.Context) (string, error) {
			request, err := signer.PresignGetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
			if err != nil {
				return "", errors.New("sign AWS identity: check credentials; for SSO, run aws sso login --profile <profile>")
			}
			return request.URL, nil
		},
	}, nil
}

func (a *iamAuth) bearer(ctx context.Context) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if a.token != "" && a.clock().Add(time.Minute).Before(a.expires) {
		return a.token, nil
	}
	proof, err := a.presign(ctx)
	if err != nil {
		return "", err
	}
	body, err := json.Marshal(struct {
		Token      string
		Expiration time.Time
	}{"twisp-aws-v1." + base64.RawURLEncoding.EncodeToString([]byte(proof)), a.clock().Add(14 * time.Minute)})
	if err != nil {
		return "", errors.New("encode IAM token request")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.authURL+"token/iam", bytes.NewReader(body))
	if err != nil {
		return "", errors.New("invalid IAM authentication endpoint")
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := a.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("Twisp IAM token exchange: %w", transportError(err))
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("Twisp IAM token exchange HTTP %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, (64<<10)+1))
	if err != nil || len(data) > 64<<10 {
		return "", errors.New("cannot read Twisp IAM token response (maximum 64 KiB)")
	}
	token := strings.TrimSpace(string(data))
	parts := strings.Split(token, ".")
	if len(parts) != 3 || strings.ContainsAny(token, "\r\n\t ") {
		return "", errors.New("Twisp IAM returned an invalid JWT")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", errors.New("Twisp IAM returned an invalid JWT payload")
	}
	var claims struct {
		Expires int64 `json:"exp"`
	}
	// The HTTPS issuer supplies the token; exp is read only to schedule refresh.
	if err := json.Unmarshal(payload, &claims); err != nil || claims.Expires == 0 {
		return "", errors.New("Twisp IAM JWT has no valid expiration")
	}
	expires := time.Unix(claims.Expires, 0)
	if !a.clock().Add(time.Minute).Before(expires) {
		return "", errors.New("Twisp IAM JWT expires within one minute; check the system clock")
	}
	a.token, a.expires = token, expires
	return token, nil
}
