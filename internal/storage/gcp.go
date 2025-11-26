package storage

import (
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

const _gcpEndpoint = "storage.googleapis.com"

const (
	_xAmzPrefix  = "X-Amz-"
	_xGoogPrefix = "X-Goog-"
)

// gcpTransport wraps http.Transport, add an auth header to support GCP native auth
type gcpTransport struct {
	tokenSrc     oauth2.TokenSource
	backend      http.RoundTripper
	currentToken atomic.Pointer[oauth2.Token]
}

// newGCPTrans returns a new GCPTransport
func newGCPTrans(secure bool) (*gcpTransport, error) {
	tokenSrc := google.ComputeTokenSource("")
	// in fact never return err
	backend, err := minio.DefaultTransport(secure)
	if err != nil {
		return nil, fmt.Errorf("storage: create gcp http transport %w", err)
	}
	return &gcpTransport{tokenSrc: tokenSrc, backend: backend}, nil
}

// newGCPTransWithToken returns a new GCPTransport with a fixed token
func newGCPTransWithToken(secure bool, token string) (*gcpTransport, error) {
	tk := oauth2.Token{AccessToken: token}
	tokenSrc := oauth2.ReuseTokenSourceWithExpiry(&tk, nil, 0)
	// in fact never return err
	backend, err := minio.DefaultTransport(secure)
	if err != nil {
		return nil, fmt.Errorf("storage: create default transport %w", err)
	}
	return &gcpTransport{tokenSrc: tokenSrc, backend: backend}, nil
}

// newGCPTransWithTokenSource returns a new GCPTransport with a token source
func newGCPTransWithTokenSrc(secure bool, tokenSrc oauth2.TokenSource) (*gcpTransport, error) {
	backend, err := minio.DefaultTransport(secure)
	if err != nil {
		return nil, fmt.Errorf("storage: create default transport %w", err)
	}
	return &gcpTransport{tokenSrc: tokenSrc, backend: backend}, nil
}

// RoundTrip wraps original http.RoundTripper by Adding a Bearer token acquired from tokenSrc
func (t *gcpTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	for k, v := range req.Header {
		if strings.HasPrefix(k, _xAmzPrefix) {
			req.Header[strings.Replace(k, _xAmzPrefix, _xGoogPrefix, 1)] = v
			delete(req.Header, k)
		}
	}

	// here Valid() means the token won't be expired in 10 sec
	// so the http client timeout shouldn't be longer, or we need to change the default `expiryDelta` time
	currentToken := t.currentToken.Load()
	if currentToken.Valid() {
		req.Header.Set("Authorization", "Bearer "+currentToken.AccessToken)
	} else {
		newToken, err := t.tokenSrc.Token()
		if err != nil {
			return nil, fmt.Errorf("storage: get gcp token %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+newToken.AccessToken)
		t.currentToken.Store(newToken)
	}

	return t.backend.RoundTrip(req)
}

// newGCPClient returns a minio.Client which is compatible for GCS
func newGCPClient(cfg Config) (*MinioClient, error) {
	if cfg.Endpoint == "" {
		cfg.Endpoint = _gcpEndpoint
	}

	opts := minio.Options{Secure: cfg.UseSSL}
	switch cfg.Credential.Type {
	case IAM:
		trans, err := newGCPTrans(opts.Secure)
		if err != nil {
			return nil, fmt.Errorf("storage: create gcp http transport %w", err)
		}
		opts.Transport = trans
		opts.Creds = credentials.NewStaticV2("", "", "")
	case Static:
		// region can not be empty
		opts.Region = "auto"
		if len(cfg.Credential.Token) != 0 {
			trans, err := newGCPTransWithToken(cfg.UseSSL, cfg.Credential.Token)
			if err != nil {
				return nil, err
			}
			opts.Transport = trans
			opts.Creds = credentials.NewStaticV2("", "", "")
		} else {
			opts.Creds = credentials.NewStaticV2(cfg.Credential.AK, cfg.Credential.SK, "")
		}
	case OAuth2TokenSource:
		opts.Region = "auto"
		trans, err := newGCPTransWithTokenSrc(cfg.UseSSL, cfg.Credential.OAuth2TokenSource)
		if err != nil {
			return nil, err
		}
		opts.Transport = trans
		opts.Creds = credentials.NewStaticV2("", "", "")
	default:
		return nil, fmt.Errorf("storage: gcp unsupported credential type %v", cfg.Credential.Type)
	}

	return newInternalMinio(cfg, &opts)
}
