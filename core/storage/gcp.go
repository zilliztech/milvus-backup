package storage

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/atomic"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

const _gcpEndpoint = "storage.googleapis.com"

const (
	_xAmzPrefix  = "X-Amz-"
	_xGoogPrefix = "X-Goog-"
)

// WrapHTTPTransport wraps http.Transport, add an auth header to support GCP native auth
type gcpHTTPTransport struct {
	tokenSrc     oauth2.TokenSource
	backend      http.RoundTripper
	currentToken atomic.Pointer[oauth2.Token]
}

// newWrapHTTPTransport returns a new gcpHTTPTransport
func newWrapHTTPTransport(secure bool) (*gcpHTTPTransport, error) {
	tokenSrc := google.ComputeTokenSource("")
	// in fact never return err
	backend, err := minio.DefaultTransport(secure)
	if err != nil {
		return nil, fmt.Errorf("storage: create gcp http transport %w", err)
	}
	return &gcpHTTPTransport{tokenSrc: tokenSrc, backend: backend}, nil
}

func newTokenHTTPTransport(secure bool, token string) (*gcpHTTPTransport, error) {
	tk := oauth2.Token{AccessToken: token}
	tokenSrc := oauth2.ReuseTokenSourceWithExpiry(&tk, nil, 0)
	// in fact never return err
	backend, err := minio.DefaultTransport(secure)
	if err != nil {
		return nil, fmt.Errorf("storage: create default transport %w", err)
	}
	return &gcpHTTPTransport{tokenSrc: tokenSrc, backend: backend}, nil
}

// RoundTrip wraps original http.RoundTripper by Adding a Bearer token acquired from tokenSrc
func (t *gcpHTTPTransport) RoundTrip(req *http.Request) (*http.Response, error) {
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

// NewGCPClient returns a minio.Client which is compatible for GCS
func NewGCPClient(cfg Config) (*MinioClient, error) {
	adderss := cfg.Endpoint
	if adderss == "" {
		adderss = _gcpEndpoint
	}
	opts := minio.Options{Secure: cfg.UseSSL}

	if cfg.UseIAM {
		trans, err := newWrapHTTPTransport(opts.Secure)
		if err != nil {
			return nil, fmt.Errorf("storage: create gcp http transport %w", err)
		}
		opts.Transport = trans
		opts.Creds = credentials.NewStaticV2("", "", "")
	} else {
		// region can not be empty
		opts.Region = "auto"
		if len(cfg.Token) != 0 {
			trans, err := newTokenHTTPTransport(cfg.UseSSL, cfg.Token)
			if err != nil {
				return nil, err
			}
			opts.Transport = trans
			opts.Creds = credentials.NewStaticV2("", "", "")
		} else {
			opts.Creds = credentials.NewStaticV2(cfg.AK, cfg.SK, "")
		}
	}

	cli, err := minio.New(adderss, &opts)
	if err != nil {
		return nil, fmt.Errorf("storage: create gcp client %w", err)
	}

	return &MinioClient{cfg: cfg, cli: cli}, nil
}
