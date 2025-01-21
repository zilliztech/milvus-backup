package client

import (
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type Cfg struct {
	Host      string // Remote address, "localhost:19530".
	EnableTLS bool   // Enable TLS for connection.
	DialOpts  []grpc.DialOption

	Username string // Username for auth.
	Password string // Password for auth.
}

func (cfg *Cfg) parseGrpcAuth() string {
	if cfg.Username != "" || cfg.Password != "" {
		value := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", cfg.Username, cfg.Password)))
		return value
	}

	return ""
}

func (cfg *Cfg) parseGrpc() (*url.URL, []grpc.DialOption, error) {
	remoteURL := &url.URL{Host: cfg.Host}
	// Remote Host should never be empty.
	if remoteURL.Host == "" {
		return nil, nil, errors.New("client: empty remote host of milvus address")
	}

	opts := cfg.DialOpts
	if cfg.EnableTLS {
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	if remoteURL.Port() == "" && cfg.EnableTLS {
		remoteURL.Host += ":443"
	}

	return remoteURL, opts, nil
}

func (cfg *Cfg) parseRestfulAuth() string {
	if cfg.Username != "" || cfg.Password != "" {
		return fmt.Sprintf("%s:%s", cfg.Username, cfg.Password)
	}

	return ""
}

func (cfg *Cfg) parseRestful() *url.URL {
	u := &url.URL{Host: cfg.Host}
	if cfg.EnableTLS {
		u.Scheme = "https"
	} else {
		u.Scheme = "http"
	}

	return u
}
