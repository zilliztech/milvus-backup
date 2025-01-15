package client

import (
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"regexp"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

var _httpsScheme = regexp.MustCompile(`^https?://`)
var _httpScheme = regexp.MustCompile(`^http?://`)

type Cfg struct {
	Address   string // Remote address, "localhost:19530".
	EnableTLS bool   // Enable TLS for connection.
	DialOpts  []grpc.DialOption

	Username string // Username for auth.
	Password string // Password for auth.
}

func (cfg *Cfg) parseAuth() string {
	if cfg.Username != "" || cfg.Password != "" {
		value := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", cfg.Username, cfg.Password)))
		return value
	}

	return ""
}

func (cfg *Cfg) parseGrpc() (*url.URL, []grpc.DialOption, error) {
	var opts []grpc.DialOption
	address := cfg.Address
	if !_httpsScheme.MatchString(address) {
		address = fmt.Sprintf("tcp://%s", address)
	}
	remoteURL, err := url.Parse(address)
	if err != nil {
		return nil, nil, errors.New("milvus address parse fail")
	}
	// Remote Host should never be empty.
	if remoteURL.Host == "" {
		return nil, nil, errors.New("empty remote host of milvus address")
	}

	if remoteURL.Scheme == "https" || cfg.EnableTLS {
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	if remoteURL.Port() == "" && cfg.EnableTLS {
		remoteURL.Host += ":443"
	}

	return remoteURL, opts, nil
}

func (cfg *Cfg) parseRestful() (*url.URL, error) {
	address := cfg.Address
	if !_httpsScheme.MatchString(address) && !_httpScheme.MatchString(address) {
		if cfg.EnableTLS {
			address = fmt.Sprintf("https://%s", address)
		} else {
			address = fmt.Sprintf("http://%s", address)
		}
	}

	return url.Parse(address)
}
