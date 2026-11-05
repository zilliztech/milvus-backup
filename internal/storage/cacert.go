package storage

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"
)

// tlsTransport returns a transport whose TLS configuration trusts the CA
// certificates in the file at caCertPath in addition to the system pool. The
// file is read here, at client creation, so a missing or malformed file fails
// the task loudly instead of surfacing as an opaque x509 error on the first
// request.
func tlsTransport(caCertPath string) (*http.Transport, error) {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	if tr.TLSClientConfig == nil {
		tr.TLSClientConfig = &tls.Config{}
	}
	if tr.TLSClientConfig.MinVersion == 0 {
		tr.TLSClientConfig.MinVersion = tls.VersionTLS12
	}

	if tr.TLSClientConfig.RootCAs == nil {
		// The system pool is what the default transport falls back to
		// when SSL_CERT_FILE is unset; start from it so the store's public
		// chain, if any, keeps verifying.
		pool, err := x509.SystemCertPool()
		if err != nil {
			pool = x509.NewCertPool()
		}
		tr.TLSClientConfig.RootCAs = pool
	}

	b, err := os.ReadFile(caCertPath)
	if err != nil {
		return nil, fmt.Errorf("storage: read ca cert %w", err)
	}
	if !tr.TLSClientConfig.RootCAs.AppendCertsFromPEM(b) {
		return nil, fmt.Errorf("storage: failed to append ca certificates from %s", caCertPath)
	}

	return tr, nil
}
