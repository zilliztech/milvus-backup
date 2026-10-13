package storage

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestCA generates a self-signed CA certificate and returns its PEM
// encoding along with the parsed certificate and private key, so tests can
// sign further certificates with it.
func newTestCA(t *testing.T) ([]byte, *x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "backup-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	require.NoError(t, err)
	ca, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), ca, key
}

func writeTestCA(t *testing.T) string {
	t.Helper()

	caPEM, _, _ := newTestCA(t)
	path := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(path, caPEM, 0o600))

	return path
}

func TestTLSTransport(t *testing.T) {
	t.Run("MissingFile", func(t *testing.T) {
		_, err := tlsTransport(filepath.Join(t.TempDir(), "no-such-ca.pem"))
		assert.ErrorContains(t, err, "read ca cert")
	})

	t.Run("NotACertificate", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "ca.pem")
		require.NoError(t, os.WriteFile(path, []byte("not a pem"), 0o600))

		_, err := tlsTransport(path)
		assert.ErrorContains(t, err, "failed to append ca certificates")
	})

	t.Run("TrustsCertificatesSignedByTheCA", func(t *testing.T) {
		caPEM, ca, caKey := newTestCA(t)
		path := filepath.Join(t.TempDir(), "ca.pem")
		require.NoError(t, os.WriteFile(path, caPEM, 0o600))

		tr, err := tlsTransport(path)
		require.NoError(t, err)

		// minio's default transport enforces TLS 1.2, and the CA transport
		// must keep that floor rather than resetting the TLS config.
		assert.Equal(t, uint16(tls.VersionTLS12), tr.TLSClientConfig.MinVersion)

		leafDER, err := x509.CreateCertificate(rand.Reader,
			&x509.Certificate{
				SerialNumber: big.NewInt(2),
				Subject:      pkix.Name{CommonName: "backup-test-leaf"},
				NotBefore:    time.Now().Add(-time.Hour),
				NotAfter:     time.Now().Add(time.Hour),
				KeyUsage:     x509.KeyUsageDigitalSignature,
				ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
			},
			ca, &caKey.PublicKey, caKey)
		require.NoError(t, err)
		leaf, err := x509.ParseCertificate(leafDER)
		require.NoError(t, err)

		_, err = leaf.Verify(x509.VerifyOptions{
			Roots:     tr.TLSClientConfig.RootCAs,
			KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		})
		assert.NoError(t, err)
	})
}

func TestNewInternalMinioAppliesCACert(t *testing.T) {
	cfg := Config{
		Provider:   "minio",
		Endpoint:   "localhost:9000",
		UseSSL:     true,
		Bucket:     "test-bucket",
		CACertPath: writeTestCA(t),
		Credential: Credential{Type: Static, AK: "ak", SK: "sk"},
	}

	t.Run("SetsTheTransport", func(t *testing.T) {
		opts := &minio.Options{Secure: true, Creds: credentials.NewStaticV4("ak", "sk", "")}

		_, err := newInternalMinio(cfg, opts)
		require.NoError(t, err)
		assert.NotNil(t, opts.Transport)
	})

	t.Run("MissingFileFailsTheClient", func(t *testing.T) {
		bad := cfg
		bad.CACertPath = filepath.Join(t.TempDir(), "no-such-ca.pem")

		_, err := newInternalMinio(bad, &minio.Options{Secure: true, Creds: credentials.NewStaticV4("ak", "sk", "")})
		assert.ErrorContains(t, err, "read ca cert")
	})
}

// TestMinioClientSelfSignedTLS drives a real TLS handshake against a server
// presenting a self-signed certificate: the configured CA must be what
// admits the connection, and its absence must fail with an x509 error.
func TestMinioClientSelfSignedTLS(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// StatObject first resolves the bucket region: answer the location
		// request with an empty LocationConstraint, like minio does.
		if r.URL.Query().Has("location") {
			w.Header().Set("Content-Type", "application/xml")
			fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>`)
			return
		}
		w.Header().Set("ETag", `"deadbeef"`)
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	caPath := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(caPath, certPEM, 0o600))

	endpoint := strings.TrimPrefix(srv.URL, "https://")
	cfg := Config{
		Provider:   "minio",
		Endpoint:   endpoint,
		UseSSL:     true,
		Bucket:     "test-bucket",
		Credential: Credential{Type: Static, AK: "ak", SK: "sk"},
	}

	t.Run("WithoutTheCA", func(t *testing.T) {
		cli, err := newInternalMinio(cfg, &minio.Options{
			Secure:     true,
			Creds:      credentials.NewStaticV4("ak", "sk", ""),
			MaxRetries: 1,
		})
		require.NoError(t, err)

		_, err = cli.HeadObject(context.Background(), "key")
		assert.ErrorContains(t, err, "x509")
	})

	t.Run("WithTheCA", func(t *testing.T) {
		trusted := cfg
		trusted.CACertPath = caPath

		cli, err := newInternalMinio(trusted, &minio.Options{
			Secure:     true,
			Creds:      credentials.NewStaticV4("ak", "sk", ""),
			MaxRetries: 1,
		})
		require.NoError(t, err)

		_, err = cli.HeadObject(context.Background(), "key")
		assert.NoError(t, err)
	})
}
