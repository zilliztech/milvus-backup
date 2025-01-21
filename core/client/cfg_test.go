package client

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCfg_ParseAuth(t *testing.T) {
	cfg := &Cfg{Username: "username", Password: "password"}
	got := cfg.parseGrpcAuth()
	assert.Equal(t, "dXNlcm5hbWU6cGFzc3dvcmQ=", got)

	cfg = &Cfg{}
	got = cfg.parseGrpcAuth()
	assert.Equal(t, "", got)
}

func TestCfg_ParseGrpc(t *testing.T) {
	cfg := &Cfg{Host: ""}
	_, _, err := cfg.parseGrpc()
	assert.Error(t, err)

	cfg = &Cfg{Host: "localhost:19530"}
	u, _, err := cfg.parseGrpc()
	assert.NoError(t, err)
	assert.Equal(t, "localhost:19530", u.Host)

	cfg = &Cfg{Host: "localhost", EnableTLS: true}
	u, _, err = cfg.parseGrpc()
	assert.NoError(t, err)
	assert.Equal(t, "localhost:443", u.Host)
}

func TestCfg_ParseRestful(t *testing.T) {
	cfg := &Cfg{Host: "localhost:19530", EnableTLS: false}
	u := cfg.parseRestful()
	assert.Equal(t, "http", u.Scheme)

	cfg = &Cfg{Host: "localhost:19530", EnableTLS: true}
	u = cfg.parseRestful()
	assert.Equal(t, "https", u.Scheme)
}

func TestCfg_ParseRestfulAuth(t *testing.T) {
	cfg := &Cfg{Username: "username", Password: "password"}
	got := cfg.parseRestfulAuth()
	assert.Equal(t, "username:password", got)
}
