package jobstate

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/restoreplan"
)

func TestLocalPersistenceBackend(t *testing.T) {
	store := localPersistenceBackend{Directory: t.TempDir()}
	id := "job"
	_, err := store.Load(t.Context(), id)
	assert.ErrorIs(t, err, ErrPersistenceKeyNotFound)
	require.NoError(t, store.Save(t.Context(), id, []byte("first")))
	require.NoError(t, store.Save(t.Context(), id, []byte("replacement")))
	b, err := store.Load(t.Context(), id)
	require.NoError(t, err)
	assert.Equal(t, "replacement", string(b))
	files, err := os.ReadDir(store.Directory)
	require.NoError(t, err)
	require.Len(t, files, 1)
	info, err := os.Stat(filepath.Join(store.Directory, id))
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0600), info.Mode().Perm())
	require.NoError(t, store.Delete(t.Context(), id))
	assert.NoError(t, store.Delete(t.Context(), id))
}
func persistenceTestStorage(t *testing.T, server *httptest.Server) cfg.StorageConfig {
	t.Helper()
	host, port, err := net.SplitHostPort(server.Listener.Addr().String())
	require.NoError(t, err)
	s := cfg.New().Restore.Persistence.Storage
	s.Provider.Val = cfg.ProviderS3
	s.Address.Val = host
	s.Port.Val, err = strconv.Atoi(port)
	require.NoError(t, err)
	s.Region.Val = "us-east-1"
	s.BucketName.Val = "state"
	s.RootPath.Val = "restore/"
	s.Auth.Type.Val = cfg.AuthStatic
	s.Auth.AccessKeyID.Val = "key"
	s.Auth.SecretAccessKey.Val = "secret"
	return s
}

func TestS3PersistenceBackend(t *testing.T) {
	var saved []byte
	denied := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/state/restore/"+"job", r.URL.Path)
		if denied {
			w.WriteHeader(http.StatusForbidden)
			_, _ = fmt.Fprint(w, `<Error><Code>AccessDenied</Code><Message>denied</Message></Error>`)
			return
		}
		switch r.Method {
		case http.MethodPut:
			var err error
			saved, err = io.ReadAll(r.Body)
			assert.NoError(t, err)
			w.Header().Set("ETag", `"etag"`)
		case http.MethodGet:
			if saved == nil {
				w.WriteHeader(http.StatusNotFound)
				_, _ = fmt.Fprint(w, `<Error><Code>NoSuchKey</Code></Error>`)
				return
			}
			w.Header().Set("Last-Modified", "Mon, 02 Jan 2006 15:04:05 GMT")
			w.Header().Set("Content-Length", fmt.Sprint(len(saved)))
			_, _ = w.Write(saved)
		case http.MethodDelete:
			saved = nil
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer server.Close()
	c := encryptionConfig(t)
	c.Encryption.Enabled.Val = false
	c.Storage = persistenceTestStorage(t, server)
	store, err := openPersistenceBackend(c)
	require.NoError(t, err)
	_, err = store.Load(t.Context(), "job")
	assert.ErrorIs(t, err, ErrPersistenceKeyNotFound)
	require.NoError(t, store.Save(t.Context(), "job", []byte("persisted state")))
	b, err := store.Load(t.Context(), "job")
	require.NoError(t, err)
	assert.Equal(t, []byte("persisted state"), b)
	denied = true
	_, err = store.Load(t.Context(), "job")
	assert.Error(t, err)
	assert.NotErrorIs(t, err, ErrPersistenceKeyNotFound)
	denied = false
	require.NoError(t, store.Delete(t.Context(), "job"))
	_, err = store.Load(t.Context(), "job")
	assert.ErrorIs(t, err, ErrPersistenceKeyNotFound)
}

func TestLocalPersistenceCheck(t *testing.T) {
	backend := localPersistenceBackend{Directory: filepath.Join(t.TempDir(), "state")}
	require.NoError(t, backend.Check(t.Context()))
	entries, err := os.ReadDir(backend.Directory)
	require.NoError(t, err)
	assert.Empty(t, entries, "the check must remove its probe")
	for _, key := range []string{"restore/job", "backup/job"} {
		value := []byte{0, 255, 1, '\n'}
		require.NoError(t, backend.Save(t.Context(), key, value))
		got, err := backend.Load(t.Context(), key)
		require.NoError(t, err)
		assert.Equal(t, value, got)
	}
	require.NoError(t, backend.Check(t.Context()))
	_, err = backend.Load(t.Context(), "backup/job")
	assert.NoError(t, err, "checking must not touch task data")
}

func TestPersistenceRejectsInvalidKeys(t *testing.T) {
	local := localPersistenceBackend{Directory: t.TempDir()}
	s3 := &objectPersistenceBackend{}
	for _, backend := range []PersistenceBackend{local, s3} {
		for _, key := range []string{"", ".", "..", "../escape", "/absolute", "a/../b", `a\b`} {
			assert.Error(t, backend.Save(t.Context(), key, []byte("value")), key)
			_, err := backend.Load(t.Context(), key)
			assert.Error(t, err, key)
			assert.Error(t, backend.Delete(t.Context(), key), key)
		}
	}
	entries, err := os.ReadDir(local.Directory)
	require.NoError(t, err)
	assert.Empty(t, entries)
}

func TestS3PersistenceCheck(t *testing.T) {
	for _, tc := range []struct {
		name, denied, want string
		corrupt            bool
	}{
		{name: "Healthy"},
		{name: "Encrypted"},
		{name: "WriteDenied", denied: http.MethodPut, want: "save"},
		{name: "ReadDenied", denied: http.MethodGet, want: "load"},
		{name: "DeleteDenied", denied: http.MethodDelete, want: "delete"},
		{name: "ContentMismatch", corrupt: true, want: "mismatch"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			objects := make(map[string][]byte)
			var calls []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				assert.True(t, strings.HasPrefix(r.URL.Path, "/state/records/.persistence-check-"))
				calls = append(calls, r.Method)
				if r.Method == tc.denied {
					w.WriteHeader(http.StatusForbidden)
					_, _ = fmt.Fprint(w, `<Error><Code>AccessDenied</Code><Message>denied</Message></Error>`)
					return
				}
				switch r.Method {
				case http.MethodPut:
					var reader io.Reader = r.Body
					if strings.Contains(r.Header.Get("Content-Encoding"), "aws-chunked") || strings.HasPrefix(r.Header.Get("X-Amz-Content-Sha256"), "STREAMING-") {
						reader = httputil.NewChunkedReader(r.Body)
					}
					data, err := io.ReadAll(reader)
					assert.NoError(t, err)
					if tc.name == "Encrypted" {
						assert.Contains(t, string(data), `"encryptionVersion":1`)
						assert.Contains(t, string(data), `"wrappedKey"`)
					}
					objects[r.URL.Path] = data
					w.Header().Set("ETag", `"etag"`)
				case http.MethodGet:
					data := objects[r.URL.Path]
					if tc.corrupt {
						data = []byte("different bytes")
					}
					w.Header().Set("Last-Modified", "Mon, 02 Jan 2006 15:04:05 GMT")
					w.Header().Set("Content-Length", fmt.Sprint(len(data)))
					_, _ = w.Write(data)
				case http.MethodDelete:
					delete(objects, r.URL.Path)
					w.WriteHeader(http.StatusNoContent)
				}
			}))
			t.Cleanup(server.Close)
			c := encryptionConfig(t)
			c.Encryption.Enabled.Val = tc.name == "Encrypted"
			c.Storage = persistenceTestStorage(t, server)
			c.Storage.RootPath.Val = "records/"
			backend, err := openPersistenceBackend(c)
			require.NoError(t, err)
			err = backend.Check(t.Context())
			if tc.want == "" {
				assert.NoError(t, err)
			} else {
				assert.ErrorContains(t, err, tc.want)
			}
			mu.Lock()
			defer mu.Unlock()
			assert.Equal(t, http.MethodDelete, calls[len(calls)-1], "even a failed probe must attempt cleanup")
			if tc.denied != http.MethodDelete {
				assert.Empty(t, objects)
			}
		})
	}
}

type canceledProbeBackend struct {
	PersistenceBackend
	cancel  context.CancelFunc
	deleted bool
}

func (b *canceledProbeBackend) Save(context.Context, string, []byte) error {
	b.cancel()
	return context.Canceled
}
func (b *canceledProbeBackend) Delete(ctx context.Context, _ string) error {
	b.deleted = ctx.Err() == nil
	return ctx.Err()
}

func TestPersistenceCheckCleansUpAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	backend := &canceledProbeBackend{cancel: cancel}
	err := checkPersistenceBackend(ctx, backend)
	assert.ErrorIs(t, err, context.Canceled)
	assert.True(t, backend.deleted, "cleanup must not inherit the canceled probe context")
}

type startupCheckBackend struct {
	PersistenceBackend
	checks atomic.Int32
	err    error
	wait   bool
}

func (b *startupCheckBackend) Check(ctx context.Context) error {
	b.checks.Add(1)
	if b.wait {
		<-ctx.Done()
		return ctx.Err()
	}
	return b.err
}

func TestInitializePersistence(t *testing.T) {
	config := cfg.New().Restore.Persistence
	config.Enabled.Val = true
	config.Storage.Provider.Val = "local"
	config.Storage.RootPath.Val = t.TempDir()
	store := NewStore()
	store.ConfigurePersistence(config)
	backend := &startupCheckBackend{PersistenceBackend: localPersistenceBackend{Directory: config.Storage.RootPath.Val}, err: assert.AnError}
	store.persistence.backend = backend
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() { store.InitializePersistence(t.Context(), config) })
	}
	wg.Wait()
	assert.EqualValues(t, 1, backend.checks.Load(), "same startup configuration must be checked once")
	tracker, err := store.AddRestoreTask("job", true)
	require.NoError(t, err, "a failed startup check must not block registration")
	tracker.SavePlan(&restoreplan.Plan{})
	tracker.Finish(assert.AnError)
	_, err = backend.Load(t.Context(), restoreStateKey("job"))
	assert.NoError(t, err, "startup failure must not disable future persistence")
	backend.err = nil
	assert.NoError(t, store.CheckPersistence(t.Context()))
	assert.EqualValues(t, 2, backend.checks.Load())
}

func TestInitializeDisabledPersistence(t *testing.T) {
	config := cfg.New().Restore.Persistence
	config.Storage.Provider.Val = "invalid"
	config.Storage.RootPath.Val = filepath.Join(t.TempDir(), "unused")
	store := NewStore()
	store.InitializePersistence(t.Context(), config)
	assert.Nil(t, store.persistence.backend)
	assert.NoDirExists(t, config.Storage.RootPath.Val)
	assert.NoError(t, store.CheckPersistence(t.Context()))
}

func TestInitializePersistenceTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		config := cfg.New().Restore.Persistence
		config.Enabled.Val = true
		store := NewStore()
		store.ConfigurePersistence(config)
		backend := &startupCheckBackend{wait: true}
		store.persistence.backend = backend
		started := time.Now()
		store.InitializePersistence(t.Context(), config)
		assert.Equal(t, 15*time.Second, time.Since(started))
		assert.EqualValues(t, 1, backend.checks.Load())
	})
}
