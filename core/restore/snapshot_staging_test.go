package restore

import (
	"bytes"
	"context"
	"io"
	"iter"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"

	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/storage"
)

func TestBundleRoot(t *testing.T) {
	t.Run("BundlePrefix", func(t *testing.T) {
		root, err := bundleRoot("bundle/exports/abc/snapshots/111/metadata/555.json")
		require.NoError(t, err)
		assert.Equal(t, "bundle/exports/abc/", root)
	})

	t.Run("BareSnapshotsDir", func(t *testing.T) {
		// A path without anything above the anchor sits at no bundle root.
		_, err := bundleRoot("snapshots/111/metadata/555.json")
		assert.Error(t, err)
	})

	t.Run("WrongShape", func(t *testing.T) {
		_, err := bundleRoot("bundle/snapshots/111/555.json")
		assert.ErrorContains(t, err, "metadata")

		_, err = bundleRoot("bundle/snapshots/111/metadata/555.json/extra")
		assert.Error(t, err)
	})
}

// memStore is an in-memory storage.Client for the staging tests: objects are
// key→bytes, listings are prefix scans, and writes land in the map.
type memStore struct {
	cfg storage.Config

	mu      sync.Mutex
	objects map[string][]byte
}

func newMemStore(cfg storage.Config) *memStore {
	return &memStore{cfg: cfg, objects: make(map[string][]byte)}
}

func (s *memStore) Config() storage.Config { return s.cfg }

func (s *memStore) CopyObject(context.Context, storage.CopyObjectInput) error {
	panic("a staging copy streams; the direct copier is never used")
}

func (s *memStore) HeadObject(_ context.Context, key string) (storage.ObjectAttr, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	body, ok := s.objects[key]
	if !ok {
		return storage.ObjectAttr{}, assert.AnError
	}
	return storage.ObjectAttr{Key: key, Length: int64(len(body))}, nil
}

func (s *memStore) GetObject(_ context.Context, key string) (*storage.Object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	body, ok := s.objects[key]
	if !ok {
		return nil, assert.AnError
	}
	return &storage.Object{Length: int64(len(body)), Body: io.NopCloser(bytes.NewReader(body))}, nil
}

func (s *memStore) UploadObject(_ context.Context, i storage.UploadObjectInput) error {
	body, err := io.ReadAll(i.Body)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[i.Key] = body
	return nil
}

func (s *memStore) DeleteObject(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.objects, key)
	return nil
}

func (s *memStore) NewObjectIter(_ context.Context, prefix string, _ bool) iter.Seq2[storage.ObjectAttr, error] {
	return func(yield func(storage.ObjectAttr, error) bool) {
		s.mu.Lock()
		var attrs []storage.ObjectAttr
		for key, body := range s.objects {
			if strings.HasPrefix(key, prefix) {
				attrs = append(attrs, storage.ObjectAttr{Key: key, Length: int64(len(body))})
			}
		}
		s.mu.Unlock()
		sort.Slice(attrs, func(i, j int) bool { return attrs[i].Key < attrs[j].Key })

		for _, attr := range attrs {
			if !yield(attr, nil) {
				return
			}
		}
	}
}

func (s *memStore) BucketExist(context.Context, string) (bool, error) { return true, nil }

func (s *memStore) CreateBucket(context.Context) error { return nil }

// keys returns the sorted keys the store holds, for assertions.
func (s *memStore) keys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]string, 0, len(s.objects))
	for key := range s.objects {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// stage copies the whole bundle subtree — not only the metadata file — into a
// temp dir on the milvus storage, keeping the bundle's relative layout intact
// under it, and the cleanup removes the temp dir once the restore is over.
func TestSnapshotStaging_Stage(t *testing.T) {
	backup := newMemStore(storage.Config{Provider: cfg.ProviderS3, Region: "us-west-2", Bucket: "backup-bucket"})
	milvusStorage := newMemStore(storage.Config{Provider: cfg.ProviderMinio, Endpoint: "minio:9000", Bucket: "milvus-bucket"})

	objects := map[string]string{
		"backup/mybackup/bundle/snapshots/111/metadata/555.json":              "metadata",
		"backup/mybackup/bundle/snapshots/111/manifests/555/333.avro":         "manifest",
		"backup/mybackup/bundle/files/insert_log/111/222/333/_data/0.parquet": "data",
		// outside the bundle: the backup's own meta, which never gets staged
		"backup/mybackup/full_meta.json": "meta",
	}
	for key, body := range objects {
		backup.objects[key] = []byte(body)
	}

	staging := &snapshotStaging{
		backupDir:     "backup/mybackup/",
		backupStorage: backup,
		milvusStorage: milvusStorage,
		copySem:       semaphore.NewWeighted(4),
	}

	uri, cleanup, err := staging.stage(t.Context(), zap.NewNop(), "task1", collref.New("db", "coll"),
		"bundle/snapshots/111/metadata/555.json")
	require.NoError(t, err)

	// Everything under the bundle root is staged, nothing outside it is, and
	// the uri names the staged metadata with the bundle layout intact.
	staged := milvusStorage.keys()
	require.Len(t, staged, 3)
	for _, key := range staged {
		assert.Contains(t, key, "restore-temp-task1-db-coll-")
	}
	assert.True(t, strings.HasPrefix(uri, "s3://milvus-bucket/restore-temp-task1-db-coll-"), uri)
	assert.True(t, strings.HasSuffix(uri, "/bundle/snapshots/111/metadata/555.json"), uri)
	assert.Contains(t, backup.keys(), "backup/mybackup/full_meta.json")

	cleanup(t.Context())
	assert.Empty(t, milvusStorage.keys())
}

// keepTempFiles leaves the staged copy in place, for inspecting a failed restore.
func TestSnapshotStaging_KeepTempFiles(t *testing.T) {
	backup := newMemStore(storage.Config{Provider: cfg.ProviderS3, Region: "us-west-2", Bucket: "backup-bucket"})
	milvusStorage := newMemStore(storage.Config{Provider: cfg.ProviderMinio, Endpoint: "minio:9000", Bucket: "milvus-bucket"})
	backup.objects["backup/mybackup/bundle/snapshots/111/metadata/555.json"] = []byte("metadata")

	staging := &snapshotStaging{
		backupDir:     "backup/mybackup/",
		backupStorage: backup,
		milvusStorage: milvusStorage,
		copySem:       semaphore.NewWeighted(4),
		keepTempFiles: true,
	}

	_, cleanup, err := staging.stage(t.Context(), zap.NewNop(), "task1", collref.New("db", "coll"),
		"bundle/snapshots/111/metadata/555.json")
	require.NoError(t, err)

	cleanup(t.Context())
	assert.NotEmpty(t, milvusStorage.keys())
}
