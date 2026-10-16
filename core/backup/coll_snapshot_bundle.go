package backup

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path"
	"sort"

	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"

	"github.com/zilliztech/milvus-backup/internal/snapshotbundle"
	"github.com/zilliztech/milvus-backup/internal/storage"
)

const (
	_storageV2 int64 = 2
	_storageV3 int64 = 3
)

// bundleStore adapts the backup storage client to the snapshot bundle reader
// and writer.
type bundleStore struct {
	cli storage.Client
}

func (b bundleStore) Read(ctx context.Context, key string) ([]byte, error) {
	obj, err := b.cli.GetObject(ctx, key)
	if err != nil {
		// A HEAD that also fails means the object is not there — or at least
		// not readable, which the staged-commit logic treats the same way: the
		// caller either writes the object or reports an error, and the
		// read-back after a write still verifies the published bytes.
		if _, headErr := b.cli.HeadObject(ctx, key); headErr != nil {
			return nil, fmt.Errorf("%w: %s: %w", snapshotbundle.ErrObjectNotFound, key, err)
		}
		return nil, fmt.Errorf("backup: read bundle object %s: %w", key, err)
	}
	defer obj.Body.Close()
	data, err := io.ReadAll(obj.Body)
	if err != nil {
		return nil, fmt.Errorf("backup: read bundle object %s: %w", key, err)
	}
	return data, nil
}

func (b bundleStore) Write(ctx context.Context, key string, data []byte) error {
	err := b.cli.UploadObject(ctx, storage.UploadObjectInput{
		Key:  key,
		Body: bytes.NewReader(data),
		Size: int64(len(data)),
	})
	if err != nil {
		return fmt.Errorf("backup: write bundle object %s: %w", key, err)
	}
	return nil
}

func (b bundleStore) Remove(ctx context.Context, key string) error {
	if err := b.cli.DeleteObject(ctx, key); err != nil {
		return fmt.Errorf("backup: delete bundle object %s: %w", key, err)
	}
	return nil
}

// bundleFileSet is the copy plan of one snapshot export: every file to move
// from the milvus storage into the bundle, plus the path mappings the bundle
// metadata rewrite consumes.
type bundleFileSet struct {
	attrs      []storage.CopyAttr
	mapping    map[string]string
	seen       map[string]struct{}
	totalBytes int64
}

// enumerateBundleFiles lists the concrete objects a snapshot references. The
// file set of a v1/v2 segment comes straight out of its manifest; a v3 segment
// owns its insert files through its packed manifest objects, which are listed
// by walking the segment prefix, and its LOB files live under the partition's
// lobs prefix — walking that prefix takes a superset: stale objects a lagging
// GC has not removed yet ride along, and restore only reads what the rewritten
// bundle references.
func (st *collSnapshotTask) enumerateBundleFiles(ctx context.Context, snapshot *snapshotbundle.SnapshotData) (*bundleFileSet, error) {
	set := &bundleFileSet{
		mapping: make(map[string]string),
		seen:    make(map[string]struct{}),
		attrs:   make([]storage.CopyAttr, 0),
	}
	for _, segment := range snapshot.Segments {
		if segment.GetStorageVersion() >= _storageV3 {
			if err := st.enumerateV3Segment(ctx, set, segment); err != nil {
				return nil, err
			}
		} else if err := st.enumerateLegacySegment(set, segment); err != nil {
			return nil, err
		}

		for _, fieldBinlog := range segment.GetStatslogs() {
			for _, binlog := range fieldBinlog.GetBinlogs() {
				if err := st.addBundleCopy(set, binlog.GetLogPath(), 0); err != nil {
					return nil, err
				}
			}
		}
		for _, fieldBinlog := range segment.GetDeltalogs() {
			for _, binlog := range fieldBinlog.GetBinlogs() {
				if err := st.addBundleCopy(set, binlog.GetLogPath(), 0); err != nil {
					return nil, err
				}
			}
		}
		for _, fieldBinlog := range segment.GetBm25Statslogs() {
			for _, binlog := range fieldBinlog.GetBinlogs() {
				if err := st.addBundleCopy(set, binlog.GetLogPath(), 0); err != nil {
					return nil, err
				}
			}
		}
		for _, indexFile := range segment.GetIndexFiles() {
			for _, filePath := range indexFile.GetIndexFilePaths() {
				if err := st.addBundleCopy(set, filePath, 0); err != nil {
					return nil, err
				}
			}
		}
		if segment.GetStorageVersion() < _storageV3 {
			// Storage v3 text and JSON index files are manifest-owned; their
			// protobuf lists are metadata placeholders that the segment prefix
			// walk already covers.
			for _, index := range segment.GetTextIndexFiles() {
				for _, filePath := range index.GetFiles() {
					if err := st.addBundleCopy(set, filePath, 0); err != nil {
						return nil, err
					}
				}
			}
			for _, index := range segment.GetJsonKeyIndexFiles() {
				for _, filePath := range index.GetFiles() {
					if err := st.addBundleCopy(set, filePath, 0); err != nil {
						return nil, err
					}
				}
			}
		}
	}

	if err := st.fillBundleFileSizes(ctx, set); err != nil {
		return nil, err
	}
	sort.Slice(set.attrs, func(i, j int) bool { return set.attrs[i].Src.Key < set.attrs[j].Src.Key })
	return set, nil
}

func (st *collSnapshotTask) enumerateLegacySegment(set *bundleFileSet, segment *datapb.SegmentDescription) error {
	for _, fieldBinlog := range segment.GetBinlogs() {
		for _, binlog := range fieldBinlog.GetBinlogs() {
			if err := st.addBundleCopy(set, binlog.GetLogPath(), 0); err != nil {
				return err
			}
		}
	}
	// The storage v2 manifest reference moves into the bundle as one opaque
	// string, the way the server's restore maps it back.
	if segment.GetStorageVersion() == _storageV2 && segment.GetManifestPath() != "" {
		if err := st.addBundleCopy(set, segment.GetManifestPath(), 0); err != nil {
			return err
		}
	}
	return nil
}

func (st *collSnapshotTask) enumerateV3Segment(ctx context.Context, set *bundleFileSet, segment *datapb.SegmentDescription) error {
	if segment.GetManifestPath() == "" {
		return fmt.Errorf("backup: snapshot segment %d has storage v3 but no manifest path", segment.GetSegmentId())
	}
	basePath, _, err := snapshotbundle.UnmarshalManifestPath(segment.GetManifestPath())
	if err != nil {
		return fmt.Errorf("backup: parse manifest path of segment %d: %w", segment.GetSegmentId(), err)
	}
	// The manifest root itself is only a rewrite anchor, never copied.
	st.mapBundlePath(set, basePath)

	// The packed manifest owns the segment's insert files: list the whole
	// segment prefix rather than trusting the protobuf lists, which may hold
	// stale placeholders after a format migration.
	manifestObjects := 0
	prefix := basePath
	if prefix[len(prefix)-1] != '/' {
		prefix += "/"
	}
	for attr, err := range st.milvusStorage.NewObjectIter(ctx, prefix, true) {
		if err != nil {
			return fmt.Errorf("backup: list manifest files of segment %d: %w", segment.GetSegmentId(), err)
		}
		manifestObjects++
		if err := st.addBundleCopy(set, attr.Key, attr.Length); err != nil {
			return err
		}
	}
	if segment.GetNumOfRows() > 0 && manifestObjects == 0 {
		return fmt.Errorf("backup: storage v3 segment %d has %d rows but no manifest objects", segment.GetSegmentId(), segment.GetNumOfRows())
	}

	// LOB files are referenced through manifest content, not through the
	// protobuf lists, so their prefix is walked at partition level. Segments
	// of one partition share the prefix and the file set dedupes them.
	lobsPrefix := path.Join(path.Dir(basePath), "lobs") + "/"
	for attr, err := range st.milvusStorage.NewObjectIter(ctx, lobsPrefix, true) {
		if err != nil {
			return fmt.Errorf("backup: list lob files of segment %d: %w", segment.GetSegmentId(), err)
		}
		if err := st.addBundleCopy(set, attr.Key, attr.Length); err != nil {
			return err
		}
	}
	return nil
}

// addBundleCopy maps one source object into the bundle and records the copy.
// A size already known from a listing is kept; the rest are filled in by HEAD
// requests before the copy starts.
func (st *collSnapshotTask) addBundleCopy(set *bundleFileSet, src string, knownSize int64) error {
	key := snapshotbundle.NormalizeObjectPath(src)
	if key == "" {
		return nil
	}
	if _, ok := set.seen[key]; ok {
		return nil
	}
	dest := st.bundleDestPath(key)
	if dest == key {
		return fmt.Errorf("backup: bundle dest key %s is same as src key %s", dest, key)
	}
	set.seen[key] = struct{}{}
	set.mapping[src] = dest
	set.mapping[key] = dest
	set.attrs = append(set.attrs, storage.CopyAttr{
		Src:     storage.ObjectAttr{Key: key, Length: knownSize},
		DestKey: dest,
	})
	return nil
}

// mapBundlePath records a rewrite-only mapping: the path is referenced by the
// bundle metadata but is not itself an object to copy.
func (st *collSnapshotTask) mapBundlePath(set *bundleFileSet, src string) {
	key := snapshotbundle.NormalizeObjectPath(src)
	set.mapping[src] = st.bundleDestPath(key)
	set.mapping[key] = st.bundleDestPath(key)
}

func (st *collSnapshotTask) bundleDestPath(key string) string {
	return snapshotbundle.ExportedSnapshotPath(st.milvusRootPath, key, st.exportDir)
}

// fillBundleFileSizes heads every file whose size no listing reported. It
// doubles as the existence check: a referenced file that is gone fails the
// backup before anything is copied.
func (st *collSnapshotTask) fillBundleFileSizes(ctx context.Context, set *bundleFileSet) error {
	group, groupCtx := errgroup.WithContext(ctx)
	for index := range set.attrs {
		if set.attrs[index].Src.Length > 0 {
			continue
		}
		group.Go(func() error {
			if st.copySem != nil {
				if err := st.copySem.Acquire(groupCtx, 1); err != nil {
					return fmt.Errorf("backup: acquire copy semaphore: %w", err)
				}
				defer st.copySem.Release(1)
			}
			attr, err := st.milvusStorage.HeadObject(groupCtx, set.attrs[index].Src.Key)
			if err != nil {
				return fmt.Errorf("backup: head snapshot file %s: %w", set.attrs[index].Src.Key, err)
			}
			set.attrs[index].Src.Length = attr.Length
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return err
	}
	var total int64
	for _, attr := range set.attrs {
		total += attr.Src.Length
	}
	set.totalBytes = total
	return nil
}
