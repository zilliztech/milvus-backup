package backup

import (
	"context"
	"iter"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"

	"github.com/zilliztech/milvus-backup/internal/snapshotbundle"
	"github.com/zilliztech/milvus-backup/internal/storage"
)

func newEnumerateTask(t *testing.T, cli storage.Client) *collSnapshotTask {
	t.Helper()
	return &collSnapshotTask{
		milvusStorage:  cli,
		milvusRootPath: "files",
		exportDir:      "backup/inst/bundle",
	}
}

func TestEnumerateBundleFiles_LegacySegment(t *testing.T) {
	cli := storage.NewMockClient(t)
	cli.EXPECT().HeadObject(mock.Anything, "files/insert_log/1/2/1001/101/1").Return(storage.ObjectAttr{Length: 10}, nil)
	// The same file stored under a URI form maps by both forms.
	cli.EXPECT().HeadObject(mock.Anything, "files/stats_log/1/2/1001/101/2").Return(storage.ObjectAttr{Length: 20}, nil)
	cli.EXPECT().HeadObject(mock.Anything, "files/index_files/1001/301/1").Return(storage.ObjectAttr{Length: 30}, nil)
	// The storage v2 manifest reference moves as one opaque string, keyed by the
	// whole reference rather than any path inside it.
	v2Manifest := snapshotbundle.MarshalManifestPath("files/insert_log/1/2/1001", 2)
	cli.EXPECT().HeadObject(mock.Anything, v2Manifest).Return(storage.ObjectAttr{Length: 40}, nil)

	task := newEnumerateTask(t, cli)
	snapshot := &snapshotbundle.SnapshotData{
		Segments: []*datapb.SegmentDescription{
			{
				SegmentId:      1001,
				StorageVersion: 2,
				Binlogs: []*datapb.FieldBinlog{
					{FieldID: 101, Binlogs: []*datapb.Binlog{{LogPath: "files/insert_log/1/2/1001/101/1"}}},
				},
				Statslogs: []*datapb.FieldBinlog{
					{FieldID: 101, Binlogs: []*datapb.Binlog{{LogPath: "s3://bucket/files/stats_log/1/2/1001/101/2"}}},
				},
				IndexFiles: []*indexpb.IndexFilePathInfo{
					{SegmentID: 1001, FieldID: 101, BuildID: 301, IndexFilePaths: []string{"files/index_files/1001/301/1"}},
				},
				ManifestPath: v2Manifest,
			},
		},
	}

	set, err := task.enumerateBundleFiles(context.Background(), snapshot)
	require.NoError(t, err)

	require.Len(t, set.attrs, 4)
	assert.EqualValues(t, 100, set.totalBytes)
	for _, attr := range set.attrs {
		assert.Contains(t, attr.DestKey, "backup/inst/bundle/files/")
	}
	// The path as the metadata stores it and its normalized object key both
	// map to the same destination, so the rewrite resolves whichever form it
	// reads back from a manifest.
	assert.Equal(t, "backup/inst/bundle/files/insert_log/1/2/1001/101/1", set.mapping["files/insert_log/1/2/1001/101/1"])
	assert.Equal(t, "backup/inst/bundle/files/stats_log/1/2/1001/101/2", set.mapping["s3://bucket/files/stats_log/1/2/1001/101/2"])
	// The v2 manifest reference maps as a whole string, nested under files/.
	assert.Equal(t, "backup/inst/bundle/files/"+v2Manifest, set.mapping[v2Manifest])
}

func TestEnumerateBundleFiles_V3Segment(t *testing.T) {
	cli := storage.NewMockClient(t)
	segmentPrefix := "files/insert_log/1/2/1001/"
	lobsPrefix := "files/insert_log/1/2/lobs/"
	cli.EXPECT().NewObjectIter(mock.Anything, segmentPrefix, true).Return(iterOver(
		storage.ObjectAttr{Key: "files/insert_log/1/2/1001/manifest", Length: 5},
		storage.ObjectAttr{Key: "files/insert_log/1/2/1001/101/1.parquet", Length: 50},
	))
	cli.EXPECT().NewObjectIter(mock.Anything, lobsPrefix, true).Return(iterOver(
		storage.ObjectAttr{Key: "files/insert_log/1/2/lobs/101/_data/1.vx", Length: 500},
	))

	task := newEnumerateTask(t, cli)
	snapshot := &snapshotbundle.SnapshotData{
		Segments: []*datapb.SegmentDescription{
			{
				SegmentId:      1001,
				PartitionId:    2,
				StorageVersion: 3,
				NumOfRows:      10,
				ManifestPath:   snapshotbundle.MarshalManifestPath("files/insert_log/1/2/1001", 3),
			},
		},
	}

	set, err := task.enumerateBundleFiles(context.Background(), snapshot)
	require.NoError(t, err)

	// The manifest root is only a rewrite anchor, never a file to copy.
	require.Len(t, set.attrs, 3)
	assert.EqualValues(t, 555, set.totalBytes)
	assert.Equal(t, "backup/inst/bundle/files/insert_log/1/2/1001", set.mapping["files/insert_log/1/2/1001"])
	for _, attr := range set.attrs {
		assert.NotEqual(t, "backup/inst/bundle/files/insert_log/1/2/1001", attr.DestKey)
	}

	// A second segment of the same partition walks the same prefixes; the file
	// set dedupes them.
	snapshot.Segments = append(snapshot.Segments, &datapb.SegmentDescription{
		SegmentId:      1002,
		PartitionId:    2,
		StorageVersion: 3,
		NumOfRows:      10,
		ManifestPath:   snapshotbundle.MarshalManifestPath("files/insert_log/1/2/1002", 3),
	})
	cli.EXPECT().NewObjectIter(mock.Anything, "files/insert_log/1/2/1002/", true).Return(iterOver(
		storage.ObjectAttr{Key: "files/insert_log/1/2/1002/manifest", Length: 5},
	))
	cli.EXPECT().NewObjectIter(mock.Anything, lobsPrefix, true).Return(iterOver(
		storage.ObjectAttr{Key: "files/insert_log/1/2/lobs/101/_data/1.vx", Length: 500},
	))

	set, err = task.enumerateBundleFiles(context.Background(), snapshot)
	require.NoError(t, err)
	require.Len(t, set.attrs, 4)
}

func TestEnumerateBundleFiles_V3WithoutManifestObjects(t *testing.T) {
	cli := storage.NewMockClient(t)
	cli.EXPECT().NewObjectIter(mock.Anything, "files/insert_log/1/2/1001/", true).Return(iterOver())

	task := newEnumerateTask(t, cli)
	snapshot := &snapshotbundle.SnapshotData{
		Segments: []*datapb.SegmentDescription{
			{
				SegmentId:      1001,
				PartitionId:    2,
				StorageVersion: 3,
				NumOfRows:      10,
				ManifestPath:   snapshotbundle.MarshalManifestPath("files/insert_log/1/2/1001", 3),
			},
		},
	}

	_, err := task.enumerateBundleFiles(context.Background(), snapshot)
	require.ErrorContains(t, err, "no manifest objects")
}

func TestEnumerateBundleFiles_V3WithoutManifestPath(t *testing.T) {
	task := newEnumerateTask(t, storage.NewMockClient(t))
	snapshot := &snapshotbundle.SnapshotData{
		Segments: []*datapb.SegmentDescription{{SegmentId: 1001, StorageVersion: 3}},
	}

	_, err := task.enumerateBundleFiles(context.Background(), snapshot)
	require.ErrorContains(t, err, "no manifest path")
}

func iterOver(attrs ...storage.ObjectAttr) iter.Seq2[storage.ObjectAttr, error] {
	return func(yield func(storage.ObjectAttr, error) bool) {
		for _, attr := range attrs {
			if !yield(attr, nil) {
				return
			}
		}
	}
}
