package snapshotbundle

import (
	"fmt"
	"path"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Storage format versions a segment can carry, matching the server's
// storage.StorageV1/V2/V3 values.
const (
	storageV2 int64 = 2
	storageV3 int64 = 3
)

// SnapshotData is the in-memory form of one snapshot: its metadata JSON
// content plus the loaded per-segment manifests.
type SnapshotData struct {
	SnapshotInfo *datapb.SnapshotInfo
	Collection   *datapb.CollectionDescription
	Segments     []*datapb.SegmentDescription
	Indexes      []*indexpb.IndexInfo

	MetadataPath  string
	ManifestPaths []string
	SegmentIDs    []int64
	BuildIDs      []int64
	Layout        datapb.SnapshotLayout
}

// RewriteSnapshotWithMapping clones a snapshot and rewrites every file
// reference through mappings so it points into the exported bundle. The
// in-memory source is left unchanged.
func RewriteSnapshotWithMapping(
	snapshot *SnapshotData,
	mappings map[string]string,
	targetRoot string,
	metadataURI string,
) (*SnapshotData, error) {
	if snapshot == nil {
		return nil, merr.WrapErrServiceInternalMsg("snapshot cannot be nil")
	}
	if snapshot.SnapshotInfo == nil {
		return nil, merr.WrapErrDataIntegrityMsg("snapshot info cannot be nil")
	}
	if snapshot.Collection == nil {
		return nil, merr.WrapErrDataIntegrityMsg("collection description cannot be nil")
	}
	if targetRoot == "" {
		return nil, merr.WrapErrServiceInternalMsg("target root cannot be empty")
	}
	if metadataURI == "" {
		return nil, merr.WrapErrServiceInternalMsg("metadata URI cannot be empty")
	}
	rewriter := snapshotPathRewriter{mappings: mappings}
	// The export writes a self-contained snapshot. Clone the source metadata
	// before rewriting paths so the in-memory referenced snapshot stays intact.
	exported := &SnapshotData{
		SnapshotInfo: proto.Clone(snapshot.SnapshotInfo).(*datapb.SnapshotInfo),
		Collection:   proto.Clone(snapshot.Collection).(*datapb.CollectionDescription),
		SegmentIDs:   append([]int64(nil), snapshot.SegmentIDs...),
		BuildIDs:     append([]int64(nil), snapshot.BuildIDs...),
		Layout:       datapb.SnapshotLayout_SnapshotLayoutSelfContained,
	}
	// Pin IDs are cluster-local lifecycle state. A portable bundle must not
	// retain the source cluster's active pin records.
	exported.SnapshotInfo.PinIds = nil
	exported.SnapshotInfo.PinExpireAtMs = nil
	exported.SnapshotInfo.S3Location = metadataURI
	exported.Indexes = make([]*indexpb.IndexInfo, 0, len(snapshot.Indexes))
	for i, index := range snapshot.Indexes {
		if index == nil {
			return nil, merr.WrapErrDataIntegrityMsg("snapshot index at index %d cannot be nil", i)
		}
		exported.Indexes = append(exported.Indexes, proto.Clone(index).(*indexpb.IndexInfo))
	}
	exported.Segments = make([]*datapb.SegmentDescription, 0, len(snapshot.Segments))
	for i, segment := range snapshot.Segments {
		if segment == nil {
			return nil, merr.WrapErrDataIntegrityMsg("snapshot segment at index %d cannot be nil", i)
		}
		cloned := proto.Clone(segment).(*datapb.SegmentDescription)
		if err := rewriter.rewriteSegment(cloned); err != nil {
			return nil, err
		}
		exported.Segments = append(exported.Segments, cloned)
	}
	return exported, nil
}

type snapshotPathRewriter struct {
	mappings map[string]string
}

func (r snapshotPathRewriter) rewriteSegment(segment *datapb.SegmentDescription) error {
	includeInsert := true
	includeManifestOwnedIndexes := true
	if segment.GetStorageVersion() >= storageV3 {
		// Storage v3 insert files are owned by the packed manifest. Drop legacy
		// protobuf insert binlogs from exported metadata to avoid copying the
		// same physical data through two path representations.
		segment.Binlogs = nil
		if err := r.rewriteStorageV3Manifest(segment); err != nil {
			return err
		}
		includeInsert = false
		includeManifestOwnedIndexes = false
	}
	if err := rewriteSegmentFilePaths(segment, includeInsert, includeManifestOwnedIndexes, r.rewritePath); err != nil {
		return err
	}
	if segment.GetStorageVersion() == storageV2 && segment.GetManifestPath() != "" {
		rewritten, err := r.rewritePath(
			segment.GetManifestPath(),
			fmt.Sprintf("storage v2 manifest segment %d", segment.GetSegmentId()),
		)
		if err != nil {
			return err
		}
		segment.ManifestPath = rewritten
	}
	return nil
}

func (r snapshotPathRewriter) rewriteStorageV3Manifest(segment *datapb.SegmentDescription) error {
	if segment.GetManifestPath() == "" {
		return merr.WrapErrDataIntegrityMsg("storage v3 segment %d requires manifest path", segment.GetSegmentId())
	}
	basePath, version, err := UnmarshalManifestPath(segment.GetManifestPath())
	if err != nil {
		return merr.WrapErrDataIntegrity(err, "failed to parse manifest path for segment %d", segment.GetSegmentId())
	}
	rewrittenBasePath, err := r.rewritePath(basePath, fmt.Sprintf("storage v3 manifest root segment %d", segment.GetSegmentId()))
	if err != nil {
		return err
	}
	segment.ManifestPath = MarshalManifestPath(rewrittenBasePath, version)
	return nil
}

type segmentPathRewriteFunc func(src string, context string) (string, error)

func rewriteSegmentFilePaths(
	segment *datapb.SegmentDescription,
	includeInsert bool,
	includeManifestOwnedIndexes bool,
	rewrite segmentPathRewriteFunc,
) error {
	if includeInsert {
		if err := rewriteFieldBinlogPaths(segment.GetBinlogs(), "insert binlog", segment.GetSegmentId(), rewrite); err != nil {
			return err
		}
	}
	if err := rewriteFieldBinlogPaths(segment.GetStatslogs(), "stats binlog", segment.GetSegmentId(), rewrite); err != nil {
		return err
	}
	if err := rewriteFieldBinlogPaths(segment.GetDeltalogs(), "delta binlog", segment.GetSegmentId(), rewrite); err != nil {
		return err
	}
	if err := rewriteFieldBinlogPaths(segment.GetBm25Statslogs(), "bm25 stats binlog", segment.GetSegmentId(), rewrite); err != nil {
		return err
	}
	if err := rewriteIndexFilePaths(segment.GetIndexFiles(), segment.GetSegmentId(), rewrite); err != nil {
		return err
	}
	if includeManifestOwnedIndexes {
		if err := rewriteTextIndexPaths(segment.GetTextIndexFiles(), segment.GetSegmentId(), rewrite); err != nil {
			return err
		}
		return rewriteJSONKeyIndexPaths(segment.GetJsonKeyIndexFiles(), segment.GetSegmentId(), rewrite)
	}
	return nil
}

func rewriteFieldBinlogPaths(fieldBinlogs []*datapb.FieldBinlog, fileType string, segmentID int64, rewrite segmentPathRewriteFunc) error {
	for fieldIdx, fieldBinlog := range fieldBinlogs {
		if fieldBinlog == nil {
			return merr.WrapErrDataIntegrityMsg("%s segment %d field binlog at index %d cannot be nil", fileType, segmentID, fieldIdx)
		}
		for binlogIdx, binlog := range fieldBinlog.GetBinlogs() {
			if binlog == nil {
				return merr.WrapErrDataIntegrityMsg("%s segment %d field %d binlog at index %d cannot be nil", fileType, segmentID, fieldBinlog.GetFieldID(), binlogIdx)
			}
			rewritten, err := rewrite(binlog.GetLogPath(), fmt.Sprintf("%s segment %d field %d", fileType, segmentID, fieldBinlog.GetFieldID()))
			if err != nil {
				return err
			}
			binlog.LogPath = rewritten
		}
	}
	return nil
}

func rewriteIndexFilePaths(indexFiles []*indexpb.IndexFilePathInfo, segmentID int64, rewrite segmentPathRewriteFunc) error {
	for i, indexFile := range indexFiles {
		if indexFile == nil {
			return merr.WrapErrDataIntegrityMsg("index file segment %d entry at index %d cannot be nil", segmentID, i)
		}
		for i, filePath := range indexFile.GetIndexFilePaths() {
			rewritten, err := rewrite(filePath, fmt.Sprintf("index file segment %d field %d build %d", segmentID, indexFile.GetFieldID(), indexFile.GetBuildID()))
			if err != nil {
				return err
			}
			indexFile.IndexFilePaths[i] = rewritten
		}
	}
	return nil
}

func rewriteTextIndexPaths(indexes map[int64]*datapb.TextIndexStats, segmentID int64, rewrite segmentPathRewriteFunc) error {
	for fieldID, index := range indexes {
		if index == nil {
			return merr.WrapErrDataIntegrityMsg("text index segment %d field %d cannot be nil", segmentID, fieldID)
		}
		if index.GetFieldID() != 0 {
			fieldID = index.GetFieldID()
		}
		for i, filePath := range index.GetFiles() {
			rewritten, err := rewrite(filePath, fmt.Sprintf("text index segment %d field %d build %d", segmentID, fieldID, index.GetBuildID()))
			if err != nil {
				return err
			}
			index.Files[i] = rewritten
		}
	}
	return nil
}

func rewriteJSONKeyIndexPaths(indexes map[int64]*datapb.JsonKeyStats, segmentID int64, rewrite segmentPathRewriteFunc) error {
	for fieldID, index := range indexes {
		if index == nil {
			return merr.WrapErrDataIntegrityMsg("json key index segment %d field %d cannot be nil", segmentID, fieldID)
		}
		if index.GetFieldID() != 0 {
			fieldID = index.GetFieldID()
		}
		for i, filePath := range index.GetFiles() {
			rewritten, err := rewrite(filePath, fmt.Sprintf("json key index segment %d field %d build %d", segmentID, fieldID, index.GetBuildID()))
			if err != nil {
				return err
			}
			index.Files[i] = rewritten
		}
	}
	return nil
}

func (r snapshotPathRewriter) rewritePath(src string, context string) (string, error) {
	if src == "" {
		return "", nil
	}
	if dst, ok := r.mappings[src]; ok {
		return dst, nil
	}
	normalized := NormalizeObjectPath(src)
	if dst, ok := r.mappings[normalized]; ok {
		return dst, nil
	}
	return "", merr.WrapErrDataIntegrityMsg("missing snapshot file mapping for %s: %s", context, src)
}

// LOBBasePath returns the per-field directory a segment's LOB files live
// under: a sibling of the segment directory at partition level.
func LOBBasePath(manifestBasePath string, fieldID int64) string {
	return path.Join(path.Dir(manifestBasePath), "lobs", fmt.Sprintf("%d", fieldID))
}
