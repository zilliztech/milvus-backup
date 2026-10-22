package snapshotbundle

import (
	"bytes"
	"context"
	"errors"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ErrObjectNotFound reports a missing object. Store implementations must
// return an error wrapping it when a Read finds nothing at the key.
var ErrObjectNotFound = errors.New("snapshotbundle: object not found")

// Store is the object storage the bundle is written to and read from.
type Store interface {
	Read(ctx context.Context, key string) ([]byte, error)
	Write(ctx context.Context, key string, data []byte) error
	Remove(ctx context.Context, key string) error
}

// PrepareBundleStaged writes every segment manifest to its final location and
// the metadata JSON to a private staging object, verifying the staged bytes by
// read-back. The returned path is where CommitBundleMetadata publishes the
// metadata; the returned byte count covers manifests plus metadata and does
// not count the staging object.
func PrepareBundleStaged(
	ctx context.Context,
	store Store,
	snapshot *SnapshotData,
	rootPath string,
	layout datapb.SnapshotLayout,
	stagingMetadataPath string,
) (string, int64, error) {
	if snapshot == nil {
		return "", 0, merr.WrapErrServiceInternalMsg("snapshot cannot be nil")
	}
	if snapshot.SnapshotInfo == nil {
		return "", 0, merr.WrapErrDataIntegrityMsg("snapshot info cannot be nil")
	}
	collectionID := snapshot.SnapshotInfo.GetCollectionId()
	if collectionID <= 0 {
		return "", 0, merr.WrapErrServiceInternalMsg("invalid collection ID: %d", collectionID)
	}
	if snapshot.Collection == nil {
		return "", 0, merr.WrapErrDataIntegrityMsg("collection description cannot be nil")
	}
	snapshotID := snapshot.SnapshotInfo.GetId()
	if snapshotID <= 0 {
		return "", 0, merr.WrapErrServiceInternalMsg("invalid snapshot ID: %d", snapshotID)
	}
	if layout == datapb.SnapshotLayout_SnapshotLayoutUnknown {
		layout = datapb.SnapshotLayout_SnapshotLayoutReferenced
	}
	snapshot.Layout = layout

	metadataPath, metadataData, manifestBytes, err := writeBundleManifests(ctx, store, snapshot, rootPath)
	if err != nil {
		return "", 0, err
	}

	stagingMetadataPath = NormalizeObjectPath(stagingMetadataPath)
	if stagingMetadataPath == metadataPath {
		return "", 0, merr.WrapErrServiceInternalMsg("staging metadata path must differ from final metadata path")
	}
	if err := ctx.Err(); err != nil {
		return "", 0, err
	}
	if err := store.Write(ctx, stagingMetadataPath, metadataData); err != nil {
		return "", 0, merr.Wrap(err, "failed to write staged snapshot metadata object")
	}
	staged, err := store.Read(ctx, stagingMetadataPath)
	if err != nil {
		return "", 0, merr.Wrap(err, "failed to verify staged snapshot metadata object")
	}
	if !bytes.Equal(staged, metadataData) {
		return "", 0, merr.WrapErrDataIntegrityMsg("staged snapshot metadata differs from prepared metadata")
	}

	return metadataPath, manifestBytes + int64(len(metadataData)), nil
}

// SaveBundle writes every segment manifest and publishes the metadata JSON
// directly. This is how the server persists a referenced snapshot; an export
// bundle goes through PrepareBundleStaged and CommitBundleMetadata instead, so
// a half-written export is never visible under the published path.
func SaveBundle(
	ctx context.Context,
	store Store,
	snapshot *SnapshotData,
	rootPath string,
	layout datapb.SnapshotLayout,
) (string, int64, error) {
	if snapshot == nil {
		return "", 0, merr.WrapErrServiceInternalMsg("snapshot cannot be nil")
	}
	if snapshot.SnapshotInfo == nil {
		return "", 0, merr.WrapErrDataIntegrityMsg("snapshot info cannot be nil")
	}
	collectionID := snapshot.SnapshotInfo.GetCollectionId()
	if collectionID <= 0 {
		return "", 0, merr.WrapErrServiceInternalMsg("invalid collection ID: %d", collectionID)
	}
	if snapshot.Collection == nil {
		return "", 0, merr.WrapErrDataIntegrityMsg("collection description cannot be nil")
	}
	snapshotID := snapshot.SnapshotInfo.GetId()
	if snapshotID <= 0 {
		return "", 0, merr.WrapErrServiceInternalMsg("invalid snapshot ID: %d", snapshotID)
	}
	if layout == datapb.SnapshotLayout_SnapshotLayoutUnknown {
		layout = datapb.SnapshotLayout_SnapshotLayoutReferenced
	}
	snapshot.Layout = layout

	metadataPath, metadataData, manifestBytes, err := writeBundleManifests(ctx, store, snapshot, rootPath)
	if err != nil {
		return "", 0, err
	}

	// Metadata is the publication marker for a complete snapshot. Recheck the
	// caller context after manifest writes so a canceled operation does not
	// publish a partially prepared snapshot.
	if err := ctx.Err(); err != nil {
		return "", 0, err
	}
	if err := store.Write(ctx, metadataPath, metadataData); err != nil {
		return "", 0, merr.Wrap(err, "failed to write snapshot metadata object")
	}
	return metadataPath, manifestBytes + int64(len(metadataData)), nil
}

// writeBundleManifests writes every segment manifest to its final location and
// marshals the metadata JSON.
func writeBundleManifests(
	ctx context.Context,
	store Store,
	snapshot *SnapshotData,
	rootPath string,
) (string, []byte, int64, error) {
	// The bundle carries the source snapshot's format version, capped at what
	// this build can write: a newer source is rewritten at the newest format
	// this build understands, so the copy never claims a format it cannot emit.
	formatVersion := snapshot.FormatVersion
	if formatVersion <= 0 || formatVersion > SnapshotFormatVersion {
		formatVersion = SnapshotFormatVersion
	}
	snapshot.FormatVersion = formatVersion

	manifestDir, metadataPath := GetSnapshotPaths(rootPath, snapshot.SnapshotInfo.GetCollectionId(), snapshot.SnapshotInfo.GetId())
	manifestPaths := make([]string, 0, len(snapshot.Segments))
	var manifestBytes int64
	for _, segment := range snapshot.Segments {
		manifestPath := GetSegmentManifestPath(manifestDir, segment.GetSegmentId())
		manifestData, err := MarshalSegmentManifest(segment, formatVersion)
		if err != nil {
			return "", nil, 0, merr.WrapErrServiceInternalErr(err, "failed to marshal segment manifest")
		}
		if err := store.Write(ctx, manifestPath, manifestData); err != nil {
			return "", nil, 0, merr.Wrap(err, "failed to write segment manifest object")
		}
		manifestBytes += int64(len(manifestData))
		manifestPaths = append(manifestPaths, manifestPath)
	}

	storagev2Manifests := make([]*datapb.StorageV2SegmentManifest, 0)
	for _, segment := range snapshot.Segments {
		if segment.GetManifestPath() != "" {
			storagev2Manifests = append(storagev2Manifests, &datapb.StorageV2SegmentManifest{
				SegmentId: segment.GetSegmentId(),
				Manifest:  segment.GetManifestPath(),
			})
		}
	}
	metadataData, err := marshalSnapshotMetadata(snapshot, manifestPaths, storagev2Manifests)
	if err != nil {
		return "", nil, 0, err
	}
	return metadataPath, metadataData, manifestBytes, nil
}

// CommitBundleMetadata publishes the staged metadata idempotently. A write
// error is treated as successful when a read-back proves that the expected
// bytes reached the final object.
func CommitBundleMetadata(
	ctx context.Context,
	store Store,
	stagingMetadataPath string,
	metadataPath string,
	metadataURI string,
) (int64, error) {
	stagingMetadataPath = NormalizeObjectPath(stagingMetadataPath)
	metadataPath = NormalizeObjectPath(metadataPath)
	if stagingMetadataPath == "" || metadataPath == "" || metadataURI == "" {
		return 0, merr.WrapErrServiceInternalMsg("staging path, metadata path, and metadata URI are required")
	}
	if stagingMetadataPath == metadataPath {
		return 0, merr.WrapErrServiceInternalMsg("staging metadata path must differ from final metadata path")
	}

	staged, err := store.Read(ctx, stagingMetadataPath)
	if err != nil {
		if errors.Is(err, ErrObjectNotFound) {
			return 0, merr.WrapErrDataIntegrityMsg("staged snapshot metadata object is missing")
		}
		return 0, merr.Wrap(err, "failed to read staged snapshot metadata object")
	}
	if err := validateStagedSnapshotMetadata(staged, metadataURI); err != nil {
		return 0, err
	}

	final, err := store.Read(ctx, metadataPath)
	if err == nil {
		if !bytes.Equal(final, staged) {
			return 0, merr.WrapErrDataIntegrityMsg("published snapshot metadata differs from staged metadata")
		}
		return int64(len(staged)), nil
	}
	if !errors.Is(err, ErrObjectNotFound) {
		return 0, merr.Wrap(err, "failed to inspect published snapshot metadata object")
	}

	writeErr := store.Write(ctx, metadataPath, staged)
	final, readErr := store.Read(ctx, metadataPath)
	if readErr == nil {
		if !bytes.Equal(final, staged) {
			return 0, merr.WrapErrDataIntegrityMsg("published snapshot metadata differs from staged metadata")
		}
		return int64(len(staged)), nil
	}
	if writeErr != nil {
		return 0, merr.Wrap(writeErr, "snapshot metadata write result could not be verified")
	}
	return 0, merr.Wrap(readErr, "failed to verify published snapshot metadata object")
}

func marshalSnapshotMetadata(snapshot *SnapshotData, manifestPaths []string, storagev2Manifests []*datapb.StorageV2SegmentManifest) ([]byte, error) {
	metadata := &datapb.SnapshotMetadata{
		FormatVersion:         int32(snapshot.FormatVersion),
		SnapshotInfo:          snapshot.SnapshotInfo,
		Collection:            snapshot.Collection,
		Indexes:               snapshot.Indexes,
		ManifestList:          manifestPaths,
		Storagev2ManifestList: storagev2Manifests,
		SegmentIds:            snapshot.SegmentIDs,
		BuildIds:              snapshot.BuildIDs,
		Layout:                snapshot.Layout,
	}
	opts := protojson.MarshalOptions{
		Multiline:       true,
		Indent:          "  ",
		UseProtoNames:   true,
		EmitUnpopulated: false,
	}
	jsonData, err := opts.Marshal(metadata)
	if err != nil {
		return nil, merr.WrapErrServiceInternalErr(err, "failed to marshal metadata to JSON")
	}
	return jsonData, nil
}

func validateStagedSnapshotMetadata(data []byte, metadataURI string) error {
	metadata, err := ParseSnapshotMetadata(data)
	if err != nil {
		return merr.WrapErrDataIntegrity(err, "invalid staged snapshot metadata")
	}
	if metadata.GetSnapshotInfo() == nil {
		return merr.WrapErrDataIntegrityMsg("invalid staged snapshot metadata: snapshot info cannot be nil")
	}
	if metadata.GetCollection() == nil {
		return merr.WrapErrDataIntegrityMsg("invalid staged snapshot metadata: collection cannot be nil")
	}
	if metadata.GetLayout() != datapb.SnapshotLayout_SnapshotLayoutSelfContained {
		return merr.WrapErrDataIntegrityMsg("invalid staged snapshot metadata: layout must be self-contained")
	}
	if metadata.GetSnapshotInfo().GetS3Location() != metadataURI {
		return merr.WrapErrDataIntegrityMsg("staged snapshot metadata location does not match the export job")
	}
	return nil
}

// LoadSnapshotData reads one snapshot's metadata JSON and, when requested,
// its per-segment manifests. Paths resolve as written: backup reads source
// snapshots in the Milvus bucket and bundles at the URI it wrote them to, so
// no relocation rebasing happens here.
func LoadSnapshotData(ctx context.Context, store Store, metadataPath string, includeManifests bool) (*SnapshotData, error) {
	if metadataPath == "" {
		return nil, merr.WrapErrServiceInternalMsg("metadata file path cannot be empty")
	}
	data, err := store.Read(ctx, NormalizeObjectPath(metadataPath))
	if err != nil {
		return nil, merr.Wrap(err, "failed to read metadata file")
	}
	metadata, err := ParseSnapshotMetadata(data)
	if err != nil {
		return nil, merr.WrapErrDataIntegrity(err, "invalid snapshot metadata")
	}
	if metadata.GetSnapshotInfo() == nil {
		return nil, merr.WrapErrDataIntegrityMsg("invalid snapshot metadata: snapshot info cannot be nil")
	}
	if metadata.GetCollection() == nil {
		return nil, merr.WrapErrDataIntegrityMsg("invalid snapshot metadata: collection cannot be nil")
	}
	layout := metadata.GetLayout()
	if layout == datapb.SnapshotLayout_SnapshotLayoutUnknown {
		// Metadata written before layout was introduced is the referenced layout:
		// manifests point at the original Milvus files instead of an exported bundle.
		layout = datapb.SnapshotLayout_SnapshotLayoutReferenced
	}

	var segments []*datapb.SegmentDescription
	if includeManifests {
		for _, manifestPath := range metadata.GetManifestList() {
			manifestData, err := store.Read(ctx, NormalizeObjectPath(manifestPath))
			if err != nil {
				return nil, merr.Wrap(err, "failed to read manifest file")
			}
			segment, err := ParseSegmentManifest(manifestData, int(metadata.GetFormatVersion()))
			if err != nil {
				return nil, merr.WrapErrDataIntegrity(err, "invalid snapshot segment manifest")
			}
			segments = append(segments, segment)
		}
		if err := validateSnapshotSegmentIDs(metadata.GetSegmentIds(), segments); err != nil {
			return nil, err
		}
		if err := applyStorageManifestPaths(metadata.GetStoragev2ManifestList(), segments); err != nil {
			return nil, err
		}
	}

	return &SnapshotData{
		SnapshotInfo:  metadata.GetSnapshotInfo(),
		Collection:    metadata.GetCollection(),
		Segments:      segments,
		Indexes:       metadata.GetIndexes(),
		FormatVersion: int(metadata.GetFormatVersion()),
		MetadataPath:  metadataPath,
		ManifestPaths: append([]string(nil), metadata.GetManifestList()...),
		SegmentIDs:    metadata.GetSegmentIds(),
		BuildIDs:      metadata.GetBuildIds(),
		Layout:        layout,
	}, nil
}

func validateSnapshotSegmentIDs(expected []int64, segments []*datapb.SegmentDescription) error {
	// Older snapshot metadata may omit segment_ids. When present, it is the
	// integrity declaration for the manifest set and must match exactly.
	expectedSet := make(map[int64]struct{}, len(expected))
	for _, segmentID := range expected {
		if _, ok := expectedSet[segmentID]; ok {
			return merr.WrapErrDataIntegrityMsg("snapshot segment IDs contain duplicate segment %d", segmentID)
		}
		expectedSet[segmentID] = struct{}{}
	}
	loadedSet := make(map[int64]struct{}, len(segments))
	for index, segment := range segments {
		if segment == nil {
			return merr.WrapErrDataIntegrityMsg("snapshot manifest at index %d produced a nil segment", index)
		}
		segmentID := segment.GetSegmentId()
		if _, ok := loadedSet[segmentID]; ok {
			return merr.WrapErrDataIntegrityMsg("snapshot segment IDs do not match manifests: duplicate manifest for segment %d", segmentID)
		}
		loadedSet[segmentID] = struct{}{}
	}
	if len(expected) == 0 {
		return nil
	}
	for _, segmentID := range expected {
		if _, ok := loadedSet[segmentID]; !ok {
			return merr.WrapErrDataIntegrityMsg("snapshot segment IDs do not match manifests: missing segment %d", segmentID)
		}
	}
	for _, segment := range segments {
		if _, ok := expectedSet[segment.GetSegmentId()]; !ok {
			return merr.WrapErrDataIntegrityMsg("snapshot segment IDs do not match manifests: unexpected segment %d", segment.GetSegmentId())
		}
	}
	return nil
}

func applyStorageManifestPaths(manifestMappings []*datapb.StorageV2SegmentManifest, segments []*datapb.SegmentDescription) error {
	segmentsByID := make(map[int64]*datapb.SegmentDescription, len(segments))
	for _, segment := range segments {
		segmentsByID[segment.GetSegmentId()] = segment
	}
	seen := make(map[int64]struct{}, len(manifestMappings))
	for index, mapping := range manifestMappings {
		if mapping == nil {
			return merr.WrapErrDataIntegrityMsg("storage manifest mapping at index %d cannot be nil", index)
		}
		segmentID := mapping.GetSegmentId()
		if mapping.GetManifest() == "" {
			return merr.WrapErrDataIntegrityMsg("storage manifest mapping for segment %d cannot be empty", segmentID)
		}
		if _, ok := seen[segmentID]; ok {
			return merr.WrapErrDataIntegrityMsg("duplicate storage manifest mapping for segment %d", segmentID)
		}
		segment, ok := segmentsByID[segmentID]
		if !ok {
			return merr.WrapErrDataIntegrityMsg("storage manifest mapping references unknown segment %d", segmentID)
		}
		seen[segmentID] = struct{}{}
		segment.ManifestPath = mapping.GetManifest()
	}
	return nil
}
