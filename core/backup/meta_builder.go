package backup

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/pbconv"
)

type metaBuilder struct {
	mu sync.Mutex

	data *backuppb.BackupInfo

	// index
	nameToCollID      map[collref.Name]int64
	collectionBackups map[int64]*backuppb.CollectionBackupInfo       // coll id - > collection backup info
	partitionBackups  map[partitionKey]*backuppb.PartitionBackupInfo // coll id -> part id - > partition backup info

	// pendings hold the data-side writes that arrived before their
	// collection's DDL did: DDL and DML tasks run independently, so a
	// segment list, a POS record or a snapshot bundle can show up while
	// the collection entry is not there yet. addCollection drains its own
	// collection's pendings as it registers; anything still pending at
	// build time means a collection's DDL never landed, which is a bug.
	pendingSegments map[int64][]*backuppb.SegmentBackupInfo // coll id -> segments
	pendingPOS      map[collref.Name]posRecord
	pendingSnapshot map[collref.Name]snapshotRecord
}

type partitionKey struct {
	collectionID int64
	partitionID  int64
}

type posRecord struct {
	channelCP    map[string]string
	maxChannelTS uint64
	sealTime     uint64
}

type snapshotRecord struct {
	snapshot   *backuppb.SnapshotBackupInfo
	snapshotTs uint64

	// dynField is the $meta field as the bundle's schema carries it, nil when
	// the collection has no dynamic schema. The snapshot metadata holds the
	// internal schema, which keeps the dynamic field the public
	// DescribeCollection hides from the DDL half.
	dynField *backuppb.FieldSchema
}

func newMetaBuilder(taskID, backupName string) *metaBuilder {
	info := &backuppb.BackupInfo{Id: taskID, Name: backupName}
	return &metaBuilder{
		data: info,

		nameToCollID:      make(map[collref.Name]int64),
		collectionBackups: make(map[int64]*backuppb.CollectionBackupInfo),
		partitionBackups:  make(map[partitionKey]*backuppb.PartitionBackupInfo),

		pendingSegments: make(map[int64][]*backuppb.SegmentBackupInfo),
		pendingPOS:      make(map[collref.Name]posRecord),
		pendingSnapshot: make(map[collref.Name]snapshotRecord),
	}
}

func (builder *metaBuilder) setVersion(version string) {
	builder.mu.Lock()
	defer builder.mu.Unlock()

	builder.data.MilvusVersion = version
}

func (builder *metaBuilder) setClusterInfoAndTSS(cch string, pchs []string, flushAllTss map[string]string) {
	builder.mu.Lock()
	defer builder.mu.Unlock()

	builder.data.FlushAllMsgsBase64 = flushAllTss
	builder.data.PhysicalChannelNames = pchs
	builder.data.ControlChannelName = cch
}

func (builder *metaBuilder) addDatabase(databaseBackup *backuppb.DatabaseBackupInfo) {
	builder.mu.Lock()
	defer builder.mu.Unlock()

	builder.data.DatabaseBackups = append(builder.data.DatabaseBackups, databaseBackup)
}

func (builder *metaBuilder) addCollection(collRef collref.Name, collectionBackup *backuppb.CollectionBackupInfo) {
	builder.mu.Lock()
	defer builder.mu.Unlock()

	builder.data.CollectionBackups = append(builder.data.CollectionBackups, collectionBackup)
	// add to index
	builder.nameToCollID[collRef] = collectionBackup.GetCollectionId()
	builder.collectionBackups[collectionBackup.GetCollectionId()] = collectionBackup
	for _, partition := range collectionBackup.GetPartitionBackups() {
		key := partitionKey{collectionID: collectionBackup.GetCollectionId(), partitionID: partition.GetPartitionId()}
		builder.partitionBackups[key] = partition
	}

	builder.drainPendings(collRef, collectionBackup)
}

// drainPendings applies the data-side writes that beat this collection's DDL
// to the now-registered entry. The lock is the caller's.
func (builder *metaBuilder) drainPendings(collRef collref.Name, collectionBackup *backuppb.CollectionBackupInfo) {
	collID := collectionBackup.GetCollectionId()

	for _, segment := range builder.pendingSegments[collID] {
		builder.attachSegment(segment)
	}
	delete(builder.pendingSegments, collID)

	if pos, ok := builder.pendingPOS[collRef]; ok {
		setCollPOS(collectionBackup, pos)
		delete(builder.pendingPOS, collRef)
	}

	if snapshot, ok := builder.pendingSnapshot[collRef]; ok {
		setCollSnapshot(collectionBackup, snapshot)
		delete(builder.pendingSnapshot, collRef)
	}
}

// checkDrained refuses to build meta while data-side writes are still
// unclaimed. Every covered collection runs a DDL task, so a leftover means a
// DDL failed while the job went on — building anyway would silently drop
// data from the meta.
func (builder *metaBuilder) checkDrained() error {
	if len(builder.pendingSegments) != 0 || len(builder.pendingPOS) != 0 || len(builder.pendingSnapshot) != 0 {
		return fmt.Errorf("backup: %d segment lists, %d pos records, %d snapshot bundles never claimed by a collection",
			len(builder.pendingSegments), len(builder.pendingPOS), len(builder.pendingSnapshot))
	}

	return nil
}

// setFormat records which format the backup is being written in. Empty, the default,
// is the binlog format.
func (builder *metaBuilder) setFormat(format string) {
	builder.mu.Lock()
	defer builder.mu.Unlock()

	builder.data.Format = format
}

// addSnapshot attaches a collection's exported bundle. snapshotTs is the snapshot's
// own boundary as Milvus reports it, which is what backup_timestamp means for this
// format — the flush response the binlog path reads it from has no counterpart here.
// bundleSchema is the collection schema inside the snapshot metadata: its dynamic
// field is the one the DDL half cannot get from DescribeCollection, and it is recorded
// when the meta still lacks it. A bundle that arrives before its collection's DDL
// waits in the pendings.
func (builder *metaBuilder) addSnapshot(collRef collref.Name, snapshot *backuppb.SnapshotBackupInfo, snapshotTs uint64, bundleSchema *schemapb.CollectionSchema) error {
	dynField, err := bundleDynField(bundleSchema)
	if err != nil {
		return fmt.Errorf("backup: convert dynamic field in snapshot bundle of %s: %w", collRef.String(), err)
	}
	record := snapshotRecord{snapshot: snapshot, snapshotTs: snapshotTs, dynField: dynField}

	builder.mu.Lock()
	defer builder.mu.Unlock()

	collID := builder.nameToCollID[collRef]
	collBackup, ok := builder.collectionBackups[collID]
	if !ok {
		builder.pendingSnapshot[collRef] = record
		return nil
	}

	setCollSnapshot(collBackup, record)
	return nil
}

func setCollSnapshot(collBackup *backuppb.CollectionBackupInfo, record snapshotRecord) {
	collBackup.SnapshotBackup = record.snapshot
	collBackup.BackupTimestamp = record.snapshotTs
	collBackup.Size = record.snapshot.GetTotalBytes()

	// The $meta field is recorded once: the bundle and the etcd scan are two
	// sources for the same field, and whichever lands first wins.
	if record.dynField != nil &&
		collBackup.GetSchema().GetEnableDynamicField() &&
		!hasDynField(collBackup.GetSchema().GetFields()) {
		collBackup.Schema.Fields = append(collBackup.Schema.Fields, record.dynField)
	}
}

// bundleDynField picks the dynamic field out of a snapshot bundle's collection
// schema, converted for the backup meta. Nil when the schema carries none.
func bundleDynField(schema *schemapb.CollectionSchema) (*backuppb.FieldSchema, error) {
	for _, field := range schema.GetFields() {
		if field.GetIsDynamic() {
			return convDynField(field)
		}
	}

	return nil, nil
}

func hasDynField(fields []*backuppb.FieldSchema) bool {
	for _, field := range fields {
		if field.GetIsDynamic() {
			return true
		}
	}

	return false
}

func (builder *metaBuilder) addPOS(collRef collref.Name, channelCP map[string]string, maxChannelTS uint64, sealTime uint64) {
	builder.mu.Lock()
	defer builder.mu.Unlock()

	record := posRecord{channelCP: channelCP, maxChannelTS: maxChannelTS, sealTime: sealTime}

	collID := builder.nameToCollID[collRef]
	collBackup, ok := builder.collectionBackups[collID]
	if !ok {
		builder.pendingPOS[collRef] = record
		return
	}

	setCollPOS(collBackup, record)
}

func setCollPOS(collBackup *backuppb.CollectionBackupInfo, record posRecord) {
	collBackup.ChannelCheckpoints = record.channelCP
	collBackup.BackupTimestamp = record.maxChannelTS
	collBackup.BackupPhysicalTimestamp = record.sealTime
}

func (builder *metaBuilder) addSegments(segments []*backuppb.SegmentBackupInfo) {
	builder.mu.Lock()
	defer builder.mu.Unlock()

	for _, segment := range segments {
		if _, ok := builder.collectionBackups[segment.GetCollectionId()]; !ok {
			builder.pendingSegments[segment.GetCollectionId()] = append(builder.pendingSegments[segment.GetCollectionId()], segment)
			continue
		}

		builder.attachSegment(segment)
	}
}

// attachSegment hangs the segment on the collection/partition tree and
// accounts its size. The lock is the caller's, and the collection entry must
// exist: callers either checked or drained. A segment whose partition the
// collection does not know is dropped with a warning instead — that is the
// milvus#47145/#47154 case, not an ordering problem.
func (builder *metaBuilder) attachSegment(segment *backuppb.SegmentBackupInfo) {
	collBackup := builder.collectionBackups[segment.GetCollectionId()]

	builder.data.Size += segment.GetSize()
	collBackup.Size += segment.GetSize()

	if segment.GetIsL0() && segment.GetPartitionId() == _allPartitionID {
		collBackup.L0Segments = append(collBackup.L0Segments, segment)
		return
	}

	key := partitionKey{collectionID: segment.GetCollectionId(), partitionID: segment.GetPartitionId()}
	partBackup, ok := builder.partitionBackups[key]
	if !ok {
		log.Warn("partition not found, may be milvus bug, see milvus#47145 and milvus#47154",
			zap.Int64("collection_id", segment.GetCollectionId()),
			zap.Int64("partition_id", segment.GetPartitionId()),
			zap.Int64("segment_id", segment.GetSegmentId()))

		return
	}
	partBackup.SegmentBackups = append(partBackup.SegmentBackups, segment)
	partBackup.Size += segment.GetSize()
}

// backupCollectionIDs returns the collection ids that have been captured by
// this backup. The index extra task uses it to scope the etcd field-index scan
// to the backed-up collections only.
func (builder *metaBuilder) backupCollectionIDs() []int64 {
	builder.mu.Lock()
	defer builder.mu.Unlock()

	colls := builder.data.GetCollectionBackups()
	ids := make([]int64, 0, len(colls))
	for _, coll := range colls {
		ids = append(ids, coll.GetCollectionId())
	}

	return ids
}

// collectionsMissingDynField names, in db.collection form, the backed-up
// collections that have dynamic schema enabled but whose recorded schema still
// lacks the $meta field. A snapshot-format collection records it from its own
// bundle; anything else can get it only through the etcd-backed dynamic field
// task, so this is what a caller needs in order to say which collections an
// absent task leaves incomplete.
func (builder *metaBuilder) collectionsMissingDynField() []string {
	builder.mu.Lock()
	defer builder.mu.Unlock()

	var names []string
	for _, coll := range builder.data.GetCollectionBackups() {
		if coll.GetSchema().GetEnableDynamicField() && !hasDynField(coll.GetSchema().GetFields()) {
			names = append(names, coll.GetDbName()+"."+coll.GetCollectionName())
		}
	}
	sort.Strings(names)

	return names
}

// addIndexExtraInfo merges the extra index attributes read from etcd into the
// index infos already collected via DescribeIndex. The caller must only pass
// indexes that belong to backed-up collections (the etcd scan is scoped per
// collection). Every index is expected to have a matching IndexInfo; a miss
// means etcd and DescribeIndex disagree, which would produce an incomplete
// backup that breaks CDC secondary restore, so it is reported as an error
// rather than silently skipped.
func (builder *metaBuilder) addIndexExtraInfo(indexes []*indexpb.FieldIndex) error {
	builder.mu.Lock()
	defer builder.mu.Unlock()

	// group index by collection id and index id
	indexMap := make(map[int64]map[int64]*backuppb.IndexInfo)
	for _, coll := range builder.data.GetCollectionBackups() {
		indexMap[coll.GetCollectionId()] = make(map[int64]*backuppb.IndexInfo)
		for _, index := range coll.GetIndexInfos() {
			indexMap[coll.GetCollectionId()][index.GetIndexId()] = index
		}
	}

	for _, index := range indexes {
		info := index.GetIndexInfo()
		collIndexes, ok := indexMap[info.GetCollectionID()]
		if !ok {
			return fmt.Errorf("backup: index extra info references collection %d not in backup", info.GetCollectionID())
		}
		indexInfo, ok := collIndexes[info.GetIndexID()]
		if !ok {
			return fmt.Errorf("backup: index %d of collection %d exists in etcd but not in backup index infos",
				info.GetIndexID(), info.GetCollectionID())
		}

		indexInfo.FieldId = info.GetFieldID()
		indexInfo.TypeParams = pbconv.MilvusKVToBakKV(info.GetTypeParams())
		indexInfo.CreateTime = index.GetCreateTime()
		indexInfo.IndexParams = pbconv.MilvusKVToBakKV(info.GetIndexParams())
		indexInfo.UserIndexParams = pbconv.MilvusKVToBakKV(info.GetUserIndexParams())
		indexInfo.IsAutoIndex = info.GetIsAutoIndex()
		indexInfo.MinIndexVersion = info.GetMinIndexVersion()
		indexInfo.MaxIndexVersion = info.GetMaxIndexVersion()
	}

	return nil
}

// addDynamicFields injects the dynamic field schema (read directly from etcd)
// into each collection backup that has dynamic schema enabled. The Milvus
// DescribeCollection API filters out IsDynamic fields, so without this the
// backup metadata is missing the actual $meta field attributes (Nullable,
// DefaultValue) and secondary restore would have to reconstruct it blindly.
// See zilliztech/milvus-backup#1013.
//
// A collection that already carries the field — recorded from its snapshot
// bundle — is left alone: the bundle's copy is the schema the data was written
// under, and a second $meta would be a corrupt schema.
//
// Returns an error if any collection has EnableDynamicField=true but the
// caller did not supply a matching dynamic field. We treat this as a hard
// failure rather than silently producing an incomplete backup, since the
// resulting file would break secondary restore.
func (builder *metaBuilder) addDynamicFields(dynFields map[int64]*schemapb.FieldSchema) error {
	builder.mu.Lock()
	defer builder.mu.Unlock()

	for _, coll := range builder.data.GetCollectionBackups() {
		if !coll.GetSchema().GetEnableDynamicField() || hasDynField(coll.GetSchema().GetFields()) {
			continue
		}

		dynField, ok := dynFields[coll.GetCollectionId()]
		if !ok {
			return fmt.Errorf("backup: %q missing dynamic field in etcd", coll.GetCollectionName())
		}

		bakField, err := convDynField(dynField)
		if err != nil {
			return fmt.Errorf("backup: convert dynamic field of %q: %w", coll.GetCollectionName(), err)
		}
		coll.Schema.Fields = append(coll.Schema.Fields, bakField)
	}

	return nil
}

// convDynField converts a milvus schemapb.FieldSchema (read from etcd) into
// the backup proto FieldSchema. It only fills the subset of attributes that
// matter for the dynamic field ($meta).
func convDynField(field *schemapb.FieldSchema) (*backuppb.FieldSchema, error) {
	var defaultValueBase64 string
	if field.GetDefaultValue() != nil {
		bytes, err := proto.Marshal(field.GetDefaultValue())
		if err != nil {
			return nil, fmt.Errorf("backup: marshal dynamic field default value: %w", err)
		}
		defaultValueBase64 = base64.StdEncoding.EncodeToString(bytes)
	}

	return &backuppb.FieldSchema{
		FieldID:            field.GetFieldID(),
		Name:               field.GetName(),
		IsPrimaryKey:       field.GetIsPrimaryKey(),
		Description:        field.GetDescription(),
		AutoID:             field.GetAutoID(),
		DataType:           backuppb.DataType(field.GetDataType()),
		TypeParams:         pbconv.MilvusKVToBakKV(field.GetTypeParams()),
		IndexParams:        pbconv.MilvusKVToBakKV(field.GetIndexParams()),
		IsDynamic:          field.GetIsDynamic(),
		IsPartitionKey:     field.GetIsPartitionKey(),
		Nullable:           field.GetNullable(),
		ElementType:        backuppb.DataType(field.GetElementType()),
		IsFunctionOutput:   field.GetIsFunctionOutput(),
		DefaultValueBase64: defaultValueBase64,
	}, nil
}

func (builder *metaBuilder) setRBACMeta(rbacMeta *backuppb.RBACMeta) {
	builder.mu.Lock()
	defer builder.mu.Unlock()

	builder.data.RbacMeta = rbacMeta
}

func (builder *metaBuilder) setRPCChannelInfo(rpcChannelInfo *backuppb.RPCChannelInfo) {
	builder.mu.Lock()
	defer builder.mu.Unlock()

	builder.data.RpcChannelInfo = rpcChannelInfo
}

func (builder *metaBuilder) buildBackupMeta() ([]byte, error) {
	builder.mu.Lock()
	defer builder.mu.Unlock()

	if err := builder.checkDrained(); err != nil {
		return nil, err
	}

	info := &backuppb.BackupInfo{
		Id:              builder.data.GetId(),
		Name:            builder.data.GetName(),
		Format:          builder.data.GetFormat(),
		BackupTimestamp: builder.data.GetBackupTimestamp(),
		Size:            builder.data.GetSize(),
		MilvusVersion:   builder.data.GetMilvusVersion(),
	}

	data, err := json.Marshal(info)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (builder *metaBuilder) buildFullMeta() ([]byte, error) {
	builder.mu.Lock()
	defer builder.mu.Unlock()

	if err := builder.checkDrained(); err != nil {
		return nil, err
	}

	data, err := json.Marshal(builder.data)
	if err != nil {
		return nil, fmt.Errorf("backup: build full meta: %w", err)
	}

	return data, nil
}

func (builder *metaBuilder) buildCollectionMeta() ([]byte, error) {
	builder.mu.Lock()
	defer builder.mu.Unlock()

	if err := builder.checkDrained(); err != nil {
		return nil, err
	}

	// Shallow-copy each collection, keep partition metadata but strip all segment data.
	// Segments are stored separately in segment_meta.json and full_meta.json.
	stripped := make([]*backuppb.CollectionBackupInfo, 0, len(builder.data.GetCollectionBackups()))
	for _, coll := range builder.data.GetCollectionBackups() {
		c := *coll
		c.L0Segments = nil
		strippedParts := make([]*backuppb.PartitionBackupInfo, 0, len(coll.GetPartitionBackups()))
		for _, part := range coll.GetPartitionBackups() {
			p := *part
			p.SegmentBackups = nil
			strippedParts = append(strippedParts, &p)
		}
		c.PartitionBackups = strippedParts
		stripped = append(stripped, &c)
	}

	info := &backuppb.CollectionLevelBackupInfo{Infos: stripped}
	data, err := json.Marshal(info)
	if err != nil {
		return nil, fmt.Errorf("backup: build collection meta: %w", err)
	}

	return data, nil
}

func (builder *metaBuilder) buildPartitionMeta() ([]byte, error) {
	builder.mu.Lock()
	defer builder.mu.Unlock()

	if err := builder.checkDrained(); err != nil {
		return nil, err
	}

	// Shallow-copy each partition and strip nested segment data.
	partitions := make([]*backuppb.PartitionBackupInfo, 0)
	for _, coll := range builder.data.GetCollectionBackups() {
		for _, part := range coll.GetPartitionBackups() {
			p := *part
			p.SegmentBackups = nil
			partitions = append(partitions, &p)
		}
	}

	info := &backuppb.PartitionLevelBackupInfo{Infos: partitions}
	data, err := json.Marshal(info)
	if err != nil {
		return nil, fmt.Errorf("backup: build partition meta: %w", err)
	}

	return data, nil
}

func (builder *metaBuilder) buildSegmentMeta() ([]byte, error) {
	builder.mu.Lock()
	defer builder.mu.Unlock()

	if err := builder.checkDrained(); err != nil {
		return nil, err
	}

	segments := make([]*backuppb.SegmentBackupInfo, 0)
	for _, collection := range builder.data.GetCollectionBackups() {
		for _, partition := range collection.GetPartitionBackups() {
			segments = append(segments, partition.GetSegmentBackups()...)
		}
	}

	info := &backuppb.SegmentLevelBackupInfo{Infos: segments}
	data, err := json.Marshal(info)
	if err != nil {
		return nil, fmt.Errorf("backup: build segment meta: %w", err)
	}

	return data, nil
}
