package secondary

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/restore/conv"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/log"
)

// The option keys a snapshot import broadcast carries on the wire, which the
// import api refuses from a caller: a coordinator sets them when it turns a
// request into the WAL body, naming the bundle the body's single empty file
// stands for, and the target reconstructs the job from exactly that. A
// secondary restore plays the coordinator's part, so it sets them itself.
const (
	_snapshotSourceURIKey = "_snapshot_source_uri"
	_snapshotLayoutKey    = "_snapshot_layout"

	_snapshotLayoutReferenced    = "referenced"
	_snapshotLayoutSelfContained = "self-contained"

	// _snapshotEndTSKey carries the per-channel end bounds of the import: a
	// json object of physical channel to hybrid timestamp. The import api takes
	// at most one scalar end_ts, which cannot name the different points the
	// channels were flushed at, so the cut travels as a map of its own and the
	// import's own expansion applies it channel by channel.
	_snapshotEndTSKey = "_snapshot_end_ts"
)

// The server bounds the partition mapping option at 64 KiB.
const _snapshotPartitionMappingMaxBytes = 64 * 1024

type snapDMLTaskArgs struct {
	TaskID string

	// PchTS is each physical channel's flush boundary, read off the flush-all
	// messages the backup captured. A snapshot bundle is frozen after that
	// boundary, so it also holds what the source sealed in between; the import
	// is cut at these bounds to hold exactly what the checkpoint the restore
	// replays names.
	PchTS map[string]uint64

	StreamCli  milvus.Stream
	RestfulCli milvus.Restful
}

// collSnapDMLTask restores one collection's data from a snapshot backup the
// way the binlog task does from binlogs, with one difference in what goes
// over the stream: a single snapshot-sourced import message covering the
// whole bundle, where the binlog task sends one message per segment batch.
// The target expands that message into the same import job the import api
// would have created — every partition at once, the snapshot's L0 deletes
// folded by the server-side rewrite — and reads the bundle itself, through
// the external spec when it sits on another storage service. No bytes move
// through this process and nothing is staged.
type collSnapDMLTask struct {
	taskID string

	collBackup *backuppb.CollectionBackupInfo

	// metadataURI is the bundle's metadata as a complete uri, which the target
	// resolves the bundle's files against.
	metadataURI string
	// externalSpec carries the bundle storage's credentials; empty when the
	// target reads the bundle with its own.
	externalSpec string
	// layout is the bundle's, as the import message must name it: the target
	// reconstructs the job from the message alone and reads the bundle only
	// once the job is pending.
	layout string
	// partitionMapping routes every source partition onto its same-named
	// target partition; empty for a partition-key collection, whose rows the
	// server hashes across the partitions it has.
	partitionMapping string
	// partitionIDs are the target partitions the message names, in the order
	// the server pairs them with the mapping's destinations.
	partitionIDs []int64
	// channelEndTS is the flush boundary of every channel the collection lives
	// on, as the json map the target's snapshot import takes. See
	// snapshotChannelEndTS for what the cut is for.
	channelEndTS string

	streamCli  milvus.Stream
	restfulCli milvus.Restful

	logger *zap.Logger
}

// newCollSnapDMLTask shapes the import of one collection's bundle. The
// bundle's layout is read off its metadata here, as the one source read of
// the whole path; the partition routing is derived from the backup meta, which
// is also the DDL the secondary replayed, so the target partition ids are the
// source's. The channel end bounds come from the same flush messages the
// restore forwards as its checkpoint, so the import and the checkpoint name
// the same point of the source stream.
func newCollSnapDMLTask(args snapDMLTaskArgs, collBackup *backuppb.CollectionBackupInfo, metadataURI, externalSpec string, layout datapb.SnapshotLayout) (*collSnapDMLTask, error) {
	collRef := collref.New(collBackup.GetDbName(), collBackup.GetCollectionName())

	channelEndTS, err := snapshotChannelEndTS(collBackup, args.PchTS)
	if err != nil {
		return nil, err
	}

	mapping, partitionIDs, err := snapshotPartitionRouting(collBackup)
	if err != nil {
		return nil, err
	}

	return &collSnapDMLTask{
		taskID: args.TaskID,

		collBackup: collBackup,

		metadataURI:      metadataURI,
		externalSpec:     externalSpec,
		layout:           snapshotLayoutOption(layout),
		partitionMapping: mapping,
		partitionIDs:     partitionIDs,
		channelEndTS:     channelEndTS,

		streamCli:  args.StreamCli,
		restfulCli: args.RestfulCli,

		logger: log.With(zap.String("task_id", args.TaskID), zap.String("coll", collRef.String())),
	}, nil
}

// snapshotChannelEndTS renders the flush boundary of every channel the
// collection lives on as the json object a snapshot import takes, physical
// channel to hybrid timestamp.
//
// A snapshot bundle is frozen after the backup's flush, so it carries the rows
// the source sealed in between; the restore replays the captured flush-all
// messages as the secondary's replication checkpoint, and the CDC resumes
// there. Without the cut everything in the bundle is imported and the rows past
// the checkpoint are replayed a second time, duplicating them. A channel whose
// boundary cannot be resolved is refused rather than imported uncut: the
// duplicate it would produce is the failure this cut exists to prevent.
func snapshotChannelEndTS(collBackup *backuppb.CollectionBackupInfo, pchTS map[string]uint64) (string, error) {
	vchs := collBackup.GetVirtualChannelNames()
	if len(vchs) == 0 {
		return "", fmt.Errorf("secondary: collection %s carries no virtual channels",
			collref.New(collBackup.GetDbName(), collBackup.GetCollectionName()))
	}

	endTS := make(map[string]string, len(vchs))
	for _, vch := range vchs {
		pch := funcutil.ToPhysicalChannel(vch)
		ts, ok := pchTS[pch]
		if !ok {
			return "", fmt.Errorf("secondary: no flush all ts for pch %s", pch)
		}
		endTS[pch] = strconv.FormatUint(ts, 10)
	}

	bs, err := json.Marshal(endTS)
	if err != nil {
		return "", fmt.Errorf("secondary: marshal the channel end bounds: %w", err)
	}

	return string(bs), nil
}

// snapshotLayoutOption renders a bundle layout as the option value the import
// message carries. A bundle written before layouts were recorded is the
// referenced layout, which the bundle reader already resolves it to.
func snapshotLayoutOption(layout datapb.SnapshotLayout) string {
	if layout == datapb.SnapshotLayout_SnapshotLayoutSelfContained {
		return _snapshotLayoutSelfContained
	}
	return _snapshotLayoutReferenced
}

func (st *collSnapDMLTask) Execute(ctx context.Context) error {
	st.logger.Info("start restore collection dml from snapshot",
		zap.String("metadata_uri", st.metadataURI),
		zap.String("layout", st.layout),
		zap.Bool("external_spec_set", st.externalSpec != ""),
		zap.Int64s("partition_ids", st.partitionIDs))

	jobID, err := st.sendImportMsg(ctx)
	if err != nil {
		return fmt.Errorf("secondary: send snapshot import msg: %w", err)
	}

	if err := st.importJob().check(ctx, jobID); err != nil {
		return fmt.Errorf("secondary: check snapshot import job %d: %w", jobID, err)
	}

	st.logger.Info("restore collection dml from snapshot done", zap.Int64("job_id", jobID))
	return nil
}

// options is the snapshot import contract as the target's coordinator would
// have written it into the WAL body: the public keys of a snapshot-sourced
// backup import, the two reference keys that name the bundle, and the
// per-channel end bounds the target cuts it at. The scalar end_ts is not sent
// — one value cannot name the different points the channels were flushed at —
// and the L0 flag and storage_version stay unset: the snapshot's own metadata
// is the authority for both, and the server rejects them next to a snapshot
// source. So does the ezk: the server reads the source's key off the
// snapshot's own schema and drops any the request carries.
func (st *collSnapDMLTask) options() map[string]string {
	opts := map[string]string{
		"skip_disk_quota_check": "true",
		"auto_commit":           "false",
		"backup":                "true",
		"source_type":           milvus.SourceTypeSnapshot,
		_snapshotSourceURIKey:   st.metadataURI,
		_snapshotLayoutKey:      st.layout,
		_snapshotEndTSKey:       st.channelEndTS,
	}
	if st.externalSpec != "" {
		opts["external_spec"] = st.externalSpec
	}
	if st.partitionMapping != "" {
		opts["partition_mapping"] = st.partitionMapping
	}

	return opts
}

// sendImportMsg broadcasts the one import message of the collection and
// returns the job id it minted into it. The body carries exactly one file
// with no path, which is how a coordinator encodes a snapshot source whose
// reference lives in the options; a target older than the contract rejects
// the empty path instead of reading metadata as row data.
func (st *collSnapDMLTask) sendImportMsg(ctx context.Context) (int64, error) {
	jobID := rand.Int64()

	schema, err := conv.Schema(st.collBackup.GetSchema())
	if err != nil {
		return 0, fmt.Errorf("secondary: convert schema: %w", err)
	}
	if err := checkDynamicField(schema); err != nil {
		return 0, err
	}
	appendSysFields(schema)

	err = st.streamCli.Send(ctx, func(ts uint64) []message.MutableMessage {
		header := &message.ImportMessageHeader{}
		body := &message.ImportMsg{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Import,
				Timestamp: ts,
			},
			Options:        st.options(),
			DbName:         st.collBackup.GetDbName(),
			CollectionName: st.collBackup.GetCollectionName(),
			CollectionID:   st.collBackup.GetCollectionId(),
			PartitionIDs:   st.partitionIDs,
			Files:          []*msgpb.ImportFile{{Id: 1}},
			Schema:         schema,
			JobID:          jobID,
		}

		builder := message.NewImportMessageBuilderV1().
			WithHeader(header).
			WithBody(body).
			WithBroadcast(st.collBackup.GetVirtualChannelNames())

		broadcast := builder.MustBuildBroadcast().WithBroadcastID(rand.Uint64())
		return broadcast.SplitIntoMutableMessage()
	})
	if err != nil {
		return 0, fmt.Errorf("secondary: broadcast import: %w", err)
	}

	return jobID, nil
}

// importJob is the lifecycle view of the import job this collection submits,
// read off the task's fields at call time so a client swapped in later is
// honored.
func (st *collSnapDMLTask) importJob() bulkInsertJob {
	return bulkInsertJob{
		dbName:       st.collBackup.GetDbName(),
		collectionID: st.collBackup.GetCollectionId(),
		vchannels:    st.collBackup.GetVirtualChannelNames(),

		streamCli:  st.streamCli,
		restfulCli: st.restfulCli,

		logger: st.logger,
	}
}

// snapshotPartitionRouting derives what the import message says about
// partitions: the identity mapping of every source partition onto its
// same-named target, as the json the import api takes, and the target
// partition ids in the order the server pairs with it — the mapping's
// destination names sorted, the way the proxy resolves them for a request.
// Keeping the backup's partition layout is what the mapping is for: without
// it the import routes every row of an ordinary collection into _default. A
// secondary's replayed DDL recreates the partitions with the source's ids, so
// those are the target ids.
//
// A partition-key collection takes no mapping — the server hashes its rows
// across the partitions it has — and the message names every partition,
// arranged by partition-key index as the proxy arranges them.
func snapshotPartitionRouting(collBackup *backuppb.CollectionBackupInfo) (string, []int64, error) {
	schema, err := conv.Schema(collBackup.GetSchema())
	if err != nil {
		return "", nil, fmt.Errorf("secondary: convert schema: %w", err)
	}

	parts := collBackup.GetPartitionBackups()
	if typeutil.HasPartitionKey(schema) {
		byName := make(map[string]int64, len(parts))
		for _, part := range parts {
			byName[part.GetPartitionName()] = part.GetPartitionId()
		}
		_, ids, err := typeutil.RearrangePartitionsForPartitionKey(byName)
		if err != nil {
			return "", nil, fmt.Errorf("secondary: arrange the partition-key partitions: %w", err)
		}
		return "", ids, nil
	}

	sorted := slices.SortedFunc(slices.Values(parts), func(a, b *backuppb.PartitionBackupInfo) int {
		return strings.Compare(a.GetPartitionName(), b.GetPartitionName())
	})
	mapping := make(map[string]string, len(sorted))
	ids := make([]int64, 0, len(sorted))
	for _, part := range sorted {
		mapping[part.GetPartitionName()] = part.GetPartitionName()
		ids = append(ids, part.GetPartitionId())
	}
	if len(mapping) == 0 {
		return "", nil, nil
	}

	bs, err := json.Marshal(mapping)
	if err != nil {
		return "", nil, fmt.Errorf("secondary: marshal partition mapping: %w", err)
	}
	// Fail where the oversized input can be named rather than at job submission.
	if len(bs) > _snapshotPartitionMappingMaxBytes {
		return "", nil, fmt.Errorf("secondary: the partition mapping of %d partitions is %d bytes, over the 64 KiB the import api accepts",
			len(mapping), len(bs))
	}

	return string(bs), ids, nil
}
