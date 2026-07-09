// Package wash folds a backup's L0 (delete-only) segments into per-data-segment
// deltalogs — the offline equivalent of Milvus L0 compaction's fan-out. It does
// NOT rewrite insert data: the deletes are written as additional deltalogs on
// the data segments, and a plain restore drops the deleted rows at read time via
// the existing per-segment delta path. After a wash the backup contains no L0
// segments, so it restores correctly on any Milvus version (no new import
// option, no version skew in the shared datanode pool) and its data segments are
// commit_timestamp-safe.
//
// The deltalog codec itself lives in a separate Milvus binary (`l0fanout`); this
// package computes the fan-out plan, invokes that binary, folds the results back
// into the backup metadata, and drops the L0 segments.
package wash

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/meta"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
)

// allPartitionID marks a collection-wide (all-partitions) L0 segment.
const allPartitionID int64 = -1

// -------- JSON contract with the Milvus `l0fanout` binary --------

type fanoutPlan struct {
	Tasks []fanoutTask `json:"tasks"`
}

type fanoutTask struct {
	L0DeltaPaths     []string       `json:"l0DeltaPaths"`
	L0StorageVersion int64          `json:"l0StorageVersion"`
	PkType           string         `json:"pkType"`
	Targets          []fanoutTarget `json:"targets"`
}

type fanoutTarget struct {
	CollectionID   int64  `json:"collectionID"`
	PartitionID    int64  `json:"partitionID"`
	SegmentID      int64  `json:"segmentID"`
	LogID          int64  `json:"logID"`
	OutputPath     string `json:"outputPath"`
	StorageVersion int64  `json:"storageVersion"`
}

type fanoutResult struct {
	Written []fanoutWritten `json:"written"`
}

type fanoutWritten struct {
	SegmentID     int64  `json:"segmentID"`
	LogID         int64  `json:"logID"`
	Path          string `json:"path"`
	EntriesNum    int64  `json:"entriesNum"`
	MemorySize    int64  `json:"memorySize"`
	TimestampFrom uint64 `json:"timestampFrom"`
	TimestampTo   uint64 `json:"timestampTo"`
}

// Task washes one backup.
type Task struct {
	cli         storage.Client
	backupDir   string
	params      *cfg.Config
	l0fanoutBin string
}

func NewTask(cli storage.Client, backupDir string, params *cfg.Config, l0fanoutBin string) *Task {
	return &Task{cli: cli, backupDir: backupDir, params: params, l0fanoutBin: l0fanoutBin}
}

func (t *Task) Execute(ctx context.Context) error {
	info, err := meta.Read(ctx, t.cli, t.backupDir)
	if err != nil {
		return fmt.Errorf("wash: read backup meta: %w", err)
	}

	plan, dataSegByID, err := t.buildPlan(ctx, info)
	if err != nil {
		return fmt.Errorf("wash: build plan: %w", err)
	}
	if len(plan.Tasks) == 0 {
		log.Info("wash: no L0 segments to fold, nothing to do", zap.String("backupDir", t.backupDir))
		return nil
	}

	result, err := t.runFanout(ctx, plan)
	if err != nil {
		return fmt.Errorf("wash: run l0fanout: %w", err)
	}

	// Fold the written deltalogs onto the data segments.
	for _, w := range result.Written {
		seg := dataSegByID[w.SegmentID]
		if seg == nil {
			return fmt.Errorf("wash: l0fanout reported unknown segment %d", w.SegmentID)
		}
		seg.Deltalogs = append(seg.Deltalogs, &backuppb.FieldBinlog{
			Binlogs: []*backuppb.Binlog{{
				LogId:         w.LogID,
				LogPath:       w.Path,
				LogSize:       w.MemorySize,
				EntriesNum:    w.EntriesNum,
				TimestampFrom: w.TimestampFrom,
				TimestampTo:   w.TimestampTo,
			}},
		})
	}

	dropL0(info)

	if err := meta.Write(ctx, t.cli, t.backupDir, info); err != nil {
		return fmt.Errorf("wash: write backup meta: %w", err)
	}
	log.Info("wash: done",
		zap.String("backupDir", t.backupDir),
		zap.Int("foldedDeltalogs", len(result.Written)))
	return nil
}

// buildPlan computes the fan-out plan and a segmentID -> data-segment index (for
// folding the results back). Partition-level L0 segments fan into same-partition
// data segments; collection-level L0 segments (partitionID == allPartitionID)
// fan into every data segment of the collection.
func (t *Task) buildPlan(ctx context.Context, info *backuppb.BackupInfo) (*fanoutPlan, map[int64]*backuppb.SegmentBackupInfo, error) {
	plan := &fanoutPlan{}
	dataSegByID := make(map[int64]*backuppb.SegmentBackupInfo)
	var logSeq int64

	for _, coll := range info.GetCollectionBackups() {
		collID := coll.GetCollectionId()
		pkType, err := pkTypeName(coll)
		if err != nil {
			return nil, nil, fmt.Errorf("collection %d: %w", collID, err)
		}

		// Index data segments (non-L0) per partition and globally.
		dataByPart := make(map[int64][]*backuppb.SegmentBackupInfo)
		var allData []*backuppb.SegmentBackupInfo
		for _, part := range coll.GetPartitionBackups() {
			for _, seg := range part.GetSegmentBackups() {
				if seg.GetIsL0() {
					continue
				}
				dataByPart[part.GetPartitionId()] = append(dataByPart[part.GetPartitionId()], seg)
				allData = append(allData, seg)
				dataSegByID[seg.GetSegmentId()] = seg
			}
		}

		// Collect L0 segments: partition-scoped (inside partitions) and
		// collection-scoped (CollectionBackupInfo.L0Segments).
		addTask := func(l0 *backuppb.SegmentBackupInfo, targets []*backuppb.SegmentBackupInfo) error {
			if len(targets) == 0 {
				return nil
			}
			deltaPaths, err := t.listL0Deltalogs(ctx, collID, l0)
			if err != nil {
				return err
			}
			if len(deltaPaths) == 0 {
				return nil
			}
			ft := fanoutTask{
				L0DeltaPaths:     deltaPaths,
				L0StorageVersion: l0.GetStorageVersion(),
				PkType:           pkType,
			}
			for _, seg := range targets {
				logSeq++
				logID := logID(l0.GetSegmentId(), logSeq)
				ft.Targets = append(ft.Targets, fanoutTarget{
					CollectionID:   collID,
					PartitionID:    seg.GetPartitionId(),
					SegmentID:      seg.GetSegmentId(),
					LogID:          logID,
					OutputPath:     t.deltalogKey(collID, seg.GetPartitionId(), seg.GetSegmentId(), logID),
					StorageVersion: seg.GetStorageVersion(),
				})
			}
			plan.Tasks = append(plan.Tasks, ft)
			return nil
		}

		for _, part := range coll.GetPartitionBackups() {
			for _, seg := range part.GetSegmentBackups() {
				if !seg.GetIsL0() {
					continue
				}
				if err := addTask(seg, dataByPart[part.GetPartitionId()]); err != nil {
					return nil, nil, err
				}
			}
		}
		for _, l0 := range coll.GetL0Segments() {
			if err := addTask(l0, allData); err != nil {
				return nil, nil, err
			}
		}
	}
	return plan, dataSegByID, nil
}

// listL0Deltalogs lists the deltalog object keys of one L0 segment.
func (t *Task) listL0Deltalogs(ctx context.Context, collID int64, l0 *backuppb.SegmentBackupInfo) ([]string, error) {
	dir := mpath.BackupDeltaLogDir(t.backupDir,
		mpath.CollectionID(collID),
		mpath.PartitionID(l0.GetPartitionId()),
		mpath.SegmentID(l0.GetSegmentId()),
	)
	keys, _, err := storage.ListPrefixFlat(ctx, t.cli, dir, true)
	if err != nil {
		return nil, fmt.Errorf("list L0 deltalogs under %s: %w", dir, err)
	}
	return keys, nil
}

// deltalogKey builds the output object key for a new deltalog on a data segment.
func (t *Task) deltalogKey(collID, partID, segID, logID int64) string {
	dir := mpath.BackupDeltaLogDir(t.backupDir,
		mpath.CollectionID(collID),
		mpath.PartitionID(partID),
		mpath.SegmentID(segID),
	)
	return dir + strconv.FormatInt(logID, 10)
}

// runFanout serializes the plan, invokes the l0fanout binary, and parses the result.
func (t *Task) runFanout(ctx context.Context, plan *fanoutPlan) (*fanoutResult, error) {
	tmpDir, err := os.MkdirTemp("", "wash-l0-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmpDir)

	planPath := tmpDir + "/plan.json"
	resultPath := tmpDir + "/result.json"
	cfgPath := tmpDir + "/milvus.yaml"

	planBytes, err := json.Marshal(plan)
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(planPath, planBytes, 0o600); err != nil {
		return nil, err
	}
	if err := os.WriteFile(cfgPath, []byte(t.milvusYAML()), 0o600); err != nil {
		return nil, err
	}

	cmd := exec.CommandContext(ctx, t.l0fanoutBin,
		"--config", cfgPath, "--plan", planPath, "--output", resultPath)
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("l0fanout %s: %w", t.l0fanoutBin, err)
	}

	resultBytes, err := os.ReadFile(resultPath)
	if err != nil {
		return nil, err
	}
	result := &fanoutResult{}
	if err := json.Unmarshal(resultBytes, result); err != nil {
		return nil, err
	}
	return result, nil
}

// milvusYAML renders a minimal milvus.yaml describing the backup object storage,
// so l0fanout's paramtable-based chunk manager can read/write the backup files.
func (t *Task) milvusYAML() string {
	m := &t.params.Minio
	storageType := m.BackupStorageType.Val
	if storageType == "" {
		storageType = m.StorageType.Val
	}
	commonType := "remote"
	if storageType == "local" {
		commonType = "local"
	}
	return fmt.Sprintf(`common:
  storageType: %s
minio:
  address: %s
  port: %d
  accessKeyID: %s
  secretAccessKey: %s
  token: %s
  useSSL: %t
  bucketName: %s
  rootPath: %s
  useIAM: %t
  iamEndpoint: %s
  region: %s
  cloudProvider: %s
  gcpCredentialJSON: %s
`,
		commonType,
		m.BackupAddress.Val, m.BackupPort.Val,
		m.BackupAccessKeyID.Val, m.BackupSecretAccessKey.Val, m.BackupToken.Val,
		m.BackupUseSSL.Val, m.BackupBucketName.Val, m.BackupRootPath.Val,
		m.BackupUseIAM.Val, m.BackupIAMEndpoint.Val, m.BackupRegion.Val,
		storageType, m.BackupGcpCredentialJSON.Val,
	)
}

// dropL0 removes every L0 segment from the metadata so a restore does not
// re-import them (their deletes are now folded into the data segments).
func dropL0(info *backuppb.BackupInfo) {
	for _, coll := range info.GetCollectionBackups() {
		coll.L0Segments = nil
		for _, part := range coll.GetPartitionBackups() {
			part.SegmentBackups = lo.Filter(part.GetSegmentBackups(), func(seg *backuppb.SegmentBackupInfo, _ int) bool {
				return !seg.GetIsL0()
			})
		}
	}
}

// pkTypeName returns the primary-key field type name ("Int64" | "VarChar").
func pkTypeName(coll *backuppb.CollectionBackupInfo) (string, error) {
	for _, field := range coll.GetSchema().GetFields() {
		if !field.GetIsPrimaryKey() {
			continue
		}
		switch field.GetDataType() {
		case backuppb.DataType_Int64:
			return "Int64", nil
		case backuppb.DataType_VarChar:
			return "VarChar", nil
		default:
			return "", fmt.Errorf("unsupported primary key type %v", field.GetDataType())
		}
	}
	return "", fmt.Errorf("no primary key field in schema")
}

// logID derives a deltalog id that is unique within a data segment's delta dir.
// L0 segment ids and a per-run sequence make collisions with existing logIDs of
// the target segment vanishingly unlikely.
func logID(l0SegID, seq int64) int64 {
	return l0SegID*1_000_000 + seq
}
