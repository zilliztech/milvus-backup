package jobstate

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/restoreplan"
)

// Version 3 groups binlog imports by partition name.
const restorePersistedStateVersion = 3
const markerAvailable = "available"
const markerStain = "stain"

// This DTO deliberately omits runtime trackers, locks, clients and semaphores.
// Protobufs are kept as binary, including unknown fields and oneofs.
type restorePersistedState struct {
	Version              int
	Marker               string
	Location             persistenceLocation
	Status               RestoreStatus
	Plan                 *restoreplan.Plan
	Metadata             [][]byte
	SecondaryBackup      []byte
	Collections          map[collref.Name]collectionPersistedState
	SecondaryCollections map[int64]*SecondaryCollectionProgress
	Stream               *milvus.StreamCheckpointSnapshot
}
type collectionPersistedState struct {
	DDL  *ddlPersistedState
	Jobs []jobPersistedState
}
type ddlPersistedState struct {
	Step                 int
	CollectionID         int64
	CreateTargetPrepared bool
	State                RestoreDDLState
	ErrorMessage         string
	EndTime              time.Time
}
type jobPersistedState struct {
	ID               string
	State            RestoreJobState
	ErrorMessage     string
	Size             int64
	Progress         int32
	EndTime          time.Time
	CommitRequested  bool
	PrimaryCompleted bool
	CompletedMirrors map[string]bool
	Binlog           *binlogPersistedState
	Snapshot         *snapshotProgress
}
type binlogPersistedState struct {
	Batch      *restoreplan.BinlogImportBatch
	StagingDir string
}
type snapshotProgress struct{ TargetPrepared, DataRestored bool }

// Called only after workers have stopped, while holding the task lock.
func (t *restoreTask) encodeLocked(marker string, location persistenceLocation) ([]byte, error) {
	out := restorePersistedState{Version: restorePersistedStateVersion, Marker: marker, Location: location, Status: t.snapshotLocked(), Collections: make(map[collref.Name]collectionPersistedState)}
	if t.plan == nil {
		return nil, fmt.Errorf("restore persisted state: missing plan")
	}
	plan := *t.plan
	plan.Collections = append([]restoreplan.Collection(nil), t.plan.Collections...)
	for i, coll := range plan.Collections {
		b, err := proto.Marshal(protoadapt.MessageV2Of(coll.Metadata))
		if err != nil {
			return nil, err
		}
		out.Metadata = append(out.Metadata, b)
		plan.Collections[i].Metadata = nil
	}
	if plan.Secondary != nil {
		secondary := *plan.Secondary
		b, err := proto.Marshal(protoadapt.MessageV2Of(secondary.Backup))
		if err != nil {
			return nil, err
		}
		out.SecondaryBackup = b
		secondary.Backup = nil
		plan.Secondary = &secondary
		if t.secondaryProgress == nil || t.secondaryProgress.StreamCheckpoint == nil {
			return nil, fmt.Errorf("restore persisted state: missing secondary progress")
		}
		out.SecondaryCollections = t.secondaryProgress.Collections
		out.Stream, err = t.secondaryProgress.StreamCheckpoint.Snapshot()
		if err != nil {
			return nil, err
		}
	}
	out.Plan = &plan
	for name, coll := range t.collTask {
		coll.mu.RLock()
		saved := collectionPersistedState{}
		if d := coll.ddl; d != nil {
			saved.DDL = &ddlPersistedState{d.step, d.collectionID, d.createTargetPrepared, d.state, d.errorMessage, d.endTime}
		}
		for _, j := range coll.jobs {
			v := jobPersistedState{
				ID:               j.id,
				State:            j.state,
				ErrorMessage:     j.errorMessage,
				Size:             j.size,
				Progress:         j.progress,
				EndTime:          j.endTime,
				CommitRequested:  j.twoPCProgress.commitRequested,
				PrimaryCompleted: j.twoPCProgress.primaryCompleted,
				CompletedMirrors: j.twoPCProgress.completedMirrors,
			}
			if b := j.binlogImport; b != nil {
				v.Binlog = &binlogPersistedState{b.batch, b.stagingDir}
			}
			if s := j.snapshotRestore; s != nil {
				v.Snapshot = &snapshotProgress{s.targetPrepared, s.dataRestored}
			}
			saved.Jobs = append(saved.Jobs, v)
		}
		out.Collections[name] = saved
		coll.mu.RUnlock()
	}
	return json.Marshal(out)
}

func decodeRestore(id string, data []byte, cleanup bool) (*restoreTask, error) {
	var in restorePersistedState
	if err := json.Unmarshal(data, &in); err != nil {
		return nil, fmt.Errorf("decode restore persisted state: %w", err)
	}
	if in.Version != restorePersistedStateVersion {
		return nil, fmt.Errorf("restore persisted state: unsupported version %d", in.Version)
	}
	if in.Status.ID != id || !in.Status.EnableResume || in.Plan == nil || (in.Marker != markerAvailable && in.Marker != markerStain) || (in.Status.State != RestoreStateFail && in.Status.State != RestoreStateFailedCleanupPending) {
		return nil, fmt.Errorf("restore persisted state: invalid identity, plan, marker or state")
	}
	if len(in.Metadata) != len(in.Plan.Collections) {
		return nil, fmt.Errorf("restore persisted state: inconsistent collection metadata")
	}
	t := newRestoreTask(id, true)
	t.plan = in.Plan
	t.state, t.errorMessage, t.startTime, t.endTime = in.Status.State, in.Status.ErrorMessage, in.Status.StartTime, in.Status.EndTime
	// A stain invalidates only the durable progress. Never publish it as a
	// failed task that a subsequent memory-first Resume could adopt.
	if in.Marker == markerStain && !cleanup {
		return nil, fmt.Errorf("%w: saved restore progress is no longer usable; the previous attempt may have changed the target, so this persisted state cannot be resumed", ErrRestoreConflict)
	}
	if cleanup {
		t.state = RestoreStateFailedCleanupPending
	}
	for i, b := range in.Metadata {
		meta := new(backuppb.CollectionBackupInfo)
		if err := proto.Unmarshal(b, protoadapt.MessageV2Of(meta)); err != nil {
			return nil, err
		}
		t.plan.Collections[i].Metadata = meta
	}
	for name, c := range in.Status.Colls {
		saved, ok := in.Collections[name]
		if !ok || len(c.Jobs) != len(saved.Jobs) {
			return nil, fmt.Errorf("restore persisted state: inconsistent progress for %s", name)
		}
		coll := &restoreCollTask{target: name, totalSize: c.TotalSize, startTime: c.StartTime}
		if d := saved.DDL; d != nil {
			coll.ddl = &restoreCollDDL{d.Step, d.CollectionID, d.CreateTargetPrepared, d.State, d.ErrorMessage, d.EndTime}
		}
		for _, j := range saved.Jobs {
			job := &restoreJob{
				id:           j.ID,
				state:        j.State,
				errorMessage: j.ErrorMessage,
				size:         j.Size,
				progress:     j.Progress,
				endTime:      j.EndTime,
				twoPCProgress: import2PCProgress{
					commitRequested:  j.CommitRequested,
					primaryCompleted: j.PrimaryCompleted,
					completedMirrors: j.CompletedMirrors,
				},
			}
			if b := j.Binlog; b != nil {
				if b.Batch == nil {
					return nil, fmt.Errorf("restore persisted state: missing import batch")
				}
				prefix := fmt.Sprintf("restore-temp-%s-%s-%s-", id, name.DBName(), name.CollName())
				if err := validateStagingDir(b.StagingDir, prefix); err != nil {
					return nil, err
				}
				job.binlogImport = &binlogImportState{b.Batch, b.StagingDir}
			}
			if s := j.Snapshot; s != nil {
				job.snapshotRestore = &snapshotRestoreState{s.TargetPrepared, s.DataRestored}
			}
			coll.jobs = append(coll.jobs, job)
		}
		t.collTask[name] = coll
	}
	if err := t.validateCollections(); err != nil {
		return nil, err
	}
	if err := t.restoreSecondary(&in); err != nil {
		return nil, err
	}

	return t, nil
}

func (t *restoreTask) restoreSecondary(in *restorePersistedState) error {
	if secondary := t.plan.Secondary; secondary != nil {
		secondary.Backup = new(backuppb.BackupInfo)
		if err := proto.Unmarshal(in.SecondaryBackup, protoadapt.MessageV2Of(secondary.Backup)); err != nil {
			return err
		}
		metadata := make(map[int64]*backuppb.CollectionBackupInfo, len(secondary.Collections))
		for _, coll := range secondary.Backup.GetCollectionBackups() {
			metadata[coll.GetCollectionId()] = coll
			progress := t.collTask[collref.New(coll.GetDbName(), coll.GetCollectionName())]
			if progress == nil || progress.ddl == nil || len(progress.jobs) != 1 || secondary.Collections[coll.GetCollectionId()] == nil {
				return fmt.Errorf("restore persisted state: inconsistent secondary collection %d", coll.GetCollectionId())
			}
		}
		checkpoint, err := milvus.RestoreStreamCheckpoint(in.Stream)
		if err != nil {
			return err
		}
		if len(in.SecondaryCollections) != len(secondary.Collections) {
			return fmt.Errorf("restore persisted state: inconsistent secondary collection count")
		}
		for id, coll := range secondary.Collections {
			progress := in.SecondaryCollections[id]
			meta := metadata[id]
			if coll == nil || meta == nil || progress == nil || len(progress.BinlogImports) != len(coll.Phases) {
				return fmt.Errorf("restore persisted state: missing secondary collection %d", id)
			}
			if dir := progress.StagingDir; dir != "" && (!coll.StageBinlogs || dir != SecondaryStagingDir(t.id, meta.GetDbName(), meta.GetCollectionName())) {
				return fmt.Errorf("restore persisted state: invalid staging directory %q for secondary collection %d", dir, id)
			}
			for phase, batches := range coll.Phases {
				if len(progress.BinlogImports[phase]) != len(batches) {
					return fmt.Errorf("restore persisted state: inconsistent secondary batches")
				}
			}
		}
		t.secondaryProgress = &SecondaryProgress{StreamCheckpoint: checkpoint, Collections: in.SecondaryCollections}
	}
	return nil
}

// An empty directory means staging was never allocated or has been cleaned.
func validateStagingDir(dir, prefix string) error {
	if dir == "" {
		return nil
	}
	path := strings.TrimSuffix(dir, "/")
	id, err := uuid.Parse(strings.TrimPrefix(path, prefix))
	if err != nil || dir != prefix+id.String()+"/" || !fs.ValidPath(path) || strings.ContainsAny(path, "\\\x00") {
		return fmt.Errorf("restore persisted state: invalid staging directory %q", dir)
	}
	return nil
}

func (t *restoreTask) validateCollections() error {
	for _, coll := range t.plan.Collections {
		progress := t.collTask[coll.Target]
		if progress == nil {
			return fmt.Errorf("restore persisted state: missing planned collection %s", coll.Target)
		}
		if t.plan.Option.MetaOnly {
			if progress.ddl == nil {
				return fmt.Errorf("restore persisted state: missing DDL")
			}
			continue
		}
		switch t.plan.Strategy {
		case restoreplan.StrategyPerSeg, restoreplan.StrategyMergedSeg:
			if progress.ddl == nil {
				return fmt.Errorf("restore persisted state: missing binlog DDL")
			}
			for _, job := range progress.jobs {
				if job.binlogImport == nil {
					return fmt.Errorf("restore persisted state: missing import batch")
				}
				if b := job.binlogImport.batch; b.PartitionName == "" && !b.L0 {
					return fmt.Errorf("restore persisted state: empty partition name for non-L0 batch in %s", coll.Target)
				}
			}
		case restoreplan.StrategyRestoreSnap, restoreplan.StrategyImportSnap:
			if t.plan.Strategy == restoreplan.StrategyImportSnap && progress.ddl == nil {
				return fmt.Errorf("restore persisted state: missing snapshot import DDL")
			}
			if len(progress.jobs) != 1 {
				return fmt.Errorf("restore persisted state: snapshot requires one job")
			}
			if t.plan.Strategy == restoreplan.StrategyRestoreSnap && progress.jobs[0].snapshotRestore == nil {
				return fmt.Errorf("restore persisted state: missing snapshot checkpoint")
			}
		default:
			return fmt.Errorf("restore persisted state: invalid resolved strategy %d", t.plan.Strategy)
		}
	}
	return nil
}
