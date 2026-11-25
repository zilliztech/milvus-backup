package jobstate

import (
	"fmt"

	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/restoreplan"
)

// SecondaryProgress is shared across executions of the same restore task.
// Only the admitted execution or cleanup writes it. Concurrent import workers
// update disjoint entries, and StreamCheckpoint synchronizes channel acknowledgements.
type SecondaryProgress struct {
	// StreamCheckpoint keeps replay checkpoints when Resume is enabled.
	StreamCheckpoint *milvus.StreamCheckpoint
	Collections      map[int64]*SecondaryCollectionProgress
}

type SecondaryCollectionProgress struct {
	// BinlogImports uses the same phase and batch indices as the published plan.
	BinlogImports  [][]SecondaryImportProgress
	SnapshotImport SecondaryImportProgress
	StagingDir     string
}

type SecondaryImportProgress struct {
	JobID int64
	State SecondaryImportState
}

type SecondaryImportState uint8

const (
	SecondaryImportPending SecondaryImportState = iota
	SecondaryImportCommitting
	SecondaryImportFailed
	SecondaryImportCompleted
)

// SecondaryStagingDir names the collection directory shared by its binlog batches.
func SecondaryStagingDir(taskID, dbName, collName string) string {
	return fmt.Sprintf("restore-temp-%s-%s-%s/", taskID, dbName, collName)
}

func NewSecondaryCollectionProgress(plan *restoreplan.SecondaryCollection) *SecondaryCollectionProgress {
	progress := &SecondaryCollectionProgress{
		BinlogImports: make([][]SecondaryImportProgress, len(plan.Phases)),
	}
	for phase, batches := range plan.Phases {
		progress.BinlogImports[phase] = make([]SecondaryImportProgress, len(batches))
	}
	return progress
}

func NewSecondaryProgress(plan *restoreplan.Secondary) *SecondaryProgress {
	progress := &SecondaryProgress{
		Collections: make(map[int64]*SecondaryCollectionProgress, len(plan.Collections)),
	}
	for id, coll := range plan.Collections {
		progress.Collections[id] = NewSecondaryCollectionProgress(coll)
	}
	return progress
}

// SaveSecondaryPlan publishes the plan and its matching progress together.
func (t *RestoreTracker) SaveSecondaryPlan(plan *restoreplan.Plan, progress *SecondaryProgress) {
	t.task.mu.Lock()
	defer t.task.mu.Unlock()
	t.task.plan = plan
	t.task.secondaryProgress = progress
}

// SecondaryProgress returns the existing progress for an admitted Resume or Cleanup.
func (t *RestoreTracker) SecondaryProgress() *SecondaryProgress {
	t.task.mu.RLock()
	defer t.task.mu.RUnlock()
	return t.task.secondaryProgress
}
