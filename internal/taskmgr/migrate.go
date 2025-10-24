package taskmgr

import (
	"sync"
	"time"

	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"

	"github.com/zilliztech/milvus-backup/internal/progressbar"
)

type MigrateTaskOpt func(task *MigrateTask)

func SetMigrateJobID(jobID string) MigrateTaskOpt {
	return func(task *MigrateTask) {
		task.mu.Lock()
		defer task.mu.Unlock()

		task.migrateJobID = jobID
	}
}

func IncMigrateCopiedSize(size int64, cost time.Duration) MigrateTaskOpt {
	return func(task *MigrateTask) {
		task.mu.Lock()
		defer task.mu.Unlock()

		task.copiedSize += size

		if task.bar != nil {
			task.bar.EwmaIncrInt64(size, cost)
		}
	}
}

func SetMigrateCopyStart() MigrateTaskOpt {
	return func(task *MigrateTask) {
		opts := []mpb.BarOption{
			mpb.PrependDecorators(
				decor.Name("Uploading", decor.WC{C: decor.DindentRight | decor.DextraSpace}),
				decor.Counters(decor.SizeB1024(0), "% .2f / % .2f")),
			mpb.AppendDecorators(
				decor.EwmaETA(decor.ET_STYLE_GO, 30),
				decor.Name(" ] "),
				decor.EwmaSpeed(decor.SizeB1024(0), "% .2f", 30),
			),
			mpb.BarRemoveOnComplete(),
		}

		task.mu.Lock()
		defer task.mu.Unlock()

		// The size in backupinfo does not include all partition L0 (partition id == -1)
		// and meta, so the total cannot be set directly.
		// Need to use dyn total.
		task.bar = progressbar.Progress().New(0, mpb.BarStyle().Rbound("|"), opts...)
		task.bar.SetTotal(task.totalSize, false)
	}
}

func SetMigrateCopyComplete() MigrateTaskOpt {
	return func(task *MigrateTask) {
		task.mu.Lock()
		defer task.mu.Unlock()

		if task.bar != nil {
			task.bar.SetTotal(-1, true)
		}
	}
}

type MigrateTask struct {
	mu sync.RWMutex

	id string

	totalSize  int64
	copiedSize int64

	bar *mpb.Bar

	migrateJobID string
}

func newMigrateTask(id string, totalSize int64) *MigrateTask {
	return &MigrateTask{id: id, totalSize: totalSize}
}

func (t *MigrateTask) MigrateJobID() string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.migrateJobID
}
