package restore

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/restoreplan"
)

func TestWaitRestoreTerminal(t *testing.T) {
	// File rotation workers must not be started inside a synctest bubble.
	log.InitLogger(&log.Config{Level: "info"})
	for _, tc := range []struct {
		name string
		err  error
	}{{name: "Success"}, {name: "Failure", err: assert.AnError}} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				config := cfg.New().Restore.Persistence
				config.Enabled.Val = true
				config.Type.Val = "local"
				config.Local.Directory.Val = t.TempDir()
				store := jobstate.NewStore()
				store.ConfigurePersistence(config)
				tracker, err := store.AddRestoreTask("job", true)
				require.NoError(t, err)
				tracker.SavePlan(&restoreplan.Plan{})
				require.True(t, tracker.Start())

				done := make(chan error, 1)
				go func() { done <- waitRestoreTerminal(t.Context(), store, "job") }()
				synctest.Wait()
				assert.Empty(t, done, "CLI must remain alive while the task executes")

				tracker.Finish(tc.err)
				err = <-done
				cold := jobstate.NewStore()
				cold.ConfigurePersistence(config)
				_, loadErr := cold.GetRestoreTask("job")
				if tc.err != nil {
					assert.ErrorContains(t, err, "restore job: "+tc.err.Error())
					assert.NoError(t, loadErr, "failed progress must be saved before CLI returns")
				} else {
					assert.NoError(t, err)
					assert.ErrorIs(t, loadErr, jobstate.ErrTaskNotFound)
				}
			})
		})
	}

	t.Run("AlreadyTerminal", func(t *testing.T) {
		store := jobstate.NewStore()
		tracker, err := store.AddRestoreTask("job", true)
		require.NoError(t, err)
		tracker.Finish(nil)
		assert.NoError(t, waitRestoreTerminal(t.Context(), store, "job"))
	})

	t.Run("CanceledWhileWaiting", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			store := jobstate.NewStore()
			tracker, err := store.AddRestoreTask("job", true)
			require.NoError(t, err)
			require.True(t, tracker.Start())
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- waitRestoreTerminal(ctx, store, "job") }()
			synctest.Wait()
			assert.Empty(t, done)
			cancel()
			assert.ErrorIs(t, <-done, context.Canceled)
		})
	})

	t.Run("MissingTask", func(t *testing.T) {
		assert.ErrorIs(t, waitRestoreTerminal(t.Context(), jobstate.NewStore(), "missing"), jobstate.ErrTaskNotFound)
	})
}
