package jobstate

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/restoreplan"
)

type faultyRestoreStore struct {
	PersistenceBackend
	saveErr, deleteErr    error
	saves, loads, deletes int
}

// This implementation has no Store or generic backend methods.
type recordingRestorePersistence struct {
	ids             []string
	persistedStates []restorePersistedState
	deleted         []string
	saveErr         error
}

func (p *recordingRestorePersistence) saveRestoreState(id string, encode func(persistenceLocation) ([]byte, error)) error {
	data, err := encode(persistenceLocation{})
	if err != nil {
		return err
	}
	var persistedState restorePersistedState
	if err := json.Unmarshal(data, &persistedState); err != nil {
		return err
	}
	p.ids = append(p.ids, id)
	p.persistedStates = append(p.persistedStates, persistedState)
	return p.saveErr
}

func (p *recordingRestorePersistence) deleteRestoreState(id string) error {
	p.deleted = append(p.deleted, id)
	return nil
}

func TestRestoreTaskPersistenceInterface(t *testing.T) {
	store := NewStore()
	tracker, err := store.AddRestoreTask("job", true)
	require.NoError(t, err)
	persistence := &recordingRestorePersistence{}
	tracker.task.persistence = persistence
	tracker.SavePlan(&restoreplan.Plan{})
	require.True(t, tracker.Start())
	tracker.Finish(assert.AnError)
	require.Len(t, persistence.persistedStates, 1)
	assert.Equal(t, markerAvailable, persistence.persistedStates[0].Marker)
	assert.Equal(t, RestoreStateFail, persistence.persistedStates[0].Status.State)

	persistence.saveErr = ErrPersistence
	_, _, err = store.StartResume("job")
	assert.ErrorIs(t, err, ErrPersistence)
	assert.Equal(t, RestoreStateFail, tracker.Snapshot().State)
	require.Len(t, persistence.persistedStates, 2)
	assert.Equal(t, markerStain, persistence.persistedStates[1].Marker)

	persistence.saveErr = nil
	owner, _, err := store.StartResume("job")
	require.NoError(t, err)
	assert.Equal(t, RestoreStateExecuting, owner.Snapshot().State)
	owner.Finish(nil)
	assert.Equal(t, []string{"job", "job", "job"}, persistence.ids)
	assert.Equal(t, []string{"job"}, persistence.deleted)
}

func TestNonResumableTaskSkipsPersistenceInterface(t *testing.T) {
	persistence := &recordingRestorePersistence{}
	tracker := &RestoreTracker{task: newRestoreTask("job", false)}
	tracker.task.persistence = persistence
	tracker.SavePlan(&restoreplan.Plan{})
	tracker.Finish(assert.AnError)
	tracker.Finish(nil)
	assert.Empty(t, persistence.persistedStates)
	assert.Empty(t, persistence.deleted)
}

func TestStorePersistedStateEncodingOnRetry(t *testing.T) {
	store, _, config := persistenceFixture(t)
	backend := &faultyRestoreStore{PersistenceBackend: store.persistence.backend, saveErr: assert.AnError}
	store.persistence.backend = backend
	encodes := 0
	err := store.saveRestoreState("job", func(location persistenceLocation) ([]byte, error) {
		encodes++
		assert.Equal(t, persistenceLocationFromConfig(config), location)
		return []byte("persisted state"), nil
	})
	assert.ErrorIs(t, err, ErrPersistence)
	assert.Equal(t, 3, backend.saves)
	assert.Equal(t, 1, encodes)

	config.Enabled.Val = false
	store.ConfigurePersistence(config)
	err = store.saveRestoreState("job", func(persistenceLocation) ([]byte, error) {
		encodes++
		return nil, assert.AnError
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, encodes, "disabled persistence must not encode persisted states")
}

func (s *faultyRestoreStore) Load(ctx context.Context, id string) ([]byte, error) {
	s.loads++
	return s.PersistenceBackend.Load(ctx, id)
}

func (s *faultyRestoreStore) Save(ctx context.Context, id string, b []byte) error {
	s.saves++
	if s.saveErr != nil {
		return s.saveErr
	}
	return s.PersistenceBackend.Save(ctx, id, b)
}
func (s *faultyRestoreStore) Delete(ctx context.Context, id string) error {
	s.deletes++
	if s.deleteErr != nil {
		return s.deleteErr
	}
	return s.PersistenceBackend.Delete(ctx, id)
}

const fixtureStagingDir = "restore-temp-job-db-coll-3480ce04-a23c-404e-ac64-0e7916111111/"

func persistenceFixture(t *testing.T) (*Store, *RestoreTracker, cfg.PersistenceConfig) {
	t.Helper()
	config := cfg.New().Restore.Persistence
	config.Enabled.Val = true
	config.Storage.Provider.Val = "local"
	config.Storage.RootPath.Val = t.TempDir()
	store := NewStore()
	store.ConfigurePersistence(config)
	tr, err := store.AddRestoreTask("job", true)
	require.NoError(t, err)
	name := collref.New("db", "coll")
	coll := tr.NewCollTracker(name, 17)
	ddl := coll.DDL()
	ddl.SetDDLProgress(3, 123)
	ddl.MarkCreateTargetPrepared()
	ddl.SetSuccess()
	batch := restoreplan.BinlogImportBatch{PartitionName: "p", L0: true, Timestamp: 234, StorageVersion: 2, Dirs: []restoreplan.Directory{{Insert: "insert", Delta: "delta", Size: 17}}}
	job := coll.NewBinlogImport(batch).Tracker
	job.Bind("remote-job")
	job.SetStagingDir(fixtureStagingDir)
	job.MarkPrimaryCompleted()
	job.MarkMirrorCompleted("secondary")
	tr.SavePlan(&restoreplan.Plan{Option: restoreplan.Option{EnableResume: true}, Strategy: restoreplan.StrategyPerSeg, Collections: []restoreplan.Collection{{Target: name, Metadata: &backuppb.CollectionBackupInfo{CollectionId: 456, CollectionName: "source"}}}})
	require.True(t, tr.Start())
	tr.Finish(assert.AnError)
	return store, tr, config
}
func TestRestorePersistenceRoundTrip(t *testing.T) {
	_, original, config := persistenceFixture(t)
	cold := NewStore()
	cold.ConfigurePersistence(config)
	loaded, err := cold.restoreTracker("job")
	require.NoError(t, err)
	// Time monotonic readings do not survive encoding; compare the serialized DTO.
	before, err := original.task.encodeLocked(markerAvailable, persistenceLocationFromConfig(config))
	require.NoError(t, err)
	after, err := loaded.task.encodeLocked(markerAvailable, persistenceLocationFromConfig(config))
	require.NoError(t, err)
	assert.JSONEq(t, string(before), string(after))
	owner, plan, err := cold.StartResume("job")
	require.NoError(t, err)
	assert.Equal(t, restoreplan.StrategyPerSeg, plan.Strategy)
	jobs := owner.TrackerForCollection(collref.New("db", "coll")).TrackersForBinlogImports()
	require.Len(t, jobs, 1)
	assert.Equal(t, "remote-job", jobs[0].Tracker.job.id)
	assert.True(t, jobs[0].Tracker.CommitRequested())
	assert.True(t, jobs[0].Tracker.PrimaryCompleted())
	assert.True(t, jobs[0].Tracker.MirrorCompleted("secondary"))
	assert.Equal(t, fixtureStagingDir, jobs[0].Tracker.StagingDir())
	// Another process sees the claim, not a reusable failed persisted state.
	restart := NewStore()
	restart.ConfigurePersistence(config)
	_, err = restart.GetRestoreTask("job")
	assert.ErrorIs(t, err, ErrRestoreConflict)
	assert.NotContains(t, restart.restoreTask, "job")
	_, _, err = restart.StartResume("job")
	assert.ErrorIs(t, err, ErrRestoreConflict)
	owner.Finish(assert.AnError)
	// A rejected stale read must not cache the old persisted state.
	_, _, err = restart.StartResume("job")
	require.NoError(t, err)
}
func TestRestorePersistenceFailures(t *testing.T) {
	store, tr, config := persistenceFixture(t)
	backend := &faultyRestoreStore{PersistenceBackend: store.persistence.backend, saveErr: assert.AnError}
	store.persistence.backend = backend
	_, _, err := store.StartResume("job")
	assert.ErrorIs(t, err, ErrPersistence)
	assert.Equal(t, RestoreStateFail, tr.Snapshot().State)
	assert.Equal(t, 3, backend.saves)
	backend.saveErr = nil
	owner, _, err := store.StartResume("job")
	require.NoError(t, err)
	job := owner.TrackerForCollection(collref.New("db", "coll")).TrackersForBinlogImports()[0].Tracker
	job.Bind("latest-job")
	backend.saveErr = assert.AnError
	status := owner.Finish(errors.New("execution failed"))
	assert.Equal(t, "execution failed", status.ErrorMessage)
	assert.Equal(t, RestoreStateFail, status.State)
	cold := NewStore()
	cold.ConfigurePersistence(config)
	_, _, err = cold.StartResume("job")
	assert.ErrorIs(t, err, ErrRestoreConflict)
	// A memory hit never reloads the obsolete stain. A fresh full stain is mandatory.
	backend.saveErr = nil
	_, _, err = store.StartResume("job")
	require.NoError(t, err)
	assert.Equal(t, "latest-job", job.job.id)
	saved, err := backend.Load(t.Context(), restoreStateKey("job"))
	require.NoError(t, err)
	var persistedState restorePersistedState
	require.NoError(t, json.Unmarshal(saved, &persistedState))
	assert.Equal(t, markerStain, persistedState.Marker)
	assert.Equal(t, "latest-job", persistedState.Collections[collref.New("db", "coll")].Jobs[0].ID)
	backend.deleteErr = assert.AnError
	owner.SetSuccess()
	assert.Equal(t, RestoreStateSuccess, owner.Snapshot().State)
	assert.Empty(t, owner.Snapshot().ErrorMessage)
	// Deletion is best effort, including retries through Cleanup.
	_, _, err = store.StartCleanup("job")
	assert.NoError(t, err)
	_, err = backend.Load(t.Context(), restoreStateKey("job"))
	assert.NoError(t, err)
	backend.deleteErr = nil
	_, _, err = store.StartCleanup("job")
	require.NoError(t, err)
	_, err = backend.Load(t.Context(), restoreStateKey("job"))
	assert.ErrorIs(t, err, ErrPersistenceKeyNotFound)
}
func TestCleanupPersistenceRetry(t *testing.T) {
	store, tr, config := persistenceFixture(t)
	backend := &faultyRestoreStore{PersistenceBackend: store.persistence.backend, saveErr: assert.AnError}
	store.persistence.backend = backend
	_, _, err := store.StartCleanup("job")
	assert.ErrorIs(t, err, ErrPersistence)
	assert.Equal(t, RestoreStateFailedCleanupPending, tr.Snapshot().State)
	_, _, err = store.StartResume("job")
	assert.ErrorIs(t, err, ErrRestoreConflict)
	backend.saveErr = nil
	owner, _, err := store.StartCleanup("job")
	require.NoError(t, err)
	owner.FinishCleanup(assert.AnError)
	cold := NewStore()
	cold.ConfigurePersistence(config)
	resumed, _, err := cold.StartCleanup("job")
	require.NoError(t, err)
	assert.Equal(t, fixtureStagingDir, resumed.TrackerForCollection(collref.New("db", "coll")).TrackersForBinlogImports()[0].Tracker.StagingDir())
	resumed.FinishCleanup(nil)
	_, err = backend.Load(t.Context(), restoreStateKey("job"))
	assert.ErrorIs(t, err, ErrPersistenceKeyNotFound)
	assert.Equal(t, RestoreStateFailedCleaned, resumed.Snapshot().State)
}
func TestRestoreConcurrentColdLoad(t *testing.T) {
	_, _, config := persistenceFixture(t)
	store := NewStore()
	store.ConfigurePersistence(config)
	owners := make([]*RestoreTracker, 16)
	var wg sync.WaitGroup
	for i := range owners {
		wg.Go(func() { var err error; owners[i], err = store.restoreTracker("job"); assert.NoError(t, err) })
	}
	wg.Wait()
	for _, owner := range owners {
		assert.Same(t, owners[0].task, owner.task)
	}
	store.ConfigurePersistence(config)
	_, err := store.AddRestoreTask("job", true)
	assert.ErrorIs(t, err, ErrRestoreConflict)
}
func TestRestorePersistedStateRejectsInvalidRecords(t *testing.T) {
	_, tr, config := persistenceFixture(t)
	b, err := tr.task.encodeLocked(markerAvailable, persistenceLocationFromConfig(config))
	require.NoError(t, err)
	var persistedState restorePersistedState
	require.NoError(t, json.Unmarshal(b, &persistedState))
	persistedState.Version++
	b, err = json.Marshal(persistedState)
	require.NoError(t, err)
	_, err = decodeRestore("job", b, false)
	assert.ErrorContains(t, err, "unsupported version")
	_, err = decodeRestore("job", []byte("{"), false)
	assert.Error(t, err)
	persistedState.Version = restorePersistedStateVersion
	persistedState.Status.ID = "another"
	b, err = json.Marshal(persistedState)
	require.NoError(t, err)
	_, err = decodeRestore("job", b, false)
	assert.Error(t, err)
}
func TestColdLoadRejectsSnapshotImportWithoutDDL(t *testing.T) {
	_, tr, config := persistenceFixture(t)
	tr.task.plan.Strategy = restoreplan.StrategyImportSnap
	tr.task.collTask[collref.New("db", "coll")].ddl = nil
	data, err := tr.task.encodeLocked(markerAvailable, persistenceLocationFromConfig(config))
	require.NoError(t, err)
	_, err = decodeRestore("job", data, false)
	assert.ErrorContains(t, err, "missing snapshot import DDL")
}

func TestSecondaryStagingDir(t *testing.T) {
	assert.Equal(t, "restore-temp-job-db-coll/", SecondaryStagingDir("job", "db", "coll"))
	assert.Equal(t, "restore-temp-team/job-db-coll/", SecondaryStagingDir("team/job", "db", "coll"))
	assert.Equal(t, "restore-temp-team%2Fjob-db-coll/", SecondaryStagingDir("team%2Fjob", "db", "coll"))
}

func TestSecondaryPersistenceRoundTrip(t *testing.T) {
	_, tr, config := persistenceFixture(t)
	meta := &backuppb.CollectionBackupInfo{CollectionId: 456, DbName: "db", CollectionName: "coll"}
	secondary := &restoreplan.Secondary{
		SourceID: "source",
		TargetID: "target",
		Backup: &backuppb.BackupInfo{
			Name:              "secondary-backup",
			CollectionBackups: []*backuppb.CollectionBackupInfo{meta},
		},
		TargetChannels: map[string]string{"source-channel": "target-channel"},
		Collections: map[int64]*restoreplan.SecondaryCollection{
			456: {StageBinlogs: true, Phases: [3][]restoreplan.SecondaryImport{
				{{PartitionID: 4, Batch: restoreplan.BinlogImportBatch{L0: true, Timestamp: 100}}},
			}},
		},
	}
	plan := *tr.task.plan
	plan.Secondary = secondary
	progress := NewSecondaryProgress(secondary)
	progress.Collections[456].StagingDir = "restore-temp-job-db-coll/"
	progress.StreamCheckpoint = milvus.NewStreamCheckpoint(map[string]uint64{"source-channel": 100})
	progress.Collections[456].BinlogImports[0][0] = SecondaryImportProgress{JobID: 99, State: SecondaryImportCommitting}
	tr.SaveSecondaryPlan(&plan, progress)
	tr.Finish(assert.AnError)
	cold := NewStore()
	cold.ConfigurePersistence(config)
	loaded, err := cold.restoreTracker("job")
	require.NoError(t, err)
	assert.Equal(t, progress.Collections, loaded.SecondaryProgress().Collections)
	assert.Equal(t, secondary.TargetChannels, loaded.task.plan.Secondary.TargetChannels)
	assert.True(t, proto.Equal(protoadapt.MessageV2Of(secondary.Backup), protoadapt.MessageV2Of(loaded.task.plan.Secondary.Backup)))
	_, _, err = cold.StartResume("job")
	assert.NoError(t, err)
	for _, dir := range []string{"insert_log/", "restore-temp-job-db-other/"} {
		progress.Collections[456].StagingDir = dir
		data, err := tr.task.encodeLocked(markerStain, persistenceLocationFromConfig(config))
		require.NoError(t, err)
		_, err = decodeRestore("job", data, true)
		assert.ErrorContains(t, err, "invalid staging directory")
	}
}

func TestPersistenceMemoryPriorityAndPreparationFailure(t *testing.T) {
	store, tr, config := persistenceFixture(t)
	// An inaccessible startup backend cannot force a reload of an in-memory task.
	invalid := config
	invalid.Storage.Provider.Val = "invalid"
	store.ConfigurePersistence(invalid)
	status, err := store.GetRestoreTask("job")
	require.NoError(t, err)
	assert.Equal(t, tr.Snapshot(), status)
	fresh := NewStore()
	fresh.ConfigurePersistence(config)
	tracker, err := fresh.AddRestoreTask("unprepared", true)
	require.NoError(t, err)
	tracker.Finish(assert.AnError)
	backend, err := openPersistenceBackend(config)
	require.NoError(t, err)
	_, err = backend.Load(t.Context(), restoreStateKey("unprepared"))
	assert.ErrorIs(t, err, ErrPersistenceKeyNotFound)
	// An ordinary task must not depend on a persistence service or save its state.
	fresh.ConfigurePersistence(cfg.PersistenceConfig{})
	_, err = fresh.AddRestoreTask("ordinary", false)
	assert.NoError(t, err)
}

func TestMetaOnlyPersistenceRetainsDDL(t *testing.T) {
	_, tr, config := persistenceFixture(t)
	name := collref.New("db", "coll")
	tr.task.plan.Option.MetaOnly = true
	tr.task.collTask[name].jobs = nil
	tr.Finish(assert.AnError)
	cold := NewStore()
	cold.ConfigurePersistence(config)
	owner, plan, err := cold.StartResume("job")
	require.NoError(t, err)
	assert.True(t, plan.Option.MetaOnly)
	ddl := owner.TrackerForCollection(name).DDL()
	assert.Equal(t, RestoreDDLSuccess, ddl.State())
	step, id := ddl.DDLProgress()
	assert.Equal(t, 3, step)
	assert.EqualValues(t, 123, id)
	assert.True(t, ddl.CreateTargetPrepared())
}

func TestColdCleanupNeverPublishesResumableProgress(t *testing.T) {
	for _, marker := range []string{markerAvailable, markerStain} {
		name := "Available"
		if marker == markerStain {
			name = "Stain"
		}
		t.Run(name, func(t *testing.T) {
			_, original, config := persistenceFixture(t)
			require.NoError(t, original.task.saveLocked(marker))
			cold := NewStore()
			cold.ConfigurePersistence(config)
			// Pause between loading the resource records and admitting Cleanup.
			// Concurrent readers must already see Pending, never an ordinary Failed task.
			_, err := cold.loadRestore("job", true)
			require.NoError(t, err)
			status, err := cold.GetRestoreTask("job")
			require.NoError(t, err)
			assert.Equal(t, RestoreStateFailedCleanupPending, status.State)
			_, _, err = cold.StartResume("job")
			assert.ErrorIs(t, err, ErrRestoreConflict)
			backend := &faultyRestoreStore{PersistenceBackend: cold.persistence.backend, saveErr: assert.AnError}
			cold.persistence.backend = backend
			_, _, err = cold.StartCleanup("job")
			assert.ErrorIs(t, err, ErrPersistence)
			_, _, err = cold.StartResume("job")
			assert.ErrorIs(t, err, ErrRestoreConflict)
			backend.saveErr = nil
			cleaner, _, err := cold.StartCleanup("job")
			require.NoError(t, err)
			jobs := cleaner.TrackerForCollection(collref.New("db", "coll")).TrackersForBinlogImports()
			require.Len(t, jobs, 1)
			assert.Equal(t, fixtureStagingDir, jobs[0].Tracker.StagingDir())
			cleaner.FinishCleanup(nil)
			_, err = backend.Load(t.Context(), restoreStateKey("job"))
			assert.ErrorIs(t, err, ErrPersistenceKeyNotFound)
		})
	}
}

func TestDisabledPersistenceUsesMemoryOnly(t *testing.T) {
	originalStore, _, config := persistenceFixture(t)
	before, err := originalStore.persistence.backend.Load(t.Context(), restoreStateKey("job"))
	require.NoError(t, err)
	config.Enabled.Val = false
	disabled := NewStore()
	disabled.ConfigurePersistence(config)
	// Do not load or reserve IDs from the disabled durable namespace.
	_, err = disabled.GetRestoreTask("job")
	assert.ErrorIs(t, err, ErrTaskNotFound)
	tracker, err := disabled.AddRestoreTask("job", true)
	require.NoError(t, err)
	tracker.SavePlan(&restoreplan.Plan{})
	status := tracker.Finish(assert.AnError)
	assert.Equal(t, RestoreStateFail, status.State)
	owner, _, err := disabled.StartResume("job")
	require.NoError(t, err)
	assert.Same(t, tracker.task, owner.task)
	status = owner.Finish(nil)
	assert.Equal(t, RestoreStateSuccess, status.State)
	after, err := originalStore.persistence.backend.Load(t.Context(), restoreStateKey("job"))
	require.NoError(t, err)
	assert.Equal(t, before, after, "disabled persistence must neither save progress nor delete old persisted states")
}

func TestDisabledPersistenceIgnoresBackendConfiguration(t *testing.T) {
	config := cfg.New().Restore.Persistence
	config.Storage.Provider.Val = "invalid"
	store := NewStore()
	store.ConfigurePersistence(config)
	tracker, err := store.AddRestoreTask("job", true)
	require.NoError(t, err)
	tracker.SavePlan(&restoreplan.Plan{})
	tracker.Finish(assert.AnError)
	cleaner, _, err := store.StartCleanup("job")
	require.NoError(t, err)
	cleaner.FinishCleanup(nil)
	assert.Equal(t, RestoreStateFailedCleaned, cleaner.Snapshot().State)
}

func TestAddRestoreTaskOnlyRegistersInMemory(t *testing.T) {
	t.Run("ExistingPersistedStateIsNotReadOrModified", func(t *testing.T) {
		originalStore, _, config := persistenceFixture(t)
		before, err := originalStore.persistence.backend.Load(t.Context(), restoreStateKey("job"))
		require.NoError(t, err)
		fresh := NewStore()
		fresh.ConfigurePersistence(config)
		tracker, err := fresh.AddRestoreTask("job", true)
		require.NoError(t, err)
		assert.Equal(t, RestoreStateInitial, tracker.Snapshot().State)
		assert.Empty(t, tracker.Snapshot().Colls)
		assert.Nil(t, fresh.persistence.backend)
		after, err := originalStore.persistence.backend.Load(t.Context(), restoreStateKey("job"))
		require.NoError(t, err)
		assert.Equal(t, before, after)
	})

	t.Run("UnavailableStorageDoesNotBlockCreation", func(t *testing.T) {
		config := cfg.New().Restore.Persistence
		config.Enabled.Val = true
		config.Storage.Provider.Val = "local"
		config.Storage.RootPath.Val = filepath.Join(t.TempDir(), "not-a-directory")
		require.NoError(t, os.WriteFile(config.Storage.RootPath.Val, []byte("occupied"), 0600))
		store := NewStore()
		store.ConfigurePersistence(config)
		tracker, err := store.AddRestoreTask("job", true)
		require.NoError(t, err)
		tracker.SavePlan(&restoreplan.Plan{})
		status := tracker.Finish(assert.AnError)
		assert.Equal(t, RestoreStateFail, status.State)
		assert.Equal(t, assert.AnError.Error(), status.ErrorMessage)
		_, _, err = store.StartResume("job")
		assert.ErrorIs(t, err, ErrPersistence)
		assert.Equal(t, RestoreStateFail, tracker.Snapshot().State)
		// Once storage recovers, Resume must write its marker before executing.
		require.NoError(t, os.Remove(config.Storage.RootPath.Val))
		_, _, err = store.StartResume("job")
		require.NoError(t, err)
		assert.Equal(t, RestoreStateExecuting, tracker.Snapshot().State)
	})

	t.Run("BackendInitializationFailureDoesNotBlockCreation", func(t *testing.T) {
		config := cfg.New().Restore.Persistence
		config.Enabled.Val = true
		config.Storage.Provider.Val = "invalid"
		store := NewStore()
		store.ConfigurePersistence(config)
		tracker, err := store.AddRestoreTask("job", true)
		require.NoError(t, err)
		tracker.SavePlan(&restoreplan.Plan{})
		status := tracker.Finish(assert.AnError)
		assert.Equal(t, assert.AnError.Error(), status.ErrorMessage)
		_, _, err = store.StartResume("job")
		assert.ErrorIs(t, err, ErrPersistence)
		assert.Equal(t, RestoreStateFail, tracker.Snapshot().State)
	})

	t.Run("DuplicateMemoryIDKeepsOriginalTask", func(t *testing.T) {
		store := NewStore()
		original, err := store.AddRestoreTask("job", true)
		require.NoError(t, err)
		original.SetExecuting()
		duplicate, err := store.AddRestoreTask("job", false)
		assert.ErrorIs(t, err, ErrRestoreConflict)
		assert.Nil(t, duplicate)
		assert.Same(t, original.task, store.restoreTask["job"])
		status, err := store.GetRestoreTask("job")
		require.NoError(t, err)
		assert.Equal(t, RestoreStateExecuting, status.State)
		assert.True(t, status.EnableResume)
	})

	t.Run("TasksUseSystemConfiguration", func(t *testing.T) {
		config := cfg.New().Restore.Persistence
		config.Enabled.Val = true
		config.Storage.Provider.Val = "local"
		config.Storage.RootPath.Val = t.TempDir()
		store := NewStore()
		tracker, err := store.AddRestoreTask("job", true)
		require.NoError(t, err)
		store.ConfigurePersistence(config)
		tracker.SavePlan(&restoreplan.Plan{})
		tracker.Finish(assert.AnError)
		cold := NewStore()
		cold.ConfigurePersistence(config)
		_, _, err = cold.StartResume("job")
		assert.NoError(t, err)
	})
}

func TestRestoreTasksSharePersistenceBackend(t *testing.T) {
	previous, _, config := persistenceFixture(t)
	store := NewStore()
	store.ConfigurePersistence(config)
	backend := &faultyRestoreStore{PersistenceBackend: previous.persistence.backend}
	store.persistence.backend = backend

	// Both a cold-loaded task and a newly registered task use this backend.
	_, err := store.GetRestoreTask("job")
	require.NoError(t, err)
	assert.Equal(t, 1, backend.loads)
	fresh, err := store.AddRestoreTask("new-job", true)
	require.NoError(t, err)
	assert.Equal(t, 1, backend.loads, "registration must not read persistence")
	assert.Zero(t, backend.saves)
	fresh.SavePlan(&restoreplan.Plan{})
	fresh.Finish(assert.AnError)
	assert.Equal(t, 1, backend.saves)

	owner, _, err := store.StartResume("job")
	require.NoError(t, err)
	assert.Equal(t, 2, backend.saves)
	owner.Finish(nil)
	assert.Equal(t, 1, backend.deletes)

	owner, _, err = store.StartResume("new-job")
	require.NoError(t, err)
	assert.Equal(t, 3, backend.saves)
	owner.Finish(nil)
	assert.Equal(t, 2, backend.deletes)
	assert.Same(t, backend, store.persistence.backend)
	_, err = backend.Load(t.Context(), restoreStateKey("job"))
	assert.ErrorIs(t, err, ErrPersistenceKeyNotFound)
	_, err = backend.Load(t.Context(), restoreStateKey("new-job"))
	assert.ErrorIs(t, err, ErrPersistenceKeyNotFound)
}

func TestRestorePersistenceConcurrentInitialization(t *testing.T) {
	config := cfg.New().Restore.Persistence
	config.Enabled.Val = true
	config.Storage.Provider.Val = "s3"
	config.Storage.Address.Val = "localhost"
	config.Storage.Port.Val = 9000
	config.Storage.BucketName.Val = "restore-state"
	store := NewStore()
	store.ConfigurePersistence(config)
	backends := make([]PersistenceBackend, 16)
	errs := make([]error, len(backends))
	var wg sync.WaitGroup
	for i := range backends {
		wg.Go(func() {
			backends[i], _, errs[i] = store.persistenceStore()
		})
	}
	wg.Wait()
	for i, backend := range backends {
		require.NoError(t, errs[i])
		assert.Same(t, backends[0], backend, "concurrent callers must reuse one S3 client")
	}
}

func TestColdLoadRejectsUnnamedNonL0Batch(t *testing.T) {
	_, tr, config := persistenceFixture(t)
	batch := tr.task.collTask[collref.New("db", "coll")].jobs[0].binlogImport.batch
	batch.PartitionName = ""
	batch.L0 = false
	data, err := tr.task.encodeLocked(markerAvailable, persistenceLocationFromConfig(config))
	require.NoError(t, err)
	_, err = decodeRestore("job", data, false)
	assert.ErrorContains(t, err, "empty partition name for non-L0 batch")
}

func TestColdLoadRejectsUnsafeStagingDir(t *testing.T) {
	_, tr, config := persistenceFixture(t)
	target := collref.New("db", "coll")
	for _, tc := range []struct{ name, dir string }{
		{"StoragePrefix", "insert_log/"},
		{"NonUUID", "restore-temp-job-db-coll-invalid/"},
		{"DifferentTask", strings.Replace(fixtureStagingDir, "-job-", "-other-", 1)},
		{"MissingDelimiter", strings.TrimSuffix(fixtureStagingDir, "/")},
		{"ParentComponent", fixtureStagingDir + "../"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tr.task.collTask[target].jobs[0].binlogImport.stagingDir = tc.dir
			data, err := tr.task.encodeLocked(markerAvailable, persistenceLocationFromConfig(config))
			require.NoError(t, err)
			for _, cleanup := range []bool{false, true} {
				task, err := decodeRestore("job", data, cleanup)
				assert.ErrorContains(t, err, "invalid staging directory")
				assert.Nil(t, task)
			}
		})
	}
}
