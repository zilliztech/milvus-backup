package server

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
)

func TestServerStartupChecksPersistence(t *testing.T) {
	for _, tc := range []struct {
		name             string
		enabled, blocked bool
	}{
		{name: "Healthy", enabled: true},
		{name: "FailureDoesNotBlockStartup", enabled: true, blocked: true},
		{name: "Disabled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			params := cfg.New()
			dir := filepath.Join(t.TempDir(), "state")
			params.Restore.Persistence.Enabled.Val = tc.enabled
			params.Restore.Persistence.Type.Val = "local"
			params.Restore.Persistence.Local.Directory.Val = dir
			if tc.blocked {
				require.NoError(t, os.WriteFile(dir, []byte("blocked"), 0600))
			}
			t.Cleanup(func() { jobstate.Default().ConfigurePersistence(cfg.PersistenceConfig{}) })
			server, err := New(params)
			require.NoError(t, err)
			require.NotNil(t, server)
			if tc.blocked {
				_, err = jobstate.Default().AddRestoreTask(uuid.NewString(), true)
				assert.NoError(t, err)
			} else if tc.enabled {
				entries, err := os.ReadDir(dir)
				require.NoError(t, err)
				assert.Empty(t, entries)
			} else {
				assert.NoDirExists(t, dir)
			}
		})
	}
}
