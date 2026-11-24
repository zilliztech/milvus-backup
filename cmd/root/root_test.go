package root

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/core/server"
	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
	"github.com/zilliztech/milvus-backup/internal/log"
)

func TestCLIStartupChecksPersistence(t *testing.T) {
	for _, tc := range []struct {
		name             string
		enabled, blocked bool
	}{
		{name: "Healthy", enabled: true},
		{name: "FailureOnlyLogs", enabled: true, blocked: true},
		{name: "Disabled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			dir := filepath.Join(root, "state")
			logFile := filepath.Join(root, "startup.log")
			if tc.blocked {
				require.NoError(t, os.WriteFile(dir, []byte("blocked"), 0600))
			}
			configFile := filepath.Join(root, "backup.yaml")
			body := fmt.Sprintf("configVersion: v2\nlog:\n  console: false\n  file:\n    path: %q\nrestore:\n  persistence:\n    enabled: %t\n    storage:\n      provider: local\n      rootPath: %q\n", logFile, tc.enabled, dir)
			require.NoError(t, os.WriteFile(configFile, []byte(body), 0600))
			t.Cleanup(func() {
				jobstate.Default().ConfigurePersistence(cfg.PersistenceConfig{})
				log.InitLogger(&log.Config{Level: "info"})
			})
			params := (&Options{Config: configFile}).InitGlobalVars()
			require.NotNil(t, params)
			if tc.blocked {
				require.NoError(t, log.L().Sync())
				data, err := os.ReadFile(logFile)
				require.NoError(t, err)
				assert.Contains(t, string(data), "persistence startup check failed; continuing startup")
				_, err = jobstate.Default().AddRestoreTask(uuid.NewString(), true)
				assert.NoError(t, err)
			} else if tc.enabled {
				entries, err := os.ReadDir(dir)
				require.NoError(t, err, "CLI startup must actually probe the backend")
				assert.Empty(t, entries)
			} else {
				assert.NoDirExists(t, dir)
			}
			if tc.enabled {
				require.NoError(t, os.Remove(dir))
				_, err := server.New(params)
				require.NoError(t, err)
				assert.NoDirExists(t, dir, "server CLI startup must not repeat the probe")
			}
		})
	}
}
