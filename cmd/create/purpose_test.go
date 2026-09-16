package create

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/core/backup"
)

func TestExpandPreset(t *testing.T) {
	t.Run("SecondaryForcesAllFourKnobs", func(t *testing.T) {
		var opt backup.Option
		require.NoError(t, expandPreset("secondary", &opt))

		assert.True(t, opt.BackupRBAC)
		assert.True(t, opt.BackupIndexExtra)
		assert.Equal(t, backup.FormatBinlog, opt.Format)
		assert.Equal(t, backup.StrategyBulkFlush, opt.Strategy)
	})

	t.Run("SecondaryOverridesConflictingFlags", func(t *testing.T) {
		opt := backup.Option{
			Format:   backup.FormatSnapshot,
			Strategy: backup.StrategySerialFlush,
			// BackupRBAC and BackupIndexExtra stay at their zero value here,
			// standing in for --rbac=false and a missing --backup_index_extra.
		}
		require.NoError(t, expandPreset("secondary", &opt))

		assert.True(t, opt.BackupRBAC)
		assert.True(t, opt.BackupIndexExtra)
		assert.Equal(t, backup.FormatBinlog, opt.Format)
		assert.Equal(t, backup.StrategyBulkFlush, opt.Strategy)
	})

	t.Run("ArchiveForcesBinlogAndRBACLeavesTheRest", func(t *testing.T) {
		var opt backup.Option
		require.NoError(t, expandPreset("archive", &opt))

		assert.True(t, opt.BackupRBAC)
		assert.Equal(t, backup.FormatBinlog, opt.Format)
		assert.Equal(t, backup.StrategyAuto, opt.Strategy)
		assert.False(t, opt.BackupIndexExtra)

		opt = backup.Option{Format: backup.FormatSnapshot, Strategy: backup.StrategySerialFlush}
		require.NoError(t, expandPreset("archive", &opt))
		assert.Equal(t, backup.FormatBinlog, opt.Format, "archive overrides an explicit snapshot format")
		assert.Equal(t, backup.StrategySerialFlush, opt.Strategy, "archive leaves the caller's strategy alone")
	})

	t.Run("CloneForcesRBACOnly", func(t *testing.T) {
		var opt backup.Option
		require.NoError(t, expandPreset("clone", &opt))

		assert.True(t, opt.BackupRBAC)
		assert.Equal(t, backup.FormatAuto, opt.Format)
		assert.Equal(t, backup.StrategyAuto, opt.Strategy)
		assert.False(t, opt.BackupIndexExtra)

		opt = backup.Option{Format: backup.FormatSnapshot, Strategy: backup.StrategySkipFlush}
		require.NoError(t, expandPreset("clone", &opt))
		assert.Equal(t, backup.FormatSnapshot, opt.Format)
		assert.Equal(t, backup.StrategySkipFlush, opt.Strategy)
	})

	t.Run("IndexExtraIsForcedOnlyOnSecondary", func(t *testing.T) {
		for _, name := range []string{"archive", "clone"} {
			var opt backup.Option
			require.NoError(t, expandPreset(name, &opt))
			assert.False(t, opt.BackupIndexExtra, "%s must not force backup_index_extra", name)

			opt = backup.Option{BackupIndexExtra: true}
			require.NoError(t, expandPreset(name, &opt))
			assert.True(t, opt.BackupIndexExtra, "%s must not clear the caller's backup_index_extra", name)
		}
	})

	t.Run("NoPresetIsNoOp", func(t *testing.T) {
		opt := backup.Option{
			Strategy:         backup.StrategySkipFlush,
			Format:           backup.FormatSnapshot,
			BackupRBAC:       false,
			BackupIndexExtra: false,
		}
		require.NoError(t, expandPreset("", &opt))

		assert.Equal(t, backup.FormatSnapshot, opt.Format)
		assert.Equal(t, backup.StrategySkipFlush, opt.Strategy)
		assert.False(t, opt.BackupRBAC)
		assert.False(t, opt.BackupIndexExtra)
	})

	t.Run("UnknownPurposeFails", func(t *testing.T) {
		var opt backup.Option
		err := expandPreset("dr", &opt)
		assert.ErrorContains(t, err, "invalid for dr")
	})
}

func TestPresetSummary(t *testing.T) {
	t.Run("EmptyWithoutPreset", func(t *testing.T) {
		assert.Empty(t, presetSummary("", backup.Option{}))
	})

	t.Run("NamesExpandedKnobs", func(t *testing.T) {
		var opt backup.Option
		require.NoError(t, expandPreset("secondary", &opt))
		assert.Equal(t, "for=secondary rbac=true format=binlog strategy=bulk_flush index_extra=true",
			presetSummary("secondary", opt))
	})

	t.Run("ReportsCallerValuesOnClone", func(t *testing.T) {
		opt := backup.Option{Format: backup.FormatSnapshot}
		require.NoError(t, expandPreset("clone", &opt))
		assert.Equal(t, "for=clone rbac=true format=snapshot strategy=auto index_extra=false",
			presetSummary("clone", opt))
	})
}
