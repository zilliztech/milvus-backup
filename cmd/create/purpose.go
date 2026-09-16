package create

import (
	"fmt"
	"strings"

	"github.com/samber/lo"

	"github.com/zilliztech/milvus-backup/core/backup"
)

// The --for purpose preset is CLI sugar, phase one: the CLI expands it into the
// option values the matching restore path depends on, and nothing about it
// reaches the core task or the backup meta. Should the preset stick, a later
// change can move the expansion down and record the purpose alongside the meta.

// preset is the expansion of one purpose: the values the matching restore path
// requires. The preset always wins over the flags the caller passed — a create
// that only looks successful and fails at restore time is the failure mode the
// preset exists to prevent, so a conflicting flag is overridden, not reported.
// A FormatAuto / StrategyAuto preset value leaves the caller's choice alone.
type preset struct {
	format   backup.Format
	strategy backup.Strategy

	rbac bool

	// indexExtra forces --backup_index_extra on. Secondary is the one purpose
	// that cannot serve without it: the etcd-sourced index extra info and
	// dynamic field schemas it gates are what a secondary restore replays.
	indexExtra bool
}

var _presets = map[string]preset{
	// Secondary replays the source's writes from a flush point, and bulk_flush
	// records one with the least interruption of the source; a serial flush adds
	// nothing a secondary needs. This mirrors what the secondary CI job passes
	// by hand: create --backup_index_extra --format=binlog.
	"secondary": {format: backup.FormatBinlog, strategy: backup.StrategyBulkFlush, rbac: true, indexExtra: true},
	"archive":   {format: backup.FormatBinlog, rbac: true},
	"clone":     {rbac: true},
}

func SupportPurpose() []string { return lo.Keys(_presets) }

// expandPreset applies the preset named name to opt: every knob the preset pins
// takes the preset value, overriding whatever the caller passed, and every knob
// it leaves alone keeps the caller's value. An empty name is a no-op: a create
// without a preset keeps every knob the caller set.
func expandPreset(name string, opt *backup.Option) error {
	if name == "" {
		return nil
	}

	ps, ok := _presets[name]
	if !ok {
		return fmt.Errorf("invalid for %s, only support %s", name, strings.Join(SupportPurpose(), ","))
	}

	if ps.format != backup.FormatAuto {
		opt.Format = ps.format
	}
	if ps.strategy != backup.StrategyAuto {
		opt.Strategy = ps.strategy
	}
	opt.BackupRBAC = ps.rbac
	if ps.indexExtra {
		opt.BackupIndexExtra = true
	}

	return nil
}

// presetSummary renders the purpose-relevant knobs of an expanded option as one
// line, for printing at create time. A create without a preset returns the
// empty string.
func presetSummary(name string, opt backup.Option) string {
	if name == "" {
		return ""
	}
	return fmt.Sprintf("for=%s rbac=%v format=%s strategy=%s index_extra=%v",
		name, opt.BackupRBAC, formatName(opt.Format), strategyName(opt.Strategy), opt.BackupIndexExtra)
}

var _formatNames = map[backup.Format]string{
	backup.FormatAuto:     "auto",
	backup.FormatBinlog:   "binlog",
	backup.FormatSnapshot: "snapshot",
}

var _strategyNames = map[backup.Strategy]string{
	backup.StrategyAuto:        "auto",
	backup.StrategyMetaOnly:    "meta_only",
	backup.StrategySkipFlush:   "skip_flush",
	backup.StrategyBulkFlush:   "bulk_flush",
	backup.StrategySerialFlush: "serial_flush",
}

// formatName renders a format as the flag value that selects it.
func formatName(f backup.Format) string {
	if name, ok := _formatNames[f]; ok {
		return name
	}
	return f.String()
}

// strategyName renders a strategy as the flag value that selects it.
func strategyName(s backup.Strategy) string {
	if name, ok := _strategyNames[s]; ok {
		return name
	}
	return s.String()
}
