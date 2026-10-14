package restore

import (
	"errors"
	"fmt"

	"github.com/samber/lo"
)

var _strategyMap = map[string]Strategy{
	"per_seg":      StrategyPerSeg,
	"merged_seg":   StrategyMergedSeg,
	"restore_snap": StrategyRestoreSnap,
	"import_snap":  StrategyImportSnap,
}

func SupportStrategy() []string { return lo.Keys(_strategyMap) }

func ParseStrategy(strategy string) (Strategy, error) {
	if strategy == "" {
		return StrategyAuto, nil
	}

	if strategy == "snapshot" {
		return StrategyAuto, errors.New(`restore: "snapshot" is a backup format, not a restore strategy; the format comes from the backup itself`)
	}

	if s, ok := _strategyMap[strategy]; ok {
		return s, nil
	}

	return StrategyAuto, fmt.Errorf("restore: invalid strategy %s", strategy)
}
