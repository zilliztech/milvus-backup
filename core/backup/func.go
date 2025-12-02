package backup

import (
	"errors"
	"fmt"
	"time"

	"github.com/samber/lo"

	"github.com/zilliztech/milvus-backup/internal/validate"
)

const _maxNameLength = 128

func DefaultName(t time.Time) string {
	datetime := t.UTC().Format("2006_01_02_15_04_05")
	nanosecond := t.Nanosecond()
	return fmt.Sprintf("backup_%s_%d", datetime, nanosecond)
}

func ValidateName(name string) error {
	if validate.HasWhitespace(name) {
		return errors.New("backup name should not contain whitespace")
	}

	if len(name) > _maxNameLength {
		return fmt.Errorf("backup name length should not exceed %d", _maxNameLength)
	}

	firstRune := []rune(name)[0]
	if firstRune != '_' && !validate.IsAlpha(firstRune) {
		return errors.New("backup name should start with an underscore or letter")
	}

	if validate.HasSpecialChar(name) {
		return errors.New("backup name should only contain numbers, letters and underscores")
	}

	return nil
}

var _strategyMap = map[string]Strategy{
	"meta_only":    StrategyMetaOnly,
	"skip_flush":   StrategySkipFlush,
	"bulk_flush":   StrategyBulkFlush,
	"serial_flush": StrategySerialFlush,
}

func SupportStrategy() []string { return lo.Keys(_strategyMap) }

func ParseStrategy(strategy string) (Strategy, error) {
	if strategy == "" {
		return StrategyAuto, nil
	}

	if s, ok := _strategyMap[strategy]; ok {
		return s, nil
	}

	return StrategyAuto, fmt.Errorf("backup: invalid strategy %s", strategy)
}
