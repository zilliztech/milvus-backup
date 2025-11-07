package backup

import (
	"errors"
	"fmt"
	"time"

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

	firstRune := []rune(name)[0] // 正确得到第一个 Unicode 字符（rune）
	if firstRune != '_' && !validate.IsAlpha(firstRune) {
		return errors.New("backup name should start with an underscore or letter")
	}

	if validate.HasSpecialChar(name) {
		return errors.New("backup name should only contain numbers, letters and underscores")
	}

	return nil
}
