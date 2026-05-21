package storage

import (
	"context"
	"fmt"
	"strings"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/internal/log"
)

// _missingSampleSize bounds how many missing keys are listed in the error so a
// large failure does not produce an unreadable message.
const _missingSampleSize = 5

type VerifyPrefixOpt struct {
	// Cli is the storage client used to list the objects under Prefix.
	Cli Client
	// Prefix is the destination prefix to list recursively.
	Prefix string
	// Expected maps every object key that must exist under Prefix to its
	// expected size.
	Expected map[string]int64
}

// VerifyPrefixTask verifies that every object in Expected exists under Prefix
// with a matching size. It lists the prefix once instead of issuing a HeadObject
// per object, so the request count drops from O(N) to O(N/1000) (ListObjects
// returns up to 1000 keys per request) and it only needs ListObjects permission.
type VerifyPrefixTask struct {
	opt VerifyPrefixOpt

	logger *zap.Logger
}

func NewVerifyPrefixTask(opt VerifyPrefixOpt) *VerifyPrefixTask {
	return &VerifyPrefixTask{
		opt:    opt,
		logger: log.L().With(zap.String("prefix", opt.Prefix)),
	}
}

func (t *VerifyPrefixTask) Execute(ctx context.Context) error {
	if len(t.opt.Expected) == 0 {
		return nil
	}
	t.logger.Info("start verify prefix", zap.Int("expected_num", len(t.opt.Expected)))

	iter, err := t.opt.Cli.ListPrefix(ctx, t.opt.Prefix, true)
	if err != nil {
		return fmt.Errorf("storage: verify prefix list prefix %w", err)
	}

	// Track keys not yet seen in the listing; drop each as it is found.
	missing := make(map[string]int64, len(t.opt.Expected))
	for key, size := range t.opt.Expected {
		missing[key] = size
	}

	for iter.HasNext() {
		attr, err := iter.Next()
		if err != nil {
			return fmt.Errorf("storage: verify prefix iter object %w", err)
		}

		want, ok := t.opt.Expected[attr.Key]
		if !ok {
			continue
		}
		if attr.Length != want {
			return fmt.Errorf("storage: verify prefix size mismatch, key=%s want=%d got=%d", attr.Key, want, attr.Length)
		}
		delete(missing, attr.Key)
	}

	if len(missing) > 0 {
		return fmt.Errorf("storage: verify prefix %d objects missing under %s, e.g. [%s]",
			len(missing), t.opt.Prefix, sampleKeys(missing))
	}

	t.logger.Info("verify prefix success", zap.Int("verified_num", len(t.opt.Expected)))
	return nil
}

// sampleKeys returns up to _missingSampleSize keys joined by ", " for error
// messages.
func sampleKeys(keys map[string]int64) string {
	sample := make([]string, 0, _missingSampleSize)
	for key := range keys {
		sample = append(sample, key)
		if len(sample) == _missingSampleSize {
			break
		}
	}
	return strings.Join(sample, ", ")
}
