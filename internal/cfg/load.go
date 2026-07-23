package cfg

import (
	"github.com/zilliztech/milvus-backup/internal/cfg/param"
)

// Load loads configuration from yaml + overrides + env.
//
// precedence: overrides (--set) > env > config file > default
func Load(configPath string, overrides map[string]string) (*Config, error) {
	cfg := New()

	src, err := param.NewSource(configPath, overrides)
	if err != nil {
		return nil, err
	}
	if err := cfg.Resolve(src); err != nil {
		return nil, err
	}

	return cfg, nil
}
