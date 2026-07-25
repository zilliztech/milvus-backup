package cfg

import (
	"github.com/zilliztech/milvus-backup/internal/cfg/param"
)

// Load loads configuration from yaml + overrides + env.
//
// precedence: overrides (--set) > env > config file > default
func Load(configPath string, overrides map[string]string) (*Config, error) {
	src, err := param.NewSource(configPath, overrides)
	if err != nil {
		return nil, err
	}

	return LoadFrom(src)
}

// LoadFrom resolves a v1 configuration from an already read source. It is the
// entry point the version dispatcher uses, so the file is read only once.
func LoadFrom(src *param.Source) (*Config, error) {
	cfg := New()

	if err := cfg.Resolve(src); err != nil {
		return nil, err
	}

	return cfg, nil
}
