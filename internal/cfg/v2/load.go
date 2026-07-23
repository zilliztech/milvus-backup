package v2

import (
	"errors"
	"fmt"
	"strings"

	"github.com/zilliztech/milvus-backup/internal/cfg/param"
)

// Load loads a v2 configuration from yaml + overrides + env.
//
// precedence: overrides (--set) > env > config file > default, among the names
// the v2 schema defines. v1 names are rejected, not aliased.
func Load(configPath string, overrides map[string]string) (*Config, error) {
	src, err := param.NewSource(configPath, overrides)
	if err != nil {
		return nil, err
	}

	return LoadFrom(src)
}

// LoadFrom resolves a v2 configuration from an already read source. It is the
// entry point the version dispatcher uses, so the file is read only once.
func LoadFrom(src *param.Source) (*Config, error) {
	cfg := New()

	if err := checkVersion(src); err != nil {
		return nil, err
	}
	// Reject unknown names before resolving, so a typo is reported as a typo
	// rather than as whatever the default silently did instead.
	if err := checkKeys(cfg, src); err != nil {
		return nil, err
	}
	if err := cfg.Resolve(src); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

// checkVersion verifies the file carries the v2 discriminator. A source with
// no config file is env-only and has no version to check.
func checkVersion(src *param.Source) error {
	if src.ConfigFilePath() == "" {
		return nil
	}

	raw, ok := src.ConfigFileValue(VersionKey)
	if !ok {
		return fmt.Errorf("cfg: %s is not a v2 config: missing %q, expected %q",
			src.ConfigFilePath(), VersionKey, Version)
	}

	version, ok := raw.(string)
	if !ok || !strings.EqualFold(version, Version) {
		return fmt.Errorf("cfg: %s declares %s %v, but the v2 loader only accepts %q",
			src.ConfigFilePath(), VersionKey, raw, Version)
	}

	return nil
}

// checkKeys rejects every config file key and --set path the v2 schema does
// not declare, so misspellings and v1 leftovers fail instead of falling back
// to a default.
func checkKeys(cfg *Config, src *param.Source) error {
	configKeys, envNames := param.DeclaredKeys(cfg)
	configKeys[strings.ToLower(VersionKey)] = struct{}{}

	var errs []error
	for _, key := range src.ConfigFileKeys() {
		if _, ok := configKeys[key]; !ok {
			errs = append(errs, unknownKeyErr("config file key", key))
		}
	}

	// An override may be spelled either way, so it is unknown only when it is
	// neither a config key nor an environment variable name.
	for _, key := range src.OverrideKeys() {
		_, isConfigKey := configKeys[strings.ToLower(key)]
		_, isEnvName := envNames[strings.ToLower(key)]
		if !isConfigKey && !isEnvName {
			errs = append(errs, unknownKeyErr("--set key", key))
		}
	}

	return errors.Join(errs...)
}

func unknownKeyErr(kind, key string) error {
	to, isLegacy := Migration(key)
	switch {
	case isLegacy && to == "":
		return fmt.Errorf("cfg: v1 %s %q was removed in v2", kind, key)
	case isLegacy:
		return fmt.Errorf("cfg: v1 %s %q is not accepted by a v2 config, use %s instead", kind, key, to)
	default:
		return fmt.Errorf("cfg: unknown v2 %s %q", kind, key)
	}
}
