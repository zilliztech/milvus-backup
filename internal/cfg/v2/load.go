package v2

import (
	"fmt"
	"strings"

	"github.com/zilliztech/milvus-backup/internal/cfg/param"
	"github.com/zilliztech/milvus-backup/internal/log"
)

// Load loads a v2 configuration from yaml + overrides + env.
//
// precedence: overrides (--set) > env > config file > default, among the names
// the v2 schema defines. v1 names are not aliased; they are warned about and
// ignored.
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
	// Unknown names are warned about and ignored, not rejected: a stray or
	// misspelled key should not stop a backup. They are never applied either
	// way, so resolving proceeds from the declared keys alone.
	for _, w := range checkKeys(cfg, src) {
		log.Warn(w)
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

// checkKeys returns a warning for every config file key and --set path the v2
// schema does not declare. Unknown names are ignored rather than applied, so a
// misspelling or v1 leftover is surfaced without stopping the load.
func checkKeys(cfg *Config, src *param.Source) []string {
	configKeys, envNames := param.DeclaredKeys(cfg)
	configKeys[strings.ToLower(VersionKey)] = struct{}{}

	var warnings []string
	for _, key := range src.ConfigFileKeys() {
		if _, ok := configKeys[key]; !ok {
			warnings = append(warnings, unknownKeyWarning("config file key", key))
		}
	}

	// An override may be spelled either way, so it is unknown only when it is
	// neither a config key nor an environment variable name.
	for _, key := range src.OverrideKeys() {
		_, isConfigKey := configKeys[strings.ToLower(key)]
		_, isEnvName := envNames[strings.ToLower(key)]
		if !isConfigKey && !isEnvName {
			warnings = append(warnings, unknownKeyWarning("--set key", key))
		}
	}

	return warnings
}

func unknownKeyWarning(kind, key string) string {
	to, isLegacy := Migration(key)
	switch {
	case isLegacy && to == "":
		return fmt.Sprintf("cfg: v1 %s %q was removed in v2, ignoring it", kind, key)
	case isLegacy:
		return fmt.Sprintf("cfg: v1 %s %q is not accepted by a v2 config, ignoring it; use %s instead", kind, key, to)
	default:
		return fmt.Sprintf("cfg: unknown v2 %s %q, ignoring it", kind, key)
	}
}
