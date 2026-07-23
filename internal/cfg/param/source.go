package param

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"syscall"

	"gopkg.in/yaml.v3"
)

// Source holds the raw, schema-independent inputs a configuration is resolved
// from: the --set overrides and the flattened config file. Environment
// variables are read from the process environment on lookup.
type Source struct {
	path string

	// override is keyed by lower-cased key, while overrideKeys keeps the
	// spelling the operator used so errors quote it back unchanged.
	override     map[string]string
	overrideKeys []string

	configFile map[string]any
}

// NewSource reads configPath and flattens it into dotted lower-case keys.
// An empty configPath yields a source backed by overrides and env only.
func NewSource(configPath string, overrides map[string]string) (*Source, error) {
	s := &Source{override: make(map[string]string, len(overrides)), configFile: map[string]any{}}

	for k, v := range overrides {
		s.override[strings.ToLower(k)] = v
		s.overrideKeys = append(s.overrideKeys, k)
	}
	slices.Sort(s.overrideKeys)

	if configPath == "" {
		return s, nil
	}

	resolved, err := ResolveConfigFilePath(configPath)
	if err != nil {
		return nil, err
	}
	raw, err := os.ReadFile(resolved)
	if err != nil {
		return nil, fmt.Errorf("cfg: read config file %s: %w", resolved, err)
	}

	var decoded any
	if err := yaml.Unmarshal(raw, &decoded); err != nil {
		return nil, fmt.Errorf("cfg: parse yaml %s: %w", resolved, err)
	}

	out := map[string]any{}
	if err := flattenAny("", decoded, out); err != nil {
		return nil, fmt.Errorf("cfg: flatten yaml %s: %w", resolved, err)
	}
	s.path = resolved
	s.configFile = out

	return s, nil
}

func (s *Source) lookupOverride(key string) (string, bool) {
	val, ok := s.override[strings.ToLower(key)]
	return val, ok
}

func (s *Source) lookupEnv(key string) (string, bool) {
	val, ok := os.LookupEnv(key)
	return val, ok
}

func (s *Source) lookupConfigFile(key string) (any, bool) {
	val, ok := s.configFile[strings.ToLower(key)]
	return val, ok
}

// ConfigFileKeys returns the flattened config file keys, sorted. Schema
// versions that reject unknown keys compare it against the keys they declare.
func (s *Source) ConfigFileKeys() []string {
	keys := make([]string, 0, len(s.configFile))
	for k := range s.configFile {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	return keys
}

// OverrideKeys returns the --set keys as they were given, sorted. An override
// may name either a config key or an env key, so both are valid spellings.
func (s *Source) OverrideKeys() []string { return s.overrideKeys }

// ConfigFilePath returns the config file the source was read from, or an
// empty string when the configuration comes from overrides and env only.
func (s *Source) ConfigFilePath() string { return s.path }

// ConfigFileValue returns the raw flattened value the config file holds for key.
func (s *Source) ConfigFileValue(key string) (any, bool) { return s.lookupConfigFile(key) }

func ResolveConfigFilePath(configPath string) (string, error) {
	// If user passes an explicit existing path, use it directly.
	if _, err := os.Stat(configPath); err == nil {
		return configPath, nil
	}

	base := filepath.Base(configPath)

	// If MILVUSCONF is set, load from that directory.
	if confDir, ok := syscall.Getenv("MILVUSCONF"); ok && confDir != "" {
		p := filepath.Join(confDir, base)
		if _, err := os.Stat(p); err == nil {
			return p, nil
		}
	}

	// Try ./configs/<base>
	if cwd, err := os.Getwd(); err == nil {
		p := filepath.Join(cwd, "configs", base)
		if _, err := os.Stat(p); err == nil {
			return p, nil
		}
	}

	// Fallback to repo-relative configs/ based on source file location.
	_, fpath, _, ok := runtime.Caller(0)
	if ok {
		// internal/cfg/param/source.go -> ../../../configs
		p := filepath.Join(filepath.Dir(fpath), "..", "..", "..", "configs", base)
		if _, err := os.Stat(p); err == nil {
			return p, nil
		}
	}

	return "", fmt.Errorf("cfg: cannot locate config file %q (tried: %q, $MILVUSCONF, ./configs, repo configs)", configPath, base)
}

func flattenAny(prefix string, v any, out map[string]any) error {
	switch vv := v.(type) {
	case map[string]any:
		for k, child := range vv {
			key := strings.ToLower(k)
			full := key
			if prefix != "" {
				full = prefix + "." + key
			}
			if err := flattenAny(full, child, out); err != nil {
				return err
			}
		}
		return nil
	case map[any]any:
		for k, child := range vv {
			ks, ok := k.(string)
			if !ok {
				return fmt.Errorf("cfg: yaml map key must be string, got %T", k)
			}
			key := strings.ToLower(ks)
			full := key
			if prefix != "" {
				full = prefix + "." + key
			}
			if err := flattenAny(full, child, out); err != nil {
				return err
			}
		}
		return nil
	case []any:
		// Lists are kept as-is, and decoded by the List value type.
		out[prefix] = vv
		return nil
	case nil:
		// ignore nil leaf
		return nil
	default:
		if prefix == "" {
			return fmt.Errorf("cfg: yaml root must be a map, got %T", v)
		}
		out[prefix] = vv
		return nil
	}
}
