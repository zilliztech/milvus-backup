package cfg

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"

	"gopkg.in/yaml.v3"
)

// Load loads configuration from yaml + overrides + env.
//
// precedence: overrides (--set) > env > config file > default
func Load(configPath string, overrides map[string]string) (*Config, error) {
	cfg := New()

	src, err := newSource(configPath, overrides)
	if err != nil {
		return nil, err
	}
	if err := cfg.Resolve(src); err != nil {
		return nil, err
	}
	return cfg, nil
}

func newSource(configPath string, overrides map[string]string) (*source, error) {
	s := &source{
		override:   map[string]string{},
		configFile: map[string]any{},
	}

	for k, v := range overrides {
		s.override[strings.ToLower(k)] = v
	}

	if configPath == "" {
		return s, nil
	}

	resolved, err := resolveConfigFilePath(configPath)
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
	s.configFile = out
	return s, nil
}

func resolveConfigFilePath(configPath string) (string, error) {
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
		// internal/cfg/load.go -> ../../configs
		p := filepath.Join(filepath.Dir(fpath), "..", "..", "configs", base)
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
		// keep as-is; currently we don't have list-typed schema, but preserve for future use
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

