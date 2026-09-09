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

// Input is one value a source holds: the raw value, the kind of source it
// came from, and the key or variable name it was spelled with there. Sources
// the process reads directly stamp the current schema's spelling; a source a
// schema translation builds stamps the spelling of the schema the value came
// from, so provenance survives the rename.
type Input struct {
	Value     any
	Kind      SourceKind
	SourceKey string
}

// Source holds the raw, schema-independent inputs a configuration is resolved
// from: the --set overrides, a snapshot of the process environment, and the
// flattened config file. Nothing is read after construction, so a source
// resolves the same way every time it is consulted.
type Source struct {
	path string

	// override is keyed by lower-cased key, while overrideKeys keeps the
	// spelling the operator used so errors quote it back unchanged.
	override     map[string]string
	overrideKeys []string

	// env is the environment snapshot, keyed by variable name.
	env map[string]string

	// configFile holds the flattened file keyed by lower-cased dotted key.
	// Values keep the Input they were read as, so a translated source can
	// name the schema version each value actually came from.
	configFile map[string]Input
}

// NewSource reads configPath and flattens it into dotted lower-case keys, and
// snapshots the process environment. An empty configPath yields a source
// backed by overrides and env only.
func NewSource(configPath string, overrides map[string]string) (*Source, error) {
	s := &Source{
		override:   make(map[string]string, len(overrides)),
		env:        snapshotEnv(),
		configFile: map[string]Input{},
	}

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

	flat := map[string]any{}
	if err := flattenAny("", decoded, flat); err != nil {
		return nil, fmt.Errorf("cfg: flatten yaml %s: %w", resolved, err)
	}
	for k, v := range flat {
		s.configFile[k] = Input{Value: v, Kind: SourceConfigFile, SourceKey: k}
	}
	s.path = resolved

	return s, nil
}

// NewTranslatedSource builds a source from the product of a schema
// translation rather than from a file read off disk. file holds the
// translated entries keyed by lower-cased target config key, each carrying
// the source schema's spelling it came from; override holds the --set values
// the translation does not rename, keyed by the spelling the operator gave.
// The environment is left empty: the translation already carried over every
// value worth resolving, under the target schema's names.
func NewTranslatedSource(path string, file map[string]Input, override map[string]string) *Source {
	s := &Source{
		path:         path,
		override:     make(map[string]string, len(override)),
		overrideKeys: make([]string, 0, len(override)),
		env:          map[string]string{},
		configFile:   file,
	}

	for k, v := range override {
		s.override[strings.ToLower(k)] = v
		s.overrideKeys = append(s.overrideKeys, k)
	}
	slices.Sort(s.overrideKeys)

	return s
}

// WithOverrides returns a copy of the source with overrides merged into the
// --set layer: a key already present takes the new value, and the spelling the
// latest caller used is the one errors quote back. The receiver is never
// mutated, so one loaded source forks into as many independent views as
// needed.
func (s *Source) WithOverrides(overrides map[string]string) *Source {
	out := &Source{
		path: s.path,
		// The env and file layers are never written after construction, so the
		// copy shares them; only the override layer being merged is rebuilt.
		env:        s.env,
		configFile: s.configFile,
		override:   make(map[string]string, len(s.override)+len(overrides)),
	}
	for k, v := range s.override {
		out.override[k] = v
	}

	spelling := make(map[string]string, len(s.overrideKeys)+len(overrides))
	for _, k := range s.overrideKeys {
		spelling[strings.ToLower(k)] = k
	}
	for k, v := range overrides {
		lower := strings.ToLower(k)
		out.override[lower] = v
		spelling[lower] = k
	}

	out.overrideKeys = make([]string, 0, len(spelling))
	for _, k := range spelling {
		out.overrideKeys = append(out.overrideKeys, k)
	}
	slices.Sort(out.overrideKeys)

	return out
}

func snapshotEnv() map[string]string {
	env := make(map[string]string)
	for _, kv := range os.Environ() {
		name, value, ok := strings.Cut(kv, "=")
		if ok {
			env[name] = value
		}
	}

	return env
}

func (s *Source) lookupOverride(key string) (string, bool) {
	val, ok := s.override[strings.ToLower(key)]
	return val, ok
}

func (s *Source) lookupEnv(key string) (string, bool) {
	val, ok := s.env[key]
	return val, ok
}

func (s *Source) lookupConfigFile(key string) (Input, bool) {
	val, ok := s.configFile[strings.ToLower(key)]
	return val, ok
}

// OverrideValue returns the raw --set value held for key, matched
// case-insensitively. Schema translation reads overrides through it to carry
// them into the target schema's spelling.
func (s *Source) OverrideValue(key string) (string, bool) { return s.lookupOverride(key) }

// EnvValue returns the value the environment snapshot holds for the variable
// name. Variable names are matched exactly, as os.LookupEnv does.
func (s *Source) EnvValue(name string) (string, bool) { return s.lookupEnv(name) }

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
func (s *Source) ConfigFileValue(key string) (any, bool) {
	in, ok := s.lookupConfigFile(key)
	if !ok {
		return nil, false
	}

	return in.Value, true
}

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
