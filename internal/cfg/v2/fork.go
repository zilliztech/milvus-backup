package v2

import (
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"

	"github.com/zilliztech/milvus-backup/internal/cfg/param"
)

// Fork derives an independent copy of c with overrides applied, spelled the
// --set way: dotted config keys (or credential environment names) mapped to
// string values.
//
// A fork is a frozen copy, not a re-load: every leaf keeps the receiver's
// resolved value and provenance unless the override map names it, the process
// environment is not consulted, and backup.storage does not re-inherit from an
// overridden milvus.storage — overrides apply to leaves, and nothing cascades.
// A nil or empty map yields a validated deep copy.
//
// An unknown key is an error, not a warning: unlike a --set on the command
// line, a programmatic caller has no "keep going anyway" case, so a
// misspelling is a bug worth failing on. The derived configuration is
// validated before it is returned, so a fork either differs from c exactly at
// the overridden keys or fails outright.
func (c *Config) Fork(overrides map[string]string) (*Config, error) {
	if err := checkForkKeys(c, overrides); err != nil {
		return nil, err
	}

	raw, err := Render(c, nil)
	if err != nil {
		return nil, err
	}
	src, err := param.NewSourceYAML(raw, overrides)
	if err != nil {
		return nil, fmt.Errorf("cfg: fork config: %w", err)
	}

	forked := New()
	if err := forked.Resolve(src); err != nil {
		return nil, fmt.Errorf("cfg: fork config: %w", err)
	}
	// Re-resolution is only the application mechanism: it parses and stamps
	// the overridden leaves, but it also re-derives every other leaf from the
	// rendered values, losing the original provenance and re-running storage
	// inheritance. Restore each leaf the map does not name from the receiver,
	// so the fork differs from it exactly at the overridden keys.
	restoreUntouched(c, forked, overrides)

	if err := forked.Validate(); err != nil {
		return nil, fmt.Errorf("cfg: fork config: %w", err)
	}

	return forked, nil
}

// checkForkKeys rejects every override key the schema does not declare, with a
// migration hint when one names a v1 key.
func checkForkKeys(c *Config, overrides map[string]string) error {
	configKeys, envNames := param.DeclaredKeys(c)

	var errs []error
	for _, key := range slices.Sorted(maps.Keys(overrides)) {
		k := strings.ToLower(key)
		if _, ok := configKeys[k]; ok {
			continue
		}
		if _, ok := envNames[k]; ok {
			continue
		}
		errs = append(errs, errors.New(unknownForkKey(key)))
	}

	return errors.Join(errs...)
}

func unknownForkKey(key string) string {
	to, isLegacy := Migration(key)
	switch {
	case isLegacy && to == "":
		return fmt.Sprintf("cfg: fork override %q is a v1 key that was removed in v2", key)
	case isLegacy:
		return fmt.Sprintf("cfg: fork override %q is a v1 key, use %s instead", key, to)
	default:
		return fmt.Sprintf("cfg: fork override %q matches no v2 config key or credential environment name", key)
	}
}

// restoreUntouched copies every leaf the override map does not name from
// parent to forked. Walk visits both trees in the same deterministic field
// order, so pairing them by index is safe.
func restoreUntouched(parent, forked *Config, overrides map[string]string) {
	overridden := make(map[string]struct{}, len(overrides))
	for key := range overrides {
		overridden[strings.ToLower(key)] = struct{}{}
	}

	var parents []reflect.Value
	param.Walk(parent, func(_ string, f param.Field) {
		parents = append(parents, reflect.ValueOf(f).Elem())
	})

	i := 0
	param.Walk(forked, func(_ string, f param.Field) {
		p := parents[i]
		i++
		if namesOverridden(f, overridden) {
			return
		}
		reflect.ValueOf(f).Elem().Set(p)
		// The struct copy shares the list backing array with the parent; clone
		// it so the two configurations are independent.
		if l, ok := f.(*param.List); ok {
			l.Val = slices.Clone(l.Val)
		}
	})
}

// namesOverridden reports whether the override map names f, by config key or
// by environment name — the two spellings a resolution lookup accepts.
func namesOverridden(f param.Field, overridden map[string]struct{}) bool {
	for _, key := range f.ConfigKeys() {
		if _, ok := overridden[strings.ToLower(key)]; ok {
			return true
		}
	}
	for _, key := range f.EnvNames() {
		if _, ok := overridden[strings.ToLower(key)]; ok {
			return true
		}
	}

	return false
}
