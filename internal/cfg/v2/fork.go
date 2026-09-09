package v2

import (
	"fmt"
	"slices"
	"strings"

	"github.com/zilliztech/milvus-backup/internal/cfg/param"
)

// Fork derives an independent configuration from the receiver: the source the
// receiver was loaded from, plus overrides as one more --set layer, resolved
// and validated from scratch. The receiver is never mutated, so one loaded
// configuration serves every request while each request can still run on its
// own view.
//
// A fork is reload-equivalent: it behaves as if the process had been
// restarted with the extra overrides. Re-resolution therefore re-reads the
// environment snapshot the source was built from and re-runs backup.storage
// inheritance, so overriding a milvus.storage leaf cascades into backup.storage
// leaves that were never set explicitly — exactly what an extra --set at
// startup would do. Provenance is reproduced by the re-resolution rather than
// copied: a file leaf keeps its file stamp, an env leaf its env stamp, a
// first-load override its override stamp.
//
// Overrides follow --set semantics: a dotted config key or a credential
// environment name, a list comma-separated. An unknown key is an error rather
// than a warning: a config file may predate the schema, but a key handed to
// Fork was written against this build, so a misspelling is a bug in the
// caller.
//
// Fork(nil) is a validated independent copy. Fork on a Config that was never
// loaded from a source — one New built by hand — is an error, since there is
// nothing to re-resolve.
func (c *Config) Fork(overrides map[string]string) (*Config, error) {
	if c.src == nil {
		return nil, fmt.Errorf("cfg: cannot fork a configuration that was not loaded from a source")
	}
	if err := checkForkKeys(c, overrides); err != nil {
		return nil, err
	}

	src := c.src.WithOverrides(overrides)
	forked, err := Resolve(src)
	if err != nil {
		return nil, err
	}
	if err := forked.Validate(); err != nil {
		return nil, err
	}
	forked.src = src

	return forked, nil
}

// checkForkKeys rejects override keys the v2 schema does not declare, naming
// the v2 replacement when the key is a v1 leftover. Where the load-time check
// warns and ignores, Fork fails: an override that names nothing would silently
// not apply, which is exactly the mistake the caller is trying to make.
func checkForkKeys(c *Config, overrides map[string]string) error {
	configKeys, envNames := param.DeclaredKeys(c)
	configKeys[strings.ToLower(VersionKey)] = struct{}{}

	// Sort so the first error is deterministic; a map iterates in random order.
	keys := make([]string, 0, len(overrides))
	for k := range overrides {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	for _, key := range keys {
		lower := strings.ToLower(key)
		if _, ok := configKeys[lower]; ok {
			continue
		}
		// An override may name a credential by its environment variable, the
		// same spelling --set accepts.
		if _, ok := envNames[lower]; ok {
			continue
		}

		to, isLegacy := Migration(key)
		switch {
		case isLegacy && to == "":
			return fmt.Errorf("cfg: cannot fork: v1 key %q was removed in v2 and names nothing now", key)
		case isLegacy:
			return fmt.Errorf("cfg: cannot fork: v1 key %q is not accepted by a v2 config; use %s instead", key, to)
		default:
			return fmt.Errorf("cfg: cannot fork: unknown v2 key %q", key)
		}
	}

	return nil
}
