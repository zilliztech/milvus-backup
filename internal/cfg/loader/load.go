// Package loader resolves a milvus-backup configuration file of either schema
// version into the v2 configuration the rest of the program runs on.
//
// A v1 file is decoded with the v1 schema and then translated, rather than
// teaching the v2 schema to read v1 names: one file is decoded as one schema
// version, and the mapping between them lives in one place, next to the
// `config migrate` command that writes the same mapping out as a file.
package loader

import (
	"fmt"
	"strings"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/cfg/migrate"
	"github.com/zilliztech/milvus-backup/internal/cfg/param"
	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
	"github.com/zilliztech/milvus-backup/internal/log"
)

// versionV1 is what a config file declares to ask for the v1 schema. v1 files
// predate the discriminator, so it is never required; it exists so a file can
// say which schema it is written in rather than being identified by omission.
const versionV1 = "v1"

// Load resolves the configuration at configPath into a v2 configuration,
// whichever schema version the file is written in.
//
// precedence: overrides (--set) > env > config file > default, among the names
// the file's own schema version defines. A v1 file is resolved with v1 names,
// including the v1 environment variables, and translated afterwards.
func Load(configPath string, overrides map[string]string) (*v2.Config, error) {
	src, err := param.NewSource(configPath, overrides)
	if err != nil {
		return nil, err
	}

	version, declared := v2.DeclaredVersion(src)
	if !declared {
		// With no file, overrides and env are all there is, and they name v2
		// parameters. With a file, the missing discriminator dates it to v1.
		if src.ConfigFilePath() == "" {
			return v2.LoadFrom(src)
		}

		return loadV1(src)
	}

	switch {
	case strings.EqualFold(version, v2.Version):
		return v2.LoadFrom(src)
	case strings.EqualFold(version, versionV1):
		return loadV1(src)
	default:
		return nil, fmt.Errorf("cfg: %s declares %s %q, which is not a schema version this build knows (want %q or %q)",
			src.ConfigFilePath(), v2.VersionKey, version, versionV1, v2.Version)
	}
}

// loadV1 resolves a v1 file and translates it into the v2 configuration the
// program runs on, so a deployment that has not migrated yet keeps working.
func loadV1(src *param.Source) (*v2.Config, error) {
	v1, err := cfg.LoadFrom(src)
	if err != nil {
		return nil, err
	}

	out, err := migrate.Translate(v1)
	if err != nil {
		return nil, err
	}

	// Say so once per run: the v1 schema is still read, but the translation is a
	// compatibility step, and `config migrate` turns it into a file to keep.
	log.Warn("cfg: loaded a v1 configuration and translated it to v2; "+
		"run `milvus-backup config migrate` to convert the file itself",
		zap.String("path", src.ConfigFilePath()))

	return out, nil
}
