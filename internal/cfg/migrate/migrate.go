// Package migrate carries a v1 configuration across the schema version
// boundary at the source level: the flattened v1 key map (file, --set
// overrides, and the v1 environment variables) is translated into a flattened
// v2 key map, and the v2 schema performs all resolution. Both schema versions
// therefore flow through the same Source → Resolve → Validate pipeline, and
// provenance survives translation at full fidelity: every translated entry
// names the v1 key or variable it was read from.
//
// The package is the only place in the tree that knows the v1 key space; the
// loader calls into it for a v1 file and everything else runs on v2 alone.
// Removing v1 support means removing this package, the loader's v1 branch,
// and the two v1 source kinds.
//
// Two entry points share the translation. Migrate backs the `config migrate`
// command: it resolves the translated source, and returns a report of
// everything that needs a human's attention. Translate is the config loader's
// compatibility path for a v1 file at startup.
//
// Migration renames configuration keys and environment variables; it does not
// relocate values between mechanisms. A secret an operator keeps in an
// environment variable stays there: the migrated file records the v2 variable
// to set rather than baking the secret in. Translate is the exception, since
// a running process needs the secret itself rather than advice about it.
package migrate

import (
	"fmt"

	"github.com/zilliztech/milvus-backup/internal/cfg/param"
	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
)

// Migrate translates the v1 source into a resolved v2 configuration and a
// report. The returned config always renders; the report carries the
// warnings, the embedded comments, the environment renames, and any
// validation problems (via Report.Err) that --strict promotes to a failure.
func Migrate(src *param.Source) (*v2.Config, *Report, error) {
	r := newReport()

	out, err := v2.Resolve(newTranslator(src, r).translate())
	if err != nil {
		return nil, r, fmt.Errorf("cfg: resolve translated v1 config: %w", err)
	}

	// crossStorage=false maps to transfer.mode=auto, which only diverges from
	// v1 when the two backends differ: for the same backend both do a
	// storage-side copy, so warning then would be noise. The judgement needs
	// the resolved backends, so it is made here rather than in the
	// translation.
	if out.Transfer.Mode.Val == v2.TransferAuto && !sameBackend(&out.Milvus.Storage, &out.Backup.Storage) {
		r.warnf("minio.crossStorage=false mapped to transfer.mode=auto, but milvus and backup storage differ; auto streams between them where v1 could attempt direct copy — verify this is intended")
	}

	scanLegacyEnv(src, r)
	r.recordValidation(out.Validate())

	return out, r, nil
}

// Translate translates the v1 source and resolves it into the v2
// configuration the program runs on, so a deployment that has not migrated
// yet keeps working. Unlike Migrate it carries every value into the result,
// including a secret that reached v1 through an environment variable, which
// the process needs in hand to connect.
func Translate(src *param.Source) (*v2.Config, error) {
	out, err := v2.LoadFrom(newTranslator(src, nil).translate())
	if err != nil {
		return nil, fmt.Errorf("cfg: translate v1 config to v2: %w", err)
	}

	return out, nil
}

// scanLegacyEnv reports the v1 environment variables that are set in the
// source's environment snapshot and were renamed in v2, so a deployment
// carrying them over does not silently lose the value (a written v2 file does
// not read v1 variables; only the loader's translation does).
func scanLegacyEnv(src *param.Source, r *Report) {
	seen := make(map[string]bool)
	for _, f := range v1Fields {
		for _, env := range f.env {
			if seen[env] {
				continue
			}
			seen[env] = true
			if _, ok := src.EnvValue(env); !ok {
				continue
			}
			if to, legacy := v2.Migration(env); legacy {
				r.EnvRenames = append(r.EnvRenames, EnvRename{From: env, To: to})
			}
		}
	}
}

// sameBackend reports whether two storage configs name the same backend, i.e.
// the same provider reached at the same endpoint. Buckets and root paths may
// differ within one backend.
func sameBackend(a, b *v2.StorageConfig) bool {
	return a.Provider.Val == b.Provider.Val &&
		a.Address.Val == b.Address.Val &&
		a.Port.Val == b.Port.Val &&
		a.Region.Val == b.Region.Val &&
		a.UseSSL.Val == b.UseSSL.Val &&
		a.AccountName.Val == b.AccountName.Val
}
