package migrate

import (
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/zilliztech/milvus-backup/internal/cfg/param"
)

// Report collects everything about a migration a human should see: warnings
// about values that changed meaning, the per-field comments embedded in the
// output file, the validation problems the migrated config still has, and the
// v1 environment variables that need renaming.
//
// A nil *Report is valid and discards everything written to it. It marks the
// runtime translate path, which maps a v1 config into v2 without producing
// migration advice for anyone to read, so every method here tolerates a nil
// receiver.
type Report struct {
	// Comments maps a lower-cased v2 config key to the head comment Render
	// writes above it.
	Comments map[string]string

	Warnings    []string
	EnvRenames  []EnvRename
	validations []error

	// deferred is the set of lower-cased config keys whose value was left in an
	// environment variable, so validation must not treat their emptiness in the
	// file as a mistake.
	deferred map[string]bool
}

// EnvRename names a v1 environment variable found set in the environment and
// the v2 variable that replaces it. To may name more than one option when the
// replacement depends on the storage provider.
type EnvRename struct {
	From string
	To   string
}

func newReport() *Report {
	return &Report{Comments: map[string]string{}, deferred: map[string]bool{}}
}

func (r *Report) warnf(format string, args ...any) {
	if r == nil {
		return
	}
	r.Warnings = append(r.Warnings, fmt.Sprintf(format, args...))
}

// comment records a head comment for a v2 field, keyed by its config key.
func (r *Report) comment(f param.Field, text string) {
	if r == nil {
		return
	}
	if keys := f.ConfigKeys(); len(keys) > 0 {
		r.Comments[strings.ToLower(keys[0])] = text
	}
}

// commentKey records a head comment for a v2 field named directly by its
// config key, for callers that do not hold the field.
func (r *Report) commentKey(key, text string) {
	if r == nil {
		return
	}
	r.Comments[strings.ToLower(key)] = text
}

// deferToEnv marks a field as supplied through an environment variable, so its
// empty value in the file is expected rather than a validation error.
func (r *Report) deferToEnv(f param.Field) {
	if r == nil {
		return
	}
	if keys := f.ConfigKeys(); len(keys) > 0 {
		r.deferred[strings.ToLower(keys[0])] = true
	}
}

// recordValidation runs the v2 validator over the migrated config and keeps
// every problem except those on fields whose value the operator deliberately
// left in an environment variable, which are empty in the file on purpose.
func (r *Report) recordValidation(err error) {
	if r == nil || err == nil {
		return
	}

	var joined interface{ Unwrap() []error }
	if !errors.As(err, &joined) {
		if !r.isEnvDeferred(err) {
			r.validations = append(r.validations, err)
		}
		return
	}
	for _, e := range joined.Unwrap() {
		r.recordValidation(e)
	}
}

// isEnvDeferred reports whether a validation error is about a field the
// migration moved to an environment variable, which is expected to be empty.
func (r *Report) isEnvDeferred(err error) bool {
	msg := strings.ToLower(err.Error())
	for key := range r.deferred {
		if strings.Contains(msg, key) {
			return true
		}
	}

	return false
}

// Err returns the migrated config's validation problems joined together, or nil
// when it is valid v2. --strict turns this into a hard failure.
func (r *Report) Err() error { return errors.Join(r.validations...) }

// WriteTo prints the report to w. srcPath names the file that was migrated. It
// writes nothing when there is nothing to say.
func (r *Report) WriteTo(w io.Writer, srcPath string) {
	if len(r.Warnings) == 0 && len(r.validations) == 0 && len(r.EnvRenames) == 0 {
		return
	}

	fmt.Fprintf(w, "# migration report for %s\n", srcPath)
	for _, warn := range r.Warnings {
		fmt.Fprintf(w, "warning: %s\n", warn)
	}
	for _, e := range r.validations {
		fmt.Fprintf(w, "warning: migrated config is not valid v2: %s\n", e)
	}

	if len(r.EnvRenames) > 0 {
		fmt.Fprintln(w, "v1 environment variables are set but inert under a v2 config, rename them:")
		for _, er := range r.EnvRenames {
			fmt.Fprintf(w, "  %s -> %s\n", er.From, er.To)
		}
	}
}
