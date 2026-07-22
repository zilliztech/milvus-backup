package param

import (
	"fmt"
	"io"
	"reflect"
	"strings"
	"text/tabwriter"
)

// Resolver is implemented by every configuration value and by the structs that
// group them, so a whole schema resolves through one call.
type Resolver interface {
	Resolve(*Source) error
}

// Resolve resolves rs in order, stopping at the first failure.
func Resolve(s *Source, rs ...Resolver) error {
	for _, r := range rs {
		if err := r.Resolve(s); err != nil {
			return err
		}
	}

	return nil
}

// Entry is one resolved configuration parameter, ready to be displayed.
type Entry struct {
	Name      string
	Value     string
	Source    SourceKind
	SourceKey string
}

// Field is one leaf of a configuration schema: a value that knows the keys it
// is spelled with and how to display itself.
type Field interface {
	Display(name string) Entry
	ConfigKeys() []string
	EnvNames() []string
}

func maskSecret(s string) string {
	if s == "" {
		return ""
	}
	if len(s) <= 4 {
		return "****"
	}
	return s[:2] + "****" + s[len(s)-2:]
}

// Walk visits every field of the configuration struct pointed to by cfg,
// depth first, passing the dotted Go field path of each leaf.
func Walk(cfg any, fn func(name string, field Field)) {
	fieldType := reflect.TypeOf((*Field)(nil)).Elem()

	var collect func(v reflect.Value, prefix string)
	collect = func(v reflect.Value, prefix string) {
		t := v.Type()
		for i := range v.NumField() {
			field := v.Field(i)
			name := t.Field(i).Name
			if prefix != "" {
				name = prefix + "." + name
			}

			if field.Addr().Type().Implements(fieldType) {
				fn(name, field.Addr().Interface().(Field))
				continue
			}

			if field.Kind() == reflect.Struct {
				collect(field, name)
			}
		}
	}

	collect(reflect.ValueOf(cfg).Elem(), "")
}

// Entries returns one entry per configuration parameter of cfg.
func Entries(cfg any) []Entry {
	var entries []Entry
	Walk(cfg, func(name string, field Field) { entries = append(entries, field.Display(name)) })

	return entries
}

// DeclaredKeys returns every config file key and environment variable name the
// schema of cfg declares, lower-cased for comparison against a Source.
func DeclaredKeys(cfg any) (configKeys, envNames map[string]struct{}) {
	configKeys, envNames = map[string]struct{}{}, map[string]struct{}{}
	Walk(cfg, func(_ string, field Field) {
		for _, key := range field.ConfigKeys() {
			configKeys[strings.ToLower(key)] = struct{}{}
		}
		for _, key := range field.EnvNames() {
			envNames[strings.ToLower(key)] = struct{}{}
		}
	})

	return configKeys, envNames
}

// WriteTable prints entries in a table showing the parameter name, current
// value (secrets masked), the source the value came from (override, env,
// config, default), and the source key.
func WriteTable(w io.Writer, entries []Entry) error {
	// Render into a strings.Builder first, whose Write never fails, so the
	// only error-returning I/O is the single write and flush to the underlying
	// writer below.
	var sb strings.Builder
	fmt.Fprintln(&sb, "PARAMETER\tVALUE\tSOURCE\tSOURCE_KEY")
	fmt.Fprintln(&sb, "---------\t-----\t------\t----------")
	for _, e := range entries {
		fmt.Fprintf(&sb, "%s\t%s\t%s\t%s\n", e.Name, e.Value, e.Source, e.SourceKey)
	}

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := io.WriteString(tw, sb.String()); err != nil {
		return fmt.Errorf("cfg: write config table: %w", err)
	}
	if err := tw.Flush(); err != nil {
		return fmt.Errorf("cfg: flush config table: %w", err)
	}

	return nil
}
