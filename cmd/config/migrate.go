package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/zilliztech/milvus-backup/cmd/root"
	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/cfg/migrate"
	"github.com/zilliztech/milvus-backup/internal/cfg/param"
	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
)

type migrateOptions struct {
	output string
	force  bool
	strict bool
}

func (o *migrateOptions) run(cmd *cobra.Command, opt *root.Options) error {
	if err := ensureNotV2(opt.Config); err != nil {
		return err
	}

	// Load the v1 config directly rather than through root.InitGlobalVars,
	// which also initializes logging and panics on error. Overrides are not
	// applied: --set paths are v1-scoped runtime overrides, not part of the
	// file being migrated.
	src, err := cfg.Load(opt.Config, nil)
	if err != nil {
		return fmt.Errorf("load v1 config %s: %w", opt.Config, err)
	}

	out, report := migrate.Migrate(src)

	data, err := v2.Render(out, report.Comments)
	if err != nil {
		return err
	}

	// The report goes to stderr so stdout stays a clean YAML document that can
	// be redirected straight into a file.
	report.WriteTo(cmd.ErrOrStderr(), opt.Config)

	if o.strict {
		if err := report.Err(); err != nil {
			return fmt.Errorf("migrated config is not valid v2 (--strict): %w", err)
		}
	}

	return o.write(cmd, data)
}

func (o *migrateOptions) write(cmd *cobra.Command, data []byte) error {
	if o.output == "" || o.output == "-" {
		_, err := cmd.OutOrStdout().Write(data)
		return err
	}

	if !o.force {
		if _, err := os.Stat(o.output); err == nil {
			return fmt.Errorf("output file %s already exists, use --force to overwrite", o.output)
		}
	}
	if err := os.WriteFile(o.output, data, 0o600); err != nil {
		return fmt.Errorf("write %s: %w", o.output, err)
	}
	cmd.PrintErrf("wrote v2 config to %s\n", o.output)

	return nil
}

// ensureNotV2 rejects a config file that already declares configVersion: v2, so
// a mistaken re-run does not silently reload a v2 file with the v1 loader.
func ensureNotV2(configPath string) error {
	src, err := param.NewSource(configPath, nil)
	if err != nil {
		return err
	}
	if raw, ok := src.ConfigFileValue(v2.VersionKey); ok {
		if s, _ := raw.(string); strings.EqualFold(s, v2.Version) {
			return fmt.Errorf("%s is already a v2 config", configPath)
		}
	}

	return nil
}

func newMigrateCmd(opt *root.Options) *cobra.Command {
	var o migrateOptions

	cmd := &cobra.Command{
		Use:           "migrate",
		SilenceUsage:  true,
		SilenceErrors: true,
		Short:         "translate a v1 config file into a complete v2 config file",
		Long: "Translate the v1 configuration named by --config into a complete v2 " +
			"configuration file.\n\n" +
			"Keys and environment variables are renamed, not relocated: a secret kept " +
			"in a v1 environment variable is left out of the file and reported with the " +
			"v2 variable to set instead. The migration report is written to stderr; the " +
			"v2 file to stdout, or to --output.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return o.run(cmd, opt)
		},
	}

	cmd.Flags().StringVarP(&o.output, "output", "o", "", "write the v2 config here (default: stdout)")
	cmd.Flags().BoolVar(&o.force, "force", false, "overwrite the output file if it already exists")
	cmd.Flags().BoolVar(&o.strict, "strict", false, "fail if the migrated config is not valid v2")

	return cmd
}
