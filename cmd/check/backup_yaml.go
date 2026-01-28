package check

import (
	"strings"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/zilliztech/milvus-backup/cmd/root"
)

func newBackupYamlCmd(opt *root.Options) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "backup_yaml",
		Short: "backup_yaml is a subcommand to check. It prints the current backup config file in yaml format to stdio.",

		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := opt.LoadConfig()
			if err != nil {
				return err
			}

			masked := cfg.MaskedConfig()
			bytes, err := yaml.Marshal(masked)
			if err != nil {
				return err
			}

			cmd.Println(strings.Repeat("-", 80))
			cmd.Print(string(bytes))
			return nil
		},
	}

	return cmd
}
