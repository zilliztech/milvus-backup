package root

import (
	"errors"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/zilliztech/milvus-backup/core/paramtable"
	"github.com/zilliztech/milvus-backup/internal/log"
)

type Options struct {
	Config        string
	YamlOverrides []string
}

func (o *Options) InitGlobalVars() *paramtable.BackupParams {
	var params paramtable.BackupParams
	params.GlobalInitWithYaml(o.Config)
	params.Init()

	log.InitLogger(&params.LogCfg)

	return &params
}

func NewCmd(opt *Options) *cobra.Command {
	cmd := &cobra.Command{
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd: true,
		},
		Use:   "milvus-backup",
		Short: "milvus-backup is a backup&restore tool for milvus.",
		Long:  `milvus-backup is a backup&restore tool for milvus.`,
		Run: func(cmd *cobra.Command, args []string) {
			cmd.Printf("execute %s args:%v error:%v\n", cmd.Name(), args, errors.New("unrecognized command"))
			os.Exit(1)
		},
		// TODO: remove this, the Override should be done in the paramtable, not by set env
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			if err := setEnvs(opt.YamlOverrides); err != nil {
				cmd.Println(err)
				os.Exit(1)
			}
		},
	}

	cmd.PersistentFlags().StringVarP(&opt.Config, "config", "", "backup.yaml", "config YAML file of milvus")
	cmd.PersistentFlags().StringSliceVar(&opt.YamlOverrides, "set", []string{}, "Override yaml values using a capitalized snake case format (--set MILVUS_USER=Marco)")

	return cmd
}

// Set environment variables from yamlOverrides
func setEnvs(envs []string) error {
	for _, e := range envs {
		env := strings.Split(e, "=")
		if err := os.Setenv(env[0], env[1]); err != nil {
			return err
		}
	}

	return nil
}
