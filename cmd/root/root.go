package root

import (
	"errors"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/log"
)

type Options struct {
	Config        string
	YamlOverrides []string
}

func (o *Options) InitGlobalVars() *cfg.Config {
	overrides, err := parseOverrides(o.YamlOverrides)
	if err != nil {
		panic(err)
	}

	params, err := cfg.Load(o.Config, overrides)
	if err != nil {
		panic(err)
	}

	log.InitLogger(&params.Log)

	return params
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
	}

	cmd.PersistentFlags().StringVarP(&opt.Config, "config", "", "backup.yaml", "config YAML file of milvus")
	cmd.PersistentFlags().StringSliceVar(&opt.YamlOverrides, "set", []string{}, "Override yaml values using a capitalized snake case format (--set MILVUS_USER=Marco)")

	return cmd
}

func parseOverrides(envs []string) (map[string]string, error) {
	out := make(map[string]string, len(envs))
	for _, e := range envs {
		parts := strings.SplitN(e, "=", 2)
		if len(parts) != 2 {
			return nil, errors.New("invalid --set format, want KEY=VALUE")
		}
		out[parts[0]] = parts[1]
	}
	return out, nil
}
