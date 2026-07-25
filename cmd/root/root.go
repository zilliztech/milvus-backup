package root

import (
	"errors"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/zilliztech/milvus-backup/internal/cfg/loader"
	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
	"github.com/zilliztech/milvus-backup/internal/log"
)

type Options struct {
	Config        string
	YamlOverrides []string
}

// InitGlobalVars loads the configuration, whichever schema version the file is
// written in, and initializes logging from it. Everything downstream sees v2.
func (o *Options) InitGlobalVars() *v2.Config {
	overrides, err := parseOverrides(o.YamlOverrides)
	if err != nil {
		panic(err)
	}

	params, err := loader.Load(o.Config, overrides)
	if err != nil {
		panic(err)
	}

	log.InitLogger(logConfig(&params.Log))

	return params
}

// logConfig maps the log section onto the logger's own configuration, so the
// log package does not have to know about a configuration schema.
func logConfig(c *v2.LogConfig) *log.Config {
	return &log.Config{
		Level:   c.Level.Val,
		Console: c.Console.Val,
		File: log.FileLogConfig{
			Filename:   c.File.Path.Val,
			MaxSize:    c.File.MaxSizeMiB.Val,
			MaxDays:    c.File.MaxDays.Val,
			MaxBackups: c.File.MaxBackups.Val,
		},
	}
}

func NewCmd(opt *Options) *cobra.Command {
	cmd := &cobra.Command{
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd: true,
		},
		Use:   "milvus-backup",
		Short: "milvus-backup is a backup & restore tool for Milvus",
		Long:  `milvus-backup is a backup & restore tool for Milvus.`,
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
