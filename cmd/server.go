package cmd

import (
	"context"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/zilliztech/milvus-backup/core"
	"github.com/zilliztech/milvus-backup/internal/util/paramtable"
)

var (
	milvus_config string
	port          string
)

var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "server subcommand start milvus-backup RESTAPI server.",

	Run: func(cmd *cobra.Command, args []string) {
		var params paramtable.ComponentParam
		params.GlobalInitWithYaml(milvus_config)
		params.InitOnce()

		context := context.Background()
		server, err := core.NewServer(context, params, core.Port(port))
		if err != nil {
			fmt.Errorf("Fail to create backup server, %s", err.Error())
		}
		server.Init()
		server.Start()
	},
}

func init() {
	serverCmd.Flags().StringVarP(&milvus_config, "milvus_config", "c", "milvus.yaml", "config YAML file of milvus")
	serverCmd.Flags().StringVarP(&port, "port", "p", "8080", "Port to listen")

	rootCmd.AddCommand(serverCmd)
}
