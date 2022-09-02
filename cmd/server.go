package cmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/zilliztech/milvus-backup/core"
	"github.com/zilliztech/milvus-backup/internal/util/paramtable"
)

var (
	milvusConfig string
	port         string
)

var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "server subcommand start milvus-backup RESTAPI server.",

	Run: func(cmd *cobra.Command, args []string) {
		var params paramtable.ComponentParam
		params.GlobalInitWithYaml(milvusConfig)
		params.InitOnce()

		context := context.Background()
		server, err := core.NewServer(context, params, core.Port(port))
		if err != nil {
			fmt.Errorf("fail to create backup server, %s", err.Error())
		}
		server.Init()
		server.Start()
	},
}

func init() {
	serverCmd.Flags().StringVarP(&milvusConfig, "milvusConfig", "c", "milvus.yaml", "config YAML file of milvus")
	serverCmd.Flags().StringVarP(&port, "port", "p", "8080", "Port to listen")

	rootCmd.AddCommand(serverCmd)
}
