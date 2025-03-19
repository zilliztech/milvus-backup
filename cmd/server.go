package cmd

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/zilliztech/milvus-backup/core"
	"github.com/zilliztech/milvus-backup/core/paramtable"
)

const (
	DefaultServerPort = "8080"
	ServerPortEnvKey  = "SERVER_PORT"
)

var (
	port string
)

var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "server subcommand start milvus-backup RESTAPI server.",

	Run: func(cmd *cobra.Command, args []string) {
		var params paramtable.BackupParams
		fmt.Println("config:" + config)
		params.GlobalInitWithYaml(config)
		params.Init()

		context := context.Background()
		server, err := core.NewServer(context, &params, core.Port(port))
		if err != nil {
			fmt.Printf("fail to create backup server: %s\n", err.Error())
			os.Exit(1)
		}
		server.Init()
		server.Start()
	},
}

func init() {
	serverPort := os.Getenv(ServerPortEnvKey)
	_, err := strconv.Atoi(serverPort)
	if err != nil {
		serverPort = DefaultServerPort
	}
	serverCmd.Flags().StringVarP(&port, "port", "p", serverPort, "Port to listen")

	rootCmd.AddCommand(serverCmd)
}
