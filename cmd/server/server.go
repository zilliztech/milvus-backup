package server

import (
	"fmt"
	"os"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/zilliztech/milvus-backup/cmd/root"
	"github.com/zilliztech/milvus-backup/core/server"
	"github.com/zilliztech/milvus-backup/internal/cfg"
)

const (
	_defaultServerPort = "8080"
	_serverPortEnvKey  = "SERVER_PORT"
)

type options struct {
	port string
}

func (o *options) complete() error {
	// try to get port from args
	if o.port == "" {
		// try to get port from env
		port := os.Getenv(_serverPortEnvKey)
		if port == "" {
			// use default port
			port = _defaultServerPort
		}
		o.port = port
	}

	return nil
}

func (o *options) validate() error {
	// check if port is valid
	_, err := strconv.Atoi(o.port)
	if err != nil {
		return fmt.Errorf("invalid port: %s", o.port)
	}
	return nil
}

func (o *options) addFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&o.port, "port", "p", "", "Port to listen")
}

func (o *options) run(params *cfg.Config) error {
	srv, err := server.New(params, server.Port(o.port))
	if err != nil {
		return fmt.Errorf("fail to create server, %w", err)
	}

	if err := srv.Run(); err != nil {
		return fmt.Errorf("fail to run server, %w", err)
	}

	return nil
}

func NewCmd(opt *root.Options) *cobra.Command {
	var o options
	cmd := &cobra.Command{
		Use:   "server",
		Short: "server subcommand start milvus-backup RESTAPI server.",

		RunE: func(cmd *cobra.Command, args []string) error {
			params := opt.InitGlobalVars()

			if err := o.complete(); err != nil {
				return err
			}

			if err := o.validate(); err != nil {
				return err
			}

			return o.run(params)
		},
	}

	o.addFlags(cmd)

	return cmd
}
