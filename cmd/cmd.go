package cmd

import (
	"fmt"
	"os"

	"github.com/zilliztech/milvus-backup/cmd/check"
	"github.com/zilliztech/milvus-backup/cmd/create"
	"github.com/zilliztech/milvus-backup/cmd/del"
	"github.com/zilliztech/milvus-backup/cmd/get"
	"github.com/zilliztech/milvus-backup/cmd/list"
	"github.com/zilliztech/milvus-backup/cmd/migrate"
	"github.com/zilliztech/milvus-backup/cmd/restore"
	"github.com/zilliztech/milvus-backup/cmd/root"
	"github.com/zilliztech/milvus-backup/cmd/server"
)

func Execute() {
	var opt root.Options

	cmd := root.NewCmd(&opt)

	cmd.AddCommand(check.NewCmd(&opt))
	cmd.AddCommand(create.NewCmd(&opt))
	cmd.AddCommand(del.NewCmd(&opt))
	cmd.AddCommand(get.NewCmd(&opt))
	cmd.AddCommand(list.NewCmd(&opt))
	cmd.AddCommand(restore.NewCmd(&opt))
	cmd.AddCommand(server.NewCmd(&opt))
	cmd.AddCommand(migrate.NewCmd(&opt))

	cmd.SetOut(os.Stdout)
	cmd.SetErr(os.Stderr)

	cmd.Println(fmt.Sprintf("config: %s", opt.Config))

	if err := cmd.Execute(); err != nil {
		cmd.PrintErrln(err)
		os.Exit(1)
	}
}
