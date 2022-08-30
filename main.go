package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/zilliztech/milvus-backup/core"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/util/paramtable"
)

const COMMAND_DESC = `
NAME:
   milvus_backup - A milvus backup tool

USAGE:
   milvus_backup [options...]

   milvus_backup can use in two mode: command line or RESTAPI server, you can choose mode like this:
   milvus_backup --mode=cmd
   milvus_backup --mode=server 

GLOBAL OPTIONS:
   --mode                           serve mode: cmd or server
   --help                           show help
   --port                           the port to listen if in SERVER mode
   --milvus_config                  milvus config file to connect to the milvus cluster, default: milvus.yaml
`

var (
	help          bool
	mode          string
	port          string
	milvus_config string
)

func init() {
	flag.BoolVar(&help, "help", false, "show help")
	flag.StringVar(&mode, "mode", "server", "service mode cli or server")
	flag.StringVar(&port, "port", "8080", "port to listen, default 8080")
	flag.StringVar(&milvus_config, "milvus_config", "milvus.yaml", "milvus config file to connect to the milvus cluster")
}

func main() {
	flag.Parse()
	if help == true {
		fmt.Println(COMMAND_DESC)
		return
	}

	if mode == "server" {
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
	} else if mode == "cmd" {

	}
	log.Info("Done")
}
