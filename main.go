package main

import (
	"flag"
	"github.com/zilliztech/milvus-backup/cmd"
)

const COMMAND_DESC = `
NAME:
	milvus_backup - A milvus backup tool
   
	milvus_backup can use in two mode: command line or RESTAPI server.
   	1, You you start a REST API server by:
		milvus_backup --mode=server [SERVER_OPTIONS...]
   	2, Or you can use it as CLI

CLI USAGE:
	milvus_backup create_backup backup_name [collection_names]
	milvus_backup list_backups
    milvus_backup get_backup backup_name
    milvus_backup delete_backup backup_name
    milvus_backup load_backup backup_name

GLOBAL OPTIONS:
	--mode                           serve mode: cmd or server
   	--help                           show help
	--milvus_config                  milvus config file to connect to the milvus cluster, default: milvus.yaml

SERVER_OPTIONS:
	--port                           the port to listen if in SERVER mode
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
	cmd.Execute()
	//flag.Parse()
	//if help == true {
	//	fmt.Println(COMMAND_DESC)
	//	return
	//}
	//
	//if mode == "server" {
	//	var params paramtable.ComponentParam
	//	params.GlobalInitWithYaml(milvus_config)
	//	params.InitOnce()
	//
	//	context := context.Background()
	//	server, err := core.NewServer(context, params, core.Port(port))
	//	if err != nil {
	//		fmt.Errorf("Fail to create backup server, %s", err.Error())
	//	}
	//	server.Init()
	//	server.Start()
	//} else {
	//
	//}
	//log.Info("Done")
}
