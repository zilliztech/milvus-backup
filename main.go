package main

import (
	"github.com/zilliztech/milvus-backup/cmd"
	"github.com/zilliztech/milvus-backup/version"
)

func main() {
	version.Print()

	cmd.Execute()
}
