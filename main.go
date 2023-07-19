package main

import (
	"github.com/zilliztech/milvus-backup/cmd"
	_ "github.com/zilliztech/milvus-backup/docs"
)

var (
	version = "dev"
	commit  = "unknown"
	date    = "unknown"
)

// @title           Milvus Backup Service
// @version         1.0
// @description     A data backup & restore tool for Milvus
// @contact.name   wanganyang
// @contact.email  wayasxxx@gmail.com
// @license.name  Apache 2.0
// @license.url   http://www.apache.org/licenses/LICENSE-2.0.html
// @BasePath  /api/v1
func main() {
	cmd.SetVersionInfo(version, commit, date)
	cmd.Execute()
}
