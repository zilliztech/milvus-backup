package paramtable

import (
	"testing"
)

func TestRootPathParams(t *testing.T) {
	var params BackupParams
	params.GlobalInitWithYaml("backup.yaml")
	params.Init()

	//cfg := &MinioConfig{}
	//cfg.initRootPath()
	println(params.MinioCfg.RootPath)
}
