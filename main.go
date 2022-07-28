package main

import (
	"context"

	"github.com/zilliztech/milvus-backup/core"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/util/paramtable"

	"go.uber.org/zap"
)

func main() {
	var params paramtable.ComponentParam
	milvusYamlFile := "milvus.yaml"
	params.GlobalInitWithYaml(milvusYamlFile)
	params.InitOnce()
	log.Info("Done")

	context := context.Background()
	backupContext := core.CreateBackupContext(context, params)

	storageType := backupContext.GetMilvusSource().GetParams().CommonCfg.StorageType
	log.Info("storage type", zap.String("storage_type", storageType))

	log.Info("Done")
}
