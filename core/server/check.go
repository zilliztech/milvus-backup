package server

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/zilliztech/milvus-backup/core/check"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/storage"
)

func (s *Server) handleCheck(c *gin.Context) {
	ctx := c.Request.Context()

	// Lead the response with the effective configuration and the source of each
	// value, so a successful check reports what it actually ran against.
	var buff strings.Builder
	buff.WriteString("Configuration:\n")
	if err := s.params.WriteTable(&buff); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	grpc, err := milvus.NewGrpc(&s.params.Milvus)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	milvusStorage, err := storage.NewMilvusStorage(ctx, &s.params.Minio)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	backupStorage, err := storage.NewBackupStorage(ctx, &s.params.Minio)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	taskArgs := check.TaskArgs{
		Params:        s.params,
		Grpc:          grpc,
		MilvusStorage: milvusStorage,
		BackupStorage: backupStorage,
		Output:        &buff,
	}

	task := check.NewTask(taskArgs)
	err = task.Execute(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.String(http.StatusOK, buff.String())
}
