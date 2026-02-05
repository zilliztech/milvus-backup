package backup

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/zilliztech/milvus-backup/internal/log"
)

type collIndexExtraTask struct {
	taskID string

	kv clientv3.KV

	etcdRootPath string

	metaBuilder *metaBuilder

	logger *zap.Logger
}

func newCollIndexExtraTask(taskID string, kv clientv3.KV, etcdRootPath string, metaBuilder *metaBuilder) *collIndexExtraTask {
	return &collIndexExtraTask{
		taskID:       taskID,
		kv:           kv,
		etcdRootPath: etcdRootPath,
		metaBuilder:  metaBuilder,
		logger:       log.With(zap.String("task_id", taskID)),
	}
}

func (ciet *collIndexExtraTask) Execute(ctx context.Context) error {
	prefix := fmt.Sprintf("%s/meta/field-index/", ciet.etcdRootPath)
	ciet.logger.Info("start to get index info from etcd", zap.String("prefix", prefix))
	resp, err := ciet.kv.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("backup: get indexes from etcd %w", err)
	}
	ciet.logger.Info("get indexes from etcd done", zap.Int("count", len(resp.Kvs)))

	indexes := make([]*indexpb.FieldIndex, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		index := &indexpb.FieldIndex{}
		if err := proto.Unmarshal(kv.Value, index); err != nil {
			return fmt.Errorf("backup: unmarshal index %w", err)
		}
		if index.GetDeleted() {
			ciet.logger.Info("skip deleted index", zap.Int64("index_id", index.GetIndexInfo().GetIndexID()))
			continue
		}
		indexes = append(indexes, index)
	}

	ciet.metaBuilder.addIndexExtraInfo(indexes)

	ciet.logger.Info("backup index extra info done")
	return nil
}
