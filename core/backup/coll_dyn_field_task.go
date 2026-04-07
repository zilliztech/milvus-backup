package backup

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/zilliztech/milvus-backup/internal/log"
)

// collDynFieldTask reads the dynamic field ($meta) schema for each collection
// directly from etcd. The Milvus DescribeCollection API filters out fields
// with IsDynamic == true (see milvus internal/proxy/task.go), so the backup
// metadata gathered via gRPC never contains the actual $meta field attributes
// (Nullable, DefaultValue, ...). This causes secondary restore to misalign
// the $meta field schema. See zilliztech/milvus-backup#1013.
type collDynFieldTask struct {
	taskID string

	kv clientv3.KV

	etcdRootPath string

	metaBuilder *metaBuilder

	logger *zap.Logger
}

func newCollDynFieldTask(taskID string, kv clientv3.KV, etcdRootPath string, metaBuilder *metaBuilder) *collDynFieldTask {
	return &collDynFieldTask{
		taskID:       taskID,
		kv:           kv,
		etcdRootPath: etcdRootPath,
		metaBuilder:  metaBuilder,
		logger:       log.With(zap.String("task_id", taskID)),
	}
}

func (cdft *collDynFieldTask) Execute(ctx context.Context) error {
	// Milvus stores collection field schemas under
	//   {MetaRootPath}/root-coord/fields/{collectionID}/{fieldID}
	// where MetaRootPath is "{etcdRootPath}/meta" in milvus-backup config terms.
	prefix := fmt.Sprintf("%s/meta/root-coord/fields/", cdft.etcdRootPath)
	cdft.logger.Info("start to get field schemas from etcd", zap.String("prefix", prefix))
	resp, err := cdft.kv.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("backup: get field schemas from etcd %w", err)
	}
	cdft.logger.Info("get field schemas from etcd done", zap.Int("count", len(resp.Kvs)))

	dynFields := make(map[int64]*schemapb.FieldSchema)
	for _, kv := range resp.Kvs {
		collID, ok := parseCollIDFromFieldKey(string(kv.Key), prefix)
		if !ok {
			cdft.logger.Warn("skip field with unparsable key", zap.String("key", string(kv.Key)))
			continue
		}
		field := &schemapb.FieldSchema{}
		if err := proto.Unmarshal(kv.Value, field); err != nil {
			// The key may be a tombstone or a non-FieldSchema payload, skip
			// rather than fail the whole backup.
			cdft.logger.Warn("skip field that cannot be unmarshalled",
				zap.String("key", string(kv.Key)), zap.Error(err))
			continue
		}
		if !field.GetIsDynamic() {
			continue
		}
		if existing, ok := dynFields[collID]; ok {
			cdft.logger.Warn("multiple dynamic fields found for one collection, keeping the first",
				zap.Int64("collection_id", collID),
				zap.Int64("existing_field_id", existing.GetFieldID()),
				zap.Int64("duplicate_field_id", field.GetFieldID()))
			continue
		}
		cdft.logger.Info("found dynamic field",
			zap.String("key", string(kv.Key)),
			zap.Int64("collection_id", collID),
			zap.Int64("field_id", field.GetFieldID()),
			zap.String("name", field.GetName()),
			zap.Bool("nullable", field.GetNullable()))
		dynFields[collID] = field
	}

	if err := cdft.metaBuilder.addDynamicFields(dynFields); err != nil {
		return err
	}

	cdft.logger.Info("backup dynamic field info done", zap.Int("dynamic_field_count", len(dynFields)))
	return nil
}

// parseCollIDFromFieldKey parses the collection id from an etcd field key like
// "{prefix}{collectionID}/{fieldID}".
func parseCollIDFromFieldKey(key, prefix string) (int64, bool) {
	if !strings.HasPrefix(key, prefix) {
		return 0, false
	}
	rest := strings.TrimPrefix(key, prefix)
	idx := strings.Index(rest, "/")
	if idx <= 0 {
		return 0, false
	}
	collID, err := strconv.ParseInt(rest[:idx], 10, 64)
	if err != nil {
		return 0, false
	}
	return collID, true
}
