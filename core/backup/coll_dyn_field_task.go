package backup

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
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

	etcd *etcdMeta

	etcdRootPath string

	metaBuilder *metaBuilder

	logger *zap.Logger
}

func newCollDynFieldTask(taskID string, etcd *etcdMeta, etcdRootPath string, metaBuilder *metaBuilder) *collDynFieldTask {
	return &collDynFieldTask{
		taskID:       taskID,
		etcd:         etcd,
		etcdRootPath: etcdRootPath,
		metaBuilder:  metaBuilder,
		logger:       log.With(zap.String("task_id", taskID)),
	}
}

func (cdft *collDynFieldTask) Execute(ctx context.Context) error {
	// Milvus stores collection field schemas under
	//   {MetaRootPath}/root-coord/fields/{collectionID}/{fieldID}
	// where MetaRootPath is "{etcdRootPath}/meta" in milvus-backup config terms.
	// The collection id is the first segment, so the scan is scoped to the
	// backed-up collections rather than read whole. A cluster-wide read grows
	// with the instance instead of with the backup, and nearly all of it is
	// waste: every field of every collection comes back so that at most one
	// dynamic field per backed-up collection can be picked out.
	collIDs := cdft.metaBuilder.backupCollectionIDs()

	dynFields := make(map[int64]*schemapb.FieldSchema)
	for _, collID := range collIDs {
		prefix := fmt.Sprintf("%s/meta/root-coord/fields/%d/", cdft.etcdRootPath, collID)
		cdft.logger.Info("start to get field schemas from etcd", zap.String("prefix", prefix))
		resp, err := cdft.etcd.getPrefix(ctx, prefix)
		if err != nil {
			return fmt.Errorf("backup: get field schemas: %w", err)
		}
		cdft.logger.Info("get field schemas from etcd done",
			zap.Int64("coll_id", collID), zap.Int("count", len(resp.Kvs)))

		for _, kv := range resp.Kvs {
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
	}

	if err := cdft.metaBuilder.addDynamicFields(dynFields); err != nil {
		return err
	}

	cdft.logger.Info("backup dynamic field info done", zap.Int("dynamic_field_count", len(dynFields)))
	return nil
}
