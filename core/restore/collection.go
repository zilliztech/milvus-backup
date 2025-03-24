package restore

import (
	"context"
	"errors"
	"fmt"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/samber/lo"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"
	"google.golang.org/protobuf/proto"

	"github.com/zilliztech/milvus-backup/core/client"
	"github.com/zilliztech/milvus-backup/core/meta"
	"github.com/zilliztech/milvus-backup/core/paramtable"
	"github.com/zilliztech/milvus-backup/core/pbconv"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/storage"
	"github.com/zilliztech/milvus-backup/core/storage/mpath"
	"github.com/zilliztech/milvus-backup/internal/common"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/util/retry"
)

const (
	_bulkInsertTimeout             = 60 * time.Minute
	_bulkInsertCheckInterval       = 3 * time.Second
	_bulkInsertRestfulAPIChunkSize = 256
)

type tearDownFn func(ctx context.Context) error

type CollectionTask struct {
	task *backuppb.RestoreCollectionTask

	parentTaskID string

	meta *meta.MetaManager

	copyParallelism    int
	restoreParallelism int

	backupBucketName string
	backupPath       string
	backupRootPath   string
	backupStorage    storage.ChunkManager

	milvusBucketName string
	milvusStorage    storage.ChunkManager

	restoredSize atomic.Int64

	grpcCli    client.Grpc
	restfulCli client.RestfulBulkInsert

	tearDownFns []tearDownFn

	useV2Restore bool

	logger *zap.Logger
}

func newCollectionTask(task *backuppb.RestoreCollectionTask,
	meta *meta.MetaManager,
	params *paramtable.BackupParams,
	parentTaskID,
	backupBucketName,
	backupPath string,
	backupStorage storage.ChunkManager,
	milvusStorage storage.ChunkManager,
	grpcCli client.Grpc,
	restfulCli client.RestfulBulkInsert,
) *CollectionTask {
	logger := log.With(
		zap.String("backup_db_name", task.GetCollBackup().GetDbName()),
		zap.String("backup_collection_name", task.GetCollBackup().GetCollectionName()),
		zap.String("target_db_name", task.GetTargetDbName()),
		zap.String("target_collection_name", task.GetTargetCollectionName()))

	return &CollectionTask{
		task: task,

		parentTaskID: parentTaskID,

		meta: meta,

		copyParallelism:    params.BackupCfg.BackupCopyDataParallelism,
		restoreParallelism: params.BackupCfg.RestoreParallelism,

		backupBucketName: backupBucketName,
		backupPath:       backupPath,
		backupRootPath:   params.MinioCfg.BackupRootPath,
		backupStorage:    backupStorage,

		milvusBucketName: params.MinioCfg.BucketName,
		milvusStorage:    milvusStorage,

		grpcCli:    grpcCli,
		restfulCli: restfulCli,

		logger: logger,
	}
}

func (ct *CollectionTask) Execute(ctx context.Context) error {
	ct.task.StateCode = backuppb.RestoreTaskStateCode_EXECUTING

	// tear down restore task
	defer func() {
		if err := ct.tearDown(ctx); err != nil {
			ct.task.StateCode = backuppb.RestoreTaskStateCode_FAIL
			ct.task.ErrorMessage = err.Error()
			ct.logger.Error("restore collection tear down failed", zap.Error(err))
		}
	}()

	err := ct.privateExecute(ctx)
	if err != nil {
		ct.task.StateCode = backuppb.RestoreTaskStateCode_FAIL
		ct.task.ErrorMessage = err.Error()
		ct.logger.Error("restore collection failed", zap.Error(err))
		return err
	}

	ct.task.StateCode = backuppb.RestoreTaskStateCode_SUCCESS
	ct.logger.Info("restore collection success")
	return nil
}

func (ct *CollectionTask) privateExecute(ctx context.Context) error {
	ct.logger.Info("start restore collection", zap.String("backup_bucket_name", ct.backupBucketName),
		zap.String("backup_path", ct.backupPath))

	// restore collection schema
	if err := ct.dropExistedColl(ctx); err != nil {
		return fmt.Errorf("restore_collection: drop exist collection: %w", err)
	}
	if err := ct.createColl(ctx); err != nil {
		return fmt.Errorf("restore_collection: create collection: %w", err)
	}

	// restore collection index
	if err := ct.dropExistedIndex(ctx); err != nil {
		return fmt.Errorf("restore_collection: drop exist index: %w", err)
	}
	if err := ct.createIndex(ctx); err != nil {
		return fmt.Errorf("restore_collection: create index: %w", err)
	}

	// restore collection data
	if err := ct.restoreData(ctx); err != nil {
		return fmt.Errorf("restore_collection: restore data: %w", err)
	}

	return nil
}

func (ct *CollectionTask) restoreData(ctx context.Context) error {
	if ct.task.GetMetaOnly() {
		ct.logger.Info("skip restore data")
		return nil
	}

	// restore collection data
	if ct.task.UseV2Restore {
		if err := ct.restoreDataV2(ctx); err != nil {
			return fmt.Errorf("restore_collection: restore data v2: %w", err)
		}
	} else {
		if err := ct.restoreDataV1(ctx); err != nil {
			return fmt.Errorf("restore_collection: restore data v1: %w", err)
		}
	}

	return nil
}

func (ct *CollectionTask) restoreDataV2(ctx context.Context) error {
	// restore partition segment
	ct.logger.Info("start restore partition segment", zap.Int("partition_num", len(ct.task.GetCollBackup().GetPartitionBackups())))
	for _, part := range ct.task.GetCollBackup().GetPartitionBackups() {
		if err := ct.restorePartitionV2(ctx, part); err != nil {
			return fmt.Errorf("restore_collection: restore partition v2: %w", err)
		}
	}

	// restore global l0 segment
	ct.logger.Info("start restore global L0 segment", zap.Int("l0_segments", len(ct.task.GetCollBackup().GetL0Segments())))
	if err := ct.restoreL0SegV2(ctx, -1, "", ct.task.GetCollBackup().GetL0Segments()); err != nil {
		return fmt.Errorf("restore_collection: restore global L0 segment: %w", err)
	}

	return nil
}

func (ct *CollectionTask) restoreDataV1(ctx context.Context) error {
	// restore partition segment
	ct.logger.Info("start restore partition segment", zap.Int("partition_num", len(ct.task.GetCollBackup().GetPartitionBackups())))
	for _, part := range ct.task.GetCollBackup().GetPartitionBackups() {
		if err := ct.restorePartitionV1(ctx, part); err != nil {
			return fmt.Errorf("restore_collection: restore partition data v1: %w", err)
		}
	}

	// restore global l0 segment
	ct.logger.Info("start restore global L0 segment", zap.Int("l0_segments", len(ct.task.GetCollBackup().GetL0Segments())))
	if err := ct.restoreL0SegV1(ctx, -1, "", ct.task.GetCollBackup().GetL0Segments()); err != nil {
		return fmt.Errorf("restore_collection: restore global L0 segment: %w", err)
	}

	return nil
}

func (ct *CollectionTask) tearDown(ctx context.Context) error {
	ct.logger.Info("restore task tear down")

	slices.Reverse(ct.tearDownFns)

	if ct.tearDownFns != nil {
		for _, fn := range ct.tearDownFns {
			if err := fn(ctx); err != nil {
				ct.logger.Error("tear down restore task failed", zap.Error(err))
				return err
			}
		}
	}

	return nil
}

func (ct *CollectionTask) dropExistedColl(ctx context.Context) error {
	if !ct.task.GetDropExistCollection() {
		ct.logger.Info("skip drop existed collection")
		return nil
	}

	ct.logger.Info("start drop existed collection")
	exist, err := ct.grpcCli.HasCollection(ctx, ct.task.GetTargetDbName(), ct.task.GetTargetCollectionName())
	if err != nil {
		return fmt.Errorf("restore_collection: failed to check collection exist: %w", err)
	}
	if !exist {
		ct.logger.Info("collection not exist, skip drop collection")
		return nil
	}

	if err := ct.grpcCli.DropCollection(ctx, ct.task.GetTargetDbName(), ct.task.TargetCollectionName); err != nil {
		return fmt.Errorf("restore_collection: failed to drop collection: %w", err)
	}

	return nil
}

func (ct *CollectionTask) createColl(ctx context.Context) error {
	if ct.task.GetSkipCreateCollection() {
		ct.logger.Info("skip create collection")
		return nil
	}

	fields, err := ct.fields()
	if err != nil {
		return fmt.Errorf("restore_collection: failed to get fields: %w", err)
	}
	ct.logger.Info("restore collection fields", zap.Any("fields", fields))

	functions := ct.functions()
	ct.logger.Info("restore collection functions", zap.Any("functions", functions))
	schema := &schemapb.CollectionSchema{
		Name:               ct.task.GetTargetCollectionName(),
		Description:        ct.task.GetCollBackup().GetSchema().GetDescription(),
		AutoID:             ct.task.GetCollBackup().GetSchema().GetAutoID(),
		Functions:          functions,
		Fields:             fields,
		EnableDynamicField: ct.task.GetCollBackup().GetSchema().GetEnableDynamicField(),
	}

	// overwrite shardNum by request parameter
	shardNum := ct.task.GetCollBackup().GetShardsNum()
	if shardNum > ct.task.GetMaxShardNum() && ct.task.GetMaxShardNum() != 0 {
		shardNum = ct.task.GetMaxShardNum()
		ct.logger.Info("overwrite shardNum by request parameter",
			zap.Int32("oldShardNum", ct.task.GetCollBackup().GetShardsNum()),
			zap.Int32("newShardNum", shardNum))
	}

	opt := client.CreateCollectionInput{
		DB:           ct.task.GetTargetDbName(),
		Schema:       schema,
		ConsLevel:    commonpb.ConsistencyLevel(ct.task.GetCollBackup().GetConsistencyLevel()),
		ShardNum:     shardNum,
		PartitionNum: ct.partitionNum(),
	}
	err = retry.Do(ctx, func() error {
		return ct.grpcCli.CreateCollection(ctx, opt)
	}, retry.Attempts(10), retry.Sleep(1*time.Second))
	if err != nil {
		return fmt.Errorf("restore_collection: call create collection api after retry: %w", err)
	}

	return nil
}

func (ct *CollectionTask) fields() ([]*schemapb.FieldSchema, error) {
	bakFields := ct.task.GetCollBackup().GetSchema().GetFields()
	fields := make([]*schemapb.FieldSchema, 0, len(bakFields))

	for _, bakField := range bakFields {
		fieldRestore := &schemapb.FieldSchema{
			FieldID:          bakField.GetFieldID(),
			Name:             bakField.GetName(),
			IsPrimaryKey:     bakField.GetIsPrimaryKey(),
			AutoID:           bakField.GetAutoID(),
			Description:      bakField.GetDescription(),
			DataType:         schemapb.DataType(bakField.GetDataType()),
			TypeParams:       pbconv.BakKVToMilvusKV(bakField.GetTypeParams(), ct.task.SkipParams.GetFieldTypeParams()...),
			IndexParams:      pbconv.BakKVToMilvusKV(bakField.GetIndexParams(), ct.task.SkipParams.GetFieldIndexParams()...),
			IsDynamic:        bakField.GetIsDynamic(),
			IsPartitionKey:   bakField.GetIsPartitionKey(),
			Nullable:         bakField.GetNullable(),
			ElementType:      schemapb.DataType(bakField.GetElementType()),
			IsFunctionOutput: bakField.IsFunctionOutput,
		}
		if bakField.DefaultValueProto != "" {
			var defaultValue schemapb.ValueField
			err := proto.Unmarshal([]byte(bakField.DefaultValueProto), &defaultValue)
			if err != nil {
				return nil, fmt.Errorf("restore_collection: failed to unmarshal default value: %w", err)
			}
			fieldRestore.DefaultValue = &defaultValue
		}

		fields = append(fields, fieldRestore)
	}

	return fields, nil
}

// partitionNum returns the partition number of the collection
// if partition key was set, return the length of partition in backup
// else return 0
func (ct *CollectionTask) partitionNum() int {
	var hasPartitionKey bool
	for _, field := range ct.task.GetCollBackup().GetSchema().GetFields() {
		if field.GetIsPartitionKey() {
			hasPartitionKey = true
			break
		}
	}

	if hasPartitionKey {
		return len(ct.task.GetCollBackup().GetPartitionBackups())
	}

	return 0
}

func (ct *CollectionTask) functions() []*schemapb.FunctionSchema {
	bakFuncs := ct.task.GetCollBackup().GetSchema().GetFunctions()
	functions := make([]*schemapb.FunctionSchema, 0, len(bakFuncs))
	for _, bakFunc := range bakFuncs {
		fun := &schemapb.FunctionSchema{
			Name:             bakFunc.Name,
			Id:               bakFunc.Id,
			Description:      bakFunc.Description,
			Type:             schemapb.FunctionType(bakFunc.Type),
			InputFieldNames:  bakFunc.InputFieldNames,
			InputFieldIds:    bakFunc.InputFieldIds,
			OutputFieldNames: bakFunc.OutputFieldNames,
			OutputFieldIds:   bakFunc.OutputFieldIds,
			Params:           pbconv.BakKVToMilvusKV(bakFunc.Params),
		}
		functions = append(functions, fun)
	}

	return functions
}

func (ct *CollectionTask) dropExistedIndex(ctx context.Context) error {
	if !ct.task.GetDropExistIndex() {
		ct.logger.Info("skip drop existed index")
		return nil
	}

	ct.logger.Info("start drop existed index")
	indexes, err := ct.grpcCli.ListIndex(ctx, ct.task.GetTargetDbName(), ct.task.GetTargetCollectionName())
	if err != nil {
		log.Error("fail in DescribeIndex", zap.Error(err))
		return nil
	}

	for _, index := range indexes {
		err = ct.grpcCli.DropIndex(ctx, ct.task.GetTargetDbName(), ct.task.TargetCollectionName, index.IndexName)
		if err != nil {
			return fmt.Errorf("restore_collection: failed to drop index %s: %w", index.IndexName, err)
		}
		ct.logger.Info("drop index", zap.String("field_name", index.FieldName),
			zap.String("index_name", index.IndexName))
	}

	return nil
}

func (ct *CollectionTask) createIndex(ctx context.Context) error {
	if !ct.task.GetRestoreIndex() {
		ct.logger.Info("skip restore index")
		return nil
	}
	ct.logger.Info("start restore index")

	vectorFields := make(map[string]struct{})
	for _, field := range ct.task.GetCollBackup().GetSchema().GetFields() {
		typStr, ok := schemapb.DataType_name[int32(field.DataType)]
		if !ok {
			return fmt.Errorf("restore_collection: invalid field data type %d", field.DataType)
		}

		if strings.HasSuffix(strings.ToLower(typStr), "vector") {
			vectorFields[field.Name] = struct{}{}
		}
	}

	indexes := ct.task.GetCollBackup().GetIndexInfos()
	var vectorIndexes, scalarIndexes []*backuppb.IndexInfo
	for _, index := range indexes {
		if _, ok := vectorFields[index.GetFieldName()]; ok {
			vectorIndexes = append(vectorIndexes, index)
		} else {
			scalarIndexes = append(scalarIndexes, index)
		}
	}

	if err := ct.restoreVectorFieldIdx(ctx, vectorIndexes); err != nil {
		return fmt.Errorf("restore_collection: restore vector field index: %w", err)
	}
	if err := ct.restoreScalarFieldIdx(ctx, scalarIndexes); err != nil {
		return fmt.Errorf("restore_collection: restore scalar field index: %w", err)
	}

	return nil
}

func (ct *CollectionTask) restoreScalarFieldIdx(ctx context.Context, indexes []*backuppb.IndexInfo) error {
	for _, index := range indexes {
		ct.logger.Info("source index",
			zap.String("indexName", index.GetIndexName()),
			zap.Any("params", index.GetParams()))

		opt := client.CreateIndexInput{
			DB:             ct.task.GetTargetDbName(),
			CollectionName: ct.task.GetTargetCollectionName(),
			FieldName:      index.GetFieldName(),
			IndexName:      index.GetIndexName(),
			Params:         index.GetParams(),
		}
		if err := ct.grpcCli.CreateIndex(ctx, opt); err != nil {
			return fmt.Errorf("restore_collection: restore scalar idx %s: %w", index.GetIndexName(), err)
		}
	}

	return nil
}

func (ct *CollectionTask) restoreVectorFieldIdx(ctx context.Context, indexes []*backuppb.IndexInfo) error {
	for _, index := range indexes {
		ct.logger.Info("source  index",
			zap.String("indexName", index.GetIndexName()),
			zap.Any("params", index.GetParams()))

		params := make(map[string]string)
		if ct.task.GetUseAutoIndex() {
			ct.logger.Info("use auto index", zap.String("fieldName", index.GetFieldName()))
			params = map[string]string{"index_type": "AUTOINDEX", "metric_type": index.GetParams()["metric_type"]}
		} else {
			ct.logger.Info("use source index", zap.String("fieldName", index.GetFieldName()))
			params = index.GetParams()
			if params["index_type"] == "marisa-trie" {
				params["index_type"] = "Trie"
			}
		}

		opt := client.CreateIndexInput{
			DB:             ct.task.GetTargetDbName(),
			CollectionName: ct.task.GetTargetCollectionName(),
			FieldName:      index.GetFieldName(),
			IndexName:      index.GetIndexName(),
			Params:         params,
		}
		if err := ct.grpcCli.CreateIndex(ctx, opt); err != nil {
			return fmt.Errorf("restore_collection: restore vec idx %s: %w", index.GetIndexName(), err)
		}
	}

	return nil
}

func (ct *CollectionTask) cleanTempFiles(dir string) tearDownFn {
	return func(ctx context.Context) error {
		if len(dir) == 0 {
			return errors.New("restore_collection: empty temporary file dir")
		}

		ct.logger.Info("delete temporary file", zap.String("dir", dir))
		err := ct.milvusStorage.RemoveWithPrefix(ctx, ct.milvusBucketName, dir)
		if err != nil {
			return fmt.Errorf("restore_collection: failed to delete temporary file: %w", err)
		}

		return nil
	}
}

func (ct *CollectionTask) createPartition(ctx context.Context, partitionName string) error {
	// pre-check whether partition exist, if not create it
	ct.logger.Debug("check partition exist", zap.String("partition_name", partitionName))
	exist, err := ct.grpcCli.HasPartition(ctx, ct.task.GetTargetDbName(), ct.task.GetTargetCollectionName(), partitionName)
	if err != nil {
		return fmt.Errorf("restore_collection: failed to check partition exist: %w", err)
	}
	if exist {
		ct.logger.Info("partition exist, skip create partition")
		return nil
	}

	err = ct.grpcCli.CreatePartition(ctx, ct.task.GetTargetDbName(), ct.task.GetTargetCollectionName(), partitionName)
	if err != nil {
		ct.logger.Debug("create partition failed", zap.String("partition_name", partitionName), zap.Error(err))
		return fmt.Errorf("restore_collection: failed to create partition: %w", err)
	}
	ct.logger.Debug("create partition success", zap.String("partition_name", partitionName))
	return nil
}

func (ct *CollectionTask) copyFiles(ctx context.Context, paths []string) ([]string, error) {
	isSameBucket := ct.milvusBucketName == ct.backupBucketName
	isSameStorage := ct.backupStorage.Config().StorageType == ct.milvusStorage.Config().StorageType
	// if milvus bucket and backup bucket are not the same, should copy the data first
	if isSameBucket && isSameStorage {
		ct.logger.Info("milvus and backup store in the same bucket, no need to copy the data")
		return paths, nil
	}

	tempPaths := make([]string, 0, len(paths))
	tempDir := fmt.Sprintf("restore-temp-%s-%s-%s/", ct.parentTaskID, ct.task.GetTargetDbName(), ct.task.GetTargetCollectionName())
	ct.logger.Info("milvus and backup store in different bucket, copy the data first",
		zap.Strings("paths", paths),
		zap.String("copy_data_path", tempDir))
	for _, p := range paths {
		// empty delta path, no need to copy
		if len(p) == 0 {
			tempPaths = append(tempPaths, p)
			continue
		}

		tempPath := path.Join(tempDir, strings.Replace(p, ct.backupRootPath, "", 1)) + "/"
		ct.logger.Info("copy temporary restore file", zap.String("from", p), zap.String("to", tempPath))

		opt := storage.CopyOption{
			WorkerNum:    ct.copyParallelism,
			RPS:          1000,
			CopyByServer: true,
		}
		copier := storage.NewCopier(ct.backupStorage, ct.milvusStorage, opt)
		if err := copier.Copy(ctx, p, tempPath, ct.backupBucketName, ct.milvusBucketName); err != nil {
			ct.logger.Error("fail to copy backup date from backup bucket to restore target milvus bucket after retry", zap.Error(err))
			return nil, fmt.Errorf("restore_collection: failed to copy backup data: %w", err)
		}
		tempPaths = append(tempPaths, tempPath)
		ct.tearDownFns = append(ct.tearDownFns, ct.cleanTempFiles(tempPath))
	}

	return tempPaths, nil
}

type restoreGroup struct {
	insertLogDir string
	deltaLogDir  string

	size int64
}

func (ct *CollectionTask) restoreNotL0SegV1(ctx context.Context, part *backuppb.PartitionBackupInfo) error {
	notL0Groups, err := ct.notL0Groups(ctx, part)
	if err != nil {
		return fmt.Errorf("restore_collection: get not L0 groups: %w", err)
	}

	wp, err := common.NewWorkerPool(ctx, ct.restoreParallelism, 0)
	if err != nil {
		return fmt.Errorf("restore_collection: restore data v1 create worker pool: %w", err)
	}
	wp.Start()

	for _, g := range notL0Groups {
		job := func(ctx context.Context) error {
			paths, err := ct.copyFiles(ctx, []string{g.insertLogDir, g.deltaLogDir})
			if err != nil {
				return fmt.Errorf("restore_collection: restore data v1 copy files: %w", err)
			}

			if err := ct.bulkInsertViaGrpc(ctx, part.GetPartitionName(), paths, false); err != nil {
				return fmt.Errorf("restore_collection: restore data v1 bulk insert via grpc: %w", err)
			}

			opt := meta.AddCollectionRestoredSize(ct.task.GetCollBackup().GetCollectionId(), g.size)
			ct.meta.UpdateRestoreTask(ct.parentTaskID, opt)
			ct.restoredSize.Add(g.size)
			ct.task.RestoredSize = ct.restoredSize.Load()
			return nil
		}
		wp.Submit(job)
	}
	wp.Done()
	if err := wp.Wait(); err != nil {
		return fmt.Errorf("restore_collection: wait worker pool: %w", err)
	}

	return nil
}

func (ct *CollectionTask) restoreNotL0SegV2(ctx context.Context, part *backuppb.PartitionBackupInfo) error {
	notL0Groups, err := ct.notL0Groups(ctx, part)
	if err != nil {
		return fmt.Errorf("restore_collection: get not L0 groups: %w", err)
	}

	chunkedGroups := lo.Chunk(notL0Groups, _bulkInsertRestfulAPIChunkSize)
	for _, groups := range chunkedGroups {
		paths := make([][]string, 0, len(groups))
		for _, g := range groups {
			paths = append(paths, []string{g.insertLogDir, g.deltaLogDir})
		}

		if err := ct.bulkInsertViaRestful(ctx, part.GetPartitionName(), paths, false); err != nil {
			return fmt.Errorf("restore_collection: bulk insert via restful: %w", err)
		}
	}

	return nil
}

func (ct *CollectionTask) restoreL0SegV1(ctx context.Context, partitionID int64, partitionName string, l0Segs []*backuppb.SegmentBackupInfo) error {
	for _, seg := range l0Segs {
		opts := []mpath.PathOption{
			mpath.CollectionID(ct.task.GetCollBackup().CollectionId),
			mpath.PartitionID(partitionID),
			mpath.SegmentID(seg.SegmentId)}

		deltaLogDir := mpath.DeltaLogDir(ct.backupPath, opts...)
		paths, err := ct.copyFiles(ctx, []string{deltaLogDir})
		if err != nil {
			return fmt.Errorf("restore_collection: restore L0 segment copy files: %w", err)
		}

		if err := ct.bulkInsertViaGrpc(ctx, partitionName, paths, true); err != nil {
			return fmt.Errorf("restore_collection: restore L0 segment bulk insert via grpc: %w", err)
		}
	}

	return nil
}

func (ct *CollectionTask) restoreL0SegV2(ctx context.Context, partitionID int64, partitionName string, l0Segs []*backuppb.SegmentBackupInfo) error {
	for _, seg := range l0Segs {
		opts := []mpath.PathOption{
			mpath.CollectionID(ct.task.GetCollBackup().CollectionId),
			mpath.PartitionID(partitionID),
			mpath.SegmentID(seg.SegmentId)}

		deltaLogDir := mpath.DeltaLogDir(ct.backupPath, opts...)
		if err := ct.bulkInsertViaRestful(ctx, partitionName, [][]string{{deltaLogDir}}, true); err != nil {
			return fmt.Errorf("restore_collection: restore L0 segment bulk insert via restful: %w", err)
		}
	}

	return nil
}

func (ct *CollectionTask) restorePartitionV1(ctx context.Context, part *backuppb.PartitionBackupInfo) error {
	ct.logger.Info("start restore partition", zap.String("partition", part.GetPartitionName()))

	if err := ct.createPartition(ctx, part.GetPartitionName()); err != nil {
		return fmt.Errorf("restore_collection: restore partition: %w", err)
	}

	ct.logger.Info("start restore not L0 segment", zap.String("partition_name", part.GetPartitionName()))
	// restore not L0 data groups
	if err := ct.restoreNotL0SegV1(ctx, part); err != nil {
		return fmt.Errorf("restore_collection: restore not L0 groups: %w", err)
	}

	ct.logger.Info("start restore L0 segment", zap.String("partition_name", part.GetPartitionName()))
	// restore partition L0 segment
	l0Segs := lo.Filter(part.GetSegmentBackups(), func(seg *backuppb.SegmentBackupInfo, _ int) bool {
		return seg.IsL0
	})
	if err := ct.restoreL0SegV1(ctx, part.GetPartitionId(), part.GetPartitionName(), l0Segs); err != nil {
		return fmt.Errorf("restore_collection: restore L0 segment: %w", err)
	}

	return nil
}

func (ct *CollectionTask) restorePartitionV2(ctx context.Context, part *backuppb.PartitionBackupInfo) error {
	ct.logger.Info("start restore partition v2", zap.String("partition", part.GetPartitionName()))

	if err := ct.createPartition(ctx, part.GetPartitionName()); err != nil {
		return fmt.Errorf("restore_collection: restore partition: %w", err)
	}

	ct.logger.Info("start restore partition not L0 segment v2", zap.String("partition_name", part.GetPartitionName()))
	if err := ct.restoreNotL0SegV2(ctx, part); err != nil {
		return fmt.Errorf("restore_collection: restore not L0 groups: %w", err)
	}

	l0Seg := lo.Filter(part.GetSegmentBackups(), func(seg *backuppb.SegmentBackupInfo, _ int) bool {
		return seg.IsL0
	})
	if err := ct.restoreL0SegV2(ctx, part.GetPartitionId(), part.GetPartitionName(), l0Seg); err != nil {
		return fmt.Errorf("restore_collection: restore L0 segment: %w", err)
	}

	return nil
}

func (ct *CollectionTask) notL0Groups(ctx context.Context, part *backuppb.PartitionBackupInfo) ([]restoreGroup, error) {
	gm := make(map[int64]struct{})
	for _, seg := range part.GetSegmentBackups() {
		if seg.IsL0 {
			continue
		}

		gm[seg.GetGroupId()] = struct{}{}
	}
	groupIDs := maps.Keys(gm)

	groups := make([]restoreGroup, 0, len(groupIDs))
	if len(groupIDs) == 1 && groupIDs[0] == 0 {
		// backward compatible old backup without group id
		opts := []mpath.PathOption{
			mpath.CollectionID(ct.task.GetCollBackup().CollectionId),
			mpath.PartitionID(part.GetPartitionId()),
		}
		insertLogDir, deltaLogDir, err := ct.verifyBackupPartitionPaths(ctx, opts...)
		if err != nil {
			return nil, fmt.Errorf("restore_collection: get partition backup binlog files: %w", err)
		}
		return []restoreGroup{{insertLogDir: insertLogDir, deltaLogDir: deltaLogDir, size: part.Size}}, nil
	}

	for _, gID := range groupIDs {
		opts := []mpath.PathOption{
			mpath.CollectionID(ct.task.GetCollBackup().CollectionId),
			mpath.PartitionID(part.GetPartitionId()),
			mpath.GroupID(gID),
		}
		insertLogDir, deltaLogDir, err := ct.verifyBackupPartitionPaths(ctx, opts...)
		if err != nil {
			return nil, fmt.Errorf("restore_collection: get partition backup binlog files with group: %w", err)
		}
		groups = append(groups, restoreGroup{insertLogDir: insertLogDir, deltaLogDir: deltaLogDir, size: part.Size})
	}

	return groups, nil
}

// backupTS Only truncate the binlog by timestamp when CDC needs to be connected after restore.
func (ct *CollectionTask) backupTS() uint64 {
	if ct.task.GetTruncateBinlogByTs() {
		return ct.task.GetCollBackup().BackupTimestamp
	}

	return 0
}

func (ct *CollectionTask) bulkInsertViaGrpc(ctx context.Context, partition string, paths []string, isL0 bool) error {
	ct.logger.Info("start bulk insert via grpc", zap.Strings("paths", paths), zap.String("partition", partition))
	in := client.GrpcBulkInsertInput{
		DB:             ct.task.GetTargetDbName(),
		CollectionName: ct.task.GetTargetCollectionName(),
		PartitionName:  partition,
		Paths:          paths,
		BackupTS:       ct.backupTS(),
		IsL0:           isL0,
	}
	jobID, err := ct.grpcCli.BulkInsert(ctx, in)
	if err != nil {
		return fmt.Errorf("restore_collection: failed to bulk insert via grpc: %w", err)
	}
	ct.logger.Info("create bulk insert via grpc success", zap.Int64("job_id", jobID))

	// wait for bulk insert job done
	var lastProgress int
	lastUpdateTime := time.Now()
	for range time.Tick(_bulkInsertCheckInterval) {
		state, err := ct.grpcCli.GetBulkInsertState(ctx, jobID)
		if err != nil {
			return fmt.Errorf("restore_collection: failed to get bulk insert state: %w", err)
		}

		ct.logger.Info("bulk insert task state", zap.Int64("jobID", jobID), zap.Any("state", state.State),
			zap.Any("info", state.Infos))
		switch state.State {
		case commonpb.ImportState_ImportFailed:
			return fmt.Errorf("restore_collection: bulk insert failed: %s", getFailedReason(state.Infos))
		case commonpb.ImportState_ImportCompleted:
			ct.logger.Info("bulk insert task success", zap.Int64("job_id", jobID))
			return nil
		default:
			currentProgress := getProcess(state.Infos)
			if currentProgress > lastProgress {
				lastUpdateTime = time.Now()
			} else if time.Now().Sub(lastUpdateTime) >= _bulkInsertTimeout {
				ct.logger.Warn("bulk insert task timeout", zap.Int64("job_id", jobID),
					zap.Duration("timeout", _bulkInsertTimeout))
				return errors.New("restore_collection: bulk insert timeout")
			}
			continue
		}
	}

	return errors.New("restore_collection: walk into unreachable code")
}

func (ct *CollectionTask) bulkInsertViaRestful(ctx context.Context, partition string, paths [][]string, isL0 bool) error {
	ct.logger.Info("start bulk insert via restful", zap.Int("paths_num", len(paths)), zap.String("partition", partition))
	in := client.RestfulBulkInsertInput{
		DB:             ct.task.GetTargetDbName(),
		CollectionName: ct.task.GetTargetCollectionName(),
		PartitionName:  partition,
		Paths:          paths,
		BackupTS:       ct.backupTS(),
		IsL0:           isL0,
	}
	jobID, err := ct.restfulCli.BulkInsert(ctx, in)
	if err != nil {
		return fmt.Errorf("restore_collection: failed to bulk insert via restful: %w", err)
	}
	ct.logger.Info("create bulk insert via restful success", zap.String("job_id", jobID))

	// wait for bulk insert job done
	var lastProgress int
	lastUpdateTime := time.Now()
	for range time.Tick(_bulkInsertCheckInterval) {
		resp, err := ct.restfulCli.GetBulkInsertState(ctx, ct.task.GetTargetDbName(), jobID)
		if err != nil {
			return fmt.Errorf("restore_collection: failed to get bulk insert state: %w", err)
		}

		ct.logger.Info("bulk insert task state", zap.String("job_id", jobID),
			zap.String("state", resp.Data.State),
			zap.Int("progress", resp.Data.Progress))
		switch resp.Data.State {
		case string(client.ImportStateFailed):
			return fmt.Errorf("restore_collection: bulk insert failed: %s", resp.Data.Reason)
		case string(client.ImportStateCompleted):
			ct.logger.Info("bulk insert task success", zap.String("job_id", jobID))
			return nil
		default:
			currentProgress := resp.Data.Progress
			if currentProgress > lastProgress {
				lastUpdateTime = time.Now()
			} else if time.Now().Sub(lastUpdateTime) >= _bulkInsertTimeout {
				ct.logger.Warn("bulk insert task timeout", zap.String("job_id", jobID),
					zap.Duration("timeout", _bulkInsertTimeout))
				return errors.New("restore_collection: bulk insert timeout")
			}
			continue
		}
	}

	return errors.New("restore_collection: walk into unreachable code")
}

func getProcess(infos []*commonpb.KeyValuePair) int {
	m := lo.SliceToMap(infos, func(info *commonpb.KeyValuePair) (string, string) {
		return info.Key, info.Value
	})
	if val, ok := m["progress_percent"]; ok {
		progress, err := strconv.Atoi(val)
		if err != nil {
			return 0
		}
		return progress
	}
	return 0
}

func getFailedReason(infos []*commonpb.KeyValuePair) string {
	m := lo.SliceToMap(infos, func(info *commonpb.KeyValuePair) (string, string) {
		return info.Key, info.Value
	})

	if val, ok := m["failed_reason"]; ok {
		return val
	}
	return ""
}

func (ct *CollectionTask) verifyBackupPartitionPaths(ctx context.Context, pathOpt ...mpath.PathOption) (string, string, error) {
	insertLogDir := mpath.InsertLogDir(ct.backupPath, pathOpt...)
	deltaLogDir := mpath.DeltaLogDir(ct.backupPath, pathOpt...)

	exist, err := ct.backupStorage.Exist(ctx, ct.backupBucketName, deltaLogDir)
	if err != nil {
		return "", "", fmt.Errorf("restore_collection: check delta log exist: %w", err)
	}

	if exist {
		return insertLogDir, deltaLogDir, nil
	} else {
		return insertLogDir, "", nil
	}
}
