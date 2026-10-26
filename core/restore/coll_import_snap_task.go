package restore

import (
	"context"
	"encoding/json"
	"fmt"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/tasklet"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
)

// collImportSnapTask restores one collection of a snapshot backup through the
// target's import api: the DDL task creates the schema, partitions and indexes,
// then one import task reads the whole bundle. It mirrors collBinlogTask's
// shape — a DDL head and its data tasks — with a single job on the data side,
// because the import api expands the snapshot's partitions and segments itself.
type collImportSnapTask struct {
	ddl tasklet.Tasklet
	imp tasklet.Tasklet

	logger *zap.Logger
}

func (ct *collImportSnapTask) Execute(ctx context.Context) error {
	if err := ct.execute(ctx); err != nil {
		ct.logger.Error("restore collection failed", zap.Error(err))
		return err
	}

	ct.logger.Info("restore collection success")
	return nil
}

func (ct *collImportSnapTask) execute(ctx context.Context) error {
	if err := ct.ddl.Execute(ctx); err != nil {
		return fmt.Errorf("restore_collection: restore collection ddl: %w", err)
	}

	if err := ct.imp.Execute(ctx); err != nil {
		return fmt.Errorf("restore_collection: import snapshot: %w", err)
	}

	return nil
}

// snapshotImportTask submits one snapshot-sourced import job — the snapshot
// metadata URI with the source_type=snapshot contract — and waits it to
// completion. The job covers every partition of the collection: partition
// placement goes to the server as the partition mapping, and the snapshot's L0
// deletes fold into the rewrite on the server's own scope. No bytes move
// through this process; the source storage is read by Milvus, authorized by
// the external spec when the backup sits on another service.
type snapshotImportTask struct {
	taskID string
	target collref.Name

	metadataURI      string
	externalSpec     string
	partitionMapping string
	ezk              string

	restfulCli milvus.Restful
	// job is the import's half of the collection's node, minted when the plan
	// was built; it binds the server-side job id at submit and settles here.
	job *jobstate.RestoreJobTracker

	logger *zap.Logger
}

func (it *snapshotImportTask) Execute(ctx context.Context) error {
	if err := it.execute(ctx); err != nil {
		it.job.SetFailed(err)
		return err
	}

	it.job.SetCompleted()
	return nil
}

func (it *snapshotImportTask) execute(ctx context.Context) error {
	// Exactly one entry holding exactly one complete metadata URI: the contract
	// the import api keys the snapshot source on. BackupTS, storage version and
	// the L0 flag stay unset — the server rejects them as ambiguous next to a
	// snapshot source.
	in := milvus.BulkInsertV2Input{
		DB:               it.target.DBName(),
		CollectionName:   it.target.CollName(),
		Paths:            [][]string{{it.metadataURI}},
		SourceType:       milvus.SourceTypeSnapshot,
		ExternalSpec:     it.externalSpec,
		PartitionMapping: it.partitionMapping,
		EZK:              it.ezk,
	}
	it.logger.Info("start import snapshot via restful", zap.String("metadata_uri", it.metadataURI))

	jobID, err := it.restfulCli.BulkInsert(ctx, in)
	if err != nil {
		return fmt.Errorf("restore: import snapshot via restful: %w", err)
	}
	it.logger.Info("create snapshot import job success", zap.String("job_id", jobID))
	it.job.Bind(jobID)

	state := restfulJobState(it.logger, it.restfulCli, it.target.DBName(), jobID, milvus.ImportStateCompleted)
	return waitImportJob(ctx, it.logger, it.job, jobID, state)
}

// snapshotPartitionMapping renders the identity mapping of every source
// partition name onto the same-named target partition, as the json string the
// import api takes. It keeps the backup's partition layout: without a mapping
// the import routes every row of an ordinary collection into its _default
// partition. A partition-key collection takes no mapping — the server hashes
// its rows across the partitions it has.
func snapshotPartitionMapping(collBackup *backuppb.CollectionBackupInfo) (string, error) {
	for _, field := range collBackup.GetSchema().GetFields() {
		if field.GetIsPartitionKey() {
			return "", nil
		}
	}

	mapping := make(map[string]string, len(collBackup.GetPartitionBackups()))
	for _, part := range collBackup.GetPartitionBackups() {
		mapping[part.GetPartitionName()] = part.GetPartitionName()
	}
	if len(mapping) == 0 {
		return "", nil
	}

	bs, err := json.Marshal(mapping)
	if err != nil {
		return "", fmt.Errorf("restore: marshal partition mapping: %w", err)
	}
	// The server bounds the option to 64 KiB; fail where the oversized input
	// can be named rather than at job submission.
	if len(bs) > 64*1024 {
		return "", fmt.Errorf("restore: the partition mapping of %d partitions is %d bytes, over the 64 KiB the import api accepts",
			len(mapping), len(bs))
	}

	return string(bs), nil
}
