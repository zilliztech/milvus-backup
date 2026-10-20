package apiv1

import (
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
)

// The renderers turn the transport-neutral reads — the artifact meta and the
// jobstate views — into the v1 wire shapes. They used to live next to the pb
// conversions in internal/pbconv and moved with the wire types: the mapping
// they encode (the state codes, the progress percent) is the v1 contract.
// The CLI's get command renders the same brief, which is why these are
// exported.

func NewRestoreCollTaskResp(collRef collref.Name, coll jobstate.RestoreCollStatus) *RestoreCollectionTaskResponse {
	return &RestoreCollectionTaskResponse{
		StateCode:            restoreStateToPB(coll.State),
		ErrorMessage:         coll.ErrorMessage,
		StartTime:            coll.StartTime.Unix(),
		EndTime:              coll.EndTime.Unix(),
		Progress:             coll.Progress,
		TargetDbName:         collRef.DBName(),
		TargetCollectionName: collRef.CollName(),
	}
}

func NewRestoreTaskResp(status jobstate.RestoreStatus) *RestoreBackupTaskResponse {
	collTaskResps := make([]*RestoreCollectionTaskResponse, 0, len(status.Colls))
	for collRef, coll := range status.Colls {
		collTaskResps = append(collTaskResps, NewRestoreCollTaskResp(collRef, coll))
	}

	return &RestoreBackupTaskResponse{
		Id:                     status.ID,
		StateCode:              restoreStateToPB(status.State),
		ErrorMessage:           status.ErrorMessage,
		StartTime:              status.StartTime.Unix(),
		EndTime:                status.EndTime.Unix(),
		Progress:               status.Progress,
		CollectionRestoreTasks: collTaskResps,
	}
}

// restoreStateToPB renders a restore state as the v1 wire code.
func restoreStateToPB(state jobstate.RestoreState) backuppb.RestoreTaskStateCode {
	switch state {
	case jobstate.RestoreStateExecuting:
		return backuppb.RestoreTaskStateCode_EXECUTING
	case jobstate.RestoreStateSuccess:
		return backuppb.RestoreTaskStateCode_SUCCESS
	case jobstate.RestoreStateFail:
		return backuppb.RestoreTaskStateCode_FAIL
	default:
		return backuppb.RestoreTaskStateCode_INITIAL
	}
}

// newBackupInfoBrief merges the two halves of the v1 get_backup answer: the
// job snapshot overlays progress, the persisted artifact carries everything
// else. The wire's state code and progress percent are renderings of the
// jobstate snapshot, computed here — the state package knows nothing about
// the pb enums.
func NewBackupInfoBrief(task *jobstate.BackupStatus, backup *backuppb.BackupInfo, metaSize int64) *BackupInfoBrief {
	brief := &BackupInfoBrief{MetaSize: metaSize}

	if task != nil {
		brief.Name = task.Name
		brief.Id = task.ID
		brief.StateCode = backupStateToPB(task.State)
		brief.ErrorMessage = task.ErrorMessage
		brief.StartTime = task.StartTime.Unix()
		brief.EndTime = task.EndTime.Unix()
		brief.Progress = backupProgress(task)
	}

	if backup != nil {
		brief.Name = backup.GetName()
		brief.BackupTimestamp = backup.GetBackupTimestamp()
		brief.CollectionBackups = newCollBackupBrief(backup.GetCollectionBackups())
		brief.Size = backup.GetSize()
		brief.MilvusVersion = backup.GetMilvusVersion()
		brief.RpcChannelInfo = backup.GetRpcChannelInfo()
		brief.DatabaseBackups = backup.GetDatabaseBackups()
		brief.Format = backup.GetFormat()
	}

	return brief
}

// backupStateToPB renders the job's state as the v1 wire code. The pb enum is
// coarser than the job's: both executing phases read as EXECUTING.
func backupStateToPB(state jobstate.BackupState) backuppb.BackupTaskStateCode {
	switch state {
	case jobstate.BackupStateInitial:
		return backuppb.BackupTaskStateCode_BACKUP_INITIAL
	case jobstate.BackupStateDatabaseExecuting, jobstate.BackupStateCollectionExecuting:
		return backuppb.BackupTaskStateCode_BACKUP_EXECUTING
	case jobstate.BackupStateSuccess:
		return backuppb.BackupTaskStateCode_BACKUP_SUCCESS
	case jobstate.BackupStateFail:
		return backuppb.BackupTaskStateCode_BACKUP_FAIL
	default:
		return backuppb.BackupTaskStateCode_BACKUP_INITIAL
	}
}

// backupProgress renders the job's percent on the v1 wire. The mapping is the
// historical contract: registration reports 1, the database phase 5, the
// collection phase averages its collections and maps them onto [5, 100].
func backupProgress(task *jobstate.BackupStatus) int32 {
	switch task.State {
	case jobstate.BackupStateInitial:
		return 1
	case jobstate.BackupStateDatabaseExecuting:
		return 5
	case jobstate.BackupStateCollectionExecuting:
		// A job that covers no collection has nothing to average.
		if len(task.Colls) == 0 {
			return 5
		}

		// map [0, 100] to [5, 100]
		var totalProgress int32
		for _, coll := range task.Colls {
			totalProgress += backupCollProgress(coll)
		}

		return int32(float64(totalProgress)/float64(len(task.Colls))*0.95) + 5
	case jobstate.BackupStateSuccess:
		return 100
	case jobstate.BackupStateFail:
		return 0
	default:
		return 0
	}
}

// backupCollProgress renders one collection's percent out of its two halves.
// A failure on either half zeroes it. The data half is the bulk of the work,
// so when it exists it drives the percent: pending reads as 10, flushing as
// 12, prepare as 15, the copy maps its copied ratio onto [15, 100]. A
// collection with no data half at all — meta_only — is read from its schema
// half alone.
func backupCollProgress(coll jobstate.BackupCollStatus) int32 {
	if coll.DDL != nil && coll.DDL.State == jobstate.BackupCollDDLStateFail {
		return 0
	}
	if coll.DML != nil && coll.DML.State == jobstate.BackupCollDMLStateFail {
		return 0
	}

	if coll.DML != nil {
		switch coll.DML.State {
		case jobstate.BackupCollDMLStatePending:
			return 10
		case jobstate.BackupCollDMLStateFlushing:
			return 12
		case jobstate.BackupCollDMLStatePrepare:
			return 15
		case jobstate.BackupCollDMLStateExecuting:
			// if total size is 0, it means there is no data should be backup, so we just return 15
			if coll.DML.TotalSize == 0 {
				return 15
			}
			// the range is [15, 100], so we need to map [0, 100] to [15, 100]
			progress := float64(coll.DML.CopiedSize) / float64(coll.DML.TotalSize) * 100
			return int32(progress*0.85) + 15
		case jobstate.BackupCollDMLStateSuccess:
			return 100
		}
	}

	if coll.DDL != nil {
		switch coll.DDL.State {
		case jobstate.BackupCollDDLStatePending:
			return 1
		case jobstate.BackupCollDDLStateExecuting:
			return 5
		case jobstate.BackupCollDDLStateSuccess:
			return 100
		}
	}

	return 1
}

func newCollBackupBrief(colls []*backuppb.CollectionBackupInfo) []*CollectionBackupInfoBrief {
	briefs := make([]*CollectionBackupInfoBrief, 0, len(colls))
	for _, coll := range colls {
		brief := &CollectionBackupInfoBrief{
			Id:                      coll.GetId(),
			CollectionId:            coll.GetCollectionId(),
			DbName:                  coll.GetDbName(),
			CollectionName:          coll.GetCollectionName(),
			Schema:                  coll.GetSchema(),
			ShardsNum:               coll.GetShardsNum(),
			BackupTimestamp:         coll.GetBackupTimestamp(),
			Size:                    coll.GetSize(),
			HasIndex:                coll.GetHasIndex(),
			IndexInfos:              coll.GetIndexInfos(),
			LoadState:               coll.GetLoadState(),
			BackupPhysicalTimestamp: coll.GetBackupPhysicalTimestamp(),
			ChannelCheckpoints:      coll.GetChannelCheckpoints(),
			Properties:              coll.GetProperties(),
		}
		briefs = append(briefs, brief)
	}

	return briefs
}
