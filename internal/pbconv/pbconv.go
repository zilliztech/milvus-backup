package pbconv

import (
	"encoding/base64"
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
)

func BakKVToMilvusKV(kv []*backuppb.KeyValuePair, skipKeys ...string) []*commonpb.KeyValuePair {
	skip := lo.SliceToMap(skipKeys, func(item string) (string, struct{}) {
		return item, struct{}{}
	})

	return lo.FilterMap(kv, func(item *backuppb.KeyValuePair, i int) (*commonpb.KeyValuePair, bool) {
		if _, ok := skip[item.GetKey()]; ok {
			return nil, false
		}

		return &commonpb.KeyValuePair{Key: item.GetKey(), Value: item.GetValue()}, true
	})
}

func MilvusKVToBakKV(kv []*commonpb.KeyValuePair) []*backuppb.KeyValuePair {
	return lo.Map(kv, func(item *commonpb.KeyValuePair, _ int) *backuppb.KeyValuePair {
		return &backuppb.KeyValuePair{
			Key:   item.GetKey(),
			Value: item.GetValue(),
		}
	})
}

func MilvusKVToMap(kvs []*commonpb.KeyValuePair) map[string]string {
	res := make(map[string]string, len(kvs))
	for _, kv := range kvs {
		res[kv.GetKey()] = kv.GetValue()
	}
	return res
}

func RestoreCollTaskViewToResp(collRef collref.Name, taskView jobstate.RestoreCollTaskView) *backuppb.RestoreCollectionTaskResponse {
	return &backuppb.RestoreCollectionTaskResponse{
		Id:                   taskView.ID(),
		StateCode:            taskView.StateCode(),
		ErrorMessage:         taskView.ErrorMessage(),
		StartTime:            taskView.StartTime().Unix(),
		EndTime:              taskView.EndTime().Unix(),
		Progress:             taskView.Progress(),
		TargetDbName:         collRef.DBName(),
		TargetCollectionName: collRef.CollName(),
	}
}

func RestoreTaskViewToResp(view jobstate.RestoreTaskView) *backuppb.RestoreBackupTaskResponse {
	collTasks := view.CollTasks()
	collTaskResps := make([]*backuppb.RestoreCollectionTaskResponse, 0, len(collTasks))
	for collRef, taskView := range collTasks {
		collTaskResps = append(collTaskResps, RestoreCollTaskViewToResp(collRef, taskView))
	}

	return &backuppb.RestoreBackupTaskResponse{
		Id:                     view.ID(),
		StateCode:              view.StateCode(),
		ErrorMessage:           view.ErrorMessage(),
		StartTime:              view.StartTime().Unix(),
		EndTime:                view.EndTime().Unix(),
		Progress:               view.Progress(),
		CollectionRestoreTasks: collTaskResps,
	}
}

func Base64MsgPosition(position *msgpb.MsgPosition) (string, error) {
	positionByte, err := proto.Marshal(position)
	if err != nil {
		return "", fmt.Errorf("utils: encode msg position %w", err)
	}
	return base64.StdEncoding.EncodeToString(positionByte), nil
}

func Base64DecodeMsgPosition(position string) (*msgpb.MsgPosition, error) {
	decodeBytes, err := base64.StdEncoding.DecodeString(position)
	if err != nil {
		return nil, fmt.Errorf("utils: base64 decode msg position %w", err)
	}

	var msgPosition msgpb.MsgPosition
	if err = proto.Unmarshal(decodeBytes, &msgPosition); err != nil {
		return nil, fmt.Errorf("utils: unmarshal msg position %w", err)
	}
	return &msgPosition, nil
}

// NewBackupInfoBrief merges the two halves of the v1 get_backup answer: the
// job snapshot overlays progress, the persisted artifact carries everything
// else. The wire's state code and progress percent are renderings of the
// jobstate snapshot, computed here — the state package knows nothing about
// the pb enums.
func NewBackupInfoBrief(task *jobstate.BackupStatus, backup *backuppb.BackupInfo, metaSize int64) *backuppb.BackupInfoBrief {
	brief := &backuppb.BackupInfoBrief{MetaSize: metaSize}

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

// backupCollProgress renders one collection's percent. Schema work covers
// [1, 15], the data copy maps its copied ratio onto [15, 100].
func backupCollProgress(coll jobstate.BackupCollStatus) int32 {
	switch coll.State {
	case jobstate.BackupCollStatePending:
		return 1
	case jobstate.BackupCollStateDDLExecuting:
		return 5
	case jobstate.BackupCollStateDDLDone:
		return 10
	case jobstate.BackupCollStateDMLPrepare:
		return 15
	case jobstate.BackupCollStateDMLExecuting:
		// if total size is 0, it means there is no data should be backup, so we just return 15
		if coll.TotalSize == 0 {
			return 15
		}
		// the range is [15, 100], so we need to map [0, 100] to [15, 100]
		progress := float64(coll.CopiedSize) / float64(coll.TotalSize) * 100
		mapped := int32(progress*0.85) + 15
		return mapped
	case jobstate.BackupCollStateSuccess, jobstate.BackupCollStateDMLDone:
		return 100
	case jobstate.BackupCollStateFail:
		return 0
	default:
		return 0
	}
}

func newCollBackupBrief(colls []*backuppb.CollectionBackupInfo) []*backuppb.CollectionBackupInfoBrief {
	briefs := make([]*backuppb.CollectionBackupInfoBrief, 0, len(colls))
	for _, coll := range colls {
		brief := &backuppb.CollectionBackupInfoBrief{
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
