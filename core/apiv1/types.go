// Package apiv1 is the JSON contract of the /api/v1 endpoints: the request
// bodies the transports bind and the response shapes they render. The types
// used to be pb messages, which made every field a generated-file edit; they
// are plain structs now, and pb keeps only what it is genuinely for: the
// artifact messages and the enums, which these types reference.
//
// The shape is frozen: json tags, and which fields the wire omits, are what
// clients already depend on. The server package pins the rendering of every
// endpoint with goldens; a tag change that shows up there is a breaking
// change and needs the compat announcement, not a quiet edit.
//
// Omission follows the historical tags, not Go taste: success responses carry
// no code field (0 is omitted), an empty payload renders as "data": null, and
// the fields listed below — size, progress, state_code, has_index — are never
// omitted even at their zero value.
package apiv1

import (
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

// The v1 wire types are the JSON contract of the /api/v1 endpoints. They used
// to be pb messages, which made every field a generated-file edit; they are
// plain structs now, and pb keeps only what it is genuinely for: the artifact
// messages and the enums, which these types reference.
//
// The shape is frozen: json tags, and which fields the wire omits, are what
// clients already depend on. golden_test.go pins the rendering of every
// endpoint; a tag change that shows up there is a breaking change and needs
// the compat announcement, not a quiet edit.
//
// Omission follows the historical tags, not Go taste: success responses carry
// no code field (0 is omitted), an empty payload renders as "data": null, and
// the fields listed below — size, progress, state_code, has_index — are never
// omitted even at their zero value.

type CreateBackupRequest struct {
	// Uuid of request, defaults to a generated one when unset.
	RequestId string `json:"requestId,omitempty"`
	// Backup name, validated server-side: numbers, letters and underscores.
	BackupName string `json:"backup_name,omitempty"`
	// Collection names to backup, empty to backup all.
	//
	// Deprecated: use filter.
	CollectionNames []string `json:"collection_names,omitempty"`
	// Async or not: return after the job is registered instead of waiting for
	// it to settle.
	Async bool `json:"async,omitempty"`
	// Database and collections to backup. Accepts both shapes it always has:
	// a JSON string holding a JSON object, or the object itself.
	//
	// Deprecated: use filter.
	DbCollections JSON `json:"db_collections,omitempty"`
	// Force backup skip flush. Data must already be on disk when using it.
	//
	// Deprecated: set strategy=skip_flush instead.
	Force bool `json:"force,omitempty"`
	// Only backup meta, including collection schema and index info.
	//
	// Deprecated: set strategy=meta_only instead.
	MetaOnly bool `json:"meta_only,omitempty"`
	// Stop GC during the backup so the data to backup is not compacted or
	// GCed away; use it when the data is very large.
	GcPauseEnable bool `json:"gc_pause_enable,omitempty"`
	// Gc pause seconds, set it larger than the time cost of the backup.
	//
	// Deprecated: the server derives it from the config.
	GcPauseSeconds int32 `json:"gc_pause_seconds,omitempty"`
	// Gc pause API address.
	GcPauseAddress string `json:"gc_pause_address,omitempty"`
	// Whether backup RBAC.
	Rbac bool `json:"rbac,omitempty"`
	// Backup root path, overriding the config for this call.
	BackupRootPath string `json:"backup_root_path,omitempty"`
	// Filter for backup: database name to its collection list.
	Filter map[string]CollFilter `json:"filter,omitempty"`
	// Strategy for backup, one of [meta_only, skip_flush, bulk_flush,
	// serial_flush]; auto selects when unset.
	Strategy string `json:"strategy,omitempty"`
	// Also copy index-extra params (build_level and friends) verbatim.
	WithIndexExtra bool `json:"with_index_extra,omitempty"`
	// Format for backup, one of [auto, binlog, snapshot]; auto selects when
	// unset.
	Format string `json:"format,omitempty"`
}

// CollFilter is the per-database collection list of a filter. The single
// entry ["*"] selects every collection of the database.
type CollFilter struct {
	Colls []string `json:"colls,omitempty"`
}

// JSON is a field's raw JSON, kept verbatim at bind time. The deprecated
// db_collections family uses it because the field predates the filter: the
// wire has always accepted two shapes there — a JSON string holding the JSON
// object, or the object itself — and a plain string or map type would reject
// one of them.
type JSON string

// UnmarshalJSON stores the field's raw JSON verbatim; the parse happens
// where the filter is built, so a value in neither shape surfaces as that
// error, not as a bind error.
func (j *JSON) UnmarshalJSON(data []byte) error {
	*j = JSON(data)
	return nil
}

type GetBackupRequest struct {
	RequestId string `json:"requestId,omitempty"`
	// Backup name to query; backup_name or backup_id is needed.
	BackupName string `json:"backup_name,omitempty"`
	// Backup to query.
	BackupID string `json:"backup_id,omitempty"`
	// Bucket name; with path, overrides the config for this call.
	BucketName string `json:"bucket_name,omitempty"`
	// Path; with bucket_name, overrides the config for this call.
	Path string `json:"path,omitempty"`
	// Return a simple response without too much detail.
	//
	// Deprecated: always simple.
	WithoutDetail bool `json:"without_detail,omitempty"`
}

type ListBackupsRequest struct {
	RequestId string `json:"requestId,omitempty"`
	// Only return backups containing this collection.
	//
	// Deprecated: rejected.
	CollectionName string `json:"collection_name,omitempty"`
}

type DeleteBackupRequest struct {
	RequestId string `json:"requestId,omitempty"`
	// Backup name to delete.
	BackupName string `json:"backup_name,omitempty"`
}

type GetRestoreStateRequest struct {
	RequestId string `json:"requestId,omitempty"`
	// Restore task id to query.
	ID string `json:"id,omitempty"`
}

type RestoreSecondaryRequest struct {
	// Uuid of request; doubles as the restore task id.
	RequestId string `json:"requestId,omitempty"`
	// Backup name to restore.
	BackupName string `json:"backup_name,omitempty"`
	// Cdc source cluster id.
	SourceClusterID string `json:"sourceClusterID,omitempty"`
	// Cdc target cluster id.
	TargetClusterID string `json:"targetClusterID,omitempty"`
	// Async or not: return after the job is registered instead of waiting for
	// it to settle.
	Async bool `json:"async,omitempty"`
	// Path, overriding the config for this call.
	Path string `json:"path,omitempty"`
}

type RestoreBackupRequest struct {
	// Uuid of request, defaults to a generated one when unset.
	RequestId string `json:"requestId,omitempty"`
	// Backup name to restore.
	BackupName string `json:"backup_name,omitempty"`
	// Collections to restore.
	CollectionNames []string `json:"collection_names,omitempty"`
	// Rename the restored collections by appending a suffix.
	CollectionSuffix string `json:"collection_suffix,omitempty"`
	// Rename the restored collections by map; wins over collection_suffix.
	CollectionRenames map[string]string `json:"collection_renames,omitempty"`
	// Execute asynchronously: return after the job is registered.
	Async bool `json:"async,omitempty"`
	// Bucket name; with path, overrides the config for this call.
	BucketName string `json:"bucket_name,omitempty"`
	// Path; with bucket_name, overrides the config for this call.
	Path string `json:"path,omitempty"`
	// Database and collections to restore, for example
	// {"db1":["collection1"],"db2":["collection2","collection3"]}. Accepts
	// both shapes it always has: a JSON string holding a JSON object, or the
	// object itself.
	DbCollections JSON `json:"db_collections,omitempty"`
	// Only restore meta, not data.
	MetaOnly bool `json:"metaOnly,omitempty"`
	// Restore index info.
	RestoreIndex bool `json:"restoreIndex,omitempty"`
	// Use autoindex when restoring vector index.
	UseAutoIndex bool `json:"useAutoIndex,omitempty"`
	// Drop existing target collection before create.
	DropExistCollection bool `json:"dropExistCollection,omitempty"`
	// Drop existing index of target collection before create.
	DropExistIndex bool `json:"dropExistIndex,omitempty"`
	// Skip collection create; use when the collection exists and only index
	// or data is restored.
	SkipCreateCollection bool `json:"skipCreateCollection,omitempty"`
	// Restore task id, defaults to a generated one when unset.
	ID string `json:"id,omitempty"`
	// Skip the disk quota check in import.
	//
	// Deprecated: the check follows the config.
	SkipImportDiskQuotaCheck bool `json:"skipImportDiskQuotaCheck,omitempty"`
	// Whether restore RBAC.
	Rbac bool `json:"rbac,omitempty"`
	// Target max shard number.
	MaxShardNum int32 `json:"maxShardNum,omitempty"`
	// Skip the named params in the restore process.
	SkipParams *SkipParams `json:"skipParams,omitempty"`
	// Deprecated: set strategy=merged_seg instead.
	UseV2Restore bool `json:"useV2Restore,omitempty"`
	// Truncate binlog by timestamp, for cdc.
	TruncateBinlogByTs bool `json:"truncateBinlogByTs,omitempty"`
	// Which database and collections to restore after rename. Accepts both
	// shapes db_collections accepts.
	DbCollectionsAfterRename JSON `json:"db_collections_after_rename,omitempty"`
	// Per-database restore plan with renames and a task filter.
	RestorePlan *RestorePlan `json:"restorePlan,omitempty"`
	// Map of old ezk to new ezk, replacing encryption keys during restore.
	EzkMapping map[string]string `json:"ezkMapping,omitempty"`
	// Strategy for restore, one of [auto, per_seg, merged_seg, restore_snap,
	// import_snap]; auto, like leaving the field unset, lets the restore pick
	// the plan against the backup's format.
	Strategy string `json:"strategy,omitempty"`
	// REST endpoints of the peer clusters that replicate this target, e.g.
	// http://10.0.0.2:19530; when set, every import job is committed
	// two-phase so its mirror commits on every peer.
	Peers []string `json:"peers,omitempty"`
}

// SkipParams names the collection, field and index params the restore leaves
// untouched.
type SkipParams struct {
	CollectionProperties []string `json:"collectionProperties,omitempty"`
	FieldIndexParams     []string `json:"fieldIndexParams,omitempty"`
	FieldTypeParams      []string `json:"fieldTypeParams,omitempty"`
	IndexParams          []string `json:"indexParams,omitempty"`
}

// RestorePlan maps databases and their collections onto restore targets, and
// optionally filters which collection tasks run.
type RestorePlan struct {
	Mapping []*RestoreMapping     `json:"mapping,omitempty"`
	Filter  map[string]CollFilter `json:"filter,omitempty"`
}

type RestoreMapping struct {
	// Source database name; empty is an error.
	Source string `json:"source,omitempty"`
	// Target database name.
	Target string `json:"target,omitempty"`
	// Whether to restore with the source properties.
	WithProp bool                        `json:"withProp,omitempty"`
	Colls    []*RestoreCollectionMapping `json:"colls,omitempty"`
}

type RestoreCollectionOverride struct {
	ShardNum    int32  `json:"shardNum,omitempty"`
	Description string `json:"description,omitempty"`
}

type RestoreCollectionMapping struct {
	Source   string                     `json:"source,omitempty"`
	Target   string                     `json:"target,omitempty"`
	Override *RestoreCollectionOverride `json:"override,omitempty"`
}

type BackupInfoResponse struct {
	// Uuid of the request answered.
	RequestId string `json:"requestId,omitempty"`
	// Response code. 0 means success, others fail. Absent on success.
	Code backuppb.ResponseCode `json:"code,omitempty"`
	// Error msg if fail.
	Msg string `json:"msg,omitempty"`
	// Backup info entity; null when the response carries none.
	Data *BackupInfoBrief `json:"data"`
}

func (r *BackupInfoResponse) GetRequestId() string           { return r.RequestId }
func (r *BackupInfoResponse) GetCode() backuppb.ResponseCode { return r.Code }
func (r *BackupInfoResponse) GetMsg() string                 { return r.Msg }

// BackupInfoBrief merges the two halves of a backup's answer: the ephemeral
// job read overlays progress on the persisted artifact read.
type BackupInfoBrief struct {
	// From the task.
	Id           string                       `json:"id,omitempty"`
	StateCode    backuppb.BackupTaskStateCode `json:"state_code"`
	ErrorMessage string                       `json:"errorMessage,omitempty"`
	StartTime    int64                        `json:"start_time,omitempty"`
	EndTime      int64                        `json:"end_time,omitempty"`
	Progress     int32                        `json:"progress"`

	// From the backup meta.
	Name              string                         `json:"name,omitempty"`
	BackupTimestamp   uint64                         `json:"backup_timestamp,omitempty"`
	CollectionBackups []*CollectionBackupInfoBrief   `json:"collection_backups,omitempty"`
	Size              int64                          `json:"size"`
	MilvusVersion     string                         `json:"milvus_version,omitempty"`
	RpcChannelInfo    *backuppb.RPCChannelInfo       `json:"rpc_channel_info,omitempty"`
	DatabaseBackups   []*backuppb.DatabaseBackupInfo `json:"database_backups,omitempty"`
	// Sum of all meta files size.
	MetaSize int64 `json:"meta_size"`
	// See BackupInfo.format.
	Format string `json:"format,omitempty"`
}

type CollectionBackupInfoBrief struct {
	Id             string                     `json:"id,omitempty"`
	CollectionId   int64                      `json:"collection_id,omitempty"`
	DbName         string                     `json:"db_name,omitempty"`
	CollectionName string                     `json:"collection_name,omitempty"`
	Schema         *backuppb.CollectionSchema `json:"schema,omitempty"`
	ShardsNum      int32                      `json:"shards_num,omitempty"`
	// Logical time of backup, used for restore.
	BackupTimestamp uint64                `json:"backup_timestamp,omitempty"`
	Size            int64                 `json:"size"`
	HasIndex        bool                  `json:"has_index"`
	IndexInfos      []*backuppb.IndexInfo `json:"index_infos,omitempty"`
	LoadState       string                `json:"load_state,omitempty"`
	// Physical unix time of backup.
	BackupPhysicalTimestamp uint64                   `json:"backup_physical_timestamp,omitempty"`
	ChannelCheckpoints      map[string]string        `json:"channel_checkpoints,omitempty"`
	Properties              []*backuppb.KeyValuePair `json:"properties,omitempty"`
}

type ListBackupsResponse struct {
	// Uuid of the request answered.
	RequestId string `json:"requestId,omitempty"`
	// Response code. 0 means success, others fail. Absent on success.
	Code backuppb.ResponseCode `json:"code,omitempty"`
	// Error msg if fail.
	Msg string `json:"msg,omitempty"`
	// Backup info entities; null when there are none.
	Data []*BackupSummary `json:"data"`
}

func (r *ListBackupsResponse) GetRequestId() string           { return r.RequestId }
func (r *ListBackupsResponse) GetCode() backuppb.ResponseCode { return r.Code }
func (r *ListBackupsResponse) GetMsg() string                 { return r.Msg }

type BackupSummary struct {
	Id            string `json:"id,omitempty"`
	Name          string `json:"name,omitempty"`
	Size          int64  `json:"size"`
	MilvusVersion string `json:"milvus_version,omitempty"`
}

type DeleteBackupResponse struct {
	// Uuid of the request answered.
	RequestId string `json:"requestId,omitempty"`
	// Response code. 0 means success, others fail. Absent on success.
	Code backuppb.ResponseCode `json:"code,omitempty"`
	// Error msg if fail.
	Msg string `json:"msg,omitempty"`
}

func (r *DeleteBackupResponse) GetRequestId() string           { return r.RequestId }
func (r *DeleteBackupResponse) GetCode() backuppb.ResponseCode { return r.Code }
func (r *DeleteBackupResponse) GetMsg() string                 { return r.Msg }

type RestoreBackupResponse struct {
	// Uuid of the request answered; absent on the failures a request answers
	// before its id is known.
	RequestId string `json:"requestId,omitempty"`
	// Response code. 0 means success, others fail. Absent on success.
	Code backuppb.ResponseCode `json:"code,omitempty"`
	// Error msg if fail.
	Msg string `json:"msg,omitempty"`
	// Restore task info entity; null when the response carries none.
	Data *RestoreBackupTaskResponse `json:"data"`
}

func (r *RestoreBackupResponse) GetRequestId() string           { return r.RequestId }
func (r *RestoreBackupResponse) GetCode() backuppb.ResponseCode { return r.Code }
func (r *RestoreBackupResponse) GetMsg() string                 { return r.Msg }

type RestoreBackupTaskResponse struct {
	Id           string                        `json:"id,omitempty"`
	StateCode    backuppb.RestoreTaskStateCode `json:"state_code"`
	ErrorMessage string                        `json:"errorMessage,omitempty"`
	StartTime    int64                         `json:"start_time,omitempty"`
	EndTime      int64                         `json:"end_time,omitempty"`
	Progress     int32                         `json:"progress"`

	CollectionRestoreTasks []*RestoreCollectionTaskResponse `json:"collection_restore_tasks,omitempty"`
}

type RestoreCollectionTaskResponse struct {
	Id                   string                        `json:"id,omitempty"`
	StateCode            backuppb.RestoreTaskStateCode `json:"state_code"`
	ErrorMessage         string                        `json:"errorMessage,omitempty"`
	StartTime            int64                         `json:"start_time,omitempty"`
	EndTime              int64                         `json:"end_time,omitempty"`
	Progress             int32                         `json:"progress"`
	TargetDbName         string                        `json:"target_db_name,omitempty"`
	TargetCollectionName string                        `json:"target_collection_name,omitempty"`
}
