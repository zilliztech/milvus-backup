package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/app"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/restore"
	"github.com/zilliztech/milvus-backup/core/utils"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/filter"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/pbconv"
	"github.com/zilliztech/milvus-backup/internal/validate"
)

// restoreBackupUC is the slice of app.Restore the handler needs. The consumer
// defines it: this narrow interface is what handler tests stub out.
type restoreBackupUC interface {
	Start(ctx context.Context, req app.RestoreRequest) (app.RestoreJob, error)
	TaskView(taskID string) (app.RestoreTaskView, error)
}

// RestoreBackup Restore interface
// @Summary Restore interface
// @Description Submit a request to restore the data from backup
// @Tags Restore
// @Accept application/json
// @Produce application/json
// @Param request_id header string false "request_id"
// @Param object body backuppb.RestoreBackupRequest   true  "RestoreBackupRequest JSON"
// @Success 200 {object} backuppb.RestoreBackupResponse
// @Router /restore [post]
func (s *Server) handleRestoreBackup(c *gin.Context) {
	var request backuppb.RestoreBackupRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("invalid request body: %s", err)})
		return
	}

	// The restore outlives the HTTP request, so the usecase runs detached
	// from it, exactly as this endpoint always has.
	resp := s.restore(context.Background(), &request)

	writeResponse(c, "restore backup fail", resp)
}

// restore keeps the v1 contract of this endpoint: request id and task id are
// defaulted on the request, every failure before the task exists responds
// without a request id, and the async flag makes the server run the job in
// its own goroutine.
func (s *Server) restore(ctx context.Context, request *backuppb.RestoreBackupRequest) *backuppb.RestoreBackupResponse {
	completeRestoreRequest(request)
	if err := validateRestoreRequest(request); err != nil {
		return &backuppb.RestoreBackupResponse{Code: backuppb.ResponseCode_Parameter_Error, Msg: err.Error()}
	}

	req, err := newRestoreRequest(request)
	if err != nil {
		return &backuppb.RestoreBackupResponse{Code: backuppb.ResponseCode_Fail, Msg: err.Error()}
	}

	uc := s.config.newRestoreBackup(s.params)

	job, err := uc.Start(ctx, *req)
	if err != nil {
		// A backup that is not there is the caller's mistake; everything
		// else is the server's.
		code := backuppb.ResponseCode_Fail
		var notFound *app.BackupNotFoundError
		if errors.As(err, &notFound) {
			code = backuppb.ResponseCode_Parameter_Error
		}
		return &backuppb.RestoreBackupResponse{Code: code, Msg: err.Error()}
	}

	if request.GetAsync() {
		return s.restoreAsync(uc, job, request.GetRequestId())
	}

	if err := job.Execute(ctx); err != nil {
		return &backuppb.RestoreBackupResponse{Code: backuppb.ResponseCode_Fail, Msg: err.Error()}
	}

	view, err := uc.TaskView(job.TaskID())
	if err != nil {
		resp := &backuppb.RestoreBackupResponse{RequestId: request.GetRequestId()}
		resp.Code = backuppb.ResponseCode_Fail
		log.Error("get restore task fail", zap.String("taskId", job.TaskID()), zap.Error(err))
		resp.Msg = err.Error()
		return resp
	}

	return &backuppb.RestoreBackupResponse{
		RequestId: request.GetRequestId(),
		Code:      backuppb.ResponseCode_Success,
		Msg:       "success",
		Data:      pbconv.RestoreTaskViewToResp(view),
	}
}

// restoreAsync launches the job in the server's own goroutine — async is a
// deployment concern of the HTTP server, not of the usecase — then reports
// the freshly registered job's view.
func (s *Server) restoreAsync(uc restoreBackupUC, job app.RestoreJob, requestID string) *backuppb.RestoreBackupResponse {
	go func() {
		if err := job.Execute(context.Background()); err != nil {
			log.Error("restore backup task execute fail", zap.String("backupId", job.TaskID()), zap.Error(err))
		}
	}()

	view, err := uc.TaskView(job.TaskID())
	if err != nil {
		resp := &backuppb.RestoreBackupResponse{RequestId: requestID}
		resp.Code = backuppb.ResponseCode_Fail
		log.Error("get restore task fail", zap.String("taskId", job.TaskID()), zap.Error(err))
		resp.Msg = err.Error()
		return resp
	}

	return &backuppb.RestoreBackupResponse{
		RequestId: requestID,
		Code:      backuppb.ResponseCode_Success,
		Msg:       "restore backup is executing asynchronously",
		Data:      pbconv.RestoreTaskViewToResp(view),
	}
}

// completeRestoreRequest defaults the request id and the restore task id.
func completeRestoreRequest(request *backuppb.RestoreBackupRequest) {
	if len(request.GetRequestId()) == 0 {
		request.RequestId = uuid.NewString()
	}

	if len(request.GetId()) == 0 {
		taskID := "restore_" + fmt.Sprint(time.Now().UTC().Format("2006_01_02_15_04_05_")) + fmt.Sprint(time.Now().Nanosecond())
		request.Id = taskID
	}
}

func validateRestoreRequest(request *backuppb.RestoreBackupRequest) error {
	if len(request.GetBackupName()) == 0 {
		return errors.New("backup name is required")
	}

	if request.GetRestorePlan() != nil && len(request.GetCollectionSuffix()) != 0 {
		return errors.New("restore plan and collection suffix cannot be set at the same time")
	}

	if request.GetRestorePlan() != nil && len(request.GetCollectionRenames()) != 0 {
		return errors.New("restore plan and collection renames cannot be set at the same time")
	}

	if len(request.GetCollectionSuffix()) != 0 {
		if has := validate.HasSpecialChar(request.GetCollectionSuffix()); has {
			return errors.New("only alphanumeric characters and underscores are allowed in collection suffix")
		}
	}

	if request.GetDropExistCollection() && request.GetSkipCreateCollection() {
		return errors.New("drop_exist_collection and skip_create_collection cannot be true at the same time")
	}

	return nil
}

// newRestoreRequest translates the v1 pb grammar into the usecase request:
// the plan and the option are built here because they are parse products of
// pb-only fields, including the deprecated db_collections.
func newRestoreRequest(request *backuppb.RestoreBackupRequest) (*app.RestoreRequest, error) {
	plan, err := newPlanFromRequest(request)
	if err != nil {
		return nil, fmt.Errorf("server: create restore plan: %w", err)
	}

	return &app.RestoreRequest{
		TaskID:     request.GetId(),
		BackupName: request.GetBackupName(),
		BucketName: request.GetBucketName(),
		Path:       request.GetPath(),
		Plan:       plan,
		Option:     newOptionFromRequest(request),
	}, nil
}

func newSkipParamsFromRequest(request *backuppb.RestoreBackupRequest) restore.SkipParams {
	return restore.SkipParams{
		CollectionProperties: request.GetSkipParams().GetCollectionProperties(),
		FieldIndexParams:     request.GetSkipParams().GetFieldIndexParams(),
		FieldTypeParams:      request.GetSkipParams().GetFieldTypeParams(),
		IndexParams:          request.GetSkipParams().GetIndexParams(),
	}
}

func newOptionFromRequest(request *backuppb.RestoreBackupRequest) *restore.Option {
	return &restore.Option{
		DropExistIndex:       request.GetDropExistIndex(),
		RebuildIndex:         request.GetRestoreIndex(),
		UseAutoIndex:         request.GetUseAutoIndex(),
		DropExistCollection:  request.GetDropExistCollection(),
		SkipCreateCollection: request.GetSkipCreateCollection(),
		MaxShardNum:          request.GetMaxShardNum(),
		SkipParams:           newSkipParamsFromRequest(request),
		MetaOnly:             request.GetMetaOnly(),
		UseV2Restore:         request.GetUseV2Restore(),
		TruncateBinlogByTs:   request.GetTruncateBinlogByTs(),
		RestoreRBAC:          request.GetRbac(),
		EZKMapping:           request.GetEzkMapping(),
	}
}

func newPlanFromRequest(request *backuppb.RestoreBackupRequest) (*restore.Plan, error) {
	backupFilter, err := newBackupFilter(request)
	if err != nil {
		return nil, fmt.Errorf("restore: create backup filter: %w", err)
	}

	dbMapper, err := newDBMapper(request.GetRestorePlan())
	if err != nil {
		return nil, fmt.Errorf("restore: create db mapper: %w", err)
	}
	collMapper, err := newCollMapper(request)
	if err != nil {
		return nil, fmt.Errorf("restore: create coll mapper: %w", err)
	}

	taskFilter, err := newTaskFilter(request)
	if err != nil {
		return nil, fmt.Errorf("restore: create task filter: %w", err)
	}

	return &restore.Plan{
		BackupFilter:  backupFilter,
		DBMapper:      dbMapper,
		CollMapper:    collMapper,
		CollOverrides: newCollOverridesFromPlan(request.GetRestorePlan()),
		TaskFilter:    taskFilter,
	}, nil
}

// newTableMapperFromCollRename creates a new TableMapper with the given rename map.
// Rename map format: key: oldName, value: newName
// rule 1. key: db1.* value: db2.*
// rule 2. key: db1.coll1 value: db2.coll2
// rule 3. key: coll1 value: coll2 , under default db
// rule 4. key: db1. value: db2.
func newTableMapperFromCollRename(collRename map[string]string) (*restore.TableMapper, error) {
	// add default db in collection_renames if not set
	nameMapping := make(map[string][]collref.Name)
	dbWildcard := make(map[string]string)

	for k, v := range collRename {
		rule, err := filter.InferMapperRuleType(k, v)
		if err != nil {
			return nil, err
		}

		switch rule {
		case 1:
			dbWildcard[k[:len(k)-2]] = v[:len(v)-2]
		case 2, 3:
			oldName, err := collref.Parse(k)
			if err != nil {
				return nil, fmt.Errorf("restore: parse collection name %s %w", k, err)
			}
			newName, err := collref.Parse(v)
			if err != nil {
				return nil, fmt.Errorf("restore: parse collection name %s %w", v, err)
			}

			nameMapping[oldName.String()] = append(nameMapping[oldName.String()], newName)
		case 4:
			// handle in db mapping
			continue
		}
	}

	return &restore.TableMapper{DBWildcard: dbWildcard, NameMapping: nameMapping}, nil
}

func newCollMapperFromPlan(plan *backuppb.RestorePlan) (restore.CollMapper, error) {
	nameMapping := make(map[string][]collref.Name)
	for _, mapping := range plan.Mapping {
		if mapping.GetSource() == "" {
			return nil, fmt.Errorf("restore: source database name is empty")
		}

		if mapping.GetTarget() == "" {
			return nil, fmt.Errorf("restore: target database name is empty")
		}

		for _, collMapping := range mapping.Colls {
			oldName := collref.New(mapping.GetSource(), collMapping.GetSource())
			newName := collref.New(mapping.GetTarget(), collMapping.GetTarget())
			nameMapping[oldName.String()] = append(nameMapping[oldName.String()], newName)
		}
	}

	return &restore.TableMapper{NameMapping: nameMapping}, nil
}

func newCollMapper(request *backuppb.RestoreBackupRequest) (restore.CollMapper, error) {
	if request.GetRestorePlan() != nil {
		return newCollMapperFromPlan(request.GetRestorePlan())
	}

	if len(request.GetCollectionRenames()) != 0 {
		mapper, err := newTableMapperFromCollRename(request.GetCollectionRenames())
		if err != nil {
			return nil, fmt.Errorf("restore: create map renamer %w", err)
		}
		return mapper, nil
	}

	if len(request.GetCollectionSuffix()) != 0 {
		mapper := restore.NewSuffixMapper(request.GetCollectionSuffix())
		return mapper, nil
	}

	return restore.NewDefaultCollMapper(), nil
}

func newDBMapper(plan *backuppb.RestorePlan) (map[string][]restore.DBMapping, error) {
	if plan == nil {
		return nil, nil
	}

	dbMapper := make(map[string][]restore.DBMapping)
	for _, mapping := range plan.Mapping {
		if mapping.GetSource() == "" {
			return nil, fmt.Errorf("restore: source database name is empty")
		}

		if mapping.GetTarget() == "" {
			return nil, fmt.Errorf("restore: target database name is empty")
		}

		mapper := restore.DBMapping{Target: mapping.GetTarget(), WithProp: mapping.GetWithProp()}
		dbMapper[mapping.GetSource()] = append(dbMapper[mapping.GetSource()], mapper)
	}

	return dbMapper, nil
}

func newCollOverridesFromPlan(plan *backuppb.RestorePlan) map[string]restore.CollOverride {
	if plan == nil {
		return nil
	}

	overrides := make(map[string]restore.CollOverride)
	for _, mapping := range plan.GetMapping() {
		for _, collMapping := range mapping.GetColls() {
			o := collMapping.GetOverride()
			if o == nil {
				continue
			}
			if o.GetShardNum() == 0 && o.GetDescription() == "" {
				continue
			}
			target := collref.New(mapping.GetTarget(), collMapping.GetTarget())
			overrides[target.String()] = restore.CollOverride{
				ShardNum:    o.GetShardNum(),
				Description: o.GetDescription(),
			}
		}
	}

	if len(overrides) == 0 {
		return nil
	}
	return overrides
}

func newFilterFromDBCollections(dbCollections string) (filter.Filter, error) {
	dbColls := make(map[string][]string)
	if err := json.Unmarshal([]byte(dbCollections), &dbColls); err != nil {
		return filter.Filter{}, fmt.Errorf("restore: unmarshal dbCollections: %w", err)
	}

	collFilter := make(map[string]filter.CollFilter, len(dbColls))
	for dbName, colls := range dbColls {
		if dbName == "" {
			dbName = collref.DefaultDBName
		}

		if len(colls) == 0 {
			collFilter[dbName] = filter.CollFilter{AllowAll: true}
		} else {
			collName := make(map[string]struct{}, len(colls))
			for _, coll := range colls {
				collName[coll] = struct{}{}
			}
			collFilter[dbName] = filter.CollFilter{CollName: collName}
		}
	}

	return filter.Filter{DBCollFilter: collFilter}, nil
}

func newFilterFromCollectionNames(collectionNames []string) (filter.Filter, error) {
	collFilter := make(map[string]filter.CollFilter)
	for _, name := range collectionNames {
		dbName, err := collref.Parse(name)
		if err != nil {
			return filter.Filter{}, fmt.Errorf("restore: parse collection name %s: %w", name, err)
		}
		f, ok := collFilter[dbName.DBName()]
		if !ok {
			f = filter.CollFilter{CollName: make(map[string]struct{})}
			collFilter[dbName.DBName()] = f
		}
		f.CollName[dbName.CollName()] = struct{}{}
	}

	return filter.Filter{DBCollFilter: collFilter}, nil
}

func newBackupFilter(request *backuppb.RestoreBackupRequest) (filter.Filter, error) {
	// from db collection
	dbCollectionsStr := utils.GetDBCollections(request.GetDbCollections())
	if dbCollectionsStr != "" {
		return newFilterFromDBCollections(dbCollectionsStr)
	}

	// from collection names
	if len(request.GetCollectionNames()) != 0 {
		return newFilterFromCollectionNames(request.GetCollectionNames())
	}

	return filter.Filter{}, nil
}

func newFilterFromPlan(plan *backuppb.RestorePlan) (filter.Filter, error) {
	return filter.FromPB(plan.GetFilter())
}

func newTaskFilter(request *backuppb.RestoreBackupRequest) (filter.Filter, error) {
	// from restore plan
	if request.GetRestorePlan() != nil {
		return newFilterFromPlan(request.GetRestorePlan())
	}

	// from db collection
	dbCollectionsStr := utils.GetDBCollections(request.GetDbCollectionsAfterRename())
	if dbCollectionsStr != "" {
		return newFilterFromDBCollections(dbCollectionsStr)
	}

	return filter.Filter{}, nil
}
