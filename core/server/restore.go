package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/app"
	"github.com/zilliztech/milvus-backup/core/apiv1"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/restore"
	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/filter"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/validate"
)

// restoreJob is the slice of app.RestoreJob the handler needs. The consumer
// defines it: app returns a concrete type, and this narrow interface is what
// handler tests stub out.
type restoreJob interface {
	// Run executes the job; where its goroutine goes is this server's
	// deployment decision, selected by the request's async flag.
	Run(ctx context.Context) error
}

// restoreJobFactory builds and registers the job for one restore request.
// Unlike the other new* constructors it takes the request: a restore job is
// per-request, so construction and registration are one step.
type restoreJobFactory func(ctx context.Context, params *cfg.Config, req app.RestoreRequest) (restoreJob, error)

// RestoreBackup Restore interface
// @Summary Restore interface
// @Description Submit a request to restore the data from backup
// @Tags Restore
// @Accept application/json
// @Produce application/json
// @Param request_id header string false "request_id"
// @Param object body apiv1.RestoreBackupRequest   true  "RestoreBackupRequest JSON"
// @Success 200 {object} apiv1.RestoreBackupResponse
// @Router /restore [post]
func (s *Server) handleRestoreBackup(c *echo.Context) error {
	var request apiv1.RestoreBackupRequest
	if err := json.NewDecoder(c.Request().Body).Decode(&request); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]any{"error": fmt.Sprintf("invalid request body: %s", err)})
	}

	// The restore outlives the HTTP request, so the job runs detached
	// from it, exactly as this endpoint always has.
	resp := s.restore(context.Background(), &request)

	return writeResponse(c, resp)
}

// restore keeps the v1 contract of this endpoint: request id and task id are
// defaulted on the request, every failure before the task exists responds
// without a request id, and the async flag makes the server run the job in
// its own goroutine.
func (s *Server) restore(ctx context.Context, request *apiv1.RestoreBackupRequest) *apiv1.RestoreBackupResponse {
	completeRestoreRequest(request)
	if err := validateRestoreRequest(request); err != nil {
		return &apiv1.RestoreBackupResponse{Code: backuppb.ResponseCode_Parameter_Error, Msg: err.Error()}
	}

	req, err := newRestoreRequest(request)
	if err != nil {
		return &apiv1.RestoreBackupResponse{Code: backuppb.ResponseCode_Fail, Msg: err.Error()}
	}

	// The v1 bucket_name and path fields ask for a different backup location
	// for this call. That is a config override, not a request option: fork
	// the loaded config with them as one more override layer. The fork is
	// reload-equivalent, so unset backup.storage leaves still cascade from
	// milvus.storage, and the server's own config is never mutated.
	params := s.params
	overrides := make(map[string]string)
	if bucket := request.BucketName; bucket != "" {
		log.Info("use bucket name from request", zap.String("bucketName", bucket))
		overrides["backup.storage.bucketName"] = bucket
	}
	if path := request.Path; path != "" {
		log.Info("use path from request", zap.String("path", path))
		overrides["backup.storage.rootPath"] = path
	}
	if len(overrides) != 0 {
		var err error
		params, err = params.Fork(overrides)
		if err != nil {
			return &apiv1.RestoreBackupResponse{Code: backuppb.ResponseCode_Fail, Msg: err.Error()}
		}
	}

	job, err := s.config.newRestoreJob(ctx, params, *req)
	if err != nil {
		// A backup that is not there is the caller's mistake; everything
		// else is the server's.
		code := backuppb.ResponseCode_Fail
		if errors.Is(err, app.ErrBackupNotFound) {
			code = backuppb.ResponseCode_Parameter_Error
		}
		return &apiv1.RestoreBackupResponse{Code: code, Msg: err.Error()}
	}

	if request.Async {
		return s.restoreAsync(job, request)
	}

	if err := job.Run(ctx); err != nil {
		// The sync path settles the failure here, mirroring the async
		// goroutine's log — writeResponse no longer logs on the way out.
		log.Error("restore backup task execute fail", zap.String("backupId", request.ID), zap.Error(err))
		return &apiv1.RestoreBackupResponse{Code: backuppb.ResponseCode_Fail, Msg: err.Error()}
	}

	view, err := s.restoreTaskView(ctx, request.ID)
	if err != nil {
		resp := &apiv1.RestoreBackupResponse{RequestId: request.RequestId}
		resp.Code = backuppb.ResponseCode_Fail
		log.Error("get restore task fail", zap.String("taskId", request.ID), zap.Error(err))
		resp.Msg = err.Error()
		return resp
	}

	return &apiv1.RestoreBackupResponse{
		RequestId: request.RequestId,
		Code:      backuppb.ResponseCode_Success,
		Msg:       "success",
		Data:      apiv1.NewRestoreTaskResp(view),
	}
}

// restoreAsync launches the job in the server's own goroutine — async is a
// deployment concern of the HTTP server, not of the job — then reports the
// freshly registered job's view.
func (s *Server) restoreAsync(job restoreJob, request *apiv1.RestoreBackupRequest) *apiv1.RestoreBackupResponse {
	go func() {
		if err := job.Run(context.Background()); err != nil {
			log.Error("restore backup task execute fail", zap.String("backupId", request.ID), zap.Error(err))
		}
	}()

	view, err := s.restoreTaskView(context.Background(), request.ID)
	if err != nil {
		resp := &apiv1.RestoreBackupResponse{RequestId: request.RequestId}
		resp.Code = backuppb.ResponseCode_Fail
		log.Error("get restore task fail", zap.String("taskId", request.ID), zap.Error(err))
		resp.Msg = err.Error()
		return resp
	}

	return &apiv1.RestoreBackupResponse{
		RequestId: request.RequestId,
		Code:      backuppb.ResponseCode_Success,
		Msg:       "restore backup is executing asynchronously",
		Data:      apiv1.NewRestoreTaskResp(view),
	}
}

// restoreTaskView reads the snapshot of a restore job through the get-restore
// usecase — the same usecase the /get_restore endpoint reads state through,
// so the job half has one reader in this package.
func (s *Server) restoreTaskView(ctx context.Context, taskID string) (jobstate.RestoreStatus, error) {
	uc, err := s.config.newGetRestore()
	if err != nil {
		return jobstate.RestoreStatus{}, err
	}

	return uc.Execute(ctx, taskID)
}

// completeRestoreRequest defaults the request id and the restore task id.
func completeRestoreRequest(request *apiv1.RestoreBackupRequest) {
	if len(request.RequestId) == 0 {
		request.RequestId = uuid.NewString()
	}

	if len(request.ID) == 0 {
		taskID := "restore_" + fmt.Sprint(time.Now().UTC().Format("2006_01_02_15_04_05_")) + fmt.Sprint(time.Now().Nanosecond())
		request.ID = taskID
	}
}

func validateRestoreRequest(request *apiv1.RestoreBackupRequest) error {
	if len(request.BackupName) == 0 {
		return errors.New("backup name is required")
	}

	if request.RestorePlan != nil && len(request.CollectionSuffix) != 0 {
		return errors.New("restore plan and collection suffix cannot be set at the same time")
	}

	if request.RestorePlan != nil && len(request.CollectionRenames) != 0 {
		return errors.New("restore plan and collection renames cannot be set at the same time")
	}

	if len(request.CollectionSuffix) != 0 {
		if has := validate.HasSpecialChar(request.CollectionSuffix); has {
			return errors.New("only alphanumeric characters and underscores are allowed in collection suffix")
		}
	}

	if request.DropExistCollection && request.SkipCreateCollection {
		return errors.New("drop_exist_collection and skip_create_collection cannot be true at the same time")
	}

	if request.Strategy != "" {
		if _, err := restore.ParseStrategy(request.Strategy); err != nil {
			return err
		}
	}

	for _, peer := range request.Peers {
		u, err := url.Parse(peer)
		if err != nil || u.Scheme == "" || u.Host == "" {
			return fmt.Errorf("invalid peer %q, expect an http(s) url like http://10.0.0.2:19530", peer)
		}
	}

	return nil
}

// newRestoreRequest translates the v1 grammar into the restore request: the
// plan and the option are built here because they are parse products of
// wire-only fields, including the deprecated db_collections.
func newRestoreRequest(request *apiv1.RestoreBackupRequest) (*app.RestoreRequest, error) {
	plan, err := newPlanFromRequest(request)
	if err != nil {
		return nil, fmt.Errorf("server: create restore plan: %w", err)
	}

	option, err := newOptionFromRequest(request)
	if err != nil {
		return nil, fmt.Errorf("server: create restore option: %w", err)
	}

	return &app.RestoreRequest{
		TaskID:     request.ID,
		BackupName: request.BackupName,
		Plan:       plan,
		Option:     option,
	}, nil
}

func newSkipParamsFromRequest(request *apiv1.RestoreBackupRequest) restore.SkipParams {
	skip := request.SkipParams
	if skip == nil {
		return restore.SkipParams{}
	}

	return restore.SkipParams{
		CollectionProperties: skip.CollectionProperties,
		FieldIndexParams:     skip.FieldIndexParams,
		FieldTypeParams:      skip.FieldTypeParams,
		IndexParams:          skip.IndexParams,
	}
}

func newOptionFromRequest(request *apiv1.RestoreBackupRequest) (*restore.Option, error) {
	strategy, err := toRestoreStrategy(request)
	if err != nil {
		return nil, err
	}

	return &restore.Option{
		DropExistIndex:       request.DropExistIndex,
		RebuildIndex:         request.RestoreIndex,
		UseAutoIndex:         request.UseAutoIndex,
		DropExistCollection:  request.DropExistCollection,
		SkipCreateCollection: request.SkipCreateCollection,
		MaxShardNum:          request.MaxShardNum,
		SkipParams:           newSkipParamsFromRequest(request),
		MetaOnly:             request.MetaOnly,
		Strategy:             strategy,
		TruncateBinlogByTs:   request.TruncateBinlogByTs,
		RestoreRBAC:          request.Rbac,
		EZKMapping:           request.EzkMapping,
		PeerEndpoints:        request.Peers,
	}, nil
}

// toRestoreStrategy resolves the request's strategy pin, with the deprecated
// use_v2_restore bool as the fallback: an explicit strategy wins over it, and
// without either the restore selects a plan on its own.
func toRestoreStrategy(request *apiv1.RestoreBackupRequest) (restore.Strategy, error) {
	if request.Strategy != "" {
		return restore.ParseStrategy(request.Strategy)
	}

	if request.UseV2Restore { //nolint:staticcheck // SA1019: deprecated field for backward compatibility
		log.Warn("use_v2_restore option is deprecated, pls use strategy=merged_seg instead")
		return restore.StrategyMergedSeg, nil
	}

	return restore.StrategyAuto, nil
}

func newPlanFromRequest(request *apiv1.RestoreBackupRequest) (*restore.Plan, error) {
	backupFilter, err := newBackupFilter(request)
	if err != nil {
		return nil, fmt.Errorf("restore: create backup filter: %w", err)
	}

	dbMapper, err := newDBMapper(request.RestorePlan)
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
		CollOverrides: newCollOverridesFromPlan(request.RestorePlan),
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

func newCollMapperFromPlan(plan *apiv1.RestorePlan) (restore.CollMapper, error) {
	nameMapping := make(map[string][]collref.Name)
	for _, mapping := range plan.Mapping {
		if mapping.Source == "" {
			return nil, fmt.Errorf("restore: source database name is empty")
		}

		if mapping.Target == "" {
			return nil, fmt.Errorf("restore: target database name is empty")
		}

		for _, collMapping := range mapping.Colls {
			oldName := collref.New(mapping.Source, collMapping.Source)
			newName := collref.New(mapping.Target, collMapping.Target)
			nameMapping[oldName.String()] = append(nameMapping[oldName.String()], newName)
		}
	}

	return &restore.TableMapper{NameMapping: nameMapping}, nil
}

func newCollMapper(request *apiv1.RestoreBackupRequest) (restore.CollMapper, error) {
	if request.RestorePlan != nil {
		return newCollMapperFromPlan(request.RestorePlan)
	}

	if len(request.CollectionRenames) != 0 {
		mapper, err := newTableMapperFromCollRename(request.CollectionRenames)
		if err != nil {
			return nil, fmt.Errorf("restore: create map renamer %w", err)
		}
		return mapper, nil
	}

	if len(request.CollectionSuffix) != 0 {
		mapper := restore.NewSuffixMapper(request.CollectionSuffix)
		return mapper, nil
	}

	return restore.NewDefaultCollMapper(), nil
}

func newDBMapper(plan *apiv1.RestorePlan) (map[string][]restore.DBMapping, error) {
	if plan == nil {
		return nil, nil
	}

	dbMapper := make(map[string][]restore.DBMapping)
	for _, mapping := range plan.Mapping {
		if mapping.Source == "" {
			return nil, fmt.Errorf("restore: source database name is empty")
		}

		if mapping.Target == "" {
			return nil, fmt.Errorf("restore: target database name is empty")
		}

		mapper := restore.DBMapping{Target: mapping.Target, WithProp: mapping.WithProp}
		dbMapper[mapping.Source] = append(dbMapper[mapping.Source], mapper)
	}

	return dbMapper, nil
}

func newCollOverridesFromPlan(plan *apiv1.RestorePlan) map[string]restore.CollOverride {
	if plan == nil {
		return nil
	}

	overrides := make(map[string]restore.CollOverride)
	for _, mapping := range plan.Mapping {
		for _, collMapping := range mapping.Colls {
			o := collMapping.Override
			if o == nil {
				continue
			}
			if o.ShardNum == 0 && o.Description == "" {
				continue
			}
			target := collref.New(mapping.Target, collMapping.Target)
			overrides[target.String()] = restore.CollOverride{
				ShardNum:    o.ShardNum,
				Description: o.Description,
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

func newBackupFilter(request *apiv1.RestoreBackupRequest) (filter.Filter, error) {
	// from db collection
	dbCollectionsStr, err := dbCollectionsJSON(request.DbCollections)
	if err != nil {
		return filter.Filter{}, err
	}
	if dbCollectionsStr != "" {
		return newFilterFromDBCollections(dbCollectionsStr)
	}

	// from collection names
	if len(request.CollectionNames) != 0 {
		return newFilterFromCollectionNames(request.CollectionNames)
	}

	return filter.Filter{}, nil
}

func newFilterFromPlan(plan *apiv1.RestorePlan) (filter.Filter, error) {
	return filterFromWire(plan.Filter)
}

func newTaskFilter(request *apiv1.RestoreBackupRequest) (filter.Filter, error) {
	// from restore plan
	if request.RestorePlan != nil {
		return newFilterFromPlan(request.RestorePlan)
	}

	// from db collection
	dbCollectionsStr, err := dbCollectionsJSON(request.DbCollectionsAfterRename)
	if err != nil {
		return filter.Filter{}, err
	}
	if dbCollectionsStr != "" {
		return newFilterFromDBCollections(dbCollectionsStr)
	}

	return filter.Filter{}, nil
}
