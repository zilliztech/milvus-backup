package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/app"
	"github.com/zilliztech/milvus-backup/core/apiv1"
	"github.com/zilliztech/milvus-backup/core/backup"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/filter"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
	"github.com/zilliztech/milvus-backup/internal/log"
)

// backupJobFactory builds and registers the job for one create request.
// Unlike the other new* constructors it takes the request: a backup job is
// per-request, so construction and registration are one step.
type backupJobFactory func(ctx context.Context, params *cfg.Config, req app.CreateBackupRequest) (app.AsyncJob[jobstate.BackupStatus], error)

// CreateBackup Create backup interface
// @Summary Create backup interface
// @Description Create a backup with the given name and collections
// @Description Storage preflight failures return HTTP 200 with code 503 (Storage_Not_Ready), without registering a task. The caller may retry the same request.
// @Description A backup name whose artifact directory already exists fails with code Fail, also without registering a task.
// @Tags Backup
// @Accept application/json
// @Produce application/json
// @Param request_id header string false "request_id"
// @Param object body apiv1.CreateBackupRequest   true  "CreateBackupRequest JSON"
// @Success 200 {object} apiv1.BackupInfoResponse
// @Router /create [post]
func (s *Server) handleCreateBackup(c *echo.Context) error {
	var requestBody apiv1.CreateBackupRequest
	if err := json.NewDecoder(c.Request().Body).Decode(&requestBody); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]any{"error": "Invalid request body"})
	}
	requestBody.RequestId = c.Request().Header.Get("request_id")

	log.Info("receive create backup request", zap.Any("request", &requestBody))
	resp := s.createBackup(c.Request().Context(), &requestBody)
	log.Info("response create backup response", zap.Any("resp", resp))
	return writeResponse(c, resp)
}

// createBackup maps the v1 request onto the backup job and the outcome onto
// the v1 wire shape. Binding, request_id defaulting, the deprecated fields,
// name validation and the error-to-code mapping stay here; the action itself
// lives in app.NewBackupJob.
func (s *Server) createBackup(ctx context.Context, request *apiv1.CreateBackupRequest) *apiv1.BackupInfoResponse {
	if request.RequestId == "" {
		request.RequestId = uuid.NewString()
	}

	resp := &apiv1.BackupInfoResponse{RequestId: request.RequestId}

	if err := backup.ValidateName(request.BackupName); err != nil {
		resp.Code = backuppb.ResponseCode_Parameter_Error
		resp.Msg = err.Error()
		return resp
	}

	// The v1 backup_root_path field asks for a different backup location for
	// this call. That is a config override, not a request option: fork the
	// loaded config with it as one more override layer. The fork is
	// reload-equivalent, so unset backup.storage leaves still cascade from
	// milvus.storage, and the server's own config is never mutated.
	params := s.params
	if root := request.BackupRootPath; root != "" {
		log.Info("use backup root from request", zap.String("backup_root", root))
		var err error
		params, err = params.Fork(map[string]string{"backup.storage.rootPath": root})
		if err != nil {
			resp.Code = backuppb.ResponseCode_Fail
			resp.Msg = err.Error()
			return resp
		}
	}

	req, err := s.toCreateBackupRequest(request)
	if err != nil {
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = err.Error()
		return resp
	}

	job, err := s.config.newBackupJob(ctx, params, req)
	if err != nil {
		// Admission refused the request before any job existed, so this log
		// is the failure's only settlement — a registered job that fails
		// later logs at its own settlement instead.
		log.Error("create backup fail",
			zap.String("request_id", request.RequestId),
			zap.String("backup_name", req.Option.BackupName),
			zap.Error(err))
		resp.Code = createBackupErrorCode(err)
		resp.Msg = err.Error()
		return resp
	}

	if request.Async {
		return runCreateBackupAsync(job, request.RequestId)
	}
	return runCreateBackupSync(ctx, job, request.RequestId)
}

func createBackupErrorCode(err error) backuppb.ResponseCode {
	if errors.Is(err, app.ErrStorageNotReady) {
		return backuppb.ResponseCode_Storage_Not_Ready
	}
	return backuppb.ResponseCode_Fail
}

// runCreateBackupSync releases the job into the background and waits for it
// to settle on the request path. The v1 success response carries only code
// and msg — whatever a v1 response needs, this handler assembles itself, and
// this one needs nothing. The historical handler additionally read the
// persisted meta after a successful run and answered Fail when that read
// failed, but the read only fed a payload the response then discarded, so it
// is gone together with the payload.
func runCreateBackupSync(ctx context.Context, job app.AsyncJob[jobstate.BackupStatus], requestID string) *apiv1.BackupInfoResponse {
	// The job runs detached from the request — a client disconnect must not
	// kill a half-done backup — and the handler answers with the outcome it
	// settles on.
	job.Run(context.Background())

	status, err := job.Wait(ctx)
	if err != nil {
		// The request context died while waiting; the backup itself keeps
		// running to completion in the background.
		return &apiv1.BackupInfoResponse{
			RequestId: requestID,
			Code:      backuppb.ResponseCode_Fail,
			Msg:       err.Error(),
		}
	}
	if status.State == jobstate.BackupStateFail {
		return &apiv1.BackupInfoResponse{
			RequestId: requestID,
			Code:      backuppb.ResponseCode_Fail,
			Msg:       status.ErrorMessage,
		}
	}

	return &apiv1.BackupInfoResponse{Code: backuppb.ResponseCode_Success, Msg: "success"}
}

// runCreateBackupAsync releases the job and returns immediately; Run is what
// releases it into the background, the flag only selects not waiting for it.
func runCreateBackupAsync(job app.AsyncJob[jobstate.BackupStatus], requestID string) *apiv1.BackupInfoResponse {
	job.Run(context.Background())

	return &apiv1.BackupInfoResponse{
		RequestId: requestID,
		Code:      backuppb.ResponseCode_Success,
		Msg:       "create backup is executing asynchronously",
	}
}

// toCreateBackupRequest renders the v1 request into the usecase request. The
// deprecated fields (db_collections, collection_names, force, meta_only)
// keep their old meaning here: accepting or rejecting them is a transport
// decision.
func (s *Server) toCreateBackupRequest(request *apiv1.CreateBackupRequest) (app.CreateBackupRequest, error) {
	f, err := toFilter(request)
	if err != nil {
		return app.CreateBackupRequest{}, fmt.Errorf("server: build filter: %w", err)
	}

	strategy, err := toStrategy(request)
	if err != nil {
		return app.CreateBackupRequest{}, fmt.Errorf("server: build strategy: %w", err)
	}

	format, err := toFormat(request)
	if err != nil {
		return app.CreateBackupRequest{}, fmt.Errorf("server: build format: %w", err)
	}

	manageAddr := request.GcPauseAddress

	return app.CreateBackupRequest{
		TaskID: request.RequestId,
		Option: backup.Option{
			BackupName:       request.BackupName,
			PauseGC:          request.GcPauseEnable || s.params.Backup.PauseGC.Val,
			ManageAddr:       manageAddr,
			Strategy:         strategy,
			Format:           format,
			BackupRBAC:       request.Rbac,
			BackupIndexExtra: request.WithIndexExtra,
			Filter:           f,
		},
	}, nil
}

func toFilter(request *apiv1.CreateBackupRequest) (filter.Filter, error) {
	if request.Filter != nil {
		return filterFromWire(request.Filter)
	}

	dbCollectionsStr, err := dbCollectionsJSON(request.DbCollections) //nolint:staticcheck // SA1019: deprecated field for backward compatibility
	if err != nil {
		return filter.Filter{}, err
	}
	if len(dbCollectionsStr) > 0 {
		return dbCollectionsToFilter(dbCollectionsStr)
	}

	if len(request.CollectionNames) > 0 { //nolint:staticcheck // SA1019: deprecated field for backward compatibility
		return collectionNamesToFilter(request.CollectionNames) //nolint:staticcheck // SA1019: deprecated field for backward compatibility
	}

	return filter.Filter{}, nil
}

// dbCollectionsJSON renders the deprecated db_collections family into the
// JSON-object string the filter builder parses. The wire has always accepted
// two shapes — a JSON string holding the object, or the object itself — and
// the pb field normalized both through the well-known-types wrapper. A value
// that is neither shape surfaces where the filter is built, not as a bind
// error, the way the pb field behaved.
func dbCollectionsJSON(raw apiv1.JSON) (string, error) {
	trimmed := bytes.TrimSpace([]byte(raw))
	if len(trimmed) == 0 {
		return "", nil
	}

	if trimmed[0] == '"' {
		var s string
		if err := json.Unmarshal(trimmed, &s); err != nil {
			return "", fmt.Errorf("server: db_collections is not a json string: %w", err)
		}
		return s, nil
	}

	return string(trimmed), nil
}

func dbCollectionsToFilter(dbCollectionsStr string) (filter.Filter, error) {
	var dbCollections map[string][]string
	if err := json.Unmarshal([]byte(dbCollectionsStr), &dbCollections); err != nil {
		return filter.Filter{}, fmt.Errorf("server: unmarshal dbCollections: %w", err)
	}

	dbCollFilter := make(map[string]filter.CollFilter)
	for dbName, colls := range dbCollections {
		if len(colls) == 0 {
			dbCollFilter[dbName] = filter.CollFilter{AllowAll: true}
		} else {
			collName := make(map[string]struct{}, len(colls))
			for _, coll := range colls {
				collName[coll] = struct{}{}
			}
			dbCollFilter[dbName] = filter.CollFilter{CollName: collName}
		}
	}

	return filter.Filter{DBCollFilter: dbCollFilter}, nil
}

func collectionNamesToFilter(collectionNames []string) (filter.Filter, error) {
	dbCollFilter := make(map[string]filter.CollFilter)
	for _, nameStr := range collectionNames {
		collRef, err := collref.Parse(nameStr)
		if err != nil {
			return filter.Filter{}, fmt.Errorf("server: invalid collection name %s", nameStr)
		}

		if _, ok := dbCollFilter[collRef.DBName()]; !ok {
			dbCollFilter[collRef.DBName()] = filter.CollFilter{CollName: make(map[string]struct{})}
		}
		dbCollFilter[collRef.DBName()].CollName[collRef.CollName()] = struct{}{}
	}

	return filter.Filter{DBCollFilter: dbCollFilter}, nil
}

func filterFromWire(wire map[string]apiv1.CollFilter) (filter.Filter, error) {
	colls := make(map[string][]string, len(wire))
	for dbName, f := range wire {
		colls[dbName] = f.Colls
	}

	f, err := filter.FromDBColls(colls)
	if err != nil {
		return filter.Filter{}, fmt.Errorf("server: build filter from wire: %w", err)
	}

	return f, nil
}

func toStrategy(request *apiv1.CreateBackupRequest) (backup.Strategy, error) {
	if request.Strategy != "" {
		return backup.ParseStrategy(request.Strategy)
	}

	if request.Force { //nolint:staticcheck // SA1019: deprecated field for backward compatibility
		log.Warn("force option is deprecated, pls use strategy=skip_flush instead")
		return backup.StrategySkipFlush, nil
	}

	if request.MetaOnly { //nolint:staticcheck // SA1019: deprecated field for backward compatibility
		log.Warn("meta_only option is deprecated, pls use strategy=meta_only instead")
		return backup.StrategyMetaOnly, nil
	}

	return backup.StrategyAuto, nil
}

func toFormat(request *apiv1.CreateBackupRequest) (backup.Format, error) {
	if request.Format != "" {
		return backup.ParseFormat(request.Format)
	}

	return backup.FormatAuto, nil
}
