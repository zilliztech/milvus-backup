package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/app"
	"github.com/zilliztech/milvus-backup/core/backup"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/utils"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/filter"
	"github.com/zilliztech/milvus-backup/internal/log"
)

// createBackupUC is the slice of app.CreateBackup the handler needs. The
// consumer defines it: app returns concrete types, and this narrow interface
// is what handler tests stub out.
type createBackupUC interface {
	// Execute runs the backup job synchronously and returns the finished view.
	Execute(ctx context.Context, req app.CreateBackupRequest) (*app.BackupView, error)
	// Start registers the job and returns it ready to run; the async flag's
	// goroutine placement is this server's deployment decision.
	Start(req app.CreateBackupRequest) (app.BackupJob, error)
}

// CreateBackup Create backup interface
// @Summary Create backup interface
// @Description Create a backup with the given name and collections
// @Tags Backup
// @Accept application/json
// @Produce application/json
// @Param request_id header string false "request_id"
// @Param object body backuppb.CreateBackupRequest   true  "CreateBackupRequest JSON"
// @Success 200 {object} backuppb.BackupInfoResponse
// @Router /create [post]
func (s *Server) handleCreateBackup(c *gin.Context) {
	var requestBody backuppb.CreateBackupRequest
	if err := c.ShouldBindJSON(&requestBody); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request body"})
		return
	}
	requestBody.RequestId = c.GetHeader("request_id")

	log.Info("receive create backup request", zap.Any("request", &requestBody))
	resp := s.createBackup(c.Request.Context(), &requestBody)
	log.Info("response create backup response", zap.Any("resp", resp))
	writeResponse(c, "create backup fail", resp)
}

// createBackup maps the v1 request onto the create usecase and the usecase's
// outcome onto the v1 wire shape. Binding, request_id defaulting, the
// deprecated pb fields, name validation and the error-to-code mapping stay
// here; the action itself lives in app.CreateBackup.
func (s *Server) createBackup(ctx context.Context, request *backuppb.CreateBackupRequest) *backuppb.BackupInfoResponse {
	if request.GetRequestId() == "" {
		request.RequestId = uuid.NewString()
	}

	resp := &backuppb.BackupInfoResponse{RequestId: request.GetRequestId()}

	if err := backup.ValidateName(request.GetBackupName()); err != nil {
		resp.Code = backuppb.ResponseCode_Parameter_Error
		resp.Msg = err.Error()
		return resp
	}

	uc, err := s.config.newCreateBackup(ctx, s.params)
	if err != nil {
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = err.Error()
		return resp
	}

	req, err := s.toCreateBackupRequest(request)
	if err != nil {
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = err.Error()
		return resp
	}

	if request.GetAsync() {
		return runCreateBackupAsync(uc, request.GetRequestId(), req)
	}
	return runCreateBackupSync(ctx, uc, request.GetRequestId(), req)
}

// runCreateBackupSync runs the job on the request path. The v1 success
// response carries only code and msg: the view the usecase returns is
// deliberately not rendered, because the historical handler computed the
// payload and then dropped it on the floor, and the wire behavior is kept.
func runCreateBackupSync(ctx context.Context, uc createBackupUC, requestID string, req app.CreateBackupRequest) *backuppb.BackupInfoResponse {
	if _, err := uc.Execute(ctx, req); err != nil {
		return &backuppb.BackupInfoResponse{
			RequestId: requestID,
			Code:      backuppb.ResponseCode_Fail,
			Msg:       err.Error(),
		}
	}

	return &backuppb.BackupInfoResponse{Code: backuppb.ResponseCode_Success, Msg: "success"}
}

// runCreateBackupAsync starts the job and returns immediately; running it in
// the background is this server's deployment concern, the flag only selects it.
func runCreateBackupAsync(uc createBackupUC, requestID string, req app.CreateBackupRequest) *backuppb.BackupInfoResponse {
	resp := &backuppb.BackupInfoResponse{RequestId: requestID}

	job, err := uc.Start(req)
	if err != nil {
		resp.Code = backuppb.ResponseCode_Fail
		resp.Msg = err.Error()
		return resp
	}

	go func() {
		if err := job.Run(context.Background()); err != nil {
			log.Error("create backup task execute fail", zap.String("backupId", requestID), zap.Error(err))
		}
	}()

	resp.Code = backuppb.ResponseCode_Success
	resp.Msg = "create backup is executing asynchronously"
	return resp
}

// toCreateBackupRequest renders the v1 pb request into the usecase request.
// The deprecated pb fields (db_collections, collection_names, force,
// meta_only) keep their old meaning here: accepting or rejecting them is a
// transport decision.
func (s *Server) toCreateBackupRequest(request *backuppb.CreateBackupRequest) (app.CreateBackupRequest, error) {
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

	manageAddr := ""
	if request.GetGcPauseAddress() != "" {
		manageAddr = request.GetGcPauseAddress()
	}

	rootPath := ""
	if request.GetBackupRootPath() != "" {
		rootPath = request.GetBackupRootPath()
		log.Info("use backup root from request", zap.String("backup_root", rootPath))
	}

	return app.CreateBackupRequest{
		TaskID:   request.GetRequestId(),
		RootPath: rootPath,
		Option: backup.Option{
			BackupName:       request.GetBackupName(),
			PauseGC:          request.GetGcPauseEnable() || s.params.Backup.PauseGC.Val,
			ManageAddr:       manageAddr,
			Strategy:         strategy,
			Format:           format,
			BackupRBAC:       request.GetRbac(),
			BackupIndexExtra: request.GetWithIndexExtra(),
			Filter:           f,
		},
	}, nil
}

func toFilter(request *backuppb.CreateBackupRequest) (filter.Filter, error) {
	if request.GetFilter() != nil {
		return pbFilterToFilter(request.GetFilter())
	}

	dbCollectionsStr := utils.GetDBCollections(request.GetDbCollections()) //nolint:staticcheck // SA1019: deprecated field for backward compatibility
	if len(dbCollectionsStr) > 0 {
		return dbCollectionsToFilter(dbCollectionsStr)
	}

	if len(request.GetCollectionNames()) > 0 { //nolint:staticcheck // SA1019: deprecated field for backward compatibility
		return collectionNamesToFilter(request.GetCollectionNames()) //nolint:staticcheck // SA1019: deprecated field for backward compatibility
	}

	return filter.Filter{}, nil
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

func pbFilterToFilter(pbFilter map[string]*backuppb.CollFilter) (filter.Filter, error) {
	f, err := filter.FromPB(pbFilter)
	if err != nil {
		return filter.Filter{}, fmt.Errorf("server: build filter from pb: %w", err)
	}

	return f, nil
}

func toStrategy(request *backuppb.CreateBackupRequest) (backup.Strategy, error) {
	if request.GetStrategy() != "" {
		return backup.ParseStrategy(request.GetStrategy())
	}

	if request.GetForce() { //nolint:staticcheck // SA1019: deprecated field for backward compatibility
		log.Warn("force option is deprecated, pls use strategy=skip_flush instead")
		return backup.StrategySkipFlush, nil
	}

	if request.GetMetaOnly() { //nolint:staticcheck // SA1019: deprecated field for backward compatibility
		log.Warn("meta_only option is deprecated, pls use strategy=meta_only instead")
		return backup.StrategyMetaOnly, nil
	}

	return backup.StrategyAuto, nil
}

func toFormat(request *backuppb.CreateBackupRequest) (backup.Format, error) {
	if request.GetFormat() != "" {
		return backup.ParseFormat(request.GetFormat())
	}

	return backup.FormatAuto, nil
}
