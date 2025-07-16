package restore

import (
	"encoding/json"
	"fmt"
	"regexp"

	"github.com/google/uuid"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/meta"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/utils"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/namespace"
)

// collMapper is the interface for renaming collection.
type collMapper interface {
	// renameNS renames the given namespace (database and collection) according to the renaming rules.
	tagetNS(ns namespace.NS) []namespace.NS
}

var _ collMapper = (*defaultCollMapper)(nil)

// defaultCollMapper is the default collMapper that returns the original namespace.
type defaultCollMapper struct{}

func newDefaultCollMapper() *defaultCollMapper                      { return &defaultCollMapper{} }
func (r *defaultCollMapper) tagetNS(ns namespace.NS) []namespace.NS { return []namespace.NS{ns} }

var _ collMapper = (*suffixMapper)(nil)

// suffixMapper mapping the collection by adding a suffix.
type suffixMapper struct {
	suffix string
}

func newSuffixMapper(suffix string) *suffixMapper {
	return &suffixMapper{suffix: suffix}
}

func (s *suffixMapper) tagetNS(ns namespace.NS) []namespace.NS {
	return []namespace.NS{namespace.New(ns.DBName(), ns.CollName()+s.suffix)}
}

// tableMapper generates target namespace from source namespace by lookup a mapping table.
type tableMapper struct {
	dbWildcard map[string]string         // dbName -> newDbName, from db1.*:db2.*
	nsMapping  map[string][]namespace.NS // dbName.collName -> newCollName, from db1.coll1:db2.coll2 and coll1:coll2
}

func (r *tableMapper) tagetNS(ns namespace.NS) []namespace.NS {
	if newNSes, ok := r.nsMapping[ns.String()]; ok {
		return newNSes
	}

	if newDBName, ok := r.dbWildcard[ns.DBName()]; ok {
		return []namespace.NS{namespace.New(newDBName, ns.CollName())}
	}

	return []namespace.NS{ns}
}

// rename map format: key: oldName, value: newName
// rule 1. key: db1.* value: db2.*
// rule 2. key: db1.coll1 value: db2.coll2
// rule 3. key: coll1 value: coll2 , under default db
// rule 4. key: db1. value: db2.

var (
	_rule1Regex = regexp.MustCompile(`^(\w+)\.\*$`)
	_rule2Regex = regexp.MustCompile(`^(\w+)\.(\w+)$`)
	_rule3Regex = regexp.MustCompile(`^(\w+)$`)
	_rule4Regex = regexp.MustCompile(`^(\w+)\.$`)
)

func inferRuleType(k, v string) (int, error) {
	if _rule1Regex.MatchString(k) && _rule1Regex.MatchString(v) {
		return 1, nil
	}

	if _rule2Regex.MatchString(k) && _rule2Regex.MatchString(v) {
		return 2, nil
	}

	if _rule3Regex.MatchString(k) && _rule3Regex.MatchString(v) {
		return 3, nil
	}

	if _rule4Regex.MatchString(k) && _rule4Regex.MatchString(v) {
		return 4, nil
	}

	return 0, fmt.Errorf("restore: invalid rename rule: %s -> %s", k, v)
}

// newRenameGenerator creates a new mapRenamer with the given rename map.
func newTableMapperFromCollRename(collRename map[string]string) (*tableMapper, error) {
	// add default db in collection_renames if not set
	nsMapping := make(map[string][]namespace.NS)
	dbWildcard := make(map[string]string)

	for k, v := range collRename {
		rule, err := inferRuleType(k, v)
		if err != nil {
			return nil, err
		}

		switch rule {
		case 1:
			dbWildcard[k[:len(k)-2]] = v[:len(v)-2]
		case 2, 3:
			oldNS, err := namespace.Parse(k)
			if err != nil {
				return nil, fmt.Errorf("restore: parse namespace %s %w", k, err)
			}
			newNS, err := namespace.Parse(v)
			if err != nil {
				return nil, fmt.Errorf("restore: parse namespace %s %w", v, err)
			}

			nsMapping[oldNS.String()] = append(nsMapping[oldNS.String()], newNS)
		case 4:
			// handle in db mapping
			continue
		}
	}

	return &tableMapper{dbWildcard: dbWildcard, nsMapping: nsMapping}, nil
}

func newCollMapperFromPlan(plan *backuppb.RestorePlan) (collMapper, error) {
	nsMapping := make(map[string][]namespace.NS)
	for _, mapping := range plan.Mapping {
		if mapping.GetSource() == "" {
			return nil, fmt.Errorf("restore: source database name is empty")
		}

		if mapping.GetTarget() == "" {
			return nil, fmt.Errorf("restore: target database name is empty")
		}

		for _, collMapping := range mapping.Colls {
			oldNS := namespace.New(mapping.GetSource(), collMapping.GetSource())
			newNS := namespace.New(mapping.GetTarget(), collMapping.GetTarget())
			nsMapping[oldNS.String()] = append(nsMapping[oldNS.String()], newNS)
		}
	}

	return &tableMapper{nsMapping: nsMapping}, nil
}

func newCollMapper(request *backuppb.RestoreBackupRequest) (collMapper, error) {
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
		mapper := newSuffixMapper(request.GetCollectionSuffix())
		return mapper, nil
	}

	return newDefaultCollMapper(), nil
}

func newDBMapper(plan *backuppb.RestorePlan) (map[string][]dbMapping, error) {
	if plan == nil {
		return nil, nil
	}

	dbMapper := make(map[string][]dbMapping)
	for _, mapping := range plan.Mapping {
		if mapping.GetSource() == "" {
			return nil, fmt.Errorf("restore: source database name is empty")
		}

		if mapping.GetTarget() == "" {
			return nil, fmt.Errorf("restore: target database name is empty")
		}

		mapper := dbMapping{Target: mapping.GetTarget(), WithProp: mapping.GetWithProp()}
		dbMapper[mapping.GetSource()] = append(dbMapper[mapping.GetSource()], mapper)
	}

	return dbMapper, nil
}

func newDBFilterFromDBCollections(dbCollections string) (map[string]struct{}, error) {
	dbColl := meta.DbCollections{}
	if err := json.Unmarshal([]byte(dbCollections), &dbColl); err != nil {
		return nil, fmt.Errorf("restore: unmarshal dbCollections: %w", err)
	}

	dbFilter := make(map[string]struct{}, len(dbColl))
	for dbName := range dbColl {
		if dbName == "" {
			dbName = namespace.DefaultDBName
		}
		dbFilter[dbName] = struct{}{}
	}

	return dbFilter, nil
}

func newDBBackupFilter(request *backuppb.RestoreBackupRequest) (map[string]struct{}, error) {
	// from db collection
	dbCollectionsStr := utils.GetDBCollections(request.GetDbCollections())
	if dbCollectionsStr != "" {
		return newDBFilterFromDBCollections(dbCollectionsStr)
	}

	// from collection names
	if len(request.GetCollectionNames()) != 0 {
		dbFilter := make(map[string]struct{}, len(request.GetCollectionNames()))
		for _, ns := range request.GetCollectionNames() {
			dbName, err := namespace.Parse(ns)
			if err != nil {
				return nil, fmt.Errorf("restore: parse namespace %s: %w", ns, err)
			}
			dbFilter[dbName.DBName()] = struct{}{}
		}

		return dbFilter, nil
	}

	return nil, nil
}

func newCollFilterFromDBCollections(dbCollections string) (map[string]collFilter, error) {
	dbColls := meta.DbCollections{}
	if err := json.Unmarshal([]byte(dbCollections), &dbColls); err != nil {
		return nil, fmt.Errorf("restore: unmarshal dbCollections: %w", err)
	}

	collBackupFilter := make(map[string]collFilter, len(dbColls))
	for dbName, colls := range dbColls {
		if dbName == "" {
			dbName = namespace.DefaultDBName
		}

		if len(colls) == 0 {
			collBackupFilter[dbName] = collFilter{AllowAll: true}
		} else {
			collName := make(map[string]struct{}, len(colls))
			for _, coll := range colls {
				collName[coll] = struct{}{}
			}

			collBackupFilter[dbName] = collFilter{CollName: collName}
		}
	}

	return collBackupFilter, nil
}

func newCollFilterFromCollectionNames(collectionNames []string) (map[string]collFilter, error) {
	collBackupFilter := make(map[string]collFilter)
	for _, ns := range collectionNames {
		dbName, err := namespace.Parse(ns)
		if err != nil {
			return nil, fmt.Errorf("restore: parse namespace %s: %w", ns, err)
		}
		filter, ok := collBackupFilter[dbName.DBName()]
		if !ok {
			filter = collFilter{CollName: make(map[string]struct{})}
			collBackupFilter[dbName.DBName()] = filter
		}
		filter.CollName[dbName.CollName()] = struct{}{}
	}

	return collBackupFilter, nil
}

func newCollBackupFilter(request *backuppb.RestoreBackupRequest) (map[string]collFilter, error) {
	// from db collection
	dbCollectionsStr := utils.GetDBCollections(request.GetDbCollections())
	if dbCollectionsStr != "" {
		return newCollFilterFromDBCollections(dbCollectionsStr)
	}

	// from collection names
	if len(request.GetCollectionNames()) != 0 {
		return newCollFilterFromCollectionNames(request.GetCollectionNames())
	}

	return nil, nil
}

func newDBTaskFilterFromPlan(plan *backuppb.RestorePlan) (map[string]struct{}, error) {
	dbTaskFilter := make(map[string]struct{})
	for dbName := range plan.GetFilter() {
		dbTaskFilter[dbName] = struct{}{}
	}

	return dbTaskFilter, nil
}

func newDBTaskFilter(request *backuppb.RestoreBackupRequest) (map[string]struct{}, error) {
	// from restore plan
	if request.GetRestorePlan() != nil {
		return newDBTaskFilterFromPlan(request.GetRestorePlan())
	}

	// from db collection
	dbCollectionsStr := utils.GetDBCollections(request.GetDbCollectionsAfterRename())
	if dbCollectionsStr != "" {
		return newDBFilterFromDBCollections(dbCollectionsStr)
	}

	return nil, nil
}

func newCollTaskFilterFromPlan(plan *backuppb.RestorePlan) map[string]collFilter {
	collTaskFilter := make(map[string]collFilter)
	for dbName, filter := range plan.GetFilter() {
		if len(filter.GetColls()) == 1 && filter.GetColls()[0] == "*" {
			collTaskFilter[dbName] = collFilter{AllowAll: true}
			continue
		}

		collTaskFilter[dbName] = collFilter{CollName: make(map[string]struct{}, len(filter.GetColls()))}
		for _, coll := range filter.GetColls() {
			collTaskFilter[dbName].CollName[coll] = struct{}{}
		}
	}
	return collTaskFilter
}

func newCollTaskFilterFromDBCollections(dbCollections string) (map[string]collFilter, error) {
	dbColl := meta.DbCollections{}
	if err := json.Unmarshal([]byte(dbCollections), &dbColl); err != nil {
		return nil, fmt.Errorf("restore: unmarshal dbCollections: %w", err)
	}

	collTaskFilter := make(map[string]collFilter, len(dbColl))
	for dbName, colls := range dbColl {
		if dbName == "" {
			dbName = namespace.DefaultDBName
		}

		if len(colls) == 0 {
			collTaskFilter[dbName] = collFilter{AllowAll: true}
		} else {
			filter := collFilter{CollName: make(map[string]struct{}, len(colls))}
			for _, coll := range colls {
				filter.CollName[coll] = struct{}{}
			}
			collTaskFilter[dbName] = filter
		}
	}

	return collTaskFilter, nil
}

func newCollTaskFilter(request *backuppb.RestoreBackupRequest) (map[string]collFilter, error) {
	// from restore plan
	if request.GetRestorePlan() != nil {
		return newCollTaskFilterFromPlan(request.GetRestorePlan()), nil
	}

	// from db collection
	dbCollectionsStr := utils.GetDBCollections(request.GetDbCollectionsAfterRename())
	if dbCollectionsStr != "" {
		return newCollTaskFilterFromDBCollections(dbCollectionsStr)
	}

	return nil, nil
}

type dbMapping struct {
	Target   string
	WithProp bool
}

type collFilter struct {
	AllowAll bool
	CollName map[string]struct{}
}

// planner converts RestorePlan to restore tasks.
type planner struct {
	// There is no need to do filtering before mapping.
	// It is mainly for backward compatibility and can be
	// removed after the dbCollection parameter is completely deprecated.
	dbBackupFilter   map[string]struct{}
	collBackupFilter map[string]collFilter

	// mapping
	dbMapper   map[string][]dbMapping
	collMapper collMapper

	// filter after mapping
	dbTaskFilter map[string]struct{}
	// dbName -> collFilter
	collTaskFilter map[string]collFilter

	request *backuppb.RestoreBackupRequest

	logger *zap.Logger
}

func newPlanner(request *backuppb.RestoreBackupRequest) (*planner, error) {
	logger := log.L().With(zap.String("backup_name", request.GetBackupName()))

	dbBackupFilter, err := newDBBackupFilter(request)
	if err != nil {
		return nil, fmt.Errorf("restore: create db backup filter %w", err)
	}
	collBackupFilter, err := newCollBackupFilter(request)
	if err != nil {
		return nil, fmt.Errorf("restore: create collection backup filter %w", err)
	}

	dbMapper, err := newDBMapper(request.GetRestorePlan())
	if err != nil {
		return nil, fmt.Errorf("restore: create db mapping %w", err)
	}
	collMappers, err := newCollMapper(request)
	if err != nil {
		return nil, fmt.Errorf("restore: create collection mapper %w", err)
	}

	dbTaskFilter, err := newDBTaskFilter(request)
	if err != nil {
		return nil, fmt.Errorf("restore: create db task filter %w", err)
	}
	collTaskFilter, err := newCollTaskFilter(request)
	if err != nil {
		return nil, fmt.Errorf("restore: create collection task filter %w", err)
	}

	return &planner{
		dbBackupFilter:   dbBackupFilter,
		collBackupFilter: collBackupFilter,

		dbMapper:   dbMapper,
		collMapper: collMappers,

		dbTaskFilter:   dbTaskFilter,
		collTaskFilter: collTaskFilter,

		request: request,

		logger: logger,
	}, nil
}

func (p *planner) Plan(backup *backuppb.BackupInfo) ([]*backuppb.RestoreDatabaseTask, []*backuppb.RestoreCollectionTask) {
	dbNames := lo.Map(backup.GetDatabaseBackups(), func(db *backuppb.DatabaseBackupInfo, _ int) string { return db.GetDbName() })
	p.logger.Info("databases in backup", zap.Strings("db_names", dbNames))
	collNSs := lo.Map(backup.GetCollectionBackups(), func(coll *backuppb.CollectionBackupInfo, _ int) string {
		return namespace.New(coll.GetDbName(), coll.GetCollectionName()).String()
	})
	p.logger.Info("collections in backup", zap.Strings("coll_names", collNSs))

	// filter backup
	dbBackup := p.filterDBBackup(backup.GetDatabaseBackups())
	collBackup := p.filterCollBackup(backup.GetCollectionBackups())
	dbNames = lo.Map(dbBackup, func(db *backuppb.DatabaseBackupInfo, _ int) string { return db.GetDbName() })
	p.logger.Info("databases backup after filtering", zap.Strings("db_names", dbNames))
	collNSs = lo.Map(collBackup, func(coll *backuppb.CollectionBackupInfo, _ int) string {
		return namespace.New(coll.GetDbName(), coll.GetCollectionName()).String()
	})
	p.logger.Info("collections backup after filtering", zap.Strings("coll_names", collNSs))

	// generate restore tasks
	dbTasks := p.newDBTaskPBs(dbBackup)
	collTasks := p.newCollTaskPBs(collBackup)
	dbNames = lo.Map(dbTasks, func(db *backuppb.RestoreDatabaseTask, _ int) string { return db.GetTargetDbName() })
	p.logger.Info("databases task after mapping", zap.Strings("db_names", dbNames))
	collNSs = lo.Map(collTasks, func(coll *backuppb.RestoreCollectionTask, _ int) string {
		return namespace.New(coll.GetTargetDbName(), coll.GetTargetCollectionName()).String()
	})
	p.logger.Info("collections task after mapping", zap.Strings("coll_names", collNSs))

	// filter task
	dbTasks = p.filterDBTask(dbTasks)
	collTasks = p.filterCollTask(collTasks)
	dbNames = lo.Map(dbTasks, func(db *backuppb.RestoreDatabaseTask, _ int) string { return db.GetTargetDbName() })
	p.logger.Info("databases task after filtering", zap.Strings("db_names", dbNames))
	collNSs = lo.Map(collTasks, func(coll *backuppb.RestoreCollectionTask, _ int) string {
		return namespace.New(coll.GetTargetDbName(), coll.GetTargetCollectionName()).String()
	})
	p.logger.Info("collections task after filtering", zap.Strings("coll_names", collNSs))

	return dbTasks, collTasks
}

func (p *planner) filterDBBackup(dbBackups []*backuppb.DatabaseBackupInfo) []*backuppb.DatabaseBackupInfo {
	if len(p.dbBackupFilter) == 0 {
		return dbBackups
	}

	return lo.Filter(dbBackups, func(dbBackup *backuppb.DatabaseBackupInfo, _ int) bool {
		_, ok := p.dbBackupFilter[dbBackup.GetDbName()]
		return ok
	})
}

func (p *planner) filterCollBackup(collBackups []*backuppb.CollectionBackupInfo) []*backuppb.CollectionBackupInfo {
	if len(p.collBackupFilter) == 0 {
		return collBackups
	}

	return lo.Filter(collBackups, func(collBackup *backuppb.CollectionBackupInfo, _ int) bool {
		filter, ok := p.collBackupFilter[collBackup.GetDbName()]
		if !ok {
			return false
		}

		if filter.AllowAll {
			return true
		}

		_, ok = filter.CollName[collBackup.GetCollectionName()]
		return ok
	})
}

func (p *planner) newDBTaskPB(dbBak *backuppb.DatabaseBackupInfo) []*backuppb.RestoreDatabaseTask {
	mappings, ok := p.dbMapper[dbBak.GetDbName()]
	if !ok {
		p.logger.Debug("no mapping for database, restore directly", zap.String("db_name", dbBak.GetDbName()))
		task := &backuppb.RestoreDatabaseTask{Id: uuid.NewString(), DbBackup: dbBak, TargetDbName: dbBak.GetDbName()}
		return []*backuppb.RestoreDatabaseTask{task}
	}

	tasks := make([]*backuppb.RestoreDatabaseTask, 0, len(mappings))
	for _, mapping := range mappings {
		p.logger.Debug("generate restore database task", zap.String("source", dbBak.GetDbName()), zap.String("target", mapping.Target))
		task := &backuppb.RestoreDatabaseTask{
			Id:           uuid.NewString(),
			DbBackup:     dbBak,
			TargetDbName: mapping.Target,
			WithProp:     mapping.WithProp,
		}
		tasks = append(tasks, task)
	}

	return tasks
}

func (p *planner) newDBTaskPBs(dbBackups []*backuppb.DatabaseBackupInfo) []*backuppb.RestoreDatabaseTask {
	var dbTasks []*backuppb.RestoreDatabaseTask
	for _, dbBackup := range dbBackups {
		tasks := p.newDBTaskPB(dbBackup)
		dbTasks = append(dbTasks, tasks...)
	}

	return dbTasks
}

func (p *planner) newCollTaskPB(collBak *backuppb.CollectionBackupInfo) []*backuppb.RestoreCollectionTask {
	sourceNS := namespace.New(collBak.GetDbName(), collBak.GetCollectionName())
	targetNSes := p.collMapper.tagetNS(sourceNS)
	size := lo.SumBy(collBak.GetPartitionBackups(), func(partition *backuppb.PartitionBackupInfo) int64 { return partition.GetSize() })

	tasks := make([]*backuppb.RestoreCollectionTask, 0, len(targetNSes))
	for _, targetNS := range targetNSes {
		p.logger.Debug("generate restore collection task", zap.String("source", sourceNS.String()), zap.String("target", targetNS.String()))

		collTaskPB := &backuppb.RestoreCollectionTask{
			Id:                   uuid.NewString(),
			CollBackup:           collBak,
			TargetDbName:         targetNS.DBName(),
			TargetCollectionName: targetNS.CollName(),
			ToRestoreSize:        size,
			MetaOnly:             p.request.GetMetaOnly(),
			RestoreIndex:         p.request.GetRestoreIndex(),
			UseAutoIndex:         p.request.GetUseAutoIndex(),
			DropExistCollection:  p.request.GetDropExistCollection(),
			DropExistIndex:       p.request.GetDropExistIndex(),
			SkipCreateCollection: p.request.GetSkipCreateCollection(),
			MaxShardNum:          p.request.GetMaxShardNum(),
			SkipParams:           p.request.GetSkipParams(),
			UseV2Restore:         p.request.GetUseV2Restore(),
			TruncateBinlogByTs:   p.request.GetTruncateBinlogByTs(),
		}

		tasks = append(tasks, collTaskPB)
	}

	return tasks
}

func (p *planner) newCollTaskPBs(collBackups []*backuppb.CollectionBackupInfo) []*backuppb.RestoreCollectionTask {
	var dbNameCollNameTask []*backuppb.RestoreCollectionTask
	for _, collBackup := range collBackups {
		tasks := p.newCollTaskPB(collBackup)
		dbNameCollNameTask = append(dbNameCollNameTask, tasks...)
	}

	return dbNameCollNameTask
}

func (p *planner) filterDBTask(dbTask []*backuppb.RestoreDatabaseTask) []*backuppb.RestoreDatabaseTask {
	if len(p.dbTaskFilter) == 0 {
		return dbTask
	}

	return lo.Filter(dbTask, func(task *backuppb.RestoreDatabaseTask, _ int) bool {
		_, ok := p.dbTaskFilter[task.GetTargetDbName()]
		return ok
	})
}

func (p *planner) filterCollTask(collTask []*backuppb.RestoreCollectionTask) []*backuppb.RestoreCollectionTask {
	if len(p.collTaskFilter) == 0 {
		return collTask
	}

	return lo.Filter(collTask, func(task *backuppb.RestoreCollectionTask, _ int) bool {
		filter, ok := p.collTaskFilter[task.GetTargetDbName()]
		if !ok {
			return false
		}

		if filter.AllowAll {
			return true
		}

		_, ok = filter.CollName[task.GetTargetCollectionName()]
		return ok
	})
}
