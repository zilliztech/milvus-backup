package restore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/cobra"

	"github.com/zilliztech/milvus-backup/cmd/root"
	"github.com/zilliztech/milvus-backup/core/client/milvus"
	"github.com/zilliztech/milvus-backup/core/meta"
	"github.com/zilliztech/milvus-backup/core/paramtable"
	"github.com/zilliztech/milvus-backup/core/restore"
	"github.com/zilliztech/milvus-backup/core/storage"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/namespace"
)

type options struct {
	backupName            string
	collectionNames       string
	renameSuffix          string
	renameCollectionNames string
	databases             string
	databaseCollections   string

	restoreIndex bool
	rebuildIndex bool

	metaOnly             bool
	useAutoIndex         bool
	dropExistCollection  bool
	dropExistIndex       bool
	skipCreateCollection bool
	rbac                 bool
	useV2Restore         bool
}

func (o *options) validate() error {
	// TODO: add more validation
	if o.backupName == "" {
		return errors.New("backup name is required")
	}

	if o.collectionNames != "" && o.databaseCollections != "" {
		return errors.New("collection_names and database_collections cannot be set at the same time")
	}

	if o.collectionNames != "" && o.databases != "" {
		return errors.New("collection_names and databases cannot be set at the same time")
	}

	if o.databaseCollections != "" && o.databases != "" {
		return errors.New("database_collections and databases cannot be set at the same time")
	}

	if o.renameSuffix != "" && o.renameCollectionNames != "" {
		return errors.New("suffix and rename flag cannot be set at the same time")
	}

	if o.dropExistCollection && o.skipCreateCollection {
		return errors.New("drop_exist_collection and skip_create_collection cannot be true at the same time")
	}

	if o.restoreIndex {
		log.Warn("restore_index is deprecated, use rebuild_index instead")
	}

	return nil
}

func (o *options) addFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&o.backupName, "name", "n", "", "backup name to restore")

	cmd.Flags().StringVarP(&o.collectionNames, "collections", "c", "", "collectionNames to restore")
	cmd.Flags().StringVarP(&o.renameSuffix, "suffix", "s", "", "add a suffix to collection name to restore")
	cmd.Flags().StringVarP(&o.renameCollectionNames, "rename", "r", "", "rename collections to new names, format: db1.collection1:db2.collection1_new,db1.collection2:db2.collection2_new")
	cmd.Flags().StringVarP(&o.databases, "databases", "d", "", "databases to restore, if not set, restore all databases")
	cmd.Flags().StringVarP(&o.databaseCollections, "database_collections", "a", "", "databases and collections to restore, json format: {\"db1\":[\"c1\", \"c2\"],\"db2\":[]}")

	cmd.Flags().BoolVarP(&o.restoreIndex, "restore_index", "", false, "[DEPRECATED] Use --rebuild_index instead. restore index info")
	cmd.Flags().BoolVarP(&o.rebuildIndex, "rebuild_index", "", false, "Rebuild index from meta information.")

	cmd.Flags().BoolVarP(&o.metaOnly, "meta_only", "", false, "if true, restore meta only")

	cmd.Flags().BoolVarP(&o.useAutoIndex, "use_auto_index", "", false, "if true, replace vector index with autoindex")
	cmd.Flags().BoolVarP(&o.dropExistCollection, "drop_exist_collection", "", false, "if true, drop existing target collection before create")
	cmd.Flags().BoolVarP(&o.dropExistIndex, "drop_exist_index", "", false, "if true, drop existing index of target collection before create")
	cmd.Flags().BoolVarP(&o.skipCreateCollection, "skip_create_collection", "", false, "if true, will skip collection, use when collection exist, restore index or data")
	cmd.Flags().BoolVarP(&o.rbac, "rbac", "", false, "whether restore RBAC meta")
	cmd.Flags().BoolVarP(&o.useV2Restore, "use_v2_restore", "", false, "if true, use multi-segment merged restore")
}

func (o *options) toOption() *restore.Option {
	rebuildIndex := o.restoreIndex || o.rebuildIndex

	return &restore.Option{
		DropExistIndex:       o.dropExistIndex,
		RebuildIndex:         rebuildIndex,
		UseAutoIndex:         o.useAutoIndex,
		DropExistCollection:  o.dropExistCollection,
		SkipCreateCollection: o.skipCreateCollection,
		MetaOnly:             o.metaOnly,
		UseV2Restore:         o.useV2Restore,
		RestoreRBAC:          o.rbac,
	}
}

func (o *options) toBackupFilter() (map[string]struct{}, map[string]restore.CollFilter, error) {
	if o.collectionNames != "" {
		return o.collectionNamesToFilter()
	}

	if o.databases != "" {
		return o.databasesToFilter()
	}

	if o.databaseCollections != "" {
		return o.dbCollectionsToFilter()
	}

	return nil, nil, nil
}

func (o *options) toCollMapper() (restore.CollMapper, error) {
	if o.renameCollectionNames != "" {
		return o.renameCollectionNamesToMapper()
	}

	if o.renameSuffix != "" {
		return restore.NewSuffixMapper(o.renameSuffix), nil
	}

	return restore.NewDefaultCollMapper(), nil
}

func (o *options) toPlan() (*restore.Plan, error) {
	dbFilter, collFilter, err := o.toBackupFilter()
	if err != nil {
		return nil, err
	}

	collMapper, err := o.toCollMapper()
	if err != nil {
		return nil, err
	}

	return &restore.Plan{
		DBBackupFilter:   dbFilter,
		CollBackupFilter: collFilter,

		// not support db mapping now
		CollMapper: collMapper,

		// not support db task filter now
	}, nil
}

func (o *options) collectionNamesToFilter() (map[string]struct{}, map[string]restore.CollFilter, error) {
	dbFilter := make(map[string]struct{})
	collFilter := make(map[string]restore.CollFilter)

	nsStrs := strings.Split(o.collectionNames, ",")
	for _, nsStr := range nsStrs {
		ns, err := namespace.Parse(nsStr)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid collection name %s", nsStr)
		}

		dbFilter[ns.DBName()] = struct{}{}
		collFilter[ns.DBName()] = restore.CollFilter{CollName: map[string]struct{}{ns.CollName(): {}}}
	}

	return dbFilter, collFilter, nil
}

func (o *options) databasesToFilter() (map[string]struct{}, map[string]restore.CollFilter, error) {
	dbFilter := make(map[string]struct{})
	collFilter := make(map[string]restore.CollFilter)

	splits := strings.Split(o.databases, ",")
	for _, db := range splits {
		dbFilter[db] = struct{}{}
		collFilter[db] = restore.CollFilter{AllowAll: true}
	}

	return dbFilter, collFilter, nil
}

func (o *options) dbCollectionsToFilter() (map[string]struct{}, map[string]restore.CollFilter, error) {
	dbColls := make(map[string][]string)
	if err := json.Unmarshal([]byte(o.databaseCollections), &dbColls); err != nil {
		return nil, nil, fmt.Errorf("unmarshal dbCollections: %w", err)
	}

	dbFilter := make(map[string]struct{})
	collFilter := make(map[string]restore.CollFilter)
	for dbName, colls := range dbColls {
		dbFilter[dbName] = struct{}{}
		if len(colls) == 0 {
			collFilter[dbName] = restore.CollFilter{AllowAll: true}
		} else {
			collName := make(map[string]struct{}, len(colls))
			for _, coll := range colls {
				collName[coll] = struct{}{}
			}
			collFilter[dbName] = restore.CollFilter{CollName: collName}
		}
	}

	return dbFilter, collFilter, nil
}

func (o *options) renameCollectionNamesToMapper() (*restore.TableMapper, error) {
	renames := strings.Split(o.renameCollectionNames, ",")
	renameMap := make(map[string]string)

	for _, rename := range renames {
		if strings.Contains(rename, ":") {
			splits := strings.Split(rename, ":")
			renameMap[splits[0]] = splits[1]
		} else {
			return nil, fmt.Errorf("rename collection format error: %s", rename)
		}
	}

	return newTableMapperFromCollRename(renameMap)
}

func (o *options) toArgs(params *paramtable.BackupParams) (restore.TaskArgs, error) {
	plan, err := o.toPlan()
	if err != nil {
		return restore.TaskArgs{}, err
	}

	backupStorage, err := storage.NewBackupStorage(context.Background(), &params.MinioCfg)
	if err != nil {
		return restore.TaskArgs{}, fmt.Errorf("create backup storage: %w", err)
	}
	milvusStorage, err := storage.NewMilvusStorage(context.Background(), &params.MinioCfg)
	if err != nil {
		return restore.TaskArgs{}, fmt.Errorf("create milvus storage: %w", err)
	}
	milvusClient, err := milvus.NewGrpc(&params.MilvusCfg)
	if err != nil {
		return restore.TaskArgs{}, fmt.Errorf("create milvus grpc client: %w", err)
	}
	restfulClient, err := milvus.NewRestful(&params.MilvusCfg)
	if err != nil {
		return restore.TaskArgs{}, fmt.Errorf("create milvus restful client: %w", err)
	}

	backupDir := meta.BackupDirPath(params.MinioCfg.BackupRootPath, o.backupName)
	exist, err := meta.Exist(context.Background(), backupStorage, backupDir)
	if err != nil {
		return restore.TaskArgs{}, fmt.Errorf("check backup exist: %w", err)
	}
	if !exist {
		return restore.TaskArgs{}, fmt.Errorf("backup %s not found", o.backupName)
	}

	backup, err := meta.Read(context.Background(), backupStorage, backupDir)
	if err != nil {
		return restore.TaskArgs{}, fmt.Errorf("read backup meta: %w", err)
	}

	return restore.TaskArgs{
		TaskID:         uuid.NewString(),
		Backup:         backup,
		Plan:           plan,
		Option:         o.toOption(),
		Params:         params,
		BackupDir:      meta.BackupDirPath(params.MinioCfg.BackupRootPath, o.backupName),
		BackupRootPath: params.MinioCfg.BackupRootPath,
		BackupStorage:  backupStorage,
		MilvusStorage:  milvusStorage,
		Grpc:           milvusClient,
		Restful:        restfulClient,
	}, nil
}

func (o *options) run(cmd *cobra.Command, params *paramtable.BackupParams) error {
	start := time.Now()

	args, err := o.toArgs(params)
	if err != nil {
		return err
	}

	task, err := restore.NewTask(args)
	if err != nil {
		return err
	}
	if err := task.Prepare(context.Background()); err != nil {
		return err
	}
	if err := task.Execute(context.Background()); err != nil {
		return err
	}

	duration := time.Since(start)
	cmd.Println(fmt.Sprintf("duration:%.2f s", duration.Seconds()))

	return nil
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
func newTableMapperFromCollRename(collRename map[string]string) (*restore.TableMapper, error) {
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

	return &restore.TableMapper{DBWildcard: dbWildcard, NSMapping: nsMapping}, nil
}

func NewCmd(opt *root.Options) *cobra.Command {
	var o options
	cmd := &cobra.Command{
		Use:   "restore",
		Short: "restore a backup",
		Long:  "restore a backup",

		RunE: func(cmd *cobra.Command, args []string) error {
			params := opt.InitGlobalVars()

			if err := o.validate(); err != nil {
				return err
			}

			err := o.run(cmd, params)
			cobra.CheckErr(err)

			return nil
		},
	}

	o.addFlags(cmd)

	return cmd
}
