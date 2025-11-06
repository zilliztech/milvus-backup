package create

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/cobra"

	"github.com/zilliztech/milvus-backup/internal/taskmgr"

	"github.com/zilliztech/milvus-backup/cmd/root"
	"github.com/zilliztech/milvus-backup/core/backup"
	"github.com/zilliztech/milvus-backup/core/client/milvus"
	"github.com/zilliztech/milvus-backup/core/meta"
	"github.com/zilliztech/milvus-backup/core/paramtable"
	"github.com/zilliztech/milvus-backup/internal/filter"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/namespace"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
)

type options struct {
	backupName string

	collectionNames string
	databases       string
	dbCollections   string

	filter string

	force    bool
	metaOnly bool
	rbac     bool
}

func (o *options) validate() error {
	if len(o.collectionNames) != 0 {
		log.Warn("collection_names is deprecated, use filter instead !")
	}
	if len(o.databases) != 0 {
		log.Warn("databases is deprecated, use filter instead !")
	}
	if len(o.dbCollections) != 0 {
		log.Warn("database_collections is deprecated, use filter instead !")
	}

	if len(o.backupName) != 0 && backup.ValidateName(o.backupName) != nil {
		return fmt.Errorf("invalid backup name %s", o.backupName)
	}

	return nil
}

func (o *options) complete() error {
	if o.backupName == "" {
		o.backupName = backup.DefaultName(time.Now())
	}

	return nil
}

func (o *options) addFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&o.backupName, "name", "n", "", "backup name, if unset will generate a name automatically")

	cmd.Flags().StringVarP(&o.collectionNames, "colls", "c", "", "[DEPRECATED] Use --filter instead. collectionNames to backup, use ',' to connect multiple collections")
	cmd.Flags().StringVarP(&o.databases, "databases", "d", "", "[DEPRECATED] Use --filter instead. databases to backup")
	cmd.Flags().StringVarP(&o.dbCollections, "database_collections", "a", "", "[DEPRECATED] Use --filter instead. databases and collections")

	cmd.Flags().StringVarP(&o.filter, "filter", "", "", "Specify which collections to backup, if not set, backup all collections. example: db1.coll1,db2.col1")

	cmd.Flags().BoolVarP(&o.force, "force", "f", false, "force backup, will skip flush, should make sure data has been stored into disk when using it")
	cmd.Flags().BoolVarP(&o.metaOnly, "meta_only", "", false, "only backup collection meta instead of data")
	cmd.Flags().BoolVarP(&o.rbac, "rbac", "", false, "whether backup RBAC meta")
}

func (o *options) toFilter() (filter.Filter, error) {
	if o.filter != "" {
		return o.parseFilter(o.filter)
	}

	if o.dbCollections != "" {
		return o.dbCollectionsToFilter()
	}

	if o.collectionNames != "" {
		return o.collectionNamesToFilter()
	}

	if o.databases != "" {
		return o.databasesToFilter()
	}

	return filter.Filter{}, nil
}

func (o *options) parseFilter(filterStr string) (filter.Filter, error) {
	f, err := filter.Parse(filterStr)
	if err != nil {
		return filter.Filter{}, fmt.Errorf("parse filter: %w", err)
	}

	return f, nil
}

func (o *options) collectionNamesToFilter() (filter.Filter, error) {
	nsStrs := strings.Split(o.collectionNames, ",")

	dbCollFilter := make(map[string]filter.CollFilter, len(nsStrs))
	for _, nsStr := range nsStrs {
		ns, err := namespace.Parse(nsStr)
		if err != nil {
			return filter.Filter{}, fmt.Errorf("invalid collection name %s", nsStrs)
		}

		if _, ok := dbCollFilter[ns.DBName()]; !ok {
			dbCollFilter[ns.DBName()] = filter.CollFilter{CollName: make(map[string]struct{})}
		}
		dbCollFilter[ns.DBName()].CollName[ns.CollName()] = struct{}{}
	}

	return filter.Filter{DBCollFilter: dbCollFilter}, nil
}

func (o *options) dbCollectionsToFilter() (filter.Filter, error) {
	dbColls := make(map[string][]string)
	if err := json.Unmarshal([]byte(o.dbCollections), &dbColls); err != nil {
		return filter.Filter{}, fmt.Errorf("unmarshal dbCollections: %w", err)
	}

	dbCollFilter := make(map[string]filter.CollFilter)
	for dbName, colls := range dbColls {
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

func (o *options) databasesToFilter() (filter.Filter, error) {
	splits := strings.Split(o.databases, ",")
	dbCollFilter := make(map[string]filter.CollFilter, len(splits))
	for _, db := range splits {
		dbCollFilter[db] = filter.CollFilter{AllowAll: true}
	}

	return filter.Filter{DBCollFilter: dbCollFilter}, nil
}

func (o *options) toOption(params *paramtable.BackupParams) (backup.Option, error) {
	f, err := o.toFilter()
	if err != nil {
		return backup.Option{}, err
	}

	return backup.Option{
		BackupName: o.backupName,
		PauseGC:    params.BackupCfg.GcPauseEnable,
		SkipFlush:  o.force,
		MetaOnly:   o.metaOnly,
		BackupRBAC: o.rbac,
		Filter:     f,
	}, nil
}

func (o *options) toArgs(params *paramtable.BackupParams) (backup.TaskArgs, error) {
	backupStorage, err := storage.NewBackupStorage(context.Background(), &params.MinioCfg)
	if err != nil {
		return backup.TaskArgs{}, fmt.Errorf("create backup storage: %w", err)
	}
	milvusStorage, err := storage.NewMilvusStorage(context.Background(), &params.MinioCfg)
	if err != nil {
		return backup.TaskArgs{}, fmt.Errorf("create milvus storage: %w", err)
	}

	milvusClient, err := milvus.NewGrpc(&params.MilvusCfg)
	if err != nil {
		return backup.TaskArgs{}, fmt.Errorf("create milvus grpc client: %w", err)
	}
	restfulClient, err := milvus.NewRestful(&params.MilvusCfg)
	if err != nil {
		return backup.TaskArgs{}, fmt.Errorf("create milvus restful client: %w", err)
	}
	manage := milvus.NewManage(params.MilvusCfg.Address)

	backupDir := mpath.BackupDir(params.MinioCfg.BackupRootPath, o.backupName)
	option, err := o.toOption(params)
	if err != nil {
		return backup.TaskArgs{}, err
	}

	return backup.TaskArgs{
		TaskID:        uuid.NewString(),
		MilvusStorage: milvusStorage,
		Option:        option,
		BackupStorage: backupStorage,
		BackupDir:     backupDir,
		Params:        params,
		Grpc:          milvusClient,
		Restful:       restfulClient,
		Manage:        manage,
		Meta:          meta.NewMetaManager(),
		TaskMgr:       taskmgr.DefaultMgr,
	}, nil
}

func (o *options) run(cmd *cobra.Command, params *paramtable.BackupParams) error {
	start := time.Now()

	args, err := o.toArgs(params)
	if err != nil {
		return fmt.Errorf("create: convert to args: %w", err)
	}

	task := backup.NewTask(args)
	if err := task.Execute(context.Background()); err != nil {
		return fmt.Errorf("create: execute task: %w", err)
	}

	cmd.Println("create backup success")
	duration := time.Since(start)
	cmd.Println(fmt.Sprintf("duration:%.2f s", duration.Seconds()))

	return nil
}

func NewCmd(opt *root.Options) *cobra.Command {
	var o options

	cmd := &cobra.Command{
		Use:   "create",
		Short: "create a backup.",

		RunE: func(cmd *cobra.Command, args []string) error {
			params := opt.InitGlobalVars()

			if err := o.complete(); err != nil {
				return err
			}

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
