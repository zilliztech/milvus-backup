package create

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/cobra"

	"github.com/zilliztech/milvus-backup/cmd/flags"
	"github.com/zilliztech/milvus-backup/cmd/root"
	"github.com/zilliztech/milvus-backup/core/backup"
	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
	"github.com/zilliztech/milvus-backup/internal/filter"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
	"github.com/zilliztech/milvus-backup/internal/taskmgr"
)

// removedFlags are the create flags dropped in 0.6, after 0.5 accepted them with
// a deprecation warning.
var removedFlags = []flags.Removed{
	{Name: "colls", Shorthand: "c", Advice: "use --filter instead"},
	{Name: "databases", Shorthand: "d", Advice: "use --filter instead"},
	{Name: "database_collections", Shorthand: "a", Advice: "use --filter instead"},
	{Name: "force", Shorthand: "f", NoValue: true, Advice: "use --strategy=skip_flush instead"},
	{Name: "meta_only", NoValue: true, Advice: "use --strategy=meta_only instead"},
}

type options struct {
	backupName string

	filter string

	strategy string

	format string

	backupIndexExtra bool

	rbac bool
}

func (o *options) validate() error {
	if len(o.backupName) != 0 && backup.ValidateName(o.backupName) != nil {
		return fmt.Errorf("invalid backup name %s", o.backupName)
	}

	if _, err := backup.ParseStrategy(o.strategy); err != nil {
		return fmt.Errorf("invalid strategy %s, only support %s", o.strategy, strings.Join(backup.SupportStrategy(), ","))
	}

	if _, err := backup.ParseFormat(o.format); err != nil {
		return fmt.Errorf("invalid format %s, only support %s", o.format, strings.Join(backup.SupportFormat(), ","))
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

	cmd.Flags().StringVarP(&o.filter, "filter", "", "", "specify which collections to backup, if not set, backup all collections. example: db1.coll1,db2.col1")

	cmd.Flags().StringVarP(&o.strategy, "strategy", "", "", "backup strategy, one of [meta_only, skip_flush, bulk_flush, serial_flush], if not set will auto select")

	cmd.Flags().StringVarP(&o.format, "format", "", "", "backup format, one of [auto, binlog, snapshot], if not set will auto select")

	cmd.Flags().BoolVarP(&o.backupIndexExtra, "backup_index_extra", "", false, "whether backup index extra info")

	cmd.Flags().BoolVarP(&o.rbac, "rbac", "", false, "whether backup RBAC meta")

	flags.AddRemoved(cmd, removedFlags)
}

func (o *options) toFilter() (filter.Filter, error) {
	if o.filter == "" {
		return filter.Filter{}, nil
	}

	f, err := filter.Parse(o.filter)
	if err != nil {
		return filter.Filter{}, fmt.Errorf("parse filter: %w", err)
	}

	return f, nil
}

func (o *options) toOption(params *v2.Config) (backup.Option, error) {
	f, err := o.toFilter()
	if err != nil {
		return backup.Option{}, err
	}

	// already validated in validate()
	strategy, err := backup.ParseStrategy(o.strategy)
	if err != nil {
		return backup.Option{}, err
	}

	// already validated in validate()
	format, err := backup.ParseFormat(o.format)
	if err != nil {
		return backup.Option{}, err
	}

	return backup.Option{
		BackupName: o.backupName,
		PauseGC:    params.Backup.PauseGC.Val,

		Strategy: strategy,

		Format: format,

		BackupRBAC: o.rbac,

		BackupIndexExtra: o.backupIndexExtra,

		Filter: f,
	}, nil
}

func (o *options) toArgs(params *v2.Config) (backup.TaskArgs, error) {
	backupStorage, err := storage.NewBackupStorage(context.Background(), params)
	if err != nil {
		return backup.TaskArgs{}, fmt.Errorf("create backup storage: %w", err)
	}
	milvusStorage, err := storage.NewMilvusStorage(context.Background(), params)
	if err != nil {
		return backup.TaskArgs{}, fmt.Errorf("create milvus storage: %w", err)
	}

	backupDir := mpath.BackupDir(params.Backup.Storage.RootPath.Val, o.backupName)
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
		TaskMgr:       taskmgr.DefaultMgr(),
	}, nil
}

func (o *options) run(cmd *cobra.Command, params *v2.Config) error {
	start := time.Now()

	args, err := o.toArgs(params)
	if err != nil {
		return fmt.Errorf("create: convert to args: %w", err)
	}

	task, err := backup.NewTask(args)
	if err != nil {
		return fmt.Errorf("create: new backup task error: %w", err)
	}

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
		Short: "create a backup",

		RunE: func(cmd *cobra.Command, args []string) error {
			if err := flags.CheckRemoved(cmd, removedFlags); err != nil {
				return err
			}

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
