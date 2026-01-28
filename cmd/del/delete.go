package del

import (
	"context"
	"errors"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/zilliztech/milvus-backup/cmd/root"
	"github.com/zilliztech/milvus-backup/core/del"
	"github.com/zilliztech/milvus-backup/core/paramtable"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
)

type options struct {
	name string
}

func (o *options) validate() error {
	if o.name == "" {
		return errors.New("backup name is required")
	}

	return nil
}

func (o *options) addFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&o.name, "name", "n", "", "delete backup with this name")
}

func (o *options) run(cmd *cobra.Command, params *paramtable.BackupParams) error {
	backupStorage, err := storage.NewBackupStorage(context.Background(), &params.MinioCfg)
	if err != nil {
		return fmt.Errorf("delete: create backup storage: %w", err)
	}

	task := del.NewTask(backupStorage, mpath.BackupDir(params.MinioCfg.BackupRootPath, o.name))
	if err := task.Execute(context.Background()); err != nil {
		return fmt.Errorf("delete: execute task: %w", err)
	}

	cmd.Println("delete backup done")

	return nil
}

func NewCmd(opt *root.Options) *cobra.Command {
	var o options

	cmd := &cobra.Command{
		Use:   "delete",
		Short: "delete backup by name.",

		RunE: func(cmd *cobra.Command, args []string) error {
			params, err := opt.LoadConfig()
			if err != nil {
				return err
			}

			if err := o.validate(); err != nil {
				return err
			}

			return o.run(cmd, params)
		},
	}

	o.addFlags(cmd)

	return cmd
}
