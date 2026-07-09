package wash

import (
	"context"
	"errors"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/zilliztech/milvus-backup/cmd/root"
	"github.com/zilliztech/milvus-backup/core/wash"
	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
)

type options struct {
	name        string
	l0fanoutBin string
}

func (o *options) validate() error {
	if o.name == "" {
		return errors.New("backup name is required")
	}

	return nil
}

func (o *options) addFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&o.name, "name", "n", "", "wash the backup with this name")
	cmd.Flags().StringVar(&o.l0fanoutBin, "l0fanout", "l0fanout",
		"path to the Milvus l0fanout binary (the deltalog codec)")
}

func (o *options) run(cmd *cobra.Command, params *cfg.Config) error {
	backupStorage, err := storage.NewBackupStorage(context.Background(), &params.Minio)
	if err != nil {
		return fmt.Errorf("wash: create backup storage: %w", err)
	}

	backupDir := mpath.BackupDir(params.Minio.BackupRootPath.Val, o.name)
	task := wash.NewTask(backupStorage, backupDir, params, o.l0fanoutBin)
	if err := task.Execute(context.Background()); err != nil {
		return fmt.Errorf("wash: execute task: %w", err)
	}

	cmd.Println("wash backup done")

	return nil
}

func NewCmd(opt *root.Options) *cobra.Command {
	var o options

	cmd := &cobra.Command{
		Use:   "wash",
		Short: "fold a backup's L0 (delete-only) segments into per-segment deltalogs so it restores without L0 import.",

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
