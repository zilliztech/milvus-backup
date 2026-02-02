package list

import (
	"context"
	"errors"
	"fmt"

	"github.com/samber/lo"
	"github.com/spf13/cobra"

	"github.com/zilliztech/milvus-backup/cmd/root"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/meta"
	"github.com/zilliztech/milvus-backup/internal/storage"
)

type options struct {
	collectionName string
}

func (o *options) validate() error {
	if o.collectionName != "" {
		return errors.New("collectionName is deprecated")
	}

	return nil
}

func (o *options) addFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&o.collectionName, "collection", "c", "", "[DEPRECATED] only list backups contains a certain collection")
}

func (o *options) run(cmd *cobra.Command, params *cfg.Config) error {
	ctx := context.Background()
	if err := o.validate(); err != nil {
		return err
	}

	backupStorage, err := storage.NewBackupStorage(ctx, &params.Minio)
	if err != nil {
		return fmt.Errorf("cmd: create backup storage %w", err)
	}

	summaries, err := meta.List(ctx, backupStorage, params.Minio.BackupRootPath.Val)
	if err != nil {
		return fmt.Errorf("cmd: list backup %w", err)
	}
	names := lo.Map(summaries, func(summary *backuppb.BackupSummary, _ int) string {
		return summary.GetName()
	})

	cmd.Println(">> Backups:")
	for _, name := range names {
		cmd.Println(name)
	}

	return nil
}

func NewCmd(opt *root.Options) *cobra.Command {
	var o options
	cmd := &cobra.Command{
		Use:   "list",
		Short: "Shows all backup in object storage.",

		Run: func(cmd *cobra.Command, args []string) {
			params := opt.InitGlobalVars()
			err := o.run(cmd, params)
			cobra.CheckErr(err)
		},
	}

	o.addFlags(cmd)

	return cmd
}
