package get

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/zilliztech/milvus-backup/cmd/root"
	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/meta"
	"github.com/zilliztech/milvus-backup/internal/pbconv"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
)

type options struct {
	backupName string
	detail     bool
}

func (o *options) addFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&o.backupName, "name", "n", "", "get backup with this name")
	cmd.Flags().BoolVarP(&o.detail, "detail", "d", false, "[DEPRECATED] get complete backup info")
}

func (o *options) validate() error {
	if o.backupName == "" {
		return fmt.Errorf("backup name is required")
	}
	return nil
}

func (o *options) run(cmd *cobra.Command, params *cfg.Config) error {
	ctx := context.Background()

	backupStorage, err := storage.NewBackupStorage(ctx, &params.Minio)
	if err != nil {
		return fmt.Errorf("create backup storage: %w", err)
	}

	backupDir := mpath.BackupDir(params.Minio.BackupRootPath.Value(), o.backupName)
	backupInfo, err := meta.Read(ctx, backupStorage, backupDir)
	if err != nil {
		return fmt.Errorf("read backup meta: %w", err)
	}

	metaSize, err := storage.Size(ctx, backupStorage, mpath.MetaDir(backupDir))
	if err != nil {
		return fmt.Errorf("get meta size: %w", err)
	}

	brief := pbconv.NewBackupInfoBrief(nil, backupInfo, metaSize)
	output, err := json.MarshalIndent(brief, "", "    ")
	if err != nil {
		return fmt.Errorf("fail to marshal backup info: %w", err)
	}

	cmd.Println(string(output))

	return nil
}

func NewCmd(opt *root.Options) *cobra.Command {
	var o options
	cmd := &cobra.Command{
		Use:   "get",
		Short: "get subcommand get backup by name.",

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
