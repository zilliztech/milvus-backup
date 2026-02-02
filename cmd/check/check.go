package check

import (
	"context"

	"github.com/spf13/cobra"

	"github.com/zilliztech/milvus-backup/cmd/root"
	"github.com/zilliztech/milvus-backup/core/check"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/storage"
)

func NewCmd(opt *root.Options) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "check",
		Short: "check if the connects is right.",

		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			params := opt.InitGlobalVars()

			grpc, err := milvus.NewGrpc(&params.Milvus)
			cobra.CheckErr(err)

			milvusStorage, err := storage.NewMilvusStorage(ctx, &params.Minio)
			cobra.CheckErr(err)

			backupStorage, err := storage.NewBackupStorage(ctx, &params.Minio)
			cobra.CheckErr(err)

			taskArgs := check.TaskArgs{
				Params:        params,
				Grpc:          grpc,
				MilvusStorage: milvusStorage,
				BackupStorage: backupStorage,
				Output:        cmd.OutOrStdout(),
			}

			task := check.NewTask(taskArgs)
			err = task.Execute(ctx)
			cobra.CheckErr(err)

			return nil
		},
	}

	cmd.AddCommand(newBackupYamlCmd(opt))

	return cmd
}
