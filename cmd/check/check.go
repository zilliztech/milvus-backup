package check

import (
	"context"
	"fmt"
	"io"

	"github.com/spf13/cobra"

	"github.com/zilliztech/milvus-backup/cmd/root"
	"github.com/zilliztech/milvus-backup/core/check"
	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/storage"
)

// writeConfig prints a labeled table of the effective configuration to w.
func writeConfig(w io.Writer, c *cfg.Config) error {
	if _, err := io.WriteString(w, "Configuration:\n"); err != nil {
		return fmt.Errorf("check: write config header: %w", err)
	}
	if err := c.WriteTable(w); err != nil {
		return fmt.Errorf("check: write config table: %w", err)
	}
	return nil
}

func NewCmd(opt *root.Options) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "check",
		Short: "check if the connects is right.",

		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			params := opt.InitGlobalVars()

			// Print the effective configuration before constructing any client,
			// so the resolved values and their sources are visible even when
			// connecting to Milvus or the object storage fails.
			cobra.CheckErr(writeConfig(cmd.OutOrStdout(), params))

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

	cmd.AddCommand(newConfigCmd())

	return cmd
}
