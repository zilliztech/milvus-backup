package del

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/zilliztech/milvus-backup/cmd/root"
	"github.com/zilliztech/milvus-backup/core"
	"github.com/zilliztech/milvus-backup/core/paramtable"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

type options struct {
	name string
}

func (o *options) validate() error {
	if o.name == "" {
		return fmt.Errorf("backup name is required")
	}

	return nil
}

func (o *options) addFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&o.name, "name", "n", "", "delete backup with this name")
}

func (o *options) run(cmd *cobra.Command, params *paramtable.BackupParams) error {
	ctx := context.Background()
	backupContext := core.CreateBackupContext(ctx, params)

	resp := backupContext.DeleteBackup(ctx, &backuppb.DeleteBackupRequest{
		BackupName: o.name,
	})

	if resp.GetCode() != backuppb.ResponseCode_Success {
		return fmt.Errorf("delete backup failed: %s", resp.GetMsg())
	}

	cmd.Println(resp.GetMsg())

	return nil
}

func NewCmd(opt *root.Options) *cobra.Command {
	var o options

	cmd := &cobra.Command{
		Use:   "delete",
		Short: "delete backup by name.",

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
