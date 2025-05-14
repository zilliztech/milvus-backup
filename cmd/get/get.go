package get

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/zilliztech/milvus-backup/cmd/root"
	"github.com/zilliztech/milvus-backup/core"
	"github.com/zilliztech/milvus-backup/core/paramtable"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

type options struct {
	backupName string
	detail     bool
}

func (o *options) addFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&o.backupName, "name", "n", "", "get backup with this name")
	cmd.Flags().BoolVarP(&o.detail, "detail", "d", false, "get complete backup info")
}

func (o *options) validate() error {
	if o.backupName == "" {
		return fmt.Errorf("backup name is required")
	}
	return nil
}

func (o *options) run(cmd *cobra.Command, params *paramtable.BackupParams) error {
	ctx := context.Background()
	backupContext := core.CreateBackupContext(ctx, params)
	resp := backupContext.GetBackup(ctx, &backuppb.GetBackupRequest{
		BackupName:    o.backupName,
		WithoutDetail: !o.detail,
	})

	if resp.GetCode() != backuppb.ResponseCode_Success {
		return fmt.Errorf("get backup failed: %s", resp.GetMsg())
	}

	output, err := json.MarshalIndent(resp.GetData(), "", "    ")
	if err != nil {
		return fmt.Errorf("fail to marshal backup info: %w", err)
	}

	cmd.Println(string(output))
	cmd.Println(resp.GetCode())

	return nil
}

func NewCmd(opt *root.Options) *cobra.Command {
	var o options
	cmd := &cobra.Command{
		Use:   "get",
		Short: "get subcommand get backup by name.",

		RunE: func(cmd *cobra.Command, args []string) error {
			var params paramtable.BackupParams
			params.GlobalInitWithYaml(opt.Config)
			params.Init()

			if err := o.validate(); err != nil {
				return err
			}

			err := o.run(cmd, &params)
			cobra.CheckErr(err)

			return nil
		},
	}

	o.addFlags(cmd)

	return cmd
}
