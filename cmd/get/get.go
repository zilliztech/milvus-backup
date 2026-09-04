package get

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/zilliztech/milvus-backup/app"
	"github.com/zilliztech/milvus-backup/cmd/flags"
	"github.com/zilliztech/milvus-backup/cmd/root"
	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
	"github.com/zilliztech/milvus-backup/internal/pbconv"
)

// removedFlags are the get flags dropped in 0.6.
var removedFlags = []flags.Removed{
	{Name: "detail", Shorthand: "d", NoValue: true, Advice: "get always prints the complete backup info"},
}

type options struct {
	backupName string
}

func (o *options) addFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&o.backupName, "name", "n", "", "get backup with this name")

	flags.AddRemoved(cmd, removedFlags)
}

func (o *options) validate() error {
	if o.backupName == "" {
		return fmt.Errorf("backup name is required")
	}
	return nil
}

func (o *options) run(cmd *cobra.Command, params *v2.Config) error {
	ctx := context.Background()

	uc, err := app.NewGetBackup(ctx, params)
	if err != nil {
		return fmt.Errorf("cmd: create get backup usecase %w", err)
	}

	view, err := uc.Execute(ctx, app.GetBackupRequest{Name: o.backupName})
	if err != nil {
		return fmt.Errorf("cmd: get backup %w", err)
	}

	brief := pbconv.NewBackupInfoBrief(view.Task, view.Meta, view.MetaSize)
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
		Short: "get a backup by name",

		RunE: func(cmd *cobra.Command, args []string) error {
			if err := flags.CheckRemoved(cmd, removedFlags); err != nil {
				return err
			}

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
