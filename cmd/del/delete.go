package del

import (
	"context"
	"errors"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/zilliztech/milvus-backup/cmd/root"
	"github.com/zilliztech/milvus-backup/app"
	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
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

func (o *options) run(cmd *cobra.Command, params *v2.Config) error {
	ctx := context.Background()

	uc, err := app.NewDeleteBackup(ctx, params)
	if err != nil {
		return fmt.Errorf("cmd: create delete backup usecase: %w", err)
	}

	if err := uc.Execute(ctx, o.name); err != nil {
		return fmt.Errorf("cmd: delete backup: %w", err)
	}

	cmd.Println("delete backup done")

	return nil
}

func NewCmd(opt *root.Options) *cobra.Command {
	var o options

	cmd := &cobra.Command{
		Use:   "delete",
		Short: "delete a backup by name",

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
