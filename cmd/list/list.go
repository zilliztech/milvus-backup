package list

import (
	"context"
	"fmt"

	"github.com/samber/lo"
	"github.com/spf13/cobra"

	"github.com/zilliztech/milvus-backup/app"
	"github.com/zilliztech/milvus-backup/cmd/flags"
	"github.com/zilliztech/milvus-backup/cmd/root"
	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
)

// removedFlags are the list flags dropped in 0.6.
var removedFlags = []flags.Removed{
	{Name: "collection", Shorthand: "c", Advice: "listing backups by collection is no longer supported"},
}

func run(cmd *cobra.Command, params *v2.Config) error {
	ctx := context.Background()

	uc, err := app.NewListBackups(ctx, params)
	if err != nil {
		return fmt.Errorf("cmd: create list backups usecase %w", err)
	}

	summaries, err := uc.Execute(ctx)
	if err != nil {
		return fmt.Errorf("cmd: list backups %w", err)
	}
	names := lo.Map(summaries, func(summary app.BackupSummary, _ int) string {
		return summary.Name
	})

	cmd.Println(">> Backups:")
	for _, name := range names {
		cmd.Println(name)
	}

	return nil
}

func NewCmd(opt *root.Options) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "list all backups in object storage",

		RunE: func(cmd *cobra.Command, args []string) error {
			if err := flags.CheckRemoved(cmd, removedFlags); err != nil {
				return err
			}

			params := opt.InitGlobalVars()
			cobra.CheckErr(run(cmd, params))

			return nil
		},
	}

	flags.AddRemoved(cmd, removedFlags)

	return cmd
}
