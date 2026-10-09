package del

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/spf13/cobra"

	"github.com/zilliztech/milvus-backup/app"
	"github.com/zilliztech/milvus-backup/cmd/root"
	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
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

func (o *options) run(cmd *cobra.Command, params *cfg.Config) error {
	ctx := context.Background()

	store := jobstate.Default()
	taskID := uuid.NewString()
	job, err := app.NewDeleteJob(ctx, params, store, app.DeleteBackupRequest{
		TaskID:     taskID,
		BackupName: o.name,
	})
	if err != nil {
		return fmt.Errorf("cmd: create delete backup job: %w", err)
	}

	// The CLI exposes no async form: the job runs in the background and the
	// command waits on it, reading back the outcome Run recorded.
	job.Run(context.Background())

	status, err := job.Wait(ctx)
	if err != nil {
		return fmt.Errorf("cmd: wait delete backup: %w", err)
	}
	if status.State == jobstate.DeleteStateFail {
		return fmt.Errorf("cmd: delete backup: %s", status.ErrorMessage)
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
