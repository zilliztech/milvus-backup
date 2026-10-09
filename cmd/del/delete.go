package del

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"github.com/vbauerster/mpb/v8/cwriter"

	"github.com/zilliztech/milvus-backup/app"
	"github.com/zilliztech/milvus-backup/cmd/root"
	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
	"github.com/zilliztech/milvus-backup/internal/progressbar"
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
	// command waits on it. On a terminal it renders progress from job-store
	// snapshots; piped output keeps the old plain behavior, so scripts and CI
	// logs never see bar frames.
	job.Run(context.Background())

	var status jobstate.DeleteStatus
	if cwriter.IsTerminal(int(os.Stdout.Fd())) {
		p := progressbar.Progress()
		status, err = renderDelete(ctx, p, store, taskID, 200*time.Millisecond)
		// mpb buffers intercepted writes and the bar's last frame until the
		// container shuts down, so the Wait must precede any plain output.
		p.Wait()
	} else {
		status, err = job.Wait(ctx)
	}
	if err != nil {
		return fmt.Errorf("cmd: wait delete backup: %w", err)
	}
	if status.State == jobstate.DeleteStateFail {
		return fmt.Errorf("cmd: delete backup: %s", status.ErrorMessage)
	}

	cmd.Println(deleteSummaryLine(status))

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
