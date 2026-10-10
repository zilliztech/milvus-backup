package migrate

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"github.com/vbauerster/mpb/v8/cwriter"

	"github.com/zilliztech/milvus-backup/cmd/root"
	"github.com/zilliztech/milvus-backup/core/migrate"
	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
	"github.com/zilliztech/milvus-backup/internal/progressbar"
)

type options struct {
	backupName string
	clusterID  string
}

func (o *options) validate() error {
	if o.clusterID == "" {
		return errors.New("cluster id is required")
	}

	if o.backupName == "" {
		return errors.New("backup name is required")
	}

	return nil
}

func (o *options) run(cmd *cobra.Command, params *cfg.Config) error {
	ctx := context.Background()

	store := jobstate.Default()
	taskID := uuid.NewString()
	task, err := migrate.NewTask(taskID, o.backupName, o.clusterID, params)
	if err != nil {
		return err
	}

	if err := task.Prepare(ctx); err != nil {
		return err
	}

	// The CLI exposes no async form: the job runs in the background and the
	// command waits on it. On a terminal it renders upload progress from
	// job-store snapshots; piped output keeps the old plain behavior, so
	// scripts and CI logs never see bar frames.
	done := make(chan error, 1)
	go func() {
		done <- task.Execute(ctx)
	}()

	if cwriter.IsTerminal(int(os.Stdout.Fd())) {
		p := progressbar.Progress()
		err = renderMigrate(ctx, p, store, taskID, 200*time.Millisecond, done)
		// mpb buffers intercepted writes and the bar's last frame until the
		// container shuts down, so the Wait must precede any plain output.
		p.Wait()
	} else {
		err = <-done
	}
	if err != nil {
		return err
	}

	status, err := store.GetMigrateTask(taskID)
	if err != nil {
		return err
	}

	cmd.Printf("Successfully triggered migration with backup name: %s target cluster: %s \n", o.backupName, o.clusterID)
	cmd.Printf("migration job id: %s. \n", status.MigrateJobID)
	cmd.Printf("You can check the progress of the migration job in Zilliz Cloud console.\n")

	return nil
}

func (o *options) addFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&o.backupName, "name", "n", "", "need to migrate backup name")
	cmd.Flags().StringVarP(&o.clusterID, "cluster_id", "c", "", "target cluster id")
}

func NewCmd(opt *root.Options) *cobra.Command {
	var o options

	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "migrate backup data to a Zilliz Cloud cluster",
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
