package restore

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/zilliztech/milvus-backup/cmd/root"
	corerestore "github.com/zilliztech/milvus-backup/core/restore"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
)

const resumeCompatibility = "Restore Resume is Alpha (experimental). Its HTTP endpoints and request parameters, CLI commands and flags, configuration keys, and persisted state format may change without backward compatibility."

func newControlCmd(opt *root.Options, cleanup bool) *cobra.Command {
	name := "resume"
	if cleanup {
		name = "cleanup"
	}
	var id string
	summary := name + " a saved restore task (Alpha)"
	cmd := &cobra.Command{Use: name, Short: summary, Long: summary + ".\n\n" + resumeCompatibility, Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		opt.InitGlobalVars()
		store := jobstate.Default()
		if cleanup {
			if err := corerestore.CleanupFailed(cmd.Context(), store, id); err != nil {
				return err
			}
			cmd.Println("restore cleanup completed: " + id)
			return nil
		}
		if err := corerestore.Resume(context.Background(), store, id); err != nil {
			return err
		}
		if err := waitRestoreTerminal(cmd.Context(), store, id); err != nil {
			return err
		}
		cmd.Println("restore completed: " + id)
		return nil
	}}
	cmd.Flags().StringVar(&id, "id", "", "restore task ID")
	_ = cmd.MarkFlagRequired("id")
	return cmd
}

// The CLI owns the executor process, so it must stay alive until the task and
// its terminal persistence have settled. HTTP callers can return after admission.
func waitRestoreTerminal(ctx context.Context, store *jobstate.Store, id string) error {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		status, err := store.GetRestoreTask(id)
		if err != nil {
			return err
		}
		if status.Terminal() {
			if status.Failed() {
				return fmt.Errorf("restore %s: %s", id, status.ErrorMessage)
			}
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
