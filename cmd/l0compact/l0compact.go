package l0compact

import (
	"context"
	"errors"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/zilliztech/milvus-backup/cmd/root"
	"github.com/zilliztech/milvus-backup/core/backup"
	corel0 "github.com/zilliztech/milvus-backup/core/l0compact"
	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
)

type options struct {
	name   string
	output string
	force  bool
}

func (o *options) validate() error {
	if o.name == "" {
		return errors.New("backup name is required (--name)")
	}
	// Validate like `create` does: rejects blanks, special characters, and names
	// not starting with a letter/underscore — so `/`, `.`, `..` can't escape the
	// backup root via path.Join.
	if err := backup.ValidateName(o.name); err != nil {
		return fmt.Errorf("invalid --name %q: %w", o.name, err)
	}
	if o.output == "" {
		o.output = o.name + "_l0compacted"
	}
	if o.output == o.name {
		return errors.New("output must differ from source backup name")
	}
	if err := backup.ValidateName(o.output); err != nil {
		return fmt.Errorf("invalid --output %q: %w", o.output, err)
	}
	return nil
}

func (o *options) run(cmd *cobra.Command, params *cfg.Config) error {
	ctx := context.Background()
	cli, err := storage.NewBackupStorage(ctx, &params.Minio)
	if err != nil {
		return fmt.Errorf("l0compact: create storage: %w", err)
	}
	srcDir := mpath.BackupDir(params.Minio.BackupRootPath.Val, o.name)
	dstDir := mpath.BackupDir(params.Minio.BackupRootPath.Val, o.output)
	// Destination handling (reject-if-exists, or clear when --force) lives in the
	// task so it is applied uniformly.
	task := corel0.NewTask(cli, srcDir, dstDir, corel0.WithForce(o.force))
	if err := task.Execute(ctx); err != nil {
		return err
	}
	cmd.Printf("l0compact done: %s -> %s\n", o.name, o.output)
	return nil
}

func NewCmd(opt *root.Options) *cobra.Command {
	var o options
	cmd := &cobra.Command{
		Use:   "l0compact",
		Short: "fold a backup's L0 (delete-only) segments into per-segment deltalogs so it restores without L0 import",
		Long: "Fold a backup's L0 (delete-only) segments into per-segment deltalogs so it restores without L0 import.\n\n" +
			"The output is a complete, standalone backup: every insert_log and delta_log object from the source is\n" +
			"physically copied into the output backup (the source is left untouched). This roughly doubles the\n" +
			"object-storage space used by the source backup, so ensure the bucket has room before running.",
		RunE: func(cmd *cobra.Command, args []string) error {
			params := opt.InitGlobalVars()
			if err := o.validate(); err != nil {
				return err
			}
			return o.run(cmd, params)
		},
	}
	cmd.Flags().StringVarP(&o.name, "name", "n", "", "name of the backup to l0compact")
	cmd.Flags().StringVarP(&o.output, "output", "o", "", "output backup name (default: <name>_l0compacted)")
	cmd.Flags().BoolVar(&o.force, "force", false, "overwrite the destination backup if it already exists")
	return cmd
}
