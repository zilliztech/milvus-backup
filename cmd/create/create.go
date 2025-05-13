package create

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/zilliztech/milvus-backup/cmd/root"
	"github.com/zilliztech/milvus-backup/core"
	"github.com/zilliztech/milvus-backup/core/paramtable"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/utils"
)

type options struct {
	backupName      string
	collectionNames string
	databases       string
	dbCollections   string
	force           bool
	metaOnly        bool
	rbac            bool
}

func (o *options) validate() error {
	// TODO: add more validation
	return nil
}

func (o *options) addFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&o.backupName, "name", "n", "", "backup name, if unset will generate a name automatically")
	cmd.Flags().StringVarP(&o.collectionNames, "colls", "c", "", "collectionNames to backup, use ',' to connect multiple collections")
	cmd.Flags().StringVarP(&o.databases, "databases", "d", "", "databases to backup")
	cmd.Flags().StringVarP(&o.dbCollections, "database_collections", "a", "", "databases and collections to backup, json format: {\"db1\":[\"c1\", \"c2\"],\"db2\":[]}")
	cmd.Flags().BoolVarP(&o.force, "force", "f", false, "force backup, will skip flush, should make sure data has been stored into disk when using it")
	cmd.Flags().BoolVarP(&o.metaOnly, "meta_only", "", false, "only backup collection meta instead of data")
	cmd.Flags().BoolVarP(&o.rbac, "rbac", "", false, "whether backup RBAC meta")
}

// TODO: Clarify the priority of each option and remove some duplicate options,
// issue: https://github.com/zilliztech/milvus-backup/issues/637.
func (o *options) toRequest() (*backuppb.CreateBackupRequest, error) {
	var collNames []string
	if o.collectionNames != "" {
		collNames = strings.Split(o.collectionNames, ",")
	}

	// convert databases to dbCollections
	if o.dbCollections == "" && o.databases != "" {
		dbColls := make(map[string][]string)
		splits := strings.Split(o.databases, ",")
		for _, db := range splits {
			dbColls[db] = []string{}
		}

		dbCollsBytes, err := json.Marshal(dbColls)
		if err != nil {
			return nil, fmt.Errorf("marshal dbCollections: %w", err)
		}

		o.dbCollections = string(dbCollsBytes)
	}

	return &backuppb.CreateBackupRequest{
		BackupName:      o.backupName,
		CollectionNames: collNames,
		DbCollections:   utils.WrapDBCollections(o.dbCollections),
		Force:           o.force,
		MetaOnly:        o.metaOnly,
		Rbac:            o.rbac,
	}, nil
}

func (o *options) run(cmd *cobra.Command, params *paramtable.BackupParams) error {
	ctx := context.Background()
	backupContext := core.CreateBackupContext(ctx, params)

	start := time.Now()

	req, err := o.toRequest()
	if err != nil {
		return fmt.Errorf("create: convert to request: %w", err)
	}

	resp := backupContext.CreateBackup(ctx, req)
	if resp.GetCode() != backuppb.ResponseCode_Success {
		return fmt.Errorf("create backup failed: %s", resp.GetMsg())
	}

	cmd.Println(resp.GetMsg())
	duration := time.Since(start)
	cmd.Println(fmt.Sprintf("duration:%.2f s", duration.Seconds()))

	return nil
}

func NewCmd(opt *root.Options) *cobra.Command {
	var o options

	cmd := &cobra.Command{
		Use:   "create",
		Short: "create subcommand create a backup.",

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
