package restore

import (
	"context"
	"encoding/json"
	"errors"
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
	backupName            string
	collectionNames       string
	renameSuffix          string
	renameCollectionNames string
	databases             string
	databaseCollections   string

	metaOnly             bool
	restoreIndex         bool
	useAutoIndex         bool
	dropExistCollection  bool
	dropExistIndex       bool
	skipCreateCollection bool
	rbac                 bool
	useV2Restore         bool
}

func (o *options) validate() error {
	// TODO: add more validation
	if o.backupName == "" {
		return errors.New("backup name is required")
	}

	if o.renameSuffix != "" && o.renameCollectionNames != "" {
		return errors.New("suffix and rename flag cannot be set at the same time")
	}

	if o.dropExistCollection && o.skipCreateCollection {
		return errors.New("drop_exist_collection and skip_create_collection cannot be true at the same time")
	}

	return nil
}

func (o *options) addFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&o.backupName, "name", "n", "", "backup name to restore")
	cmd.Flags().StringVarP(&o.collectionNames, "collections", "c", "", "collectionNames to restore")
	cmd.Flags().StringVarP(&o.renameSuffix, "suffix", "s", "", "add a suffix to collection name to restore")
	cmd.Flags().StringVarP(&o.renameCollectionNames, "rename", "r", "", "rename collections to new names, format: db1.collection1:db2.collection1_new,db1.collection2:db2.collection2_new")
	cmd.Flags().StringVarP(&o.databases, "databases", "d", "", "databases to restore, if not set, restore all databases")
	cmd.Flags().StringVarP(&o.databaseCollections, "database_collections", "a", "", "databases and collections to restore, json format: {\"db1\":[\"c1\", \"c2\"],\"db2\":[]}")

	cmd.Flags().BoolVarP(&o.metaOnly, "meta_only", "", false, "if true, restore meta only")
	cmd.Flags().BoolVarP(&o.restoreIndex, "restore_index", "", false, "if true, restore index")
	cmd.Flags().BoolVarP(&o.useAutoIndex, "use_auto_index", "", false, "if true, replace vector index with autoindex")
	cmd.Flags().BoolVarP(&o.dropExistCollection, "drop_exist_collection", "", false, "if true, drop existing target collection before create")
	cmd.Flags().BoolVarP(&o.dropExistIndex, "drop_exist_index", "", false, "if true, drop existing index of target collection before create")
	cmd.Flags().BoolVarP(&o.skipCreateCollection, "skip_create_collection", "", false, "if true, will skip collection, use when collection exist, restore index or data")
	cmd.Flags().BoolVarP(&o.rbac, "rbac", "", false, "whether restore RBAC meta")
	cmd.Flags().BoolVarP(&o.useV2Restore, "use_v2_restore", "", false, "if true, use multi-segment merged restore")
}

func (o *options) toRequest() (*backuppb.RestoreBackupRequest, error) {
	var colls []string
	if o.collectionNames != "" {
		colls = strings.Split(o.collectionNames, ",")
	}

	renameMap := make(map[string]string)
	if o.renameCollectionNames != "" {
		renames := strings.Split(o.renameCollectionNames, ",")
		for _, rename := range renames {
			if strings.Contains(rename, ":") {
				splits := strings.Split(rename, ":")
				renameMap[splits[0]] = splits[1]
			} else {
				return nil, fmt.Errorf("rename collection format error: %s", rename)
			}
		}
	}

	if o.databaseCollections == "" && o.databases != "" {
		dbColls := make(map[string][]string)
		splits := strings.Split(o.databases, ",")
		for _, db := range splits {
			dbColls[db] = []string{}
		}

		dbCollsBytes, err := json.Marshal(dbColls)
		if err != nil {
			return nil, fmt.Errorf("marshal dbCollections: %w", err)
		}

		o.databaseCollections = string(dbCollsBytes)
	}

	return &backuppb.RestoreBackupRequest{
		BackupName:           o.backupName,
		CollectionNames:      colls,
		CollectionSuffix:     o.renameSuffix,
		CollectionRenames:    renameMap,
		DbCollections:        utils.WrapDBCollections(o.databaseCollections),
		MetaOnly:             o.metaOnly,
		RestoreIndex:         o.restoreIndex,
		UseAutoIndex:         o.useAutoIndex,
		DropExistCollection:  o.dropExistCollection,
		DropExistIndex:       o.dropExistIndex,
		SkipCreateCollection: o.skipCreateCollection,
		Rbac:                 o.rbac,
		UseV2Restore:         o.useV2Restore,
	}, nil
}

func (o *options) run(cmd *cobra.Command, params *paramtable.BackupParams) error {
	ctx := context.Background()
	backupContext := core.CreateBackupContext(ctx, params)
	req, err := o.toRequest()
	if err != nil {
		return fmt.Errorf("restore: convert to request: %w", err)
	}

	start := time.Now()
	resp := backupContext.RestoreBackup(ctx, req)
	if resp.GetCode() != backuppb.ResponseCode_Success {
		return fmt.Errorf("restore backup failed: %s", resp.GetMsg())
	}

	cmd.Println(resp.GetMsg())
	duration := time.Since(start)
	cmd.Println(fmt.Sprintf("duration:%.2f s", duration.Seconds()))

	return nil
}

func NewCmd(opt *root.Options) *cobra.Command {
	var o options
	cmd := &cobra.Command{
		Use:   "restore",
		Short: "restore a backup",
		Long:  "restore a backup",

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
