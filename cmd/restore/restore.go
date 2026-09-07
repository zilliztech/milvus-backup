package restore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/cobra"

	"github.com/zilliztech/milvus-backup/app"
	"github.com/zilliztech/milvus-backup/cmd/flags"
	"github.com/zilliztech/milvus-backup/cmd/root"
	"github.com/zilliztech/milvus-backup/core/restore"
	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/filter"
)

// removedFlags are the restore flags dropped in 0.6, after 0.5 accepted them
// with a deprecation warning.
var removedFlags = []flags.Removed{
	{Name: "collections", Shorthand: "c", Advice: "use --filter instead"},
	{Name: "databases", Shorthand: "d", Advice: "use --filter instead"},
	{Name: "database_collections", Shorthand: "a", Advice: "use --filter instead"},
	{Name: "restore_index", NoValue: true, Advice: "use --rebuild_index instead"},
}

type options struct {
	backupName            string
	renameSuffix          string
	renameCollectionNames string

	filter string

	rebuildIndex bool

	metaOnly             bool
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

	cmd.Flags().StringVarP(&o.filter, "filter", "", "", "Specify which collections to restore, if not set, restore all collections in backup. Matched against the name after --suffix or --rename is applied. example: db1.coll1,db2.coll2")

	cmd.Flags().StringVarP(&o.renameSuffix, "suffix", "s", "", "add a suffix to collection name to restore")
	cmd.Flags().StringVarP(&o.renameCollectionNames, "rename", "r", "", "rename collections to new names, format: db1.collection1:db2.collection1_new,db1.collection2:db2.collection2_new")

	cmd.Flags().BoolVarP(&o.rebuildIndex, "rebuild_index", "", false, "Rebuild index from meta information.")

	cmd.Flags().BoolVarP(&o.metaOnly, "meta_only", "", false, "if true, restore meta only")

	cmd.Flags().BoolVarP(&o.useAutoIndex, "use_auto_index", "", false, "if true, replace vector index with autoindex")
	cmd.Flags().BoolVarP(&o.dropExistCollection, "drop_exist_collection", "", false, "if true, drop existing target collection before create")
	cmd.Flags().BoolVarP(&o.dropExistIndex, "drop_exist_index", "", false, "if true, drop existing index of target collection before create")
	cmd.Flags().BoolVarP(&o.skipCreateCollection, "skip_create_collection", "", false, "if true, will skip collection, use when collection exist, restore index or data")
	cmd.Flags().BoolVarP(&o.rbac, "rbac", "", false, "whether restore RBAC meta")
	cmd.Flags().BoolVarP(&o.useV2Restore, "use_v2_restore", "", false, "if true, use multi-segment merged restore")

	flags.AddRemoved(cmd, removedFlags)
}

func (o *options) toOption() *restore.Option {
	return &restore.Option{
		DropExistIndex:       o.dropExistIndex,
		RebuildIndex:         o.rebuildIndex,
		UseAutoIndex:         o.useAutoIndex,
		DropExistCollection:  o.dropExistCollection,
		SkipCreateCollection: o.skipCreateCollection,
		MetaOnly:             o.metaOnly,
		UseV2Restore:         o.useV2Restore,
		RestoreRBAC:          o.rbac,
	}
}

func (o *options) toTaskFilter() (filter.Filter, error) {
	return filter.Parse(o.filter)
}

func (o *options) toCollMapper() (restore.CollMapper, error) {
	if o.renameCollectionNames != "" {
		return o.renameCollectionNamesToMapper()
	}

	if o.renameSuffix != "" {
		return restore.NewSuffixMapper(o.renameSuffix), nil
	}

	return restore.NewDefaultCollMapper(), nil
}

func (o *options) toPlan() (*restore.Plan, error) {
	collMapper, err := o.toCollMapper()
	if err != nil {
		return nil, err
	}

	taskFilter, err := o.toTaskFilter()
	if err != nil {
		return nil, err
	}

	// Plan.BackupFilter is left unset: it exists for the removed database and
	// collection flags, and the HTTP API is the only caller that still has them.
	return &restore.Plan{
		// not support db mapping now
		CollMapper: collMapper,

		TaskFilter: taskFilter,
	}, nil
}

func (o *options) renameCollectionNamesToMapper() (*restore.TableMapper, error) {
	renames := strings.Split(o.renameCollectionNames, ",")
	renameMap := make(map[string]string)

	for _, rename := range renames {
		if strings.Contains(rename, ":") {
			splits := strings.Split(rename, ":")
			renameMap[splits[0]] = splits[1]
		} else {
			return nil, fmt.Errorf("rename collection format error: %s", rename)
		}
	}

	return newTableMapperFromCollRename(renameMap)
}

// toRequest translates the flag grammar into the usecase request: the plan is
// a parse product of flag-only inputs, so it is built here.
func (o *options) toRequest() (app.RestoreRequest, error) {
	plan, err := o.toPlan()
	if err != nil {
		return app.RestoreRequest{}, err
	}

	return app.RestoreRequest{
		TaskID:     uuid.NewString(),
		BackupName: o.backupName,
		Plan:       plan,
		Option:     o.toOption(),
	}, nil
}

func (o *options) run(cmd *cobra.Command, params *v2.Config) error {
	start := time.Now()

	req, err := o.toRequest()
	if err != nil {
		return err
	}

	job, err := app.NewRestore(params).Start(cmd.Context(), req)
	if err != nil {
		return err
	}

	if err := job.Execute(context.Background()); err != nil {
		return err
	}

	duration := time.Since(start)
	cmd.Println(fmt.Sprintf("duration:%.2f s", duration.Seconds()))

	return nil
}

// newTableMapperFromCollRename creates a new TableMapper with the given rename map.
func newTableMapperFromCollRename(collRename map[string]string) (*restore.TableMapper, error) {
	// add default db in collection_renames if not set
	nameMapping := make(map[string][]collref.Name)
	dbWildcard := make(map[string]string)

	for k, v := range collRename {
		rule, err := filter.InferMapperRuleType(k, v)
		if err != nil {
			return nil, err
		}

		switch rule {
		case 1:
			dbWildcard[k[:len(k)-2]] = v[:len(v)-2]
		case 2, 3:
			oldName, err := collref.Parse(k)
			if err != nil {
				return nil, fmt.Errorf("restore: parse collection name %s %w", k, err)
			}
			newName, err := collref.Parse(v)
			if err != nil {
				return nil, fmt.Errorf("restore: parse collection name %s %w", v, err)
			}

			nameMapping[oldName.String()] = append(nameMapping[oldName.String()], newName)
		case 4:
			// handle in db mapping
			continue
		}
	}

	return &restore.TableMapper{DBWildcard: dbWildcard, NameMapping: nameMapping}, nil
}

func NewCmd(opt *root.Options) *cobra.Command {
	var o options
	cmd := &cobra.Command{
		Use:   "restore",
		Short: "restore a backup into Milvus",
		Long: "Restore a backup into Milvus.\n\n" +
			"--filter names collections as they will exist in the target Milvus, that is, after\n" +
			"--suffix or --rename has been applied. Restoring only hello_milvus under a _recover\n" +
			"suffix is therefore \"--suffix _recover --filter hello_milvus_recover\", not\n" +
			"\"--filter hello_milvus\", which matches nothing.\n\n" +
			"The removed --collections, --databases and --database_collections selected\n" +
			"collections by their name in the backup instead; --filter is not a drop-in\n" +
			"replacement for them when the restore also renames.",

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

	cmd.AddCommand(newSecondaryCmd(opt))

	return cmd
}
