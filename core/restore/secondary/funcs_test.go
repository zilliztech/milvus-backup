package secondary

import (
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

func TestCheckDynamicField(t *testing.T) {
	t.Run("DynamicDisabled", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Name:               "coll",
			EnableDynamicField: false,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk"},
			},
		}
		assert.NoError(t, checkDynamicField(schema))
	})

	t.Run("DynamicEnabledAndPresent", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Name:               "coll",
			EnableDynamicField: true,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk"},
				{FieldID: 101, Name: "$meta", IsDynamic: true},
			},
		}
		assert.NoError(t, checkDynamicField(schema))
	})

	t.Run("DynamicEnabledButMissing", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Name:               "coll",
			EnableDynamicField: true,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk"},
			},
		}
		err := checkDynamicField(schema)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), `"coll"`)
	})
}

func TestCheckIndexExtra(t *testing.T) {
	withExtra := func(name string) *backuppb.IndexInfo {
		return &backuppb.IndexInfo{
			FieldName:   "vec",
			IndexName:   name,
			FieldId:     101,
			IndexParams: []*backuppb.KeyValuePair{{Key: "index_type", Value: "IVF_FLAT"}},
		}
	}
	withoutExtra := func(name string) *backuppb.IndexInfo {
		return &backuppb.IndexInfo{FieldName: "vec", IndexName: name}
	}
	newBackup := func(indexes ...*backuppb.IndexInfo) *backuppb.BackupInfo {
		return &backuppb.BackupInfo{
			Name: "bak1",
			CollectionBackups: []*backuppb.CollectionBackupInfo{
				{DbName: "default", CollectionName: "coll1", IndexInfos: indexes},
			},
		}
	}

	t.Run("EveryIndexHasExtra", func(t *testing.T) {
		assert.NoError(t, checkIndexExtra(newBackup(withExtra("idx1"), withExtra("idx2"))))
	})

	t.Run("NoIndexHasExtra", func(t *testing.T) {
		err := checkIndexExtra(newBackup(withoutExtra("idx1"), withoutExtra("idx2")))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), `"bak1"`)
		assert.Contains(t, err.Error(), "default.coll1/idx1")
		assert.Contains(t, err.Error(), "default.coll1/idx2")
		assert.Contains(t, err.Error(), "--backup_index_extra")
	})

	t.Run("IndexParamsMissingCountsAsNoExtra", func(t *testing.T) {
		index := withExtra("idx1")
		index.IndexParams = nil
		err := checkIndexExtra(newBackup(index))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "default.coll1/idx1")
	})

	t.Run("NoIndexAtAll", func(t *testing.T) {
		assert.NoError(t, checkIndexExtra(newBackup()))
	})

	t.Run("NoCollection", func(t *testing.T) {
		assert.NoError(t, checkIndexExtra(&backuppb.BackupInfo{Name: "bak1"}))
	})

	t.Run("PartiallyPopulatedNamesBothSides", func(t *testing.T) {
		backup := &backuppb.BackupInfo{
			Name: "bak1",
			CollectionBackups: []*backuppb.CollectionBackupInfo{
				{DbName: "default", CollectionName: "coll1", IndexInfos: []*backuppb.IndexInfo{withExtra("good_idx")}},
				{DbName: "db2", CollectionName: "coll2", IndexInfos: []*backuppb.IndexInfo{withoutExtra("bad_idx")}},
			},
		}
		err := checkIndexExtra(backup)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), `"bak1"`)
		assert.Contains(t, err.Error(), "default.coll1/good_idx")
		assert.Contains(t, err.Error(), "db2.coll2/bad_idx")
		assert.Contains(t, err.Error(), "inconsistent")
		assert.Contains(t, err.Error(), "--backup_index_extra")
	})
}

func TestIndexNames(t *testing.T) {
	backup := &backuppb.BackupInfo{
		CollectionBackups: []*backuppb.CollectionBackupInfo{
			{DbName: "default", CollectionName: "coll1", IndexInfos: []*backuppb.IndexInfo{
				{IndexName: "idx1", FieldId: 101, IndexParams: []*backuppb.KeyValuePair{{Key: "index_type", Value: "IVF_FLAT"}}},
				{IndexName: "idx2"},
			}},
		},
	}

	all := indexNames(backup, func(*backuppb.IndexInfo) bool { return true })
	assert.Equal(t, []string{"default.coll1/idx1", "default.coll1/idx2"}, all)

	assert.Equal(t, []string{"default.coll1/idx1"}, indexNames(backup, hasIndexExtra))
}
