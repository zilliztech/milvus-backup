package backup

import (
	"testing"

	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/meta"
)

func TestTaskCheckCollFieldIDs(t *testing.T) {
	unreplayable := &backuppb.CollectionSchema{
		Fields: []*backuppb.FieldSchema{
			{Name: "pk", FieldID: 100},
			{Name: "vec", FieldID: 101},
			{Name: "added", FieldID: 104},
		},
		StructArrayFields: []*backuppb.StructArrayFieldSchema{{
			Name: "struct", FieldID: 102,
			Fields: []*backuppb.FieldSchema{{Name: "sub", FieldID: 103}},
		}},
		Properties: []*backuppb.KeyValuePair{{Key: common.MaxFieldIDKey, Value: "104"}},
	}

	tests := []struct {
		name      string
		format    string
		schema    *backuppb.CollectionSchema
		wantError string
	}{
		{
			name:      "RejectsUnreplayableSchema",
			format:    meta.FormatBinlog,
			schema:    unreplayable,
			wantError: `field "added" changes id from 104 to 102`,
		},
		{
			name:   "AcceptsReplayableSchema",
			format: meta.FormatBinlog,
			schema: &backuppb.CollectionSchema{Fields: []*backuppb.FieldSchema{
				{Name: "pk", FieldID: 100},
				{Name: "vec", FieldID: 101},
			}},
		},
		{
			name:   "SnapshotSkipsReplay",
			format: meta.FormatSnapshot,
			schema: unreplayable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := newMetaBuilder("task", "backup")
			builder.addCollection(collref.New("db", "coll"), &backuppb.CollectionBackupInfo{
				CollectionId:   1,
				DbName:         "db",
				CollectionName: "coll",
				Schema:         tt.schema,
			})
			task := &Task{resolvedFormat: tt.format, metaBuilder: builder}

			err := task.checkCollFieldIDs()
			if tt.wantError == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.ErrorContains(t, err, tt.wantError)
		})
	}
}
