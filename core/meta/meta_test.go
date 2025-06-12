package meta

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/storage"
)

func TestRead(t *testing.T) {
	cli := storage.NewMockClient(t)

	result := &backuppb.BackupInfo{Name: "backup1"}
	byts, err := json.Marshal(result)
	assert.NoError(t, err)

	cli.EXPECT().
		GetObject(mock.Anything, "backup/backup1/meta/full_meta.json").
		Return(&storage.Object{Length: int64(len(byts)), Body: io.NopCloser(bytes.NewReader(byts))}, nil)

	backupInfo, err := Read(context.Background(), "backup/backup1", cli)
	assert.NoError(t, err)
	assert.Equal(t, "backup1", backupInfo.Name)
}
