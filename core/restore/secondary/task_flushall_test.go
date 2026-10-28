package secondary

import (
	"context"
	"encoding/base64"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

// marshalFlushAllMsg builds one FlushAllMsgsBase64 entry the way the backup
// path records it: the ImmutableMessage from FlushAllResponse, marshaled and
// base64-encoded.
func marshalFlushAllMsg(t *testing.T, props map[string]string) (string, *commonpb.ImmutableMessage) {
	t.Helper()

	msg := &commonpb.ImmutableMessage{
		Id:         &commonpb.MessageID{Id: "wp-entry-42", WALName: commonpb.WALName_WoodPecker},
		Payload:    []byte("flush-all-payload"),
		Properties: props,
	}
	raw, err := proto.Marshal(msg)
	require.NoError(t, err)

	return base64.StdEncoding.EncodeToString(raw), msg
}

func TestSendFlushAllStripsUnreplicableMarker(t *testing.T) {
	flushAllProps := func() map[string]string {
		return map[string]string{
			"_t":  "flushall",
			"_tt": "453512",
			"_vc": "backup-test-upstream-rootcoord-dm_0_450095203130000000v0",
		}
	}

	run := func(t *testing.T, props map[string]string) *commonpb.ImmutableMessage {
		msgB64, src := marshalFlushAllMsg(t, props)

		stream := &recordingStream{}
		task := &Task{
			args: TaskArgs{Backup: &backuppb.BackupInfo{
				FlushAllMsgsBase64: map[string]string{
					"backup-test-upstream-rootcoord-dm_0": msgB64,
				},
			}},
			streamCli: stream,
			logger:    zap.NewNop(),
		}

		require.NoError(t, task.sendFlushAll(context.Background()))

		forwarded := stream.messagesByType("flushall")
		require.Len(t, forwarded, 1)
		assert.Equal(t, src.GetPayload(), forwarded[0].GetPayload())
		assert.Equal(t, src.GetId().GetId(), forwarded[0].GetId().GetId())
		assert.Equal(t, src.GetId().GetWALName(), forwarded[0].GetId().GetWALName())

		return forwarded[0]
	}

	t.Run("StripsTheMarker", func(t *testing.T) {
		props := flushAllProps()
		props["_ur"] = ""

		forwarded := run(t, props)

		assert.NotContains(t, forwarded.GetProperties(), "_ur")
		assert.Equal(t, flushAllProps(), forwarded.GetProperties())
	})

	t.Run("LeavesOlderMessagesAlone", func(t *testing.T) {
		forwarded := run(t, flushAllProps())

		assert.Equal(t, flushAllProps(), forwarded.GetProperties())
	})
}
