package milvus

import (
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestStreamCheckpointPersistence(t *testing.T) {
	checkpoint := NewStreamCheckpoint(map[string]uint64{"a": 100, "b": 110})
	first, second, third := newMsg(20), newMsg(30), newMsg(40)
	second.Payload = []byte("original-payload")
	second.Properties["original-header"] = "original-value"
	require.NoError(t, checkpoint.record("ddl", 40, map[string][]*commonpb.ImmutableMessage{"a": {first, second, third}, "b": {first}}, false))
	checkpoint.confirm("a", 20)
	snapshot, err := checkpoint.Snapshot()
	require.NoError(t, err)
	restored, err := RestoreStreamCheckpoint(snapshot)
	require.NoError(t, err)
	tick, pending := restored.replay()
	assert.Equal(t, uint64(40), tick)
	assert.True(t, restored.Sent("ddl"))
	require.Len(t, pending["a"], 2)
	assert.True(t, proto.Equal(second, pending["a"][0]))
	assert.True(t, proto.Equal(third, pending["a"][1]))
	assert.NoError(t, restored.ValidatePosition("a", 20))
	assert.NoError(t, restored.ValidatePosition("a", 30))
	assert.Error(t, restored.ValidatePosition("a", 0))
	restored.confirm("a", 30)
	_, original := checkpoint.replay()
	assert.Len(t, original["a"], 2)
	snapshot.Pending["a"][0] = []byte{0xff}
	_, err = RestoreStreamCheckpoint(snapshot)
	assert.Error(t, err)
}
