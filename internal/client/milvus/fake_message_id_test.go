package milvus

import (
	"encoding/base64"
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	wpb "github.com/zilliztech/woodpecker/proto"
	"google.golang.org/protobuf/proto"
)

// TestFakeMessageIDRoundTrip encodes a fake id and decodes it back with the
// same codec the matching milvus WAL unmarshaler uses, so an encoding drift
// between the tool and milvus fails here instead of as a
// WALNAME_MISMATCH storm against a live cluster.
func TestFakeMessageIDRoundTrip(t *testing.T) {
	for _, walName := range []message.WALName{
		message.WALNameKafka,
		message.WALNameRocksmq,
		message.WALNameWoodpecker,
		message.WALNamePulsar,
	} {
		t.Run(walName.String(), func(t *testing.T) {
			require.True(t, SupportedFakeWALName(walName))
			id := NewFakeMessageID(42, walName)
			assert.Equal(t, walName, id.WALName())

			encoded := id.Marshal()
			switch walName {
			case message.WALNameKafka, message.WALNameRocksmq:
				v, err := message.DecodeInt64(encoded)
				require.NoError(t, err)
				assert.EqualValues(t, 42, v)
			case message.WALNameWoodpecker:
				raw, err := base64.StdEncoding.DecodeString(encoded)
				require.NoError(t, err)
				var wp wpb.LogMessageIdData
				require.NoError(t, proto.Unmarshal(raw, &wp))
				assert.EqualValues(t, 0, wp.GetSegId())
				assert.EqualValues(t, 42, wp.GetEntryId())
			case message.WALNamePulsar:
				raw, err := base64.StdEncoding.DecodeString(encoded)
				require.NoError(t, err)
				pid, err := pulsar.DeserializeMessageID(raw)
				require.NoError(t, err)
				assert.EqualValues(t, 42, pid.LedgerID())
				assert.EqualValues(t, 0, pid.EntryID())
			}

			proto := id.IntoProto()
			assert.Equal(t, commonpb.WALName(walName), proto.GetWALName())
			assert.Equal(t, encoded, proto.GetId())
		})
	}
}

// TestFakeMessageIDWoodpeckerGolden pins the woodpecker encoding to a value
// observed from a live cluster: the empty checkpoint id {SegmentId:0,
// EntryId:2} serializes to "EAI=" in a real deployment's logs.
func TestFakeMessageIDWoodpeckerGolden(t *testing.T) {
	assert.Equal(t, "EAI=", NewFakeMessageID(2, message.WALNameWoodpecker).Marshal())
	assert.Equal(t, "a", NewFakeMessageID(10, message.WALNameKafka).Marshal())
}

// TestFakeMessageIDOrdering keeps the comparator on the counter only, which
// is what orders messages within one replay stream.
func TestFakeMessageIDOrdering(t *testing.T) {
	lo := NewFakeMessageID(1, message.WALNameWoodpecker)
	hi := NewFakeMessageID(2, message.WALNameWoodpecker)
	assert.True(t, lo.LT(hi))
	assert.True(t, lo.LTE(hi))
	assert.True(t, hi.EQ(NewFakeMessageID(2, message.WALNameWoodpecker)))
	assert.False(t, hi.LT(lo))
}

// TestFakeMessageIDUnsupportedPanics documents that an unsupported WAL name
// is rejected up front by SupportedFakeWALName, and that Marshal treats the
// leftover case as a programming error.
func TestFakeMessageIDUnsupportedPanics(t *testing.T) {
	assert.False(t, SupportedFakeWALName(message.WALNameUnknown))
	assert.False(t, SupportedFakeWALName(message.WALNameTest))
	assert.Panics(t, func() {
		NewFakeMessageID(1, message.WALNameUnknown).Marshal()
	})
}
