package l0compact

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// envHeaderArgs configures the descriptor fields used by buildV1Envelope.
type envHeaderArgs struct {
	typeCode    int8
	payloadType int32
	fieldID     int64
	extras      map[string]any
}

// buildV1Envelope wraps payload in a v1 envelope using the exported
// BuildV1Envelope writer, so read and write are tested together.
func buildV1Envelope(t *testing.T, args envHeaderArgs, payload []byte) []byte {
	t.Helper()
	blob, err := BuildV1Envelope(args.typeCode, args.payloadType, args.fieldID, args.extras, payload)
	require.NoError(t, err)
	return blob
}

func TestEnvelopeRoundTrip(t *testing.T) {
	payload := []byte("PARQUET-BYTES-STANDIN") // any bytes; envelope is opaque to payload
	blob := buildV1Envelope(t, envHeaderArgs{
		typeCode:    eventDelete,
		payloadType: 20, // String
		fieldID:     0,
		extras:      map[string]any{},
	}, payload)

	desc, events, err := parseV1Envelope(blob)
	require.NoError(t, err)
	assert.EqualValues(t, 20, desc.PayloadDataType)
	assert.EqualValues(t, 0, desc.FieldID)
	require.Len(t, events, 1)
	assert.Equal(t, payload, events[0].Payload)
	assert.NotContains(t, desc.Extras, "version", "json variant should have no MULTI_FIELD version")
}

// Milvus stores nullable in the descriptor extras as a bool, next to string
// entries such as original_size, so the extra map is heterogeneous. See
// NewInsertBinlogWriter in milvus' internal/storage/binlog_writer.go.
func TestEnvelopeNonStringExtras(t *testing.T) {
	payload := []byte("PARQUET-BYTES-STANDIN")
	blob := buildV1Envelope(t, envHeaderArgs{
		typeCode:    eventInsert,
		payloadType: 5, // Int64
		fieldID:     100,
		extras: map[string]any{
			"original_size": "12345",
			"nullable":      true,
		},
	}, payload)

	desc, events, err := parseV1Envelope(blob)
	require.NoError(t, err)
	assert.Equal(t, "12345", desc.Extras["original_size"])
	assert.Equal(t, true, desc.Extras["nullable"])
	require.Len(t, events, 1)
	assert.Equal(t, payload, events[0].Payload)
}

// A bool-valued extra must not stop ReadDeltalog from recognizing the
// MULTI_FIELD marker, which decides how the payload is decoded.
func TestEnvelopeVersionExtraAmongNonStrings(t *testing.T) {
	blob := buildV1Envelope(t, envHeaderArgs{
		typeCode:    eventDelete,
		payloadType: 5,
		fieldID:     0,
		extras: map[string]any{
			"version":  "MULTI_FIELD",
			"nullable": false,
		},
	}, []byte("PARQUET-BYTES-STANDIN"))

	desc, _, err := parseV1Envelope(blob)
	require.NoError(t, err)
	assert.Equal(t, "MULTI_FIELD", desc.Extras["version"])
}
