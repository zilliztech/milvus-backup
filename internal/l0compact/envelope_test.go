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
	extras      map[string]string
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
		extras:      map[string]string{},
	}, payload)

	desc, events, err := parseV1Envelope(blob)
	require.NoError(t, err)
	assert.EqualValues(t, 20, desc.PayloadDataType)
	assert.EqualValues(t, 0, desc.FieldID)
	require.Len(t, events, 1)
	assert.Equal(t, payload, events[0].Payload)
	assert.NotContains(t, desc.Extras, "version", "json variant should have no MULTI_FIELD version")
}
