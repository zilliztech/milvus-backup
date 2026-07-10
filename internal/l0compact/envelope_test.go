package l0compact

import "testing"

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
	if err != nil {
		t.Fatal(err)
	}
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
	if err != nil {
		t.Fatal(err)
	}
	if desc.PayloadDataType != 20 || desc.FieldID != 0 {
		t.Fatalf("desc=%+v", desc)
	}
	if len(events) != 1 || string(events[0].Payload) != string(payload) {
		t.Fatalf("events=%d payload mismatch", len(events))
	}
	if _, ok := desc.Extras["version"]; ok {
		t.Fatal("json variant should have no MULTI_FIELD version")
	}
}
