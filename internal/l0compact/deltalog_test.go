package l0compact

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestDeltalogV1JSONRoundTrip(t *testing.T) {
	in := []DeleteEntry{{PK: PrimaryKey{Type: PKInt64, Int: 42}, Ts: 100}}
	blob, err := WriteDeltalog(in, KindV1, PKInt64)
	if err != nil {
		t.Fatal(err)
	}
	out, err := ReadDeltalog(blob, KindV1, PKInt64)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 || out[0].PK.Int != 42 || out[0].Ts != 100 {
		t.Fatalf("got %+v", out)
	}
}

func TestDeltalogV2RoundTripVarChar(t *testing.T) {
	in := []DeleteEntry{{PK: PrimaryKey{Type: PKVarChar, Str: "k1"}, Ts: 7}}
	blob, err := WriteDeltalog(in, KindV2, PKVarChar)
	if err != nil {
		t.Fatal(err)
	}
	out, err := ReadDeltalog(blob, KindV2, PKVarChar)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 || out[0].PK.Str != "k1" || out[0].Ts != 7 {
		t.Fatalf("got %+v", out)
	}
}

// TestDeltalogV1MultiEvent guards the fix for a v1 deltalog file that carries
// more than one delete event (real Milvus can flush multiple events into one
// file). The reader must accumulate deletes from ALL events; before the fix it
// read only events[0] and silently dropped the rest.
func TestDeltalogV1MultiEvent(t *testing.T) {
	// event 1 (via the normal writer): pk=1 @ ts=10
	blob, err := WriteDeltalog([]DeleteEntry{{PK: PrimaryKey{Type: PKInt64, Int: 1}, Ts: 10}}, KindV1, PKInt64)
	if err != nil {
		t.Fatal(err)
	}
	// append a second Delete event to the SAME file: pk=2 @ ts=20
	payload2, err := writeParquetStringColumn([]string{`{"pk":2,"ts":20,"pkType":5}`}, 0)
	if err != nil {
		t.Fatal(err)
	}
	blob = append(blob, buildDeltaEvent(payload2)...)

	out, err := ReadDeltalog(blob, KindV1, PKInt64)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 2 { // was 1 before the fix
		t.Fatalf("want 2 entries from 2 events, got %d: %+v", len(out), out)
	}
	seen := map[int64]uint64{out[0].PK.Int: out[0].Ts, out[1].PK.Int: out[1].Ts}
	if seen[1] != 10 || seen[2] != 20 {
		t.Fatalf("got %+v", out)
	}
}

// buildDeltaEvent encodes one v1 Delete data event: 17B header + 16B fixpart + payload.
func buildDeltaEvent(payload []byte) []byte {
	var buf bytes.Buffer
	eventLen := int32(eventHeaderSize + dataEventFixPartSize + len(payload))
	_ = binary.Write(&buf, endian, eventHeader{TypeCode: eventDelete, EventLength: eventLen})
	_ = binary.Write(&buf, endian, struct{ StartTs, EndTs uint64 }{})
	buf.Write(payload)
	return buf.Bytes()
}

func TestDeltalogV1MultiFieldRead(t *testing.T) {
	// multi-field variant: envelope with version=MULTI_FIELD wrapping a 2-col parquet.
	payload, _ := WriteParquetPKTs([]PrimaryKey{{Type: PKInt64, Int: 9}}, []uint64{3}, PKInt64)
	blob, _ := BuildV1Envelope(eventDelete, 5 /*Int64*/, 0, map[string]string{"version": "MULTI_FIELD"}, payload)
	out, err := ReadDeltalog(blob, KindV1, PKInt64)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 || out[0].PK.Int != 9 || out[0].Ts != 3 {
		t.Fatalf("got %+v", out)
	}
}
