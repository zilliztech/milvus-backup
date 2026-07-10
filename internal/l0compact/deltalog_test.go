package l0compact

import "testing"

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
