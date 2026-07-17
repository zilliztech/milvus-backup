package l0compact

import "testing"

func TestClassifyVersion(t *testing.T) {
	cases := map[int64]StorageKind{0: KindV1, 1: KindV1, 2: KindV2, 3: KindV2}
	for v, want := range cases {
		got, err := ClassifyVersion(v)
		if err != nil || got != want {
			t.Fatalf("v=%d got=%v err=%v want=%v", v, got, err, want)
		}
	}
	if _, err := ClassifyVersion(4); err == nil {
		t.Fatal("want fail-fast on version 4")
	}
}
