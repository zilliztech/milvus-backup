package utils

import (
	"testing"
)

func TestTs(t *testing.T) {
	ts := 443727974068387848
	time, logical := ParseTS(uint64(ts))
	println(time.Unix())
	println(logical)

}
