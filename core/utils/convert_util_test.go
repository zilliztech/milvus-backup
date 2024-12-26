package utils

import (
	"testing"
)

func TestTs(t *testing.T) {
	ts := 377316862683774976
	time, logical := ParseTS(uint64(ts))
	println(time.Unix())
	println(logical)

	res := ComposeTS(452472464411983874, 0)
	println(res)

	res2 := ComposeTS(452474786168963076, 0)
	println(res2)
}
