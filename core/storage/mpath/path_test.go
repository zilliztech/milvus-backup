package mpath

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type testCase struct {
	base     string
	expected string
}

func TestInsertLogDir(t *testing.T) {
	collectionID := int64(1)
	partitionID := int64(2)

	// Without group id
	cases := []testCase{
		{base: "base", expected: "base/binlogs/insert_log/1/2/"},
		{base: "base/", expected: "base/binlogs/insert_log/1/2/"},
		{base: "base/subdir", expected: "base/subdir/binlogs/insert_log/1/2/"},
		{base: "base/subdir/", expected: "base/subdir/binlogs/insert_log/1/2/"},
	}
	for _, test := range cases {
		r := InsertLogDir(test.base, CollectionID(collectionID), PartitionID(partitionID))
		assert.Equal(t, test.expected, r)
	}

	// With group id
	groupID := int64(3)
	cases = []testCase{
		{base: "base", expected: "base/binlogs/insert_log/1/2/3/"},
		{base: "base/", expected: "base/binlogs/insert_log/1/2/3/"},
		{base: "base/subdir", expected: "base/subdir/binlogs/insert_log/1/2/3/"},
		{base: "base/subdir/", expected: "base/subdir/binlogs/insert_log/1/2/3/"},
	}
	for _, test := range cases {
		r := InsertLogDir(test.base, CollectionID(collectionID), PartitionID(partitionID), GroupID(groupID))
		assert.Equal(t, test.expected, r)
	}
}

func TestPartitionDeltaLogDir(t *testing.T) {
	collectionID := int64(1)
	partitionID := int64(2)

	// Without group id
	cases := []testCase{
		{base: "base", expected: "base/binlogs/delta_log/1/2/"},
		{base: "base/", expected: "base/binlogs/delta_log/1/2/"},
		{base: "base/subdir", expected: "base/subdir/binlogs/delta_log/1/2/"},
		{base: "base/subdir/", expected: "base/subdir/binlogs/delta_log/1/2/"},
	}
	for _, test := range cases {
		r := DeltaLogDir(test.base, CollectionID(collectionID), PartitionID(partitionID))
		assert.Equal(t, test.expected, r)
	}

	// With group id
	groupID := int64(3)
	cases = []testCase{
		{base: "base", expected: "base/binlogs/delta_log/1/2/3/"},
		{base: "base/", expected: "base/binlogs/delta_log/1/2/3/"},
		{base: "base/subdir", expected: "base/subdir/binlogs/delta_log/1/2/3/"},
		{base: "base/subdir/", expected: "base/subdir/binlogs/delta_log/1/2/3/"},
	}
	for _, test := range cases {
		r := DeltaLogDir(test.base, CollectionID(collectionID), PartitionID(partitionID), GroupID(groupID))
		assert.Equal(t, test.expected, r)
	}

	// with segment id
	segmentID := int64(4)
	cases = []testCase{
		{base: "base", expected: "base/binlogs/delta_log/1/2/4/"},
		{base: "base/", expected: "base/binlogs/delta_log/1/2/4/"},
		{base: "base/subdir", expected: "base/subdir/binlogs/delta_log/1/2/4/"},
		{base: "base/subdir/", expected: "base/subdir/binlogs/delta_log/1/2/4/"},
	}
	for _, test := range cases {
		r := DeltaLogDir(test.base, CollectionID(collectionID), PartitionID(partitionID), SegmentID(segmentID))
		assert.Equal(t, test.expected, r)
	}
}
