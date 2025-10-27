package mpath

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMetaKey(t *testing.T) {
	backupDir := "base/backup/backup1"
	expect := "base/backup/backup1/meta/backup_meta.json"
	r := MetaKey(backupDir, BackupMeta)
	assert.Equal(t, expect, r)
}

func TestMetaDir(t *testing.T) {
	backupRoot := "base/backup"
	backupName := "backup1"

	expect := "base/backup/backup1/meta/"

	r := MetaDir(backupRoot, backupName)
	assert.Equal(t, expect, r)
}

type testCase struct {
	base     string
	expected string
}

func TestJoin(t *testing.T) {
	expect := "base/binlogs/insert_log/1/2/3/4/5"
	opts := []Option{CollectionID(1), PartitionID(2), SegmentID(3), FieldID(4), LogID(5)}
	s := Join("base/binlogs/insert_log", opts...)
	assert.Equal(t, expect, s)
}

func TestBackupInsertLogDir(t *testing.T) {
	collectionID := int64(1)
	partitionID := int64(2)

	// Without group id
	t.Run("WithoutGroupID", func(t *testing.T) {
		cases := []testCase{
			{base: "", expected: "binlogs/insert_log/1/2/"},
			{base: "base", expected: "base/binlogs/insert_log/1/2/"},
			{base: "base/", expected: "base/binlogs/insert_log/1/2/"},
			{base: "base/subdir", expected: "base/subdir/binlogs/insert_log/1/2/"},
			{base: "base/subdir/", expected: "base/subdir/binlogs/insert_log/1/2/"},
		}
		for _, test := range cases {
			r := BackupInsertLogDir(test.base, CollectionID(collectionID), PartitionID(partitionID))
			assert.Equal(t, test.expected, r)
		}
	})

	// With group id
	t.Run("WithGroupID", func(t *testing.T) {
		groupID := int64(3)
		cases := []testCase{
			{base: "", expected: "binlogs/insert_log/1/2/3/"},
			{base: "base", expected: "base/binlogs/insert_log/1/2/3/"},
			{base: "base/", expected: "base/binlogs/insert_log/1/2/3/"},
			{base: "base/subdir", expected: "base/subdir/binlogs/insert_log/1/2/3/"},
			{base: "base/subdir/", expected: "base/subdir/binlogs/insert_log/1/2/3/"},
		}
		for _, test := range cases {
			r := BackupInsertLogDir(test.base, CollectionID(collectionID), PartitionID(partitionID), GroupID(groupID))
			assert.Equal(t, test.expected, r)
		}
	})
}

func TestBackupDeltaLogDir(t *testing.T) {
	collectionID := int64(1)
	partitionID := int64(2)

	// Without group id
	t.Run("WithoutGroupID", func(t *testing.T) {
		cases := []testCase{
			{base: "", expected: "binlogs/delta_log/1/2/"},
			{base: "base", expected: "base/binlogs/delta_log/1/2/"},
			{base: "base/", expected: "base/binlogs/delta_log/1/2/"},
			{base: "base/subdir", expected: "base/subdir/binlogs/delta_log/1/2/"},
			{base: "base/subdir/", expected: "base/subdir/binlogs/delta_log/1/2/"},
		}
		for _, test := range cases {
			r := BackupDeltaLogDir(test.base, CollectionID(collectionID), PartitionID(partitionID))
			assert.Equal(t, test.expected, r)
		}
	})

	t.Run("WithGroupID", func(t *testing.T) {
		groupID := int64(3)
		cases := []testCase{
			{base: "", expected: "binlogs/delta_log/1/2/3/"},
			{base: "base", expected: "base/binlogs/delta_log/1/2/3/"},
			{base: "base/", expected: "base/binlogs/delta_log/1/2/3/"},
			{base: "base/subdir", expected: "base/subdir/binlogs/delta_log/1/2/3/"},
			{base: "base/subdir/", expected: "base/subdir/binlogs/delta_log/1/2/3/"},
		}
		for _, test := range cases {
			r := BackupDeltaLogDir(test.base, CollectionID(collectionID), PartitionID(partitionID), GroupID(groupID))
			assert.Equal(t, test.expected, r)
		}
	})

	// l0 segment id
	t.Run("L0SegmentDir", func(t *testing.T) {
		segmentID := int64(4)
		cases := []testCase{
			{base: "", expected: "binlogs/delta_log/1/2/4/"},
			{base: "base", expected: "base/binlogs/delta_log/1/2/4/"},
			{base: "base/", expected: "base/binlogs/delta_log/1/2/4/"},
			{base: "base/subdir", expected: "base/subdir/binlogs/delta_log/1/2/4/"},
			{base: "base/subdir/", expected: "base/subdir/binlogs/delta_log/1/2/4/"},
		}
		for _, test := range cases {
			r := BackupDeltaLogDir(test.base, CollectionID(collectionID), PartitionID(partitionID), SegmentID(segmentID))
			assert.Equal(t, test.expected, r)
		}
	})
}

func TestMilvusInsertLogDir(t *testing.T) {
	collectionID := int64(1)
	partitionID := int64(2)
	segementID := int64(3)

	cases := []testCase{
		{base: "", expected: "insert_log/1/2/3/"},
		{base: "base", expected: "base/insert_log/1/2/3/"},
		{base: "base/", expected: "base/insert_log/1/2/3/"},
		{base: "base/subdir", expected: "base/subdir/insert_log/1/2/3/"},
		{base: "base/subdir/", expected: "base/subdir/insert_log/1/2/3/"},
	}

	for _, test := range cases {
		r := MilvusInsertLogDir(test.base, CollectionID(collectionID), PartitionID(partitionID), SegmentID(segementID))
		assert.Equal(t, test.expected, r)
	}
}

func TestMilvusDeltaLogDir(t *testing.T) {
	collectionID := int64(1)
	partitionID := int64(2)
	segementID := int64(3)

	cases := []testCase{
		{base: "", expected: "delta_log/1/2/3/"},
		{base: "base", expected: "base/delta_log/1/2/3/"},
		{base: "base/", expected: "base/delta_log/1/2/3/"},
		{base: "base/subdir", expected: "base/subdir/delta_log/1/2/3/"},
		{base: "base/subdir/", expected: "base/subdir/delta_log/1/2/3/"},
	}

	for _, test := range cases {
		r := MilvusDeltaLogDir(test.base, CollectionID(collectionID), PartitionID(partitionID), SegmentID(segementID))
		assert.Equal(t, test.expected, r)
	}
}

func TestParseInsertLogPath(t *testing.T) {
	t.Run("ValidPath", func(t *testing.T) {
		expected := InsertLogPath{Root: "base", CollectionID: 1, PartitionID: 2, SegmentID: 3, FieldID: 4, LogID: 5}
		path := "base/insert_log/1/2/3/4/5"
		result, err := ParseInsertLogPath(path)
		assert.NoError(t, err)
		assert.Equal(t, expected, result)

		// with sub path
		expected = InsertLogPath{Root: "base/subdir", CollectionID: 1, PartitionID: 2, SegmentID: 3, FieldID: 4, LogID: 5}
		path = "base/subdir/insert_log/1/2/3/4/5"
		result, err = ParseInsertLogPath(path)
		assert.NoError(t, err)
		assert.Equal(t, expected, result)

		// root is empty
		expected = InsertLogPath{Root: "", CollectionID: 1, PartitionID: 2, SegmentID: 3, FieldID: 4, LogID: 5}
		path = "insert_log/1/2/3/4/5"
		result, err = ParseInsertLogPath(path)
		assert.NoError(t, err)
		assert.Equal(t, expected, result)
	})

	t.Run("InvalidPath", func(t *testing.T) {
		path := "base/invalid_log/1/2/3/4/5/"
		_, err := ParseInsertLogPath(path)
		assert.Error(t, err)

		path = "base/insert_log/1/2/3/4"
		_, err = ParseInsertLogPath(path)
		assert.Error(t, err)

		path = "base/insert_log/a/2/3/4/5"
		_, err = ParseInsertLogPath(path)
		assert.Error(t, err)

		path = "base/insert_log/1/b/3/4/5"
		_, err = ParseInsertLogPath(path)
		assert.Error(t, err)

		path = "base/insert_log/1/2/c/4/5"
		_, err = ParseInsertLogPath(path)
		assert.Error(t, err)

		path = "base/insert_log/1/2/3/d/5"
		_, err = ParseInsertLogPath(path)
		assert.Error(t, err)

		path = "base/insert_log/1/2/3/4/e"
		_, err = ParseInsertLogPath(path)
		assert.Error(t, err)

		path = "base/delta_log/1/2/3/4/5"
		_, err = ParseDeltaLogPath(path)
		assert.Error(t, err)
	})
}

func TestParseDeltaLogPath(t *testing.T) {
	t.Run("ValidDeltaLog", func(t *testing.T) {
		expected := DeltaLogPath{Root: "base", CollectionID: 1, PartitionID: 2, SegmentID: 3, LogID: 4}
		path := "base/delta_log/1/2/3/4"
		result, err := ParseDeltaLogPath(path)
		assert.NoError(t, err)
		assert.Equal(t, expected, result)

		// with negative partition id, -1 means all partition
		expected = DeltaLogPath{Root: "base", CollectionID: 1, PartitionID: -1, SegmentID: 2, LogID: 3}
		path = "base/delta_log/1/-1/2/3"
		result, err = ParseDeltaLogPath(path)
		assert.NoError(t, err)
		assert.Equal(t, expected, result)

		// with sub path
		expected = DeltaLogPath{Root: "base/subdir", CollectionID: 1, PartitionID: 2, SegmentID: 3, LogID: 4}
		path = "base/subdir/delta_log/1/2/3/4"
		result, err = ParseDeltaLogPath(path)
		assert.NoError(t, err)
		assert.Equal(t, expected, result)

		// root is empty
		expected = DeltaLogPath{Root: "", CollectionID: 1, PartitionID: 2, SegmentID: 3, LogID: 4}
		path = "delta_log/1/2/3/4"
		result, err = ParseDeltaLogPath(path)
		assert.NoError(t, err)
		assert.Equal(t, expected, result)
	})

	t.Run("InvalidDeltaLog", func(t *testing.T) {
		path := "base/invalid_log/1/2/3/4"
		_, err := ParseDeltaLogPath(path)
		assert.Error(t, err)

		path = "base/delta_log/1/2/3"
		_, err = ParseDeltaLogPath(path)
		assert.Error(t, err)

		path = "base/delta_log/a/2/3/4"
		_, err = ParseDeltaLogPath(path)
		assert.Error(t, err)

		path = "base/delta_log/1/b/3/4"
		_, err = ParseDeltaLogPath(path)
		assert.Error(t, err)

		path = "base/delta_log/1/2/c/4"
		_, err = ParseDeltaLogPath(path)
		assert.Error(t, err)

		path = "base/delta_log/1/2/3/d"
		_, err = ParseDeltaLogPath(path)
		assert.Error(t, err)

		path = "base/insert_log/1/2/3/4"
		_, err = ParseDeltaLogPath(path)
		assert.Error(t, err)
	})
}

func TestBackupDir(t *testing.T) {
	r := BackupDir("root", "name")
	assert.Equal(t, "root/name/", r)

	r = BackupDir("root/subdir", "name")
	assert.Equal(t, "root/subdir/name/", r)
}
