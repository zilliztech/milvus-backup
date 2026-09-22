package storage

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestVerifyPrefixTask_Execute(t *testing.T) {
	t.Run("AllPresent", func(t *testing.T) {
		cli := NewMockClient(t)
		objs := []ObjectAttr{
			{Key: "dest/a", Length: 1},
			{Key: "dest/b", Length: 2},
		}
		cli.EXPECT().
			NewObjectIter(mock.Anything, "dest/", true).
			Return(NewMockObjectIterator(objs)).Once()

		task := NewVerifyPrefixTask(VerifyPrefixOpt{
			Cli:      cli,
			Prefix:   "dest/",
			Expected: map[string]int64{"dest/a": 1, "dest/b": 2},
		})
		assert.NoError(t, task.Execute(context.Background()))
	})

	t.Run("ExtraObjectsIgnored", func(t *testing.T) {
		cli := NewMockClient(t)
		objs := []ObjectAttr{
			{Key: "dest/a", Length: 1},
			{Key: "dest/extra", Length: 9},
		}
		cli.EXPECT().
			NewObjectIter(mock.Anything, "dest/", true).
			Return(NewMockObjectIterator(objs)).Once()

		task := NewVerifyPrefixTask(VerifyPrefixOpt{
			Cli:      cli,
			Prefix:   "dest/",
			Expected: map[string]int64{"dest/a": 1},
		})
		assert.NoError(t, task.Execute(context.Background()))
	})

	t.Run("SizeMismatch", func(t *testing.T) {
		cli := NewMockClient(t)
		objs := []ObjectAttr{{Key: "dest/a", Length: 3}}
		cli.EXPECT().
			NewObjectIter(mock.Anything, "dest/", true).
			Return(NewMockObjectIterator(objs)).Once()

		task := NewVerifyPrefixTask(VerifyPrefixOpt{
			Cli:      cli,
			Prefix:   "dest/",
			Expected: map[string]int64{"dest/a": 1},
		})
		err := task.Execute(context.Background())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "size mismatch")
	})

	t.Run("Missing", func(t *testing.T) {
		cli := NewMockClient(t)
		objs := []ObjectAttr{{Key: "dest/a", Length: 1}}
		cli.EXPECT().
			NewObjectIter(mock.Anything, "dest/", true).
			Return(NewMockObjectIterator(objs)).Once()

		task := NewVerifyPrefixTask(VerifyPrefixOpt{
			Cli:      cli,
			Prefix:   "dest/",
			Expected: map[string]int64{"dest/a": 1, "dest/b": 2},
		})
		err := task.Execute(context.Background())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "missing")
		assert.Contains(t, err.Error(), "dest/b")
	})

	t.Run("ManyMissingSampleCapped", func(t *testing.T) {
		cli := NewMockClient(t)
		cli.EXPECT().
			NewObjectIter(mock.Anything, "dest/", true).
			Return(NewMockObjectIterator(nil)).Once()

		expected := make(map[string]int64, _missingSampleSize+3)
		for i := range _missingSampleSize + 3 {
			expected[fmt.Sprintf("dest/%d", i)] = 1
		}
		task := NewVerifyPrefixTask(VerifyPrefixOpt{Cli: cli, Prefix: "dest/", Expected: expected})
		err := task.Execute(context.Background())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "missing")
	})

	t.Run("EmptyExpectedSkipsList", func(t *testing.T) {
		cli := NewMockClient(t)
		task := NewVerifyPrefixTask(VerifyPrefixOpt{
			Cli:      cli,
			Prefix:   "dest/",
			Expected: map[string]int64{},
		})
		// No NewObjectIter expectation: an empty expected set must not call the API.
		assert.NoError(t, task.Execute(context.Background()))
	})

	t.Run("IterError", func(t *testing.T) {
		cli := NewMockClient(t)
		cli.EXPECT().
			NewObjectIter(mock.Anything, "dest/", true).
			Return(seqFailing(assert.AnError)).Once()

		task := NewVerifyPrefixTask(VerifyPrefixOpt{
			Cli:      cli,
			Prefix:   "dest/",
			Expected: map[string]int64{"dest/a": 1},
		})
		err := task.Execute(context.Background())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "iter object")
	})
}

func TestExpectedDestObjects(t *testing.T) {
	t.Run("MapsKeysAndSkipsDirMarkers", func(t *testing.T) {
		cli := NewMockClient(t)
		objs := []ObjectAttr{
			{Key: "src/a", Length: 1},
			{Key: "src/sub/b", Length: 2},
			{Key: "src/dir/", Length: 0}, // directory marker, must be skipped
		}
		cli.EXPECT().
			NewObjectIter(mock.Anything, "src/", true).
			Return(NewMockObjectIterator(objs)).Once()

		expected, err := ExpectedDestObjects(context.Background(), cli, "src/", "dest/")
		assert.NoError(t, err)
		assert.Equal(t, map[string]int64{"dest/a": 1, "dest/sub/b": 2}, expected)
	})

	t.Run("IterError", func(t *testing.T) {
		cli := NewMockClient(t)
		cli.EXPECT().
			NewObjectIter(mock.Anything, "src/", true).
			Return(seqFailingAfter([]ObjectAttr{{Key: "src/a", Length: 1}}, assert.AnError)).Once()

		_, err := ExpectedDestObjects(context.Background(), cli, "src/", "dest/")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "list prefix")
	})
}
