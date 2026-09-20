package storage

import (
	"bytes"
	"context"
	"io"
	"os"
	"path"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
)

func TestLocalClient_BucketExist(t *testing.T) {
	cli := &LocalClient{}
	exist, err := cli.BucketExist(context.Background(), "")
	assert.NoError(t, err)
	assert.True(t, exist)
}

func TestLocalClient_CreateBucket(t *testing.T) {
	cli := &LocalClient{}
	err := cli.CreateBucket(context.Background())
	assert.NoError(t, err)
}

func TestLocalClient_UploadObject(t *testing.T) {
	key := path.Join(t.TempDir(), "test.txt")
	cli := &LocalClient{}
	err := cli.UploadObject(context.Background(), UploadObjectInput{
		Key:  key,
		Body: bytes.NewReader([]byte{0}),
		Size: 1,
	})

	assert.NoError(t, err)
	assert.FileExists(t, key)

	file, err := os.Open(key)
	assert.NoError(t, err)
	defer file.Close()

	info, err := file.Stat()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), info.Size())

	content, err := io.ReadAll(file)
	assert.NoError(t, err)
	assert.Equal(t, []byte{0}, content)
}

func TestLocalClient_DeleteObject(t *testing.T) {
	key := path.Join(t.TempDir(), "test.txt")
	err := os.WriteFile(key, []byte{0}, 0644)
	assert.NoError(t, err)
	assert.FileExists(t, key)

	cli := &LocalClient{}
	err = cli.DeleteObject(context.Background(), key)
	assert.NoError(t, err)
	assert.NoFileExists(t, key)
}

func TestLocalClient_HeadObject(t *testing.T) {
	key := path.Join(t.TempDir(), "test.txt")
	err := os.WriteFile(key, []byte{0}, 0644)
	assert.NoError(t, err)
	assert.FileExists(t, key)

	cli := &LocalClient{}
	attr, err := cli.HeadObject(context.Background(), key)
	assert.NoError(t, err)
	assert.Equal(t, key, attr.Key)
	assert.Equal(t, int64(1), attr.Length)
}

func TestLocalClient_GetObject(t *testing.T) {
	key := path.Join(t.TempDir(), "test.txt")
	err := os.WriteFile(key, []byte{0}, 0644)
	assert.NoError(t, err)
	assert.FileExists(t, key)

	cli := &LocalClient{}
	obj, err := cli.GetObject(context.Background(), key)
	assert.NoError(t, err)
	defer obj.Body.Close()

	assert.Equal(t, int64(1), obj.Length)
	content, err := io.ReadAll(obj.Body)
	assert.NoError(t, err)
	assert.Equal(t, []byte{0}, content)
}

func TestLocalClient_CopyObject(t *testing.T) {
	srcKey := path.Join(t.TempDir(), "test.txt")
	err := os.WriteFile(srcKey, []byte{0}, 0644)
	assert.NoError(t, err)
	assert.FileExists(t, srcKey)

	destKey := path.Join(t.TempDir(), "backup", "test.txt")
	cli := &LocalClient{}
	assert.NoFileExists(t, destKey)
	err = cli.CopyObject(context.Background(), CopyObjectInput{SrcCli: cli, SrcAttr: ObjectAttr{Key: srcKey, Length: 1}, DestKey: destKey})
	assert.NoError(t, err)
	assert.FileExists(t, destKey)
}

func TestLocalClient_NewObjectIter(t *testing.T) {
	dir := t.TempDir()

	keys := []string{
		path.Join(dir, "backup", "meta", "test.txt"),
		path.Join(dir, "backup", "meta", "test2.txt"),
		path.Join(dir, "backup", "meta", "test3.txt"),
		path.Join(dir, "backup", "binlogs", "test.txt"),
		path.Join(dir, "backup", "binlogs", "test2.txt"),
		path.Join(dir, "backup", "binlogs", "test3.txt"),
	}

	for _, key := range keys {
		err := os.MkdirAll(path.Dir(key), 0755)
		assert.NoError(t, err)
		err = os.WriteFile(key, []byte{0}, 0644)
		assert.NoError(t, err)
	}

	t.Run("PrefixIsFile", func(t *testing.T) {
		cli := &LocalClient{}
		file := path.Join(dir, "backup", "meta", "test.txt")

		for _, recursive := range []bool{false, true} {
			var entries []ObjectAttr
			for entry, err := range cli.NewObjectIter(context.Background(), file, recursive) {
				assert.NoError(t, err)
				entries = append(entries, entry)
			}
			assert.Equal(t, []ObjectAttr{{Key: file, Length: 1}}, entries)
		}
	})

	t.Run("Empty", func(t *testing.T) {
		cli := &LocalClient{}

		for _, recursive := range []bool{false, true} {
			var count int
			for _, err := range cli.NewObjectIter(context.Background(), path.Join(dir, "not_exist"), recursive) {
				assert.NoError(t, err)
				count++
			}
			assert.Equal(t, 0, count)
		}
	})

	t.Run("StatErrorSurfacesThroughFirstIteration", func(t *testing.T) {
		// The sequence is lazy: a prefix whose path crosses a regular file
		// (ENOTDIR) must surface the stat error on the first iteration.
		cli := &LocalClient{}
		for _, err := range cli.NewObjectIter(context.Background(), path.Join(dir, "backup", "meta", "test.txt", "nested"), true) {
			assert.Error(t, err)
			return
		}
		assert.Fail(t, "expected a stat error from the first iteration")
	})

	t.Run("Recursive", func(t *testing.T) {
		cli := &LocalClient{}

		var entries []ObjectAttr
		for entry, err := range cli.NewObjectIter(context.Background(), path.Join(dir, "backup"), true) {
			assert.NoError(t, err)
			entries = append(entries, entry)
		}

		assert.Len(t, entries, 6)
		names := lo.Map(entries, func(entry ObjectAttr, _ int) string { return entry.Key })
		assert.ElementsMatch(t, keys, names)
	})

	t.Run("NonRecursive", func(t *testing.T) {
		cli := &LocalClient{}

		var entries []ObjectAttr
		for entry, err := range cli.NewObjectIter(context.Background(), path.Join(dir, "backup"), false) {
			assert.NoError(t, err)
			entries = append(entries, entry)
		}

		assert.Len(t, entries, 2)
		names := lo.Map(entries, func(entry ObjectAttr, _ int) string { return entry.Key })
		assert.ElementsMatch(t, []string{path.Join(dir, "backup", "binlogs"), path.Join(dir, "backup", "meta")}, names)
	})
}
