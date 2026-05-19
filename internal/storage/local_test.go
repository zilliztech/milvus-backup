package storage

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
)

func newTestLocalClient(t *testing.T) *LocalClient {
	t.Helper()
	return newLocalClient(Config{LocalPath: t.TempDir(), Bucket: "test-bucket"})
}

func TestLocalClient_BucketExist(t *testing.T) {
	t.Run("NotExist", func(t *testing.T) {
		cli := newTestLocalClient(t)
		exist, err := cli.BucketExist(context.Background(), "")
		assert.NoError(t, err)
		assert.False(t, exist)
	})

	t.Run("Exist", func(t *testing.T) {
		cli := newTestLocalClient(t)
		err := os.MkdirAll(cli.baseDir, 0755)
		assert.NoError(t, err)

		exist, err := cli.BucketExist(context.Background(), "")
		assert.NoError(t, err)
		assert.True(t, exist)
	})
}

func TestLocalClient_CreateBucket(t *testing.T) {
	cli := newTestLocalClient(t)
	err := cli.CreateBucket(context.Background())
	assert.NoError(t, err)
	assert.DirExists(t, cli.baseDir)
}

func TestLocalClient_UploadObject(t *testing.T) {
	cli := newTestLocalClient(t)
	err := cli.CreateBucket(context.Background())
	assert.NoError(t, err)

	key := "subdir/test.txt"
	err = cli.UploadObject(context.Background(), UploadObjectInput{
		Key:  key,
		Body: bytes.NewReader([]byte{0}),
		Size: 1,
	})
	assert.NoError(t, err)

	absPath := cli.absPath(key)
	assert.FileExists(t, absPath)

	file, err := os.Open(absPath)
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
	cli := newTestLocalClient(t)
	err := cli.CreateBucket(context.Background())
	assert.NoError(t, err)

	key := "test.txt"
	absPath := cli.absPath(key)
	err = os.WriteFile(absPath, []byte{0}, 0644)
	assert.NoError(t, err)
	assert.FileExists(t, absPath)

	err = cli.DeleteObject(context.Background(), key)
	assert.NoError(t, err)
	assert.NoFileExists(t, absPath)
}

func TestLocalClient_HeadObject(t *testing.T) {
	cli := newTestLocalClient(t)
	err := cli.CreateBucket(context.Background())
	assert.NoError(t, err)

	key := "test.txt"
	absPath := cli.absPath(key)
	err = os.WriteFile(absPath, []byte{0}, 0644)
	assert.NoError(t, err)

	attr, err := cli.HeadObject(context.Background(), key)
	assert.NoError(t, err)
	assert.Equal(t, key, attr.Key)
	assert.Equal(t, int64(1), attr.Length)
}

func TestLocalClient_GetObject(t *testing.T) {
	cli := newTestLocalClient(t)
	err := cli.CreateBucket(context.Background())
	assert.NoError(t, err)

	key := "test.txt"
	absPath := cli.absPath(key)
	err = os.WriteFile(absPath, []byte{0}, 0644)
	assert.NoError(t, err)

	obj, err := cli.GetObject(context.Background(), key)
	assert.NoError(t, err)
	defer obj.Body.Close()

	assert.Equal(t, int64(1), obj.Length)
	content, err := io.ReadAll(obj.Body)
	assert.NoError(t, err)
	assert.Equal(t, []byte{0}, content)
}

func TestLocalClient_CopyObject(t *testing.T) {
	srcCli := newTestLocalClient(t)
	err := srcCli.CreateBucket(context.Background())
	assert.NoError(t, err)

	srcKey := "test.txt"
	err = os.WriteFile(srcCli.absPath(srcKey), []byte{0}, 0644)
	assert.NoError(t, err)

	destCli := newLocalClient(Config{LocalPath: t.TempDir(), Bucket: "dest-bucket"})
	err = destCli.CreateBucket(context.Background())
	assert.NoError(t, err)

	destKey := "backup/test.txt"
	err = destCli.CopyObject(context.Background(), CopyObjectInput{
		SrcCli:  srcCli,
		SrcAttr: ObjectAttr{Key: srcKey, Length: 1},
		DestKey: destKey,
	})
	assert.NoError(t, err)
	assert.FileExists(t, destCli.absPath(destKey))
}

func TestLocalClient_ListPrefix(t *testing.T) {
	cli := newTestLocalClient(t)
	err := cli.CreateBucket(context.Background())
	assert.NoError(t, err)

	keys := []string{
		filepath.Join("backup", "meta", "test.txt"),
		filepath.Join("backup", "meta", "test2.txt"),
		filepath.Join("backup", "meta", "test3.txt"),
		filepath.Join("backup", "binlogs", "test.txt"),
		filepath.Join("backup", "binlogs", "test2.txt"),
		filepath.Join("backup", "binlogs", "test3.txt"),
	}

	for _, key := range keys {
		abs := cli.absPath(key)
		err := os.MkdirAll(filepath.Dir(abs), 0755)
		assert.NoError(t, err)
		err = os.WriteFile(abs, []byte{0}, 0644)
		assert.NoError(t, err)
	}

	t.Run("PrefixIsFile", func(t *testing.T) {
		key := filepath.Join("backup", "meta", "test.txt")
		iter, err := cli.ListPrefix(context.Background(), key, false)
		assert.NoError(t, err)

		assert.True(t, iter.HasNext())
		entry, err := iter.Next()
		assert.NoError(t, err)
		assert.Equal(t, key, entry.Key)
		assert.Equal(t, int64(1), entry.Length)

		iter, err = cli.ListPrefix(context.Background(), key, true)
		assert.NoError(t, err)
		assert.True(t, iter.HasNext())
		entry, err = iter.Next()
		assert.NoError(t, err)
		assert.Equal(t, key, entry.Key)
		assert.Equal(t, int64(1), entry.Length)
	})

	t.Run("Empty", func(t *testing.T) {
		iter, err := cli.ListPrefix(context.Background(), "not_exist", false)
		assert.NoError(t, err)
		assert.False(t, iter.HasNext())

		iter, err = cli.ListPrefix(context.Background(), "not_exist", true)
		assert.NoError(t, err)
		assert.False(t, iter.HasNext())
	})

	t.Run("Recursive", func(t *testing.T) {
		iter, err := cli.ListPrefix(context.Background(), "backup", true)
		assert.NoError(t, err)

		var entries []ObjectAttr
		for iter.HasNext() {
			entry, err := iter.Next()
			assert.NoError(t, err)
			entries = append(entries, entry)
		}

		assert.Len(t, entries, 6)
		names := lo.Map(entries, func(entry ObjectAttr, _ int) string { return entry.Key })
		assert.ElementsMatch(t, keys, names)
	})

	t.Run("NonRecursive", func(t *testing.T) {
		iter, err := cli.ListPrefix(context.Background(), "backup", false)
		assert.NoError(t, err)

		var entries []ObjectAttr
		for iter.HasNext() {
			entry, err := iter.Next()
			assert.NoError(t, err)
			entries = append(entries, entry)
		}
		assert.Len(t, entries, 2)
		names := lo.Map(entries, func(entry ObjectAttr, _ int) string { return entry.Key })
		assert.ElementsMatch(t, []string{filepath.Join("backup", "binlogs"), filepath.Join("backup", "meta")}, names)
	})
}
