package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/samber/lo"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/zilliztech/milvus-backup/internal/log"
)

const _deleteConcurrent = 10

func Size(ctx context.Context, cli Client, prefix string) (int64, error) {
	_, sizes, err := ListPrefixFlat(ctx, cli, prefix, true)
	if err != nil {
		return 0, err
	}

	return lo.Sum(sizes), nil
}

func ListPrefixFlat(ctx context.Context, cli Client, prefix string, recursive bool) ([]string, []int64, error) {
	iter, err := cli.ListPrefix(ctx, prefix, recursive)
	if err != nil {
		return nil, nil, err
	}
	defer iter.Close()

	var keys []string
	var sizes []int64
	for {
		attr, ok, err := iter.Next(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("storage: list prefix flat %w", err)
		}
		if !ok {
			break
		}
		keys = append(keys, attr.Key)
		sizes = append(sizes, attr.Length)
	}

	return keys, sizes, nil
}

// ExpectedDestObjects lists srcPrefix on src and returns the destination keys
// (mapped by replacing srcPrefix with destPrefix, matching CopyPrefixTask) to
// their sizes. The result feeds VerifyPrefixTask to verify a prefix copy.
// Directory markers (empty objects whose key ends with "/") are skipped,
// consistent with what CopyPrefixTask copies.
func ExpectedDestObjects(ctx context.Context, src Client, srcPrefix, destPrefix string) (map[string]int64, error) {
	keys, sizes, err := ListPrefixFlat(ctx, src, srcPrefix, true)
	if err != nil {
		return nil, fmt.Errorf("storage: expected dest objects list prefix %w", err)
	}

	expected := make(map[string]int64, len(keys))
	for idx, key := range keys {
		if sizes[idx] == 0 && strings.HasSuffix(key, "/") {
			continue
		}
		destKey := strings.Replace(key, srcPrefix, destPrefix, 1)
		expected[destKey] = sizes[idx]
	}

	return expected, nil
}

func DeletePrefix(ctx context.Context, cli Client, prefix string) error {
	if prefix == "" {
		return fmt.Errorf("storage: delete prefix empty prefix")
	}

	iter, err := cli.ListPrefix(ctx, prefix, true)
	if err != nil {
		return fmt.Errorf("storage: delete prefix list prefix %w", err)
	}
	defer iter.Close()

	// Derive a cancellable context so in-flight deletions can be stopped when
	// the loop bails early, then join them through the single Wait below.
	// defer cancel() satisfies vet's lostcancel check; it is a no-op on the
	// happy path where Wait has already joined every deletion.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	g, subCtx := errgroup.WithContext(ctx)
	g.SetLimit(_deleteConcurrent)

	var loopErr error
	for {
		attr, ok, err := iter.Next(ctx)
		if err != nil {
			loopErr = fmt.Errorf("storage: delete prefix iter object %w", err)
			break
		}
		if !ok {
			break
		}
		if !strings.HasPrefix(attr.Key, prefix) {
			loopErr = fmt.Errorf("storage: delete prefix key %s not in prefix %s", attr.Key, prefix)
			break
		}

		g.Go(func() error {
			log.Debug("delete object", zap.String("key", attr.Key))
			return cli.DeleteObject(subCtx, attr.Key)
		})
	}

	// If the loop bailed early, stop the in-flight deletions so Wait below
	// returns promptly instead of running them to completion.
	if loopErr != nil {
		cancel()
	}

	waitErr := g.Wait()
	if loopErr != nil {
		return loopErr
	}
	if waitErr != nil {
		return fmt.Errorf("storage: delete prefix %w", waitErr)
	}

	return nil
}

func Exist(ctx context.Context, cli Client, prefix string) (bool, error) {
	iter, err := cli.ListPrefix(ctx, prefix, false)
	if err != nil {
		return false, fmt.Errorf("storage: exist list prefix %w", err)
	}
	defer iter.Close()

	_, ok, err := iter.Next(ctx)
	if err != nil {
		return false, fmt.Errorf("storage: exist list prefix %w", err)
	}

	return ok, nil
}

func CreateBucketIfNotExist(ctx context.Context, cli Client, prefix string) error {
	exist, err := cli.BucketExist(ctx, prefix)
	if err != nil {
		return fmt.Errorf("storage: create bucket if not exist %w", err)
	}

	if exist {
		return nil
	}

	if err := cli.CreateBucket(ctx); err != nil {
		return fmt.Errorf("storage: create bucket if not exist %w", err)
	}

	return nil
}

func Read(ctx context.Context, cli Client, key string) ([]byte, error) {
	obj, err := cli.GetObject(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("storage: read to byte slice get object %w", err)
	}
	defer obj.Body.Close()

	byts, err := io.ReadAll(obj.Body)
	if err != nil {
		return nil, fmt.Errorf("storage: read to byte slice read all %w", err)
	}

	return byts, nil
}

func Write(ctx context.Context, cli Client, key string, body []byte) error {
	i := UploadObjectInput{Key: key, Body: bytes.NewReader(body), Size: int64(len(body))}
	if err := cli.UploadObject(ctx, i); err != nil {
		return fmt.Errorf("storage: write from byte slice upload object %w", err)
	}

	return nil
}
