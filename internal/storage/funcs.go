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

	var keys []string
	var sizes []int64
	for iter.HasNext() {
		attr, err := iter.Next()
		if err != nil {
			return nil, nil, fmt.Errorf("storage: list prefix flat %w", err)
		}
		keys = append(keys, attr.Key)
		sizes = append(sizes, attr.Length)
	}

	return keys, sizes, nil
}

func DeletePrefix(ctx context.Context, cli Client, prefix string) error {
	if prefix == "" {
		return fmt.Errorf("storage: delete prefix empty prefix")
	}

	iter, err := cli.ListPrefix(ctx, prefix, true)
	if err != nil {
		return fmt.Errorf("storage: delete prefix list prefix %w", err)
	}

	g, subCtx := errgroup.WithContext(ctx)
	g.SetLimit(_deleteConcurrent)
	for iter.HasNext() {
		attr, err := iter.Next()
		if err != nil {
			return fmt.Errorf("storage: delete prefix iter object %w", err)
		}
		if !strings.HasPrefix(attr.Key, prefix) {
			return fmt.Errorf("storage: delete prefix key %s not in prefix %s", attr.Key, prefix)
		}

		g.Go(func() error {
			log.Debug("delete object", zap.String("key", attr.Key))
			return cli.DeleteObject(subCtx, attr.Key)
		})
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("storage: delete prefix %w", err)
	}

	return nil
}

func Exist(ctx context.Context, cli Client, prefix string) (bool, error) {
	iter, err := cli.ListPrefix(ctx, prefix, false)
	if err != nil {
		return false, fmt.Errorf("storage: exist list prefix %w", err)
	}

	if !iter.HasNext() {
		return false, nil
	}

	return true, nil
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
