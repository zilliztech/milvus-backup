package storage

import (
	"bytes"
	"context"
	"errors"
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
	var keys []string
	var sizes []int64
	for attr, err := range cli.NewObjectIter(ctx, prefix, recursive) {
		if err != nil {
			return nil, nil, fmt.Errorf("storage: list prefix flat: %w", err)
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
		return nil, fmt.Errorf("storage: expected dest objects list src prefix: %w", err)
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
	return DeleteWithCallback(ctx, cli, prefix, nil)
}

// DeleteWithCallback is DeletePrefix with a progress hook: onDeleted runs
// once per successfully deleted object, with the count of that deletion —
// 1 on the per-key path, the batch size on the batch path. The per-key path
// fires it from concurrent delete workers, so it must be goroutine-safe; a
// nil onDeleted deletes silently.
//
// Clients implementing batchDeleter delete a whole batch per request. When
// the backend rejects the batch request with NotImplemented (a self-hosted
// S3-compatible gateway without multi-object delete), the run degrades to
// the per-key path: a rejected batch deleted nothing, and re-deleting is
// idempotent, so restarting the prefix per-key loses no work.
func DeleteWithCallback(ctx context.Context, cli Client, prefix string, onDeleted func(int)) error {
	if prefix == "" {
		return fmt.Errorf("storage: delete prefix: prefix is empty")
	}

	if bd, ok := cli.(batchDeleter); ok {
		err := deleteInBatches(ctx, cli, bd, prefix, onDeleted)
		if !errors.Is(err, errBatchDeleteUnsupported) {
			return err
		}
		log.Warn("storage: multi-object delete unsupported, degrade to per-key deletion",
			zap.String("prefix", prefix), zap.Error(err))
	}

	return deletePerKey(ctx, cli, prefix, onDeleted)
}

// deleteInBatches is DeleteWithCallback's batch path: the listing fills a
// batch up to the client's batch size, and each full batch is handed to
// deleteBatch for one DeleteObjects request. Unlike the per-key path there
// is no fan-out: one batched request does the work of a thousand single
// ones, so sequential batches already reach tens of thousands of keys per
// second and the loop stays free of the errgroup/cancel machinery the
// per-key path needs. Every batch is a freshly allocated slice handed off by
// value — the deleted one becomes garbage, so there is no aliasing between
// what is being deleted and what the listing is appending to.
func deleteInBatches(ctx context.Context, cli Client, bd batchDeleter, prefix string, onDeleted func(int)) error {
	batch := make([]string, 0, bd.DeleteObjectsBatchSize())
	for attr, err := range cli.NewObjectIter(ctx, prefix, true) {
		if err != nil {
			return fmt.Errorf("storage: delete prefix iter object: %w", err)
		}
		if !strings.HasPrefix(attr.Key, prefix) {
			return fmt.Errorf("storage: delete prefix key %s not under prefix %s", attr.Key, prefix)
		}

		batch = append(batch, attr.Key)
		if len(batch) == bd.DeleteObjectsBatchSize() {
			if err := deleteBatch(ctx, bd, batch, onDeleted); err != nil {
				return err
			}
			batch = make([]string, 0, bd.DeleteObjectsBatchSize())
		}
	}
	if len(batch) == 0 {
		return nil
	}

	return deleteBatch(ctx, bd, batch, onDeleted)
}

func deleteBatch(ctx context.Context, bd batchDeleter, keys []string, onDeleted func(int)) error {
	log.Debug("delete objects", zap.Int("count", len(keys)))
	if err := bd.DeleteObjects(ctx, keys); err != nil {
		return fmt.Errorf("storage: delete prefix: %w", err)
	}
	if onDeleted != nil {
		onDeleted(len(keys))
	}

	return nil
}

func deletePerKey(ctx context.Context, cli Client, prefix string, onDeleted func(int)) error {
	// Derive a cancellable context so in-flight deletions can be stopped when
	// the loop bails early, then join them through the single Wait below.
	// defer cancel() satisfies vet's lostcancel check; it is a no-op on the
	// happy path where Wait has already joined every deletion.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	g, subCtx := errgroup.WithContext(ctx)
	g.SetLimit(_deleteConcurrent)

	var loopErr error
	for attr, err := range cli.NewObjectIter(ctx, prefix, true) {
		if err != nil {
			loopErr = fmt.Errorf("storage: delete prefix iter object: %w", err)
			break
		}
		if !strings.HasPrefix(attr.Key, prefix) {
			loopErr = fmt.Errorf("storage: delete prefix key %s not under prefix %s", attr.Key, prefix)
			break
		}

		g.Go(func() error {
			log.Debug("delete object", zap.String("key", attr.Key))
			if err := cli.DeleteObject(subCtx, attr.Key); err != nil {
				return err
			}
			if onDeleted != nil {
				onDeleted(1)
			}
			return nil
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
		return fmt.Errorf("storage: delete prefix: %w", waitErr)
	}

	return nil
}

func Exist(ctx context.Context, cli Client, prefix string) (bool, error) {
	for _, err := range cli.NewObjectIter(ctx, prefix, false) {
		if err != nil {
			return false, fmt.Errorf("storage: exist list prefix: %w", err)
		}
		// One yielded object is enough to prove existence; returning here
		// stops the listing through the sequence itself.
		return true, nil
	}
	return false, nil
}

func CreateBucketIfNotExist(ctx context.Context, cli Client, prefix string) error {
	exist, err := cli.BucketExist(ctx, prefix)
	if err != nil {
		return fmt.Errorf("storage: create bucket if not exist: %w", err)
	}

	if exist {
		return nil
	}

	if err := cli.CreateBucket(ctx); err != nil {
		return fmt.Errorf("storage: create bucket if not exist: %w", err)
	}

	return nil
}

func Read(ctx context.Context, cli Client, key string) ([]byte, error) {
	obj, err := cli.GetObject(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("storage: read to byte slice get object: %w", err)
	}
	defer obj.Body.Close()

	byts, err := io.ReadAll(obj.Body)
	if err != nil {
		return nil, fmt.Errorf("storage: read to byte slice read all: %w", err)
	}

	return byts, nil
}

func Write(ctx context.Context, cli Client, key string, body []byte) error {
	i := UploadObjectInput{Key: key, Body: bytes.NewReader(body), Size: int64(len(body))}
	if err := cli.UploadObject(ctx, i); err != nil {
		return fmt.Errorf("storage: write from byte slice upload object: %w", err)
	}

	return nil
}
