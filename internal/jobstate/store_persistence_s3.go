package jobstate

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/storage"
)

type s3PersistenceBackend struct {
	config storage.Config
	prefix string
	mu     sync.Mutex
	client storage.Client
}

func newS3PersistenceBackend(endpoint, region, accessKey, secretKey, token string, secure bool, bucket, prefix string) (*s3PersistenceBackend, error) {
	if endpoint == "" || bucket == "" {
		return nil, fmt.Errorf("persistence: S3 endpoint and bucket are required")
	}
	cred := storage.Credential{Type: storage.Static, AK: accessKey, SK: secretKey, Token: token}
	if accessKey == "" && secretKey == "" {
		cred.Type = storage.IAM
	}
	return &s3PersistenceBackend{
		config: storage.Config{
			Provider:   cfg.ProviderS3,
			Endpoint:   endpoint,
			Region:     region,
			UseSSL:     secure,
			Bucket:     bucket,
			Credential: cred,
		},
		prefix: prefix,
	}, nil
}

// Construct on first use so region discovery uses the operation's context outside the Store lock.
func (s *s3PersistenceBackend) clientForKey(ctx context.Context, key string) (storage.Client, error) {
	if err := validatePersistenceKey(ctx, key); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.client == nil {
		cli, err := storage.NewClient(ctx, s.config)
		if err != nil {
			return nil, err
		}
		s.client = cli
	}
	return s.client, nil
}

func (s *s3PersistenceBackend) Load(ctx context.Context, key string) ([]byte, error) {
	cli, err := s.clientForKey(ctx, key)
	if err != nil {
		return nil, err
	}
	obj, err := cli.GetObject(ctx, path.Join(s.prefix, key))
	if err != nil {
		var missing *types.NoSuchKey
		if errors.As(err, &missing) {
			return nil, ErrPersistenceKeyNotFound
		}
		return nil, err
	}
	defer obj.Body.Close()
	return io.ReadAll(obj.Body)
}

func (s *s3PersistenceBackend) Save(ctx context.Context, key string, b []byte) error {
	cli, err := s.clientForKey(ctx, key)
	if err != nil {
		return err
	}
	return cli.UploadObject(ctx, storage.UploadObjectInput{Key: path.Join(s.prefix, key), Body: bytes.NewReader(b), Size: int64(len(b))})
}

func (s *s3PersistenceBackend) Delete(ctx context.Context, key string) error {
	cli, err := s.clientForKey(ctx, key)
	if err != nil {
		return err
	}
	return cli.DeleteObject(ctx, path.Join(s.prefix, key))
}

func (s *s3PersistenceBackend) Check(ctx context.Context) error {
	return checkPersistenceBackend(ctx, s)
}
