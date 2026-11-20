package jobstate

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type s3PersistenceBackend struct {
	Client         *minio.Client
	Bucket, Prefix string
}

func newS3PersistenceBackend(endpoint, region, accessKey, secretKey, token string, secure bool, bucket, prefix string) (*s3PersistenceBackend, error) {
	if endpoint == "" || bucket == "" {
		return nil, fmt.Errorf("persistence: S3 endpoint and bucket are required")
	}
	creds := credentials.NewStaticV4(accessKey, secretKey, token)
	if accessKey == "" && secretKey == "" {
		creds = credentials.NewIAM("")
	}
	cli, err := minio.New(endpoint, &minio.Options{Creds: creds, Secure: secure, Region: region})
	if err != nil {
		return nil, err
	}
	return &s3PersistenceBackend{Client: cli, Bucket: bucket, Prefix: prefix}, nil
}
func (s *s3PersistenceBackend) Load(ctx context.Context, key string) ([]byte, error) {
	if err := validatePersistenceKey(ctx, key); err != nil {
		return nil, err
	}
	obj, err := s.Client.GetObject(ctx, s.Bucket, path.Join(s.Prefix, key), minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = obj.Close() }()
	b, err := io.ReadAll(obj)
	if minio.ToErrorResponse(err).Code == "NoSuchKey" {
		return nil, ErrPersistenceKeyNotFound
	}
	return b, err
}
func (s *s3PersistenceBackend) Save(ctx context.Context, key string, b []byte) error {
	if err := validatePersistenceKey(ctx, key); err != nil {
		return err
	}
	_, err := s.Client.PutObject(ctx, s.Bucket, path.Join(s.Prefix, key), bytes.NewReader(b), int64(len(b)), minio.PutObjectOptions{ContentType: "application/octet-stream", DisableMultipart: true})
	return err
}
func (s *s3PersistenceBackend) Delete(ctx context.Context, key string) error {
	if err := validatePersistenceKey(ctx, key); err != nil {
		return err
	}
	return s.Client.RemoveObject(ctx, s.Bucket, path.Join(s.Prefix, key), minio.RemoveObjectOptions{})
}

func (s *s3PersistenceBackend) Check(ctx context.Context) error {
	return checkPersistenceBackend(ctx, s)
}
