package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"cloud.google.com/go/storage"
	"go.uber.org/zap"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/util/retry"
)

var _ Client = (*GCPNativeClient)(nil)

type GCPNativeClient struct {
	client *storage.Client

	projectID string
	cfg       Config
}

func (gcm *GCPNativeClient) UploadObject(ctx context.Context, i UploadObjectInput) error {
	wc := gcm.client.Bucket(gcm.cfg.Bucket).Object(i.Key).NewWriter(ctx)
	defer func() {
		if err := wc.Close(); err != nil {
			log.Error("Failed to close writer", zap.Error(err))
		}
	}()

	if _, err := io.Copy(wc, i.Body); err != nil {
		return fmt.Errorf("storage: gcp native upload object %w", err)
	}

	return nil
}

func (gcm *GCPNativeClient) CopyObject(ctx context.Context, i CopyObjectInput) error {
	srcCli, ok := i.SrcCli.(*GCPNativeClient)
	if !ok {
		return fmt.Errorf("storage: gcp native copy object only support gcp native client")
	}

	srcObj := gcm.client.Bucket(srcCli.cfg.Bucket).Object(i.SrcKey)
	dstObj := gcm.client.Bucket(gcm.cfg.Bucket).Object(i.DestKey)

	if _, err := dstObj.CopierFrom(srcObj).Run(ctx); err != nil {
		return fmt.Errorf("storage: gcp native copy object from %s to %s: %w", i.SrcKey, i.DestKey, err)
	}

	return nil
}

func (gcm *GCPNativeClient) HeadObject(ctx context.Context, key string) (ObjectAttr, error) {
	obj := gcm.client.Bucket(gcm.cfg.Bucket).Object(key)
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		return ObjectAttr{}, fmt.Errorf("storage: gcp native head object %w", err)
	}

	return ObjectAttr{Key: key, Length: attrs.Size}, nil
}

func (gcm *GCPNativeClient) GetObject(ctx context.Context, key string) (*Object, error) {
	reader, err := gcm.client.Bucket(gcm.cfg.Bucket).Object(key).NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("storage: gcp native get object %w", err)
	}

	return &Object{Length: reader.Attrs.Size, Body: reader}, nil
}

func (gcm *GCPNativeClient) DeleteObject(ctx context.Context, key string) error {
	return retry.Do(ctx, func() error {
		if err := gcm.client.Bucket(gcm.cfg.Bucket).Object(key).Delete(ctx); err != nil {
			return fmt.Errorf("storage: gcp native delete object %w", err)
		}
		return nil
	})
}

type GcpNativeObjectIterator struct {
	cli *GCPNativeClient

	iter *storage.ObjectIterator

	hasNext bool
	nextObj *storage.ObjectAttrs
}

func (g *GcpNativeObjectIterator) HasNext() bool { return g.hasNext }

func (g *GcpNativeObjectIterator) Next() (ObjectAttr, error) {
	currObj := g.nextObj
	next, err := g.iter.Next()
	if err != nil {
		if errors.Is(err, iterator.Done) {
			g.hasNext = false
			return ObjectAttr{Key: currObj.Name, Length: currObj.Size}, nil
		}
		return ObjectAttr{}, fmt.Errorf("storage: gcp native list prefix %w", err)
	}

	g.nextObj = next
	g.hasNext = true

	return ObjectAttr{Key: currObj.Name, Length: currObj.Size}, nil

}

func (gcm *GCPNativeClient) ListPrefix(ctx context.Context, prefix string, recursive bool) (ObjectIterator, error) {
	delimiter := ""
	if !recursive {
		delimiter = "/"
	}

	iter := gcm.client.Bucket(gcm.cfg.Bucket).Objects(ctx, &storage.Query{
		Prefix:    prefix,
		Delimiter: delimiter,
	})

	next, err := iter.Next()
	if err != nil {
		if errors.Is(err, iterator.Done) {
			return &GcpNativeObjectIterator{cli: gcm, iter: iter, hasNext: false}, nil
		}
		return nil, fmt.Errorf("storage: gcp native list prefix %w", err)
	}

	return &GcpNativeObjectIterator{cli: gcm, iter: iter, nextObj: next, hasNext: true}, nil
}

func (gcm *GCPNativeClient) BucketExist(ctx context.Context, _ string) (bool, error) {
	bucket := gcm.client.Bucket(gcm.cfg.Bucket)
	_, err := bucket.Attrs(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrBucketNotExist) {
			return false, nil
		} else {
			return false, fmt.Errorf("storage: gcp native get bucket attrs %w", err)
		}
	}

	return true, nil
}

func (gcm *GCPNativeClient) CreateBucket(ctx context.Context) error {
	if err := gcm.client.Bucket(gcm.cfg.Bucket).Create(ctx, gcm.projectID, nil); err != nil {
		return fmt.Errorf("storage: gcp native create bucket %w", err)
	}
	return nil
}

func (gcm *GCPNativeClient) Config() Config {
	return gcm.cfg
}

func newGCPNativeClient(ctx context.Context, cfg Config) (*GCPNativeClient, error) {
	var opts []option.ClientOption
	if cfg.Endpoint != "" {
		address := "https://"
		if !cfg.UseSSL {
			address = "http://"
		}

		address = address + cfg.Endpoint + "/storage/v1/"
		opts = append(opts, option.WithEndpoint(address))
	}

	// Read the credentials file
	jsonData, err := os.ReadFile(cfg.Credential.GCPCredJSON)
	if err != nil {
		return nil, fmt.Errorf("unable to read credentials file:: %v", err)
	}

	creds, err := google.CredentialsFromJSON(ctx, jsonData, storage.ScopeReadWrite)
	if err != nil {
		return nil, fmt.Errorf("failed to create credentials from JSON: %v", err)

	}
	projectID, err := getProjectID(jsonData)
	if err != nil {
		return nil, fmt.Errorf("storage get project id: %w", err)
	}
	opts = append(opts, option.WithCredentials(creds))

	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("storage: create gcp native client: %w", err)
	}

	return &GCPNativeClient{client: client, cfg: cfg, projectID: projectID}, nil
}

func getProjectID(byts []byte) (string, error) {
	var data map[string]any
	if err := json.Unmarshal(byts, &data); err != nil {
		return "", fmt.Errorf("storage: parse gcp credential json file :%w", err)
	}

	propertyValue, ok := data["project_id"]
	if !ok {
		return "", fmt.Errorf("storage: gcp native get project id, project_id not found in credentials file")
	}

	projectID := fmt.Sprintf("%v", propertyValue)
	return projectID, nil
}
