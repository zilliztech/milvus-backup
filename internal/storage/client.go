package storage

import (
	"context"
	"errors"
	"io"
	"iter"

	minioCred "github.com/minio/minio-go/v7/pkg/credentials"
	"golang.org/x/oauth2"
)

type CopyObjectInput struct {
	SrcCli Client

	SrcAttr ObjectAttr

	DestKey string
}

type UploadObjectInput struct {
	Key  string
	Body io.Reader

	// The size of the file to be uploaded, if unknown, set to 0 or negative
	// Configuring this parameter can help reduce memory usage.
	Size int64
}

type Object struct {
	Length int64
	Body   io.ReadCloser
}

type ObjectAttr struct {
	Key    string
	Length int64
}

func (o *ObjectAttr) IsEmpty() bool { return o.Length == 0 }

type Config struct {
	Provider string

	Endpoint string
	UseSSL   bool
	Region   string

	// MilvusEndpoint is the endpoint the Milvus server itself uses to reach
	// this storage, when it differs from Endpoint: a container port mapping, a
	// private link, or an internal DNS name gives one store two endpoints.
	// Snapshot URIs handed to Milvus name it, since Milvus connects to it.
	// Empty means Endpoint serves both.
	MilvusEndpoint string

	Credential Credential

	Bucket string

	// SourceSAS is a read-scoped SAS that authorizes reading the copy source
	// when a snapshot-format copy crosses Azure storage accounts: no single
	// credential can read another account's blobs, so the destination-side
	// Credential never covers the source side. It is set from the source
	// account's own config (its explicit token, or one minted from its
	// credential) and rendered into the extfs handed to Milvus alongside the
	// destination credentials.
	SourceSAS string

	// MultipartCopyThresholdMiB is the file size threshold above which multipart copy is used.
	// Default is 500 MiB if not set. GCP does not support multipart copy.
	MultipartCopyThresholdMiB int64
}

type Credential struct {
	Type CredentialType

	// Static credential
	AK    string
	SK    string
	Token string

	// IAM
	IAMEndpoint string

	// GCPCredJSON
	GCPCredJSON string

	// MinioCredential
	MinioCredProvider minioCred.Provider

	// OAuth2 TokenSource
	OAuth2TokenSource oauth2.TokenSource

	// Azure Specific
	AzureAccountName string
}

type CredentialType uint8

const (
	Unknown CredentialType = iota
	Static
	IAM

	// GCPCredJSON For GCPNative storage. pass the json file path to the GCPNative storage.
	GCPCredJSON
	// MinioCredProvider For S3 compatible storage (now only support minio, aws),
	// pass a struct which implements minioCred.Provider
	MinioCredProvider
	// OAuth2TokenSource for some object storage which need OAuth2 to auth.
	OAuth2TokenSource
)

func (c CredentialType) String() string {
	switch c {
	case Static:
		return "Static"
	case IAM:
		return "IAM"
	case GCPCredJSON:
		return "GCPCredJSON"
	case MinioCredProvider:
		return "MinioCredProvider"
	case OAuth2TokenSource:
		return "OAuth2TokenSource"
	case Unknown:
		return "Unknown"
	}

	return "Can not find the credential type"
}

// Client is the interface for storage service.
// All implementations should include retry logic internally for idempotent operations.
type Client interface {
	Config() Config

	// CopyObject copy an object from src to dest, call on dest client.
	// The implementation of CopyObject must directly use the copy API provided by the service provider,
	CopyObject(ctx context.Context, i CopyObjectInput) error
	// HeadObject determine if an object exists, and you have permission to access it.
	HeadObject(ctx context.Context, key string) (ObjectAttr, error)
	// GetObject get an object
	GetObject(ctx context.Context, key string) (*Object, error)
	// UploadObject stream upload an object
	UploadObject(ctx context.Context, i UploadObjectInput) error
	// DeleteObject delete an object
	DeleteObject(ctx context.Context, key string) error

	// NewObjectIter returns an iterator over the objects sharing prefix, as a
	// range-over-function sequence:
	//
	//	for attr, err := range cli.NewObjectIter(ctx, prefix, true) {
	//		if err != nil {
	//			return err
	//		}
	//		// use attr
	//	}
	//
	// The sequence is driven by the range: no request is made until the first
	// iteration, so construction cannot fail, and listing errors surface as a
	// non-nil err — the last value yielded before the sequence stops.
	// Exhaustion simply ends the range.
	//
	// Leaving the loop early — break, return, or panic — stops the listing
	// and releases its resources. A provider-backed listing may hold a
	// background goroutine; ending the range is what stops it, so there is no
	// Close to call and an early return leaks nothing.
	//
	// Each range over the returned sequence runs an independent listing.
	NewObjectIter(ctx context.Context, prefix string, recursive bool) iter.Seq2[ObjectAttr, error]

	// BucketExist use a prefix to check if bucket exist.
	// Using a prefix to confirm whether a bucket exists can avoid requesting the head Bucket permission.
	BucketExist(ctx context.Context, prefix string) (bool, error)
	// CreateBucket create a bucket.
	CreateBucket(ctx context.Context) error
}

// errBatchDeleteUnsupported marks a DeleteObjects rejection that retrying
// cannot fix — the backend answered NotImplemented — so DeleteWithCallback
// degrades to per-key deletion instead of failing the run.
var errBatchDeleteUnsupported = errors.New("storage: batch delete unsupported by the backend")

// batchDeleter is an optional Client capability: a multi-object delete that
// removes a batch of keys in one request, like S3 DeleteObjects.
// DeleteWithCallback probes for it with a type assertion; clients without it
// take the per-key fan-out path. Clients embedding the Client interface (the
// GCP wrapper) do not promote these methods, which is exactly what keeps the
// probe from selecting a batch API the backend does not have.
type batchDeleter interface {
	// DeleteObjects deletes keys in one request. The caller never passes more
	// than DeleteObjectsBatchSize keys. A request-level rejection the backend
	// cannot fix by retrying (NotImplemented) must wrap
	// errBatchDeleteUnsupported so the caller degrades instead of retrying.
	DeleteObjects(ctx context.Context, keys []string) error

	// DeleteObjectsBatchSize is the maximum number of keys one DeleteObjects
	// call accepts: 1000 for S3 DeleteObjects. Backends with a different
	// ceiling (Azure Blob Batch allows 256) report their own.
	DeleteObjectsBatchSize() int
}
