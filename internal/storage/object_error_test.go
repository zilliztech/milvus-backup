package storage

import (
	"fmt"
	"testing"

	gcs "cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
)

func TestIsObjectNotFound(t *testing.T) {
	for _, err := range []error{gcs.ErrObjectNotExist, &types.NoSuchKey{}, minio.ErrorResponse{Code: "NoSuchKey"}, &azcore.ResponseError{ErrorCode: "BlobNotFound"}} {
		assert.True(t, IsObjectNotFound(fmt.Errorf("lookup: %w", err)))
	}
	for _, err := range []error{gcs.ErrBucketNotExist, &types.NoSuchBucket{}, minio.ErrorResponse{Code: "AccessDenied"}, &azcore.ResponseError{ErrorCode: "ContainerNotFound"}} {
		assert.False(t, IsObjectNotFound(fmt.Errorf("lookup: %w", err)))
	}
}
