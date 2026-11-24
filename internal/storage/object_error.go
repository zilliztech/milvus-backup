package storage

import (
	"errors"

	gcs "cloud.google.com/go/storage"
	"github.com/aws/smithy-go"
	"github.com/minio/minio-go/v7"
)

// IsObjectNotFound recognizes a missing key without treating an inaccessible
// bucket or a bad credential as a missing object.
func IsObjectNotFound(err error) bool {
	if errors.Is(err, gcs.ErrObjectNotExist) || isBlobNotFound(err) {
		return true
	}
	var awsErr smithy.APIError
	if errors.As(err, &awsErr) && awsErr.ErrorCode() == "NoSuchKey" {
		return true
	}
	var xmlErr minio.ErrorResponse
	return errors.As(err, &xmlErr) && xmlErr.Code == "NoSuchKey"
}
