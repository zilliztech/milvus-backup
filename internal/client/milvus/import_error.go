package milvus

import (
	"errors"
	"fmt"
	"strings"
)

var (
	ErrImportJobNotFound = errors.New("import job not found")
	ErrRemoteJobFailed   = errors.New("remote job failed")
)

const missingImportJobPrefix = "import job does not exist, jobID="

// ImportLookupError decodes the application status returned by GetImportState
// and jobs/import/describe. Both Milvus codes are shared with other errors.
func ImportLookupError(jobID string, code int, message string) error {
	if code == 0 {
		return nil
	}
	if code == 2100 || code == 2101 {
		reason := strings.TrimSuffix(message, ": importing data failed")
		reason = strings.TrimSuffix(reason, ": importing data failed on server side")
		if reason == missingImportJobPrefix+jobID {
			return fmt.Errorf("%w: %s", ErrImportJobNotFound, message)
		}
	}
	return fmt.Errorf("client: query import job %s (code %d): %s", jobID, code, message)
}

// ImportFailure decodes a successful lookup whose job state is Failed. GC
// between DataCoord's two reads puts the missing-ID message in the job reason.
func ImportFailure(jobID, reason string) error {
	if strings.HasPrefix(reason, missingImportJobPrefix) {
		if reason == missingImportJobPrefix+jobID {
			return fmt.Errorf("%w: %s", ErrImportJobNotFound, reason)
		}
		return fmt.Errorf("client: inconsistent failure for import job %s: %s", jobID, reason)
	}
	return fmt.Errorf("%w: %s", ErrRemoteJobFailed, reason)
}
