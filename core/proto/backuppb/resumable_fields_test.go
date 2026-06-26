package backuppb

import (
	"encoding/json"
	"testing"
)

// The server binds the REST body with gin's ShouldBindJSON (encoding/json over
// the struct json tags). This guards that a curl JSON body using the documented
// snake_case keys actually populates the resumable-restore fields.
func TestRestoreBackupRequest_ResumableJSONBinding(t *testing.T) {
	body := `{
		"backup_name": "rrtest2",
		"useV2Restore": true,
		"breakpoint": "apple_recover1",
		"resume": true,
		"segments_per_batch": 20,
		"max_retry": 5,
		"retry_backoff_sec": 3,
		"retry_max_backoff_sec": 20
	}`

	var req RestoreBackupRequest
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if got := req.GetBreakpoint(); got != "apple_recover1" {
		t.Errorf("breakpoint = %q, want apple_recover1", got)
	}
	if !req.GetResume() {
		t.Error("resume = false, want true")
	}
	if got := req.GetSegmentsPerBatch(); got != 20 {
		t.Errorf("segments_per_batch = %d, want 20", got)
	}
	if got := req.GetMaxRetry(); got != 5 {
		t.Errorf("max_retry = %d, want 5", got)
	}
	if got := req.GetRetryBackoffSec(); got != 3 {
		t.Errorf("retry_backoff_sec = %d, want 3", got)
	}
	if got := req.GetRetryMaxBackoffSec(); got != 20 {
		t.Errorf("retry_max_backoff_sec = %d, want 20", got)
	}
	// sanity: existing field still binds
	if !req.GetUseV2Restore() {
		t.Error("useV2Restore = false, want true")
	}
}
