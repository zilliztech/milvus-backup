package log

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// captureStderr builds a logger with newStdLogger while stderr is a pipe, and
// returns everything fn logs through it. The logger resolves stderr when it is
// built, so it has to be built inside the redirect.
func captureStderr(t *testing.T, fn func(lg *zap.Logger)) string {
	t.Helper()

	r, w, err := os.Pipe()
	require.NoError(t, err)

	orig := os.Stderr
	os.Stderr = w
	defer func() { os.Stderr = orig }()

	lg, _ := newStdLogger()
	fn(lg)
	require.NoError(t, w.Close())

	out, err := io.ReadAll(r)
	require.NoError(t, err)

	return string(out)
}

func TestNewStdLogger(t *testing.T) {
	// The configuration is read before the log settings inside it can be
	// applied, so the logger in use at that point is this one. Sending it
	// nowhere would discard exactly the warnings raised while reading.
	t.Run("WritesToStderr", func(t *testing.T) {
		out := captureStderr(t, func(lg *zap.Logger) {
			lg.Warn("cfg: unknown v2 config file key")
		})

		assert.Contains(t, out, "cfg: unknown v2 config file key")
		assert.Contains(t, out, "[WARN]")
	})

	// A caller skip here would be one too many: the package-level Warn and
	// friends already add one for their own frame. The assertion is on the
	// exact line, because an extra skip lands on captureStderr — still in this
	// file, so matching the file name alone would pass either way.
	t.Run("ReportsTheCallSite", func(t *testing.T) {
		var warnLine int
		out := captureStderr(t, func(lg *zap.Logger) {
			_, _, line, _ := runtime.Caller(0)
			warnLine = line + 2
			lg.Warn("from the test")
		})

		assert.Contains(t, out, fmt.Sprintf("log/log_test.go:%d", warnLine))
	})
}
