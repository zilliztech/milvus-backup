package server

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
)

// stubCheck stands in for app.Check: a canned error and the writer the handler
// handed it, so tests can assert what the handler fed the usecase.
type stubCheck struct {
	executeErr error
	output     io.Writer
	calls      int
}

func (s *stubCheck) Execute(_ context.Context, output io.Writer) error {
	s.output = output
	s.calls++
	if s.executeErr != nil {
		return s.executeErr
	}

	_, err := io.WriteString(output, "\nMilvus version: 2.6.0\nSuccess!\n")
	return err
}

// withCheck wires the stub as the check usecase. newErr simulates the
// client-construction failure, which happens before any Execute call.
func withCheck(stub *stubCheck, newErr error) Option {
	return func(c *config) {
		c.newCheck = func(context.Context, *v2.Config) (checkUC, error) {
			return stub, newErr
		}
	}
}

func getCheck(t *testing.T, s *Server) *httptest.ResponseRecorder {
	t.Helper()

	w := httptest.NewRecorder()
	s.engine.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/check", nil))

	return w
}

func TestHandleCheck(t *testing.T) {
	t.Run("RendersConfigThenUsecaseReport", func(t *testing.T) {
		stub := &stubCheck{}
		s := newListTestServer(t, withCheck(stub, nil))

		w := getCheck(t, s)

		require.Equal(t, http.StatusOK, w.Code)
		body := w.Body.String()
		configIdx := strings.Index(body, "Configuration:")
		reportIdx := strings.Index(body, "Milvus version: 2.6.0")
		assert.GreaterOrEqual(t, configIdx, 0)
		assert.Greater(t, reportIdx, configIdx)
		assert.Contains(t, body, "Success!")
		assert.Equal(t, 1, stub.calls)
	})

	t.Run("MapsConstructorErrorTo500", func(t *testing.T) {
		s := newListTestServer(t, withCheck(&stubCheck{}, errors.New("dial timeout")))

		w := getCheck(t, s)

		assert.Equal(t, http.StatusInternalServerError, w.Code)
		assert.Contains(t, w.Body.String(), "dial timeout")
	})

	t.Run("MapsExecuteErrorTo500", func(t *testing.T) {
		stub := &stubCheck{executeErr: errors.New("milvus unreachable")}
		s := newListTestServer(t, withCheck(stub, nil))

		w := getCheck(t, s)

		assert.Equal(t, http.StatusInternalServerError, w.Code)
		assert.Contains(t, w.Body.String(), "milvus unreachable")
		assert.Equal(t, 1, stub.calls)
	})
}
