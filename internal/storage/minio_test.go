package storage

import (
	"errors"
	"net/http"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
)

func TestSplitIntoParts(t *testing.T) {
	t.Run("TotalSizeLEMinPartSize", func(t *testing.T) {
		_, err := splitIntoParts(1024)
		assert.Error(t, err)

		_, err = splitIntoParts(_minPartSize)
		assert.Error(t, err)
	})

	t.Run("TotalSizeGTMinPartSize", func(t *testing.T) {
		// min + 1
		size := _minPartSize + 1
		parts, err := splitIntoParts(size)
		assert.NoError(t, err)
		assert.LessOrEqual(t, int64(len(parts)), _maxParts)
		assert.Equal(t, size, lo.SumBy(parts, func(p part) int64 { return p.Size }))

		// 1GB
		size = 1 * _GiB
		parts, err = splitIntoParts(size)
		assert.NoError(t, err)
		assert.LessOrEqual(t, int64(len(parts)), _maxParts)
		assert.Equal(t, size, lo.SumBy(parts, func(p part) int64 { return p.Size }))

		// 10GB
		size = 10 * _GiB
		parts, err = splitIntoParts(size)
		assert.NoError(t, err)
		assert.LessOrEqual(t, int64(len(parts)), size)
		assert.Equal(t, size, lo.SumBy(parts, func(p part) int64 { return p.Size }))

		// 100GB
		size = 100 * _GiB
		parts, err = splitIntoParts(size)
		assert.NoError(t, err)
		assert.LessOrEqual(t, int64(len(parts)), _maxParts)
		assert.Equal(t, size, lo.SumBy(parts, func(p part) int64 { return p.Size }))

		// 1TB
		size = 1 * _TiB
		parts, err = splitIntoParts(size)
		assert.NoError(t, err)
		assert.LessOrEqual(t, int64(len(parts)), _maxParts)
		assert.Equal(t, size, lo.SumBy(parts, func(p part) int64 { return p.Size }))

		// max - 1
		size = _maxMultiCopySize - 1
		parts, err = splitIntoParts(size)
		assert.NoError(t, err)
		assert.LessOrEqual(t, int64(len(parts)), _maxParts)
		assert.Equal(t, size, lo.SumBy(parts, func(p part) int64 { return p.Size }))
	})

	t.Run("TotalSizeGTMaxPartSize", func(t *testing.T) {
		_, err := splitIntoParts(_maxMultiCopySize + 1)
		assert.Error(t, err)
	})
}

func TestIsDeleteSuccessful(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"NilError", nil, true},
		{"Status200", minio.ErrorResponse{StatusCode: http.StatusOK}, true},
		{"Status404", minio.ErrorResponse{StatusCode: http.StatusNotFound}, false},
		{"Status403", minio.ErrorResponse{StatusCode: http.StatusForbidden}, false},
		{"Status500", minio.ErrorResponse{StatusCode: http.StatusInternalServerError}, false},
		{"NonMinioError", errors.New("error"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isDeleteSuccessful(tt.err))
		})
	}
}
