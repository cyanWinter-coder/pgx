package pgx

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func TestCopyFromConnectionBufferIsReused(t *testing.T) {
	if os.Getenv("PGX_TEST_DATABASE") == "" {
		t.Skip("PGX_TEST_DATABASE is not set")
	}

	ctx := context.Background()
	conn := mustConnect(t, mustParseConfig(t, os.Getenv("PGX_TEST_DATABASE")))
	defer conn.Close(ctx)

	_, err := conn.Exec(ctx, `create temporary table uid505_buffer_reuse(
		id bigint,
		data text not null
	)`)
	require.NoError(t, err)

	large := strings.Repeat("x", 100_000)
	errBufferFailure := errors.New("buffer failure")
	var row int
	_, err = conn.CopyFrom(ctx, Identifier{"uid505_buffer_reuse"}, []string{"id", "data"},
		CopyFromFunc(func() ([]any, error) {
			row++
			if row == 2 {
				return nil, errBufferFailure
			}
			return []any{int64(row), large}, nil
		}),
	)
	require.ErrorIs(t, err, errBufferFailure)

	firstPointer := unsafe.SliceData(conn.wbuf)
	firstCapacity := cap(conn.wbuf)
	require.Greater(t, firstCapacity, 65536)
	require.NotNil(t, firstPointer)

	_, err = conn.CopyFrom(ctx, Identifier{"uid505_buffer_reuse"}, []string{"id", "data"},
		CopyFromRows([][]any{{int64(1), large}}))
	require.NoError(t, err)
	require.Equal(t, firstCapacity, cap(conn.wbuf))
	require.Equal(t, firstPointer, unsafe.SliceData(conn.wbuf), "successful COPY must reuse the connection buffer")

	row = 0
	_, err = conn.CopyFrom(ctx, Identifier{"uid505_buffer_reuse"}, []string{"id", "data"},
		CopyFromFunc(func() ([]any, error) {
			row++
			if row == 2 {
				return nil, errBufferFailure
			}
			return []any{int64(row), large}, nil
		}),
	)
	require.ErrorIs(t, err, errBufferFailure)
	require.Equal(t, firstCapacity, cap(conn.wbuf))
	require.Equal(t, firstPointer, unsafe.SliceData(conn.wbuf), "failed COPY must return the buffer for reuse")
}
