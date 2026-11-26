package pgx_test

import (
	"context"
	"database/sql/driver"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var errUID505EncodeFailure = errors.New("uid505: encode row failed")

type uid505EncodeFailureValuer struct{}

func (uid505EncodeFailureValuer) Value() (driver.Value, error) {
	return nil, errUID505EncodeFailure
}

type uid505BlockingCopySource struct {
	ctx     context.Context
	started chan struct{}
	once    sync.Once

	sent int
	err  error
}

func (s *uid505BlockingCopySource) Next() bool {
	if s.sent < 2 {
		s.sent++
		return true
	}

	s.once.Do(func() { close(s.started) })
	<-s.ctx.Done()
	s.err = s.ctx.Err()
	return false
}

func (s *uid505BlockingCopySource) Values() ([]any, error) {
	return []any{int64(s.sent), strings.Repeat("x", 100_000)}, nil
}

func (s *uid505BlockingCopySource) Err() error {
	return s.err
}

func uid505TestConn(t *testing.T) *pgx.Conn {
	t.Helper()
	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	t.Cleanup(func() { closeConn(t, conn) })
	return conn
}

func uid505AssertReusable(t *testing.T, conn *pgx.Conn, table string) {
	t.Helper()

	require.False(t, conn.IsClosed())
	ensureConnValid(t, conn)

	var rowCount int64
	err := conn.QueryRow(context.Background(), "select count(*) from "+table).Scan(&rowCount)
	require.NoError(t, err)
	require.Zero(t, rowCount)

	_, err = conn.CopyFrom(
		context.Background(),
		pgx.Identifier{table},
		[]string{"id", "data"},
		pgx.CopyFromRows([][]any{{int64(1), "after recovery"}}),
	)
	require.NoError(t, err)

	var data string
	err = conn.QueryRow(context.Background(), "select data from "+table+" where id = 1").Scan(&data)
	require.NoError(t, err)
	require.Equal(t, "after recovery", data)
}

func TestUID505CopyFromCancelDrainsAndReusesConn(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn := uid505TestConn(t)
	mustExec(t, conn, `create temporary table uid505_copy_cancel(
		id bigint,
		data text not null
	)`)

	source := &uid505BlockingCopySource{ctx: ctx, started: make(chan struct{})}
	go func() {
		select {
		case <-source.started:
			cancel()
		case <-ctx.Done():
		}
	}()

	copied, err := conn.CopyFrom(ctx, pgx.Identifier{"uid505_copy_cancel"}, []string{"id", "data"}, source)
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
	require.EqualValues(t, 2, copied, "rows already written before cancellation")

	var pgErr *pgconn.PgError
	require.False(t, errors.As(err, &pgErr), "client cancellation must not be classified as a server rejection: %v", err)

	uid505AssertReusable(t, conn, "uid505_copy_cancel")
}

func TestUID505CopyFromEncodingFailurePreservesCauseAndReusesConn(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn := uid505TestConn(t)
	mustExec(t, conn, `create temporary table uid505_copy_encode(
		id bigint,
		data text not null
	)`)

	var row int
	copied, err := conn.CopyFrom(ctx, pgx.Identifier{"uid505_copy_encode"}, []string{"id", "data"},
		pgx.CopyFromFunc(func() ([]any, error) {
			row++
			if row == 3 {
				return []any{int64(row), uid505EncodeFailureValuer{}}, nil
			}
			return []any{int64(row), strings.Repeat("x", 100_000)}, nil
		}),
	)

	require.Error(t, err)
	require.ErrorIs(t, err, errUID505EncodeFailure)
	require.EqualValues(t, 2, copied, "rows written before the encoding failure")

	var pgErr *pgconn.PgError
	require.False(t, errors.As(err, &pgErr), "client encoding failure must retain its own cause: %v", err)

	uid505AssertReusable(t, conn, "uid505_copy_encode")
}

func TestUID505CopyFromServerRejectionIsDistinctFromCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn := uid505TestConn(t)
	mustExec(t, conn, `create temporary table uid505_copy_server(
		id bigint primary key,
		data text not null
	)`)
	mustExec(t, conn, `insert into uid505_copy_server values (2, 'existing')`)

	copied, err := conn.CopyFrom(ctx, pgx.Identifier{"uid505_copy_server"}, []string{"id", "data"},
		pgx.CopyFromRows([][]any{
			{int64(1), strings.Repeat("x", 100_000)},
			{int64(2), strings.Repeat("x", 100_000)}, // duplicate primary key
			{int64(3), strings.Repeat("x", 100_000)},
		}),
	)

	require.Error(t, err)
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	require.Equal(t, "23505", pgErr.Code)
	require.False(t, errors.Is(err, context.Canceled))
	require.Zero(t, copied, "the server does not acknowledge rows from an aborted COPY")

	// The pre-existing row is still visible, but none of the rejected COPY rows were inserted.
	var rowCount int64
	require.NoError(t, conn.QueryRow(context.Background(), "select count(*) from uid505_copy_server").Scan(&rowCount))
	require.EqualValues(t, 1, rowCount)

	require.False(t, conn.IsClosed())
	ensureConnValid(t, conn)

	_, err = conn.CopyFrom(context.Background(), pgx.Identifier{"uid505_copy_server"}, []string{"id", "data"},
		pgx.CopyFromRows([][]any{{int64(10), "after server rejection"}}))
	require.NoError(t, err)
}

func TestUID505CopyFromCancellationKeepsPoolConnReusable(t *testing.T) {
	if os.Getenv("PGX_TEST_DATABASE") == "" {
		t.Skip("PGX_TEST_DATABASE is not set")
	}

	poolConfig, err := pgxpool.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	poolConfig.MaxConns = 1

	pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	require.NoError(t, err)
	defer pool.Close()

	poolConn, err := pool.Acquire(context.Background())
	require.NoError(t, err)
	conn := poolConn.Conn()

	mustExec(t, conn, `create temporary table uid505_copy_pool(
		id bigint,
		data text not null
	)`)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	source := &uid505BlockingCopySource{ctx: ctx, started: make(chan struct{})}
	go func() {
		select {
		case <-source.started:
			cancel()
		case <-ctx.Done():
		}
	}()

	_, err = conn.CopyFrom(ctx, pgx.Identifier{"uid505_copy_pool"}, []string{"id", "data"}, source)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, conn.IsClosed())

	poolConn.Release()

	var one int
	require.NoError(t, pool.QueryRow(context.Background(), "select 1").Scan(&one))
	require.Equal(t, 1, one)
}
