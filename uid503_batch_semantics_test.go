package pgx_test

import (
	"context"
	"io"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/internal/faultyconn"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxtest"
	"github.com/stretchr/testify/require"
)

func TestUID503BatchResultsStickyError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgxtest.RunWithQueryExecModes(ctx, t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		mustExec(t, conn, `create temporary table batch_sticky_error(id int)`)

		batch := &pgx.Batch{}
		batch.Queue("select 1 / 0")
		batch.Queue("insert into batch_sticky_error values (1)")
		batch.Queue("select 42")

		br := conn.SendBatch(ctx, batch)

		_, firstErr := br.Exec()
		require.Error(t, firstErr)

		_, err := br.Exec()
		require.ErrorIs(t, err, firstErr)
		require.Equal(t, firstErr, err)

		rows, err := br.Query()
		require.ErrorIs(t, err, firstErr)
		require.ErrorIs(t, rows.Err(), firstErr)

		err = br.QueryRow().Scan(new(int32))
		require.ErrorIs(t, err, firstErr)

		require.ErrorIs(t, br.Close(), firstErr)
		require.ErrorIs(t, br.Close(), firstErr)

		var count int
		require.NoError(t, conn.QueryRow(ctx, "select count(*) from batch_sticky_error").Scan(&count))
		require.Zero(t, count)
		require.NoError(t, conn.Ping(ctx))
	})
}

func TestUID503BatchResultsCloseDiscardsOpenRows(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgxtest.RunWithQueryExecModes(ctx, t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		mustExec(t, conn, `create temporary table batch_early_close(id int)`)

		for i, readFirstRow := range []bool{false, true} {
			batch := &pgx.Batch{}
			batch.Queue("select n from generate_series(1, 100) n")
			batch.Queue("insert into batch_early_close values ($1)", i+1)

			br := conn.SendBatch(ctx, batch)
			rows, err := br.Query()
			require.NoError(t, err)

			if readFirstRow {
				var n int
				require.True(t, rows.Next())
				require.NoError(t, rows.Scan(&n))
			}

			// Closing the batch while Rows are still open must discard the rest of that result and the remaining
			// batch results without surfacing a protocol desynchronization error.
			require.NoError(t, br.Close())
			require.NoError(t, br.Close())

			require.False(t, conn.IsClosed())
			require.False(t, conn.PgConn().IsBusy())
			require.Equal(t, byte('I'), conn.PgConn().TxStatus())
		}

		var count int
		require.NoError(t, conn.QueryRow(ctx, "select count(*) from batch_early_close").Scan(&count))
		require.Equal(t, 2, count)
		require.NoError(t, conn.Ping(ctx))
	})
}

func TestUID503BatchResultsNetworkError(t *testing.T) {
	t.Parallel()

	for _, mode := range []pgx.QueryExecMode{
		pgx.QueryExecModeExec,
		pgx.QueryExecModeCacheStatement,
		pgx.QueryExecModeCacheDescribe,
		pgx.QueryExecModeDescribeExec,
		pgx.QueryExecModeSimpleProtocol,
	} {
		t.Run(mode.String(), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			config := mustParseConfig(t, os.Getenv("PGX_TEST_DATABASE"))
			config.DefaultQueryExecMode = mode
			var faulty *faultyconn.Conn
			config.AfterNetConnect = func(ctx context.Context, _ *pgconn.Config, conn net.Conn) (net.Conn, error) {
				faulty = faultyconn.New(conn)
				return faulty, nil
			}

			conn := mustConnect(t, config)
			defer closeConn(t, conn)

			batch := &pgx.Batch{}
			batch.Queue("select pg_sleep(30)")
			batch.Queue("select 42")
			br := conn.SendBatch(ctx, batch)

			// Fail the network connection after the batch has been sent but before its result has been read.
			require.NoError(t, faulty.Close())

			_, firstErr := br.Exec()
			require.Error(t, firstErr)

			_, err := br.Exec()
			require.ErrorIs(t, err, firstErr)

			rows, err := br.Query()
			require.ErrorIs(t, err, firstErr)
			require.ErrorIs(t, rows.Err(), firstErr)

			require.ErrorIs(t, br.Close(), firstErr)
			require.ErrorIs(t, br.Close(), firstErr)
			require.True(t, conn.IsClosed())
		})
	}
}

func TestUID503BatchTxFailureRollback(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgxtest.RunWithQueryExecModes(ctx, t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		mustExec(t, conn, `create temporary table batch_tx_short_circuit(id int)`)

		batch := &pgx.Batch{}
		batch.BeginTx(pgx.TxOptions{})
		batch.Queue("insert into batch_tx_short_circuit values (1)")
		batch.Queue("select 1 / 0")
		batch.Queue("insert into batch_tx_short_circuit values (2)")

		br := conn.SendBatch(ctx, batch)

		_, err := br.Exec() // BEGIN
		require.NoError(t, err)
		_, err = br.Exec() // first insert
		require.NoError(t, err)

		_, firstErr := br.Exec() // failing select
		require.Error(t, firstErr)

		_, err = br.Exec()
		require.ErrorIs(t, err, firstErr)
		require.Equal(t, firstErr, err)

		require.ErrorIs(t, br.Close(), firstErr)
		require.Equal(t, byte('E'), conn.PgConn().TxStatus())

		tx := br.Tx()
		require.NotNil(t, tx)
		require.NoError(t, tx.Rollback(ctx))
		require.ErrorIs(t, tx.Rollback(ctx), pgx.ErrTxClosed)
		require.Equal(t, byte('I'), conn.PgConn().TxStatus())

		var count int
		require.NoError(t, conn.QueryRow(ctx, "select count(*) from batch_tx_short_circuit").Scan(&count))
		require.Zero(t, count)
	})
}

func TestUID503PreprocessingSendsNoCommands(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	config := mustParseConfig(t, os.Getenv("PGX_TEST_DATABASE"))
	config.DefaultQueryExecMode = pgx.QueryExecModeCacheStatement

	var faulty *faultyconn.Conn
	config.AfterNetConnect = func(ctx context.Context, _ *pgconn.Config, conn net.Conn) (net.Conn, error) {
		faulty = faultyconn.New(conn)
		return faulty, nil
	}

	conn := mustConnect(t, config)
	defer closeConn(t, conn)

	var binds, executes atomic.Int32
	faulty.HandleFrontendMessage = func(w io.Writer, msg pgproto3.FrontendMessage) error {
		switch msg.(type) {
		case *pgproto3.Bind:
			binds.Add(1)
		case *pgproto3.Execute:
			executes.Add(1)
		}

		buf, err := msg.Encode(nil)
		if err != nil {
			return err
		}
		_, err = w.Write(buf)
		return err
	}

	batch := &pgx.Batch{}
	batch.Queue("select 1")
	batch.Queue("select $1::int", 1, 2) // argument count fails during preprocessing

	err := conn.SendBatch(ctx, batch).Close()
	require.Error(t, err)
	require.Zero(t, binds.Load())
	require.Zero(t, executes.Load())
}

func TestUID503BatchSentinelErrors(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	empty := conn.SendBatch(ctx, &pgx.Batch{})
	_, err := empty.Exec()
	require.ErrorIs(t, err, pgx.ErrBatchEmpty)
	rows, err := empty.Query()
	require.ErrorIs(t, err, pgx.ErrBatchEmpty)
	require.ErrorIs(t, rows.Err(), pgx.ErrBatchEmpty)
	require.NoError(t, empty.Close())
	_, err = empty.Exec()
	require.ErrorIs(t, err, pgx.ErrBatchClosed)
	require.NoError(t, empty.Close())

	batch := &pgx.Batch{}
	batch.Queue("select 1")
	br := conn.SendBatch(ctx, batch)
	require.NoError(t, br.Close())
	_, err = br.Exec()
	require.ErrorIs(t, err, pgx.ErrBatchClosed)
	rows, err = br.Query()
	require.ErrorIs(t, err, pgx.ErrBatchClosed)
	require.ErrorIs(t, rows.Err(), pgx.ErrBatchClosed)
	require.NoError(t, br.Close())
}
