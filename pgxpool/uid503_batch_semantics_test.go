package pgxpool_test

import (
	"context"
	"net"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/internal/faultyconn"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func uid503NewSingleConnPool(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()

	config, err := pgxpool.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	config.MaxConns = 1

	pool, err := pgxpool.NewWithConfig(ctx, config)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return pool
}

func uid503RequirePoolReleased(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	require.Eventually(t, func() bool {
		return pool.Stat().AcquiredConns() == 0
	}, 2*time.Second, 10*time.Millisecond)
}

func TestUID503PoolBatchStickyError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := uid503NewSingleConnPool(t, ctx)
	_, err := pool.Exec(ctx, `create temporary table pool_batch_sticky(id int)`)
	require.NoError(t, err)

	batch := &pgx.Batch{}
	batch.Queue("select 1 / 0")
	batch.Queue("insert into pool_batch_sticky values (1)")

	br := pool.SendBatch(ctx, batch)
	_, firstErr := br.Exec()
	require.Error(t, firstErr)

	_, err = br.Exec()
	require.ErrorIs(t, err, firstErr)
	require.Equal(t, firstErr, err)

	rows, err := br.Query()
	require.ErrorIs(t, err, firstErr)
	require.ErrorIs(t, rows.Err(), firstErr)

	require.ErrorIs(t, br.Close(), firstErr)
	require.ErrorIs(t, br.Close(), firstErr)
	uid503RequirePoolReleased(t, pool)

	var count int
	require.NoError(t, pool.QueryRow(ctx, "select count(*) from pool_batch_sticky").Scan(&count))
	require.Zero(t, count)
	require.EqualValues(t, 1, pool.Stat().TotalConns())
}

func TestUID503PoolEarlyCloseReuse(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := uid503NewSingleConnPool(t, ctx)
	_, err := pool.Exec(ctx, `create temporary table pool_batch_early_close(id int)`)
	require.NoError(t, err)

	batch := &pgx.Batch{}
	batch.Queue("select n from generate_series(1, 100) n")
	batch.Queue("insert into pool_batch_early_close values (1)")

	br := pool.SendBatch(ctx, batch)
	rows, err := br.Query()
	require.NoError(t, err)
	require.True(t, rows.Next())

	require.NoError(t, br.Close())
	require.NoError(t, br.Close())
	uid503RequirePoolReleased(t, pool)

	var count int
	require.NoError(t, pool.QueryRow(ctx, "select count(*) from pool_batch_early_close").Scan(&count))
	require.Equal(t, 1, count)
	require.EqualValues(t, 1, pool.Stat().TotalConns())
}

func TestUID503PoolNetworkError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	config, err := pgxpool.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	config.MaxConns = 1

	var faulty *faultyconn.Conn
	config.ConnConfig.AfterNetConnect = func(ctx context.Context, _ *pgconn.Config, conn net.Conn) (net.Conn, error) {
		faulty = faultyconn.New(conn)
		return faulty, nil
	}

	pool, err := pgxpool.NewWithConfig(ctx, config)
	require.NoError(t, err)
	defer pool.Close()

	batch := &pgx.Batch{}
	batch.Queue("select pg_sleep(30)")
	batch.Queue("select 42")

	br := pool.SendBatch(ctx, batch)
	require.NotNil(t, faulty)
	require.NoError(t, faulty.Close())

	_, firstErr := br.Exec()
	require.Error(t, firstErr)
	_, err = br.Exec()
	require.ErrorIs(t, err, firstErr)
	require.ErrorIs(t, br.Close(), firstErr)
	uid503RequirePoolReleased(t, pool)

	// The failed connection must have been destroyed and replaced transparently.
	require.NoError(t, pool.Ping(ctx))
	require.EqualValues(t, 1, pool.Stat().TotalConns())
}

func TestUID503PoolTxRollback(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := uid503NewSingleConnPool(t, ctx)
	_, err := pool.Exec(ctx, `create temporary table pool_batch_tx(id int)`)
	require.NoError(t, err)

	batch := &pgx.Batch{}
	batch.BeginTx(pgx.TxOptions{})
	batch.Queue("insert into pool_batch_tx values (1)")
	batch.Queue("select 1 / 0")
	batch.Queue("insert into pool_batch_tx values (2)")

	br := pool.SendBatch(ctx, batch)
	_, err = br.Exec() // BEGIN
	require.NoError(t, err)
	_, err = br.Exec() // insert
	require.NoError(t, err)

	_, firstErr := br.Exec()
	require.Error(t, firstErr)
	_, err = br.Exec()
	require.ErrorIs(t, err, firstErr)
	require.ErrorIs(t, br.Close(), firstErr)

	tx := br.Tx()
	require.NotNil(t, tx)
	require.NoError(t, tx.Rollback(ctx))
	require.ErrorIs(t, tx.Rollback(ctx), pgx.ErrTxClosed)
	uid503RequirePoolReleased(t, pool)

	var count int
	require.NoError(t, pool.QueryRow(ctx, "select count(*) from pool_batch_tx").Scan(&count))
	require.Zero(t, count)
	require.EqualValues(t, 1, pool.Stat().TotalConns())
}
