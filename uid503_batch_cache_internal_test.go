package pgx

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUID503PreprocessingInvalidatesCaches(t *testing.T) {
	t.Parallel()

	for _, mode := range []QueryExecMode{QueryExecModeCacheStatement, QueryExecModeCacheDescribe} {
		t.Run(mode.String(), func(t *testing.T) {
			ctx := context.Background()
			config := mustParseConfig(t, os.Getenv("PGX_TEST_DATABASE"))
			config.DefaultQueryExecMode = mode
			conn := mustConnect(t, config)
			defer conn.Close(ctx)

			sql := "select $1::int"
			_, err := conn.Exec(ctx, sql, int32(1))
			require.NoError(t, err)

			cache := conn.statementCache
			if mode == QueryExecModeCacheDescribe {
				cache = conn.descriptionCache
			}
			require.NotNil(t, cache)
			require.NotNil(t, cache.Get(sql))

			batch := &Batch{}
			batch.Queue("select 1")
			batch.Queue(sql, 1, 2) // mismatched argument count fails while preprocessing

			results := conn.SendBatch(ctx, batch)
			require.Error(t, results.(interface{ earlyError() error }).earlyError())
			require.Nil(t, cache.Get(sql))
			require.Error(t, results.Close())
			require.Nil(t, cache.Get(sql))

			// Preprocessing failed before any batch Bind/Execute was queued, so the connection remains usable.
			require.False(t, conn.IsClosed())
			require.NoError(t, conn.Ping(ctx))
		})
	}
}
