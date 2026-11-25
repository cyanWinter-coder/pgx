package pgconn_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jackc/pgx/v5/pgconn"
)

func uid504NewMultiHostConfig(t *testing.T, hosts ...string) *pgconn.Config {
	t.Helper()

	config, err := pgconn.ParseConfig("sslmode=disable user=pgx_test database=pgx_test host=ignored")
	require.NoError(t, err)

	config.LookupFunc = func(_ context.Context, host string) ([]string, error) {
		return []string{host}, nil
	}
	config.Host = hosts[0]
	config.Port = 1
	config.TLSConfig = nil
	config.Fallbacks = make([]*pgconn.FallbackConfig, 0, len(hosts)-1)
	for _, host := range hosts[1:] {
		config.Fallbacks = append(config.Fallbacks, &pgconn.FallbackConfig{
			Host:      host,
			Port:      1,
			TLSConfig: nil,
		})
	}

	return config
}

func TestUID504ConnectConfigAllHostsFail(t *testing.T) {
	t.Parallel()

	hosts := []string{"fallback-a.example", "fallback-b.example", "fallback-c.example"}
	config := uid504NewMultiHostConfig(t, hosts...)

	var mu sync.Mutex
	var dialed []string
	config.DialFunc = func(_ context.Context, _, address string) (net.Conn, error) {
		mu.Lock()
		dialed = append(dialed, address)
		mu.Unlock()
		return nil, fmt.Errorf("synthetic failure at %s", address)
	}

	conn, err := pgconn.ConnectConfig(context.Background(), config)
	require.Nil(t, conn)
	require.Error(t, err)

	for _, host := range hosts {
		require.ErrorContains(t, err, host)
		require.ErrorContains(t, err, fmt.Sprintf("synthetic failure at %s:1", host))
	}
	require.ElementsMatch(t, []string{
		"fallback-a.example:1",
		"fallback-b.example:1",
		"fallback-c.example:1",
	}, dialed)
}

func TestUID504ConnectConfigSharedBudget(t *testing.T) {
	t.Parallel()

	config := uid504NewMultiHostConfig(t, "budget-a.example", "budget-b.example", "budget-c.example")
	config.ConnectTimeout = 120 * time.Millisecond

	var attempts atomic.Int64
	config.DialFunc = func(ctx context.Context, _, _ string) (net.Conn, error) {
		attempts.Add(1)
		<-ctx.Done()
		return nil, ctx.Err()
	}

	start := time.Now()
	conn, err := pgconn.ConnectConfig(context.Background(), config)
	elapsed := time.Since(start)

	require.Nil(t, conn)
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.True(t, pgconn.Timeout(err), err)
	require.Equal(t, int64(1), attempts.Load(), "must not continue after the shared budget is exhausted")
	require.Less(t, elapsed, 500*time.Millisecond, "elapsed: %s", elapsed)
}

func TestUID504BudgetRemainingAfterFastFailure(t *testing.T) {
	t.Parallel()

	config := uid504NewMultiHostConfig(t, "shared-a.example", "shared-b.example", "shared-c.example")
	config.ConnectTimeout = 300 * time.Millisecond

	var mu sync.Mutex
	var dialed []string
	var secondAttemptRemaining time.Duration
	config.DialFunc = func(ctx context.Context, _, address string) (net.Conn, error) {
		mu.Lock()
		dialed = append(dialed, address)
		mu.Unlock()

		if address == "shared-a.example:1" {
			select {
			case <-time.After(75 * time.Millisecond):
				return nil, errors.New("fast first-host failure")
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		if address == "shared-b.example:1" {
			if deadline, ok := ctx.Deadline(); ok {
				secondAttemptRemaining = time.Until(deadline)
			}
			<-ctx.Done()
			return nil, ctx.Err()
		}

		return nil, errors.New("third host must not be attempted after budget exhaustion")
	}

	conn, err := pgconn.ConnectConfig(context.Background(), config)
	require.Nil(t, conn)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"shared-a.example:1", "shared-b.example:1"}, dialed)
	require.Greater(t, secondAttemptRemaining, time.Duration(0))
	require.Less(t, secondAttemptRemaining, 260*time.Millisecond,
		"second host should receive the budget left after the first attempt, not a fresh timeout")
}

func TestUID504ConnectConfigCancellationStops(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	config := uid504NewMultiHostConfig(t, "cancel-a.example", "cancel-b.example", "cancel-c.example")

	var attempts atomic.Int64
	config.DialFunc = func(ctx context.Context, _, _ string) (net.Conn, error) {
		n := attempts.Add(1)
		if n == 1 {
			cancel()
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}

	conn, err := pgconn.ConnectConfig(ctx, config)
	require.Nil(t, conn)
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, pgconn.Timeout(err))
	require.Equal(t, int64(1), attempts.Load(), "must stop fallback after caller cancellation")
}

func TestUID504ParentContextBudget(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()

	config := uid504NewMultiHostConfig(t, "ctx-a.example", "ctx-b.example", "ctx-c.example")
	var attempts atomic.Int64
	config.DialFunc = func(ctx context.Context, _, _ string) (net.Conn, error) {
		attempts.Add(1)
		<-ctx.Done()
		return nil, ctx.Err()
	}

	start := time.Now()
	conn, err := pgconn.ConnectConfig(ctx, config)
	elapsed := time.Since(start)

	require.Nil(t, conn)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.True(t, pgconn.Timeout(err), err)
	require.Equal(t, int64(1), attempts.Load())
	require.Less(t, elapsed, 500*time.Millisecond)
}
