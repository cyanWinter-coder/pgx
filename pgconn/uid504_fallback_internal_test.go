package pgconn

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jackc/pgx/v5/internal/pgmock"
	"github.com/jackc/pgx/v5/pgproto3"
)

func uid504FallbackConnectConfigs(addrs ...string) []*connectOneConfig {
	configs := make([]*connectOneConfig, 0, len(addrs))
	for i, addr := range addrs {
		configs = append(configs, &connectOneConfig{
			network:          "tcp",
			address:          addr,
			originalHostname: fmt.Sprintf("host%d", i+1),
		})
	}
	return configs
}

func TestUID504ConnectPreferredAllHostsFail(t *testing.T) {
	t.Parallel()

	addrs := []string{"192.0.2.1:5432", "192.0.2.2:5432", "192.0.2.3:5432"}
	var mu sync.Mutex
	var attempted []string

	config := &Config{
		DialFunc: func(_ context.Context, _, address string) (net.Conn, error) {
			mu.Lock()
			attempted = append(attempted, address)
			mu.Unlock()
			return nil, fmt.Errorf("synthetic failure for %s", address)
		},
	}

	conn, errs := connectPreferred(context.Background(), config, uid504FallbackConnectConfigs(addrs...))
	require.Nil(t, conn)
	require.Len(t, errs, len(addrs))

	joined := errors.Join(errs...)
	for i, addr := range addrs {
		require.ErrorContains(t, joined, addr)
		require.ErrorContains(t, joined, fmt.Sprintf("host%d", i+1))
		require.ErrorContains(t, joined, fmt.Sprintf("synthetic failure for %s", addr))
	}
	require.Equal(t, addrs, attempted)
}

func TestUID504SharedBudgetStopsAfterExhaustion(t *testing.T) {
	t.Parallel()

	const budget = 100 * time.Millisecond
	var attempts atomic.Int64

	config := &Config{
		ConnectTimeout: budget,
		DialFunc: func(ctx context.Context, _, _ string) (net.Conn, error) {
			attempts.Add(1)
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	start := time.Now()
	conn, errs := connectPreferred(
		context.Background(),
		config,
		uid504FallbackConnectConfigs("192.0.2.1:5432", "192.0.2.2:5432", "192.0.2.3:5432"),
	)
	elapsed := time.Since(start)
	joined := errors.Join(errs...)

	require.Nil(t, conn)
	require.Error(t, joined)
	require.ErrorIs(t, joined, context.DeadlineExceeded)
	require.True(t, Timeout(joined), "expected pgconn.Timeout to recognize budget exhaustion: %v", joined)
	require.Equal(t, int64(1), attempts.Load(), "must stop after the shared budget is exhausted")
	require.Less(t, elapsed, 400*time.Millisecond, "elapsed: %s", elapsed)
}

func TestUID504CancellationStopsFallback(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	var attempts atomic.Int64

	config := &Config{
		DialFunc: func(ctx context.Context, _, _ string) (net.Conn, error) {
			n := attempts.Add(1)
			if n == 1 {
				cancel()
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	conn, errs := connectPreferred(
		ctx,
		config,
		uid504FallbackConnectConfigs("192.0.2.1:5432", "192.0.2.2:5432", "192.0.2.3:5432"),
	)
	joined := errors.Join(errs...)

	require.Nil(t, conn)
	require.ErrorIs(t, joined, context.Canceled)
	require.Equal(t, int64(1), attempts.Load(), "must not try another address after caller cancellation")
}

type uid504CloseSignaledConn struct {
	net.Conn
	once   sync.Once
	closed chan struct{}
}

func (c *uid504CloseSignaledConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

func TestUID504ClosesDialConnReturnedWithCancelError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()

	closed := make(chan struct{})
	trackedConn := &uid504CloseSignaledConn{Conn: clientConn, closed: closed}

	config := &Config{
		DialFunc: func(context.Context, string, string) (net.Conn, error) {
			cancel()
			return trackedConn, context.Canceled
		},
	}

	conn, errs := connectPreferred(ctx, config, uid504FallbackConnectConfigs("192.0.2.1:5432"))
	joined := errors.Join(errs...)

	require.Nil(t, conn)
	require.ErrorIs(t, joined, context.Canceled)

	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("established connection returned alongside dial error was not closed")
	}
}

func uid504StartFallbackMockServer(t *testing.T) (string, <-chan error) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	errCh := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			errCh <- err
			return
		}
		defer conn.Close()

		steps := append(pgmock.AcceptUnauthenticatedConnRequestSteps(), pgmock.WaitForClose())
		errCh <- (&pgmock.Script{Steps: steps}).Run(pgproto3.NewBackend(conn, conn))
	}()

	t.Cleanup(func() { _ = ln.Close() })
	return ln.Addr().String(), errCh
}

func uid504FallbackConfig(t *testing.T) *Config {
	t.Helper()

	config, err := ParseConfig("sslmode=disable user=pgx_test database=pgx_test host=127.0.0.1")
	require.NoError(t, err)
	return config
}

func TestUID504CancelClosesDeliveredCandidate(t *testing.T) {
	t.Parallel()

	addr, serverErr := uid504StartFallbackMockServer(t)
	config := uid504FallbackConfig(t)

	ctx, cancel := context.WithCancel(context.Background())
	var validatedConn *PgConn
	config.ValidateConnect = func(_ context.Context, conn *PgConn) error {
		validatedConn = conn
		// A misbehaving validation callback can return nil even though the caller
		// has canceled. The candidate must still be closed instead of delivered.
		cancel()
		return nil
	}

	pgConn, errs := connectPreferred(ctx, config, uid504FallbackConnectConfigs(addr))
	joined := errors.Join(errs...)

	require.Nil(t, pgConn)
	require.ErrorIs(t, joined, context.Canceled)
	require.NotNil(t, validatedConn)
	require.True(t, validatedConn.IsClosed())

	select {
	case <-serverErr:
		// Immediate teardown can produce EOF or an EOF variant on the mock.
	case <-time.After(time.Second):
		t.Fatal("canceled candidate connection was not closed")
	}
}

func TestUID504CancelAfterConnectNilCloses(t *testing.T) {
	t.Parallel()

	addr, serverErr := uid504StartFallbackMockServer(t)
	host, portString, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	port, err := strconv.ParseUint(portString, 10, 16)
	require.NoError(t, err)

	config := uid504FallbackConfig(t)
	config.Host = host
	config.Port = uint16(port)
	config.TLSConfig = nil

	ctx, cancel := context.WithCancel(context.Background())
	var afterConnectConn *PgConn
	config.AfterConnect = func(_ context.Context, conn *PgConn) error {
		afterConnectConn = conn
		cancel()
		return nil
	}

	pgConn, err := ConnectConfig(ctx, config)
	require.Nil(t, pgConn)
	require.ErrorIs(t, err, context.Canceled)
	require.NotNil(t, afterConnectConn)
	require.True(t, afterConnectConn.IsClosed())

	select {
	case <-serverErr:
	case <-time.After(time.Second):
		t.Fatal("connection canceled after AfterConnect was not closed")
	}
}

func TestUID504ValidateConnectPgErrorContinues(t *testing.T) {
	t.Parallel()

	rejectAddr, rejectServerErr := uid504StartFallbackMockServer(t)
	acceptAddr, acceptServerErr := uid504StartFallbackMockServer(t)

	config := uid504FallbackConfig(t)
	var validations atomic.Int64
	config.ValidateConnect = func(context.Context, *PgConn) error {
		if validations.Add(1) == 1 {
			// Even a fatal SQLSTATE-shaped error from ValidateConnect is a
			// validation rejection, not an authentication failure.
			return &PgError{Severity: "ERROR", Code: "28P01", Message: "rejected by ValidateConnect"}
		}
		return nil
	}

	pgConn, errs := connectPreferred(
		context.Background(),
		config,
		uid504FallbackConnectConfigs(rejectAddr, acceptAddr),
	)
	joined := errors.Join(errs...)

	require.NotNil(t, pgConn)
	require.NoError(t, joined)
	require.Equal(t, int64(2), validations.Load())
	require.NoError(t, pgConn.Close(context.Background()))

	for _, errCh := range []<-chan error{rejectServerErr, acceptServerErr} {
		select {
		case <-errCh:
			// A rejected candidate is closed without a Terminate message, so the
			// mock can report EOF variants. Completion is the close signal.
		case <-time.After(time.Second):
			t.Fatal("mock server did not observe connection close")
		}
	}
}

func TestUID504PreferTLSDoesNotRedialFailedAddress(t *testing.T) {
	t.Parallel()

	config, err := ParseConfig("host=h1,h2,h3 port=5432 sslmode=prefer user=pgx_test database=pgx_test")
	require.NoError(t, err)
	config.LookupFunc = func(_ context.Context, host string) ([]string, error) {
		return []string{host}, nil
	}

	configs, resolveErrs := buildConnectOneConfigs(context.Background(), config)
	require.Empty(t, resolveErrs)
	require.Len(t, configs, 6)

	var gotAddrs []string
	var tlsKinds []string
	for _, c := range configs {
		gotAddrs = append(gotAddrs, c.address)
		if c.tlsConfig != nil {
			tlsKinds = append(tlsKinds, "tls")
		} else {
			tlsKinds = append(tlsKinds, "plain")
		}
	}
	require.Equal(t, []string{
		"h1:5432", "h1:5432",
		"h2:5432", "h2:5432",
		"h3:5432", "h3:5432",
	}, gotAddrs)
	require.Equal(t, []string{"tls", "plain", "tls", "plain", "tls", "plain"}, tlsKinds)

	var mu sync.Mutex
	var attempted []string
	config.DialFunc = func(_ context.Context, _, address string) (net.Conn, error) {
		mu.Lock()
		attempted = append(attempted, address)
		mu.Unlock()
		return nil, errors.New("synthetic dial failure")
	}

	pgConn, errs := connectPreferred(context.Background(), config, configs)
	require.Nil(t, pgConn)
	require.Len(t, errs, 3)
	require.Equal(t, []string{"h1:5432", "h2:5432", "h3:5432"}, attempted,
		"the plaintext fallback must not redial an address whose network dial already failed")
}

func TestUID504TLSNegotiationFailureAllowsPlainFallback(t *testing.T) {
	t.Parallel()

	// The first Dial returns a server that rejects PostgreSQL TLS negotiation
	// with N. That is not a dial failure, so the plaintext candidate for the
	// same address must still be dialed once.
	var dials atomic.Int64
	config := uid504FallbackConfig(t)
	config.ConnectTimeout = 100 * time.Millisecond
	config.DialFunc = func(context.Context, string, string) (net.Conn, error) {
		n := dials.Add(1)
		serverConn, clientConn := net.Pipe()

		go func() {
			if n == 1 {
				_, _ = io.ReadFull(serverConn, make([]byte, 8)) // SSLRequest
				_, _ = serverConn.Write([]byte{'N'})
			}
			_, _ = io.Copy(io.Discard, serverConn)
			_ = serverConn.Close()
		}()

		return clientConn, nil
	}

	configs := []*connectOneConfig{
		{network: "tcp", address: "tls-plain.example:5432", originalHostname: "tls-plain.example", tlsConfig: &tls.Config{InsecureSkipVerify: true}},
		{network: "tcp", address: "tls-plain.example:5432", originalHostname: "tls-plain.example", tlsConfig: nil},
	}

	pgConn, errs := connectPreferred(context.Background(), config, configs)
	require.Nil(t, pgConn)
	require.Error(t, errors.Join(errs...))
	require.Equal(t, int64(2), dials.Load())
}
