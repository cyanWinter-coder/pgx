package pgx

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/jackc/pgx/v5/internal/pgio"
	"github.com/jackc/pgx/v5/pgconn"
)

// CopyFromRows returns a [CopyFromSource] interface over the provided rows slice
// making it usable by [Conn.CopyFrom].
func CopyFromRows(rows [][]any) CopyFromSource {
	return &copyFromRows{rows: rows, idx: -1}
}

type copyFromRows struct {
	rows [][]any
	idx  int
}

func (ctr *copyFromRows) Next() bool {
	ctr.idx++
	return ctr.idx < len(ctr.rows)
}

func (ctr *copyFromRows) Values() ([]any, error) {
	return ctr.rows[ctr.idx], nil
}

func (ctr *copyFromRows) Err() error {
	return nil
}

// CopyFromSlice returns a [CopyFromSource] interface over a dynamic func
// making it usable by [Conn.CopyFrom].
func CopyFromSlice(length int, next func(int) ([]any, error)) CopyFromSource {
	return &copyFromSlice{next: next, idx: -1, len: length}
}

type copyFromSlice struct {
	next func(int) ([]any, error)
	idx  int
	len  int
	err  error
}

func (cts *copyFromSlice) Next() bool {
	cts.idx++
	return cts.idx < cts.len
}

func (cts *copyFromSlice) Values() ([]any, error) {
	values, err := cts.next(cts.idx)
	if err != nil {
		cts.err = err
	}
	return values, err
}

func (cts *copyFromSlice) Err() error {
	return cts.err
}

// CopyFromFunc returns a [CopyFromSource] interface that relies on nxtf for values.
// nxtf returns rows until it either signals an 'end of data' by returning row=nil and err=nil,
// or it returns an error. If nxtf returns an error, the copy is aborted.
func CopyFromFunc(nxtf func() (row []any, err error)) CopyFromSource {
	return &copyFromFunc{next: nxtf}
}

type copyFromFunc struct {
	next     func() ([]any, error)
	valueRow []any
	err      error
}

func (g *copyFromFunc) Next() bool {
	g.valueRow, g.err = g.next()
	// only return true if valueRow exists and no error
	return g.valueRow != nil && g.err == nil
}

func (g *copyFromFunc) Values() ([]any, error) {
	return g.valueRow, g.err
}

func (g *copyFromFunc) Err() error {
	return g.err
}

// CopyFromSource is the interface used by [Conn.CopyFrom] as the source for copy data.
type CopyFromSource interface {
	// Next returns true if there is another row and makes the next row data
	// available to Values(). When there are no more rows available or an error
	// has occurred it returns false.
	Next() bool

	// Values returns the values for the current row.
	Values() ([]any, error)

	// Err returns any error that has been encountered by the CopyFromSource. If
	// this is not nil *Conn.CopyFrom will abort the copy.
	Err() error
}

type copyFrom struct {
	conn          *Conn
	tableName     Identifier
	columnNames   []string
	rowSrc        CopyFromSource
	readerErrChan chan error
	mode          QueryExecMode
}

func (ct *copyFrom) run(ctx context.Context) (int64, error) {
	if ct.conn.copyFromTracer != nil {
		ctx = ct.conn.copyFromTracer.TraceCopyFromStart(ctx, ct.conn, TraceCopyFromStartData{
			TableName:   ct.tableName,
			ColumnNames: ct.columnNames,
		})
	}

	quotedTableName := ct.tableName.Sanitize()
	cbuf := &bytes.Buffer{}
	for i, cn := range ct.columnNames {
		if i != 0 {
			cbuf.WriteString(", ")
		}
		cbuf.WriteString(quoteIdentifier(cn))
	}
	quotedColumnNames := cbuf.String()

	var sd *pgconn.StatementDescription
	switch ct.mode {
	case QueryExecModeExec, QueryExecModeSimpleProtocol:
		// These modes don't support the binary format. Before the inclusion of the
		// QueryExecModes, Conn.Prepare was called on every COPY operation to get
		// the OIDs. These prepared statements were not cached.
		//
		// Since that's the same behavior provided by QueryExecModeDescribeExec,
		// we'll default to that mode.
		ct.mode = QueryExecModeDescribeExec
		fallthrough
	case QueryExecModeCacheStatement, QueryExecModeCacheDescribe, QueryExecModeDescribeExec:
		var err error
		sd, err = ct.conn.getStatementDescription(
			ctx,
			ct.mode,
			fmt.Sprintf("select %s from %s", quotedColumnNames, quotedTableName),
		)
		if err != nil {
			return 0, fmt.Errorf("statement description failed: %w", err)
		}
	default:
		return 0, fmt.Errorf("unknown QueryExecMode: %v", ct.mode)
	}

	r, w := io.Pipe()
	doneChan := make(chan struct{})
	var writtenRowCount int64

	go func() {
		defer close(doneChan)

		// ct.conn.wbuf is retained on the connection so a large COPY encoding buffer is reused by later CopyFrom
		// calls. The deferred write-back also runs on encoding failures, where buildCopyBuf returns the buffer rather
		// than nil. Resetting the length discards a partially encoded row while preserving its capacity.
		buf := ct.conn.wbuf[:0]
		defer func() { ct.conn.wbuf = buf[:0] }()

		// Purposely NOT using defer w.Close(). See https://github.com/golang/go/issues/24283.
		buf = append(buf, "PGCOPY\n\377\r\n\000"...)
		buf = pgio.AppendInt32(buf, 0)
		buf = pgio.AppendInt32(buf, 0)

		moreRows := true
		for moreRows {
			var rowCount int64
			var err error
			moreRows, rowCount, buf, err = ct.buildCopyBuf(buf, sd)
			if err != nil {
				w.CloseWithError(err)
				return
			}

			if ct.rowSrc.Err() != nil {
				w.CloseWithError(ct.rowSrc.Err())
				return
			}

			if len(buf) > 0 {
				_, err = w.Write(buf)
				if err != nil {
					w.Close()
					return
				}
				writtenRowCount += rowCount
			}

			buf = buf[:0]
		}

		w.Close()
	}()

	// pgConn.CopyFrom only observes ctx through its network deadline watcher. Once cancellation fires, close the pipe
	// so its reader goroutine cannot remain blocked in Read and prevent CopyFail cleanup. Closing with context.Cause
	// preserves a custom cancellation cause in the returned error chain.
	stopContextPipeCloser := context.AfterFunc(ctx, func() {
		err := context.Cause(ctx)
		if err == nil {
			err = ctx.Err()
		}
		w.CloseWithError(err)
	})

	// Detach cancellation from pgconn's deadline watcher. Canceling a network write halfway through a CopyData
	// message would make the protocol stream unrecoverable. Instead, stop the row source through the pipe closer
	// above, let any in-progress write reach a message boundary, and then send CopyFail and drain the backend.
	copyCtx := context.WithoutCancel(ctx)
	commandTag, err := ct.conn.pgConn.CopyFrom(copyCtx, r, fmt.Sprintf("copy %s ( %s ) from stdin binary;", quotedTableName, quotedColumnNames))
	stopContextPipeCloser()

	r.Close()
	<-doneChan

	if ct.conn.copyFromTracer != nil {
		ct.conn.copyFromTracer.TraceCopyFromEnd(ctx, ct.conn, TraceCopyFromEndData{
			CommandTag: commandTag,
			Err:        err,
		})
	}

	rowsAffected := commandTag.RowsAffected()
	if err != nil && rowsAffected == 0 {
		// The server does not provide a row count for an aborted COPY. For a client-side encoding or cancellation
		// failure, report the rows that were successfully handed to the COPY protocol writer. Server rejections keep
		// the authoritative CommandTag/PgError result (normally zero affected rows because COPY is transactional).
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) {
			rowsAffected = writtenRowCount
		}
	}

	return rowsAffected, err
}

func (ct *copyFrom) buildCopyBuf(buf []byte, sd *pgconn.StatementDescription) (bool, int64, []byte, error) {
	const sendBufSize = 65536 - 5 // The packet has a 5-byte header
	lastBufLen := 0
	largestRowLen := 0
	var rowCount int64

	for ct.rowSrc.Next() {
		lastBufLen = len(buf)

		values, err := ct.rowSrc.Values()
		if err != nil {
			return false, rowCount, buf, err
		}
		if len(values) != len(ct.columnNames) {
			return false, rowCount, buf, fmt.Errorf("expected %d values, got %d values", len(ct.columnNames), len(values))
		}

		buf = pgio.AppendInt16(buf, int16(len(ct.columnNames)))
		for i, val := range values {
			encodedBuf, encodeErr := encodeCopyValue(ct.conn.typeMap, buf, sd.Fields[i].DataTypeOID, val)
			if encodeErr != nil {
				// Discard the partially appended failed row while retaining all complete rows that preceded it.
				// Keep using the pre-call buf because encodeCopyValue returns a nil buffer on error.
				buf = buf[:lastBufLen]
				return false, rowCount, buf, encodeErr
			}
			buf = encodedBuf
		}
		rowCount++

		rowLen := len(buf) - lastBufLen
		if rowLen > largestRowLen {
			largestRowLen = rowLen
		}

		// Try not to overflow size of the buffer PgConn.CopyFrom will be reading into. If that happens then the nature of
		// io.Pipe means that the next Read will be short. This can lead to pathological send sizes such as 65531, 13, 65531
		// 13, 65531, 13, 65531, 13.
		if len(buf) > sendBufSize-largestRowLen {
			return true, rowCount, buf, nil
		}
	}

	return false, rowCount, buf, nil
}

// CopyFrom uses the PostgreSQL copy protocol to perform bulk data insertion. It returns the number of rows copied and
// an error.
//
// If encoding or reading the source fails after some complete row batches have been written to the connection, the
// returned count includes those rows. The original source or encoding error remains in the error chain. A rejection
// reported by the server returns the server's CommandTag count (zero for an aborted COPY) and a [*pgconn.PgError].
// The connection is drained and remains usable after these errors.
//
// CopyFrom requires all values use the binary format. A pgtype.Type that supports the binary format must be registered
// for the type of each column. Almost all types implemented by pgx support the binary format.
//
// Even though enum types appear to be strings they still must be registered to use with [Conn.CopyFrom]. This can be done with
// [Conn.LoadType] and [pgtype.Map.RegisterType].
func (c *Conn) CopyFrom(ctx context.Context, tableName Identifier, columnNames []string, rowSrc CopyFromSource) (int64, error) {
	ct := &copyFrom{
		conn:          c,
		tableName:     tableName,
		columnNames:   columnNames,
		rowSrc:        rowSrc,
		readerErrChan: make(chan error),
		mode:          c.config.DefaultQueryExecMode,
	}

	return ct.run(ctx)
}
