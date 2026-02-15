package decoder

import (
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// BaseRows implements the Rows interface for Conn.Query.
// This is a copy of pgx.baseRows to handle tuples that comes from logical replication protocol
type BaseRows struct {
	typeMap *pgtype.Map
	//resultReader *pgconn.ResultReader

	fds []pgconn.FieldDescription

	rows [][][]byte

	values [][]byte

	commandTag pgconn.CommandTag
	err        error
	closed     bool

	scanPlans []pgtype.ScanPlan
	scanTypes []reflect.Type

	//conn              *Conn
	//multiResultReader *pgconn.MultiResultReader

	//queryTracer QueryTracer
	//batchTracer BatchTracer
	//ctx         context.Context
	startTime time.Time
	sql       string
	args      []any
	rowCount  int
}

func NewBaseRows(typeMap *pgtype.Map, fds []pgconn.FieldDescription, rows [][][]byte) *BaseRows {
	r := BaseRows{
		typeMap: typeMap,
		fds:     fds,
		rows:    rows,
	}

	return &r
}

func (rows *BaseRows) FieldDescriptions() []pgconn.FieldDescription {
	return rows.fds
}

func (rows *BaseRows) Close() {
	if rows.closed {
		return
	}

	rows.closed = true
}

func (rows *BaseRows) CommandTag() pgconn.CommandTag {
	return rows.commandTag
}

func (rows *BaseRows) Err() error {
	return rows.err
}

// fatal signals an error occurred after the query was sent to the server. It
// closes the rows automatically.
func (rows *BaseRows) fatal(err error) {
	if rows.err != nil {
		return
	}

	rows.err = err
	rows.Close()
}

func (rows *BaseRows) Next() bool {
	if rows.closed {
		return false
	}

	if len(rows.rows) > rows.rowCount {
		rows.values = rows.rows[rows.rowCount]
		rows.rowCount++
		return true
	} else {
		rows.Close()
		return false
	}
}

func (rows *BaseRows) Scan(dest ...any) error {
	m := rows.typeMap
	fieldDescriptions := rows.FieldDescriptions()
	values := rows.values

	if len(fieldDescriptions) != len(values) {
		err := fmt.Errorf("number of field descriptions must equal number of values, got %d and %d", len(fieldDescriptions), len(values))
		rows.fatal(err)
		return err
	}

	if len(dest) == 1 {
		if rc, ok := dest[0].(pgx.RowScanner); ok {
			err := rc.ScanRow(rows)
			if err != nil {
				rows.fatal(err)
			}
			return err
		}
	}

	if len(fieldDescriptions) != len(dest) {
		err := fmt.Errorf("number of field descriptions must equal number of destinations, got %d and %d", len(fieldDescriptions), len(dest))
		rows.fatal(err)
		return err
	}

	if rows.scanPlans == nil {
		rows.scanPlans = make([]pgtype.ScanPlan, len(values))
		rows.scanTypes = make([]reflect.Type, len(values))
		for i := range dest {
			rows.scanPlans[i] = m.PlanScan(fieldDescriptions[i].DataTypeOID, fieldDescriptions[i].Format, dest[i])
			rows.scanTypes[i] = reflect.TypeOf(dest[i])
		}
	}

	for i, dst := range dest {
		if dst == nil {
			continue
		}

		if rows.scanTypes[i] != reflect.TypeOf(dst) {
			rows.scanPlans[i] = m.PlanScan(fieldDescriptions[i].DataTypeOID, fieldDescriptions[i].Format, dest[i])
			rows.scanTypes[i] = reflect.TypeOf(dest[i])
		}

		err := rows.scanPlans[i].Scan(values[i], dst)
		if err != nil {
			err = pgx.ScanArgError{ColumnIndex: i, Err: err}
			rows.fatal(err)
			return err
		}
	}

	return nil
}

func (rows *BaseRows) Values() ([]any, error) {
	if rows.closed {
		return nil, errors.New("rows is closed")
	}

	values := make([]any, 0, len(rows.FieldDescriptions()))

	for i := range rows.FieldDescriptions() {
		buf := rows.values[i]
		fd := &rows.FieldDescriptions()[i]

		if buf == nil {
			values = append(values, nil)
			continue
		}

		if dt, ok := rows.typeMap.TypeForOID(fd.DataTypeOID); ok {
			value, err := dt.Codec.DecodeValue(rows.typeMap, fd.DataTypeOID, fd.Format, buf)
			if err != nil {
				rows.fatal(err)
			}
			values = append(values, value)
		} else {
			switch fd.Format {
			case pgx.TextFormatCode:
				values = append(values, string(buf))
			case pgx.BinaryFormatCode:
				newBuf := make([]byte, len(buf))
				copy(newBuf, buf)
				values = append(values, newBuf)
			default:
				rows.fatal(errors.New("unknown format code"))
			}
		}

		if rows.Err() != nil {
			return nil, rows.Err()
		}
	}

	return values, rows.Err()
}

func (rows *BaseRows) RawValues() [][]byte {
	return rows.values
}

func (rows *BaseRows) Conn() *pgx.Conn {
	return nil
}
