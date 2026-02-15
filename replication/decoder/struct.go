package decoder

import (
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/codercms/pg-cdc/replication/types"
)

// StructDecoder decodes tuple data into user-defined struct using
// [pgx.RowToAddrOfStructByNameLax]
//
// TFull defines full row attributes,
// while TKey is used only for DELETE and UPDATE (old tuple) decoding when REPLICA IDENTITY is set to DEFAULT
//
// In case when REPLICA IDENTITY is set to FULL, TFull is only used datatype for decoding
type StructDecoder[TFull any, TKey any] struct {
}

type ToastableStruct interface {
	SetToastedColumns(cols []string)
}

func (c *StructDecoder[TFull, TKey]) DecodeTuple(
	typMap *pgtype.Map,
	rel *types.TableInfo,
	cols []*pglogrepl.TupleDataColumn,
	probablyOnlyKey bool,
) (any, error) {
	if probablyOnlyKey {
		row := make([][]byte, 0, len(rel.Key))

		for _, key := range rel.KeyFDs {
			row = append(row, cols[key.TableAttributeNumber].Data)
		}

		pgxRows := NewBaseRows(typMap, rel.KeyFDs, [][][]byte{row})

		res, err := pgx.CollectOneRow(pgxRows, pgx.RowToAddrOfStructByNameLax[TKey])
		if err != nil {
			return nil, err
		}

		return res, nil
	}

	row := make([][]byte, 0, len(cols))
	var toasted []string

	for idx, col := range cols {
		row = append(row, col.Data)

		if col.DataType == pglogrepl.TupleDataTypeToast {
			toasted = append(toasted, rel.Columns[idx].Name)
		}
	}

	pgxRows := NewBaseRows(typMap, rel.FDs, [][][]byte{row})

	res, err := pgx.CollectOneRow(pgxRows, pgx.RowToAddrOfStructByNameLax[TFull])
	if err != nil {
		return nil, err
	}

	if toastable, ok := any(res).(ToastableStruct); ok {
		toastable.SetToastedColumns(toasted)
	}

	return res, nil
}
