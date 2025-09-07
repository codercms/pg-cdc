package decoder

import (
	"fmt"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/codercms/pg-cdc/replication/types"
)

// DefaultDecoder decodes rows as an map[string]any
//
// Unchanged TOASTed values is stored as an UnchangedToastedData
type DefaultDecoder struct {
}

func (c *DefaultDecoder) DecodeTuple(
	typMap *pgtype.Map,
	rel *types.TableInfo,
	cols []*pglogrepl.TupleDataColumn,
) (any, error) {
	values := make(map[string]any, len(rel.Columns))

	for idx, col := range cols {
		if idx >= len(rel.Columns) {
			continue
		}

		colName := rel.Columns[idx].Name
		switch col.DataType {
		case 'n': // null
			values[colName] = nil

		case 't': // text
			val, err := c.decodeColumnData(typMap, col.Data, rel.Columns[idx].DataType)
			if err != nil {
				return nil, fmt.Errorf("failed to decode column data: %w", err)
			}

			values[colName] = val

		case 'u': // unchanged toast
			values[colName] = UnchangedToastedData
		}
	}

	return values, nil
}

func (c *DefaultDecoder) decodeColumnData(typeMap *pgtype.Map, data []byte, dataType uint32) (any, error) {
	if dt, ok := typeMap.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(typeMap, dataType, pgtype.TextFormatCode, data)
	}

	return string(data), nil
}
