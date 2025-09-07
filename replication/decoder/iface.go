package decoder

import (
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/codercms/pg-cdc/replication/types"
)

type Decoder interface {
	DecodeTuple(
		typMap *pgtype.Map,
		rel *types.TableInfo,
		cols []*pglogrepl.TupleDataColumn,
	) (any, error)
}
