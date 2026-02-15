package types

import (
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
)

type TableInfo struct {
	OID uint32

	Schema string
	Name   string

	Columns []*pglogrepl.RelationMessageColumn

	// Key contains Columns that is part of record uniq key for replication
	Key []*pglogrepl.RelationMessageColumn

	// FDs normalized field descriptions, useful for struct decoding
	FDs []pgconn.FieldDescription

	// KeyFDs normalized field descriptions for table replication key
	KeyFDs []pgconn.FieldDescription
}

func (ti *TableInfo) String() string {
	return ti.Schema + "." + ti.Name
}
