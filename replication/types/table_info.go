package types

import "github.com/jackc/pglogrepl"

type TableInfo struct {
	OID uint32

	Schema string
	Name   string

	Columns []*pglogrepl.RelationMessageColumn

	// Key contains Columns that is part of record uniq key for replication
	Key []*pglogrepl.RelationMessageColumn
}

func (ti *TableInfo) String() string {
	return ti.Schema + "." + ti.Name
}
