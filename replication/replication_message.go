package replication

import "github.com/jackc/pglogrepl"

type LogicalReplicationMessage interface {
	logReplMsg()
}

type PrimaryKeepaliveMessage struct {
	pglogrepl.PrimaryKeepaliveMessage
}
type XLogData struct {
	pglogrepl.XLogData
	pglogrepl.Message
}

func (PrimaryKeepaliveMessage) logReplMsg() {}
func (XLogData) logReplMsg()                {}
