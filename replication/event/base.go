package event

import (
	"github.com/jackc/pglogrepl"

	"github.com/codercms/pg-cdc/replication/types"
)

type Event interface {
	Operation() OperationType

	GetLSN() pglogrepl.LSN
}

type BaseEvent struct {
	Rel *types.TableInfo
	LSN pglogrepl.LSN
}

func (e *BaseEvent) GetLSN() pglogrepl.LSN {
	return e.LSN
}
