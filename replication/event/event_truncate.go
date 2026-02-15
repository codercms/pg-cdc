package event

import "github.com/jackc/pglogrepl"

type TruncateEvent struct {
	LSN pglogrepl.LSN

	Truncated []BaseEvent
}

func (e *TruncateEvent) Operation() OperationType {
	return TruncateOp
}

func (e *TruncateEvent) GetLSN() pglogrepl.LSN {
	return e.LSN
}
