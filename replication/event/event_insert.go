package event

type InsertEvent struct {
	BaseEvent

	Data any
}

func (e *InsertEvent) Operation() OperationType {
	return InsertOp
}
