package event

type DeleteEvent struct {
	BaseEvent

	OldData any
}

func (e *DeleteEvent) Operation() OperationType {
	return DeleteOp
}
