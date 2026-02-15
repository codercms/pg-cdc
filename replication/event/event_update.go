package event

type UpdateEvent struct {
	BaseEvent

	Data    any
	OldData any
}

func (e *UpdateEvent) Operation() OperationType {
	return UpdateOp
}
