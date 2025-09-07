package event

type OperationType uint8

const (
	InsertOp OperationType = iota + 1
	UpdateOp
	DeleteOp
	TruncateOp
)

func (o OperationType) String() string {
	switch o {
	case InsertOp:
		return "INSERT"
	case UpdateOp:
		return "UPDATE"
	case DeleteOp:
		return "DELETE"
	case TruncateOp:
		return "TRUNCATE"
	}

	return "unknown"
}
